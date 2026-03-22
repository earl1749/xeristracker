from __future__ import annotations

import base64
import json
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

from config.settings import (
    DEBUG_CHANNEL_ID, GROQ_API_KEY, GROQ_ENABLED, GROQ_MIN_CONFIDENCE,
    GROQ_MODEL, GROQ_URL, LEARNED_PROGRAMS_FILE, MINT, SUSPICION_THRESHOLD, WSOL_MINT,
)
from config.data_registy import (
    ALL_KNOWN_PROGRAMS, ALL_SWAP_PROGRAMS, AGGREGATOR_PROGRAMS,
    DEX_PROGRAMS, DISCRIMINATORS, EXCHANGE_REGISTRY, KNOWN_TOKEN_LABELS,
    LIMIT_ORDER_PROGRAMS, SWAP_PROGRAMS, SYSTEM_PROGRAMS, TOKEN_PROGRAMS,
    exchange_name,
)
from core.amm import ConstantProductAMM
from core.models import MarketState, OrderType
from helpers.discord_utils import send_message
from helpers.formatters import get_timestamp
from helpers.tx_utils import (
    build_amm_from_market_state, get_all_instructions, get_all_program_ids,
    get_quote_price_usd, get_signer_token_deltas, project_limit_buy,
    project_limit_sell, usd_to_quote_amount,
)
from utils.json_loader import load_learned_programs, save_learned_programs


# ═════════════════════════════════════════════════════════════════════════════
# Suspicion scorer
# ═════════════════════════════════════════════════════════════════════════════

class SuspicionScorer:
    def score(self, tx_data: Dict, signer: str) -> Tuple[float, List[str]]:
        signals: List[str] = []
        total   = 0.0
        meta    = tx_data.get("meta", {})
        message = tx_data.get("transaction", {}).get("message", {})
        logs    = meta.get("logMessages") or []
        ixs     = message.get("instructions", [])
        logs_lc = " ".join(logs).lower()

        if "limit" in logs_lc or "order" in logs_lc:
            total += 0.25; signals.append("log:limit/order")
        if "place" in logs_lc or "init" in logs_lc:
            total += 0.15; signals.append("log:place/init")
        if "cancel" in logs_lc:
            total += 0.20; signals.append("log:cancel")

        pre_sol  = meta.get("preBalances",  [])
        post_sol = meta.get("postBalances", [])
        idx = self._signer_index(tx_data, signer)
        if idx is not None and idx < len(pre_sol) and idx < len(post_sol):
            prog_ids = {ix.get("programId") for ix in ixs if ix.get("programId")}
            if (pre_sol[idx] - post_sol[idx] > 500_000_000
                    and self._token_delta(tx_data, signer) == 0
                    and not (prog_ids & DEX_PROGRAMS)):
                total += 0.20; signals.append("sol_locked:no_fill")

        from core.classifier import TokenFlowAnalyzer  # local import avoids circular
        analyzer = TokenFlowAnalyzer(MINT)
        analysis = analyzer.analyze_transaction(tx_data, signer)
        if not analysis["has_target_token_movement"] and not (set(analysis["programs_involved"]) & LIMIT_ORDER_PROGRAMS):
            total += 0.10; signals.append("no_target_movement")

        accounts: set = set()
        for ix in ixs:
            accts = ix.get("accounts", [])
            accounts.update(accts if isinstance(accts, list) else [])
        n = len(accounts)
        if   n >= 20: total += 0.25; signals.append(f"accounts:{n}(high)")
        elif n >= 10: total += 0.18; signals.append(f"accounts:{n}(med)")
        elif n >= 6:  total += 0.10; signals.append(f"accounts:{n}(low)")

        inner_programs: set = set()
        inner_ix_count = 0
        for g in (meta.get("innerInstructions") or []):
            for ix in g.get("instructions", []):
                inner_ix_count += 1
                pid = ix.get("programId")
                if pid: inner_programs.add(pid)
        if inner_programs & LIMIT_ORDER_PROGRAMS:
            total += 0.20; signals.append("inner:known_limit_program")
        if inner_ix_count >= 10:
            total += 0.15; signals.append(f"inner_ix_count:{inner_ix_count}")

        pre_tok  = meta.get("preTokenBalances",  []) or []
        post_tok = meta.get("postTokenBalances", []) or []
        new_accounts = {b.get("mint") for b in post_tok if b.get("mint")} - \
                       {b.get("mint") for b in pre_tok  if b.get("mint")}
        if new_accounts:
            total += 0.10; signals.append(f"new_ata:{len(new_accounts)}")

        all_pids     = {ix.get("programId") for ix in ixs if ix.get("programId")} | inner_programs
        unknown_pids = all_pids - ALL_KNOWN_PROGRAMS - SYSTEM_PROGRAMS
        if unknown_pids:
            total += 0.15; signals.append(f"unknown_programs:{len(unknown_pids)}")

        return min(total, 1.0), signals

    @staticmethod
    def _signer_index(tx_data: Dict, signer: str) -> Optional[int]:
        keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        for i, k in enumerate(keys):
            pub = k.get("pubkey") if isinstance(k, dict) else k
            if pub == signer: return i
        return None

    @staticmethod
    def _token_delta(tx_data: Dict, signer: str) -> int:
        meta = tx_data.get("meta", {})
        pre  = [b for b in (meta.get("preTokenBalances")  or []) if b.get("owner") == signer and b.get("mint") == MINT]
        post = [b for b in (meta.get("postTokenBalances") or []) if b.get("owner") == signer and b.get("mint") == MINT]
        pre_amt  = sum(int((b.get("uiTokenAmount") or {}).get("amount") or "0") for b in pre)
        post_amt = sum(int((b.get("uiTokenAmount") or {}).get("amount") or "0") for b in post)
        return post_amt - pre_amt


# ═════════════════════════════════════════════════════════════════════════════
# Token flow analyzer
# ═════════════════════════════════════════════════════════════════════════════

class TokenFlowAnalyzer:
    def __init__(self, target_mint: str):
        self.target_mint    = target_mint
        self.token_decimals: Dict[str, int] = {}
        self.token_symbols:  Dict[str, str] = KNOWN_TOKEN_LABELS.copy()

    def analyze_transaction(self, tx_data: Dict, user_wallet: str) -> Dict[str, Any]:
        movements         = self._collect_all_movements(tx_data, user_wallet)
        swap_info         = self._identify_swap_patterns(movements, user_wallet)
        programs_involved = self._get_programs_involved(tx_data)
        is_swap_related   = bool(programs_involved & ALL_SWAP_PROGRAMS) or swap_info["is_swap"]
        return {
            "movements":                movements,
            "swap_info":                swap_info,
            "programs_involved":        programs_involved,
            "is_swap_related":          is_swap_related,
            "target_token_change":      movements["by_mint"].get(self.target_mint, 0),
            "has_target_token_movement": abs(movements["by_mint"].get(self.target_mint, 0)) > 1e-6,
            "transaction_type":         self._determine_transaction_type(movements, swap_info, programs_involved),
        }

    def _collect_all_movements(self, tx_data: Dict, user_wallet: str) -> Dict[str, Any]:
        balance_movements  = self._get_balance_changes(tx_data, user_wallet)
        transfer_movements = self._get_transfer_movements(tx_data, user_wallet)
        log_movements      = self._parse_log_movements(tx_data, user_wallet)
        merged: Dict[str, float] = defaultdict(float)
        for src in (balance_movements, transfer_movements, log_movements):
            for mint, delta in src.items():
                if abs(delta) > 1e-6:
                    merged[mint] += delta
        result: Dict[str, Any] = {
            "by_mint":  dict(merged),
            "total_in": 0.0, "total_out": 0.0, "net": 0.0,
            "source_breakdown": {
                "balance_changes": balance_movements,
                "transfers":       transfer_movements,
                "logs":            log_movements,
            },
        }
        for mint, delta in merged.items():
            if delta > 0: result["total_in"]  += delta
            else:         result["total_out"] += abs(delta)
        result["net"] = result["total_in"] - result["total_out"]
        return result

    def _get_balance_changes(self, tx_data: Dict, user_wallet: str) -> Dict[str, float]:
        meta     = tx_data.get("meta", {})
        pre_all  = meta.get("preTokenBalances",  []) or []
        post_all = meta.get("postTokenBalances", []) or []
        pre_by_account:  Dict[int, dict] = {}
        post_by_account: Dict[int, dict] = {}
        for bal in pre_all:
            if bal.get("owner") != user_wallet: continue
            idx = bal.get("accountIndex"); mint = bal.get("mint")
            if idx is None or not mint: continue
            amt = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
            dec = int((bal.get("uiTokenAmount") or {}).get("decimals") or 6)
            self.token_decimals[mint] = dec
            pre_by_account[idx] = {"mint": mint, "amount": amt}
        for bal in post_all:
            if bal.get("owner") != user_wallet: continue
            idx = bal.get("accountIndex"); mint = bal.get("mint")
            if idx is None or not mint: continue
            amt = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
            dec = int((bal.get("uiTokenAmount") or {}).get("decimals") or 6)
            self.token_decimals[mint] = dec
            post_by_account[idx] = {"mint": mint, "amount": amt}
        changes: Dict[str, float] = defaultdict(float)
        for idx in set(pre_by_account) | set(post_by_account):
            pre  = pre_by_account.get(idx)
            post = post_by_account.get(idx)
            if pre and post and pre["mint"] == post["mint"]:
                delta = post["amount"] - pre["amount"]
                if delta:
                    dec = self.token_decimals.get(pre["mint"], 6)
                    changes[pre["mint"]] += delta / (10 ** dec)
            elif post:
                dec = self.token_decimals.get(post["mint"], 6)
                changes[post["mint"]] += post["amount"] / (10 ** dec)
            elif pre:
                dec = self.token_decimals.get(pre["mint"], 6)
                changes[pre["mint"]] -= pre["amount"] / (10 ** dec)
        return dict(changes)

    def _get_transfer_movements(self, tx_data: Dict, user_wallet: str) -> Dict[str, float]:
        meta      = tx_data.get("meta", {})
        message   = tx_data.get("transaction", {}).get("message", {})
        movements: Dict[str, float] = defaultdict(float)
        for ix in message.get("instructions", []):
            self._process_instruction_for_transfers(ix, user_wallet, movements, tx_data)
        for inner_group in meta.get("innerInstructions", []) or []:
            for ix in inner_group.get("instructions", []):
                self._process_instruction_for_transfers(ix, user_wallet, movements, tx_data)
        return dict(movements)

    def _process_instruction_for_transfers(self, ix: Dict, user_wallet: str,
                                            movements: Dict[str, float], tx_data: Dict):
        if ix.get("programId") not in TOKEN_PROGRAMS:
            return
        transfer_info = self._decode_token_transfer(ix, tx_data)
        if not transfer_info:
            return
        mint   = transfer_info.get("mint")
        amount = transfer_info.get("amount", 0)
        source = transfer_info.get("source")
        dest   = transfer_info.get("destination")
        if not mint or amount <= 0:
            return
        meta     = tx_data.get("meta", {})
        keys     = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        key_list = [k.get("pubkey") if isinstance(k, dict) else k for k in keys]
        account_owner: Dict[str, str] = {}
        account_mint:  Dict[str, str] = {}
        for bal in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
            idx = bal.get("accountIndex")
            if idx is None or idx >= len(key_list): continue
            token_acc = key_list[idx]
            if bal.get("owner"):   account_owner[token_acc] = bal["owner"]
            if bal.get("mint"):
                account_mint[token_acc] = bal["mint"]
                self.token_decimals[bal["mint"]] = int((bal.get("uiTokenAmount") or {}).get("decimals") or 6)
        source_owner = account_owner.get(source, source)
        dest_owner   = account_owner.get(dest,   dest)
        mint = mint or account_mint.get(source) or account_mint.get(dest)
        if not mint: return
        dec            = self.token_decimals.get(mint, 6)
        decimal_amount = amount / (10 ** dec)
        if source_owner == user_wallet: movements[mint] -= decimal_amount
        if dest_owner   == user_wallet: movements[mint] += decimal_amount

    def _decode_token_transfer(self, ix: Dict, tx_data: Dict) -> Optional[Dict]:
        data     = ix.get("data", "")
        accounts = ix.get("accounts", [])
        if not data or not accounts: return None
        raw = None
        try:
            _B58 = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
            _MAP = {c: i for i, c in enumerate(_B58)}
            n = 0
            for ch in data.encode():
                if ch not in _MAP: raise ValueError()
                n = n * 58 + _MAP[ch]
            pad = len(data) - len(data.lstrip("1"))
            raw = b"\x00" * pad + n.to_bytes((n.bit_length() + 7) // 8 or 1, "big")
        except Exception:
            try:   raw = base64.b64decode(data + "==")
            except Exception: return None
        if not raw or len(raw) < 9: return None
        disc = raw[0]
        if disc == 3 and len(accounts) >= 2:
            return {"type": "transfer", "amount": int.from_bytes(raw[1:9], "little"),
                    "source": accounts[0], "destination": accounts[1],
                    "mint": self._get_mint_for_account(accounts[0], tx_data)}
        if disc == 12 and len(accounts) >= 4:
            return {"type": "transfer_checked", "amount": int.from_bytes(raw[1:9], "little"),
                    "source": accounts[0], "destination": accounts[2], "mint": accounts[1]}
        return None

    def _get_mint_for_account(self, account: str, tx_data: Dict) -> Optional[str]:
        meta     = tx_data.get("meta", {})
        keys     = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        key_list = [k.get("pubkey") if isinstance(k, dict) else k for k in keys]
        try:    idx = key_list.index(account)
        except ValueError: return None
        for bal in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
            if bal.get("accountIndex") == idx: return bal.get("mint")
        return None

    def _parse_log_movements(self, tx_data: Dict, user_wallet: str) -> Dict[str, float]:
        return {}

    def _get_programs_involved(self, tx_data: Dict) -> Set[str]:
        return get_all_program_ids(tx_data)

    def _identify_swap_patterns(self, movements: Dict, user_wallet: str) -> Dict[str, Any]:
        by_mint      = movements["by_mint"]
        positive     = [m for m, d in by_mint.items() if d >  1e-6]
        negative     = [m for m, d in by_mint.items() if d < -1e-6]
        is_swap      = len(positive) >= 1 and len(negative) >= 1
        is_pure_swap = is_swap and abs(movements["net"]) < 1e-6
        main_in      = max(positive, key=lambda m: by_mint[m]) if positive else None
        main_out     = min(negative, key=lambda m: by_mint[m]) if negative else None
        main_in_amt  = by_mint[main_in]      if main_in  else 0.0
        main_out_amt = abs(by_mint[main_out]) if main_out else 0.0
        return {
            "is_swap": is_swap, "is_pure_swap": is_pure_swap,
            "positive_mints": positive, "negative_mints": negative,
            "main_in_token":  main_in,  "main_out_token": main_out,
            "main_in_amount": main_in_amt, "main_out_amount": main_out_amt,
            "estimated_price": main_out_amt / main_in_amt if main_in_amt > 0 else 0,
        }

    def _determine_transaction_type(self, movements: Dict, swap_info: Dict,
                                     programs_involved: Set[str]) -> str:
        target_change = movements["by_mint"].get(self.target_mint, 0)
        if programs_involved & LIMIT_ORDER_PROGRAMS:
            return "LIMIT_PLACEMENT" if abs(target_change) < 1e-6 else "LIMIT_FILL"
        if swap_info["is_swap"]:
            if target_change > 0: return "MARKET_BUY"
            if target_change < 0: return "MARKET_SELL"
        if len(movements["by_mint"]) == 1 and self.target_mint in movements["by_mint"]:
            return "TRANSFER"
        return "UNKNOWN"


# ═════════════════════════════════════════════════════════════════════════════
# Groq classify prompt builder
# ═════════════════════════════════════════════════════════════════════════════

def _build_classify_prompt(tx_data: Dict, signer: str,
                            suspicion_signals: List[str], sol_price_usd: float = 150.0) -> str:
    meta     = tx_data.get("meta", {})
    all_ixs  = get_all_instructions(tx_data)
    prog_ids = sorted({ix.get("programId") for ix in all_ixs if ix.get("programId")})
    deltas       = get_signer_token_deltas(tx_data, signer)
    target_delta = deltas.get(MINT, 0.0)

    negative_quotes = [(m, abs(d)) for m, d in deltas.items() if m != MINT and d < 0 and abs(d) > 1e-12]
    positive_quotes = [(m, abs(d)) for m, d in deltas.items() if m != MINT and d > 0 and abs(d) > 1e-12]

    def pick_quote(cands):
        if not cands: return None
        pref = ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", WSOL_MINT]
        for p in pref:
            for mint, _ in cands:
                if mint == p: return mint
        return max(cands, key=lambda x: x[1])[0]

    quote_out_mint = pick_quote(negative_quotes)
    quote_in_mint  = pick_quote(positive_quotes)

    signer_idx = None
    keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
    for i, k in enumerate(keys):
        pub = k.get("pubkey") if isinstance(k, dict) else k
        if pub == signer: signer_idx = i; break

    signer_sol_delta = 0.0; signer_sol_spent = 0.0
    pre_bal  = meta.get("preBalances",  [])
    post_bal = meta.get("postBalances", [])
    fee      = meta.get("fee", 5000)
    if signer_idx is not None and signer_idx < len(pre_bal) and signer_idx < len(post_bal):
        raw_delta        = post_bal[signer_idx] - pre_bal[signer_idx]
        signer_sol_delta = raw_delta / 1e9
        signer_sol_spent = max(0.0, (pre_bal[signer_idx] - post_bal[signer_idx] - fee)) / 1e9

    only_fee_paid = (abs(target_delta) < 1e-12 and not negative_quotes
                     and not positive_quotes and signer_sol_spent < 0.005)

    has_cancel = has_new_order = False
    for ix in all_ixs:
        raw = TransactionClassifier._decode_ix_data(ix.get("data", ""))
        if raw and len(raw) >= 8:
            disc = DISCRIMINATORS.get(raw[:8])
            if disc == "cancel_order": has_cancel = True
            elif disc == "new_order":  has_new_order = True
    logs_lc = " ".join(meta.get("logMessages") or []).lower()
    if "cancel" in logs_lc or "withdraw order" in logs_lc: has_cancel = True
    if any(k in logs_lc for k in ["new order", "place order", "limit"]): has_new_order = True

    known_market_hits = {pid: exchange_name(pid) for pid in prog_ids if pid in (ALL_SWAP_PROGRAMS | DEX_PROGRAMS)}
    known_limit_hits  = {pid: exchange_name(pid) for pid in prog_ids if pid in LIMIT_ORDER_PROGRAMS}
    swap_like  = bool(known_market_hits)
    limit_like = bool(known_limit_hits)

    quote_usd_locked = 0.0
    if quote_out_mint in ("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                          "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"):
        quote_usd_locked = abs(deltas.get(quote_out_mint, 0.0))
    elif quote_out_mint == WSOL_MINT:
        quote_usd_locked = abs(deltas.get(WSOL_MINT, 0.0)) * sol_price_usd
    elif signer_sol_spent > 0.005:
        quote_usd_locked = signer_sol_spent * sol_price_usd

    facts = {
        "signer": signer, "target_mint": MINT,
        "target_token_delta": round(target_delta, 6),
        "signer_sol_delta": round(signer_sol_delta, 6),
        "signer_sol_spent_beyond_fee": round(signer_sol_spent, 6),
        "tx_fee_sol": round(fee / 1e9, 9),
        "only_fee_paid": only_fee_paid,
        "signer_token_deltas": {KNOWN_TOKEN_LABELS.get(m, m[:8] + "…"): round(d, 6) for m, d in deltas.items()},
        "quote_out_token": KNOWN_TOKEN_LABELS.get(quote_out_mint, quote_out_mint[:8] + "…" if quote_out_mint else None),
        "quote_in_token":  KNOWN_TOKEN_LABELS.get(quote_in_mint,  quote_in_mint[:8]  + "…" if quote_in_mint  else None),
        "quote_usd_locked_in_escrow": round(quote_usd_locked, 4),
        "limit_order_programs_detected": known_limit_hits,
        "swap_programs_detected": known_market_hits,
        "all_program_ids": prog_ids,
        "swap_like": swap_like, "limit_like": limit_like,
        "has_cancel": has_cancel, "has_new_order": has_new_order,
        "suspicion_signals": suspicion_signals,
        "log_messages_sample": (meta.get("logMessages") or [])[:20],
    }
    return f"""You are a strict Solana transaction classifier for a token monitoring bot.

Classify into exactly one:
MARKET_BUY, MARKET_SELL, LIMIT_BUY, LIMIT_SELL, CANCEL_LIMIT, TRANSFER, UNKNOWN

Hard constraints:
- only_fee_paid == true → ALWAYS UNKNOWN
- target_token_delta == 0 → NEVER MARKET_BUY or MARKET_SELL
- limit_like == false and has_new_order == false → NEVER LIMIT_BUY or LIMIT_SELL
- has_cancel == true → prefer CANCEL_LIMIT

Facts:
{json.dumps(facts, indent=2)}

Return ONLY valid JSON:
{{"order_type":"...","confidence":0.0,"order_size_usd":0.0,"order_size_tokens":0.0,"quote_token":"...","exchange":"...","reason":"..."}}""".strip()


# ═════════════════════════════════════════════════════════════════════════════
# Transaction classifier
# ═════════════════════════════════════════════════════════════════════════════

class TransactionClassifier:
    def __init__(self) -> None:
        self._scorer  = SuspicionScorer()
        self._learned: Dict[str, Dict] = load_learned_programs(LEARNED_PROGRAMS_FILE)
        if self._learned:
            print(f"📚 Loaded {len(self._learned)} learned program(s)")

    def _known_role(self, pid: str) -> Optional[str]:
        if pid in EXCHANGE_REGISTRY: return EXCHANGE_REGISTRY[pid]["role"]
        if pid in self._learned:     return self._learned[pid]["role"]
        return None

    def _signer_sol_flows(self, tx_data: Dict, signer: str) -> Tuple[float, float]:
        meta  = tx_data.get("meta", {})
        keys  = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        si    = next((i for i, k in enumerate(keys)
                      if (k.get("pubkey") if isinstance(k, dict) else k) == signer), None)
        if si is None: return 0.0, 0.0
        pre_b  = meta.get("preBalances",  [])
        post_b = meta.get("postBalances", [])
        fee    = meta.get("fee", 5000)
        if si >= len(pre_b) or si >= len(post_b): return 0.0, 0.0
        diff = pre_b[si] - post_b[si]
        return max(0.0, (diff - fee)) / 1e9, max(0.0, (-diff - fee)) / 1e9

    def _derive_trade_value_from_flows(self, deltas: Dict[str, float], ms: MarketState,
                                        target_delta: float, sol_spent: float,
                                        sol_received: float) -> Tuple[float, str]:
        stablecoins = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
                       "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT"}
        for mint, symbol in stablecoins.items():
            amt = abs(deltas.get(mint, 0.0))
            if amt > 0: return amt, symbol
        wsol_amt = abs(deltas.get(WSOL_MINT, 0.0))
        if wsol_amt > 0: return wsol_amt * ms.sol_price_usd, "SOL"
        if sol_spent    > 0.001: return sol_spent    * ms.sol_price_usd, "SOL"
        if sol_received > 0.001: return sol_received * ms.sol_price_usd, "SOL"
        if abs(target_delta) > 0 and ms.current_price > 0:
            return abs(target_delta) * ms.current_price, ""
        return 0.0, ""

    def _learn(self, program_ids: set, order_type: OrderType,
               exchange: str, confidence: float) -> None:
        if confidence < 0.80: return
        if order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL, OrderType.CANCEL_LIMIT):
            role = "limit"
        elif order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL):
            role = "market"
        else:
            return
        candidates = [p for p in program_ids
                      if p not in EXCHANGE_REGISTRY and p not in SYSTEM_PROGRAMS
                      and p not in self._learned]
        if len(candidates) > 1:
            print(f"   📚 Skipping learn: {len(candidates)} unknown programs, ambiguous")
            return
        changed = False
        for pid in candidates:
            self._learned[pid] = {
                "name": exchange if exchange != "Unknown" else f"Learned ({pid[:8]}…)",
                "role": role, "confidence": confidence, "seen": 1,
            }
            print(f"   📚 Learned new program: {pid[:16]}… → {role} ({exchange})")
            changed = True
        if changed:
            save_learned_programs(self._learned, LEARNED_PROGRAMS_FILE)

    async def _handle_unknown_program(self, tx_data: Dict, signer: str, signature: str,
                                       suspicion: float, signals: List[str]) -> None:
        all_ixs  = get_all_instructions(tx_data)
        all_pids: set = set()
        for ix in all_ixs:
            pid = ix.get("programId")
            if pid: all_pids.add(pid)
        known   = ALL_KNOWN_PROGRAMS | set(self._learned.keys()) | SYSTEM_PROGRAMS
        unknown = all_pids - known
        if not unknown: return
        for pid in unknown:
            print(f"   ❓ Unknown program: {pid}  suspicion={suspicion:.2f}  signals={signals}")
        entry = {"timestamp": get_timestamp(), "signature": signature, "signer": signer,
                 "unknown_pids": list(unknown), "suspicion": suspicion, "signals": signals}
        try:
            with open("runtime/unknown_programs.jsonl", "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            print(f"   ⚠️ Could not log unknown program: {e}")
        if DEBUG_CHANNEL_ID:
            await send_message(DEBUG_CHANNEL_ID, embeds=[{
                "author": {"name": "❓ Unknown Program Detected"},
                "title":  "New unrecognized program in XERIS tx",
                "description": (
                    f"```yaml\nSignature:  {signature[:32]}...\nSigner:     {signer[:16]}...\n"
                    f"Suspicion:  {suspicion:.2f}\nSignals:    {', '.join(signals)}\nPrograms:\n"
                    + "\n".join(f"  - {pid}" for pid in unknown) + "\n```\n"
                    f"[Solscan TX](https://solscan.io/tx/{signature})"
                ),
                "color": 0xF59E0B, "timestamp": get_timestamp(),
            }])

    async def classify(self, tx_data: Dict, signer: str, ms: MarketState) -> Tuple[OrderType, Optional[Dict]]:
        meta     = tx_data.get("meta", {})
        pre_tok  = meta.get("preTokenBalances",  []) or []
        post_tok = meta.get("postTokenBalances", []) or []
        pre_bal  = meta.get("preBalances",  [])
        post_bal = meta.get("postBalances", [])
        fee      = meta.get("fee", 5000)
        keys     = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        si       = next((i for i, k in enumerate(keys)
                         if (k.get("pubkey") if isinstance(k, dict) else k) == signer), None)

        sol_beyond_fee = 0
        if si is not None and si < len(pre_bal) and si < len(post_bal):
            sol_beyond_fee = max(0, (pre_bal[si] - post_bal[si]) - fee)

        tok_changed = any(
            int((b.get("uiTokenAmount") or {}).get("amount") or 0)
            != int((next((p for p in post_tok if p.get("accountIndex") == b.get("accountIndex")), {})
                    .get("uiTokenAmount") or {}).get("amount") or 0)
            for b in pre_tok)

        programs = get_all_program_ids(tx_data)
        has_meaningful_program = bool(programs & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS | LIMIT_ORDER_PROGRAMS))
        if not tok_changed and sol_beyond_fee < 10_000 and not has_meaningful_program:
            return OrderType.UNKNOWN, None

        order_type, info = self._rule_based(tx_data, signer, ms)
        if order_type != OrderType.UNKNOWN:
            return order_type, info

        suspicion, signals = self._scorer.score(tx_data, signer)
        print(f"   🔍 Suspicion {suspicion:.2f}  [{', '.join(signals) or 'none'}]")
        if suspicion < SUSPICION_THRESHOLD:
            return OrderType.UNKNOWN, None

        await self._handle_unknown_program(tx_data, signer, tx_data.get("signature", ""), suspicion, signals)

        deltas       = get_signer_token_deltas(tx_data, signer)
        target_delta = deltas.get(MINT, 0.0)
        logs_lc      = " ".join(tx_data.get("meta", {}).get("logMessages") or []).lower()
        looks_limitish = bool(programs & LIMIT_ORDER_PROGRAMS) or any(
            k in logs_lc for k in ["limit", "order", "place order", "cancel"])
        if abs(target_delta) < 1e-12 and not looks_limitish:
            return OrderType.UNKNOWN, None

        if GROQ_ENABLED:
            order_type, info, conf = await self._groq_classify(tx_data, signer, ms, signals)
            MIN_CONF = {
                OrderType.MARKET_BUY:   0.75, OrderType.MARKET_SELL:  0.75,
                OrderType.LIMIT_BUY:    0.80, OrderType.LIMIT_SELL:   0.80,
                OrderType.CANCEL_LIMIT: 0.80, OrderType.TRANSFER:     0.85,
            }
            if order_type != OrderType.UNKNOWN and conf >= MIN_CONF.get(order_type, 0.99):
                should_learn = (
                    bool(programs & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS))
                    if order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL)
                    else (bool(programs & LIMIT_ORDER_PROGRAMS) or any(
                        k in logs_lc for k in ["limit", "order", "cancel"]))
                )
                if should_learn:
                    self._learn(programs, order_type, info.get("exchange", "Unknown"), conf)
                all_known = ALL_KNOWN_PROGRAMS | set(self._learned.keys())
                if (programs - all_known - SYSTEM_PROGRAMS
                        and order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL)):
                    print(f"   ⚠️ Groq said {order_type.value} but program unconfirmed — skipping")
                    return OrderType.UNKNOWN, None
                return order_type, info

        return OrderType.UNKNOWN, None

    def _rule_based(self, tx_data: Dict, signer: str, ms: MarketState) -> Tuple[OrderType, Optional[Dict]]:
        meta = tx_data.get("meta", {})
        if meta.get("err"):
            return OrderType.UNKNOWN, None

        programs     = get_all_program_ids(tx_data)
        all_ixs      = get_all_instructions(tx_data)
        deltas       = get_signer_token_deltas(tx_data, signer)
        target_delta = deltas.get(MINT, 0.0)
        sol_spent, sol_received = self._signer_sol_flows(tx_data, signer)

        learned_limit  = {p for p in programs if self._known_role(p) in ("limit",  "hybrid")}
        learned_market = {p for p in programs if self._known_role(p) in ("market", "hybrid")}
        limit_hits  = (programs & LIMIT_ORDER_PROGRAMS) | learned_limit
        market_hits = (programs & ALL_SWAP_PROGRAMS) | (programs & DEX_PROGRAMS) | learned_market

        has_cancel    = False
        has_new_order = False
        logs_lc = " ".join(meta.get("logMessages") or []).lower()

        for ix in all_ixs:
            raw = self._decode_ix_data(ix.get("data", ""))
            if raw and len(raw) >= 8:
                disc = DISCRIMINATORS.get(raw[:8])
                if disc == "cancel_order":  has_cancel = True
                elif disc == "new_order":   has_new_order = True

        if "cancel" in logs_lc or "withdraw order" in logs_lc:
            has_cancel = True
        if any(k in logs_lc for k in ["new order", "place order", "limit", "create order"]):
            has_new_order = True

        negative_quotes = [(m, abs(d)) for m, d in deltas.items() if m != MINT and d < 0 and abs(d) > 1e-12]
        positive_quotes = [(m, abs(d)) for m, d in deltas.items() if m != MINT and d > 0 and abs(d) > 1e-12]

        def pick_quote(cands):
            if not cands: return None
            pref = ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", WSOL_MINT]
            for p in pref:
                for mint, _ in cands:
                    if mint == p: return mint
            return max(cands, key=lambda x: x[1])[0]

        def quote_usd(mint, abs_amt):
            if not mint or abs_amt <= 0: return 0.0
            if mint in ("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"):
                return abs_amt
            if mint == WSOL_MINT: return abs_amt * ms.sol_price_usd
            return 0.0

        def ex_names(hits):
            return ", ".join(
                exchange_name(p) if p in EXCHANGE_REGISTRY
                else self._learned.get(p, {}).get("name", f"Learned ({p[:8]}…)")
                for p in sorted(hits)
            )

        # 1. CANCEL
        if limit_hits and has_cancel:
            return OrderType.CANCEL_LIMIT, {
                "wallet": signer, "signature": tx_data.get("signature", ""),
                "exchange": ex_names(limit_hits), "quote_token": "",
            }

        # 2. Executed market trade
        if abs(target_delta) > 1e-12:
            usd_value, quote_symbol = self._derive_trade_value_from_flows(
                deltas, ms, target_delta, sol_spent, sol_received)
            has_buy  = target_delta > 0 and (any(d < 0 for m, d in deltas.items() if m != MINT) or sol_spent  > 0.001)
            has_sell = target_delta < 0 and (any(d > 0 for m, d in deltas.items() if m != MINT) or sol_received > 0.001)
            if has_buy:
                return OrderType.MARKET_BUY,  {"wallet": signer, "amount": abs(target_delta),
                    "usd_value": usd_value, "exchange": ex_names(market_hits | limit_hits), "quote_token": quote_symbol}
            if has_sell:
                return OrderType.MARKET_SELL, {"wallet": signer, "amount": abs(target_delta),
                    "usd_value": usd_value, "exchange": ex_names(market_hits | limit_hits), "quote_token": quote_symbol}

        # 3. LIMIT BUY
        if limit_hits and abs(target_delta) < 1e-12:
            has_placement = has_new_order or any(k in logs_lc for k in ["place", "init", "create", "order", "limit"])
            if has_placement:
                quote_mint = pick_quote(negative_quotes)
                usd_value  = quote_usd(quote_mint, abs(deltas.get(quote_mint, 0.0)))
                if usd_value < 5.0:
                    s_sol, _ = self._signer_sol_flows(tx_data, signer)
                    if s_sol > 0.02:
                        usd_value = s_sol * ms.sol_price_usd
                quote_amount = usd_to_quote_amount(usd_value, ms)
                proj = project_limit_buy(quote_amount, ms)
                if not proj: return OrderType.UNKNOWN, None
                return OrderType.LIMIT_BUY, {
                    "wallet": signer, "amount": proj["tokens_bought"], "usd_value": usd_value,
                    "target_price": proj["new_price_usd"], "predicted_mcap": proj["new_mcap"],
                    "exchange": ex_names(limit_hits),
                    "quote_token": KNOWN_TOKEN_LABELS.get(quote_mint,
                        f"{quote_mint[:8]}…" if quote_mint else (ms.quote_symbol or "")),
                }

        # 4. LIMIT SELL
        if limit_hits and target_delta < 0 and not market_hits:
            _, sol_rcv = self._signer_sol_flows(tx_data, signer)
            if sol_rcv > 0.001: return OrderType.UNKNOWN, None
            has_placement = has_new_order or any(k in logs_lc for k in ["place", "init", "create", "order", "limit"])
            if has_placement:
                amount    = abs(target_delta)
                if amount <= 0: return OrderType.UNKNOWN, None
                usd_value = amount * ms.current_price
                if usd_value < 5.0: return OrderType.UNKNOWN, None
                proj = project_limit_sell(amount, ms)
                if not proj: return OrderType.UNKNOWN, None
                return OrderType.LIMIT_SELL, {
                    "wallet": signer, "amount": amount, "usd_value": usd_value,
                    "target_price": proj["new_price_usd"], "predicted_mcap": proj["new_mcap"],
                    "exchange": ex_names(limit_hits), "quote_token": ms.quote_symbol or "",
                }

        # 5. TRANSFER
        non_target = [m for m, d in deltas.items() if m != MINT and abs(d) > 1e-12]
        if abs(target_delta) > 1e-12 and not limit_hits and not market_hits and not non_target:
            return OrderType.TRANSFER, {
                "wallet": signer, "amount": abs(target_delta),
                "usd_value": abs(target_delta) * ms.current_price, "to": "unknown", "quote_token": "",
            }

        return OrderType.UNKNOWN, None

    async def _groq_classify(self, tx_data: Dict, signer: str, ms: MarketState,
                              signals: List[str]) -> Tuple[OrderType, Optional[Dict], float]:
        try:
            prompt = _build_classify_prompt(tx_data, signer, signals, sol_price_usd=ms.sol_price_usd)
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(GROQ_URL,
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                    json={"model": GROQ_MODEL, "max_tokens": 256, "temperature": 0.0,
                          "messages": [
                              {"role": "system", "content": "You are a strict Solana transaction classifier. Respond only with valid JSON."},
                              {"role": "user",   "content": prompt}]})
            if resp.status_code != 200:
                print(f"   ⚡ Groq {resp.status_code}")
                return OrderType.UNKNOWN, None, 0.0
            raw    = resp.json()["choices"][0]["message"]["content"].strip()
            parsed = json.loads(raw.replace("```json", "").replace("```", "").strip())
            ai_type          = parsed.get("order_type", "UNKNOWN").upper()
            confidence       = float(parsed.get("confidence", 0))
            exchange         = parsed.get("exchange", "Unknown")
            groq_size_usd    = float(parsed.get("order_size_usd",    0) or 0)
            groq_size_tokens = float(parsed.get("order_size_tokens", 0) or 0)
            groq_quote_token = parsed.get("quote_token", "")
            print(f"   ⚡ Groq: {ai_type}  conf={confidence:.2f}  ${groq_size_usd:.2f}  via {exchange}")
            try:    order_type = OrderType(ai_type)
            except ValueError: return OrderType.UNKNOWN, None, 0.0

            deltas       = get_signer_token_deltas(tx_data, signer)
            target_delta = deltas.get(MINT, 0.0)
            token_result = self._parse_token_changes(tx_data, signer)
            logs_lc      = " ".join(tx_data.get("meta", {}).get("logMessages") or []).lower()

            # Hard validation
            if order_type == OrderType.MARKET_BUY  and (target_delta <= 0 or not token_result or token_result[0] != "BUY"):
                return OrderType.UNKNOWN, None, 0.0
            if order_type == OrderType.MARKET_SELL and (target_delta >= 0 or not token_result or token_result[0] != "SELL"):
                return OrderType.UNKNOWN, None, 0.0
            if order_type == OrderType.TRANSFER:
                if abs(target_delta) < 1e-12 or not token_result or token_result[0] != "TRANSFER":
                    return OrderType.UNKNOWN, None, 0.0
                programs = get_all_program_ids(tx_data)
                if programs & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS | LIMIT_ORDER_PROGRAMS):
                    return OrderType.UNKNOWN, None, 0.0
            if order_type == OrderType.CANCEL_LIMIT:
                all_ixs    = get_all_instructions(tx_data)
                has_cancel = "cancel" in logs_lc or "withdraw order" in logs_lc
                if not has_cancel:
                    for ix in all_ixs:
                        raw_ix = self._decode_ix_data(ix.get("data", ""))
                        if raw_ix and len(raw_ix) >= 8 and DISCRIMINATORS.get(raw_ix[:8]) == "cancel_order":
                            has_cancel = True; break
                if not has_cancel: return OrderType.UNKNOWN, None, 0.0

            amount       = token_result[1] if token_result else 0.0
            quote_token  = (token_result[3] if token_result else "") or ""
            info: dict   = {"wallet": signer, "amount": amount,
                             "usd_value": amount * ms.current_price,
                             "exchange": exchange, "quote_token": quote_token}

            if order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL):
                info["amount"]    = groq_size_tokens if groq_size_tokens > 0 else amount
                info["usd_value"] = groq_size_usd    if groq_size_usd    > 0 else info["amount"] * ms.current_price
                if info["amount"] <= 0 or info["usd_value"] <= 0:
                    return OrderType.UNKNOWN, None, 0.0
                amm = build_amm_from_market_state(ms)
                if not amm or amm.token_reserve <= 0 or amm.sol_reserve <= 0:
                    return OrderType.UNKNOWN, None, 0.0
                if order_type == OrderType.LIMIT_BUY:
                    R_token = amm.token_reserve; R_sol = amm.sol_reserve; k = R_token * R_sol
                    new_R_token = R_token - info["amount"]
                    if new_R_token <= 0: return OrderType.UNKNOWN, None, 0.0
                    new_R_sol       = k / new_R_token
                    new_price_sol   = new_R_sol / new_R_token
                    new_price_usd   = new_price_sol * ms.sol_price_usd
                    new_mcap        = new_price_usd * ms.total_supply if ms.total_supply > 0 else ms.current_market_cap * (new_price_usd / ms.current_price)
                    info["target_price"]   = new_price_usd
                    info["predicted_mcap"] = new_mcap
                else:
                    proj = amm.sell_tokens(info["amount"])
                    info["target_price"]   = proj.new_price
                    info["predicted_mcap"] = proj.new_market_cap_usd

            return order_type, info, confidence

        except Exception as e:
            print(f"   ⚡ Groq error: {e}")
            return OrderType.UNKNOWN, None, 0.0

    @staticmethod
    def _decode_ix_data(data: str) -> Optional[bytes]:
        if not data: return None
        _B58_ALPHA = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        _B58_MAP   = {c: i for i, c in enumerate(_B58_ALPHA)}
        try:
            n = 0
            for ch in data.encode():
                if ch not in _B58_MAP: raise ValueError()
                n = n * 58 + _B58_MAP[ch]
            pad = len(data) - len(data.lstrip("1"))
            return b"\x00" * pad + n.to_bytes((n.bit_length() + 7) // 8 or 1, "big")
        except Exception:
            pass
        try:   return base64.b64decode(data + "==")
        except Exception: return None

    def _parse_token_changes(self, tx_data: Dict, signer: str) -> Optional[Tuple]:
        deltas       = get_signer_token_deltas(tx_data, signer)
        target_delta = deltas.get(MINT, 0.0)
        if abs(target_delta) < 1e-12: return None
        programs   = get_all_program_ids(tx_data)
        swap_like  = bool(programs & ALL_SWAP_PROGRAMS or programs & DEX_PROGRAMS)
        limit_like = bool(programs & LIMIT_ORDER_PROGRAMS)
        neg = [(m, abs(d)) for m, d in deltas.items() if m != MINT and d < 0 and abs(d) > 1e-12]
        pos = [(m, abs(d)) for m, d in deltas.items() if m != MINT and d > 0 and abs(d) > 1e-12]
        def pick(cands):
            pref = ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", WSOL_MINT]
            for p in pref:
                for mint, _ in cands:
                    if mint == p: return mint
            return max(cands, key=lambda x: x[1])[0] if cands else None
        if target_delta > 0:
            qm = pick(neg); qs = KNOWN_TOKEN_LABELS.get(qm, f"{qm[:8]}…" if qm else "SOL")
            if swap_like:  return ("BUY",      target_delta,       signer, qs)
            if not limit_like: return ("TRANSFER", target_delta,   signer, "unknown")
        if target_delta < 0:
            qm = pick(pos); qs = KNOWN_TOKEN_LABELS.get(qm, f"{qm[:8]}…" if qm else "SOL")
            if swap_like:  return ("SELL",     abs(target_delta),  signer, qs)
            if not limit_like: return ("TRANSFER", abs(target_delta), signer, "unknown")
        return None
