from __future__ import annotations

import base64
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from config.settings import MINT, WSOL_MINT
from config.data_registy import KNOWN_TOKEN_LABELS
from core.amm import ConstantProductAMM
from core.models import MarketState


# ═════════════════════════════════════════════════════════════════════════════
# Instruction / program helpers
# ═════════════════════════════════════════════════════════════════════════════

def get_all_instructions(tx_data: Dict) -> List[Dict]:
    message = tx_data.get("transaction", {}).get("message", {})
    meta    = tx_data.get("meta", {})
    out     = list(message.get("instructions", []) or [])
    for group in meta.get("innerInstructions", []) or []:
        out.extend(group.get("instructions", []) or [])
    return out


def get_all_program_ids(tx_data: Dict) -> Set[str]:
    return {ix.get("programId") for ix in get_all_instructions(tx_data) if ix.get("programId")}


def get_signer_token_deltas(tx_data: Dict, signer: str) -> Dict[str, float]:
    meta = tx_data.get("meta", {})
    pre  = [b for b in (meta.get("preTokenBalances")  or []) if b.get("owner") == signer]
    post = [b for b in (meta.get("postTokenBalances") or []) if b.get("owner") == signer]

    pre_map:  Dict[Tuple[str, int], Tuple[int, int]] = {}
    post_map: Dict[Tuple[str, int], Tuple[int, int]] = {}

    for bal in pre:
        mint = bal.get("mint"); idx = bal.get("accountIndex")
        if mint is None or idx is None: continue
        amt = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
        dec = int((bal.get("uiTokenAmount") or {}).get("decimals") or 0)
        pre_map[(mint, idx)] = (amt, dec)

    for bal in post:
        mint = bal.get("mint"); idx = bal.get("accountIndex")
        if mint is None or idx is None: continue
        amt = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
        dec = int((bal.get("uiTokenAmount") or {}).get("decimals") or 0)
        post_map[(mint, idx)] = (amt, dec)

    deltas: Dict[str, float] = defaultdict(float)
    for mint, idx in set(pre_map) | set(post_map):
        pre_amt,  pre_dec  = pre_map.get( (mint, idx), (0, 0))
        post_amt, post_dec = post_map.get((mint, idx), (0, pre_dec))
        dec     = post_dec if post_dec else pre_dec
        divisor = 10 ** dec if dec >= 0 else 1
        deltas[mint] += (post_amt - pre_amt) / divisor

    return dict(deltas)


# ═════════════════════════════════════════════════════════════════════════════
# Quote price helpers
# ═════════════════════════════════════════════════════════════════════════════

def get_quote_price_usd(ms: MarketState) -> float:
    quote = (getattr(ms, "quote_symbol", "") or "").upper()
    if quote in {"USDC", "USDT"}:
        return 1.0
    if quote in {"SOL", "WSOL"} or getattr(ms, "quote_mint", "") == WSOL_MINT:
        return max(0.0, getattr(ms, "sol_price_usd", 0.0))
    return max(0.0, getattr(ms, "quote_to_usd", 0.0))


def usd_to_quote_amount(usd_value: float, ms: MarketState) -> float:
    quote_price_usd = get_quote_price_usd(ms)
    if usd_value <= 0 or quote_price_usd <= 0:
        return 0.0
    return usd_value / quote_price_usd


# ═════════════════════════════════════════════════════════════════════════════
# AMM projections
# ═════════════════════════════════════════════════════════════════════════════

def build_amm_from_market_state(ms: MarketState, fee_rate: float = 0.0025) -> Optional[ConstantProductAMM]:
    if (ms.pool_token_reserve <= 0 or ms.pool_sol_reserve <= 0
            or ms.total_supply <= 0 or ms.sol_price_usd <= 0):
        return None
    try:
        return ConstantProductAMM(
            token_reserve=ms.pool_token_reserve,
            sol_reserve=ms.pool_sol_reserve,
            total_supply=ms.total_supply,
            sol_price_usd=ms.sol_price_usd,
            fee_rate=fee_rate,
        )
    except Exception:
        return None


def project_limit_buy(quote_amount: float, ms: MarketState) -> Optional[Dict[str, float]]:
    if (quote_amount <= 0 or ms.pool_token_reserve <= 0
            or ms.pool_quote_reserve <= 0 or ms.total_supply <= 0):
        return None
    quote_to_usd = get_quote_price_usd(ms)
    if quote_to_usd <= 0:
        return None

    x = ms.pool_token_reserve; y = ms.pool_quote_reserve; k = x * y
    y_new = y + quote_amount; x_new = k / y_new
    if x_new <= 0 or x_new >= x:
        return None

    tokens_bought       = x - x_new
    price_new_quote     = y_new / x_new
    price_new_usd       = price_new_quote * quote_to_usd
    mcap_new            = price_new_usd * ms.total_supply
    current_price_quote = y / x
    price_impact_pct    = ((price_new_quote - current_price_quote) / current_price_quote * 100
                           if current_price_quote > 0 else 0.0)

    return {
        "tokens_bought":    tokens_bought,
        "quote_amount":     quote_amount,
        "new_price_usd":    price_new_usd,
        "new_mcap":         mcap_new,
        "price_impact_pct": price_impact_pct,
    }


def project_limit_sell(token_amount: float, ms: MarketState) -> Optional[Dict[str, float]]:
    if (token_amount <= 0 or ms.pool_token_reserve <= 0
            or ms.pool_quote_reserve <= 0 or ms.total_supply <= 0):
        return None
    quote_to_usd = get_quote_price_usd(ms)
    if quote_to_usd <= 0:
        return None

    x = ms.pool_token_reserve; y = ms.pool_quote_reserve; k = x * y
    x_new = x + token_amount; y_new = k / x_new
    if y_new <= 0 or y_new >= y:
        return None

    quote_received      = y - y_new
    price_new_quote     = y_new / x_new
    price_new_usd       = price_new_quote * quote_to_usd
    mcap_new            = price_new_usd * ms.total_supply
    current_price_quote = y / x
    price_impact_pct    = ((price_new_quote - current_price_quote) / current_price_quote * 100
                           if current_price_quote > 0 else 0.0)

    return {
        "quote_received":   quote_received,
        "token_amount":     token_amount,
        "new_price_usd":    price_new_usd,
        "new_mcap":         mcap_new,
        "price_impact_pct": price_impact_pct,
    }


# ═════════════════════════════════════════════════════════════════════════════
# MCap estimation for whale embeds
# ═════════════════════════════════════════════════════════════════════════════

def estimate_mcap_before_after_any_quote(
    current_mcap: float,
    usd_value: float,
    pool_quote_reserve_after: float,
    is_buy: bool,
    ms: MarketState,
) -> Dict[str, float]:
    """Estimate before/after mcap for a known trade size."""
    quote_price_usd = get_quote_price_usd(ms)
    quote_amount    = (usd_value / quote_price_usd) if quote_price_usd > 0 else 0.0

    # Reconstruct pre-trade quote reserve
    q_before = (pool_quote_reserve_after - quote_amount) if is_buy else (pool_quote_reserve_after + quote_amount)
    q_before = max(q_before, 1e-9)

    if pool_quote_reserve_after > 0 and q_before > 0:
        before_mcap = current_mcap * (q_before / pool_quote_reserve_after)
    else:
        before_mcap = current_mcap

    change_usd = current_mcap - before_mcap
    change_pct = (change_usd / before_mcap * 100) if before_mcap > 0 else 0.0

    return {
        "before_mcap":     before_mcap,
        "after_mcap":      current_mcap,
        "change_usd":      change_usd,
        "change_pct":      change_pct,
        "quote_amount":    quote_amount,
        "quote_price_usd": quote_price_usd,
    }
