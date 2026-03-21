from __future__ import annotations
import asyncio
import base64
import io
import json
import random
import re
import sqlite3
import time
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from core.x_rss_monitor import x_post_monitor
import httpx
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import websockets

matplotlib.use("Agg")

# ── Internal modules ──────────────────────────────────────────────────────────
from config.settings import (
    ALERT_CHANNEL_ID, CHART_COOLDOWN_SECONDS, CHART_WAIT_MESSAGE_DELETE_SECONDS,
    CLEANUP_INTERVAL, DB_PATH, DEBUG_CHANNEL_ID, DEV_WALLET, DISCORD_API,
    DISCORD_TOKEN, GATEWAY_URL, GROQ_API_KEY, GROQ_ENABLED, GROQ_MIN_CONFIDENCE,
    GROQ_MODEL, GROQ_URL, HELIUS_API_KEY, LEARNED_PROGRAMS_FILE, MINT,
    ORDER_TTL_HOURS, ORDER_TTL_SECS, PRICE_ALERT_COOLDOWN, PRICE_ALERT_THRESHOLD,
    PRICE_UPDATE_INTERVAL, RPC_URL, SUMMARY_ALERT_INTERVAL, SUSPICION_THRESHOLD,
    VALID_CA, WHALE_MIN_USD, WS_URL, WSOL_MINT,
)
from config.data_registy import (
    ALL_KNOWN_PROGRAMS, ALL_SWAP_PROGRAMS, AGGREGATOR_PROGRAMS,
    DEX_PROGRAMS, DISCRIMINATORS, EXCHANGE_REGISTRY, KNOWN_TOKEN_LABELS,
    LIMIT_ORDER_PROGRAMS, SWAP_PROGRAMS, SYSTEM_PROGRAMS, TOKEN_PROGRAMS,
    exchange_name,
)
from core.amm import ConstantProductAMM
from core.models import AMMTradeProjection, LimitOrder, MarketState, OrderType
from utils.json_loader import load_learned_programs, save_learned_programs

# ── Timeframe config for charts ───────────────────────────────────────────────
TIMEFRAME_MAP = {
    "1m":  {"label": "1M",  "gt_timeframe": "minute", "aggregate": 1,  "resolution": 1},
    "5m":  {"label": "5M",  "gt_timeframe": "minute", "aggregate": 5,  "resolution": 5},
    "15m": {"label": "15M", "gt_timeframe": "minute", "aggregate": 15, "resolution": 15},
    "1h":  {"label": "1H",  "gt_timeframe": "hour",   "aggregate": 1,  "resolution": 60},
    "1d":  {"label": "1D",  "gt_timeframe": "day",    "aggregate": 1,  "resolution": 1440},
}

# ── Chart cooldown state ──────────────────────────────────────────────────────
_chart_cooldowns:    Dict[str, float]          = {}
_chart_pending_jobs: Dict[str, asyncio.Task]   = {}

# ── Global references (set by helius_monitor) ────────────────────────────────
_db_ref: Optional["DatabaseManager"] = None
_ms_ref: Optional[MarketState]       = None


# ═════════════════════════════════════════════════════════════════════════════
# Utility helpers
# ═════════════════════════════════════════════════════════════════════════════

def get_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def format_usd(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value/1_000:.2f}K"
    return f"${value:,.2f}"

def format_tokens(amount: float) -> str:
    if amount >= 1_000_000:
        return f"{amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"{amount/1_000:.2f}K"
    return f"{amount:,.0f}"

def _pct_from_current(mcap: float, ms: MarketState) -> float:
    if ms.current_market_cap > 0 and mcap > 0:
        return (mcap - ms.current_market_cap) / ms.current_market_cap * 100
    return 0.0

def risk_score_bar(score: int) -> str:
    bar = "█" * score + "░" * (10 - score)
    return f"`{bar}` **{score}/10**"

def rug_label_emoji(label: str) -> str:
    u = label.upper()
    if "LIKELY SAFE" in u: return "🟢"
    if "CAUTION"     in u: return "🟡"
    if "HIGH RISK"   in u: return "🔴"
    if "LIKELY RUG"  in u: return "💀"
    return "⚪"

def score_to_color(score: int) -> int:
    if score <= 3: return 0x10B981
    if score <= 5: return 0xF59E0B
    if score <= 7: return 0xF97316
    return 0xEF4444

def _format_placed_at(order: Dict) -> str:
    ts = order.get("timestamp", 0)
    if not ts:
        return "Unknown"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def _format_expires_at(order: Dict) -> str:
    ts = order.get("timestamp", 0)
    if not ts:
        return "Unknown"
    return datetime.fromtimestamp(ts + ORDER_TTL_SECS, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def _format_time_remaining(order: Dict) -> str:
    placed_at  = order.get("timestamp", 0)
    expires_at = placed_at + ORDER_TTL_SECS
    remaining  = expires_at - time.time()
    if remaining <= 0:
        return "⚠️ Expired"
    days    = int(remaining // 86400)
    hours   = int((remaining % 86400) // 3600)
    minutes = int((remaining % 3600) // 60)
    if days > 0:
        return f"{days}d {hours}h left"
    if hours > 0:
        return f"{hours}h {minutes}m left"
    return f"⚠️ {minutes}m left"

def _format_time_placed(order: Dict) -> str:
    ts = order.get("timestamp", 0)
    if not ts:
        return "Unknown"
    elapsed = time.time() - ts
    if elapsed < 3600:
        return f"{int(elapsed // 60)}m ago"
    if elapsed < 86400:
        return f"{int(elapsed // 3600)}h {int((elapsed % 3600) // 60)}m ago"
    return f"{int(elapsed // 86400)}d {int((elapsed % 86400) // 3600)}h ago"

def _expiry_bar(order: Dict) -> str:
    placed_at = order.get("timestamp", 0)
    elapsed   = time.time() - placed_at
    pct_used  = min(elapsed / ORDER_TTL_SECS, 1.0)
    filled    = max(0, 10 - round(pct_used * 10))
    empty     = 10 - filled
    return "█" * filled + "░" * empty

def _pick_best_pair(pairs: List[Dict]) -> Optional[Dict]:
    if not pairs:
        return None
    return max(
        pairs,
        key=lambda p: (
            float((p.get("liquidity") or {}).get("usd") or 0),
            float((p.get("volume")    or {}).get("h24") or 0),
            float(p.get("fdv") or p.get("marketCap") or 0),
        ),
    )

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

# ═════════════════════════════════════════════════════════════════════════════
# Transaction helpers
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
# Discord queue & messaging
# ═════════════════════════════════════════════════════════════════════════════

class DiscordQueue:
    CAPACITY      = 25
    REFILL_PERIOD = 30.0

    def __init__(self) -> None:
        self._queue: asyncio.Queue = asyncio.Queue()
        self._tokens   = float(self.CAPACITY)
        self._last_ref = time.monotonic()
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def enqueue(self, channel_id: int, payload: dict) -> None:
        await self._queue.put((channel_id, payload))

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(
            self.CAPACITY,
            self._tokens + (now - self._last_ref) / self.REFILL_PERIOD * self.CAPACITY,
        )
        self._last_ref = now

    async def _worker(self) -> None:
        while True:
            channel_id, payload = await self._queue.get()
            self._refill()
            if self._tokens < 1:
                await asyncio.sleep((1 - self._tokens) / self.CAPACITY * self.REFILL_PERIOD)
                self._refill()
            self._tokens -= 1
            await _send_message_direct(channel_id, payload)
            self._queue.task_done()


_discord_queue: Optional[DiscordQueue] = None


def init_discord_queue() -> None:
    global _discord_queue
    _discord_queue = DiscordQueue()
    _discord_queue.start()


async def _discord_request(method: str, path: str, **kwargs):
    headers = {
        "Authorization": f"Bot {DISCORD_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "XerisBot/2.0",
    }
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        return await client.request(
            method,
            f"{DISCORD_API}{path}",
            headers=headers,
            **kwargs,
        )


async def _send_message_direct(channel_id: int, payload: dict, max_retries: int = 5) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            r = await _discord_request(
                "POST",
                f"/channels/{channel_id}/messages",
                json=payload,
            )

            if r.status_code in (200, 201):
                print("   ✅ Message sent")
                return True

            if r.status_code == 429:
                retry_after = 2.0
                try:
                    data = r.json()
                    retry_after = float(data.get("retry_after", retry_after))
                    global_rl = bool(data.get("global", False))
                    print(
                        f"   ⚠️ Discord 429 rate limit "
                        f"(global={global_rl}) retry_after={retry_after:.2f}s"
                    )
                except Exception:
                    text_preview = (r.text or "")[:200].replace("\n", " ")
                    print(f"   ⚠️ Discord 429 non-JSON response: {text_preview}")

                await asyncio.sleep(retry_after + 0.25)
                continue

            text_preview = (r.text or "")[:200].replace("\n", " ")
            print(f"   ❌ Discord {r.status_code}: {text_preview}")
            return False

        except Exception as e:
            print(f"   ❌ Discord send error (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(min(2 * attempt, 5))

    print("   ❌ Message send failed after retries")
    return False


async def send_message(
    channel_id: int,
    content: str = None,
    embeds: list = None,
    mention_everyone: bool = False,
) -> bool:
    payload: dict = {}

    parts = []
    if mention_everyone:
        parts.append("@everyone")
    if content:
        cleaned = content.strip()
        if cleaned:
            parts.append(cleaned)

    if parts:
        payload["content"] = " ".join(parts)

    if embeds:
        payload["embeds"] = embeds

    if not payload:
        print("   ⚠️ send_message skipped: empty payload")
        return False

    if _discord_queue:
        await _discord_queue.enqueue(channel_id, payload)
        return True

    return await _send_message_direct(channel_id, payload)


async def send_typing(channel_id: int) -> None:
    try:
        r = await _discord_request("POST", f"/channels/{channel_id}/typing")
        if r.status_code not in (200, 204):
            print(f"   ⚠️ Typing failed: {r.status_code} {(r.text or '')[:120]}")
    except Exception as e:
        print(f"   ⚠️ Typing error: {e}")


async def delete_message(channel_id: int, message_id: int) -> bool:
    try:
        r = await _discord_request("DELETE", f"/channels/{channel_id}/messages/{message_id}")

        if r.status_code in (200, 204):
            return True

        if r.status_code == 429:
            retry_after = 2.0
            try:
                data = r.json()
                retry_after = float(data.get("retry_after", retry_after))
            except Exception:
                pass

            await asyncio.sleep(retry_after + 0.25)
            r = await _discord_request("DELETE", f"/channels/{channel_id}/messages/{message_id}")
            return r.status_code in (200, 204)

        print(f"   ⚠️ Delete failed: {r.status_code} {(r.text or '')[:150]}")
        return False

    except Exception as e:
        print(f"   ⚠️ Delete error: {e}")
        return False


async def send_message_get_id(
    channel_id: int,
    content: str = None,
    embeds: list = None,
    mention_everyone: bool = False,
    max_retries: int = 5,
) -> Optional[int]:
    payload: dict = {}

    parts = []
    if mention_everyone:
        parts.append("@everyone")
    if content:
        cleaned = content.strip()
        if cleaned:
            parts.append(cleaned)

    if parts:
        payload["content"] = " ".join(parts)

    if embeds:
        payload["embeds"] = embeds

    if not payload:
        print("   ⚠️ send_message_get_id skipped: empty payload")
        return None

    for attempt in range(1, max_retries + 1):
        try:
            r = await _discord_request(
                "POST",
                f"/channels/{channel_id}/messages",
                json=payload,
            )

            if r.status_code in (200, 201):
                try:
                    return int(r.json()["id"])
                except Exception:
                    print("   ⚠️ Message sent but failed to parse message ID")
                    return None

            if r.status_code == 429:
                retry_after = 2.0
                try:
                    data = r.json()
                    retry_after = float(data.get("retry_after", retry_after))
                    global_rl = bool(data.get("global", False))
                    print(
                        f"   ⚠️ Discord 429 rate limit "
                        f"(global={global_rl}) retry_after={retry_after:.2f}s"
                    )
                except Exception:
                    text_preview = (r.text or "")[:200].replace("\n", " ")
                    print(f"   ⚠️ Discord 429 non-JSON response: {text_preview}")

                await asyncio.sleep(retry_after + 0.25)
                continue

            print(f"   ❌ Discord {r.status_code}: {(r.text or '')[:150]}")
            return None

        except Exception as e:
            print(f"   ❌ Discord send_get_id error (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(min(2 * attempt, 5))

    print("   ❌ send_message_get_id failed after retries")
    return None


async def send_temp_message(
    channel_id: int,
    content: str = None,
    embeds: list = None,
    delete_after: int = 8,
    mention_everyone: bool = False,
) -> None:
    msg_id = await send_message_get_id(
        channel_id,
        content=content,
        embeds=embeds,
        mention_everyone=mention_everyone,
    )
    if not msg_id:
        return

    async def _auto_delete():
        try:
            await asyncio.sleep(delete_after)
            await delete_message(channel_id, msg_id)
        except Exception as e:
            print(f"⚠️ Temp delete error: {e}")

    asyncio.create_task(_auto_delete())

# ═════════════════════════════════════════════════════════════════════════════
# Database
# ═════════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    async def _exec(self, sql: str, params: tuple = ()) -> None:
        def _go():
            c = self._get_conn(); c.execute(sql, params); c.commit()
        await asyncio.to_thread(_go)

    async def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        def _go():
            c = self._get_conn()
            return [dict(r) for r in c.execute(sql, params).fetchall()]
        return await asyncio.to_thread(_go)

    async def _fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        def _go():
            c = self._get_conn()
            row = c.execute(sql, params).fetchone()
            return dict(row) if row else None
        return await asyncio.to_thread(_go)

    async def initialize(self) -> None:
        def _go():
            c = self._get_conn()
            c.execute("""
                CREATE TABLE IF NOT EXISTS limit_orders (
                    signature      TEXT PRIMARY KEY,
                    wallet         TEXT NOT NULL,
                    order_type     TEXT NOT NULL,
                    token_amount   REAL NOT NULL,
                    usd_value      REAL NOT NULL,
                    predicted_mcap REAL NOT NULL,
                    target_price   REAL NOT NULL DEFAULT 0,
                    quote_token    TEXT NOT NULL DEFAULT '',
                    exchange       TEXT NOT NULL DEFAULT '',
                    timestamp      REAL NOT NULL,
                    is_active      INTEGER NOT NULL DEFAULT 1,
                    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
            c.execute("""
                CREATE TABLE IF NOT EXISTS x_watch_state (
                    username       TEXT PRIMARY KEY,
                    user_id        TEXT NOT NULL DEFAULT '',
                    last_post_id   TEXT NOT NULL DEFAULT '',
                    last_post_time TEXT NOT NULL DEFAULT '',
                    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
            for col, defn in [("quote_token", "TEXT NOT NULL DEFAULT ''"),
                               ("exchange",    "TEXT NOT NULL DEFAULT ''")]:
                try:
                    c.execute(f"ALTER TABLE limit_orders ADD COLUMN {col} {defn}")
                except Exception:
                    pass
            c.execute("CREATE INDEX IF NOT EXISTS idx_wallet_active ON limit_orders(wallet, is_active)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_mcap_active   ON limit_orders(predicted_mcap, is_active)")
            c.commit()
        await asyncio.to_thread(_go)
        print("✅ Database initialized")

    async def close(self) -> None:
        def _go():
            if self._conn:
                self._conn.close()
        await asyncio.to_thread(_go)
        self._conn = None

    async def upsert_limit_order(self, order: LimitOrder,
                                  quote_token: str = "", exchange: str = "") -> None:
        if order.token_amount <= 0 or order.usd_value <= 0:
            print(f"   ⚠️ Skipping zero-value limit order from {order.wallet[:8]}…")
            return
        await self._exec("""
            INSERT OR REPLACE INTO limit_orders
              (signature, wallet, order_type, token_amount, usd_value,
               predicted_mcap, target_price, quote_token, exchange, timestamp, is_active)
            VALUES (?,?,?,?,?,?,?,?,?,?,1)
        """, (order.signature, order.wallet, order.order_type.value,
              order.token_amount, order.usd_value, order.predicted_mcap,
              order.target_price, quote_token, exchange, order.timestamp))

    async def deactivate_by_signature(self, sig: str) -> None:
        await self._exec("UPDATE limit_orders SET is_active = 0 WHERE signature = ?", (sig,))

    async def deactivate_one_by_wallet(self, wallet: str, order_type: str) -> Optional[Dict]:
        row = await self._fetchone("""
            SELECT * FROM limit_orders
            WHERE wallet = ? AND order_type = ? AND is_active = 1
            ORDER BY timestamp DESC LIMIT 1
        """, (wallet, order_type))
        if row:
            await self._exec("UPDATE limit_orders SET is_active = 0 WHERE signature = ?",
                             (row["signature"],))
        return row

    async def cleanup_stale(self, max_age_hours: int = ORDER_TTL_HOURS) -> int:
        cutoff = time.time() - max_age_hours * 3600
        rows = await self._fetchall(
            "SELECT * FROM limit_orders WHERE timestamp < ? AND is_active = 1", (cutoff,))
        if rows:
            await self._exec(
                "UPDATE limit_orders SET is_active = 0 WHERE timestamp < ? AND is_active = 1",
                (cutoff,))
            print(f"🧹 Cleaned {len(rows)} stale order(s) older than {max_age_hours}h")
        return len(rows)

    async def get_active_orders(self) -> List[Dict]:
        return await self._fetchall(
            "SELECT * FROM limit_orders WHERE is_active = 1 ORDER BY predicted_mcap ASC")

    async def get_orders_by_wallet(self, wallet: str) -> List[Dict]:
        return await self._fetchall(
            "SELECT * FROM limit_orders WHERE wallet = ? AND is_active = 1", (wallet,))

    async def get_x_watch_state(self, username: str) -> Optional[Dict]:
        return await self._fetchone(
            "SELECT * FROM x_watch_state WHERE username = ?", (username.lower(),))

    async def upsert_x_watch_state(self, username: str, user_id: str,
                                    last_post_id: str, last_post_time: str = "") -> None:
        await self._exec("""
            INSERT INTO x_watch_state (username, user_id, last_post_id, last_post_time, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username) DO UPDATE SET
                user_id = excluded.user_id,
                last_post_id = excluded.last_post_id,
                last_post_time = excluded.last_post_time,
                updated_at = CURRENT_TIMESTAMP
        """, (username.lower(), user_id, last_post_id, last_post_time))


# ═════════════════════════════════════════════════════════════════════════════
# Token flow analysis
# ═════════════════════════════════════════════════════════════════════════════

class TokenFlowAnalyzer:
    def __init__(self, target_mint: str):
        self.target_mint   = target_mint
        self.token_decimals: Dict[str, int] = {}
        self.token_symbols:  Dict[str, str] = KNOWN_TOKEN_LABELS.copy()

    def analyze_transaction(self, tx_data: Dict, user_wallet: str) -> Dict[str, Any]:
        movements        = self._collect_all_movements(tx_data, user_wallet)
        swap_info        = self._identify_swap_patterns(movements, user_wallet)
        programs_involved = self._get_programs_involved(tx_data)
        is_swap_related  = bool(programs_involved & ALL_SWAP_PROGRAMS) or swap_info["is_swap"]
        return {
            "movements":               movements,
            "swap_info":               swap_info,
            "programs_involved":       programs_involved,
            "is_swap_related":         is_swap_related,
            "target_token_change":     movements["by_mint"].get(self.target_mint, 0),
            "has_target_token_movement": abs(movements["by_mint"].get(self.target_mint, 0)) > 1e-6,
            "transaction_type":        self._determine_transaction_type(movements, swap_info, programs_involved),
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
        meta    = tx_data.get("meta", {})
        pre_all  = meta.get("preTokenBalances",  []) or []
        post_all = meta.get("postTokenBalances", []) or []
        pre_by_account  = {}
        post_by_account = {}
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
        meta    = tx_data.get("meta", {})
        message = tx_data.get("transaction", {}).get("message", {})
        movements: Dict[str, float] = defaultdict(float)
        for ix in message.get("instructions", []):
            self._process_instruction_for_transfers(ix, user_wallet, movements, tx_data)
        for inner_group in meta.get("innerInstructions", []) or []:
            for ix in inner_group.get("instructions", []):
                self._process_instruction_for_transfers(ix, user_wallet, movements, tx_data)
        return dict(movements)

    def _process_instruction_for_transfers(self, ix: Dict, user_wallet: str,
                                            movements: Dict[str, float], tx_data: Dict):
        program_id = ix.get("programId")
        if program_id not in TOKEN_PROGRAMS:
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
        return {}   # placeholder — add DEX-specific log parsing here if needed

    def _get_programs_involved(self, tx_data: Dict) -> Set[str]:
        return get_all_program_ids(tx_data)

    def _identify_swap_patterns(self, movements: Dict, user_wallet: str) -> Dict[str, Any]:
        by_mint       = movements["by_mint"]
        positive      = [m for m, d in by_mint.items() if d >  1e-6]
        negative      = [m for m, d in by_mint.items() if d < -1e-6]
        is_swap       = len(positive) >= 1 and len(negative) >= 1
        is_pure_swap  = is_swap and abs(movements["net"]) < 1e-6
        main_in       = max(positive, key=lambda m: by_mint[m]) if positive else None
        main_out      = min(negative, key=lambda m: by_mint[m]) if negative else None
        main_in_amt   = by_mint[main_in]  if main_in  else 0.0
        main_out_amt  = abs(by_mint[main_out]) if main_out else 0.0
        return {
            "is_swap": is_swap, "is_pure_swap": is_pure_swap,
            "positive_mints": positive, "negative_mints": negative,
            "main_in_token": main_in,  "main_out_token": main_out,
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
        pre_b = meta.get("preBalances",  [])
        post_b= meta.get("postBalances", [])
        fee   = meta.get("fee", 5000)
        if si >= len(pre_b) or si >= len(post_b): return 0.0, 0.0
        diff  = pre_b[si] - post_b[si]
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
            self._learned[pid] = {"name": exchange if exchange != "Unknown" else f"Learned ({pid[:8]}…)",
                                   "role": role, "confidence": confidence, "seen": 1}
            print(f"   📚 Learned new program: {pid[:16]}… → {role} ({exchange})")
            changed = True
        if changed:
            save_learned_programs(self._learned, LEARNED_PROGRAMS_FILE)

    async def _handle_unknown_program(self, tx_data: Dict, signer: str, signature: str,
                                       suspicion: float, signals: List[str]) -> None:
        meta    = tx_data.get("meta", {})
        message = tx_data.get("transaction", {}).get("message", {})
        all_ixs = get_all_instructions(tx_data)
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
        meta    = tx_data.get("meta", {})
        pre_tok = meta.get("preTokenBalances",  []) or []
        post_tok= meta.get("postTokenBalances", []) or []
        pre_bal = meta.get("preBalances",  [])
        post_bal= meta.get("postBalances", [])
        fee     = meta.get("fee", 5000)
        keys    = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        si      = next((i for i, k in enumerate(keys)
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
            MIN_CONF = {OrderType.MARKET_BUY: 0.75, OrderType.MARKET_SELL: 0.75,
                        OrderType.LIMIT_BUY:  0.80, OrderType.LIMIT_SELL:  0.80,
                        OrderType.CANCEL_LIMIT: 0.80, OrderType.TRANSFER:  0.85}
            if order_type != OrderType.UNKNOWN and conf >= MIN_CONF.get(order_type, 0.99):
                should_learn = bool(programs & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS)) \
                               if order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL) \
                               else (bool(programs & LIMIT_ORDER_PROGRAMS) or any(
                                   k in logs_lc for k in ["limit", "order", "cancel"]))
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
        meta    = tx_data.get("meta", {})
        if meta.get("err"): return OrderType.UNKNOWN, None

        programs = get_all_program_ids(tx_data)
        all_ixs  = get_all_instructions(tx_data)
        deltas   = get_signer_token_deltas(tx_data, signer)
        target_delta = deltas.get(MINT, 0.0)
        sol_spent, sol_received = self._signer_sol_flows(tx_data, signer)

        learned_limit  = {p for p in programs if self._known_role(p) in ("limit",  "hybrid")}
        learned_market = {p for p in programs if self._known_role(p) in ("market", "hybrid")}
        limit_hits  = (programs & LIMIT_ORDER_PROGRAMS) | learned_limit
        market_hits = (programs & ALL_SWAP_PROGRAMS) | (programs & DEX_PROGRAMS) | learned_market

        has_cancel = has_new_order = False
        logs_lc = " ".join(meta.get("logMessages") or []).lower()
        for ix in all_ixs:
            raw = self._decode_ix_data(ix.get("data", ""))
            if raw and len(raw) >= 8:
                disc = DISCRIMINATORS.get(raw[:8])
                if disc == "cancel_order": has_cancel = True
                elif disc == "new_order":  has_new_order = True
        if "cancel" in logs_lc or "withdraw order" in logs_lc: has_cancel = True
        if any(k in logs_lc for k in ["new order", "place order", "limit"]): has_new_order = True

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
                        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"): return abs_amt
            if mint == WSOL_MINT: return abs_amt * ms.sol_price_usd
            return 0.0

        def ex_names(hits):
            return ", ".join(
                exchange_name(p) if p in EXCHANGE_REGISTRY
                else self._learned.get(p, {}).get("name", f"Learned ({p[:8]}…)")
                for p in sorted(hits))

        # 1. CANCEL
        if limit_hits and has_cancel:
            return OrderType.CANCEL_LIMIT, {
                "wallet": signer, "signature": tx_data.get("signature", ""),
                "exchange": ex_names(limit_hits), "quote_token": ""}

        # 2. Executed market trade
        if abs(target_delta) > 1e-12:
            usd_value, quote_symbol = self._derive_trade_value_from_flows(
                deltas, ms, target_delta, sol_spent, sol_received)
            has_buy  = target_delta > 0 and (any(d < 0 for m, d in deltas.items() if m != MINT) or sol_spent > 0.001)
            has_sell = target_delta < 0 and (any(d > 0 for m, d in deltas.items() if m != MINT) or sol_received > 0.001)
            exchange_hits = sorted(market_hits | limit_hits)
            if has_buy:
                return OrderType.MARKET_BUY, {
                    "wallet": signer, "amount": abs(target_delta),
                    "usd_value": usd_value, "exchange": ex_names(market_hits | limit_hits),
                    "quote_token": quote_symbol}
            if has_sell:
                return OrderType.MARKET_SELL, {
                    "wallet": signer, "amount": abs(target_delta),
                    "usd_value": usd_value, "exchange": ex_names(market_hits | limit_hits),
                    "quote_token": quote_symbol}

        # 3. LIMIT BUY
        if limit_hits and abs(target_delta) < 1e-12:
            has_placement = has_new_order or any(k in logs_lc for k in ["place", "init", "create"])
            if has_placement:
                quote_mint = pick_quote(negative_quotes)
                usd_value  = quote_usd(quote_mint, abs(deltas.get(quote_mint, 0.0)))
                if usd_value < 5.0:
                    s_sol, _ = self._signer_sol_flows(tx_data, signer)
                    if s_sol > 0.02:
                        usd_value = s_sol * ms.sol_price_usd
                if usd_value >= 5.0:
                    # Compute token amount from USD value using current price
                    amount = usd_value / ms.current_price if ms.current_price > 0 else 0.0
                    if amount <= 0:
                        return OrderType.UNKNOWN, None
        
                    # Build AMM from current market state
                    amm = build_amm_from_market_state(ms)
                    if not amm or amm.token_reserve <= 0 or amm.sol_reserve <= 0:
                        return OrderType.UNKNOWN, None
        
                    # Constant product calculation for buying 'amount' tokens
                    # pool state before trade
                    R_token = amm.token_reserve   # token reserve (XERIS)
                    R_sol   = amm.sol_reserve     # SOL reserve
                    k = R_token * R_sol
        
                    # After buying 'amount' tokens, token reserve decreases
                    new_R_token = R_token - amount
                    if new_R_token <= 0:
                        # Not enough liquidity – ignore this order
                        return OrderType.UNKNOWN, None
        
                    new_R_sol = k / new_R_token
                    sol_needed = new_R_sol - R_sol   # SOL required to buy 'amount' tokens
        
                    # New price (in SOL per token)
                    new_price_sol = new_R_sol / new_R_token
                    new_price_usd = new_price_sol * ms.sol_price_usd
        
                    # New market cap
                    if ms.total_supply > 0:
                        new_mcap = new_price_usd * ms.total_supply
                    else:
                        # Fallback: scale current market cap by price change
                        new_mcap = ms.current_market_cap * (new_price_usd / ms.current_price)
        
                    predicted_mcap = new_mcap
        
                    return OrderType.LIMIT_BUY, {
                        "wallet": signer,
                        "amount": amount,
                        "usd_value": usd_value,
                        "target_price": new_price_usd,
                        "predicted_mcap": predicted_mcap,
                        "exchange": ex_names(limit_hits),
                        "quote_token": KNOWN_TOKEN_LABELS.get(quote_mint, f"{quote_mint[:8]}…" if quote_mint else "")
                    }

        # 4. LIMIT SELL
        if limit_hits and target_delta < 0 and not market_hits:
            _, sol_rcv = self._signer_sol_flows(tx_data, signer)
            if sol_rcv > 0.001: return OrderType.UNKNOWN, None
            has_placement = has_new_order or any(k in logs_lc for k in ["place", "init", "create"])
            if has_placement:
                amount    = abs(target_delta)
                usd_value = amount * ms.current_price
                if usd_value >= 5.0:
                    amm = build_amm_from_market_state(ms)
                    if not amm:
                        return OrderType.UNKNOWN, None
        
                    # For a limit sell, compute the market cap after selling 'amount' tokens
                    proj = amm.sell_tokens(amount)
                    predicted_mcap = proj.new_market_cap_usd
        
                    return OrderType.LIMIT_SELL, {
                        "wallet": signer, "amount": amount, "usd_value": usd_value,
                        "target_price": proj.new_price,
                        "predicted_mcap": predicted_mcap,
                        "exchange": ex_names(limit_hits), "quote_token": ""}

        # 5. TRANSFER
        non_target = [m for m, d in deltas.items() if m != MINT and abs(d) > 1e-12]
        if abs(target_delta) > 1e-12 and not limit_hits and not market_hits and not non_target:
            return OrderType.TRANSFER, {
                "wallet": signer, "amount": abs(target_delta),
                "usd_value": abs(target_delta) * ms.current_price,
                "to": "unknown", "quote_token": ""}

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
                if abs(target_delta) < 1e-12 or not token_result or token_result[0] != "TRANSFER": return OrderType.UNKNOWN, None, 0.0
                programs = get_all_program_ids(tx_data)
                if programs & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS | LIMIT_ORDER_PROGRAMS):  return OrderType.UNKNOWN, None, 0.0
            if order_type == OrderType.CANCEL_LIMIT:
                all_ixs   = get_all_instructions(tx_data)
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
                # ... (keep the existing USD/value estimation code) ...
                info["amount"] = amount
                info["usd_value"] = usd_value
            
                if info["amount"] <= 0 or info["usd_value"] <= 0:
                    return OrderType.UNKNOWN, None, 0.0
            
                amm = build_amm_from_market_state(ms)
                if not amm or amm.token_reserve <= 0 or amm.sol_reserve <= 0:
                    return OrderType.UNKNOWN, None, 0.0
            
                if order_type == OrderType.LIMIT_BUY:
                    # Use constant product calculation for buying tokens
                    R_token = amm.token_reserve
                    R_sol   = amm.sol_reserve
                    k = R_token * R_sol
            
                    new_R_token = R_token - info["amount"]
                    if new_R_token <= 0:
                        return OrderType.UNKNOWN, None, 0.0
            
                    new_R_sol = k / new_R_token
                    new_price_sol = new_R_sol / new_R_token
                    new_price_usd = new_price_sol * ms.sol_price_usd
            
                    if ms.total_supply > 0:
                        new_mcap = new_price_usd * ms.total_supply
                    else:
                        new_mcap = ms.current_market_cap * (new_price_usd / ms.current_price)
            
                    info["target_price"]   = new_price_usd
                    info["predicted_mcap"] = new_mcap
            
                else:  # LIMIT_SELL
                    # Use existing sell_tokens method
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
            if swap_like: return ("BUY", target_delta, signer, qs)
            if not limit_like: return ("TRANSFER", target_delta, signer, "unknown")
        if target_delta < 0:
            qm = pick(pos); qs = KNOWN_TOKEN_LABELS.get(qm, f"{qm[:8]}…" if qm else "SOL")
            if swap_like: return ("SELL", abs(target_delta), signer, qs)
            if not limit_like: return ("TRANSFER", abs(target_delta), signer, "unknown")
        return None

    def _estimate_target_price(self, tx_data: Dict, token_amount: float, ms: MarketState) -> float:
        if token_amount <= 0: return 0.0
        meta = tx_data.get("meta", {})
        for line in (meta.get("logMessages") or []):
            for kw in ("price:", "Price:", "limit_price:", "limitPrice:"):
                idx = line.find(kw)
                if idx != -1:
                    try:
                        value = float(line[idx + len(kw):].split()[0].strip(",}"))
                        if value > 0: return value
                    except Exception: pass
        return 0.0

    @staticmethod
    def _predict_mcap(target_price: float, ms: MarketState) -> float:
        if target_price <= 0: return 0.0
        if ms.total_supply > 0: return target_price * ms.total_supply
        if ms.current_price > 0 and ms.current_market_cap > 0:
            return ms.current_market_cap * (target_price / ms.current_price)
        return 0.0


# ═════════════════════════════════════════════════════════════════════════════
# Order tracker
# ═════════════════════════════════════════════════════════════════════════════

class OrderTracker:
    def __init__(self, db: DatabaseManager, classifier: TransactionClassifier, ms: MarketState) -> None:
        self.db         = db
        self.classifier = classifier
        self.ms         = ms
        self._seen: OrderedDict = OrderedDict()
        self._seen_max = 2000

    def _mark_seen(self, sig: str) -> bool:
        if sig in self._seen: return True
        self._seen[sig] = None
        if len(self._seen) > self._seen_max: self._seen.popitem(last=False)
        return False

    async def process(self, tx_data: Dict, signature: str) -> Optional[Dict]:
        if self._mark_seen(signature): return None
        keys   = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        signer = (keys[0].get("pubkey") if isinstance(keys[0], dict) else keys[0]) if keys else ""
        if not signer: return None

        order_type, info = await self.classifier.classify(tx_data, signer, self.ms)
        if not info: return None

        if order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL, OrderType.TRANSFER):
            if info.get("amount", 0) <= 0 or info.get("usd_value", 0) <= 0: return None
        if order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL):
            if info.get("usd_value", 0) < 5: return None
            order = LimitOrder(
                signature=signature, wallet=info["wallet"], order_type=order_type,
                token_amount=info["amount"], usd_value=info["usd_value"],
                predicted_mcap=info.get("predicted_mcap", 0.0),
                target_price=info.get("target_price",   0.0), timestamp=time.time())
            await self.db.upsert_limit_order(order, quote_token=info.get("quote_token", ""),
                                              exchange=info.get("exchange", ""))
            return {"action": "new_limit", "order": order, "info": info}
        elif order_type == OrderType.CANCEL_LIMIT:
            cancelled = await self._handle_cancel(signer, signature)
            return {"action": "cancel_limit", "cancelled": cancelled, "wallet": signer}
        elif order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL):
            fills = await self._detect_fills(order_type, signature)
            return {"action": "market", "type": order_type, "info": info, "fills": fills}
        elif order_type == OrderType.TRANSFER:
            return {"action": "transfer", "info": info}
        return None

    async def _handle_cancel(self, wallet: str, sig: str) -> Optional[Dict]:
        for otype in ("LIMIT_BUY", "LIMIT_SELL"):
            row = await self.db.deactivate_one_by_wallet(wallet, otype)
            if row:
                await send_message(ALERT_CHANNEL_ID, embeds=[_embed_cancelled(wallet, row, sig)])
                return row
        return None

    async def _detect_fills(self, market_type: OrderType, signature: str) -> List[Dict]:
        ms = self.ms; filled = []
        if ms.current_market_cap <= 0: return filled
        for order in await self.db.get_active_orders():
            predicted = order["predicted_mcap"]
            if predicted <= 0: continue
            if order["token_amount"] <= 0 or order["usd_value"] <= 0:
                await self.db.deactivate_by_signature(order["signature"]); continue
            proximity = abs(predicted - ms.current_market_cap) / ms.current_market_cap
            if proximity < 0.01:
                if (market_type == OrderType.MARKET_BUY  and order["order_type"] == "LIMIT_SELL") or \
                   (market_type == OrderType.MARKET_SELL and order["order_type"] == "LIMIT_BUY"):
                    await self.db.deactivate_by_signature(order["signature"])
                    await send_message(ALERT_CHANNEL_ID, embeds=[_embed_filled(order, market_type, signature, ms)])
                    filled.append(order)
        return filled


# ═════════════════════════════════════════════════════════════════════════════
# Alert manager
# ═════════════════════════════════════════════════════════════════════════════

class AlertManager:
    def __init__(self, db: DatabaseManager, ms: MarketState) -> None:
        self.db            = db
        self.ms            = ms
        self._last         = 0.0
        self._last_cleanup = 0.0

    async def tick(self) -> None:
        now = time.time()
        if now - self._last_cleanup >= CLEANUP_INTERVAL:
            removed = await self.db.cleanup_stale(max_age_hours=ORDER_TTL_HOURS)
            if removed > 0: await send_message(ALERT_CHANNEL_ID, embeds=[_embed_cleanup(removed)])
            self._last_cleanup = now
        if now - self._last < SUMMARY_ALERT_INTERVAL: return
        await self._send_summary()
        self._last = now

    async def _send_summary(self) -> None:
        ms     = self.ms
        orders = await self.db.get_active_orders()
        if not orders: return
        buys  = [o for o in orders if o["order_type"] == "LIMIT_BUY"]
        sells = [o for o in orders if o["order_type"] == "LIMIT_SELL"]
        support_lvls    = sorted(o["predicted_mcap"] for o in buys  if o.get("predicted_mcap", 0) > 0)
        resistance_lvls = sorted(o["predicted_mcap"] for o in sells if o.get("predicted_mcap", 0) > 0)
        nearest_support    = support_lvls[0]    if support_lvls    else None
        nearest_resistance = resistance_lvls[0] if resistance_lvls else None
        embed: dict = {
            "author": {"name": "📊 ACTIVE LIMIT ORDER BOOK — Snapshot"},
            "title":  "Live Support & Resistance Levels",
            "description": (f"```yaml\nActive Orders : {len(orders)}\n"
                            f"Buy  Orders   : {len(buys)}   |  Wall: {format_usd(sum(o['usd_value'] for o in buys))}\n"
                            f"Sell Orders   : {len(sells)}   |  Wall: {format_usd(sum(o['usd_value'] for o in sells))}\n```"),
            "color":  0x8B5CF6, "fields": [],
        }
        if buys:
            lines = ""
            for i, o in enumerate(sorted(buys, key=lambda x: x["predicted_mcap"])[:6]):
                dist = _pct_from_current(o["predicted_mcap"], ms)
                qt   = f" [{o.get('quote_token','')}]" if o.get("quote_token") else ""
                lines += (f"`{i+1}.` {format_usd(o['usd_value'])}{qt} · "
                          f"mcap `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%) · "
                          f"`{o['wallet'][:6]}…` · ⏳ `{_format_time_remaining(o)}`\n")
            embed["fields"].append({"name": f"🛡️ SUPPORT LEVELS  ({len(buys)} orders)", "value": lines, "inline": False})
        if sells:
            lines = ""
            for i, o in enumerate(sorted(sells, key=lambda x: x["predicted_mcap"])[:6]):
                dist = _pct_from_current(o["predicted_mcap"], ms)
                qt   = f" [{o.get('quote_token','')}]" if o.get("quote_token") else ""
                lines += (f"`{i+1}.` {format_usd(o['usd_value'])}{qt} · "
                          f"mcap `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%) · "
                          f"`{o['wallet'][:6]}…` · ⏳ `{_format_time_remaining(o)}`\n")
            embed["fields"].append({"name": f"⚠️ RESISTANCE LEVELS  ({len(sells)} orders)", "value": lines, "inline": False})
        embed["fields"].append({
            "name": "📈 Market Context",
            "value": (f"┌ Price:              `${ms.current_price:.8f}`\n"
                      f"├ Market Cap:         `{format_usd(ms.current_market_cap)}`\n"
                      f"├ Nearest Support:    `{format_usd(nearest_support)}`\n"
                      f"└ Nearest Resistance: `{format_usd(nearest_resistance)}`"),
            "inline": False,
        })
        embed["footer"]    = {"text": f"Order Book · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"}
        embed["timestamp"] = get_timestamp()
        await send_message(ALERT_CHANNEL_ID, embeds=[embed])


# ═════════════════════════════════════════════════════════════════════════════
# Embed builders
# ═════════════════════════════════════════════════════════════════════════════

def _embed_cleanup(count: int) -> dict:
    return {"author": {"name": "🧹 Order Book Cleanup"},
            "title":  f"Removed {count} expired limit order(s)",
            "description": f"```yaml\nExpired After : 7 days\nOrders Removed: {count}\n```",
            "color": 0x6B7280, "footer": {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
            "timestamp": get_timestamp()}

def _embed_cancelled(wallet: str, order: Dict, sig: str) -> dict:
    return {"author": {"name": "❌ LIMIT ORDER CANCELLED"}, "title": f"🗑️ {order['order_type']} Cancelled",
            "description": (f"```yaml\nWallet:   {wallet[:8]}...{wallet[-8:]}\n"
                            f"Size:     {format_tokens(order['token_amount'])} XERIS\n"
                            f"Value:    {format_usd(order['usd_value'])}\n"
                            f"Target:   {format_usd(order['predicted_mcap'])} mcap\n"
                            f"Placed:   {_format_placed_at(order)}\n```"),
            "color": 0x9CA3AF,
            "fields": [{"name": "🔗 Transaction", "value": f"[Solscan](https://solscan.io/tx/{sig})", "inline": False}],
            "footer": {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
            "timestamp": get_timestamp()}

def _embed_filled(order: Dict, fill_type: OrderType, sig: str, ms: MarketState) -> dict:
    return {"author": {"name": "✅ LIMIT ORDER FILLED"}, "title": f"💹 {order['order_type']} Executed",
            "description": (f"```yaml\nSize:     {format_tokens(order['token_amount'])} XERIS\n"
                            f"Value:    {format_usd(order['usd_value'])}\n"
                            f"Wallet:   {order['wallet'][:8]}...{order['wallet'][-8:]}\n```\n"
                            f"> Filled by a **{fill_type.value}** market order."),
            "color": 0x10B981,
            "fields": [
                {"name": "📊 Levels",
                 "value": f"┌ Predicted MCap: `{format_usd(order['predicted_mcap'])}`\n└ Current MCap:   `{format_usd(ms.current_market_cap)}`",
                 "inline": False},
                {"name": "🔗 Links",
                 "value": f"[Original](https://solscan.io/tx/{order['signature']}) · [Fill Tx](https://solscan.io/tx/{sig})",
                 "inline": False}],
            "footer": {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
            "timestamp": get_timestamp()}

def _build_limit_order_embed(order: LimitOrder, ms: MarketState,
                              quote_token: str = "", exchange: str = "") -> dict:
    is_buy    = order.order_type == OrderType.LIMIT_BUY
    color     = 0x10B981 if is_buy else 0xEF4444
    direction = "BUY" if is_buy else "SELL"
    dist      = _pct_from_current(order.predicted_mcap, ms) if order.predicted_mcap > 0 else 0.0
    order_dict = {"timestamp": order.timestamp}
    placed_at  = datetime.fromtimestamp(order.timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    expires_at = datetime.fromtimestamp(order.timestamp + ORDER_TTL_SECS, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pair_label = (f"{quote_token} → XERIS" if is_buy and quote_token
                  else f"XERIS → {quote_token}" if not is_buy and quote_token else "XERIS")
    return {
        "author": {"name": f"🎯 NEW LIMIT {'BUY' if is_buy else 'SELL'} DETECTED"},
        "title":  f"{'📈' if is_buy else '📉'} Limit {direction} · {format_usd(order.usd_value)}",
        "description": (f"```yaml\nType:         LIMIT {direction}\nPair:         {pair_label}\n"
                        f"Size:         {format_tokens(order.token_amount)} XERIS\nValue:        {format_usd(order.usd_value)}\n"
                        f"Target MCap:  {format_usd(order.predicted_mcap) if order.predicted_mcap > 0 else 'Unknown'}\n"
                        f"Distance:     {f'{dist:+.2f}% from current' if order.predicted_mcap > 0 else 'Unknown'}\n"
                        f"Role:         {'Support Level' if is_buy else 'Resistance Level'}\n"
                        f"Placed At:    {placed_at}\n"
                        + (f"Exchange:     {exchange}\n" if exchange else "") + "```"),
        "color":  color,
        "fields": [
            {"name": "👤 Wallet",  "value": f"```{order.wallet}```", "inline": False},
            {"name": "📊 Market",
             "value": f"┌ Current MCap: `{format_usd(ms.current_market_cap)}`\n└ Target MCap:  `{format_usd(order.predicted_mcap)}` ({dist:+.1f}%)",
             "inline": False},
            {"name": "⏳ Lifetime",
             "value": f"`{_expiry_bar(order_dict)}` {_format_time_remaining(order_dict)}\nPlaced: `{placed_at}` · Expires: `{expires_at}`",
             "inline": False},
            {"name": "🔗",
             "value": f"[Tx](https://solscan.io/tx/{order.signature}) · [Wallet](https://solscan.io/account/{order.wallet}) · [Chart](https://dexscreener.com/solana/{MINT})",
             "inline": False}],
        "footer": {"text": f"Limit Order Tracker · expires in 7 days · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}

def _build_whale_embed(tx_type: str, amount: float, wallet: str, usd_value: float,
                        signature: str, ms: MarketState, quote_token: str = "", exchange: str = "") -> dict:
    is_buy = tx_type == "BUY"; color = 0x10B981 if is_buy else 0xEF4444
    amm    = build_amm_from_market_state(ms, fee_rate=0.0025)
    if amm:
        proj     = amm.buy_with_sol(usd_value / ms.sol_price_usd) if is_buy else amm.sell_tokens(amount)
        new_mcap = proj.new_market_cap_usd; diff = new_mcap - ms.current_market_cap
        impact   = proj.price_impact_pct
    else:
        new_mcap = ms.current_market_cap; diff = impact = 0.0
    tier = ("💎 MEGA WHALE" if usd_value >= 50_000 else "🌊 WHALE" if usd_value >= 10_000
            else "⭐ BIG FISH" if usd_value >= 5_000 else "💫 FISH")
    pair_label = (f"{quote_token} → XERIS" if is_buy and quote_token
                  else f"XERIS → {quote_token}" if not is_buy and quote_token
                  else f"XERIS {'bought' if is_buy else 'sold'}")
    return {
        "author": {"name": f"{tier} DETECTED"},
        "title":  f"{'📈' if is_buy else '📉'} {tx_type} · {format_usd(usd_value)}",
        "description": (f"```yaml\nPair:   {pair_label}\nTrade:  {format_tokens(amount)} XERIS\n"
                        f"USD:    {format_usd(usd_value)}\nImpact: {impact:.2f}% of MCap\n"
                        + (f"Via:    {exchange}\n" if exchange else "") + "```"),
        "color":  color,
        "fields": [
            {"name": "💰 Market Metrics",
             "value": (f"┌ Price: `${ms.current_price:.8f}`\n"
                       f"├ MCap: `{format_usd(ms.current_market_cap)}`\n"
                       f"└ New MCap: `{format_usd(new_mcap)}` ({'+' if diff>=0 else ''}{format_usd(diff)})"),
             "inline": False},
            {"name": "👤 Wallet", "value": f"```{wallet}```", "inline": False},
            {"name": "🔗 Links",
             "value": f"[TX](https://solscan.io/tx/{signature}) · [Wallet](https://solscan.io/account/{wallet}) · [Chart](https://dexscreener.com/solana/{MINT})",
             "inline": False}],
        "footer": {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}

def _build_dev_sell_embed(amount: float, wallet: str, usd_value: float, signature: str,
                           ms: MarketState, quote_token: str = "") -> dict:
    amm    = build_amm_from_market_state(ms, fee_rate=0.0025)
    proj   = amm.sell_tokens(amount) if amm else None
    new_mcap = proj.new_market_cap_usd if proj else ms.current_market_cap
    impact   = abs(proj.price_impact_pct) if proj else 0.0
    return {
        "author": {"name": "⚠️ DEVELOPER ACTIVITY ALERT"}, "title": "🚨 Dev Wallet Sell Detected",
        "description": (f"```diff\n- Developer has executed a SELL transaction\n```\n"
                        f"**⚠️ Monitor price action closely**\n> Amount: **{format_usd(usd_value)}** ({impact:.2f}% of MCap)"),
        "color": 0xDC2626,
        "fields": [
            {"name": "💸 Details",
             "value": (f"```yaml\nPair:   XERIS → {quote_token or '?'}\nTokens: {format_tokens(amount)} XERIS\n"
                       f"USD:    {format_usd(usd_value)}\nImpact: {impact:.2f}%\n```"),
             "inline": False},
            {"name": "📊 MCap Impact",
             "value": f"┌ Before: `{format_usd(ms.current_market_cap)}`\n└ After:  `{format_usd(new_mcap)}`",
             "inline": False},
            {"name": "👤 Dev Wallet", "value": f"```{wallet}```", "inline": False},
            {"name": "🔍 Links",
             "value": f"[TX](https://solscan.io/tx/{signature}) · [Wallet](https://solscan.io/account/{wallet}) · [Chart](https://dexscreener.com/solana/{MINT})",
             "inline": False}],
        "footer": {"text": f"Dev Monitor · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}

def _build_price_embed(pct: float, ref: float, ms: MarketState) -> dict:
    is_pump = pct > 0; sign = "+" if is_pump else ""
    abs_pct = abs(pct); color = 0x10B981 if is_pump else 0xEF4444
    ref_mcap = ms.current_market_cap / (1 + pct / 100)
    mcap_chg = ms.current_market_cap - ref_mcap
    bars     = "█" * min(12, round(abs_pct / 2)) + "░" * max(0, 12 - round(abs_pct / 2))
    return {
        "author": {"name": "⚡ Price Alert — XerisCoin"},
        "title":  f"{'🚀 PUMP' if is_pump else '📉 DUMP'} · {sign}{pct:.2f}%",
        "description": f"```diff\n{'+ ' if is_pump else '- '}{sign}{pct:.2f}% from reference\n```",
        "color":  color,
        "fields": [
            {"name": "💹 Price",
             "value": f"```yaml\nRef: ${ref:.8f}\nNow: ${ms.current_price:.8f}\nΔ:   {sign}{pct:.2f}%\n```",
             "inline": True},
            {"name": "📊 MCap",
             "value": f"```yaml\nNow: {format_usd(ms.current_market_cap)}\nΔ:   {'+' if mcap_chg>=0 else ''}{format_usd(abs(mcap_chg))}\n```",
             "inline": True},
            {"name": "📈 Momentum", "value": f"`{bars}` **{abs_pct:.1f}%**", "inline": False},
            {"name": "🔗 Charts",
             "value": f"[DexScreener](https://dexscreener.com/solana/{MINT}) · [Birdeye](https://birdeye.so/token/{MINT})",
             "inline": False}],
        "footer": {"text": f"Threshold ±{PRICE_ALERT_THRESHOLD}% · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}

# ═════════════════════════════════════════════════════════════════════════════
# Price fetching
# ═════════════════════════════════════════════════════════════════════════════

async def update_price(ms: MarketState) -> None:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r    = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{MINT}")
            data = r.json()
        pair = _pick_best_pair(data.get("pairs") or [])
        if not pair: print("⚠️ No pairs on DexScreener"); return
        new_price     = float(pair.get("priceUsd") or 0)
        liq           = pair.get("liquidity") or {}
        ms.pool_token_reserve = float(liq.get("base") or 0)
        ms.pool_sol_reserve   = float(liq.get("quote") or 0)
        ms.pool_liquidity_usd = float(liq.get("usd")   or 0)
        fdv  = pair.get("fdv")
        mcap = pair.get("marketCap")
        
        if mcap:
            ms.current_market_cap = float(mcap)
        elif fdv:
            ms.current_market_cap = float(fdv)
        else:
            ms.current_market_cap = 0.0
        if ms.price_reference == 0.0 and new_price > 0: ms.price_reference = new_price
        ms.current_price = new_price; ms.last_price_update = time.time()
        print(f"💰 ${ms.current_price:.8f}  |  MCap {format_usd(ms.current_market_cap)}")
        if ms.price_reference > 0: await _check_price_alert(ms)
        if ms.total_supply <= 0 and ms.current_price > 0 and ms.current_market_cap > 0:
            ms.total_supply = ms.current_market_cap / ms.current_price
    except Exception as e:
        print(f"❌ Price error: {e}")

async def _check_price_alert(ms: MarketState) -> None:
    if ms.price_reference <= 0 or ms.current_price <= 0: return
    pct = (ms.current_price - ms.price_reference) / ms.price_reference * 100
    if abs(pct) < PRICE_ALERT_THRESHOLD: return
    direction = "up" if pct > 0 else "down"
    last = ms.last_alert_up_time if direction == "up" else ms.last_alert_down_time
    if time.time() - last < PRICE_ALERT_COOLDOWN: return
    await send_message(ALERT_CHANNEL_ID, embeds=[_build_price_embed(pct, ms.price_reference, ms)])
    if direction == "up": ms.last_alert_up_time   = time.time()
    else:                 ms.last_alert_down_time = time.time()
    ms.last_alert_direction = direction
    ms.price_reference      = ms.current_price


# ═════════════════════════════════════════════════════════════════════════════
# RPC helpers
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_tx(signature: str, retries: int = 3) -> Optional[Dict]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
               "params": [signature, {"encoding": "jsonParsed",
                                       "maxSupportedTransactionVersion": 0,
                                       "commitment": "confirmed"}]}
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(RPC_URL, json=payload)
            if r.status_code == 200:
                result = r.json()
                if "error" not in result:
                    tx = result.get("result")
                    if tx: return tx
        except Exception as e:
            print(f"   fetch attempt {attempt+1}: {e}")
        if attempt < retries - 1:
            await asyncio.sleep(2 ** attempt + random.random())
    return None

async def fetch_price_for_ca(ca: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r    = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
            data = r.json()
        p = _pick_best_pair(data.get("pairs") or [])
        if not p: return {}
        return {"price": float(p.get("priceUsd") or 0),
                "mcap":  float(p.get("fdv") or p.get("marketCap") or 0),
                "volume_24h": float((p.get("volume") or {}).get("h24") or 0),
                "change_24h": float((p.get("priceChange") or {}).get("h24") or 0),
                "liquidity":  float((p.get("liquidity") or {}).get("usd") or 0),
                "dex": p.get("dexId", "unknown"), "pair_addr": p.get("pairAddress", ""),
                "name": p.get("baseToken", {}).get("name",   "Unknown"),
                "symbol": p.get("baseToken", {}).get("symbol", "???")}
    except Exception as e:
        print(f"❌ DexScreener error: {e}"); return {}

async def fetch_top_holders(ca: str) -> list:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts",
               "params": [ca, {"commitment": "confirmed"}]}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(RPC_URL, json=payload)
        return r.json().get("result", {}).get("value", [])
    except Exception as e:
        print(f"❌ Holder fetch error: {e}"); return []

async def fetch_token_metadata(ca: str) -> dict:
    result = {"deployer": None, "mint_authority": None, "freeze_authority": None,
              "created_at": None, "token_age_days": None, "decimals": 6, "supply": 0}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(RPC_URL, json={"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                "params": [ca, {"encoding": "jsonParsed", "commitment": "confirmed"}]})
            info = r.json().get("result", {}).get("value", {}).get("data", {}).get("parsed", {}).get("info", {})
            result["mint_authority"]   = info.get("mintAuthority")
            result["freeze_authority"] = info.get("freezeAuthority")
            result["decimals"]         = info.get("decimals", 6)
            result["supply"]           = int(info.get("supply", "0")) / (10 ** result["decimals"])
        async with httpx.AsyncClient(timeout=20.0) as client:
            r    = await client.post(RPC_URL, json={"jsonrpc": "2.0", "id": 1,
                "method": "getSignaturesForAddress", "params": [ca, {"limit": 1000, "commitment": "confirmed"}]})
            sigs = r.json().get("result", [])
            if sigs:
                bt = sigs[-1].get("blockTime")
                if bt:
                    dt = datetime.fromtimestamp(bt, tz=timezone.utc)
                    result["created_at"]     = dt.strftime("%Y-%m-%d %H:%M UTC")
                    result["token_age_days"] = (datetime.now(timezone.utc) - dt).days
    except Exception as e:
        print(f"⚠️ Token metadata error: {e}")
    return result

async def fetch_pumpfun_metadata(ca: str) -> dict:
    result = {"is_pumpfun": False, "creator": None, "description": None, "graduated": False,
              "reply_count": 0, "name": None, "symbol": None, "image_url": None,
              "telegram": None, "twitter": None, "website": None}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"https://frontend-api.pump.fun/coins/{ca}")
            if r.status_code == 200:
                d = r.json()
                result.update({"is_pumpfun": True, "creator": d.get("creator"),
                                "description": (d.get("description") or "")[:300],
                                "graduated": d.get("complete", False), "reply_count": d.get("reply_count", 0),
                                "name": d.get("name"), "symbol": d.get("symbol"),
                                "image_url": d.get("image_uri"), "telegram": d.get("telegram"),
                                "twitter": d.get("twitter"), "website": d.get("website")})
    except Exception as e:
        print(f"⚠️ Pump.fun error: {e}")
    return result

async def scan_socials(ca: str, token_name: str, token_symbol: str) -> dict:
    results = {"twitter": None, "website": None, "twitter_handle": None}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r  = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
            data = r.json(); pairs = data.get("pairs") or []
            if pairs:
                info    = pairs[0].get("info") or {}
                for s in info.get("socials") or []:
                    if s.get("type") == "twitter":
                        results["twitter"] = s.get("url")
                        handle = (s.get("url") or "").rstrip("/").split("/")[-1]
                        results["twitter_handle"] = f"@{handle}" if handle else None
                for w in info.get("websites") or []:
                    if w.get("url"): results["website"] = w["url"]; break
    except Exception as e:
        print(f"⚠️ Social scan error: {e}")
    return results

async def fetch_deployer_history(deployer_wallet: str) -> dict:
    result = {"wallet": deployer_wallet, "total_prev": 0, "wallet_age_days": None}
    if not deployer_wallet: return result
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r    = await client.post(RPC_URL, json={"jsonrpc": "2.0", "id": 1,
                "method": "getSignaturesForAddress", "params": [deployer_wallet, {"limit": 1000, "commitment": "confirmed"}]})
            sigs = r.json().get("result", [])
            if sigs:
                bt = sigs[-1].get("blockTime")
                if bt:
                    dt = datetime.fromtimestamp(bt, tz=timezone.utc)
                    result["wallet_age_days"] = (datetime.now(timezone.utc) - dt).days
            result["total_prev"] = max(0, len(sigs) - 1)
    except Exception as e:
        print(f"⚠️ Deployer history error: {e}")
    return result

async def fetch_all_intelligence(ca: str, name: str, symbol: str) -> dict:
    print(f"   🔎 Gathering intelligence for {ca[:16]}...")
    token_meta, pumpfun, socials = await asyncio.gather(
        fetch_token_metadata(ca), fetch_pumpfun_metadata(ca), scan_socials(ca, name, symbol))
    if pumpfun.get("twitter") and not socials.get("twitter"):
        socials["twitter"] = pumpfun["twitter"]
        handle = pumpfun["twitter"].rstrip("/").split("/")[-1]
        socials["twitter_handle"] = f"@{handle}"
    if pumpfun.get("website") and not socials.get("website"):
        socials["website"] = pumpfun["website"]
    if pumpfun.get("telegram"): socials["telegram"] = pumpfun["telegram"]
    deployer_wallet = token_meta.get("mint_authority") or pumpfun.get("creator")
    deployer = await fetch_deployer_history(deployer_wallet) if deployer_wallet else {}
    return {"token_meta": token_meta, "pumpfun": pumpfun, "socials": socials, "deployer": deployer}


# ═════════════════════════════════════════════════════════════════════════════
# AI analysis
# ═════════════════════════════════════════════════════════════════════════════

async def groq_analyze(ca: str, price_data: dict, holders: list, intelligence: dict) -> dict:
    socials    = intelligence.get("socials", {})
    token_meta = intelligence.get("token_meta", {})
    pumpfun    = intelligence.get("pumpfun", {})
    deployer   = intelligence.get("deployer", {})
    total_supply = 1_000_000_000
    amounts      = [float(h.get("uiAmount") or 0) for h in holders] if holders else []
    top10_pct    = sum(amounts[:10]) / total_supply * 100 if amounts else 0.0
    top5_pct     = sum(amounts[:5])  / total_supply * 100 if amounts else 0.0
    biggest      = (amounts[0] / total_supply * 100) if amounts else 0.0
    holder_lines = "\n".join(f"  #{i+1}: {float(h.get('uiAmount',0)):,.0f} tokens ({float(h.get('uiAmount',0))/total_supply*100:.2f}%)"
                             for i, h in enumerate(holders[:10])) if holders else "  No holder data"
    liq = price_data.get("liquidity", 0); vol = price_data.get("volume_24h", 0); mcap = price_data.get("mcap", 0)
    deployer_wallet  = deployer.get("wallet") or token_meta.get("mint_authority") or "UNKNOWN"
    deployer_age     = deployer.get("wallet_age_days")
    prompt = f"""You are an elite crypto risk analyst. Return ONLY raw JSON — no markdown.

CONTRACT: {ca}
Token: {price_data.get('name','Unknown')} ({price_data.get('symbol','???')})
Price: ${price_data.get('price',0):.8f}  MCap: ${mcap:,.0f}  24h Vol: ${vol:,.0f}
Liquidity: ${liq:,.0f}  DEX: {price_data.get('dex','unknown')}
Mint Authority: {token_meta.get('mint_authority') or 'REVOKED'}
Freeze Authority: {token_meta.get('freeze_authority') or 'REVOKED'}
Token Age: {token_meta.get('token_age_days','?')} days
Deployer Age: {f"{deployer_age} days" if deployer_age else "Unknown"}
Pump.fun: {pumpfun.get('is_pumpfun',False)}  Graduated: {pumpfun.get('graduated',False)}
Holders Top5: {top5_pct:.2f}%  Top10: {top10_pct:.2f}%  Biggest: {biggest:.2f}%
{holder_lines}
Twitter: {socials.get('twitter_handle') or 'NOT FOUND'}
Website: {socials.get('website') or 'NOT FOUND'}

Return: {{"risk_score":<0-10>,"rug_label":"<LIKELY SAFE|PROCEED WITH CAUTION|HIGH RISK|LIKELY RUG>","summary":"<2 sentences>","team_analysis":"<2-3 sentences>","red_flags":["<max 5>"],"green_flags":["<max 4>"],"holder_analysis":"<2 sentences>","liquidity_analysis":"<2 sentences>","social_analysis":"<2 sentences>","mint_freeze_risk":"<1 sentence>","trade_advice":"<1 sentence>"}}"""
    try:
        async with httpx.AsyncClient(timeout=35.0) as client:
            r = await client.post(GROQ_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 1200, "temperature": 0.3})
            if r.status_code == 200:
                raw = r.json()["choices"][0]["message"]["content"]
                return json.loads(raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip())
            return {"error": f"Groq API error {r.status_code}"}
    except Exception as e:
        return {"error": str(e)}



# ═════════════════════════════════════════════════════════════════════════════
# Chart generation
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_geckoterminal(ca: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ca}/pools",
                                  headers={"Accept": "application/json;version=20230302"})
        if r.status_code != 200: return {}
        pools = r.json().get("data", []) or []
        if not pools: return {}
        def score(p):
            a = p.get("attributes", {}) or {}
            return (float(a.get("reserve_in_usd") or 0),
                    float((a.get("volume_usd") or {}).get("h24") or 0),
                    float(a.get("fdv_usd") or 0))
        best  = max(pools, key=score)
        attrs = best.get("attributes", {}) or {}
        return {
            "pool_address": best.get("id", "").replace("solana_", ""),
            "name": attrs.get("name", "Unknown"),
            "price_usd": float(attrs.get("base_token_price_usd") or 0),
            "price_change_5m":  float((attrs.get("price_change_percentage") or {}).get("m5")  or 0),
            "price_change_1h":  float((attrs.get("price_change_percentage") or {}).get("h1")  or 0),
            "price_change_24h": float((attrs.get("price_change_percentage") or {}).get("h24") or 0),
            "volume_24h": float((attrs.get("volume_usd") or {}).get("h24") or 0),
            "liquidity":  float(attrs.get("reserve_in_usd") or 0),
            "fdv":        float(attrs.get("fdv_usd") or 0),
            "market_cap": float(attrs.get("market_cap_usd") or 0),
            "buys_24h":   int((attrs.get("transactions") or {}).get("h24", {}).get("buys")  or 0),
            "sells_24h":  int((attrs.get("transactions") or {}).get("h24", {}).get("sells") or 0),
        }
    except Exception as e:
        print(f"❌ GeckoTerminal error: {e}"); return {}

async def fetch_geckoterminal_ohlcv(pool_address: str, timeframe: str = "minute",
                                     aggregate: int = 1, limit: int = 100) -> Optional[list]:
    try:
        url = (f"https://api.geckoterminal.com/api/v2/networks/solana/pools/"
               f"{pool_address}/ohlcv/{timeframe}?aggregate={aggregate}&limit={limit}")
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(url, headers={"Accept": "application/json;version=20230302"})
        if r.status_code != 200: return None
        rows = r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if not rows: return None
        candles = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 6: continue
            try: candles.append({"unixTime": int(row[0]), "o": float(row[1]), "h": float(row[2]),
                                  "l": float(row[3]), "c": float(row[4]), "v": float(row[5])})
            except Exception: continue
        candles.sort(key=lambda x: x["unixTime"])
        if len(candles) < 5: return None
        print(f"✅ GeckoTerminal OHLCV fetched {len(candles)} candles")
        return candles
    except Exception as e:
        print(f"❌ GeckoTerminal OHLCV error: {e}"); return None

async def generate_chart_image(ca: str, timeframe: str, token_name: str, pool_address: str) -> Optional[bytes]:
    tf_cfg = TIMEFRAME_MAP.get(timeframe, TIMEFRAME_MAP["15m"])
    candles = await fetch_geckoterminal_ohlcv(pool_address, tf_cfg["gt_timeframe"], tf_cfg["aggregate"], 100)
    if not candles: return None
    try:
        BG = "#0d1117"; GRID = "#21262d"; GREEN = "#26a641"; RED = "#da3633"
        VOL_GREEN = "#1a4d2e"; VOL_RED = "#4d1a1a"; TEXT = "#e6edf3"; SUBTEXT = "#8b949e"
        timestamps = [c["unixTime"] for c in candles]
        opens  = [float(c["o"]) for c in candles]; highs  = [float(c["h"]) for c in candles]
        lows   = [float(c["l"]) for c in candles]; closes = [float(c["c"]) for c in candles]
        volumes= [float(c["v"]) for c in candles]; n = len(candles); xs = list(range(n))
        fig, (ax, ax_vol) = plt.subplots(2, 1, figsize=(13, 7),
            gridspec_kw={"height_ratios": [3, 1], "hspace": 0.03}, facecolor=BG)
        for a in (ax, ax_vol):
            a.set_facecolor(BG); a.tick_params(colors=SUBTEXT, labelsize=7.5)
            for spine in a.spines.values(): spine.set_color(GRID)
            a.yaxis.grid(True, color=GRID, linewidth=0.4, linestyle="--", alpha=0.5)
            a.xaxis.grid(False)
        low_min = min(lows); high_max = max(highs)
        whole_range = max(high_max - low_min, 1e-12)
        min_body = whole_range * 0.0015; width = 0.55
        for i in xs:
            o = opens[i]; h = highs[i]; l = lows[i]; c = closes[i]
            color = GREEN if c >= o else RED
            ax.plot([i, i], [l, h], color=color, linewidth=0.9, zorder=2)
            bh = max(abs(c - o), min_body); bb = (o + c) / 2 - bh / 2
            ax.bar(i, bh, bottom=bb, width=width, color=color, linewidth=0, zorder=3)
        for i in xs:
            ax_vol.bar(i, volumes[i], width=width,
                       color=VOL_GREEN if closes[i] >= opens[i] else VOL_RED, linewidth=0, alpha=0.9)
        last_close = closes[-1]
        fmt = "%.10f" if last_close < 0.000001 else "%.8f" if last_close < 0.0001 else "%.6f" if last_close < 0.01 else "%.4f"
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter(fmt)); ax.yaxis.tick_right()
        ax_vol.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}K" if x >= 1000 else f"{x:.0f}"))
        ax_vol.yaxis.tick_right()
        step = max(1, n // 8); tick_xs = xs[::step]; tick_labels = []
        resolution = tf_cfg["resolution"]
        for i in tick_xs:
            dt = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
            if   resolution >= 1440: tick_labels.append(dt.strftime("%m/%d"))
            elif resolution >= 60:   tick_labels.append(dt.strftime("%d %H:%M"))
            else:                    tick_labels.append(dt.strftime("%H:%M"))
        ax_vol.set_xticks(tick_xs); ax_vol.set_xticklabels(tick_labels, color=SUBTEXT, fontsize=7)
        ax.set_xticks([])
        first_close = closes[0]; pct = ((last_close - first_close) / first_close * 100) if first_close > 0 else 0.0
        pct_color = GREEN if pct >= 0 else RED; pct_sign = "+" if pct >= 0 else ""
        fig.text(0.01, 0.97, f"{token_name}", color=TEXT, fontsize=13, fontweight="bold", va="top")
        fig.text(0.01, 0.925, f"{fmt % last_close}   {pct_sign}{pct:.2f}%   {tf_cfg['label']}",
                 color=pct_color, fontsize=10, va="top")
        fig.text(0.99, 0.97, "GeckoTerminal · XerisBot", color=SUBTEXT, fontsize=8, va="top", ha="right")
        ax.set_xlim(-0.6, n - 0.4); ax_vol.set_xlim(-0.6, n - 0.4)
        price_range = max(high_max - low_min, max(last_close * 0.02, 1e-12))
        pad = price_range * 0.06; ax.set_ylim(low_min - pad, high_max + pad)
        plt.tight_layout(rect=[0, 0, 1, 0.92])
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=140, bbox_inches="tight", facecolor=BG, edgecolor="none")
        plt.close(fig); buf.seek(0)
        print(f"✅ Chart rendered ({len(candles)} candles)"); return buf.read()
    except Exception as e:
        print(f"❌ Chart render error: {e}")
        try: plt.close("all")
        except Exception: pass
        return None

def _chart_job_key(channel_id: int, ca: str, timeframe: str) -> str:
    return f"{channel_id}:{ca}:{timeframe}"

def _chart_remaining_seconds(channel_id: int) -> int:
    return max(0, int(_chart_cooldowns.get(str(channel_id), 0.0) - time.time()))

def _set_chart_cooldown(channel_id: int) -> None:
    _chart_cooldowns[str(channel_id)] = time.time() + CHART_COOLDOWN_SECONDS


# ═════════════════════════════════════════════════════════════════════════════
# Bot commands
# ═════════════════════════════════════════════════════════════════════════════

async def cmd_price(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    data = await fetch_price_for_ca(ca)
    if not data:
        await send_message(channel_id, embeds=[{"title": "❌ Token Not Found",
            "description": f"No data found for:\n```{ca}```", "color": 0xEF4444}]); return
    change = data.get("change_24h", 0); sign = "+" if change >= 0 else ""
    color  = 0x10B981 if change >= 0 else 0xEF4444
    vol_mcap = (data["volume_24h"] / data["mcap"] * 100) if data.get("mcap") else 0
    await send_message(channel_id, embeds=[{
        "author": {"name": f"💰 Price Info · {data['name']} ({data['symbol']})"},
        "color":  color,
        "fields": [
            {"name": "📊 Market Data",
             "value": (f"```yaml\nPrice:     ${data['price']:.8f}\n24h Chg:   {sign}{change:.2f}%\n"
                       f"MCap:      {format_usd(data['mcap'])}\nVol (24h): {format_usd(data['volume_24h'])}\n"
                       f"V/MC:      {vol_mcap:.2f}%\nLiquidity: {format_usd(data['liquidity'])}\nDEX: {data['dex'].upper()}\n```"),
             "inline": False},
            {"name": "🔗 Charts",
             "value": (f"[DexScreener](https://dexscreener.com/solana/{ca}) · "
                       f"[Birdeye](https://birdeye.so/token/{ca}) · [Solscan](https://solscan.io/token/{ca})"),
             "inline": False}],
        "footer": {"text": f"Via DexScreener · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}])

async def cmd_whale(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    holders, price_data = await asyncio.gather(fetch_top_holders(ca), fetch_price_for_ca(ca))
    if not holders:
        await send_message(channel_id, embeds=[{"title": "❌ No Holder Data",
            "description": f"Could not fetch holders for:\n```{ca}```", "color": 0xEF4444}]); return
    total = 1_000_000_000; rows = []
    for i, h in enumerate(holders[:15]):
        amt  = float(h.get("uiAmount") or 0); pct = (amt / total) * 100
        addr = h.get("address", "???"); short = f"{addr[:6]}...{addr[-6:]}"
        bar  = "█" * max(1, round(pct)) + "░" * max(0, 10 - round(pct))
        rows.append(f"#{i+1:>2}  {short}  {pct:5.2f}%  {bar}")
    top10_pct = sum(float(h.get("uiAmount", 0)) / total * 100 for h in holders[:10])
    top5_pct  = sum(float(h.get("uiAmount", 0)) / total * 100 for h in holders[:5])
    risk_color = 0xEF4444 if top10_pct > 50 else (0xF59E0B if top10_pct > 30 else 0x10B981)
    name = price_data.get("name", "Unknown") if price_data else "Unknown"
    symbol = price_data.get("symbol", "???") if price_data else "???"
    await send_message(channel_id, embeds=[{
        "author":      {"name": f"🐳 Top Holders · {name} ({symbol})"},
        "color":       risk_color,
        "description": f"```\nRank  Wallet            Share  Bar\n{'─'*42}\n" + "\n".join(rows) + "\n```",
        "fields": [
            {"name": "📊 Concentration",
             "value": (f"```yaml\nTop 5:  {top5_pct:.2f}%\nTop 10: {top10_pct:.2f}%\n"
                       f"Risk:   {'🔴 HIGH' if top10_pct > 50 else ('🟡 MEDIUM' if top10_pct > 30 else '🟢 LOW')}\n```"),
             "inline": False},
            {"name": "🔍 Explore",
             "value": f"[Solscan Holders](https://solscan.io/token/{ca}#holders) · [Birdeye](https://birdeye.so/token/{ca})",
             "inline": False}],
        "footer": {"text": f"Via Helius RPC · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}])

async def cmd_chart(channel_id: int, ca: str, timeframe: str = "15m", bypass_cooldown: bool = False) -> None:
    tf_clean = timeframe.lower().strip()
    if tf_clean not in TIMEFRAME_MAP: tf_clean = "15m"
    job_key = _chart_job_key(channel_id, ca, tf_clean)
    if not bypass_cooldown:
        remaining = _chart_remaining_seconds(channel_id)
        if remaining > 0:
            existing = _chart_pending_jobs.get(job_key)
            if existing and not existing.done():
                await send_temp_message(channel_id,
                    content=f"⏳ Chart on cooldown. **{tf_clean}** request already queued. Wait **{remaining}s**.",
                    delete_after=CHART_WAIT_MESSAGE_DELETE_SECONDS); return
            await send_temp_message(channel_id,
                content=f"⏳ Chart on cooldown. Queued **{tf_clean}** — auto-sends in **{remaining}s**.",
                delete_after=CHART_WAIT_MESSAGE_DELETE_SECONDS)
            async def _delayed():
                try: await asyncio.sleep(remaining); await cmd_chart(channel_id, ca, tf_clean, bypass_cooldown=True)
                except asyncio.CancelledError: pass
                finally: _chart_pending_jobs.pop(job_key, None)
            _chart_pending_jobs[job_key] = asyncio.create_task(_delayed()); return

    await send_typing(channel_id); _set_chart_cooldown(channel_id)
    tf_cfg = TIMEFRAME_MAP[tf_clean]; tf_label = tf_cfg["label"]; res = tf_cfg["resolution"]
    gt = await fetch_geckoterminal(ca)
    if not gt or not gt.get("pool_address"):
        await send_message(channel_id, embeds=[{"title": "⚠️ Token Not Found",
            "description": f"Could not find pool data for:\n```{ca}```", "color": 0xF59E0B,
            "fields": [{"name": "🔗 Try manually", "value": f"[DexScreener](https://dexscreener.com/solana/{ca})", "inline": False}],
            "footer": {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
            "timestamp": get_timestamp()}]); return

    pool = gt["pool_address"]; name = gt["name"]; price = gt["price_usd"]
    p24h = gt["price_change_24h"]; color = 0x10B981 if p24h >= 0 else 0xEF4444
    total_txns = gt["buys_24h"] + gt["sells_24h"]
    buy_ratio  = (gt["buys_24h"] / total_txns * 100) if total_txns > 0 else 0.0
    def gt_url(r): return f"https://www.geckoterminal.com/solana/pools/{pool}?resolution={r}"
    def pct_str(p): return f"{'🟢' if p >= 0 else '🔴'} {'+' if p >= 0 else ''}{p:.2f}%"
    await send_message(channel_id, embeds=[{"description": f"📊 Generating **{tf_label}** chart for `{name}`…", "color": 0x6366F1}])
    chart_bytes = await generate_chart_image(ca=ca, timeframe=tf_clean, token_name=name, pool_address=pool)
    embed = {
        "author": {"name": f"📊 {name} · {tf_label} Chart"},
        "title": f"${price:.8f}", "url": gt_url(res), "color": color,
        "description": f"[`1m`]({gt_url(1)})  ·  [`5m`]({gt_url(5)})  ·  [`15m`]({gt_url(15)})  ·  [`1H`]({gt_url(60)})  ·  [`1D`]({gt_url(1440)})",
        "fields": [
            {"name": "📈 Price Change",
             "value": f"```yaml\n5m  : {pct_str(gt['price_change_5m'])}\n1h  : {pct_str(gt['price_change_1h'])}\n24h : {pct_str(p24h)}\n```",
             "inline": True},
            {"name": "💧 Market Data",
             "value": f"```yaml\nLiquidity : {format_usd(gt['liquidity'])}\nVol 24h   : {format_usd(gt['volume_24h'])}\nFDV       : {format_usd(gt['fdv'])}\n```",
             "inline": True},
            {"name": "🔄 24h Txns",
             "value": f"```yaml\nBuys      : {gt['buys_24h']:,}\nSells     : {gt['sells_24h']:,}\nBuy Ratio : {buy_ratio:.1f}%\n```",
             "inline": True},
            {"name": "🔗 Links",
             "value": f"[DexScreener](https://dexscreener.com/solana/{ca}) · [Birdeye](https://birdeye.so/token/{ca}) · [GeckoTerminal]({gt_url(res)})",
             "inline": False}],
        "footer": {"text": f"GeckoTerminal · {tf_label} · cooldown {CHART_COOLDOWN_SECONDS}s · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}
    if chart_bytes:
        embed["image"] = {"url": "attachment://chart.png"}
        headers = {"Authorization": f"Bot {DISCORD_TOKEN}", "User-Agent": "XerisBot/2.0"}
        async with httpx.AsyncClient(timeout=25.0) as client:
            r = await client.post(f"{DISCORD_API}/channels/{channel_id}/messages", headers=headers,
                files={"file": ("chart.png", chart_bytes, "image/png")},
                data={"payload_json": json.dumps({"embeds": [embed]})})
        if r.status_code not in (200, 201):
            embed["description"] += "\n\n⚠️ *Image upload failed.*"
            await send_message(channel_id, embeds=[embed])
    else:
        embed["description"] += "\n\n⚠️ *Chart unavailable — click a timeframe above to view on GeckoTerminal*"
        await send_message(channel_id, embeds=[embed])

async def cmd_order(channel_id: int, db: DatabaseManager, ms: MarketState) -> None:
    await send_typing(channel_id)
    orders = await db.get_active_orders()
    if not orders:
        await send_message(channel_id, embeds=[{
            "author": {"name": "📋 Limit Order Book"}, "title": "No Active Limit Orders",
            "description": "> No limit orders are currently being tracked for XERIS.",
            "color": 0x6B7280, "footer": {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
            "timestamp": get_timestamp()}]); return
    buys  = sorted([o for o in orders if o["order_type"] == "LIMIT_BUY"],
                   key=lambda x: (x.get("predicted_mcap", 0) <= 0, x.get("predicted_mcap", 0)))
    sells = sorted([o for o in orders if o["order_type"] == "LIMIT_SELL"],
                   key=lambda x: (x.get("predicted_mcap", 0) <= 0, x.get("predicted_mcap", 0)))
    total_buy_wall  = sum(o["usd_value"] for o in buys)
    total_sell_wall = sum(o["usd_value"] for o in sells)
    await send_message(channel_id, embeds=[{
        "author": {"name": "📋 XERIS · Live Limit Order Book"},
        "title":  f"{len(orders)} Active Order(s) Tracked",
        "description": (f"```yaml\nCurrent Price : ${ms.current_price:.8f}\n"
                        f"Current MCap  : {format_usd(ms.current_market_cap)}\n"
                        f"───────────────────────────────\n"
                        f"Buy  Orders   : {len(buys)}  │  Wall: {format_usd(total_buy_wall)}\n"
                        f"Sell Orders   : {len(sells)}  │  Wall: {format_usd(total_sell_wall)}\n```"),
        "color": 0x8B5CF6,
        "footer": {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}])
    for label, order_list, color, wall_label, wall_total in [
        ("🛡️ BUY ORDERS (Support)",     buys,  0x10B981, "Total Buy Wall",  total_buy_wall),
        ("⚠️ SELL ORDERS (Resistance)", sells, 0xEF4444, "Total Sell Wall", total_sell_wall)]:
        if not order_list:
            await send_message(channel_id, embeds=[{"description": "> No active orders.", "color": 0x6B7280}]); continue
        lines = ""; page = 1; start_i = 0
        for i, o in enumerate(order_list[:10]):
            dist = _pct_from_current(o["predicted_mcap"], ms)
            qt   = f"[{o['quote_token']}] " if o.get("quote_token") else ""
            exch = o.get("exchange", ""); wallet_s = f"{o['wallet'][:6]}…{o['wallet'][-4:]}"
            lines += (f"**`#{i+1}`** {qt}`{format_tokens(o['token_amount'])} XERIS` · **{format_usd(o['usd_value'])}**\n"
                      f"┣ Target MCap : `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%)\n"
                      f"┣ Wallet      : [`{wallet_s}`](https://solscan.io/account/{o['wallet']})"
                      + (f" via `{exch}`" if exch else "") + "\n"
                      f"┣ Placed      : `{_format_time_placed(o)}`\n"
                      f"┗ Remaining   : `{_format_time_remaining(o)}`\n\n")
            if (i + 1) % 5 == 0 and i + 1 < len(order_list):
                await send_message(channel_id, embeds=[{"author": {"name": f"{label} · Page {page}"},
                    "description": lines, "color": color,
                    "footer": {"text": f"Orders #{start_i+1}–#{i+1} of {len(order_list)}"}}])
                lines = ""; page += 1; start_i = i + 1
        if lines:
            await send_message(channel_id, embeds=[{
                "author": {"name": f"{label} · {len(order_list)} total"},
                "description": lines, "color": color,
                "fields": [{"name": f"💰 {wall_label}", "value": f"`{format_usd(wall_total)}`", "inline": True},
                            {"name": "📊 Nearest", "value": f"`{format_usd(order_list[0]['predicted_mcap'])}`", "inline": True}],
                "footer": {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
                "timestamp": get_timestamp()}])

async def cmd_analyze(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    await send_message(channel_id, embeds=[{"title": "🔍 Running Full Risk Scan...",
        "description": f"```{ca}```\n⛓️ Fetching on-chain data...\n🤖 AI analysis via Groq LLaMA3 (15-25s)",
        "color": 0x6366F1}])
    price_data, holders = await asyncio.gather(fetch_price_for_ca(ca), fetch_top_holders(ca))
    if not price_data:
        await send_message(channel_id, embeds=[{"title": "❌ Token Not Found",
            "description": f"No market data for:\n```{ca}```", "color": 0xEF4444}]); return
    name = price_data.get("name", "Unknown"); symbol = price_data.get("symbol", "???")
    intelligence = await fetch_all_intelligence(ca, name, symbol)
    socials      = intelligence.get("socials", {}); token_meta = intelligence.get("token_meta", {})
    pumpfun      = intelligence.get("pumpfun", {}); deployer   = intelligence.get("deployer", {})
    ai = await groq_analyze(ca, price_data, holders, intelligence)
    if "error" in ai:
        await send_message(channel_id, embeds=[{"title": "❌ AI Analysis Failed",
            "description": f"```{ai['error']}```", "color": 0xEF4444}]); return
    score = int(ai.get("risk_score", 5)); rug_label = ai.get("rug_label", "UNKNOWN")
    color = score_to_color(score); rug_emoji = rug_label_emoji(rug_label)
    liq = price_data.get("liquidity", 0); vol = price_data.get("volume_24h", 0); mcap = price_data.get("mcap", 0)
    total_supply = 1_000_000_000
    top5_pct  = sum(float(h.get("uiAmount",0)) for h in holders[:5])  / total_supply * 100 if holders else 0
    top10_pct = sum(float(h.get("uiAmount",0)) for h in holders[:10]) / total_supply * 100 if holders else 0
    dep_wallet = deployer.get("wallet") or token_meta.get("mint_authority") or "Unknown"
    dep_short  = f"{dep_wallet[:8]}...{dep_wallet[-6:]}" if dep_wallet and len(dep_wallet) > 14 else dep_wallet
    await send_message(channel_id, embeds=[{
        "author": {"name": f"🛡️ AI Risk Report · {name} ({symbol})"},
        "title":  f"`{ca[:20]}...{ca[-8:]}`",
        "description": f"> {ai.get('summary', 'No summary.')}",
        "color": color,
        "fields": [
            {"name": f"{rug_emoji} Risk Score", "value": risk_score_bar(score), "inline": True},
            {"name": "⚠️ Verdict", "value": f"**{rug_emoji} {rug_label}**", "inline": True},
            {"name": "📅 Token Age", "value": f"`{token_meta.get('token_age_days','?')} days`", "inline": True},
            {"name": "📊 Market Data",
             "value": (f"```yaml\nPrice:    ${price_data['price']:.8f}\nMCap:     {format_usd(mcap)}\n"
                       f"Vol 24h:  {format_usd(vol)}\nLiq:      {format_usd(liq)}\n```"),
             "inline": True},
            {"name": "📣 Socials & Holders",
             "value": (f"```yaml\nTwitter:  {socials.get('twitter_handle') or 'NOT FOUND'}\n"
                       f"Website:  {'Found ✅' if socials.get('website') else 'Not Found ❌'}\n"
                       f"Top 5:    {top5_pct:.1f}%\nTop 10:   {top10_pct:.1f}%\n```"),
             "inline": True},
            {"name": "🔴 Red Flags", "value": "\n".join(f"• {f}" for f in ai.get("red_flags", [])) or "• None detected", "inline": False},
            {"name": "🟢 Green Signals", "value": "\n".join(f"• {f}" for f in ai.get("green_flags", [])) or "• None detected", "inline": False},
            {"name": "💡 Trade Advice", "value": f"> {ai.get('trade_advice','N/A')[:1020]}", "inline": False},
            {"name": "🔗 Verify",
             "value": (f"[DexScreener](https://dexscreener.com/solana/{ca}) · [Solscan](https://solscan.io/token/{ca})"
                       + (f" · [Pump.fun](https://pump.fun/{ca})" if pumpfun.get("is_pumpfun") else "")),
             "inline": False}],
        "footer": {"text": f"Powered by Groq LLaMA-3.3-70B · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}])

async def cmd_help(channel_id: int) -> None:
    await send_message(channel_id, embeds=[{
        "author": {"name": "🤖 XerisBot — Command Reference"},
        "color":  0x6366F1,
        "description": "All commands use `!` prefix.",
        "fields": [
            {"name": "📈 !price <CA>",       "value": "Price, market cap, 24h volume & change", "inline": False},
            {"name": "🐳 !whale <CA>",       "value": "Top 15 holders with concentration risk",  "inline": False},
            {"name": "📊 !chart <CA> [tf]",  "value": "Live chart · Timeframes: `1m` `5m` `15m` `1h` `1d`", "inline": False},
            {"name": "🛡️ !analyze <CA>",    "value": "Full AI risk analysis via Groq LLaMA3",   "inline": False},
            {"name": "📋 !order",            "value": "Live limit order book (support & resistance)", "inline": False},
            {"name": "❓ !help",             "value": "Show this menu",                           "inline": False}],
        "footer": {"text": "XerisBot · Helius + DexScreener + Groq"},
        "timestamp": get_timestamp()}])


# ═════════════════════════════════════════════════════════════════════════════
# Discord command router
# ═════════════════════════════════════════════════════════════════════════════

async def handle_message(msg: dict) -> None:
    content    = (msg.get("content") or "").strip()
    channel_id = int(msg.get("channel_id", 0))
    author     = msg.get("author", {})
    if author.get("bot") or not content.startswith("!"): return
    parts = content.split(); command = parts[0].lower()
    arg   = parts[1].strip() if len(parts) > 1 else None
    print(f"\n💬 {content} | ch:{channel_id} | {author.get('username')}")
    if command == "!help":
        await cmd_help(channel_id)
    elif command == "!price":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content="❌ Usage: `!price <contract_address>`"); return
        await cmd_price(channel_id, arg)
    elif command == "!whale":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content="❌ Usage: `!whale <contract_address>`"); return
        await cmd_whale(channel_id, arg)
    elif command == "!chart":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content="❌ Usage: `!chart <CA> [tf]`\nTimeframes: `1m` `5m` `15m` `1h` `1d`"); return
        tf = parts[2].lower() if len(parts) > 2 else "15m"
        if tf not in TIMEFRAME_MAP: tf = "15m"
        await cmd_chart(channel_id, arg, timeframe=tf)
    elif command == "!analyze":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content="❌ Usage: `!analyze <contract_address>`"); return
        await cmd_analyze(channel_id, arg)
    elif command in ("!order", "!orders"):
        if _db_ref and _ms_ref:
            await cmd_order(channel_id, _db_ref, _ms_ref)
        else:
            await send_message(channel_id, content="❌ Order tracker not initialized yet.")


# ═════════════════════════════════════════════════════════════════════════════
# Discord gateway
# ═════════════════════════════════════════════════════════════════════════════

async def discord_gateway() -> None:
    heartbeat_interval = None; sequence = None
    print("\n📡 Connecting to Discord Gateway...")
    while True:
        try:
            async with websockets.connect(GATEWAY_URL) as ws:
                print("✅ Discord Gateway connected")
                heartbeat_task = None
                async def send_heartbeat():
                    while True:
                        await asyncio.sleep(heartbeat_interval / 1000)
                        await ws.send(json.dumps({"op": 1, "d": sequence}))
                async for raw in ws:
                    data = json.loads(raw); op = data.get("op"); t = data.get("t"); d = data.get("d") or {}
                    if s := data.get("s"): sequence = s
                    if op == 10:
                        heartbeat_interval = d["heartbeat_interval"]
                        heartbeat_task     = asyncio.create_task(send_heartbeat())
                        await ws.send(json.dumps({"op": 2, "d": {"token": DISCORD_TOKEN, "intents": 33280,
                            "properties": {"$os": "linux", "$browser": "xerisbot", "$device": "xerisbot"}}}))
                        print("✅ Discord Gateway identified")
                    elif t == "READY":
                        user = d.get("user", {})
                        print(f"✅ Logged in as {user.get('username')}#{user.get('discriminator')}")
                    elif t == "MESSAGE_CREATE":
                        await handle_message(d)
        except Exception as e:
            print(f"❌ Gateway error: {e} — reconnecting in 5s...")
            await asyncio.sleep(5)


# ═════════════════════════════════════════════════════════════════════════════
# Helius monitor
# ═════════════════════════════════════════════════════════════════════════════

async def helius_monitor(db: DatabaseManager, ms: MarketState) -> None:
    global _db_ref, _ms_ref
    _db_ref = db; _ms_ref = ms
    classifier    = TransactionClassifier()
    tracker       = OrderTracker(db, classifier, ms)
    alert_manager = AlertManager(db, ms)
    print("\n🔭 Starting Helius monitor...")
    await update_price(ms)
    retry_count = 0; tx_count = 0
    while True:
        try:
            print(f"\n📡 Helius WS connecting (attempt {retry_count+1})...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=60) as ws:
                print("✅ Helius WebSocket connected")
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                    "params": [{"mentions": [MINT]}, {"commitment": "confirmed"}]}))
                print(f"✅ Subscribed to {MINT[:16]}...")
                retry_count = 0
                while True:
                    try:
                        if time.time() - ms.last_price_update > PRICE_UPDATE_INTERVAL:
                            await update_price(ms)
                        await alert_manager.tick()
                        msg  = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)
                        if "params" not in data: continue
                        signature = data["params"]["result"]["value"]["signature"]
                        tx_count += 1
                        print(f"\n{'─'*50}")
                        print(f"TX #{tx_count}  {signature[:24]}…  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
                        tx_data = await fetch_tx(signature, retries=3)
                        if not tx_data: continue
                        result = await tracker.process(tx_data, signature)
                        if not result: continue
                        action = result["action"]
                        if action == "new_limit":
                            order: LimitOrder = result["order"]; info = result["info"]
                            qt = info.get("quote_token", ""); exch = info.get("exchange", "")
                            role = "SUPPORT" if order.order_type == OrderType.LIMIT_BUY else "RESISTANCE"
                            print(f"  📌 {order.order_type.value}  {format_tokens(order.token_amount)} XERIS  {format_usd(order.usd_value)}  → {role}")
                            if order.usd_value >= WHALE_MIN_USD:
                                await send_message(ALERT_CHANNEL_ID, embeds=[_build_limit_order_embed(order, ms, qt, exch)])
                        elif action == "cancel_limit":
                            if result.get("cancelled"): print(f"  🗑️  CANCEL  {result['cancelled']['order_type']}")
                        elif action == "market":
                            tx_type: OrderType = result["type"]; info = result["info"]; fills = result.get("fills", [])
                            usd_val = info.get("usd_value", info["amount"] * ms.current_price)
                            side = tx_type.value.replace("MARKET_", "")
                            qt   = info.get("quote_token", ""); exch = info.get("exchange", "")
                            if info["amount"] <= 0 or usd_val <= 0: continue
                            print(f"  💱 {tx_type.value}  {format_tokens(info['amount'])} XERIS  {format_usd(usd_val)}")
                            wallet = info.get("wallet", "")
                            if wallet == DEV_WALLET and "SELL" in tx_type.value:
                                print("🚨 DEV SELL!")
                                await send_message(ALERT_CHANNEL_ID, embeds=[_build_dev_sell_embed(info["amount"], wallet, usd_val, signature, ms, qt)])
                            elif usd_val >= WHALE_MIN_USD:
                                print(f"🐋 WHALE {tx_type.value} — {format_usd(usd_val)}")
                                await send_message(ALERT_CHANNEL_ID,
                                    embeds=[_build_whale_embed(side, info["amount"], wallet, usd_val, signature, ms, qt, exch)],
                                    mention_everyone=True)
                        elif action == "transfer":
                            print(f"  ↗️  TRANSFER  {format_tokens(result['info']['amount'])} XERIS")
                    except asyncio.TimeoutError:
                        await ws.ping()
                    except websockets.exceptions.ConnectionClosed:
                        print("⚠️ Helius WS closed"); break
                    except Exception as e:
                        print(f"❌ {e}")
        except Exception as e:
            retry_count += 1; wait = min(30 * retry_count, 300)
            print(f"❌ Helius error: {e} — retry in {wait}s")
            await asyncio.sleep(wait)

# ═════════════════════════════════════════════════════════════════════════════
# Startup announcement
# ═════════════════════════════════════════════════════════════════════════════

async def announce_startup() -> None:
    await asyncio.sleep(3)
    await send_message(ALERT_CHANNEL_ID, embeds=[{
        "author": {"name": "XerisBot — System Online"},
        "title":  "🛰️ Bot Started · All Systems Active",
        "description": (
            "```\n╔══════════════════════════════════════╗\n"
            "║  REAL-TIME MONITORING ACTIVE        ║\n"
            "║  • Whale & Dev Activity Tracking    ║\n"
            "║  • Limit Order Detection            ║\n"
            "║  • Price Movement Alerts            ║\n"
            "║  • AI Risk Analysis (!analyze)      ║\n"
            "╚══════════════════════════════════════╝\n```\n> Type `!help` to see all commands"),
        "color":  0x10B981,
        "fields": [
            {"name": "🐋 Whale Threshold", "value": f"`${WHALE_MIN_USD:,} USD`",        "inline": True},
            {"name": "📈 Price Alert",     "value": f"`±{PRICE_ALERT_THRESHOLD}%`",      "inline": True},
            {"name": "🤖 AI Engine",       "value": f"`Groq {GROQ_MODEL}`",              "inline": True},
            {"name": "🎯 Monitored Token", "value": f"`{MINT}`",                         "inline": False}],
        "footer": {"text": f"Started at {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp()}])


# ═════════════════════════════════════════════════════════════════════════════
# Main entry (called from app.py)
# ═════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    from pathlib import Path
    Path("runtime").mkdir(exist_ok=True)   # ensure runtime dir exists

    print("\n" + "=" * 62)
    print("  🚀  XERISBOT — DISCORD BOT + THREE-TIER MONITOR")
    print("=" * 62)
    print(f"  Mint        {MINT}")
    print(f"  Dev         {DEV_WALLET}")
    print(f"  Whale ≥     ${WHALE_MIN_USD:,}")
    print(f"  Channel     {ALERT_CHANNEL_ID}")
    print(f"  Debug Ch    {DEBUG_CHANNEL_ID or 'disabled'}")
    print(f"  Groq        {'✅ ' + GROQ_MODEL if GROQ_ENABLED else '❌ disabled'}")
    print(f"  Order TTL   {ORDER_TTL_HOURS}h ({ORDER_TTL_HOURS // 24} days)")
    print(f"  DB          {DB_PATH}")
    print("=" * 62 + "\n")

    init_discord_queue()
    db = DatabaseManager(DB_PATH)
    await db.initialize()
    removed = await db.cleanup_stale()
    if removed:
        print(f"🧹 Startup cleanup: removed {removed} stale order(s)")
    ms = MarketState()
    
    try:
        await asyncio.gather(
            discord_gateway(),
            helius_monitor(db, ms),
            x_post_monitor(db),
            announce_startup(),
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n👋 Shutting down…")
    finally:
        await db.close()
        if _discord_queue:
            await _discord_queue.stop()
        print("✅ Clean shutdown")
