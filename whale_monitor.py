import asyncio
import json
from unittest import signals
import httpx
import websockets
import time
import os
import re
import random
import sqlite3
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, Dict, List
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple, Set, Any
from collections import defaultdict
from collections import defaultdict
from typing import Set, Any
import base64
import io
import io
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

def load_env():
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key   = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and value:
                os.environ[key] = value

load_env()

PROGRAMS_FILE = os.getenv("PROGRAMS_FILE", "programs.json")

WSOL_MINT         = "So11111111111111111111111111111111111111112"
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022    = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

def load_programs() -> Dict[str, Any]:
    """Load program definitions from JSON file."""
    try:
        with open(PROGRAMS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Could not load programs.json: {e}")
        # Return minimal default set
        return {
            "known_programs": {},
            "aggregator_programs": [],
            "swap_programs": [],
            "token_programs": [
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
            ]
        }

# Load programs
_PROGRAMS_CACHE = load_programs()

# Build program sets
EXCHANGE_REGISTRY: Dict[str, Dict] = _PROGRAMS_CACHE.get("known_programs", {})
DEX_PROGRAMS: set = {pid for pid, v in EXCHANGE_REGISTRY.items() if v.get("role") in ("market", "hybrid")}
LIMIT_ORDER_PROGRAMS: set = {pid for pid, v in EXCHANGE_REGISTRY.items() if v.get("role") in ("limit", "hybrid")}
ALL_KNOWN_PROGRAMS: set = set(EXCHANGE_REGISTRY.keys())

# Additional program sets for better classification
AGGREGATOR_PROGRAMS: Set[str] = set(_PROGRAMS_CACHE.get("aggregator_programs", []))
SWAP_PROGRAMS: Set[str] = set(_PROGRAMS_CACHE.get("swap_programs", []))
ALL_SWAP_PROGRAMS: Set[str] = AGGREGATOR_PROGRAMS | SWAP_PROGRAMS
TOKEN_PROGRAMS: Set[str] = set(_PROGRAMS_CACHE.get("token_programs", []))

SYSTEM_PROGRAMS: set = {
    SPL_TOKEN_PROGRAM,
    SPL_TOKEN_2022,
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bN",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ",
    "BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi",
    "L2TExMFKdjpN9kozasaurPirfHy9P8sbXoAN1qA3S95",
    "TokenRouterHb9WbEnygEMRPVXsWnfAGFGYpBLSxTFV",
    "AddressLookupTab1e1111111111111111111111111",
    "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo",
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
}

def exchange_name(pid: str) -> str:
    entry = EXCHANGE_REGISTRY.get(pid)
    return entry["name"] if entry else f"Unknown ({pid[:8]}…)"

DISCRIMINATORS: Dict[bytes, str] = {
    bytes.fromhex("b712469c946da122"): "new_order",
    bytes.fromhex("a4b235c6d7e8f900"): "cancel_order",
    bytes.fromhex("e5c2f8d2a37fe3a5"): "new_order",
    bytes.fromhex("d8c1ec3b84f53e72"): "cancel_order",
    bytes.fromhex("9d6b3c4e5f7a8b2d"): "new_order",
    bytes.fromhex("1a2b3c4d5e6f7a8b"): "cancel_order",
    bytes.fromhex("3f4a5b6c7d8e9f00"): "new_order",
    bytes.fromhex("0f1a2b3c4d5e6f7a"): "cancel_order",
}

KNOWN_TOKEN_LABELS: Dict[str, str] = {
    "So11111111111111111111111111111111111111112":   "SOL",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB":  "USDT",
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263":  "BONK",
    "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm":  "WIF",
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN":   "JUP",
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So":  "mSOL",
    "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs":  "ETH",
    "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh":  "BTC",
    "HZ1JovNiVvGrCNiiYWY1ZZgEs1y2qkFjK3pCCZHIg9do":  "RENDER",
    "SHDWyBxihqiCj6YekG2GUr7wqKLeLAMK1gHZck9pL6y":   "SHDW",
    "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE":   "ORCA",
}

ORDER_TTL_HOURS = 168
ORDER_TTL_SECS  = ORDER_TTL_HOURS * 3600


class OrderType(Enum):
    MARKET_BUY   = "MARKET_BUY"
    MARKET_SELL  = "MARKET_SELL"
    LIMIT_BUY    = "LIMIT_BUY"
    LIMIT_SELL   = "LIMIT_SELL"
    CANCEL_LIMIT = "CANCEL_LIMIT"
    TRANSFER     = "TRANSFER"
    UNKNOWN      = "UNKNOWN"


@dataclass
class LimitOrder:
    signature:      str
    wallet:         str
    order_type:     OrderType
    token_amount:   float
    usd_value:      float
    predicted_mcap: float
    target_price:   float
    timestamp:      float
    is_active:      bool = True


@dataclass
class MarketState:
    current_price:        float = 0.0
    current_market_cap:   float = 0.0
    total_supply:         float = 0.0
    last_price_update:    float = 0.0
    price_reference:      float = 0.0
    last_alert_up_time:   float = 0.0
    last_alert_down_time: float = 0.0
    last_alert_direction: Optional[str] = None
    sol_price_usd:        float = 150.0


MINT            = os.getenv("MINT",         "9ezFthWrDUpSSeMdpLW6SDD9TJigHdc4AuQ5QN5bpump")
DEV_WALLET      = os.getenv("DEV_WALLET",   "6XjutcUVEidzb3o1yXLYGC2ZSnjde2YvAUF9CiPVqxwm")
WHALE_MIN_USD   = int(os.getenv("WHALE_MIN_USD", "500"))
DB_PATH         = os.getenv("DB_PATH",      "limit_orders.db")
HELIUS_API_KEY  = os.getenv("HELIUS_API_KEY",  "")
DISCORD_TOKEN   = os.getenv("DISCORD_TOKEN",   "")
DISCORD_CHANNEL = os.getenv("DISCORD_CHANNEL", "")
GROQ_API_KEY    = os.getenv("GROQ_API_KEY",    "")
DEBUG_CHANNEL   = os.getenv("DEBUG_CHANNEL_ID", "0")

for _name, _val in [
    ("HELIUS_API_KEY",  HELIUS_API_KEY),
    ("DISCORD_TOKEN",   DISCORD_TOKEN),
    ("DISCORD_CHANNEL", DISCORD_CHANNEL),
    ("GROQ_API_KEY",    GROQ_API_KEY),
]:
    if not _val:
        raise ValueError(f"❌ {_name} environment variable is required!")

ALERT_CHANNEL_ID = int(DISCORD_CHANNEL)
DEBUG_CHANNEL_ID = int(DEBUG_CHANNEL) if DEBUG_CHANNEL and DEBUG_CHANNEL != "0" else None

GROQ_MODEL             = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_MIN_CONFIDENCE    = 0.65
GROQ_ENABLED           = bool(GROQ_API_KEY)
SUSPICION_THRESHOLD    = 0.25
SUMMARY_ALERT_INTERVAL = 600
PRICE_UPDATE_INTERVAL  = 30
PRICE_ALERT_THRESHOLD  = 5.0
PRICE_ALERT_COOLDOWN   = 300
CLEANUP_INTERVAL       = 3600

WS_URL  = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

DISCORD_API = "https://discord.com/api/v10"
GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
GROQ_URL    = "https://api.groq.com/openai/v1/chat/completions"
VALID_CA    = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


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
def get_all_instructions(tx_data: Dict) -> List[Dict]:
    """
    Return both outer + inner instructions in one flat list.
    """
    message = tx_data.get("transaction", {}).get("message", {})
    meta = tx_data.get("meta", {})

    out = list(message.get("instructions", []) or [])
    for group in meta.get("innerInstructions", []) or []:
        out.extend(group.get("instructions", []) or [])
    return out

def get_signer_token_deltas(tx_data: Dict, signer: str) -> Dict[str, float]:
    """
    Compute token deltas ONLY for token accounts owned by the signer.
    Returns: {mint: net_delta_ui_amount}
    """
    meta = tx_data.get("meta", {})

    pre = [b for b in (meta.get("preTokenBalances") or []) if b.get("owner") == signer]
    post = [b for b in (meta.get("postTokenBalances") or []) if b.get("owner") == signer]

    pre_map: Dict[Tuple[str, int], Tuple[int, int]] = {}
    post_map: Dict[Tuple[str, int], Tuple[int, int]] = {}

    for bal in pre:
        mint = bal.get("mint")
        idx = bal.get("accountIndex")
        if mint is None or idx is None:
            continue
        amt = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
        dec = int((bal.get("uiTokenAmount") or {}).get("decimals") or 0)
        pre_map[(mint, idx)] = (amt, dec)

    for bal in post:
        mint = bal.get("mint")
        idx = bal.get("accountIndex")
        if mint is None or idx is None:
            continue
        amt = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
        dec = int((bal.get("uiTokenAmount") or {}).get("decimals") or 0)
        post_map[(mint, idx)] = (amt, dec)

    deltas = defaultdict(float)
    all_keys = set(pre_map) | set(post_map)

    for mint, idx in all_keys:
        pre_amt, pre_dec = pre_map.get((mint, idx), (0, 0))
        post_amt, post_dec = post_map.get((mint, idx), (0, pre_dec))
        dec = post_dec if post_dec else pre_dec
        divisor = 10 ** dec if dec >= 0 else 1
        deltas[mint] += (post_amt - pre_amt) / divisor

    return dict(deltas)

def get_all_program_ids(tx_data: Dict) -> Set[str]:
    programs = set()
    for ix in get_all_instructions(tx_data):
        pid = ix.get("programId")
        if pid:
            programs.add(pid)
    return programs

def _pick_best_pair(pairs: List[Dict]) -> Optional[Dict]:
    if not pairs:
        return None
    return max(
        pairs,
        key=lambda p: (
            float((p.get("liquidity") or {}).get("usd") or 0),
            float((p.get("volume") or {}).get("h24") or 0),
            float(p.get("fdv") or p.get("marketCap") or 0),
        ),
    )

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
        "Content-Type":  "application/json",
        "User-Agent":    "XerisBot/2.0",
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        return await client.request(method, f"{DISCORD_API}{path}", headers=headers, **kwargs)

async def _send_message_direct(channel_id: int, payload: dict) -> bool:
    r = await _discord_request("POST", f"/channels/{channel_id}/messages", json=payload)
    if r.status_code not in (200, 201):
        print(f"   ❌ Discord {r.status_code}: {r.text[:150]}")
        return False
    print("   ✅ Message sent")
    return True

async def send_message(channel_id: int, content: str = None,
                        embeds: list = None, mention_everyone: bool = False) -> None:
    payload: dict = {}
    if content or mention_everyone:
        payload["content"] = ("@everyone " if mention_everyone else "") + (content or "")
    if embeds:
        payload["embeds"] = embeds
    if _discord_queue:
        await _discord_queue.enqueue(channel_id, payload)
    else:
        await _send_message_direct(channel_id, payload)

async def send_typing(channel_id: int) -> None:
    await _discord_request("POST", f"/channels/{channel_id}/typing")


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
            c = self._get_conn()
            c.execute(sql, params)
            c.commit()
        await asyncio.to_thread(_go)

    async def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        def _go():
            c = self._get_conn()
            rows = c.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
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
                )
            """)
            for col, defn in [("quote_token", "TEXT NOT NULL DEFAULT ''"),
                               ("exchange",    "TEXT NOT NULL DEFAULT ''")]:
                try:
                    c.execute(f"ALTER TABLE limit_orders ADD COLUMN {col} {defn}")
                except Exception:
                    pass
            c.execute("CREATE INDEX IF NOT EXISTS idx_wallet_active ON limit_orders(wallet, is_active)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_mcap_active ON limit_orders(predicted_mcap, is_active)")
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
        await self._exec(
            """
            INSERT OR REPLACE INTO limit_orders
              (signature, wallet, order_type, token_amount, usd_value,
               predicted_mcap, target_price, quote_token, exchange, timestamp, is_active)
            VALUES (?,?,?,?,?,?,?,?,?,?,1)
            """,
            (order.signature, order.wallet, order.order_type.value,
             order.token_amount, order.usd_value, order.predicted_mcap,
             order.target_price, quote_token, exchange, order.timestamp),
        )

    async def deactivate_by_signature(self, sig: str) -> None:
        await self._exec("UPDATE limit_orders SET is_active = 0 WHERE signature = ?", (sig,))

    async def deactivate_one_by_wallet(self, wallet: str, order_type: str) -> Optional[Dict]:
        row = await self._fetchone(
            """
            SELECT * FROM limit_orders
            WHERE wallet = ? AND order_type = ? AND is_active = 1
            ORDER BY timestamp DESC LIMIT 1
            """,
            (wallet, order_type),
        )
        if row:
            await self._exec(
                "UPDATE limit_orders SET is_active = 0 WHERE signature = ?",
                (row["signature"],),
            )
            return row
        return None

    async def cleanup_stale(self, max_age_hours: int = ORDER_TTL_HOURS) -> int:
        cutoff = time.time() - max_age_hours * 3600
        rows = await self._fetchall(
            "SELECT * FROM limit_orders WHERE timestamp < ? AND is_active = 1", (cutoff,)
        )
        if rows:
            await self._exec(
                "UPDATE limit_orders SET is_active = 0 WHERE timestamp < ? AND is_active = 1",
                (cutoff,),
            )
            print(f"🧹 Cleaned {len(rows)} stale order(s) older than {max_age_hours}h")
        return len(rows)

    async def get_active_orders(self) -> List[Dict]:
        return await self._fetchall(
            "SELECT * FROM limit_orders WHERE is_active = 1 ORDER BY predicted_mcap ASC"
        )

    async def get_orders_by_wallet(self, wallet: str) -> List[Dict]:
        return await self._fetchall(
            "SELECT * FROM limit_orders WHERE wallet = ? AND is_active = 1", (wallet,)
        )


class SuspicionScorer:
    def score(self, tx_data: Dict, signer: str) -> Tuple[float, List[str]]:
        signals: List[str] = []
        total = 0.0
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
        n_accts = len(accounts)
        if n_accts >= 20:
            total += 0.25; signals.append(f"accounts:{n_accts}(high)")
        elif n_accts >= 10:
            total += 0.18; signals.append(f"accounts:{n_accts}(med)")
        elif n_accts >= 6:
            total += 0.10; signals.append(f"accounts:{n_accts}(low)")

        inner_programs: set = set()
        inner_ix_count = 0
        for g in (meta.get("innerInstructions") or []):
            for ix in g.get("instructions", []):
                inner_ix_count += 1
                pid = ix.get("programId")
                if pid:
                    inner_programs.add(pid)
        if inner_programs & LIMIT_ORDER_PROGRAMS:
            total += 0.20; signals.append("inner:known_limit_program")
        if inner_ix_count >= 10:
            total += 0.15; signals.append(f"inner_ix_count:{inner_ix_count}")

        pre_tok  = meta.get("preTokenBalances",  []) or []
        post_tok = meta.get("postTokenBalances", []) or []
        pre_mints  = {b.get("mint") for b in pre_tok  if b.get("mint")}
        post_mints = {b.get("mint") for b in post_tok if b.get("mint")}
        new_accounts_created = post_mints - pre_mints
        if new_accounts_created:
            total += 0.10; signals.append(f"new_ata:{len(new_accounts_created)}")

        all_program_ids = {ix.get("programId") for ix in ixs if ix.get("programId")} | inner_programs
        unknown_programs = all_program_ids - ALL_KNOWN_PROGRAMS - SYSTEM_PROGRAMS
        if unknown_programs:
            total += 0.15; signals.append(f"unknown_programs:{len(unknown_programs)}")

        return min(total, 1.0), signals

    @staticmethod
    def _signer_index(tx_data: Dict, signer: str) -> Optional[int]:
        keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        for i, k in enumerate(keys):
            pub = k.get("pubkey") if isinstance(k, dict) else k
            if pub == signer:
                return i
        return None

    @staticmethod
    def _token_delta(tx_data: Dict, signer: str) -> int:
        meta = tx_data.get("meta", {})
        pre  = [b for b in (meta.get("preTokenBalances")  or [])
                if b.get("owner") == signer and b.get("mint") == MINT]
        post = [b for b in (meta.get("postTokenBalances") or [])
                if b.get("owner") == signer and b.get("mint") == MINT]
        pre_amt  = sum(int((b.get("uiTokenAmount") or {}).get("amount") or "0") for b in pre)
        post_amt = sum(int((b.get("uiTokenAmount") or {}).get("amount") or "0") for b in post)
        return post_amt - pre_amt

class TokenFlowAnalyzer:
    """
    Advanced token flow analyzer that handles any smart contract interaction,
    including complex multi-hop swaps through any DEX or aggregator.
    """
    
    def __init__(self, target_mint: str):
        self.target_mint = target_mint
        self.token_decimals: Dict[str, int] = {}
        self.token_symbols: Dict[str, str] = KNOWN_TOKEN_LABELS.copy()
        
    def analyze_transaction(self, tx_data: Dict, user_wallet: str) -> Dict[str, Any]:
        """
        Comprehensive analysis of all token movements in a transaction.
        Returns detailed flow information regardless of which contracts are involved.
        """
        meta = tx_data.get("meta", {})
        message = tx_data.get("transaction", {}).get("message", {})
        
        # Collect all token movements
        movements = self._collect_all_movements(tx_data, user_wallet)
        
        # Identify swap patterns
        swap_info = self._identify_swap_patterns(movements, user_wallet)
        
        # Detect program involvement
        programs_involved = self._get_programs_involved(tx_data)
        
        # Check if this is a swap-related transaction
        is_swap_related = bool(programs_involved & ALL_SWAP_PROGRAMS) or swap_info["is_swap"]
        
        return {
            "movements": movements,
            "swap_info": swap_info,
            "programs_involved": programs_involved,
            "is_swap_related": is_swap_related,
            "target_token_change": movements["by_mint"].get(self.target_mint, 0),
            "has_target_token_movement": abs(movements["by_mint"].get(self.target_mint, 0)) > 0.000001,
            "transaction_type": self._determine_transaction_type(movements, swap_info, programs_involved),
        }
    
    def _collect_all_movements(self, tx_data: Dict, user_wallet: str) -> Dict[str, Any]:
        """
        Collect ALL token movements from every possible source:
        - Direct balance changes
        - Inner instruction transfers
        - Program logs
        - Account creations/closures
        - CPI calls
        """
        meta = tx_data.get("meta", {})
        
        # 1. Get balance-based changes
        balance_movements = self._get_balance_changes(tx_data, user_wallet)
        
        # 2. Get transfer-based movements from instructions
        transfer_movements = self._get_transfer_movements(tx_data, user_wallet)
        
        # 3. Parse program logs for additional info
        log_movements = self._parse_log_movements(tx_data, user_wallet)
        
        # 4. Check for account creations that might indicate new token accounts
        
        # Merge all movements
        merged = defaultdict(float)
        all_sources = [balance_movements, transfer_movements, log_movements]

        
        for source in all_sources:
            for mint, delta in source.items():
                if abs(delta) > 0.000001:  # Ignore tiny dust movements
                    merged[mint] += delta
        
        # Convert to dict and add metadata
        result = {
            "by_mint": dict(merged),
            "total_in": 0.0,
            "total_out": 0.0,
            "net": 0.0,
            "source_breakdown": {
                "balance_changes": balance_movements,
                "transfers": transfer_movements,
                "logs": log_movements,
            }
        }
        
        # Calculate totals
        for mint, delta in merged.items():
            if delta > 0:
                result["total_in"] += delta
            else:
                result["total_out"] += abs(delta)
        
        result["net"] = result["total_in"] - result["total_out"]
        return result
    
    def _get_balance_changes(self, tx_data: Dict, user_wallet: str) -> Dict[str, float]:
        """Get signer-owned token movements from pre/post token balances."""
        meta = tx_data.get("meta", {})
        pre_all = meta.get("preTokenBalances", []) or []
        post_all = meta.get("postTokenBalances", []) or []

        pre_by_account = {}
        post_by_account = {}

        for bal in pre_all:
            if bal.get("owner") != user_wallet:
                continue
            idx = bal.get("accountIndex")
            mint = bal.get("mint")
            if idx is None or not mint:
                continue
            amount = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
            decimals = int((bal.get("uiTokenAmount") or {}).get("decimals") or 6)
            self.token_decimals[mint] = decimals
            pre_by_account[idx] = {"mint": mint, "amount": amount}

        for bal in post_all:
            if bal.get("owner") != user_wallet:
                continue
            idx = bal.get("accountIndex")
            mint = bal.get("mint")
            if idx is None or not mint:
                continue
            amount = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
            decimals = int((bal.get("uiTokenAmount") or {}).get("decimals") or 6)
            self.token_decimals[mint] = decimals
            post_by_account[idx] = {"mint": mint, "amount": amount}

        changes = defaultdict(float)
        all_accounts = set(pre_by_account.keys()) | set(post_by_account.keys())

        for idx in all_accounts:
            pre = pre_by_account.get(idx)
            post = post_by_account.get(idx)

            if pre and post and pre["mint"] == post["mint"]:
                delta = post["amount"] - pre["amount"]
                if delta != 0:
                    decimals = self.token_decimals.get(pre["mint"], 6)
                    changes[pre["mint"]] += delta / (10 ** decimals)
            elif post:
                decimals = self.token_decimals.get(post["mint"], 6)
                changes[post["mint"]] += post["amount"] / (10 ** decimals)
            elif pre:
                decimals = self.token_decimals.get(pre["mint"], 6)
                changes[pre["mint"]] -= pre["amount"] / (10 ** decimals)

        return dict(changes)
        
    def _get_transfer_movements(self, tx_data: Dict, user_wallet: str) -> Dict[str, float]:
        """Extract token movements from all instruction transfers (inner and outer)."""
        meta = tx_data.get("meta", {})
        message = tx_data.get("transaction", {}).get("message", {})
        movements = defaultdict(float)
        
        # Check all instructions (outer)
        for ix in message.get("instructions", []):
            self._process_instruction_for_transfers(ix, user_wallet, movements, tx_data)
        
        # Check inner instructions
        for inner_group in meta.get("innerInstructions", []) or []:
            for ix in inner_group.get("instructions", []):
                self._process_instruction_for_transfers(ix, user_wallet, movements, tx_data)
        
        return dict(movements)
    
    def _process_instruction_for_transfers(
        self, ix: Dict, user_wallet: str, movements: Dict[str, float], tx_data: Dict
    ):
        """Process a single instruction for token transfers using token-account owner resolution."""
        program_id = ix.get("programId")

        if program_id in TOKEN_PROGRAMS:
            transfer_info = self._decode_token_transfer(ix, tx_data)
            if not transfer_info:
                return

            mint = transfer_info.get("mint")
            amount = transfer_info.get("amount", 0)
            source = transfer_info.get("source")
            dest = transfer_info.get("destination")

            if not mint or amount <= 0:
                return

            # Resolve token account -> owner
            meta = tx_data.get("meta", {})
            keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
            key_list = [k.get("pubkey") if isinstance(k, dict) else k for k in keys]

            account_owner: Dict[str, str] = {}
            account_mint: Dict[str, str] = {}

            for bal in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
                idx = bal.get("accountIndex")
                if idx is None or idx >= len(key_list):
                    continue
                token_acc = key_list[idx]
                owner = bal.get("owner")
                bal_mint = bal.get("mint")
                if owner:
                    account_owner[token_acc] = owner
                if bal_mint:
                    account_mint[token_acc] = bal_mint
                    decimals = int((bal.get("uiTokenAmount") or {}).get("decimals") or 6)
                    self.token_decimals[bal_mint] = decimals

            source_owner = account_owner.get(source, source)
            dest_owner = account_owner.get(dest, dest)

            # Fallback mint resolution
            mint = mint or account_mint.get(source) or account_mint.get(dest)
            if not mint:
                return

            decimals = self.token_decimals.get(mint, 6)
            decimal_amount = amount / (10 ** decimals)

            if source_owner == user_wallet:
                movements[mint] -= decimal_amount
            if dest_owner == user_wallet:
                movements[mint] += decimal_amount

        elif program_id in SWAP_PROGRAMS:
            self._extract_swap_amounts_from_logs(ix, user_wallet, movements, tx_data)
    
    def _decode_token_transfer(self, ix: Dict, tx_data: Dict) -> Optional[Dict]:
        data = ix.get("data", "")
        accounts = ix.get("accounts", [])
        if not data or not accounts:
            return None

        raw = None
        try:
            _B58 = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
            _MAP = {c: i for i, c in enumerate(_B58)}
            n = 0
            for ch in data.encode():
                if ch not in _MAP:
                    raise ValueError()
                n = n * 58 + _MAP[ch]
            pad = len(data) - len(data.lstrip("1"))
            raw = b"\x00" * pad + n.to_bytes((n.bit_length() + 7) // 8 or 1, "big")
        except Exception:
            try:
                raw = base64.b64decode(data + "==")
            except Exception:
                return None

        if not raw or len(raw) < 9:
            return None

        discriminator = raw[0]

        if discriminator == 3 and len(accounts) >= 2:
            return {
                "type": "transfer",
                "amount": int.from_bytes(raw[1:9], "little"),
                "source": accounts[0],
                "destination": accounts[1],
                "mint": self._get_mint_for_account(accounts[0], tx_data),
            }

        if discriminator == 12 and len(accounts) >= 4:  # TransferChecked, NOT 4
            return {
                "type": "transfer_checked",
                "amount": int.from_bytes(raw[1:9], "little"),
                "source": accounts[0],
                "destination": accounts[2],  # layout: [src, mint, dst, authority]
                "mint": accounts[1],
            }

        return None

    def _get_mint_for_account(self, account: str, tx_data: Dict) -> Optional[str]:
        meta = tx_data.get("meta", {})
        keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        key_list = [k.get("pubkey") if isinstance(k, dict) else k for k in keys]
        try:
            idx = key_list.index(account)
        except ValueError:
            return None
        for bal in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
            if bal.get("accountIndex") == idx:
                return bal.get("mint")
        return None
    
    def _parse_log_movements(self, tx_data: Dict, user_wallet: str) -> Dict[str, float]:
        """Parse program logs for token movement information."""
        meta = tx_data.get("meta", {})
        logs = meta.get("logMessages", []) or []
        movements = defaultdict(float)
        
        for log in logs:
            # Many DEXes log swap amounts
            if "swap" in log.lower() or "exchange" in log.lower():
                # Try to extract amounts
                import re
                
                # Look for patterns like "amount_in: 1000" or "amount_out: 500"
                in_match = re.search(r'(?:amount_in|in_amount|input).*?(\d+)', log, re.IGNORECASE)
                out_match = re.search(r'(?:amount_out|out_amount|output).*?(\d+)', log, re.IGNORECASE)
                
                # Look for token mints in logs
                mint_match = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}', log)
                
                if mint_match and (in_match or out_match):
                    mint = mint_match.group()
                    if in_match:
                        amount = float(in_match.group(1)) / (10 ** self.token_decimals.get(mint, 6))
                        # Can't determine direction without more context
                    if out_match:
                        amount = float(out_match.group(1)) / (10 ** self.token_decimals.get(mint, 6))
        
        return dict(movements)
    
    def _detect_account_creations(self, tx_data: Dict, user_wallet: str) -> Dict[str, float]:
        """Detect new token accounts created for the user."""
        meta = tx_data.get("meta", {})
        pre_accounts = {b.get("accountIndex") for b in (meta.get("preTokenBalances") or []) 
                       if b.get("owner") == user_wallet}
        post_accounts = {b.get("accountIndex") for b in (meta.get("postTokenBalances") or [])
                        if b.get("owner") == user_wallet}
        
        new_accounts = post_accounts - pre_accounts
        movements = defaultdict(float)
        
        for acc_idx in new_accounts:
            for bal in meta.get("postTokenBalances") or []:
                if bal.get("accountIndex") == acc_idx and bal.get("owner") == user_wallet:
                    mint = bal.get("mint")
                    amount = int((bal.get("uiTokenAmount") or {}).get("amount") or "0")
                    decimals = self.token_decimals.get(mint, 6)
                    movements[mint] += amount / (10 ** decimals)
        
        return dict(movements)
    
    def _get_programs_involved(self, tx_data: Dict) -> Set[str]:
        """Get all programs involved in the transaction (outer + inner)."""
        message = tx_data.get("transaction", {}).get("message", {})
        meta = tx_data.get("meta", {})
        programs = set()
        
        # Outer instructions
        for ix in message.get("instructions", []):
            pid = ix.get("programId")
            if pid:
                programs.add(pid)
        
        # Inner instructions
        for inner_group in meta.get("innerInstructions", []) or []:
            for ix in inner_group.get("instructions", []):
                pid = ix.get("programId")
                if pid:
                    programs.add(pid)
        
        return programs
    
    def _identify_swap_patterns(self, movements: Dict, user_wallet: str) -> Dict[str, Any]:
        """
        Identify if the movements match a swap pattern:
        - One token decreases, another increases
        - Net token change close to zero (swapping one for another)
        - Multiple tokens might be involved in multi-hop swaps
        """
        by_mint = movements["by_mint"]
        
        # A swap typically has at least one positive and one negative movement
        positive_mints = [m for m, delta in by_mint.items() if delta > 0.000001]
        negative_mints = [m for m, delta in by_mint.items() if delta < -0.000001]
        
        is_swap = len(positive_mints) >= 1 and len(negative_mints) >= 1
        
        # Check net change (should be close to zero for pure swaps)
        net_change = movements["net"]
        is_pure_swap = is_swap and abs(net_change) < 0.000001
        
        # Identify the main token being swapped
        main_in_token = None
        main_out_token = None
        main_in_amount = 0
        main_out_amount = 0
        
        if positive_mints:
            main_in_token = max(positive_mints, key=lambda m: by_mint[m])
            main_in_amount = by_mint[main_in_token]
        
        if negative_mints:
            main_out_token = min(negative_mints, key=lambda m: by_mint[m])
            main_out_amount = abs(by_mint[main_out_token])
        
        return {
            "is_swap": is_swap,
            "is_pure_swap": is_pure_swap,
            "positive_mints": positive_mints,
            "negative_mints": negative_mints,
            "main_in_token": main_in_token,
            "main_out_token": main_out_token,
            "main_in_amount": main_in_amount,
            "main_out_amount": main_out_amount,
            "estimated_price": main_out_amount / main_in_amount if main_in_amount > 0 else 0,
        }
    
    def _determine_transaction_type(self, movements: Dict, swap_info: Dict, 
                                     programs_involved: Set[str]) -> str:
        """Determine the overall transaction type."""
        target_change = movements["by_mint"].get(self.target_mint, 0)
        
        # Check for limit orders first
        if programs_involved & LIMIT_ORDER_PROGRAMS:
            if abs(target_change) < 0.000001:
                return "LIMIT_PLACEMENT"
            else:
                return "LIMIT_FILL"
        
        # Check for swaps
        if swap_info["is_swap"]:
            if target_change > 0:
                return "MARKET_BUY"
            elif target_change < 0:
                return "MARKET_SELL"
        
        # Check for transfers
        if len(movements["by_mint"]) == 1 and self.target_mint in movements["by_mint"]:
            return "TRANSFER"
        
        # Check for cancellations
        if "cancel" in str(programs_involved).lower():
            return "CANCEL_LIMIT"
        
        return "UNKNOWN"
    
    def _extract_swap_amounts_from_logs(self, ix: Dict, user_wallet: str, 
                                        movements: Dict[str, float], tx_data: Dict):
        """Extract swap amounts from program logs."""
        # This is a placeholder - actual implementation would parse specific DEX logs
        pass

def _build_classify_prompt(tx_data: Dict, signer: str, suspicion_signals: List[str], sol_price_usd: float = 150.0) -> str:
    meta    = tx_data.get("meta", {})
    all_ixs = get_all_instructions(tx_data)
    prog_ids = sorted({ix.get("programId") for ix in all_ixs if ix.get("programId")})

    deltas       = get_signer_token_deltas(tx_data, signer)
    target_delta = deltas.get(MINT, 0.0)

    negative_quotes = []
    positive_quotes = []
    for mint, delta in deltas.items():
        if mint == MINT or abs(delta) < 1e-12:
            continue
        if delta < 0:
            negative_quotes.append((mint, abs(delta)))
        elif delta > 0:
            positive_quotes.append((mint, abs(delta)))

    def pick_quote(cands):
        if not cands:
            return None
        preferred = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
            WSOL_MINT,
        ]
        for pref in preferred:
            for mint, _ in cands:
                if mint == pref:
                    return mint
        return max(cands, key=lambda x: x[1])[0]

    quote_out_mint = pick_quote(negative_quotes)
    quote_in_mint  = pick_quote(positive_quotes)

    signer_idx = None
    keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
    for i, k in enumerate(keys):
        pub = k.get("pubkey") if isinstance(k, dict) else k
        if pub == signer:
            signer_idx = i
            break

    signer_sol_delta   = 0.0
    signer_sol_spent   = 0.0
    pre_bal  = meta.get("preBalances", [])
    post_bal = meta.get("postBalances", [])
    if signer_idx is not None and signer_idx < len(pre_bal) and signer_idx < len(post_bal):
        raw_delta          = post_bal[signer_idx] - pre_bal[signer_idx]
        signer_sol_delta   = raw_delta / 1e9
        fee                = meta.get("fee", 5000)
        # SOL locked into escrow = amount spent BEYOND just the fee
        signer_sol_spent   = max(0.0, (pre_bal[signer_idx] - post_bal[signer_idx] - fee)) / 1e9

    # Detect fee-only: no token change, only fee deducted from signer
    fee_lamports  = meta.get("fee", 5000)
    only_fee_paid = (
        abs(target_delta) < 1e-12
        and not negative_quotes
        and not positive_quotes
        and signer_sol_spent < 0.005  # less than 0.001 SOL beyond fee = fee-only
    )

    has_cancel    = False
    has_new_order = False
    for ix in all_ixs:
        raw = TransactionClassifier._decode_ix_data(ix.get("data", ""))
        if raw and len(raw) >= 8:
            disc = DISCRIMINATORS.get(raw[:8])
            if disc == "cancel_order":
                has_cancel = True
            elif disc == "new_order":
                has_new_order = True

    logs_lc = " ".join(meta.get("logMessages") or []).lower()
    if "cancel" in logs_lc or "withdraw order" in logs_lc or "close order" in logs_lc:
        has_cancel = True
    if any(k in logs_lc for k in ["new order", "place order", "post only", "post-only", "limit"]):
        has_new_order = True

    # Build NAMED program hits (not just IDs)
    known_market_hits_named = {}
    known_limit_hits_named  = {}
    for pid in prog_ids:
        if pid in (ALL_SWAP_PROGRAMS | DEX_PROGRAMS):
            known_market_hits_named[pid] = exchange_name(pid)
        if pid in LIMIT_ORDER_PROGRAMS:
            known_limit_hits_named[pid]  = exchange_name(pid)

    swap_like  = bool(known_market_hits_named)
    limit_like = bool(known_limit_hits_named)

    # Derive USD value of quote locked in escrow
    quote_usd_locked = 0.0
    if quote_out_mint in (
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    ):
        quote_usd_locked = abs(deltas.get(quote_out_mint, 0.0))
    elif quote_out_mint == WSOL_MINT:
        quote_usd_locked = abs(deltas.get(WSOL_MINT, 0.0)) * sol_price_usd # rough SOL price fallback
    elif signer_sol_spent > 0.005:
        quote_usd_locked = signer_sol_spent * sol_price_usd  # native SOL went to escrow

    facts = {
        "signer":             signer,
        "target_mint":        MINT,
        "target_token_delta": round(target_delta, 6),

        # SOL movement
        "signer_sol_delta":       round(signer_sol_delta, 6),
        "signer_sol_spent_beyond_fee": round(signer_sol_spent, 6),
        "tx_fee_sol":             round(fee_lamports / 1e9, 9),
        "only_fee_paid":          only_fee_paid,

        # Token deltas for signer
        "signer_token_deltas": {
            KNOWN_TOKEN_LABELS.get(m, m[:8] + "…"): round(d, 6)
            for m, d in deltas.items()
        },
        "quote_out_token":    KNOWN_TOKEN_LABELS.get(quote_out_mint, quote_out_mint[:8] + "…" if quote_out_mint else None),
        "quote_in_token":     KNOWN_TOKEN_LABELS.get(quote_in_mint,  quote_in_mint[:8]  + "…" if quote_in_mint  else None),
        "quote_usd_locked_in_escrow": round(quote_usd_locked, 4),

        # Programs (with names so you can identify them)
        "limit_order_programs_detected": known_limit_hits_named,
        "swap_programs_detected":        known_market_hits_named,
        "all_program_ids":               prog_ids,
        "swap_like":   swap_like,
        "limit_like":  limit_like,

        # Decoded instruction signals
        "has_cancel":    has_cancel,
        "has_new_order": has_new_order,

        "suspicion_signals":    suspicion_signals,
        "log_messages_sample":  (meta.get("logMessages") or [])[:20],
    }

    return f"""
You are a strict Solana transaction classifier for a token monitoring bot.

IMPORTANT CONTEXT — HOW LIMIT ORDERS WORK ON SOLANA:
- When a user places a LIMIT BUY order, they lock their QUOTE token (SOL, USDC, etc.)
  into an escrow/vault owned by the limit order program. The TARGET token does NOT move yet.
  So: target_token_delta == 0, signer_sol_delta < 0 (or stablecoin delta < 0).
- When a user places a LIMIT SELL order, they lock their TARGET tokens into escrow.
  So: target_token_delta < 0, no immediate quote received.
- A FEE-ONLY transaction means only the tiny tx fee (~0.000005 SOL) was deducted,
  no real order was placed. only_fee_paid=true means this is just a fee transaction — return UNKNOWN.

Classify into exactly one:
MARKET_BUY, MARKET_SELL, LIMIT_BUY, LIMIT_SELL, CANCEL_LIMIT, TRANSFER, UNKNOWN

Decision rules:
- UNKNOWN:   only_fee_paid == true  →  ALWAYS return UNKNOWN, never anything else.
- MARKET_BUY:
    target_token_delta > 0 AND signer paid a quote token now AND swap_like == true
- MARKET_SELL:
    target_token_delta < 0 AND signer received a quote token AND swap_like == true
- LIMIT_BUY:
    limit_like == true (limit_order_programs_detected is non-empty)
    AND target_token_delta == 0
    AND (signer_sol_spent_beyond_fee > 0.005 OR quote_usd_locked_in_escrow > 1.0)
    AND has_cancel == false
- LIMIT_SELL:
    limit_like == true
    AND target_token_delta <= 0
    AND no immediate quote token received (quote_in_token is null)
    AND has_cancel == false
- CANCEL_LIMIT:
    has_cancel == true
- TRANSFER:
    target_token_delta != 0 AND swap_like == false AND limit_like == false
- UNKNOWN:
    anything ambiguous, contradictory, or where only_fee_paid == true

Hard constraints:
- If only_fee_paid == true → ALWAYS UNKNOWN, no exceptions.
- If target_token_delta == 0 → NEVER output MARKET_BUY or MARKET_SELL.
- If limit_like == false and has_new_order == false → NEVER output LIMIT_BUY or LIMIT_SELL.
- If has_cancel == true → prefer CANCEL_LIMIT.
- The "quote_usd_locked_in_escrow" field is your best estimate of the order SIZE in USD.
- Do not invent fills from logs alone.

Facts:
{json.dumps(facts, indent=2)}

Return ONLY valid JSON — no markdown, no extra text:
{{
  "order_type": "MARKET_BUY|MARKET_SELL|LIMIT_BUY|LIMIT_SELL|CANCEL_LIMIT|TRANSFER|UNKNOWN",
  "confidence": 0.0,
  "order_size_usd": 0.0,
  "order_size_tokens": 0.0,
  "quote_token": "SOL|USDC|USDT|...",
  "exchange": "exchange name from limit_order_programs_detected or swap_programs_detected",
  "reason": "one short sentence"
}}
""".strip()


LEARNED_PROGRAMS_FILE = os.getenv("LEARNED_PROGRAMS_FILE", "learned_programs.json")

def _load_learned_programs() -> Dict[str, Dict]:
    try:
        with open(LEARNED_PROGRAMS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_learned_programs(cache: Dict[str, Dict]) -> None:
    try:
        with open(LEARNED_PROGRAMS_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"   ⚠️ Could not save learned programs: {e}")


class TransactionClassifier:
    def __init__(self) -> None:
        self._scorer  = SuspicionScorer()
        self._learned: Dict[str, Dict] = _load_learned_programs()
        if self._learned:
            print(f"📚 Loaded {len(self._learned)} learned program(s) from cache")

    def _known_role(self, pid: str) -> Optional[str]:
        if pid in EXCHANGE_REGISTRY:
            return EXCHANGE_REGISTRY[pid]["role"]
        if pid in self._learned:
            return self._learned[pid]["role"]
        return None

    def _learn(self, program_ids: set, order_type: OrderType,
            exchange: str, confidence: float) -> None:
        if confidence < 0.80:
            return

        if order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL, OrderType.CANCEL_LIMIT):
            role = "limit"
        elif order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL):
            role = "market"
        else:
            # Never learn UNKNOWN or TRANSFER as an exchange role
            return

        candidates = [
            pid for pid in program_ids
            if pid not in EXCHANGE_REGISTRY
            and pid not in SYSTEM_PROGRAMS
            and pid not in self._learned
        ]

        if len(candidates) > 1:
            print(f"   📚 Skipping learn: {len(candidates)} unknown programs, ambiguous which is {exchange}")
            return

        changed = False
        for pid in candidates:
            self._learned[pid] = {
                "name": exchange if exchange != "Unknown" else f"Learned ({pid[:8]}…)",
                "role": role,
                "confidence": confidence,
                "seen": 1,
            }
            print(f"   📚 Learned new program: {pid[:16]}… → {role} ({exchange})")
            changed = True

        for pid in program_ids:
            if pid in self._learned and pid not in candidates:
                self._learned[pid]["seen"] = self._learned[pid].get("seen", 1) + 1

        if changed:
            _save_learned_programs(self._learned)

    async def _handle_unknown_program(
        self, tx_data: Dict, signer: str, signature: str,
        suspicion: float, signals: List[str]
    ) -> None:
        analyzer = TokenFlowAnalyzer(MINT)
        analysis = analyzer.analyze_transaction(tx_data, signer)
        if analysis["has_target_token_movement"]:
           print(f"   ✅ Unknown program transaction shows token movement - will process")
        message = tx_data.get("transaction", {}).get("message", {})
        ixs     = message.get("instructions", [])
        meta    = tx_data.get("meta", {})
        all_pids = {ix.get("programId") for ix in ixs if ix.get("programId")}
        for g in (meta.get("innerInstructions") or []):
            for ix in g.get("instructions", []):
                pid = ix.get("programId")
                if pid:
                    all_pids.add(pid)
        known   = ALL_KNOWN_PROGRAMS | set(self._learned.keys()) | SYSTEM_PROGRAMS
        unknown = all_pids - known
        if not unknown:
            return
        for pid in unknown:
            print(f"   ❓ Unknown program: {pid}  suspicion={suspicion:.2f}  signals={signals}")
        entry = {
            "timestamp":    get_timestamp(),
            "signature":    signature,
            "signer":       signer,
            "unknown_pids": list(unknown),
            "suspicion":    suspicion,
            "signals":      signals,
        }
        try:
            with open("unknown_programs.jsonl", "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            print(f"   ⚠️ Could not log unknown program: {e}")
        if DEBUG_CHANNEL_ID:
            await send_message(DEBUG_CHANNEL_ID, embeds=[{
                "author": {"name": "❓ Unknown Program Detected"},
                "title":  "New unrecognized program in XERIS tx",
                "description": (
                    f"```yaml\n"
                    f"Signature:  {signature[:32]}...\n"
                    f"Signer:     {signer[:16]}...\n"
                    f"Suspicion:  {suspicion:.2f}\n"
                    f"Signals:    {', '.join(signals)}\n"
                    f"Programs:\n"
                    + "\n".join(f"  - {pid}" for pid in unknown)
                    + "\n```\n"
                    f"> Add to EXCHANGE_REGISTRY to classify future txs.\n"
                    f"[Solscan TX](https://solscan.io/tx/{signature})"
                ),
                "color":     0xF59E0B,
                "footer":    {"text": f"XerisBot Debug · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
                "timestamp": get_timestamp(),
            }])

    async def classify(self, tx_data: Dict, signer: str, ms: MarketState) -> Tuple[OrderType, Optional[Dict]]:
        meta = tx_data.get("meta", {})

        # Short-circuit: fee-only transaction — nothing meaningful happened
        pre_tok  = meta.get("preTokenBalances",  []) or []
        post_tok = meta.get("postTokenBalances", []) or []
        pre_bal  = meta.get("preBalances",  [])
        post_bal = meta.get("postBalances", [])
        fee      = meta.get("fee", 5000)

        keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        signer_idx = next(
            (i for i, k in enumerate(keys)
            if (k.get("pubkey") if isinstance(k, dict) else k) == signer), None
        )
        sol_beyond_fee = 0
        if signer_idx is not None and signer_idx < len(pre_bal) and signer_idx < len(post_bal):
            sol_beyond_fee = max(0, (pre_bal[signer_idx] - post_bal[signer_idx]) - fee)

        tok_changed = any(
            int((b.get("uiTokenAmount") or {}).get("amount") or 0)
            != int((next((p for p in post_tok if p.get("accountIndex") == b.get("accountIndex")), {})
                    .get("uiTokenAmount") or {}).get("amount") or 0)
            for b in pre_tok
        )

        programs = get_all_program_ids(tx_data)
        has_meaningful_program = bool(
            programs & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS | LIMIT_ORDER_PROGRAMS)
        )

        if not tok_changed and sol_beyond_fee < 10_000 and not has_meaningful_program:
            return OrderType.UNKNOWN, None
        order_type, info = self._rule_based(tx_data, signer, ms)
        if order_type != OrderType.UNKNOWN:
            return order_type, info

        suspicion, signals = self._scorer.score(tx_data, signer)
        print(f"   🔍 Suspicion {suspicion:.2f}  [{', '.join(signals) or 'none'}]")

        if suspicion < SUSPICION_THRESHOLD:
            return OrderType.UNKNOWN, None

        await self._handle_unknown_program(
            tx_data, signer, tx_data.get("signature", ""), suspicion, signals
        )

        deltas = get_signer_token_deltas(tx_data, signer)
        target_delta = deltas.get(MINT, 0.0)
        program_ids = get_all_program_ids(tx_data)
        logs_lc = " ".join(tx_data.get("meta", {}).get("logMessages") or []).lower()

        looks_limitish = bool(program_ids & LIMIT_ORDER_PROGRAMS) or any(
            k in logs_lc for k in ["limit", "order", "place order", "post only", "post-only", "cancel"]
        )
        looks_marketish = bool(program_ids & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS))

        # If zero target movement and no limit/cancel evidence, do not ask Groq
        if abs(target_delta) < 1e-12 and not looks_limitish:
            return OrderType.UNKNOWN, None

        # If target moved but there is no market/limit evidence, Groq may still help
        # so we allow it to continue
# In classify(), replace the GROQ_ENABLED block and the check after it:
        if GROQ_ENABLED:
            order_type, info, conf = await self._groq_classify(tx_data, signer, ms, signals)

            MIN_CONF_BY_TYPE = {
                OrderType.MARKET_BUY:   0.75,
                OrderType.MARKET_SELL:  0.75,
                OrderType.LIMIT_BUY:    0.80,
                OrderType.LIMIT_SELL:   0.80,
                OrderType.CANCEL_LIMIT: 0.80,
                OrderType.TRANSFER:     0.85,
            }
            min_conf = MIN_CONF_BY_TYPE.get(order_type, 0.99)

            if order_type != OrderType.UNKNOWN and conf >= min_conf:
                program_ids = get_all_program_ids(tx_data)
                logs_lc = " ".join(tx_data.get("meta", {}).get("logMessages") or []).lower()

                should_learn = False
                if order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL):
                    should_learn = bool(program_ids & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS))
                elif order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL, OrderType.CANCEL_LIMIT):
                    should_learn = bool(program_ids & LIMIT_ORDER_PROGRAMS) or any(
                        k in logs_lc for k in ["limit", "order", "cancel", "place order", "post only"]
                    )

                if should_learn:
                    self._learn(program_ids, order_type, info.get("exchange", "Unknown"), conf)

                all_known = ALL_KNOWN_PROGRAMS | set(self._learned.keys())
                has_unknown_pids = bool(program_ids - all_known - SYSTEM_PROGRAMS)
                if has_unknown_pids and order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL):
                    print(f"   ⚠️ Groq said {order_type.value} but program unconfirmed — skipping storage")
                    return OrderType.UNKNOWN, None

                return order_type, info

        return OrderType.UNKNOWN, None

    def _rule_based(
            self, tx_data: Dict, signer: str, ms: MarketState
        ) -> Tuple[OrderType, Optional[Dict]]:
            meta = tx_data.get("meta", {})
            if meta.get("err"):
                return OrderType.UNKNOWN, None

            programs     = get_all_program_ids(tx_data)
            all_ixs      = get_all_instructions(tx_data)
            deltas       = get_signer_token_deltas(tx_data, signer)
            target_delta = deltas.get(MINT, 0.0)

            learned_limit_hits  = {p for p in programs if self._known_role(p) in ("limit", "hybrid")}
            learned_market_hits = {p for p in programs if self._known_role(p) in ("market", "hybrid")}

            limit_hits  = (programs & LIMIT_ORDER_PROGRAMS) | learned_limit_hits
            market_hits = (programs & ALL_SWAP_PROGRAMS) | (programs & DEX_PROGRAMS) | learned_market_hits

            # ── cancel detection ────────────────────────────────────────────────
            has_cancel = False
            for ix in all_ixs:
                raw = self._decode_ix_data(ix.get("data", ""))
                if raw and len(raw) >= 8 and DISCRIMINATORS.get(raw[:8]) == "cancel_order":
                    has_cancel = True
                    break
            logs_lc = " ".join(meta.get("logMessages") or []).lower()
            if "cancel" in logs_lc or "withdraw order" in logs_lc or "close order" in logs_lc:
                has_cancel = True

            # ── new-order detection ─────────────────────────────────────────────
            has_new_order = False
            for ix in all_ixs:
                raw = self._decode_ix_data(ix.get("data", ""))
                if raw and len(raw) >= 8 and DISCRIMINATORS.get(raw[:8]) == "new_order":
                    has_new_order = True
                    break
            if any(k in logs_lc for k in ["new order", "place order", "post only", "post-only", "limit"]):
                has_new_order = True

            # ── quote token helpers ─────────────────────────────────────────────
            negative_quotes = []
            positive_quotes = []
            for mint, delta in deltas.items():
                if mint == MINT or abs(delta) < 1e-12:
                    continue
                if delta < 0:
                    negative_quotes.append((mint, abs(delta)))
                elif delta > 0:
                    positive_quotes.append((mint, abs(delta)))

            def pick_quote(cands: List[Tuple[str, float]]) -> Optional[str]:
                if not cands:
                    return None
                preferred = [
                    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
                    WSOL_MINT,
                ]
                for pref in preferred:
                    for mint, _ in cands:
                        if mint == pref:
                            return mint
                return max(cands, key=lambda x: x[1])[0]

            def quote_usd_value(mint: Optional[str], abs_amount: float) -> float:
                if not mint or abs_amount <= 0:
                    return 0.0
                if mint in (
                    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                ):
                    return abs_amount
                if mint == WSOL_MINT:
                    return abs_amount * ms.sol_price_usd
                return 0.0

            # ── SOL balance helper (fee-aware) ──────────────────────────────────
            # Returns (sol_spent_to_escrow, sol_received_after_fee)
            # native SOL is invisible to get_signer_token_deltas(), so we must
            # read it directly from pre/post SOL balances.
            def signer_sol_change() -> Tuple[float, float]:
                keys_l = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
                si = next(
                    (i for i, k in enumerate(keys_l)
                    if (k.get("pubkey") if isinstance(k, dict) else k) == signer), None
                )
                if si is None:
                    return 0.0, 0.0
                pre_b  = meta.get("preBalances",  [])
                post_b = meta.get("postBalances", [])
                fee    = meta.get("fee", 5000)
                if si >= len(pre_b) or si >= len(post_b):
                    return 0.0, 0.0
                diff    = pre_b[si] - post_b[si]                    # positive = signer lost SOL
                spent   = max(0.0, (diff - fee)) / 1e9              # SOL locked to escrow
                received = max(0.0, (-diff - fee)) / 1e9            # SOL received from swap
                return spent, received

            # ===================================================================
            # 1) CANCEL LIMIT — always first, unambiguous
            # ===================================================================
            if limit_hits and has_cancel:
                return OrderType.CANCEL_LIMIT, {
                    "wallet":      signer,
                    "signature":   tx_data.get("signature", ""),
                    "exchange":    ", ".join(
                        exchange_name(p) if p in EXCHANGE_REGISTRY
                        else self._learned.get(p, {}).get("name", f"Learned ({p[:8]}…)")
                        for p in sorted(limit_hits)
                    ),
                    "quote_token": "",
                }

            # ===================================================================
            # 2) MARKET BUY / SELL
            #    Checked BEFORE limit. Real token movement + swap program = market,
            #    regardless of whether a limit program is also present.
            # ===================================================================
            if abs(target_delta) > 1e-12 and market_hits:
                if target_delta > 0:
                    quote_mint   = pick_quote(negative_quotes)
                    quote_symbol = KNOWN_TOKEN_LABELS.get(
                        quote_mint, f"{quote_mint[:8]}…" if quote_mint else "SOL"
                    )
                    usd_value = quote_usd_value(quote_mint, abs(deltas.get(quote_mint, 0.0)))
                    if usd_value < 5.0:
                        usd_value = 0.0
                    if usd_value <= 0:
                        usd_value = target_delta * ms.current_price
                    ex_names = [
                        exchange_name(p) if p in EXCHANGE_REGISTRY
                        else self._learned.get(p, {}).get("name", f"Learned ({p[:8]}…)")
                        for p in sorted(market_hits)
                    ]
                    return OrderType.MARKET_BUY, {
                        "wallet":      signer,
                        "amount":      target_delta,
                        "usd_value":   usd_value,
                        "exchange":    ", ".join(ex_names),
                        "quote_token": quote_symbol,
                    }

                if target_delta < 0:
                    quote_mint   = pick_quote(positive_quotes)
                    quote_symbol = KNOWN_TOKEN_LABELS.get(
                        quote_mint, f"{quote_mint[:8]}…" if quote_mint else "SOL"
                    )
                    usd_value = quote_usd_value(quote_mint, abs(deltas.get(quote_mint, 0.0)))
                    if usd_value <= 0:
                        usd_value = abs(target_delta) * ms.current_price
                    ex_names = [
                        exchange_name(p) if p in EXCHANGE_REGISTRY
                        else self._learned.get(p, {}).get("name", f"Learned ({p[:8]}…)")
                        for p in sorted(market_hits)
                    ]
                    return OrderType.MARKET_SELL, {
                        "wallet":      signer,
                        "amount":      abs(target_delta),
                        "usd_value":   usd_value,
                        "exchange":    ", ".join(ex_names),
                        "quote_token": quote_symbol,
                    }

            # ===================================================================
            # 2b) HYBRID GUARD
            #     Both market + limit programs present, zero token movement.
            #     Only allow limit detection if there is real escrow evidence.
            #     Without it, return UNKNOWN — never guess limit from programs alone.
            # ===================================================================
            if limit_hits and market_hits and abs(target_delta) < 1e-12:
                sol_spent, _ = signer_sol_change()
                if not negative_quotes and sol_spent <= 0.01:
                    return OrderType.UNKNOWN, None

            # ===================================================================
            # 3) LIMIT BUY
            #    All conditions must be true:
            #      - known limit program present
            #      - target token did NOT move (still in escrow waiting for fill)
            #      - a new-order signal exists (prevents fills/rent calls)
            #      - real value left the signer (quote token OR SOL to escrow)
            #      - derived USD value >= $5
            # ===================================================================
            if limit_hits and abs(target_delta) < 1e-12:
                has_placement_signal = has_new_order or any(
                    k in logs_lc for k in ["place", "init", "create", "new order"]
                )
                if has_placement_signal:
                    quote_mint = pick_quote(negative_quotes)
                    usd_value  = quote_usd_value(quote_mint, abs(deltas.get(quote_mint, 0.0)))

                    if usd_value < 5.0:
                        usd_value = 0.0

                    # SOL-to-escrow fallback — threshold 0.01 SOL skips ATA rent
                    # (~0.002 SOL) and compute budget noise
                    if usd_value <= 0:
                        sol_spent, _ = signer_sol_change()
                        if sol_spent > 0.01:
                            usd_value = sol_spent * ms.sol_price_usd

                    if usd_value >= 5.0:
                        amount = usd_value / ms.current_price if ms.current_price > 0 else 0.0
                        tp     = ms.current_price
                        ex_names = [
                            exchange_name(p) if p in EXCHANGE_REGISTRY
                            else self._learned.get(p, {}).get("name", f"Learned ({p[:8]}…)")
                            for p in sorted(limit_hits)
                        ]
                        return OrderType.LIMIT_BUY, {
                            "wallet":         signer,
                            "amount":         amount,
                            "usd_value":      usd_value,
                            "target_price":   tp,
                            "predicted_mcap": self._predict_mcap(tp, ms),
                            "exchange":       ", ".join(ex_names),
                            "quote_token":    KNOWN_TOKEN_LABELS.get(
                                quote_mint, f"{quote_mint[:8]}…" if quote_mint else ""
                            ),
                        }

            # ===================================================================
            # 4) LIMIT SELL
            #    All conditions must be true:
            #      - known limit program present
            #      - target token decreased (locked into escrow)
            #      - NO market program overlap
            #      - signer did NOT receive SOL — SOL receipt means market sell.
            #        (positive_quotes is empty for SOL-quote sells because native
            #         SOL is invisible to get_signer_token_deltas, so we MUST
            #         check the SOL balance directly here)
            #      - a new-order signal exists
            #      - derived USD value >= $5
            # ===================================================================
            if limit_hits and target_delta < 0 and not market_hits:
                _, sol_received = signer_sol_change()

                # If signer received meaningful SOL, this is a market sell
                if sol_received > 0.001:
                    return OrderType.UNKNOWN, None

                # Require a new-order signal — prevents limit fills from
                # being stored as new limit sell placements
                has_placement_signal = has_new_order or any(
                    k in logs_lc for k in ["place", "init", "create", "new order"]
                )
                if not has_placement_signal:
                    return OrderType.UNKNOWN, None

                amount    = abs(target_delta)
                usd_value = amount * ms.current_price
                if usd_value < 5.0:
                    return OrderType.UNKNOWN, None

                tp = ms.current_price
                ex_names = [
                    exchange_name(p) if p in EXCHANGE_REGISTRY
                    else self._learned.get(p, {}).get("name", f"Learned ({p[:8]}…)")
                    for p in sorted(limit_hits)
                ]
                return OrderType.LIMIT_SELL, {
                    "wallet":         signer,
                    "amount":         amount,
                    "usd_value":      usd_value,
                    "target_price":   tp,
                    "predicted_mcap": self._predict_mcap(tp, ms),
                    "exchange":       ", ".join(ex_names),
                    "quote_token":    "",
                }

            # ===================================================================
            # 5) TRANSFER
            # ===================================================================
            non_target_changes = [
                mint for mint, delta in deltas.items()
                if mint != MINT and abs(delta) > 1e-12
            ]
            if abs(target_delta) > 1e-12 and not limit_hits and not market_hits and not non_target_changes:
                return OrderType.TRANSFER, {
                    "wallet":      signer,
                    "amount":      abs(target_delta),
                    "usd_value":   abs(target_delta) * ms.current_price,
                    "to":          "unknown",
                    "quote_token": "",
                }

            return OrderType.UNKNOWN, None

    async def _groq_classify(
        self, tx_data: Dict, signer: str, ms: MarketState, signals: List[str]
    ) -> Tuple[OrderType, Optional[Dict], float]:
        try:
            prompt = _build_classify_prompt(tx_data, signer, signals, sol_price_usd=ms.sol_price_usd)

            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    GROQ_URL,
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": GROQ_MODEL,
                        "max_tokens": 256,
                        "temperature": 0.0,
                        "messages": [
                            {
                                "role": "system",
                                "content": (
                                    "You are a strict Solana transaction classifier. "
                                    "You must follow the provided hard constraints exactly. "
                                    "When facts are insufficient or contradictory, return UNKNOWN. "
                                    "Do not infer a fill, swap, or transfer if target-token movement does not support it. "
                                    "Respond only with valid JSON."
                                ),
                            },
                            {"role": "user", "content": prompt},
                        ],
                    },
                )

            if resp.status_code != 200:
                print(f"   ⚡ Groq {resp.status_code}")
                return OrderType.UNKNOWN, None, 0.0

            raw = resp.json()["choices"][0]["message"]["content"].strip()
            parsed = json.loads(raw.replace("```json", "").replace("```", "").strip())

            ai_type = parsed.get("order_type", "UNKNOWN").upper()
            confidence = float(parsed.get("confidence", 0))
            exchange = parsed.get("exchange", "Unknown")
            reason = parsed.get("reason", "")
            groq_size_usd    = float(parsed.get("order_size_usd", 0) or 0)
            groq_size_tokens = float(parsed.get("order_size_tokens", 0) or 0)
            groq_quote_token = parsed.get("quote_token", "")

            print(f"   ⚡ Groq: {ai_type}  conf={confidence:.2f}  size=${groq_size_usd:.2f}  via {exchange}  — {reason}")

            try:
                order_type = OrderType(ai_type)
            except ValueError:
                return OrderType.UNKNOWN, None, 0.0

            token_result = self._parse_token_changes(tx_data, signer)
            deltas = get_signer_token_deltas(tx_data, signer)
            target_delta = deltas.get(MINT, 0.0)
            program_ids = get_all_program_ids(tx_data)
            all_ixs = get_all_instructions(tx_data)
            logs_lc = " ".join(tx_data.get("meta", {}).get("logMessages") or []).lower()

            # -----------------------------
            # Hard validation rules
            # -----------------------------
            if order_type == OrderType.MARKET_BUY:
                if target_delta <= 0 or not token_result or token_result[0] != "BUY":
                    print("   ⚠️ Rejecting Groq MARKET_BUY: hard facts disagree")
                    return OrderType.UNKNOWN, None, 0.0

            if order_type == OrderType.MARKET_SELL:
                if target_delta >= 0 or not token_result or token_result[0] != "SELL":
                    print("   ⚠️ Rejecting Groq MARKET_SELL: hard facts disagree")
                    return OrderType.UNKNOWN, None, 0.0

            if order_type == OrderType.TRANSFER:
                if abs(target_delta) < 1e-12 or not token_result or token_result[0] != "TRANSFER":
                    print("   ⚠️ Rejecting Groq TRANSFER: no real transfer evidence")
                    return OrderType.UNKNOWN, None, 0.0
                if program_ids & (ALL_SWAP_PROGRAMS | DEX_PROGRAMS | LIMIT_ORDER_PROGRAMS):
                    print("   ⚠️ Rejecting Groq TRANSFER: exchange program present")
                    return OrderType.UNKNOWN, None, 0.0

            if order_type == OrderType.CANCEL_LIMIT:
                has_cancel = (
                    "cancel" in logs_lc
                    or "withdraw order" in logs_lc
                    or "close order" in logs_lc
                )
                if not has_cancel:
                    for ix in all_ixs:
                        raw_ix = self._decode_ix_data(ix.get("data", ""))
                        if raw_ix and len(raw_ix) >= 8 and DISCRIMINATORS.get(raw_ix[:8]) == "cancel_order":
                            has_cancel = True
                            break
                if not has_cancel:
                    print("   ⚠️ Rejecting Groq CANCEL_LIMIT: no cancel evidence")
                    return OrderType.UNKNOWN, None, 0.0

            if order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL, OrderType.TRANSFER):
                if not token_result or token_result[1] <= 0:
                    print(f"   ⚠️ Rejecting Groq {order_type.value}: no real target-token movement")
                    return OrderType.UNKNOWN, None, 0.0

            # -----------------------------
            # Build info payload
            # -----------------------------
            amount = token_result[1] if token_result else 0.0
            quote_token = token_result[3] if token_result else ""

            info: dict = {
                "wallet": signer,
                "amount": amount,
                "usd_value": amount * ms.current_price,
                "exchange": exchange,
                "quote_token": quote_token or "",
            }

            # -----------------------------
            # Derive non-zero size for limit orders
            # -----------------------------
            if order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL):
                usd_value = 0.0
                amount    = 0.0

                # ── Priority 1: trust Groq's own size answer ──────────────────
                if groq_size_usd > 1.0:
                    usd_value = groq_size_usd
                    amount = (
                        groq_size_tokens if groq_size_tokens > 0
                        else (usd_value / ms.current_price if ms.current_price > 0 else 0.0)
                    )
                    if groq_quote_token:
                        info["quote_token"] = groq_quote_token
                    print(f"   ✅ Size from Groq: ${usd_value:.2f} / {amount:.2f} tokens")

                # ── Priority 2: stablecoin or wSOL sent to escrow ────────────
                if usd_value < 1.0:
                    quote_candidates = [
                        (mint, abs(delta))
                        for mint, delta in deltas.items()
                        if mint != MINT and delta < 0 and abs(delta) > 1e-12
                    ]
                    quote_mint = None
                    if quote_candidates:
                        preferred = [
                            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
                            WSOL_MINT,
                        ]
                        for pref in preferred:
                            for mint, _ in quote_candidates:
                                if mint == pref:
                                    quote_mint = mint
                                    break
                            if quote_mint:
                                break
                        if not quote_mint:
                            quote_mint = max(quote_candidates, key=lambda x: x[1])[0]

                    if quote_mint in (
                        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                    ):
                        usd_value = abs(deltas.get(quote_mint, 0.0))
                    elif quote_mint == WSOL_MINT:
                        usd_value = abs(deltas.get(quote_mint, 0.0)) * ms.sol_price_usd

                    if usd_value > 1.0:
                        amount = usd_value / ms.current_price if ms.current_price > 0 else 0.0
                        print(f"   ✅ Size from quote delta: ${usd_value:.2f}")

                # ── Priority 3: native SOL locked into escrow beyond tx fee ──
                if usd_value < 1.0:
                    keys_list = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
                    signer_idx = None
                    for i, k in enumerate(keys_list):
                        pub = k.get("pubkey") if isinstance(k, dict) else k
                        if pub == signer:
                            signer_idx = i
                            break
                    pre_b  = tx_data.get("meta", {}).get("preBalances", [])
                    post_b = tx_data.get("meta", {}).get("postBalances", [])
                    fee    = tx_data.get("meta", {}).get("fee", 5000)
                    if signer_idx is not None and signer_idx < len(pre_b) and signer_idx < len(post_b):
                        sol_to_escrow = max(0.0, (pre_b[signer_idx] - post_b[signer_idx] - fee)) / 1e9
                        if sol_to_escrow > 0.005:
                            usd_value = sol_to_escrow * ms.sol_price_usd
                            amount    = usd_value / ms.current_price if ms.current_price > 0 else 0.0
                            print(f"   ✅ Size from SOL escrow: {sol_to_escrow:.4f} SOL = ${usd_value:.2f}")

                info["amount"]    = amount
                info["usd_value"] = usd_value

                if info["amount"] <= 0 or info["usd_value"] <= 0:
                    print(f"   ⚠️ Rejecting Groq {order_type.value}: could not derive non-zero order size")
                    return OrderType.UNKNOWN, None, 0.0

                tp = self._estimate_target_price(tx_data, info["amount"], ms)
                info["target_price"]   = tp
                info["predicted_mcap"] = self._predict_mcap(tp, ms)

            return order_type, info, confidence

        except Exception as e:
            print(f"   ⚡ Groq error: {e}")
            return OrderType.UNKNOWN, None, 0.0

    @staticmethod
    def _decode_ix_data(data: str) -> Optional[bytes]:
        if not data:
            return None
        _B58_ALPHA = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        _B58_MAP   = {c: i for i, c in enumerate(_B58_ALPHA)}
        try:
            n = 0
            for ch in data.encode():
                if ch not in _B58_MAP:
                    raise ValueError()
                n = n * 58 + _B58_MAP[ch]
            pad    = len(data) - len(data.lstrip("1"))
            result = n.to_bytes((n.bit_length() + 7) // 8 or 1, "big")
            return b"\x00" * pad + result
        except Exception:
            pass
        try:
            import base64
            return base64.b64decode(data + "==")
        except Exception:
            return None

    def _parse_token_changes(
        self, tx_data: Dict, signer: str
    ) -> Optional[Tuple[str, float, str, str]]:
        """
        Signer-centric parser.

        Returns:
        ("BUY", amount, signer, quote_symbol)
        ("SELL", amount, signer, quote_symbol)
        ("TRANSFER", amount, signer, receiver_or_unknown)
        or None
        """
        deltas = get_signer_token_deltas(tx_data, signer)
        target_delta = deltas.get(MINT, 0.0)

        if abs(target_delta) < 1e-12:
            return None

        programs = get_all_program_ids(tx_data)
        swap_like = bool(programs & ALL_SWAP_PROGRAMS or programs & DEX_PROGRAMS)
        limit_like = bool(programs & LIMIT_ORDER_PROGRAMS)

        # Find strongest quote candidates from signer deltas
        negative_quotes = []
        positive_quotes = []

        for mint, delta in deltas.items():
            if mint == MINT or abs(delta) < 1e-12:
                continue
            if delta < 0:
                negative_quotes.append((mint, abs(delta)))
            elif delta > 0:
                positive_quotes.append((mint, abs(delta)))

        # Prefer stablecoin / SOL as quote token if present
        def pick_quote(cands: List[Tuple[str, float]]) -> Optional[str]:
            if not cands:
                return None
            preferred = [
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
                WSOL_MINT,
            ]
            for pref in preferred:
                for mint, _ in cands:
                    if mint == pref:
                        return mint
            return max(cands, key=lambda x: x[1])[0]

        # MARKET BUY
        if target_delta > 0:
            quote_mint = pick_quote(negative_quotes)
            quote_symbol = KNOWN_TOKEN_LABELS.get(
                quote_mint, f"{quote_mint[:8]}…" if quote_mint else "SOL"
            )

            if swap_like:
                return ("BUY", target_delta, signer, quote_symbol)

            if not limit_like:
                return ("TRANSFER", target_delta, signer, "unknown")

        # MARKET SELL
        if target_delta < 0:
            quote_mint = pick_quote(positive_quotes)
            quote_symbol = KNOWN_TOKEN_LABELS.get(
                quote_mint, f"{quote_mint[:8]}…" if quote_mint else "SOL"
            )

            if swap_like:
                return ("SELL", abs(target_delta), signer, quote_symbol)

            if not limit_like:
                return ("TRANSFER", abs(target_delta), signer, "unknown")

        return None

    @staticmethod
    def _get_quote_token(pre_all: List[Dict], post_all: List[Dict],
                         tx_data: Optional[Dict] = None) -> str:
        mint_deltas: Dict[str, int] = {}
        all_mints = {
            b.get("mint") for b in pre_all + post_all
            if b.get("mint") and b.get("mint") != MINT
        }
        for mint in all_mints:
            pre_amt  = sum(int((b.get("uiTokenAmount") or {}).get("amount") or "0")
                           for b in pre_all  if b.get("mint") == mint)
            post_amt = sum(int((b.get("uiTokenAmount") or {}).get("amount") or "0")
                           for b in post_all if b.get("mint") == mint)
            delta = abs(post_amt - pre_amt)
            if delta > 0:
                mint_deltas[mint] = delta
        if mint_deltas:
            top_mint = max(mint_deltas, key=lambda m: mint_deltas[m])
            return KNOWN_TOKEN_LABELS.get(top_mint, f"{top_mint[:8]}…")
        if tx_data:
            output_mint = TransactionClassifier._find_output_mint_from_accounts(tx_data)
            if output_mint:
                return KNOWN_TOKEN_LABELS.get(output_mint, f"{output_mint[:8]}…")
        return "SOL"

    @staticmethod
    def _find_output_mint_from_accounts(tx_data: Dict) -> Optional[str]:
        meta     = tx_data.get("meta", {})
        message  = tx_data.get("transaction", {}).get("message", {})
        pre_all  = meta.get("preTokenBalances",  []) or []
        post_all = meta.get("postTokenBalances", []) or []
        pre_mints  = {b.get("mint") for b in pre_all  if b.get("mint")}
        post_mints = {b.get("mint") for b in post_all if b.get("mint")}
        new_mints  = post_mints - pre_mints - {MINT}
        if new_mints:
            for m in new_mints:
                if m != WSOL_MINT:
                    return m
        ixs = message.get("instructions", [])
        for ix in ixs:
            parsed = ix.get("parsed")
            if isinstance(parsed, dict):
                info = parsed.get("info", {})
                for key in ("tokenMint", "mint", "outputMint", "outMint"):
                    m = info.get(key)
                    if m and m != MINT:
                        return m
        return None

    @staticmethod
    def _get_output_token_amount(tx_data: Dict, output_mint: str) -> float:
        meta     = tx_data.get("meta", {})
        pre_all  = meta.get("preTokenBalances",  []) or []
        post_all = meta.get("postTokenBalances", []) or []
        for b in post_all:
            if b.get("mint") == output_mint:
                decimals = int((b.get("uiTokenAmount") or {}).get("decimals") or 6)
                raw      = int((b.get("uiTokenAmount") or {}).get("amount") or "0")
                pre_b    = next(
                    (x for x in pre_all
                     if x.get("mint") == output_mint
                     and x.get("accountIndex") == b.get("accountIndex")), None
                )
                pre_raw = int((pre_b.get("uiTokenAmount") or {}).get("amount") or "0") if pre_b else 0
                delta   = raw - pre_raw
                if delta > 0:
                    return delta / (10 ** decimals)
        return 0.0

    def _is_transfer(self, tx_data: Dict, signer: str) -> bool:
        meta = tx_data.get("meta", {})
        if set(self._account_keys(tx_data)) & (DEX_PROGRAMS | LIMIT_ORDER_PROGRAMS):
            return False
        pre_all  = meta.get("preTokenBalances",  []) or []
        post_all = meta.get("postTokenBalances", []) or []
        all_mints = {b.get("mint") for b in pre_all + post_all if b.get("mint")}
        if len(all_mints) > 1:
            return False
        pre_sol  = meta.get("preBalances",  [])
        post_sol = meta.get("postBalances", [])
        max_sol_delta = max(
            (abs(post_sol[i] - pre_sol[i]) for i in range(min(len(pre_sol), len(post_sol)))),
            default=0,
        )
        if max_sol_delta > 5_000_000:
            return False
        pre      = [b for b in pre_all  if b.get("mint") == MINT]
        post     = [b for b in post_all if b.get("mint") == MINT]
        pre_map  = {b["owner"]: int((b.get("uiTokenAmount") or {}).get("amount") or "0")
                    for b in pre  if "owner" in b}
        post_map = {b["owner"]: int((b.get("uiTokenAmount") or {}).get("amount") or "0")
                    for b in post if "owner" in b}
        owners  = (set(pre_map) | set(post_map)) - DEX_PROGRAMS - LIMIT_ORDER_PROGRAMS
        gainers = {o for o in owners if post_map.get(o, 0) - pre_map.get(o, 0) > 0}
        losers  = {o for o in owners if post_map.get(o, 0) - pre_map.get(o, 0) < 0}
        return len(gainers) == 1 and len(losers) == 1

    @staticmethod
    def _account_keys(tx_data: Dict) -> List[str]:
        keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        return [k.get("pubkey") if isinstance(k, dict) else k for k in keys]

    def _estimate_target_price(self, tx_data: Dict, token_amount: float, ms: MarketState) -> float:
        if token_amount <= 0:
            return ms.current_price

        meta = tx_data.get("meta", {})

        # 1. Use stablecoin / WSOL output if available
        output_mint = self._find_output_mint_from_accounts(tx_data)
        if output_mint and output_mint != WSOL_MINT:
            output_usd = self._token_amount_to_usd(tx_data, output_mint, ms)
            if output_usd > 0:
                candidate = output_usd / token_amount
                if self._price_is_sane(candidate, ms):
                    return candidate

        # 2. Use signer-only SOL delta, not max delta across all accounts
        keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        signer = None
        if keys:
            signer = keys[0].get("pubkey") if isinstance(keys[0], dict) else keys[0]

        signer_idx = None
        if signer:
            for i, k in enumerate(keys):
                pub = k.get("pubkey") if isinstance(k, dict) else k
                if pub == signer:
                    signer_idx = i
                    break

        pre_b = meta.get("preBalances", [])
        post_b = meta.get("postBalances", [])
        if signer_idx is not None and signer_idx < len(pre_b) and signer_idx < len(post_b):
            sol_spent = abs(post_b[signer_idx] - pre_b[signer_idx]) / 1e9
            if sol_spent > 0.005 and ms.sol_price_usd > 0:
                candidate = (sol_spent * ms.sol_price_usd) / token_amount
                if self._price_is_sane(candidate, ms):
                    return candidate

        # 3. Parse explicit price logs
        for line in (meta.get("logMessages") or []):
            for kw in ("price:", "Price:", "limit_price:", "limitPrice:"):
                idx = line.find(kw)
                if idx != -1:
                    try:
                        c = float(line[idx + len(kw):].split()[0].strip(",}"))
                        if c > 0 and self._price_is_sane(c, ms):
                            return c
                    except (ValueError, IndexError):
                        pass

        return ms.current_price * 0.97

    @staticmethod
    def _price_is_sane(price: float, ms: MarketState) -> bool:
        if ms.current_price <= 0 or price <= 0:
            return False
        ratio = price / ms.current_price
        return 0.001 <= ratio <= 1000

    def _token_amount_to_usd(self, tx_data: Dict, mint: str, ms: MarketState) -> float:
        meta     = tx_data.get("meta", {})
        pre_all  = meta.get("preTokenBalances",  []) or []
        post_all = meta.get("postTokenBalances", []) or []
        pre_amt  = sum(int((b.get("uiTokenAmount") or {}).get("amount") or "0")
                       for b in pre_all  if b.get("mint") == mint)
        post_amt = sum(int((b.get("uiTokenAmount") or {}).get("amount") or "0")
                       for b in post_all if b.get("mint") == mint)
        decimals = 6
        for b in pre_all + post_all:
            if b.get("mint") == mint:
                decimals = int((b.get("uiTokenAmount") or {}).get("decimals") or 6)
                break
        delta = abs(post_amt - pre_amt) / (10 ** decimals)
        STABLECOINS = {
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        }
        if mint in STABLECOINS:
            return delta
        if mint == WSOL_MINT:
            return delta * ms.sol_price_usd
        return 0.0

    @staticmethod
    def _predict_mcap(target_price: float, ms: MarketState) -> float:
        if ms.current_price > 0 and ms.current_market_cap > 0 and target_price > 0:
            return ms.current_market_cap * (target_price / ms.current_price)
        return 0.0


class OrderTracker:
    def __init__(self, db: DatabaseManager, classifier: TransactionClassifier,
                 ms: MarketState) -> None:
        self.db         = db
        self.classifier = classifier
        self.ms         = ms
        self._seen: OrderedDict = OrderedDict()
        self._seen_max = 2000

    def _mark_seen(self, sig: str) -> bool:
        if sig in self._seen:
            return True
        self._seen[sig] = None
        if len(self._seen) > self._seen_max:
            self._seen.popitem(last=False)
        return False

    async def process(self, tx_data: Dict, signature: str) -> Optional[Dict]:
        
        if self._mark_seen(signature):
            return None
        keys   = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        signer = (keys[0].get("pubkey") if isinstance(keys[0], dict) else keys[0]) if keys else ""
        if not signer:
            return None
        order_type, info = await self.classifier.classify(tx_data, signer, self.ms)
        if not info:
            return None

        if order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL, OrderType.TRANSFER):
            if info.get("amount", 0) <= 0 or info.get("usd_value", 0) <= 0:
                print(f"   ⚠️ Ignoring {order_type.value}: zero-sized classification")
                return None
        if order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL):
            if info.get("usd_value", 0) < 5:
                print(f"   ⚠️ Skipping tiny limit order value: {info.get('usd_value', 0):.2f} USD (likely fee/rent)")
                return None

            order = LimitOrder(
                signature      = signature,
                wallet         = info["wallet"],
                order_type     = order_type,
                token_amount   = info["amount"],
                usd_value      = info["usd_value"],
                predicted_mcap = info.get("predicted_mcap", 0.0),
                target_price   = info.get("target_price",   0.0),
                timestamp      = time.time(),
            )
            await self.db.upsert_limit_order(
                order,
                quote_token = info.get("quote_token", ""),
                exchange    = info.get("exchange", ""),
            )
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
        ms     = self.ms
        filled: List[Dict] = []
        if ms.current_market_cap <= 0:
            return filled
        for order in await self.db.get_active_orders():
            predicted = order["predicted_mcap"]
            if predicted <= 0:
                continue
            if order["token_amount"] <= 0 or order["usd_value"] <= 0:
                await self.db.deactivate_by_signature(order["signature"])
                continue
            proximity = abs(predicted - ms.current_market_cap) / ms.current_market_cap
            if (market_type == OrderType.MARKET_BUY
                    and order["order_type"] == "LIMIT_SELL"
                    and proximity < 0.01):
                await self.db.deactivate_by_signature(order["signature"])
                await send_message(ALERT_CHANNEL_ID,
                                   embeds=[_embed_filled(order, market_type, signature, ms)])
                filled.append(order)
            elif (market_type == OrderType.MARKET_SELL
                      and order["order_type"] == "LIMIT_BUY"
                      and proximity < 0.01):
                await self.db.deactivate_by_signature(order["signature"])
                await send_message(ALERT_CHANNEL_ID,
                                   embeds=[_embed_filled(order, market_type, signature, ms)])
                filled.append(order)
        return filled


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
            if removed > 0:
                await send_message(ALERT_CHANNEL_ID, embeds=[_embed_cleanup(removed)])
            self._last_cleanup = now
        if now - self._last < SUMMARY_ALERT_INTERVAL:
            return
        await self._send_summary()
        self._last = now

    async def _send_summary(self) -> None:
        ms     = self.ms
        orders = await self.db.get_active_orders()
        if not orders:
            return
        buys               = [o for o in orders if o["order_type"] == "LIMIT_BUY"]
        sells              = [o for o in orders if o["order_type"] == "LIMIT_SELL"]
        support_lvls       = sorted(o["predicted_mcap"] for o in buys)
        resistance_lvls    = sorted(o["predicted_mcap"] for o in sells)
        nearest_support    = support_lvls[0]    if support_lvls    else None
        nearest_resistance = resistance_lvls[0] if resistance_lvls else None
        embed: dict = {
            "author": {"name": "📊 ACTIVE LIMIT ORDER BOOK — 10-min Snapshot"},
            "title":  "Live Support & Resistance Levels",
            "description": (
                f"```yaml\n"
                f"Active Orders : {len(orders)}\n"
                f"Buy  Orders   : {len(buys)}   |  Wall: {format_usd(sum(o['usd_value'] for o in buys))}\n"
                f"Sell Orders   : {len(sells)}   |  Wall: {format_usd(sum(o['usd_value'] for o in sells))}\n"
                f"```"
            ),
            "color":  0x8B5CF6,
            "fields": [],
        }
        if buys:
            lines = ""
            for i, o in enumerate(sorted(buys, key=lambda x: x["predicted_mcap"])[:6]):
                dist      = _pct_from_current(o["predicted_mcap"], ms)
                qt        = f" [{o.get('quote_token','')}]" if o.get("quote_token") else ""
                remaining = _format_time_remaining(o)
                lines += (
                    f"`{i+1}.` {format_usd(o['usd_value'])}{qt} · "
                    f"mcap `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%) · "
                    f"`{o['wallet'][:6]}…` · ⏳ `{remaining}`\n"
                )
            embed["fields"].append(
                {"name": f"🛡️ SUPPORT LEVELS  ({len(buys)} orders)", "value": lines, "inline": False}
            )
        if sells:
            lines = ""
            for i, o in enumerate(sorted(sells, key=lambda x: x["predicted_mcap"])[:6]):
                dist      = _pct_from_current(o["predicted_mcap"], ms)
                qt        = f" [{o.get('quote_token','')}]" if o.get("quote_token") else ""
                remaining = _format_time_remaining(o)
                lines += (
                    f"`{i+1}.` {format_usd(o['usd_value'])}{qt} · "
                    f"mcap `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%) · "
                    f"`{o['wallet'][:6]}…` · ⏳ `{remaining}`\n"
                )
            embed["fields"].append(
                {"name": f"⚠️ RESISTANCE LEVELS  ({len(sells)} orders)", "value": lines, "inline": False}
            )
        embed["fields"].append({
            "name": "📈 Market Context",
            "value": (
                f"┌ Price:              `${ms.current_price:.8f}`\n"
                f"├ Market Cap:         `{format_usd(ms.current_market_cap)}`\n"
                f"├ Nearest Support:    `{format_usd(nearest_support)}`\n"
                f"└ Nearest Resistance: `{format_usd(nearest_resistance)}`"
            ),
            "inline": False,
        })
        embed["footer"]    = {"text": f"Order Book · updates every 10 min · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"}
        embed["timestamp"] = get_timestamp()
        await send_message(ALERT_CHANNEL_ID, embeds=[embed])


def _embed_cleanup(count: int) -> dict:
    return {
        "author":    {"name": "🧹 Order Book Cleanup"},
        "title":     f"Removed {count} expired limit order(s)",
        "description": (
            f"```yaml\n"
            f"Expired After : 7 days (no fill)\n"
            f"Orders Removed: {count}\n"
            f"```\n"
            f"> Orders that go unfilled for 1 week are automatically removed."
        ),
        "color":     0x6B7280,
        "footer":    {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }

def _embed_cancelled(wallet: str, order: Dict, sig: str) -> dict:
    return {
        "author": {"name": "❌ LIMIT ORDER CANCELLED"},
        "title":  f"🗑️ {order['order_type']} Cancelled",
        "description": (
            f"```yaml\n"
            f"Wallet:   {wallet[:8]}...{wallet[-8:]}\n"
            f"Size:     {format_tokens(order['token_amount'])} XERIS\n"
            f"Value:    {format_usd(order['usd_value'])}\n"
            f"Target:   {format_usd(order['predicted_mcap'])} mcap\n"
            f"Placed:   {_format_placed_at(order)}\n"
            f"```"
        ),
        "color":     0x9CA3AF,
        "fields":    [{"name": "🔗 Transaction",
                       "value": f"[Solscan](https://solscan.io/tx/{sig})", "inline": False}],
        "footer":    {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }

def _embed_filled(order: Dict, fill_type: OrderType, sig: str, ms: MarketState) -> dict:
    return {
        "author": {"name": "✅ LIMIT ORDER FILLED"},
        "title":  f"💹 {order['order_type']} Executed",
        "description": (
            f"```yaml\n"
            f"Size:     {format_tokens(order['token_amount'])} XERIS\n"
            f"Value:    {format_usd(order['usd_value'])}\n"
            f"Wallet:   {order['wallet'][:8]}...{order['wallet'][-8:]}\n"
            f"Placed:   {_format_placed_at(order)}\n"
            f"```\n> Filled by a **{fill_type.value}** market order."
        ),
        "color":  0x10B981,
        "fields": [
            {"name": "📊 Levels",
             "value": (f"┌ Predicted MCap: `{format_usd(order['predicted_mcap'])}`\n"
                       f"└ Current MCap:   `{format_usd(ms.current_market_cap)}`"),
             "inline": False},
            {"name": "🔗 Links",
             "value": (f"[Original](https://solscan.io/tx/{order['signature']}) · "
                       f"[Fill Tx](https://solscan.io/tx/{sig})"),
             "inline": False},
        ],
        "footer":    {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }

def _build_limit_order_embed(order: LimitOrder, ms: MarketState,
                              quote_token: str = "", exchange: str = "") -> dict:
    is_buy     = order.order_type == OrderType.LIMIT_BUY
    color      = 0x10B981 if is_buy else 0xEF4444
    direction  = "BUY" if is_buy else "SELL"
    dist       = _pct_from_current(order.predicted_mcap, ms)
    role       = "Support Level" if is_buy else "Resistance Level"
    pair_label = (
        f"{quote_token} → XERIS" if is_buy and quote_token
        else f"XERIS → {quote_token}" if not is_buy and quote_token
        else "XERIS"
    )
    placed_at  = datetime.fromtimestamp(order.timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    expires_at = datetime.fromtimestamp(order.timestamp + ORDER_TTL_SECS, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    order_dict = {"timestamp": order.timestamp}
    return {
        "author": {"name": f"🎯 NEW LIMIT {'BUY' if is_buy else 'SELL'} DETECTED"},
        "title":  f"{'📈' if is_buy else '📉'} Limit {direction} · {format_usd(order.usd_value)}",
        "description": (
            f"```yaml\n"
            f"Type:         LIMIT {direction}\n"
            f"Pair:         {pair_label}\n"
            f"Size:         {format_tokens(order.token_amount)} XERIS\n"
            f"Value:        {format_usd(order.usd_value)}\n"
            f"Target Price: ${order.target_price:.8f}\n"
            f"Target MCap:  {format_usd(order.predicted_mcap)}\n"
            f"Distance:     {dist:+.2f}% from current\n"
            f"Role:         {role}\n"
            f"Placed At:    {placed_at}\n"
            f"Expires At:   {expires_at}\n"
            + (f"Exchange:     {exchange}\n" if exchange else "")
            + "```"
        ),
        "color":  color,
        "fields": [
            {"name": "👤 Wallet", "value": f"```{order.wallet}```", "inline": False},
            {"name": "📊 Market",
             "value": (f"┌ Current MCap: `{format_usd(ms.current_market_cap)}`\n"
                       f"└ Target MCap:  `{format_usd(order.predicted_mcap)}` ({dist:+.1f}%)"),
             "inline": False},
            {"name": "⏳ Lifetime",
             "value": (f"`{_expiry_bar(order_dict)}` {_format_time_remaining(order_dict)}\n"
                       f"Placed: `{placed_at}` · Expires: `{expires_at}`"),
             "inline": False},
            {"name": "🔗",
             "value": (f"[Tx](https://solscan.io/tx/{order.signature}) · "
                       f"[Wallet](https://solscan.io/account/{order.wallet}) · "
                       f"[Chart](https://dexscreener.com/solana/{MINT})"),
             "inline": False},
        ],
        "footer":    {"text": f"Limit Order Tracker · {role} · expires in 7 days · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }

def _build_whale_embed(tx_type: str, amount: float, wallet: str, usd_value: float,
                        signature: str, ms: MarketState, quote_token: str = "",
                        exchange: str = "") -> dict:
    is_buy   = tx_type == "BUY"
    color    = 0x10B981 if is_buy else 0xEF4444
    new_mcap = ms.current_market_cap + usd_value if is_buy else max(0, ms.current_market_cap - usd_value)
    diff     = new_mcap - ms.current_market_cap
    impact   = (usd_value / ms.current_market_cap * 100) if ms.current_market_cap > 0 else 0
    if usd_value >= 50_000:  tier = "💎 MEGA WHALE"
    elif usd_value >= 10_000:tier = "🌊 WHALE"
    elif usd_value >= 5_000: tier = "⭐ BIG FISH"
    else:                    tier = "💫 FISH"
    pair_label = (
        f"{quote_token} → XERIS" if is_buy and quote_token
        else f"XERIS → {quote_token}" if not is_buy and quote_token
        else f"XERIS {'bought' if is_buy else 'sold'}"
    )
    return {
        "author": {"name": f"{tier} DETECTED"},
        "title":  f"{'📈' if is_buy else '📉'} {tx_type} · {format_usd(usd_value)}",
        "description": (
            f"```yaml\n"
            f"Pair:   {pair_label}\n"
            f"Trade:  {format_tokens(amount)} XERIS\n"
            f"USD:    {format_usd(usd_value)}\n"
            f"Impact: {impact:.2f}% of MCap\n"
            + (f"Via:    {exchange}\n" if exchange else "")
            + "```"
        ),
        "color":  color,
        "fields": [
            {"name": "💰 Market Metrics",
             "value": (f"┌ Price: `${ms.current_price:.8f}`\n"
                       f"├ MCap: `{format_usd(ms.current_market_cap)}`\n"
                       f"└ New MCap: `{format_usd(new_mcap)}` ({'+' if diff>=0 else ''}{format_usd(diff)})"),
             "inline": False},
            {"name": "👤 Wallet", "value": f"```{wallet}```", "inline": False},
            {"name": "🔗 Links",
             "value": (f"[TX](https://solscan.io/tx/{signature}) · "
                       f"[Wallet](https://solscan.io/account/{wallet}) · "
                       f"[Chart](https://dexscreener.com/solana/{MINT})"),
             "inline": False},
        ],
        "footer":    {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }

def _build_dev_sell_embed(amount: float, wallet: str, usd_value: float, signature: str,
                           ms: MarketState, quote_token: str = "") -> dict:
    new_mcap   = max(0, ms.current_market_cap - usd_value)
    impact     = (usd_value / ms.current_market_cap * 100) if ms.current_market_cap > 0 else 0
    pair_label = f"XERIS → {quote_token}" if quote_token else "XERIS sold"
    return {
        "author": {"name": "⚠️ DEVELOPER ACTIVITY ALERT"},
        "title":  "🚨 Dev Wallet Sell Detected",
        "description": (
            "```diff\n- Developer has executed a SELL transaction\n```\n"
            f"**⚠️ Monitor price action closely**\n"
            f"> Amount: **{format_usd(usd_value)}** ({impact:.2f}% of MCap)"
        ),
        "color":  0xDC2626,
        "fields": [
            {"name": "💸 Details",
             "value": (f"```yaml\n"
                       f"Pair:   {pair_label}\n"
                       f"Tokens: {format_tokens(amount)} XERIS\n"
                       f"USD:    {format_usd(usd_value)}\n"
                       f"Price:  ${ms.current_price:.8f}\n"
                       f"Impact: {impact:.2f}%\n```"),
             "inline": False},
            {"name": "📊 MCap Impact",
             "value": (f"┌ Before: `{format_usd(ms.current_market_cap)}`\n"
                       f"└ After:  `{format_usd(new_mcap)}`"),
             "inline": False},
            {"name": "👤 Dev Wallet", "value": f"```{wallet}```", "inline": False},
            {"name": "🔍 Links",
             "value": (f"[TX](https://solscan.io/tx/{signature}) · "
                       f"[Wallet](https://solscan.io/account/{wallet}) · "
                       f"[Chart](https://dexscreener.com/solana/{MINT})"),
             "inline": False},
        ],
        "footer":    {"text": f"Dev Monitor · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }

def _build_price_embed(pct: float, ref: float, ms: MarketState) -> dict:
    is_pump  = pct > 0
    sign     = "+" if is_pump else ""
    abs_pct  = abs(pct)
    color    = 0x10B981 if is_pump else 0xEF4444
    ref_mcap = ms.current_market_cap / (1 + pct / 100)
    mcap_chg = ms.current_market_cap - ref_mcap
    bars     = "█" * min(12, round(abs_pct / 2)) + "░" * max(0, 12 - round(abs_pct / 2))
    return {
        "author": {"name": "⚡ Price Alert — XerisCoin"},
        "title":  f"{'🚀 PUMP' if is_pump else '📉 DUMP'} · {sign}{pct:.2f}%",
        "description": (
            f"```diff\n{'+ ' if is_pump else '- '}{sign}{pct:.2f}% from reference\n```\n"
            f"> Price {'surged' if is_pump else 'dropped'} **{sign}{pct:.2f}%** from last anchor"
        ),
        "color":  color,
        "fields": [
            {"name": "💹 Price",
             "value": f"```yaml\nRef: ${ref:.8f}\nNow: ${ms.current_price:.8f}\nΔ:   {sign}{pct:.2f}%\n```",
             "inline": True},
            {"name": "📊 MCap",
             "value": (f"```yaml\n"
                       f"Now: {format_usd(ms.current_market_cap)}\n"
                       f"Δ:   {'+' if mcap_chg>=0 else ''}{format_usd(abs(mcap_chg))}\n```"),
             "inline": True},
            {"name": "📈 Momentum",
             "value": f"`{bars}` **{abs_pct:.1f}%**",
             "inline": False},
            {"name": "🔗 Charts",
             "value": (f"[DexScreener](https://dexscreener.com/solana/{MINT}) · "
                       f"[Birdeye](https://birdeye.so/token/{MINT}) · "
                       f"[Solscan](https://solscan.io/token/{MINT})"),
             "inline": False},
        ],
        "footer":    {"text": f"Threshold ±{PRICE_ALERT_THRESHOLD}% · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }


async def update_price(ms: MarketState) -> None:
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{MINT}"
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url)
            data = r.json()

        pairs = data.get("pairs") or []
        pair = _pick_best_pair(pairs)
        if not pair:
            print("⚠️ No pairs on DexScreener")
            return

        new_price = float(pair.get("priceUsd") or 0)
        fdv = pair.get("fdv")
        mcap = pair.get("marketCap")

        if fdv:
            ms.current_market_cap = float(fdv)
        elif mcap:
            ms.current_market_cap = float(mcap)
        elif new_price > 0:
            liq = float((pair.get("liquidity") or {}).get("usd") or 0)
            ms.current_market_cap = liq * 2

        if ms.price_reference == 0.0 and new_price > 0:
            ms.price_reference = new_price

        ms.current_price = new_price
        ms.last_price_update = time.time()
        print(f"💰 ${ms.current_price:.8f}  |  MCap {format_usd(ms.current_market_cap)}")

        if ms.price_reference > 0:
            await _check_price_alert(ms)
    except Exception as e:
        print(f"❌ Price error: {e}")

async def _check_price_alert(ms: MarketState) -> None:
    if ms.price_reference <= 0 or ms.current_price <= 0:
        return
    pct = (ms.current_price - ms.price_reference) / ms.price_reference * 100
    if abs(pct) < PRICE_ALERT_THRESHOLD:
        return
    direction = "up" if pct > 0 else "down"
    last      = ms.last_alert_up_time if direction == "up" else ms.last_alert_down_time
    if time.time() - last < PRICE_ALERT_COOLDOWN:
        return
    await send_message(ALERT_CHANNEL_ID, embeds=[_build_price_embed(pct, ms.price_reference, ms)])
    if direction == "up":
        ms.last_alert_up_time = time.time()
    else:
        ms.last_alert_down_time = time.time()
    ms.last_alert_direction = direction
    ms.price_reference      = ms.current_price


async def fetch_tx(signature: str, retries: int = 3) -> Optional[Dict]:
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
        "params": [signature, {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "commitment": "confirmed",
        }],
    }
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(RPC_URL, json=payload)
            if r.status_code == 200:
                result = r.json()
                if "error" not in result:
                    tx = result.get("result")
                    if tx:
                        return tx
        except Exception as e:
            print(f"   fetch attempt {attempt + 1}: {e}")
        if attempt < retries - 1:
            wait = 2 ** attempt + random.random()
            await asyncio.sleep(wait)
    return None

async def fetch_price_for_ca(ca: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
            data = r.json()

        pairs = data.get("pairs") or []
        p = _pick_best_pair(pairs)
        if not p:
            return {}

        return {
            "price": float(p.get("priceUsd") or 0),
            "mcap": float(p.get("fdv") or p.get("marketCap") or 0),
            "volume_24h": float((p.get("volume") or {}).get("h24") or 0),
            "change_24h": float((p.get("priceChange") or {}).get("h24") or 0),
            "liquidity": float((p.get("liquidity") or {}).get("usd") or 0),
            "dex": p.get("dexId", "unknown"),
            "pair_addr": p.get("pairAddress", ""),
            "name": p.get("baseToken", {}).get("name", "Unknown"),
            "symbol": p.get("baseToken", {}).get("symbol", "???"),
        }
    except Exception as e:
        print(f"❌ DexScreener error: {e}")
        return {}

async def fetch_top_holders(ca: str) -> list:
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [ca, {"commitment": "confirmed"}],
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r      = await client.post(RPC_URL, json=payload)
            result = r.json().get("result", {})
            return result.get("value", [])
    except Exception as e:
        print(f"❌ Holder fetch error: {e}")
        return []

async def scan_socials(ca: str, token_name: str, token_symbol: str) -> dict:
    results = {"twitter": None, "website": None, "twitter_handle": None}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r    = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
            data = r.json()
            pairs = data.get("pairs") or []
            if pairs:
                info     = pairs[0].get("info") or {}
                websites = info.get("websites") or []
                socials  = info.get("socials")  or []
                for s in socials:
                    if s.get("type") == "twitter":
                        results["twitter"] = s.get("url")
                        handle = (s.get("url") or "").rstrip("/").split("/")[-1]
                        results["twitter_handle"] = f"@{handle}" if handle else None
                for w in websites:
                    if w.get("url"):
                        results["website"] = w.get("url")
                        break
    except Exception as e:
        print(f"⚠️ Social scan error: {e}")
    return results

async def fetch_token_metadata(ca: str) -> dict:
    result = {
        "deployer":         None,
        "mint_authority":   None,
        "freeze_authority": None,
        "created_at":       None,
        "token_age_days":   None,
        "decimals":         6,
        "supply":           0,
    }
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
            "params": [ca, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            r      = await client.post(RPC_URL, json=payload)
            data   = r.json().get("result", {}).get("value", {})
            parsed = (data.get("data") or {}).get("parsed", {})
            info   = parsed.get("info", {})
            result["mint_authority"]   = info.get("mintAuthority")
            result["freeze_authority"] = info.get("freezeAuthority")
            result["decimals"]         = info.get("decimals", 6)
            result["supply"]           = int(info.get("supply", "0")) / (10 ** result["decimals"])
        sig_payload = {
            "jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
            "params": [ca, {"limit": 1000, "commitment": "confirmed"}],
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            r    = await client.post(RPC_URL, json=sig_payload)
            sigs = r.json().get("result", [])
            if sigs:
                block_time = sigs[-1].get("blockTime")
                if block_time:
                    created_dt             = datetime.fromtimestamp(block_time, tz=timezone.utc)
                    result["created_at"]   = created_dt.strftime("%Y-%m-%d %H:%M UTC")
                    result["token_age_days"] = (datetime.now(timezone.utc) - created_dt).days
    except Exception as e:
        print(f"⚠️ Token metadata error: {e}")
    return result

async def fetch_deployer_history(deployer_wallet: str) -> dict:
    result = {"wallet": deployer_wallet, "total_prev": 0, "wallet_age_days": None}
    if not deployer_wallet:
        return result
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
            "params": [deployer_wallet, {"limit": 1000, "commitment": "confirmed"}],
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            r    = await client.post(RPC_URL, json=payload)
            sigs = r.json().get("result", [])
            if sigs:
                block_time = sigs[-1].get("blockTime")
                if block_time:
                    created_dt               = datetime.fromtimestamp(block_time, tz=timezone.utc)
                    result["wallet_age_days"] = (datetime.now(timezone.utc) - created_dt).days
            result["total_prev"] = max(0, len(sigs) - 1)
    except Exception as e:
        print(f"⚠️ Deployer history error: {e}")
    return result

async def fetch_pumpfun_metadata(ca: str) -> dict:
    result = {
        "is_pumpfun":  False, "creator": None, "description": None,
        "graduated":   False, "reply_count": 0, "name": None,
        "symbol":      None,  "image_url": None, "telegram": None,
        "twitter":     None,  "website": None,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"https://frontend-api.pump.fun/coins/{ca}")
            if r.status_code == 200:
                d = r.json()
                result.update({
                    "is_pumpfun":  True,
                    "creator":     d.get("creator"),
                    "description": (d.get("description") or "")[:300],
                    "graduated":   d.get("complete", False),
                    "reply_count": d.get("reply_count", 0),
                    "name":        d.get("name"),
                    "symbol":      d.get("symbol"),
                    "image_url":   d.get("image_uri"),
                    "telegram":    d.get("telegram"),
                    "twitter":     d.get("twitter"),
                    "website":     d.get("website"),
                })
    except Exception as e:
        print(f"⚠️ Pump.fun error: {e}")
    return result

async def fetch_all_intelligence(ca: str, name: str, symbol: str) -> dict:
    print(f"   🔎 Gathering intelligence for {ca[:16]}...")
    token_meta, pumpfun, socials = await asyncio.gather(
        fetch_token_metadata(ca),
        fetch_pumpfun_metadata(ca),
        scan_socials(ca, name, symbol),
    )
    if pumpfun.get("twitter") and not socials.get("twitter"):
        socials["twitter"] = pumpfun["twitter"]
        handle = pumpfun["twitter"].rstrip("/").split("/")[-1]
        socials["twitter_handle"] = f"@{handle}"
    if pumpfun.get("website") and not socials.get("website"):
        socials["website"] = pumpfun["website"]
    if pumpfun.get("telegram"):
        socials["telegram"] = pumpfun["telegram"]
    deployer_wallet = token_meta.get("mint_authority") or pumpfun.get("creator")
    deployer = {}
    if deployer_wallet:
        deployer = await fetch_deployer_history(deployer_wallet)
    return {"token_meta": token_meta, "pumpfun": pumpfun, "socials": socials, "deployer": deployer}


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
    holder_lines = "\n".join(
        f"  #{i+1}: {float(h.get('uiAmount',0)):,.0f} tokens ({float(h.get('uiAmount',0))/total_supply*100:.2f}%)"
        for i, h in enumerate(holders[:10])
    ) if holders else "  No holder data available"
    liq     = price_data.get("liquidity", 0)
    vol     = price_data.get("volume_24h", 0)
    mcap    = price_data.get("mcap", 0)
    vol_liq = (vol / liq  * 100) if liq  > 0 else 0
    vol_mc  = (vol / mcap * 100) if mcap > 0 else 0
    deployer_wallet  = deployer.get("wallet") or token_meta.get("mint_authority") or "UNKNOWN"
    deployer_age     = deployer.get("wallet_age_days")
    deployer_tx      = deployer.get("total_prev", 0)
    freeze_authority = token_meta.get("freeze_authority")
    mint_authority   = token_meta.get("mint_authority")
    pf_graduated     = pumpfun.get("graduated", False)
    pf_desc          = pumpfun.get("description") or "No description"
    pf_replies       = pumpfun.get("reply_count", 0)
    pf_telegram      = socials.get("telegram") or "NOT FOUND"
    prompt = f"""You are an elite crypto risk analyst and rug pull detective specializing in Solana meme tokens.
Return ONLY a raw JSON object — no markdown, no backticks, no text outside the JSON.

CONTRACT: {ca}
Token: {price_data.get('name','Unknown')} ({price_data.get('symbol','???')})

MARKET DATA:
Price: ${price_data.get('price', 0):.8f}  MCap: ${mcap:,.0f}  24h Vol: ${vol:,.0f}
24h Chg: {price_data.get('change_24h', 0):.2f}%  Liquidity: ${liq:,.0f}
Vol/Liq: {vol_liq:.1f}%  Vol/MCap: {vol_mc:.1f}%  DEX: {price_data.get('dex','unknown')}

TOKEN METADATA:
Age: {token_meta.get('token_age_days','?')} days (created {token_meta.get('created_at','Unknown')})
Mint Authority: {mint_authority or 'REVOKED'}  Freeze Authority: {freeze_authority or 'REVOKED'}

DEPLOYER:
Wallet: {deployer_wallet}  Age: {f"{deployer_age} days" if deployer_age else "Unknown"}
Prior txs: ~{deployer_tx}  Launchpad: {'Pump.fun' if pumpfun.get('is_pumpfun') else 'Direct'}
Graduated: {'YES' if pf_graduated else 'NO'}  Desc: {pf_desc}  Replies: {pf_replies}

HOLDERS:
{holder_lines}
Top 5: {top5_pct:.2f}%  Top 10: {top10_pct:.2f}%  Biggest: {biggest:.2f}%

SOCIALS:
Twitter: {socials.get('twitter_handle') or 'NOT FOUND'}
Website: {socials.get('website') or 'NOT FOUND'}
Telegram: {pf_telegram}

Return this exact JSON:
{{
  "risk_score": <integer 0-10>,
  "rug_label": "<LIKELY SAFE|PROCEED WITH CAUTION|HIGH RISK|LIKELY RUG>",
  "summary": "<2 sentence verdict>",
  "team_analysis": "<2-3 sentences on team/deployer>",
  "red_flags": ["<max 5 flags>"],
  "green_flags": ["<max 4 signals>"],
  "holder_analysis": "<2 sentences>",
  "liquidity_analysis": "<2 sentences>",
  "social_analysis": "<2 sentences>",
  "mint_freeze_risk": "<1 sentence>",
  "trade_advice": "<1 direct sentence>"
}}"""
    try:
        async with httpx.AsyncClient(timeout=35.0) as client:
            r = await client.post(
                GROQ_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 1200, "temperature": 0.3},
            )
            if r.status_code == 200:
                raw   = r.json()["choices"][0]["message"]["content"]
                clean = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
                return json.loads(clean)
            return {"error": f"Groq API error {r.status_code}"}
    except json.JSONDecodeError as e:
        return {"error": f"Failed to parse AI response: {e}"}
    except Exception as e:
        return {"error": f"Groq request failed: {e}"}


async def cmd_price(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    data = await fetch_price_for_ca(ca)
    if not data:
        await send_message(channel_id, embeds=[{
            "title": "❌ Token Not Found", "description": f"No data found for:\n```{ca}```", "color": 0xEF4444,
        }])
        return
    change   = data.get("change_24h", 0)
    color    = 0x10B981 if change >= 0 else 0xEF4444
    sign     = "+" if change >= 0 else ""
    vol_mcap = (data["volume_24h"] / data["mcap"] * 100) if data.get("mcap") else 0
    await send_message(channel_id, embeds=[{
        "author": {"name": f"💰 Price Info · {data['name']} ({data['symbol']})"},
        "color":  color,
        "fields": [
            {"name": "📊 Market Data",
             "value": (f"```yaml\n"
                       f"Price:     ${data['price']:.8f}\n"
                       f"24h Chg:   {sign}{change:.2f}%\n"
                       f"MCap:      {format_usd(data['mcap'])}\n"
                       f"Vol (24h): {format_usd(data['volume_24h'])}\n"
                       f"V/MC:      {vol_mcap:.2f}%\n"
                       f"Liquidity: {format_usd(data['liquidity'])}\n"
                       f"DEX:       {data['dex'].upper()}\n```"),
             "inline": False},
            {"name": "🔗 Charts",
             "value": (f"[DexScreener](https://dexscreener.com/solana/{ca}) · "
                       f"[Birdeye](https://birdeye.so/token/{ca}) · "
                       f"[Solscan](https://solscan.io/token/{ca})"),
             "inline": False},
        ],
        "footer":    {"text": f"Via DexScreener · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])


async def cmd_whale(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    holders, price_data = await asyncio.gather(fetch_top_holders(ca), fetch_price_for_ca(ca))
    if not holders:
        await send_message(channel_id, embeds=[{
            "title": "❌ No Holder Data", "description": f"Could not fetch holders for:\n```{ca}```", "color": 0xEF4444,
        }])
        return
    total = 1_000_000_000
    rows  = []
    for i, h in enumerate(holders[:15]):
        amt   = float(h.get("uiAmount") or 0)
        pct   = (amt / total) * 100
        addr  = h.get("address", "???")
        short = f"{addr[:6]}...{addr[-6:]}"
        bar   = "█" * max(1, round(pct)) + "░" * max(0, 10 - round(pct))
        rows.append(f"#{i+1:>2}  {short}  {pct:5.2f}%  {bar}")
    top5_pct   = sum(float(h.get("uiAmount", 0)) / total * 100 for h in holders[:5])
    top10_pct  = sum(float(h.get("uiAmount", 0)) / total * 100 for h in holders[:10])
    risk_color = 0xEF4444 if top10_pct > 50 else (0xF59E0B if top10_pct > 30 else 0x10B981)
    name       = price_data.get("name", "Unknown") if price_data else "Unknown"
    symbol     = price_data.get("symbol", "???")   if price_data else "???"
    await send_message(channel_id, embeds=[{
        "author":      {"name": f"🐳 Top Holders · {name} ({symbol})"},
        "color":       risk_color,
        "description": f"```\nRank  Wallet            Share  Bar\n{'─'*42}\n" + "\n".join(rows) + "\n```",
        "fields": [
            {"name": "📊 Concentration",
             "value": (f"```yaml\n"
                       f"Top 5:  {top5_pct:.2f}%\n"
                       f"Top 10: {top10_pct:.2f}%\n"
                       f"Risk:   {'🔴 HIGH' if top10_pct > 50 else ('🟡 MEDIUM' if top10_pct > 30 else '🟢 LOW')}\n```"),
             "inline": False},
            {"name": "🔍 Explore",
             "value": f"[Solscan Holders](https://solscan.io/token/{ca}#holders) · [Birdeye](https://birdeye.so/token/{ca})",
             "inline": False},
        ],
        "footer":    {"text": f"Via Helius RPC · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])

TIMEFRAME_MAP = {
    "1m":  1,
    "5m":  5,
    "15m": 15,
    "1h":  60,
    "1d":  1440,
    "1D":  1440,
}

async def fetch_geckoterminal(ca: str) -> dict:
    try:
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ca}/pools"
        headers = {"Accept": "application/json;version=20230302"}
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers=headers)
            if r.status_code != 200:
                return {}
            data = r.json()
        pools = data.get("data", [])
        if not pools:
            return {}
        best  = pools[0]
        attrs = best.get("attributes", {})
        return {
            "pool_address":     best.get("id", "").replace("solana_", ""),
            "name":             attrs.get("name", "Unknown"),
            "price_usd":        float(attrs.get("base_token_price_usd") or 0),
            "price_change_5m":  float((attrs.get("price_change_percentage") or {}).get("m5")  or 0),
            "price_change_1h":  float((attrs.get("price_change_percentage") or {}).get("h1")  or 0),
            "price_change_24h": float((attrs.get("price_change_percentage") or {}).get("h24") or 0),
            "volume_24h":       float(attrs.get("volume_usd", {}).get("h24") or 0),
            "liquidity":        float(attrs.get("reserve_in_usd") or 0),
            "fdv":              float(attrs.get("fdv_usd") or 0),
            "market_cap":       float(attrs.get("market_cap_usd") or 0),
            "buys_24h":         int((attrs.get("transactions", {}).get("h24") or {}).get("buys")  or 0),
            "sells_24h":        int((attrs.get("transactions", {}).get("h24") or {}).get("sells") or 0),
        }
    except Exception as e:
        print(f"❌ GeckoTerminal error: {e}")
        return {}


  = ((last_close - first_close) / first_close * 100) if first_close > 0 else 0
        pct_color   = GREEN if pct_change >= 0 else RED
        pct_sign    = "+" if pct_change >= 0 else ""

        fig.text(
            0.01, 0.97,
            f"{token_name}   ${last_close:.8f}",
            color=TEXT, fontsize=13, fontweight="bold",
            va="top", ha="left",
        )
        fig.text(
            0.01, 0.91,
            f"{pct_sign}{pct_change:.2f}%  ·  {tf_label}  ·  {n} candles",
            color=pct_color, fontsize=9,
            va="top", ha="left",
        )
        fig.text(
            0.99, 0.97,
            "GeckoTerminal",
            color=SUBTEXT, fontsize=8,
            va="top", ha="right",
        )

        # ── Limits ──────────────────────────────────────────────────────────
        ax_candle.set_xlim(-0.5, n - 0.5)
        ax_vol.set_xlim(-0.5, n - 0.5)
        price_pad = (max(highs) - min(lows)) * 0.05
        ax_candle.set_ylim(min(lows) - price_pad, max(highs) + price_pad)

        plt.tight_layout(rect=[0, 0, 1, 0.93])

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                    facecolor=BG, edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    except Exception as e:
        print(f"❌ Chart render error: {e}")
        return None

TIMEFRAME_MAP = {
    "1m":  {"birdeye": "1m",  "label": "1M",  "resolution": 1},
    "5m":  {"birdeye": "5m",  "label": "5M",  "resolution": 5},
    "15m": {"birdeye": "15m", "label": "15M", "resolution": 15},
    "1h":  {"birdeye": "1H",  "label": "1H",  "resolution": 60},
    "1d":  {"birdeye": "1D",  "label": "1D",  "resolution": 1440},
}

async def fetch_birdeye_ohlcv(ca: str, timeframe: str = "15m") -> Optional[list]:
    """
    Fetch OHLCV from Birdeye public API — no API key needed for basic data.
    Returns list of {time, open, high, low, close, volume} or None.
    """
    tf_cfg    = TIMEFRAME_MAP.get(timeframe, TIMEFRAME_MAP["15m"])
    interval  = tf_cfg["birdeye"]
    limit     = 100

    url = (
        f"https://public-api.birdeye.so/defi/ohlcv"
        f"?address={ca}&type={interval}&limit={limit}"
    )
    try:
        headers = {
            "accept":       "application/json",
            "x-chain":      "solana",
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers=headers)
            if r.status_code != 200:
                print(f"❌ Birdeye OHLCV {r.status_code}: {r.text[:100]}")
                return None
            data  = r.json()
            items = data.get("data", {}).get("items", [])
            if not items:
                return None
            return items  # already oldest-first
    except Exception as e:
        print(f"❌ Birdeye OHLCV error: {e}")
        return None

async def generate_chart_image(
    ca: str,
    timeframe: str,
    token_name: str,
) -> Optional[bytes]:
    """
    Render a dark candlestick chart from Birdeye OHLCV data.
    Returns PNG bytes or None on failure.
    """
    candles = await fetch_birdeye_ohlcv(ca, timeframe)
    if not candles or len(candles) < 5:
        return None

    try:
        tf_cfg = TIMEFRAME_MAP.get(timeframe, TIMEFRAME_MAP["15m"])
        label  = tf_cfg["label"]

        timestamps = [c["unixTime"] for c in candles]
        opens      = [float(c["o"]) for c in candles]
        highs      = [float(c["h"]) for c in candles]
        lows       = [float(c["l"]) for c in candles]
        closes     = [float(c["c"]) for c in candles]
        volumes    = [float(c["v"]) for c in candles]

        n  = len(candles)
        xs = list(range(n))

        # ── Theme ────────────────────────────────────────────────
        BG        = "#0d1117"
        GRID      = "#21262d"
        GREEN     = "#26a641"
        RED       = "#da3633"
        VOL_GREEN = "#1a4d2e"
        VOL_RED   = "#4d1a1a"
        TEXT      = "#e6edf3"
        SUBTEXT   = "#8b949e"

        fig, (ax, ax_vol) = plt.subplots(
            2, 1,
            figsize=(13, 7),
            gridspec_kw={"height_ratios": [3, 1], "hspace": 0.03},
            facecolor=BG,
        )

        for a in (ax, ax_vol):
            a.set_facecolor(BG)
            a.tick_params(colors=SUBTEXT, labelsize=7.5)
            for spine in a.spines.values():
                spine.set_color(GRID)
            a.yaxis.grid(True, color=GRID, linewidth=0.4, linestyle="--", alpha=0.5)
            a.xaxis.grid(False)

        # ── Candles ───────────────────────────────────────────────
        w = 0.55
        for i in xs:
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            col = GREEN if c >= o else RED
            ax.plot([i, i], [l, h], color=col, linewidth=0.9, zorder=2)
            ax.bar(i, abs(c - o), bottom=min(o, c),
                   width=w, color=col, zorder=3, linewidth=0)

        # ── Volume ────────────────────────────────────────────────
        for i in xs:
            col = VOL_GREEN if closes[i] >= opens[i] else VOL_RED
            ax_vol.bar(i, volumes[i], width=w, color=col, linewidth=0, alpha=0.9)

        # ── Y axis formatting ─────────────────────────────────────
        last_close = closes[-1]
        # Auto-choose decimal places based on price magnitude
        if last_close < 0.000001:
            fmt = "%.10f"
        elif last_close < 0.0001:
            fmt = "%.8f"
        elif last_close < 0.01:
            fmt = "%.6f"
        else:
            fmt = "%.4f"

        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter(fmt))
        ax.yaxis.tick_right()
        ax_vol.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}K" if x >= 1000 else f"{x:.0f}")
        )
        ax_vol.yaxis.tick_right()

        # ── X axis timestamps ─────────────────────────────────────
        step = max(1, n // 8)
        tick_xs     = xs[::step]
        tick_labels = []
        res = tf_cfg["resolution"]
        for i in tick_xs:
            dt = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
            if res >= 1440:
                tick_labels.append(dt.strftime("%m/%d"))
            elif res >= 60:
                tick_labels.append(dt.strftime("%d %H:%M"))
            else:
                tick_labels.append(dt.strftime("%H:%M"))

        ax_vol.set_xticks(tick_xs)
        ax_vol.set_xticklabels(tick_labels, color=SUBTEXT, fontsize=7)
        ax.set_xticks([])

        # ── Header text ───────────────────────────────────────────
        first_close = closes[0]
        pct = ((last_close - first_close) / first_close * 100) if first_close > 0 else 0
        pct_col  = GREEN if pct >= 0 else RED
        pct_sign = "+" if pct >= 0 else ""

        fig.text(0.01, 0.97, f"{token_name}",
                 color=TEXT, fontsize=13, fontweight="bold", va="top")
        fig.text(0.01, 0.925, f"{fmt % last_close}   {pct_sign}{pct:.2f}%   {label}",
                 color=pct_col, fontsize=10, va="top")
        fig.text(0.99, 0.97, "Birdeye · XerisBot",
                 color=SUBTEXT, fontsize=8, va="top", ha="right")

        # ── Limits ────────────────────────────────────────────────
        ax.set_xlim(-0.6, n - 0.4)
        ax_vol.set_xlim(-0.6, n - 0.4)
        pad = (max(highs) - min(lows)) * 0.06
        ax.set_ylim(min(lows) - pad, max(highs) + pad)

        plt.tight_layout(rect=[0, 0, 1, 0.92])

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=140,
                    bbox_inches="tight", facecolor=BG, edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    except Exception as e:
        print(f"❌ Chart render error: {e}")
        try:
            plt.close("all")
        except Exception:
            pass
        return None


async def cmd_chart(channel_id: int, ca: str, timeframe: str = "15m") -> None:
    await send_typing(channel_id)

    tf_clean = timeframe.lower().strip()
    if tf_clean not in TIMEFRAME_MAP:
        tf_clean = "15m"
    tf_cfg   = TIMEFRAME_MAP[tf_clean]
    tf_label = tf_cfg["label"]
    res      = tf_cfg["resolution"]

    # Fetch pool info from GeckoTerminal for market data
    gt = await fetch_geckoterminal(ca)

    if not gt or not gt.get("pool_address"):
        await send_message(channel_id, embeds=[{
            "title":       "⚠️ Token Not Found",
            "description": f"Could not find pool data for:\n```{ca}```",
            "color":       0xF59E0B,
            "fields": [{"name": "🔗 Try manually",
                        "value": f"[DexScreener](https://dexscreener.com/solana/{ca})",
                        "inline": False}],
            "footer":    {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
            "timestamp": get_timestamp(),
        }])
        return

    pool  = gt["pool_address"]
    name  = gt["name"]
    price = gt["price_usd"]
    p5m   = gt["price_change_5m"]
    p1h   = gt["price_change_1h"]
    p24h  = gt["price_change_24h"]
    color = 0x10B981 if p24h >= 0 else 0xEF4444

    total_txns = gt["buys_24h"] + gt["sells_24h"]
    buy_ratio  = (gt["buys_24h"] / total_txns * 100) if total_txns > 0 else 0

    def gt_url(r: int) -> str:
        return f"https://www.geckoterminal.com/solana/pools/{pool}?resolution={r}"

    def pct_str(pct: float) -> str:
        sign  = "+" if pct >= 0 else ""
        emoji = "🟢" if pct >= 0 else "🔴"
        return f"{emoji} {sign}{pct:.2f}%"

    # Loading message
    await send_message(channel_id, embeds=[{
        "description": f"📊 Generating **{tf_label}** chart for `{name}`…",
        "color":       0x6366F1,
    }])

    # Generate chart image
    chart_bytes = await generate_chart_image(ca, tf_clean, name)

    embed = {
        "author":      {"name": f"📊 {name} · {tf_label} Chart"},
        "title":       f"${price:.8f}",
        "url":         gt_url(res),
        "color":       color,
        "description": (
            f"[`1m`]({gt_url(1)})  ·  "
            f"[`5m`]({gt_url(5)})  ·  "
            f"[`15m`]({gt_url(15)})  ·  "
            f"[`1H`]({gt_url(60)})  ·  "
            f"[`1D`]({gt_url(1440)})"
        ),
        "fields": [
            {"name":   "📈 Price Change",
             "value":  f"```\n5m  : {pct_str(p5m)}\n1h  : {pct_str(p1h)}\n24h : {pct_str(p24h)}\n```",
             "inline": True},
            {"name":   "💧 Market Data",
             "value":  (f"```yaml\n"
                        f"Liquidity : {format_usd(gt['liquidity'])}\n"
                        f"Vol 24h   : {format_usd(gt['volume_24h'])}\n"
                        f"FDV       : {format_usd(gt['fdv'])}\n```"),
             "inline": True},
            {"name":   "🔄 24h Txns",
             "value":  (f"```yaml\n"
                        f"Buys      : {gt['buys_24h']:,}\n"
                        f"Sells     : {gt['sells_24h']:,}\n"
                        f"Buy Ratio : {buy_ratio:.1f}%\n```"),
             "inline": True},
            {"name":  "🔗 Links",
             "value": (f"[DexScreener](https://dexscreener.com/solana/{ca}) · "
                       f"[Birdeye](https://birdeye.so/token/{ca}) · "
                       f"[GeckoTerminal]({gt_url(res)})"),
             "inline": False},
        ],
        "footer":    {"text": f"Birdeye · {tf_label} · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }

    if chart_bytes:
        embed["image"] = {"url": "attachment://chart.png"}
        headers = {
            "Authorization": f"Bot {DISCORD_TOKEN}",
            "User-Agent":    "XerisBot/2.0",
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                f"{DISCORD_API}/channels/{channel_id}/messages",
                headers=headers,
                files={"file": ("chart.png", chart_bytes, "image/png")},
                data={"payload_json": json.dumps({"embeds": [embed]})},
            )
        if r.status_code not in (200, 201):
            print(f"   ❌ Discord chart upload {r.status_code}: {r.text[:150]}")
            await send_message(channel_id, embeds=[embed])
    else:
        embed["description"] += "\n\n⚠️ *Chart unavailable — click a timeframe above to view on GeckoTerminal*"
        await send_message(channel_id, embeds=[embed])
    
async def cmd_order(channel_id: int, db: DatabaseManager, ms: MarketState) -> None:
    await send_typing(channel_id)
    orders = await db.get_active_orders()
    if not orders:
        await send_message(channel_id, embeds=[{
            "author":      {"name": "📋 Limit Order Book"},
            "title":       "No Active Limit Orders",
            "description": "> No limit orders are currently being tracked for XERIS.",
            "color":       0x6B7280,
            "footer":      {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
            "timestamp":   get_timestamp(),
        }])
        return

    buys  = sorted([o for o in orders if o["order_type"] == "LIMIT_BUY"],  key=lambda x: x["predicted_mcap"])
    sells = sorted([o for o in orders if o["order_type"] == "LIMIT_SELL"], key=lambda x: x["predicted_mcap"])
    total_buy_wall  = sum(o["usd_value"] for o in buys)
    total_sell_wall = sum(o["usd_value"] for o in sells)

    await send_message(channel_id, embeds=[{
        "author":      {"name": "📋 XERIS · Live Limit Order Book"},
        "title":       f"{len(orders)} Active Order(s) Tracked",
        "description": (
            f"```yaml\n"
            f"Current Price : ${ms.current_price:.8f}\n"
            f"Current MCap  : {format_usd(ms.current_market_cap)}\n"
            f"───────────────────────────────\n"
            f"Buy  Orders   : {len(buys)}  │  Wall: {format_usd(total_buy_wall)}\n"
            f"Sell Orders   : {len(sells)}  │  Wall: {format_usd(total_sell_wall)}\n"
            f"───────────────────────────────\n"
            f"Auto-expire   : {ORDER_TTL_HOURS // 24} days after placement\n"
            f"```"
        ),
        "color":     0x8B5CF6,
        "footer":    {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])

    for label, order_list, color, wall_label, wall_total in [
        ("🛡️ BUY ORDERS (Support)",     buys,  0x10B981, "Total Buy Wall",  total_buy_wall),
        ("⚠️ SELL ORDERS (Resistance)", sells, 0xEF4444, "Total Sell Wall", total_sell_wall),
    ]:
        if not order_list:
            desc = "> 🛡️ No active buy orders." if "BUY" in label else "> ⚠️ No active sell orders."
            await send_message(channel_id, embeds=[{"description": desc, "color": 0x6B7280}])
            continue

        lines   = ""
        page    = 1
        start_i = 0
        for i, o in enumerate(order_list[:10]):
            dist        = _pct_from_current(o["predicted_mcap"], ms)
            qt          = f"[{o['quote_token']}] " if o.get("quote_token") else ""
            exch        = o.get("exchange", "")
            wallet_s    = f"{o['wallet'][:6]}…{o['wallet'][-4:]}"
            bar         = _expiry_bar(o)
            placed      = _format_time_placed(o)
            remaining   = _format_time_remaining(o)
            expires_dt  = datetime.fromtimestamp(
                o["timestamp"] + ORDER_TTL_SECS, tz=timezone.utc
            ).strftime("%m/%d %H:%M")
            status_icon = "⚠️" if "⚠️" in remaining else "✅"

            lines += (
                f"**`#{i+1}`** {qt}`{format_tokens(o['token_amount'])} XERIS` · **{format_usd(o['usd_value'])}**\n"
                f"┣ Target MCap : `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%)\n"
                f"┣ Wallet      : [`{wallet_s}`](https://solscan.io/account/{o['wallet']})"
                + (f" via `{exch}`" if exch else "") + "\n"
                f"┣ Placed      : `{placed}`\n"
                f"┣ Expires     : `{expires_dt} UTC` · `{remaining}`\n"
                f"┗ Lifetime    : `{bar}` {status_icon}\n\n"
            )

            if (i + 1) % 5 == 0 and i + 1 < len(order_list):
                await send_message(channel_id, embeds=[{
                    "author":      {"name": f"{label} · Page {page}"},
                    "description": lines,
                    "color":       color,
                    "footer":      {"text": f"Orders #{start_i+1}–#{i+1} of {len(order_list)}"},
                }])
                lines   = ""
                page   += 1
                start_i = i + 1

        if lines:
            nearest = format_usd(order_list[0]["predicted_mcap"])
            field_name = "📊 Nearest Support" if "BUY" in label else "📊 Nearest Resistance"
            await send_message(channel_id, embeds=[{
                "author":      {"name": f"{label} · {len(order_list)} total"},
                "description": lines,
                "color":       color,
                "fields": [
                    {"name": f"💰 {wall_label}", "value": f"`{format_usd(wall_total)}`", "inline": True},
                    {"name": field_name,          "value": f"`{nearest}`",               "inline": True},
                ],
                "footer":    {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
                "timestamp": get_timestamp(),
            }])


async def cmd_analyze(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    await send_message(channel_id, embeds=[{
        "title":       "🔍 Running Full Risk Scan...",
        "description": (
            f"```{ca}```\n"
            f"⛓️ Fetching on-chain data & mint authority...\n"
            f"🕵️ Scanning deployer wallet history...\n"
            f"🎪 Checking Pump.fun metadata...\n"
            f"📣 Scanning socials...\n"
            f"🤖 AI analysis via Groq LLaMA3 (15-25s)"
        ),
        "color": 0x6366F1,
    }])
    price_data, holders = await asyncio.gather(fetch_price_for_ca(ca), fetch_top_holders(ca))
    if not price_data:
        await send_message(channel_id, embeds=[{
            "title": "❌ Token Not Found", "description": f"No market data for:\n```{ca}```", "color": 0xEF4444,
        }])
        return
    name         = price_data.get("name", "Unknown")
    symbol       = price_data.get("symbol", "???")
    intelligence = await fetch_all_intelligence(ca, name, symbol)
    socials      = intelligence.get("socials", {})
    token_meta   = intelligence.get("token_meta", {})
    pumpfun      = intelligence.get("pumpfun", {})
    deployer     = intelligence.get("deployer", {})
    ai = await groq_analyze(ca, price_data, holders, intelligence)
    if "error" in ai:
        await send_message(channel_id, embeds=[{
            "title": "❌ AI Analysis Failed", "description": f"```{ai['error']}```", "color": 0xEF4444,
        }])
        return
    score       = int(ai.get("risk_score", 5))
    rug_label   = ai.get("rug_label", "UNKNOWN")
    summary     = ai.get("summary", "No summary.")
    team_ana    = ai.get("team_analysis", "N/A")
    red_flags   = ai.get("red_flags", [])
    green_flags = ai.get("green_flags", [])
    holder_ana  = ai.get("holder_analysis", "N/A")
    liq_ana     = ai.get("liquidity_analysis", "N/A")
    social_ana  = ai.get("social_analysis", "N/A")
    mint_risk   = ai.get("mint_freeze_risk", "N/A")
    trade_adv   = ai.get("trade_advice", "N/A")
    color       = score_to_color(score)
    rug_emoji   = rug_label_emoji(rug_label)
    liq         = price_data.get("liquidity", 0)
    vol         = price_data.get("volume_24h", 0)
    mcap        = price_data.get("mcap", 0)
    vol_liq     = (vol / liq  * 100) if liq  > 0 else 0
    vol_mc      = (vol / mcap * 100) if mcap > 0 else 0
    change      = price_data.get("change_24h", 0)
    total_supply = 1_000_000_000
    top5_pct    = sum(float(h.get("uiAmount",0)) for h in holders[:5])  / total_supply * 100 if holders else 0
    top10_pct   = sum(float(h.get("uiAmount",0)) for h in holders[:10]) / total_supply * 100 if holders else 0
    dep_wallet  = deployer.get("wallet") or token_meta.get("mint_authority") or "Unknown"
    dep_short   = f"{dep_wallet[:8]}...{dep_wallet[-6:]}" if dep_wallet and len(dep_wallet) > 14 else dep_wallet
    dep_age     = deployer.get("wallet_age_days")
    dep_age_str = f"{dep_age}d old" if dep_age else "Unknown"
    launchpad   = "Pump.fun 🎪" if pumpfun.get("is_pumpfun") else "Direct Deploy"
    graduated   = "✅ Graduated" if pumpfun.get("graduated") else ("❌ Not graduated" if pumpfun.get("is_pumpfun") else "N/A")
    mint_auth   = "✅ Revoked" if not token_meta.get("mint_authority") else "⚠️ Active"
    freeze_auth = "✅ Revoked" if not token_meta.get("freeze_authority") else "⚠️ Active"
    token_age   = f"{token_meta.get('token_age_days', '?')} days" if token_meta.get("token_age_days") is not None else "Unknown"
    await send_message(channel_id, embeds=[{
        "author":      {"name": f"🛡️ AI Risk Report · {name} ({symbol})"},
        "title":       f"`{ca[:20]}...{ca[-8:]}`",
        "description": f"> {summary}",
        "color":       color,
        "fields": [
            {"name": f"{rug_emoji} Risk Score", "value": risk_score_bar(score),          "inline": True},
            {"name": "⚠️ Verdict",              "value": f"**{rug_emoji} {rug_label}**", "inline": True},
            {"name": "📅 Token Age",            "value": f"`{token_age}`",               "inline": True},
            {"name": "📊 Market Data",
             "value": (f"```yaml\n"
                       f"Price:    ${price_data['price']:.8f}\n"
                       f"MCap:     {format_usd(mcap)}\n"
                       f"Vol 24h:  {format_usd(vol)}\n"
                       f"Liq:      {format_usd(liq)}\n"
                       f"24h Chg:  {change:+.2f}%\n"
                       f"Vol/Liq:  {vol_liq:.1f}%\n"
                       f"Vol/MCap: {vol_mc:.1f}%\n```"),
             "inline": True},
            {"name": "🏗️ Team & Launch",
             "value": (f"```yaml\n"
                       f"Launchpad:  {launchpad}\n"
                       f"Graduated:  {graduated}\n"
                       f"Deployer:   {dep_short}\n"
                       f"Wallet Age: {dep_age_str}\n"
                       f"Mint Auth:  {mint_auth}\n"
                       f"Freeze:     {freeze_auth}\n```"),
             "inline": True},
            {"name": "📣 Socials & Holders",
             "value": (f"```yaml\n"
                       f"Twitter:  {socials.get('twitter_handle') or 'NOT FOUND'}\n"
                       f"Telegram: {socials.get('telegram') or 'NOT FOUND'}\n"
                       f"Website:  {'Found ✅' if socials.get('website') else 'Not Found ❌'}\n"
                       f"Top 5:    {top5_pct:.1f}% of supply\n"
                       f"Top 10:   {top10_pct:.1f}% of supply\n```"),
             "inline": True},
            {"name": "🕵️ Team / Backer Analysis",  "value": team_ana[:1020],  "inline": False},
            {"name": "🔴 Red Flags",
             "value": "\n".join(f"• {f}" for f in red_flags) if red_flags else "• None detected",
             "inline": False},
            {"name": "🟢 Green Signals",
             "value": "\n".join(f"• {f}" for f in green_flags) if green_flags else "• None detected",
             "inline": False},
            {"name": "🐳 Holder Concentration", "value": holder_ana[:1020], "inline": False},
            {"name": "💧 Liquidity Risk",        "value": liq_ana[:1020],   "inline": False},
            {"name": "🔒 Mint & Freeze",         "value": mint_risk[:1020], "inline": False},
            {"name": "📡 Social Legitimacy",     "value": social_ana[:1020],"inline": False},
            {"name": "💡 Trade Advice",          "value": f"> {trade_adv[:1020]}", "inline": False},
            {"name": "🔗 Verify",
             "value": (
                 f"[DexScreener](https://dexscreener.com/solana/{ca}) · "
                 f"[Solscan](https://solscan.io/token/{ca}) · "
                 f"[Birdeye](https://birdeye.so/token/{ca})"
                 + (f" · [Deployer](https://solscan.io/account/{dep_wallet})" if dep_wallet and dep_wallet != "Unknown" else "")
                 + (f" · [Twitter]({socials['twitter']})" if socials.get("twitter") else "")
                 + (f" · [Website]({socials['website']})" if socials.get("website") else "")
                 + (f" · [Pump.fun](https://pump.fun/{ca})" if pumpfun.get("is_pumpfun") else "")
             ),
             "inline": False},
        ],
        "footer":    {"text": f"Powered by Groq LLaMA-3.3-70B · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])


async def cmd_help(channel_id: int) -> None:
    await send_message(channel_id, embeds=[{
        "author":      {"name": "🤖 XerisBot — Command Reference"},
        "color":       0x6366F1,
        "description": "All commands use `!` prefix. For any Solana token, pass its contract address (CA).",
        "fields": [
            {"name": "📈 !price <CA>",   "value": "Price, market cap, 24h volume & change, liquidity",                      "inline": False},
            {"name": "🐳 !whale <CA>",   "value": "Top 15 holders with concentration risk rating",                           "inline": False},
            {"name": "📊 !chart <CA> [tf]", "value": "Live chart screenshot from GeckoTerminal · Timeframes: `1m` `5m` `15m` `1h` `1d` · Example: `!chart <CA> 1m`", "inline": False},
            {"name": "🛡️ !analyze <CA>", "value": "Full AI risk analysis: price + holders + socials → Groq LLaMA3 verdict", "inline": False},
            {"name": "📋 !order",
             "value": (
                 "Full live limit order book with:\n"
                 "› Placed time & exact expiry date\n"
                 "› Time remaining before auto-removal\n"
                 "› Lifetime bar (drains over 7 days) ✅ → ⚠️\n"
                 "› Target mcap, % distance, exchange\n"
                 "› Buy walls (support) & sell walls (resistance)"
             ),
             "inline": False},
            {"name": "❓ !help", "value": "Show this menu", "inline": False},
            {"name": "⚡ Auto-Monitor (background)",
             "value": (
                 f"Watching: `{MINT[:16]}...`\n"
                 f"• Whale alerts ≥ ${WHALE_MIN_USD:,} USD\n"
                 f"• Dev wallet sell alerts\n"
                 f"• Limit order detection & tracking (7-day TTL)\n"
                 f"• Unknown program logging → `unknown_programs.jsonl`\n"
                 f"• Price alerts ≥ ±{PRICE_ALERT_THRESHOLD}%\n"
                 f"• 10-min limit order book summaries\n"
                 f"• Hourly DB cleanup (removes expired orders)"
             ),
             "inline": False},
        ],
        "footer":    {"text": "XerisBot · Powered by Helius + DexScreener + Groq"},
        "timestamp": get_timestamp(),
    }])


_db_ref: Optional[DatabaseManager] = None
_ms_ref: Optional[MarketState]     = None


async def handle_message(msg: dict) -> None:
    content    = (msg.get("content") or "").strip()
    channel_id = int(msg.get("channel_id", 0))
    author     = msg.get("author", {})
    if author.get("bot"):
        return
    if not content.startswith("!"):
        return
    parts   = content.split()
    command = parts[0].lower()
    arg     = parts[1].strip() if len(parts) > 1 else None
    print(f"\n💬 {content} | ch:{channel_id} | {author.get('username')}")

    if command == "!help":
        await cmd_help(channel_id)
    elif command == "!price":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content="❌ Usage: `!price <contract_address>`")
            return
        await cmd_price(channel_id, arg)
    elif command == "!whale":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content="❌ Usage: `!whale <contract_address>`")
            return
        await cmd_whale(channel_id, arg)
    elif command == "!chart":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content=(
                "❌ Usage: `!chart <contract_address> [timeframe]`\n"
                "Timeframes: `1m` `5m` `15m` `1h` `1d`\n"
                "Example: `!chart 9ezFth...pump 1m`"
            ))
            return
        # Third part is optional timeframe, default 15m
        tf = parts[2].lower() if len(parts) > 2 else "15m"
        if tf not in TIMEFRAME_MAP:
            tf = "15m"
        await cmd_chart(channel_id, arg, timeframe=tf)
    elif command == "!analyze":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content=(
                "❌ Usage: `!analyze <contract_address>`\n"
                f"Example: `!analyze 9ezFthWrDUpSSeMdpLW6SDD9TJigHdc4AuQ5QN5bpump`"
            ))
            return
        await cmd_analyze(channel_id, arg)
    elif command in ("!order", "!orders"):
        if _db_ref and _ms_ref:
            await cmd_order(channel_id, _db_ref, _ms_ref)
        else:
            await send_message(channel_id, content="❌ Order tracker not initialized yet.")


async def discord_gateway() -> None:
    heartbeat_interval = None
    sequence           = None
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
                    data = json.loads(raw)
                    op   = data.get("op")
                    t    = data.get("t")
                    d    = data.get("d") or {}
                    s    = data.get("s")
                    if s:
                        sequence = s
                    if op == 10:
                        heartbeat_interval = d["heartbeat_interval"]
                        heartbeat_task     = asyncio.create_task(send_heartbeat())
                        await ws.send(json.dumps({
                            "op": 2,
                            "d": {
                                "token":      DISCORD_TOKEN,
                                "intents":    33280,
                                "properties": {"$os": "linux", "$browser": "xerisbot", "$device": "xerisbot"},
                            },
                        }))
                        print("✅ Discord Gateway identified")
                    elif t == "READY":
                        user = d.get("user", {})
                        print(f"✅ Logged in as {user.get('username')}#{user.get('discriminator')}")
                    elif t == "MESSAGE_CREATE":
                        await handle_message(d)
        except Exception as e:
            print(f"❌ Gateway error: {e} — reconnecting in 5s...")
            await asyncio.sleep(5)


async def helius_monitor(db: DatabaseManager, ms: MarketState) -> None:
    global _db_ref, _ms_ref
    _db_ref = db
    _ms_ref = ms
    classifier    = TransactionClassifier()
    tracker       = OrderTracker(db, classifier, ms)
    alert_manager = AlertManager(db, ms)
    print("\n🔭 Starting Helius monitor...")
    await update_price(ms)
    retry_count = 0
    tx_count    = 0
    while True:
        try:
            print(f"\n📡 Helius WS connecting (attempt {retry_count+1})...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=60) as ws:
                print("✅ Helius WebSocket connected")
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                    "params": [{"mentions": [MINT]}, {"commitment": "confirmed"}],
                }))
                print(f"✅ Subscribed to {MINT[:16]}...")
                retry_count = 0
                while True:
                    try:
                        if time.time() - ms.last_price_update > PRICE_UPDATE_INTERVAL:
                            await update_price(ms)
                        await alert_manager.tick()
                        msg  = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)
                        if "params" not in data:
                            continue
                        signature = data["params"]["result"]["value"]["signature"]
                        tx_count += 1
                        print(f"\n{'─'*50}")
                        print(f"TX #{tx_count}  {signature[:24]}…  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
                        tx_data = await fetch_tx(signature, retries=3)
                        if not tx_data:
                            continue
                        result = await tracker.process(tx_data, signature)
                        if not result:
                            continue
                        action = result["action"]

                        if action == "new_limit":
                            order: LimitOrder = result["order"]
                            info  = result["info"]
                            qt    = info.get("quote_token", "")
                            exch  = info.get("exchange", "")
                            role  = "SUPPORT" if order.order_type == OrderType.LIMIT_BUY else "RESISTANCE"
                            print(f"  📌 {order.order_type.value}  {format_tokens(order.token_amount)} XERIS"
                                  + (f" ↔ {qt}" if qt else "")
                                  + f"  {format_usd(order.usd_value)}  → {role} @ {format_usd(order.predicted_mcap)} mcap")
                            if order.usd_value >= WHALE_MIN_USD:
                                await send_message(ALERT_CHANNEL_ID,
                                                   embeds=[_build_limit_order_embed(order, ms, qt, exch)])

                        elif action == "cancel_limit":
                            cancelled = result.get("cancelled")
                            if cancelled:
                                print(f"  🗑️  CANCEL  {cancelled['order_type']}  {format_usd(cancelled['usd_value'])}")

                        elif action == "market":
                            tx_type: OrderType = result["type"]
                            info:    dict       = result["info"]
                            fills:   list       = result.get("fills", [])
                            usd_val = info.get("usd_value", info["amount"] * ms.current_price)
                            side    = tx_type.value.replace("MARKET_", "")
                            qt      = info.get("quote_token", "")
                            exch    = info.get("exchange", "")

                            if info["amount"] <= 0 or usd_val <= 0:
                                print(f"  ⏭️  {tx_type.value} skipped (0 XERIS / $0.00)")
                                continue

                            print(f"  💱 {tx_type.value}  {format_tokens(info['amount'])} XERIS"
                                  + (f" ↔ {qt}" if qt else "")
                                  + f"  {format_usd(usd_val)}"
                                  + (f"  [{exch}]" if exch else "")
                                  + (f"  → FILLED {len(fills)} order(s)" if fills else ""))

                            wallet = info.get("wallet", "")
                            if wallet == DEV_WALLET and "SELL" in tx_type.value:
                                print("🚨 DEV SELL!")
                                await send_message(ALERT_CHANNEL_ID,
                                                   embeds=[_build_dev_sell_embed(info["amount"], wallet, usd_val, signature, ms, qt)])
                            elif usd_val >= WHALE_MIN_USD:
                                print(f"🐋 WHALE {tx_type.value} — {format_usd(usd_val)}")
                                await send_message(ALERT_CHANNEL_ID,
                                                   embeds=[_build_whale_embed(side, info["amount"], wallet, usd_val, signature, ms, qt, exch)],
                                                   mention_everyone=True)

                        elif action == "transfer":
                            info = result["info"]
                            print(f"  ↗️  TRANSFER  {format_tokens(info['amount'])} XERIS  → {info.get('to', '?')[:12]}…")

                    except asyncio.TimeoutError:
                        await ws.ping()
                    except websockets.exceptions.ConnectionClosed:
                        print("⚠️ Helius WS closed")
                        break
                    except Exception as e:
                        print(f"❌ {e}")
        except Exception as e:
            retry_count += 1
            wait = min(30 * retry_count, 300)
            print(f"❌ Helius error: {e} — retry in {wait}s")
            await asyncio.sleep(wait)


async def announce_startup() -> None:
    await asyncio.sleep(3)
    await send_message(ALERT_CHANNEL_ID, embeds=[{
        "author": {"name": "XerisBot — System Online"},
        "title":  "🛰️ Bot Started · All Systems Active",
        "description": (
            "```\n"
            "╔══════════════════════════════════════╗\n"
            "║  REAL-TIME MONITORING ACTIVE        ║\n"
            "║  • Whale & Dev Activity Tracking    ║\n"
            "║  • Limit Order Detection (8 DEXes)  ║\n"
            "║  • Any-Token-Pair Detection         ║\n"
            "║  • 7-Day Order Expiry Timer         ║\n"
            "║  • Hourly DB Cleanup                ║\n"
            "║  • Unknown Program Logging          ║\n"
            "║  • Price Movement Alerts            ║\n"
            "║  • AI Risk Analysis (!analyze)      ║\n"
            "╚══════════════════════════════════════╝\n"
            "```\n"
            "> Type `!help` to see all commands"
        ),
        "color":  0x10B981,
        "fields": [
            {"name": "🐋 Whale Threshold", "value": f"`${WHALE_MIN_USD:,} USD`",         "inline": True},
            {"name": "📈 Price Alert",     "value": f"`±{PRICE_ALERT_THRESHOLD}%`",       "inline": True},
            {"name": "🤖 AI Engine",       "value": f"`Groq {GROQ_MODEL}`",               "inline": True},
            {"name": "⏳ Order TTL",       "value": f"`{ORDER_TTL_HOURS // 24} days`",    "inline": True},
            {"name": "🧹 DB Cleanup",      "value": f"`Every {CLEANUP_INTERVAL//3600}h`", "inline": True},
            {"name": "🎯 Monitored Token", "value": f"`{MINT}`", "inline": False},
        ],
        "footer":    {"text": f"Started at {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])


async def main() -> None:
    print("\n" + "=" * 62)
    print("  🚀  XERISBOT — DISCORD BOT + THREE-TIER MONITOR")
    print("=" * 62)
    print(f"  Mint        {MINT}")
    print(f"  Dev         {DEV_WALLET}")
    print(f"  Whale ≥     ${WHALE_MIN_USD:,}")
    print(f"  Channel     {ALERT_CHANNEL_ID}")
    print(f"  Debug Ch    {DEBUG_CHANNEL_ID or 'disabled'}")
    print(f"  Groq        {'✅ ' + GROQ_MODEL if GROQ_ENABLED else '❌ disabled'}")
    print(f"  Suspicion ≥ {SUSPICION_THRESHOLD}")
    print(f"  Order TTL   {ORDER_TTL_HOURS}h ({ORDER_TTL_HOURS // 24} days)")
    print(f"  DB Cleanup  every {CLEANUP_INTERVAL // 3600}h")
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
            announce_startup(),
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n👋 Shutting down…")
    finally:
        await db.close()
        if _discord_queue:
            await _discord_queue.stop()
        print("✅ Clean shutdown")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped")
    except Exception as e:
        import traceback
        print(f"❌ Fatal: {e}")
        traceback.print_exc()
