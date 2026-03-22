from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from config.settings import ORDER_TTL_SECS


# ── Timestamps ────────────────────────────────────────────────────────────────

def get_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Number formatters ─────────────────────────────────────────────────────────

def format_usd(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:,.2f}"


def format_tokens(amount: float) -> str:
    if amount >= 1_000_000:
        return f"{amount / 1_000_000:.2f}M"
    if amount >= 1_000:
        return f"{amount / 1_000:.2f}K"
    return f"{amount:,.0f}"


# ── Risk display ──────────────────────────────────────────────────────────────

def risk_score_bar(score: int) -> str:
    bar = "█" * score + "░" * (10 - score)
    return f"`{bar}` **{score}/10**"


def rug_label_emoji(label: str) -> str:
    u = label.upper()
    if "LIKELY SAFE" in u:
        return "🟢"
    if "CAUTION" in u:
        return "🟡"
    if "HIGH RISK" in u:
        return "🔴"
    if "LIKELY RUG" in u:
        return "💀"
    return "⚪"


def score_to_color(score: int) -> int:
    if score <= 3:
        return 0x10B981
    if score <= 5:
        return 0xF59E0B
    if score <= 7:
        return 0xF97316
    return 0xEF4444


# ── Market helpers ────────────────────────────────────────────────────────────

def _pct_from_current(mcap: float, current_market_cap: float) -> float:
    if current_market_cap > 0 and mcap > 0:
        return (mcap - current_market_cap) / current_market_cap * 100
    return 0.0


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


# ── Order time formatters ─────────────────────────────────────────────────────

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
