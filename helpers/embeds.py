from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from config.settings import MINT, ORDER_TTL_SECS, PRICE_ALERT_THRESHOLD
from core.models import LimitOrder, MarketState, OrderType
from helpers.formatters import (
    _expiry_bar, _format_placed_at, _format_time_remaining,
    _pct_from_current, format_tokens, format_usd, get_timestamp,
)
from helpers.tx_utils import estimate_mcap_before_after_any_quote


# ── Limit order ────────────────────────────────────────────────────────────────

def _build_limit_order_embed(
    order: LimitOrder,
    ms: MarketState,
    quote_token: str = "",
    exchange: str = "",
) -> dict:
    is_buy    = order.order_type == OrderType.LIMIT_BUY
    color     = 0x10B981 if is_buy else 0xEF4444
    direction = "BUY" if is_buy else "SELL"
    dist      = _pct_from_current(order.predicted_mcap, ms.current_market_cap) if order.predicted_mcap > 0 else 0.0
    order_dict = {"timestamp": order.timestamp}
    placed_at  = datetime.fromtimestamp(order.timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    expires_at = datetime.fromtimestamp(order.timestamp + ORDER_TTL_SECS, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pair_label = (
        f"{quote_token} → XERIS" if is_buy and quote_token
        else f"XERIS → {quote_token}" if not is_buy and quote_token
        else "XERIS"
    )
    return {
        "author":      {"name": f"🎯 NEW LIMIT {'BUY' if is_buy else 'SELL'} DETECTED"},
        "title":       f"{'📈' if is_buy else '📉'} Limit {direction} · {format_usd(order.usd_value)}",
        "description": (
            f"```yaml\nType:         LIMIT {direction}\nPair:         {pair_label}\n"
            f"Size:         {format_tokens(order.token_amount)} XERIS\nValue:        {format_usd(order.usd_value)}\n"
            f"Target MCap:  {format_usd(order.predicted_mcap) if order.predicted_mcap > 0 else 'Unknown'}\n"
            f"Distance:     {f'{dist:+.2f}% from current' if order.predicted_mcap > 0 else 'Unknown'}\n"
            f"Role:         {'Support Level' if is_buy else 'Resistance Level'}\n"
            f"Placed At:    {placed_at}\n"
            + (f"Exchange:     {exchange}\n" if exchange else "") + "```"
        ),
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
             "inline": False},
        ],
        "footer":    {"text": f"Limit Order Tracker · expires in 7 days · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }


# ── Whale ──────────────────────────────────────────────────────────────────────

def _build_whale_embed(
    tx_type: str,
    amount: float,
    wallet: str,
    usd_value: float,
    signature: str,
    ms: MarketState,
    quote_token: str = "",
    exchange: str = "",
) -> dict:
    is_buy = tx_type == "BUY"
    color  = 0x10B981 if is_buy else 0xEF4444

    snap = estimate_mcap_before_after_any_quote(
        current_mcap=ms.current_market_cap,
        usd_value=usd_value,
        pool_quote_reserve_after=ms.pool_quote_reserve,
        is_buy=is_buy,
        ms=ms,
    )

    tier = (
        "💎 MEGA WHALE" if usd_value >= 50_000 else
        "🌊 WHALE"      if usd_value >= 10_000 else
        "⭐ BIG FISH"   if usd_value >= 5_000  else
        "💫 FISH"
    )
    pair_label = (
        f"{quote_token} → XERIS" if is_buy and quote_token
        else f"XERIS → {quote_token}" if not is_buy and quote_token
        else f"XERIS {'bought' if is_buy else 'sold'}"
    )

    return {
        "author": {"name": f"{tier} DETECTED"},
        "title":  f"{'📈' if is_buy else '📉'} {tx_type} · {format_usd(usd_value)}",
        "description": (
            f"```yaml\nPair:   {pair_label}\nTrade:  {format_tokens(amount)} XERIS\n"
            f"USD:    {format_usd(usd_value)}\nImpact: {snap['change_pct']:+.2f}%\n"
            + (f"Via:    {exchange}\n" if exchange else "") + "```"
        ),
        "color":  color,
        "fields": [
            {"name": "💰 Market Metrics",
             "value": (
                 f"┌ Price: `${ms.current_price:.8f}`\n"
                 f"├ Before MCap: `{format_usd(snap['before_mcap']) if snap['before_mcap'] > 0 else 'N/A'}`\n"
                 f"├ After MCap: `{format_usd(snap['after_mcap'])}`\n"
                 f"└ Change: `{'+' if snap['change_usd'] >= 0 else ''}{format_usd(snap['change_usd'])}` ({snap['change_pct']:+.2f}%)"
             ), "inline": False},
            {"name": "🏊 Quote Math",
             "value": (
                 f"┌ Quote Token: `{ms.quote_symbol or '?'}`\n"
                 f"├ Quote/USD: `{snap['quote_price_usd']:.8f}`\n"
                 f"├ Trade Quote Amt: `{snap['quote_amount']:,.6f}`\n"
                 f"└ Quote Reserve After: `{ms.pool_quote_reserve:,.6f}`"
             ), "inline": False},
            {"name": "👤 Wallet",    "value": f"```{wallet}```", "inline": False},
            {"name": "🔗 Links",
             "value": f"[TX](https://solscan.io/tx/{signature}) · [Wallet](https://solscan.io/account/{wallet}) · [Chart](https://dexscreener.com/solana/{MINT})",
             "inline": False},
        ],
        "footer":    {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }


# ── Dev sell ───────────────────────────────────────────────────────────────────

def _build_dev_sell_embed(
    amount: float,
    wallet: str,
    usd_value: float,
    signature: str,
    ms: MarketState,
    quote_token: str = "",
) -> dict:
    snap   = estimate_mcap_before_after_any_quote(
        current_mcap=ms.current_market_cap,
        usd_value=usd_value,
        pool_quote_reserve_after=ms.pool_quote_reserve,
        is_buy=False,
        ms=ms,
    )
    impact = abs(snap["change_pct"])

    return {
        "author": {"name": "⚠️ DEVELOPER ACTIVITY ALERT"},
        "title":  "🚨 Dev Wallet Sell Detected",
        "description": (
            f"```diff\n- Developer has executed a SELL transaction\n```\n"
            f"**⚠️ Monitor price action closely**\n"
            f"> Amount: **{format_usd(usd_value)}** ({impact:.2f}% estimated mcap move)"
        ),
        "color":  0xDC2626,
        "fields": [
            {"name": "💸 Details",
             "value": (
                 f"```yaml\nPair:   XERIS → {quote_token or ms.quote_symbol or '?'}\n"
                 f"Tokens: {format_tokens(amount)} XERIS\nUSD:    {format_usd(usd_value)}\n"
                 f"Impact: {impact:.2f}%\n```"
             ), "inline": False},
            {"name": "📊 MCap Impact",
             "value": (
                 f"┌ Before: `{format_usd(snap['before_mcap']) if snap['before_mcap'] > 0 else 'N/A'}`\n"
                 f"├ After:  `{format_usd(snap['after_mcap'])}`\n"
                 f"└ Change: `{'+' if snap['change_usd'] >= 0 else ''}{format_usd(snap['change_usd'])}`"
             ), "inline": False},
            {"name": "👤 Dev Wallet", "value": f"```{wallet}```", "inline": False},
            {"name": "🔍 Links",
             "value": f"[TX](https://solscan.io/tx/{signature}) · [Wallet](https://solscan.io/account/{wallet}) · [Chart](https://dexscreener.com/solana/{MINT})",
             "inline": False},
        ],
        "footer":    {"text": f"Dev Monitor · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }


# ── Price alert ────────────────────────────────────────────────────────────────

def _build_price_embed(pct: float, ref: float, ms: MarketState) -> dict:
    is_pump  = pct > 0; sign = "+" if is_pump else ""
    abs_pct  = abs(pct); color = 0x10B981 if is_pump else 0xEF4444
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
             "value": f"```yaml\nNow: {format_usd(ms.current_market_cap)}\nΔ:   {'+' if mcap_chg >= 0 else ''}{format_usd(abs(mcap_chg))}\n```",
             "inline": True},
            {"name": "📈 Momentum", "value": f"`{bars}` **{abs_pct:.1f}%**", "inline": False},
            {"name": "🔗 Charts",
             "value": f"[DexScreener](https://dexscreener.com/solana/{MINT}) · [Birdeye](https://birdeye.so/token/{MINT})",
             "inline": False},
        ],
        "footer":    {"text": f"Threshold ±{PRICE_ALERT_THRESHOLD}% · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }


# ── Cleanup / cancel / fill ────────────────────────────────────────────────────

def _embed_cleanup(count: int) -> dict:
    return {
        "author":      {"name": "🧹 Order Book Cleanup"},
        "title":       f"Removed {count} expired limit order(s)",
        "description": f"```yaml\nExpired After : 7 days\nOrders Removed: {count}\n```",
        "color":       0x6B7280,
        "footer":      {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp":   get_timestamp(),
    }


def _embed_cancelled(wallet: str, order: Dict, sig: str) -> dict:
    return {
        "author":      {"name": "❌ LIMIT ORDER CANCELLED"},
        "title":       f"🗑️ {order['order_type']} Cancelled",
        "description": (
            f"```yaml\nWallet:   {wallet[:8]}...{wallet[-8:]}\n"
            f"Size:     {format_tokens(order['token_amount'])} XERIS\n"
            f"Value:    {format_usd(order['usd_value'])}\n"
            f"Target:   {format_usd(order['predicted_mcap'])} mcap\n"
            f"Placed:   {_format_placed_at(order)}\n```"
        ),
        "color":  0x9CA3AF,
        "fields": [{"name": "🔗 Transaction", "value": f"[Solscan](https://solscan.io/tx/{sig})", "inline": False}],
        "footer": {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }


def _embed_filled(order: Dict, fill_type: OrderType, sig: str, ms: MarketState) -> dict:
    return {
        "author": {"name": "✅ LIMIT ORDER FILLED"},
        "title":  f"💹 {order['order_type']} Executed",
        "description": (
            f"```yaml\nSize:     {format_tokens(order['token_amount'])} XERIS\n"
            f"Value:    {format_usd(order['usd_value'])}\n"
            f"Wallet:   {order['wallet'][:8]}...{order['wallet'][-8:]}\n```\n"
            f"> Filled by a **{fill_type.value}** market order."
        ),
        "color":  0x10B981,
        "fields": [
            {"name": "📊 Levels",
             "value": f"┌ Predicted MCap: `{format_usd(order['predicted_mcap'])}`\n└ Current MCap:   `{format_usd(ms.current_market_cap)}`",
             "inline": False},
            {"name": "🔗 Links",
             "value": f"[Original](https://solscan.io/tx/{order['signature']}) · [Fill Tx](https://solscan.io/tx/{sig})",
             "inline": False},
        ],
        "footer":    {"text": f"Order Tracker · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }
