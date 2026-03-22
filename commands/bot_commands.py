from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from config.settings import (
    ALERT_CHANNEL_ID, CHART_COOLDOWN_SECONDS, CHART_WAIT_MESSAGE_DELETE_SECONDS,
    DISCORD_API, DISCORD_TOKEN, MINT, VALID_CA,
)
from core.models import MarketState
from helpers.database import DatabaseManager
from helpers.discord_utils import send_message, send_temp_message, send_typing
from helpers.embeds import _build_limit_order_embed
from helpers.formatters import (
    _expiry_bar, _format_time_placed, _format_time_remaining,
    _pct_from_current, format_tokens, format_usd, get_timestamp, rug_label_emoji,
    risk_score_bar, score_to_color,
)
from helpers.rpc import (
    TIMEFRAME_MAP, fetch_all_intelligence, fetch_geckoterminal,
    fetch_price_for_ca, fetch_top_holders, generate_chart_image, groq_analyze,
)

# ── Chart cooldown state (per channel) ───────────────────────────────────────
_chart_cooldowns:    Dict[str, float]        = {}
_chart_pending_jobs: Dict[str, asyncio.Task] = {}


def _chart_job_key(channel_id: int, ca: str, timeframe: str) -> str:
    return f"{channel_id}:{ca}:{timeframe}"


def _chart_remaining_seconds(channel_id: int) -> int:
    return max(0, int(_chart_cooldowns.get(str(channel_id), 0.0) - time.time()))


def _set_chart_cooldown(channel_id: int) -> None:
    _chart_cooldowns[str(channel_id)] = time.time() + CHART_COOLDOWN_SECONDS


# ═════════════════════════════════════════════════════════════════════════════
# !price
# ═════════════════════════════════════════════════════════════════════════════

async def cmd_price(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    data = await fetch_price_for_ca(ca)
    if not data:
        await send_message(channel_id, embeds=[{
            "title": "❌ Token Not Found",
            "description": f"No data found for:\n```{ca}```",
            "color": 0xEF4444,
        }])
        return
    change   = data.get("change_24h", 0); sign = "+" if change >= 0 else ""
    color    = 0x10B981 if change >= 0 else 0xEF4444
    vol_mcap = (data["volume_24h"] / data["mcap"] * 100) if data.get("mcap") else 0
    await send_message(channel_id, embeds=[{
        "author": {"name": f"💰 Price Info · {data['name']} ({data['symbol']})"},
        "color":  color,
        "fields": [
            {"name": "📊 Market Data",
             "value": (
                 f"```yaml\nPrice:     ${data['price']:.8f}\n24h Chg:   {sign}{change:.2f}%\n"
                 f"MCap:      {format_usd(data['mcap'])}\nVol (24h): {format_usd(data['volume_24h'])}\n"
                 f"V/MC:      {vol_mcap:.2f}%\nLiquidity: {format_usd(data['liquidity'])}\nDEX: {data['dex'].upper()}\n```"
             ), "inline": False},
            {"name": "🔗 Charts",
             "value": (
                 f"[DexScreener](https://dexscreener.com/solana/{ca}) · "
                 f"[Birdeye](https://birdeye.so/token/{ca}) · [Solscan](https://solscan.io/token/{ca})"
             ), "inline": False},
        ],
        "footer":    {"text": f"Via DexScreener · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])


# ═════════════════════════════════════════════════════════════════════════════
# !whale
# ═════════════════════════════════════════════════════════════════════════════

async def cmd_whale(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    holders, price_data = await asyncio.gather(fetch_top_holders(ca), fetch_price_for_ca(ca))
    if not holders:
        await send_message(channel_id, embeds=[{
            "title": "❌ No Holder Data",
            "description": f"Could not fetch holders for:\n```{ca}```",
            "color": 0xEF4444,
        }])
        return
    total = 1_000_000_000; rows = []
    for i, h in enumerate(holders[:15]):
        amt   = float(h.get("uiAmount") or 0); pct = (amt / total) * 100
        addr  = h.get("address", "???"); short = f"{addr[:6]}...{addr[-6:]}"
        bar   = "█" * max(1, round(pct)) + "░" * max(0, 10 - round(pct))
        rows.append(f"#{i+1:>2}  {short}  {pct:5.2f}%  {bar}")
    top10_pct = sum(float(h.get("uiAmount", 0)) / total * 100 for h in holders[:10])
    top5_pct  = sum(float(h.get("uiAmount", 0)) / total * 100 for h in holders[:5])
    risk_color = 0xEF4444 if top10_pct > 50 else (0xF59E0B if top10_pct > 30 else 0x10B981)
    name   = price_data.get("name",   "Unknown") if price_data else "Unknown"
    symbol = price_data.get("symbol", "???")     if price_data else "???"
    await send_message(channel_id, embeds=[{
        "author":      {"name": f"🐳 Top Holders · {name} ({symbol})"},
        "color":       risk_color,
        "description": f"```\nRank  Wallet            Share  Bar\n{'─'*42}\n" + "\n".join(rows) + "\n```",
        "fields": [
            {"name": "📊 Concentration",
             "value": (
                 f"```yaml\nTop 5:  {top5_pct:.2f}%\nTop 10: {top10_pct:.2f}%\n"
                 f"Risk:   {'🔴 HIGH' if top10_pct > 50 else ('🟡 MEDIUM' if top10_pct > 30 else '🟢 LOW')}\n```"
             ), "inline": False},
            {"name": "🔍 Explore",
             "value": f"[Solscan Holders](https://solscan.io/token/{ca}#holders) · [Birdeye](https://birdeye.so/token/{ca})",
             "inline": False},
        ],
        "footer":    {"text": f"Via Helius RPC · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])


# ═════════════════════════════════════════════════════════════════════════════
# !chart
# ═════════════════════════════════════════════════════════════════════════════

async def cmd_chart(
    channel_id: int, ca: str, timeframe: str = "15m", bypass_cooldown: bool = False
) -> None:
    tf_clean = timeframe.lower().strip()
    if tf_clean not in TIMEFRAME_MAP:
        tf_clean = "15m"

    job_key = _chart_job_key(channel_id, ca, tf_clean)

    if not bypass_cooldown:
        remaining = _chart_remaining_seconds(channel_id)
        if remaining > 0:
            existing = _chart_pending_jobs.get(job_key)
            if existing and not existing.done():
                await send_temp_message(
                    channel_id,
                    content=f"⏳ Chart on cooldown. **{tf_clean}** request already queued. Wait **{remaining}s**.",
                    delete_after=CHART_WAIT_MESSAGE_DELETE_SECONDS,
                )
                return
            await send_temp_message(
                channel_id,
                content=f"⏳ Chart on cooldown. Queued **{tf_clean}** — auto-sends in **{remaining}s**.",
                delete_after=CHART_WAIT_MESSAGE_DELETE_SECONDS,
            )

            async def _delayed():
                try:
                    await asyncio.sleep(remaining)
                    await cmd_chart(channel_id, ca, tf_clean, bypass_cooldown=True)
                except asyncio.CancelledError:
                    pass
                finally:
                    _chart_pending_jobs.pop(job_key, None)

            _chart_pending_jobs[job_key] = asyncio.create_task(_delayed())
            return

    await send_typing(channel_id)
    _set_chart_cooldown(channel_id)

    tf_cfg   = TIMEFRAME_MAP[tf_clean]; tf_label = tf_cfg["label"]; res = tf_cfg["resolution"]
    gt       = await fetch_geckoterminal(ca)

    if not gt or not gt.get("pool_address"):
        await send_message(channel_id, embeds=[{
            "title": "⚠️ Token Not Found",
            "description": f"Could not find pool data for:\n```{ca}```",
            "color": 0xF59E0B,
            "fields": [{"name": "🔗 Try manually",
                        "value": f"[DexScreener](https://dexscreener.com/solana/{ca})", "inline": False}],
            "footer":    {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
            "timestamp": get_timestamp(),
        }])
        return

    pool  = gt["pool_address"]; name = gt["name"]; price = gt["price_usd"]
    p24h  = gt["price_change_24h"]; color = 0x10B981 if p24h >= 0 else 0xEF4444
    total_txns = gt["buys_24h"] + gt["sells_24h"]
    buy_ratio  = (gt["buys_24h"] / total_txns * 100) if total_txns > 0 else 0.0

    def gt_url(r): return f"https://www.geckoterminal.com/solana/pools/{pool}?resolution={r}"
    def pct_str(p): return f"{'🟢' if p >= 0 else '🔴'} {'+' if p >= 0 else ''}{p:.2f}%"

    await send_message(channel_id, embeds=[{"description": f"📊 Generating **{tf_label}** chart for `{name}`…", "color": 0x6366F1}])
    chart_bytes = await generate_chart_image(ca=ca, timeframe=tf_clean, token_name=name, pool_address=pool)

    embed = {
        "author":      {"name": f"📊 {name} · {tf_label} Chart"},
        "title":       f"${price:.8f}", "url": gt_url(res), "color": color,
        "description": (f"[`1m`]({gt_url(1)})  ·  [`5m`]({gt_url(5)})  ·  "
                        f"[`15m`]({gt_url(15)})  ·  [`1H`]({gt_url(60)})  ·  [`1D`]({gt_url(1440)})"),
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
             "inline": False},
        ],
        "footer":    {"text": f"GeckoTerminal · {tf_label} · cooldown {CHART_COOLDOWN_SECONDS}s · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }

    if chart_bytes:
        embed["image"] = {"url": "attachment://chart.png"}
        headers = {"Authorization": f"Bot {DISCORD_TOKEN}", "User-Agent": "XerisBot/2.0"}
        async with httpx.AsyncClient(timeout=25.0) as client:
            r = await client.post(
                f"{DISCORD_API}/channels/{channel_id}/messages", headers=headers,
                files={"file": ("chart.png", chart_bytes, "image/png")},
                data={"payload_json": json.dumps({"embeds": [embed]})},
            )
        if r.status_code not in (200, 201):
            embed["description"] += "\n\n⚠️ *Image upload failed.*"
            await send_message(channel_id, embeds=[embed])
    else:
        embed["description"] += "\n\n⚠️ *Chart unavailable — click a timeframe above to view on GeckoTerminal*"
        await send_message(channel_id, embeds=[embed])


# ═════════════════════════════════════════════════════════════════════════════
# !analyze
# ═════════════════════════════════════════════════════════════════════════════

async def cmd_analyze(channel_id: int, ca: str) -> None:
    await send_typing(channel_id)
    await send_message(channel_id, embeds=[{
        "title": "🔍 Running Full Risk Scan...",
        "description": f"```{ca}```\n⛓️ Fetching on-chain data...\n🤖 AI analysis via Groq LLaMA3 (15-25s)",
        "color": 0x6366F1,
    }])
    price_data, holders = await asyncio.gather(fetch_price_for_ca(ca), fetch_top_holders(ca))
    if not price_data:
        await send_message(channel_id, embeds=[{
            "title": "❌ Token Not Found",
            "description": f"No market data for:\n```{ca}```",
            "color": 0xEF4444,
        }])
        return
    name   = price_data.get("name",   "Unknown")
    symbol = price_data.get("symbol", "???")
    intelligence = await fetch_all_intelligence(ca, name, symbol)
    socials    = intelligence.get("socials", {})
    token_meta = intelligence.get("token_meta", {})
    pumpfun    = intelligence.get("pumpfun", {})
    ai         = await groq_analyze(ca, price_data, holders, intelligence)

    if "error" in ai:
        await send_message(channel_id, embeds=[{
            "title": "❌ AI Analysis Failed",
            "description": f"```{ai['error']}```",
            "color": 0xEF4444,
        }])
        return

    score     = int(ai.get("risk_score", 5)); rug_label = ai.get("rug_label", "UNKNOWN")
    color     = score_to_color(score);        rug_emoji  = rug_label_emoji(rug_label)
    liq  = price_data.get("liquidity", 0);    vol  = price_data.get("volume_24h", 0)
    mcap = price_data.get("mcap", 0);         total_supply = 1_000_000_000
    top5_pct  = sum(float(h.get("uiAmount", 0)) for h in holders[:5])  / total_supply * 100 if holders else 0
    top10_pct = sum(float(h.get("uiAmount", 0)) for h in holders[:10]) / total_supply * 100 if holders else 0

    await send_message(channel_id, embeds=[{
        "author":      {"name": f"🛡️ AI Risk Report · {name} ({symbol})"},
        "title":       f"`{ca[:20]}...{ca[-8:]}`",
        "description": f"> {ai.get('summary', 'No summary.')}",
        "color":       color,
        "fields": [
            {"name": f"{rug_emoji} Risk Score", "value": risk_score_bar(score), "inline": True},
            {"name": "⚠️ Verdict",   "value": f"**{rug_emoji} {rug_label}**",             "inline": True},
            {"name": "📅 Token Age", "value": f"`{token_meta.get('token_age_days','?')} days`", "inline": True},
            {"name": "📊 Market Data",
             "value": (
                 f"```yaml\nPrice:    ${price_data['price']:.8f}\nMCap:     {format_usd(mcap)}\n"
                 f"Vol 24h:  {format_usd(vol)}\nLiq:      {format_usd(liq)}\n```"
             ), "inline": True},
            {"name": "📣 Socials & Holders",
             "value": (
                 f"```yaml\nTwitter:  {socials.get('twitter_handle') or 'NOT FOUND'}\n"
                 f"Website:  {'Found ✅' if socials.get('website') else 'Not Found ❌'}\n"
                 f"Top 5:    {top5_pct:.1f}%\nTop 10:   {top10_pct:.1f}%\n```"
             ), "inline": True},
            {"name": "🔴 Red Flags",
             "value": "\n".join(f"• {f}" for f in ai.get("red_flags", [])) or "• None detected",
             "inline": False},
            {"name": "🟢 Green Signals",
             "value": "\n".join(f"• {f}" for f in ai.get("green_flags", [])) or "• None detected",
             "inline": False},
            {"name": "💡 Trade Advice", "value": f"> {ai.get('trade_advice','N/A')[:1020]}", "inline": False},
            {"name": "🔗 Verify",
             "value": (
                 f"[DexScreener](https://dexscreener.com/solana/{ca}) · [Solscan](https://solscan.io/token/{ca})"
                 + (f" · [Pump.fun](https://pump.fun/{ca})" if pumpfun.get("is_pumpfun") else "")
             ), "inline": False},
        ],
        "footer":    {"text": f"Powered by Groq LLaMA-3.3-70B · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])


# ═════════════════════════════════════════════════════════════════════════════
# !order
# ═════════════════════════════════════════════════════════════════════════════

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

    buys  = sorted([o for o in orders if o["order_type"] == "LIMIT_BUY"],
                   key=lambda x: (x.get("predicted_mcap", 0) <= 0, x.get("predicted_mcap", 0)))
    sells = sorted([o for o in orders if o["order_type"] == "LIMIT_SELL"],
                   key=lambda x: (x.get("predicted_mcap", 0) <= 0, x.get("predicted_mcap", 0)))
    total_buy_wall  = sum(o["usd_value"] for o in buys)
    total_sell_wall = sum(o["usd_value"] for o in sells)

    await send_message(channel_id, embeds=[{
        "author": {"name": "📋 XERIS · Live Limit Order Book"},
        "title":  f"{len(orders)} Active Order(s) Tracked",
        "description": (
            f"```yaml\nCurrent Price : ${ms.current_price:.8f}\n"
            f"Current MCap  : {format_usd(ms.current_market_cap)}\n"
            f"───────────────────────────────\n"
            f"Buy  Orders   : {len(buys)}  │  Wall: {format_usd(total_buy_wall)}\n"
            f"Sell Orders   : {len(sells)}  │  Wall: {format_usd(total_sell_wall)}\n```"
        ),
        "color":     0x8B5CF6,
        "footer":    {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])

    for label, order_list, color, wall_label, wall_total in [
        ("🛡️ BUY ORDERS (Support)",      buys,  0x10B981, "Total Buy Wall",  total_buy_wall),
        ("⚠️ SELL ORDERS (Resistance)", sells, 0xEF4444, "Total Sell Wall", total_sell_wall),
    ]:
        if not order_list:
            await send_message(channel_id, embeds=[{"description": "> No active orders.", "color": 0x6B7280}])
            continue
        lines   = ""; page = 1; start_i = 0
        for i, o in enumerate(order_list[:10]):
            dist     = _pct_from_current(o["predicted_mcap"], ms.current_market_cap)
            qt       = f"[{o['quote_token']}] " if o.get("quote_token") else ""
            exch     = o.get("exchange", "")
            wallet_s = f"{o['wallet'][:6]}…{o['wallet'][-4:]}"
            lines += (
                f"**`#{i+1}`** {qt}`{format_tokens(o['token_amount'])} XERIS` · **{format_usd(o['usd_value'])}**\n"
                f"┣ Target MCap : `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%)\n"
                f"┣ Wallet      : [`{wallet_s}`](https://solscan.io/account/{o['wallet']})"
                + (f" via `{exch}`" if exch else "") + "\n"
                f"┣ Placed      : `{_format_time_placed(o)}`\n"
                f"┗ Remaining   : `{_format_time_remaining(o)}`\n\n"
            )
            if (i + 1) % 5 == 0 and i + 1 < len(order_list):
                await send_message(channel_id, embeds=[{
                    "author":      {"name": f"{label} · Page {page}"},
                    "description": lines, "color": color,
                    "footer":      {"text": f"Orders #{start_i+1}–#{i+1} of {len(order_list)}"},
                }])
                lines = ""; page += 1; start_i = i + 1
        if lines:
            await send_message(channel_id, embeds=[{
                "author":      {"name": f"{label} · {len(order_list)} total"},
                "description": lines, "color": color,
                "fields": [
                    {"name": f"💰 {wall_label}", "value": f"`{format_usd(wall_total)}`", "inline": True},
                    {"name": "📊 Nearest",       "value": f"`{format_usd(order_list[0]['predicted_mcap'])}`", "inline": True},
                ],
                "footer":    {"text": f"XerisBot · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
                "timestamp": get_timestamp(),
            }])


# ═════════════════════════════════════════════════════════════════════════════
# !help
# ═════════════════════════════════════════════════════════════════════════════

async def cmd_help(channel_id: int) -> None:
    await send_message(channel_id, embeds=[{
        "author":      {"name": "🤖 XerisBot — Command Reference"},
        "color":       0x6366F1,
        "description": "All commands use `!` prefix.",
        "fields": [
            {"name": "📈 !price <CA>",          "value": "Price, market cap, 24h volume & change",          "inline": False},
            {"name": "🐳 !whale <CA>",          "value": "Top 15 holders with concentration risk",           "inline": False},
            {"name": "📊 !chart <CA> [tf]",     "value": "Live chart · Timeframes: `1m` `5m` `15m` `1h` `1d`", "inline": False},
            {"name": "🛡️ !analyze <CA>",       "value": "Full AI risk analysis via Groq LLaMA3",            "inline": False},
            {"name": "📋 !order",               "value": "Live limit order book (support & resistance)",     "inline": False},
            {"name": "🎯 !raid <@username>",    "value": "Add an X account to watch (max 3 accounts)",      "inline": False},
            {"name": "📋 !raidlist",            "value": "Show all currently watched X accounts",           "inline": False},
            {"name": "🗑️ !unraid <@username>", "value": "Remove an X account from watch list",             "inline": False},
            {"name": "❓ !help",               "value": "Show this menu",                                   "inline": False},
        ],
        "footer":    {"text": "XerisBot · Helius + DexScreener + Groq"},
        "timestamp": get_timestamp(),
    }])


# ═════════════════════════════════════════════════════════════════════════════
# Message router (called from discord_gateway)
# ═════════════════════════════════════════════════════════════════════════════

async def handle_message(msg: dict, db: Optional[DatabaseManager] = None, ms: Optional[MarketState] = None) -> None:
    content    = (msg.get("content") or "").strip()
    channel_id = int(msg.get("channel_id", 0))
    author     = msg.get("author", {})

    if author.get("bot") or not content.startswith("!"):
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
            await send_message(channel_id, content="❌ Usage: `!chart <CA> [tf]`\nTimeframes: `1m` `5m` `15m` `1h` `1d`")
            return
        tf = parts[2].lower() if len(parts) > 2 else "15m"
        if tf not in TIMEFRAME_MAP:
            tf = "15m"
        await cmd_chart(channel_id, arg, timeframe=tf)

    elif command == "!analyze":
        if not arg or not VALID_CA.match(arg):
            await send_message(channel_id, content="❌ Usage: `!analyze <contract_address>`")
            return
        await cmd_analyze(channel_id, arg)

    elif command in ("!order", "!orders"):
        if db and ms:
            await cmd_order(channel_id, db, ms)
        else:
            await send_message(channel_id, content="❌ Order tracker not initialized yet.")

    # Raid commands are handled in x_rss_monitor via the raid_command_handler import
    elif command in ("!raid", "!unraid", "!raidlist"):
        if db:
            from x_rss_monitor import handle_raid_command
            await handle_raid_command(command, parts, channel_id, author, db)
        else:
            await send_message(channel_id, content="❌ Monitor not initialized yet.")
