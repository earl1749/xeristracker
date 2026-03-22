

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Optional

import websockets

from config.settings import (
    ALERT_CHANNEL_ID, CLEANUP_INTERVAL, DB_PATH, DEBUG_CHANNEL_ID,
    DEV_WALLET, DISCORD_TOKEN, GATEWAY_URL, GROQ_ENABLED, GROQ_MODEL,
    MINT, ORDER_TTL_HOURS, PRICE_ALERT_COOLDOWN, PRICE_ALERT_THRESHOLD,
    PRICE_UPDATE_INTERVAL, SUMMARY_ALERT_INTERVAL, WHALE_MIN_USD, WS_URL, WSOL_MINT,
)
from core.models import LimitOrder, MarketState, OrderType
from core.classifier import TransactionClassifier
from core.tracker import OrderTracker, AlertManager
from helpers.database import DatabaseManager
from helpers.discord_utils import init_discord_queue, send_message, _discord_queue
from helpers.embeds import (
    _build_dev_sell_embed, _build_price_embed, _build_whale_embed,
)
from helpers.formatters import format_tokens, format_usd, get_timestamp, _pick_best_pair
from helpers.rpc import fetch_tx
try:
    from commands.bot_commands import handle_message   # if you kept the commands/ folder
except ModuleNotFoundError:
    from commands.bot_commands import handle_message    # if you placed it in helpers/
from x_rss_monitor import x_post_monitor

import httpx

# ── Shared state references (set once helius_monitor starts) ─────────────────
_db_ref: Optional[DatabaseManager] = None
_ms_ref: Optional[MarketState]     = None


# ═════════════════════════════════════════════════════════════════════════════
# Price update
# ═════════════════════════════════════════════════════════════════════════════

async def update_price(ms: MarketState) -> None:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r       = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{MINT}")
            data    = r.json()
            sol_r   = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{WSOL_MINT}")
            sol_data= sol_r.json()

        pair     = _pick_best_pair(data.get("pairs") or [])
        if not pair:
            print("⚠️ No pairs on DexScreener")
            return

        sol_pair = _pick_best_pair(sol_data.get("pairs") or [])
        if sol_pair:
            ms.sol_price_usd = float(sol_pair.get("priceUsd") or 0)

        liq          = pair.get("liquidity") or {}
        quote_token  = pair.get("quoteToken") or {}
        ms.pool_token_reserve = float(liq.get("base")  or 0)
        ms.pool_quote_reserve = float(liq.get("quote") or 0)
        ms.pool_liquidity_usd = float(liq.get("usd")   or 0)
        ms.quote_mint         = quote_token.get("address", "") or ""
        ms.quote_symbol       = (quote_token.get("symbol") or "").upper()

        if ms.quote_symbol in {"USDC", "USDT"}:
            ms.quote_to_usd = 1.0
        elif ms.quote_symbol in {"SOL", "WSOL"} or ms.quote_mint == WSOL_MINT:
            ms.quote_to_usd = ms.sol_price_usd
        else:
            ms.quote_to_usd = 0.0
            if ms.quote_mint:
                try:
                    async with httpx.AsyncClient(timeout=15.0) as client:
                        qr   = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{ms.quote_mint}")
                        qp   = _pick_best_pair(qr.json().get("pairs") or [])
                        if qp:
                            ms.quote_to_usd = float(qp.get("priceUsd") or 0)
                except Exception as e:
                    print(f"⚠️ Quote token price fetch error: {e}")

        new_price = float(pair.get("priceUsd") or 0)
        fdv       = pair.get("fdv");  mcap = pair.get("marketCap")
        ms.current_market_cap = float(mcap or fdv or 0)

        if ms.price_reference == 0.0 and new_price > 0:
            ms.price_reference = new_price

        ms.current_price      = new_price
        ms.last_price_update  = time.time()

        if ms.price_reference > 0:
            await _check_price_alert(ms)

        if ms.total_supply <= 0 and ms.current_price > 0 and ms.current_market_cap > 0:
            ms.total_supply = ms.current_market_cap / ms.current_price

        print(
            f"💰 ${ms.current_price:.8f} | MCap {format_usd(ms.current_market_cap)} | "
            f"Quote {ms.quote_symbol or '?'} | Reserve {ms.pool_quote_reserve:.6f} | "
            f"Quote/USD {ms.quote_to_usd:.8f}"
        )
    except Exception as e:
        print(f"❌ Price error: {e}")


async def _check_price_alert(ms: MarketState) -> None:
    if ms.price_reference <= 0 or ms.current_price <= 0:
        return
    pct       = (ms.current_price - ms.price_reference) / ms.price_reference * 100
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


# ═════════════════════════════════════════════════════════════════════════════
# Helius WebSocket monitor
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
            print(f"\n📡 Helius WS connecting (attempt {retry_count + 1})...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=60) as ws:
                print("✅ Helius WebSocket connected")
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                    "params":  [{"mentions": [MINT]}, {"commitment": "confirmed"}],
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
                        print(f"TX #{tx_count}  {signature[:24]}…  "
                              f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")

                        tx_data = await fetch_tx(signature, retries=3)
                        if not tx_data:
                            continue

                        result = await tracker.process(tx_data, signature)
                        if not result:
                            continue

                        action = result["action"]

                        if action == "new_limit":
                            order: LimitOrder = result["order"]; info = result["info"]
                            qt   = info.get("quote_token", ""); exch = info.get("exchange", "")
                            role = "SUPPORT" if order.order_type == OrderType.LIMIT_BUY else "RESISTANCE"
                            print(f"  📌 {order.order_type.value}  "
                                  f"{format_tokens(order.token_amount)} XERIS  "
                                  f"{format_usd(order.usd_value)}  → {role}")
                            if order.usd_value >= WHALE_MIN_USD:
                                from helpers.embeds import _build_limit_order_embed
                                await send_message(ALERT_CHANNEL_ID,
                                    embeds=[_build_limit_order_embed(order, ms, qt, exch)])

                        elif action == "cancel_limit":
                            if result.get("cancelled"):
                                print(f"  🗑️  CANCEL  {result['cancelled']['order_type']}")

                        elif action == "market":
                            tx_type: OrderType = result["type"]; info = result["info"]
                            fills  = result.get("fills", [])
                            usd_val = info.get("usd_value", info["amount"] * ms.current_price)
                            side   = tx_type.value.replace("MARKET_", "")
                            qt     = info.get("quote_token", ""); exch = info.get("exchange", "")
                            if info["amount"] <= 0 or usd_val <= 0:
                                continue
                            print(f"  💱 {tx_type.value}  {format_tokens(info['amount'])} XERIS  {format_usd(usd_val)}")
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
# Discord Gateway
# ═════════════════════════════════════════════════════════════════════════════

async def discord_gateway() -> None:
    heartbeat_interval = None; sequence = None
    print("\n📡 Connecting to Discord Gateway...")
    while True:
        try:
            async with websockets.connect(GATEWAY_URL) as ws:
                print("✅ Discord Gateway connected")

                async def send_heartbeat():
                    while True:
                        await asyncio.sleep(heartbeat_interval / 1000)
                        await ws.send(json.dumps({"op": 1, "d": sequence}))

                heartbeat_task = None
                async for raw in ws:
                    data = json.loads(raw); op = data.get("op"); t = data.get("t"); d = data.get("d") or {}
                    if s := data.get("s"):
                        sequence = s
                    if op == 10:
                        heartbeat_interval = d["heartbeat_interval"]
                        heartbeat_task     = asyncio.create_task(send_heartbeat())
                        await ws.send(json.dumps({
                            "op": 2,
                            "d":  {"token": DISCORD_TOKEN, "intents": 33280,
                                   "properties": {"$os": "linux", "$browser": "xerisbot", "$device": "xerisbot"}},
                        }))
                        print("✅ Discord Gateway identified")
                    elif t == "READY":
                        user = d.get("user", {})
                        print(f"✅ Logged in as {user.get('username')}#{user.get('discriminator')}")
                    elif t == "MESSAGE_CREATE":
                        await handle_message(d, db=_db_ref, ms=_ms_ref)

        except Exception as e:
            print(f"❌ Gateway error: {e} — reconnecting in 5s...")
            await asyncio.sleep(5)


# ═════════════════════════════════════════════════════════════════════════════
# Startup announcement
# ═════════════════════════════════════════════════════════════════════════════

async def announce_startup() -> None:
    await asyncio.sleep(3)
    await send_message(ALERT_CHANNEL_ID, embeds=[{
        "author":      {"name": "XerisBot — System Online"},
        "title":       "🛰️ Bot Started · All Systems Active",
        "description": (
            "```\n╔══════════════════════════════════════╗\n"
            "║  REAL-TIME MONITORING ACTIVE        ║\n"
            "║  • Whale & Dev Activity Tracking    ║\n"
            "║  • Limit Order Detection            ║\n"
            "║  • Price Movement Alerts            ║\n"
            "║  • AI Risk Analysis (!analyze)      ║\n"
            "║  • X Post Monitoring (!raidlist)    ║\n"
            "╚══════════════════════════════════════╝\n```\n"
            "> Type `!help` to see all commands"
        ),
        "color":  0x10B981,
        "fields": [
            {"name": "🐋 Whale Threshold", "value": f"`${WHALE_MIN_USD:,} USD`",        "inline": True},
            {"name": "📈 Price Alert",     "value": f"`±{PRICE_ALERT_THRESHOLD}%`",      "inline": True},
            {"name": "🤖 AI Engine",       "value": f"`Groq {GROQ_MODEL}`",              "inline": True},
            {"name": "🎯 Monitored Token", "value": f"`{MINT}`",                         "inline": False},
        ],
        "footer":    {"text": f"Started at {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"},
        "timestamp": get_timestamp(),
    }])


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    from pathlib import Path
    Path("runtime").mkdir(exist_ok=True)

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
