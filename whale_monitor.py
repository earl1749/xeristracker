import asyncio
import json
import httpx
import websockets
import time
import os
from datetime import datetime, timezone

# ------------------------------------------------------------
# CONFIG - Set these as environment variables in Railway
# ------------------------------------------------------------

MINT = os.getenv("MINT", "9ezFthWrDUpSSeMdpLW6SDD9TJigHdc4AuQ5QN5bpump")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")  # REQUIRED - set in Railway
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")  # REQUIRED - set in Railway

DEV_WALLET = os.getenv("DEV_WALLET", "6XjutcUVEidzb3o1yXLYGC2ZSnjde2YvAUF9CiPVqxwm")
WHALE_MIN_USD = int(os.getenv("WHALE_MIN_USD", "1000"))

# Validate required environment variables
if not HELIUS_API_KEY:
    raise ValueError("❌ HELIUS_API_KEY environment variable is required! Set it in Railway.")
if not DISCORD_WEBHOOK:
    raise ValueError("❌ DISCORD_WEBHOOK environment variable is required! Set it in Railway.")

WS_URL  = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# Price & market cap caches
current_price      = 0.0
current_market_cap = 0.0
total_supply       = 0.0
last_price_update  = 0
PRICE_UPDATE_INTERVAL = 30

# Price alert config
PRICE_ALERT_THRESHOLD = 5.0        # % move that triggers an alert
PRICE_ALERT_COOLDOWN  = 300        # seconds between same-direction alerts (5 min)
price_reference       = 0.0        # baseline price we measure % from
last_alert_up_time    = 0.0        # last time we sent a pump alert
last_alert_down_time  = 0.0        # last time we sent a dump alert
last_alert_direction  = None       # "up" | "down" | None

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------

def get_timestamp():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def format_usd(value: float) -> str:
    """Smart compact formatting: $1.23K / $4.56M / $789"""
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.2f}K"
    else:
        return f"${value:,.2f}"

def format_tokens(amount: float) -> str:
    if amount >= 1_000_000:
        return f"{amount/1_000_000:.2f}M"
    elif amount >= 1_000:
        return f"{amount/1_000:.2f}K"
    return f"{amount:,.0f}"

def wallet_short(addr: str) -> str:
    return f"`{addr[:6]}...{addr[-6:]}`"

def make_bar(pct: float, width: int = 12) -> str:
    """Render a text progress bar using block characters."""
    filled = round(pct / 100 * width)
    return "█" * filled + "░" * (width - filled)

def impact_label(usd: float) -> str:
    if usd >= 50_000:  return "🔴 MEGA WHALE"
    if usd >= 10_000:  return "🟠 WHALE"
    if usd >= 5_000:   return "🟡 BIG FISH"
    return              "🔵 FISH"

# ------------------------------------------------------------
# DISCORD TEST
# ------------------------------------------------------------

async def test_discord_webhook():
    print("\n🔍 TESTING DISCORD WEBHOOK...")
    embed = {
        "author": {
            "name": "XerisCoin Monitor System",
            "icon_url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
        },
        "title": "🛰️ Monitor Activated · System Online",
        "description": (
            "```\n"
            "╔══════════════════════════════════╗\n"
            "║  REAL-TIME MONITORING ACTIVE    ║\n"
            "║  • Whale Tracking               ║\n"
            "║  • Dev Activity Alerts          ║\n"
            "║  • Price Movement Detection     ║\n"
            "╚══════════════════════════════════╝\n"
            "```\n"
            "> All systems operational · Monitoring blockchain activity"
        ),
        "color": 0x10B981,  # Emerald
        "fields": [
            {
                "name": "🐋 Whale Detection",
                "value": f"```yaml\nThreshold: ${WHALE_MIN_USD:,} USD+\nStatus: Active\n```",
                "inline": True
            },
            {
                "name": "📈 Price Alerts",
                "value": f"```yaml\nTrigger: ±{PRICE_ALERT_THRESHOLD}%\nCooldown: {PRICE_ALERT_COOLDOWN}s\n```",
                "inline": True
            },
            {
                "name": "⚡ System Info",
                "value": "```yaml\nRPC: Helius Mainnet\nNetwork: Solana\nStatus: Connected\n```",
                "inline": True
            }
        ],
        "footer": {
            "text": f"Powered by Helius RPC · Started at {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        },
        "timestamp": get_timestamp()
    }
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(DISCORD_WEBHOOK, json={"embeds": [embed]})
            if r.status_code == 204:
                print("✅ Discord webhook OK!")
                return True
            else:
                print(f"❌ ERROR {r.status_code}: {r.text[:200]}")
                return False
        except Exception as e:
            print(f"❌ Exception: {e}")
            return False

# ------------------------------------------------------------
# WEBHOOK SENDER
# ------------------------------------------------------------

async def send_webhook(embeds: list):
    """Send one or more embeds to Discord."""
    print(f"\n📤 Sending Discord alert ({len(embeds)} embed(s))...")
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(DISCORD_WEBHOOK, json={"embeds": embeds})
            if r.status_code == 204:
                print("   ✅ Alert sent!")
                return True
            else:
                print(f"   ❌ Failed: {r.status_code} — {r.text[:200]}")
                return False
        except Exception as e:
            print(f"   ❌ Exception: {e}")
            return False

# ------------------------------------------------------------
# PRICE + MARKET CAP
# ------------------------------------------------------------

async def update_price():
    global current_price, current_market_cap, total_supply, last_price_update, price_reference
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{MINT}"
        async with httpx.AsyncClient() as client:
            r = await client.get(url)
            data = r.json()
            pairs = data.get("pairs") or []
            if pairs:
                pair = pairs[0]
                new_price = float(pair.get("priceUsd") or 0)

                # Market cap: prefer fdv (fully diluted) from DexScreener
                fdv  = pair.get("fdv")
                mcap = pair.get("marketCap")
                if fdv:
                    current_market_cap = float(fdv)
                elif mcap:
                    current_market_cap = float(mcap)
                elif new_price > 0:
                    liq = float((pair.get("liquidity") or {}).get("usd") or 0)
                    current_market_cap = liq * 2

                # Set reference price on first fetch
                if price_reference == 0.0 and new_price > 0:
                    price_reference = new_price

                old_price     = current_price
                current_price = new_price
                last_price_update = time.time()
                print(f"💰 Price: ${current_price:.8f} | MCap: {format_usd(current_market_cap)}")

                # Check price alert (only after we have a reference)
                if price_reference > 0 and current_price > 0:
                    await check_price_alert()
            else:
                print("⚠️ No pairs found on DexScreener")
    except Exception as e:
        print(f"❌ Price fetch error: {e}")

def estimate_new_mcap(tx_type: str, usd_value: float) -> float:
    """
    Estimate new market cap after a buy/sell.
    Buys push price up → MCap rises by ~usd_value (very rough).
    Sells push price down → MCap drops by ~usd_value.
    This is a simplified heuristic for on-screen context.
    """
    if tx_type == "BUY":
        return current_market_cap + usd_value
    else:
        return max(0, current_market_cap - usd_value)

# ------------------------------------------------------------
# PRICE ALERT ENGINE
# ------------------------------------------------------------

async def check_price_alert():
    """Fire a Discord alert when price moves ±5% from the reference."""
    global price_reference, last_alert_up_time, last_alert_down_time, last_alert_direction

    if price_reference <= 0 or current_price <= 0:
        return

    pct_change = ((current_price - price_reference) / price_reference) * 100
    now        = time.time()
    abs_change = abs(pct_change)

    if abs_change < PRICE_ALERT_THRESHOLD:
        return  # not enough movement yet

    direction = "up" if pct_change > 0 else "down"

    # Cooldown check per direction
    last_alert = last_alert_up_time if direction == "up" else last_alert_down_time
    if now - last_alert < PRICE_ALERT_COOLDOWN:
        return  # already alerted recently for this direction

    print(f"\n{'🚀' if direction == 'up' else '🔻'} PRICE ALERT: {pct_change:+.2f}% from reference!")

    embed = build_price_alert_embed(pct_change, price_reference)
    await send_webhook([embed])

    # Update state: reset reference to current price, update cooldown
    if direction == "up":
        last_alert_up_time = now
    else:
        last_alert_down_time = now

    last_alert_direction = direction
    price_reference = current_price  # re-anchor reference so next alert is relative to NOW
    print(f"   📌 Reference reset to ${current_price:.8f}")


def build_price_alert_embed(pct_change: float, ref_price: float) -> dict:
    is_pump   = pct_change > 0
    sign      = "+" if is_pump else ""
    
    # Premium color palette
    color = 0x10B981 if is_pump else 0xEF4444  # Emerald / Rose
    
    # Movement intensity with visual indicators
    abs_pct = abs(pct_change)
    if abs_pct >= 15:
        intensity = "🔥 EXTREME MOVE"
        bars = "████████████"
    elif abs_pct >= 10:
        intensity = "⚡ STRONG MOVE"
        bars = "█████████░░░"
    elif abs_pct >= 7:
        intensity = "📈 NOTABLE MOVE"
        bars = "███████░░░░░"
    else:
        intensity = "📊 MODERATE MOVE"
        bars = "█████░░░░░░░"

    mcap_change = current_market_cap - (current_market_cap / (1 + pct_change / 100))
    mcap_sign   = "+" if mcap_change >= 0 else ""

    direction_emoji = "🚀" if is_pump else "📉"
    direction = "PUMP" if is_pump else "DUMP"

    embed = {
        "author": {
            "name": f"{intensity}",
            "icon_url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
        },
        "title": f"{direction_emoji} Price Alert: {direction} {sign}{pct_change:.2f}%",
        "description": (
            f"```diff\n"
            f"{'+ ' if is_pump else '- '}{sign}{pct_change:.2f}% price movement detected\n"
            f"```\n"
            f"> Price has {'surged' if is_pump else 'dropped'} **{sign}{pct_change:.2f}%** from last reference point"
        ),
        "color": color,
        "fields": [
            {
                "name": "💹 Price Movement",
                "value": (
                    f"```yaml\n"
                    f"Reference: ${ref_price:.8f}\n"
                    f"Current:   ${current_price:.8f}\n"
                    f"Change:    {sign}{pct_change:.2f}%\n"
                    f"```"
                ),
                "inline": True
            },
            {
                "name": "📊 Market Cap Impact",
                "value": (
                    f"```yaml\n"
                    f"MCap:   {format_usd(current_market_cap)}\n"
                    f"Change: {mcap_sign}{format_usd(abs(mcap_change))}\n"
                    f"Delta:  {mcap_sign}{abs(mcap_change/current_market_cap*100):.2f}%\n"
                    f"```"
                ),
                "inline": True
            },
            {
                "name": "📈 Momentum Indicator",
                "value": f"`{bars}` **{abs_pct:.1f}%**",
                "inline": False
            },
            {
                "name": "🔗 Monitor Charts",
                "value": (
                    f"[DexScreener]({f'https://dexscreener.com/solana/{MINT}'}) · "
                    f"[Birdeye]({f'https://birdeye.so/token/{MINT}'}) · "
                    f"[Solscan]({f'https://solscan.io/token/{MINT}'})"
                ),
                "inline": False
            }
        ],
        "footer": {
            "text": f"Price Monitor · Threshold ±{PRICE_ALERT_THRESHOLD}% · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        },
        "timestamp": get_timestamp()
    }
    return embed

# ------------------------------------------------------------
# TRANSACTION FETCHER
# ------------------------------------------------------------

async def fetch_tx(signature, retries=3):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed"
            }
        ]
    }
    for attempt in range(retries):
        try:
            print(f"   🔄 Fetching tx (attempt {attempt+1}/{retries})...")
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(RPC_URL, json=payload)
                if r.status_code == 200:
                    result = r.json()
                    if "error" in result:
                        print(f"   ⚠️ RPC Error: {result['error']}")
                    else:
                        tx_data = result.get("result")
                        if tx_data:
                            print("   ✅ TX fetched")
                            return tx_data
                        else:
                            print("   ⚠️ No tx data yet (too recent?)")
                else:
                    print(f"   ⚠️ HTTP {r.status_code}")
        except Exception as e:
            print(f"   ⚠️ Attempt {attempt+1} failed: {e}")
        if attempt < retries - 1:
            wait = 1 * (attempt + 1)
            print(f"   ⏳ Retrying in {wait}s...")
            await asyncio.sleep(wait)
    print(f"   ❌ All {retries} attempts failed")
    return None

# ------------------------------------------------------------
# TRANSACTION PARSER
# ------------------------------------------------------------

def parse_tx(tx_data, signature):
    if not tx_data:
        return None

    meta = tx_data.get("meta")
    if not meta or meta.get("err"):
        return None

    tx      = tx_data.get("transaction", {})
    message = tx.get("message", {})
    account_keys = message.get("accountKeys", [])

    if account_keys:
        signer = account_keys[0].get("pubkey", "unknown") if isinstance(account_keys[0], dict) else account_keys[0]
    else:
        signer = "unknown"

    pre  = [b for b in (meta.get("preTokenBalances")  or []) if b.get("mint") == MINT]
    post = [b for b in (meta.get("postTokenBalances") or []) if b.get("mint") == MINT]

    if not pre and not post:
        return None

    def get_amount(entry):
        ui = entry.get("uiTokenAmount") or {}
        return int(ui.get("amount") or "0"), int(ui.get("decimals") or 6)

    signer_delta = 0
    for p in post:
        if p.get("owner") == signer:
            amount_post, decimals = get_amount(p)
            matching_pre = next((x for x in pre if x.get("accountIndex") == p.get("accountIndex")), None)
            amount_pre = get_amount(matching_pre)[0] if matching_pre else 0
            signer_delta += amount_post - amount_pre

    for p in pre:
        if p.get("owner") == signer and not any(x.get("accountIndex") == p.get("accountIndex") for x in post):
            amount_pre, decimals = get_amount(p)
            signer_delta -= amount_pre

    if signer_delta == 0:
        return None

    decimals = 6
    amount   = abs(signer_delta) / (10 ** decimals)
    tx_type  = "BUY" if signer_delta > 0 else "SELL"
    return tx_type, amount, signer

# ------------------------------------------------------------
# DISCORD EMBEDS — BEAUTIFUL LAYOUTS
# ------------------------------------------------------------

def build_whale_embed(tx_type: str, amount: float, wallet: str, usd_value: float, signature: str) -> dict:
    is_buy    = tx_type == "BUY"
    new_mcap  = estimate_new_mcap(tx_type, usd_value)
    mcap_diff = new_mcap - current_market_cap
    diff_sign = "+" if mcap_diff >= 0 else ""

    # Impact sizing with premium colors
    if usd_value >= 50_000:
        size_label = "MEGA WHALE"
        tier_emoji = "💎"
    elif usd_value >= 10_000:
        size_label = "WHALE"
        tier_emoji = "🌊"
    elif usd_value >= 5_000:
        size_label = "BIG FISH"
        tier_emoji = "⭐"
    else:
        size_label = "FISH"
        tier_emoji = "💫"

    # Sophisticated color scheme
    color = 0x10B981 if is_buy else 0xEF4444  # Emerald / Rose
    action_emoji = "📈" if is_buy else "📉"
    
    # Impact percentage
    impact_pct = (usd_value / current_market_cap * 100) if current_market_cap > 0 else 0
    
    # Create visual separator
    separator = "━━━━━━━━━━━━━━━━━━━━━━━"
    
    embed = {
        "author": {
            "name": f"{tier_emoji} {size_label} DETECTED",
            "icon_url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
        },
        "title": f"{action_emoji} {tx_type} · {format_usd(usd_value)}",
        "description": (
            f"```yaml\n"
            f"Trade Size: {format_tokens(amount)} XERIS\n"
            f"USD Value:  {format_usd(usd_value)}\n"
            f"Impact:     {impact_pct:.2f}% of market cap\n"
            f"```"
        ),
        "color": color,
        "fields": [
            {
                "name": "💰 Market Metrics",
                "value": (
                    f"┌ Price: `${current_price:.8f}`\n"
                    f"├ Market Cap: `{format_usd(current_market_cap)}`\n"
                    f"└ New MCap: `{format_usd(new_mcap)}` ({diff_sign}{format_usd(abs(mcap_diff))})"
                ),
                "inline": False
            },
            {
                "name": "👤 Wallet Address",
                "value": f"```{wallet}```",
                "inline": False
            },
            {
                "name": "🔗 Quick Links",
                "value": (
                    f"[Transaction]({f'https://solscan.io/tx/{signature}'}) · "
                    f"[Wallet]({f'https://solscan.io/account/{wallet}'}) · "
                    f"[Chart]({f'https://dexscreener.com/solana/{MINT}'})"
                ),
                "inline": False
            }
        ],
        "footer": {
            "text": f"XerisCoin Monitor · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}",
            "icon_url": "https://cdn.discordapp.com/emojis/1234567890.png"
        },
        "timestamp": get_timestamp()
    }
    return embed


def build_dev_sell_embed(amount: float, wallet: str, usd_value: float, signature: str) -> dict:
    new_mcap  = estimate_new_mcap("SELL", usd_value)
    mcap_diff = new_mcap - current_market_cap
    impact_pct = (usd_value / current_market_cap * 100) if current_market_cap > 0 else 0

    embed = {
        "author": {
            "name": "⚠️ DEVELOPER ACTIVITY ALERT",
            "icon_url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
        },
        "title": "🚨 Dev Wallet Sell Detected",
        "description": (
            f"```diff\n"
            f"- Developer has executed a SELL transaction\n"
            f"```\n"
            f"**⚠️ Monitor price action closely**\n"
            f"> Sell Amount: **{format_usd(usd_value)}** ({impact_pct:.2f}% of MCap)"
        ),
        "color": 0xDC2626,  # Deep red
        "fields": [
            {
                "name": "💸 Transaction Details",
                "value": (
                    f"```yaml\n"
                    f"Tokens:     {format_tokens(amount)} XERIS\n"
                    f"USD Value:  {format_usd(usd_value)}\n"
                    f"Price:      ${current_price:.8f}\n"
                    f"Impact:     {impact_pct:.2f}%\n"
                    f"```"
                ),
                "inline": False
            },
            {
                "name": "📊 Market Cap Impact",
                "value": (
                    f"┌ Before: `{format_usd(current_market_cap)}`\n"
                    f"├ After:  `{format_usd(new_mcap)}`\n"
                    f"└ Change: `{format_usd(mcap_diff)}` ({abs(impact_pct):.2f}%)"
                ),
                "inline": False
            },
            {
                "name": "👤 Developer Wallet",
                "value": f"```{wallet}```",
                "inline": False
            },
            {
                "name": "🔍 Verification Links",
                "value": (
                    f"[View Transaction]({f'https://solscan.io/tx/{signature}'}) · "
                    f"[Wallet History]({f'https://solscan.io/account/{wallet}'}) · "
                    f"[Live Chart]({f'https://dexscreener.com/solana/{MINT}'})"
                ),
                "inline": False
            }
        ],
        "footer": {
            "text": f"Dev Activity Monitor · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}",
        },
        "timestamp": get_timestamp()
    }
    return embed

# ------------------------------------------------------------
# MONITOR LOOP
# ------------------------------------------------------------

async def monitor():
    global current_price
    print("\n" + "="*60)
    print("🚀 WHALE + DEV + PRICE MONITOR STARTED")
    print("="*60)
    print(f"🎯 Mint:         {MINT}")
    print(f"👀 Dev:          {DEV_WALLET}")
    print(f"💰 Min USD:      ${WHALE_MIN_USD}")
    print(f"📈 Price Alert:  ±{PRICE_ALERT_THRESHOLD}% move")
    print(f"⏱️  Cooldown:     {PRICE_ALERT_COOLDOWN}s between alerts")
    print("="*60 + "\n")

    print("Testing Discord...")
    if not await test_discord_webhook():
        resp = input("\nContinue anyway? (y/n): ")
        if resp.lower() != "y":
            return
    else:
        print("\n✅ Discord OK!\n")

    await update_price()

    retry_count  = 0
    tx_count     = 0
    failed_fetches = 0

    while True:
        try:
            print(f"\n📡 Connecting to WebSocket (attempt {retry_count+1})...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=60) as ws:
                print("✅ WebSocket connected")

                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [MINT]},
                        {"commitment": "confirmed"}
                    ]
                }))
                print("✅ Subscribed — waiting for transactions...")
                retry_count = 0

                while True:
                    try:
                        if time.time() - last_price_update > PRICE_UPDATE_INTERVAL:
                            await update_price()

                        msg  = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)

                        if "params" not in data:
                            continue

                        signature = data["params"]["result"]["value"]["signature"]
                        tx_count += 1
                        print(f"\n{'='*50}")
                        print(f"📝 TX #{tx_count} | {signature[:20]}...")
                        print(f"⏱  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
                        print("="*50)

                        tx_data = await fetch_tx(signature, retries=3)
                        if not tx_data:
                            failed_fetches += 1
                            print(f"❌ Fetch failed (total: {failed_fetches})")
                            continue

                        parsed = parse_tx(tx_data, signature)
                        if not parsed:
                            print("❌ Not relevant — skipping")
                            continue

                        tx_type, amount, wallet = parsed
                        usd_value = amount * current_price

                        print(f"\n📊 {tx_type} | {format_tokens(amount)} XERIS | {format_usd(usd_value)} | {wallet[:16]}...")

                        # 🚨 DEV SELL
                        if wallet == DEV_WALLET and tx_type == "SELL":
                            print("\n🚨🚨🚨 DEV SELL DETECTED!")
                            embed = build_dev_sell_embed(amount, wallet, usd_value, signature)
                            await send_webhook([embed])

                        # 🐋 WHALE
                        elif usd_value >= WHALE_MIN_USD:
                            print(f"\n🐋 WHALE {tx_type} — {format_usd(usd_value)}")
                            embed = build_whale_embed(tx_type, amount, wallet, usd_value, signature)
                            await send_webhook([embed])

                        else:
                            print(f"\nℹ️ Below threshold ({format_usd(usd_value)} < {format_usd(WHALE_MIN_USD)})")

                    except asyncio.TimeoutError:
                        await ws.ping()
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        print("\n⚠️ WS closed — reconnecting...")
                        break
                    except Exception as e:
                        print(f"\n❌ Message error: {e}")
                        continue

        except Exception as e:
            retry_count += 1
            wait_time = min(30 * retry_count, 300)
            print(f"\n❌ Connection error: {e}")
            print(f"⏰ Reconnecting in {wait_time}s... (attempt {retry_count})")
            await asyncio.sleep(wait_time)

# ------------------------------------------------------------

async def main():
    try:
        await monitor()
    except KeyboardInterrupt:
        print("\n👋 Monitor stopped")
    except Exception as e:
        print(f"❌ Fatal: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
