# XerisCoin Whale & Price Monitor

Real-time Solana token monitoring bot that tracks whale trades, dev wallet activity, and price movements with beautiful Discord notifications.

## Features

🐋 **Whale Tracking** - Alerts for large trades (configurable threshold)  
🚨 **Dev Wallet Monitor** - Special alerts when dev wallet sells  
📈 **Price Alerts** - Notifications for ±5% price movements  
💰 **Market Cap Tracking** - Shows before/after market cap for every trade  
🎨 **Beautiful Discord Embeds** - Clean, elegant notifications with all key data

---

## Deploy to Railway

### Step 1: Create a Railway Account
1. Go to [railway.app](https://railway.app)
2. Sign up with GitHub (recommended)

### Step 2: Create a New Project
1. Click "New Project"
2. Select "Deploy from GitHub repo"
3. Connect your GitHub account if not already connected
4. Select the repository containing these files

### Step 3: Configure Environment Variables (Optional)
If you want to customize settings without editing the code, you can set these in Railway:

- `MINT` - Token mint address (default: 9ezFthWrDUpSSeMdpLW6SDD9TJigHdc4AuQ5QN5bpump)
- `HELIUS_API_KEY` - Your Helius API key
- `DISCORD_WEBHOOK` - Your Discord webhook URL
- `DEV_WALLET` - Developer wallet address to monitor
- `WHALE_MIN_USD` - Minimum USD value for whale alerts (default: 10)
- `PRICE_ALERT_THRESHOLD` - Price change % to trigger alert (default: 5.0)
- `PRICE_ALERT_COOLDOWN` - Seconds between same-direction alerts (default: 300)

**Note:** The current code has these values hardcoded. If you want to use environment variables instead, see the section below.

### Step 4: Deploy
1. Railway will automatically detect the Python app
2. It will install dependencies from `requirements.txt`
3. It will start the bot using the command in `Procfile`
4. Check the logs to confirm it's running

### Step 5: Monitor Logs
- Click on your deployment in Railway
- Go to "Deployments" tab
- Click "View Logs" to see real-time output
- You should see: "✅ Discord webhook OK!" and "✅ WebSocket connected"

---

## Using Environment Variables (Optional)

To make your bot configurable via Railway environment variables, add this code to the top of `whale_monitor.py` after the imports:

```python
import os

# Configuration from environment variables (with fallbacks)
MINT = os.getenv("MINT", "9ezFthWrDUpSSeMdpLW6SDD9TJigHdc4AuQ5QN5bpump")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "ce0e621e-16d6-41fc-b936-523b06754d3d")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "your-webhook-here")
DEV_WALLET = os.getenv("DEV_WALLET", "6XjutcUVEidzb3o1yXLYGC2ZSnjde2YvAUF9CiPVqxwm")
WHALE_MIN_USD = int(os.getenv("WHALE_MIN_USD", "10"))
PRICE_ALERT_THRESHOLD = float(os.getenv("PRICE_ALERT_THRESHOLD", "5.0"))
PRICE_ALERT_COOLDOWN = int(os.getenv("PRICE_ALERT_COOLDOWN", "300"))
```

Then remove the hardcoded values at the top of the file.

---

## Get Your Discord Webhook

1. Open Discord and go to your server
2. Go to Server Settings → Integrations → Webhooks
3. Click "New Webhook"
4. Choose the channel where you want alerts
5. Copy the webhook URL
6. Paste it in the code or set it as an environment variable in Railway

---

## Get a Helius API Key (Free)

1. Go to [helius.dev](https://www.helius.dev/)
2. Sign up for a free account
3. Create a new API key
4. Copy the API key
5. Paste it in the code or set it as `HELIUS_API_KEY` in Railway

---

## Customization

Edit these values in `whale_monitor.py`:

```python
WHALE_MIN_USD = 10              # Minimum USD for whale alerts
PRICE_ALERT_THRESHOLD = 5.0     # % change to trigger price alert
PRICE_ALERT_COOLDOWN = 300      # Seconds between alerts (5 min)
PRICE_UPDATE_INTERVAL = 30      # How often to fetch price (seconds)
```

---

## Alert Types

### 🐋 Whale Alert
- Triggers when a trade exceeds `WHALE_MIN_USD`
- Shows: trade size, impact %, price, market cap change
- Color: Green for buys, Red for sells
- Size labels: 🔴 MEGA WHALE ($50K+), 🟠 WHALE ($10K+), 🟡 BIG FISH ($5K+), 🔵 FISH

### 🚨 Dev Sell Alert
- Triggers when `DEV_WALLET` sells any amount
- Shows: sell amount, price, market cap impact
- Links to transaction and wallet for verification

### 📈 Price Alert
- Triggers when price moves ±5% (or your custom threshold)
- Shows: old/new price, % change, market cap impact
- Intensity: 🔥 EXTREME (15%+), ⚡ STRONG (10%+), 📈 NOTABLE (7%+), 📶 MODERATE (5%+)
- Has 5-minute cooldown per direction to prevent spam

---

## Troubleshooting

**Bot not connecting:**
- Check Railway logs for errors
- Verify Helius API key is valid
- Make sure Discord webhook URL is correct

**Not receiving alerts:**
- Check Discord webhook channel permissions
- Verify `WHALE_MIN_USD` threshold isn't too high
- Check Railway logs to see if transactions are being detected

**WebSocket disconnects:**
- Normal behavior - bot will auto-reconnect
- Uses exponential backoff (30s, 60s, 90s, up to 5min)

**High API usage:**
- Adjust `PRICE_UPDATE_INTERVAL` to fetch less frequently
- Free Helius tier should be sufficient for one token

---

## Files in This Project

- `whale_monitor.py` - Main bot code
- `requirements.txt` - Python dependencies
- `Procfile` - Tells Railway how to run the app
- `railway.json` - Railway configuration
- `runtime.txt` - Python version specification
- `README.md` - This file

---

## Support

If you encounter issues:
1. Check Railway deployment logs
2. Verify all API keys and webhooks are correct
3. Make sure the token mint address is valid
4. Test Discord webhook with a simple curl command

---

## License

MIT - Use freely, modify as needed!
