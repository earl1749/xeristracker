# 🚀 XerisTracker — Solana Token Monitor Bot

A real-time Discord bot that monitors a Solana token for whale trades, limit orders, dev wallet activity, price alerts, and X (Twitter) posts. Powered by Helius WebSocket, DexScreener, GeckoTerminal, and Groq AI.

---

## ✨ Features

| Feature | Description |
|---|---|
| 🐋 **Whale Alerts** | Notifies when buys/sells exceed your USD threshold |
| 🚨 **Dev Wallet Monitor** | Instant alert on any dev wallet sell |
| 📋 **Limit Order Tracker** | Detects & tracks limit buys/sells as support/resistance levels |
| ⚡ **Price Alerts** | Fires when price moves ±5% from reference (configurable) |
| 📊 **Live Charts** | Candlestick charts via GeckoTerminal (1m, 5m, 15m, 1h, 1d) |
| 🛡️ **AI Risk Analysis** | Full rug-pull risk report via Groq LLaMA-3 |
| 🐦 **X/Twitter Watcher** | Polls a Twitter account and posts new tweets to Discord |
| 🤖 **Auto-Learn** | Learns unknown DEX programs over time |

---

## 📁 Project Structure

```
XERISTRACKER/
├── app.py                        ← Entry point (run this)
├── xeris.py                      ← Main bot logic
├── requirements.txt
├── Procfile                      ← Railway / Heroku deploy
├── railway.toml                  ← Railway config
│
├── config/
│   ├── settings.py               ← All env vars & constants
│   └── data_registy.py           ← Program registry & token sets
│
├── core/
│   ├── models.py                 ← Dataclasses (LimitOrder, MarketState, etc.)
│   └── amm.py                    ← ConstantProductAMM price impact math
│
├── utils/
│   └── json_loader.py            ← .env loader, JSON helpers
│
├── data/
│   ├── programs.json             ← Known DEX / limit-order program IDs ← ADD THIS
│   ├── tokens.json               ← Known token labels (SOL, USDC, etc.)
│   ├── discriminators.json       ← Instruction discriminators
│   └── system_programs.json      ← System program addresses
│
└── runtime/                      ← Auto-created on first run
    ├── limit_orders.db           ← SQLite order tracker
    ├── learned_programs.json     ← Auto-learned DEX programs
    └── unknown_programs.jsonl    ← Unknown program log
```

---

## ⚙️ Environment Variables

### Required

| Variable | Description |
|---|---|
| `HELIUS_API_KEY` | Helius RPC/WebSocket API key — [helius.dev](https://helius.dev) |
| `DISCORD_TOKEN` | Discord bot token — [discord.com/developers](https://discord.com/developers/applications) |
| `DISCORD_CHANNEL` | Channel ID where whale/limit/price alerts are posted |
| `GROQ_API_KEY` | Groq API key for AI classification — [console.groq.com](https://console.groq.com) |

### Recommended

| Variable | Default | Description |
|---|---|---|
| `MINT` | *(hardcoded)* | Contract address of the token to monitor |
| `DEV_WALLET` | *(hardcoded)* | Dev wallet address to watch for sells |
| `WHALE_MIN_USD` | `500` | Minimum USD value to trigger a whale alert |

### Optional

| Variable | Default | Description |
|---|---|---|
| `DEBUG_CHANNEL_ID` | *(off)* | Channel for unknown program debug alerts |
| `DB_PATH` | `runtime/limit_orders.db` | SQLite database path |
| `PROGRAMS_FILE` | `data/programs.json` | Path to known programs registry |
| `LEARNED_PROGRAMS_FILE` | `runtime/learned_programs.json` | Auto-learn output path |
| `CHART_COOLDOWN_SECONDS` | `15` | Seconds between chart requests per channel |
| `GROQ_MODEL` | `llama-3.3-70b-versatile` | Groq model for TX classification & risk analysis |

### X / Twitter Watcher (Optional)

| Variable | Default | Description |
|---|---|---|
| `X_BEARER_TOKEN` | *(off)* | Twitter API v2 Bearer Token |
| `X_USERNAME` | *(off)* | Twitter handle to watch (without `@`) |
| `X_CHANNEL_ID` | *(off)* | Discord channel ID to post tweets to |
| `X_POLL_SECONDS` | `60` | How often to check for new tweets |
| `X_INCLUDE_REPLIES` | `false` | Include replies in the feed |
| `X_INCLUDE_RETWEETS` | `false` | Include retweets in the feed |

---

## 🏃 Running Locally

**1. Clone / download the project**

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Create your `.env` file**
```env
HELIUS_API_KEY=your_helius_key
DISCORD_TOKEN=your_discord_bot_token
DISCORD_CHANNEL=123456789012345678
GROQ_API_KEY=your_groq_key
MINT=YourTokenContractAddress
DEV_WALLET=DevWalletAddress
WHALE_MIN_USD=500
```

**4. Run**
```bash
python app.py
```

---

## 🚂 Deploying to Railway

**1.** Push your project to a GitHub repository

**2.** Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**

**3.** Add all environment variables in Railway dashboard → your service → **Variables**

**4.** Railway auto-detects Python, installs `requirements.txt`, and starts with `python app.py`

### ⚠️ Persistent Database (Important!)

Railway's filesystem resets on every redeploy. To keep your limit order history:

- Railway dashboard → your service → **Volumes** → **Add Volume**
- Mount path: `/app/runtime`
- Add env var: `DB_PATH=/app/runtime/limit_orders.db`

---

## 💬 Discord Commands

All commands use the `!` prefix in any channel the bot can read.

| Command | Description |
|---|---|
| `!price <CA>` | Price, market cap, 24h volume & change |
| `!whale <CA>` | Top 15 holders with concentration risk bar |
| `!chart <CA> [tf]` | Live candlestick chart — timeframes: `1m` `5m` `15m` `1h` `1d` |
| `!analyze <CA>` | Full AI risk analysis (rug score, red flags, trade advice) |
| `!order` | Live limit order book — support & resistance levels |
| `!help` | Show all commands |

---

## 🔍 How Transaction Classification Works

When a transaction involving your token is detected via Helius WebSocket, the bot runs it through a 3-stage classifier:

```
Stage 1 — Rule-based
  Checks program IDs against data/programs.json
  Reads token balance changes & instruction discriminators
  → Fast, no API cost

Stage 2 — Suspicion scorer
  Scores the tx on 8+ signals (unknown programs, account count, inner instructions, etc.)
  Only proceeds to Stage 3 if score ≥ threshold

Stage 3 — Groq AI (if enabled)
  Sends structured tx facts to Groq LLaMA-3
  Returns: order_type, confidence, exchange, size_usd
  → Auto-learns new program IDs with high confidence
```

**Order types detected:** `MARKET_BUY`, `MARKET_SELL`, `LIMIT_BUY`, `LIMIT_SELL`, `CANCEL_LIMIT`, `TRANSFER`

---

## 📦 Adding New DEX Programs

Edit `data/programs.json` and add an entry under `known_programs`:

```json
"ProgramAddressHere11111111111111111111111111": {
  "name": "My DEX",
  "role": "market"
}
```

Roles: `market` (spot swaps), `limit` (limit/DCA orders), `hybrid` (both)

Also add the address to `swap_programs` or `aggregator_programs` as appropriate.

The bot will also auto-learn unknown programs over time — check `runtime/learned_programs.json` after it runs for a while.

---

## 📝 Notes

- The bot runs as a **worker** (no HTTP server needed), making it ideal for Railway's worker deployments
- Groq AI is used for **both** transaction classification and the `!analyze` risk report command
- Chart images are generated with `matplotlib` — no external chart service required
- All Discord communication is done via raw Gateway WebSocket + REST API (no `discord.py` dependency)
