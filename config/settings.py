"""
Central settings module.
Loads .env, then exposes every constant the rest of the app needs.
Import everything with:  from config.settings import *
"""

import os
import re

from utils.json_loader import load_env

# Load .env first so all os.getenv() calls below see the values
load_env()

# ── Solana constants ─────────────────────────────────────────────────────────
WSOL_MINT         = "So11111111111111111111111111111111111111112"
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022    = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

# ── Token / wallet identities ────────────────────────────────────────────────
MINT       = os.getenv("MINT",       "9ezFthWrDUpSSeMdpLW6SDD9TJigHdc4AuQ5QN5bpump")
DEV_WALLET = os.getenv("DEV_WALLET", "6XjutcUVEidzb3o1yXLYGC2ZSnjde2YvAUF9CiPVqxwm")

# ── API keys / credentials ───────────────────────────────────────────────────
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
        raise ValueError(f"❌  {_name} environment variable is required!")

ALERT_CHANNEL_ID = int(DISCORD_CHANNEL)
DEBUG_CHANNEL_ID = int(DEBUG_CHANNEL) if DEBUG_CHANNEL and DEBUG_CHANNEL != "0" else None

# ── X / Twitter ──────────────────────────────────────────────────────────────
X_BEARER_TOKEN    = os.getenv("X_BEARER_TOKEN", "")
X_USERNAME        = os.getenv("X_USERNAME", "").strip().lstrip("@")
X_CHANNEL_ID      = int(os.getenv("X_CHANNEL_ID", "0")) if os.getenv("X_CHANNEL_ID") else 0
X_POLL_SECONDS    = int(os.getenv("X_POLL_SECONDS", "60"))
X_INCLUDE_REPLIES  = os.getenv("X_INCLUDE_REPLIES",  "false").lower() == "true"
X_INCLUDE_RETWEETS = os.getenv("X_INCLUDE_RETWEETS", "false").lower() == "true"
X_API_BASE        = os.getenv("X_API_BASE", "https://api.x.com/2").rstrip("/")

# ── File paths ───────────────────────────────────────────────────────────────
DB_PATH              = os.getenv("DB_PATH",              "runtime/limit_orders.db")
PROGRAMS_FILE        = os.getenv("PROGRAMS_FILE",        "data/programs.json")
LEARNED_PROGRAMS_FILE = os.getenv("LEARNED_PROGRAMS_FILE", "runtime/learned_programs.json")

# ── Thresholds & intervals ───────────────────────────────────────────────────
WHALE_MIN_USD          = int(os.getenv("WHALE_MIN_USD", "500"))
GROQ_MODEL             = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_MIN_CONFIDENCE    = 0.65
GROQ_ENABLED           = bool(GROQ_API_KEY)
SUSPICION_THRESHOLD    = 0.25
SUMMARY_ALERT_INTERVAL = 7200       # seconds between order-book snapshots
PRICE_UPDATE_INTERVAL  = 30         # seconds between DexScreener polls
PRICE_ALERT_THRESHOLD  = 5.0        # percent
PRICE_ALERT_COOLDOWN   = 300        # seconds
CLEANUP_INTERVAL       = 3600       # seconds between DB cleanups
ORDER_TTL_HOURS        = 168        # 7 days
ORDER_TTL_SECS         = ORDER_TTL_HOURS * 3600
ALERT_CHANNEL_ID       = 1483822900795670678

CHART_COOLDOWN_SECONDS           = int(os.getenv("CHART_COOLDOWN_SECONDS", "15"))
CHART_WAIT_MESSAGE_DELETE_SECONDS = int(os.getenv("CHART_WAIT_DELETE_SECONDS", "8"))

# ── Endpoint URLs ────────────────────────────────────────────────────────────
WS_URL      = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
RPC_URL     = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
DISCORD_API = "https://discord.com/api/v10"
GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
GROQ_URL    = "https://api.groq.com/openai/v1/chat/completions"

# ── Regex helpers ─────────────────────────────────────────────────────────────
VALID_CA = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")