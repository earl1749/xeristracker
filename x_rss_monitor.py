"""
x_rss_monitor.py
────────────────
Monitors X (Twitter) accounts for new posts and relays them to Discord.

Accounts are stored in the x_watch_state table (max 3 total).
A single default account from settings is always seeded on startup.

Discord commands (handled via handle_raid_command):
  !raid @username   — add an account to watch (max 3)
  !unraid @username — remove a watched account (default account cannot be removed)
  !raidlist         — list all currently watched accounts
"""

from __future__ import annotations

import asyncio
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx

from config.settings import ALERT_CHANNEL_ID, DISCORD_TOKEN
from helpers.database import DatabaseManager
from helpers.discord_utils import send_message, send_message_with_image
from helpers.formatters import get_timestamp

# ── Config ────────────────────────────────────────────────────────────────────

# The one hardcoded default account that is always watched and cannot be removed.
# Change this to your project's X handle (without @).
DEFAULT_X_ACCOUNT = "XerisCoin"

# How often to poll each account (seconds)
POLL_INTERVAL = 90

# Max total watched accounts (including the default)
MAX_ACCOUNTS = 3

# Nitter instances to try in order (public RSS proxy for X).
# These rotate frequently — update if all fail.
NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
    "https://nitter.catsarch.com",
    "https://nitter.unixfox.eu",
    "https://nitter.moomoo.me",
    "https://nitter.esmailelbob.xyz",
    "https://nitter.tiekoetter.com",
    "https://nitter.42l.fr",
]

# RSSHub fallback — reliable alternative to nitter.
# Use the public demo (rate-limited) or set "" to skip.
RSSHUB_INSTANCE = "rsshub-production-69fe.up.railway.app"

# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_at(username: str) -> str:
    """Normalize @username or username → lowercase username."""
    return username.lstrip("@").strip().lower()


async def _try_url(client: httpx.AsyncClient, url: str) -> Optional[str]:
    """Fetch a single URL, return text if it looks like RSS/Atom, else None."""
    try:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 XerisBot/2.0"})
        if r.status_code == 200 and ("<rss" in r.text or "<feed" in r.text):
            return r.text
    except Exception:
        pass
    return None


async def _fetch_rss(username: str) -> Optional[str]:
    """
    Try multiple RSS sources in order:
      1. Each nitter instance  (/<username>/rss)
      2. RSSHub                (/twitter/user/<username>)
    Returns the first valid feed text, or None if all fail.
    """
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        # 1. Nitter instances
        for base in NITTER_INSTANCES:
            result = await _try_url(client, f"{base}/{username}/rss")
            if result:
                print(f"   ✅ RSS via {base}")
                return result

        # 2. RSSHub fallback
        if RSSHUB_INSTANCE:
            result = await _try_url(client, f"{RSSHUB_INSTANCE}/twitter/user/{username}")
            if result:
                print(f"   ✅ RSS via RSSHub")
                return result

    print(f"   ⚠️ All RSS sources failed for @{username} — will retry next cycle")
    return None


def _parse_rss_posts(xml_text: str) -> List[Dict]:
    """Parse nitter RSS → list of post dicts sorted oldest-first."""
    posts = []
    try:
        root  = ET.fromstring(xml_text)
        ns    = {"media": "http://search.yahoo.com/mrss/"}
        items = root.findall(".//item")
        for item in items:
            guid    = (item.findtext("guid") or "").strip()
            title   = (item.findtext("title") or "").strip()
            link    = (item.findtext("link")  or "").strip()
            pub_str = (item.findtext("pubDate") or "").strip()
            # Extract image if present
            media_content = item.find("media:content", ns)
            image_url = None
            if media_content is not None:
                image_url = media_content.get("url")

            if not guid:
                continue

            # Parse timestamp
            ts = 0
            try:
                dt = datetime.strptime(pub_str, "%a, %d %b %Y %H:%M:%S %z")
                ts = int(dt.timestamp())
            except Exception:
                pass

            # Extract post ID from guid / link
            post_id = ""
            m = re.search(r"/status/(\d+)", guid + link)
            if m:
                post_id = m.group(1)

            posts.append({
                "post_id":   post_id or guid,
                "title":     title,
                "link":      link,
                "timestamp": ts,
                "image_url": image_url,
            })
    except Exception as e:
        print(f"   ⚠️ RSS parse error: {e}")

    # Sort oldest → newest
    posts.sort(key=lambda p: p["timestamp"])
    return posts


def _build_post_embed(username: str, post: Dict) -> dict:
    title   = post["title"]
    link    = post["link"]
    ts      = post["timestamp"]
    dt_str  = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if ts else "Unknown"

    # Trim overly long tweet text
    if len(title) > 280:
        title = title[:277] + "…"

    embed = {
        "author":      {"name": f"🐦 @{username} posted on X", "url": f"https://x.com/{username}"},
        "description": title,
        "url":         link,
        "color":       0x1D9BF0,  # X blue
        "fields": [
            {"name": "🔗 View Post", "value": f"[Open on X]({link})", "inline": True},
            {"name": "🕐 Posted",    "value": f"`{dt_str}`",          "inline": True},
        ],
        "footer":    {"text": f"X Monitor · @{username}"},
        "timestamp": get_timestamp(),
    }

    if post.get("image_url"):
        embed["image"] = {"url": post["image_url"]}

    return embed


# ═════════════════════════════════════════════════════════════════════════════
# Per-account polling
# ═════════════════════════════════════════════════════════════════════════════

async def _check_account(username: str, db: DatabaseManager) -> None:
    """Fetch latest posts for one account and alert if new."""
    xml_text = await _fetch_rss(username)
    if not xml_text:
        print(f"   ⚠️ RSS unavailable for @{username}")
        return

    posts = _parse_rss_posts(xml_text)
    if not posts:
        return

    state        = await db.get_x_watch_state(username)
    last_post_id = (state or {}).get("last_post_id", "")

    new_posts = []
    if last_post_id:
        for p in posts:
            if p["post_id"] == last_post_id:
                break
            new_posts.append(p)
    else:
        # First run: save latest and don't spam
        new_posts = []

    latest = posts[-1]

    # Save state regardless
    await db.upsert_x_watch_state(
        username=username,
        user_id="",
        last_post_id=latest["post_id"],
        last_post_time=str(latest["timestamp"]),
    )

    if not new_posts:
        return

    print(f"   📢 {len(new_posts)} new post(s) from @{username}")

    for post in new_posts[-5:]:  # safety: cap at 5 per cycle to avoid spam
        embed = _build_post_embed(username, post)
        if post.get("image_url"):
            await send_message_with_image(
                ALERT_CHANNEL_ID,
                content=f"📣 New post from **@{username}**",
                image_url=post["image_url"],
            )
        else:
            await send_message(ALERT_CHANNEL_ID, embeds=[embed])
        await asyncio.sleep(1)  # slight delay between posts


# ═════════════════════════════════════════════════════════════════════════════
# Main monitor coroutine (called from xeris.py main)
# ═════════════════════════════════════════════════════════════════════════════

async def x_post_monitor(db: DatabaseManager) -> None:
    """
    Background task: ensures the default account is seeded, then polls
    all watched accounts in a round-robin loop every POLL_INTERVAL seconds.
    """
    print("\n🐦 Starting X post monitor...")

    # Seed the default account if not already present
    existing = await db.get_x_watch_state(DEFAULT_X_ACCOUNT.lower())
    if not existing:
        await db.add_x_watch(DEFAULT_X_ACCOUNT.lower(), added_by="system")
        print(f"   ✅ Seeded default X account: @{DEFAULT_X_ACCOUNT}")

    print(f"   📡 Polling every {POLL_INTERVAL}s · max {MAX_ACCOUNTS} accounts")

    while True:
        try:
            accounts = await db.get_all_x_watched()
            if not accounts:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            for row in accounts:
                username = row["username"]
                try:
                    await _check_account(username, db)
                except Exception as e:
                    print(f"   ❌ Error checking @{username}: {e}")
                await asyncio.sleep(3)  # brief pause between accounts

        except Exception as e:
            print(f"❌ X monitor error: {e}")

        await asyncio.sleep(POLL_INTERVAL)


# ═════════════════════════════════════════════════════════════════════════════
# Raid command handler (called from commands/bot_commands.py)
# ═════════════════════════════════════════════════════════════════════════════

async def handle_raid_command(
    command: str,
    parts: List[str],
    channel_id: int,
    author: dict,
    db: DatabaseManager,
) -> None:
    """
    Routes !raid / !unraid / !raidlist to the appropriate handler.
    Called by handle_message in commands/bot_commands.py.
    """
    if command == "!raidlist":
        await _cmd_raidlist(channel_id, db)

    elif command == "!raid":
        raw_arg = parts[1].strip() if len(parts) > 1 else ""
        username = _strip_at(raw_arg)
        if not username or not re.match(r"^[a-zA-Z0-9_]{1,50}$", username):
            await send_message(channel_id, content="❌ Usage: `!raid @username`\nProvide a valid X username.")
            return
        await _cmd_raid_add(channel_id, username, author.get("username", "Unknown"), db)

    elif command == "!unraid":
        raw_arg = parts[1].strip() if len(parts) > 1 else ""
        username = _strip_at(raw_arg)
        if not username:
            await send_message(channel_id, content="❌ Usage: `!unraid @username`")
            return
        await _cmd_raid_remove(channel_id, username, db)


# ── !raidlist ─────────────────────────────────────────────────────────────────

async def _cmd_raidlist(channel_id: int, db: DatabaseManager) -> None:
    accounts = await db.get_all_x_watched()
    if not accounts:
        await send_message(channel_id, embeds=[{
            "title":       "📋 X Watch List",
            "description": "> No accounts are currently being monitored.",
            "color":       0x6B7280,
        }])
        return

    slots_used = len(accounts)
    rows       = []
    for i, row in enumerate(accounts):
        uname    = row["username"]
        added_by = row.get("added_by", "system")
        is_default = uname.lower() == DEFAULT_X_ACCOUNT.lower()
        lock   = " 🔒" if is_default else ""
        source = "system default" if is_default else f"added by @{added_by}"
        rows.append(f"`{i+1}.` **@{uname}**{lock} — {source}")

    await send_message(channel_id, embeds=[{
        "author":      {"name": "🐦 X Account Watch List"},
        "title":       f"{slots_used}/{MAX_ACCOUNTS} slots used",
        "description": "\n".join(rows),
        "color":       0x1D9BF0,
        "fields": [
            {"name": "ℹ️ Info",
             "value": (
                 f"• Max **{MAX_ACCOUNTS}** accounts total\n"
                 f"• 🔒 Default account cannot be removed\n"
                 f"• Use `!raid @username` to add · `!unraid @username` to remove"
             ), "inline": False},
        ],
        "footer":    {"text": f"X Monitor · polls every {POLL_INTERVAL}s"},
        "timestamp": get_timestamp(),
    }])


# ── !raid add ─────────────────────────────────────────────────────────────────

async def _cmd_raid_add(
    channel_id: int, username: str, added_by: str, db: DatabaseManager
) -> None:
    # Check current count
    count = await db.count_x_watched()
    if count >= MAX_ACCOUNTS:
        accounts = await db.get_all_x_watched()
        names    = ", ".join(f"@{r['username']}" for r in accounts)
        await send_message(channel_id, embeds=[{
            "title":       "❌ Watch List Full",
            "description": (
                f"Maximum of **{MAX_ACCOUNTS}** accounts reached.\n\n"
                f"**Currently watching:** {names}\n\n"
                f"Use `!unraid @username` to free a slot first."
            ),
            "color":       0xEF4444,
        }])
        return

    # Check if already watched
    existing = await db.get_x_watch_state(username)
    if existing:
        await send_message(channel_id, embeds=[{
            "title":       "⚠️ Already Watching",
            "description": f"`@{username}` is already on the watch list.",
            "color":       0xF59E0B,
        }])
        return

    # Validate the account exists by fetching RSS
    await send_message(channel_id, content=f"🔍 Verifying `@{username}` exists on X...")
    xml_text = await _fetch_rss(username)
    if not xml_text:
        await send_message(channel_id, embeds=[{
            "title":       "❌ Account Not Found",
            "description": (
                f"Could not find RSS feed for `@{username}`.\n\n"
                "Make sure the handle is correct and the account is public."
            ),
            "color":       0xEF4444,
        }])
        return

    # Add to DB
    await db.add_x_watch(username, added_by=added_by)
    count_after = await db.count_x_watched()

    await send_message(channel_id, embeds=[{
        "author":      {"name": "✅ X Account Added"},
        "title":       f"@{username} is now being monitored",
        "description": (
            f"> New posts from **@{username}** will be relayed here automatically.\n\n"
            f"🔗 [View Profile](https://x.com/{username})"
        ),
        "color":       0x10B981,
        "fields": [
            {"name": "👤 Added By",   "value": f"`@{added_by}`",                    "inline": True},
            {"name": "📊 Slots Used", "value": f"`{count_after}/{MAX_ACCOUNTS}`",   "inline": True},
            {"name": "⏱️ Poll Rate",  "value": f"every `{POLL_INTERVAL}s`",         "inline": True},
        ],
        "footer":    {"text": "Use !raidlist to see all watched accounts"},
        "timestamp": get_timestamp(),
    }])


# ── !unraid remove ────────────────────────────────────────────────────────────

async def _cmd_raid_remove(channel_id: int, username: str, db: DatabaseManager) -> None:
    # Protect the default account
    if username.lower() == DEFAULT_X_ACCOUNT.lower():
        await send_message(channel_id, embeds=[{
            "title":       "🔒 Cannot Remove Default Account",
            "description": f"`@{DEFAULT_X_ACCOUNT}` is the project's default monitored account and cannot be removed.",
            "color":       0xF59E0B,
        }])
        return

    removed = await db.remove_x_watch(username)
    if not removed:
        await send_message(channel_id, embeds=[{
            "title":       "❌ Not Found",
            "description": f"`@{username}` is not on the current watch list.",
            "color":       0xEF4444,
        }])
        return

    count_after = await db.count_x_watched()
    await send_message(channel_id, embeds=[{
        "author":      {"name": "🗑️ X Account Removed"},
        "title":       f"@{username} has been removed",
        "description": f"> Posts from **@{username}** will no longer be monitored.",
        "color":       0x6B7280,
        "fields": [
            {"name": "📊 Slots Used",  "value": f"`{count_after}/{MAX_ACCOUNTS}`",  "inline": True},
            {"name": "➕ Add Another", "value": "Use `!raid @username`",             "inline": True},
        ],
        "footer":    {"text": "Use !raidlist to see all watched accounts"},
        "timestamp": get_timestamp(),
    }])
