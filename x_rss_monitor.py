from __future__ import annotations

import asyncio
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx

from config.settings import ALERT_CHANNEL_ID
from helpers.database import DatabaseManager
from helpers.discord_utils import send_message, send_message_with_image
from helpers.formatters import get_timestamp

# ── Channel config ─────────────────────────────────────────────────────────────

# Where XerisCoin's own posts go (your X announcements channel)
X_ANNOUNCE_CHANNEL_ID: int = 1483822900795670678   # ← change to your announcements channel ID

# Where raided/watched accounts' posts go (separate raid/alpha channel)
RAID_CHANNEL_ID: int = 1481659347460161607          # ← change to your raid channel ID

# ── Account config ─────────────────────────────────────────────────────────────

# The hardcoded default — always seeded, cannot be removed, posts to X_ANNOUNCE_CHANNEL_ID
DEFAULT_X_ACCOUNT = "XerisCoin"

# How often to poll each account (seconds)
POLL_INTERVAL = 90

# Max total watched accounts (including the default)
MAX_ACCOUNTS = 3

# ── RSS sources ────────────────────────────────────────────────────────────────

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

# Your self-hosted RSSHub instance — set "" to skip
RSSHUB_INSTANCE = "https://rsshub.app"

# ═════════════════════════════════════════════════════════════════════════════
# RSS fetching
# ═════════════════════════════════════════════════════════════════════════════

def _strip_at(username: str) -> str:
    return username.lstrip("@").strip().lower()


async def _try_url(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 XerisBot/2.0"})
        if r.status_code == 200 and ("<rss" in r.text or "<feed" in r.text):
            return r.text
    except Exception:
        pass
    return None


async def _fetch_rss(username: str) -> Optional[str]:
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        for base in NITTER_INSTANCES:
            result = await _try_url(client, f"{base}/{username}/rss")
            if result:
                print(f"   ✅ RSS via {base}")
                return result
        if RSSHUB_INSTANCE:
            result = await _try_url(client, f"{RSSHUB_INSTANCE}/twitter/user/{username}")
            if result:
                print(f"   ✅ RSS via RSSHub")
                return result
    print(f"   ⚠️ All RSS sources failed for @{username} — will retry next cycle")
    return None


# ═════════════════════════════════════════════════════════════════════════════
# RSS parsing
# ═════════════════════════════════════════════════════════════════════════════

def _parse_timestamp(pub_str: str) -> int:
    """Try multiple date formats used by nitter/RSSHub."""
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",   # nitter:  Mon, 01 Jan 2024 12:00:00 +0000
        "%a, %d %b %Y %H:%M:%S GMT",  # some instances use GMT literally
        "%Y-%m-%dT%H:%M:%S%z",        # RSSHub Atom
        "%Y-%m-%dT%H:%M:%SZ",         # ISO 8601 UTC
    ]
    for fmt in formats:
        try:
            return int(datetime.strptime(pub_str.strip(), fmt).timestamp())
        except Exception:
            continue
    return 0


def _clean_content(raw: str) -> str:
    """Strip HTML tags and clean up whitespace from RSS content."""
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", "", raw)
    # Decode common HTML entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_rss_posts(xml_text: str) -> List[Dict]:
    posts = []
    try:
        root  = ET.fromstring(xml_text)
        ns    = {"media": "http://search.yahoo.com/mrss/",
                 "content": "http://purl.org/rss/1.0/modules/content/"}
        items = root.findall(".//item")
        for item in items:
            guid    = (item.findtext("guid")    or "").strip()
            title   = (item.findtext("title")   or "").strip()
            link    = (item.findtext("link")    or "").strip()
            pub_str = (item.findtext("pubDate") or "").strip()

            # Get full post content — prefer content:encoded, fallback to description, then title
            content_encoded = item.findtext("content:encoded", namespaces=ns) or ""
            description     = item.findtext("description") or ""
            raw_content     = content_encoded or description or title
            content         = _clean_content(raw_content)

            # If content is empty or just a URL, fall back to title
            if not content or content.startswith("http"):
                content = _clean_content(title)

            # Trim to 500 chars
            if len(content) > 500:
                content = content[:497] + "…"

            # Extract image
            media_content = item.find("media:content", ns)
            image_url = media_content.get("url") if media_content is not None else None

            if not guid:
                continue

            ts = _parse_timestamp(pub_str)

            # Extract numeric post ID from guid or link (most reliable unique key)
            post_id = ""
            m = re.search(r"/status/(\d+)", guid + " " + link)
            if m:
                post_id = m.group(1)
            else:
                # fallback: use full guid
                post_id = guid

            posts.append({
                "post_id":   post_id,
                "content":   content,
                "link":      link,
                "timestamp": ts,
                "image_url": image_url,
            })
    except Exception as e:
        print(f"   ⚠️ RSS parse error: {e}")

    posts.sort(key=lambda p: p["timestamp"])
    return posts


# ═════════════════════════════════════════════════════════════════════════════
# Embed builder
# ═════════════════════════════════════════════════════════════════════════════

def _build_post_embed(username: str, post: Dict, is_default: bool = False) -> dict:
    content = post["content"]
    link    = post["link"]
    ts      = post["timestamp"]
    dt_str  = (datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
               if ts else "Unknown")

    color  = 0x10B981 if is_default else 0x1D9BF0
    prefix = "📢" if is_default else "🎯"

    return {
        "author":      {"name": f"{prefix} @{username} posted on X", "url": f"https://x.com/{username}"},
        "description": f"{content}\n\n🔗 {link}",
        "color":       color,
        "footer":      {"text": f"X Monitor · @{username} · {dt_str}"},
        "timestamp":   get_timestamp(),
    }


# ═════════════════════════════════════════════════════════════════════════════
# Per-account polling
# ═════════════════════════════════════════════════════════════════════════════

def _resolve_channel(row: Dict) -> int:
    """
    Pick the right Discord channel for this account:
      - Stored channel_id in DB (set via !raid @user <channel_id>) → use that
      - Default account                                             → X_ANNOUNCE_CHANNEL_ID
      - Any raided account                                          → RAID_CHANNEL_ID
    """
    stored = row.get("channel_id")
    if stored and int(stored) > 0:
        return int(stored)
    if row["username"].lower() == DEFAULT_X_ACCOUNT.lower():
        return X_ANNOUNCE_CHANNEL_ID
    return RAID_CHANNEL_ID


async def _check_account(row: Dict, db: DatabaseManager) -> None:
    username   = row["username"]
    channel_id = _resolve_channel(row)
    is_default = username.lower() == DEFAULT_X_ACCOUNT.lower()

    xml_text = await _fetch_rss(username)
    if not xml_text:
        return

    posts = _parse_rss_posts(xml_text)
    if not posts:
        return

    state        = await db.get_x_watch_state(username)
    last_post_id = (state or {}).get("last_post_id", "")
    latest       = posts[-1]

    # First run or no saved state — just save the latest post ID, send nothing
    if not last_post_id:
        print(f"   📌 First run for @{username} — saving latest post ID, no alert sent")
        await db.upsert_x_watch_state(
            username=username,
            user_id="",
            last_post_id=latest["post_id"],
            last_post_time=str(latest["timestamp"]),
        )
        return

    # Already up to date
    if latest["post_id"] == last_post_id:
        return

    # Collect only posts newer than the last saved one
    new_posts = []
    for p in posts:
        if p["post_id"] == last_post_id:
            break
        new_posts.append(p)

    if not new_posts:
        # IDs don't match but nothing found — update to latest to avoid re-checking
        await db.upsert_x_watch_state(
            username=username,
            user_id="",
            last_post_id=latest["post_id"],
            last_post_time=str(latest["timestamp"]),
        )
        return

    # Save state first before sending (prevents re-alerting on crash)
    await db.upsert_x_watch_state(
        username=username,
        user_id="",
        last_post_id=latest["post_id"],
        last_post_time=str(latest["timestamp"]),
    )

    print(f"   📢 {len(new_posts)} new post(s) from @{username} → ch:{channel_id}")

    # Send newest last, cap at 3 to avoid spam
    for post in new_posts[-3:]:
        embed = _build_post_embed(username, post, is_default=is_default)
        if post.get("image_url"):
            await send_message_with_image(
                channel_id,
                content=f"{'📢' if is_default else '🎯'} **@{username}** • {post['content'][:200]}",
                image_url=post["image_url"],
            )
        else:
            await send_message(channel_id, embeds=[embed])
        await asyncio.sleep(1.5)


# ═════════════════════════════════════════════════════════════════════════════
# Main monitor coroutine
# ═════════════════════════════════════════════════════════════════════════════

async def x_post_monitor(db: DatabaseManager) -> None:
    print("\n🐦 Starting X post monitor...")

    # Seed the default account
    existing = await db.get_x_watch_state(DEFAULT_X_ACCOUNT.lower())
    if not existing:
        await db.add_x_watch(DEFAULT_X_ACCOUNT.lower(), added_by="system")
        print(f"   ✅ Seeded default X account: @{DEFAULT_X_ACCOUNT} → ch:{X_ANNOUNCE_CHANNEL_ID}")

    print(f"   📡 Polling every {POLL_INTERVAL}s · max {MAX_ACCOUNTS} accounts")
    print(f"   📣 Default channel : {X_ANNOUNCE_CHANNEL_ID}")
    print(f"   🎯 Raid channel    : {RAID_CHANNEL_ID}")

    while True:
        try:
            accounts = await db.get_all_x_watched()
            for row in accounts:
                try:
                    await _check_account(row, db)
                except Exception as e:
                    print(f"   ❌ Error checking @{row['username']}: {e}")
                await asyncio.sleep(3)
        except Exception as e:
            print(f"❌ X monitor error: {e}")
        await asyncio.sleep(POLL_INTERVAL)


# ═════════════════════════════════════════════════════════════════════════════
# Raid command handler
# ═════════════════════════════════════════════════════════════════════════════

async def handle_raid_command(
    command: str,
    parts: List[str],
    channel_id: int,
    author: dict,
    db: DatabaseManager,
) -> None:
    if command == "!raidlist":
        await _cmd_raidlist(channel_id, db)

    elif command == "!raid":
        raw_arg = parts[1].strip() if len(parts) > 1 else ""
        username = _strip_at(raw_arg)
        if not username or not re.match(r"^[a-zA-Z0-9_]{1,50}$", username):
            await send_message(channel_id, content=(
                "❌ Usage: `!raid @username [channel_id]`\n"
                "Example: `!raid @elonmusk` — posts go to the default raid channel\n"
                "Example: `!raid @elonmusk 1234567890` — posts go to that specific channel"
            ))
            return
        # Optional channel_id argument
        target_channel = None
        if len(parts) > 2:
            try:
                target_channel = int(parts[2].strip().lstrip("#<@>!&"))
            except ValueError:
                await send_message(channel_id, content="❌ Invalid channel ID. Use the numeric channel ID.")
                return
        await _cmd_raid_add(channel_id, username, author.get("username", "Unknown"), db, target_channel)

    elif command == "!unraid":
        raw_arg = parts[1].strip() if len(parts) > 1 else ""
        username = _strip_at(raw_arg)
        if not username:
            await send_message(channel_id, content="❌ Usage: `!unraid @username`")
            return
        await _cmd_raid_remove(channel_id, username, db)


# ── !raidlist ──────────────────────────────────────────────────────────────────

async def _cmd_raidlist(channel_id: int, db: DatabaseManager) -> None:
    accounts = await db.get_all_x_watched()
    if not accounts:
        await send_message(channel_id, embeds=[{
            "title":       "📋 X Watch List",
            "description": "> No accounts are currently being monitored.",
            "color":       0x6B7280,
        }])
        return

    rows = []
    for i, row in enumerate(accounts):
        uname      = row["username"]
        is_default = uname.lower() == DEFAULT_X_ACCOUNT.lower()
        ch         = _resolve_channel(row)
        lock       = " 🔒" if is_default else ""
        ch_label   = "📢 Announcements" if ch == X_ANNOUNCE_CHANNEL_ID else f"🎯 <#{ch}>"
        added_by   = row.get("added_by", "system")
        source     = "system default" if is_default else f"added by @{added_by}"
        rows.append(f"`{i+1}.` **@{uname}**{lock} → {ch_label} — {source}")

    await send_message(channel_id, embeds=[{
        "author":      {"name": "🐦 X Account Watch List"},
        "title":       f"{len(accounts)}/{MAX_ACCOUNTS} slots used",
        "description": "\n".join(rows),
        "color":       0x1D9BF0,
        "fields": [
            {"name": "📢 Announce Channel", "value": f"`{X_ANNOUNCE_CHANNEL_ID}`", "inline": True},
            {"name": "🎯 Raid Channel",     "value": f"`{RAID_CHANNEL_ID}`",       "inline": True},
            {"name": "ℹ️ Info",
             "value": (
                 f"• 🔒 Default account always posts to announcements\n"
                 f"• New raids post to raid channel by default\n"
                 f"• `!raid @user <channel_id>` to post to a specific channel\n"
                 f"• `!unraid @user` to remove"
             ), "inline": False},
        ],
        "footer":    {"text": f"X Monitor · polls every {POLL_INTERVAL}s"},
        "timestamp": get_timestamp(),
    }])


# ── !raid add ──────────────────────────────────────────────────────────────────

async def _cmd_raid_add(
    channel_id: int,
    username: str,
    added_by: str,
    db: DatabaseManager,
    target_channel: Optional[int] = None,
) -> None:
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
            "color": 0xEF4444,
        }])
        return

    existing = await db.get_x_watch_state(username)
    if existing:
        await send_message(channel_id, embeds=[{
            "title":       "⚠️ Already Watching",
            "description": f"`@{username}` is already on the watch list.",
            "color":       0xF59E0B,
        }])
        return

    await send_message(channel_id, content=f"🔍 Verifying `@{username}` exists on X...")
    xml_text = await _fetch_rss(username)
    if not xml_text:
        await send_message(channel_id, embeds=[{
            "title":       "❌ Account Not Found",
            "description": (
                f"Could not find RSS feed for `@{username}`.\n"
                "Make sure the handle is correct and the account is public."
            ),
            "color": 0xEF4444,
        }])
        return

    # Determine destination channel
    dest_channel = target_channel or RAID_CHANNEL_ID
    ch_label     = f"<#{dest_channel}> (`{dest_channel}`)"

    await db.add_x_watch(username, added_by=added_by, channel_id=dest_channel)
    count_after = await db.count_x_watched()

    await send_message(channel_id, embeds=[{
        "author":      {"name": "✅ X Account Added"},
        "title":       f"@{username} is now being monitored",
        "description": (
            f"> New posts from **@{username}** will be sent to {ch_label}\n\n"
            f"🔗 [View Profile](https://x.com/{username})"
        ),
        "color":  0x10B981,
        "fields": [
            {"name": "👤 Added By",      "value": f"`@{added_by}`",                  "inline": True},
            {"name": "📊 Slots Used",    "value": f"`{count_after}/{MAX_ACCOUNTS}`", "inline": True},
            {"name": "📣 Posts Go To",   "value": ch_label,                          "inline": True},
        ],
        "footer":    {"text": "Use !raidlist to see all watched accounts"},
        "timestamp": get_timestamp(),
    }])


# ── !unraid remove ─────────────────────────────────────────────────────────────

async def _cmd_raid_remove(channel_id: int, username: str, db: DatabaseManager) -> None:
    if username.lower() == DEFAULT_X_ACCOUNT.lower():
        await send_message(channel_id, embeds=[{
            "title":       "🔒 Cannot Remove Default Account",
            "description": f"`@{DEFAULT_X_ACCOUNT}` is the project's default and cannot be removed.",
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
            {"name": "📊 Slots Used",  "value": f"`{count_after}/{MAX_ACCOUNTS}`", "inline": True},
            {"name": "➕ Add Another", "value": "Use `!raid @username`",            "inline": True},
        ],
        "footer":    {"text": "Use !raidlist to see all watched accounts"},
        "timestamp": get_timestamp(),
    }])
