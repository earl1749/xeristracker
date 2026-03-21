from __future__ import annotations

import asyncio
import os as _os
import re
import xml.etree.ElementTree as ET
from collections import deque
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx

from config.settings import (
    X_USERNAME,
    X_CHANNEL_ID,
    X_POLL_SECONDS,
    X_INCLUDE_REPLIES,
    X_INCLUDE_RETWEETS,
)


# ── RSS sources ───────────────────────────────────────────────────────────────

_RSSHUB_SELF = _os.getenv("RSSHUB_URL", "").rstrip("/")

_RSS_SOURCES = [
    *([f"{_RSSHUB_SELF}/twitter/user/{{username}}"] if _RSSHUB_SELF else []),
    "https://rsshub.app/twitter/user/{username}",
    "https://nitter.privacydev.net/{username}/rss",
    "https://nitter.poast.org/{username}/rss",
    "https://nitter.net/{username}/rss",
]

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; XerisBot/2.0; RSS reader)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _numeric_id(value: str) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _pick_newest(items: list[dict]) -> Optional[dict]:
    if not items:
        return None
    return max(
        items,
        key=lambda p: (_numeric_id(p.get("id", "")), p.get("timestamp", 0.0)),
    )


# ── RSS fetch ────────────────────────────────────────────────────────────────

async def _fetch_rss(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.get(url)

        if r.status_code == 200 and "<item" in r.text:
            return r.text

        print(f"   ⚠️  RSS {r.status_code} from {url}")
        return None

    except Exception as e:
        host = url.split("/")[2] if "://" in url else url
        print(f"   ⚠️  RSS error ({host}): {e}")
        return None


async def _fetch_with_fallback(client: httpx.AsyncClient, username: str) -> Optional[str]:
    for template in _RSS_SOURCES:
        url = template.format(username=username)
        xml = await _fetch_rss(client, url)
        if xml:
            print(f"   ✅  RSS via {url.split('/')[2]}")
            return xml

    print(f"   ❌  All RSS sources failed for @{username}")
    return None


# ── RSS parser ───────────────────────────────────────────────────────────────

def _parse_items(xml_text: str) -> list[dict]:
    items: list[dict] = []

    try:
        root = ET.fromstring(xml_text)
        channel = root.find("channel") or root

        for item in channel.findall("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            guid = (item.findtext("guid") or link).strip()

            tweet_id = ""
            for src in (link, guid):
                m = re.search(r"/status(?:es)?/(\d+)", src)
                if m:
                    tweet_id = m.group(1)
                    break

            if not tweet_id:
                tweet_id = re.sub(r"[^0-9a-zA-Z]", "", guid)[-20:]

            created_at = ""
            ts = 0.0

            if pub_date:
                try:
                    dt = parsedate_to_datetime(pub_date)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    created_at = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    ts = dt.timestamp()
                except Exception:
                    pass

            is_retweet = title.startswith("RT @")
            is_reply = (
                title.startswith("R to @")
                or title.startswith("Replying to @")
                or (title.startswith("@") and not title.startswith(f"@{X_USERNAME}"))
            )

            items.append(
                {
                    "id": tweet_id,
                    "text": title,
                    "url": link,
                    "created_at": created_at,
                    "timestamp": ts,
                    "is_retweet": is_retweet,
                    "is_reply": is_reply,
                }
            )

    except ET.ParseError as e:
        print(f"   ⚠️  RSS XML parse error: {e}")

    return items


def _should_skip(item: dict) -> bool:
    if item.get("is_retweet") and not X_INCLUDE_RETWEETS:
        return True
    if item.get("is_reply") and not X_INCLUDE_REPLIES:
        return True
    return False


def _id_gt(a: str, b: str) -> bool:
    try:
        return int(a) > int(b)
    except Exception:
        return a > b


# ── Discord embed ────────────────────────────────────────────────────────────

def _build_kol_embed(username: str, item: dict) -> dict:
    # Import inside function to break circular import
    from xeris import get_timestamp

    text = (item.get("text") or "").strip()
    url = item.get("url") or f"https://x.com/{username}"

    if item.get("is_retweet"):
        label = "🔁 Reposted"
        color = 0x1D9BF0
    elif item.get("is_reply"):
        label = "💬 Replied"
        color = 0x6366F1
    else:
        label = "📢 New Post"
        color = 0x1D9BF0

    return {
        "author": {
            "name": f"{label} · @{username}",
            "url": f"https://x.com/{username}",
            "icon_url": "https://abs.twimg.com/favicons/twitter.3.ico",
        },
        "description": text[:4000] if text else "*No text*",
        "color": color,
        "fields": [
            {
                "name": "🔗",
                "value": f"[Open on X]({url}) · [Profile](https://x.com/{username})",
                "inline": False,
            }
        ],
        "footer": {"text": f"X Watcher · @{username} · {item.get('created_at', '')}"},
        "timestamp": get_timestamp(),
    }


# ── Main monitor ─────────────────────────────────────────────────────────────

async def x_post_monitor(db) -> None:
    # Import inside function to break circular import
    from xeris import send_message, get_timestamp

    if not X_USERNAME or not X_CHANNEL_ID:
        print("ℹ️  X watcher disabled — set X_USERNAME and X_CHANNEL_ID")
        return

    print(f"🐦 KOL watcher started → @{X_USERNAME} → channel {X_CHANNEL_ID}")
    print(
        f"   Poll every {X_POLL_SECONDS}s | "
        f"replies={X_INCLUDE_REPLIES} | RTs={X_INCLUDE_RETWEETS}"
    )

    last_seen_id: str = ""
    recent_sent_ids: deque[str] = deque(maxlen=100)

    async with httpx.AsyncClient(
        timeout=15.0,
        follow_redirects=True,
        headers=_HEADERS,
    ) as client:
        state = await db.get_x_watch_state(X_USERNAME)
        if state:
            last_seen_id = state.get("last_post_id", "")
            print(f"   ▶  Resumed — last post ID: {last_seen_id or '(none)'}")
        else:
            xml = await _fetch_with_fallback(client, X_USERNAME)
            if xml:
                valid_items = [i for i in _parse_items(xml) if not _should_skip(i)]
                newest = _pick_newest(valid_items)
                if newest:
                    last_seen_id = newest["id"]
                    await db.upsert_x_watch_state(
                        X_USERNAME,
                        "",
                        last_seen_id,
                        newest.get("created_at", ""),
                    )
                    print(f"   ✅  Initialized — seeded at post ID {last_seen_id}")

        while True:
            await asyncio.sleep(X_POLL_SECONDS)

            try:
                xml = await _fetch_with_fallback(client, X_USERNAME)
                if not xml:
                    continue

                items = _parse_items(xml)
                if not items:
                    continue

                new_posts = [
                    i
                    for i in items
                    if not _should_skip(i)
                    and i.get("id")
                    and (not last_seen_id or _id_gt(i["id"], last_seen_id))
                    and i["id"] not in recent_sent_ids
                ]

                if not new_posts:
                    continue

                new_posts.sort(
                    key=lambda p: (p.get("timestamp", 0.0), _numeric_id(p.get("id", "")))
                )

                for post in new_posts:
                    embed = _build_kol_embed(X_USERNAME, post)

                    await send_message(
                        X_CHANNEL_ID,
                        embeds=[embed],
                        mention_everyone=True,
                    )

                    recent_sent_ids.append(post["id"])
                    print(f"🐦 Posted: @{X_USERNAME} — {post['text'][:80]}…")

                    await asyncio.sleep(1.5)

                valid_items = [i for i in items if not _should_skip(i)]
                newest = _pick_newest(valid_items)
                if newest:
                    last_seen_id = newest["id"]
                    await db.upsert_x_watch_state(
                        X_USERNAME,
                        "",
                        last_seen_id,
                        newest.get("created_at", ""),
                    )

            except Exception as e:
                print(f"❌ X watcher error: {e}")
                await asyncio.sleep(15)
