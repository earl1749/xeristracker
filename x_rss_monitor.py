from __future__ import annotations

import asyncio
import html
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

_RSSHUB_SELF = _os.getenv(
    "RSSHUB_URL",
    "https://rsshub-production-69fe.up.railway.app",
).rstrip("/")

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


def _extract_post_id_from_link(link: str) -> Optional[str]:
    if not link:
        return None

    m = re.search(r"/status(?:es)?/(\d+)", link)
    if m:
        return m.group(1)

    return None


def _extract_first_image_url(raw_html: str) -> Optional[str]:
    if not raw_html:
        return None

    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw_html, re.IGNORECASE)
    if m:
        return html.unescape(m.group(1).strip())

    return None


def _strip_html(raw_html: str) -> str:
    if not raw_html:
        return ""

    text = raw_html
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize_post_text(title: str, description: str) -> str:
    desc_text = _strip_html(description).strip()
    title = html.unescape((title or "").strip())

    if desc_text and desc_text != title:
        return desc_text

    return title


def _truncate_for_discord(text: str, limit: int = 1500) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _id_gt(a: str, b: str) -> bool:
    try:
        return int(a) > int(b)
    except Exception:
        return a > b


async def _fetch_rss(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.get(url)

        if r.status_code == 200 and "<item" in r.text:
            return r.text

        print(f"   ⚠️ RSS {r.status_code} from {url}")
        return None

    except Exception as e:
        host = url.split("/")[2] if "://" in url else url
        print(f"   ⚠️ RSS error ({host}): {e}")
        return None


async def _fetch_with_fallback(client: httpx.AsyncClient, username: str) -> Optional[str]:
    for template in _RSS_SOURCES:
        url = template.format(username=username)
        xml = await _fetch_rss(client, url)
        if xml:
            print(f"   ✅ RSS via {url.split('/')[2]}")
            return xml

    print(f"   ❌ All RSS sources failed for @{username}")
    return None


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
            description = (item.findtext("description") or "").strip()

            tweet_id = ""
            for src in (link, guid):
                tweet_id = _extract_post_id_from_link(src or "")
                if tweet_id:
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

            text = _normalize_post_text(title, description)
            image_url = _extract_first_image_url(description)

            is_retweet = title.startswith("RT @")
            is_reply = (
                title.startswith("R to @")
                or title.startswith("Replying to @")
                or (title.startswith("@") and not title.startswith(f"@{X_USERNAME}"))
            )

            items.append(
                {
                    "id": tweet_id,
                    "text": text,
                    "url": link,
                    "created_at": created_at,
                    "timestamp": ts,
                    "is_retweet": is_retweet,
                    "is_reply": is_reply,
                    "image_url": image_url,
                }
            )

    except ET.ParseError as e:
        print(f"   ⚠️ RSS XML parse error: {e}")

    return items


def _should_skip(item: dict) -> bool:
    if item.get("is_retweet") and not X_INCLUDE_RETWEETS:
        return True
    if item.get("is_reply") and not X_INCLUDE_REPLIES:
        return True
    return False


async def _send_post_to_discord(channel_id: int, item: dict, mention_everyone: bool = False) -> bool:
    # import inside to avoid circular import
    from xeris import send_message, send_message_with_image

    text = (item.get("text") or "").strip()
    url = (item.get("url") or "").strip()
    image_url = (item.get("image_url") or "").strip()

    body_parts: list[str] = []
    if text:
        body_parts.append(_truncate_for_discord(text, 1500))
    if url:
        body_parts.append(f"Original post: {url}")

    content = "\n\n".join(body_parts).strip()
    if not content:
        content = url or "New X post"

    if image_url:
        return await send_message_with_image(
            channel_id=channel_id,
            content=content,
            image_url=image_url,
            mention_everyone=mention_everyone,
        )

    return await send_message(
        channel_id,
        content=content,
        mention_everyone=mention_everyone,
    )


async def x_post_monitor(db) -> None:
    if not X_USERNAME or not X_CHANNEL_ID:
        print("ℹ️ X watcher disabled — set X_USERNAME and X_CHANNEL_ID")
        return

    print(f"🐦 X RSS watcher started → @{X_USERNAME} → channel {X_CHANNEL_ID}")
    print(
        f"   Poll every {X_POLL_SECONDS}s | "
        f"replies={X_INCLUDE_REPLIES} | RTs={X_INCLUDE_RETWEETS}"
    )

    last_seen_id: str = ""
    recent_sent_ids: deque[str] = deque(maxlen=200)

    async with httpx.AsyncClient(
        timeout=20.0,
        follow_redirects=True,
        headers=_HEADERS,
    ) as client:
        state = await db.get_x_watch_state(X_USERNAME)
        if state:
            last_seen_id = (state.get("last_post_id") or "").strip()
            print(f"   ▶ Resumed — last post ID: {last_seen_id or '(none)'}")
        else:
            xml = await _fetch_with_fallback(client, X_USERNAME)
            if xml:
                valid_items = [i for i in _parse_items(xml) if not _should_skip(i) and i.get("id")]
                newest = _pick_newest(valid_items)
                if newest:
                    last_seen_id = newest["id"]
                    await db.upsert_x_watch_state(
                        X_USERNAME,
                        "rss",
                        last_seen_id,
                        newest.get("created_at", ""),
                    )
                    print(f"   ✅ Initialized — seeded at post ID {last_seen_id}")

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
                    ok = await _send_post_to_discord(
                        X_CHANNEL_ID,
                        post,
                        mention_everyone=True,
                    )

                    if ok:
                        recent_sent_ids.append(post["id"])
                        last_seen_id = post["id"]

                        await db.upsert_x_watch_state(
                            X_USERNAME,
                            "rss",
                            last_seen_id,
                            post.get("created_at", ""),
                        )

                        print(f"🐦 Posted: @{X_USERNAME} — {post['text'][:80]}…")
                    else:
                        print(f"   ⚠️ Failed to send post ID {post['id']}")

                    await asyncio.sleep(1.5)

            except Exception as e:
                print(f"❌ X watcher error: {e}")
                await asyncio.sleep(15)
