from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import httpx

from config.settings import DISCORD_API, DISCORD_TOKEN


# ═════════════════════════════════════════════════════════════════════════════
# Low-level request
# ═════════════════════════════════════════════════════════════════════════════

async def _discord_request(method: str, path: str, **kwargs):
    headers = {
        "Authorization": f"Bot {DISCORD_TOKEN}",
        "Content-Type":  "application/json",
        "User-Agent":    "XerisBot/2.0",
    }
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        return await client.request(method, f"{DISCORD_API}{path}", headers=headers, **kwargs)


# ═════════════════════════════════════════════════════════════════════════════
# Rate-limited queue
# ═════════════════════════════════════════════════════════════════════════════

class DiscordQueue:
    CAPACITY      = 25
    REFILL_PERIOD = 30.0

    def __init__(self) -> None:
        self._queue: asyncio.Queue  = asyncio.Queue()
        self._tokens   = float(self.CAPACITY)
        self._last_ref = time.monotonic()
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def enqueue(self, channel_id: int, payload: dict) -> None:
        await self._queue.put((channel_id, payload))

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(
            self.CAPACITY,
            self._tokens + (now - self._last_ref) / self.REFILL_PERIOD * self.CAPACITY,
        )
        self._last_ref = now

    async def _worker(self) -> None:
        while True:
            channel_id, payload = await self._queue.get()
            self._refill()
            if self._tokens < 1:
                await asyncio.sleep((1 - self._tokens) / self.CAPACITY * self.REFILL_PERIOD)
                self._refill()
            self._tokens -= 1
            await _send_message_direct(channel_id, payload)
            self._queue.task_done()


_discord_queue: Optional[DiscordQueue] = None


def init_discord_queue() -> None:
    global _discord_queue
    _discord_queue = DiscordQueue()
    _discord_queue.start()


async def get_discord_queue() -> Optional[DiscordQueue]:
    return _discord_queue


# ═════════════════════════════════════════════════════════════════════════════
# Direct send (with retry)
# ═════════════════════════════════════════════════════════════════════════════

async def _send_message_direct(channel_id: int, payload: dict, max_retries: int = 5) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            r = await _discord_request("POST", f"/channels/{channel_id}/messages", json=payload)
            if r.status_code in (200, 201):
                print("   ✅ Message sent")
                return True
            if r.status_code == 429:
                retry_after = 2.0
                try:
                    data       = r.json()
                    retry_after = float(data.get("retry_after", retry_after))
                    print(f"   ⚠️ 429 rate-limit global={data.get('global')} retry={retry_after:.2f}s")
                except Exception:
                    pass
                await asyncio.sleep(retry_after + 0.25)
                continue
            print(f"   ❌ Discord {r.status_code}: {(r.text or '')[:200]}")
            return False
        except Exception as e:
            print(f"   ❌ Send error ({attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(min(2 * attempt, 5))
    print("   ❌ Message send failed after retries")
    return False


# ═════════════════════════════════════════════════════════════════════════════
# Public helpers
# ═════════════════════════════════════════════════════════════════════════════

async def send_message(
    channel_id: int,
    content: str = None,
    embeds: list = None,
    mention_everyone: bool = False,
) -> bool:
    payload: dict = {}
    parts = []
    if mention_everyone:
        parts.append("@everyone")
    if content:
        cleaned = content.strip()
        if cleaned:
            parts.append(cleaned)
    if parts:
        payload["content"] = " ".join(parts)
    if embeds:
        payload["embeds"] = embeds
    if not payload:
        return False
    if _discord_queue:
        await _discord_queue.enqueue(channel_id, payload)
        return True
    return await _send_message_direct(channel_id, payload)


async def send_typing(channel_id: int) -> None:
    try:
        r = await _discord_request("POST", f"/channels/{channel_id}/typing")
        if r.status_code not in (200, 204):
            print(f"   ⚠️ Typing failed: {r.status_code}")
    except Exception as e:
        print(f"   ⚠️ Typing error: {e}")


async def delete_message(channel_id: int, message_id: int) -> bool:
    try:
        r = await _discord_request("DELETE", f"/channels/{channel_id}/messages/{message_id}")
        if r.status_code in (200, 204):
            return True
        if r.status_code == 429:
            retry_after = 2.0
            try:
                retry_after = float(r.json().get("retry_after", retry_after))
            except Exception:
                pass
            await asyncio.sleep(retry_after + 0.25)
            r = await _discord_request("DELETE", f"/channels/{channel_id}/messages/{message_id}")
            return r.status_code in (200, 204)
        print(f"   ⚠️ Delete failed: {r.status_code}")
        return False
    except Exception as e:
        print(f"   ⚠️ Delete error: {e}")
        return False


async def send_message_get_id(
    channel_id: int,
    content: str = None,
    embeds: list = None,
    mention_everyone: bool = False,
    max_retries: int = 5,
) -> Optional[int]:
    payload: dict = {}
    parts = []
    if mention_everyone:
        parts.append("@everyone")
    if content:
        cleaned = content.strip()
        if cleaned:
            parts.append(cleaned)
    if parts:
        payload["content"] = " ".join(parts)
    if embeds:
        payload["embeds"] = embeds
    if not payload:
        return None

    for attempt in range(1, max_retries + 1):
        try:
            r = await _discord_request("POST", f"/channels/{channel_id}/messages", json=payload)
            if r.status_code in (200, 201):
                try:
                    return int(r.json()["id"])
                except Exception:
                    return None
            if r.status_code == 429:
                retry_after = 2.0
                try:
                    retry_after = float(r.json().get("retry_after", retry_after))
                except Exception:
                    pass
                await asyncio.sleep(retry_after + 0.25)
                continue
            print(f"   ❌ Discord {r.status_code}: {(r.text or '')[:150]}")
            return None
        except Exception as e:
            print(f"   ❌ send_get_id error ({attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(min(2 * attempt, 5))
    return None


async def send_message_with_image(
    channel_id: int,
    content: str,
    image_url: str,
    mention_everyone: bool = False,
    max_retries: int = 3,
) -> bool:
    headers = {"Authorization": f"Bot {DISCORD_TOKEN}", "User-Agent": "XerisBot/2.0"}
    parts   = []
    if mention_everyone:
        parts.append("@everyone")
    if content:
        cleaned = content.strip()
        if cleaned:
            parts.append(cleaned)
    final_content = " ".join(parts).strip() or " "

    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                img_resp = await client.get(image_url, headers={"User-Agent": "XerisBot/2.0"})
                if img_resp.status_code != 200 or not img_resp.content:
                    return await send_message(channel_id, content=(final_content + f"\n\n{image_url}").strip())

                ct = (img_resp.headers.get("content-type") or "").lower()
                if "png"  in ct: filename, mime = "image.png",  "image/png"
                elif "webp" in ct: filename, mime = "image.webp", "image/webp"
                elif "gif"  in ct: filename, mime = "image.gif",  "image/gif"
                else:              filename, mime = "image.jpg",  "image/jpeg"

                r = await client.post(
                    f"{DISCORD_API}/channels/{channel_id}/messages",
                    headers=headers,
                    data={"content": final_content},
                    files={"file": (filename, img_resp.content, mime)},
                )

            if r.status_code in (200, 201):
                print("   ✅ Message with image sent")
                return True
            if r.status_code == 429:
                retry_after = 2.0
                try:
                    retry_after = float(r.json().get("retry_after", retry_after))
                except Exception:
                    pass
                await asyncio.sleep(retry_after + 0.25)
                continue
            return await send_message(channel_id, content=(final_content + f"\n\n{image_url}").strip())
        except Exception as e:
            print(f"   ❌ send_with_image error ({attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(min(2 * attempt, 5))

    return await send_message(channel_id, content=(final_content + f"\n\n{image_url}").strip())


async def send_temp_message(
    channel_id: int,
    content: str = None,
    embeds: list = None,
    delete_after: int = 8,
    mention_everyone: bool = False,
) -> None:
    msg_id = await send_message_get_id(
        channel_id, content=content, embeds=embeds, mention_everyone=mention_everyone
    )
    if not msg_id:
        return

    async def _auto_delete():
        try:
            await asyncio.sleep(delete_after)
            await delete_message(channel_id, msg_id)
        except Exception as e:
            print(f"⚠️ Temp delete error: {e}")

    asyncio.create_task(_auto_delete())
