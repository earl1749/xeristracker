from __future__ import annotations

import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Dict, List, Optional

from config.settings import (
    ALERT_CHANNEL_ID, CLEANUP_INTERVAL, ORDER_TTL_HOURS, SUMMARY_ALERT_INTERVAL, WHALE_MIN_USD,
)
from core.classifier import TransactionClassifier
from core.models import LimitOrder, MarketState, OrderType
from helpers.database import DatabaseManager
from helpers.discord_utils import send_message
from helpers.embeds import _embed_cancelled, _embed_cleanup, _embed_filled
from helpers.formatters import (
    _format_time_remaining, _pct_from_current, format_tokens, format_usd, get_timestamp,
)


# ═════════════════════════════════════════════════════════════════════════════
# Order tracker
# ═════════════════════════════════════════════════════════════════════════════

class OrderTracker:
    def __init__(self, db: DatabaseManager, classifier: TransactionClassifier, ms: MarketState) -> None:
        self.db         = db
        self.classifier = classifier
        self.ms         = ms
        self._seen: OrderedDict = OrderedDict()
        self._seen_max = 2000

    def _mark_seen(self, sig: str) -> bool:
        if sig in self._seen: return True
        self._seen[sig] = None
        if len(self._seen) > self._seen_max:
            self._seen.popitem(last=False)
        return False

    async def process(self, tx_data: Dict, signature: str) -> Optional[Dict]:
        if self._mark_seen(signature): return None

        keys   = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        signer = (keys[0].get("pubkey") if isinstance(keys[0], dict) else keys[0]) if keys else ""
        if not signer: return None

        order_type, info = await self.classifier.classify(tx_data, signer, self.ms)
        if not info: return None

        if order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL, OrderType.TRANSFER):
            if info.get("amount", 0) <= 0 or info.get("usd_value", 0) <= 0:
                return None

        if order_type in (OrderType.LIMIT_BUY, OrderType.LIMIT_SELL):
            if info.get("usd_value", 0) < 5:
                return None
            order = LimitOrder(
                signature=signature, wallet=info["wallet"], order_type=order_type,
                token_amount=info["amount"], usd_value=info["usd_value"],
                predicted_mcap=info.get("predicted_mcap", 0.0),
                target_price=info.get("target_price",   0.0),
                timestamp=time.time(),
            )
            await self.db.upsert_limit_order(
                order,
                quote_token=info.get("quote_token", ""),
                exchange=info.get("exchange", ""),
            )
            return {"action": "new_limit", "order": order, "info": info}

        elif order_type == OrderType.CANCEL_LIMIT:
            cancelled = await self._handle_cancel(signer, signature)
            return {"action": "cancel_limit", "cancelled": cancelled, "wallet": signer}

        elif order_type in (OrderType.MARKET_BUY, OrderType.MARKET_SELL):
            fills = await self._detect_fills(order_type, signature)
            return {"action": "market", "type": order_type, "info": info, "fills": fills}

        elif order_type == OrderType.TRANSFER:
            return {"action": "transfer", "info": info}

        return None

    async def _handle_cancel(self, wallet: str, sig: str) -> Optional[Dict]:
        for otype in ("LIMIT_BUY", "LIMIT_SELL"):
            row = await self.db.deactivate_one_by_wallet(wallet, otype)
            if row:
                await send_message(ALERT_CHANNEL_ID, embeds=[_embed_cancelled(wallet, row, sig)])
                return row
        return None

    async def _detect_fills(self, market_type: OrderType, signature: str) -> List[Dict]:
        ms = self.ms; filled = []
        if ms.current_market_cap <= 0: return filled
        for order in await self.db.get_active_orders():
            predicted = order["predicted_mcap"]
            if predicted <= 0: continue
            if order["token_amount"] <= 0 or order["usd_value"] <= 0:
                await self.db.deactivate_by_signature(order["signature"])
                continue
            proximity = abs(predicted - ms.current_market_cap) / ms.current_market_cap
            if proximity < 0.01:
                if ((market_type == OrderType.MARKET_BUY  and order["order_type"] == "LIMIT_SELL") or
                    (market_type == OrderType.MARKET_SELL and order["order_type"] == "LIMIT_BUY")):
                    await self.db.deactivate_by_signature(order["signature"])
                    await send_message(ALERT_CHANNEL_ID,
                        embeds=[_embed_filled(order, market_type, signature, ms)])
                    filled.append(order)
        return filled


# ═════════════════════════════════════════════════════════════════════════════
# Alert manager
# ═════════════════════════════════════════════════════════════════════════════

class AlertManager:
    def __init__(self, db: DatabaseManager, ms: MarketState) -> None:
        self.db            = db
        self.ms            = ms
        self._last         = 0.0
        self._last_cleanup = 0.0

    async def tick(self) -> None:
        now = time.time()
        if now - self._last_cleanup >= CLEANUP_INTERVAL:
            removed = await self.db.cleanup_stale(max_age_hours=ORDER_TTL_HOURS)
            if removed > 0:
                await send_message(ALERT_CHANNEL_ID, embeds=[_embed_cleanup(removed)])
            self._last_cleanup = now
        if now - self._last < SUMMARY_ALERT_INTERVAL:
            return
        await self._send_summary()
        self._last = now

    async def _send_summary(self) -> None:
        ms     = self.ms
        orders = await self.db.get_active_orders()
        if not orders: return

        buys  = [o for o in orders if o["order_type"] == "LIMIT_BUY"]
        sells = [o for o in orders if o["order_type"] == "LIMIT_SELL"]
        support_lvls    = sorted(o["predicted_mcap"] for o in buys  if o.get("predicted_mcap", 0) > 0)
        resistance_lvls = sorted(o["predicted_mcap"] for o in sells if o.get("predicted_mcap", 0) > 0)
        nearest_support    = support_lvls[0]    if support_lvls    else None
        nearest_resistance = resistance_lvls[0] if resistance_lvls else None

        embed: dict = {
            "author":      {"name": "📊 ACTIVE LIMIT ORDER BOOK — Snapshot"},
            "title":       "Live Support & Resistance Levels",
            "description": (
                f"```yaml\nActive Orders : {len(orders)}\n"
                f"Buy  Orders   : {len(buys)}   |  Wall: {format_usd(sum(o['usd_value'] for o in buys))}\n"
                f"Sell Orders   : {len(sells)}   |  Wall: {format_usd(sum(o['usd_value'] for o in sells))}\n```"
            ),
            "color":  0x8B5CF6,
            "fields": [],
        }

        if buys:
            lines = ""
            for i, o in enumerate(sorted(buys, key=lambda x: x["predicted_mcap"])[:6]):
                dist = _pct_from_current(o["predicted_mcap"], ms.current_market_cap)
                qt   = f" [{o.get('quote_token','')}]" if o.get("quote_token") else ""
                lines += (f"`{i+1}.` {format_usd(o['usd_value'])}{qt} · "
                          f"mcap `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%) · "
                          f"`{o['wallet'][:6]}…` · ⏳ `{_format_time_remaining(o)}`\n")
            embed["fields"].append({
                "name": f"🛡️ SUPPORT LEVELS  ({len(buys)} orders)",
                "value": lines, "inline": False,
            })

        if sells:
            lines = ""
            for i, o in enumerate(sorted(sells, key=lambda x: x["predicted_mcap"])[:6]):
                dist = _pct_from_current(o["predicted_mcap"], ms.current_market_cap)
                qt   = f" [{o.get('quote_token','')}]" if o.get("quote_token") else ""
                lines += (f"`{i+1}.` {format_usd(o['usd_value'])}{qt} · "
                          f"mcap `{format_usd(o['predicted_mcap'])}` ({dist:+.1f}%) · "
                          f"`{o['wallet'][:6]}…` · ⏳ `{_format_time_remaining(o)}`\n")
            embed["fields"].append({
                "name": f"⚠️ RESISTANCE LEVELS  ({len(sells)} orders)",
                "value": lines, "inline": False,
            })

        embed["fields"].append({
            "name": "📈 Market Context",
            "value": (
                f"┌ Price:              `${ms.current_price:.8f}`\n"
                f"├ Market Cap:         `{format_usd(ms.current_market_cap)}`\n"
                f"├ Nearest Support:    `{format_usd(nearest_support)}`\n"
                f"└ Nearest Resistance: `{format_usd(nearest_resistance)}`"
            ),
            "inline": False,
        })
        embed["footer"]    = {"text": f"Order Book · {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"}
        embed["timestamp"] = get_timestamp()
        await send_message(ALERT_CHANNEL_ID, embeds=[embed])
