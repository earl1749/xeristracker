from __future__ import annotations

import asyncio
import sqlite3
import time
from typing import Dict, List, Optional

from config.settings import DB_PATH, ORDER_TTL_HOURS, ORDER_TTL_SECS
from core.models import LimitOrder


class DatabaseManager:
    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    async def _exec(self, sql: str, params: tuple = ()) -> None:
        def _go():
            c = self._get_conn(); c.execute(sql, params); c.commit()
        await asyncio.to_thread(_go)

    async def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        def _go():
            c = self._get_conn()
            return [dict(r) for r in c.execute(sql, params).fetchall()]
        return await asyncio.to_thread(_go)

    async def _fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        def _go():
            c = self._get_conn()
            row = c.execute(sql, params).fetchone()
            return dict(row) if row else None
        return await asyncio.to_thread(_go)

    async def initialize(self) -> None:
        def _go():
            c = self._get_conn()
            c.execute("""
                CREATE TABLE IF NOT EXISTS limit_orders (
                    signature      TEXT PRIMARY KEY,
                    wallet         TEXT NOT NULL,
                    order_type     TEXT NOT NULL,
                    token_amount   REAL NOT NULL,
                    usd_value      REAL NOT NULL,
                    predicted_mcap REAL NOT NULL,
                    target_price   REAL NOT NULL DEFAULT 0,
                    quote_token    TEXT NOT NULL DEFAULT '',
                    exchange       TEXT NOT NULL DEFAULT '',
                    timestamp      REAL NOT NULL,
                    is_active      INTEGER NOT NULL DEFAULT 1,
                    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
            c.execute("""
                CREATE TABLE IF NOT EXISTS x_watch_state (
                    username       TEXT PRIMARY KEY,
                    user_id        TEXT NOT NULL DEFAULT '',
                    last_post_id   TEXT NOT NULL DEFAULT '',
                    last_post_time TEXT NOT NULL DEFAULT '',
                    added_by       TEXT NOT NULL DEFAULT 'system',
                    channel_id     INTEGER NOT NULL DEFAULT 0,
                    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
            # Migrations
            for col, defn in [
                ("quote_token", "TEXT NOT NULL DEFAULT ''"),
                ("exchange",    "TEXT NOT NULL DEFAULT ''"),
            ]:
                try:
                    c.execute(f"ALTER TABLE limit_orders ADD COLUMN {col} {defn}")
                except Exception:
                    pass
            # added_by migration for x_watch_state
            try:
                c.execute("ALTER TABLE x_watch_state ADD COLUMN added_by TEXT NOT NULL DEFAULT 'system'")
            except Exception:
                pass
            # channel_id migration for x_watch_state
            try:
                c.execute("ALTER TABLE x_watch_state ADD COLUMN channel_id INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass

            c.execute("CREATE INDEX IF NOT EXISTS idx_wallet_active ON limit_orders(wallet, is_active)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_mcap_active   ON limit_orders(predicted_mcap, is_active)")
            c.commit()
        await asyncio.to_thread(_go)
        print("✅ Database initialized")

    async def close(self) -> None:
        def _go():
            if self._conn:
                self._conn.close()
        await asyncio.to_thread(_go)
        self._conn = None

    # ── Limit orders ──────────────────────────────────────────────────────────

    async def upsert_limit_order(self, order: LimitOrder,
                                  quote_token: str = "", exchange: str = "") -> None:
        if order.token_amount <= 0 or order.usd_value <= 0:
            print(f"   ⚠️ Skipping zero-value limit order from {order.wallet[:8]}…")
            return
        await self._exec("""
            INSERT OR REPLACE INTO limit_orders
              (signature, wallet, order_type, token_amount, usd_value,
               predicted_mcap, target_price, quote_token, exchange, timestamp, is_active)
            VALUES (?,?,?,?,?,?,?,?,?,?,1)
        """, (order.signature, order.wallet, order.order_type.value,
              order.token_amount, order.usd_value, order.predicted_mcap,
              order.target_price, quote_token, exchange, order.timestamp))

    async def deactivate_by_signature(self, sig: str) -> None:
        await self._exec("UPDATE limit_orders SET is_active = 0 WHERE signature = ?", (sig,))

    async def deactivate_one_by_wallet(self, wallet: str, order_type: str) -> Optional[Dict]:
        row = await self._fetchone("""
            SELECT * FROM limit_orders
            WHERE wallet = ? AND order_type = ? AND is_active = 1
            ORDER BY timestamp DESC LIMIT 1
        """, (wallet, order_type))
        if row:
            await self._exec("UPDATE limit_orders SET is_active = 0 WHERE signature = ?",
                             (row["signature"],))
        return row

    async def cleanup_stale(self, max_age_hours: int = ORDER_TTL_HOURS) -> int:
        cutoff = time.time() - max_age_hours * 3600
        rows   = await self._fetchall(
            "SELECT * FROM limit_orders WHERE timestamp < ? AND is_active = 1", (cutoff,))
        if rows:
            await self._exec(
                "UPDATE limit_orders SET is_active = 0 WHERE timestamp < ? AND is_active = 1",
                (cutoff,))
            print(f"🧹 Cleaned {len(rows)} stale order(s) older than {max_age_hours}h")
        return len(rows)

    async def get_active_orders(self) -> List[Dict]:
        return await self._fetchall(
            "SELECT * FROM limit_orders WHERE is_active = 1 ORDER BY predicted_mcap ASC")

    async def get_orders_by_wallet(self, wallet: str) -> List[Dict]:
        return await self._fetchall(
            "SELECT * FROM limit_orders WHERE wallet = ? AND is_active = 1", (wallet,))

    # ── X watch state ─────────────────────────────────────────────────────────

    async def get_x_watch_state(self, username: str) -> Optional[Dict]:
        return await self._fetchone(
            "SELECT * FROM x_watch_state WHERE username = ?", (username.lower(),))

    async def get_all_x_watched(self) -> List[Dict]:
        return await self._fetchall(
            "SELECT * FROM x_watch_state ORDER BY updated_at ASC")

    async def upsert_x_watch_state(
        self,
        username: str,
        user_id: str,
        last_post_id: str,
        last_post_time: str = "",
        added_by: str = "system",
    ) -> None:
        await self._exec("""
            INSERT INTO x_watch_state
              (username, user_id, last_post_id, last_post_time, added_by, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username) DO UPDATE SET
                user_id        = excluded.user_id,
                last_post_id   = excluded.last_post_id,
                last_post_time = excluded.last_post_time,
                updated_at     = CURRENT_TIMESTAMP
        """, (username.lower(), user_id, last_post_id, last_post_time, added_by))

    async def add_x_watch(self, username: str, added_by: str = "system", channel_id: int = 0) -> bool:
        """Add a new account to watch. Returns False if already exists."""
        existing = await self.get_x_watch_state(username)
        if existing:
            return False
        await self._exec("""
            INSERT OR IGNORE INTO x_watch_state
              (username, user_id, last_post_id, last_post_time, added_by, channel_id, updated_at)
            VALUES (?, '', '', '', ?, ?, CURRENT_TIMESTAMP)
        """, (username.lower(), added_by, channel_id))
        return True

    async def remove_x_watch(self, username: str) -> bool:
        """Remove a watched account. Returns False if not found."""
        existing = await self.get_x_watch_state(username)
        if not existing:
            return False
        await self._exec("DELETE FROM x_watch_state WHERE username = ?", (username.lower(),))
        return True

    async def count_x_watched(self) -> int:
        rows = await self.get_all_x_watched()
        return len(rows)
