from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path

from inventory_manager import InventoryManager
from order_manager import OrderManager


class StateStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    async def init(self) -> None:
        await asyncio.to_thread(self._init_sync)

    def _init_sync(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cycle_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    cycle INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    fills INTEGER NOT NULL,
                    fills_1m INTEGER NOT NULL,
                    realized_pnl REAL NOT NULL,
                    equity REAL NOT NULL,
                    drawdown_pct REAL NOT NULL,
                    active_orders INTEGER NOT NULL,
                    extra_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_exposure (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    cycle INTEGER NOT NULL,
                    market_id TEXT NOT NULL,
                    yes_exposure REAL NOT NULL,
                    no_exposure REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS open_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    cycle INTEGER NOT NULL,
                    order_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    queue_ahead_ratio REAL NOT NULL
                )
                """
            )

    async def save_cycle(
        self,
        *,
        cycle: int,
        mode: str,
        fills: int,
        fills_1m: int,
        realized_pnl: float,
        equity: float,
        drawdown_pct: float,
        order_manager: OrderManager,
        inventory: InventoryManager,
        extra: dict | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._save_cycle_sync,
            cycle,
            mode,
            fills,
            fills_1m,
            realized_pnl,
            equity,
            drawdown_pct,
            order_manager,
            inventory,
            extra or {},
        )

    def _save_cycle_sync(
        self,
        cycle: int,
        mode: str,
        fills: int,
        fills_1m: int,
        realized_pnl: float,
        equity: float,
        drawdown_pct: float,
        order_manager: OrderManager,
        inventory: InventoryManager,
        extra: dict,
    ) -> None:
        ts = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO cycle_snapshots
                (ts, cycle, mode, fills, fills_1m, realized_pnl, equity, drawdown_pct, active_orders, extra_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    cycle,
                    mode,
                    fills,
                    fills_1m,
                    realized_pnl,
                    equity,
                    drawdown_pct,
                    len(order_manager.get_all_orders()),
                    json.dumps(extra, separators=(",", ":")),
                ),
            )

            for market_id, exp in inventory.market_exposure.items():
                conn.execute(
                    """
                    INSERT INTO market_exposure (ts, cycle, market_id, yes_exposure, no_exposure)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (ts, cycle, market_id, float(exp.get("yes", 0.0)), float(exp.get("no", 0.0))),
                )

            for order in order_manager.get_all_orders():
                conn.execute(
                    """
                    INSERT INTO open_orders
                    (ts, cycle, order_id, market_id, side, price, size, created_at, queue_ahead_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ts,
                        cycle,
                        order.order_id,
                        order.market_id,
                        order.side,
                        order.price,
                        order.size,
                        order.created_at.isoformat(),
                        order.queue_ahead_ratio,
                    ),
                )

    async def load_last_equity(self) -> float | None:
        return await asyncio.to_thread(self._load_last_equity_sync)

    def _load_last_equity_sync(self) -> float | None:
        if not self.db_path.exists():
            return None
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT equity FROM cycle_snapshots ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            return None
        return float(row[0])
