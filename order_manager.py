from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from market_maker import Quote


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DryRunOrder:
    order_id: str
    market_id: str
    side: str
    price: float
    size: float
    created_at: datetime
    queue_ahead_ratio: float
    status: str = "open"


class OrderManager:
    """Dry-run order manager: no network calls, no execution."""

    def __init__(self) -> None:
        self.active_orders: dict[str, dict[str, DryRunOrder]] = {}
        self.last_market_action_ts: dict[str, float] = {}

    async def place_dry_run(self, quote: Quote) -> DryRunOrder:
        await asyncio.sleep(0)
        order = DryRunOrder(
            order_id=str(uuid.uuid4()),
            market_id=quote.market_id,
            side=quote.side,
            price=quote.price,
            size=quote.size,
            created_at=datetime.now(timezone.utc),
            queue_ahead_ratio=random.random(),
        )
        self.active_orders.setdefault(quote.market_id, {})[quote.side] = order
        self.last_market_action_ts[quote.market_id] = order.created_at.timestamp()
        logger.info(
            "[DRY-RUN] place %s %s price=%.4f size=%.4f",
            quote.market_id,
            quote.side,
            quote.price,
            quote.size,
        )
        return order

    async def cancel_market(self, market_id: str) -> list[DryRunOrder]:
        await asyncio.sleep(0)
        orders = list(self.active_orders.pop(market_id, {}).values())
        for order in orders:
            logger.info("[DRY-RUN] cancel %s %s", order.market_id, order.order_id)
        if orders:
            self.last_market_action_ts[market_id] = datetime.now(timezone.utc).timestamp()
        return orders

    def get_market_orders(self, market_id: str) -> list[DryRunOrder]:
        return list(self.active_orders.get(market_id, {}).values())

    def get_all_orders(self) -> list[DryRunOrder]:
        orders: list[DryRunOrder] = []
        for market_orders in self.active_orders.values():
            orders.extend(market_orders.values())
        return orders

    def remove_order(self, market_id: str, side: str) -> DryRunOrder | None:
        market_orders = self.active_orders.get(market_id)
        if not market_orders:
            return None
        order = market_orders.pop(side, None)
        if not market_orders:
            self.active_orders.pop(market_id, None)
        if order is not None:
            logger.info("[DRY-RUN] fill %s %s %s", order.market_id, order.side, order.order_id)
            self.last_market_action_ts[market_id] = datetime.now(timezone.utc).timestamp()
        return order

    def is_market_stale(self, market_id: str, stale_after_seconds: float) -> bool:
        orders = self.get_market_orders(market_id)
        if not orders:
            return False
        now_ts = datetime.now(timezone.utc).timestamp()
        return any((now_ts - order.created_at.timestamp()) >= stale_after_seconds for order in orders)

    def can_requote(self, market_id: str, min_action_interval_seconds: float) -> bool:
        if min_action_interval_seconds <= 0:
            return True
        last_ts = self.last_market_action_ts.get(market_id)
        if last_ts is None:
            return True
        now_ts = datetime.now(timezone.utc).timestamp()
        return (now_ts - last_ts) >= min_action_interval_seconds
