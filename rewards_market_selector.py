from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EPSILON = 1e-9


@dataclass(frozen=True)
class MarketSnapshot:
    market_id: str
    slug: str
    token_id: str | None
    no_token_id: str | None
    rewards: float
    competition: float
    liquidity: float
    best_bid: float
    best_ask: float
    tick_size: float = 0.001
    yes_exposure: float = 0.0
    no_exposure: float = 0.0
    rewards_min_size: float = 0.0
    rewards_max_spread: float = 0.0
    accepting_orders: bool = True
    end_ts: int | None = None
    volume_24h: float = 0.0

    @property
    def reward_score(self) -> float:
        return self.rewards / max(self.competition, EPSILON)

    @property
    def reward_efficiency(self) -> float:
        """Rewards per unit of 24h volume. Higher = more LP reward per dollar of flow.
        Returns 0.0 for zero-volume markets: no flow means no fills, so rewards are uncollectable."""
        if self.volume_24h <= 0:
            return 0.0
        return self.rewards / self.volume_24h

    @property
    def spread(self) -> float:
        return max(self.best_ask - self.best_bid, 0.0)

    @property
    def mid_price(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread_bps(self) -> float:
        mid = self.mid_price
        if mid <= 0:
            return 0.0
        return (self.spread / mid) * 10_000


class RewardsMarketSelector:
    def __init__(
        self,
        top_n: int,
        min_liquidity: float,
        min_spread_bps: float,
        min_volume_24h: float = 0.0,
        max_volume_24h: float = 0.0,
        min_reward_efficiency: float = 0.0,
    ) -> None:
        self.top_n = top_n
        self.min_liquidity = min_liquidity
        self.min_spread_bps = min_spread_bps
        self.min_volume_24h = min_volume_24h
        self.max_volume_24h = max_volume_24h
        self.min_reward_efficiency = min_reward_efficiency

    async def load_markets(self, snapshot_path: str | Path) -> list[MarketSnapshot]:
        path = Path(snapshot_path)
        raw = await asyncio.to_thread(path.read_text, encoding="utf-8")
        payload = json.loads(raw)

        if not isinstance(payload, list):
            raise ValueError("ranking_snapshot.json must contain a top-level list")

        markets: list[MarketSnapshot] = []
        for row in payload:
            market = self._parse_market(row)
            if market is None:
                continue
            markets.append(market)
        return markets

    def select_markets(self, markets: list[MarketSnapshot], top_n_override: int | None = None) -> list[MarketSnapshot]:
        filtered = [
            m
            for m in markets
            if m.accepting_orders
            and m.rewards > 0
            and m.liquidity >= self.min_liquidity
            and m.spread_bps >= self.min_spread_bps
            and (self.min_volume_24h <= 0 or m.volume_24h >= self.min_volume_24h)
            and (self.max_volume_24h <= 0 or m.volume_24h <= self.max_volume_24h)
            and (self.min_reward_efficiency <= 0 or m.reward_efficiency >= self.min_reward_efficiency)
        ]
        ranked = sorted(filtered, key=lambda m: m.reward_score, reverse=True)
        if top_n_override is None:
            limit = self.top_n
        else:
            limit = top_n_override
        if limit is None or limit <= 0:
            return ranked
        return ranked[:limit]

    def rotate_markets(
        self,
        ranked_markets: list[MarketSnapshot],
        cycle_number: int,
        active_markets: int,
        rotation_step: int,
    ) -> list[MarketSnapshot]:
        if not ranked_markets:
            return []
        take_n = max(1, min(active_markets, len(ranked_markets)))
        step = max(rotation_step, 1)
        start = ((max(cycle_number, 1) - 1) * step) % len(ranked_markets)
        ordered = ranked_markets[start:] + ranked_markets[:start]
        return ordered[:take_n]

    def _parse_market(self, row: dict[str, Any]) -> MarketSnapshot | None:
        if not isinstance(row, dict):
            return None

        def _to_ts(value: Any) -> int | None:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                v = int(value)
                if v > 10_000_000_000:
                    v //= 1000
                return v
            if isinstance(value, str):
                s = value.strip()
                if not s:
                    return None
                if s.isdigit():
                    return _to_ts(int(s))
                try:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return int(dt.timestamp())
                except Exception:
                    return None
            return None

        def _num(key: str, default: float = 0.0) -> float:
            value = row.get(key, default)
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        market_id = str(row.get("market_id") or row.get("id") or "").strip()
        slug = str(row.get("slug") or row.get("question") or market_id).strip()
        if not market_id:
            return None

        best_bid = _num("best_bid")
        best_ask = _num("best_ask")
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            return None

        competition = _num("competition")
        if competition <= 0:
            competition = _num("competition_score", 1.0)

        return MarketSnapshot(
            market_id=market_id,
            slug=slug,
            token_id=str(row.get("token_id") or row.get("asset_id") or row.get("yes_token_id") or "").strip() or None,
            no_token_id=str(row.get("no_token_id") or row.get("no_asset_id") or "").strip() or None,
            rewards=_num("rewards"),
            competition=max(competition, 1.0),
            liquidity=_num("liquidity"),
            best_bid=best_bid,
            best_ask=best_ask,
            tick_size=max(_num("tick_size", 0.001), 0.0001),
            yes_exposure=_num("yes_exposure"),
            no_exposure=_num("no_exposure"),
            rewards_min_size=max(_num("rewards_min_size"), 0.0),
            rewards_max_spread=max(_num("rewards_max_spread"), 0.0),
            accepting_orders=bool(row.get("accepting_orders", True)),
            end_ts=(
                _to_ts(row.get("end_ts"))
                or _to_ts(row.get("end_time"))
                or _to_ts(row.get("end_date_iso"))
                or _to_ts(row.get("endDate"))
            ),
            volume_24h=max(_num("volume_24h"), 0.0),
        )
