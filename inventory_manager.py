from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class InventoryLimits:
    max_position_per_market: float
    global_exposure_limit: float
    imbalance_threshold: float


class InventoryManager:
    """Dry-run inventory manager with neutral-bias checks."""

    def __init__(self, limits: InventoryLimits) -> None:
        self.limits = limits
        self.market_exposure: dict[str, dict[str, float]] = {}

    def set_market_exposure(self, market_id: str, yes_exposure: float, no_exposure: float) -> None:
        self.market_exposure[market_id] = {
            "yes": yes_exposure,
            "no": no_exposure,
        }

    def has_market(self, market_id: str) -> bool:
        return market_id in self.market_exposure

    def get_market_exposure(self, market_id: str) -> tuple[float, float]:
        data = self.market_exposure.get(market_id, {"yes": 0.0, "no": 0.0})
        return float(data["yes"]), float(data["no"])

    def get_global_exposure(self) -> float:
        total = 0.0
        for exp in self.market_exposure.values():
            total += abs(float(exp["yes"])) + abs(float(exp["no"]))
        return total

    def check_market_limit(self, exposure: float) -> bool:
        return abs(exposure) <= self.limits.max_position_per_market

    def check_global_limit(self, exposure: float) -> bool:
        return abs(exposure) <= self.limits.global_exposure_limit

    def needs_rebalance(self, yes_exposure: float, no_exposure: float) -> bool:
        return abs(yes_exposure - no_exposure) > self.limits.imbalance_threshold

    def side_allowed(
        self,
        market_id: str,
        side: Literal["buy", "sell"],
        size: float,
    ) -> bool:
        yes_exp, no_exp = self.get_market_exposure(market_id)
        projected_yes = yes_exp + (size if side == "buy" else 0.0)
        projected_no = no_exp + (size if side == "sell" else 0.0)

        if not self.check_market_limit(projected_yes - projected_no):
            return False
        if not self.check_global_limit(self.get_global_exposure() + size):
            return False

        if self.needs_rebalance(yes_exp, no_exp):
            if yes_exp > no_exp and side == "buy":
                return False
            if no_exp > yes_exp and side == "sell":
                return False

        return True

    def apply_fill(self, market_id: str, side: Literal["buy", "sell"], size: float) -> None:
        yes_exp, no_exp = self.get_market_exposure(market_id)
        if side == "buy":
            yes_exp += size
        else:
            no_exp += size
        self.set_market_exposure(market_id, yes_exp, no_exp)
