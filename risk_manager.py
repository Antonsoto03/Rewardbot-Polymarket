from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RiskConfig:
    max_drawdown_pct: float
    max_fills_per_minute: int
    volatility_spike_bps: float


class RiskManager:
    """Safety checks scaffold for dry-run stage.

    Real execution controls (cancel-all, kill-switches) are intentionally
    deferred until live order routing is implemented.
    """

    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def should_halt_on_volatility(self, observed_move_bps: float) -> bool:
        return observed_move_bps >= self.config.volatility_spike_bps

    def can_place_more_fills(self, fills_last_minute: int) -> bool:
        return fills_last_minute < self.config.max_fills_per_minute

    def within_drawdown(self, drawdown_pct: float) -> bool:
        return drawdown_pct <= self.config.max_drawdown_pct
