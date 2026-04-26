from __future__ import annotations

from dataclasses import dataclass

from rewards_market_selector import MarketSnapshot


@dataclass(frozen=True)
class QuoteConfig:
    distance_to_mid_bps: float
    size: float
    quote_style: str = "mid"
    top_of_book_improve_ticks: int = 1
    touch_gap_ticks: int = 1


@dataclass(frozen=True)
class Quote:
    market_id: str
    token_id: str | None
    side: str
    price: float
    size: float


def price_delta_bps(current: float, target: float) -> float:
    if current <= 0:
        return float("inf")
    return abs(target - current) / current * 10_000.0


def calculate_mid_price(best_bid: float, best_ask: float) -> float | None:
    if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
        return None
    return (best_bid + best_ask) / 2.0


def generate_quotes(market: MarketSnapshot, config: QuoteConfig) -> list[Quote]:
    mid = calculate_mid_price(market.best_bid, market.best_ask)
    if mid is None:
        return []

    if config.quote_style == "top_of_book":
        improve = max(config.top_of_book_improve_ticks, 0) * market.tick_size
        raw_bid = market.best_bid + improve
        raw_ask = market.best_ask - improve
    elif config.quote_style == "maker_cross_controlled":
        # Aggressive maker mode: quote close to touch without crossing.
        gap = max(config.touch_gap_ticks, 1) * market.tick_size
        raw_bid = market.best_ask - gap
        raw_ask = market.best_bid + gap
    else:
        distance = max(config.distance_to_mid_bps, 0.0) / 10_000.0
        raw_bid = mid * (1.0 - distance)
        raw_ask = mid * (1.0 + distance)

    # Keep quotes near mid while guaranteeing no spread crossing.
    bid = min(raw_bid, market.best_ask - market.tick_size)
    ask = max(raw_ask, market.best_bid + market.tick_size)

    if bid <= 0 or ask <= 0 or bid >= ask:
        return []

    return [
        Quote(market_id=market.market_id, token_id=market.token_id, side="buy", price=bid, size=config.size),
        Quote(market_id=market.market_id, token_id=market.token_id, side="sell", price=ask, size=config.size),
    ]


def should_requote(current_quotes: list[Quote], new_quotes: list[Quote], min_requote_bps: float) -> bool:
    current_by_key = {(q.token_id, q.side): q for q in current_quotes}
    new_by_key = {(q.token_id, q.side): q for q in new_quotes}
    all_keys = set(current_by_key.keys()) | set(new_by_key.keys())
    for key in all_keys:
        old_q = current_by_key.get(key)
        new_q = new_by_key.get(key)
        if old_q is None or new_q is None:
            return True
        if abs(old_q.size - new_q.size) > 1e-9:
            return True
        if price_delta_bps(old_q.price, new_q.price) >= min_requote_bps:
            return True
    return False
