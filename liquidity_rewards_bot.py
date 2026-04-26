from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import random
import time
from collections import deque
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from execution_adapter import ExecutionAdapter, ExecutionConfig, LiveHttpExecutionClient
from inventory_manager import InventoryLimits, InventoryManager
from market_maker import Quote, QuoteConfig, calculate_mid_price, generate_quotes, should_requote
from order_manager import DryRunOrder, OrderManager
from rewards_market_selector import MarketSnapshot, RewardsMarketSelector
from risk_manager import RiskConfig, RiskManager
from state_store import StateStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket liquidity rewards bot")
    parser.add_argument("--mode", choices=["dry", "paper", "live"], default="dry", help="Execution mode")
    parser.add_argument("--confirm-live", default="", help="Must be YES to run in live mode")
    parser.add_argument("--enable-live-execution", action="store_true", help="Allow live order routing")
    parser.add_argument("--live-preflight-only", action="store_true", help="Validate live auth/connectivity and exit")
    parser.add_argument("--halt-on-order-error", action=argparse.BooleanOptionalAction, default=True, help="Kill-switch on order/cancel errors")
    parser.add_argument("--live-health-max-failures", type=int, default=3, help="Consecutive live heartbeat failures before kill-switch")
    parser.add_argument("--live-heartbeat-seconds", type=float, default=5.0, help="Minimum seconds between live heartbeats")
    parser.add_argument("--ranking-file", default="ranking_snapshot.json", help="Path to ranking snapshot JSON")
    parser.add_argument("--state-db", default="bot_state.sqlite", help="SQLite path for bot state")
    parser.add_argument("--disable-state-persistence", action="store_true", help="Disable SQLite state snapshots")
    parser.add_argument("--resume-state", action="store_true", help="Resume initial equity from last persisted snapshot")
    parser.add_argument("--top-n", type=int, default=5, help="How many markets to select")
    parser.add_argument("--active-markets", type=int, default=3, help="How many selected markets to actively quote per cycle")
    parser.add_argument("--rotation-step", type=int, default=1, help="How many positions to rotate each cycle")
    parser.add_argument("--market-hold-cycles", type=int, default=6, help="Keep same live market for N cycles to reduce churn")
    parser.add_argument("--no-fill-rotate-cycles", type=int, default=30, help="Force rotate live market after N no-fill cycles on same market")
    parser.add_argument("--min-liquidity", type=float, default=10_000.0, help="Minimum market liquidity")
    parser.add_argument("--min-spread-bps", type=float, default=15.0, help="Skip tighter spreads than this")
    parser.add_argument("--min-volume-24h", type=float, default=0.0, help="Skip markets with less than this 24h volume (0 disables)")
    parser.add_argument("--max-volume-24h", type=float, default=0.0, help="Skip markets with more than this 24h volume (0 disables); filters out high adverse-selection markets")
    parser.add_argument("--min-reward-efficiency", type=float, default=0.0, help="Skip markets where rewards/volume_24h is below this ratio (0 disables); targets low-volume high-reward sweet spot")
    parser.add_argument("--reward-efficiency-weight", type=float, default=0.0, help="Weight [0-1] given to reward_efficiency in live selection score (0 disables; blends with reward_score)")
    parser.add_argument("--live-min-top-size", type=float, default=25.0, help="Skip live markets with tiny top-of-book size")
    parser.add_argument("--live-min-book-levels", type=int, default=5, help="Skip live markets with too few visible book levels")
    parser.add_argument("--live-selection-max-spread-bps", type=float, default=300.0, help="Spread cap used only for live market ranking/selection")
    parser.add_argument("--live-selection-target-spread-bps", type=float, default=160.0, help="Target spread used to penalize high-spread markets in live ranking")
    parser.add_argument("--live-fillable-prefetch-candidates", type=int, default=10, help="Evaluate top-N ranked live markets by real book quality before final selection")
    parser.add_argument("--live-fillable-cache-cycles", type=int, default=4, help="Reuse fillability snapshot for N cycles to reduce API load")
    parser.add_argument("--live-fillable-min-score", type=float, default=0.75, help="Minimum fillability score to prioritize a live market")
    parser.add_argument("--live-fallback-candidates", type=int, default=5, help="When active-markets=1 in live mode, try up to N ranked candidates in the same cycle")
    parser.add_argument("--live-market-blacklist", default="", help="Comma-separated market_ids to exclude in live mode")
    parser.add_argument("--live-no-fill-cooldown-threshold", type=int, default=120, help="After N no-fill cycles on a market, apply temporary cooldown")
    parser.add_argument("--live-no-fill-cooldown-cycles", type=int, default=120, help="How many cycles to exclude a market after no-fill cooldown trigger")
    parser.add_argument("--live-min-recent-trades", type=int, default=0, help="Skip live markets with too few recent trades (0 disables this filter)")
    parser.add_argument("--live-activity-window-seconds", type=int, default=300, help="Window in seconds for recent trade activity filter")
    parser.add_argument("--live-strict-activity-filter", action=argparse.BooleanOptionalAction, default=False, help="If true, hard-skip markets below live-min-recent-trades; otherwise treat as soft signal")
    # --- Protección 1: Mid-drift detector ---
    parser.add_argument(
        "--mid-drift-pause-bps",
        type=float,
        default=50.0,
        help=(
            "If the mid price shifts more than this many bps in a single cycle, cancel open orders "
            "and pause quoting on that market for --mid-drift-pause-cycles cycles (0 disables). "
            "Protects against being on the wrong side of a fast price move (adverse selection)."
        ),
    )
    parser.add_argument(
        "--mid-drift-pause-cycles",
        type=int,
        default=3,
        help="Cycles to pause quoting after detecting a mid-drift event (default 3 ~ 15s at 5s refresh).",
    )
    # --- Protección 2: Cap de exposición por mercado ---
    parser.add_argument(
        "--max-market-exposure-usd",
        type=float,
        default=0.0,
        help=(
            "Max total USD exposure per market: sum of (yes_pos * mid) + (no_pos * (1-mid)). "
            "When exceeded, new quotes are blocked but inventory unwind still runs (0 disables)."
        ),
    )
    # --- Protección 3: Monitor de eficiencia de fills ---
    parser.add_argument(
        "--min-fill-efficiency-pct",
        type=float,
        default=0.0,
        help=(
            "After --fill-efficiency-warmup-cycles cycles on a market, warn and apply cooldown "
            "if the fill rate (fills/active_cycles) is below this percentage (0 disables)."
        ),
    )
    parser.add_argument(
        "--fill-efficiency-warmup-cycles",
        type=int,
        default=100,
        help="Minimum active cycles on a market before fill-efficiency check kicks in.",
    )
    parser.add_argument(
        "--fill-efficiency-cooldown-cycles",
        type=int,
        default=200,
        help="Cycles to cool down a market that fails the fill-efficiency check.",
    )
    parser.add_argument("--rewards-capture-mode", action=argparse.BooleanOptionalAction, default=True, help="Enable rewards-focused eligibility filters and size floor")
    parser.add_argument("--rewards-min-daily-rate", type=float, default=0.0, help="Skip markets below this daily rewards rate in capture mode")
    parser.add_argument("--rewards-size-buffer", type=float, default=1.0, help="Multiplier over market rewards_min_size for quote sizing")
    parser.add_argument("--rewards-max-min-size", type=float, default=0.0, help="In capture mode, skip markets with rewards_min_size above this (0 disables)")
    parser.add_argument("--rewards-max-market-notional-usd", type=float, default=0.0, help="Skip markets whose required rewards size implies higher per-side notional than this (0 disables)")
    parser.add_argument("--rewards-require-dual-sided", action=argparse.BooleanOptionalAction, default=True, help="In live capture mode require dual-sided quoting (YES and NO)")
    parser.add_argument("--rewards-capital-usd", type=float, default=0.0, help="Capital reference used to filter markets by rewards min_size (0 disables)")
    parser.add_argument("--rewards-capital-utilization", type=float, default=0.8, help="Fraction of rewards-capital-usd considered deployable")
    parser.add_argument("--stable-mid-min", type=float, default=0.10, help="Stable-mode lower bound for market mid price")
    parser.add_argument("--stable-mid-max", type=float, default=0.90, help="Stable-mode upper bound for market mid price")
    parser.add_argument("--stable-min-hours-to-end", type=float, default=48.0, help="Stable-mode minimum hours to market end (if end_ts available)")
    parser.add_argument("--stable-max-top-imbalance-ratio", type=float, default=4.0, help="Stable-mode max allowed ratio between top bid/ask sizes")
    parser.add_argument("--live-reserve-balance-usd", type=float, default=2.0, help="Keep this USD balance unused in live mode")
    parser.add_argument("--live-per-order-balance-fraction", type=float, default=1.0, help="Fraction of per-order balance budget allowed for each quote")
    parser.add_argument("--live-min-order-size", type=float, default=5.0, help="Minimum live order size; smaller quotes are skipped")
    parser.add_argument("--live-auto-single-side-on-low-balance", action=argparse.BooleanOptionalAction, default=True, help="Auto-disable dual-side live quoting when usable balance is too low")
    parser.add_argument("--live-min-usable-balance-usd", type=float, default=8.0, help="Pause live quoting below this usable balance")
    parser.add_argument("--live-stop-after-no-fill-cycles", type=int, default=240, help="Kill-switch if no fills for this many consecutive cycles")
    parser.add_argument("--live-stop-after-posts-without-fill", type=int, default=300, help="Kill-switch after this many posted orders since last fill")
    parser.add_argument("--distance-to-mid-bps", type=float, default=8.0, help="Quote distance from mid in bps")
    parser.add_argument("--quote-style", choices=["mid", "top_of_book", "maker_cross_controlled"], default="mid", help="Quote style")
    parser.add_argument("--top-of-book-improve-ticks", type=int, default=1, help="Ticks to improve from top when quote-style=top_of_book")
    parser.add_argument("--touch-gap-ticks", type=int, default=1, help="Ticks gap from touch when quote-style=maker_cross_controlled")
    parser.add_argument("--min-distance-to-mid-bps", type=float, default=4.0, help="Lower bound for adaptive quote distance")
    parser.add_argument("--no-fill-tighten-cycles", type=int, default=20, help="Tighten quotes after this many consecutive no-fill cycles")
    parser.add_argument("--tighten-step-bps", type=float, default=10.0, help="Bps reduction applied each no-fill-tighten-cycles block")
    parser.add_argument("--quote-size", type=float, default=25.0, help="Quote size per side")
    parser.add_argument("--max-position-per-market", type=float, default=100.0, help="Max net position per market")
    parser.add_argument("--global-exposure-limit", type=float, default=500.0, help="Max global exposure")
    parser.add_argument("--imbalance-threshold", type=float, default=50.0, help="Neutral-bias imbalance threshold")
    parser.add_argument("--refresh-seconds", type=float, default=5.0, help="Repricing interval in seconds")
    parser.add_argument("--cycles", type=int, default=1, help="Bot cycles (-1 for infinite)")
    parser.add_argument("--stale-order-seconds", type=float, default=30.0, help="Cancel+replace if order age exceeds this")
    parser.add_argument("--min-requote-bps", type=float, default=1.0, help="Minimum price move needed to requote")
    parser.add_argument("--live-requote-epsilon-bps", type=float, default=12.0, help="Extra live requote threshold to avoid micro-churn")
    parser.add_argument("--live-replace-cooldown-seconds", type=float, default=12.0, help="Minimum seconds between live cancel+replace on same market")
    parser.add_argument("--live-min-quote-age-seconds", type=float, default=25.0, help="Do not replace live quotes before this age unless stale")
    parser.add_argument("--min-action-interval-seconds", type=float, default=2.0, help="Anti-spam cooldown per market")
    parser.add_argument("--simulate-fills", action="store_true", help="Enable probabilistic fill simulation")
    parser.add_argument("--fill-base-prob", type=float, default=0.01, help="Base fill probability per cycle")
    parser.add_argument("--fill-sensitivity", type=float, default=0.25, help="Extra fill probability from queue proximity")
    parser.add_argument("--fill-max-prob", type=float, default=0.60, help="Upper bound for fill probability")
    parser.add_argument("--min-book-seconds-before-fill", type=float, default=2.0, help="Minimum resting time before fills can occur")
    parser.add_argument("--queue-time-boost-seconds", type=float, default=20.0, help="Seconds to reach full time-in-book bonus")
    parser.add_argument("--max-time-bonus", type=float, default=0.25, help="Max additional fill probability from resting time")
    parser.add_argument("--initial-equity", type=float, default=10_000.0, help="Starting equity for drawdown checks")
    parser.add_argument("--max-drawdown-pct", type=float, default=15.0, help="Kill-switch drawdown percentage")
    parser.add_argument("--max-fills-per-minute", type=int, default=120, help="Kill-switch fill rate")
    parser.add_argument("--max-cancels-per-minute", type=int, default=24, help="Throttle cancel/replace churn in live mode")
    parser.add_argument("--daily-loss-limit-usd", type=float, default=25.0, help="Daily loss kill-switch in USD")
    parser.add_argument("--simulated-taker-fee-pct", type=float, default=1.0, help="Taker fee percentage deducted from fills in dry/paper simulation (Polymarket charges ~1%%)")
    parser.add_argument("--volatility-spike-bps", type=float, default=500.0, help="Reserved risk threshold")
    parser.add_argument("--hard-max-spread-bps", type=float, default=300.0, help="Hard skip if spread_bps exceeds this")
    parser.add_argument("--min-reward-score", type=float, default=5.0, help="Hard skip if reward_score is below this")
    parser.add_argument("--inventory-unwind-threshold", type=float, default=2.0, help="Unwind when token position exceeds this size (hedge mode)")
    parser.add_argument("--inventory-unwind-size", type=float, default=2.0, help="Max unwind size per cycle per token")
    parser.add_argument("--inventory-unwind-style", choices=["passive", "touch", "aggressive"], default="aggressive", help="Pricing mode for inventory unwind orders")
    parser.add_argument(
        "--inventory-mode",
        choices=["hedge", "capture", "dynamic"],
        default="hedge",
        help=(
            "hedge: unwind any token position above threshold (default, aggressive risk control). "
            "capture: only unwind when dollar imbalance between YES and NO exceeds threshold "
            "(preserves hedge between sides, keeps both quotes active for rewards). "
            "dynamic: like capture but imbalance threshold scales with reward_score."
        ),
    )
    parser.add_argument(
        "--inventory-imbalance-threshold-usd",
        type=float,
        default=20.0,
        help="Dollar imbalance (|YES_pos*mid - NO_pos*(1-mid)|) above which capture/dynamic mode triggers unwind",
    )
    parser.add_argument(
        "--open-orders-cache-cycles",
        type=int,
        default=2,
        help="Reuse get_open_orders_count result for N cycles to reduce API calls (0 disables cache)",
    )
    parser.add_argument(
        "--open-orders-retries",
        type=int,
        default=2,
        help="Extra retries when live open-order count is unavailable (0 disables retries)",
    )
    parser.add_argument(
        "--open-orders-retry-delay-seconds",
        type=float,
        default=0.35,
        help="Base backoff (seconds) between open-order count retries",
    )
    parser.add_argument("--enable-live-unwind", action=argparse.BooleanOptionalAction, default=True, help="Enable automatic inventory unwind logic in live mode")
    parser.add_argument("--enable-live-dual-buy", action=argparse.BooleanOptionalAction, default=True, help="In live mode, quote both sides as BUY YES + BUY NO when possible")
    parser.add_argument("--enable-no-token-unwind", action=argparse.BooleanOptionalAction, default=True, help="Allow unwind orders on no_token_id side")
    parser.add_argument("--ignore-recoverable-order-errors-live", action=argparse.BooleanOptionalAction, default=True, help="Do not kill bot on recoverable live order errors (balance/min-size)")
    parser.add_argument("--random-seed", type=int, default=42, help="Seed for reproducible fill simulation")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    return parser


def _validate_mode_args(args: argparse.Namespace) -> None:
    if args.mode == "live":
        if args.confirm_live != "YES":
            raise ValueError("Live mode requires --confirm-live YES")
        if not args.enable_live_execution:
            raise ValueError("Live mode requires --enable-live-execution")


def _runtime_simulate_fills(args: argparse.Namespace) -> bool:
    if args.mode == "live":
        return False
    if args.mode == "paper":
        return True
    return args.simulate_fills


def _make_client_order_id(run_id: str, quote: Quote) -> str:
    raw = (
        f"{run_id}|{quote.market_id}|{quote.token_id or ''}|"
        f"{quote.side}|{quote.price:.8f}|{quote.size:.8f}"
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"mm-{digest}"


def _to_live_quotes(
    market: MarketSnapshot,
    quotes: list[Quote],
    token_positions: dict[str, float],
    enable_dual_buy: bool,
) -> list[Quote]:
    if not quotes:
        return []

    out: list[Quote] = []
    for q in quotes:
        if q.side == "buy":
            out.append(q)
            continue
        if enable_dual_buy and market.no_token_id:
            # SELL YES equivalent as BUY NO, keeps strategy delta-neutral without short inventory.
            no_buy_price = max(min(1.0 - q.price, 0.999), 0.001)
            out.append(
                Quote(
                    market_id=q.market_id,
                    token_id=market.no_token_id,
                    side="buy",
                    price=no_buy_price,
                    size=q.size,
                )
            )
            continue
        # In live mode, only place SELL on a token if we hold inventory.
        if not market.token_id:
            continue
        yes_pos = token_positions.get(market.token_id, 0.0)
        sell_size = min(q.size, max(yes_pos, 0.0))
        if sell_size <= 0:
            continue
        out.append(
            Quote(
                market_id=q.market_id,
                token_id=market.token_id,
                side="sell",
                price=q.price,
                size=sell_size,
            )
        )
    return out


def _token_position(token_positions: dict[str, float], token_id: str | None) -> float:
    if not token_id:
        return 0.0
    return token_positions.get(token_id, 0.0)


def _compute_inventory_imbalance(yes_pos: float, no_pos: float, yes_mid: float) -> float:
    """Net dollar exposure: positive = long YES, negative = long NO.

    Holding balanced YES and NO positions is risk-neutral for a binary market:
    their values move in opposite directions. Only the imbalance between them
    represents real directional risk that needs hedging.
    """
    yes_dollar = yes_pos * max(yes_mid, 0.0)
    no_dollar = no_pos * max(1.0 - yes_mid, 0.0)
    return yes_dollar - no_dollar


def _build_unwind_quote_from_top(
    market_id: str,
    token_id: str,
    pos: float,
    bid: float,
    ask: float,
    tick_size: float,
    unwind_threshold: float,
    unwind_size: float,
    unwind_style: str,
) -> Quote | None:
    if abs(pos) < max(unwind_threshold, 0.0):
        return None
    qty = min(max(unwind_size, 0.0), abs(pos))
    if qty <= 0:
        return None

    if pos > 0:
        if unwind_style == "aggressive":
            price = bid
        elif unwind_style == "passive":
            price = max(bid + tick_size, min(ask - (2 * tick_size), ask - tick_size))
        else:
            price = max(bid + tick_size, ask - tick_size)
        price = min(max(price, bid), ask)
        return Quote(market_id=market_id, token_id=token_id, side="sell", price=price, size=qty)

    if unwind_style == "aggressive":
        price = ask
    elif unwind_style == "passive":
        price = min(ask - tick_size, max(bid + (2 * tick_size), bid + tick_size))
    else:
        price = min(ask - tick_size, bid + tick_size)
    price = max(min(price, ask), bid)
    return Quote(market_id=market_id, token_id=token_id, side="buy", price=price, size=qty)


async def _build_live_unwind_quotes(
    execution: ExecutionAdapter,
    market: MarketSnapshot,
    token_positions: dict[str, float],
    unwind_threshold: float,
    unwind_size: float,
    unwind_style: str,
    enable_no_token_unwind: bool,
    inventory_mode: str = "hedge",
    imbalance_threshold_usd: float = 20.0,
) -> list[Quote]:
    yes_pos = _token_position(token_positions, market.token_id)
    no_pos = _token_position(token_positions, market.no_token_id)

    # Always fetch a fresh YES book — the snapshot price is from the start of the
    # cycle and may be stale by the time we actually place unwind orders.
    yes_bid, yes_ask = market.best_bid, market.best_ask
    if market.token_id:
        fresh_yes = await execution.get_top_of_book(market.token_id)
        if fresh_yes:
            yes_bid, yes_ask = fresh_yes[0], fresh_yes[1]

    if inventory_mode in ("capture", "dynamic"):
        # In capture/dynamic mode the risk is not the raw position size but the
        # dollar imbalance between YES and NO exposure.  A balanced position
        # (holding equal $ of YES + NO) is risk-neutral because they move
        # inversely; only the excess on one side creates directional risk.
        mid = (yes_bid + yes_ask) / 2.0 if yes_bid < yes_ask else market.mid_price
        imbalance = _compute_inventory_imbalance(yes_pos, no_pos, mid)

        if abs(imbalance) < max(imbalance_threshold_usd, 0.0):
            # Portfolio is balanced enough — stay in book, keep collecting rewards.
            return []

        out: list[Quote] = []
        if imbalance > 0 and market.token_id:
            # Net long YES → sell YES to rebalance.
            sell_size = min(imbalance / max(yes_bid, 1e-6), unwind_size)
            q = _build_unwind_quote_from_top(
                market_id=market.market_id,
                token_id=market.token_id,
                pos=sell_size,  # positive → will SELL
                bid=yes_bid,
                ask=yes_ask,
                tick_size=market.tick_size,
                unwind_threshold=0.0,  # already decided to unwind
                unwind_size=sell_size,
                unwind_style=unwind_style,
            )
            if q:
                out.append(q)
        elif imbalance < 0 and enable_no_token_unwind and market.no_token_id:
            # Net long NO → sell NO to rebalance.
            top_no = await execution.get_top_of_book(market.no_token_id)
            if top_no:
                no_mid = max(1.0 - mid, 1e-6)
                sell_size = min(abs(imbalance) / no_mid, unwind_size)
                q = _build_unwind_quote_from_top(
                    market_id=market.market_id,
                    token_id=market.no_token_id,
                    pos=sell_size,  # positive → will SELL
                    bid=top_no[0],
                    ask=top_no[1],
                    tick_size=market.tick_size,
                    unwind_threshold=0.0,
                    unwind_size=sell_size,
                    unwind_style=unwind_style,
                )
                if q:
                    out.append(q)
        return out

    # hedge mode: check each token independently against the absolute threshold.
    out = []
    if market.token_id:
        q_yes = _build_unwind_quote_from_top(
            market_id=market.market_id,
            token_id=market.token_id,
            pos=yes_pos,
            bid=yes_bid,
            ask=yes_ask,
            tick_size=market.tick_size,
            unwind_threshold=unwind_threshold,
            unwind_size=unwind_size,
            unwind_style=unwind_style,
        )
        if q_yes:
            out.append(q_yes)

    no_pos_check = _token_position(token_positions, market.no_token_id)
    if enable_no_token_unwind and market.no_token_id and abs(no_pos_check) >= max(unwind_threshold, 0.0):
        top_no = await execution.get_top_of_book(market.no_token_id)
        if top_no:
            q_no = _build_unwind_quote_from_top(
                market_id=market.market_id,
                token_id=market.no_token_id,
                pos=no_pos_check,
                bid=top_no[0],
                ask=top_no[1],
                tick_size=market.tick_size,
                unwind_threshold=unwind_threshold,
                unwind_size=unwind_size,
                unwind_style=unwind_style,
            )
            if q_no:
                out.append(q_no)
    return out


def _is_recoverable_live_order_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        "not enough balance / allowance" in msg
        or "lower than the minimum" in msg
        or "order would be marketable" in msg
        or "price is outside" in msg
        or "already canceled" in msg
        or "could not insert order" in msg
    )


def _cap_live_quotes_by_budget(
    quotes: list[Quote],
    per_order_budget_usd: float | None,
    per_order_fraction: float,
) -> list[Quote]:
    if not quotes or per_order_budget_usd is None:
        return quotes

    budget = max(per_order_budget_usd, 0.0) * max(min(per_order_fraction, 1.0), 0.0)
    if budget <= 0:
        return []

    out: list[Quote] = []
    for q in quotes:
        price = max(float(q.price), 1e-9)
        max_size = budget / price
        size = min(float(q.size), max_size)
        if size <= 0:
            continue
        out.append(Quote(market_id=q.market_id, token_id=q.token_id, side=q.side, price=q.price, size=size))
    return out


def _filter_quotes_by_min_size(quotes: list[Quote], min_size: float) -> list[Quote]:
    threshold = max(min_size, 0.0)
    if threshold <= 0:
        return quotes
    return [q for q in quotes if q.size >= threshold]


def _has_dual_token_quotes(quotes: list[Quote]) -> bool:
    tokens = {q.token_id for q in quotes if q.token_id}
    return len(tokens) >= 2


def _live_selection_score(
    market: MarketSnapshot,
    target_spread_bps: float,
    efficiency_weight: float = 0.0,
) -> float:
    target = max(target_spread_bps, 1.0)
    spread = max(market.spread_bps, 1.0)
    spread_factor = min(target / spread, 1.0)
    base = market.reward_score * spread_factor

    w = max(min(efficiency_weight, 1.0), 0.0)
    if w <= 0.0 or market.reward_efficiency <= 0.0:
        return base

    # Normalize efficiency relative to reward_score so the blend is scale-consistent.
    # We express efficiency as a multiplier: how much extra weight to give markets
    # where rewards are dense relative to volume (low adverse selection).
    eff_factor = min(market.reward_efficiency / max(market.reward_score, 1e-9), 2.0)
    return base * (1.0 - w + w * eff_factor)


async def _rank_live_fillable_markets(
    execution: ExecutionAdapter,
    ranked_markets: list[MarketSnapshot],
    args: argparse.Namespace,
    cycle: int,
    cache: dict[str, tuple[int, float]],
) -> list[MarketSnapshot]:
    prefetch = max(int(args.live_fillable_prefetch_candidates), 0)
    if prefetch <= 0 or not ranked_markets:
        return ranked_markets

    cache_cycles = max(int(args.live_fillable_cache_cycles), 1)
    top = ranked_markets[:prefetch]
    tail = ranked_markets[prefetch:]
    scored: list[tuple[float, float, MarketSnapshot]] = []

    for market in top:
        cached = cache.get(market.market_id)
        if cached and (cycle - cached[0]) <= cache_cycles:
            fill_score = cached[1]
        else:
            fill_score = 0.0
            if market.token_id:
                snapshot = await execution.get_book_snapshot(market.token_id)
                if snapshot:
                    bid_size = float(snapshot.get("bid_size") or 0.0)
                    ask_size = float(snapshot.get("ask_size") or 0.0)
                    bid_levels = int(snapshot.get("bid_levels") or 0)
                    ask_levels = int(snapshot.get("ask_levels") or 0)

                    min_top = max(float(args.live_min_top_size), 1e-9)
                    min_levels = max(int(args.live_min_book_levels), 1)
                    top_ratio = min(bid_size, ask_size) / min_top
                    level_ratio = min(bid_levels, ask_levels) / float(min_levels)
                    spread_ratio = max(float(market.spread_bps), 1.0) / max(float(args.live_selection_max_spread_bps), 1.0)
                    spread_component = max(0.0, 1.0 - spread_ratio)

                    # Emphasize real tradability (size/levels) while still rewarding tighter spreads.
                    fill_score = (0.50 * min(top_ratio, 3.0)) + (0.35 * min(level_ratio, 3.0)) + (0.15 * spread_component)
            cache[market.market_id] = (cycle, fill_score)

        if fill_score >= max(float(args.live_fillable_min_score), 0.0):
            base_score = _live_selection_score(market, args.live_selection_target_spread_bps, args.reward_efficiency_weight)
            scored.append((fill_score, base_score, market))

    if not scored:
        return ranked_markets

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    chosen_ids = {m.market_id for _, _, m in scored}
    prioritized = [m for _, _, m in scored]
    carry = [m for m in ranked_markets if m.market_id not in chosen_ids]
    return prioritized + carry


def _parse_market_blacklist(raw: str) -> set[str]:
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _reward_min_size_target(market: MarketSnapshot, size_buffer: float) -> float:
    base = max(float(market.rewards_min_size), 0.0)
    if base <= 0:
        return 0.0
    return base * max(float(size_buffer), 0.0)


def _market_meets_rewards_spread_rule(market: MarketSnapshot) -> bool:
    # Rewards max spread is typically expressed in cents in Polymarket rewards metadata.
    if market.rewards_max_spread <= 0:
        return True
    max_abs_spread = market.rewards_max_spread / 100.0
    return market.spread <= max_abs_spread


def _market_meets_stability_rule(market: MarketSnapshot, stable_mid_min: float, stable_mid_max: float) -> bool:
    mid = market.mid_price
    low = max(min(stable_mid_min, 1.0), 0.0)
    high = max(min(stable_mid_max, 1.0), low)
    return low <= mid <= high


def _market_has_time_buffer(market: MarketSnapshot, min_hours_to_end: float) -> bool:
    if min_hours_to_end <= 0:
        return True
    if market.end_ts is None:
        return True
    remaining_seconds = market.end_ts - int(time.time())
    return remaining_seconds >= int(min_hours_to_end * 3600)


def _top_imbalance_ratio(bid_size: float, ask_size: float) -> float:
    b = max(bid_size, 1e-9)
    a = max(ask_size, 1e-9)
    return max(b / a, a / b)


def _to_ts(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = int(value)
        # normalize ms->s
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
            return int(dt.timestamp())
        except Exception:
            return None
    return None


def _parse_trade_row(row: dict) -> tuple[str | None, str | None, str | None, float | None, float | None, int | None]:
    if not isinstance(row, dict):
        return None, None, None, None, None, None
    trade_id = str(row.get("id") or row.get("trade_id") or row.get("transaction_hash") or "").strip() or None
    token_id = str(row.get("asset_id") or row.get("token_id") or row.get("market_asset_id") or "").strip() or None
    side_raw = str(row.get("side") or row.get("maker_side") or row.get("taker_side") or "").strip().lower()
    side = None
    if side_raw in {"buy", "bid", "b"}:
        side = "buy"
    elif side_raw in {"sell", "ask", "s"}:
        side = "sell"

    price = None
    size = None
    try:
        price = float(row.get("price")) if row.get("price") is not None else None
    except Exception:
        price = None
    for key in ("size", "amount", "matched_amount", "quantity"):
        if row.get(key) is not None:
            try:
                size = float(row.get(key))
                break
            except Exception:
                pass

    ts = (
        _to_ts(row.get("timestamp"))
        or _to_ts(row.get("created_at"))
        or _to_ts(row.get("time"))
        or _to_ts(row.get("match_time"))
    )

    return trade_id, token_id, side, price, size, ts


async def _compute_live_trade_pnl_estimate(
    execution: ExecutionAdapter,
    seen_trade_ids: set[str],
    token_positions: dict[str, float],
    token_cashflows: dict[str, float],
    token_last_marks: dict[str, float],
    fill_timestamps: deque[float],
    last_trade_ts: int | None,
) -> tuple[float, int | None, int, int, dict[str, int]]:
    fetch_after = (last_trade_ts - 5) if last_trade_ts else None
    raw_trades = await execution.get_recent_trades(after_ts=fetch_after)

    newest_ts = last_trade_ts
    new_fills = 0
    fills_by_token: dict[str, int] = {}
    for row in raw_trades:
        trade_id, token_id, side, price, size, ts = _parse_trade_row(row)
        if not trade_id or trade_id in seen_trade_ids:
            continue
        seen_trade_ids.add(trade_id)
        if ts is not None and (newest_ts is None or ts > newest_ts):
            newest_ts = ts
        if not token_id or not side or price is None or size is None or size <= 0:
            continue

        if side == "buy":
            token_positions[token_id] = token_positions.get(token_id, 0.0) + size
            token_cashflows[token_id] = token_cashflows.get(token_id, 0.0) - (price * size)
        else:
            token_positions[token_id] = token_positions.get(token_id, 0.0) - size
            token_cashflows[token_id] = token_cashflows.get(token_id, 0.0) + (price * size)
        token_last_marks[token_id] = price
        fill_timestamps.append(float(ts) if ts is not None else time.time())
        new_fills += 1
        fills_by_token[token_id] = fills_by_token.get(token_id, 0) + 1

    _prune_fill_window(fill_timestamps, time.time())

    mark_value = 0.0
    missing_marks = 0
    for token_id, pos in token_positions.items():
        if abs(pos) < 1e-12:
            continue
        top = await execution.get_top_of_book(token_id)
        if top:
            mid = (top[0] + top[1]) / 2.0
            token_last_marks[token_id] = mid
            mark_value += pos * mid
            continue
        fallback_mark = token_last_marks.get(token_id)
        if fallback_mark is None:
            missing_marks += 1
            continue
        mark_value += pos * fallback_mark

    cash_value = sum(token_cashflows.values())
    pnl_est = cash_value + mark_value
    return pnl_est, newest_ts, new_fills, missing_marks, fills_by_token


async def _replace_quotes(execution: ExecutionAdapter, market_id: str, quotes: list[Quote]) -> int:
    await execution.cancel_market(market_id)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    posted = 0
    try:
        for quote in quotes:
            client_order_id = _make_client_order_id(run_id, quote)
            await execution.place_quote(quote, client_order_id=client_order_id)
            posted += 1
    except Exception as exc:
        # Do not leave partially replaced quotes on one side after a post failure.
        try:
            await execution.cancel_market(market_id)
        except Exception as cleanup_exc:
            logging.error("Cleanup cancel failed after partial replace on %s: %s", market_id, cleanup_exc)
        raise RuntimeError(f"Replace failed after posting {posted}/{len(quotes)} quotes") from exc
    return posted


async def _refresh_live_market_top_of_book(
    execution: ExecutionAdapter, market: MarketSnapshot
) -> tuple[MarketSnapshot | None, dict | None]:
    if not market.token_id:
        return None, None
    snapshot = await execution.get_book_snapshot(market.token_id)
    if not snapshot:
        return None, None
    bid = float(snapshot.get("bid") or 0.0)
    ask = float(snapshot.get("ask") or 0.0)
    if bid <= 0 or ask <= bid:
        return None, snapshot
    return replace(market, best_bid=bid, best_ask=ask), snapshot


def _fill_probability(order: DryRunOrder, market: MarketSnapshot, args: argparse.Namespace) -> float:
    mid = market.mid_price
    spread = market.spread
    if mid <= 0 or spread <= 0:
        return 0.0

    half_spread = max(spread / 2.0, 1e-9)
    distance = abs(order.price - mid)
    distance_ratio = min(distance / half_spread, 1.0)
    age_seconds = (datetime.now(timezone.utc) - order.created_at).total_seconds()
    if age_seconds < max(args.min_book_seconds_before_fill, 0.0):
        return 0.0

    proximity = 1.0 - distance_ratio
    queue_priority = 1.0 - order.queue_ahead_ratio
    queue_component = args.fill_sensitivity * proximity * max(queue_priority, 0.0)

    boost_window = max(args.queue_time_boost_seconds, 0.1)
    time_progress = min(age_seconds / boost_window, 1.0)
    time_component = max(args.max_time_bonus, 0.0) * time_progress

    prob = args.fill_base_prob + queue_component + time_component
    return max(0.0, min(prob, args.fill_max_prob))


def _prune_fill_window(fill_timestamps: deque[float], now_ts: float) -> None:
    while fill_timestamps and (now_ts - fill_timestamps[0]) > 60.0:
        fill_timestamps.popleft()


def _prune_time_window(timestamps: deque[float], now_ts: float, window_seconds: float = 60.0) -> None:
    while timestamps and (now_ts - timestamps[0]) > max(window_seconds, 0.1):
        timestamps.popleft()


def _effective_requote_bps(args: argparse.Namespace, mode: str) -> float:
    if mode == "live":
        return max(args.min_requote_bps, args.live_requote_epsilon_bps)
    return args.min_requote_bps


async def _simulate_cycle_fills(
    simulate_fills: bool,
    args: argparse.Namespace,
    order_manager: OrderManager,
    inventory: InventoryManager,
    market_by_id: dict[str, MarketSnapshot],
    fill_timestamps: deque[float],
) -> tuple[int, float]:
    if not simulate_fills:
        return 0, 0.0

    fills = 0
    pnl_delta = 0.0
    now_ts = time.time()

    for order in list(order_manager.get_all_orders()):
        market = market_by_id.get(order.market_id)
        if market is None:
            continue

        prob = _fill_probability(order, market, args)
        if random.random() >= prob:
            continue

        filled = order_manager.remove_order(order.market_id, order.side)
        if filled is None:
            continue

        inventory.apply_fill(filled.market_id, filled.side, filled.size)
        mid = market.mid_price
        edge = (mid - filled.price) * filled.size if filled.side == "buy" else (filled.price - mid) * filled.size
        # Subtract simulated taker fee so dry/paper PnL reflects real costs.
        fee_cost = filled.price * filled.size * (max(args.simulated_taker_fee_pct, 0.0) / 100.0)
        pnl_delta += edge - fee_cost

        fills += 1
        fill_timestamps.append(now_ts)

    _prune_fill_window(fill_timestamps, now_ts)
    return fills, pnl_delta


async def run_bot(args: argparse.Namespace) -> None:
    _validate_mode_args(args)
    simulate_fills = _runtime_simulate_fills(args)

    random.seed(args.random_seed)

    selector = RewardsMarketSelector(
        top_n=args.top_n,
        min_liquidity=args.min_liquidity,
        min_spread_bps=args.min_spread_bps,
        min_volume_24h=args.min_volume_24h,
        max_volume_24h=args.max_volume_24h,
        min_reward_efficiency=args.min_reward_efficiency,
    )
    quote_config = QuoteConfig(
        distance_to_mid_bps=args.distance_to_mid_bps,
        size=args.quote_size,
        quote_style=args.quote_style,
        top_of_book_improve_ticks=args.top_of_book_improve_ticks,
        touch_gap_ticks=args.touch_gap_ticks,
    )
    inventory = InventoryManager(
        InventoryLimits(
            max_position_per_market=args.max_position_per_market,
            global_exposure_limit=args.global_exposure_limit,
            imbalance_threshold=args.imbalance_threshold,
        )
    )
    risk = RiskManager(
        RiskConfig(
            max_drawdown_pct=args.max_drawdown_pct,
            max_fills_per_minute=args.max_fills_per_minute,
            volatility_spike_bps=args.volatility_spike_bps,
        )
    )
    order_manager = OrderManager()
    live_client = None
    if args.mode == "live":
        live_client = LiveHttpExecutionClient.from_env()

    execution = ExecutionAdapter(
        config=ExecutionConfig(mode=args.mode, enable_live_execution=args.enable_live_execution),
        order_manager=order_manager,
        live_client=live_client,
    )

    if args.mode == "live":
        auth_ok = await execution.validate_trading_auth()
        if not auth_ok:
            raise RuntimeError(
                "Live auth preflight failed. Check PRIVATE_KEY/FUNDER_ADDRESS/SIGNATURE_TYPE and "
                "re-derive POLY_API_KEY/POLY_API_SECRET/POLY_API_PASSPHRASE in this same session."
            )
        logging.info("Live auth preflight passed.")
        if args.live_preflight_only:
            logging.info("Live preflight-only mode requested. Exiting without placing orders.")
            return

    state_store = StateStore(args.state_db)
    if not args.disable_state_persistence:
        await state_store.init()

    fill_timestamps: deque[float] = deque()
    cycle = 0
    infinite = args.cycles < 0
    ranking_path = Path(args.ranking_file)

    base_equity = args.initial_equity
    if args.resume_state and not args.disable_state_persistence:
        last_equity = await state_store.load_last_equity()
        if last_equity is not None:
            base_equity = last_equity
            logging.info("Resuming equity from state DB: %.2f", base_equity)

    realized_pnl = 0.0
    equity = base_equity
    peak_equity = equity
    daily_anchor_day = datetime.now().date()
    daily_anchor_equity = equity
    last_heartbeat_ts = 0.0
    consecutive_heartbeat_failures = 0
    live_quote_state: dict[str, tuple[list[Quote], float]] = {}
    live_seen_trade_ids: set[str] = set()
    live_token_positions: dict[str, float] = {}
    live_token_cashflows: dict[str, float] = {}
    live_token_last_marks: dict[str, float] = {}
    cancel_timestamps: deque[float] = deque()
    live_last_primary_market_id: str | None = None
    live_last_primary_market_age = 0
    # Start from "now" in live mode to avoid replaying entire historical trade history
    # as if it were fresh fills for this bot session.
    live_last_trade_ts: int | None = int(time.time()) if args.mode == "live" else None
    consecutive_no_fill_cycles = 0
    live_market_no_fill_cycles: dict[str, int] = {}
    live_market_cooldown_until: dict[str, int] = {}
    live_fillability_cache: dict[str, tuple[int, float]] = {}
    # Cache for get_open_orders_count: {market_id: (cycle_checked, count)}
    # Reduces expensive paginated API calls from every cycle to every N cycles.
    live_open_orders_cache: dict[str, tuple[int, int]] = {}
    # Protection 1 – mid-drift: last known mid and pause registry per market.
    live_last_mid_by_market: dict[str, float] = {}
    live_mid_drift_pause_until: dict[str, int] = {}
    # Protection 3 – fill efficiency: total active cycles and fill count per market.
    live_market_active_cycles: dict[str, int] = {}
    live_market_fill_count: dict[str, int] = {}

    async def _get_open_orders_cached(market_id: str) -> tuple[int | None, bool]:
        """Returns (count, known). known=False means API failed and no cached fallback exists."""
        cache_ttl = max(int(args.open_orders_cache_cycles), 0)
        if cache_ttl > 0:
            cached = live_open_orders_cache.get(market_id)
            if cached and (cycle - cached[0]) < cache_ttl:
                return cached[1], True
        retries = max(int(args.open_orders_retries), 0)
        base_delay = max(float(args.open_orders_retry_delay_seconds), 0.0)
        attempts = retries + 1
        count: int | None = None
        for attempt in range(attempts):
            count = await execution.get_open_orders_count(market_id=market_id)
            if count is not None:
                break
            if attempt < (attempts - 1) and base_delay > 0:
                await asyncio.sleep(base_delay * (attempt + 1))
        if count is not None:
            live_open_orders_cache[market_id] = (cycle, count)
            return count, True
        cached = live_open_orders_cache.get(market_id)
        if cached:
            logging.warning(
                "Open-order count unavailable for %s; using stale cached value=%d from cycle=%d",
                market_id,
                cached[1],
                cached[0],
            )
            return cached[1], True
        logging.warning("Open-order count unavailable for %s and no cache present", market_id)
        return None, False

    live_market_blacklist = _parse_market_blacklist(args.live_market_blacklist)
    live_posted_since_fill = 0
    live_paused_low_balance = False
    if args.mode == "live" and live_market_blacklist:
        logging.info("Live market blacklist loaded: %d markets", len(live_market_blacklist))

    while infinite or cycle < args.cycles:
        cycle += 1
        cycle_max_mid_move_bps = 0.0
        markets = await selector.load_markets(ranking_path)
        market_by_id = {m.market_id: m for m in markets}
        ranking_pool_n = 0 if args.rewards_capture_mode else None
        ranked_selected = selector.select_markets(markets, top_n_override=ranking_pool_n)
        if args.rewards_capture_mode and ranked_selected:
            per_market_capital: float | None = None
            if args.rewards_capital_usd > 0:
                util = max(min(args.rewards_capital_utilization, 1.0), 0.0)
                per_market_capital = (args.rewards_capital_usd * util) / max(args.active_markets, 1)

            capture_filtered: list[MarketSnapshot] = []
            for m in ranked_selected:
                if not m.accepting_orders:
                    continue
                if m.rewards < max(args.rewards_min_daily_rate, 0.0):
                    continue
                if args.rewards_max_min_size > 0 and m.rewards_min_size > args.rewards_max_min_size:
                    continue
                if not _market_meets_stability_rule(m, args.stable_mid_min, args.stable_mid_max):
                    continue
                if not _market_has_time_buffer(m, args.stable_min_hours_to_end):
                    continue
                target_size = _reward_min_size_target(m, args.rewards_size_buffer)
                if args.rewards_max_market_notional_usd > 0 and target_size > 0:
                    required_notional = target_size * max(m.mid_price, 1e-9)
                    if required_notional > args.rewards_max_market_notional_usd:
                        continue
                if per_market_capital is not None and target_size > 0:
                    required_notional = target_size * max(m.mid_price, 1e-9)
                    if required_notional > per_market_capital:
                        continue
                capture_filtered.append(m)

            if capture_filtered:
                ranked_selected = capture_filtered
            else:
                ranked_selected = []
                logging.warning("Rewards capture prefilter removed all candidates this cycle")
            logging.info(
                "Rewards capture prefilter: kept=%d/%d capital_per_market=%s",
                len(capture_filtered),
                len(markets),
                f"{per_market_capital:.2f}" if per_market_capital is not None else "n/a",
            )
        if args.top_n > 0 and len(ranked_selected) > args.top_n:
            ranked_selected = ranked_selected[: args.top_n]
        if args.mode == "live" and ranked_selected:
            eligible = [
                m
                for m in ranked_selected
                if m.market_id not in live_market_blacklist
                and cycle >= live_market_cooldown_until.get(m.market_id, 0)
            ]
            capped = [m for m in eligible if m.spread_bps <= max(args.live_selection_max_spread_bps, 0.0)]
            candidates = capped if capped else eligible
            if not candidates:
                candidates = ranked_selected
            ranked_selected = sorted(
                candidates,
                key=lambda m: _live_selection_score(m, args.live_selection_target_spread_bps, args.reward_efficiency_weight),
                reverse=True,
            )
            before_fillable = len(ranked_selected)
            ranked_selected = await _rank_live_fillable_markets(
                execution=execution,
                ranked_markets=ranked_selected,
                args=args,
                cycle=cycle,
                cache=live_fillability_cache,
            )
            if ranked_selected:
                logging.debug(
                    "Live fillable ranking applied: candidates=%d top_market=%s",
                    before_fillable,
                    ranked_selected[0].market_id,
                )
        selected = selector.rotate_markets(
            ranked_markets=ranked_selected,
            cycle_number=cycle,
            active_markets=args.active_markets,
            rotation_step=args.rotation_step,
        )
        if args.mode == "live":
            # Do not rotate away from markets where we still carry inventory.
            pinned_markets: list[MarketSnapshot] = []
            for m in ranked_selected:
                yes_pos = abs(_token_position(live_token_positions, m.token_id))
                no_pos = abs(_token_position(live_token_positions, m.no_token_id))
                if yes_pos >= max(args.inventory_unwind_threshold, 0.0) or no_pos >= max(args.inventory_unwind_threshold, 0.0):
                    pinned_markets.append(m)

            if pinned_markets:
                pinned_ids = {m.market_id for m in pinned_markets}
                selected = pinned_markets[: max(args.active_markets, 1)]
                if len(selected) < max(args.active_markets, 1):
                    extra = [m for m in ranked_selected if m.market_id not in pinned_ids]
                    selected.extend(extra[: max(args.active_markets, 1) - len(selected)])
            elif args.active_markets == 1 and ranked_selected:
                ranked_by_id = {m.market_id: m for m in ranked_selected}
                stale_market_id = None
                if live_last_primary_market_id:
                    no_fill_for_primary = live_market_no_fill_cycles.get(live_last_primary_market_id, 0)
                    no_fill_gate = max(no_fill_for_primary, consecutive_no_fill_cycles)
                    primary_open_orders, primary_open_orders_known = await _get_open_orders_cached(live_last_primary_market_id)
                    if no_fill_gate >= max(args.no_fill_rotate_cycles, 1):
                        if primary_open_orders_known and (primary_open_orders or 0) <= 0:
                            stale_market_id = live_last_primary_market_id
                            logging.info(
                                "Forced rotation: market %s no fills primary=%d global=%d cycles",
                                stale_market_id,
                                no_fill_for_primary,
                                consecutive_no_fill_cycles,
                            )
                        elif primary_open_orders_known:
                            logging.info(
                                "Hold primary market %s despite no-fill gate: open_orders=%d",
                                live_last_primary_market_id,
                                primary_open_orders,
                            )
                        else:
                            logging.info(
                                "Hold primary market %s: open-orders state unknown this cycle",
                                live_last_primary_market_id,
                            )
                if (
                    live_last_primary_market_id
                    and live_last_primary_market_id in ranked_by_id
                    and stale_market_id is None
                    and live_last_primary_market_age < max(args.market_hold_cycles, 1)
                ):
                    selected = [ranked_by_id[live_last_primary_market_id]]
                    live_last_primary_market_age += 1
                else:
                    if stale_market_id is not None:
                        alternatives = [m for m in ranked_selected if m.market_id != stale_market_id]
                        selected = [alternatives[0]] if alternatives else [ranked_selected[0]]
                    else:
                        selected = [ranked_selected[0]]
                    live_last_primary_market_id = selected[0].market_id
                    live_last_primary_market_age = 1

        selected_ids = {m.market_id for m in selected}
        current_selected_market_id = selected[0].market_id if (args.mode == "live" and args.active_markets == 1 and selected) else None
        cycle_candidates = selected
        if args.mode == "live" and args.active_markets == 1 and selected:
            primary = selected[0]
            extra_limit = max(args.live_fallback_candidates - 1, 0)
            extras = [m for m in ranked_selected if m.market_id != primary.market_id][:extra_limit]
            cycle_candidates = [primary] + extras

        cycle_fills, pnl_delta = await _simulate_cycle_fills(
            simulate_fills=simulate_fills,
            args=args,
            order_manager=order_manager,
            inventory=inventory,
            market_by_id=market_by_id,
            fill_timestamps=fill_timestamps,
        )
        realized_pnl += pnl_delta
        equity = base_equity + realized_pnl

        if args.mode == "live":
            live_pnl_est, live_last_trade_ts, live_new_fills, live_missing_marks, live_fills_by_token = await _compute_live_trade_pnl_estimate(
                execution=execution,
                seen_trade_ids=live_seen_trade_ids,
                token_positions=live_token_positions,
                token_cashflows=live_token_cashflows,
                token_last_marks=live_token_last_marks,
                fill_timestamps=fill_timestamps,
                last_trade_ts=live_last_trade_ts,
            )
            realized_pnl = live_pnl_est
            equity = base_equity + realized_pnl
            cycle_fills = live_new_fills
        current_market_fills = cycle_fills
        if args.mode == "live" and current_selected_market_id:
            current_market_fills = 0
            current_market = market_by_id.get(current_selected_market_id)
            if current_market is not None:
                if current_market.token_id:
                    current_market_fills += live_fills_by_token.get(current_market.token_id, 0)
                if current_market.no_token_id:
                    current_market_fills += live_fills_by_token.get(current_market.no_token_id, 0)
        if cycle_fills > 0:
            consecutive_no_fill_cycles = 0
            if args.mode == "live":
                live_posted_since_fill = 0
        else:
            consecutive_no_fill_cycles += 1
        if current_selected_market_id:
            if current_market_fills > 0:
                live_market_no_fill_cycles[current_selected_market_id] = 0
                # Protection 3: track fill count for efficiency monitor.
                live_market_fill_count[current_selected_market_id] = (
                    live_market_fill_count.get(current_selected_market_id, 0) + current_market_fills
                )
            else:
                live_market_no_fill_cycles[current_selected_market_id] = live_market_no_fill_cycles.get(current_selected_market_id, 0) + 1
                no_fill_count = live_market_no_fill_cycles[current_selected_market_id]
                if (
                    args.mode == "live"
                    and no_fill_count >= max(args.live_no_fill_cooldown_threshold, 1)
                    and args.live_no_fill_cooldown_cycles > 0
                ):
                    open_orders_primary, open_orders_primary_known = await _get_open_orders_cached(current_selected_market_id)
                    if open_orders_primary_known and (open_orders_primary or 0) <= 0:
                        cooldown_until = cycle + args.live_no_fill_cooldown_cycles
                        previous = live_market_cooldown_until.get(current_selected_market_id, 0)
                        if cooldown_until > previous:
                            live_market_cooldown_until[current_selected_market_id] = cooldown_until
                            live_market_no_fill_cycles[current_selected_market_id] = 0
                            logging.info(
                                "Applying no-fill cooldown: market=%s until_cycle=%d",
                                current_selected_market_id,
                                cooldown_until,
                            )

            # --- Protection 3: Fill efficiency monitor ---
            # After a warmup period, if a market's fill rate is below the minimum,
            # apply an extended cooldown — it's too competitive or illiquid to earn rewards.
            if (
                args.mode == "live"
                and args.min_fill_efficiency_pct > 0
                and args.fill_efficiency_warmup_cycles > 0
            ):
                active_c = live_market_active_cycles.get(current_selected_market_id, 0)
                fills_c = live_market_fill_count.get(current_selected_market_id, 0)
                if active_c >= args.fill_efficiency_warmup_cycles:
                    fill_rate_pct = (fills_c / active_c) * 100.0
                    if fill_rate_pct < args.min_fill_efficiency_pct:
                        cooldown_until = cycle + max(args.fill_efficiency_cooldown_cycles, 1)
                        previous = live_market_cooldown_until.get(current_selected_market_id, 0)
                        if cooldown_until > previous:
                            live_market_cooldown_until[current_selected_market_id] = cooldown_until
                            # Reset counters so the market gets a fresh evaluation after cooldown.
                            live_market_active_cycles[current_selected_market_id] = 0
                            live_market_fill_count[current_selected_market_id] = 0
                            logging.warning(
                                "Low fill efficiency: market=%s fill_rate=%.1f%% < min=%.1f%% "
                                "over %d cycles — applying cooldown until cycle %d",
                                current_selected_market_id,
                                fill_rate_pct,
                                args.min_fill_efficiency_pct,
                                active_c,
                                cooldown_until,
                            )
        if args.mode == "live" and consecutive_no_fill_cycles >= max(args.live_stop_after_no_fill_cycles, 1):
            logging.warning(
                "Risk kill-switch: no fills for %d consecutive cycles (limit=%d)",
                consecutive_no_fill_cycles,
                args.live_stop_after_no_fill_cycles,
            )
            await execution.cancel_all()
            cancel_timestamps.append(time.time())
            break
        peak_equity = max(peak_equity, equity)
        now_day = datetime.now().date()
        if now_day != daily_anchor_day:
            daily_anchor_day = now_day
            daily_anchor_equity = equity
            logging.info("Daily anchor reset: day=%s equity=%.2f", daily_anchor_day.isoformat(), daily_anchor_equity)

        daily_loss = max(daily_anchor_equity - equity, 0.0)
        drawdown_pct = ((peak_equity - equity) / peak_equity * 100.0) if peak_equity > 0 else 0.0
        fills_last_minute = len(fill_timestamps)
        expected_daily_rewards = sum(m.rewards for m in selected)

        if args.mode == "live":
            logging.info(
                "Mode=%s Cycle %d: loaded=%d ranked=%d active=%d fills=%d fills_1m=%d pnl_est=%.4f equity=%.2f dd=%.2f%% mark_missing=%d max_mid_move=%.1fbps exp_daily_rewards=%.2f",
                args.mode,
                cycle,
                len(markets),
                len(ranked_selected),
                len(selected),
                cycle_fills,
                fills_last_minute,
                realized_pnl,
                equity,
                drawdown_pct,
                live_missing_marks,
                cycle_max_mid_move_bps,
                expected_daily_rewards,
            )
        else:
            logging.info(
                "Mode=%s Cycle %d: loaded=%d ranked=%d active=%d fills=%d fills_1m=%d pnl=%.4f equity=%.2f dd=%.2f%% exp_daily_rewards=%.2f",
                args.mode,
                cycle,
                len(markets),
                len(ranked_selected),
                len(selected),
                cycle_fills,
                fills_last_minute,
                realized_pnl,
                equity,
                drawdown_pct,
                expected_daily_rewards,
            )

        if daily_loss >= max(args.daily_loss_limit_usd, 0.0):
            logging.warning(
                "Risk kill-switch: daily loss exceeded (loss=%.2f >= limit=%.2f)",
                daily_loss,
                args.daily_loss_limit_usd,
            )
            await execution.cancel_all()
            break

        if args.mode == "live":
            now_ts = time.time()
            if (now_ts - last_heartbeat_ts) >= max(args.live_heartbeat_seconds, 0.1):
                is_alive = await execution.health_check()
                last_heartbeat_ts = now_ts
                if not is_alive:
                    consecutive_heartbeat_failures += 1
                    logging.warning(
                        "Live heartbeat failed (%d/%d)",
                        consecutive_heartbeat_failures,
                        args.live_health_max_failures,
                    )
                else:
                    consecutive_heartbeat_failures = 0

                if consecutive_heartbeat_failures >= max(args.live_health_max_failures, 1):
                    logging.warning("Risk kill-switch: connection lost, cancelling all orders")
                    await execution.cancel_all()
                    cancel_timestamps.append(time.time())
                    break

        if not risk.can_place_more_fills(fills_last_minute):
            logging.warning("Risk kill-switch: max fills/min exceeded (%d)", fills_last_minute)
            await execution.cancel_all()
            cancel_timestamps.append(time.time())
            break

        if not risk.within_drawdown(drawdown_pct):
            logging.warning("Risk kill-switch: max drawdown exceeded (%.2f%%)", drawdown_pct)
            await execution.cancel_all()
            cancel_timestamps.append(time.time())
            break

        if (
            args.mode == "live"
            and args.volatility_spike_bps > 0
            and risk.should_halt_on_volatility(cycle_max_mid_move_bps)
        ):
            logging.warning(
                "Risk kill-switch: volatility spike detected (%.1fbps >= limit=%.1fbps)",
                cycle_max_mid_move_bps,
                args.volatility_spike_bps,
            )
            await execution.cancel_all()
            cancel_timestamps.append(time.time())
            break

        live_per_order_budget_usd: float | None = None
        cycle_enable_dual_buy = bool(args.enable_live_dual_buy)
        live_allow_new_quotes = True
        if args.mode == "live":
            live_balance_usd = await execution.get_collateral_balance_usd()
            if live_balance_usd is not None:
                reserve = max(args.live_reserve_balance_usd, 0.0)
                usable = max(live_balance_usd - reserve, 0.0)
                if usable < max(args.live_min_usable_balance_usd, 0.0):
                    live_allow_new_quotes = False
                    if not live_paused_low_balance:
                        live_paused_low_balance = True
                        logging.warning(
                            "Low usable balance %.4f < %.4f: pausing live quoting and canceling all",
                            usable,
                            args.live_min_usable_balance_usd,
                        )
                        await execution.cancel_all()
                        cancel_timestamps.append(time.time())
                elif live_paused_low_balance:
                    live_paused_low_balance = False
                    logging.info(
                        "Usable balance recovered %.4f >= %.4f: resuming live quoting",
                        usable,
                        args.live_min_usable_balance_usd,
                    )
                if (
                    args.live_auto_single_side_on_low_balance
                    and cycle_enable_dual_buy
                    and usable < (max(args.live_min_order_size, 0.0) * 2.0)
                ):
                    cycle_enable_dual_buy = False
                    logging.info(
                        "Low usable balance %.4f: auto switching to single-side live quoting",
                        usable,
                    )
                side_factor = 2 if cycle_enable_dual_buy else 1
                denom = max(len(selected) * side_factor, 1)
                live_per_order_budget_usd = usable / denom
                logging.info(
                    "Live budget: balance=%.4f reserve=%.4f usable=%.4f per_order=%.4f dual_side=%s",
                    live_balance_usd,
                    reserve,
                    usable,
                    live_per_order_budget_usd,
                    cycle_enable_dual_buy,
                )

        if args.mode == "live":
            for market_id in list(live_quote_state.keys()):
                if market_id not in selected_ids:
                    try:
                        await execution.cancel_market(market_id)
                        cancel_timestamps.append(time.time())
                        live_quote_state.pop(market_id, None)
                    except Exception as exc:
                        logging.error("Order cancel error for market %s: %s", market_id, exc)
                        if args.halt_on_order_error:
                            logging.warning("Risk kill-switch: halting on order error")
                            await execution.cancel_all()
                            cancel_timestamps.append(time.time())
                            return
        else:
            for market_id in list(order_manager.active_orders.keys()):
                if market_id not in selected_ids and order_manager.can_requote(market_id, args.min_action_interval_seconds):
                    try:
                        await execution.cancel_market(market_id)
                    except Exception as exc:
                        logging.error("Order cancel error for market %s: %s", market_id, exc)
                        if args.halt_on_order_error:
                            logging.warning("Risk kill-switch: halting on order error")
                            await execution.cancel_all()
                            return

        live_market_acted = False
        for market in cycle_candidates:
            if args.mode == "live" and not market.token_id:
                logging.warning("Skipping live market %s: missing token_id/asset_id in ranking snapshot", market.market_id)
                continue
            if args.mode == "live" and not market.no_token_id:
                logging.warning("Skipping live market %s: missing no_token_id in ranking snapshot", market.market_id)
                continue

            if args.mode == "live":
                refreshed_market, book_snapshot = await _refresh_live_market_top_of_book(execution, market)
                if refreshed_market is None:
                    logging.warning("Skipping live market %s: no real top-of-book available", market.market_id)
                    continue
                market = refreshed_market

                mid_now = market.mid_price
                last_mid = live_last_mid_by_market.get(market.market_id)
                live_last_mid_by_market[market.market_id] = mid_now
                drift_bps = 0.0
                if last_mid is not None and last_mid > 0:
                    drift_bps = abs(mid_now - last_mid) / last_mid * 10_000
                    cycle_max_mid_move_bps = max(cycle_max_mid_move_bps, drift_bps)

                # --- Protection 1: Mid-drift detector ---
                # If the mid price jumped too far in one cycle someone informed is
                # moving the market.  Cancel open orders immediately and pause quoting
                # to avoid being filled on the wrong side of the move.
                if args.mid_drift_pause_bps > 0 and drift_bps >= args.mid_drift_pause_bps:
                    pause_until = cycle + max(args.mid_drift_pause_cycles, 1)
                    live_mid_drift_pause_until[market.market_id] = pause_until
                    logging.warning(
                        "Mid-drift detected: market=%s drift=%.1fbps (%.4f->%.4f) "
                        "cancelling orders and pausing until cycle %d",
                        market.market_id, drift_bps, last_mid, mid_now, pause_until,
                    )
                    try:
                        await execution.cancel_market(market.market_id)
                        live_open_orders_cache.pop(market.market_id, None)
                        cancel_timestamps.append(time.time())
                    except Exception as exc:
                        logging.error("Cancel on mid-drift failed for %s: %s", market.market_id, exc)
                    continue

                if cycle < live_mid_drift_pause_until.get(market.market_id, 0):
                    logging.debug(
                        "Market %s in mid-drift pause (resumes cycle %d)",
                        market.market_id, live_mid_drift_pause_until[market.market_id],
                    )
                    continue

                if args.live_min_recent_trades > 0:
                    activity_after = int(time.time()) - max(int(args.live_activity_window_seconds), 1)
                    recent_trade_count = await execution.get_market_recent_trade_count(market.market_id, after_ts=activity_after)
                    if recent_trade_count < args.live_min_recent_trades:
                        if args.live_strict_activity_filter:
                            logging.info(
                                "Skipping live market %s: low recent trades=%d (min=%d window=%ds)",
                                market.market_id,
                                recent_trade_count,
                                args.live_min_recent_trades,
                                args.live_activity_window_seconds,
                            )
                            continue
                        logging.debug(
                            "Soft activity signal for market %s: recent trades=%d (min=%d window=%ds)",
                            market.market_id,
                            recent_trade_count,
                            args.live_min_recent_trades,
                            args.live_activity_window_seconds,
                        )

                top_bid_size = float((book_snapshot or {}).get("bid_size") or 0.0)
                top_ask_size = float((book_snapshot or {}).get("ask_size") or 0.0)
                bid_levels = int((book_snapshot or {}).get("bid_levels") or 0)
                ask_levels = int((book_snapshot or {}).get("ask_levels") or 0)
                if top_bid_size < max(args.live_min_top_size, 0.0) or top_ask_size < max(args.live_min_top_size, 0.0):
                    logging.info(
                        "Skipping live market %s: top size too small bid=%.4f ask=%.4f (min=%.4f)",
                        market.market_id,
                        top_bid_size,
                        top_ask_size,
                        args.live_min_top_size,
                    )
                    continue
                if bid_levels < max(args.live_min_book_levels, 1) or ask_levels < max(args.live_min_book_levels, 1):
                    logging.info(
                        "Skipping live market %s: book levels too shallow bid_levels=%d ask_levels=%d (min=%d)",
                        market.market_id,
                        bid_levels,
                        ask_levels,
                        args.live_min_book_levels,
                    )
                    continue
                if args.rewards_capture_mode:
                    imbalance = _top_imbalance_ratio(top_bid_size, top_ask_size)
                    if imbalance > max(args.stable_max_top_imbalance_ratio, 1.0):
                        logging.info(
                            "Skipping live market %s: top imbalance %.2f > max %.2f",
                            market.market_id,
                            imbalance,
                            args.stable_max_top_imbalance_ratio,
                        )
                        continue

            if not inventory.has_market(market.market_id):
                inventory.set_market_exposure(market.market_id, market.yes_exposure, market.no_exposure)

            mid = calculate_mid_price(market.best_bid, market.best_ask)
            if mid is None:
                logging.warning("Skipping invalid market %s", market.market_id)
                continue

            adaptive_distance_bps = args.distance_to_mid_bps
            if args.mode == "live" and args.no_fill_tighten_cycles > 0 and args.tighten_step_bps > 0:
                # The lower bound should cap tightening, not widen the baseline quote.
                # Clamp it so default/base settings can't accidentally move quotes farther away.
                min_distance_floor = max(min(args.min_distance_to_mid_bps, args.distance_to_mid_bps), 0.0)
                # Use per-market no-fill counter so a quiet market tightens its own
                # quotes without penalising other markets that are filling normally.
                market_no_fill = live_market_no_fill_cycles.get(market.market_id, consecutive_no_fill_cycles)
                tighten_blocks = market_no_fill // args.no_fill_tighten_cycles
                adaptive_distance_bps = max(
                    min_distance_floor,
                    args.distance_to_mid_bps - (tighten_blocks * args.tighten_step_bps),
                )
                if tighten_blocks > 0:
                    logging.info(
                        "Adaptive tighten active: market=%s no_fill_cycles=%d distance_bps=%.2f",
                        market.market_id,
                        market_no_fill,
                        adaptive_distance_bps,
                    )
            required_rewards_size = 0.0
            if args.rewards_capture_mode:
                required_rewards_size = _reward_min_size_target(market, args.rewards_size_buffer)
            cycle_quote_config = QuoteConfig(
                distance_to_mid_bps=adaptive_distance_bps,
                size=quote_config.size,
                quote_style=quote_config.quote_style,
                top_of_book_improve_ticks=quote_config.top_of_book_improve_ticks,
                touch_gap_ticks=quote_config.touch_gap_ticks,
            )
            if required_rewards_size > cycle_quote_config.size:
                cycle_quote_config = replace(cycle_quote_config, size=required_rewards_size)

            if market.spread_bps > max(args.hard_max_spread_bps, 0.0):
                logging.info(
                    "Skipping market %s: spread_bps %.2f > hard_max_spread_bps %.2f",
                    market.market_id,
                    market.spread_bps,
                    args.hard_max_spread_bps,
                )
                continue
            if market.reward_score < max(args.min_reward_score, 0.0):
                logging.info(
                    "Skipping market %s: reward_score %.4f < min_reward_score %.4f",
                    market.market_id,
                    market.reward_score,
                    args.min_reward_score,
                )
                continue
            if args.rewards_capture_mode:
                if not market.accepting_orders:
                    logging.info("Skipping market %s: not accepting orders", market.market_id)
                    continue
                if args.rewards_max_min_size > 0 and market.rewards_min_size > args.rewards_max_min_size:
                    logging.info(
                        "Skipping market %s: rewards_min_size %.2f > rewards_max_min_size %.2f",
                        market.market_id,
                        market.rewards_min_size,
                        args.rewards_max_min_size,
                    )
                    continue
                if not _market_meets_stability_rule(market, args.stable_mid_min, args.stable_mid_max):
                    logging.info(
                        "Skipping market %s: mid %.4f out of stable range [%.2f, %.2f]",
                        market.market_id,
                        market.mid_price,
                        args.stable_mid_min,
                        args.stable_mid_max,
                    )
                    continue
                if not _market_has_time_buffer(market, args.stable_min_hours_to_end):
                    logging.info(
                        "Skipping market %s: near end (min_hours_to_end=%.2f)",
                        market.market_id,
                        args.stable_min_hours_to_end,
                    )
                    continue
                if not _market_meets_rewards_spread_rule(market):
                    logging.info(
                        "Skipping market %s: spread %.4f > rewards_max_spread %.4f",
                        market.market_id,
                        market.spread,
                        market.rewards_max_spread / 100.0,
                    )
                    continue
                if args.rewards_max_market_notional_usd > 0 and required_rewards_size > 0:
                    required_notional = required_rewards_size * max(market.mid_price, 1e-9)
                    if required_notional > args.rewards_max_market_notional_usd:
                        logging.info(
                            "Skipping market %s: required notional %.2f > rewards_max_market_notional_usd %.2f",
                            market.market_id,
                            required_notional,
                            args.rewards_max_market_notional_usd,
                        )
                        continue
                if required_rewards_size > 0 and args.max_position_per_market < required_rewards_size:
                    logging.info(
                        "Skipping market %s: max_position_per_market %.4f < required rewards size %.4f",
                        market.market_id,
                        args.max_position_per_market,
                        required_rewards_size,
                    )
                    continue

            if args.mode == "live":
                unwind_quotes: list[Quote] = []
                if args.enable_live_unwind:
                    # In dynamic mode, scale the imbalance tolerance by reward_score:
                    # high-reward markets justify holding more inventory before unwinding.
                    effective_imbalance_threshold = args.inventory_imbalance_threshold_usd
                    if args.inventory_mode == "dynamic" and market.reward_score > 0:
                        scale = min(market.reward_score / 10.0, 3.0)
                        effective_imbalance_threshold *= scale
                    unwind_quotes = await _build_live_unwind_quotes(
                        execution=execution,
                        market=market,
                        token_positions=live_token_positions,
                        unwind_threshold=args.inventory_unwind_threshold,
                        unwind_size=args.inventory_unwind_size,
                        unwind_style=args.inventory_unwind_style,
                        enable_no_token_unwind=args.enable_no_token_unwind,
                        inventory_mode=args.inventory_mode,
                        imbalance_threshold_usd=effective_imbalance_threshold,
                    )
                if unwind_quotes:
                    # Unwind runs even when balance is too low for new quotes.
                    inventory_filtered = unwind_quotes
                    yes_p = _token_position(live_token_positions, market.token_id)
                    no_p = _token_position(live_token_positions, market.no_token_id)
                    imb = _compute_inventory_imbalance(yes_p, no_p, market.mid_price)
                    logging.info(
                        "Inventory unwind market %s: quotes=%d yes_pos=%.4f no_pos=%.4f imbalance_usd=%.2f",
                        market.market_id,
                        len(unwind_quotes),
                        yes_p,
                        no_p,
                        imb,
                    )
                elif not live_allow_new_quotes:
                    # No unwind needed and balance is too low: skip new quotes entirely.
                    continue
                else:
                    # --- Protection 2: Per-market USD exposure cap ---
                    # Block new quotes when a single market is consuming too much capital,
                    # but only after confirming no unwind was needed (unwind always runs above).
                    if args.max_market_exposure_usd > 0:
                        yes_exp_usd = abs(_token_position(live_token_positions, market.token_id) * mid)
                        no_exp_usd = abs(_token_position(live_token_positions, market.no_token_id) * max(1.0 - mid, 0.0))
                        market_exposure_usd = yes_exp_usd + no_exp_usd
                        if market_exposure_usd > args.max_market_exposure_usd:
                            logging.info(
                                "Market %s over-exposed: $%.2f > max $%.2f — holding, no new quotes",
                                market.market_id, market_exposure_usd, args.max_market_exposure_usd,
                            )
                            continue
                    quotes = generate_quotes(market, cycle_quote_config)
                    if not quotes:
                        logging.warning("No valid quotes generated for %s", market.market_id)
                        continue
                    execution_quotes = _to_live_quotes(
                        market,
                        quotes,
                        live_token_positions,
                        enable_dual_buy=cycle_enable_dual_buy,
                    )
                    inventory_filtered = _cap_live_quotes_by_budget(
                        execution_quotes,
                        live_per_order_budget_usd,
                        args.live_per_order_balance_fraction,
                    )
                    raw_count = len(inventory_filtered)
                    min_live_size = args.live_min_order_size
                    if args.rewards_capture_mode:
                        min_live_size = max(min_live_size, required_rewards_size)
                    inventory_filtered = _filter_quotes_by_min_size(inventory_filtered, min_live_size)
                    if raw_count and not inventory_filtered:
                        logging.info(
                            "Skipping live quotes for %s: capped sizes below min_size=%.4f",
                            market.market_id,
                            min_live_size,
                        )
                    if (
                        args.rewards_capture_mode
                        and args.rewards_require_dual_sided
                        and not _has_dual_token_quotes(inventory_filtered)
                    ):
                        logging.info(
                            "Skipping live market %s: dual-sided requirement not met after sizing (quotes=%d)",
                            market.market_id,
                            len(inventory_filtered),
                        )
                        continue
            else:
                quotes = generate_quotes(market, cycle_quote_config)
                if not quotes:
                    logging.warning("No valid quotes generated for %s", market.market_id)
                    continue
                execution_quotes = quotes
                inventory_filtered = [q for q in execution_quotes if inventory.side_allowed(market.market_id, q.side, q.size)]

            if not inventory_filtered:
                yes_exp, no_exp = inventory.get_market_exposure(market.market_id)
                logging.info("Inventory blocked market %s (yes=%.2f no=%.2f)", market.market_id, yes_exp, no_exp)
                if order_manager.get_market_orders(market.market_id):
                    try:
                        await execution.cancel_market(market.market_id)
                        cancel_timestamps.append(time.time())
                    except Exception as exc:
                        logging.error("Order cancel error for market %s: %s", market.market_id, exc)
                        if args.halt_on_order_error:
                            logging.warning("Risk kill-switch: halting on order error")
                            await execution.cancel_all()
                            cancel_timestamps.append(time.time())
                            return
                continue

            logging.info(
                "Market %s (%s) mid=%.4f spread_bps=%.2f reward_score=%.4f rewards_min_size=%.2f target_size=%.2f active_sides=%d",
                market.market_id,
                market.slug,
                mid,
                market.spread_bps,
                market.reward_score,
                market.rewards_min_size,
                required_rewards_size,
                len(inventory_filtered),
            )

            if args.mode == "live":
                prev_state = live_quote_state.get(market.market_id)
                prev_quotes = prev_state[0] if prev_state else []
                prev_ts = prev_state[1] if prev_state else 0.0
                quote_age = (time.time() - prev_ts) if prev_state else 0.0
                market_stale = quote_age >= args.stale_order_seconds if prev_state else False
                open_orders_market, open_orders_known = await _get_open_orders_cached(market.market_id)
                # Requote whenever there are no open orders, regardless of whether we had prior state.
                # This covers both first-time quoting and externally canceled orders.
                missing_open_orders = open_orders_known and (open_orders_market == 0)
                has_open_orders = open_orders_known and (open_orders_market is not None) and (open_orders_market > 0)
                in_replace_cooldown = (
                    prev_state is not None
                    and has_open_orders
                    and quote_age < max(args.live_replace_cooldown_seconds, 0.0)
                )
                in_min_quote_age = (
                    prev_state is not None
                    and has_open_orders
                    and quote_age < max(args.live_min_quote_age_seconds, 0.0)
                )
                requote_bps = _effective_requote_bps(args, args.mode)
                requires_requote = (
                    missing_open_orders
                    or market_stale
                    or (
                        not in_replace_cooldown
                        and not in_min_quote_age
                        and should_requote(prev_quotes, inventory_filtered, requote_bps)
                    )
                )
                if not open_orders_known and not market_stale:
                    # Avoid aggressive re-quote churn when open-order visibility is degraded.
                    requires_requote = False
                    logging.debug(
                        "Skipping requote for %s: open-orders state unknown and quotes not stale",
                        market.market_id,
                    )
            else:
                current_orders = order_manager.get_market_orders(market.market_id)
                current_quotes = [
                    Quote(market_id=order.market_id, token_id=None, side=order.side, price=order.price, size=order.size)
                    for order in current_orders
                ]
                market_stale = order_manager.is_market_stale(market.market_id, args.stale_order_seconds)
                requote_bps = _effective_requote_bps(args, args.mode)
                requires_requote = (
                    not current_orders
                    or market_stale
                    or should_requote(current_quotes, inventory_filtered, requote_bps)
                )

            if not requires_requote:
                logging.debug("Keeping existing quotes for %s", market.market_id)
                if args.mode == "live" and args.active_markets == 1:
                    live_market_acted = True
                    break
                continue

            if not order_manager.can_requote(market.market_id, args.min_action_interval_seconds):
                logging.debug("Requote throttled for %s", market.market_id)
                continue

            if args.mode == "live":
                now_ts = time.time()
                _prune_time_window(cancel_timestamps, now_ts, window_seconds=60.0)
                if len(cancel_timestamps) >= max(args.max_cancels_per_minute, 1):
                    logging.warning(
                        "Cancel throttle active (%d/%d in 60s). Skipping requote for %s",
                        len(cancel_timestamps),
                        args.max_cancels_per_minute,
                        market.market_id,
                    )
                    continue

            try:
                posted_now = await _replace_quotes(execution, market.market_id, inventory_filtered)
                if args.mode == "live":
                    live_posted_since_fill += posted_now
                    cancel_timestamps.append(time.time())
                    # Invalidate cached order count — we just placed new orders.
                    live_open_orders_cache.pop(market.market_id, None)
                    # Protection 3: count this as an active quoting cycle for efficiency tracking.
                    live_market_active_cycles[market.market_id] = (
                        live_market_active_cycles.get(market.market_id, 0) + 1
                    )
                if args.mode == "live":
                    live_quote_state[market.market_id] = (inventory_filtered, time.time())
                if args.mode == "live" and args.active_markets == 1:
                    live_market_acted = True
                    break
            except Exception as exc:
                logging.error("Order replace error for market %s: %s", market.market_id, exc)
                if (
                    args.mode == "live"
                    and args.ignore_recoverable_order_errors_live
                    and _is_recoverable_live_order_error(exc)
                ):
                    logging.warning(
                        "Recoverable live order error on %s. Continuing without kill-switch.",
                        market.market_id,
                    )
                    continue
                if args.halt_on_order_error:
                    logging.warning("Risk kill-switch: halting on order error")
                    await execution.cancel_all()
                    cancel_timestamps.append(time.time())
                    return

        if args.mode == "live" and live_posted_since_fill >= max(args.live_stop_after_posts_without_fill, 1):
            logging.warning(
                "Risk kill-switch: posted_without_fill=%d >= limit=%d",
                live_posted_since_fill,
                args.live_stop_after_posts_without_fill,
            )
            await execution.cancel_all()
            cancel_timestamps.append(time.time())
            break

        if args.mode == "live" and args.active_markets == 1 and not live_market_acted:
            logging.info("No live candidate qualified this cycle (candidates=%d)", len(cycle_candidates))

        if not args.disable_state_persistence:
            await state_store.save_cycle(
                cycle=cycle,
                mode=args.mode,
                fills=cycle_fills,
                fills_1m=fills_last_minute,
                realized_pnl=realized_pnl,
                equity=equity,
                drawdown_pct=drawdown_pct,
                order_manager=order_manager,
                inventory=inventory,
                extra={"selected_ids": sorted(selected_ids), "simulate_fills": simulate_fills},
            )

        if infinite or cycle < args.cycles:
            await asyncio.sleep(max(args.refresh_seconds, 0.1))

    active_orders_final = await execution.get_open_orders_count() if args.mode == "live" else sum(
        len(v) for v in order_manager.active_orders.values()
    )
    if args.mode == "live":
        active_orders_label = "unknown" if active_orders_final is None else str(active_orders_final)
        logging.info(
            "Run completed. Mode=%s Active orders=%s Estimated PnL=%.4f Equity=%.2f",
            args.mode,
            active_orders_label,
            realized_pnl,
            equity,
        )
    else:
        logging.info(
            "Run completed. Mode=%s Active orders=%d Realized PnL=%.4f Equity=%.2f",
            args.mode,
            active_orders_final,
            realized_pnl,
            equity,
        )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        asyncio.run(run_bot(args))
    except KeyboardInterrupt:
        logging.info("Shutdown requested by user. Exiting loop.")


if __name__ == "__main__":
    main()
