# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Polymarket liquidity rewards bot — a market-making system that provides two-sided quotes on Polymarket prediction markets to capture rewards while managing inventory and risk.

## Running the Bot

The virtual environment is at `.venv`. Use `.venv\Scripts\python.exe` (or `python` if the venv is activated).

**Dry mode (simulated, no real orders):**
```bash
python liquidity_rewards_bot.py --ranking-file ranking_snapshot.json --top-n 5 --min-liquidity 10000 --min-spread-bps 15 --distance-to-mid-bps 8 --quote-size 25 --log-level INFO
```

**Paper mode (realistic simulation):**
```bash
.venv/Scripts/python.exe liquidity_rewards_bot.py --mode paper --ranking-file ranking_snapshot_live.json --cycles 180 --refresh-seconds 0.20 --top-n 8 --active-markets 2 --quote-size 200 --max-position-per-market 700 --global-exposure-limit 1800
```

**Stress test:**
```bash
.venv/Scripts/python.exe liquidity_rewards_bot.py --mode paper --ranking-file ranking_snapshot_live.json --cycles 300 --refresh-seconds 0.10 --top-n 15 --active-markets 4 --quote-size 180 --max-drawdown-pct 18 --daily-loss-limit-usd 120 --log-level INFO
```

**Reproducible test run (for verifying fixes):**
```bash
.venv/Scripts/python.exe liquidity_rewards_bot.py --mode paper --ranking-file ranking_snapshot_live.json --cycles 180 --refresh-seconds 0.20 --top-n 8 --active-markets 2 --rotation-step 1 --market-hold-cycles 8 --disable-state-persistence --random-seed 42
```

**Run test suite:**
```bash
.venv/Scripts/python.exe test_fixes.py
```

**PnL and performance reports:**
```bash
python rewards_pnl_report.py --state-db bot_state.sqlite --hours 24.0 --rewards-usd 100.0 --mode live
python live_performance_report.py --state-db bot_state.sqlite --hours 24.0 --days-rewards 2
```

**Build a fresh live market snapshot:**
```bash
python build_live_snapshot.py
```

## Architecture

### Module Responsibilities

| File | Role |
|---|---|
| `liquidity_rewards_bot.py` | Main async loop, quote generation, fill simulation, P&L tracking (~1,943 lines) |
| `execution_adapter.py` | Mode-agnostic order execution; wraps live CLOB client and dry-run stub |
| `rewards_market_selector.py` | Market filtering (liquidity, spread, rewards, time-to-expiry) and ranking |
| `state_store.py` | SQLite persistence for cycle snapshots, positions, open orders |
| `inventory_manager.py` | Per-market and global position tracking and limit enforcement |
| `risk_manager.py` | Kill-switches: drawdown, daily loss, fill rate, error counts |
| `market_maker.py` | Quote sizing helpers |
| `order_manager.py` | Dry-run order lifecycle tracking |
| `build_live_snapshot.py` | Fetches live Polymarket data and writes `ranking_snapshot_live.json` |
| `live_performance_report.py` | Reads SQLite and prints a human-readable performance summary |
| `rewards_pnl_report.py` | Rewards-adjusted P&L analysis |
| `test_fixes.py` | Unit tests using mocked execution |

### Operational Modes

- **`dry`** — Fully local, probabilistic fill simulation, no network calls.
- **`paper`** — Same fill simulation but uses live book data (if available); closer to real conditions.
- **`live`** — Real orders via `py-clob-client`. Requires `--confirm-live YES` and `--enable-live-execution`.

### Core Data Flow (one cycle)

1. Load `ranking_snapshot_live.json` → `MarketSnapshot` dataclasses
2. `RewardsMarketSelector` filters and ranks markets (reward/competition ratio)
3. Active-market rotation selects which subset to quote this cycle
4. For each selected market:
   - Refresh top-of-book (live mode only)
   - `generate_quotes()` produces bid/ask at configured `--distance-to-mid-bps`
   - `InventoryManager` checks per-market and global exposure
   - `RiskManager` checks drawdown, daily loss, fill efficiency
   - `_replace_quotes()` cancels stale orders and places new ones
5. `_simulate_cycle_fills()` (dry/paper) or real fills (live) update positions
6. `_compute_live_trade_pnl_estimate()` updates P&L and equity
7. `StateStore` saves snapshot to `bot_state.sqlite`
8. Sleep `--refresh-seconds`

### Quote Generation Strategies (`--quote-style`)

- **`mid`** — Fixed bps distance from mid-price (default).
- **`top_of_book`** — Quote just inside the best bid/ask.
- **`maker_cross_controlled`** — Aggressive maker-cross with configurable aggressiveness.

### Adaptive Mechanisms

- **No-fill tightening** (`--no-fill-tighten-cycles`, `--tighten-step-bps`) — narrows spread when fills dry up.
- **Mid-drift pause** (`--mid-drift-pause-bps`) — halts quoting after a large mid-price jump.
- **No-fill rotation** (`--no-fill-rotate-cycles`) — forces market rotation after N quiet cycles.
- **Imbalance unwinding** (`--imbalance-threshold`) — skews or reduces quotes to rebalance yes/no inventory.

### State & Credentials

- **`bot_state.sqlite`** — three tables: `cycle_snapshots`, `market_exposure`, `open_orders`.
- **`ranking_snapshot_live.json`** — input market data (~732 KB); regenerate with `build_live_snapshot.py`.
- **`clob_creds.json`** — CLOB L2 credentials (api_key, api_secret, api_passphrase).
- Live mode also reads env vars: `POLY_BASE_URL`, `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_API_PASSPHRASE`, `PRIVATE_KEY`, `FUNDER_ADDRESS`, `CHAIN_ID`, `SIGNATURE_TYPE`.

## Key CLI Parameters

The bot has 100+ flags. The most impactful ones to understand:

| Flag | Effect |
|---|---|
| `--top-n` | Markets to select from snapshot |
| `--active-markets` | Markets quoted simultaneously per cycle |
| `--rotation-step` | Markets swapped each cycle |
| `--market-hold-cycles` | Minimum cycles before rotating a market out |
| `--distance-to-mid-bps` | Quote distance (tighter = more fills, more risk) |
| `--quote-size` | Size per side in USD |
| `--max-position-per-market` | Net position cap per market |
| `--global-exposure-limit` | Sum of all market exposures cap |
| `--max-drawdown-pct` | Kill-switch threshold |
| `--daily-loss-limit-usd` | Daily loss kill-switch |
| `--disable-state-persistence` | Skip SQLite writes (useful for testing) |
| `--random-seed` | Reproducible fill simulation |

## Testing

`test_fixes.py` uses `unittest.mock` to patch `ExecutionAdapter` and validates three specific behaviors:
1. Fill attribution correctly maps fills to `token_id`s
2. `_replace_quotes()` cleans up correctly on partial failures
3. Exposure cap uses `abs()` on net position

There is no pytest config; run directly with the venv Python interpreter.
