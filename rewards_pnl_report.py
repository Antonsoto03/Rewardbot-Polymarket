from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Daily net report: trading PnL estimate + manual rewards input"
    )
    parser.add_argument("--state-db", default="bot_state.sqlite", help="Path to bot sqlite state DB")
    parser.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours")
    parser.add_argument("--rewards-usd", type=float, default=0.0, help="Rewards collected in USD for the same window")
    parser.add_argument("--mode", choices=["live", "paper", "dry", "all"], default="live", help="Filter by mode")
    return parser


def fetch_rows(db_path: Path, since_ts: float, mode: str) -> list[tuple]:
    query = """
        SELECT ts, cycle, mode, fills, realized_pnl, equity, drawdown_pct
        FROM cycle_snapshots
        WHERE ts >= ?
    """
    params: list[object] = [since_ts]
    if mode != "all":
        query += " AND mode = ?"
        params.append(mode)
    query += " ORDER BY ts ASC, id ASC"

    with sqlite3.connect(db_path) as conn:
        return conn.execute(query, params).fetchall()


def main() -> None:
    args = build_parser().parse_args()
    db_path = Path(args.state_db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=max(args.hours, 0.01))
    rows = fetch_rows(db_path, since.timestamp(), args.mode)
    if not rows:
        print("No rows in selected window.")
        return

    first = rows[0]
    last = rows[-1]

    first_ts, _, first_mode, _, first_pnl, first_equity, _ = first
    last_ts, last_cycle, last_mode, _, last_pnl, last_equity, last_dd = last

    trading_delta = float(last_pnl) - float(first_pnl)
    equity_delta = float(last_equity) - float(first_equity)
    rewards = float(args.rewards_usd)
    net = trading_delta + rewards
    fills_total = int(sum(int(r[3]) for r in rows))

    print("=== Rewards + Trading Net Report ===")
    print(f"window_hours: {args.hours:.2f}")
    print(f"mode_filter: {args.mode}")
    print(f"rows: {len(rows)}")
    print(f"from_utc: {datetime.fromtimestamp(first_ts, tz=timezone.utc).isoformat()}")
    print(f"to_utc:   {datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()}")
    print(f"last_cycle: {last_cycle} (mode={last_mode})")
    print(f"fills_total_counter: {fills_total}")
    print(f"trading_pnl_est_delta_usd: {trading_delta:.4f}")
    print(f"equity_delta_usd: {equity_delta:.4f}")
    print(f"rewards_input_usd: {rewards:.4f}")
    print(f"net_usd (trading_est + rewards): {net:.4f}")
    print(f"last_drawdown_pct: {float(last_dd):.4f}")
    print("")
    print("Note: trading_pnl_est is an estimate from bot marks, not official realized exchange PnL.")


if __name__ == "__main__":
    main()

