from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib import parse, request

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams, RequestArgs, AssetType
from py_clob_client.headers.headers import create_level_2_headers


@dataclass
class RewardDay:
    day: str
    entries: int
    earnings_raw: float
    earnings_usd_est: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Live performance report (balance, rewards, fills, errors)")
    parser.add_argument("--state-db", default="bot_state.sqlite", help="Path to state sqlite")
    parser.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours")
    parser.add_argument("--logs-glob", default="run_live*.log", help="Glob for live logs")
    parser.add_argument("--days-rewards", type=int, default=2, help="How many recent UTC days to query rewards for")
    return parser


def _build_client() -> ClobClient:
    base_url = os.environ["POLY_BASE_URL"]
    chain_id = int(os.environ["CHAIN_ID"])
    signature_type = int(os.environ["SIGNATURE_TYPE"])
    private_key = os.environ["PRIVATE_KEY"]
    funder = os.environ["FUNDER_ADDRESS"]
    api_key = os.environ["POLY_API_KEY"]
    api_secret = os.environ["POLY_API_SECRET"]
    api_passphrase = os.environ["POLY_API_PASSPHRASE"]

    client = ClobClient(
        host=base_url,
        chain_id=chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder,
    )
    client.set_api_creds(
        ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
    )
    return client


def _get_collateral_balance_usd(client: ClobClient, signature_type: int) -> float:
    payload = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=signature_type)
    )
    micro = float((payload or {}).get("balance") or 0.0)
    return micro / 1_000_000.0


def _auth_get_json(client: ClobClient, path: str, query: dict[str, str]) -> object:
    request_path = path
    req_args = RequestArgs(method="GET", request_path=request_path)
    headers = create_level_2_headers(client.signer, client.creds, req_args)
    qs = parse.urlencode(query)
    url = f"{client.host}{path}"
    if qs:
        url = f"{url}?{qs}"
    req = request.Request(url=url, method="GET", headers=headers)
    with request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def _get_reward_total_for_day(client: ClobClient, signature_type: int, maker_address: str, day: date) -> RewardDay:
    payload = _auth_get_json(
        client,
        "/rewards/user/total",
        {
            "date": day.isoformat(),
            "signature_type": str(signature_type),
            "maker_address": maker_address,
        },
    )
    items = payload if isinstance(payload, list) else []
    raw_sum = 0.0
    usd_sum = 0.0
    for it in items:
        if not isinstance(it, dict):
            continue
        earnings = float(it.get("earnings") or 0.0)
        rate = float(it.get("asset_rate") or 0.0)
        raw_sum += earnings
        usd_sum += earnings * rate
    return RewardDay(day=day.isoformat(), entries=len(items), earnings_raw=raw_sum, earnings_usd_est=usd_sum)


def _read_cycle_stats(db_path: Path, since_ts: float) -> dict[str, float]:
    if not db_path.exists():
        return {
            "rows": 0,
            "fills_total_counter": 0,
            "trading_pnl_est_delta": 0.0,
            "equity_delta": 0.0,
            "last_equity": 0.0,
            "last_drawdown_pct": 0.0,
        }
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT ts, fills, realized_pnl, equity, drawdown_pct
            FROM cycle_snapshots
            WHERE mode = 'live' AND ts >= ?
            ORDER BY ts ASC, id ASC
            """,
            (since_ts,),
        ).fetchall()
    if not rows:
        return {
            "rows": 0,
            "fills_total_counter": 0,
            "trading_pnl_est_delta": 0.0,
            "equity_delta": 0.0,
            "last_equity": 0.0,
            "last_drawdown_pct": 0.0,
        }
    fills_total = int(sum(int(r[1]) for r in rows))
    first = rows[0]
    last = rows[-1]
    return {
        "rows": len(rows),
        "fills_total_counter": fills_total,
        "trading_pnl_est_delta": float(last[2]) - float(first[2]),
        "equity_delta": float(last[3]) - float(first[3]),
        "last_equity": float(last[3]),
        "last_drawdown_pct": float(last[4]),
    }


def _decode_log(path: Path) -> str:
    data = path.read_bytes()
    for enc in ("utf-16", "utf-16-le", "utf-8", "cp1252"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return ""


def _read_log_stats(log_glob: str, since_dt: datetime) -> dict[str, int]:
    posted = 0
    replace_errors = 0
    recoverable = 0
    kill = 0
    no_candidate = 0
    market_lines = 0
    for p in Path(".").glob(log_glob):
        if not p.is_file():
            continue
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        if mtime < since_dt:
            continue
        txt = _decode_log(p)
        posted += len(re.findall(r"\[LIVE\] posted order", txt))
        replace_errors += len(re.findall(r"Order replace error", txt))
        recoverable += len(re.findall(r"Recoverable live order error", txt))
        kill += len(re.findall(r"Risk kill-switch", txt))
        no_candidate += len(re.findall(r"No live candidate qualified this cycle", txt))
        market_lines += len(re.findall(r"Market 0x", txt))
    return {
        "posted_orders": posted,
        "order_replace_errors": replace_errors,
        "recoverable_errors": recoverable,
        "killswitch_events": kill,
        "no_candidate_cycles": no_candidate,
        "market_lines": market_lines,
    }


def main() -> None:
    args = build_parser().parse_args()
    hours = max(args.hours, 0.01)
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)

    client = _build_client()
    signature_type = int(os.environ["SIGNATURE_TYPE"])
    maker = os.environ["FUNDER_ADDRESS"]

    balance_usd = _get_collateral_balance_usd(client, signature_type)
    open_orders = client.get_orders()
    open_orders_count = len(open_orders if isinstance(open_orders, list) else [])

    db_stats = _read_cycle_stats(Path(args.state_db), since.timestamp())
    log_stats = _read_log_stats(args.logs_glob, since)

    rewards_days: list[RewardDay] = []
    for offset in range(max(args.days_rewards, 1)):
        d = (now - timedelta(days=offset)).date()
        try:
            rewards_days.append(_get_reward_total_for_day(client, signature_type, maker, d))
        except Exception:
            rewards_days.append(RewardDay(day=d.isoformat(), entries=0, earnings_raw=0.0, earnings_usd_est=0.0))

    rewards_window_est = sum(r.earnings_usd_est for r in rewards_days)
    net_est = db_stats["trading_pnl_est_delta"] + rewards_window_est

    print("=== Live Account/Execution Report ===")
    print(f"window_hours: {hours:.2f}")
    print(f"from_utc: {since.isoformat()}")
    print(f"to_utc:   {now.isoformat()}")
    print("")
    print("--- Account ---")
    print(f"collateral_balance_usd: {balance_usd:.4f}")
    print(f"open_orders_count: {open_orders_count}")
    print("")
    print("--- Execution (logs) ---")
    for k, v in log_stats.items():
        print(f"{k}: {v}")
    print("")
    print("--- Trading (state DB, live) ---")
    for k in ("rows", "fills_total_counter", "trading_pnl_est_delta", "equity_delta", "last_equity", "last_drawdown_pct"):
        v = db_stats[k]
        if isinstance(v, float):
            print(f"{k}: {v:.4f}")
        else:
            print(f"{k}: {v}")
    print("")
    print("--- Rewards (API) ---")
    for r in rewards_days:
        print(
            f"{r.day}: entries={r.entries} earnings_raw={r.earnings_raw:.6f} earnings_usd_est={r.earnings_usd_est:.6f}"
        )
    print(f"rewards_window_usd_est: {rewards_window_est:.6f}")
    print("")
    print("--- Net ---")
    print(f"net_est_usd (trading_pnl_est_delta + rewards_window_usd_est): {net_est:.6f}")
    print("")
    print("Note: trading_pnl_est is bot estimate; rewards use /rewards/user/total API and asset_rate conversion.")


if __name__ == "__main__":
    main()

