import json
import time

from py_clob_client.client import ClobClient

CLOB_HOST = "https://clob.polymarket.com"
OUT_FILE = "ranking_snapshot_live.json"
MAX_MARKETS = 1500
DEFAULT_SPREAD_BPS = 30.0
MIN_PRICE = 0.001
MAX_PRICE = 0.999


def as_list(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "markets", "items"):
            val = payload.get(key)
            if isinstance(val, list):
                return val
    return []


def to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def to_int_ts(value):
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            v = int(value)
            if v > 10_000_000_000:
                v //= 1000
            return v
        s = str(value).strip()
        if not s:
            return None
        if s.isdigit():
            return to_int_ts(int(s))
        # basic ISO support without extra deps
        from datetime import datetime, timezone

        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def choose_yes_token(tokens):
    if not isinstance(tokens, list):
        return None
    for token in tokens:
        if isinstance(token, dict) and str(token.get("outcome", "")).lower() == "yes":
            return token
    for token in tokens:
        if isinstance(token, dict):
            return token
    return None


def choose_no_token(tokens):
    if not isinstance(tokens, list):
        return None
    for token in tokens:
        if isinstance(token, dict) and str(token.get("outcome", "")).lower() == "no":
            return token
    return None


def synthetic_bid_ask(mid, spread_bps=DEFAULT_SPREAD_BPS):
    half = max(mid * (spread_bps / 10000.0) / 2.0, 0.0001)
    bid = max(mid - half, MIN_PRICE)
    ask = min(mid + half, MAX_PRICE)
    if ask <= bid:
        ask = min(bid + 0.0002, MAX_PRICE)
    if ask <= bid:
        return None, None
    return bid, ask


def extract_top_of_book(payload):
    bids = []
    asks = []
    if isinstance(payload, dict):
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
    else:
        bids = getattr(payload, "bids", []) or []
        asks = getattr(payload, "asks", []) or []

    def px(item):
        if isinstance(item, dict):
            return to_float(item.get("price"), 0.0)
        if isinstance(item, (list, tuple)) and item:
            return to_float(item[0], 0.0)
        return to_float(getattr(item, "price", 0.0), 0.0)

    bid_prices = [px(x) for x in bids]
    ask_prices = [px(x) for x in asks]
    bid_prices = [x for x in bid_prices if x > 0]
    ask_prices = [x for x in ask_prices if x > 0]
    if not bid_prices or not ask_prices:
        return None, None
    bid = max(bid_prices)
    ask = min(ask_prices)
    if bid <= 0 or ask <= bid:
        return None, None
    return bid, ask


def main():
    clob = ClobClient(CLOB_HOST)
    payload = clob.get_sampling_simplified_markets()
    markets = as_list(payload)
    print(f"source=get_sampling_simplified_markets items={len(markets)}")

    out = []
    stats = {
        "markets_seen": 0,
        "skipped_no_market_id": 0,
        "skipped_not_active": 0,
        "skipped_no_yes_token": 0,
        "skipped_no_mid": 0,
        "book_top_ok": 0,
        "book_top_fallback": 0,
    }

    for m in markets[:MAX_MARKETS]:
        stats["markets_seen"] += 1
        market_id = m.get("condition_id") or m.get("id") or m.get("market_id")
        if not market_id:
            stats["skipped_no_market_id"] += 1
            continue

        if not bool(m.get("active", True)) or bool(m.get("closed", False)) or bool(m.get("archived", False)):
            stats["skipped_not_active"] += 1
            continue

        yes_token = choose_yes_token(m.get("tokens"))
        if not yes_token:
            stats["skipped_no_yes_token"] += 1
            continue
        no_token = choose_no_token(m.get("tokens"))

        yes_token_id = yes_token.get("token_id") or yes_token.get("asset_id") or yes_token.get("id")
        no_token_id = None
        if no_token:
            no_token_id = no_token.get("token_id") or no_token.get("asset_id") or no_token.get("id")

        if not yes_token_id:
            stats["skipped_no_yes_token"] += 1
            continue

        mid = to_float(yes_token.get("price"), default=0.0)
        if mid <= 0:
            stats["skipped_no_mid"] += 1
            continue

        bid = None
        ask = None
        try:
            book = clob.get_order_book(str(yes_token_id))
            bid, ask = extract_top_of_book(book)
            if bid is not None and ask is not None:
                stats["book_top_ok"] += 1
        except Exception:
            pass
        if bid is None or ask is None:
            bid, ask = synthetic_bid_ask(mid)
            stats["book_top_fallback"] += 1
            if bid is None:
                stats["skipped_no_mid"] += 1
                continue

        rewards_obj = m.get("rewards") if isinstance(m.get("rewards"), dict) else {}
        rewards_total = 0.0
        for rate in rewards_obj.get("rates", []) if isinstance(rewards_obj.get("rates"), list) else []:
            rewards_total += to_float(rate.get("rewards_daily_rate"), 0.0)
        rewards_min_size = to_float(rewards_obj.get("min_size"), 0.0)
        rewards_max_spread = to_float(rewards_obj.get("max_spread"), 0.0)
        end_ts = (
            to_int_ts(m.get("end_ts"))
            or to_int_ts(m.get("end_time"))
            or to_int_ts(m.get("endDate"))
            or to_int_ts(m.get("end_date_iso"))
        )
        if end_ts is None:
            sec = m.get("seconds_to_end") or m.get("seconds_to_expiry")
            try:
                sec_v = int(sec)
                if sec_v > 0:
                    end_ts = int(time.time()) + sec_v
            except Exception:
                pass

        volume_24h = to_float(
            m.get("volume_24h")
            or m.get("volume_24hr")
            or m.get("volume_num_min")
            or m.get("volume"),
            default=0.0,
        )
        out.append(
            {
                "market_id": str(market_id),
                "slug": str(m.get("question") or m.get("slug") or market_id),
                "asset_id": str(yes_token_id),
                "token_id": str(yes_token_id),
                "no_token_id": str(no_token_id) if no_token_id else None,
                "rewards": rewards_total,
                "competition": 1.0,
                "liquidity": to_float(m.get("liquidity"), 0.0),
                "best_bid": bid,
                "best_ask": ask,
                "tick_size": 0.001,
                "rewards_min_size": rewards_min_size,
                "rewards_max_spread": rewards_max_spread,
                "accepting_orders": bool(m.get("accepting_orders", True)),
                "end_ts": end_ts,
                "volume_24h": volume_24h,
            }
        )

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"saved {len(out)} markets -> {OUT_FILE}")
    print("stats:", stats)


if __name__ == "__main__":
    main()
