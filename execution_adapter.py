from __future__ import annotations

import asyncio
import logging
import os
import time
from urllib import parse, request
from dataclasses import dataclass
from datetime import datetime, timezone

from market_maker import Quote
from order_manager import DryRunOrder, OrderManager

logger = logging.getLogger(__name__)


@dataclass
class LiveHttpConfig:
    base_url: str
    auth_mode: str
    relayer_api_key: str = ""
    relayer_api_key_address: str = ""
    poly_api_key: str = ""
    poly_api_secret: str = ""
    poly_api_passphrase: str = ""
    chain_id: int = 137
    signature_type: int = 1
    private_key: str = ""
    funder_address: str = ""


class LiveHttpExecutionClient:
    def __init__(self, config: LiveHttpConfig) -> None:
        self.config = config
        self._clob_client = None

        if self.config.auth_mode == "poly_l2":
            try:
                from py_clob_client.client import ClobClient
                from py_clob_client.clob_types import ApiCreds
            except ImportError as exc:
                raise RuntimeError("Missing dependency py-clob-client. Install with: pip install py-clob-client") from exc

            client = ClobClient(
                host=self.config.base_url,
                chain_id=self.config.chain_id,
                key=self.config.private_key,
                signature_type=self.config.signature_type,
                funder=self.config.funder_address,
            )
            client.set_api_creds(
                ApiCreds(
                    api_key=self.config.poly_api_key,
                    api_secret=self.config.poly_api_secret,
                    api_passphrase=self.config.poly_api_passphrase,
                )
            )
            self._clob_client = client

    @classmethod
    def from_env(cls) -> "LiveHttpExecutionClient":
        base_url = os.getenv("POLY_BASE_URL", "").strip()
        auth_mode = os.getenv("POLY_AUTH_MODE", "relayer").strip().lower()

        if not base_url:
            raise ValueError("Missing POLY_BASE_URL")

        relayer_api_key = os.getenv("RELAYER_API_KEY", "").strip()
        relayer_api_key_address = os.getenv("RELAYER_API_KEY_ADDRESS", "").strip()
        poly_api_key = os.getenv("POLY_API_KEY", "").strip()
        poly_api_secret = os.getenv("POLY_API_SECRET", "").strip()
        poly_api_passphrase = os.getenv("POLY_API_PASSPHRASE", "").strip()
        private_key = os.getenv("PRIVATE_KEY", "").strip()
        funder_address = os.getenv("FUNDER_ADDRESS", "").strip()
        chain_id = int(os.getenv("CHAIN_ID", "137"))
        signature_type = int(os.getenv("SIGNATURE_TYPE", "1"))

        if auth_mode == "relayer":
            if not relayer_api_key:
                raise ValueError("Missing RELAYER_API_KEY")
            if not relayer_api_key_address:
                raise ValueError("Missing RELAYER_API_KEY_ADDRESS")
        elif auth_mode == "poly_l2":
            if not poly_api_key:
                raise ValueError("Missing POLY_API_KEY")
            if not poly_api_secret:
                raise ValueError("Missing POLY_API_SECRET")
            if not poly_api_passphrase:
                raise ValueError("Missing POLY_API_PASSPHRASE")
            if not private_key:
                raise ValueError("Missing PRIVATE_KEY")
            if not funder_address:
                raise ValueError("Missing FUNDER_ADDRESS")
        else:
            raise ValueError("Unsupported POLY_AUTH_MODE. Use 'relayer' or 'poly_l2'")

        return cls(
            LiveHttpConfig(
                base_url=base_url.rstrip("/"),
                auth_mode=auth_mode,
                relayer_api_key=relayer_api_key,
                relayer_api_key_address=relayer_api_key_address,
                poly_api_key=poly_api_key,
                poly_api_secret=poly_api_secret,
                poly_api_passphrase=poly_api_passphrase,
                chain_id=chain_id,
                signature_type=signature_type,
                private_key=private_key,
                funder_address=funder_address,
            )
        )

    async def place_limit_order(self, quote: Quote, client_order_id: str) -> None:
        if self.config.auth_mode != "poly_l2":
            raise RuntimeError("Live order placement requires POLY_AUTH_MODE=poly_l2")
        if self._clob_client is None:
            raise RuntimeError("CLOB client not initialized")
        if not quote.token_id:
            raise RuntimeError("Missing token_id for live order placement")

        from py_clob_client.clob_types import OrderArgs

        side = "BUY" if quote.side == "buy" else "SELL"
        order_args = OrderArgs(
            token_id=quote.token_id,
            price=float(quote.price),
            size=float(quote.size),
            side=side,
        )

        try:
            await asyncio.to_thread(self._clob_client.create_and_post_order, order_args)
        except Exception as exc:
            msg = str(exc)
            if "invalid signature" in msg.lower():
                raise RuntimeError(
                    "Live order rejected: invalid signature. Re-derive CLOB creds with the same "
                    "PRIVATE_KEY/FUNDER_ADDRESS/SIGNATURE_TYPE active in this session."
                ) from exc
            raise
        logger.info("[LIVE] posted order %s %s cid=%s", quote.market_id, quote.side, client_order_id)

    async def cancel_market_orders(self, market_id: str, asset_id: str | None = None) -> None:
        if self.config.auth_mode != "poly_l2":
            raise RuntimeError("Live market cancel requires POLY_AUTH_MODE=poly_l2")
        if self._clob_client is None:
            raise RuntimeError("CLOB client not initialized")

        await asyncio.to_thread(self._clob_client.cancel_market_orders, market_id, asset_id or "")

    async def cancel_all_orders(self) -> None:
        if self.config.auth_mode != "poly_l2":
            raise RuntimeError("Live cancel-all requires POLY_AUTH_MODE=poly_l2")
        if self._clob_client is None:
            raise RuntimeError("CLOB client not initialized")

        await asyncio.to_thread(self._clob_client.cancel_all)

    async def health_check(self) -> bool:
        if self.config.auth_mode != "poly_l2":
            return False
        if self._clob_client is None:
            return False
        try:
            await asyncio.to_thread(self._clob_client.get_ok)
            return True
        except Exception as exc:
            logger.warning("Live health-check failed: %s", exc)
            return False

    async def validate_trading_auth(self) -> bool:
        if self.config.auth_mode != "poly_l2":
            return False
        if self._clob_client is None:
            return False
        try:
            # Requires valid L2 credentials; fails early if signature/auth is invalid.
            await asyncio.to_thread(self._clob_client.get_api_keys)
            return True
        except Exception as exc:
            logger.warning("Live auth preflight failed: %s", exc)
            return False

    async def get_recent_trades(self, after_ts: int | None = None) -> list[dict]:
        if self.config.auth_mode != "poly_l2":
            return []
        if self._clob_client is None:
            return []

        from py_clob_client.clob_types import TradeParams

        def _fetch() -> list[dict]:
            params = TradeParams(
                maker_address=self.config.funder_address,
                after=after_ts,
            )
            trades = self._clob_client.get_trades(params=params)
            return trades if isinstance(trades, list) else []

        try:
            return await asyncio.to_thread(_fetch)
        except Exception as exc:
            logger.warning("Live get_recent_trades failed: %s", exc)
            return []

    async def get_market_recent_trade_count(self, market_id: str, after_ts: int | None = None) -> int:
        if self.config.auth_mode != "poly_l2":
            return 0
        if not market_id:
            return 0

        def _fetch_count() -> int:
            params = {"market": market_id, "next_cursor": "MA=="}
            if after_ts is not None:
                params["after"] = int(after_ts)
            url = f"{self.config.base_url}/data/trades?{parse.urlencode(params)}"
            req = request.Request(url=url, method="GET")
            with request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8")
            import json

            payload = json.loads(raw)
            if isinstance(payload, dict):
                items = payload.get("data") or payload.get("trades") or payload.get("items") or []
                return len(items) if isinstance(items, list) else 0
            if isinstance(payload, list):
                return len(payload)
            return 0

        try:
            return await asyncio.to_thread(_fetch_count)
        except Exception as exc:
            logger.debug("Live get_market_recent_trade_count failed for %s: %s", market_id, exc)
            return 0

    async def get_collateral_balance_usd(self) -> float | None:
        if self.config.auth_mode != "poly_l2":
            return None
        if self._clob_client is None:
            return None

        def _fetch_balance() -> float | None:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=self.config.signature_type,
            )
            payload = self._clob_client.get_balance_allowance(params)
            if not isinstance(payload, dict):
                return None
            raw = payload.get("balance")
            if raw is None:
                return None
            micro = float(raw)
            return micro / 1_000_000.0

        try:
            return await asyncio.to_thread(_fetch_balance)
        except Exception as exc:
            logger.warning("Live get_collateral_balance_usd failed: %s", exc)
            return None

    async def get_open_orders_count(self, market_id: str | None = None) -> int | None:
        if self.config.auth_mode != "poly_l2":
            return 0
        if self._clob_client is None:
            return None

        from py_clob_client.clob_types import OpenOrderParams

        def _count_orders() -> int:
            total = 0
            next_cursor = "MA=="
            pages = 0
            while True:
                pages += 1
                params = OpenOrderParams(market=market_id) if market_id else OpenOrderParams()
                payload = self._clob_client.get_orders(params=params, next_cursor=next_cursor)
                if isinstance(payload, dict):
                    items = payload.get("data") or payload.get("orders") or payload.get("items") or []
                    if isinstance(items, list):
                        total += len(items)
                    next_cursor_local = payload.get("next_cursor") or payload.get("nextCursor")
                elif isinstance(payload, list):
                    total += len(payload)
                    next_cursor_local = None
                else:
                    next_cursor_local = None

                if not next_cursor_local or next_cursor_local == "LTE=" or pages >= 20:
                    break
                next_cursor = next_cursor_local
            return total

        try:
            return await asyncio.to_thread(_count_orders)
        except Exception as exc:
            logger.warning("Live get_open_orders_count failed: %s", exc)
            return None

    async def get_top_of_book(self, token_id: str) -> tuple[float, float] | None:
        snapshot = await self.get_book_snapshot(token_id)
        if not snapshot:
            return None
        bid = float(snapshot.get("bid") or 0.0)
        ask = float(snapshot.get("ask") or 0.0)
        if bid <= 0 or ask <= bid:
            return None
        return bid, ask

    async def get_book_snapshot(self, token_id: str) -> dict | None:
        if self.config.auth_mode != "poly_l2":
            return None
        if self._clob_client is None:
            return None

        def _extract() -> dict | None:
            payload = self._clob_client.get_order_book(token_id)
            bids = []
            asks = []

            if isinstance(payload, dict):
                bids = payload.get("bids") or []
                asks = payload.get("asks") or []
            else:
                bids = getattr(payload, "bids", []) or []
                asks = getattr(payload, "asks", []) or []

            def _px(item):
                if isinstance(item, dict):
                    return float(item.get("price") or 0)
                if isinstance(item, (list, tuple)) and item:
                    return float(item[0])
                price_attr = getattr(item, "price", None)
                if price_attr is not None:
                    return float(price_attr)
                return 0.0

            def _sz(item):
                if isinstance(item, dict):
                    return float(item.get("size") or item.get("amount") or 0)
                if isinstance(item, (list, tuple)) and len(item) > 1:
                    return float(item[1] or 0)
                size_attr = getattr(item, "size", None)
                if size_attr is not None:
                    return float(size_attr)
                return 0.0

            bid_pairs = [(_px(x), _sz(x)) for x in bids]
            ask_pairs = [(_px(x), _sz(x)) for x in asks]
            bid_pairs = [x for x in bid_pairs if x[0] > 0]
            ask_pairs = [x for x in ask_pairs if x[0] > 0]
            bid = max((p for p, _ in bid_pairs), default=0.0)
            ask = min((p for p, _ in ask_pairs), default=0.0)
            if bid <= 0 or ask <= bid:
                return None

            bid_size = 0.0
            ask_size = 0.0
            for p, s in bid_pairs:
                if abs(p - bid) < 1e-12:
                    bid_size = max(bid_size, s)
            for p, s in ask_pairs:
                if abs(p - ask) < 1e-12:
                    ask_size = max(ask_size, s)

            return {
                "bid": bid,
                "ask": ask,
                "bid_size": bid_size,
                "ask_size": ask_size,
                "bid_levels": len(bid_pairs),
                "ask_levels": len(ask_pairs),
            }

        try:
            return await asyncio.to_thread(_extract)
        except Exception as exc:
            logger.warning("Live get_book_snapshot failed for %s: %s", token_id, exc)
            return None


class LiveExecutionClient:
    async def place_limit_order(self, quote: Quote, client_order_id: str) -> None:  # pragma: no cover
        raise NotImplementedError

    async def cancel_market_orders(self, market_id: str, asset_id: str | None = None) -> None:  # pragma: no cover
        raise NotImplementedError

    async def cancel_all_orders(self) -> None:  # pragma: no cover
        raise NotImplementedError

    async def health_check(self) -> bool:  # pragma: no cover
        raise NotImplementedError

    async def validate_trading_auth(self) -> bool:  # pragma: no cover
        raise NotImplementedError

    async def get_open_orders_count(self, market_id: str | None = None) -> int | None:  # pragma: no cover
        raise NotImplementedError

    async def get_top_of_book(self, token_id: str) -> tuple[float, float] | None:  # pragma: no cover
        raise NotImplementedError

    async def get_book_snapshot(self, token_id: str) -> dict | None:  # pragma: no cover
        raise NotImplementedError

    async def get_recent_trades(self, after_ts: int | None = None) -> list[dict]:  # pragma: no cover
        raise NotImplementedError

    async def get_market_recent_trade_count(self, market_id: str, after_ts: int | None = None) -> int:  # pragma: no cover
        raise NotImplementedError

    async def get_collateral_balance_usd(self) -> float | None:  # pragma: no cover
        raise NotImplementedError


@dataclass
class ExecutionConfig:
    mode: str
    enable_live_execution: bool


class ExecutionAdapter:
    def __init__(
        self,
        config: ExecutionConfig,
        order_manager: OrderManager,
        live_client: LiveExecutionClient | None = None,
    ) -> None:
        self.config = config
        self.order_manager = order_manager
        self.live_client = live_client

    async def place_quote(self, quote: Quote, client_order_id: str | None = None) -> DryRunOrder | None:
        if self.config.mode in {"dry", "paper"}:
            return await self.order_manager.place_dry_run(quote)

        if self.config.mode == "live":
            if not self.config.enable_live_execution:
                raise RuntimeError("Live mode blocked: use --enable-live-execution to allow live routing")
            if self.live_client is None:
                raise RuntimeError("Live mode requires a configured execution client")
            if not client_order_id:
                raise RuntimeError("Live mode requires client_order_id for idempotency")
            await self.live_client.place_limit_order(quote, client_order_id)
            return None

        raise ValueError(f"Unsupported mode: {self.config.mode}")

    async def cancel_market(self, market_id: str, asset_id: str | None = None) -> list[DryRunOrder]:
        if self.config.mode in {"dry", "paper"}:
            return await self.order_manager.cancel_market(market_id)

        if self.config.mode == "live":
            if not self.config.enable_live_execution:
                raise RuntimeError("Live mode blocked: use --enable-live-execution to allow live routing")
            if self.live_client is None:
                raise RuntimeError("Live mode requires a configured execution client")
            await self.live_client.cancel_market_orders(market_id, asset_id=asset_id)
            return []

        raise ValueError(f"Unsupported mode: {self.config.mode}")

    async def cancel_all(self) -> None:
        if self.config.mode == "live":
            if not self.config.enable_live_execution:
                raise RuntimeError("Live mode blocked: use --enable-live-execution to allow live routing")
            if self.live_client is None:
                raise RuntimeError("Live mode requires a configured execution client")
            await self.live_client.cancel_all_orders()
            return
        for market_id in list(self.order_manager.active_orders.keys()):
            await self.cancel_market(market_id)

    async def health_check(self) -> bool:
        if self.config.mode in {"dry", "paper"}:
            return True
        if self.live_client is None:
            return False
        return await self.live_client.health_check()

    async def validate_trading_auth(self) -> bool:
        if self.config.mode in {"dry", "paper"}:
            return True
        if self.live_client is None:
            return False
        return await self.live_client.validate_trading_auth()

    async def get_open_orders_count(self, market_id: str | None = None) -> int | None:
        if self.config.mode in {"dry", "paper"}:
            if market_id:
                return len(self.order_manager.get_market_orders(market_id))
            return len(self.order_manager.get_all_orders())
        if self.live_client is None:
            return None
        return await self.live_client.get_open_orders_count(market_id=market_id)

    async def get_top_of_book(self, token_id: str) -> tuple[float, float] | None:
        if self.config.mode in {"dry", "paper"}:
            return None
        if self.live_client is None:
            return None
        return await self.live_client.get_top_of_book(token_id)

    async def get_book_snapshot(self, token_id: str) -> dict | None:
        if self.config.mode in {"dry", "paper"}:
            return None
        if self.live_client is None:
            return None
        return await self.live_client.get_book_snapshot(token_id)

    async def get_recent_trades(self, after_ts: int | None = None) -> list[dict]:
        if self.config.mode in {"dry", "paper"}:
            return []
        if self.live_client is None:
            return []
        return await self.live_client.get_recent_trades(after_ts=after_ts)

    async def get_market_recent_trade_count(self, market_id: str, after_ts: int | None = None) -> int:
        if self.config.mode in {"dry", "paper"}:
            return 0
        if self.live_client is None:
            return 0
        return await self.live_client.get_market_recent_trade_count(market_id, after_ts=after_ts)

    async def get_collateral_balance_usd(self) -> float | None:
        if self.config.mode in {"dry", "paper"}:
            return None
        if self.live_client is None:
            return None
        return await self.live_client.get_collateral_balance_usd()
