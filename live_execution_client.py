from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from market_maker import Quote


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
    place_order_path: str = "/v1/orders"
    cancel_market_path: str = "/v1/orders/cancel_market"
    cancel_all_path: str = "/v1/orders/cancel_all"
    health_path: str = "/health"
    timeout_seconds: float = 10.0
    max_retries: int = 3
    retry_backoff_seconds: float = 0.5


class LiveHttpExecutionClient:
    def __init__(self, config: LiveHttpConfig) -> None:
        self.config = config

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
        else:
            raise ValueError("Unsupported POLY_AUTH_MODE. Use 'relayer' or 'poly_l2'")

        config = LiveHttpConfig(
            base_url=base_url.rstrip("/"),
            auth_mode=auth_mode,
            relayer_api_key=relayer_api_key,
            relayer_api_key_address=relayer_api_key_address,
            poly_api_key=poly_api_key,
            poly_api_secret=poly_api_secret,
            poly_api_passphrase=poly_api_passphrase,
            place_order_path=os.getenv("POLY_PLACE_ORDER_PATH", "/v1/orders"),
            cancel_market_path=os.getenv("POLY_CANCEL_MARKET_PATH", "/v1/orders/cancel_market"),
            cancel_all_path=os.getenv("POLY_CANCEL_ALL_PATH", "/v1/orders/cancel_all"),
            health_path=os.getenv("POLY_HEALTH_PATH", "/health"),
            timeout_seconds=float(os.getenv("POLY_HTTP_TIMEOUT_SECONDS", "10")),
            max_retries=int(os.getenv("POLY_HTTP_MAX_RETRIES", "3")),
            retry_backoff_seconds=float(os.getenv("POLY_HTTP_RETRY_BACKOFF_SECONDS", "0.5")),
        )
        return cls(config)

    async def place_limit_order(self, quote: Quote, client_order_id: str) -> None:
        payload = {
            "client_order_id": client_order_id,
            "market_id": quote.market_id,
            "side": quote.side,
            "price": quote.price,
            "size": quote.size,
            "order_type": "limit",
        }
        await self._request_with_retry("POST", self.config.place_order_path, payload)

    async def cancel_market_orders(self, market_id: str) -> None:
        payload = {"market_id": market_id}
        await self._request_with_retry("POST", self.config.cancel_market_path, payload)

    async def cancel_all_orders(self) -> None:
        await self._request_with_retry("POST", self.config.cancel_all_path, {})

    async def health_check(self) -> bool:
        try:
            await self._request_with_retry("GET", self.config.health_path, None)
            return True
        except Exception as exc:
            logger.warning("Live health-check failed: %s", exc)
            return False

    async def _request_with_retry(self, method: str, path: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        attempts = max(1, self.config.max_retries)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await asyncio.to_thread(self._request_sync, method, path, payload)
            except Exception as exc:
                last_exc = exc
                if attempt >= attempts:
                    break
                backoff = self.config.retry_backoff_seconds * attempt
                await asyncio.sleep(backoff)
        if last_exc is None:
            raise RuntimeError("Unknown HTTP error")
        raise last_exc

    def _request_sync(self, method: str, path: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")

        url = f"{self.config.base_url}{path}"
        req = request.Request(url=url, method=method, data=body)
        req.add_header("Content-Type", "application/json")
        ts_ms = str(int(time.time() * 1000))
        if self.config.auth_mode == "relayer":
            req.add_header("RELAYER_API_KEY", self.config.relayer_api_key)
            req.add_header("RELAYER_API_KEY_ADDRESS", self.config.relayer_api_key_address)
        else:
            req.add_header("POLY_API_KEY", self.config.poly_api_key)
            req.add_header("POLY_PASSPHRASE", self.config.poly_api_passphrase)
            req.add_header("POLY_TIMESTAMP", ts_ms)
            # Placeholder: real CLOB L2 requires HMAC-SHA256 canonical signing.
            # We keep this explicit so live execution doesn't silently run with invalid signatures.
            raise RuntimeError("POLY_AUTH_MODE=poly_l2 not fully implemented: HMAC signature generation required")

        try:
            with request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"HTTPError {exc.code} {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"URLError {exc.reason}") from exc

        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}
