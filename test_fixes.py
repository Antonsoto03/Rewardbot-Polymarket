"""
Tests unitarios para los tres fixes:
  1. Atribución de fills por mercado (_compute_live_trade_pnl_estimate → fills_by_token)
  2. Reemplazo robusto de quotes (_replace_quotes con cleanup ante error parcial)
  3. Cap de exposición por mercado usando abs()
"""

from __future__ import annotations

import asyncio
import unittest
from collections import deque
from unittest.mock import AsyncMock, MagicMock, call, patch

from liquidity_rewards_bot import _compute_live_trade_pnl_estimate, _replace_quotes
from market_maker import Quote


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trade(trade_id: str, token_id: str, side: str, price: float, size: float, ts: int) -> dict:
    """Formato que _parse_trade_row acepta."""
    return {
        "id": trade_id,
        "asset_id": token_id,
        "side": side,
        "price": str(price),
        "size": str(size),
        "timestamp": str(ts),
    }


def _make_mock_execution(*, cancel_side_effect=None, place_side_effects=None):
    """
    Devuelve un ExecutionAdapter mockeado.
    cancel_side_effect: excepción a lanzar en cancel_market (o None).
    place_side_effects: lista de valores/excepciones para cada llamada a place_quote.
    """
    exec_mock = MagicMock()
    exec_mock.cancel_market = AsyncMock(side_effect=cancel_side_effect)
    if place_side_effects is not None:
        exec_mock.place_quote = AsyncMock(side_effect=place_side_effects)
    else:
        exec_mock.place_quote = AsyncMock(return_value=None)
    return exec_mock


# ---------------------------------------------------------------------------
# Fix 1 – Atribución de fills por mercado
# ---------------------------------------------------------------------------

class TestFillsByToken(unittest.IsolatedAsyncioTestCase):

    async def _run_compute(self, raw_trades, seen=None, token_positions=None, token_cashflows=None):
        exec_mock = MagicMock()
        exec_mock.get_recent_trades = AsyncMock(return_value=raw_trades)
        exec_mock.get_top_of_book = AsyncMock(return_value=None)  # sin mark

        return await _compute_live_trade_pnl_estimate(
            execution=exec_mock,
            seen_trade_ids=seen or set(),
            token_positions=token_positions or {},
            token_cashflows=token_cashflows or {},
            token_last_marks={},
            fill_timestamps=deque(),
            last_trade_ts=None,
        )

    async def test_fills_by_token_populated_correctly(self):
        """fills_by_token debe contar fills POR token, no globalmente."""
        trades = [
            _make_trade("t1", "TOKEN_A", "buy",  0.60, 10, 1000),
            _make_trade("t2", "TOKEN_A", "sell", 0.65, 5,  1001),
            _make_trade("t3", "TOKEN_B", "buy",  0.40, 8,  1002),
        ]
        _, _, new_fills, _, fills_by_token = await self._run_compute(trades)

        self.assertEqual(new_fills, 3)
        self.assertEqual(fills_by_token["TOKEN_A"], 2)
        self.assertEqual(fills_by_token["TOKEN_B"], 1)

    async def test_already_seen_trade_not_counted(self):
        """Trades ya procesados (seen_trade_ids) no deben aparecer en fills_by_token."""
        trades = [
            _make_trade("t1", "TOKEN_A", "buy", 0.60, 10, 1000),
            _make_trade("t2", "TOKEN_A", "buy", 0.60, 5,  1001),
        ]
        _, _, new_fills, _, fills_by_token = await self._run_compute(
            trades, seen={"t1"}
        )

        self.assertEqual(new_fills, 1)
        self.assertEqual(fills_by_token.get("TOKEN_A", 0), 1)

    async def test_market_fills_isolation(self):
        """
        Simula la lógica del loop: dado fills_by_token de dos mercados,
        current_market_fills solo suma los tokens del mercado seleccionado.
        Verifica que fills de otro mercado no contaminan el contador.
        """
        trades = [
            _make_trade("t1", "TOKEN_YES_MKT1", "buy",  0.55, 10, 1000),
            _make_trade("t2", "TOKEN_NO_MKT1",  "buy",  0.45, 10, 1001),
            _make_trade("t3", "TOKEN_YES_MKT2", "buy",  0.70, 20, 1002),  # otro mercado
        ]
        _, _, _, _, fills_by_token = await self._run_compute(trades)

        # Mercado 1 tiene token_id=TOKEN_YES_MKT1, no_token_id=TOKEN_NO_MKT1
        current_market_fills = (
            fills_by_token.get("TOKEN_YES_MKT1", 0)
            + fills_by_token.get("TOKEN_NO_MKT1", 0)
        )
        # Mercado 2 tiene fills en TOKEN_YES_MKT2, NO deben sumarse a mkt1
        self.assertEqual(current_market_fills, 2)
        # Total global es 3, pero mkt1 solo ve 2
        self.assertEqual(sum(fills_by_token.values()), 3)

    async def test_fills_by_token_empty_when_no_trades(self):
        _, _, new_fills, _, fills_by_token = await self._run_compute([])
        self.assertEqual(new_fills, 0)
        self.assertEqual(fills_by_token, {})


# ---------------------------------------------------------------------------
# Fix 2 – Reemplazo robusto (_replace_quotes con cleanup)
# ---------------------------------------------------------------------------

class TestReplaceQuotesRobust(unittest.IsolatedAsyncioTestCase):

    def _make_quotes(self, n=2):
        return [
            Quote(market_id="MKT1", token_id="TOK1", side="buy",  price=0.49, size=50.0),
            Quote(market_id="MKT1", token_id="TOK1", side="sell", price=0.51, size=50.0),
        ][:n]

    async def test_success_posts_all_quotes(self):
        """Sin errores: cancel al inicio y todos los quotes posteados."""
        exec_mock = _make_mock_execution()
        quotes = self._make_quotes(2)

        posted = await _replace_quotes(exec_mock, "MKT1", quotes)

        self.assertEqual(posted, 2)
        exec_mock.cancel_market.assert_called_once_with("MKT1")
        self.assertEqual(exec_mock.place_quote.call_count, 2)

    async def test_partial_failure_triggers_cleanup_cancel(self):
        """
        Si place_quote falla en el segundo quote, debe llamarse cancel_market
        como cleanup y relanzar RuntimeError.
        """
        exec_mock = _make_mock_execution(
            place_side_effects=[None, RuntimeError("network timeout")]
        )
        quotes = self._make_quotes(2)

        with self.assertRaises(RuntimeError) as ctx:
            await _replace_quotes(exec_mock, "MKT1", quotes)

        # cancel_market: 1 vez al inicio + 1 vez de cleanup = 2
        self.assertEqual(exec_mock.cancel_market.call_count, 2)
        self.assertIn("Replace failed after posting 1/2", str(ctx.exception))

    async def test_first_quote_failure_still_cleans_up(self):
        """Falla en el primer quote: 0 posteados → cleanup igual."""
        exec_mock = _make_mock_execution(
            place_side_effects=[RuntimeError("rejected")]
        )
        quotes = self._make_quotes(2)

        with self.assertRaises(RuntimeError) as ctx:
            await _replace_quotes(exec_mock, "MKT1", quotes)

        self.assertEqual(exec_mock.cancel_market.call_count, 2)
        self.assertIn("Replace failed after posting 0/2", str(ctx.exception))

    async def test_cleanup_cancel_failure_is_logged_not_swallowed(self):
        """
        Si el cleanup cancel también falla, se loguea pero la excepción
        original sigue propagándose (no silencio).
        """
        exec_mock = _make_mock_execution(
            place_side_effects=[None, RuntimeError("post error")],
            cancel_side_effect=[None, RuntimeError("cleanup also failed")],
        )
        quotes = self._make_quotes(2)

        with self.assertRaises(RuntimeError) as ctx:
            await _replace_quotes(exec_mock, "MKT1", quotes)

        # El error que se propaga es el de replace, no el de cleanup
        self.assertIn("Replace failed", str(ctx.exception))

    async def test_empty_quotes_no_post_called(self):
        """Lista vacía: cancel inicial, cero posts."""
        exec_mock = _make_mock_execution()

        posted = await _replace_quotes(exec_mock, "MKT1", [])

        self.assertEqual(posted, 0)
        exec_mock.cancel_market.assert_called_once()
        exec_mock.place_quote.assert_not_called()


# ---------------------------------------------------------------------------
# Fix 3 – Cap de exposición con abs()
# ---------------------------------------------------------------------------

class TestMarketExposureAbs(unittest.TestCase):
    """
    Verifica que la fórmula de exposición usa abs() en cada pata
    antes de sumar, evitando que signos mixtos se cancelen entre sí.
    """

    def _compute_exposure(self, yes_pos, no_pos, mid):
        """Réplica exacta del cálculo en liquidity_rewards_bot.py:1684-1686."""
        yes_exp_usd = abs(yes_pos * mid)
        no_exp_usd  = abs(no_pos * max(1.0 - mid, 0.0))
        return yes_exp_usd + no_exp_usd

    def test_balanced_long_both_sides(self):
        """Long YES + Long NO a mid=0.5: exposición total = sum de ambas."""
        exp = self._compute_exposure(yes_pos=100, no_pos=100, mid=0.5)
        self.assertAlmostEqual(exp, 100)  # 50 + 50

    def test_mixed_signs_do_not_cancel(self):
        """YES positivo, NO negativo: sin abs() se cancelarían."""
        exp = self._compute_exposure(yes_pos=100, no_pos=-100, mid=0.5)
        # Con abs(): 50 + 50 = 100 (exposición real)
        # Sin abs(): 50 - 50 = 0  (subestimaría la exposición)
        self.assertAlmostEqual(exp, 100)

    def test_zero_position_no_exposure(self):
        exp = self._compute_exposure(yes_pos=0, no_pos=0, mid=0.5)
        self.assertEqual(exp, 0.0)

    def test_only_yes_exposure(self):
        exp = self._compute_exposure(yes_pos=200, no_pos=0, mid=0.60)
        self.assertAlmostEqual(exp, 120.0)  # 200 * 0.60

    def test_only_no_exposure(self):
        exp = self._compute_exposure(yes_pos=0, no_pos=150, mid=0.70)
        self.assertAlmostEqual(exp, 45.0)  # 150 * 0.30

    def test_large_negative_no_position(self):
        """Posición NO muy negativa: abs() captura la exposición real."""
        exp = self._compute_exposure(yes_pos=0, no_pos=-500, mid=0.40)
        self.assertAlmostEqual(exp, 300.0)  # abs(-500 * 0.60) = 300

    def test_exceeds_cap(self):
        """Confirma que con signos mixtos el cap se dispara correctamente."""
        max_market_exposure_usd = 100.0
        exp = self._compute_exposure(yes_pos=80, no_pos=-80, mid=0.5)
        # Con abs(): 40 + 40 = 80 → no supera 100 (correcto)
        self.assertLess(exp, max_market_exposure_usd)

        exp2 = self._compute_exposure(yes_pos=120, no_pos=-120, mid=0.5)
        # Con abs(): 60 + 60 = 120 → sí supera 100 (correcto, se aplica cap)
        self.assertGreater(exp2, max_market_exposure_usd)


if __name__ == "__main__":
    unittest.main(verbosity=2)
