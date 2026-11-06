import os
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import (PaperEngine, FEE_RATE, QUOTE_TTL, handle_binance, handle_bybit,
                 fee_adjusted_sale)


class PaperEngineTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "trades.sqlite3")
        self.engine = PaperEngine(self.path)

    def tearDown(self):
        self.engine.db.close()
        self.dir.cleanup()

    def book(self, bid=100, ask=100.02, now=10000, venue="binance_public",
             symbol="BTCUSDT"):
        self.engine.on_book(venue, symbol, bid, ask, 100, 100, now=now)

    def test_initial_state_and_paper_only(self):
        state = self.engine.snapshot()
        self.assertEqual(state["mode"], "PAPER_ONLY")
        self.assertFalse(state["execution_enabled"])
        self.assertEqual(state["cash"], 500)
        self.assertEqual(state["closed_trades"], 0)

    def test_entry_uses_ask_slippage_and_entry_fee(self):
        self.book()
        self.assertTrue(self.engine.enter("binance_public", "BTCUSDT", now=10000))
        p = self.engine.positions[("binance_public", "BTCUSDT")]
        self.assertAlmostEqual(p.entry_price, 100.02 * 1.0005)
        self.assertAlmostEqual(p.entry_fee, 0.05)
        self.assertAlmostEqual(self.engine.cash, 449.95)

    def test_no_double_exposure_or_stale_quotes(self):
        self.book()
        self.assertFalse(self.engine.enter("binance_public", "BTCUSDT",
                                           now=10000 + QUOTE_TTL + 1))
        self.book()
        self.assertTrue(self.engine.enter("binance_public", "BTCUSDT", now=10000))
        self.book(venue="bybit_kz", now=10000)
        self.assertFalse(self.engine.enter("bybit_kz", "BTCUSDT", now=10000))

    def test_flat_price_loses_after_costs(self):
        self.book()
        self.engine.enter("binance_public", "BTCUSDT", now=10000)
        self.assertTrue(self.engine.exit("binance_public", "BTCUSDT",
                                         "TEST", now=10001))
        last = self.engine.snapshot()["trades"][0]
        self.assertLess(last["pnl"], -0.1)
        self.assertAlmostEqual(self.engine.cash, 500 + last["pnl"])
        self.assertEqual(last["entry_fee"], 50 * FEE_RATE)
        self.assertAlmostEqual(self.engine.daily_pnl(10001), last["pnl"])

    def test_stop_take_use_net_after_costs(self):
        self.book()
        self.engine.enter("binance_public", "BTCUSDT", now=10000)
        self.book(bid=101.2, ask=101.22, now=10010)
        self.assertEqual(self.engine.snapshot()["trades"][0]["reason"], "TAKE_PROFIT")
        self.book(now=10400, venue="bybit_kz", symbol="ETHUSDT")
        self.assertTrue(self.engine.enter("bybit_kz", "ETHUSDT", now=10400))
        self.book(bid=99, ask=99.02, now=10401, venue="bybit_kz", symbol="ETHUSDT")
        self.assertEqual(self.engine.snapshot()["trades"][0]["reason"], "STOP_LOSS")

    def test_momentum_requires_real_warmup_and_buys(self):
        for i in range(13):
            self.book(now=10000+i)
            self.engine.on_trade("binance_public", "BTCUSDT",
                                 100 + i * 0.035, 1, True, now=10000+i)
        self.assertIn(("binance_public", "BTCUSDT"), self.engine.positions)

    def test_no_entry_on_sell_pressure(self):
        for i in range(13):
            self.book(now=10000+i)
            self.engine.on_trade("binance_public", "BTCUSDT",
                                 100 + i * 0.04, 1, False, now=10000+i)
        self.assertEqual(len(self.engine.positions), 0)

    def test_persistence_across_restart(self):
        self.book()
        self.engine.enter("binance_public", "BTCUSDT", now=10000)
        self.engine.db.close()
        other = PaperEngine(self.path)
        try:
            self.assertAlmostEqual(other.cash, 449.95)
            self.assertIn(("binance_public", "BTCUSDT"), other.positions)
        finally:
            other.db.close()

    def test_public_stream_parsers(self):
        handle_binance(self.engine, {"s":"BTCUSDT", "b":"100", "a":"100.02",
                                      "B":"8", "A":"4"})
        self.assertIn(("binance_public", "BTCUSDT"), self.engine.quotes)
        handle_binance(self.engine, {"e":"aggTrade", "s":"BTCUSDT", "p":"100",
                                      "q":"2", "m":False})
        self.assertEqual(len(self.engine.tape[("binance_public", "BTCUSDT")]), 1)
        handle_bybit(self.engine, {"topic":"orderbook.1.ETHUSDT",
                                   "data":{"b":[["200","10"]],
                                           "a":[["200.02","12"]]}})
        self.assertIn(("bybit_kz", "ETHUSDT"), self.engine.quotes)
        handle_bybit(self.engine, {"topic":"publicTrade.ETHUSDT",
                                   "data":[{"s":"ETHUSDT","p":"200","v":"1","S":"Buy"}]})
        self.assertEqual(len(self.engine.tape[("bybit_kz", "ETHUSDT")]), 1)

    def test_fee_adjusted_sale_deducts_both_cost_models(self):
        self.assertAlmostEqual(fee_adjusted_sale(1, 100), 100*0.9995*0.999)


if __name__ == "__main__":
    unittest.main()
