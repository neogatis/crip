import asyncio
import os
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from live_execution import (GuardedExecution, BybitSpotClient,
                            ExecutionDisabled, VenueError, AmbiguousOrder,
                            quantize_step, sign_bybit, KZ_API)


class FakeVenue:
    base = KZ_API
    key = "local-fake-key"
    secret = "local-fake-secret"

    def __init__(self):
        self.calls = 0
        self.accept = True
        self.error = None
        self.price = "100.02"

    async def ticker(self, symbol):
        return {"symbol": symbol, "ask1Price": self.price, "bid1Price": "100"}

    async def instrument(self, symbol):
        return {"symbol": symbol, "status": "Trading", "quoteCoin": "USDT",
                "priceFilter": {"tickSize": "0.01"},
                "lotSizeFilter": {"basePrecision": "0.0001",
                                  "minOrderAmt": "5", "maxLimitOrderQty": "100"}}

    async def wallet(self):
        return Decimal("100")

    async def submit_order(self, order):
        self.calls += 1
        self.last_order = order
        if self.error:
            raise self.error
        return {"orderId": "fake-no-real-order", "orderLinkId": order["orderLinkId"]}

    async def query_order(self, symbol, link_id):
        return {"orderLinkId": link_id, "orderStatus": "Filled",
                "cumExecQty": "0.25", "cumExecValue": "25", "avgPrice": "100.02"}


class LiveGuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dbpath = str(Path(self.tmp.name) / "journal.sqlite3")
        self.venue = FakeVenue()

    def tearDown(self):
        self.tmp.cleanup()

    def ctrl(self, enabled=True, ack=True):
        return GuardedExecution(self.venue, self.dbpath,
                                live_enabled=enabled, acknowledged_risk=ack)

    def signal(self, **changes):
        obj = {"venue": "bybit_kz", "bid": 100, "ask": 100.02}
        obj.update(changes)
        return obj

    async def test_disabled_by_default(self):
        client = BybitSpotClient()
        self.assertEqual(client.base, KZ_API)
        self.assertEqual(GuardedExecution(client, self.dbpath).status()["mode"],
                         "DISABLED")
        bot = self.ctrl(enabled=False)
        with self.assertRaises(ExecutionDisabled):
            await bot.preview_or_submit("BTCUSDT", self.signal(),
                                        observed_at=100, now=100)
        self.assertEqual(self.venue.calls, 0)
        bot.db.close()

    async def test_ack_and_keys_required(self):
        bot = self.ctrl(enabled=True, ack=False)
        with self.assertRaises(ExecutionDisabled):
            await bot.preview_or_submit("BTCUSDT", self.signal(), observed_at=100,
                                        now=100)
        self.assertEqual(self.venue.calls, 0)
        bot.db.close()

    async def test_rejects_cross_venue_and_stale_quotes(self):
        bot = self.ctrl()
        for q, seen, now in ((self.signal(venue="binance_public"), 100, 100),
                             (self.signal(), 100, 105),
                             (self.signal(ask=100.5), 100, 100)):
            with self.assertRaises(ExecutionDisabled):
                await bot.preview_or_submit("BTCUSDT", q, observed_at=seen,
                                            now=now)
        self.assertEqual(self.venue.calls, 0)
        bot.db.close()

    async def test_limit_ioc_and_exchange_native_protection(self):
        bot = self.ctrl()
        result = await bot.preview_or_submit("BTCUSDT", self.signal(),
                                             observed_at=100, now=100)
        self.assertEqual(result["state"], "ACKNOWLEDGED_NOT_CONFIRMED_FILLED")
        o = self.venue.last_order
        self.assertEqual(o["category"], "spot")
        self.assertEqual(o["side"], "Buy")
        self.assertEqual(o["orderType"], "Limit")
        self.assertEqual(o["timeInForce"], "IOC")
        self.assertEqual(o["isLeverage"], 0)
        self.assertEqual(o["tpOrderType"], "Market")
        self.assertEqual(o["slOrderType"], "Market")
        self.assertLess(Decimal(o["stopLoss"]), Decimal(o["price"]))
        self.assertGreater(Decimal(o["takeProfit"]), Decimal(o["price"]))
        self.assertLessEqual(Decimal(o["qty"]) * Decimal(o["price"]), 25)
        self.assertEqual(self.venue.calls, 1)
        bot.db.close()

    async def test_restart_does_not_repeat_live_buy(self):
        bot = self.ctrl()
        await bot.preview_or_submit("BTCUSDT", self.signal(), observed_at=100,
                                    now=100)
        bot.db.close()
        reopened = self.ctrl()
        with self.assertRaises(ExecutionDisabled):
            await reopened.preview_or_submit("BTCUSDT", self.signal(),
                                             observed_at=120, now=120)
        self.assertEqual(self.venue.calls, 1)
        reopened.db.close()

    async def test_network_error_keeps_unknown_journal(self):
        bot = self.ctrl()
        self.venue.error = AmbiguousOrder("network unknown")
        with self.assertRaises(AmbiguousOrder):
            await bot.preview_or_submit("BTCUSDT", self.signal(),
                                        observed_at=100, now=100)
        rows = bot.status()["attempts"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state"], "UNKNOWN")
        with self.assertRaises(ExecutionDisabled):
            await bot.preview_or_submit("BTCUSDT", self.signal(),
                                        observed_at=102, now=102)
        self.assertEqual(self.venue.calls, 1)
        bot.db.close()

    async def test_ack_is_not_fill_and_reconciliation(self):
        bot = self.ctrl()
        res = await bot.preview_or_submit("BTCUSDT", self.signal(),
                                          observed_at=100, now=100)
        updated = await bot.reconcile(res["order_link_id"])
        self.assertEqual(updated["state"], "Filled")
        self.assertTrue(updated["requires_position_protection_verification"])
        with self.assertRaises(ExecutionDisabled):
            await bot.preview_or_submit("BTCUSDT", self.signal(),
                                        observed_at=120, now=120)
        bot.db.close()

    async def test_insufficient_balance_and_below_minimum(self):
        class NoFunds(FakeVenue):
            async def wallet(self):
                return Decimal("0")
        bot = GuardedExecution(NoFunds(), self.dbpath, live_enabled=True,
                               acknowledged_risk=True)
        with self.assertRaises(ExecutionDisabled):
            await bot.preview_or_submit("BTCUSDT", self.signal(),
                                        observed_at=100, now=100)
        bot.db.close()
        self.assertEqual(self.venue.calls, 0)

    def test_hmac_and_decimal_step(self):
        digest = sign_bybit("1658384314791", "XXXXXXXXXX", "secret",
                            "category=option&symbol=BTC-29JUL22-25000-C")
        self.assertEqual(len(digest), 64)
        self.assertEqual(quantize_step("100.019", "0.01"), Decimal("100.01"))


if __name__ == "__main__":
    unittest.main()
