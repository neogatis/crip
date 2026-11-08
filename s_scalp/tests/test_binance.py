import os
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from binance_execution import (BinanceSpotClient, Supervisor, ExecutionDisabled,
                               UnknownOrderState, build_opoco, pnl_from_trades,
                               Limits, hmac_signature, step_round)


ASSET = {
    "symbol": "BTCUSDT", "quoteAsset": "USDT", "baseAsset": "BTC",
    "status": "TRADING", "isSpotTradingAllowed": True,
    "opoAllowed": True, "otoAllowed": True, "ocoAllowed": True,
    "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
        {"filterType": "LOT_SIZE", "stepSize": "0.0001", "minQty": "0.0001"},
        {"filterType": "NOTIONAL", "minNotional": "5"},
    ],
}
QUOTE = {"bid": 100, "ask": 100.02, "venue": "binance_testnet"}


class FakeBinance:
    mode = "testnet"
    key = "fake-key"
    secret = "fake-secret"

    def __init__(self):
        self.submissions = 0
        self.cancellations = 0
        self.sent_payload = None
        self.return_exception = None
        self.quote = {"bidPrice": "100", "askPrice": "100.02"}
        self.free = "100"
        self.list_status = "EXECUTING"
        self.working_status = "NEW"
        self.buy_filled = "0"
        self.sell_filled = "0"
        self.fee_asset = "USDT"

    async def instrument(self, symbol):
        return ASSET

    async def best_quote(self, symbol):
        return self.quote

    async def account(self):
        return {"balances":[{"asset":"USDT","free":self.free}]}

    async def place_opoco(self, params):
        self.submissions += 1
        self.sent_payload = params
        if self.return_exception:
            raise self.return_exception
        return {"orderListId": 777, "orders": [
            {"orderId": 1}, {"orderId": 2}, {"orderId": 3}]}

    async def list_state(self, list_id):
        return {"orderListId": 777, "listOrderStatus": self.list_status,
                "orders": [{"orderId": 1}, {"orderId": 2}, {"orderId": 3}]}

    async def order(self, symbol, order_id):
        if int(order_id)==1:
            return {"orderId":1, "side":"BUY",
                    "status": self.working_status, "executedQty":self.buy_filled}
        if int(order_id)==2:
            return {"orderId":2, "side":"SELL",
                    "status":"FILLED" if self.sell_filled!="0" else "PENDING_NEW",
                    "executedQty":self.sell_filled}
        return {"orderId":3, "side":"SELL","status":"CANCELED",
                "executedQty":"0"}

    async def trades(self,symbol,order_id):
        if int(order_id)==1:
            return [{"qty":"0.25","quoteQty":"25.0",
                     "commission":"0.025","commissionAsset":self.fee_asset}]
        if int(order_id)==2:
            return [{"qty":"0.25","quoteQty":"25.4",
                     "commission":"0.025","commissionAsset":self.fee_asset}]
        return []

    async def cancel_list(self,symbol,list_id):
        self.cancellations+=1
        return {"listOrderStatus":"ALL_DONE"}


class BinanceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=str(Path(self.tmp.name)/"journal.sqlite3")
        self.fake=FakeBinance()

    def tearDown(self):
        self.tmp.cleanup()

    def supervisor(self, enabled=True):
        return Supervisor(self.fake, self.path, live=enabled,
                          acknowledged=enabled, platform_verified=enabled)

    def test_hmac_and_filters(self):
        import hashlib, hmac
        params=[("symbol","BTCUSDT"),("timestamp","123456")]
        raw="symbol=BTCUSDT&timestamp=123456"
        expected=hmac.new(b"SECRET",raw.encode(),hashlib.sha256).hexdigest()
        self.assertEqual(hmac_signature("SECRET",params),expected)
        self.assertEqual(step_round("0.12345","0.001"),Decimal("0.123"))
        a,notional=build_opoco("BTCUSDT",ASSET,"100","100.02",Limits(),
                                client_id="ssc123")
        self.assertEqual(a["workingSide"],"BUY")
        self.assertEqual(a["pendingSide"],"SELL")
        self.assertEqual(a["pendingAboveType"],"TAKE_PROFIT")
        self.assertEqual(a["pendingBelowType"],"STOP_LOSS")
        self.assertNotIn("pendingQuantity",a)
        self.assertLessEqual(notional,Decimal("25"))
        self.assertLess(Decimal(a["pendingBelowStopPrice"]),
                        Decimal(a["workingPrice"]))
        self.assertGreater(Decimal(a["pendingAboveStopPrice"]),
                           Decimal(a["workingPrice"]))

    def test_reject_ineligible_symbol(self):
        asset={**ASSET,"opoAllowed":False}
        with self.assertRaises(ExecutionDisabled):
            build_opoco("BTCUSDT",asset,100,100.02,Limits(),client_id="x")
        # Even if a venue returns this in its info, Supervisor checks instrument:
        # the build stage needs to enforce symbol's capabilities.
        self.assertFalse(asset["opoAllowed"])

    async def test_observe_never_sends(self):
        sup=self.supervisor(enabled=False)
        with self.assertRaises(ExecutionDisabled):
            await sup.open_signal("BTCUSDT", QUOTE,observed_at=100,now=100)
        self.assertEqual(self.fake.submissions,0)
        self.assertEqual(sup.status()["mode"],"OBSERVE_ONLY")
        sup.db.close()

    async def test_fresh_signal_places_single_order_list(self):
        sup=self.supervisor()
        result=await sup.open_signal("BTCUSDT",QUOTE,observed_at=100,now=100)
        self.assertEqual(result["status"],"ACTIVE_NOT_FILLED")
        self.assertEqual(self.fake.submissions,1)
        self.assertEqual(self.fake.sent_payload["workingType"],"LIMIT")
        with self.assertRaises(ExecutionDisabled):
            await sup.open_signal("ETHUSDT",QUOTE,observed_at=100,now=100)
        sup.db.close()
        recovered=self.supervisor()
        with self.assertRaises(ExecutionDisabled):
            await recovered.open_signal("BTCUSDT",QUOTE,observed_at=105,now=105)
        self.assertEqual(self.fake.submissions,1)
        recovered.db.close()

    async def test_quote_staleness_cross_venue_and_spread(self):
        sup=self.supervisor()
        for quote,at,now in [(QUOTE,100,104),
                             ({**QUOTE,"venue":"bybit_kz"},100,100),
                             ({**QUOTE,"ask":101.0},100,100)]:
            with self.assertRaises(ExecutionDisabled):
                await sup.open_signal("BTCUSDT",quote,observed_at=at,now=now)
        self.assertEqual(self.fake.submissions,0)
        sup.db.close()

    async def test_unknown_does_not_auto_retry(self):
        sup=self.supervisor()
        self.fake.return_exception=UnknownOrderState("no response")
        with self.assertRaises(UnknownOrderState):
            await sup.open_signal("BTCUSDT",QUOTE,observed_at=100,now=100)
        self.assertEqual(self.fake.submissions,1)
        self.assertTrue(sup.status()["halted"])
        self.assertEqual(sup.status()["orders"][0]["status"],"UNKNOWN")
        with self.assertRaises(ExecutionDisabled):
            await sup.open_signal("BTCUSDT",QUOTE,observed_at=110,now=110)
        self.assertEqual(self.fake.submissions,1)
        sup.db.close()

    async def test_partial_fill_halts(self):
        sup=self.supervisor()
        await sup.open_signal("BTCUSDT",QUOTE,observed_at=100,now=100)
        self.fake.buy_filled="0.12"
        self.fake.working_status="PARTIALLY_FILLED"
        result=await sup.reconcile(now=106)
        self.assertEqual(result[0]["status"],"PARTIAL_UNPROTECTED")
        self.assertTrue(sup.status()["halted"])
        sup.db.close()

    async def test_cancel_stale_empty_buy(self):
        sup=self.supervisor()
        await sup.open_signal("BTCUSDT",QUOTE,observed_at=100,now=100)
        result=await sup.reconcile(now=155)
        self.assertEqual(self.fake.cancellations,1)
        self.assertEqual(result[0]["status"],"NO_FILL")
        self.assertFalse(sup.status()["halted"])
        sup.db.close()

    async def test_close_verified_and_pnl(self):
        sup=self.supervisor()
        await sup.open_signal("BTCUSDT",QUOTE,observed_at=100,now=100)
        self.fake.buy_filled="0.25"
        self.fake.working_status="FILLED"
        self.fake.sell_filled="0.25"
        self.fake.list_status="ALL_DONE"
        result=await sup.reconcile(now=200)
        self.assertEqual(result[0]["status"],"CLOSED")
        self.assertEqual(Decimal(result[0]["pnl_usdt"]),Decimal("0.35"))
        self.assertFalse(sup.status()["halted"])
        sup.db.close()

    async def test_non_quote_fee_halts(self):
        sup=self.supervisor()
        await sup.open_signal("BTCUSDT",QUOTE,observed_at=100,now=100)
        self.fake.buy_filled="0.25"
        self.fake.working_status="FILLED"
        self.fake.sell_filled="0.25"
        self.fake.list_status="ALL_DONE"
        self.fake.fee_asset="BNB"
        await sup.reconcile(now=200)
        self.assertTrue(sup.status()["halted"])
        self.assertEqual(sup.status()["orders"][0]["status"],"REVIEW_FEES")
        sup.db.close()

    def test_pnl_dust_or_unpriced_fee_fails_closed(self):
        buy=[{"qty":"1","quoteQty":"100","commission":"0","commissionAsset":"BTC"}]
        sell=[{"qty":".999","quoteQty":"101","commission":"0","commissionAsset":"USDT"}]
        self.assertIsNone(pnl_from_trades(buy,sell,"BTC"))
        sell[0]["qty"]="1"
        self.assertEqual(pnl_from_trades(buy,sell,"BTC"),Decimal("1"))
        buy[0]["commissionAsset"]="BNB"
        self.assertIsNone(pnl_from_trades(buy,sell,"BTC"))


if __name__=="__main__":
    unittest.main()
