"""Optional S-Scalp auto-order worker. Never deployed with the paper dashboard.

Only same-venue Bybit spot order flow can trigger a safeguarded LIVE order.
Starts in OBSERVE_ONLY unless explicit flags and API credentials are configured.
"""
import asyncio
from collections import deque
import json
import logging
import os
import time

from aiohttp import ClientSession, ClientTimeout, WSMsgType, web

from app import PaperEngine, SYMBOLS
from live_execution import (BybitSpotClient, GuardedExecution,
                            ExecutionDisabled, VenueError, AmbiguousOrder)

LOG = logging.getLogger("s_scalp.auto")
STREAMS = {
    "kz": ("bybit_kz", "wss://stream.bybit.kz/v5/public/spot"),
    "testnet": ("bybit_testnet", "wss://stream-testnet.bybit.com/v5/public/spot"),
}


class AutoSignals(PaperEngine):
    def __init__(self, db_path, controller, venue):
        super().__init__(db_path)
        self.controller = controller
        self.venue = venue
        self._tasks = set()
        self._last_signal = {}
        self._last_error = ""
        self.signals_seen = 0

    def enter(self, venue, symbol, now=None):
        # The parent momentum detector calls this when an eligible signal
        # fires. The replacement never makes virtual trades.
        if venue != self.venue or symbol not in SYMBOLS:
            return False
        now = time.time() if now is None else now
        if now - self._last_signal.get(symbol, 0) < 60:
            return False
        quote = self.quotes.get((venue, symbol))
        if quote is None:
            return False
        self._last_signal[symbol] = now
        self.signals_seen += 1
        if not self.controller.ready():
            LOG.info("Eligible %s signal seen (OBSERVE_ONLY, no real order)", symbol)
            return False
        task = asyncio.create_task(self._submit_signal(
            symbol, {"venue": venue, "bid": quote.bid, "ask": quote.ask},
            quote.received_at))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return False

    async def _submit_signal(self, symbol, quote, observed_at):
        try:
            result = await self.controller.preview_or_submit(
                symbol, quote, observed_at=observed_at)
            LOG.warning("REAL order ACCEPTED, NOT CONFIRMED FILLED: %s %s",
                        symbol, result["order_link_id"])
        except ExecutionDisabled as exc:
            LOG.info("%s signal denied: %s", symbol, exc)
        except (VenueError, AmbiguousOrder) as exc:
            self._last_error = str(exc)
            LOG.error("%s order ambiguous/failed, halt until review: %s",
                      symbol, exc)
        except Exception:
            self._last_error = "Unexpected execution failure: inspect logs"
            LOG.exception("Unexpected execution failure")


def parse_bybit(engine, venue, payload):
    topic = str(payload.get("topic", ""))
    if topic.startswith("orderbook.1."):
        sym = topic.rsplit(".", 1)[-1]
        data = payload.get("data", {})
        bids, asks = data.get("b", []), data.get("a", [])
        if bids and asks:
            engine.on_book(venue, sym, bids[0][0], asks[0][0],
                           bids[0][1], asks[0][1])
    elif topic.startswith("publicTrade."):
        for trade in payload.get("data", []):
            engine.on_trade(venue, trade.get("s"), trade.get("p"), trade.get("v"),
                            trade.get("S") == "Buy")


async def run_ws(engine, venue, url):
    backoff = 2
    while True:
        ping = None
        try:
            timeout = ClientTimeout(total=None, sock_connect=15, sock_read=None)
            async with ClientSession(timeout=timeout) as session:
                async with session.ws_connect(url, heartbeat=20,
                                              receive_timeout=65) as ws:
                    engine.feed[venue] = {"connected": True,
                                          "last_message": None, "error": ""}
                    LOG.info("Trading signal market feed connected: %s", venue)
                    backoff = 2
                    topics = ([f"orderbook.1.{s}" for s in SYMBOLS]
                              + [f"publicTrade.{s}" for s in SYMBOLS])
                    await ws.send_json({"op": "subscribe", "args": topics})
                    async def ping_loop():
                        while True:
                            await asyncio.sleep(18)
                            await ws.send_json({"op": "ping"})
                    ping = asyncio.create_task(ping_loop())
                    async for m in ws:
                        if m.type == WSMsgType.TEXT:
                            try:
                                parse_bybit(engine, venue, json.loads(m.data))
                                engine.feed[venue]["last_message"] = time.time()
                            except (TypeError, KeyError, ValueError) as exc:
                                LOG.warning("Invalid venue market frame: %s", exc)
                        elif m.type in (WSMsgType.CLOSED, WSMsgType.CLOSE, WSMsgType.ERROR):
                            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine.feed.setdefault(venue, {})["error"] = str(exc)[:120]
            LOG.warning("Trading signal market feed disconnected: %s", exc)
        finally:
            engine.feed.setdefault(venue, {})["connected"] = False
            if ping:
                ping.cancel()
                await asyncio.gather(ping, return_exceptions=True)
        await asyncio.sleep(backoff)
        backoff = min(60, backoff * 2)


async def run():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    env = os.environ.get("SCALP_VENUE_ENV", "testnet").lower()
    if env not in STREAMS:
        raise RuntimeError("Invalid SCALP_VENUE_ENV")
    venue, endpoint = STREAMS[env]
    enabled = os.environ.get("SCALP_REAL_ORDERS", "") == "ENABLED"
    ack = (os.environ.get("SCALP_REAL_RISK_ACK", "") == "I_APPROVE_SPOT_RISK"
           and os.environ.get("SCALP_PROTECTIVE_ORDERS_VERIFIED", "") == "YES")
    client = BybitSpotClient(os.environ.get("BYBIT_API_KEY", ""),
                             os.environ.get("BYBIT_API_SECRET", ""),
                             env=env)
    controller = GuardedExecution(
        client, os.environ.get("SCALP_AUTO_JOURNAL", "/data/scalp_auto.sqlite3"),
        live_enabled=enabled, acknowledged_risk=ack)
    db = os.environ.get("SCALP_SIGNAL_DB", "/data/scalp_signal.sqlite3")
    engine = AutoSignals(db, controller, venue)
    if controller.ready():
        LOG.warning("LIVE ORDERS ARMED (%s), ONE ORDER UNTIL RECONCILED!", env)
    else:
        LOG.info("OBSERVE_ONLY: real orders disabled")
    app = web.Application()
    app["engine"] = engine
    async def health(request):
        feed = engine.feed.get(venue, {})
        fresh = bool(feed.get("connected") and feed.get("last_message")
                     and time.time()-feed["last_message"] < 30)
        return web.json_response({
            "service": "S-Scalp auto worker",
            "mode": "LIVE_ARMED" if controller.ready() else "OBSERVE_ONLY",
            "feed_fresh": fresh, "venue": venue,
            "signals_seen": engine.signals_seen,
            "live_order_attempts": len(controller.status()["attempts"]),
            "last_error": engine._last_error[:120],
        })
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", "8080"))).start()
    try:
        await run_ws(engine, venue, endpoint)
    finally:
        await runner.cleanup()
        controller.db.close()
        engine.db.close()


if __name__ == "__main__":
    asyncio.run(run())
