"""Independent Binance Spot signal worker.

OBSERVE_ONLY by default. Exchange orders require Binance keys, an eligible
account, deliberate operator flags, and a fresh same-venue trade signal.
"""
import asyncio
import json
import logging
import os
from pathlib import Path
import time

from aiohttp import ClientSession, ClientTimeout, WSMsgType, web
from app import PaperEngine, SYMBOLS, login as paper_login, logout as paper_logout
from app import authorized as paper_authorized, headers as response_headers
from binance_execution import (
    BinanceSpotClient, Supervisor, ExecutionDisabled,
    VenueFailure, UnknownOrderState,
)

LOG = logging.getLogger("s_scalp.binance")
LIVE_STREAM = "wss://stream.binance.com:9443/stream?streams="
TEST_STREAM = "wss://stream.testnet.binance.vision:9443/stream?streams="
PUBLIC_STREAM = "wss://data-stream.binance.vision:443/stream?streams="


def config():
    mode = os.environ.get("BINANCE_EXECUTION_ENV", "observe").lower()
    if mode not in {"observe", "testnet", "live"}:
        raise ValueError("Invalid BINANCE_EXECUTION_ENV")
    exchange_mode = "live" if mode == "live" else "testnet"
    venue = {"observe": "binance_public", "testnet": "binance_testnet",
             "live": "binance_global"}[mode]
    stream = {"observe": PUBLIC_STREAM, "testnet": TEST_STREAM,
              "live": LIVE_STREAM}[mode]
    armed = os.environ.get("SCALP_REAL_ORDERS", "OFF") == "ON"
    consent = os.environ.get("SCALP_REAL_RISK_ACK", "") == "I_AUTHORIZE_SPOT_TRADING"
    platform = (mode == "testnet" or (
        mode == "live" and os.environ.get("BINANCE_ACCOUNT_PLATFORM") == "GLOBAL"
        and os.environ.get("BINANCE_PLATFORM_API_VERIFIED") == "YES"
        and os.environ.get("BINANCE_LIVE_ACCESS_APPROVED") == "YES"))
    return mode, exchange_mode, venue, stream, armed, consent, platform


class BinanceSignals(PaperEngine):
    def __init__(self, db_path, supervisor, venue):
        super().__init__(db_path)
        self.supervisor = supervisor
        self.venue = venue
        self.last_signal = {}
        self.signal_events = 0
        self.pending = set()
        self.last_error = ""

    def enter(self, venue, symbol, now=None):
        # Parent momentum detector delegates entry here. Never open a
        # virtual position or a live order through PaperEngine.enter().
        now = time.time() if now is None else now
        if venue != self.venue or symbol not in SYMBOLS:
            return False
        if now - self.last_signal.get(symbol, 0) < 60:
            return False
        quote = self.quotes.get((venue, symbol))
        if not quote:
            return False
        self.last_signal[symbol] = now
        self.signal_events += 1
        if not self.supervisor.ready():
            LOG.info("Strategy signal %s; OBSERVE_ONLY (no order)", symbol)
            return False
        coro = self.try_submit(symbol, quote, now)
        task = asyncio.create_task(coro)
        self.pending.add(task)
        task.add_done_callback(self.pending.discard)
        return False

    async def try_submit(self, symbol, quote, at):
        try:
            result = await self.supervisor.open_signal(
                symbol, {"venue": self.venue, "bid": quote.bid, "ask": quote.ask},
                observed_at=at)
            LOG.warning("Binance OPOCO accepted, NOT executed: %s", result["order_list_client_id"])
        except ExecutionDisabled as exc:
            LOG.info("Entry refused: %s", exc)
        except (VenueFailure, UnknownOrderState) as exc:
            self.last_error = str(exc)
            LOG.error("Trading suspended / unknown order: %s", exc)
        except Exception:
            self.last_error = "Unexpected execution error; see internal logs"
            self.supervisor.pause("Unexpected exception in execution task")
            LOG.exception("Unexpected error placing OPOCO")

    def process(self, payload, now=None):
        sym = payload.get("s")
        if sym not in SYMBOLS:
            return
        event = payload.get("e")
        if event == "aggTrade":
            self.on_trade(self.venue, sym, payload.get("p"), payload.get("q"),
                          not payload.get("m", True), now=now)
        elif all(x in payload for x in ("b", "a", "B", "A")):
            self.on_book(self.venue, sym, payload["b"], payload["a"],
                         payload["B"], payload["A"], now=now)


async def market_feed(engine, prefix):
    streams = "/".join(
        f"{symbol.lower()}@{name}" for symbol in SYMBOLS
        for name in ("bookTicker", "aggTrade"))
    url = prefix + streams
    backoff = 2
    while True:
        try:
            timeout = ClientTimeout(total=None, sock_connect=20, sock_read=None)
            async with ClientSession(timeout=timeout) as session:
                async with session.ws_connect(url, heartbeat=25,
                                              receive_timeout=75) as ws:
                    engine.feed[engine.venue] = {
                        "connected": True, "last_message": None, "error": ""}
                    backoff = 2
                    LOG.info("Binance market stream connected: %s", engine.venue)
                    async for message in ws:
                        if message.type == WSMsgType.TEXT:
                            try:
                                obj = json.loads(message.data)
                                engine.process(obj.get("data", obj))
                                engine.feed[engine.venue]["last_message"] = time.time()
                            except (ValueError, TypeError, AttributeError) as exc:
                                LOG.warning("Discarded malformed market frame: %s", exc)
                        elif message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine.feed.setdefault(engine.venue, {})["error"] = str(exc)[:120]
            LOG.warning("Market connection unavailable: %s", exc)
        finally:
            engine.feed.setdefault(engine.venue, {})["connected"] = False
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


async def reconciliation(engine):
    while True:
        await asyncio.sleep(5)
        if not engine.supervisor.ready():
            continue
        try:
            results = await engine.supervisor.reconcile()
            for result in results:
                LOG.info("Reconciled %s %s", result["id"], result["status"])
        except asyncio.CancelledError:
            raise
        except Exception:
            LOG.exception("Unexpected reconciliation failure")
            engine.supervisor.pause("Reconciliation loop crashed")


def status_dict(engine, mode):
    feed = engine.feed.get(engine.venue, {})
    market_fresh = bool(feed.get("connected") and feed.get("last_message")
                        and time.time() - feed["last_message"] <= 10)
    summary = engine.supervisor.status()
    realized = sum(float(x["pnl_usdt"]) for x in summary["orders"]
                   if x["status"] == "CLOSED" and x["pnl_usdt"] is not None)
    return {"service": "S-Scalp Binance", "execution_environment": mode,
            "bot_mode": summary["mode"], "halted": summary["halted"],
            "halt_reason": summary["reason"], "feed": engine.venue,
            "market_fresh": market_fresh, "market_error": feed.get("error",""),
            "last_market_message": feed.get("last_message"),
            "signals_since_boot": engine.signal_events,
            "realized_usdt": round(realized, 6),
            "trades": summary["orders"], "updated_at": time.time(),
            "last_error": engine.last_error,
            "quote_age_limit_seconds": engine.supervisor.limits.max_quote_age_sec}


async def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    mode, exchange_mode, venue, stream, armed, ack, platform = config()
    client = BinanceSpotClient(exchange_mode, os.environ.get("BINANCE_API_KEY",""),
                               os.environ.get("BINANCE_API_SECRET",""))
    supervisor = Supervisor(client,
                            os.environ.get("SCALP_BINANCE_JOURNAL",
                                           "/data/binance_orders.sqlite3"),
                            live=armed, acknowledged=ack, platform_verified=platform)
    engine = BinanceSignals(
        os.environ.get("SCALP_BINANCE_SIGNALS",
                       "/data/binance_signals.sqlite3"),
        supervisor, venue)
    if supervisor.ready():
        LOG.warning("TRADE MODE ARMED, strict guardrails active: %s", mode)
    else:
        LOG.info("OBSERVE_ONLY. Real order submission disabled.")

    app = web.Application(client_max_size=2048)
    app["engine"] = engine

    async def health(req):
        f = engine.feed.get(venue,{})
        fresh = bool(f.get("connected") and f.get("last_message")
                     and time.time() - f["last_message"] <= 30)
        return web.json_response(
            {"service":"S-Scalp Binance", "mode": "ARMED" if supervisor.ready() else "OBSERVE_ONLY",
             "market_feed": "online" if fresh else "waiting", "status":"ok"})

    async def index(req):
        if not paper_authorized(req):
            raise web.HTTPFound("/login")
        return response_headers(web.Response(
            text=Path(__file__).with_name("binance_dashboard.html").read_text(encoding="utf-8"),
            content_type="text/html"))

    async def status(req):
        if not paper_authorized(req):
            raise web.HTTPUnauthorized(text="Login required")
        return response_headers(web.json_response(status_dict(engine,mode)))

    app.router.add_get("/", index)
    app.router.add_route("*", "/login", paper_login)
    app.router.add_post("/logout", paper_logout)
    app.router.add_get("/api/status", status)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT","8080"))).start()
    tasks = [asyncio.create_task(market_feed(engine,stream)),
             asyncio.create_task(reconciliation(engine))]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        for task in list(engine.pending):
            task.cancel()
        await asyncio.gather(*tasks,*list(engine.pending),return_exceptions=True)
        await runner.cleanup()
        supervisor.db.close()
        engine.db.close()


if __name__ == "__main__":
    asyncio.run(main())
