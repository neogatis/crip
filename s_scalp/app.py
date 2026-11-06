"""S-Scalp: public spot WebSocket scanner + conservative, paper-only execution.

There is deliberately NO code path to submit an exchange order or use API keys.
"""
import asyncio
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import secrets
import sqlite3
import time
from urllib.parse import quote

from aiohttp import ClientSession, ClientTimeout, WSMsgType, web

LOG = logging.getLogger("s_scalp")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
VENUES = ("binance_public", "bybit_kz")
BINANCE_WS = "wss://data-stream.binance.vision:443/stream?streams="
BYBIT_WS = "wss://stream.bybit.kz/v5/public/spot"
FEE_RATE = 0.001  # default illustration, taker per side; verify effective actual tier
SLIPPAGE_RATE = 0.0005  # assumed per side, not a measured live execution cost
QUOTE_BUDGET = 50.0
STARTING_CASH = 500.0
MAX_POSITIONS = 2
DAILY_LOSS_LIMIT = 10.0
MAX_SPREAD = 0.0008
MOMENTUM_THRESHOLD = 0.003
MIN_TRADES = 12
MIN_BUY_RATIO = 0.60
WINDOW_SECONDS = 20.0
MIN_WARMUP_SECONDS = 8.0
QUOTE_TTL = 2.5
COOLDOWN_SECONDS = 120.0
TAKE_PROFIT_NET = 0.006
STOP_LOSS_NET = -0.006
MAX_HOLD_SECONDS = 120.0
MAX_EVENTS = 100


def utc_day(now=None):
    return datetime.fromtimestamp(now or time.time(), tz=timezone.utc).date().isoformat()


def fee_adjusted_sale(qty, bid, fee=FEE_RATE, slippage=SLIPPAGE_RATE):
    return qty * bid * (1.0 - slippage) * (1.0 - fee)


@dataclass
class Quote:
    bid: float
    ask: float
    bid_qty: float
    ask_qty: float
    received_at: float


@dataclass
class Position:
    venue: str
    symbol: str
    qty: float
    notional: float
    entry_price: float
    entry_fee: float
    opened_at: float
    entry_id: int


class PaperEngine:
    """No leverage, no shorting, no API keys, no orders to exchanges."""

    def __init__(self, db_path, starting_cash=STARTING_CASH, fee=FEE_RATE,
                 slippage=SLIPPAGE_RATE, budget=QUOTE_BUDGET):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.db_path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""CREATE TABLE IF NOT EXISTS account (
            id INTEGER PRIMARY KEY CHECK (id = 1), cash REAL NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS positions (
            venue TEXT, symbol TEXT, qty REAL, notional REAL, entry_price REAL,
            entry_fee REAL, opened_at REAL, entry_id INTEGER,
            PRIMARY KEY (venue, symbol))""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, venue TEXT, symbol TEXT,
            opened_at REAL, closed_at REAL, entry_price REAL, exit_price REAL,
            qty REAL, entry_fee REAL, exit_fee REAL, pnl REAL, reason TEXT)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL, type TEXT,
            venue TEXT, symbol TEXT, detail TEXT)""")
        self.db.execute("INSERT OR IGNORE INTO account (id,cash) VALUES (1,?)",
                        (starting_cash,))
        self.db.commit()
        self.cash = float(self.db.execute("SELECT cash FROM account WHERE id=1").fetchone()[0])
        self.positions = {
            (r["venue"], r["symbol"]): Position(*tuple(r))
            for r in self.db.execute("SELECT venue,symbol,qty,notional,entry_price,entry_fee,opened_at,entry_id FROM positions")
        }
        self.quotes = {}
        self.tape = defaultdict(deque)
        self.cooldowns = {}
        self.signal_count = 0
        self.feed = {venue: {"connected": False, "last_message": None, "error": ""}
                     for venue in VENUES}
        self.fee = fee
        self.slippage = slippage
        self.budget = budget
        self._next_entry_id = int(time.time() * 1000)

    def event(self, kind, venue, symbol, detail, now=None):
        self.db.execute("INSERT INTO events(at,type,venue,symbol,detail) VALUES (?,?,?,?,?)",
                        (now if now is not None else time.time(), kind, venue, symbol, detail))
        self.db.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT ?)",
                        (MAX_EVENTS,))
        self.db.commit()

    def daily_pnl(self, now=None):
        day = utc_day(now)
        start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()
        value = self.db.execute(
            "SELECT COALESCE(SUM(pnl),0) FROM trades WHERE closed_at>=?", (start,)
        ).fetchone()[0]
        return float(value)

    def enter(self, venue, symbol, now=None):
        now = time.time() if now is None else now
        key = (venue, symbol)
        quote = self.quotes.get(key)
        if quote is None or now - quote.received_at > QUOTE_TTL:
            return False
        if (quote.bid <= 0 or quote.ask <= quote.bid or
                (quote.ask - quote.bid) / quote.ask > MAX_SPREAD):
            return False
        if quote.ask_qty * quote.ask < self.budget:
            return False
        if (key in self.positions or len(self.positions) >= MAX_POSITIONS or
                self.cash < self.budget * (1 + self.fee) or
                self.daily_pnl(now) <= -DAILY_LOSS_LIMIT or
                now < self.cooldowns.get(key, 0)):
            return False
        # One live hypothesis for a coin across both feeds: no double exposure.
        if any(p.symbol == symbol for p in self.positions.values()):
            return False
        price = quote.ask * (1.0 + self.slippage)
        qty = self.budget / price
        entry_fee = self.budget * self.fee
        self._next_entry_id += 1
        pos = Position(venue, symbol, qty, self.budget, price, entry_fee, now,
                       self._next_entry_id)
        self.cash -= self.budget + entry_fee
        self.positions[key] = pos
        with self.db:
            self.db.execute("UPDATE account SET cash=? WHERE id=1", (self.cash,))
            self.db.execute(
                "INSERT INTO positions VALUES (?,?,?,?,?,?,?,?)", tuple(asdict(pos).values())
            )
        self.signal_count += 1
        self.event("OPEN", venue, symbol, "Paper BUY @ %.8f; notional %.2f USDT" % (price, self.budget), now)
        return True

    def exit(self, venue, symbol, reason, now=None):
        now = time.time() if now is None else now
        key = (venue, symbol)
        pos = self.positions.get(key)
        quote = self.quotes.get(key)
        if not pos or not quote or now - quote.received_at > QUOTE_TTL or quote.bid <= 0:
            return False
        if quote.bid_qty * quote.bid < pos.qty * quote.bid:
            return False
        price = quote.bid * (1 - self.slippage)
        gross = pos.qty * price
        exit_fee = gross * self.fee
        pnl = gross - exit_fee - pos.notional - pos.entry_fee
        self.cash += gross - exit_fee
        with self.db:
            self.db.execute("UPDATE account SET cash=? WHERE id=1", (self.cash,))
            self.db.execute("DELETE FROM positions WHERE venue=? AND symbol=?", key)
            self.db.execute(
                """INSERT INTO trades(venue,symbol,opened_at,closed_at,entry_price,
                exit_price,qty,entry_fee,exit_fee,pnl,reason)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (venue, symbol, pos.opened_at, now, pos.entry_price, price,
                 pos.qty, pos.entry_fee, exit_fee, pnl, reason)
            )
        del self.positions[key]
        self.cooldowns[key] = now + COOLDOWN_SECONDS
        self.event("CLOSE", venue, symbol, "%s; P&L %.4f USDT" % (reason, pnl), now)
        return True

    def mark(self, venue, symbol, now=None):
        now = time.time() if now is None else now
        key = (venue, symbol)
        pos = self.positions.get(key)
        quote = self.quotes.get(key)
        if not pos or not quote or now - quote.received_at > QUOTE_TTL:
            return
        net = (fee_adjusted_sale(pos.qty, quote.bid, self.fee, self.slippage)
               - pos.notional - pos.entry_fee)
        denominator = pos.notional + pos.entry_fee
        if net / denominator >= TAKE_PROFIT_NET:
            self.exit(venue, symbol, "TAKE_PROFIT", now)
        elif net / denominator <= STOP_LOSS_NET:
            self.exit(venue, symbol, "STOP_LOSS", now)
        elif now - pos.opened_at >= MAX_HOLD_SECONDS:
            self.exit(venue, symbol, "TIMEOUT", now)

    def on_book(self, venue, symbol, bid, ask, bid_qty, ask_qty, now=None):
        now = time.time() if now is None else now
        if symbol not in SYMBOLS:
            return
        try:
            values = tuple(float(x) for x in (bid, ask, bid_qty, ask_qty))
        except (ValueError, TypeError):
            return
        if not (0 < values[0] < values[1] and values[2] >= 0 and values[3] >= 0):
            return
        self.quotes[(venue, symbol)] = Quote(*values, now)
        self.mark(venue, symbol, now)

    def on_trade(self, venue, symbol, price, qty, taker_buy, now=None):
        now = time.time() if now is None else now
        if symbol not in SYMBOLS:
            return
        try:
            price, qty = float(price), float(qty)
        except (TypeError, ValueError):
            return
        if price <= 0 or qty <= 0:
            return
        key = (venue, symbol)
        history = self.tape[key]
        history.append((now, price, qty * price, bool(taker_buy)))
        while history and history[0][0] < now - WINDOW_SECONDS:
            history.popleft()
        self.mark(venue, symbol, now)
        if len(history) < MIN_TRADES or now - history[0][0] < MIN_WARMUP_SECONDS:
            return
        # Evaluate only after at least eight real seconds of trade observations.
        first_price = history[0][1]
        move = price / first_price - 1.0
        total_quote = sum(x[2] for x in history)
        buy_quote = sum(x[2] for x in history if x[3])
        if (move >= MOMENTUM_THRESHOLD and total_quote > 0 and
                buy_quote / total_quote >= MIN_BUY_RATIO):
            self.enter(venue, symbol, now)

    def snapshot(self):
        now = time.time()
        open_positions = []
        unrealized = 0.0
        for key, pos in self.positions.items():
            quote = self.quotes.get(key)
            fresh = quote is not None and now - quote.received_at <= QUOTE_TTL
            net = (fee_adjusted_sale(pos.qty, quote.bid, self.fee, self.slippage)
                   - pos.notional - pos.entry_fee) if fresh else None
            if net is not None:
                unrealized += net
            open_positions.append({
                **asdict(pos), "paper_pnl": net, "quote_fresh": fresh
            })
        trades = [dict(row) for row in self.db.execute(
            "SELECT * FROM trades ORDER BY id DESC LIMIT 40"
        )]
        events = [dict(row) for row in self.db.execute(
            "SELECT at,type,venue,symbol,detail FROM events ORDER BY id DESC LIMIT 25"
        )]
        prices = []
        for (venue, symbol), q in self.quotes.items():
            prices.append({"venue": venue, "symbol": symbol, "bid": q.bid, "ask": q.ask,
                           "spread_pct": (q.ask / q.bid - 1) * 100,
                           "age_seconds": round(now - q.received_at, 2)})
        realized = sum(float(t["pnl"]) for t in trades)
        total_realized = float(self.db.execute(
            "SELECT COALESCE(SUM(pnl),0) FROM trades").fetchone()[0])
        n = int(self.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0])
        wins = int(self.db.execute(
            "SELECT COUNT(*) FROM trades WHERE pnl > 0").fetchone()[0])
        return {"mode": "PAPER_ONLY", "execution_enabled": False,
                "updated_at": now, "starting_cash": STARTING_CASH,
                "cash": self.cash, "equity_estimate": self.cash
                + sum(p.notional + p.entry_fee + (p.get("paper_pnl") or 0)
                      for p in open_positions),
                "unrealized_estimate": unrealized, "realized_total": total_realized,
                "daily_realized": self.daily_pnl(now),
                "closed_trades": n, "wins": wins,
                "win_rate": round(100 * wins / n, 2) if n else 0,
                "signal_count_since_start": self.signal_count,
                "fee_bps_side": self.fee * 10000,
                "slippage_bps_side_assumed": self.slippage * 10000,
                "positions": open_positions, "trades": trades, "events": events,
                "quotes": sorted(prices, key=lambda x: (x["symbol"], x["venue"])),
                "feeds": self.feed}


def handle_binance(engine, data):
    sym = data.get("s")
    if sym not in SYMBOLS:
        return
    if data.get("e") == "aggTrade":
        engine.on_trade("binance_public", sym, data.get("p"), data.get("q"),
                        not data.get("m", True))
    elif all(k in data for k in ("b", "a", "B", "A")):
        engine.on_book("binance_public", sym, data["b"], data["a"], data["B"], data["A"])


def handle_bybit(engine, payload):
    topic = str(payload.get("topic", ""))
    if topic.startswith("orderbook.1."):
        sym = topic.rsplit(".", 1)[-1]
        data = payload.get("data", {})
        bids, asks = data.get("b", []), data.get("a", [])
        if bids and asks:
            engine.on_book("bybit_kz", sym, bids[0][0], asks[0][0],
                           bids[0][1], asks[0][1])
    elif topic.startswith("publicTrade."):
        for data in payload.get("data", []):
            engine.on_trade("bybit_kz", data.get("s"), data.get("p"),
                            data.get("v"), data.get("S") == "Buy")


async def websocket_feed(engine, venue):
    if venue == "binance_public":
        streams = "/".join(
            "%s@%s" % (symbol.lower(), event)
            for symbol in SYMBOLS for event in ("bookTicker", "aggTrade")
        )
        url = BINANCE_WS + streams
    else:
        url = BYBIT_WS
    backoff = 2
    while True:
        ping_task = None
        try:
            timeout = ClientTimeout(total=None, sock_connect=20, sock_read=None)
            async with ClientSession(timeout=timeout) as session:
                async with session.ws_connect(url, heartbeat=20, receive_timeout=70,
                                              max_msg_size=1048576) as ws:
                    engine.feed[venue].update(connected=True, error="")
                    LOG.info("Feed connected: %s", venue)
                    backoff = 2
                    if venue == "bybit_kz":
                        topics = [f"orderbook.1.{s}" for s in SYMBOLS] + [
                            f"publicTrade.{s}" for s in SYMBOLS]
                        await ws.send_json({"op": "subscribe", "args": topics})
                        async def bybit_ping():
                            while True:
                                await asyncio.sleep(18)
                                await ws.send_json({"op": "ping"})
                        ping_task = asyncio.create_task(bybit_ping())
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                if venue == "binance_public":
                                    data = payload.get("data", payload)
                                    handle_binance(engine, data)
                                else:
                                    handle_bybit(engine, payload)
                                engine.feed[venue]["last_message"] = time.time()
                            except (ValueError, TypeError, KeyError, AttributeError) as exc:
                                LOG.warning("Malformed %s frame: %s", venue, exc)
                        elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSED, WSMsgType.CLOSE):
                            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine.feed[venue]["error"] = str(exc)[:180]
            LOG.warning("%s disconnected: %s", venue, exc)
        finally:
            engine.feed[venue]["connected"] = False
            if ping_task:
                ping_task.cancel()
                await asyncio.gather(ping_task, return_exceptions=True)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


def headers(resp):
    resp.headers.update({
        "Cache-Control": "no-store, max-age=0",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Referrer-Policy": "no-referrer",
        "Content-Security-Policy": "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                                   "script-src 'self' 'unsafe-inline'; connect-src 'self'; "
                                   "img-src 'self'; object-src 'none'; frame-ancestors 'none'; "
                                   "form-action 'self'; base-uri 'none'"
    })
    return resp


def auth_cookie(password, expiry):
    signature = hmac.new(password.encode(), str(expiry).encode(), hashlib.sha256).hexdigest()
    return f"{expiry}:{signature}"


def authorized(request):
    password = os.environ.get("SCALP_DASHBOARD_PASSWORD", "")
    if not password:
        return False
    cookie = request.cookies.get("sscalp_session", "")
    try:
        expiry, signature = cookie.split(":", 1)
        expiry = int(expiry)
    except (ValueError, TypeError):
        return False
    expected = auth_cookie(password, expiry).split(":", 1)[1]
    return time.time() < expiry and hmac.compare_digest(signature, expected)


def protected(request):
    if not os.environ.get("SCALP_DASHBOARD_PASSWORD"):
        raise web.HTTPServiceUnavailable(text="Set SCALP_DASHBOARD_PASSWORD")
    if not authorized(request):
        raise web.HTTPUnauthorized(text="Login required")


async def index(request):
    if not authorized(request):
        raise web.HTTPFound("/login")
    html = Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")
    return headers(web.Response(text=html, content_type="text/html"))


async def login(request):
    if not os.environ.get("SCALP_DASHBOARD_PASSWORD"):
        raise web.HTTPServiceUnavailable(text="Set SCALP_DASHBOARD_PASSWORD")
    if request.method == "POST":
        data = await request.post()
        value = str(data.get("password", ""))[:256]
        if not hmac.compare_digest(value, os.environ["SCALP_DASHBOARD_PASSWORD"]):
            await asyncio.sleep(0.35)
            return headers(web.Response(status=401, text="Wrong password. <a href='/login'>Try again</a>",
                                        content_type="text/html"))
        expires = int(time.time() + 86400)
        response = web.HTTPFound("/")
        response.set_cookie("sscalp_session", auth_cookie(value, expires), max_age=86400,
                            secure=os.environ.get("COOKIE_SECURE", "1") != "0",
                            httponly=True, samesite="Strict", path="/")
        raise response
    return headers(web.Response(text="""<!doctype html><html lang="ru"><meta name="viewport" content="width=device-width,initial-scale=1"><meta charset="utf-8"><title>S-Scalp | Вход</title><style>body{background:#07111c;color:#f4f6ff;display:grid;place-items:center;min-height:90vh;font:16px system-ui}main{padding:32px;background:#122034;border-radius:18px;width:min(360px,90vw)}input,button{width:100%;box-sizing:border-box;padding:14px;margin-top:16px;border-radius:9px;border:0}button{background:#55d5c0;color:#062022;font-weight:800}input{background:#0a1726;color:white}</style><main><small>PRIVATE RESEARCH · PAPER ONLY</small><h1>S / SCALP</h1><p>Симулятор, реальные котировки. Без реальных ордеров.</p><form method="post"><input name="password" type="password" placeholder="Пароль панели" autocomplete="current-password" required autofocus><button>Открыть</button></form></main></html>""", content_type="text/html"))


async def api(request):
    protected(request)
    return headers(web.json_response(request.app["engine"].snapshot()))


async def health(request):
    engine = request.app["engine"]
    return headers(web.json_response({
        "status": "ok" if any(x["connected"] and x["last_message"]
                              and time.time() - x["last_message"] < 30
                              for x in engine.feed.values()) else "waiting_for_market_data",
        "mode": "PAPER_ONLY", "execution_enabled": False
    }))


async def logout(request):
    resp = web.HTTPFound("/login")
    resp.del_cookie("sscalp_session", path="/")
    raise resp


async def start():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    engine = PaperEngine(os.environ.get("SCALP_DB_PATH", "./data/scalp.sqlite3"))
    app = web.Application(client_max_size=2048)
    app["engine"] = engine
    app.router.add_get("/", index)
    app.router.add_route("*", "/login", login)
    app.router.add_get("/api/status", api)
    app.router.add_get("/health", health)
    app.router.add_post("/logout", logout)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 8080)))
    await site.start()
    LOG.info("S-Scalp paper dashboard running (authentication %s)",
             "enabled" if os.environ.get("SCALP_DASHBOARD_PASSWORD") else "NOT CONFIGURED: fail closed")
    tasks = [asyncio.create_task(websocket_feed(engine, venue)) for venue in VENUES]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()
        engine.db.close()


if __name__ == "__main__":
    asyncio.run(start())
