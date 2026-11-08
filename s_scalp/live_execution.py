"""Guarded Bybit Kazakhstan spot order execution.

This is an OPTIONAL alpha module. It does not execute real orders unless
EXPLICIT LIVE mode, credentials, and a signed operational acknowledgement
are all provided by the operator. Never enable before exchange verification.

Acknowledgement != fill. Any uncertain request PAUSES until manual review.
"""
import asyncio
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from urllib.parse import urlencode

import aiohttp

KZ_API = "https://api.bybit.kz"
TEST_API = "https://api-testnet.bybit.com"
ALLOWED = frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT"})
RECV_WINDOW = "5000"


class ExecutionDisabled(RuntimeError):
    pass


class VenueError(RuntimeError):
    pass


class AmbiguousOrder(RuntimeError):
    pass


def dec(value):
    return Decimal(str(value))


def quantize_step(number, step, rounding=ROUND_DOWN):
    number, step = dec(number), dec(step)
    if step <= 0:
        raise ValueError("invalid price/quantity increment")
    return (number / step).to_integral_value(rounding=rounding) * step


def plain(value):
    return format(value, "f")


def sign_bybit(timestamp, key, secret, payload, recv_window=RECV_WINDOW):
    raw = f"{timestamp}{key}{recv_window}{payload}"
    return hmac.new(secret.encode(), raw.encode(), hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class Guardrails:
    # Fixed low limits until live accounting and venue-specific verification
    max_order_usdt: Decimal = Decimal("25")
    max_daily_submitted_usdt: Decimal = Decimal("25")
    max_total_submitted_usdt: Decimal = Decimal("25")
    max_spread_pct: Decimal = Decimal("0.08")
    max_quote_age_seconds: float = 2.5
    buy_price_ceiling_pct: Decimal = Decimal("0.08")
    take_profit_pct: Decimal = Decimal("0.70")
    stop_loss_pct: Decimal = Decimal("0.50")


class BybitSpotClient:
    def __init__(self, api_key="", api_secret="", env="kz", session=None):
        if env not in ("kz", "testnet"):
            raise ValueError("Only Kazakhstan and official testnet endpoints permitted")
        self.base = KZ_API if env == "kz" else TEST_API
        self.key = api_key
        self.secret = api_secret
        self.session = session

    async def request(self, method, path, params=None, *, private=False):
        if not path.startswith("/v5/"):
            raise ValueError("Only V5 endpoints supported")
        params = params or {}
        method = method.upper()
        if method not in ("GET", "POST"):
            raise ValueError("method not supported")
        headers = {"Content-Type": "application/json"}
        query = urlencode(sorted((str(k), str(v)) for k, v in params.items())) if method == "GET" else ""
        body = json.dumps(params, separators=(",", ":"), ensure_ascii=False) if method == "POST" else ""
        if private:
            if not self.key or not self.secret:
                raise ExecutionDisabled("No Bybit API credentials configured")
            stamp = str(int(time.time() * 1000))
            headers.update({
                "X-BAPI-API-KEY": self.key,
                "X-BAPI-TIMESTAMP": stamp,
                "X-BAPI-RECV-WINDOW": RECV_WINDOW,
                "X-BAPI-SIGN": sign_bybit(stamp, self.key, self.secret,
                                         query if method == "GET" else body),
            })
        url = self.base + path + (("?" + query) if query else "")
        own = self.session is None
        session = self.session or aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8))
        try:
            try:
                async with session.request(method, url, data=body if method == "POST" else None,
                                           headers=headers) as resp:
                    # Do not include credentials, signed headers, or sensitive
                    # request body in errors.
                    if resp.status >= 400:
                        raise VenueError(f"Bybit HTTP {resp.status} on {path}")
                    payload = await resp.json()
            except (aiohttp.ClientError, TimeoutError) as exc:
                # A POST timeout is ambiguous: exchange may have accepted it.
                if method == "POST" and private:
                    raise AmbiguousOrder("Order result unknown: reconcile manually by client ID") from exc
                raise VenueError("Bybit network request failed") from exc
        finally:
            if own:
                await session.close()
        if not isinstance(payload, dict) or payload.get("retCode") != 0:
            code = payload.get("retCode", "invalid response") if isinstance(payload, dict) else "invalid response"
            raise VenueError(f"Bybit {path} retCode={code}")
        return payload.get("result", {})

    async def instrument(self, symbol):
        data = await self.request("GET", "/v5/market/instruments-info",
                                  {"category": "spot", "symbol": symbol})
        records = data.get("list") or []
        if len(records) != 1 or records[0].get("status") != "Trading":
            raise VenueError("Symbol missing/not tradeable on selected venue")
        r = records[0]
        if r.get("symbol") != symbol or r.get("quoteCoin") != "USDT":
            raise VenueError("Symbol/quote currency mismatch")
        return r

    async def ticker(self, symbol):
        data = await self.request("GET", "/v5/market/tickers",
                                  {"category": "spot", "symbol": symbol})
        rows = data.get("list") or []
        if len(rows) != 1 or rows[0].get("symbol") != symbol:
            raise VenueError("Missing live ticker")
        return rows[0]

    async def wallet(self):
        data = await self.request("GET", "/v5/account/wallet-balance",
                                  {"accountType": "UNIFIED"}, private=True)
        rows = data.get("list") or []
        if not rows:
            raise VenueError("Unified account balance unavailable")
        for asset in rows[0].get("coin", []):
            if asset.get("coin") == "USDT":
                return dec(asset.get("walletBalance", "0"))
        return Decimal("0")

    async def submit_order(self, payload):
        return await self.request("POST", "/v5/order/create", payload, private=True)

    async def query_order(self, symbol, link_id):
        params = {"category": "spot", "symbol": symbol, "orderLinkId": link_id}
        for path in ("/v5/order/realtime", "/v5/order/history"):
            result = await self.request("GET", path, params, private=True)
            found = [x for x in result.get("list", []) if x.get("orderLinkId") == link_id]
            if found:
                return found[0]
        return None


class GuardedExecution:
    """One unresolved order at a time, tight capital budget, fail closed.

    The order includes exchange-native Spot Limit TakeProfit/StopLoss params.
    Those native protective orders MUST be manually validated on the chosen
    account type before any live deployment; API acknowledgment is NOT proof
    that either protection actually exists. Acknowledged orders remain blocked
    until explicitly reconciled/closed.
    """
    def __init__(self, client, journal_path, guardrails=None, live_enabled=False,
                 acknowledged_risk=False):
        self.client = client
        self._attempt_lock = asyncio.Lock()
        self.risk = guardrails or Guardrails()
        self.live_enabled = bool(live_enabled)
        self.acknowledged_risk = bool(acknowledged_risk)
        self.db = sqlite3.connect(journal_path)
        self.db.execute("""CREATE TABLE IF NOT EXISTS attempts (
            link_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            utc_day TEXT NOT NULL,
            usdt TEXT NOT NULL,
            state TEXT NOT NULL,
            exchange_order_id TEXT,
            created_at INTEGER NOT NULL)""")
        self.db.commit()

    def ready(self):
        return (self.live_enabled and self.acknowledged_risk and
                bool(self.client.key) and bool(self.client.secret))

    def status(self):
        attempts = self.db.execute(
            "SELECT link_id,symbol,utc_day,usdt,state,exchange_order_id,created_at "
            "FROM attempts ORDER BY created_at DESC LIMIT 20").fetchall()
        return {
            "mode": "LIVE_ARMED" if self.ready() else "DISABLED",
            "venue": self.client.base, "guardrails": {
                "max_order_usdt": plain(self.risk.max_order_usdt),
                "max_daily_submitted_usdt": plain(self.risk.max_daily_submitted_usdt),
                "max_total_submitted_usdt": plain(self.risk.max_total_submitted_usdt),
            },
            "attempts": [dict(zip(("link_id", "symbol", "utc_day", "usdt",
                                   "state", "exchange_order_id", "created_at"), x))
                         for x in attempts],
        }

    async def preview_or_submit(self, symbol, quote, *, observed_at, now=None):
        async with self._attempt_lock:
            return await self._preview_or_submit_locked(symbol, quote, observed_at=observed_at, now=now)

    async def _preview_or_submit_locked(self, symbol, quote, *, observed_at, now=None):
        now = time.time() if now is None else now
        if not self.ready():
            raise ExecutionDisabled("Real order trading is disabled")
        if symbol not in ALLOWED:
            raise ExecutionDisabled("Unapproved trading symbol")
        if not (0 <= now - observed_at <= self.risk.max_quote_age_seconds):
            raise ExecutionDisabled("Stale or future quote")
        bid, ask = dec(quote["bid"]), dec(quote["ask"])
        if not (bid > 0 and ask > bid and (ask / bid - 1) * 100 <= self.risk.max_spread_pct):
            raise ExecutionDisabled("Bad quote / spread too wide")
        # Only the Kazakhstan service can execute a Kazakhstan order;
        # mixed venue quotes or Binance Global signals are never accepted.
        expected_venue = "bybit_kz" if self.client.base == KZ_API else (
            "bybit_testnet" if self.client.base == TEST_API else None)
        if quote.get("venue") != expected_venue or expected_venue is None:
            raise ExecutionDisabled("Venue mismatch / unsupported live market")
        # Fresh *independent* Bybit Kazakhstan REST ticker must agree with stream.
        venue_quote = await self.client.ticker(symbol)
        venue_ask = dec(venue_quote["ask1Price"])
        venue_bid = dec(venue_quote["bid1Price"])
        if venue_ask <= venue_bid or venue_bid <= 0:
            raise ExecutionDisabled("Invalid venue best bid/ask")
        if abs(venue_ask / ask - 1) * 100 > self.risk.buy_price_ceiling_pct:
            raise ExecutionDisabled("Market moved away from observed signal")
        limits = await self.client.instrument(symbol)
        lot = limits["lotSizeFilter"]
        tick = dec(limits["priceFilter"]["tickSize"])
        step = dec(lot["basePrecision"])
        notional = self.risk.max_order_usdt
        limit_price = quantize_step(venue_ask * (1 + self.risk.buy_price_ceiling_pct / 100),
                                    tick, ROUND_DOWN)
        qty = quantize_step(notional / limit_price, step, ROUND_DOWN)
        if qty <= 0 or qty * limit_price < dec(lot["minOrderAmt"]):
            raise ExecutionDisabled("Order below exchange minimum after rounding")
        if qty > dec(lot["maxLimitOrderQty"]):
            raise ExecutionDisabled("Order above exchange limit")
        total_usdt = qty * limit_price
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        # No silent auto-retry after network uncertainty, partial fill, or restart.
        rows = self.db.execute("SELECT utc_day,usdt,state FROM attempts").fetchall()
        if rows:
            raise ExecutionDisabled("Previous order exists; reconciliation required")
        if total_usdt > self.risk.max_total_submitted_usdt:
            raise ExecutionDisabled("Lifetime live spending cap")
        if sum(dec(r[1]) for r in rows if r[0] == day) + total_usdt > self.risk.max_daily_submitted_usdt:
            raise ExecutionDisabled("Daily spending cap")
        balance = await self.client.wallet()
        if balance < total_usdt * Decimal("1.005"):
            raise ExecutionDisabled("Insufficient funds to reserve estimated fees")
        take_profit = quantize_step(limit_price * (1 + self.risk.take_profit_pct / 100),
                                    tick, ROUND_UP)
        stop_loss = quantize_step(limit_price * (1 - self.risk.stop_loss_pct / 100),
                                  tick, ROUND_DOWN)
        if not (stop_loss < limit_price < take_profit):
            raise ExecutionDisabled("Exchange precision invalidates protective stops")
        link_id = "sscalp-" + secrets.token_hex(11)
        order = {
            "category": "spot", "symbol": symbol, "side": "Buy",
            "orderType": "Limit", "qty": plain(qty), "price": plain(limit_price),
            "timeInForce": "IOC", "isLeverage": 0, "orderFilter": "Order",
            "orderLinkId": link_id, "takeProfit": plain(take_profit),
            "stopLoss": plain(stop_loss), "tpOrderType": "Market",
            "slOrderType": "Market",
        }
        # COMMIT intention BEFORE sending anything on the wire.
        with self.db:
            self.db.execute(
                "INSERT INTO attempts(link_id,symbol,utc_day,usdt,state,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (link_id, symbol, day, plain(total_usdt), "UNKNOWN", int(now)),
            )
        try:
            accepted = await self.client.submit_order(order)
        except (VenueError, AmbiguousOrder):
            # State UNKNOWN: never resend automatically.
            raise
        order_id = accepted.get("orderId")
        if not order_id:
            raise AmbiguousOrder("Exchange did not return orderId")
        with self.db:
            self.db.execute("UPDATE attempts SET state='ACKNOWLEDGED', "
                            "exchange_order_id=? WHERE link_id=?",
                            (order_id, link_id))
        return {"state": "ACKNOWLEDGED_NOT_CONFIRMED_FILLED",
                "order_link_id": link_id, "order_id": order_id,
                "symbol": symbol, "quote_budget": plain(total_usdt),
                "take_profit": plain(take_profit), "stop_loss": plain(stop_loss)}

    async def reconcile(self, link_id):
        row = self.db.execute("SELECT symbol FROM attempts WHERE link_id=?", (link_id,)).fetchone()
        if not row:
            raise ValueError("Unknown order id")
        status = await self.client.query_order(row[0], link_id)
        if not status:
            return {"state": "UNKNOWN", "note": "Not visible in exchange order queries"}
        # Never remove the risk lock automatically. A filled BUY may remain on
        # the exchange; its TP/SL protection and eventual SELL must be verified.
        with self.db:
            self.db.execute("UPDATE attempts SET state=? WHERE link_id=?",
                            (status.get("orderStatus", "UNKNOWN"), link_id))
        return {"state": status.get("orderStatus"),
                "cum_exec_qty": status.get("cumExecQty"),
                "cum_exec_value": status.get("cumExecValue"),
                "avg_price": status.get("avgPrice"),
                "requires_position_protection_verification": True}
