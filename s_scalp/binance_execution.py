"""Binance Spot automatic execution core, default OFF.

Live orders are intentionally *not* enabled by the deployed paper dashboard.
For qualifying accounts, this adapter proposes an OPOCO order list: LIMIT BUY
followed on *full fill* by exchange-hosted take-profit + stop-loss SELL orders.

Important: Partial fills are NOT protected by the pending OCO. The supervisor
halts new entries and flags an incident rather than assuming protection.
"""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import sqlite3
import time
from urllib.parse import urlencode

import aiohttp

LIVE_API = "https://api.binance.com"  # Binance Global; NOT proof of Binance Kazakhstan compatibility
TESTNET_API = "https://testnet.binance.vision"
VENUES = {"live": LIVE_API, "testnet": TESTNET_API}
ALLOWED_SYMBOLS = frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT"})
TERMINAL = frozenset({"ALL_DONE", "REJECT", "REJECTED", "EXPIRED", "CANCELED"})


class ExecutionDisabled(RuntimeError):
    pass


class VenueFailure(RuntimeError):
    pass


class UnknownOrderState(RuntimeError):
    pass


def D(x):
    return Decimal(str(x))


def fmt(x):
    return format(D(x), "f")


def step_round(amount, step, rounding=ROUND_DOWN):
    amount, step = D(amount), D(step)
    if step <= 0:
        raise ValueError("exchange increment must be positive")
    return (amount / step).to_integral_value(rounding=rounding) * step


def hmac_signature(secret, params):
    encoded = urlencode(params)
    return hmac.new(secret.encode("utf-8"), encoded.encode("utf-8"),
                    hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class Limits:
    max_order_usdt: Decimal = Decimal("25")
    max_daily_buy_usdt: Decimal = Decimal("100")
    max_daily_realized_loss_usdt: Decimal = Decimal("5")
    max_spread_pct: Decimal = Decimal("0.08")
    max_quote_age_sec: float = 2.5
    max_buy_price_jump_pct: Decimal = Decimal("0.10")
    take_profit_pct: Decimal = Decimal("0.90")
    stop_loss_pct: Decimal = Decimal("0.65")
    entry_ttl_sec: float = 45.0
    cooldown_sec: float = 120.0


class BinanceSpotClient:
    def __init__(self, mode="testnet", api_key="", api_secret="", session=None):
        if mode not in VENUES:
            raise ValueError("Binance private endpoints only support explicit testnet/live")
        self.mode, self.base = mode, VENUES[mode]
        self.key, self.secret, self.session = api_key, api_secret, session

    async def request(self, method, path, params=None, *, private=False):
        method = method.upper()
        if path not in {"/api/v3/time", "/api/v3/exchangeInfo", "/api/v3/ticker/bookTicker",
                        "/api/v3/account", "/api/v3/orderList/opoco",
                        "/api/v3/orderList", "/api/v3/order", "/api/v3/myTrades",
                        "/api/v3/account/commission"}:
            raise ValueError("endpoint not allowlisted")
        if method not in {"GET", "POST", "DELETE"}:
            raise ValueError("method not allowlisted")
        args = {k: fmt(v) if isinstance(v, Decimal) else str(v)
                for k, v in (params or {}).items()}
        if private:
            if not self.key or not self.secret:
                raise ExecutionDisabled("No exchange API credentials")
            args.update(timestamp=str(int(time.time() * 1000)), recvWindow="5000")
        ordered = list(sorted(args.items()))
        if private:
            ordered.append(("signature", hmac_signature(self.secret, ordered)))
        body = urlencode(ordered)
        headers = {"X-MBX-APIKEY": self.key} if private else {}
        if method != "GET":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        url = self.base + path
        if method == "GET":
            url += "?" + body if body else ""
        owns_session = self.session is None
        session = self.session or aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=9))
        try:
            try:
                async with session.request(method, url, data=body if method != "GET" else None,
                                           headers=headers) as response:
                    # Exchange may accept an order even if the response is lost.
                    if response.status >= 500 and method == "POST":
                        raise UnknownOrderState("HTTP 5xx: order state unknown")
                    if response.status >= 400:
                        raise VenueFailure(f"Binance HTTP {response.status} on {path}")
                    result = await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if method == "POST" and private:
                    raise UnknownOrderState("Network failure after order request; reconcile") from exc
                raise VenueFailure("Network unavailable") from exc
        finally:
            if owns_session:
                await session.close()
        if isinstance(result, dict) and "code" in result and D(result["code"]) < 0:
            # Do not include request, keys, or error messages in logs.
            raise VenueFailure(f"Binance API error {result['code']} at {path}")
        return result

    async def server_time(self):
        return await self.request("GET", "/api/v3/time")

    async def instrument(self, symbol):
        res = await self.request("GET", "/api/v3/exchangeInfo", {"symbol": symbol})
        entries = res.get("symbols", [])
        if len(entries) != 1:
            raise ExecutionDisabled("Symbol unavailable")
        asset = entries[0]
        if (asset.get("symbol") != symbol or asset.get("quoteAsset") != "USDT"
                or asset.get("status") != "TRADING"
                or asset.get("isSpotTradingAllowed") is False
                or not all(asset.get(k) is True
                           for k in ("opoAllowed", "otoAllowed", "ocoAllowed"))):
            raise ExecutionDisabled("Symbol not eligible for Spot OPOCO")
        return asset

    async def best_quote(self, symbol):
        return await self.request("GET", "/api/v3/ticker/bookTicker", {"symbol": symbol})

    async def account(self):
        return await self.request("GET", "/api/v3/account",
                                  {"omitZeroBalances": "true"}, private=True)

    async def commission(self, symbol):
        return await self.request("GET", "/api/v3/account/commission",
                                  {"symbol": symbol}, private=True)

    async def place_opoco(self, args):
        return await self.request("POST", "/api/v3/orderList/opoco", args, private=True)

    async def list_state(self, client_id):
        return await self.request("GET", "/api/v3/orderList",
                                  {"origClientOrderId": client_id}, private=True)

    async def order(self, symbol, order_id):
        return await self.request("GET", "/api/v3/order",
                                  {"symbol": symbol, "orderId": str(order_id)}, private=True)

    async def trades(self, symbol, order_id):
        return await self.request("GET", "/api/v3/myTrades",
                                  {"symbol": symbol, "orderId": str(order_id),
                                   "limit": "1000"}, private=True)

    async def cancel_list(self, symbol, list_id):
        return await self.request("DELETE", "/api/v3/orderList",
                                  {"symbol": symbol, "listClientOrderId": list_id},
                                  private=True)


def exchange_filters(asset):
    filters = {x["filterType"]: x for x in asset.get("filters", [])}
    if "PRICE_FILTER" not in filters or "LOT_SIZE" not in filters:
        raise ExecutionDisabled("Missing symbol filters")
    tick, step = D(filters["PRICE_FILTER"]["tickSize"]), D(filters["LOT_SIZE"]["stepSize"])
    minqty = D(filters["LOT_SIZE"]["minQty"])
    mincost = D(filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {})).get(
        "minNotional", "0"))
    if not (tick > 0 and step > 0 and minqty > 0 and mincost > 0):
        raise ExecutionDisabled("Incomplete symbol quantity/price limits")
    return tick, step, minqty, mincost


def build_opoco(symbol, asset, bid, ask, limits, *, client_id):
    """One LIMIT BUY and exchange-controlled net-received-quantity OCO exits."""
    bid, ask = D(bid), D(ask)
    if not all(asset.get(k) is True for k in ("opoAllowed", "otoAllowed", "ocoAllowed")):
        raise ExecutionDisabled("Instrument has no OPOCO support")
    if bid <= 0 or ask <= bid:
        raise ExecutionDisabled("Invalid market quote")
    tick, step, minqty, mincost = exchange_filters(asset)
    entry = step_round(ask * (1 + limits.max_buy_price_jump_pct / 100),
                       tick, ROUND_DOWN)
    quantity = step_round(limits.max_order_usdt / entry, step)
    notional = entry * quantity
    if quantity < minqty or notional < mincost:
        raise ExecutionDisabled("Budget below exchange spot minimum")
    if notional > limits.max_order_usdt:
        raise ExecutionDisabled("Notional limit exceeded")
    tp = step_round(entry * (1 + limits.take_profit_pct / 100), tick, ROUND_UP)
    sl = step_round(entry * (1 - limits.stop_loss_pct / 100), tick, ROUND_DOWN)
    if not (sl < bid < ask < tp and sl > 0):
        raise ExecutionDisabled("Stop/profit prices conflict with market")
    # OPOCO pendingQuantity is intentionally absent. Binance uses the net
    # received base asset, accounting for applicable commission/lot size.
    return ({
        "symbol": symbol,
        "listClientOrderId": client_id,
        "workingClientOrderId": client_id + "w",
        "pendingAboveClientOrderId": client_id + "a",
        "pendingBelowClientOrderId": client_id + "b",
        "workingSide": "BUY", "workingType": "LIMIT",
        "workingTimeInForce": "GTC", "workingPrice": fmt(entry),
        "workingQuantity": fmt(quantity),
        "pendingSide": "SELL",
        "pendingAboveType": "TAKE_PROFIT",
        "pendingAboveStopPrice": fmt(tp),
        "pendingBelowType": "STOP_LOSS",
        "pendingBelowStopPrice": fmt(sl),
        "newOrderRespType": "RESULT",
    }, notional)


def pnl_from_trades(buy_trades, sell_trades, base_asset):
    """Returns exact quote-P&L where all commissions are quote or base.

    Base BUY commissions are already reflected in the quantity OPO
    transfers to the pending SELL; quote fees add explicit costs.
    BNB or third-asset fees cannot be valued exactly without a historical
    conversion; return None to halt the next trade.
    """
    if not buy_trades or not sell_trades:
        return None
    base_in = sum((D(t["qty"]) for t in buy_trades), Decimal(0))
    base_out = sum((D(t["qty"]) for t in sell_trades), Decimal(0))
    base_buy_fee = sum((D(t["commission"]) for t in buy_trades
                        if t["commissionAsset"] == base_asset), Decimal(0))
    base_sell_fee = sum((D(t["commission"]) for t in sell_trades
                        if t["commissionAsset"] == base_asset), Decimal(0))
    # Verify asset quantity reconciliation within trade precision. Dust
    # due to LOT_SIZE rounding is not assumed to be zero-value.
    if abs((base_in - base_buy_fee) - (base_out + base_sell_fee)) > D("0.000000001"):
        return None
    for t in buy_trades + sell_trades:
        if t["commissionAsset"] not in {"USDT", base_asset}:
            return None
    spent = sum((D(t["quoteQty"]) for t in buy_trades), Decimal(0))
    received = sum((D(t["quoteQty"]) for t in sell_trades), Decimal(0))
    quoted_fees = sum((D(t["commission"]) for t in buy_trades + sell_trades
                       if t["commissionAsset"] == "USDT"), Decimal(0))
    return received - spent - quoted_fees


class Supervisor:
    """Durable one-position worker. Never assumes request ACK is a fill."""
    def __init__(self, client, db_path, limits=None, *, live=False,
                 acknowledged=False, platform_verified=False):
        self.client = client
        self.limits = limits or Limits()
        self.live = live
        self.acknowledged = acknowledged
        self.platform_verified = platform_verified
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""CREATE TABLE IF NOT EXISTS orders (
            client_id TEXT PRIMARY KEY, symbol TEXT NOT NULL,
            utc_day TEXT NOT NULL, started REAL NOT NULL,
            usdt TEXT NOT NULL, status TEXT NOT NULL,
            list_id TEXT, closed REAL, pnl_usdt TEXT,
            detail TEXT DEFAULT '')""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS control (
            id INTEGER PRIMARY KEY CHECK (id=1), halted INTEGER NOT NULL DEFAULT 0,
            reason TEXT NOT NULL DEFAULT '')""")
        self.db.execute("INSERT OR IGNORE INTO control(id,halted,reason) VALUES (1,0,'')")
        self.db.commit()
        self.lock = asyncio.Lock()

    def ready(self):
        return bool(self.live and self.acknowledged and self.platform_verified
                    and self.client.key and self.client.secret)

    def pause(self, reason):
        with self.db:
            self.db.execute("UPDATE control SET halted=1, reason=? WHERE id=1", (reason[:200],))

    def resume_after_review(self):
        # Must be an explicit human/operator action, never a network recovery.
        raise ExecutionDisabled("Manual review and a new verified run required")

    def status(self):
        halted, reason = self.db.execute(
            "SELECT halted,reason FROM control WHERE id=1").fetchone()
        rows = self.db.execute(
            "SELECT client_id,symbol,utc_day,started,usdt,status,"
            "list_id,closed,pnl_usdt,detail FROM orders ORDER BY started DESC LIMIT 30"
        ).fetchall()
        return {"mode": "ARMED" if self.ready() else "OBSERVE_ONLY",
                "halted": bool(halted), "reason": reason,
                "orders": [dict(x) for x in rows],
                "capital_limit_usdt": fmt(self.limits.max_order_usdt)}

    async def open_signal(self, symbol, quote, observed_at, now=None):
        async with self.lock:
            now = time.time() if now is None else now
            if not self.ready():
                raise ExecutionDisabled("Live order submission is disabled")
            halted = self.db.execute("SELECT halted FROM control WHERE id=1").fetchone()[0]
            if halted or os.environ.get("SCALP_KILL_SWITCH", "0") == "1":
                raise ExecutionDisabled("Operator emergency stop is active")
            if symbol not in ALLOWED_SYMBOLS:
                raise ExecutionDisabled("Symbol not allowlisted")
            if quote.get("venue") != ("binance_testnet" if self.client.mode == "testnet" else "binance_global"):
                raise ExecutionDisabled("Cross-venue signal refused")
            if not (0 <= now - observed_at <= self.limits.max_quote_age_sec):
                raise ExecutionDisabled("Market quote is stale/future")
            bid, ask = D(quote["bid"]), D(quote["ask"])
            if bid <= 0 or ask <= bid or (ask / bid - 1)*100 > self.limits.max_spread_pct:
                raise ExecutionDisabled("Spread invalid / too wide")

            today = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
            rows = self.db.execute("SELECT * FROM orders").fetchall()
            # An unresolved, partially filled, or fee-uncertain trade MUST
            # block all new buys, including after restart.
            if any(x["status"] != "CLOSED" and x["status"] != "NO_FILL" for x in rows):
                raise ExecutionDisabled("Unresolved order / position")
            total_today = sum((D(x["usdt"]) for x in rows if x["utc_day"] == today), D(0))
            if total_today + self.limits.max_order_usdt > self.limits.max_daily_buy_usdt:
                raise ExecutionDisabled("Daily notional budget exhausted")
            daily_pnl = sum((D(x["pnl_usdt"]) for x in rows
                             if x["closed"] is not None and datetime.fromtimestamp(float(x["closed"]), timezone.utc).date().isoformat() == today and x["pnl_usdt"] is not None), D(0))
            if daily_pnl <= -self.limits.max_daily_realized_loss_usdt:
                raise ExecutionDisabled("Daily loss limit reached")
            closed_times = [x["closed"] for x in rows if x["closed"] is not None]
            if closed_times and now - max(closed_times) < self.limits.cooldown_sec:
                raise ExecutionDisabled("Trade cooldown")

            # Do not assume two exchanges share account/spot-symbol rules.
            latest = await self.client.best_quote(symbol)
            venue_bid, venue_ask = D(latest["bidPrice"]), D(latest["askPrice"])
            if (venue_bid <= 0 or venue_ask <= venue_bid or
                    (venue_ask / venue_bid - 1)*100 > self.limits.max_spread_pct):
                raise ExecutionDisabled("Exchange REST bid/ask invalid")
            if abs(venue_ask / ask - 1)*100 > self.limits.max_buy_price_jump_pct:
                raise ExecutionDisabled("Market shifted before submission")
            asset = await self.client.instrument(symbol)
            snapshot = await self.client.account()
            free_usdt = next((D(x["free"]) for x in snapshot.get("balances", [])
                              if x.get("asset") == "USDT"), Decimal(0))
            if free_usdt < self.limits.max_order_usdt * D("1.005"):
                raise ExecutionDisabled("Insufficient USDT balance / fee reserve")
            client_id = "ssc" + secrets.token_hex(9)
            payload, notional = build_opoco(symbol, asset, venue_bid, venue_ask,
                                            self.limits, client_id=client_id)
            if not (0 <= time.time() - observed_at <= self.limits.max_quote_age_sec) and now > 1000000000:
                raise ExecutionDisabled("Signal expired during exchange checks")
            # Journal BEFORE any write. Never retry POST after network uncertainty.
            with self.db:
                self.db.execute(
                    "INSERT INTO orders(client_id,symbol,utc_day,started,usdt,status) "
                    "VALUES(?,?,?,?,?,?)",
                    (client_id, symbol, today, now, fmt(notional), "UNKNOWN"))
            try:
                response = await self.client.place_opoco(payload)
            except (UnknownOrderState, VenueFailure) as exc:
                self.pause("Order status unknown/rejected: inspect exchange manually")
                raise
            list_id = response.get("orderListId")
            if list_id is None:
                self.pause("Exchange returned no orderListId")
                raise UnknownOrderState("Missing response list ID")
            with self.db:
                self.db.execute(
                    "UPDATE orders SET status='ACTIVE', list_id=? WHERE client_id=?",
                    (str(list_id), client_id))
            return {"order_list_client_id": client_id, "status": "ACTIVE_NOT_FILLED",
                    "notional_usdt": fmt(notional),
                    "take_profit_trigger": payload["pendingAboveStopPrice"],
                    "stop_loss_trigger": payload["pendingBelowStopPrice"]}

    async def reconcile(self, now=None):
        """Poll exchange and close only fully settled and fee-verified lists."""
        async with self.lock:
            now = time.time() if now is None else now
            current = self.db.execute(
                "SELECT * FROM orders WHERE status NOT IN ('CLOSED','NO_FILL') "
                "ORDER BY started ASC").fetchall()
            results = []
            if not self.ready():
                return results
            for record in current:
                sym, client_id = record["symbol"], record["client_id"]
                try:
                    data = await self.client.list_state(client_id)
                    order_refs = data.get("orders", [])
                    if len(order_refs) != 3:
                        raise VenueFailure("OPOCO not showing all three orders")
                    states = [await self.client.order(sym, x["orderId"])
                              for x in order_refs]
                    buyers = [x for x in states if x.get("side") == "BUY"]
                    sellers = [x for x in states if x.get("side") == "SELL"]
                    if len(buyers) != 1 or len(sellers) != 2:
                        raise VenueFailure("Order roles do not match OPOCO")
                    buy = buyers[0]
                    filled_base = D(buy.get("executedQty", "0"))
                    list_status = data.get("listOrderStatus", "")
                    if buy.get("status") != "FILLED":
                        if filled_base > 0:
                            self.pause("Partial BUY fill: pending OCO may be inactive")
                            self._set_status(client_id, "PARTIAL_UNPROTECTED")
                        elif now - float(record["started"]) > self.limits.entry_ttl_sec:
                            # After cancelling, always re-query; if partial
                            # fill appeared, manual intervention is needed.
                            await self.client.cancel_list(sym, client_id)
                            confirmed = await self.client.order(sym, buy["orderId"])
                            if D(confirmed.get("executedQty", "0")) > 0:
                                self.pause("Late partial fill during cancel")
                                self._set_status(client_id, "PARTIAL_UNPROTECTED")
                            else:
                                self._set_status(client_id, "NO_FILL", closed=now)
                        results.append({"id": client_id, "status": self._state(client_id)})
                        continue
                    if list_status not in TERMINAL:
                        # Pending order(s) should be active. Check both exits
                        # are not rejected/canceled unexpectedly.
                        if all(x.get("status") in ("CANCELED","EXPIRED","REJECTED")
                               for x in sellers):
                            self.pause("Both protection orders unavailable")
                            self._set_status(client_id, "UNPROTECTED")
                        results.append({"id": client_id, "status": self._state(client_id)})
                        continue
                    sell_fills = [x for x in sellers if D(x.get("executedQty", "0")) > 0]
                    if not sell_fills:
                        self.pause("List ended without SELL execution")
                        self._set_status(client_id, "UNPROTECTED")
                        continue
                    buys = await self.client.trades(sym, buy["orderId"])
                    sells = []
                    for x in sell_fills:
                        sells.extend(await self.client.trades(sym, x["orderId"]))
                    base_asset = sym[:-4]
                    pnl = pnl_from_trades(buys, sells, base_asset)
                    if pnl is None:
                        self.pause("Residual quantity or third-asset commission: review")
                        self._set_status(client_id, "REVIEW_FEES")
                        continue
                    self._set_status(client_id, "CLOSED", closed=now, pnl=fmt(pnl))
                    results.append({"id": client_id, "status": "CLOSED", "pnl_usdt": fmt(pnl)})
                except (UnknownOrderState, VenueFailure, KeyError, TypeError, ValueError):
                    self.pause("Exchange reconciliation unavailable: manual review")
                    self._set_status(client_id, "UNKNOWN")
                    results.append({"id": client_id, "status": "UNKNOWN"})
            return results

    def _state(self, client_id):
        return self.db.execute("SELECT status FROM orders WHERE client_id=?",
                               (client_id,)).fetchone()[0]

    def _set_status(self, client_id, state, *, closed=None, pnl=None):
        with self.db:
            self.db.execute("UPDATE orders SET status=?, closed=COALESCE(?,closed), "
                            "pnl_usdt=COALESCE(?,pnl_usdt) WHERE client_id=?",
                            (state, closed, pnl, client_id))
