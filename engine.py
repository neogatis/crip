"""Pure functions for cross-exchange spot arbitrage feasibility checks.
An ESTIMATOR based on public order books, not executed P&L.
"""
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Sequence

D = Decimal

def dec(x):
    return D(str(x))

@dataclass(frozen=True)
class Book:
    exchange: str
    symbol: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    received_at: float

    def valid(self):
        if not self.bids or not self.asks:
            return False
        if self.bids[0][0] <= 0 or self.asks[0][0] <= 0:
            return False
        if self.bids[0][0] >= self.asks[0][0]:
            return False
        return True

def normalize_levels(raw, reverse=False, limit=50):
    levels = []
    for price, qty, *_ in raw:
        try:
            p, q = dec(price), dec(qty)
        except (InvalidOperation, TypeError, ValueError):
            continue
        if p > 0 and q > 0:
            levels.append((p, q))
    levels.sort(key=lambda x: x[0], reverse=reverse)
    return tuple(levels[:limit])

def fill_buy_quote(asks: Sequence[tuple[Decimal, Decimal]], quote_budget: Decimal):
    remaining = quote_budget
    qty = D(0)
    spent = D(0)
    for price, available in asks:
        buy_qty = min(available, remaining / price)
        cost = buy_qty * price
        qty += buy_qty
        spent += cost
        remaining -= cost
        if remaining <= D('0.00000001'):
            break
    if remaining > D('0.00000001'):
        return None
    return qty, spent

def fill_sell_base(bids: Sequence[tuple[Decimal, Decimal]], base_qty: Decimal):
    remaining = base_qty
    proceeds = D(0)
    for price, available in bids:
        sold = min(available, remaining)
        proceeds += sold * price
        remaining -= sold
        if remaining <= D('0.0000000001'):
            break
    if remaining > D('0.0000000001'):
        return None
    return proceeds

def estimate(buy: Book, sell: Book, budget, buy_fee_bps, sell_fee_bps,
             slippage_buffer_bps=10, rebalance_reserve_bps=10):
    if buy.exchange == sell.exchange or buy.symbol != sell.symbol:
        return None
    if not buy.valid() or not sell.valid():
        return None
    budget = dec(budget)
    if budget <= 0:
        return None
    buy_fee = dec(buy_fee_bps) / D(10000)
    sell_fee = dec(sell_fee_bps) / D(10000)
    buy_quote = budget / (D(1) + buy_fee)
    fill = fill_buy_quote(buy.asks, buy_quote)
    if fill is None:
        return None
    base_qty, buy_cost = fill
    proceeds = fill_sell_base(sell.bids, base_qty)
    if proceeds is None:
        return None
    buy_commission = buy_cost * buy_fee
    sell_commission = proceeds * sell_fee
    uncertainty = buy_cost * dec(slippage_buffer_bps) / D(10000)
    rebalance = buy_cost * dec(rebalance_reserve_bps) / D(10000)
    net = proceeds - sell_commission - buy_cost - buy_commission - uncertainty - rebalance
    return {
        'buy': buy.exchange, 'sell': sell.exchange, 'symbol': buy.symbol,
        'quote_budget': budget, 'base_qty': base_qty,
        'effective_buy': buy_cost / base_qty,
        'effective_sell': proceeds / base_qty,
        'buy_fee': buy_commission, 'sell_fee': sell_commission,
        'uncertainty_reserve': uncertainty, 'rebalance_reserve': rebalance,
        'gross_spread_pct': (proceeds / buy_cost - D(1)) * D(100),
        'net_usdt': net, 'net_pct': net / budget * D(100),
    }
