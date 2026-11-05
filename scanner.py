"""S-Arbitrage public-data research scanner. DOES NOT PLACE ORDERS."""
import asyncio
import json
import logging
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from adapters import ADAPTERS, supervise
from discovery import discover
from engine import dec, estimate

logging.basicConfig(level=os.environ.get('LOG_LEVEL','INFO').upper(),
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
LOG=logging.getLogger('sarb')

DEFAULT_FEES_BPS={
    'binance_public':10, 'bybit_kz':10, 'okx':10,
    'bitget':10, 'gate':20, 'kraken':40,
}

def env_list(name, default):
    return [x.strip().upper() for x in os.environ.get(name,default).split(',') if x.strip()]

class Store:
    def __init__(self, filename):
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        self.db=sqlite3.connect(filename)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute("""CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY, occurred_at TEXT NOT NULL,
            symbol TEXT NOT NULL, buy TEXT NOT NULL, sell TEXT NOT NULL,
            amount_usdt TEXT NOT NULL, net_usdt TEXT NOT NULL,
            net_pct TEXT NOT NULL, gross_pct TEXT NOT NULL,
            status TEXT NOT NULL, costs_json TEXT NOT NULL)""")
        self.db.commit()

    def save(self, e, status):
        self.db.execute("""INSERT INTO signals (occurred_at,symbol,buy,sell,
            amount_usdt,net_usdt,net_pct,gross_pct,status,costs_json)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",(
            datetime.now(timezone.utc).isoformat(),e['symbol'],e['buy'],e['sell'],
            str(e['quote_budget']),str(e['net_usdt']),str(e['net_pct']),
            str(e['gross_spread_pct']),status,json.dumps({
              'buy_fee':str(e['buy_fee']),'sell_fee':str(e['sell_fee']),
              'uncertainty':str(e['uncertainty_reserve']),
              'rebalance':str(e['rebalance_reserve'])}),
        ))
        self.db.commit()

async def telegram(session, message):
    token=os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id=os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        return
    try:
        async with session.post(f'https://api.telegram.org/bot{token}/sendMessage',
             json={'chat_id':chat_id,'text':message},timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status!=200:
                LOG.warning('Telegram HTTP %d',resp.status)
    except Exception as exc:
        LOG.warning('Telegram notification failed: %s',exc)

async def run():
    symbols=env_list('SYMBOLS','BTC,ETH,SOL')
    requested=[x.strip().lower() for x in os.environ.get('EXCHANGES',','.join(ADAPTERS)).split(',') if x.strip()]
    unknown=set(requested)-set(ADAPTERS)
    if unknown:
        raise ValueError(f'Unknown exchange adapters: {sorted(unknown)}')
    if not requested:
        raise ValueError('At least one exchange required')
    dynamic=os.environ.get('DYNAMIC_SYMBOLS','true').lower() in ('true','1','yes')
    venue_symbols={v:list(symbols) for v in requested}
    if dynamic:
        mapping,selected=await discover(requested,
            max_pairs=int(os.environ.get('MAX_PAIRS','60')),
            min_exchanges=2,
            min_gate_volume_usdt=int(os.environ.get('MIN_GATE_VOLUME_USDT','0')))
        if selected:
            symbols=selected
            venue_symbols={v:mapping.get(v,[]) for v in requested}
        else:
            LOG.warning('Discovery failed or no common pairs; keeping static safe fallback %s',symbols)
    amounts=[dec(x) for x in os.environ.get('TRADE_AMOUNTS_USDT','25,50').split(',')]
    fee_bps={**DEFAULT_FEES_BPS,**json.loads(os.environ.get('FEE_BPS_JSON','{}'))}
    min_net_pct=dec(os.environ.get('MIN_NET_PCT','0.30'))
    slippage_bps=dec(os.environ.get('UNCERTAINTY_BPS','10'))
    rebalance_bps=dec(os.environ.get('REBALANCE_BPS','10'))
    book_max_age_ms=int(os.environ.get('BOOK_MAX_AGE_MS','1500'))
    sustain_seconds=float(os.environ.get('SUSTAIN_SECONDS','1'))
    alert_cooldown=int(os.environ.get('ALERT_COOLDOWN_SECONDS','60'))
    database=os.environ.get('SQLITE_PATH','./data/signals.sqlite3')
    store=Store(database)
    books={}
    feed_counters=defaultdict(int)
    queue=asyncio.Queue(maxsize=20000)
    first_seen={}
    last_alert=defaultdict(lambda:0)
    last_scan=defaultdict(lambda:0)
    observed_spreads={}
    evaluated_candidates=0

    def send(book):
        feed_counters[book.exchange]+=1
        if queue.full():
            try: queue.get_nowait()
            except asyncio.QueueEmpty: pass
        queue.put_nowait(book)

    LOG.info('Public-data scanner: %s; symbols: %s; amounts: %s',requested,symbols,amounts)
    LOG.warning('RESEARCH ONLY: No authenticated APIs, no account fees, no trading, no executable signals.')
    async def report_diagnostics():
        nonlocal evaluated_candidates
        while True:
            await asyncio.sleep(20)
            now=time.monotonic()
            fresh=defaultdict(int)
            for b in books.values():
                if (now-b.received_at)*1000 <= book_max_age_ms:
                    fresh[b.exchange]+=1
            LOG.info('HEALTH market-data updates=%s; fresh-books=%s; queue=%d',
                     dict(feed_counters),dict(fresh),queue.qsize())
            if observed_spreads:
                highest_net=sorted(observed_spreads.values(),
                    key=lambda r:r['net_pct'],reverse=True)[:5]
                best_gross=max(observed_spreads.values(),key=lambda r:r['gross_spread_pct'])
                def describe(r):
                    return (f"{r['symbol']} {r['buy']}->{r['sell']}: "
                            f"gross={r['gross_spread_pct']:.3f}%, "
                            f"net={r['net_pct']:.3f}%, "
                            f"50USDT est-net={r['net_usdt']:.4f}")
                LOG.info('SPREAD_SNAPSHOT evaluated=%d top_net=%s best_gross=%s',
                         evaluated_candidates,
                         [describe(r) for r in highest_net],describe(best_gross))
            else:
                LOG.warning('SPREAD_SNAPSHOT no sufficiently deep simultaneous cross-venue books')
            observed_spreads.clear()
            evaluated_candidates=0
            if not fresh:
                LOG.warning('HEALTH no fresh order books; spread absence cannot be established')

    tasks=[asyncio.create_task(supervise(name,venue_symbols[name],send),name=name)
           for name in requested if venue_symbols.get(name)]
    tasks.append(asyncio.create_task(report_diagnostics(),name='market-data-health'))
    async with aiohttp.ClientSession() as session:
        try:
            # Best gross/net snapshots are diagnostic, not executable opportunities.
            while True:
                book=await queue.get()
                books[(book.exchange,book.symbol)]=book
                now=time.monotonic()
                if now-last_scan[book.symbol]<0.25:
                    continue
                last_scan[book.symbol]=now
                current=[b for (venue,sym),b in books.items()
                         if sym==book.symbol and (now-b.received_at)*1000<=book_max_age_ms]
                active=set()
                for buy in current:
                    for sell in current:
                        if buy.exchange==sell.exchange:
                            continue
                        for amount in amounts:
                            key=(buy.exchange,sell.exchange,book.symbol,str(amount))
                            r=estimate(buy,sell,amount,
                                fee_bps[buy.exchange],fee_bps[sell.exchange],
                                slippage_bps,rebalance_bps)
                            if r is not None and amount == dec(50):
                                evaluated_candidates+=1
                                existing=observed_spreads.get(key)
                                if existing is None or r['net_pct']>existing['net_pct']:
                                    observed_spreads[key]=r
                            if r is None or r['net_pct'] < min_net_pct:
                                first_seen.pop(key,None)
                                continue
                            active.add(key)
                            if key not in first_seen:
                                first_seen[key]=now
                            if now-first_seen[key] < sustain_seconds:
                                continue
                            if now-last_alert[key]<alert_cooldown:
                                continue
                            last_alert[key]=now
                            store.save(r,'RESEARCH_ONLY')
                            report=(f"[RESEARCH ONLY] {r['symbol']} {r['buy']} -> {r['sell']} | "
                                    f"{amount} USDT | est. net +{r['net_pct']:.3f}% "
                                    f"({r['net_usdt']:.4f} USDT) | PUBLIC BOOKS; NOT EXECUTABLE")
                            LOG.info(report)
                            await telegram(session,report)
                for key in list(first_seen):
                    if key[2]==book.symbol and key not in active:
                        del first_seen[key]
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)

if __name__=='__main__':
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
