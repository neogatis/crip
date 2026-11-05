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
from risk import classify_route, is_known_collision
from identity import IDENTITY_MODE, same_asset, resolve_asset
from connectivity_probe import run_probes
from dashboard import run_dashboard

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
        self.db.execute("""CREATE TABLE IF NOT EXISTS spread_reports (
            id INTEGER PRIMARY KEY, occurred_at TEXT NOT NULL,
            data_json TEXT NOT NULL)""")
        self.db.commit()

    def save_spread_report(self, payload):
        self.db.execute("INSERT INTO spread_reports (occurred_at, data_json) VALUES (?,?)",
            (datetime.now(timezone.utc).isoformat(), json.dumps(payload)))
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
            min_gate_volume_usdt=int(os.environ.get('MIN_GATE_VOLUME_USDT','0')),
            scout_mode=True)
        if selected:
            symbols=selected
            venue_symbols={v:mapping.get(v,[]) for v in requested}
        else:
            raise RuntimeError('No common active spot pairs discovered; scanner cannot run safely')
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
    scan_interval=float(os.environ.get('SCAN_INTERVAL_SECONDS','0.5'))
    observed_spreads={}
    observed_candidates={}
    observed_raw={}
    evaluated_candidates=0
    evaluated_scout=0

    def send(book):
        feed_counters[book.exchange]+=1
        if queue.full():
            try: queue.get_nowait()
            except asyncio.QueueEmpty: pass
        queue.put_nowait(book)

    LOG.info('Public-data scanner: %s; symbols: %s; amounts: %s; identity=%s',requested,symbols,amounts,IDENTITY_MODE)
    LOG.warning('RESEARCH ONLY: No authenticated APIs, no account fees, no trading, no executable signals.')
    async def report_diagnostics():
        nonlocal evaluated_candidates, evaluated_scout
        while True:
            await asyncio.sleep(20)
            now=time.monotonic()
            fresh=defaultdict(int)
            for b in books.values():
                if (now-b.received_at)*1000 <= book_max_age_ms:
                    fresh[b.exchange]+=1
            LOG.info('HEALTH market-data updates=%s; fresh-books=%s; queue=%d',
                     dict(feed_counters),dict(fresh),queue.qsize())
            # Raw best-bid/best-ask gaps are displayed separately because they
            # may not have enough depth to fill even the model's 50-USDT order.
            def build_entry(r):
                return {
                    'pair':r['symbol'],'buy':r['buy'],'sell':r['sell'],
                    'gross_pct':round(float(r['gross_spread_pct']),5),
                    'estimated_net_pct':round(float(r['net_pct']),5),
                    'estimated_net_usdt':round(float(r['net_usdt']),5),
                    'model_amount_usdt':round(float(r['quote_budget']),2),
                    **classify_route(r['buy'],r['sell'],r['symbol']),
                }
            snapshots=list(observed_spreads.values())
            ranked_gross=sorted(snapshots,key=lambda r:r['gross_spread_pct'],reverse=True)
            ranked_net=sorted(snapshots,key=lambda r:r['net_pct'],reverse=True)
            ranked_raw=sorted(observed_raw.values(),key=lambda r:r['raw_gross_pct'],reverse=True)
            scout_snapshots=list(observed_candidates.values())
            ranked_scout=sorted(scout_snapshots,key=lambda r:r['net_pct'],reverse=True)
            scout_plausible=[r for r in ranked_scout if r['gross_spread_pct']<dec(10)]
            scout_anomalies=[r for r in ranked_scout if r['gross_spread_pct']>=dec(10)]
            raw_top=[
                {'pair':r['symbol'],'buy':r['buy'],'sell':r['sell'],
                 'top_of_book_gross_pct':round(float(r['raw_gross_pct']),5),
                 'depth_checked':False,
                 **classify_route(r['buy'],r['sell'],r['symbol'])}
                for r in ranked_raw[:5]
            ]
            payload={
                'type':'SPREAD_REPORT','timestamp':datetime.now(timezone.utc).isoformat(),
                'mode':'RESEARCH_ONLY','identity_mode':IDENTITY_MODE,
                'universe_count':len(symbols),
                'curated_symbols':sum(resolve_asset(sym) is not None for sym in symbols),
                'scout_symbols':sum(resolve_asset(sym) is None for sym in symbols),
                'live_books_by_venue':dict(fresh),
                'evaluations':evaluated_candidates,
                'depth_qualified_routes':len(snapshots),
                'scout_depth_routes':len(scout_snapshots),
                'scout_net_positive':sum(r['net_pct']>0 for r in scout_snapshots),
                'scout_evaluations':evaluated_scout,
                'gross_positive':sum(r['gross_spread_pct']>0 for r in snapshots),
                'net_positive':sum(r['net_pct']>0 for r in snapshots),
                'net_above_signal_threshold':sum(r['net_pct']>=min_net_pct for r in snapshots),
                'top_of_book_gross':raw_top,
                'top_depth_adjusted_gross':[build_entry(r) for r in ranked_gross[:5]],
                'top_estimated_net':[build_entry(r) for r in ranked_net[:5]],
                'routes':[build_entry(r) for r in ranked_net[:120]],
                'gross_routes':[build_entry(r) for r in ranked_gross[:75]],
                'scout_routes':[build_entry(r) for r in scout_plausible[:120]],
                'scout_anomalies':[build_entry(r) for r in scout_anomalies[:25]],
                'note':'Two tracks: curated-native research signals plus unverified scout candidates (NO ALERTS). Neither confirms exchange contract, account eligibility, transfer network or executable profit.',
            }
            # Persist expanded rankings for the UI, but keep Railway logs compact.
            brief={k:v for k,v in payload.items() if k not in ('routes','gross_routes','scout_routes','scout_anomalies')}
            LOG.info('SPREAD_REPORT %s',json.dumps(brief,ensure_ascii=False,separators=(',',':')))
            store.save_spread_report(payload)
            observed_spreads.clear()
            observed_candidates.clear()
            observed_raw.clear()
            evaluated_candidates=0
            evaluated_scout=0
            if not fresh:
                LOG.warning('HEALTH no fresh order books; spread absence cannot be established')

    tasks=[asyncio.create_task(supervise(name,venue_symbols[name],send),name=name)
           for name in requested if venue_symbols.get(name)]
    tasks.append(asyncio.create_task(report_diagnostics(),name='market-data-health'))
    tasks.append(asyncio.create_task(run_probes(),name='regional-exchange-probes'))
    tasks.append(asyncio.create_task(run_dashboard(),name='private-research-dashboard'))
    async with aiohttp.ClientSession() as session:
        try:
            # Best gross/net snapshots are diagnostic, not executable opportunities.
            while True:
                book=await queue.get()
                books[(book.exchange,book.symbol)]=book
                now=time.monotonic()
                if now-last_scan[book.symbol]<scan_interval:
                    continue
                last_scan[book.symbol]=now
                current=[b for (venue,sym),b in books.items()
                         if sym==book.symbol and (now-b.received_at)*1000<=book_max_age_ms]
                active=set()
                for buy in current:
                    for sell in current:
                        if buy.exchange==sell.exchange or is_known_collision(buy.exchange,sell.exchange,book.symbol):
                            continue
                        if buy.asks and sell.bids and buy.asks[0][0]>0:
                            top_buy=buy.asks[0][0]
                            top_sell=sell.bids[0][0]
                            raw_gap=(top_sell/top_buy-dec(1))*dec(100)
                            raw_key=(buy.exchange,sell.exchange,book.symbol)
                            prev_raw=observed_raw.get(raw_key)
                            if prev_raw is None or raw_gap>prev_raw['raw_gross_pct']:
                                observed_raw[raw_key]={
                                    'symbol':book.symbol,'buy':buy.exchange,
                                    'sell':sell.exchange,'raw_gross_pct':raw_gap,
                                }
                        for amount in amounts:
                            key=(buy.exchange,sell.exchange,book.symbol,str(amount))
                            r=estimate(buy,sell,amount,
                                fee_bps[buy.exchange],fee_bps[sell.exchange],
                                slippage_bps,rebalance_bps)
                            vetted=same_asset(buy.exchange,sell.exchange,book.symbol)
                            if r is not None:
                                target=observed_spreads if vetted else observed_candidates
                                if vetted:
                                    evaluated_candidates+=1
                                else:
                                    evaluated_scout+=1
                                existing=target.get(key)
                                if existing is None or r['net_pct']>existing['net_pct']:
                                    target[key]=r
                            # Candidate-only lane is never alerted or saved as a trade signal.
                            if not vetted:
                                first_seen.pop(key,None)
                                continue
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
