"""Public WebSocket snapshots. No keys and no private API interaction."""
import asyncio
import json
import logging
import time
from contextlib import suppress

import websockets

from engine import Book, normalize_levels

LOG = logging.getLogger('sarb.feed')

def emit(exchange, symbol, bids, asks, send):
    b = Book(exchange=exchange, symbol=f'{symbol}/USDT',
             bids=normalize_levels(bids, reverse=True),
             asks=normalize_levels(asks), received_at=time.monotonic())
    if b.valid():
        send(b)

async def heartbeat(ws, message, every=25):
    while True:
        await asyncio.sleep(every)
        await ws.send(message)

async def listen_binance(symbols, send):
    # Public global feed: not evidence of Binance Kazakhstan executable liquidity.
    streams = '/'.join(f'{s.lower()}usdt@depth20@100ms' for s in symbols)
    url = 'wss://stream.binance.com:9443/stream?streams=' + streams
    async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            data = msg.get('data', {})
            stream = msg.get('stream', '')
            if 'bids' not in data:
                continue
            symbol = stream.split('usdt@')[0].upper()
            emit('binance_public', symbol, data['bids'], data['asks'], send)

async def listen_bybit_kz(symbols, send):
    url = 'wss://stream.bybit.kz/v5/public/spot'
    snapshots = {}
    async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
        await ws.send(json.dumps({'op':'subscribe','args':[f'orderbook.50.{s}USDT' for s in symbols]}))
        keepalive = asyncio.create_task(heartbeat(ws, json.dumps({'op':'ping'}), 20))
        try:
            async for raw in ws:
                msg = json.loads(raw)
                if not msg.get('topic', '').startswith('orderbook.50.'):
                    continue
                d = msg.get('data') or {}
                symbol = d.get('s','').removesuffix('USDT')
                if symbol not in symbols:
                    continue
                if msg.get('type') == 'snapshot':
                    snapshots[symbol] = {
                        'b': {p:q for p,q in d.get('b',[])},
                        'a': {p:q for p,q in d.get('a',[])},
                        'u': d.get('u',0),
                    }
                elif msg.get('type') == 'delta':
                    if symbol not in snapshots:
                        continue
                    st = snapshots[symbol]
                    u = d.get('u',0)
                    if u and st['u'] and u <= st['u']:
                        continue
                    for side in ('b','a'):
                        for price, qty in d.get(side,[]):
                            if float(qty) == 0:
                                st[side].pop(price,None)
                            else:
                                st[side][price]=qty
                    st['u']=u
                else:
                    continue
                st = snapshots[symbol]
                emit('bybit_kz', symbol, st['b'].items(), st['a'].items(), send)
        finally:
            keepalive.cancel()
            with suppress(asyncio.CancelledError):
                await keepalive

async def listen_okx(symbols, send):
    url='wss://ws.okx.com:8443/ws/v5/public'
    async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
        for i in range(0,len(symbols),20):
            await ws.send(json.dumps({'op':'subscribe','args':[
                {'channel':'books5','instId':f'{s}-USDT'} for s in symbols[i:i+20]]}))
            await asyncio.sleep(0.25)
        async for raw in ws:
            msg=json.loads(raw)
            if msg.get('arg',{}).get('channel')!='books5':
                continue
            symbol=msg['arg']['instId'].removesuffix('-USDT')
            for d in msg.get('data',[]):
                emit('okx',symbol,d.get('bids',[]),d.get('asks',[]),send)

async def listen_gate(symbols, send):
    url='wss://api.gateio.ws/ws/v4/'
    async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
        for symbol in symbols:
            await ws.send(json.dumps({'time':int(time.time()),'channel':'spot.order_book',
                'event':'subscribe','payload':[f'{symbol}_USDT','20','100ms']}))
            await asyncio.sleep(0.12)
        async for raw in ws:
            msg=json.loads(raw)
            if msg.get('channel')!='spot.order_book' or msg.get('event')!='update':
                continue
            d=msg.get('result') or {}
            symbol=d.get('s','').removesuffix('_USDT')
            if symbol in symbols:
                emit('gate',symbol,d.get('bids',[]),d.get('asks',[]),send)

async def listen_bitget(symbols, send):
    url='wss://ws.bitget.com/v3/ws/public'
    async with websockets.connect(url, ping_interval=None, max_size=2**22) as ws:
        for i in range(0,len(symbols),20):
            await ws.send(json.dumps({'op':'subscribe','args':[
                {'instType':'spot','topic':'books5','symbol':f'{s}USDT'} for s in symbols[i:i+20]]}))
            await asyncio.sleep(0.3)
        keepalive=asyncio.create_task(heartbeat(ws,'ping',25))
        try:
            async for raw in ws:
                if raw=='pong':
                    continue
                msg=json.loads(raw)
                arg=msg.get('arg') or {}
                if arg.get('topic')!='books5' or arg.get('instType')!='spot':
                    continue
                symbol=arg.get('symbol','').removesuffix('USDT')
                if symbol not in symbols:
                    continue
                data=msg.get('data') or []
                if isinstance(data,dict):
                    data=[data]
                for d in data:
                    emit('bitget',symbol,d.get('b',d.get('bids',[])),d.get('a',d.get('asks',[])),send)
        finally:
            keepalive.cancel()
            with suppress(asyncio.CancelledError):
                await keepalive

async def listen_kraken(symbols, send):
    url='wss://ws.kraken.com/v2'
    books={}
    async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
        for i in range(0,len(symbols),20):
            await ws.send(json.dumps({'method':'subscribe',
                'params':{'channel':'book','symbol':[f'{s}/USDT' for s in symbols[i:i+20]],
                          'depth':10, 'snapshot':True}}))
            await asyncio.sleep(0.4)
        async for raw in ws:
            msg=json.loads(raw)
            if msg.get('success') is False:
                LOG.warning('kraken subscription rejected: %s', msg.get('error', msg))
            if msg.get('channel')!='book':
                continue
            for d in msg.get('data',[]):
                pair=d.get('symbol','')
                if not pair.endswith('/USDT'):
                    continue
                symbol=pair.split('/')[0]
                if symbol == 'XBT': symbol = 'BTC'
                if symbol not in symbols:
                    continue
                if msg.get('type')=='snapshot' or symbol not in books:
                    books[symbol]={'b':{},'a':{}}
                st=books[symbol]
                for side, wire in (('b','bids'),('a','asks')):
                    for entry in d.get(wire,[]):
                        price=str(entry.get('price',''))
                        qty=str(entry.get('qty',''))
                        if not price:
                            continue
                        if float(qty) == 0:
                            st[side].pop(price,None)
                        else:
                            st[side][price]=qty
                for side in ('b','a'):
                    levels=normalize_levels(st[side].items(),reverse=(side=='b'),limit=10)
                    st[side]={str(p):str(q) for p,q in levels}
                emit('kraken',symbol,st['b'].items(),st['a'].items(),send)

ADAPTERS={
    'binance_public':listen_binance,
    'bybit_kz':listen_bybit_kz,
    'okx':listen_okx,
    'gate':listen_gate,
    'bitget':listen_bitget,
    'kraken':listen_kraken,
}

async def supervise(name, symbols, send):
    delay=1
    while True:
        try:
            LOG.info('Connecting to %s',name)
            await ADAPTERS[name](symbols,send)
            LOG.warning('%s stream ended; reconnecting',name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning('%s feed error: %s',name,exc)
        await asyncio.sleep(delay)
        delay=min(delay*2,60)
