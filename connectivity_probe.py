"""One-shot, public-only connectivity diagnostics for regional exchange feeds.
These probes do not authenticate, access balances, or submit orders.
"""
import asyncio
import json
import logging

import websockets

LOG=logging.getLogger('sarb.probe')

SOURCES=(
    ("bybit_kz", "wss://stream.bybit.kz/v5/public/spot", True),
    # Global Binance feed, not evidence of Binance Kazakhstan executable prices.
    ("binance_global_public", "wss://stream.binance.com:443/ws/btcusdt@depth20@100ms", False),
)

async def run_probes():
    for name,url,subscribe in SOURCES:
        try:
            async with websockets.connect(
                url,open_timeout=9,close_timeout=2,ping_interval=20,
                max_size=2**20,
            ) as ws:
                if subscribe:
                    await ws.send(json.dumps({
                        "op":"subscribe","args":["orderbook.1.BTCUSDT"]
                    }))
                got_data=False
                last_type=None
                for _ in range(5):
                    try:
                        raw=await asyncio.wait_for(ws.recv(),timeout=5)
                    except asyncio.TimeoutError:
                        break
                    try:
                        msg=json.loads(raw)
                    except Exception:
                        continue
                    last_type=msg.get("type",msg.get("event",msg.get("op","unknown")))
                    if name=="bybit_kz":
                        if str(msg.get("topic","")).startswith("orderbook."):
                            got_data=True
                            break
                    elif "bids" in msg or "asks" in msg:
                        got_data=True
                        break
                LOG.info("EXCHANGE_PROBE %s connected=true market_data=%s last_type=%s",
                         name,got_data,last_type)
        except Exception as exc:
            LOG.warning("EXCHANGE_PROBE %s connected=false reason=%s",
                        name,str(exc)[:280])
