"""Discover active common USDT spot markets without authenticated API access.

Only public metadata; does not imply that a market is legally tradable by a
specific person or that two assets sharing a ticker have the same identity.
"""
import asyncio
import logging
import re
from collections import Counter
from decimal import Decimal, InvalidOperation

import aiohttp
from identity import resolve_asset, IDENTITY_MODE

LOG = logging.getLogger('sarb.discovery')
SYMBOL_RE = re.compile(r'^[A-Z0-9]{2,15}$')
EXCLUDE_SUFFIXES = ('3L', '3S', '5L', '5S', '2L', '2S', 'BULL', 'BEAR')
PREFERRED = ('BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'DOGE', 'ADA', 'LINK',
             'LTC', 'AVAX', 'TRX', 'SUI', 'DOT', 'ARB', 'OP', 'APT', 'SHIB',
             'PEPE', 'BONK', 'WIF', 'UNI', 'NEAR', 'ATOM', 'ETC')

def normalize_base(symbol):
    symbol = str(symbol).upper().strip()
    if symbol == 'XBT':
        return 'BTC'
    return symbol

def safe_base(symbol):
    symbol = normalize_base(symbol)
    return bool(SYMBOL_RE.fullmatch(symbol)) and symbol not in ('USDT', 'USD') and not symbol.endswith(EXCLUDE_SUFFIXES)

def parse_markets(venue, payload):
    """Parse public listing responses to normalized base asset names."""
    pairs = set()
    if venue == 'okx':
        if payload.get('code') != '0':
            raise ValueError(f'OKX instruments error: {payload.get("msg")}')
        for x in payload.get('data', []):
            if (x.get('state') == 'live' and x.get('quoteCcy') == 'USDT'
                    and x.get('instType', 'SPOT') == 'SPOT'):
                base = normalize_base(x.get('baseCcy', ''))
                if safe_base(base): pairs.add(base)
    elif venue == 'bitget':
        if payload.get('code') != '00000':
            raise ValueError(f'Bitget instruments error: {payload.get("msg")}')
        for x in payload.get('data', []):
            if (str(x.get('status', '')).lower() == 'online'
                    and str(x.get('quoteCoin', '')).upper() == 'USDT'
                    and x.get('category', 'SPOT') == 'SPOT'
                    and str(x.get('isRwa', 'NO')).upper() != 'YES'
                    and str(x.get('isReality', 'no')).lower() != 'yes'
                    and str(x.get('symbolType', 'crypto')).lower() in ('crypto', '')):
                base = normalize_base(x.get('baseCoin', ''))
                if safe_base(base): pairs.add(base)
    elif venue == 'gate':
        if not isinstance(payload, list):
            raise ValueError('Gate pair list is not an array')
        for x in payload:
            if (x.get('quote') == 'USDT' and x.get('trade_status') == 'tradable'
                    and x.get('type', 'normal') in ('normal', None)
                    and not x.get('st_tag', False)):
                base = normalize_base(x.get('base', ''))
                if safe_base(base): pairs.add(base)
    elif venue == 'binance_public':
        if not isinstance(payload, dict) or not isinstance(payload.get('symbols'), list):
            raise ValueError('Binance exchangeInfo malformed')
        for x in payload['symbols']:
            if (x.get('quoteAsset') == 'USDT' and x.get('status') == 'TRADING'
                    and x.get('isSpotTradingAllowed', True)
                    and x.get('symbol') == str(x.get('baseAsset',''))+'USDT'):
                base=normalize_base(x.get('baseAsset',''))
                if safe_base(base): pairs.add(base)
    elif venue == 'bybit_kz':
        if str(payload.get('retCode')) != '0':
            raise ValueError(f'Bybit instruments error: {payload.get("retMsg")}')
        for x in (payload.get('result') or {}).get('list',[]):
            if (x.get('quoteCoin') == 'USDT' and x.get('status') == 'Trading'
                    and x.get('symbol') == str(x.get('baseCoin',''))+'USDT'):
                base=normalize_base(x.get('baseCoin',''))
                if safe_base(base): pairs.add(base)
    elif venue == 'kraken':
        if payload.get('error'):
            raise ValueError(f'Kraken asset pairs error: {payload["error"]}')
        for x in payload.get('result', {}).values():
            wsname = x.get('wsname', '')
            base, sep, quote = wsname.partition('/')
            if sep and quote == 'USDT' and x.get('status') == 'online':
                base = normalize_base(base)
                if safe_base(base): pairs.add(base)
    else:
        raise ValueError(f'No discovery parser for {venue}')
    return pairs

API_URLS = {
    'okx': 'https://www.okx.com/api/v5/public/instruments?instType=SPOT',
    'bitget': 'https://api.bitget.com/api/v3/market/instruments?category=SPOT',
    'gate': 'https://api.gateio.ws/api/v4/spot/currency_pairs',
    'kraken': 'https://api.kraken.com/0/public/AssetPairs',
    'binance_public': 'https://data-api.binance.vision/api/v3/exchangeInfo',
    'bybit_kz': 'https://api.bybit.kz/v5/market/instruments-info?category=spot',
}
VOLUME_URL = 'https://api.gateio.ws/api/v4/spot/tickers'

async def get_json(session, url):
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)

def parse_gate_quote_volume(payload):
    if not isinstance(payload, list):
        return {}
    data = {}
    for x in payload:
        pair = x.get('currency_pair', '')
        if not pair.endswith('_USDT'): continue
        base = normalize_base(pair[:-5])
        if not safe_base(base): continue
        try:
            amount = Decimal(str(x.get('quote_volume', '0')))
        except (InvalidOperation, ValueError):
            continue
        if amount.is_finite() and amount >= 0:
            data[base] = amount
    return data

def select_universe(available, volume=None, max_pairs=100, min_exchanges=2,
                    min_gate_volume_usdt=0, scout_mode=False):
    volume = volume or {}
    counts = Counter(s for symbols in available.values() for s in symbols)
    eligible = [s for s, count in counts.items() if count >= min_exchanges and safe_base(s)
                and (scout_mode or resolve_asset(s) is not None)
                and (s not in volume or volume[s] >= Decimal(str(min_gate_volume_usdt)))]
    # Preference to high Gate-quoted 24h volume when reported; a preferred liquid
    # reference set breaks ties, but is not an assertion about live liquidity.
    preferred = {symbol:i for i,symbol in enumerate(PREFERRED)}
    eligible.sort(key=lambda s: (
        (0 if resolve_asset(s) is not None else 1) if scout_mode else 0,
        -(1 if s in volume else 0), -volume.get(s, Decimal(0)),
        preferred.get(s, 999), -counts[s], s
    ))
    return eligible[:max(1, min(int(max_pairs), 300))]

async def discover(venues, max_pairs=100, min_exchanges=2, min_gate_volume_usdt=0,
                   scout_mode=False):
    enabled = [v for v in venues if v in API_URLS]
    if len(enabled) < 2:
        raise ValueError('Require at least two discoverable venues')
    async with aiohttp.ClientSession(headers={'User-Agent': 'S-Arbitrage-Research/0.2'}) as session:
        responses = await asyncio.gather(*(get_json(session, API_URLS[v]) for v in enabled),
                                         return_exceptions=True)
        available = {}
        for venue, response in zip(enabled, responses):
            if isinstance(response, BaseException):
                LOG.warning('%s discovery HTTP error: %s', venue, response)
                continue
            try:
                available[venue] = parse_markets(venue, response)
                LOG.info('%s: %d active USDT spot markets', venue, len(available[venue]))
            except Exception as exc:
                LOG.warning('%s discovery parse failed: %s', venue, exc)
        if len(available) < 2:
            LOG.warning('Fewer than two verified venue catalogs; no fallback to invented markets')
            return {}, []
        try:
            volumes = parse_gate_quote_volume(await get_json(session, VOLUME_URL))
            LOG.info('Gate 24h volume metadata available for %d pairs', len(volumes))
        except Exception as exc:
            LOG.warning('Gate volume metadata unavailable; no volume qualification: %s', exc)
            volumes = {}
        selected = select_universe(available, volumes, max_pairs, min_exchanges, min_gate_volume_usdt,
                                   scout_mode=scout_mode)
        mapping = {v:[s for s in selected if s in listed] for v,listed in available.items()}
        LOG.info('IDENTITY_MODE=%s selected=%d curated=%d scouting_unverified=%d',
                 IDENTITY_MODE, len(selected),
                 sum(resolve_asset(s) is not None for s in selected),
                 sum(resolve_asset(s) is None for s in selected))
        LOG.info('Selected %d shared USDT pairs (requested cap %d), %s',
                 len(selected), max_pairs, {v:len(a) for v,a in mapping.items()})
        return mapping, selected
