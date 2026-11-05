# S-Arbitrage MVP

Research-only cryptocurrency spot exchange spread scanner, **no private APIs, no trades, no transfers, and no funds at risk**.

Tracks BTC/USDT, ETH/USDT and SOL/USDT public order books from Binance global, Bybit Kazakhstan, OKX, Bitget, Gate and Kraken. Global Binance data does not imply executable Binance Kazakhstan liquidity. All signals are RESEARCH_ONLY.

## Railway deployment
Build from Dockerfile. This is a worker, so it does not require a public HTTP domain. The default persistent database path is /data/signals.sqlite3; attach a Railway volume at /data before collecting production history. If Telegram credentials are absent, signals are logged to stdout and saved to SQLite.

Variables:
- EXCHANGES=binance_public,bybit_kz,okx,bitget,gate,kraken
- SYMBOLS=BTC,ETH,SOL
- TRADE_AMOUNTS_USDT=25,50
- MIN_NET_PCT=0.30
- BOOK_MAX_AGE_MS=1500
- SUSTAIN_SECONDS=1
- ALERT_COOLDOWN_SECONDS=60
- UNCERTAINTY_BPS=10
- REBALANCE_BPS=10
- SQLITE_PATH=/data/signals.sqlite3
- TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are optional.

Estimated proceeds and expenses come from public order-book depth and illustrative fee rates, not account data. Live streams have not been verified in the initial offline tests. Quote freshness is measured at receipt; full exchange sequence/checksum validation and fill simulation remain future tasks.

## Local tests
`python -m unittest discover -s tests -v`

## Limitations
Spot only. No money movement, buy/sell requests, wallet access, or real execution. Data from venues that may not be eligible for a Kazakhstan resident is **research only**. Before any future trading, check venue licensing, accessible markets, account fees, symbol limits, transfer status, taxes and inventory balancing.
