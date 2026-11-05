# S-Arbitrage — 0.6 Dual-track Public-data Research Scanner

**Spot USDT spread research, not a trading system.** No private keys, wallet transfers, signed APIs or order submission.

## Dashboard

https://s-arbitrage-scanner-production.up.railway.app/

Password is stored only in the Railway environment variable `DASHBOARD_PASSWORD` (never in GitHub). The interface shows live feed health, spread rankings, estimated fees, filters for modeled trade amounts (25/50 USDT), and history. It refreshes every 5 seconds; reports are saved every 20 seconds in the mounted SQLite database.

## Connected feeds

Public spot order books from:
- **OKX**, **Bitget**, **Gate.io**, **Kraken**
- **Binance Global public** (`binance_public`) — **NOT Binance Kazakhstan**, and not proof of account access.
- **Bybit Kazakhstan public** (`bybit_kz`) at `stream.bybit.kz`.

Public spot market catalogs are discovered from each venue's REST endpoint; only actively trading USDT spot markets are subscribed to. A working WebSocket feed does **not** imply users in Kazakhstan are eligible to trade.

## Identity & broad scout mode (dual_track_scout_v2)

**We monitor broadly without treating ticker equality as identity verification.**

1. Dynamic discovery selects up to 200 USDT spot tickers listed on at least two exchanges. Curated native assets (BTC, ETH, STX etc) get first priority, then higher-volume market candidates. A low Gate volume alone does not exclude an otherwise listed scout candidate.
2. **Отобранные**: a conservative curated *native-asset* registry provides a canonical asset ID. These routes receive full depth estimates and threshold research alerts. Even here, exchange-by-exchange deposits, networks, fees and account access are NOT verified.
3. **Кандидаты**: all other overlapping tickers (PEPE, LINK, SHIB and many more) are visible with depth-adjusted hypothetical estimates, but **never generate trade/profit alerts** because token identity is unconfirmed.
4. **Аномалии**: candidate gross spreads ≥10% are displayed in a separate area. High differences often represent different assets or invalid quotes, **not windfall profit**. Known proven ticker collisions (e.g. Gate Definitive EDGE vs OKX/Bitget edgeX EDGE) are blocked entirely.
5. A token can move into a trusted category only after the **same chain + token contract** is confirmed from both exchanges' independent metadata, or by a reviewed mapping. That automatic cross-venue contract comparison is still **not implemented**; public metadata differs by exchange. Gate does expose chains and contract addresses through `GET /spot/currencies`, but not every venue has equivalent unauthenticated metadata.
6. All data are strictly research-only. Real exchange eligibility in Kazakhstan, networks, transfers and fees are not checked.

Historical reports remain in SQLite; old runs from different identity regimes are hidden from the current live dashboard but retained for audit.

## Railway

Project `s-arbitrage`; service `s-arbitrage-scanner`, deployment region Netherlands, volume at `/data`, database `/data/signals.sqlite3`.

Environment:
- `EXCHANGES=okx,bitget,gate,kraken,binance_public,bybit_kz`
- `DYNAMIC_SYMBOLS=true`
- `MAX_PAIRS=200` (target upper bound; active catalogs and subscription limits determine actual coverage)
- `MIN_GATE_VOLUME_USDT=100000` (ignored as an exclusion in scout mode; used for ranking)
- `TRADE_AMOUNTS_USDT=25,50`, `SCAN_INTERVAL_SECONDS=0.5`
- `BOOK_MAX_AGE_MS=1500`, `MIN_NET_PCT=0.30`, `SUSTAIN_SECONDS=1`
- `UNCERTAINTY_BPS=10`, `REBALANCE_BPS=10`
- `SQLITE_PATH=/data/signals.sqlite3`
- `DASHBOARD_PASSWORD` is secret; no default checked into source.

Indicators: `HEALTH` logs live order-book updates per venue. `SPREAD_REPORT` logs ranked *research estimates*, with `identity_mode` and `asset_identity`, as well as modeled costs. Top-of-book gross does not account for depth; depth-adjusted net uses illustrative maker/taker fees, uncertainty and rebalance buffer. No report is an executable quote.

## Tests

Docker builds run all unit tests before releasing an image:

```shell
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

## Limitations / Kazakhstan

This is **not** a real-arbitrage auto trader. Exchange user eligibility, token contracts, compatible transfer networks, minimum withdrawal sizes, fee tiers, slippage, fills, speed, capital rebalancing and taxes are **unverified**. A shared ticker or native-asset name cannot substitute for actual network validation. AFSA cautions about some international digital-asset platforms; do not use the scanner to bypass regional access restrictions. On every route, `execution_enabled=false` and `trade_readiness=RESEARCH_ONLY`.

Relevant official docs:
- https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md
- https://bybit-exchange.github.io/docs/v5/guide
- https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- https://afsa.aifc.kz/afsa-warning-on-unlicensed-digital-asset-platforms/
