# S-Arbitrage — 0.5 Public-data Research Scanner

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

## Asset identity: strict fail-closed mode

Set by `identity.py`: `curated_native_fail_closed_v1`.

1. The pair must be listed as active spot USDT on at least two independent exchanges.
2. The base asset must be an explicitly curated **native-chain asset** with a canonical asset ID and named home blockchain (e.g., BTC = Bitcoin, ETH = Ethereum, STX = Stacks). It is NOT enough for two tickers to match.
3. **All tokens outside this registry are excluded entirely** from comparison, alerts and current dashboard reports. Known collisions EDGE (Definitive/edgeX) and UP cannot appear.
4. Smarter contract tokens can be added later only after both exchanges expose independently verifiable network + contract addresses that match. There is **no automatic smart-contract verification yet**. Do not silently whitelist token tickers.
5. The explicit native registry is a conservative identity control; it does **not** prove an exchange's deposit/withdraw network, account-specific availability or actual trading legitimacy. This is marked `transfer_network_verified=false` in every record.
6. Previous SQLite history is retained for audit; pre-registry snapshots are **hidden** from the current dashboard.

This deliberately reduces coverage from ~190 same-ticker pairs to ~29 known native assets. No false claims of a 200-coin verified universe.

## Railway

Project `s-arbitrage`; service `s-arbitrage-scanner`, deployment region Netherlands, volume at `/data`, database `/data/signals.sqlite3`.

Environment:
- `EXCHANGES=okx,bitget,gate,kraken,binance_public,bybit_kz`
- `DYNAMIC_SYMBOLS=true`
- `MAX_PAIRS=200` (upper bound, **not** the effective count)
- `MIN_GATE_VOLUME_USDT=100000`
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
