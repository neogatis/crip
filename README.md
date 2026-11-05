# S-Arbitrage — 0.3 Research Scanner

**Research-only cross-exchange spot USDT scanner**. Uses only public market data, no API keys, no wallet transfers, no real orders. This worker has no public dashboard.

## What it does

- Discovers active common USDT spot pairs across OKX, Bitget, Gate and Kraken (default target 100; capped at 300).
- Subscribes to public order-book snapshots and checks 25/50 USDT simulated two-leg fills using full available depth.
- Every 20 seconds logs a `SPREAD_REPORT` JSON object with three distinct leaderboards:
  - `top_of_book_gross`: best ask vs best bid only. **No depth verification.**
  - `top_depth_adjusted_gross`: simulated buy/sell weighted-average prices for a full 50-USDT trade.
  - `top_estimated_net`: estimated profit after **assumed** buy+sell fees, uncertainty and rebalance reserves. Negative estimates are shown too.
- Saves each report into `spread_reports` in persistent SQLite, and threshold alerts into `signals`.
- Includes `kazakhstan_status` and `asset_identity` *caution* tags. **No route is marked executable.**

## Railway worker

Project `s-arbitrage`, service `s-arbitrage-scanner`. Attach a volume at `/data`, database path `/data/signals.sqlite3`.

Main variables:
- `EXCHANGES=okx,bitget,gate,kraken`
- `DYNAMIC_SYMBOLS=true`, `MAX_PAIRS=100` (can scale progressively, maximum 300)
- `MIN_GATE_VOLUME_USDT=100000` (one exchange's 24h volume only; *not* an overall liquidity guarantee)
- `TRADE_AMOUNTS_USDT=25,50`
- `SCAN_INTERVAL_SECONDS=0.5`, `BOOK_MAX_AGE_MS=1500`
- `MIN_NET_PCT=0.30`; `SUSTAIN_SECONDS=1`; `ALERT_COOLDOWN_SECONDS=60`
- `FEE_BPS_JSON`: *illustrative*, **not actual account-tier fees**
- `UNCERTAINTY_BPS=10`, `REBALANCE_BPS=10`
- Telegram variables optional. Set secrets in Railway, **never in GitHub**.

Use Railway -> Logs; filter `SPREAD_REPORT` and `HEALTH`.

## IMPORTANT: compliance & reliability

AFSA cautions about some globally available unlicensed digital-asset platforms including OKX and Bitget:
https://afsa.aifc.kz/afsa-warning-on-unlicensed-digital-asset-platforms/
https://afsa.aifc.kz/ru/frequently-asked-questions-faqs/

A symbol shared across two exchanges **does not guarantee** that the underlying contract/token/network is identical. Even for commonly recognized tickers, check token identity. `LICENSE_NOT_VERIFIED` means not verified, **not** "allowed". `AFSA_CAUTION_REVIEW` flags caution requiring regulatory review. This app deliberately does not classify any route as permitted for Kazakhstan residents. Do not fund exchanges or trade on the basis of this research data.

Every leaderboard is a **sample of the best observed readings during the latest 20-second window**, not a simultaneous cross-exchange executable order. WebSocket latency, sequence handling, clock skew, commissions, market impact, transfer rules, KYC, minimum sizes, volatility and inventory may invalidate any estimate. A positive estimate is not a prediction.

## Tests

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```
