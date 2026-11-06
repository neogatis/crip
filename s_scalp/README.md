# S-Scalp v0.1 — public live feed / virtual spot execution

Completely separate from S-Arbitrage. This is an experimental long-only paper trading tool, NOT an automated exchange trader.

## Features
- Public Binance Global spot market-data-only WebSocket and public Bybit Kazakhstan spot WebSocket.
- Real-time best bid/ask and trade prints for BTCUSDT, ETHUSDT and SOLUSDT per venue.
- Momentum hypothesis: 20-second window; rise >=0.30%; >=12 prints across >=8 seconds; >=60% taker-buy notional; spread <=0.08%; quote age <=2.5 seconds.
- Paper buy at ask + 5 bps simulated slippage, paper sell at bid - 5 bps simulated slippage, assumed 10 bps taker fee per side. These are assumptions, not measured actual execution costs.
- Max two concurrent positions, 50 USDT each, no duplicate underlying exposure, initial balance 500 USDT, UTC daily loss gate of 10 USDT realized P&L, 120-second symbol cooldown, -0.6% net stop, +0.6% net take, 120-second hold timeout.
- Persistent SQLite state; auto-reconnecting market data. Mobile-friendly read-only password-protected dashboard.
- No order placement, exchange authentication, API keys, withdrawals, transfers, leverage or derivatives. Execution is permanently disabled.

## Local

Python 3.12+ required. From the GitHub repository root:

    pip install -r s_scalp/requirements.txt
    cd s_scalp
    SCALP_DASHBOARD_PASSWORD=local-secret COOKIE_SECURE=0 python app.py

Open http://localhost:8080/ . Until feeds connect, the screen reports no fresh data.

Unit tests:

    cd s_scalp
    python -m unittest discover -s tests -v

## Railway deployment (separate service)

Use a NEW service, with GitHub repo neogatis/crip and root directory /s_scalp; do not touch s-arbitrage-scanner. Root directory holds its own Dockerfile. Set environment:

    SCALP_DASHBOARD_PASSWORD=<unique secret>
    SCALP_DB_PATH=/data/scalp.sqlite3

Mount a NEW persistent volume at /data for state across deploys. Do not point this service to the S-Arbitrage database. No password -> dashboard / private status disabled (503). /health remains public.

## Important limitations

- Virtual fills at observed top of book are **not real fills**. This ignores order latency, queues, partials, market impact, symbol-specific order filters and minimum sizes.
- During a feed outage, an open position is retained and stop orders are NOT actually placed. Stop thresholds do not guarantee a maximum loss.
- P&L is simulated. Trading fees are configurable assumptions only in source; win rate is not evidence of an edge. Robust out-of-sample and replay testing would be necessary prior to considering real trading.
- Public feed availability does not imply actual trading eligibility in Kazakhstan.

Official documentation:
- https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md
- https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
- https://bybit-exchange.github.io/docs/v5/ws/connect
- https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
