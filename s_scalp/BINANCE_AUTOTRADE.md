# S-Scalp Binance Spot — autonomous execution candidate v0.3

Status: OBSERVE_ONLY development branch. Not approved for unattended real-money trading.

This adds a Binance-specific worker without modifying the existing Railway paper dashboard or the S-Arbitrage scanner. Existing Bybit code remains separate. A working Binance Global public feed does NOT establish authenticated Binance Kazakhstan account API access.

## Implemented

- BTCUSDT, ETHUSDT, SOLUSDT public WebSocket quotes and aggregated trades with the existing 20-second momentum and buy-imbalance hypothesis.
- Signed HMAC REST API (Binance Global Spot or official Spot Testnet): symbols, filters, bid/ask, account balances, OPOCO order lists, order statuses and realized fills.
- Exchange-native OPOCO: a LIMIT BUY working order with contingent SELL take-profit and stop-loss. On full fill, the net received base quantity is assigned to the pending OCO. Symbol must enable opoAllowed, otoAllowed and ocoAllowed.
- Guardrails: 25 USDT per order, 100 USDT daily buy budget, 5 USDT daily verified realized loss limit, only one unresolved position, 120-second cooldown, 45-second unfilled entry timeout, stale quote and spread checks.
- Durable SQLite journal records UNKNOWN before sending an order. Unique client IDs, no blind retries after ambiguous replies.
- Five-second reconciliation loop checks list and child statuses and trade fills. P&L is only marked CLOSED after fully accounting for both sides and commissions. Third-asset fees or residual inventory halt further entries.
- Password-locked browser dashboard with market feed, signals, order states, risk and verified results.

## Critical limitations before live use

1. Binance Global versus Binance Kazakhstan: the production API endpoint https://api.binance.com is documented for Binance Global. Do NOT assume it is an authorized trading endpoint for an account tagged Binance KZ. Confirm regional API support, platform, and permitted endpoints with Binance.
2. Partially filled BUYs may remain UNPROTECTED until the entry order fully fills. This build flags an incident and blocks further buying; it does not yet safely liquidate every partial fill unattended.
3. Stop-loss orders do not guarantee the execution price, and outages still create risk. Cancellation is only permitted on a confirmed zero-filled working order.
4. Orders paying fees in BNB or another asset, or leaving dust, do not produce proven realized USDT P&L. They pause for review.
5. Dedicated account/subaccount inventory checks, private execution stream, automated partial-fill rescue, loss alerts and audited end-to-end demo tests are REQUIRED before unattended real funds.
6. The strategy has no demonstrated positive expectancy.

## Read-only monitor (no private API keys)

Python 3.12+:

    pip install -r s_scalp/requirements.txt
    cd s_scalp
    SCALP_DASHBOARD_PASSWORD=local-testing-password COOKIE_SECURE=0 BINANCE_EXECUTION_ENV=observe SCALP_BINANCE_JOURNAL=./data/binance_orders.sqlite3 SCALP_BINANCE_SIGNALS=./data/binance_signals.sqlite3 python binance_bot.py

Browser: http://localhost:8080/

Unit tests:

    cd s_scalp
    python -m unittest discover -s tests -v

Deploy read-only to a separate Railway service with repository root directory /s_scalp, custom Dockerfile.binance, a separate volume mounted at /data and SCALP_DASHBOARD_PASSWORD in private environment variables.

## Arming gates (DO NOT ENABLE UNTIL EXCHANGE VERIFIED)

- BINANCE_EXECUTION_ENV=testnet or live (default observe)
- SCALP_REAL_ORDERS=ON
- SCALP_REAL_RISK_ACK=I_AUTHORIZE_SPOT_TRADING
- BINANCE_API_KEY + BINANCE_API_SECRET (private server variables)
- For live Binance Global: BINANCE_ACCOUNT_PLATFORM=GLOBAL, BINANCE_PLATFORM_API_VERIFIED=YES, BINANCE_LIVE_ACCESS_APPROVED=YES
- SCALP_KILL_SWITCH=1 disables NEW entries, not pending exits or orders already in flight.

Never paste keys into chat, GitHub or logs. API key should grant Spot-trade/necessary read only, no withdrawals, and restrict source IPs. Use a dedicated small balance.

Official references:
- https://developers.binance.com/en/docs/products/spot/rest-api
- https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md
- https://github.com/binance/binance-spot-api-docs/blob/master/faqs/opo.md
- https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
- https://www.binance.com/ru-KZ/blog/all/5167078058634520405
