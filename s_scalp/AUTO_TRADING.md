# S-Scalp automatic trading prototype (v0.2, isolated branch)

**Not yet live/production ready.** This integration provides real exchange order
submission code, but is OBSERVE_ONLY unless all explicit controls are configured.
No API keys or live orders have been supplied or placed.

## What is implemented
- Autonomous signal listener for Bybit Kazakhstan spot market data, with a
  separate Bybit testnet endpoint mode for non-production testing.
- The same 20-second price momentum / buy imbalance hypothesis as paper S-Scalp.
- Restricts entries to BTCUSDT, ETHUSDT, SOLUSDT on the same venue as the account.
- Fresh ticker double-check, bid/ask spread check, symbol precision/minimums,
  USDT balance check, default 25 USDT maximum committed lifetime spend.
- Spot-only limit IOC BUY with native take-profit/stop-loss settings attached.
  No leverage, no futures, no short selling.
- SQLite durable intent saved BEFORE order submission and unique orderLinkId.
  On unknown network result, HALT; never blindly retry an order.
- Any acknowledged order blocks further new buys, including after restart.
  API acknowledgement is NOT execution confirmation, and protective sell orders
  MUST be verified on the actual venue/account before live use.
- Strict default-off switches; tests use fake venue methods with no network.

## Caveats / why not safe to run unattended with real money yet
The Bybit Spot REST API returns order acceptance asynchronously. Confirming
execution, partial fills, fee currency, and effective TP/SL child order status
requires private order/execution WebSocket + exchange reconciliation. This alpha
does not yet automatically verify native stops or compute actual realized P&L.
It can submit at most one acknowledged entry until manually reconciled, and it
does NOT close positions itself except via exchange-native attached protective
TP/SL (which remain unverified until tested with the right account mode).
Service outages, native stop failures, price gaps and slippage can still
cause losses beyond model estimates. Do not arm production yet.

## Local safety testing

    pip install -r s_scalp/requirements.txt
    cd s_scalp
    python -m unittest discover -s tests -v

To run read-only signal observation:

    cd s_scalp
    SCALP_VENUE_ENV=kz SCALP_AUTO_JOURNAL=./data/auto.sqlite3 SCALP_SIGNAL_DB=./data/signals.sqlite3 python auto_bot.py

Without exchange credentials or real order flags, auto_bot.py only logs
eligible signals, never submits orders. It is separate from the deployed paper
dashboard; S-Arbitrage and S-Scalp paper are unaffected.

## Future deployment (NOT applied)

Create a dedicated Railway service with root /s_scalp, Dockerfile.auto, a
dedicated persistent volume at /data, and a Bybit-approved egress location.
The currently deployed paper service is in the US (sfo), and Bybit restricts
requests from US IPs (403); do NOT assume a successful trading connection.
Validate location and exchange usage terms with venue support first.

For testnet, create separate testnet credentials in the TESTNET account.
The standard API testnet URL is api-testnet.bybit.com; this is not evidence
that Kazakhstan exchange-specific testnet accounts can use it.

NEVER paste API keys into chat or GitHub. Store secrets in Railway private vars,
apply IP restrictions, give spot-trading permissions only and NO withdrawals.

Arming conditions (not to be enabled until review):
- SCALP_VENUE_ENV (testnet default; set kz only after access confirmed)
- BYBIT_API_KEY + BYBIT_API_SECRET
- SCALP_REAL_ORDERS=ENABLED
- SCALP_REAL_RISK_ACK=I_APPROVE_SPOT_RISK
- SCALP_PROTECTIVE_ORDERS_VERIFIED=YES

These must remain OFF until independent venue/account tests verify attached
TP/SL behavior on spot IOC orders. Before production, implement authenticated
reconciliation, partial-fill handling, kill-switch, realized loss controls,
balance protection and private order streams.

Official references:
- https://bybit-exchange.github.io/docs/v5/guide
- https://bybit-exchange.github.io/docs/v5/order/create-order
- https://bybit-exchange.github.io/docs/v5/order/open-order
- https://bybit-exchange.github.io/docs/v5/market/instrument
- https://afsa.aifc.kz/ru/frequently-asked-questions-faqs/
