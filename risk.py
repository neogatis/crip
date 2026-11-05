"""Research-only suitability tags, not legal advice or authorization.

Regulatory reference (checked Nov 2026):
https://afsa.aifc.kz/afsa-warning-on-unlicensed-digital-asset-platforms/
https://afsa.aifc.kz/ru/frequently-asked-questions-faqs/

All currently enabled external venues are research-only.
Public pricing data does not imply permission or ability to transact from Kazakhstan.
"""
from dataclasses import dataclass

# AFSA has specifically cautioned about the following global platforms.
AFSA_CAUTION = frozenset({"okx", "bitget"})

# Recognizable symbols are ONLY a heuristic to reduce token ticker collisions.
# Even these require verification of the exact underlying asset and transfer network.
RECOGNIZABLE_BASES = frozenset({
    "BTC","ETH","SOL","XRP","BNB","DOGE","ADA","LINK","LTC","AVAX",
    "TRX","SUI","DOT","ARB","OP","APT","SHIB","PEPE","UNI",
    "NEAR","ATOM","ETC","BCH","AAVE","XLM","XMR","USDC",
})

def classify_route(buy: str, sell: str, pair: str) -> dict:
    """No eligible real-trading venue is confirmed by this research-only service."""
    base = pair.split("/", 1)[0].upper()
    caution = any(venue in AFSA_CAUTION for venue in (buy, sell))
    return {
        "trade_readiness": "RESEARCH_ONLY",
        "kazakhstan_status": "AFSA_CAUTION_REVIEW" if caution else "LICENSE_NOT_VERIFIED",
        "asset_identity": "TICKER_ONLY_REVIEW" if base not in RECOGNIZABLE_BASES
                         else "TOKEN_NETWORK_CHECK_REQUIRED",
        "account_fees_verified": False,
        "balances_verified": False,
        "execution_enabled": False,
    }

# Source-verified ticker collision: Gate EDGE/USDT is Definitive EDGE on Base;
# Bitget/OKX EDGE/USDT is edgeX EDGE on Ethereum.
# https://www.definitive.fi/blog/definitive-edge-is-live
# https://www.gate.com/ru/announcements/article/50464
# https://www.okx.com/en-eu/help/okx-to-list-edge-edgex-for-spot-trading
# https://www.bitget.com/blog/articles/bitget-lists-edgex-edge-spot-trading-innovation-zone
def is_known_collision(buy: str, sell: str, pair: str) -> bool:
    return (pair.upper() == 'EDGE/USDT' and
            'gate' in (buy, sell) and
            ('bitget' in (buy, sell) or 'okx' in (buy, sell)))
