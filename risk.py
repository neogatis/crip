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

from identity import identity_details

def classify_route(buy: str, sell: str, pair: str) -> dict:
    """Strict explicit registry; KZ eligibility and transfer channels remain unchecked."""
    caution = any(venue in AFSA_CAUTION for venue in (buy, sell))
    region = 'GLOBAL_BINANCE_NOT_KZ' if 'binance_public' in (buy,sell) else (
        'AFSA_CAUTION_REVIEW' if caution else 'LICENSE_NOT_VERIFIED')
    details=identity_details(pair)
    if details['identity_status'] == 'UNVERIFIED_EXCLUDED':
        details['identity_status']='UNVERIFIED_SCOUT'
    return {
        'trade_readiness':'RESEARCH_ONLY',
        'kazakhstan_status':region,
        'asset_identity':details['identity_status'],
        'canonical_asset_id':details.get('canonical_asset_id'),
        'asset_name':details.get('asset_name'),
        'identity_method':details['identity_method'],
        'transfer_network_verified':False,
        'account_fees_verified':False,
        'balances_verified':False,
        'execution_enabled':False,
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
