"""Fail-closed identity registry for cross-venue public spot comparisons.

Do NOT infer asset identity from ticker equality. This registry intentionally
contains only explicitly curated native chain assets; tokens issued through
smart contracts require matching exchange-confirmed chain + contract addresses
and are excluded until that data is available from BOTH venues.

Registry allows research-only price comparisons, NOT confirmation of
deposit/withdrawal routes, network support, legal eligibility or execution.
"""
from dataclasses import dataclass

@dataclass(frozen=True)
class NativeAsset:
    symbol: str
    canonical_id: str
    name: str
    home_network: str

# Explicit asset identity, not an automatic ticker matching rule.
# Do not add ERC-20/SPL contracts here without per-exchange contract verification.
_NATIVE = [
 ('BTC','bitcoin','Bitcoin','bitcoin'),
 ('ETH','ethereum','Ethereum','ethereum'),
 ('SOL','solana','Solana','solana'),
 ('XRP','ripple','XRP Ledger','xrpl'),
 ('BNB','binancecoin','BNB','bsc'),
 ('DOGE','dogecoin','Dogecoin','dogecoin'),
 ('ADA','cardano','Cardano','cardano'),
 ('LTC','litecoin','Litecoin','litecoin'),
 ('AVAX','avalanche-2','Avalanche','avalanche-c'),
 ('TRX','tron','Tron','tron'),
 ('DOT','polkadot','Polkadot','polkadot'),
 ('SUI','sui','Sui','sui'),
 ('NEAR','near','NEAR Protocol','near'),
 ('ATOM','cosmos','Cosmos Hub','cosmoshub'),
 ('ETC','ethereum-classic','Ethereum Classic','ethereum-classic'),
 ('BCH','bitcoin-cash','Bitcoin Cash','bitcoin-cash'),
 ('XLM','stellar','Stellar Lumens','stellar'),
 ('XMR','monero','Monero','monero'),
 ('ZEC','zcash','Zcash','zcash'),
 ('FIL','filecoin','Filecoin','filecoin'),
 ('ICP','internet-computer','Internet Computer','icp'),
 ('HBAR','hedera-hashgraph','Hedera','hedera'),
 ('ALGO','algorand','Algorand','algorand'),
 ('XTZ','tezos','Tezos','tezos'),
 ('DASH','dash','Dash','dash'),
 ('IOTA','iota','IOTA','iota'),
 ('STX','blockstack','Stacks','stacks'),
 ('KAS','kaspa','Kaspa','kaspa'),
 ('APT','aptos','Aptos','aptos'),
 ('TON','the-open-network','Toncoin','ton'),
 ('TAO','bittensor','Bittensor','subtensor'),
 ('EOS','eos','EOS','eos'),
]
ASSETS={s:NativeAsset(s,c,n,net) for s,c,n,net in _NATIVE}
# Blocked tokens include known collisions. Never accept a ticker absent from
# ASSETS, even when two public market listings have the same abbreviation.
KNOWN_COLLISIONS=frozenset({'EDGE','UP'})
IDENTITY_MODE='dual_track_scout_v2'

def resolve_asset(symbol: str):
    return ASSETS.get(str(symbol).upper().strip())

def same_asset(buy_venue: str, sell_venue: str, pair: str) -> bool:
    if not buy_venue or not sell_venue or buy_venue==sell_venue:
        return False
    base,sep,quote=pair.upper().partition('/')
    return bool(sep and quote=='USDT' and base not in KNOWN_COLLISIONS
                and resolve_asset(base) is not None)

def identity_details(pair: str) -> dict:
    base=pair.partition('/')[0].upper()
    a=resolve_asset(base)
    if not a:
        return {'identity_status':'UNVERIFIED_EXCLUDED','canonical_asset_id':None,
                'identity_method': 'NO_EXCHANGE_CONTRACT_MATCH'}
    return {'identity_status':'CURATED_NATIVE','canonical_asset_id':a.canonical_id,
            'asset_name':a.name,'home_network':a.home_network,
            'identity_method':'EXPLICIT_NATIVE_ASSET_REGISTRY',
            'transfer_network_verified':False}
