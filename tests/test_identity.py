import unittest
from identity import ASSETS,IDENTITY_MODE,resolve_asset,same_asset
from discovery import parse_markets,select_universe

class IdentityGateTests(unittest.TestCase):
    def test_blocks_unverified_ambiguous_tickers(self):
        for symbol in ('EDGE','UP','KOR','ARIA','IRYS','HUMA','PEPE','LINK','USDC','SHIB'):
            self.assertIsNone(resolve_asset(symbol))
            self.assertFalse(same_asset('gate','bitget',symbol+'/USDT'))
    def test_native_registry_has_explicit_identity(self):
        self.assertEqual(resolve_asset('BTC').canonical_id,'bitcoin')
        self.assertEqual(resolve_asset('ETH').home_network,'ethereum')
        self.assertTrue(same_asset('binance_public','bybit_kz','BTC/USDT'))
        self.assertFalse(same_asset('gate','gate','BTC/USDT'))
        self.assertFalse(same_asset('gate','bitget','BTC/USD'))
        self.assertEqual(len(set(a.canonical_id for a in ASSETS.values())),len(ASSETS))
    def test_universe_fail_closed(self):
        known={'gate':{'EDGE','BTC','UP','ETH','ARB'},
               'bitget':{'EDGE','BTC','UP','ETH','ARB'},
               'bybit_kz':{'EDGE','BTC','UP','ETH'}}
        result=select_universe(known,max_pairs=200,min_exchanges=2)
        self.assertEqual(set(result),{'BTC','ETH'})
        self.assertNotIn('EDGE',result)
    def test_binance_spot_only_and_exact_symbols(self):
        data={'symbols':[
            {'baseAsset':'BTC','quoteAsset':'USDT','status':'TRADING','isSpotTradingAllowed':True,'symbol':'BTCUSDT'},
            {'baseAsset':'ETH','quoteAsset':'USDT','status':'BREAK','symbol':'ETHUSDT'},
            {'baseAsset':'SOL','quoteAsset':'USDC','status':'TRADING','symbol':'SOLUSDC'},
            {'baseAsset':'DOGE','quoteAsset':'USDT','status':'TRADING','isSpotTradingAllowed':False,'symbol':'DOGEUSDT'}
        ]}
        self.assertEqual(parse_markets('binance_public',data),{'BTC'})
    def test_bybit_only_trading_spot(self):
        data={'retCode':0,'result':{'list':[
            {'baseCoin':'BTC','quoteCoin':'USDT','status':'Trading','symbol':'BTCUSDT'},
            {'baseCoin':'ETH','quoteCoin':'USDT','status':'Suspended','symbol':'ETHUSDT'},
            {'baseCoin':'ETH','quoteCoin':'USDC','status':'Trading','symbol':'ETHUSDC'}]}}
        self.assertEqual(parse_markets('bybit_kz',data),{'BTC'})
        with self.assertRaises(ValueError):
            parse_markets('bybit_kz',{'retCode':10001,'retMsg':'error'})

if __name__=='__main__':
    unittest.main()
