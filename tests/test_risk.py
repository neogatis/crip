import unittest
from risk import classify_route, is_known_collision

class RiskClassificationTests(unittest.TestCase):
    def test_afsa_warning_route_research_only(self):
        r=classify_route('bitget','kraken','LTC/USDT')
        self.assertEqual(r['kazakhstan_status'],'AFSA_CAUTION_REVIEW')
        self.assertFalse(r['execution_enabled'])
        self.assertEqual(r['trade_readiness'],'RESEARCH_ONLY')

    def test_non_warning_venues_not_automatically_licensed(self):
        r=classify_route('kraken','gate','BTC/USDT')
        self.assertEqual(r['kazakhstan_status'],'LICENSE_NOT_VERIFIED')
        self.assertNotEqual(r['trade_readiness'],'EXECUTABLE')

    def test_unknown_ticker_requires_identity_verification(self):
        r=classify_route('gate','kraken','XYZQQ/USDT')
        self.assertEqual(r['asset_identity'],'TICKER_ONLY_REVIEW')

    def test_definitive_is_not_edgex(self):
        self.assertTrue(is_known_collision('gate','bitget','EDGE/USDT'))
        self.assertTrue(is_known_collision('okx','gate','EDGE/USDT'))
        self.assertFalse(is_known_collision('okx','bitget','EDGE/USDT'))
        self.assertFalse(is_known_collision('gate','bitget','EDGEX/USDT'))

if __name__=='__main__':
    unittest.main()
