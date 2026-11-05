import time
import unittest
from decimal import Decimal as D
from engine import Book, estimate

def book(name,bids,asks):
    return Book(name,'BTC/USDT',tuple((D(str(a)),D(str(b))) for a,b in bids),
                tuple((D(str(a)),D(str(b))) for a,b in asks),time.monotonic())

class EngineTests(unittest.TestCase):
    def test_two_leg_profit(self):
        a=book('a',[(100,10)],[(101,10)])
        b=book('b',[(102,10)],[(103,10)])
        self.assertGreater(estimate(a,b,50,10,10,10,10)['net_usdt'],0)

    def test_not_enough_depth(self):
        a=book('a',[(100,1)],[(101,D('0.01'))])
        b=book('b',[(102,1)],[(103,1)])
        self.assertIsNone(estimate(a,b,50,10,10))

    def test_not_equal_quote_asset(self):
        a=book('a',[(100,1)],[(101,1)])
        b=Book('b','BTC/USD',((D(102),D(1)),),((D(103),D(1)),),time.monotonic())
        self.assertIsNone(estimate(a,b,50,10,10))

    def test_fee_changes_result(self):
        a=book('a',[(100,10)],[(101,10)])
        b=book('b',[(102,10)],[(103,10)])
        self.assertGreater(estimate(a,b,50,10,10)['net_usdt'],
                           estimate(a,b,50,40,40)['net_usdt'])

    def test_purchase_cost_covers_fee(self):
        a=book('a',[(100,10)],[(101,10)])
        b=book('b',[(102,10)],[(103,10)])
        r=estimate(a,b,50,10,10)
        self.assertLessEqual(r['base_qty']*D(101)+r['buy_fee'],D(50))

if __name__=='__main__':
    unittest.main()
