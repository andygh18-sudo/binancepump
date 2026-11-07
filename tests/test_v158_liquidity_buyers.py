import unittest
from scanner.v158_liquidity_buyers import rank_liquidity_buyers

def row(symbol, buy=0.75, volume=25_000_000, bid=200_000, ask=150_000):
    return {"symbol":symbol,"book_ready":True,"buy_pressure":buy,"trade_accel":2.0,
            "buyer_monitor_trades_10s":10,"spread_bps":3.0,"bid_depth_usdt":bid,
            "ask_depth_usdt":ask,"adaptive_cvd_z":1.5,"adaptive_trade_z":1.2,
            "quote_volume_24h_usdt":volume}

class LiquidityBuyerTests(unittest.TestCase):
    def test_ranks_and_limits_to_ten(self):
        result=rank_liquidity_buyers([row(f"T{i}USDT", buy=0.5+i*0.02) for i in range(15)])
        self.assertEqual(len(result),10)
        self.assertEqual([r["liquidity_buyer_rank"] for r in result],list(range(1,11)))
        self.assertGreaterEqual(result[0]["liquidity_buyer_combined_score"],result[-1]["liquidity_buyer_combined_score"])
    def test_excludes_unready_illiquid_and_wide_spread(self):
        bad=row("BADUSDT");bad["book_ready"]=False
        thin=row("THINUSDT",volume=1000)
        wide=row("WIDEUSDT");wide["spread_bps"]=30
        self.assertEqual(rank_liquidity_buyers([bad,thin,wide]),[])
if __name__=="__main__": unittest.main()
