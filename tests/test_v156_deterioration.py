import tempfile
import unittest
from pathlib import Path

import scanner.v156_deterioration as mod


def bad_row(price=100.0):
    return {
        "price": price, "price_10s": -0.20, "price_60s": -0.10,
        "buy_pressure": 0.50, "trade_accel": 1.00, "volume_ratio": 0.80,
        "v15_relative_strength_5m": -0.20, "exhaustion_score": 80,
        "v158_pre_ignition_score": 20, "v158_liquidity_state_score": 20,
        "v158_participation_score": 20, "v158_directional_score": 20,
        "tv_5m_rsi": 40, "tv_30m_rsi": 40, "tv_1h_rsi": 45, "tv_4h_rsi": 45,
    }


class TestV156Deterioration(unittest.TestCase):
    def setUp(self):
        self.old = (mod.V156_GRACE, mod.V156_GAP, mod.V156_CONFIRM)
        mod.V156_GRACE, mod.V156_GAP, mod.V156_CONFIRM = 30.0, 60.0, 3

    def tearDown(self):
        mod.V156_GRACE, mod.V156_GAP, mod.V156_CONFIRM = self.old

    def test_no_episode_without_real_buy(self):
        with tempfile.TemporaryDirectory() as d:
            m = mod.V156DeteriorationMonitor(Path(d) / "state.jsonl")
            self.assertIsNone(m.observe("TESTUSDT", bad_row(), 100))

    def test_time_separated_confirmation(self):
        with tempfile.TemporaryDirectory() as d:
            m = mod.V156DeteriorationMonitor(Path(d) / "state.jsonl")
            m.start_episode("TESTUSDT", 100, 90, 0)
            self.assertIsNone(m.observe("TESTUSDT", bad_row(), 31))
            self.assertIsNone(m.observe("TESTUSDT", bad_row(), 60))
            self.assertFalse(m.observe("TESTUSDT", bad_row(), 90)["alert"])
            self.assertFalse(m.observe("TESTUSDT", bad_row(), 150)["alert"])
            self.assertTrue(m.observe("TESTUSDT", bad_row(), 210)["alert"])

    def test_state_survives_reload(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "state.jsonl"
            m = mod.V156DeteriorationMonitor(p)
            eid = m.start_episode("TESTUSDT", 100, 88, 0)
            reloaded = mod.V156DeteriorationMonitor(p)
            self.assertEqual(reloaded.episodes["TESTUSDT"]["episode_id"], eid)
            self.assertTrue(reloaded.episodes["TESTUSDT"]["active"])


if __name__ == "__main__":
    unittest.main()
