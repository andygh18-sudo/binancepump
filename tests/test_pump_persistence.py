import tempfile
import unittest
from pathlib import Path

from scanner.pump_persistence import load_state, update_monitor


class PumpPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.json"
        self.state = load_state(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def row(price, score=86, buy=0.62, volume=1.4, accel=1.3, cvd=0.15, exhaustion=20):
        return {
            "symbol": "MAGICUSDT",
            "price": price,
            "_fast_pump_score": score,
            "buy_pressure": buy,
            "volume_ratio": volume,
            "trade_accel": accel,
            "_fast_cvd": cvd,
            "_fast_dynamic_exhaustion": exhaustion,
        }

    def test_starts_episode_and_does_not_confirm_immediately(self):
        row = self.row(1.0)
        alerts = update_monitor([row], [row], self.state, 1000, self.path)
        self.assertEqual(alerts, [])
        self.assertIn("MAGICUSDT", self.state["active"])
        self.assertFalse(row["pump_persistence_confirmed"])

    def test_confirms_after_five_minutes_with_persistent_price_and_flow(self):
        row = self.row(1.0)
        update_monitor([row], [row], self.state, 1000, self.path)
        row2 = self.row(1.012)
        alerts = update_monitor([row2], [row2], self.state, 1300, self.path)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "CONFIRMED")
        self.assertAlmostEqual(alerts[0]["return_pct"], 1.2, places=2)
        self.assertTrue(row2["pump_persistence_confirmed"])

    def test_confirmed_episode_emits_deterioration_once(self):
        row = self.row(1.0)
        update_monitor([row], [row], self.state, 1000, self.path)
        confirm = self.row(1.012)
        update_monitor([confirm], [confirm], self.state, 1300, self.path)
        drop = self.row(0.985, buy=0.42, volume=0.6, accel=0.6, cvd=-0.3)
        alerts = update_monitor([drop], [], self.state, 1310, self.path)
        self.assertEqual([a["type"] for a in alerts], ["DETERIORATED"])
        alerts2 = update_monitor([drop], [], self.state, 1320, self.path)
        self.assertEqual(alerts2, [])

    def test_state_survives_reload_and_completes_after_thirty_minutes(self):
        row = self.row(1.0)
        update_monitor([row], [row], self.state, 1000, self.path)
        restored = load_state(self.path)
        self.assertIn("MAGICUSDT", restored["active"])
        row2 = self.row(1.04)
        update_monitor([row2], [row2], restored, 2800, self.path)
        self.assertNotIn("MAGICUSDT", restored["active"])
        self.assertEqual(restored["completed"][-1]["outcome"], "SUSTAINED_PUMP")
        self.assertEqual(restored["completed"][-1]["sample_count"], 2)


if __name__ == "__main__":
    unittest.main()
