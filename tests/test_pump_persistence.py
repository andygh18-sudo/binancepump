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
        alerts = []
        for i in range(6):
            price = 1.0 + 0.012 * (i / 5.0)
            row = self.row(price)
            alerts = update_monitor([row], [row], self.state, 1000 + 60 * i, self.path)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "CONFIRMED")
        self.assertAlmostEqual(alerts[0]["return_pct"], 1.2, places=2)
        self.assertTrue(row["pump_persistence_confirmed"])

    def test_confirmed_episode_emits_deterioration_once(self):
        for i in range(6):
            confirm = self.row(1.0 + 0.012 * (i / 5.0))
            update_monitor([confirm], [confirm], self.state, 1000 + 60 * i, self.path)
        drop = self.row(0.985, buy=0.42, volume=0.6, accel=0.6, cvd=-0.3)
        alerts = update_monitor([drop], [], self.state, 1310, self.path)
        self.assertEqual([a["type"] for a in alerts], ["DETERIORATED"])
        alerts2 = update_monitor([drop], [], self.state, 1320, self.path)
        self.assertEqual(alerts2, [])

    def test_state_survives_reload_and_emits_sustained_outcome_after_thirty_minutes(self):
        row = self.row(1.0)
        update_monitor([row], [row], self.state, 1000, self.path)
        restored = load_state(self.path)
        self.assertIn("MAGICUSDT", restored["active"])
        row2 = self.row(1.04)
        alerts = update_monitor([row2], [row2], restored, 2800, self.path)
        self.assertNotIn("MAGICUSDT", restored["active"])
        self.assertEqual(restored["completed"][-1]["outcome"], "SUSTAINED_PUMP")
        self.assertEqual(restored["completed"][-1]["sample_count"], 2)
        outcome_alerts = [a for a in alerts if a.get("type") == "OUTCOME"]
        self.assertEqual(len(outcome_alerts), 1)
        self.assertEqual(outcome_alerts[0]["outcome"], "SUSTAINED_PUMP")
        self.assertEqual(outcome_alerts[0]["elapsed_minutes"], 30.0)

    def test_failed_burst_and_no_clear_pump_each_emit_one_final_outcome(self):
        scenarios = [
            ("FAILED_BURST", [(1000, 1.0), (1600, 1.02), (2800, 1.001)]),
            ("NO_CLEAR_PUMP", [(1000, 1.0), (1600, 1.002), (2800, 1.001)]),
        ]
        for expected, points in scenarios:
            with self.subTest(outcome=expected):
                state = load_state(self.path)
                for idx, (ts, price) in enumerate(points):
                    row = self.row(price)
                    alerts = update_monitor([row], [row], state, ts, self.path)
                    if idx < len(points) - 1:
                        self.assertFalse(any(a.get("type") == "OUTCOME" for a in alerts))
                outcome_alerts = [a for a in alerts if a.get("type") == "OUTCOME"]
                self.assertEqual(len(outcome_alerts), 1)
                self.assertEqual(outcome_alerts[0]["outcome"], expected)
                self.assertNotIn("MAGICUSDT", state["active"])
                self.assertEqual(state["completed"][-1]["outcome"], expected)


if __name__ == "__main__":
    unittest.main()
