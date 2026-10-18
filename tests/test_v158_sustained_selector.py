import json
import os
import tempfile
import unittest

from scanner.v158_sustained_selector import update_global_top2, acknowledge_global_top2


def candidate(symbol, signal=90, p5=2.0, p10=3.0, p15=4.0, buy=0.72,
              accel=2.0, volume=3.0, cvd=1.0, exhaustion=10):
    return {
        "symbol": symbol,
        "_sustained_pump_score": signal,
        "price_5m": p5,
        "price_10m": p10,
        "price_15m": p15,
        "buy_pressure": buy,
        "trade_accel": accel,
        "volume_ratio": volume,
        "cvd": cvd,
        "exhaustion_score": exhaustion,
    }


class SustainedSelectorTests(unittest.TestCase):
    def test_ranks_globally_and_alerts_at_most_two(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            alerts, ranked = update_global_top2(
                [candidate("AAAUSDT"), candidate("BBBUSDT", signal=85),
                 candidate("CCCUSDT", signal=60)],
                now=1000, path=path,
            )
            self.assertEqual(len(ranked), 3)
            self.assertEqual([r["symbol"] for r in ranked[:2]], ["AAAUSDT", "BBBUSDT"])
            self.assertLessEqual(len(alerts), 2)
            self.assertEqual({r["symbol"] for r in alerts}, {"AAAUSDT", "BBBUSDT"})
            with open(path, encoding="utf-8") as f:
                state = json.load(f)
            self.assertEqual(state["top2"], ["AAAUSDT", "BBBUSDT"])
            self.assertEqual(state["alerted_top2"], [])
            for row in alerts:
                acknowledge_global_top2(row["symbol"], row["global_rank_score"], True, now=1001, path=path)
            with open(path, encoding="utf-8") as f:
                state = json.load(f)
            self.assertEqual(set(state["alerted_top2"]), {"AAAUSDT", "BBBUSDT"})

    def test_persists_alert_history_and_does_not_repeat_unchanged_alerts(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            candidates = [candidate("AAAUSDT"), candidate("BBBUSDT", signal=85)]
            first, _ = update_global_top2(candidates, now=1000, path=path)
            for row in first:
                acknowledge_global_top2(row["symbol"], row["global_rank_score"], True, now=1001, path=path)
            second, ranked = update_global_top2(candidates, now=1010, path=path)
            self.assertEqual(len(first), 2)
            self.assertEqual(second, [])
            self.assertEqual(len(ranked), 2)
            with open(path, encoding="utf-8") as f:
                state = json.load(f)
            self.assertEqual(set(state["alerted_top2"]), {"AAAUSDT", "BBBUSDT"})

    def test_alert_waits_until_symbol_has_been_observed_for_minimum_age(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            old = os.environ.get("SUSTAINED_PUMP_MIN_OBSERVED_SECONDS")
            old_gap = os.environ.get("SUSTAINED_PUMP_OBSERVATION_GAP_SECONDS")
            os.environ["SUSTAINED_PUMP_MIN_OBSERVED_SECONDS"] = "1800"
            os.environ["SUSTAINED_PUMP_OBSERVATION_GAP_SECONDS"] = "2000"
            try:
                candidates = [candidate("AAAUSDT")]
                first, _ = update_global_top2(
                    candidates, observed_symbols=["AAAUSDT"], now=1000, path=path
                )
                self.assertEqual(first, [])
                second, _ = update_global_top2(
                    candidates, observed_symbols=["AAAUSDT"], now=2799, path=path
                )
                self.assertEqual(second, [])
                third, _ = update_global_top2(
                    candidates, observed_symbols=["AAAUSDT"], now=2801, path=path
                )
                self.assertEqual([r["symbol"] for r in third], ["AAAUSDT"])
            finally:
                if old is None:
                    os.environ.pop("SUSTAINED_PUMP_MIN_OBSERVED_SECONDS", None)
                else:
                    os.environ["SUSTAINED_PUMP_MIN_OBSERVED_SECONDS"] = old
                if old_gap is None:
                    os.environ.pop("SUSTAINED_PUMP_OBSERVATION_GAP_SECONDS", None)
                else:
                    os.environ["SUSTAINED_PUMP_OBSERVATION_GAP_SECONDS"] = old_gap

    def test_observation_age_resets_after_long_gap(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            old = os.environ.get("SUSTAINED_PUMP_MIN_OBSERVED_SECONDS")
            os.environ["SUSTAINED_PUMP_MIN_OBSERVED_SECONDS"] = "1800"
            try:
                update_global_top2([candidate("AAAUSDT")], observed_symbols=["AAAUSDT"], now=1000, path=path)
                alerts, _ = update_global_top2(
                    [candidate("AAAUSDT")], observed_symbols=["AAAUSDT"], now=1400, path=path
                )
                self.assertEqual(alerts, [])
                with open(path, encoding="utf-8") as f:
                    state = json.load(f)
                self.assertEqual(state["observations"]["AAAUSDT"]["first_seen"], 1400)
            finally:
                if old is None:
                    os.environ.pop("SUSTAINED_PUMP_MIN_OBSERVED_SECONDS", None)
                else:
                    os.environ["SUSTAINED_PUMP_MIN_OBSERVED_SECONDS"] = old

    def test_expired_candidates_are_removed(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            update_global_top2([candidate("AAAUSDT")], now=1000, path=path)
            alerts, ranked = update_global_top2(
                [], observed_symbols=["AAAUSDT"], now=3001, path=path
            )
            self.assertEqual(alerts, [])
            self.assertEqual(ranked, [])
            with open(path, encoding="utf-8") as f:
                state = json.load(f)
            self.assertNotIn("AAAUSDT", state["pool"])

    def test_new_replacement_is_not_silently_marked_alerted_before_margin(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            # Establish two alerted leaders.
            first, _ = update_global_top2(
                [candidate("AAAUSDT", signal=95), candidate("BBBUSDT", signal=90)],
                now=1000, path=path,
            )
            for row in first:
                acknowledge_global_top2(row["symbol"], row["global_rank_score"], True, now=1001, path=path)
            # A weak new symbol enters the top two but cannot clear the replacement margin.
            alerts, ranked = update_global_top2(
                [candidate("AAAUSDT", signal=95), candidate("BBBUSDT", signal=60), candidate("CCCUSDT", signal=65)],
                observed_symbols=["AAAUSDT", "BBBUSDT", "CCCUSDT"],
                now=1010, path=path,
            )
            self.assertEqual([r["symbol"] for r in ranked[:2]], ["AAAUSDT", "CCCUSDT"])
            self.assertNotIn("CCCUSDT", [r["symbol"] for r in alerts])
            with open(path, encoding="utf-8") as f:
                state = json.load(f)
            self.assertNotIn("CCCUSDT", state["alerted_top2"])


    def test_unqualified_rows_do_not_start_observation_clock(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            old_min = os.environ.get("SUSTAINED_PUMP_MIN_OBSERVED_SECONDS")
            os.environ["SUSTAINED_PUMP_MIN_OBSERVED_SECONDS"] = "1800"
            try:
                update_global_top2([], observed_symbols=["AAAUSDT"], now=1000, path=path)
                alerts, _ = update_global_top2(
                    [candidate("AAAUSDT")], observed_symbols=["AAAUSDT"], now=1500, path=path
                )
                self.assertEqual(alerts, [])
                with open(path, encoding="utf-8") as f:
                    state = json.load(f)
                self.assertEqual(state["observations"]["AAAUSDT"]["first_seen"], 1500)
            finally:
                if old_min is None:
                    os.environ.pop("SUSTAINED_PUMP_MIN_OBSERVED_SECONDS", None)
                else:
                    os.environ["SUSTAINED_PUMP_MIN_OBSERVED_SECONDS"] = old_min

    def test_failed_delivery_can_retry_and_success_acknowledges(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            candidates = [candidate("AAAUSDT")]
            first, _ = update_global_top2(candidates, now=1000, path=path)
            self.assertEqual([row["symbol"] for row in first], ["AAAUSDT"])
            second, _ = update_global_top2(candidates, now=1020, path=path)
            self.assertEqual(second, [])
            retry, _ = update_global_top2(candidates, now=1061, path=path)
            self.assertEqual([row["symbol"] for row in retry], ["AAAUSDT"])
            acknowledge_global_top2("AAAUSDT", retry[0]["global_rank_score"], True, now=1062, path=path)
            final, _ = update_global_top2(candidates, now=1070, path=path)
            self.assertEqual(final, [])

    def test_non_ascii_symbols_are_ignored(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "top2.json")
            alerts, ranked = update_global_top2([candidate("牛来USDT")], now=1000, path=path)
            self.assertEqual(alerts, [])
            self.assertEqual(ranked, [])

if __name__ == "__main__":
    unittest.main()
