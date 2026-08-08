import unittest

from api_server import (
    _V4_PROFILE_WEIGHT_OVERRIDES,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)


class V425Handikap14KumUpdateTests(unittest.TestCase):
    def test_bounded_profile_is_selected_and_agf_free(self):
        profile = extract_v4_race_profile("Handikap 14", "1400", "Kum", 10)
        resolved = resolve_v4_profile_weights(profile)

        self.assertEqual(resolved["selectedKey"], "HANDIKAP14|Kum")
        self.assertEqual(resolved["sampleRaces"], 28)
        self.assertTrue(resolved["eligible"])
        self.assertFalse(resolved["agfAllowedForRanking"])
        self.assertEqual(resolved["weights"].get("agf_score", 0.0), 0.0)
        self.assertAlmostEqual(sum(resolved["weights"].values()), 1.0, places=3)

    def test_weight_movement_stays_inside_bounded_gate(self):
        live = _V4_PROFILE_WEIGHT_OVERRIDES["HANDIKAP"]
        resolved = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 14", "1400", "Kum", 10)
        )["weights"]
        live_total = sum(live.values())
        normalized_live = {key: value / live_total for key, value in live.items()}
        keys = set(normalized_live) | set(resolved)
        movements = [
            abs(resolved.get(key, 0.0) - normalized_live.get(key, 0.0))
            for key in keys
        ]

        self.assertLessEqual(max(movements), 0.018001)
        # Runtime weights are rounded to four decimals, adding at most one
        # basis point to the exact pre-rounding 17.499% L1 movement.
        self.assertLessEqual(sum(movements), 0.1752)


if __name__ == "__main__":
    unittest.main()
