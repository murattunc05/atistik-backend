import unittest

from api_server import (
    _V4_PROFILE_WEIGHT_OVERRIDES,
    _V4_VERSION,
    _V422_CANDIDATE_VERSION,
    _V424_FULL_ONLY_SAMPLE_RACES,
    _V424_GROUP_WINNER_TOP3_BLENDS,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)


class V424GroupMetricUpdateTest(unittest.TestCase):
    def test_visible_version_and_handicap_shadow_version_are_distinct(self):
        self.assertEqual(_V4_VERSION, "4.25")
        self.assertEqual(_V422_CANDIDATE_VERSION, "4.22-handicap-candidate")

    def test_maiden_winner_top3_blend_raises_distance_signal(self):
        raw = _V4_PROFILE_WEIGHT_OVERRIDES["MAIDEN"]
        self.assertAlmostEqual(sum(raw.values()), 93.0, places=6)
        self.assertAlmostEqual(raw["degree_avg"], 8.36, places=6)
        self.assertAlmostEqual(raw["agf_score"], 14.72, places=6)
        self.assertEqual(_V424_GROUP_WINNER_TOP3_BLENDS["MAIDEN"], {"distance_suit": 0.08})

        profile = extract_v4_race_profile("Maiden/Dişi", "1400", "Kum", 10)
        resolved = resolve_v4_profile_weights(profile)

        self.assertEqual(resolved["selectedKey"], "MAIDEN")
        self.assertTrue(resolved["agfAllowedForRanking"])
        self.assertEqual(resolved["sampleRaces"], 97)
        self.assertAlmostEqual(resolved["weights"]["distance_suit"], 0.0891, places=4)
        self.assertAlmostEqual(resolved["weights"]["degree_avg"], 0.0827, places=4)
        self.assertAlmostEqual(resolved["weights"]["agf_score"], 0.1456, places=4)
        self.assertLess(resolved["weights"]["agf_score"], 16.0 / 93.0)

    def test_sartli_winner_top3_blend_applies_to_each_subtype(self):
        self.assertEqual(
            _V424_GROUP_WINNER_TOP3_BLENDS["SARTLI"],
            {"bounce_score": 0.04, "track_experience_score": 0.04},
        )
        for race_type in ("Şartlı 1", "Şartlı 3", "Şartlı 4", "Şartlı 5"):
            with self.subTest(race_type=race_type):
                resolved = resolve_v4_profile_weights(
                    extract_v4_race_profile(race_type, "1400", "Kum", 10)
                )
                self.assertGreaterEqual(resolved["weights"]["bounce_score"], 0.04)
                self.assertGreaterEqual(resolved["weights"]["track_experience_score"], 0.04)
                self.assertAlmostEqual(sum(resolved["weights"].values()), 1.0, places=3)

    def test_full_only_sample_thresholds_are_refreshed_per_profile(self):
        self.assertEqual(_V424_FULL_ONLY_SAMPLE_RACES["HANDIKAP"], 193)
        self.assertEqual(_V424_FULL_ONLY_SAMPLE_RACES["KV"], 95)
        self.assertEqual(_V424_FULL_ONLY_SAMPLE_RACES["SART4"], 81)

        h16 = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 16", "1400", "Sentetik", 10)
        )
        h16_cim = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 16", "1400", "Çim", 10)
        )
        sart1 = resolve_v4_profile_weights(
            extract_v4_race_profile("Şartlı 1", "1200", "Kum", 10)
        )

        self.assertEqual((h16["sampleRaces"], h16["minRequired"], h16["eligible"]), (5, 12, False))
        self.assertEqual((h16_cim["sampleRaces"], h16_cim["minRequired"], h16_cim["eligible"]), (31, 8, True))
        self.assertEqual((sart1["sampleRaces"], sart1["minRequired"], sart1["eligible"]), (18, 12, True))


if __name__ == "__main__":
    unittest.main()
