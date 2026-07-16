import unittest

from api_server import (
    _V4_PROFILE_WEIGHT_OVERRIDES,
    _V4_VERSION,
    _V422_CANDIDATE_VERSION,
    _V423_FULL_ONLY_SAMPLE_RACES,
    extract_v4_race_profile,
    resolve_v4_profile_weights,
)


class V423GroupMetricUpdateTest(unittest.TestCase):
    def test_visible_version_and_handicap_shadow_version_are_distinct(self):
        self.assertEqual(_V4_VERSION, "4.23")
        self.assertEqual(_V422_CANDIDATE_VERSION, "4.22-handicap-candidate")

    def test_maiden_update_keeps_raw_total_and_reduces_agf_share(self):
        raw = _V4_PROFILE_WEIGHT_OVERRIDES["MAIDEN"]
        self.assertAlmostEqual(sum(raw.values()), 93.0, places=6)
        self.assertAlmostEqual(raw["degree_avg"], 8.36, places=6)
        self.assertAlmostEqual(raw["agf_score"], 14.72, places=6)

        profile = extract_v4_race_profile("Maiden/Dişi", "1400", "Kum", 10)
        resolved = resolve_v4_profile_weights(profile)

        self.assertEqual(resolved["selectedKey"], "MAIDEN")
        self.assertTrue(resolved["agfAllowedForRanking"])
        self.assertEqual(resolved["sampleRaces"], 57)
        self.assertAlmostEqual(resolved["weights"]["degree_avg"], 0.0899, places=4)
        self.assertAlmostEqual(resolved["weights"]["agf_score"], 0.1583, places=4)
        self.assertLess(resolved["weights"]["agf_score"], 16.0 / 93.0)

    def test_full_only_sample_thresholds_are_refreshed_per_profile(self):
        self.assertEqual(_V423_FULL_ONLY_SAMPLE_RACES["HANDIKAP"], 120)
        self.assertEqual(_V423_FULL_ONLY_SAMPLE_RACES["KV"], 52)
        self.assertEqual(_V423_FULL_ONLY_SAMPLE_RACES["SART4"], 55)

        h16 = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 16", "1400", "Sentetik", 10)
        )
        h16_cim = resolve_v4_profile_weights(
            extract_v4_race_profile("Handikap 16", "1400", "Çim", 10)
        )
        sart1 = resolve_v4_profile_weights(
            extract_v4_race_profile("Şartlı 1", "1200", "Kum", 10)
        )

        self.assertEqual((h16["sampleRaces"], h16["minRequired"], h16["eligible"]), (4, 12, False))
        self.assertEqual((h16_cim["sampleRaces"], h16_cim["minRequired"], h16_cim["eligible"]), (17, 8, True))
        self.assertEqual((sart1["sampleRaces"], sart1["minRequired"], sart1["eligible"]), (7, 12, False))


if __name__ == "__main__":
    unittest.main()
