import json
import unittest
from pathlib import Path

import api_server as api


ROOT = Path(__file__).parent
STATS_PATH = ROOT / "feature_stats_shadow.json"


class ShadowModelDeploymentTest(unittest.TestCase):
    def test_deployed_artifact_is_strict_no_agf_candidate(self):
        payload = json.loads(STATS_PATH.read_text(encoding="utf-8"))
        metadata = payload.get("metadata") or {}
        feature_cols = payload.get("feature_cols") or []

        self.assertEqual(metadata.get("model_version"), "shadow-20260716-1729")
        self.assertEqual(metadata.get("model_variant"), "no-agf")
        self.assertFalse(metadata.get("includes_agf"))
        self.assertEqual(metadata.get("feature_count"), 50)
        self.assertEqual(len(feature_cols), 50)
        for forbidden in {
            "agf_score",
            "has_agf",
            "v4_score",
            "v4_rank",
            "top3_feature_avg",
            "feature_variance",
        }:
            self.assertNotIn(forbidden, feature_cols)

    def test_candidate_loads_with_expected_feature_count(self):
        api.load_ml_model()

        self.assertIsNotNone(api._ml_shadow_model)
        self.assertIsNone(api._ml_shadow_load_error)
        self.assertEqual(api._ML_SHADOW_MODE, "shadow_only")
        self.assertEqual(api._ml_shadow_metadata.get("model_version"), "shadow-20260716-1729")
        self.assertEqual(len(api._ml_shadow_feature_cols), 50)
        self.assertEqual(api._ml_shadow_model.get_booster().num_features(), 50)

    def test_shadow_inference_never_changes_visible_v4_fields(self):
        api.load_ml_model()
        horses = [
            {
                "name": name,
                "rank": rank,
                "aiScore": score,
                "v4Rank": rank,
                "v4Score": score,
                "v4Profile": {"category": "HANDIKAP", "subtype": "HANDIKAP15"},
                "metricSourceFlags": {},
                "_mf": {
                    "degree_avg": 70 - rank * 5,
                    "form_trend": 65 - rank * 3,
                    "hp_score": 60 - rank * 2,
                },
            }
            for name, rank, score in [("A", 1, 88.0), ("B", 2, 77.0), ("C", 3, 66.0)]
        ]
        visible_before = [(horse["rank"], horse["aiScore"], horse["v4Rank"], horse["v4Score"]) for horse in horses]

        api.attach_shadow_ml_predictions(
            horses,
            race_type="Handikap 15",
            distance="1400",
            track="Kum",
        )

        visible_after = [(horse["rank"], horse["aiScore"], horse["v4Rank"], horse["v4Score"]) for horse in horses]
        self.assertEqual(visible_after, visible_before)
        self.assertEqual({horse.get("mlShadowMode") for horse in horses}, {"shadow_only"})
        self.assertEqual({horse.get("mlModelVersion") for horse in horses}, {"shadow-20260716-1729"})
        self.assertEqual(sorted(horse.get("mlShadowRank") for horse in horses), [1, 2, 3])
        self.assertTrue(all(horse.get("mlShadowScore") is not None for horse in horses))


if __name__ == "__main__":
    unittest.main()
