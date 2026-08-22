import copy
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

import api_server as api
import train_shadow_ml as training


ROOT = Path(__file__).parent


class FakeRanker:
    def predict(self, matrix):
        return np.asarray(matrix)[:, 0]


def runners(race_type="MAIDEN"):
    values = []
    for index, (v4_score, form) in enumerate(
        [(70.0, 5.0), (60.0, 100.0), (50.0, 60.0), (40.0, 40.0)],
        start=1,
    ):
        values.append({
            "name": f"HORSE-{index}",
            "aiScore": v4_score,
            "rank": index,
            "v4Score": v4_score,
            "v4Rank": index,
            "v4Version": "4.25",
            "v4AppliedForRanking": True,
            "v4Profile": {
                "category": "MAIDEN" if race_type == "MAIDEN" else "SARTLI",
                "subtype": race_type,
                "track": "Kum",
            },
            "metricSourceFlags": {},
            "_mf": {"form_trend": form},
        })
    return values


def maiden_model_context():
    return (
        patch.object(api, "_maiden_shadow_model", FakeRanker()),
        patch.object(api, "_maiden_shadow_feature_cols", ["form_trend"]),
        patch.object(api, "_maiden_shadow_feature_stats", {"form_trend": {"mean": 50.0}}),
        patch.object(api, "_maiden_shadow_metadata", {"model_version": "maiden-test-v1"}),
        patch.object(api, "_maiden_shadow_manifest", {
            "modelSha256": "model-hash",
            "featureSchemaSha256": "schema-hash",
            "trainingCutoff": "31.07.2026",
        }),
        patch.object(api, "_maiden_shadow_load_error", None),
    )


class MaidenShadowCandidateTests(unittest.TestCase):
    def _attach(self, values, race_type="MAIDEN"):
        contexts = maiden_model_context()
        for context in contexts:
            context.start()
        self.addCleanup(lambda: [context.stop() for context in reversed(contexts)])
        api.attach_maiden_shadow_candidate(values, race_type, "1200", "Kum")

    def test_candidate_is_fifteen_percent_and_changes_only_live_visible_fields(self):
        values = runners()
        before = copy.deepcopy([
            {key: horse[key] for key in ("aiScore", "rank", "v4Score", "v4Rank")}
            for horse in values
        ])

        self._attach(values)

        self.assertNotEqual([horse["aiScore"] for horse in values], [
            horse["aiScore"] for horse in before
        ])
        self.assertEqual([horse["v4Score"] for horse in values], [
            horse["v4Score"] for horse in before
        ])
        self.assertEqual([horse["v4Rank"] for horse in values], [
            horse["v4Rank"] for horse in before
        ])
        self.assertTrue(all(horse["maidenCandidateAlpha"] == 0.15 for horse in values))
        self.assertTrue(all(horse["maidenCandidateStrictNoAgfMl"] for horse in values))
        self.assertTrue(all(horse["maidenCandidateUsedForRanking"] for horse in values))
        self.assertTrue(all(horse["maidenCandidateTelegramVisible"] for horse in values))
        self.assertTrue(all(not horse["v4AppliedForRanking"] for horse in values))
        self.assertTrue(all(
            len(horse["maidenCandidateFeatureVectorSha256"]) == 64 for horse in values
        ))
        self.assertEqual(values[0]["maidenCandidateScore"], 85.0)
        self.assertEqual(values[1]["maidenCandidateScore"], 71.6667)

    def test_non_maiden_race_is_untouched(self):
        values = runners("SART4")
        before = copy.deepcopy(values)

        self._attach(values, "ŞARTLI 4")

        self.assertEqual(values, before)

    def test_v4_score_rank_mismatch_fails_closed(self):
        values = runners()
        values[0]["v4Rank"], values[1]["v4Rank"] = 2, 1

        self._attach(values)

        self.assertTrue(all(horse["maidenCandidateMode"] == "integrity_invalid" for horse in values))
        self.assertTrue(all(horse["maidenCandidateRank"] is None for horse in values))
        self.assertTrue(all(not horse["maidenCandidateV4ScoreFaithful"] for horse in values))

    def test_non_finite_feature_fails_closed(self):
        values = runners()
        values[0]["_mf"]["form_trend"] = float("nan")
        before = copy.deepcopy([
            {key: horse[key] for key in ("aiScore", "rank", "v4Score", "v4Rank")}
            for horse in values
        ])

        self._attach(values)

        after = [
            {key: horse[key] for key in ("aiScore", "rank", "v4Score", "v4Rank")}
            for horse in values
        ]
        self.assertEqual(after, before)
        self.assertTrue(all(horse["maidenCandidateMode"] == "unavailable" for horse in values))
        self.assertTrue(all(not horse["maidenCandidateUsedForRanking"] for horse in values))

    def test_first_snapshot_survives_version_change(self):
        current = {
            "maiden_candidate_version": "new",
            "maiden_candidate_rank": 1,
        }
        previous = {
            "maiden_candidate_version": "old",
            "maiden_candidate_rank": 3,
        }

        api._preserve_maiden_candidate_snapshot(current, previous)

        self.assertEqual(current["maiden_candidate_version"], "old")
        self.assertEqual(current["maiden_candidate_rank"], 3)

    def test_status_exposes_non_ranking_candidate(self):
        response = api.app.test_client().get("/api/ml-status")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()["maiden_shadow"]
        self.assertEqual(payload["version"], api._MAIDEN_SHADOW_VERSION)
        self.assertEqual(payload["observation_start"], "23.08.2026")
        self.assertEqual(payload["alpha"], 0.15)
        self.assertTrue(payload["used_for_ranking"])
        self.assertTrue(payload["telegram_visible"])
        self.assertEqual(
            response.get_json()["ranking_version"],
            "v4.25+maiden-ml15-20260823-v2",
        )

    def test_frozen_artifact_hashes_and_loader_are_valid(self):
        manifest = json.loads((ROOT / "maiden_shadow_manifest.json").read_text(encoding="utf-8"))
        stats = json.loads((ROOT / "feature_stats_maiden_shadow.json").read_text(encoding="utf-8"))

        self.assertEqual(
            hashlib.sha256((ROOT / "model_maiden_shadow_ranker.json").read_bytes()).hexdigest(),
            manifest["modelSha256"],
        )
        self.assertEqual(
            hashlib.sha256((ROOT / "feature_stats_maiden_shadow.json").read_bytes()).hexdigest(),
            manifest["statsSha256"],
        )
        self.assertEqual(stats["metadata"]["model_variant"], "no-agf")
        self.assertFalse(stats["metadata"]["includes_agf"])
        api.load_maiden_shadow_model()
        self.assertIsNone(api._maiden_shadow_load_error)
        self.assertEqual(len(api._maiden_shadow_feature_cols), 58)
        self.assertEqual(api._maiden_shadow_model.get_booster().num_features(), 58)

    def test_missing_artifact_reload_clears_previous_model(self):
        api._maiden_shadow_model = object()
        api._maiden_shadow_feature_cols = ["stale"]
        api._maiden_shadow_metadata = {"model_version": "stale"}
        try:
            with patch("os.path.exists", return_value=False):
                api.load_maiden_shadow_model()

            self.assertIsNone(api._maiden_shadow_model)
            self.assertEqual(api._maiden_shadow_feature_cols, [])
            self.assertEqual(api._maiden_shadow_metadata, {})
            self.assertIsNotNone(api._maiden_shadow_load_error)
        finally:
            api.load_maiden_shadow_model()

    def test_training_and_live_feature_vectors_match(self):
        flags = {
            "hasTraining": True,
            "hasTrainingTimes": True,
            "hasHp": True,
            "hasPedigree": True,
            "hasTrainer": True,
            "hasAgeActionable": True,
            "hasTrackExperience": True,
            "hasSurfaceTransition": True,
            "hasDistanceTransition": True,
            "hasHandicapEfficiency": False,
        }
        metrics = {
            "degree_avg": 63.0,
            "form_trend": 72.0,
            "track_suit": 58.0,
            "distance_suit": 61.0,
            "training_fitness": 67.0,
            "training_degree_score": 55.0,
            "jockey_score": 64.0,
            "pedigree": 69.0,
            "hp_score": 57.0,
            "trainer_score": 62.0,
        }
        entry = {
            "race_type": "MAIDEN",
            "track": "Çim",
            "distance": "1400",
            "field_size": 8,
            "features": metrics,
            "metric_source_flags": flags,
            "v4_profile": {"category": "MAIDEN", "subtype": "MAIDEN", "track": "Cim"},
            "days_since_last_race": 65,
            "last_race_distance": 1200,
            "ranking_penalties": [{"code": "recent_long_race"}],
            "agf_allowed_for_ranking": True,
        }
        horse = {
            "v4Profile": entry["v4_profile"],
            "metricSourceFlags": flags,
            "daysSinceLastRace": 65,
            "lastRaceDistance": 1200,
            "rankingPenalties": [{"code": "recent_long_race"}],
            "agfAllowedForRanking": True,
        }

        trained = training.feature_dict(entry)
        inferred = api._shadow_feature_dict(
            metrics,
            horse=horse,
            field_size=8,
            race_type="MAIDEN",
            distance="1400",
            track="Çim",
        )
        stats = json.loads((ROOT / "feature_stats_maiden_shadow.json").read_text(encoding="utf-8"))
        for feature in stats["feature_cols"]:
            self.assertAlmostEqual(
                trained[feature], inferred[feature], places=6, msg=feature
            )


if __name__ == "__main__":
    unittest.main()
