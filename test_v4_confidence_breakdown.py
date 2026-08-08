import unittest

from api_server import (
    _V4_CONFIDENCE_SCHEMA,
    calculate_v4_confidence_breakdown,
    calculate_v4_data_quality,
)
from automation.atistik_daily_job import summarize_rankings


def resolved(metric="degree_avg", confidence=0.75):
    return {
        "weights": {metric: 1.0},
        "confidenceScore": confidence,
        "sampleRaces": 60,
        "minRequired": 30,
        "fallbackLevel": "subtype",
        "eligible": True,
        "status": "eligible_shadow",
    }


def horses(scores, metric="degree_avg", *, guarded=True):
    rows = []
    for index, score in enumerate(scores):
        metrics = {metric: float(score)}
        if metric == "training_fitness" and guarded:
            metrics["_has_training"] = True
        rows.append(
            {
                "name": f"HORSE-{index}",
                "v4Score": score,
                "v4PenaltyTotal": 0.0,
                "_mf": metrics,
                "detailFetchStatus": "ok",
            }
        )
    return rows


class V4ConfidenceBreakdownTests(unittest.TestCase):
    def test_confidence_schema_is_versioned(self):
        self.assertEqual(_V4_CONFIDENCE_SCHEMA, "v4-confidence-breakdown-v1")

    def test_compressed_top3_boundary_is_low_confidence_open_race(self):
        rows = horses([51.0, 50.8, 50.6, 50.4, 50.2, 50.0, 49.8, 49.6])
        breakdown = calculate_v4_confidence_breakdown(
            rows,
            resolved(),
            calculate_v4_data_quality(rows),
        )

        self.assertEqual(breakdown["separation"]["label"], "RED")
        self.assertEqual(breakdown["overall"]["label"], "LOW")
        self.assertTrue(breakdown["overall"]["openRace"])
        self.assertIn("TOP3_BOUNDARY_TIGHT", breakdown["overall"]["reasonCodes"])

    def test_well_separated_race_cannot_claim_high_before_calibration(self):
        rows = horses([92.0, 82.0, 72.0, 45.0, 30.0])
        breakdown = calculate_v4_confidence_breakdown(
            rows,
            resolved(),
            calculate_v4_data_quality(rows),
        )

        self.assertEqual(breakdown["separation"]["label"], "GREEN")
        self.assertEqual(breakdown["data"]["label"], "HIGH")
        self.assertEqual(breakdown["calibration"]["status"], "NOT_CALIBRATED")
        self.assertEqual(breakdown["overall"]["label"], "MEDIUM_UNCALIBRATED")
        self.assertLessEqual(breakdown["overall"]["score"], 0.69)

    def test_missing_explicit_source_guard_forces_low_data_confidence(self):
        rows = horses(
            [92.0, 82.0, 72.0, 45.0, 30.0],
            metric="training_fitness",
            guarded=False,
        )
        breakdown = calculate_v4_confidence_breakdown(
            rows,
            resolved("training_fitness"),
            calculate_v4_data_quality(rows),
        )

        self.assertEqual(breakdown["data"]["weightedRealCoverage"], 0.0)
        self.assertEqual(breakdown["data"]["label"], "LOW")
        self.assertTrue(breakdown["overall"]["lowConfidence"])

    def test_daily_summary_carries_new_confidence_fields(self):
        breakdown = {"schemaVersion": "v4-confidence-breakdown-v1"}
        decision = {"label": "LOW", "lowConfidence": True}
        summary = summarize_rankings([
            {
                "name": "ONE",
                "rank": 1,
                "v4Rank": 1,
                "v4Confidence": {"scope": "profile_evidence_only"},
                "v4ConfidenceBreakdown": breakdown,
                "v4DecisionConfidence": decision,
            }
        ])

        self.assertEqual(summary[0]["v4ConfidenceBreakdown"], breakdown)
        self.assertEqual(summary[0]["v4DecisionConfidence"], decision)


if __name__ == "__main__":
    unittest.main()
