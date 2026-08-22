import unittest
from unittest.mock import patch

from api_server import (
    _HANDICAP_HP_SHADOW_VERSION,
    app,
    attach_handicap_hp_candidate,
    extract_v4_race_profile,
    handicap_hp_candidate_log_fields,
    resolve_handicap_hp_candidate_weights,
)


BASELINE_WEIGHTS = {
    "degree_avg": 40.0,
    "hp_score": 4.0,
    "form_trend": 56.0,
}


def horses():
    profile = extract_v4_race_profile(
        race_type="Handikap 16",
        distance="1400",
        track="Kum",
        field_size=4,
    )
    profile["selectedKey"] = "HANDIKAP16|Kum"
    metrics = [
        ("A", 80.0, 100.0),
        ("B", 70.0, 80.0),
        ("C", 60.0, 60.0),
        ("D", 65.0, 0.0),
    ]
    result = []
    for rank, (name, degree, hp) in enumerate(metrics, start=1):
        baseline_score = (
            (degree * 40.0) + (hp * 4.0) + (50.0 * 56.0)
        ) / 100.0
        result.append({
            "name": name,
            "rank": rank,
            "v4Rank": rank,
            "v4Score": baseline_score,
            "v4Version": "4.25",
            "v4AppliedForRanking": True,
            "v4Profile": dict(profile),
            "v4Weights": dict(BASELINE_WEIGHTS),
            "v4PenaltyTotal": 0.0,
            "metricSourceFlags": {"hasHp": True},
            "_mf": {
                "degree_avg": degree,
                "hp_score": hp,
                "form_trend": 50.0,
                "_has_hp": True,
            },
        })
    return result


class HandicapHpShadowCandidateTests(unittest.TestCase):
    def test_formula_reduces_hp_by_three_raw_points_and_floors_at_zero(self):
        formula = resolve_handicap_hp_candidate_weights(BASELINE_WEIGHTS)
        self.assertEqual(formula["candidateRawWeights"]["hp_score"], 1.0)
        self.assertEqual(formula["candidateRawTotal"], 97.0)
        self.assertEqual(formula["actualHpRawDeltaPoints"], -3.0)

        low_hp = resolve_handicap_hp_candidate_weights({
            "degree_avg": 42.0,
            "hp_score": 2.2,
            "form_trend": 55.8,
        })
        self.assertNotIn("hp_score", low_hp["candidateRawWeights"])
        self.assertEqual(low_hp["actualHpRawDeltaPoints"], -2.2)

    @patch("api_server.time.time", return_value=1787475600)
    def test_candidate_is_pre_race_immutable_and_never_visible(self, _time):
        analyzed = horses()
        visible_ranks = [horse["rank"] for horse in analyzed]
        attach_handicap_hp_candidate(
            analyzed,
            race_type="Handikap 16",
            distance="1400",
            track="Kum",
            race_date="23.08.2026",
            race_time="17:00",
        )

        self.assertEqual([horse["rank"] for horse in analyzed], visible_ranks)
        self.assertTrue(all(
            horse["handicapHpCandidateVersion"] == _HANDICAP_HP_SHADOW_VERSION
            for horse in analyzed
        ))
        self.assertTrue(all(
            horse["handicapHpCandidateUsedForRanking"] is False
            and horse["handicapHpCandidateTelegramVisible"] is False
            and horse["handicapHpCandidateRolloutEligible"] is False
            for horse in analyzed
        ))
        d_horse = next(horse for horse in analyzed if horse["name"] == "D")
        self.assertEqual(d_horse["handicapHpCandidateBaselineRank"], 4)
        self.assertEqual(d_horse["handicapHpCandidateRank"], 3)
        self.assertTrue(all(
            horse["handicapHpCandidateReplayTop3SetAgreement"] is True
            and horse["handicapHpCandidateRaceEvidenceEligible"] is True
            for horse in analyzed
        ))
        self.assertEqual(
            len({horse["handicapHpCandidateRaceSnapshotSha256"] for horse in analyzed}),
            1,
        )
        logged = handicap_hp_candidate_log_fields(d_horse)
        self.assertFalse(logged["handicap_hp_candidate_used_for_ranking"])
        self.assertFalse(logged["handicap_hp_candidate_telegram_visible"])
        self.assertEqual(logged["handicap_hp_candidate_rank"], 3)

    @patch("api_server.time.time", return_value=1787475600)
    def test_source_guard_mismatch_fails_closed(self, _time):
        analyzed = horses()
        analyzed[0]["metricSourceFlags"]["hasHp"] = False
        with self.assertRaisesRegex(ValueError, "hasHp"):
            attach_handicap_hp_candidate(
                analyzed,
                race_type="Handikap 16",
                distance="1400",
                track="Kum",
                race_date="23.08.2026",
                race_time="17:00",
            )

    @patch("api_server.time.time", return_value=1787475600)
    def test_pre_observation_race_is_not_attached(self, _time):
        analyzed = horses()
        attach_handicap_hp_candidate(
            analyzed,
            race_type="Handikap 16",
            distance="1400",
            track="Kum",
            race_date="22.08.2026",
            race_time="17:00",
        )
        self.assertTrue(all("handicapHpCandidateVersion" not in horse for horse in analyzed))

    def test_status_exposes_top3_and_separation_contract(self):
        with app.test_client() as client:
            payload = client.get("/api/ml-status").get_json()
        candidate = payload["handicap_hp_shadow"]
        self.assertEqual(candidate["version"], _HANDICAP_HP_SHADOW_VERSION)
        self.assertEqual(candidate["primary_objective"], "winner_top3")
        self.assertFalse(candidate["used_for_ranking"])
        self.assertFalse(candidate["telegram_visible"])
        self.assertTrue(
            candidate["separation_gate"]["fragile_rescue_not_counted_as_robust"]
        )


if __name__ == "__main__":
    unittest.main()
