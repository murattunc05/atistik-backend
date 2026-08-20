import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from automation.h15_training_shadow_monitor import (
    EXPECTED_BASELINE_VERSION,
    EXPECTED_CALIBRATION_CONTRACT,
    EXPECTED_MODE,
    EXPECTED_OBSERVATION_START,
    EXPECTED_PROFILE,
    EXPECTED_RAW_ADD_POINTS,
    EXPECTED_VERSION,
    SOURCE_HASH_KEYS,
    build_report,
    calibration_metrics,
    expected_formula,
    persist,
    plackett_luce_top3_probabilities,
    sha256,
)
from automation.metric_signal_replay import (
    calibration_metrics as replay_calibration_metrics,
    plackett_luce_top3_probabilities as replay_top3_probabilities,
)


ISTANBUL = ZoneInfo("Europe/Istanbul")


def race_rows(
    index,
    baseline_winner_rank=4,
    candidate_winner_rank=3,
    *,
    partial=False,
    source=True,
    neutral=False,
):
    day = date(2026, 8, 21) + timedelta(days=index)
    race_start = datetime.combine(day, time(14, 0), tzinfo=ISTANBUL)
    created_ts = int(race_start.timestamp()) - 3600
    profile = {
        "category": "HANDIKAP",
        "subtype": EXPECTED_PROFILE,
        "distanceBucket": "mid",
        "fieldBucket": "small",
        "track": "Kum",
        "profileKey": "HANDIKAP15|mid|small|Kum",
        "selectedKey": "HANDIKAP15|Kum",
        "fallbackLevel": "subtype_track",
    }
    formula = expected_formula({"pace_score": 100.0})
    assert formula is not None
    definition_payload = {
        "schemaVersion": "h15-training-shadow-v1",
        "candidateVersion": EXPECTED_VERSION,
        "observationStart": EXPECTED_OBSERVATION_START,
        "baselineVersion": EXPECTED_BASELINE_VERSION,
        "profile": profile,
        "metric": "training_degree_score",
        "rawWeightAddPoints": EXPECTED_RAW_ADD_POINTS,
        "baselineWeights": formula["baselineWeights"],
        "baselineRawTotal": formula["baselineRawTotal"],
        "candidateRawWeights": formula["candidateRawWeights"],
        "candidateRawTotal": formula["candidateRawTotal"],
        "candidateWeights": formula["candidateWeights"],
        "weightDeltaPct": formula["weightDeltaPct"],
        "normalization": "exported_v4_weights_plus_raw_points_then_normalize",
        "calibrationContract": EXPECTED_CALIBRATION_CONTRACT,
    }
    definition_hash = sha256(definition_payload)
    baseline_other = [rank for rank in range(1, 5) if rank != baseline_winner_rank]
    candidate_other = [rank for rank in range(1, 5) if rank != candidate_winner_rank]
    # Keep adjacent replay scores close enough for the real +2 raw-point
    # formula to move the winner from rank 4 to rank 3.
    score_by_rank = {1: 70.0, 2: 69.0, 3: 68.0, 4: 67.0}
    rows = []
    for horse_index in range(4):
        winner = horse_index == 0
        baseline_rank = baseline_winner_rank if winner else baseline_other[horse_index - 1]
        requested_candidate_rank = (
            candidate_winner_rank if winner else candidate_other[horse_index - 1]
        )
        candidate_rank = (
            requested_candidate_rank if source and not neutral else baseline_rank
        )
        pace = score_by_rank[baseline_rank]
        if neutral:
            training = 50.0
        elif baseline_winner_rank == 4 and candidate_winner_rank == 3:
            training = 100.0 if winner else (0.0 if baseline_rank == 3 else 60.0)
        elif baseline_winner_rank == 2 and candidate_winner_rank == 1:
            training = 100.0 if winner else (0.0 if baseline_rank == 1 else 60.0)
        else:
            training = 60.0
        included = source
        baseline_numerator = pace * 100.0
        baseline_denominator = 100.0
        candidate_numerator = baseline_numerator + (training * 2.0 if source else 0.0)
        candidate_denominator = baseline_denominator + (2.0 if source else 0.0)
        candidate_score = candidate_numerator / candidate_denominator
        source_payload = {
            "metric": "training_degree_score",
            "metricSourceFlag": "hasTrainingTimes",
            "metricSourceFlagPresent": True,
            "metricSourceFlagValue": source,
            "mfGuard": "_has_training_times",
            "mfGuardPresent": True,
            "mfGuardValue": source,
            "guardsAgree": True,
            "hasSource": source,
            "metricValue": training,
            "neutral": bool(source and abs(training - 50.0) < 1.0),
            "actionable": bool(source and abs(training - 50.0) >= 1.0),
        }
        source_count = 4 if source else 0
        actionable_count = 0 if (not source or neutral) else 4
        neutral_count = 4 if (source and neutral) else 0
        source_with_counts = {
            **source_payload,
            "sourceCount": source_count,
            "actionableCount": actionable_count,
            "neutralCount": neutral_count,
            "unavailableCount": 4 - source_count,
            "runnerCount": 4,
            "coverage": source_count / 4,
            "actionableCoverage": actionable_count / 4,
        }
        snapshot = {"pace_score": pace, "training_degree_score": training}
        guards = {
            "_has_training_times": {"present": True, "value": source},
        }
        source_for_hash = {key: source_payload.get(key) for key in SOURCE_HASH_KEYS}
        feature_hash = sha256({
            "horseName": f"HORSE-{horse_index}",
            "features": snapshot,
            "sourceGuards": guards,
            "trainingSource": source_for_hash,
        })
        rows.append({
            "race_id": f"H15-{index:03d}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": "1",
            "race_time": "14.00",
            "city": "Ankara",
            "city_id": "2",
            "race_type": "Handikap 15",
            "track": "Kum",
            "distance": "1400",
            "field_size": 4,
            "horse_name": f"HORSE-{horse_index}",
            "finish_pos": None if partial and horse_index == 3 else horse_index + 1,
            "v4_score": score_by_rank[baseline_rank],
            "v4_rank": baseline_rank,
            "rank_pred": baseline_rank,
            "v4_applied_for_ranking": True,
            "v4_version": EXPECTED_BASELINE_VERSION,
            "v4_profile": profile,
            "v4_weights": formula["baselineWeights"],
            "metric_source_flags": {"hasTrainingTimes": source},
            "h15_training_candidate_version": EXPECTED_VERSION,
            "h15_training_candidate_mode": EXPECTED_MODE,
            "h15_training_candidate_observation_start": EXPECTED_OBSERVATION_START,
            "h15_training_candidate_created_ts": created_ts,
            "h15_training_candidate_baseline_version": EXPECTED_BASELINE_VERSION,
            "h15_training_candidate_baseline_score": score_by_rank[baseline_rank],
            "h15_training_candidate_baseline_rank": baseline_rank,
            "h15_training_candidate_baseline_weighted_numerator": baseline_numerator,
            "h15_training_candidate_baseline_available_weight_total": baseline_denominator,
            "h15_training_candidate_replay_baseline_base_score": pace,
            "h15_training_candidate_replay_baseline_score": pace,
            "h15_training_candidate_replay_baseline_rank": baseline_rank,
            "h15_training_candidate_added_metric_value": training if source else None,
            "h15_training_candidate_weighted_numerator": candidate_numerator,
            "h15_training_candidate_available_weight_total": candidate_denominator,
            "h15_training_candidate_base_score": candidate_score,
            "h15_training_candidate_penalty_total": 0.0,
            "h15_training_candidate_score": candidate_score,
            "h15_training_candidate_rank": candidate_rank,
            "h15_training_candidate_used_for_ranking": False,
            "h15_training_candidate_telegram_visible": False,
            "h15_training_candidate_rollout_eligible": False,
            "h15_training_candidate_formal_replay_only": True,
            "h15_training_candidate_profile": profile,
            "h15_training_candidate_metric": "training_degree_score",
            "h15_training_candidate_raw_weight_add_points": EXPECTED_RAW_ADD_POINTS,
            "h15_training_candidate_baseline_weights": formula["baselineWeights"],
            "h15_training_candidate_baseline_raw_total": formula["baselineRawTotal"],
            "h15_training_candidate_raw_weights": formula["candidateRawWeights"],
            "h15_training_candidate_raw_total": formula["candidateRawTotal"],
            "h15_training_candidate_weights": formula["candidateWeights"],
            "h15_training_candidate_weight_delta_pct": formula["weightDeltaPct"],
            "h15_training_candidate_definition_sha256": definition_hash,
            "h15_training_candidate_calibration_contract": EXPECTED_CALIBRATION_CONTRACT,
            "h15_training_candidate_feature_snapshot": snapshot,
            "h15_training_candidate_source_guard_snapshot": guards,
            "h15_training_candidate_score_components": {
                "pace_score": {
                    "value": pace,
                    "weightPct": formula["candidateWeights"]["pace_score"],
                    "baselineRawWeightPoints": 100.0,
                    "candidateRawWeightPoints": 100.0,
                    "guard": None,
                    "included": True,
                },
                "training_degree_score": {
                    "value": training,
                    "weightPct": formula["candidateWeights"]["training_degree_score"],
                    "baselineRawWeightPoints": 0.0,
                    "candidateRawWeightPoints": 2.0,
                    "guard": "_has_training_times",
                    "included": source,
                },
            },
            "h15_training_candidate_feature_vector_sha256": feature_hash,
            "h15_training_candidate_source": source_with_counts,
            "h15_training_candidate_replay_top3_set_agreement": True,
            "h15_training_candidate_evidence_issue": None,
            "h15_training_candidate_race_evidence_eligible": actionable_count > 0,
        })

    race_hash = sha256({
        "definitionSha256": definition_hash,
        "createdTs": created_ts,
        "replayTop3SetAgreement": True,
        "evidenceIssue": None,
        "horses": sorted(
            [
                {
                    "horseName": row["horse_name"],
                    "baselineScore": row["h15_training_candidate_baseline_score"],
                    "baselineRank": row["h15_training_candidate_baseline_rank"],
                    "replayBaselineScore": row["h15_training_candidate_replay_baseline_score"],
                    "replayBaselineRank": row["h15_training_candidate_replay_baseline_rank"],
                    "candidateScore": row["h15_training_candidate_score"],
                    "candidateRank": row["h15_training_candidate_rank"],
                    "featureVectorSha256": row["h15_training_candidate_feature_vector_sha256"],
                }
                for row in rows
            ],
            key=lambda item: item["horseName"].casefold(),
        ),
    })
    for row in rows:
        row["h15_training_candidate_race_snapshot_sha256"] = race_hash
    return rows


def reseal_race(rows):
    first = rows[0]
    agreement = bool(
        first["h15_training_candidate_replay_top3_set_agreement"]
    )
    issue = first.get("h15_training_candidate_evidence_issue")
    race_hash = sha256({
        "definitionSha256": first["h15_training_candidate_definition_sha256"],
        "createdTs": first["h15_training_candidate_created_ts"],
        "replayTop3SetAgreement": agreement,
        "evidenceIssue": issue,
        "horses": sorted(
            [
                {
                    "horseName": row["horse_name"],
                    "baselineScore": row["h15_training_candidate_baseline_score"],
                    "baselineRank": row["h15_training_candidate_baseline_rank"],
                    "replayBaselineScore": row["h15_training_candidate_replay_baseline_score"],
                    "replayBaselineRank": row["h15_training_candidate_replay_baseline_rank"],
                    "candidateScore": row["h15_training_candidate_score"],
                    "candidateRank": row["h15_training_candidate_rank"],
                    "featureVectorSha256": row["h15_training_candidate_feature_vector_sha256"],
                }
                for row in rows
            ],
            key=lambda item: item["horseName"].casefold(),
        ),
    })
    for row in rows:
        row["h15_training_candidate_race_snapshot_sha256"] = race_hash
    return rows


def historical_rescue_rows(index):
    fixture_path = (
        Path(__file__).parent
        / "tests"
        / "h15_training_plus2_history_20260819.jsonl"
    )
    values = [
        json.loads(line)
        for line in fixture_path.read_text(encoding="utf-8").splitlines()
    ]
    meta = values[0]
    fixture = next(
        row for row in values[1:] if row["id"] == meta["formalRescueRaceId"]
    )
    assert (fixture["br"], fixture["cr"]) == (4, 3)

    day = date(2026, 8, 21) + timedelta(days=index)
    race_start = datetime.combine(day, time(14, 0), tzinfo=ISTANBUL)
    created_ts = int(race_start.timestamp()) - 3600
    profile = {
        "category": "HANDIKAP",
        "subtype": EXPECTED_PROFILE,
        "distanceBucket": "mid",
        "fieldBucket": "medium",
        "track": "Kum",
        "profileKey": "HANDIKAP15|mid|medium|Kum",
        "selectedKey": "HANDIKAP15|Kum",
        "fallbackLevel": "subtype_track",
    }
    # The real replay captured 99/100 available-weight denominators. A frozen
    # 99+1 map plus the original hp source guard reproduces those totals and
    # every historical score exactly before training_degree_score +2.
    formula = expected_formula({"pace_score": 99.0, "hp_score": 1.0})
    assert formula is not None
    definition_payload = {
        "schemaVersion": "h15-training-shadow-v1",
        "candidateVersion": EXPECTED_VERSION,
        "observationStart": EXPECTED_OBSERVATION_START,
        "baselineVersion": EXPECTED_BASELINE_VERSION,
        "profile": profile,
        "metric": "training_degree_score",
        "rawWeightAddPoints": EXPECTED_RAW_ADD_POINTS,
        "baselineWeights": formula["baselineWeights"],
        "baselineRawTotal": formula["baselineRawTotal"],
        "candidateRawWeights": formula["candidateRawWeights"],
        "candidateRawTotal": formula["candidateRawTotal"],
        "candidateWeights": formula["candidateWeights"],
        "weightDeltaPct": formula["weightDeltaPct"],
        "normalization": "exported_v4_weights_plus_raw_points_then_normalize",
        "calibrationContract": EXPECTED_CALIBRATION_CONTRACT,
    }
    definition_hash = sha256(definition_payload)
    actionable_count = sum(abs(item[5] - 50.0) >= 1.0 for item in fixture["h"])
    rows = []
    for horse_index, item in enumerate(fixture["h"]):
        horse_name, visible_rank, penalty, numerator, denominator, training = item
        assert denominator in (99.0, 100.0)
        hp_source = denominator == 100.0
        pace = numerator / 99.0
        features = {
            "pace_score": pace,
            "hp_score": 0.0,
            "training_degree_score": training,
        }
        guards = {
            "_has_hp": {"present": True, "value": hp_source},
            "_has_training_times": {"present": True, "value": True},
        }
        source_payload = {
            "metric": "training_degree_score",
            "metricSourceFlag": "hasTrainingTimes",
            "metricSourceFlagPresent": True,
            "metricSourceFlagValue": True,
            "mfGuard": "_has_training_times",
            "mfGuardPresent": True,
            "mfGuardValue": True,
            "guardsAgree": True,
            "hasSource": True,
            "metricValue": training,
            "neutral": abs(training - 50.0) < 1.0,
            "actionable": abs(training - 50.0) >= 1.0,
        }
        source_for_hash = {
            key: source_payload.get(key) for key in SOURCE_HASH_KEYS
        }
        feature_hash = sha256({
            "horseName": horse_name,
            "features": features,
            "sourceGuards": guards,
            "trainingSource": source_for_hash,
        })
        replay_base = numerator / denominator
        replay_score = max(0.0, min(100.0, replay_base - penalty))
        candidate_numerator = numerator + training * 2.0
        candidate_denominator = denominator + 2.0
        candidate_base = candidate_numerator / candidate_denominator
        candidate_score = max(0.0, min(100.0, candidate_base - penalty))
        rows.append({
            "race_id": f"FORMAL-{fixture['id']}-{index}",
            "race_date": day.strftime("%d.%m.%Y"),
            "race_no": "1",
            "race_time": "14.00",
            "city": "Kocaeli",
            "city_id": "9",
            "race_type": "Handikap 15",
            "track": "Kum",
            "distance": "1400",
            "field_size": len(fixture["h"]),
            "horse_name": horse_name,
            "finish_pos": fixture["f"][horse_index],
            "v4_score": fixture["s"][horse_index],
            "v4_rank": visible_rank,
            "rank_pred": visible_rank,
            "v4_applied_for_ranking": True,
            "v4_version": EXPECTED_BASELINE_VERSION,
            "v4_profile": profile,
            "v4_weights": formula["baselineWeights"],
            "metric_source_flags": {"hasTrainingTimes": True},
            "h15_training_candidate_version": EXPECTED_VERSION,
            "h15_training_candidate_mode": EXPECTED_MODE,
            "h15_training_candidate_observation_start": EXPECTED_OBSERVATION_START,
            "h15_training_candidate_created_ts": created_ts,
            "h15_training_candidate_baseline_version": EXPECTED_BASELINE_VERSION,
            "h15_training_candidate_baseline_score": fixture["s"][horse_index],
            "h15_training_candidate_baseline_rank": visible_rank,
            "h15_training_candidate_baseline_weighted_numerator": numerator,
            "h15_training_candidate_baseline_available_weight_total": denominator,
            "h15_training_candidate_replay_baseline_base_score": replay_base,
            "h15_training_candidate_replay_baseline_score": replay_score,
            "h15_training_candidate_replay_baseline_rank": None,
            "h15_training_candidate_added_metric_value": training,
            "h15_training_candidate_weighted_numerator": candidate_numerator,
            "h15_training_candidate_available_weight_total": candidate_denominator,
            "h15_training_candidate_base_score": candidate_base,
            "h15_training_candidate_penalty_total": penalty,
            "h15_training_candidate_score": candidate_score,
            "h15_training_candidate_rank": None,
            "h15_training_candidate_used_for_ranking": False,
            "h15_training_candidate_telegram_visible": False,
            "h15_training_candidate_rollout_eligible": False,
            "h15_training_candidate_formal_replay_only": True,
            "h15_training_candidate_profile": profile,
            "h15_training_candidate_metric": "training_degree_score",
            "h15_training_candidate_raw_weight_add_points": EXPECTED_RAW_ADD_POINTS,
            "h15_training_candidate_baseline_weights": formula["baselineWeights"],
            "h15_training_candidate_baseline_raw_total": formula["baselineRawTotal"],
            "h15_training_candidate_raw_weights": formula["candidateRawWeights"],
            "h15_training_candidate_raw_total": formula["candidateRawTotal"],
            "h15_training_candidate_weights": formula["candidateWeights"],
            "h15_training_candidate_weight_delta_pct": formula["weightDeltaPct"],
            "h15_training_candidate_definition_sha256": definition_hash,
            "h15_training_candidate_calibration_contract": EXPECTED_CALIBRATION_CONTRACT,
            "h15_training_candidate_feature_snapshot": features,
            "h15_training_candidate_source_guard_snapshot": guards,
            "h15_training_candidate_score_components": {
                "pace_score": {
                    "value": pace,
                    "weightPct": formula["candidateWeights"]["pace_score"],
                    "baselineRawWeightPoints": 99.0,
                    "candidateRawWeightPoints": 99.0,
                    "guard": None,
                    "included": True,
                },
                "hp_score": {
                    "value": 0.0,
                    "weightPct": formula["candidateWeights"]["hp_score"],
                    "baselineRawWeightPoints": 1.0,
                    "candidateRawWeightPoints": 1.0,
                    "guard": "_has_hp",
                    "included": hp_source,
                },
                "training_degree_score": {
                    "value": training,
                    "weightPct": formula["candidateWeights"]["training_degree_score"],
                    "baselineRawWeightPoints": 0.0,
                    "candidateRawWeightPoints": 2.0,
                    "guard": "_has_training_times",
                    "included": True,
                },
            },
            "h15_training_candidate_feature_vector_sha256": feature_hash,
            "h15_training_candidate_source": {
                **source_payload,
                "sourceCount": len(fixture["h"]),
                "actionableCount": actionable_count,
                "neutralCount": len(fixture["h"]) - actionable_count,
                "unavailableCount": 0,
                "runnerCount": len(fixture["h"]),
                "coverage": 1.0,
                "actionableCoverage": actionable_count / len(fixture["h"]),
            },
        })

    for rank, row in enumerate(sorted(
        rows,
        key=lambda row: (
            -row["h15_training_candidate_replay_baseline_score"],
            row["h15_training_candidate_baseline_rank"],
            row["horse_name"],
        ),
    ), start=1):
        row["h15_training_candidate_replay_baseline_rank"] = rank
    for rank, row in enumerate(sorted(
        rows,
        key=lambda row: (
            -row["h15_training_candidate_score"],
            row["h15_training_candidate_baseline_rank"],
            row["horse_name"],
        ),
    ), start=1):
        row["h15_training_candidate_rank"] = rank

    visible_top3 = {
        row["horse_name"]
        for row in rows
        if row["h15_training_candidate_baseline_rank"] <= 3
    }
    replay_top3 = {
        row["horse_name"]
        for row in rows
        if row["h15_training_candidate_replay_baseline_rank"] <= 3
    }
    assert visible_top3 == replay_top3
    winner = next(row for row in rows if row["finish_pos"] == 1)
    assert winner["h15_training_candidate_replay_baseline_rank"] == 4
    assert winner["h15_training_candidate_rank"] == 3
    for row in rows:
        row["h15_training_candidate_replay_top3_set_agreement"] = True
        row["h15_training_candidate_evidence_issue"] = None
        row["h15_training_candidate_race_evidence_eligible"] = True
    return reseal_race(rows)


class H15TrainingShadowMonitorTests(unittest.TestCase):
    def test_calibration_math_matches_metric_signal_replay(self):
        scores = [62.16, 45.78, 45.33, 45.23, 42.93, 41.36]
        observed = plackett_luce_top3_probabilities(scores, 14.0)
        expected = replay_top3_probabilities(scores, 14.0)
        for left, right in zip(observed, expected):
            self.assertAlmostEqual(left, right, places=12)
        labels = [0, 1, 0, 1, 1, 0]
        values = list(zip(observed, labels))
        ours = calibration_metrics(values)
        replay = replay_calibration_metrics(values)
        self.assertEqual(ours["brier"], replay["brier"])
        self.assertEqual(ours["ece"], replay["ece"])

    def test_exact_zero_to_two_over_102_formula_fidelity(self):
        formula = expected_formula({"pace_score": 100.0})

        self.assertIsNotNone(formula)
        self.assertAlmostEqual(
            formula["candidateWeights"]["training_degree_score"],
            2.0 / 102.0 * 100.0,
            places=9,
        )
        report = build_report(race_rows(0), "2026-08-21")
        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 1)
        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 0)

    def test_formula_rank_source_and_hash_tampering_fail_closed(self):
        mutations = (
            lambda rows: rows[0]["h15_training_candidate_raw_total"] + 1.0,
            lambda rows: rows[0].__setitem__("h15_training_candidate_rank", 4),
            lambda rows: rows[0]["h15_training_candidate_source"].__setitem__("mfGuardValue", False),
            lambda rows: rows[0].__setitem__("h15_training_candidate_feature_vector_sha256", "0" * 64),
        )
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                rows = race_rows(0)
                if index == 0:
                    rows[0]["h15_training_candidate_raw_total"] += 1.0
                else:
                    mutate(rows)
                report = build_report(rows, "2026-08-21")
                self.assertEqual(report["coverage"]["integrityInvalidRaces"], 1)
                self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 0)

    def test_partial_unavailable_and_neutral_are_not_performance_evidence(self):
        rows = []
        rows.extend(race_rows(0))
        rows.extend(race_rows(1, partial=True))
        rows.extend(race_rows(2, source=False))
        rows.extend(race_rows(3, neutral=True))

        report = build_report(rows, "2026-08-24")

        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 1)
        self.assertEqual(report["coverage"]["partialRaces"], 1)
        self.assertEqual(report["coverage"]["sourceUnavailableExcludedRaces"], 1)
        self.assertEqual(report["coverage"]["neutralOnlyExcludedRaces"], 1)
        self.assertEqual(report["cumulative"]["races"], 1)

    def test_visible_replay_top3_mismatch_is_reported_and_excluded(self):
        rows = race_rows(
            4,
            baseline_winner_rank=2,
            candidate_winner_rank=2,
        )
        visible_third = next(
            row
            for row in rows
            if row["h15_training_candidate_baseline_rank"] == 3
        )
        visible_fourth = next(
            row
            for row in rows
            if row["h15_training_candidate_baseline_rank"] == 4
        )
        for key in (
            "v4_rank",
            "rank_pred",
            "h15_training_candidate_baseline_rank",
        ):
            visible_third[key], visible_fourth[key] = (
                visible_fourth[key],
                visible_third[key],
            )
        for row in rows:
            row["h15_training_candidate_replay_top3_set_agreement"] = False
            row["h15_training_candidate_evidence_issue"] = (
                "visible_replay_baseline_top3_set_mismatch"
            )
            row["h15_training_candidate_race_evidence_eligible"] = True
        reseal_race(rows)

        report = build_report(rows, "2026-08-25")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 0)
        self.assertEqual(
            report["coverage"]["replayBaselineTop3MismatchExcludedRaces"],
            1,
        )
        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 0)
        self.assertEqual(report["cumulative"]["races"], 0)
        self.assertEqual(
            report["issues"][0]["code"],
            "visible_replay_baseline_top3_set_mismatch",
        )

    def test_nonrunner_filter_matches_metric_signal_replay_agreement(self):
        rows = race_rows(
            5,
            baseline_winner_rank=2,
            candidate_winner_rank=2,
        )
        replay_third = next(
            row
            for row in rows
            if row["h15_training_candidate_replay_baseline_rank"] == 3
        )
        replay_fourth = next(
            row
            for row in rows
            if row["h15_training_candidate_replay_baseline_rank"] == 4
        )
        for key in (
            "v4_rank",
            "rank_pred",
            "h15_training_candidate_baseline_rank",
        ):
            replay_third[key], replay_fourth[key] = (
                replay_fourth[key],
                replay_third[key],
            )
        replay_fourth.update({
            "finish_pos": 99,
            "result_status": "non_runner",
            "terminal_reason": "KOSMAZ",
            "result_source": "tjk_official_results",
        })
        for row in rows:
            row["h15_training_candidate_replay_top3_set_agreement"] = False
            row["h15_training_candidate_evidence_issue"] = (
                "visible_replay_baseline_top3_set_mismatch"
            )
            row["h15_training_candidate_race_evidence_eligible"] = True
        reseal_race(rows)

        report = build_report(rows, "2026-08-26")

        self.assertEqual(report["coverage"]["integrityInvalidRaces"], 0)
        self.assertEqual(
            report["coverage"]["replayBaselineTop3MismatchExcludedRaces"],
            0,
        )
        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 1)
        self.assertEqual(report["races"][0]["snapshotFieldSize"], 4)
        self.assertEqual(report["races"][0]["fieldSize"], 3)

    def test_clean_plus_five_ten_fifteen_only_reaches_formal_replay(self):
        # The rescue is the real Aug-19 replay fixture: FORTUNELLO moves from
        # replay-baseline rank 4 to candidate rank 3. Remaining rows provide
        # clean prospective checkpoint volume without inventing another gain.
        rows = historical_rescue_rows(0)
        for index in range(1, 15):
            rows.extend(race_rows(
                index,
                baseline_winner_rank=2,
                candidate_winner_rank=1 if index <= 4 else 2,
            ))

        report = build_report(rows, "2026-09-04")

        self.assertEqual([item["atRace"] for item in report["checkpoints"]], [5, 10, 15])
        self.assertTrue(all(item["passed"] for item in report["checkpoints"]))
        self.assertEqual(report["cumulative"]["rescues"], 1)
        self.assertEqual(report["cumulative"]["damages"], 0)
        self.assertTrue(report["sourceGateReady"])
        self.assertTrue(report["separationQuality"]["passed"])
        self.assertTrue(report["rankQualityReady"])
        self.assertTrue(report["calibration"]["evidenceReady"])
        self.assertEqual(
            report["calibration"]["evidence"]["artifactSha256"],
            report["calibration"]["evidence"]["expectedArtifactSha256"],
        )
        self.assertTrue(report["calibrationGateReady"])
        self.assertLessEqual(report["calibration"]["brierDelta"], 0.005)
        self.assertLessEqual(report["calibration"]["candidate"]["ece"], 0.10)
        self.assertTrue(report["formalReplaySupported"])
        self.assertEqual(report["status"], "SUPPORTED_FOR_FORMAL_REPLAY")
        self.assertFalse(report["liveRolloutEligible"])
        self.assertFalse(report["telegramVisible"])
        self.assertEqual(report["promotionCeiling"], "formal_replay_only")

    def test_missing_calibration_artifact_holds_formal_support(self):
        rows = historical_rescue_rows(0)
        for index in range(1, 15):
            rows.extend(race_rows(
                index,
                baseline_winner_rank=2,
                candidate_winner_rank=1 if index <= 4 else 2,
            ))

        with tempfile.TemporaryDirectory() as tmp:
            with patch(
                "automation.h15_training_shadow_monitor.CALIBRATION_EVIDENCE_PATH",
                Path(tmp) / "missing.json",
            ):
                report = build_report(rows, "2026-09-04")

        self.assertEqual(report["coverage"]["fullyLabeledEvidenceRaces"], 15)
        self.assertFalse(report["calibration"]["evidenceReady"])
        self.assertEqual(
            report["calibration"]["evidence"]["reason"],
            "artifact_missing",
        )
        self.assertFalse(report["calibrationGateReady"])
        self.assertFalse(report["formalReplaySupported"])
        self.assertEqual(report["status"], "HOLD_CALIBRATION_EVIDENCE")

    def test_tampered_calibration_artifact_hash_holds(self):
        rows = historical_rescue_rows(0)
        source = (
            Path(__file__).parent
            / "automation"
            / "evidence"
            / "h15_training_degree_plus2_calibration_20260819.json"
        )
        with tempfile.TemporaryDirectory() as tmp:
            tampered = Path(tmp) / "evidence.json"
            tampered.write_bytes(source.read_bytes() + b" ")
            with patch(
                "automation.h15_training_shadow_monitor.CALIBRATION_EVIDENCE_PATH",
                tampered,
            ):
                report = build_report(rows, "2026-08-21")

        self.assertFalse(report["calibration"]["evidenceReady"])
        self.assertEqual(
            report["calibration"]["evidence"]["reason"],
            "artifact_sha256_mismatch",
        )
        self.assertEqual(report["status"], "HOLD_CALIBRATION_EVIDENCE")

    def test_schema_mismatched_calibration_artifact_holds_after_valid_hash(self):
        rows = historical_rescue_rows(0)
        source = (
            Path(__file__).parent
            / "automation"
            / "evidence"
            / "h15_training_degree_plus2_calibration_20260819.json"
        )
        payload = json.loads(source.read_text(encoding="utf-8"))
        payload["schemaVersion"] = "unexpected-schema"
        raw = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        artifact_hash = hashlib.sha256(raw).hexdigest()
        with tempfile.TemporaryDirectory() as tmp:
            mismatched = Path(tmp) / "evidence.json"
            mismatched.write_bytes(raw)
            with patch(
                "automation.h15_training_shadow_monitor.CALIBRATION_EVIDENCE_PATH",
                mismatched,
            ), patch(
                "automation.h15_training_shadow_monitor.EXPECTED_CALIBRATION_EVIDENCE_SHA256",
                artifact_hash,
            ):
                report = build_report(rows, "2026-08-21")

        self.assertFalse(report["calibration"]["evidenceReady"])
        self.assertEqual(
            report["calibration"]["evidence"]["reason"],
            "artifact_schema_mismatch",
        )
        self.assertEqual(report["status"], "HOLD_CALIBRATION_EVIDENCE")

    def test_calibration_artifact_identity_fields_are_fail_closed(self):
        rows = historical_rescue_rows(0)
        source = (
            Path(__file__).parent
            / "automation"
            / "evidence"
            / "h15_training_degree_plus2_calibration_20260819.json"
        )
        mutations = (
            (
                "candidateId",
                lambda payload: payload.__setitem__("candidateId", "wrong"),
            ),
            (
                "splitRaces",
                lambda payload: payload["splitRaces"].__setitem__("build", 19),
            ),
            (
                "baselineTemperature",
                lambda payload: payload["temperatureFit"].__setitem__("baseline", 18.0),
            ),
            (
                "sourceSnapshotAt",
                lambda payload: payload["sourceReplayIdentity"].__setitem__(
                    "sourceSnapshotAt",
                    "wrong",
                ),
            ),
        )
        for expected_field, mutate in mutations:
            with self.subTest(field=expected_field):
                payload = json.loads(source.read_text(encoding="utf-8"))
                mutate(payload)
                raw = (
                    json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
                ).encode("utf-8")
                artifact_hash = hashlib.sha256(raw).hexdigest()
                with tempfile.TemporaryDirectory() as tmp:
                    mismatched = Path(tmp) / "evidence.json"
                    mismatched.write_bytes(raw)
                    with patch(
                        "automation.h15_training_shadow_monitor.CALIBRATION_EVIDENCE_PATH",
                        mismatched,
                    ), patch(
                        "automation.h15_training_shadow_monitor.EXPECTED_CALIBRATION_EVIDENCE_SHA256",
                        artifact_hash,
                    ):
                        report = build_report(rows, "2026-08-21")
                evidence = report["calibration"]["evidence"]
                self.assertFalse(evidence["ready"])
                self.assertEqual(evidence["reason"], "artifact_identity_mismatch")
                self.assertEqual(evidence["mismatchField"], expected_field)
                self.assertEqual(report["status"], "HOLD_CALIBRATION_EVIDENCE")

    def test_monitor_is_nonblocking_after_persist_before_registry_and_commit(self):
        script = (
            Path(__file__).parent / "scripts" / "raspberry" / "run-automation.sh"
        ).read_text(encoding="utf-8")
        persist_at = script.rindex("persist_state_predictions")
        monitor_at = script.index("python3 automation/h15_training_shadow_monitor.py")
        registry_at = script.index("python3 automation/metric_signal_registry.py")
        commit_at = script.index('git -C "$DATA_DIR" add automation predictions.jsonl')

        self.assertLess(persist_at, monitor_at)
        self.assertLess(monitor_at, registry_at)
        self.assertLess(monitor_at, commit_at)
        self.assertIn("if ! python3 automation/h15_training_shadow_monitor.py", script)

    def test_cli_and_persistence(self):
        result = subprocess.run(
            [sys.executable, "automation/h15_training_shadow_monitor.py", "--help"],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            persist(build_report(race_rows(0), "2026-08-21"), root)
            self.assertTrue(
                (
                    root
                    / "automation"
                    / "runs"
                    / "2026-08-21"
                    / "h15-training-shadow-checkpoint.json"
                ).exists()
            )
            self.assertTrue(
                (root / "automation" / "h15-training-shadow" / "latest.md").exists()
            )


if __name__ == "__main__":
    unittest.main()
