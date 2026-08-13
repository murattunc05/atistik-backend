import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import train_shadow_ml as training


def row(race_id, race_date, finish_pos, horse_no=1, source_flags=None):
    return {
        "race_id": race_id,
        "race_date": race_date,
        "race_no": int(str(race_id).lstrip("R") or 1),
        "horse_no": horse_no,
        "finish_pos": finish_pos,
        "field_size": 2,
        "features": {"form_trend": 50.0},
        "metric_source_flags": source_flags or {},
    }


class TrainShadowMLInputTests(unittest.TestCase):
    def test_jsonl_input_is_supported(self):
        entries = [row("R1", "01.07.2026", 1), row("R1", "01.07.2026", 2, 2)]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "predictions.jsonl"
            path.write_text("".join(json.dumps(item) + "\n" for item in entries), encoding="utf-8")
            args = SimpleNamespace(input=str(path), export_url=None, include_partial_races=False)
            loaded, summary = training.load_entries(args, with_summary=True)

        self.assertEqual(len(loaded), 2)
        self.assertEqual(summary["complete_races"], 1)

    def test_partial_race_is_excluded_as_a_whole_by_default(self):
        entries = [
            row("R1", "01.07.2026", 1),
            row("R1", "01.07.2026", 2, 2),
            row("R2", "02.07.2026", 1),
            row("R2", "02.07.2026", None, 2),
        ]
        strict, summary = training.filter_training_entries(entries)
        legacy, _ = training.filter_training_entries(entries, include_partial_races=True)

        self.assertEqual({item["race_id"] for item in strict}, {"R1"})
        self.assertEqual(summary["partial_races"], 1)
        self.assertEqual(len(legacy), 3)

    def test_integrity_guard_accepts_competition_ties_and_terminal_99(self):
        entries = [
            row("R1", "01.07.2026", 1),
            row("R1", "01.07.2026", 2, 2),
            row("R1", "01.07.2026", 2, 3),
            row("R1", "01.07.2026", 4, 4),
            row("R1", "01.07.2026", 99, 5),
        ]
        entries[-1].update({
            "result_status": "unranked_terminal",
            "terminal_reason": "Derecesiz",
            "result_source": "tjk_official_results",
        })

        selected, summary = training.filter_training_entries(entries)

        self.assertEqual(len(selected), 5)
        self.assertEqual(summary["integrity_clean_races"], 1)
        self.assertEqual(summary["integrity_invalid_races"], 0)
        self.assertEqual(summary["valid_tie_races"], 1)
        self.assertEqual(summary["terminal_status_races"], 1)
        self.assertEqual(summary["terminal_status_rows"], 1)
        self.assertEqual(summary["excluded_terminal_rows"], 0)

    def test_non_runner_and_legacy_unknown_99_are_excluded_from_training(self):
        entries = [
            row("R1", "01.07.2026", 1),
            row("R1", "01.07.2026", 2, 2),
            row("R1", "01.07.2026", 99, 3),
            row("R2", "02.07.2026", 1),
            row("R2", "02.07.2026", 2, 2),
            row("R2", "02.07.2026", 99, 3),
        ]
        entries[-1].update({
            "result_status": "non_runner",
            "terminal_reason": "Koşmaz",
            "result_source": "tjk_official_results",
        })

        selected, summary = training.filter_training_entries(entries)

        self.assertEqual(len(selected), 4)
        self.assertNotIn(99, [item["finish_pos"] for item in selected])
        self.assertEqual(summary["excluded_terminal_races"], 2)
        self.assertEqual(summary["excluded_terminal_rows"], 2)

    def test_verified_derecesiz_uses_last_rank_not_raw_99_in_metrics(self):
        rows = [
            row("R1", "01.07.2026", 1),
            row("R1", "01.07.2026", 2, 2),
            row("R1", "01.07.2026", 99, 3),
        ]
        for rank, item in enumerate(rows, start=1):
            item["field_size"] = 3
            item["rank_pred"] = rank
        rows[-1].update({
            "result_status": "unranked_terminal",
            "terminal_reason": "Derecesiz",
            "result_source": "tjk_official_results",
        })

        metrics = training.evaluate_existing({"R1": rows}, "rank_pred")

        self.assertEqual(metrics["mae"], 0.0)
        self.assertAlmostEqual(metrics["rho"], 1.0)
        self.assertAlmostEqual(metrics["ndcg5"], 1.0)

    def test_integrity_guard_excludes_broken_or_out_of_range_full_races(self):
        broken = [
            row("R1", "01.07.2026", 1),
            row("R1", "01.07.2026", 3, 2),
        ]
        out_of_range = [
            row("R2", "02.07.2026", 1),
            row("R2", "02.07.2026", 9, 2),
        ]

        selected, summary = training.filter_training_entries(broken + out_of_range)

        self.assertEqual(selected, [])
        self.assertEqual(summary["integrity_invalid_races"], 2)
        self.assertEqual(summary["integrity_invalid_rows"], 4)
        self.assertEqual(summary["competition_pattern_invalid_races"], 2)
        self.assertEqual(summary["rank_out_of_range_races"], 2)
        self.assertEqual(summary["rank_out_of_range_rows"], 2)


class TrainShadowMLSplitTests(unittest.TestCase):
    def test_chronological_split_keeps_dates_disjoint(self):
        entries = []
        for race_id, race_date in [
            ("R1", "01.07.2026"),
            ("R2", "01.07.2026"),
            ("R3", "02.07.2026"),
            ("R4", "03.07.2026"),
        ]:
            entries.extend([row(race_id, race_date, 1), row(race_id, race_date, 2, 2)])

        train, validation = training.split_races(entries, validation_ratio=0.5)
        train_dates = {item["race_date"] for rows in train.values() for item in rows}
        validation_dates = {item["race_date"] for rows in validation.values() for item in rows}

        self.assertTrue(train)
        self.assertTrue(validation)
        self.assertTrue(train_dates.isdisjoint(validation_dates))
        self.assertEqual(train_dates, {"01.07.2026"})

    def test_walk_forward_splits_keep_dates_disjoint(self):
        entries = []
        for index in range(1, 11):
            race_date = f"{index:02d}.07.2026"
            race_id = f"R{index}"
            entries.extend([row(race_id, race_date, 1), row(race_id, race_date, 2, 2)])

        folds = training.walk_forward_splits(entries, fold_count=3, initial_train_ratio=0.5)
        self.assertEqual(len(folds), 3)
        for train, validation in folds:
            train_dates = {item["race_date"] for rows in train.values() for item in rows}
            validation_dates = {item["race_date"] for rows in validation.values() for item in rows}
            self.assertTrue(train_dates.isdisjoint(validation_dates))


class TrainShadowMLFeatureGateTests(unittest.TestCase):
    def test_no_agf_variant_excludes_agf_derived_aggregates(self):
        selected = training.without_agf_features(training.FEATURE_COLS)
        self.assertNotIn("v4_score", selected)
        self.assertNotIn("v4_rank", selected)
        self.assertIn("v4_score", training.AGF_INFLUENCED_FEATURE_COLS)
        self.assertIn("v4_rank", training.AGF_INFLUENCED_FEATURE_COLS)
        for col in training.AGF_INFLUENCED_FEATURE_COLS:
            self.assertNotIn(col, selected)

    def test_sparse_new_feature_needs_both_count_and_ratio(self):
        flag = training.SOURCE_FLAG_BY_FEATURE["pace_map_edge_score"]
        races = {}
        for index in range(100):
            flags = {flag: index < 24}
            races[f"R{index}"] = [row(f"R{index}", "01.07.2026", 1, source_flags=flags)]

        selected, coverage = training.select_feature_cols(
            races,
            ["form_trend", "pace_map_edge_score"],
        )
        self.assertEqual(selected, ["form_trend"])
        self.assertEqual(coverage["pace_map_edge_score"]["source_races"], 24)

        races["R24"][0]["metric_source_flags"][flag] = True
        selected, coverage = training.select_feature_cols(
            races,
            ["form_trend", "pace_map_edge_score"],
        )
        self.assertIn("pace_map_edge_score", selected)
        self.assertEqual(coverage["pace_map_edge_score"]["source_races"], 25)

    def test_h16_training_degree_feature_is_source_gated_and_centered(self):
        entry = row("R1", "01.07.2026", 1, source_flags={"hasTrainingTimes": True})
        entry.update({
            "race_type": "HANDİKAP 16",
            "v4_profile": {"category": "HANDIKAP", "subtype": "HANDIKAP16"},
            "features": {"form_trend": 50.0, "training_degree_score": 74.0},
        })

        features = training.feature_dict(entry)

        self.assertEqual(features["is_handikap16"], 1.0)
        self.assertEqual(features["has_training_times"], 1.0)
        self.assertEqual(features["h16_training_degree_edge"], 24.0)

        entry["metric_source_flags"]["hasTrainingTimes"] = False
        self.assertEqual(training.feature_dict(entry)["h16_training_degree_edge"], 0.0)
        entry["metric_source_flags"]["hasTrainingTimes"] = True
        entry["v4_profile"]["subtype"] = "HANDIKAP15"
        entry["race_type"] = "HANDİKAP 15"
        self.assertEqual(training.feature_dict(entry)["h16_training_degree_edge"], 0.0)

    def test_h16_interaction_source_count_ignores_other_profiles(self):
        races = {}
        for index in range(40):
            race_id = f"R{100 + index}"
            item = row(race_id, "01.07.2026", 1, source_flags={"hasTrainingTimes": True})
            item["race_type"] = "HANDİKAP 15"
            item["v4_profile"] = {"category": "HANDIKAP", "subtype": "HANDIKAP15"}
            races[race_id] = [item]
        for index in range(24):
            race_id = f"R{200 + index}"
            item = row(race_id, "01.07.2026", 1, source_flags={"hasTrainingTimes": True})
            item["race_type"] = "HANDİKAP 16"
            item["v4_profile"] = {"category": "HANDIKAP", "subtype": "HANDIKAP16"}
            races[race_id] = [item]

        selected, coverage = training.select_feature_cols(
            races,
            ["form_trend", "h16_training_degree_edge"],
        )

        self.assertNotIn("h16_training_degree_edge", selected)
        self.assertEqual(coverage["h16_training_degree_edge"]["source_races"], 24)
        self.assertEqual(coverage["h16_training_degree_edge"]["eligible_races"], 24)

        item = row("R224", "01.07.2026", 1, source_flags={"hasTrainingTimes": True})
        item["race_type"] = "HANDİKAP 16"
        item["v4_profile"] = {"category": "HANDIKAP", "subtype": "HANDIKAP16"}
        races["R224"] = [item]
        selected, coverage = training.select_feature_cols(
            races,
            ["form_trend", "h16_training_degree_edge"],
        )
        self.assertIn("h16_training_degree_edge", selected)
        self.assertEqual(coverage["h16_training_degree_edge"]["source_races"], 25)


class TrainShadowMLGateTests(unittest.TestCase):
    class FakeModel:
        def predict(self, matrix):
            return matrix[:, 0]

    def test_comparison_counts_rescue_and_damage_separately(self):
        races = {}
        first = [
            row("R1", "01.07.2026", 1, 1),
            row("R1", "01.07.2026", 2, 2),
            row("R1", "01.07.2026", 3, 3),
            row("R1", "01.07.2026", 4, 4),
        ]
        second = [
            row("R2", "02.07.2026", 1, 1),
            row("R2", "02.07.2026", 2, 2),
            row("R2", "02.07.2026", 3, 3),
            row("R2", "02.07.2026", 4, 4),
        ]
        for index, item in enumerate(first):
            item["field_size"] = 4
            item["v4_rank"] = [4, 1, 2, 3][index]
            item["v4_score"] = [40, 70, 60, 50][index]
            item["features"] = {"form_trend": [100, 30, 20, 10][index]}
        for index, item in enumerate(second):
            item["field_size"] = 4
            item["v4_rank"] = index + 1
            item["v4_score"] = [70, 60, 50, 40][index]
            item["features"] = {"form_trend": [10, 100, 90, 80][index]}
        races["R1"] = first
        races["R2"] = second

        comparison = training.compare_model_to_existing(
            self.FakeModel(), races, ["form_trend"]
        )

        self.assertEqual(comparison["rescues"], 1)
        self.assertEqual(comparison["damages"], 1)
        self.assertEqual(comparison["winnerTop3Net"], 0)

    def test_visible_v4_rank_falls_back_to_rank_pred(self):
        rows = [
            {"rank_pred": 2, "horse_name": "A"},
            {"rank_pred": 1, "horse_name": "B"},
        ]

        ranks = training.rank_from_visible_v4(rows)

        self.assertEqual(ranks[id(rows[0])], 2)
        self.assertEqual(ranks[id(rows[1])], 1)

    def test_retrain_gate_rejects_h16_without_confirmed_gain(self):
        races = {}
        for race_index in range(20):
            race_id = f"R{300 + race_index}"
            rows = []
            for horse_index, score in enumerate([80.0, 70.0, 60.0, 50.0], start=1):
                item = row(race_id, f"{race_index + 1:02d}.07.2026", horse_index, horse_index)
                item.update({
                    "field_size": 4,
                    "rank_pred": horse_index,
                    "v4_rank": horse_index,
                    "v4_score": score,
                    "race_type": "HANDİKAP 16" if race_index < 6 else "HANDİKAP 15",
                    "v4_profile": {
                        "category": "HANDIKAP",
                        "subtype": "HANDIKAP16" if race_index < 6 else "HANDIKAP15",
                    },
                    "features": {"form_trend": score},
                })
                rows.append(item)
            races[race_id] = rows

        gate = training.build_retrain_gate(
            races,
            self.FakeModel(),
            ["form_trend"],
            [
                {"comparisons": {"Overall": {"winnerTop3Net": 0}}},
                {"comparisons": {"Overall": {"winnerTop3Net": 0}}},
            ],
        )

        self.assertEqual(gate["decision"], "REJECTED")
        self.assertIn("h16_winner_top3_plus_1", gate["failedChecks"])
        self.assertFalse(gate["policy"]["automaticDeployment"])


if __name__ == "__main__":
    unittest.main()
