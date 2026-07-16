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

        selected, summary = training.filter_training_entries(entries)

        self.assertEqual(len(selected), 5)
        self.assertEqual(summary["integrity_clean_races"], 1)
        self.assertEqual(summary["integrity_invalid_races"], 0)
        self.assertEqual(summary["valid_tie_races"], 1)
        self.assertEqual(summary["terminal_status_races"], 1)
        self.assertEqual(summary["terminal_status_rows"], 1)

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


if __name__ == "__main__":
    unittest.main()
