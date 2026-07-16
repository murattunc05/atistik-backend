import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from metric_opportunity_analysis import (
    agf_allowed_for_segment,
    finish_relevance,
    load_local_entries,
    metric_diagnostics,
    normalize_weights,
    select_entries_by_label_coverage,
    split_train_holdout,
)


def race(race_id, race_date="01.06.2026", race_no=1, category="HANDIKAP", selected="HANDIKAP15|Kum"):
    return [
        {
            "race_id": race_id,
            "race_date": race_date,
            "race_no": race_no,
            "finish_pos": 1,
            "_sim_v418_profile": {"category": category, "subtype": selected},
            "_sim_v418_resolved": {"selectedKey": selected, "weights": {}},
            "_sim_v418_metrics": {
                "agf_score": 80,
                "_has_agf": True,
                "degree_avg": 70,
            },
            "features": {"agf_score": 80, "degree_avg": 70},
        },
        {
            "race_id": race_id,
            "race_date": race_date,
            "race_no": race_no,
            "finish_pos": 2,
            "_sim_v418_profile": {"category": category, "subtype": selected},
            "_sim_v418_resolved": {"selectedKey": selected, "weights": {}},
            "_sim_v418_metrics": {
                "agf_score": 40,
                "_has_agf": True,
                "degree_avg": 40,
            },
            "features": {"agf_score": 40, "degree_avg": 40},
        },
    ]


class MetricOpportunityAnalysisTest(unittest.TestCase):
    def test_full_only_selection_excludes_entire_partial_and_unlabeled_races(self):
        full = race("full")
        partial = race("partial")
        partial[1]["finish_pos"] = None
        unlabeled = race("unlabeled")
        for row in unlabeled:
            row["finish_pos"] = None

        selected, coverage = select_entries_by_label_coverage(full + partial + unlabeled)

        self.assertEqual({row["race_id"] for row in selected}, {"full"})
        self.assertEqual(coverage["fully_labeled_races"], 1)
        self.assertEqual(coverage["partially_labeled_races"], 1)
        self.assertEqual(coverage["unlabeled_races"], 1)
        self.assertEqual(coverage["partial_labeled_rows"], 1)
        self.assertEqual(coverage["partial_unlabeled_rows"], 1)
        self.assertEqual(coverage["selected_rows"], 2)

    def test_partial_rows_require_explicit_opt_in(self):
        partial = race("partial")
        partial[1]["finish_pos"] = None

        selected, coverage = select_entries_by_label_coverage(partial, include_partial=True)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["finish_pos"], 1)
        self.assertEqual(coverage["selected_rows"], 1)

    def test_integrity_summary_accepts_competition_ranking_ties(self):
        tied = race("tied")
        tied.append({**tied[1], "finish_pos": 2})
        tied.append({**tied[1], "finish_pos": 4})

        selected, coverage = select_entries_by_label_coverage(tied)

        self.assertEqual(len(selected), 4)
        self.assertEqual(coverage["integrity_clean_fully_labeled_races"], 1)
        self.assertEqual(coverage["valid_tie_races"], 1)
        self.assertEqual(coverage["competition_pattern_invalid_races"], 0)

    def test_integrity_summary_reports_broken_pattern_and_out_of_range(self):
        broken = race("broken")
        broken.extend([
            {**broken[1], "finish_pos": 4},
            {**broken[1], "finish_pos": 4},
        ])
        out_of_range = race("out-of-range")
        out_of_range[1]["finish_pos"] = 9

        selected, coverage = select_entries_by_label_coverage(broken + out_of_range)

        self.assertEqual(selected, [])
        self.assertEqual(coverage["competition_pattern_invalid_races"], 2)
        self.assertEqual(coverage["rank_out_of_range_races"], 1)
        self.assertEqual(coverage["rank_out_of_range_rows"], 1)
        self.assertEqual(coverage["integrity_clean_fully_labeled_races"], 0)
        self.assertEqual(coverage["integrity_invalid_fully_labeled_races"], 2)
        self.assertEqual(coverage["integrity_invalid_fully_labeled_rows"], 6)
        self.assertEqual(coverage["selected_races"], 0)

    def test_partial_opt_in_does_not_bypass_full_race_integrity_guard(self):
        broken = race("broken")
        broken[1]["finish_pos"] = 9

        selected, coverage = select_entries_by_label_coverage(broken, include_partial=True)

        self.assertEqual(selected, [])
        self.assertEqual(coverage["integrity_invalid_fully_labeled_races"], 1)
        self.assertEqual(coverage["selected_races"], 0)

    def test_integrity_summary_accepts_99_as_official_terminal_status(self):
        terminal = race("terminal")
        terminal.append({**terminal[1], "finish_pos": 99})

        selected, coverage = select_entries_by_label_coverage(terminal)

        self.assertEqual(len(selected), 3)
        self.assertEqual(coverage["integrity_clean_fully_labeled_races"], 1)
        self.assertEqual(coverage["terminal_status_races"], 1)
        self.assertEqual(coverage["terminal_status_rows"], 1)
        self.assertEqual(coverage["rank_out_of_range_races"], 0)
        self.assertEqual(coverage["competition_pattern_invalid_races"], 0)

    def test_local_loader_accepts_predictions_jsonl(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "predictions.jsonl"
            path.write_text(
                '{"race_id":"one","finish_pos":1}\n'
                '{"race_id":"two","finish_pos":2}\n',
                encoding="utf-8",
            )

            entries = load_local_entries(path)

        self.assertEqual([entry["race_id"] for entry in entries], ["one", "two"])

    def test_finish_relevance_maps_winner_to_100_and_last_to_zero(self):
        rows = [{"finish_pos": 3}, {"finish_pos": 1}, {"finish_pos": 2}]

        relevance = finish_relevance(rows)

        self.assertEqual(relevance[id(rows[1])], 100.0)
        self.assertEqual(relevance[id(rows[2])], 50.0)
        self.assertEqual(relevance[id(rows[0])], 0.0)

    def test_normalize_weights_drops_negative_and_sums_to_one(self):
        weights = normalize_weights({"a": 2, "b": -5, "c": 6})

        self.assertEqual(set(weights), {"a", "c"})
        self.assertAlmostEqual(sum(weights.values()), 1.0, places=6)
        self.assertGreater(weights["c"], weights["a"])

    def test_split_train_holdout_is_chronological_and_disjoint(self):
        races = [
            race("late", "03.06.2026", 1),
            race("early", "01.06.2026", 1),
            race("middle", "02.06.2026", 1),
        ]

        train, holdout = split_train_holdout(races)

        self.assertEqual([rows[0]["race_id"] for rows in train], ["early", "middle"])
        self.assertEqual([rows[0]["race_id"] for rows in holdout], ["late"])

    def test_agf_is_eligible_only_for_maiden_and_sart1(self):
        maiden = race("maiden", category="MAIDEN", selected="MAIDEN")
        sart1 = race("sart1", category="SARTLI", selected="SART1")
        handicap = race("handicap", category="HANDIKAP", selected="HANDIKAP15|Kum")

        self.assertTrue(agf_allowed_for_segment("MAIDEN", [maiden]))
        self.assertTrue(agf_allowed_for_segment("SART1", [sart1]))
        self.assertFalse(agf_allowed_for_segment("HANDIKAP", [handicap]))

    def test_metric_diagnostics_masks_agf_outside_allowed_segments(self):
        diagnostics = metric_diagnostics([race("handicap")], "HANDIKAP")
        agf = next(item for item in diagnostics if item["metric"] == "agf_score")
        degree = next(item for item in diagnostics if item["metric"] == "degree_avg")

        self.assertFalse(agf["eligible"])
        self.assertEqual(agf["reason"], "agf_disabled_for_segment")
        self.assertTrue(degree["eligible"])

    def test_sartli_group_does_not_enable_agf_because_first_race_is_sart1(self):
        diagnostics = metric_diagnostics(
            [
                race("sart1", category="SARTLI", selected="SART1"),
                race("sart4", category="SARTLI", selected="SART4"),
            ],
            "SARTLI",
        )
        agf = next(item for item in diagnostics if item["metric"] == "agf_score")

        self.assertFalse(agf["eligible"])
        self.assertEqual(agf["reason"], "agf_disabled_for_segment")


if __name__ == "__main__":
    unittest.main()
