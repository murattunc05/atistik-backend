import unittest

from metric_opportunity_analysis import (
    agf_allowed_for_segment,
    finish_relevance,
    metric_diagnostics,
    normalize_weights,
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
