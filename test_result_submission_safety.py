import unittest

from result_submission import clean_result_name, reconcile_result_submission


class ResultSubmissionSafetyTests(unittest.TestCase):
    def test_exact_race_update_then_idempotent_replay(self):
        entries = [
            {"race_id": "226100", "race_date": "15.07.2026", "race_no": "1", "horse_name": "SUPER CHIRON", "finish_pos": None},
            {"race_id": "226100", "race_date": "15.07.2026", "race_no": "1", "horse_name": "AĞA-SAÇAN", "finish_pos": None},
        ]
        results = [
            {"horse_name": "SUPERCHIRON", "finish_pos": 1},
            {"horse_name": "AĞA SAÇAN", "finish_pos": 2},
        ]

        first = reconcile_result_submission(
            entries,
            race_id="226100",
            race_date="15.07.2026",
            race_no="1",
            results=results,
        )
        second = reconcile_result_submission(
            first["entries"],
            race_id="226100",
            race_date="15.07.2026",
            race_no="1",
            results=results,
        )

        self.assertEqual(clean_result_name("SUPER CHIRON"), "SUPERCHIRON")
        self.assertEqual(first["updated"], 2)
        self.assertEqual(first["idempotent"], 0)
        self.assertEqual(second["updated"], 0)
        self.assertEqual(second["idempotent"], 2)
        self.assertEqual(second["conflicts"], [])

    def test_conflict_makes_the_whole_submission_a_no_op(self):
        entries = [
            {"race_id": "226100", "race_date": "15.07.2026", "race_no": "1", "horse_name": "A", "finish_pos": 1},
            {"race_id": "226100", "race_date": "15.07.2026", "race_no": "1", "horse_name": "B", "finish_pos": None},
        ]

        outcome = reconcile_result_submission(
            entries,
            race_id="226100",
            race_date="15.07.2026",
            race_no="1",
            results=[
                {"horse_name": "A", "finish_pos": 2},
                {"horse_name": "B", "finish_pos": 1},
            ],
        )

        self.assertEqual(outcome["updated"], 0)
        self.assertEqual(outcome["would_update"], 1)
        self.assertEqual(len(outcome["conflicts"]), 1)
        self.assertIsNone(outcome["entries"][1]["finish_pos"])

    def test_legacy_fallback_only_considers_entries_without_race_date(self):
        entries = [
            {"race_id": "old-dated", "race_date": "01.06.2026", "race_no": "1", "horse_name": "A", "finish_pos": None},
            {"race_id": "old-dated", "race_date": "01.06.2026", "race_no": "1", "horse_name": "B", "finish_pos": None},
            {"race_id": "legacy", "race_date": "", "race_no": "", "horse_name": "A", "finish_pos": None},
            {"race_id": "legacy", "horse_name": "B", "finish_pos": None},
        ]
        results = [
            {"horse_name": "A", "finish_pos": 1},
            {"horse_name": "B", "finish_pos": 2},
        ]

        outcome = reconcile_result_submission(
            entries,
            race_id="missing",
            race_date="15.07.2026",
            race_no="3",
            results=results,
            allow_legacy_fallback=True,
        )

        self.assertEqual(outcome["resolution"], "legacy_missing_date")
        self.assertEqual(outcome["resolved_race_id"], "legacy")
        self.assertEqual(outcome["updated"], 2)
        self.assertIsNone(outcome["entries"][0]["finish_pos"])
        self.assertIsNone(outcome["entries"][1]["finish_pos"])

    def test_legacy_fallback_is_disabled_without_request_date(self):
        entries = [
            {"race_id": "legacy", "horse_name": "A", "finish_pos": None},
            {"race_id": "legacy", "horse_name": "B", "finish_pos": None},
        ]

        outcome = reconcile_result_submission(
            entries,
            race_id="missing",
            race_date="",
            race_no="",
            results=[
                {"horse_name": "A", "finish_pos": 1},
                {"horse_name": "B", "finish_pos": 2},
            ],
        )

        self.assertEqual(outcome["resolution"], "none")
        self.assertEqual(outcome["matched"], 0)
        self.assertEqual(outcome["updated"], 0)

    def test_missing_modern_race_never_falls_back_to_an_old_race_by_name(self):
        entries = [
            {"race_id": "old-modern", "race_date": "01.07.2026", "race_no": "4", "horse_name": "A", "finish_pos": 1},
            {"race_id": "old-modern", "race_date": "01.07.2026", "race_no": "4", "horse_name": "B", "finish_pos": 2},
            {"race_id": "old-legacy", "horse_name": "A", "finish_pos": 3},
            {"race_id": "old-legacy", "horse_name": "B", "finish_pos": 4},
        ]

        outcome = reconcile_result_submission(
            entries,
            race_id="missing-08-july",
            race_date="08.07.2026",
            race_no="1",
            results=[
                {"horse_name": "A", "finish_pos": 1},
                {"horse_name": "B", "finish_pos": 2},
            ],
        )

        self.assertEqual(outcome["resolution"], "none")
        self.assertEqual(outcome["matched"], 0)
        self.assertEqual(outcome["updated"], 0)
        self.assertFalse(outcome["legacy_fallback_used"])
        self.assertEqual(outcome["entries"], entries)

    def test_ambiguous_legacy_overlap_is_rejected(self):
        entries = [
            {"race_id": "legacy-1", "horse_name": "A", "finish_pos": None},
            {"race_id": "legacy-1", "horse_name": "B", "finish_pos": None},
            {"race_id": "legacy-2", "horse_name": "A", "finish_pos": None},
            {"race_id": "legacy-2", "horse_name": "B", "finish_pos": None},
        ]

        outcome = reconcile_result_submission(
            entries,
            race_id="missing",
            race_date="15.07.2026",
            race_no="1",
            results=[
                {"horse_name": "A", "finish_pos": 1},
                {"horse_name": "B", "finish_pos": 2},
            ],
            allow_legacy_fallback=True,
        )

        self.assertEqual(outcome["resolution"], "none")
        self.assertEqual(outcome["updated"], 0)


if __name__ == "__main__":
    unittest.main()
