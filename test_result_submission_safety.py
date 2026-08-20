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

    def test_new_terminal_label_preserves_official_status_metadata(self):
        entries = [
            {
                "race_id": "226749",
                "race_date": "13.08.2026",
                "race_no": "9",
                "horse_name": "UĞURLU NİLGÜN",
                "finish_pos": None,
            },
        ]

        outcome = reconcile_result_submission(
            entries,
            race_id="226749",
            race_date="13.08.2026",
            race_no="9",
            results=[{
                "horse_name": "UĞURLU NİLGÜN",
                "finish_pos": 99,
                "result_status": "unranked_terminal",
                "terminal_reason": "Derecesiz",
                "result_source": "tjk_official_results",
            }],
        )

        self.assertEqual(outcome["updated"], 1)
        labeled = outcome["entries"][0]
        self.assertEqual(labeled["finish_pos"], 99)
        self.assertEqual(labeled["result_status"], "unranked_terminal")
        self.assertEqual(labeled["terminal_reason"], "Derecesiz")
        self.assertEqual(labeled["result_source"], "tjk_official_results")

    def test_nonpositive_sentinels_are_replaced_by_authoritative_results(self):
        entries = [
            {
                "race_id": "226684",
                "race_date": "10.08.2026",
                "race_no": "6",
                "horse_name": "LITTLE STONE",
                "finish_pos": 0,
            },
            {
                "race_id": "226684",
                "race_date": "10.08.2026",
                "race_no": "6",
                "horse_name": "SINIR KARTALI",
                "finish_pos": -1,
            },
        ]
        results = [
            {
                "horse_name": "LITTLE STONE",
                "finish_pos": 99,
                "result_status": "unranked_terminal",
                "terminal_reason": "Derecesiz",
                "result_source": "tjk_official_results",
            },
            {
                "horse_name": "SINIR KARTALI",
                "finish_pos": 99,
                "result_status": "non_runner",
                "terminal_reason": "Koşmaz",
                "result_source": "tjk_official_results",
            },
        ]

        repaired = reconcile_result_submission(
            entries,
            race_id="226684",
            race_date="10.08.2026",
            race_no="6",
            results=results,
        )
        replay = reconcile_result_submission(
            repaired["entries"],
            race_id="226684",
            race_date="10.08.2026",
            race_no="6",
            results=results,
        )

        self.assertEqual(repaired["updated"], 2)
        self.assertEqual(repaired["conflicts"], [])
        self.assertEqual(repaired["entries"][0]["finish_pos"], 99)
        self.assertEqual(repaired["entries"][0]["terminal_reason"], "Derecesiz")
        self.assertEqual(repaired["entries"][1]["finish_pos"], 99)
        self.assertEqual(repaired["entries"][1]["terminal_reason"], "Koşmaz")
        self.assertEqual(replay["updated"], 0)
        self.assertEqual(replay["idempotent"], 2)

    def test_sentinel_repair_is_atomic_with_a_positive_label_conflict(self):
        entries = [
            {
                "race_id": "226707",
                "race_date": "11.08.2026",
                "race_no": "1",
                "horse_name": "İKLİMECE",
                "finish_pos": 0,
            },
            {
                "race_id": "226707",
                "race_date": "11.08.2026",
                "race_no": "1",
                "horse_name": "OTHER",
                "finish_pos": 2,
            },
        ]

        outcome = reconcile_result_submission(
            entries,
            race_id="226707",
            race_date="11.08.2026",
            race_no="1",
            results=[
                {
                    "horse_name": "İKLİMECE",
                    "finish_pos": 99,
                    "result_status": "unranked_terminal",
                    "terminal_reason": "Derecesiz",
                    "result_source": "tjk_official_results",
                },
                {
                    "horse_name": "OTHER",
                    "finish_pos": 1,
                    "result_status": "finished",
                    "result_source": "tjk_official_results",
                },
            ],
        )

        self.assertEqual(outcome["would_update"], 1)
        self.assertEqual(outcome["updated"], 0)
        self.assertEqual(len(outcome["conflicts"]), 1)
        self.assertEqual(outcome["entries"], entries)

    def test_invalid_incoming_result_is_rejected(self):
        entries = [{
            "race_id": "226707",
            "race_date": "11.08.2026",
            "race_no": "1",
            "horse_name": "İKLİMECE",
            "finish_pos": 0,
        }]

        for invalid_position in (0, -1, 1.5, float("nan"), True):
            with self.subTest(finish_pos=invalid_position):
                outcome = reconcile_result_submission(
                    entries,
                    race_id="226707",
                    race_date="11.08.2026",
                    race_no="1",
                    results=[{
                        "horse_name": "İKLİMECE",
                        "finish_pos": invalid_position,
                    }],
                )

                self.assertEqual(outcome["incoming"], 0)
                self.assertEqual(outcome["matched"], 0)
                self.assertEqual(outcome["updated"], 0)
                self.assertEqual(outcome["entries"], entries)

    def test_same_position_backfills_missing_official_metadata(self):
        entries = [{
            "race_id": "226749",
            "race_date": "13.08.2026",
            "race_no": "9",
            "horse_name": "UĞURLU NİLGÜN",
            "finish_pos": 99,
            "is_winner": 0,
        }]
        result = {
            "horse_name": "UĞURLU NİLGÜN",
            "finish_pos": 99,
            "result_status": "unranked_terminal",
            "terminal_reason": "Derecesiz",
            "result_source": "tjk_official_results",
        }

        backfill = reconcile_result_submission(
            entries,
            race_id="226749",
            race_date="13.08.2026",
            race_no="9",
            results=[result],
        )
        replay = reconcile_result_submission(
            backfill["entries"],
            race_id="226749",
            race_date="13.08.2026",
            race_no="9",
            results=[result],
        )

        self.assertEqual(backfill["updated"], 1)
        self.assertEqual(backfill["idempotent"], 0)
        self.assertEqual(backfill["entries"][0]["result_status"], "unranked_terminal")
        self.assertEqual(replay["updated"], 0)
        self.assertEqual(replay["idempotent"], 1)

    def test_conflicting_nonempty_terminal_metadata_is_atomic_no_op(self):
        entries = [
            {
                "race_id": "226749",
                "race_date": "13.08.2026",
                "race_no": "9",
                "horse_name": "UĞURLU NİLGÜN",
                "finish_pos": 99,
                "is_winner": 0,
                "result_status": "non_runner",
                "terminal_reason": "Koşmaz",
                "result_source": "tjk_official_results",
            },
            {
                "race_id": "226749",
                "race_date": "13.08.2026",
                "race_no": "9",
                "horse_name": "HAÇOVALI",
                "finish_pos": 1,
                "is_winner": 1,
            },
        ]

        outcome = reconcile_result_submission(
            entries,
            race_id="226749",
            race_date="13.08.2026",
            race_no="9",
            results=[
                {
                    "horse_name": "UĞURLU NİLGÜN",
                    "finish_pos": 99,
                    "result_status": "unranked_terminal",
                    "terminal_reason": "Derecesiz",
                    "result_source": "tjk_official_results",
                },
                {
                    "horse_name": "HAÇOVALI",
                    "finish_pos": 1,
                    "result_status": "finished",
                    "result_source": "tjk_official_results",
                },
            ],
        )

        self.assertEqual(outcome["updated"], 0)
        self.assertEqual(outcome["would_update"], 1)
        self.assertEqual(outcome["entries"], entries)
        self.assertEqual(len(outcome["conflicts"]), 1)
        self.assertEqual(outcome["conflicts"][0]["conflict_type"], "result_metadata")
        self.assertEqual(
            {item["field"] for item in outcome["conflicts"][0]["metadata_conflicts"]},
            {"result_status", "terminal_reason"},
        )

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
