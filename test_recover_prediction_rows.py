import json
import tempfile
import unittest
from pathlib import Path

from recover_prediction_rows import recover_entries, write_atomic


class RecoverPredictionRowsTests(unittest.TestCase):
    def _manifest(self, root: Path, race_id: str = "226100", race_date: str = "15.07.2026") -> Path:
        path = root / "2026-07-15" / "analysis.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps(
                {
                    "raceDate": race_date,
                    "finishedAt": "2026-07-15T04:00:00Z",
                    "cities": [
                        {
                            "races": [
                                {
                                    "status": "analyzed",
                                    "raceId": race_id,
                                    "raceNo": "1",
                                    "raceType": "Maiden",
                                    "horseCount": 2,
                                    "distance": "1200",
                                    "track": "Kum",
                                    "rankings": [
                                        {"horse": "AĞA SAÇAN", "rank": 1, "aiScore": 61.2, "v4Rank": 1, "v4Score": 61.2},
                                        {"horse": "SUPER CHIRON", "rank": 2, "aiScore": 55.1, "v4Rank": 2, "v4Score": 55.1},
                                    ],
                                }
                            ]
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return path

    def test_recovers_only_missing_rows_and_marks_feature_limitation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = self._manifest(root)
            existing = [
                {"race_id": "226100", "race_date": "15.07.2026", "race_no": "1", "horse_name": "AĞASAÇAN"}
            ]

            output, summary = recover_entries(existing, [manifest])

        self.assertEqual(summary["recovered_rows"], 1)
        self.assertEqual(summary["existing_rows_skipped"], 1)
        self.assertEqual(output[-1]["horse_name"], "SUPER CHIRON")
        self.assertEqual(output[-1]["features"], {})
        self.assertEqual(output[-1]["recovery_limitations"], "rankings_only_no_features")
        self.assertEqual(output[-1]["v4_profile"], "MAIDEN")

    def test_second_pass_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self._manifest(Path(tmp))
            first, first_summary = recover_entries([], [manifest])
            second, second_summary = recover_entries(first, [manifest])

        self.assertEqual(first_summary["recovered_rows"], 2)
        self.assertEqual(second_summary["recovered_rows"], 0)
        self.assertEqual(second, first)

    def test_rejects_race_id_identity_collision(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self._manifest(Path(tmp), race_date="15.07.2026")
            existing = [
                {"race_id": "226100", "race_date": "14.07.2026", "race_no": "1", "horse_name": "OLD"}
            ]
            with self.assertRaisesRegex(ValueError, "kimligi celisiyor"):
                recover_entries(existing, [manifest])

    def test_atomic_writer_produces_valid_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "predictions.jsonl"
            write_atomic(path, [{"horse_name": "ŞİMŞEK"}, {"horse_name": "A"}])
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([row["horse_name"] for row in rows], ["ŞİMŞEK", "A"])


if __name__ == "__main__":
    unittest.main()
