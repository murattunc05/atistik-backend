import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import api_server as api


RESULT_HTML = """
<div id="dataDiv">
  <table id="queryTable">
    <tbody id="tbody0">
      <tr>
        <td>15.07.2026</td><td>İstanbul</td><td>1400</td><td>Kum</td><td>1</td><td>ok</td>
      </tr>
    </tbody>
  </table>
</div>
"""

OFFICIAL_RESULT_HTML = """
<div id="226100">
  <table>
    <tbody>
      <tr>
        <td class="gunluk-GunlukYarisSonuclari-SONUCNO">1</td>
        <td class="gunluk-GunlukYarisSonuclari-AtAdi3"><a>SUPER CHIRON(1)</a></td>
        <td class="gunluk-GunlukYarisSonuclari-Derece">1.24.00</td>
      </tr>
      <tr>
        <td class="gunluk-GunlukYarisSonuclari-SONUCNO"></td>
        <td class="gunluk-GunlukYarisSonuclari-AtAdi3"><a>AĞA SAÇAN(2)</a> (Koşmaz)</td>
        <td class="gunluk-GunlukYarisSonuclari-Derece">Koşmaz</td>
      </tr>
    </tbody>
  </table>
</div>
"""

OFFICIAL_DERECESIZ_HTML = """
<div id="226749">
  <table>
    <tbody>
      <tr>
        <td class="gunluk-GunlukYarisSonuclari-SONUCNO">1</td>
        <td class="gunluk-GunlukYarisSonuclari-AtAdi3"><a>KIZIM RABİA(10)</a></td>
        <td class="gunluk-GunlukYarisSonuclari-Derece">1.24.00</td>
      </tr>
      <tr>
        <td class="gunluk-GunlukYarisSonuclari-SONUCNO">0</td>
        <td class="gunluk-GunlukYarisSonuclari-AtAdi3"><a>UĞURLU NİLGÜN(9)</a></td>
        <td class="gunluk-GunlukYarisSonuclari-Derece">Derecesiz</td>
      </tr>
    </tbody>
  </table>
</div>
"""

UNKNOWN_HISTORY_STATUS_HTML = RESULT_HTML.replace('<td>1</td><td>ok</td>', '<td>D</td><td>ok</td>')


class FetchResultIdentityTests(unittest.TestCase):
    def _post(self, log_path: Path, race_id: str):
        response = Mock(status_code=200, text=RESULT_HTML)
        with patch.object(api, "_PREDICTIONS_PATH", str(log_path)), patch.object(
            api.requests, "get", return_value=response
        ):
            return api.app.test_client().post(
                "/api/fetch-race-results",
                json={
                    "race_id": race_id,
                    "race_date": "15.07.2026",
                    "race_no": "1",
                    "horses": [{"name": "SUPER CHIRON", "detailLink": "/horse/1"}],
                },
            )

    def test_requested_identity_is_returned_after_exact_slot_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "predictions.jsonl"
            log_path.write_text(
                json.dumps({"race_id": "226100", "race_date": "15.07.2026", "race_no": "1"}) + "\n",
                encoding="utf-8",
            )
            response = self._post(log_path, "226100")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["success"])
        self.assertEqual(response.get_json()["race_id"], "226100")

    def test_unknown_requested_identity_is_rejected_even_when_results_parse(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "predictions.jsonl"
            log_path.write_text(
                json.dumps({"race_id": "226100", "race_date": "15.07.2026", "race_no": "1"}) + "\n",
                encoding="utf-8",
            )
            response = self._post(log_path, "WRONG-RACE")

        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.get_json()["success"])
        self.assertEqual(response.get_json()["requested_race_id"], "WRONG-RACE")

    def test_official_results_include_explicit_non_runner_as_terminal_label(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "predictions.jsonl"
            log_path.write_text(
                json.dumps({"race_id": "226100", "race_date": "15.07.2026", "race_no": "1"}) + "\n",
                encoding="utf-8",
            )
            response = Mock(status_code=200, text=OFFICIAL_RESULT_HTML)
            with patch.object(api, "_PREDICTIONS_PATH", str(log_path)), patch.object(
                api.requests, "get", return_value=response
            ) as get:
                result = api.app.test_client().post(
                    "/api/fetch-race-results",
                    json={
                        "race_id": "226100",
                        "race_date": "15.07.2026",
                        "race_no": "1",
                        "city_id": "3",
                        "city_name": "İstanbul",
                        "horses": [
                            {"name": "SUPER CHIRON", "detailLink": "/horse/1"},
                            {"name": "AĞA-SAÇAN", "detailLink": "/horse/2"},
                        ],
                    },
                )

        payload = result.get_json()
        self.assertEqual(get.call_args.kwargs["params"]["QueryParameter_Tarih"], "15/07/2026")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(payload["result_source"], "tjk_official_results")
        self.assertEqual(payload["label_status"], "complete")
        self.assertEqual(payload["label_coverage"]["labeledCount"], 2)
        non_runner = next(row for row in payload["results"] if row["horse_name"] == "AĞA SAÇAN")
        self.assertEqual(non_runner["finish_pos"], 99)
        self.assertEqual(non_runner["result_status"], "non_runner")
        self.assertEqual(non_runner["terminal_reason"], "Koşmaz")

    def test_unknown_history_text_is_not_fabricated_as_terminal_99(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "predictions.jsonl"
            log_path.write_text(
                json.dumps({"race_id": "226100", "race_date": "15.07.2026", "race_no": "1"}) + "\n",
                encoding="utf-8",
            )
            response = Mock(status_code=200, text=UNKNOWN_HISTORY_STATUS_HTML)
            with patch.object(api, "_PREDICTIONS_PATH", str(log_path)), patch.object(
                api.requests, "get", return_value=response
            ):
                result = api.app.test_client().post(
                    "/api/fetch-race-results",
                    json={
                        "race_id": "226100",
                        "race_date": "15.07.2026",
                        "race_no": "1",
                        "horses": [{"name": "SUPER CHIRON", "detailLink": "/horse/1"}],
                    },
                )

        self.assertEqual(result.status_code, 404)
        self.assertFalse(result.get_json()["success"])
        self.assertNotIn(99, [row.get("finish_pos") for row in result.get_json().get("results", [])])

    def test_history_parser_never_accepts_bare_zero_or_unknown_code(self):
        self.assertIsNone(api._parse_history_finish_position("0"))
        self.assertIsNone(api._parse_history_finish_position("D"))
        self.assertIsNone(api._parse_history_finish_position("Derecesiz"))
        self.assertEqual(api._parse_history_finish_position("Koşmaz"), 99)
        self.assertEqual(api._parse_history_finish_position("3"), 3)

    def test_official_zero_plus_exact_derecesiz_is_unranked_terminal(self):
        parsed = api._parse_official_result_race(OFFICIAL_DERECESIZ_HTML, "226749")

        terminal = next(
            row for row in parsed["results"]
            if row["horse_name"] == "UĞURLU NİLGÜN"
        )
        self.assertEqual(terminal["finish_pos"], 99)
        self.assertEqual(terminal["result_status"], "unranked_terminal")
        self.assertEqual(terminal["terminal_reason"], "Derecesiz")
        self.assertEqual(parsed["explicitNonRunnerCount"], 0)
        self.assertEqual(parsed["explicitUnrankedTerminalCount"], 1)
        self.assertEqual(parsed["unresolved"], [])

    def test_official_derecesiz_completes_endpoint_label_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "predictions.jsonl"
            log_path.write_text(
                json.dumps({
                    "race_id": "226749",
                    "race_date": "13.08.2026",
                    "race_no": "9",
                }) + "\n",
                encoding="utf-8",
            )
            response = Mock(status_code=200, text=OFFICIAL_DERECESIZ_HTML)
            with patch.object(api, "_PREDICTIONS_PATH", str(log_path)), patch.object(
                api.requests, "get", return_value=response
            ):
                result = api.app.test_client().post(
                    "/api/fetch-race-results",
                    json={
                        "race_id": "226749",
                        "race_date": "13.08.2026",
                        "race_no": "9",
                        "city_id": "9",
                        "city_name": "Kocaeli",
                        "horses": [
                            {"name": "KIZIM RABİA", "detailLink": "/horse/1"},
                            {"name": "UĞURLU NİLGÜN", "detailLink": "/horse/2"},
                        ],
                    },
                )

        payload = result.get_json()
        self.assertEqual(result.status_code, 200)
        self.assertEqual(payload["label_status"], "complete")
        self.assertTrue(payload["label_coverage"]["complete"])
        self.assertEqual(payload["label_coverage"]["labeledCount"], 2)
        terminal = next(
            row for row in payload["results"]
            if row["horse_name"] == "UĞURLU NİLGÜN"
        )
        self.assertEqual(terminal["finish_pos"], 99)
        self.assertEqual(terminal["result_status"], "unranked_terminal")

    def test_submit_endpoint_backfills_terminal_metadata_then_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "predictions.jsonl"
            log_path.write_text(
                json.dumps({
                    "race_id": "226749",
                    "race_date": "13.08.2026",
                    "race_no": "9",
                    "horse_name": "UĞURLU NİLGÜN",
                    "finish_pos": 99,
                    "is_winner": 0,
                }, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            body = {
                "race_id": "226749",
                "race_date": "13.08.2026",
                "race_no": "9",
                "results": [{
                    "horse_name": "UĞURLU NİLGÜN",
                    "finish_pos": 99,
                    "result_status": "unranked_terminal",
                    "terminal_reason": "Derecesiz",
                    "result_source": "tjk_official_results",
                }],
            }
            with patch.object(api, "_PREDICTIONS_PATH", str(log_path)), patch.object(
                api, "github_backup"
            ) as backup:
                client = api.app.test_client()
                first = client.post("/api/submit-results", json=body)
                second = client.post("/api/submit-results", json=body)

            saved = json.loads(log_path.read_text(encoding="utf-8"))

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.get_json()["updated"], 1)
        self.assertEqual(first.get_json()["idempotent"], 0)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.get_json()["updated"], 0)
        self.assertEqual(second.get_json()["idempotent"], 1)
        self.assertEqual(saved["result_status"], "unranked_terminal")
        self.assertEqual(saved["terminal_reason"], "Derecesiz")
        self.assertEqual(saved["result_source"], "tjk_official_results")
        backup.assert_called_once()

    def test_official_bare_zero_never_becomes_terminal(self):
        bare_zero = OFFICIAL_DERECESIZ_HTML.replace(
            ">Derecesiz</td>",
            "></td>",
        )

        parsed = api._parse_official_result_race(bare_zero, "226749")

        self.assertNotIn(
            "UĞURLU NİLGÜN",
            [row["horse_name"] for row in parsed["results"]],
        )
        self.assertEqual(parsed["unresolved"][0]["raw_position"], "0")
        self.assertEqual(parsed["explicitUnrankedTerminalCount"], 0)

    def test_official_derecesiz_requires_exact_zero_and_exact_marker(self):
        unsafe_variants = (
            OFFICIAL_DERECESIZ_HTML.replace(
                'SONUCNO">0</td>',
                'SONUCNO"></td>',
            ),
            OFFICIAL_DERECESIZ_HTML.replace(
                ">Derecesiz</td>",
                ">Derecesiz açıklama</td>",
            ),
        )

        for html in unsafe_variants:
            with self.subTest(html=html[-160:]):
                parsed = api._parse_official_result_race(html, "226749")
                self.assertNotIn(
                    "UĞURLU NİLGÜN",
                    [row["horse_name"] for row in parsed["results"]],
                )
                self.assertEqual(parsed["explicitUnrankedTerminalCount"], 0)

    def test_daily_program_parser_exposes_explicit_non_runner_status(self):
        table = api.BeautifulSoup(
            """
            <table><tbody><tr>
              <td class="gunluk-GunlukYarisProgrami-SiraId">4</td>
              <td class="gunluk-GunlukYarisProgrami-AtAdi">
                <a href="/TR/AtKosuBilgileri?AtId=4">GLONASS</a><font color="red">(Koşmaz)</font>
              </td>
            </tr></tbody></table>
            """,
            "html.parser",
        ).find("table")

        horses = api._parse_daily_horses(table)

        self.assertEqual(len(horses), 1)
        self.assertTrue(horses[0]["isNonRunner"])
        self.assertEqual(horses[0]["runnerStatus"], "non_runner")
        self.assertEqual(horses[0]["nonRunnerReason"], "Koşmaz")

    def test_extra_official_runner_prevents_complete_coverage(self):
        coverage = api._result_label_coverage(
            [{"name": "A"}, {"name": "B"}],
            [
                {"horse_name": "A", "finish_pos": 1},
                {"horse_name": "B", "finish_pos": 2},
                {"horse_name": "EXTRA", "finish_pos": 3},
            ],
        )

        self.assertFalse(coverage["complete"])
        self.assertEqual(coverage["extraHorses"], ["EXTRA"])


if __name__ == "__main__":
    unittest.main()
