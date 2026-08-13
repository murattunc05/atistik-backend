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
