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


if __name__ == "__main__":
    unittest.main()
