import unittest
from pathlib import Path


ROOT = Path(__file__).parent


class RaspberryPredictionBackupTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.run_script = (ROOT / "scripts/raspberry/run-automation.sh").read_text(encoding="utf-8")
        cls.compose = (ROOT / "docker-compose.raspberry.yml").read_text(encoding="utf-8")

    def test_pi_disables_per_request_async_backup(self):
        self.assertIn('ATISTIK_GITHUB_AUTO_BACKUP: "false"', self.compose)

    def test_api_writes_directly_to_configured_state_path(self):
        api_server = (ROOT / "api_server.py").read_text(encoding="utf-8")
        self.assertIn("_os.environ.get('ATISTIK_PREDICTIONS_PATH', '').strip()", api_server)

    def test_job_persists_state_file_in_same_ml_data_commit(self):
        self.assertIn('persist_state_predictions', self.run_script)
        self.assertIn('cp "$STATE_PREDICTIONS" "$DATA_DIR/predictions.jsonl"', self.run_script)
        self.assertIn('git -C "$DATA_DIR" add automation predictions.jsonl', self.run_script)

    def test_state_backup_refuses_line_count_regression(self):
        self.assertIn('if ((state_lines < repo_lines)); then', self.run_script)

    def test_render_restore_checks_total_and_labeled_parity_for_every_mode(self):
        restore_body = self.run_script.split('restore_render_from_backup() {', 1)[1].split('\n}', 1)[0]
        self.assertNotIn('if [[ "$MODE" != "results" ]]', restore_body)
        self.assertIn('backend_prediction_stats "$HOST_BACKEND_URL"', restore_body)
        self.assertNotIn('backend_prediction_stats "$BACKEND_URL"', restore_body)
        self.assertIn('"$actual_total" -ge "$expected_total"', restore_body)
        self.assertIn('"$actual_labeled" -ge "$expected_labeled"', restore_body)

    def test_partial_job_is_backed_up_and_restored_before_nonzero_exit(self):
        persist_index = self.run_script.rindex('\npersist_state_predictions\n')
        restore_index = self.run_script.rindex('\nrestore_render_from_backup\n')
        status_index = self.run_script.rindex('\nif [[ "$automation_status" -ne 0 ]]')
        self.assertLess(persist_index, restore_index)
        self.assertLess(restore_index, status_index)


if __name__ == "__main__":
    unittest.main()
