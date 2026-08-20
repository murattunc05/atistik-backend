import json
import hashlib
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import api_server as api


class MlRestoreAsyncTests(unittest.TestCase):
    def test_startup_restore_is_scheduled_once_from_worker_request(self):
        empty = {
            "exists": False,
            "bytes": 0,
            "bytes_read": 0,
            "lines": 0,
            "valid_json_lines": 0,
            "prediction_lines": 0,
            "labeled_lines": 0,
        }
        with patch.object(api, "_startup_restore_initialized", False), patch.object(
            api, "_GITHUB_TOKEN", "configured"
        ), patch.object(api, "_GITHUB_ML_REPO", "owner/repo"), patch.object(
            api, "_prediction_file_stats", return_value=empty
        ), patch.object(api, "schedule_github_restore") as schedule:
            api._ensure_startup_restore_scheduled()
            api._ensure_startup_restore_scheduled()

        schedule.assert_called_once_with(force=False)

    def test_default_restore_credential_is_only_a_sha256_digest(self):
        self.assertRegex(api._RESTORE_TOKEN_SHA256_DEFAULT, r"^[0-9a-f]{64}$")

    def test_async_route_returns_before_restore_work(self):
        job = {
            "status": "running",
            "started_at": "2026-08-20T18:00:00Z",
            "finished_at": None,
        }
        with patch.object(api, "schedule_github_restore", return_value=(True, job)), patch.object(
            api, "_restore_request_authorized", return_value=True
        ):
            response = api.app.test_client().post("/api/ml-restore?force=true&async=true")

        self.assertEqual(response.status_code, 202)
        payload = response.get_json()
        self.assertTrue(payload["accepted"])
        self.assertEqual(payload["job"]["status"], "running")

    def test_atomic_restore_rejects_invalid_json_without_touching_live_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "predictions.jsonl"
            target.write_text(json.dumps({"race_id": "safe"}) + "\n", encoding="utf-8")
            with patch.object(api, "_PREDICTIONS_PATH", str(target)):
                result = api._replace_predictions_from_text("not-json\n")

            self.assertIsNone(result)
            self.assertEqual(
                target.read_text(encoding="utf-8"),
                json.dumps({"race_id": "safe"}) + "\n",
            )

    def test_atomic_restore_rejects_non_prediction_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "predictions.jsonl"
            original = json.dumps(
                {"race_id": "safe", "horse_name": "SAFE", "rank_pred": 1}
            ) + "\n"
            target.write_text(original, encoding="utf-8")
            github_metadata = json.dumps(
                {"name": "predictions.jsonl", "sha": "abc", "size": 10}
            ) + "\n"
            with patch.object(api, "_PREDICTIONS_PATH", str(target)):
                result = api._replace_predictions_from_text(github_metadata)

            self.assertIsNone(result)
            self.assertEqual(target.read_text(encoding="utf-8"), original)

    def test_atomic_restore_rejects_bad_bytes_after_valid_prediction(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "predictions.jsonl"
            original = json.dumps(
                {"race_id": "safe", "horse_name": "SAFE", "rank_pred": 1}
            ) + "\n"
            target.write_text(original, encoding="utf-8")
            incoming = Path(tmp) / "incoming.tmp"
            incoming.write_bytes(
                json.dumps(
                    {"race_id": "new", "horse_name": "NEW", "rank_pred": 1}
                ).encode("utf-8")
                + b"\n\xff\xfe"
            )
            with patch.object(api, "_PREDICTIONS_PATH", str(target)):
                result = api._install_prediction_temp(str(incoming))

            self.assertIsNone(result)
            self.assertEqual(target.read_text(encoding="utf-8"), original)

    def test_atomic_restore_rejects_valid_but_truncated_byte_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "predictions.jsonl"
            original = json.dumps(
                {"race_id": "safe", "horse_name": "SAFE", "rank_pred": 1}
            ) + "\n"
            target.write_text(original, encoding="utf-8")
            incoming = Path(tmp) / "incoming.tmp"
            incoming.write_text(
                json.dumps(
                    {"race_id": "new", "horse_name": "NEW", "rank_pred": 1}
                ) + "\n",
                encoding="utf-8",
            )
            with patch.object(api, "_PREDICTIONS_PATH", str(target)):
                result = api._install_prediction_temp(
                    str(incoming), expected_bytes=incoming.stat().st_size + 100
                )

            self.assertIsNone(result)
            self.assertEqual(target.read_text(encoding="utf-8"), original)

    def test_atomic_restore_replaces_only_fully_valid_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "predictions.jsonl"
            target.write_text(json.dumps({"race_id": "old"}) + "\n", encoding="utf-8")
            restored = "".join(
                json.dumps(
                    {"race_id": race_id, "horse_name": race_id, "rank_pred": rank}
                ) + "\n"
                for rank, race_id in enumerate(("new-1", "new-2"), start=1)
            )
            with patch.object(api, "_PREDICTIONS_PATH", str(target)):
                result = api._replace_predictions_from_text(restored)

            self.assertEqual(result["valid_json_lines"], 2)
            self.assertEqual(target.read_text(encoding="utf-8"), restored)

    def test_atomic_restore_keeps_target_host_readable(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "predictions.jsonl"
            target.write_text(
                json.dumps(
                    {"race_id": "old", "horse_name": "OLD", "rank_pred": 1}
                ) + "\n",
                encoding="utf-8",
            )
            os.chmod(target, 0o644)
            restored = json.dumps(
                {"race_id": "new", "horse_name": "NEW", "rank_pred": 1}
            ) + "\n"
            with patch.object(api, "_PREDICTIONS_PATH", str(target)):
                result = api._replace_predictions_from_text(restored)

            self.assertIsNotNone(result)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o644)

    def test_atomic_restore_refuses_valid_but_older_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "predictions.jsonl"
            current = "".join(
                json.dumps(
                    {
                        "race_id": race_id,
                        "horse_name": race_id,
                        "rank_pred": rank,
                        "finish_pos": 1,
                    }
                ) + "\n"
                for rank, race_id in enumerate(("current-1", "current-2"), start=1)
            )
            target.write_text(current, encoding="utf-8")
            incoming = json.dumps(
                {
                    "race_id": "older",
                    "horse_name": "OLDER",
                    "rank_pred": 1,
                    "finish_pos": 1,
                }
            ) + "\n"
            with patch.object(api, "_PREDICTIONS_PATH", str(target)):
                result = api._replace_predictions_from_text(
                    incoming,
                    minimum_stats={"valid_json_lines": 2, "labeled_lines": 2},
                )

            self.assertIsNone(result)
            self.assertEqual(target.read_text(encoding="utf-8"), current)

    def test_failed_restore_job_is_reported_failed(self):
        with patch.object(api, "github_restore", return_value=False), patch.object(
            api, "_prediction_file_stats", return_value={"valid_json_lines": 2}
        ):
            api._run_github_restore_job(force=True)

        self.assertEqual(api._restore_job_snapshot()["status"], "failed")

    def test_prediction_writer_is_rejected_while_restore_runs(self):
        original = api._restore_job_snapshot()
        try:
            with api._gh_restore_job_lock:
                api._gh_restore_job["status"] = "running"
            response = api.app.test_client().post("/api/analyze-race", json={})
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.get_json()["code"], "prediction_restore_in_progress")
        finally:
            with api._gh_restore_job_lock:
                api._gh_restore_job.clear()
                api._gh_restore_job.update(original)

    def test_prediction_writer_stays_rejected_after_failed_empty_startup(self):
        original = api._restore_job_snapshot()
        try:
            with api._gh_restore_job_lock:
                api._gh_restore_job["status"] = "failed"
            empty = {
                "valid_json_lines": 0,
                "prediction_lines": 0,
                "lines": 0,
            }
            with patch.object(api, "_GITHUB_TOKEN", "configured"), patch.object(
                api, "_GITHUB_ML_REPO", "owner/private"
            ), patch.object(api, "_prediction_file_stats", return_value=empty):
                response = api.app.test_client().post("/api/analyze-race", json={})
            self.assertEqual(response.status_code, 503)
        finally:
            with api._gh_restore_job_lock:
                api._gh_restore_job.clear()
                api._gh_restore_job.update(original)

    def test_prediction_writer_rejects_mixed_valid_and_invalid_jsonl(self):
        mixed = {
            "bytes": 100,
            "bytes_read": 100,
            "lines": 2,
            "valid_json_lines": 1,
            "prediction_lines": 1,
        }
        with patch.object(api, "_GITHUB_TOKEN", "configured"), patch.object(
            api, "_GITHUB_ML_REPO", "owner/private"
        ), patch.object(api, "_prediction_file_stats", return_value=mixed):
            self.assertTrue(api._prediction_writes_blocked())

    def test_restore_route_requires_header_when_secret_configured(self):
        with patch.dict(
            api._os.environ,
            {"ATISTIK_RESTORE_TOKEN": "restore-secret"},
            clear=False,
        ):
            response = api.app.test_client().post(
                "/api/ml-restore?force=true&async=true"
            )
        self.assertEqual(response.status_code, 403)

    def test_restore_route_accepts_only_token_matching_configured_hash(self):
        secret = "dedicated-restore-secret"
        expected_hash = hashlib.sha256(secret.encode("utf-8")).hexdigest()
        job = {"status": "running", "restored": False}
        with patch.dict(
            api._os.environ,
            {
                "ATISTIK_RESTORE_TOKEN": "",
                "ATISTIK_RESTORE_TOKEN_SHA256": expected_hash,
            },
            clear=False,
        ), patch.object(api, "schedule_github_restore", return_value=(True, job)):
            accepted = api.app.test_client().post(
                "/api/ml-restore?force=true&async=true",
                headers={"X-Atistik-Restore-Token": secret},
            )
            rejected = api.app.test_client().post(
                "/api/ml-restore?force=true&async=true",
                headers={"X-Atistik-Restore-Token": "wrong"},
            )

        self.assertEqual(accepted.status_code, 202)
        self.assertEqual(rejected.status_code, 403)

    def test_large_restore_path_streams_bytes_without_response_text(self):
        class StreamingResponse:
            status_code = 200

            @property
            def text(self):
                raise AssertionError("streaming restore must not materialize response.text")

            def iter_content(self, chunk_size):
                self.chunk_size = chunk_size
                yield json.dumps(
                    {
                        "race_id": "one",
                        "horse_name": "ONE",
                        "rank_pred": 1,
                        "finish_pos": 1,
                    }
                ).encode("utf-8")
                yield b"\n"
                yield json.dumps(
                    {
                        "race_id": "two",
                        "horse_name": "TWO",
                        "rank_pred": 2,
                        "finish_pos": 2,
                    }
                ).encode("utf-8")
                yield b"\n"

            def close(self):
                self.closed = True

        response = StreamingResponse()
        original_job = api._restore_job_snapshot()
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "predictions.jsonl"
            try:
                with api._gh_restore_job_lock:
                    api._gh_restore_job.update({"status": "running", "restored": False})
                with patch.object(api, "_PREDICTIONS_PATH", str(target)), patch.object(
                    api.requests, "get", return_value=response
                ) as get:
                    result = api._stream_github_predictions(
                        {"download_url": None},
                        minimum_stats={"valid_json_lines": 0, "labeled_lines": 0},
                    )
            finally:
                completed_job = api._restore_job_snapshot()
                with api._gh_restore_job_lock:
                    api._gh_restore_job.clear()
                    api._gh_restore_job.update(original_job)

            self.assertEqual(result["valid_json_lines"], 2)
            self.assertEqual(result["labeled_lines"], 2)
            self.assertTrue(response.closed)
            self.assertTrue(get.call_args.kwargs["stream"])
            self.assertEqual(get.call_args.kwargs["timeout"], (10, 180))
            self.assertEqual(completed_job["status"], "completed")
            self.assertTrue(completed_job["restored"])
            self.assertEqual(completed_job["after"]["valid_json_lines"], 2)


if __name__ == "__main__":
    unittest.main()
