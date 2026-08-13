import copy
import json
import tempfile
import unittest
from pathlib import Path

from automation.late_market_agf_shadow import IntegrityError, canonical, sha256_payload
from automation.late_market_agf_state_sync import merge
from test_late_market_agf_shadow_monitor import race


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(canonical(row) + "\n" for row in rows), encoding="utf-8")


def rehash(item):
    payload = dict(item)
    payload.pop("snapshotSha256", None)
    item["snapshotSha256"] = sha256_payload(payload)


class LateMarketAgfStateSyncTests(unittest.TestCase):
    def test_idempotent_merge(self):
        snapshot, _ = race(0)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state, destination = root / "state.jsonl", root / "dest.jsonl"
            write_jsonl(state, [snapshot])
            first = merge(state, destination)
            second = merge(state, destination)
            self.assertEqual(first, {"state": 1, "before": 0, "after": 1})
            self.assertEqual(second, {"state": 1, "before": 1, "after": 1})
            self.assertEqual(len(destination.read_text().splitlines()), 1)

    def test_same_key_conflict_and_tampered_hash_fail(self):
        snapshot, _ = race(0)
        conflict = copy.deepcopy(snapshot)
        conflict["leadMinutes"] += 1
        rehash(conflict)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state, destination = root / "state.jsonl", root / "dest.jsonl"
            write_jsonl(state, [conflict])
            write_jsonl(destination, [snapshot])
            with self.assertRaisesRegex(IntegrityError, "immutable_snapshot_conflict"):
                merge(state, destination)

            tampered = copy.deepcopy(snapshot)
            tampered["leadMinutes"] += 1
            write_jsonl(state, [tampered])
            destination.unlink()
            with self.assertRaisesRegex(IntegrityError, "state_snapshot_invalid"):
                merge(state, destination)

    def test_any_visibility_flag_rejects_snapshot(self):
        snapshot, _ = race(0)
        for flag in ("usedForRanking", "rolloutEligible", "telegramVisible"):
            item = copy.deepcopy(snapshot)
            item[flag] = True
            rehash(item)
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                state, destination = root / "state.jsonl", root / "dest.jsonl"
                write_jsonl(state, [item])
                with self.assertRaisesRegex(IntegrityError, "state_snapshot_invalid"):
                    merge(state, destination)


if __name__ == "__main__":
    unittest.main()
