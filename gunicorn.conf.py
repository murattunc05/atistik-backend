"""Gunicorn runtime invariants for the stateful predictions service."""

# The JSONL mutation locks and asynchronous restore status are process-local.
# A single worker prevents independent startup restores and read/modify/write
# paths from racing on the same predictions file. Scale with a shared store and
# distributed lock before increasing this value.
workers = 1
timeout = 240
