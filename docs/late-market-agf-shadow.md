# Late-market AGF prospective shadow

This phase is deliberately separate from the 06:37 analysis. It never calls
`/api/analyze-race`, never edits `predictions.jsonl`, never changes Telegram,
and never promotes itself to visible ranking.

Eligibility is fail-closed:

- profile must be MAIDEN or SART1;
- the immutable morning visible v4 score must not already contain sourced AGF;
- date, cityId, raceId, raceNo, race time, horse no, normalized horse name and
  TJK `QueryParameter_AtId` must all match;
- every runner must still be declared; a Koşmaz runner rejects the race;
- official TJK `agfPools` coverage must be at least 80% (100% preferred);
- the first official TJK pool is selected consistently, while all distinct
  pool values remain in the ledger;
- collection and baseline timestamps must be on the race day, and collection
  must be at least 90 minutes before the race;
- market alpha is frozen at 10% and missing AGF is neutral 50.

The timer probes at 09:55, 10:05, 10:15, 10:25, 10:35 and 10:45 Istanbul.
`Persistent=false` prevents a reboot from backfilling a late observation. The
first accepted race snapshot is immutable; later probes deduplicate it.

Install after the backend code is deployed:

```bash
sudo cp systemd/atistik-late-market-agf-shadow.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now atistik-late-market-agf-shadow.timer
systemctl list-timers atistik-late-market-agf-shadow.timer
```

Daytime evidence lives under
`/opt/atistik/backend/state/late-market-agf/probe-output/automation/late-market-agf/`.
The nightly results flow merges that state into the ML-data repo, validates it,
and writes +5/+10/+15 profile checkpoints. A late-shadow failure is warning-only
for the production result, backup and Render-restore path.
