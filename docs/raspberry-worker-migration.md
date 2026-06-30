# Atistik Raspberry Worker Migration

Bu rehber Render uzerindeki agir analiz islerini Raspberry Pi worker'a tasimak icindir. Cutover tamamlanana kadar Render/GitHub Actions canli akis olarak kalir.

## Hedef Mimari

- Render public API olarak kalir.
- Raspberry `atistik-api` container'i lokal backend'i calistirir.
- Raspberry timer'lari `automation/atistik_daily_job.py` scriptini lokal backend'e karsi calistirir.
- ML-data repo `automation/runs/YYYY-MM-DD/*.json` raporlarinin ortak deposu olarak kalir.
- GitHub `Atistik Render Fallback` workflow'u Pi raporu yoksa Render backend ile manuel veya cutover sonrasi schedule'li yedek analiz calistirir.

## Raspberry Hazirligi

Pi uzerinde hedef dizin:

```bash
sudo mkdir -p /opt/atistik
sudo chown -R pi:pi /opt/atistik
cd /opt/atistik
git clone https://github.com/murattunc05/atistik-backend.git backend
cd backend
cp .env.raspberry.example .env.raspberry
```

`.env.raspberry` icinde gercek degerler:

```bash
GITHUB_TOKEN=<predictions.jsonl backup token>
GITHUB_ML_REPO=murattunc05/atistik-ml-data
ML_DATA_REPO=murattunc05/atistik-ml-data
ML_DATA_TOKEN=<ml-data read/write token>
TZ=Europe/Istanbul
```

Token'lar repo'ya commit edilmez.

## Ilk Smoke Test

```bash
cd /opt/atistik/backend
docker compose -f docker-compose.raspberry.yml up -d --build atistik-api
curl -fsS http://127.0.0.1:5000/health
curl -fsS http://127.0.0.1:5000/api/ml-status
curl -fsS http://127.0.0.1:5000/api/ml-backup-status
```

Beklenen:

- `model_loaded=true`
- `github_backup_configured=true`
- `predictions.exists=true`
- `predictions.valid_json_lines` Render'daki canli degerle ayni veya cok yakin

## Shadow Calisma

Canli GitHub Actions kapatilmadan Pi uzerinde manuel test:

```bash
/opt/atistik/backend/scripts/raspberry/run-automation.sh analyze-dry-run
/opt/atistik/backend/scripts/raspberry/run-automation.sh analyze 2026-06-30
```

Shadow testte Telegram'in ayni gun tekrar mesaj atmamasini `state.json` korur; yine de canli grup mesaji istenmiyorsa Telegram bot timer'i test boyunca kapali tutulur.

## systemd Timer Kurulumu

Smoke test basarili olduktan sonra:

```bash
sudo cp /opt/atistik/backend/systemd/atistik-raspberry-*.service /etc/systemd/system/
sudo cp /opt/atistik/backend/systemd/atistik-raspberry-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now atistik-raspberry-api.service
```

Cutover gunune kadar timer'lari acma:

```bash
sudo systemctl enable --now atistik-raspberry-analyze.timer
sudo systemctl enable --now atistik-raspberry-results.timer
```

Kontrol:

```bash
systemctl list-timers | grep atistik
journalctl -u atistik-raspberry-analyze.service -n 200 --no-pager
journalctl -u atistik-raspberry-results.service -n 200 --no-pager
```

## Cutover Kriterleri

Cutover'dan once hepsi dogrulanmali:

- Pi backend health ve ML status saglikli.
- En az 1 manuel `analyze` run'i ML-data'ya rapor yazdi.
- Rapor icinde `totals.analyzed > 0` ve `totals.failed == 0`.
- Aksam `results` run'i en az bir kez basariyla tamamlandi veya pending sonuclar beklenen sekilde raporlandi.
- Telegram sadece ilk basarili sabah analizini gonderdi.
- Render canli akis geri alinabilir durumda.

## Fallback

Cutover sonrasi GitHub Actions primary analiz schedule'i kapatilip `Atistik Render Fallback` workflow'una schedule eklenir:

- `04:45 UTC` = `07:45 Europe/Istanbul` analyze fallback
- `20:40 UTC` = `23:40 Europe/Istanbul` results fallback

Fallback checker once ML-data'da o gunun raporunu okur. Basarili Pi raporu varsa Render'a dokunmaz. Rapor yoksa veya basarisizsa Render backend ile yedek isi calistirir ve ML-data'ya commit eder.

## Rollback

```bash
sudo systemctl disable --now atistik-raspberry-analyze.timer
sudo systemctl disable --now atistik-raspberry-results.timer
```

Sonra GitHub Actions eski Render primary schedule'i tekrar acilir veya manuel `workflow_dispatch` ile Render analizi calistirilir.
