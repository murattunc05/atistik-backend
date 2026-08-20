import base64
import json
import os
import re
import unicodedata
from datetime import UTC, date, datetime
from pathlib import Path

import requests


APP_DIR = Path(__file__).resolve().parent
LOCAL_ENV = APP_DIR / ".env"


def load_local_env(path=LOCAL_ENV):
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_chat_ids(value):
    return [chat_id.strip() for chat_id in (value or "").split(",") if chat_id.strip()]


load_local_env()

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
TELEGRAM_ANALYSIS_CHAT_IDS = parse_chat_ids(
    os.environ.get("TELEGRAM_ANALYSIS_CHAT_IDS")
    or os.environ.get("TELEGRAM_CHAT_IDS")
    or TELEGRAM_CHAT_ID
)
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
BACKEND_REPO = os.environ["BACKEND_REPO"]
ML_DATA_REPO = os.environ["ML_DATA_REPO"]
WORKFLOW_NAME = os.environ.get("WORKFLOW_NAME", "Atistik Daily Automation")
ANALYZE_CUTOFF_UTC_HOUR = int(os.environ.get("ANALYZE_CUTOFF_UTC_HOUR", "15"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "/opt/atistik-telegram-bot/data/state.json"))


def gh_get(url):
    response = requests.get(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def successful_analysis_runs():
    url = f"https://api.github.com/repos/{BACKEND_REPO}/actions/runs?per_page=20"
    runs = gh_get(url).get("workflow_runs", [])
    candidates = []
    for run in runs:
        if run.get("name") != WORKFLOW_NAME:
            continue
        if run.get("status") != "completed":
            continue
        if run.get("conclusion") != "success":
            continue
        created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        if created.hour >= ANALYZE_CUTOFF_UTC_HOUR:
            continue
        candidates.append(run)
    return list(reversed(candidates))


def run_date_from_created_at(run):
    created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
    return created.strftime("%Y-%m-%d")


def fetch_analysis(run_date):
    path = f"automation/runs/{run_date}/analysis.json"
    url = f"https://api.github.com/repos/{ML_DATA_REPO}/contents/{path}"
    data = gh_get(url)
    content = base64.b64decode(data["content"]).decode("utf-8")
    return json.loads(content)


def city_key(value):
    value = unicodedata.normalize("NFKC", str(value or ""))
    value = value.translate(
        str.maketrans(
            {
                "İ": "i", "I": "i", "ı": "i", "Ğ": "g", "ğ": "g",
                "Ü": "u", "ü": "u", "Ş": "s", "ş": "s", "Ö": "o",
                "ö": "o", "Ç": "c", "ç": "c",
            }
        )
    ).lower()
    return re.sub(r"[^a-z0-9]+", "", value)


def analysis_manifest_issues(analysis):
    issues = []
    if not isinstance(analysis, dict):
        return ["manifest_not_object"]
    if analysis.get("mode") != "analyze":
        issues.append("mode_not_analyze")
    if analysis.get("status") != "completed":
        issues.append("status_not_completed")

    totals = analysis.get("totals") or {}
    for field in ("failed", "failedCities", "unresolvedRaces", "unresolved"):
        try:
            if int(totals.get(field, 0) or 0) > 0:
                issues.append(f"nonzero_{field}")
        except (TypeError, ValueError):
            issues.append(f"invalid_total_{field}")

    cities = analysis.get("cities") or []
    if not cities:
        issues.append("cities_missing")
        return list(dict.fromkeys(issues))

    requested = [city_key(city) for city in (analysis.get("citiesRequested") or []) if city_key(city)]
    reported = [city_key(city.get("city")) for city in cities if city_key(city.get("city"))]
    if not requested:
        issues.append("cities_requested_missing")
    elif sorted(requested) != sorted(reported):
        issues.append("requested_city_set_mismatch")

    derived_races = 0
    derived_analyzed = 0
    for city in cities:
        city_status = str(city.get("status") or "").strip()
        races = city.get("races") or []
        if city_status not in {"ok", "no_races"}:
            issues.append("unresolved_city_or_race")
        if city_status == "ok" and not races:
            issues.append("ok_city_without_races")
        if city_status == "no_races" and races:
            issues.append("no_races_city_with_races")
        derived_races += len(races)
        for race in races:
            if race.get("status") != "analyzed":
                issues.append("unresolved_city_or_race")
            else:
                derived_analyzed += 1

    for field, derived in (
        ("cities", len(cities)),
        ("racesFound", derived_races),
        ("analyzed", derived_analyzed),
    ):
        if field not in totals:
            continue
        try:
            recorded = int(totals.get(field, 0) or 0)
        except (TypeError, ValueError):
            issues.append(f"invalid_total_{field}")
            continue
        if recorded != derived:
            issues.append(f"total_mismatch_{field}")

    return list(dict.fromkeys(issues))


def analysis_manifest_complete(analysis):
    return not analysis_manifest_issues(analysis)


def reject_incomplete_analysis(run_date, analysis):
    issues = analysis_manifest_issues(analysis)
    if not issues:
        return False
    print(
        f"{run_date} analysis.json eksik/guvensiz; Telegram gonderilmedi: "
        + ",".join(issues)
    )
    return True


def score_text(value):
    try:
        return f"{float(value):.1f}"
    except (TypeError, ValueError):
        return "-"


def rank_key(item):
    try:
        return int(item.get("v4Rank", 9999))
    except (TypeError, ValueError):
        return 9999


def format_analysis_message(run_date, analysis, run_url):
    totals = analysis.get("totals", {})
    lines = [
        "Atistik gunluk analiz tamamlandi",
        f"Tarih: {run_date}",
        f"Analiz edilen kosu: {totals.get('analyzed', 0)} / {totals.get('racesFound', 0)}",
        f"Hata: {totals.get('failed', 0)}",
        "",
    ]

    for city in analysis.get("cities", []) or []:
        city_name = city.get("city", "")
        for race in city.get("races", []) or []:
            rankings = race.get("rankings", []) or []
            if not rankings:
                continue

            lines.append(f"{city_name} {race.get('raceNo', '')}. Kosu")
            race_type = race.get("raceType") or ""
            horse_count = race.get("horseCount") or len(rankings)
            if race_type or horse_count:
                lines.append(f"{race_type} | {horse_count} at")

            for item in sorted(rankings, key=rank_key):
                rank = item.get("v4Rank", "")
                horse = str(item.get("horse") or "").strip()
                horse_no = str(item.get("no") or "").strip()
                has_number_suffix = bool(re.search(r"\(\d+\)$", horse))
                display_name = horse if has_number_suffix or not horse_no else f"{horse} ({horse_no})"
                score = score_text(item.get("v4Score"))
                lines.append(f"{rank}. {display_name} - v4 puan: {score}")
            lines.append("")

    return "\n".join(lines).strip()


def format_analysis_message_with_confidence(run_date, analysis, run_url):
    try:
        from telegram_bot.message_formatter import format_analysis_message as formatter
    except ImportError:
        try:
            from message_formatter import format_analysis_message as formatter
        except ImportError:
            return format_analysis_message(run_date, analysis, run_url)
    return formatter(run_date, analysis, run_url)


def update_env_chat_id(old_chat_id, new_chat_id):
    env_path = LOCAL_ENV
    if not env_path.exists():
        return

    changed = False
    lines = []
    target_keys = {"TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_IDS", "TELEGRAM_ANALYSIS_CHAT_IDS"}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        if "=" not in raw_line or raw_line.lstrip().startswith("#"):
            lines.append(raw_line)
            continue

        key, value = raw_line.split("=", 1)
        key_name = key.strip()
        if key_name not in target_keys:
            lines.append(raw_line)
            continue

        quote = value[:1] if value[:1] in {"'", '"'} and value[-1:] == value[:1] else ""
        unquoted_value = value[1:-1] if quote else value
        chat_ids = parse_chat_ids(unquoted_value)
        replaced = [new_chat_id if chat_id == old_chat_id else chat_id for chat_id in chat_ids]
        if replaced != chat_ids:
            changed = True
            new_value = ",".join(replaced)
            if quote:
                new_value = f"{quote}{new_value}{quote}"
            lines.append(f"{key}={new_value}")
        else:
            lines.append(raw_line)

    if changed:
        env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def send_telegram(text, chat_ids=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_len = 3500
    chunks = [text[i : i + max_len] for i in range(0, len(text), max_len)]
    targets = chat_ids or TELEGRAM_ANALYSIS_CHAT_IDS
    if not targets:
        raise RuntimeError("No Telegram chat id configured")

    for configured_chat_id in targets:
        chat_id = configured_chat_id
        for chunk in chunks:
            for attempt in range(2):
                response = requests.post(
                    url,
                    data={
                        "chat_id": chat_id,
                        "text": chunk,
                        "disable_web_page_preview": "true",
                    },
                    timeout=30,
                )
                if response.ok:
                    break
                try:
                    payload = response.json()
                except ValueError:
                    payload = {}
                migrated_chat_id = payload.get("parameters", {}).get("migrate_to_chat_id")
                if migrated_chat_id and attempt == 0:
                    new_chat_id = str(migrated_chat_id)
                    update_env_chat_id(chat_id, new_chat_id)
                    chat_id = new_chat_id
                    continue
                description = payload.get("description", response.text[:300])
                raise RuntimeError(
                    f"Telegram send failed for chat {configured_chat_id}: "
                    f"HTTP {response.status_code} - {description}"
                )


def main():
    state = load_state()
    notified_dates = set(state.get("notified_analysis_dates", []))

    target_date = os.environ.get("TARGET_DATE") or date.today().isoformat()
    if target_date in notified_dates:
        print(f"{target_date} icin gonderilecek yeni analiz yok.")
        return

    try:
        analysis = fetch_analysis(target_date)
        if reject_incomplete_analysis(target_date, analysis):
            return
        message = format_analysis_message_with_confidence(target_date, analysis, None)
        run_id = f"ml-data:{target_date}"
        run_date = target_date
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 404:
            raise

        run = None
        run_date = None
        for candidate in successful_analysis_runs():
            candidate_date = run_date_from_created_at(candidate)
            if candidate_date != target_date:
                continue
            run = candidate
            run_date = candidate_date
            break

        if not run or not run_date:
            print(f"{target_date} icin gonderilecek yeni analiz yok.")
            return

        run_id = str(run["id"])
        try:
            analysis = fetch_analysis(run_date)
            if reject_incomplete_analysis(run_date, analysis):
                return
            message = format_analysis_message_with_confidence(run_date, analysis, run.get("html_url"))
        except requests.HTTPError:
            print(f"{run_date} analysis.json henuz ML data repo'da bulunamadi; Telegram gonderilmedi.")
            return

    send_telegram(message)

    state["last_notified_run_id"] = run_id
    notified_dates.add(run_date)
    state["notified_analysis_dates"] = sorted(notified_dates)[-30:]
    state["last_notified_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    save_state(state)
    print(f"{run_date} analizi Telegram'a gonderildi.")


if __name__ == "__main__":
    main()
