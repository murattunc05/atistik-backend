from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, urljoin, urlparse
import prediction_logic
import concurrent.futures
import pandas as pd
import numpy as np
import time
import re
import urllib3
import unicodedata
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Flutter'dan gelen isteklere izin ver

# ══════════════════════════════════════════════════════════════════
# FAZ 8: ML MODEL YÜKLEME (XGBoost Blend)
# ══════════════════════════════════════════════════════════════════
_ml_model = None  # Legacy ML blend is intentionally disabled.
_ml_feature_cols = []
_ml_feature_stats = {}
_ml_load_error = None  # Legacy diagnostic field kept for compatibility.
_ml_shadow_model = None
_ml_shadow_feature_cols = []
_ml_shadow_feature_stats = {}
_ml_shadow_metadata = {}
_ml_shadow_load_error = None
_ML_SHADOW_MODE = "shadow_only"

def load_ml_model():
    """Load the optional shadow ranker without affecting visible v4 ranking."""
    global _ml_shadow_model, _ml_shadow_feature_cols, _ml_shadow_feature_stats
    global _ml_shadow_metadata, _ml_shadow_load_error
    import os as _o, json as _j
    model_path = _o.path.join(_o.path.dirname(__file__), 'model_shadow_ranker.json')
    stats_path = _o.path.join(_o.path.dirname(__file__), 'feature_stats_shadow.json')

    if not _o.path.exists(model_path):
        _ml_shadow_load_error = f"model_shadow_ranker.json bulunamadi: {model_path}"
        print(f"[SHADOW-ML] {_ml_shadow_load_error}")
        return

    try:
        import xgboost as xgb
        _ml_shadow_model = xgb.XGBRanker()
        _ml_shadow_model.load_model(model_path)
        _ml_shadow_load_error = None
        print(f"[SHADOW-ML] XGBoost ranker yuklendi: {model_path}")

        if _o.path.exists(stats_path):
            with open(stats_path, 'r', encoding='utf-8') as f:
                saved = _j.load(f)
            _ml_shadow_feature_cols = saved.get('feature_cols', [])
            _ml_shadow_feature_stats = saved.get('stats', {})
            _ml_shadow_metadata = saved.get('metadata', {})
            print(f"[SHADOW-ML] {len(_ml_shadow_feature_cols)} feature tanimi yuklendi")
        else:
            _ml_shadow_feature_cols = []
            _ml_shadow_feature_stats = {}
            _ml_shadow_metadata = {}
    except ImportError as ie:
        _ml_shadow_load_error = f"ImportError: {ie}"
        print(f"[SHADOW-ML] xgboost import hatasi: {ie}")
        _ml_shadow_model = None
    except Exception as e:
        _ml_shadow_load_error = f"Exception: {e}"
        print(f"[SHADOW-ML] Model yukleme hatasi: {e}")
        _ml_shadow_model = None

# Sunucu başlangıcında yükle
load_ml_model()

@app.route('/api/ml-status', methods=['GET'])
def ml_status():
    """ML model yükleme durumunu döner (teşhis endpoint'i)."""
    prediction_stats = _prediction_file_stats() if '_prediction_file_stats' in globals() else {}
    return jsonify({
        'model_loaded': _ml_shadow_model is not None,
        'feature_count': len(_ml_shadow_feature_cols),
        'load_error': _ml_shadow_load_error,
        'mode': _ML_SHADOW_MODE if _ml_shadow_model else 'unavailable',
        'model_version': _ml_shadow_metadata.get('model_version'),
        'ranking_version': f'v{globals().get("_V4_VERSION", "unknown")}',
        'metadata': _ml_shadow_metadata,
        'predictions': prediction_stats,
        'github_backup_configured': bool(globals().get('_GITHUB_TOKEN') and globals().get('_GITHUB_ML_REPO')),
    })


# ══════════════════════════════════════════════════════════════════
# FAZ 8.1: OTOMATİK SONUÇ ETİKETLEME (/api/auto-label)
# ══════════════════════════════════════════════════════════════════

@app.route('/api/auto-label', methods=['GET'])
def auto_label():
    """
    Geçmiş tarihli, etiketlenmemiş tahminler için TJK'dan
    sonuçları otomatik çekip predictions.jsonl'ı günceller.

    Güvenlik: ?secret=XXX parametresi zorunlu.
    AUTO_LABEL_SECRET ortam değişkeniyle belirlenir.
    Boş bırakılırsa token kontrolü atlanır (geliştirme modu).

    GET /api/auto-label?secret=benim_secret_kodum
    """
    import json as _j, os as _o
    from datetime import datetime, timedelta

    # ── Güvenlik kontrolü ─────────────────────────────────────────
    expected_secret = _o.environ.get('AUTO_LABEL_SECRET', '')
    if expected_secret:
        provided = request.args.get('secret', '')
        if provided != expected_secret:
            return jsonify({'success': False, 'error': 'Yetkisiz erişim'}), 403

    log_path = _o.path.join(_o.path.dirname(__file__), 'predictions.jsonl')
    if not _o.path.exists(log_path):
        return jsonify({'success': False, 'error': 'predictions.jsonl bulunamadı'}), 404

    today_str = datetime.now().strftime('%d.%m.%Y')

    # ── 1. Etiketlenmemiş + tarihi geçmiş koşuları topla ──────────
    # race_id → {race_date, race_no, horses: [{name, detail_link?}]}
    race_groups = {}
    all_entries = []

    with open(log_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = _j.loads(line)
                all_entries.append(entry)

                if entry.get('finish_pos') is not None:
                    continue  # Zaten etiketli, atla

                race_date = entry.get('race_date', '')
                if not race_date:
                    continue  # Tarihi olmayan eski kayıt, atla

                if race_date == today_str:
                    continue  # Bugünün koşusu, henüz bitmemiş olabilir

                race_id  = str(entry.get('race_id', ''))
                race_no  = str(entry.get('race_no', ''))
                h_name   = entry.get('horse_name', '')

                if race_id not in race_groups:
                    race_groups[race_id] = {
                        'race_date': race_date,
                        'race_no':   race_no,
                        'horses':    []
                    }
                race_groups[race_id]['horses'].append({'name': h_name})
            except Exception:
                all_entries.append(line)
                continue

    if not race_groups:
        return jsonify({
            'success': True,
            'message': 'Etiketlenecek geçmiş koşu bulunamadı.',
            'labeled_races': 0,
            'labeled_horses': 0,
        })

    print(f"[AUTO-LABEL] {len(race_groups)} koşu işlenecek")

    total_labeled = 0
    race_results_summary = []
    errors = []

    # ── 2. Her koşu için TJK'dan sonuç çek ───────────────────────
    for race_id, info in list(race_groups.items())[:20]:  # günde max 20 koşu
        race_date = info['race_date']
        race_no   = info['race_no']
        horses    = info['horses']

        print(f"[AUTO-LABEL] Koşu {race_id} ({race_date} / {race_no}), {len(horses)} at")

        # At geçmiş sayfalarından sonuç çek (mevcut fetch-race-results mantığı)
        horse_positions = {}
        race_errors = []

        for horse in horses:
            h_name = horse['name']
            if not h_name:
                continue

            # TJK at arama → detay link bul → geçmişten tarihe göre sıralama al
            try:
                # At arama
                search_url = f"{TARGET_URL}/TR/YarisSever/Query/AtBilgileri/AtArama"
                search_resp = requests.post(
                    search_url,
                    data={'AtAdi': h_name},
                    headers=HEADERS,
                    timeout=10
                )
                if search_resp.status_code != 200:
                    race_errors.append(f'{h_name}: arama HTTP {search_resp.status_code}')
                    continue

                soup = BeautifulSoup(search_resp.text, 'html.parser')

                # İlk eşleşen at linkini bul
                detail_link = None
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    if 'AtBilgileri' in href and 'AtId' in href:
                        detail_link = href
                        break

                if not detail_link:
                    # Alternatif: table içindeki link
                    for row in soup.find_all('tr'):
                        cells = row.find_all('td')
                        for cell in cells:
                            for a in cell.find_all('a', href=True):
                                if 'AtBilgileri' in a['href']:
                                    detail_link = a['href']
                                    break
                            if detail_link:
                                break
                        if detail_link:
                            break

                if not detail_link:
                    race_errors.append(f'{h_name}: detay link bulunamadı')
                    continue

                # Detay sayfasından tarihe göre bitiş pozisyonu al
                detail_url = urljoin(TARGET_URL, detail_link).replace('&amp;', '&')
                det_resp = requests.get(detail_url, headers=HEADERS, timeout=12, verify=False)
                if det_resp.status_code != 200:
                    race_errors.append(f'{h_name}: detay HTTP {det_resp.status_code}')
                    continue

                det_soup  = BeautifulSoup(det_resp.text, 'html.parser')
                data_div  = det_soup.find('div', id='dataDiv')
                if not data_div:
                    race_errors.append(f'{h_name}: dataDiv yok')
                    continue

                race_table = data_div.find('table', id='queryTable')
                tbody      = race_table.find('tbody', id='tbody0') if race_table else None
                if not tbody:
                    race_errors.append(f'{h_name}: tablo yok')
                    continue

                found_pos = None
                race_date_norm = race_date.replace('/', '.').replace('-', '.')[:10]
                for row in tbody.find_all('tr'):
                    if 'hidable' in row.get('class', []):
                        continue
                    cells = row.find_all('td')
                    if len(cells) < 6:
                        continue
                    row_date = cells[0].text.strip().replace('/', '.').replace('-', '.')[:10]
                    position = cells[4].text.strip()
                    if row_date == race_date_norm:
                        if position.isdigit():
                            found_pos = int(position)
                        else:
                            found_pos = 99  # K/D/F vb.
                        break

                if found_pos is not None:
                    horse_positions[h_name.strip().upper()] = found_pos
                else:
                    race_errors.append(f'{h_name}: {race_date} tarihli kayıt bulunamadı')

            except Exception as ex:
                race_errors.append(f'{h_name}: {ex}')
                continue

        if not horse_positions:
            errors.append(f'Koşu {race_id}: hiçbir at eşleştirilemedi. {race_errors[:3]}')
            continue

        # ── 3. predictions.jsonl'ı güncelle ──────────────────────
        race_labeled = 0
        new_all = []
        for entry in all_entries:
            if isinstance(entry, dict):
                if (str(entry.get('race_id', '')) == race_id and
                        entry.get('finish_pos') is None):
                    n_key = entry.get('horse_name', '').strip().upper()
                    if n_key in horse_positions:
                        pos = horse_positions[n_key]
                        entry['finish_pos'] = pos
                        entry['is_winner']  = 1 if pos == 1 else 0
                        race_labeled += 1
                new_all.append(_j.dumps(entry, ensure_ascii=False))
            else:
                new_all.append(entry)
        all_entries_ref = [
            _j.loads(l) if isinstance(l, str) else l
            for l in new_all
        ]
        all_entries = all_entries_ref  # noqa: F841 — bir sonraki iterasyon için

        # Dosyayı güncelle
        with open(log_path, 'w', encoding='utf-8') as f:
            for line in new_all:
                f.write(line + '\n')

        total_labeled += race_labeled
        race_results_summary.append({
            'race_id':   race_id,
            'race_date': race_date,
            'labeled':   race_labeled,
            'total':     len(horses),
            'errors':    len(race_errors),
        })
        print(f"[AUTO-LABEL] Koşu {race_id}: {race_labeled}/{len(horses)} at etiketlendi")

    if total_labeled > 0:
        github_backup()  # GitHub'a yedekle
        print(f"[AUTO-LABEL] Toplam {total_labeled} at etiketlendi, GitHub'a yedeklendi")

    return jsonify({
        'success':       True,
        'labeled_races': len(race_results_summary),
        'labeled_horses': total_labeled,
        'races':         race_results_summary,
        'errors':        errors[:10],
        'message':       f'{len(race_results_summary)} koşuda {total_labeled} at otomatik etiketlendi.',
    })


# ══════════════════════════════════════════════════════════════════
# FAZ 7.2: GITHUB BACKUP / RESTORE (predictions.jsonl kalıcılığı)
# ══════════════════════════════════════════════════════════════════
import os as _os
import json as _json
import base64 as _b64
import threading as _threading

_GITHUB_TOKEN    = _os.environ.get('GITHUB_TOKEN', '')
_GITHUB_ML_REPO  = _os.environ.get('GITHUB_ML_REPO', '')   # "kullanici/repo-adi"
_GITHUB_FILE     = 'predictions.jsonl'
_GITHUB_API_BASE = 'https://api.github.com'
_PREDICTIONS_PATH = _os.path.join(_os.path.dirname(__file__), 'predictions.jsonl')

# Thread-safe kilit — eşzamanlı backup/restore çakışmasını önler
_gh_lock = _threading.Lock()
# Son backup SHA'sı — güncelleme için gerekli
_gh_file_sha = None
_gh_last_read_method = None


def _prediction_file_stats(path=None):
    """Return lightweight predictions.jsonl diagnostics without exposing data."""
    target = path or _PREDICTIONS_PATH
    stats = {
        'exists': _os.path.exists(target),
        'bytes': 0,
        'lines': 0,
        'valid_json_lines': 0,
        'labeled_lines': 0,
    }
    if not stats['exists']:
        return stats

    stats['bytes'] = _os.path.getsize(target)
    try:
        with open(target, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                stats['lines'] += 1
                try:
                    entry = _json.loads(line)
                    stats['valid_json_lines'] += 1
                    if isinstance(entry, dict) and entry.get('finish_pos') is not None:
                        stats['labeled_lines'] += 1
                except Exception:
                    pass
    except Exception as exc:
        stats['error'] = str(exc)
    return stats


def _gh_headers():
    return {
        'Authorization': f'token {_GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'Atistik-ML-Backup'
    }


def _github_file_text(data):
    """Read GitHub contents response, including files where API omits content."""
    global _gh_last_read_method
    _gh_last_read_method = None

    # For files over 1 MB GitHub's contents API can omit the content field.
    # Ask the same endpoint for raw bytes first; this is the most reliable
    # restore path for private backup repos when a token is configured.
    try:
        raw_url = f'{_GITHUB_API_BASE}/repos/{_GITHUB_ML_REPO}/contents/{_GITHUB_FILE}'
        raw_headers = _gh_headers()
        raw_headers['Accept'] = 'application/vnd.github.raw+json'
        r = requests.get(raw_url, headers=raw_headers, timeout=30)
        if r.status_code == 200 and r.text.strip():
            _gh_last_read_method = 'contents_raw'
            return r.text
        print(f"[GH-BACKUP] raw contents okunamadı: HTTP {r.status_code}")
    except Exception as exc:
        print(f"[GH-BACKUP] raw contents exception: {exc}")

    raw_content = (data.get('content') or '').strip()
    if raw_content:
        _gh_last_read_method = 'contents_base64'
        return _b64.b64decode(raw_content).decode('utf-8')

    download_url = data.get('download_url')
    if download_url:
        r = requests.get(download_url, headers=_gh_headers(), timeout=30)
        if r.status_code == 200:
            _gh_last_read_method = 'download_url'
            return r.text
        print(f"[GH-BACKUP] download_url okunamadı: HTTP {r.status_code}")

    git_url = data.get('git_url') or f"{_GITHUB_API_BASE}/repos/{_GITHUB_ML_REPO}/git/blobs/{data.get('sha', '')}"
    if git_url:
        r = requests.get(git_url, headers=_gh_headers(), timeout=30)
        if r.status_code == 200:
            blob = r.json()
            blob_content = (blob.get('content') or '').strip()
            if blob_content:
                _gh_last_read_method = 'git_blob'
                return _b64.b64decode(blob_content).decode('utf-8')
        print(f"[GH-BACKUP] git blob okunamadı: HTTP {r.status_code}")

    _gh_last_read_method = 'empty'
    return ''


def github_restore(force=False):
    """
    Sunucu başlangıcında predictions.jsonl'ı GitHub'dan indirir.
    Dosya zaten varsa (ve boş değilse) dokunmaz.
    """
    global _gh_file_sha
    if not _GITHUB_TOKEN or not _GITHUB_ML_REPO:
        print("[GH-BACKUP] GITHUB_TOKEN veya GITHUB_ML_REPO tanımlı değil, restore atlanıyor.")
        return False

    local_stats = _prediction_file_stats()
    # Eğer dosya zaten gerçek JSON kayıtları içeriyorsa restore etme.
    if local_stats['valid_json_lines'] > 0 and not force:
        print(f"[GH-BACKUP] predictions.jsonl zaten mevcut ({local_stats['valid_json_lines']} kayıt), restore atlanıyor.")
        # Yine de SHA'yı al (sonraki update için gerekli)
        try:
            url = f'{_GITHUB_API_BASE}/repos/{_GITHUB_ML_REPO}/contents/{_GITHUB_FILE}'
            r = requests.get(url, headers=_gh_headers(), timeout=10)
            if r.status_code == 200:
                _gh_file_sha = r.json().get('sha')
        except Exception:
            pass
        return False

    try:
        url = f'{_GITHUB_API_BASE}/repos/{_GITHUB_ML_REPO}/contents/{_GITHUB_FILE}'
        r = requests.get(url, headers=_gh_headers(), timeout=15)

        if r.status_code == 200:
            data = r.json()
            content = _github_file_text(data)
            _gh_file_sha = data.get('sha')

            with open(_PREDICTIONS_PATH, 'w', encoding='utf-8') as f:
                f.write(content)

            restored_stats = _prediction_file_stats()
            print(f"[GH-BACKUP] ✅ Restore başarılı: {restored_stats['valid_json_lines']} kayıt GitHub'dan indirildi (method={_gh_last_read_method}).")
            return True
        elif r.status_code == 404:
            print("[GH-BACKUP] GitHub'da predictions.jsonl bulunamadı (ilk çalıştırma).")
        else:
            print(f"[GH-BACKUP] ⚠️ Restore hatası: HTTP {r.status_code}")

    except Exception as e:
        print(f"[GH-BACKUP] Restore exception: {e}")
    return False


def github_backup(force=False):
    """
    predictions.jsonl'ı GitHub'a yükler/günceller.
    Arka planda (thread) çalışır — ana isteği bloklamaz.
    """
    global _gh_file_sha
    if not _GITHUB_TOKEN or not _GITHUB_ML_REPO:
        return

    def _do_backup():
        global _gh_file_sha
        with _gh_lock:
            try:
                if not _os.path.exists(_PREDICTIONS_PATH):
                    return

                with open(_PREDICTIONS_PATH, 'r', encoding='utf-8') as f:
                    content = f.read()

                local_stats = _prediction_file_stats()
                if not force and local_stats['valid_json_lines'] == 0:
                    print("[GH-BACKUP] Boş predictions.jsonl yedeklenmedi.")
                    return

                encoded = _b64.b64encode(content.encode('utf-8')).decode('utf-8')
                url = f'{_GITHUB_API_BASE}/repos/{_GITHUB_ML_REPO}/contents/{_GITHUB_FILE}'

                payload = {
                    'message': f'ML data backup ({time.strftime("%d.%m.%Y %H:%M")})',
                    'content': encoded,
                }

                # Güncelleme yapabilmek için mevcut SHA gerekli
                if _gh_file_sha:
                    payload['sha'] = _gh_file_sha
                else:
                    # SHA'yı al
                    try:
                        r = requests.get(url, headers=_gh_headers(), timeout=10)
                        if r.status_code == 200:
                            _gh_file_sha = r.json().get('sha')
                            payload['sha'] = _gh_file_sha
                    except Exception:
                        pass

                r = requests.put(url, headers=_gh_headers(), json=payload, timeout=15)

                if r.status_code in (200, 201):
                    _gh_file_sha = r.json().get('content', {}).get('sha')
                    print(f"[GH-BACKUP] ✅ Backup başarılı: {local_stats['valid_json_lines']} kayıt GitHub'a yüklendi.")
                else:
                    print(f"[GH-BACKUP] ⚠️ Backup hatası: HTTP {r.status_code} — {r.text[:200]}")

            except Exception as e:
                print(f"[GH-BACKUP] Backup exception: {e}")

    _threading.Thread(target=_do_backup, daemon=True).start()


# Sunucu başlarken otomatik restore
github_restore()


@app.route('/api/ml-restore', methods=['POST'])
def ml_restore():
    """GitHub yedeğinden predictions.jsonl dosyasını manuel geri yükler."""
    try:
        payload = request.get_json(silent=True) or {}
        force = str(request.args.get('force', payload.get('force', ''))).lower() in {'1', 'true', 'yes'}
        before = _prediction_file_stats()
        restored = github_restore(force=force)
        after = _prediction_file_stats()
        return jsonify({
            'success': True,
            'restored': bool(restored),
            'force': force,
            'github_backup_configured': bool(_GITHUB_TOKEN and _GITHUB_ML_REPO),
            'before': before,
            'after': after,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ml-backup-status', methods=['GET'])
def ml_backup_status():
    """GitHub backup hedefini ve remote predictions.jsonl durumunu döner."""
    result = {
        'success': True,
        'github_backup_configured': bool(_GITHUB_TOKEN and _GITHUB_ML_REPO),
        'repo': _GITHUB_ML_REPO or '',
        'file': _GITHUB_FILE,
        'local': _prediction_file_stats(),
        'remote': None,
    }
    if not _GITHUB_TOKEN or not _GITHUB_ML_REPO:
        return jsonify(result)

    try:
        url = f'{_GITHUB_API_BASE}/repos/{_GITHUB_ML_REPO}/contents/{_GITHUB_FILE}'
        r = requests.get(url, headers=_gh_headers(), timeout=15)
        remote = {
            'http_status': r.status_code,
            'exists': r.status_code == 200,
        }
        if r.status_code == 200:
            data = r.json()
            content = _github_file_text(data)
            remote.update({
                'size': data.get('size', 0),
                'sha': data.get('sha', ''),
                'read_method': _gh_last_read_method,
                'line_count': len([line for line in content.splitlines() if line.strip()]),
                'valid_json_lines': 0,
            })
            for line in content.splitlines():
                if not line.strip():
                    continue
                try:
                    _json.loads(line)
                    remote['valid_json_lines'] += 1
                except Exception:
                    pass
        else:
            remote['error'] = r.text[:200]
        result['remote'] = remote
    except Exception as e:
        result['remote'] = {'error': str(e)}
    return jsonify(result)

# TJK ayarları
TARGET_URL = "https://www.tjk.org/TR/YarisSever/Query/Data/Atlar"
REFERER_URL = "https://www.tjk.org/TR/YarisSever/Query/Page/Atlar?QueryParameter_OLDUFLG=on"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": REFERER_URL
}

def map_breed_to_id(breed):
    """Irk adını TJK ID'sine çevirir"""
    breed_map = {
        'Tümü': '-1',
        'İngiliz': '1',
        'Arap': '2'
    }
    return breed_map.get(breed, '-1')

def map_gender_to_id(gender):
    """Cinsiyet adını TJK ID'sine çevirir"""
    gender_map = {
        'Tümü': '-1',
        'Erkek': '1',
        'Dişi': '2',
        'İğdiş': '3'
    }
    return gender_map.get(gender, '-1')

def map_country_to_id(country):
    """Ülke adını TJK ID'sine çevirir"""
    country_map = {
        'Tümü': '-1',
        'Türkiye': '1',
        'İngiltere': '2',
        'Fransa': '3',
        'ABD': '4',
        'İrlanda': '5'
    }
    return country_map.get(country, '-1')

@app.route('/api/search-horses', methods=['POST'])
def search_horses():
    """At arama endpoint'i"""
    try:
        data = request.json
        
        # Form payload'ını hazırla
        payload = {
            "QueryParameter_AtIsmi": data.get('horseName', ''),
            "QueryParameter_IrkId": map_breed_to_id(data.get('breed', 'Tümü')),
            "QueryParameter_CinsiyetId": map_gender_to_id(data.get('gender', 'Tümü')),
            "QueryParameter_Yas": data.get('age', ''),
            "QueryParameter_BabaId": data.get('fatherName', ''),
            "QueryParameter_AnneId": data.get('motherName', ''),
            "QueryParameter_UzerineKosanSahipId": data.get('ownerName', ''),
            "QueryParameter_YetistiricAdi": data.get('breederName', ''),
            "QueryParameter_AntronorId": data.get('trainerName', ''),
            "QueryParameter_UlkeId": map_country_to_id(data.get('country', 'Tümü')),
            "QueryParameter_OLDUFLG": "on" if data.get('includeDeadHorses', False) else "",
            "Era": "past",
            "Sort": "AtIsmi",
            "OldQueryParameter_OLDUFLG": "on" if data.get('includeDeadHorses', False) else ""
        }
        
        # TJK'ya istek gönder
        response = requests.get(
            TARGET_URL,
            params=payload,
            headers=HEADERS,
            timeout=10
        )
        
        if response.status_code != 200:
            return jsonify({
                'success': False,
                'error': f'TJK sunucusundan cevap alınamadı. Status: {response.status_code}'
            }), 500
        
        # HTML'i parse et
        soup = BeautifulSoup(response.text, 'html.parser')
        stats_table = soup.find('table', id='queryTable')
        
        if not stats_table:
            return jsonify({
                'success': True,
                'horses': [],
                'message': 'Sonuç bulunamadı'
            })
        
        table_body = stats_table.find('tbody', id='tbody0')
        if not table_body:
            return jsonify({
                'success': True,
                'horses': [],
                'message': 'Sonuç bulunamadı'
            })
        
        rows = table_body.find_all('tr')
        horses = []
        
        for row in rows:
            if 'hidable' in row.get('class', []):
                continue
            
            try:
                at_ismi_cell = row.find('td', class_='sorgu-Atlar-AtIsmi')
                irk_cell = row.find('td', class_='sorgu-Atlar-IrkAdi')
                cinsiyet_cell = row.find('td', class_='sorgu-Atlar-Cinsiyet')
                yas_cell = row.find('td', class_='sorgu-Atlar-Yas')
                orijin_cell = row.find('td', class_='sorgu-Atlar-BabaAdi')
                sahip_cell = row.find('td', class_='sorgu-Atlar-UzerineKosanSahip')
                antrenor_cell = row.find('td', class_='sorgu-Atlar-Antronoru')
                son_kosu_cell = row.find('td', class_='sorgu-Atlar-SonKosu')
                ikramiye_cell = row.find('td', class_='sorgu-Atlar-SadeAtKazanc')
                
                if not at_ismi_cell or not irk_cell:
                    continue
                
                # Orijin (Baba/Anne) bilgisini parse et
                orijin_text = " ".join(orijin_cell.text.split()) if orijin_cell else ""
                orijin_parts = orijin_text.split('/')
                baba = orijin_parts[0].strip() if len(orijin_parts) > 0 else ""
                anne = orijin_parts[1].strip() if len(orijin_parts) > 1 else ""
                
                at_ismi_link = at_ismi_cell.find('a')
                
                horse = {
                    'name': at_ismi_cell.text.strip(),
                    'detailLink': at_ismi_link['href'] if at_ismi_link else "",
                    'breed': irk_cell.text.strip(),
                    'gender': cinsiyet_cell.text.strip() if cinsiyet_cell else "",
                    'age': yas_cell.text.strip() if yas_cell else "",
                    'father': baba,
                    'mother': anne,
                    'owner': sahip_cell.text.strip() if sahip_cell else "",
                    'trainer': antrenor_cell.text.strip() if antrenor_cell else "",
                    'lastRace': son_kosu_cell.text.strip() if son_kosu_cell else "",
                    'prize': ikramiye_cell.text.strip() if ikramiye_cell else ""
                }
                
                horses.append(horse)
                
            except Exception as e:
                print(f"Satır parse hatası: {e}")
                continue
        
        return jsonify({
            'success': True,
            'horses': horses,
            'count': len(horses)
        })
        
    except requests.exceptions.RequestException as e:
        return jsonify({
            'success': False,
            'error': f'İstek hatası: {str(e)}'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Beklenmeyen hata: {str(e)}'
        }), 500


# ══════════════════════════════════════════════════════════════════
# FAZ 7: OTOMATİK SONUÇ ÇEKME
# ══════════════════════════════════════════════════════════════════

@app.route('/api/fetch-race-results', methods=['POST'])
def fetch_race_results():
    """
    Günün programındaki bir koşunun gerçek sonuçlarını otomatik çeker.
    At geçmişinden (horse-details gibi) bitiş pozisyonlarını okur.

    Body:
      {
        "race_date": "24.04.2026",   # Koşu tarihi (dd.mm.yyyy)
        "race_no": "3",              # Koşu numarası
        "horses": [
          {"name": "ERDEK", "detailLink": "/TR/.../AtBilgileri?..."},
          ...
        ]
      }

    Response:
      {
        "success": true,
        "results": [
          {"horse_name": "ERDEK", "finish_pos": 1},
          ...
        ],
        "race_id": "24.04.2026-3"
      }
    """
    try:
        data        = request.json
        race_date   = data.get('race_date', '').strip()   # "24.04.2026"
        race_no     = data.get('race_no', '').strip()     # "3"
        horses_in   = data.get('horses', [])

        if not race_date or not horses_in:
            return jsonify({'success': False, 'error': 'race_date ve horses zorunlu'}), 400

        race_id = f"{race_date}-{race_no}" if race_no else race_date

        results = []
        errors  = []

        for horse in horses_in:
            horse_name  = horse.get('name', '').strip()
            horse_no = str(horse.get('no', '') or '').strip()
            # FAZ 7.4: TJK scraper at ismine newline + derece numarası ekleyebiliyor
            # Örn: "AĞASAÇAN\n (1)" → "AĞASAÇAN"
            horse_name = horse_name.split('\n')[0].strip()
            horse_name = re.sub(r'\s*\(\s*\d+\s*\)\s*$', '', horse_name).strip()
            detail_link = horse.get('detailLink', '').strip()

            if not detail_link or not horse_name:
                continue

            try:
                detail_url = urljoin(TARGET_URL, detail_link)
                detail_url = detail_url.replace('&amp;', '&')

                resp = requests.get(detail_url, headers=HEADERS, timeout=12, verify=False)
                if resp.status_code != 200:
                    errors.append(f'{horse_name}: HTTP {resp.status_code}')
                    continue

                soup     = BeautifulSoup(resp.text, 'html.parser')
                data_div = soup.find('div', id='dataDiv')
                if not data_div:
                    errors.append(f'{horse_name}: dataDiv yok')
                    continue

                race_table = data_div.find('table', id='queryTable')
                tbody      = race_table.find('tbody', id='tbody0') if race_table else None
                if not tbody:
                    errors.append(f'{horse_name}: tablo yok')
                    continue

                # Tarihe göre eşleştir
                found_pos = None
                for row in tbody.find_all('tr'):
                    if 'hidable' in row.get('class', []):
                        continue
                    cells = row.find_all('td')
                    if len(cells) < 6:
                        continue

                    row_date = cells[0].text.strip()   # "24.04.2026"
                    position = cells[4].text.strip()   # "1", "2", "K" vs.

                    # Tarih eşleştir (gün.ay.yıl — farklı format varyantları)
                    row_date_norm = row_date.replace('/', '.').replace('-', '.')[:10]
                    race_date_norm = race_date.replace('/', '.').replace('-', '.')[:10]

                    if row_date_norm == race_date_norm:
                        # Koşu numarası da eşleştirmeye çalış
                        # cells[1] genellikle şehir, bazı formatlarda race_no var
                        # Tarihe göre buldukta ilk eşleşmeyi al (en yakın koşu)
                        if position.isdigit():
                            found_pos = int(position)
                            break
                        else:
                            # K=Kalp, D=Disklifiye, F=Foul vb. — sona koy
                            found_pos = 99
                            break

                if found_pos is not None:
                    results.append({
                        'horse_name': horse_name,
                        'horse_no': horse_no,
                        'finish_pos': found_pos,
                    })
                else:
                    errors.append(f'{horse_name}: {race_date} tarihli koşu geçmişte bulunamadı')

            except Exception as e:
                errors.append(f'{horse_name}: {str(e)}')
                continue

        if not results:
            return jsonify({
                'success': False,
                'error': 'Hiçbir at için sonuç bulunamadı.',
                'details': errors,
            }), 404

        # Sıralamaya göre tertle
        results_sorted = sorted(results, key=lambda x: x['finish_pos'])

        # FAZ 7.3: predictions.jsonl'dan numeric race_id lookup
        # fetch-race-results "28.04.2026-3" formatında ID üretiyor ama
        # predictions.jsonl'da numeric ("224666") ID var. Doğru ID'yi bul.
        import json as _fj, os as _fo
        _log_path = _fo.path.join(_fo.path.dirname(__file__), 'predictions.jsonl')
        numeric_race_id = None
        if _fo.path.exists(_log_path):
            try:
                with open(_log_path, 'r', encoding='utf-8') as _lf:
                    for _line in _lf:
                        _line = _line.strip()
                        if not _line:
                            continue
                        try:
                            _entry = _fj.loads(_line)
                            if (str(_entry.get('race_date', '')) == race_date and
                                    str(_entry.get('race_no', '')) == str(race_no)):
                                numeric_race_id = str(_entry.get('race_id', ''))
                                break
                        except Exception:
                            continue
            except Exception:
                pass

        final_race_id = numeric_race_id if numeric_race_id else race_id
        print(f'[FETCH-RESULTS] {race_id}: {len(results)} at sonucu bulundu, {len(errors)} hata → final race_id={final_race_id}')
        return jsonify({
            'success': True,
            'race_id': final_race_id,   # numeric ID (varsa), yoksa tarih-format
            'results': results_sorted,
            'errors':  errors,
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/horse-details', methods=['POST'])
def get_horse_details():
    """At detay bilgilerini getir"""
    try:
        data = request.json
        relative_url = data.get('detailLink', '')
        
        if not relative_url:
            return jsonify({
                'success': False,
                'error': 'Detay linki bulunamadı'
            }), 400
        
        detail_url = urljoin(TARGET_URL, relative_url)
        detail_url = detail_url.replace("&amp;", "&")
        
        response = requests.get(detail_url, headers=HEADERS, timeout=10, verify=False)
        
        if response.status_code != 200:
            return jsonify({
                'success': False,
                'error': f'Detay sayfası alınamadı. Status: {response.status_code}'
            }), 500
        
        soup = BeautifulSoup(response.text, 'html.parser')
        data_div = soup.find('div', id='dataDiv')
        
        if not data_div:
            return jsonify({
                'success': False,
                'error': 'Detay sayfasında veri bulunamadı'
            }), 404
        
        race_table = data_div.find('table', id='queryTable')
        if not race_table:
            return jsonify({
                'success': True,
                'races': [],
                'message': 'Yarış geçmişi bulunamadı'
            })
        
        table_body = race_table.find('tbody', id='tbody0')
        if not table_body:
            return jsonify({
                'success': True,
                'races': [],
                'message': 'Yarış geçmişi bulunamadı'
            })
        
        rows = table_body.find_all('tr')
        races = []
        
        for row in rows:
            if 'hidable' in row.get('class', []):
                continue
            
            cells = row.find_all('td')
            
            if len(cells) > 17:
                try:
                    race = {
                        'date': cells[0].text.strip(),
                        'city': cells[1].text.strip(),
                        'distance': cells[2].text.strip(),
                        'track': " ".join(cells[3].text.strip().split()),
                        'position': cells[4].text.strip(),
                        'grade': cells[5].text.strip(),
                        'jockey': cells[8].text.strip(),
                        'prize': cells[17].text.strip()
                    }
                    races.append(race)
                except IndexError:
                    continue
        
        return jsonify({
            'success': True,
            'races': races,
            'count': len(races)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Hata: {str(e)}'
        }), 500

@app.route('/api/search-races', methods=['POST'])
def search_races():
    """Yarış arama endpoint'i - Web scraping ile"""
    try:
        data = request.json
        
        # TJK yarış sorgulama sayfası - GET ile direkt HTML çekiyoruz
        base_url = "https://www.tjk.org/TR/YarisSever/Query/Page/KosuSorgulama"
        
        # Query parametreleri
        params = {
            'QueryParameter_Tarih_Start': data.get('startDate', ''),
            'QueryParameter_Tarih_End': data.get('endDate', ''),
            'QueryParameter_SehirId': '-1',  # Tüm şehirler
        }
        
        # Opsiyonel parametreler
        if data.get('distance'):
            params['QueryParameter_Mesafe'] = data.get('distance')
        if data.get('fatherName'):
            params['QueryParameter_BabaIsmi'] = data.get('fatherName')
        if data.get('motherName'):
            params['QueryParameter_AnneIsmi'] = data.get('motherName')
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.tjk.org/TR/YarisSever/Query/Page/KosuSorgulama"
        }
        
        # TJK sayfasını GET ile çek
        response = requests.get(
            base_url,
            params=params,
            headers=headers,
            timeout=15
        )
        
        if response.status_code != 200:
            return jsonify({
                'success': False,
                'error': f'TJK sayfası yüklenemedi. Status: {response.status_code}'
            }), 500
        
        # HTML'i parse et
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tablonun gövdesini sağlam şekilde bul (tbody0/thead/tbody1 farklarını tolere et)
        table = soup.find('table', id='queryTable')
        tbody = None
        if table:
            tbody = table.find('tbody') or table.find('tbody', id='tbody0') or table.find('tbody', id='tbody1')
        if not tbody:
            tbody = soup.find('tbody', id='tbody0') or soup.find('tbody', id='tbody1')
        
        if not tbody:
            return jsonify({
                'success': True,
                'races': [],
                'message': 'Sonuç bulunamadı'
            })
        
        # Tüm satırları al
        race_rows = tbody.find_all('tr')
        
        if not race_rows:
            return jsonify({
                'success': True,
                'races': [],
                'message': 'Sonuç bulunamadı'
            })
        
        races = []
        
        for row in race_rows:
            try:
                cells = row.find_all('td')
                
                if len(cells) >= 8:
                    # Detay linkini bul
                    detail_link = ''
                    link_elem = cells[0].find('a', href=True) if len(cells) > 0 else None
                    if link_elem:
                        detail_link = link_elem['href']
                    
                    # Tarih hücresinden sadece metni al
                    date_text = cells[0].text.strip() if len(cells) > 0 else ''
                    
                    # Sütun eşleştirmeleri TJK başlık sırasına göre düzeltildi
                    race = {
                        'date': date_text,                                            # 0: Tarih (dd.MM.yyyy)
                        'city': cells[1].text.strip() if len(cells) > 1 else '',      # 1: Şehir
                        'raceNumber': cells[2].text.strip() if len(cells) > 2 else '',# 2: Koşu
                        'group': cells[3].text.strip() if len(cells) > 3 else '',     # 3: Grup
                        'raceType': cells[4].text.strip() if len(cells) > 4 else '',  # 4: Koşu Cinsi
                        'apprenticeType': cells[5].text.strip() if len(cells) > 5 else '', # 5: Apr. Koş. Cinsi
                        'distance': cells[6].text.strip() if len(cells) > 6 else '',  # 6: Mesafe
                        'track': cells[7].text.strip() if len(cells) > 7 else '',     # 7: Pist
                        'detailLink': detail_link
                    }
                    
                    races.append(race)
                    
            except Exception as e:
                print(f"Satır parse hatası: {e}")
                continue
        
        return jsonify({
            'success': True,
            'races': races,
            'count': len(races)
        })
        
    except requests.exceptions.RequestException as e:
        return jsonify({
            'success': False,
            'error': f'İstek hatası: {str(e)}'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Beklenmeyen hata: {str(e)}'
        }), 500


@app.route('/api/daily-races', methods=['POST'])
def get_daily_races():
    """Günün koşularını getir"""
    try:
        data = request.json
        # Gerçek TJK SehirId değerleri (live sayfadan doğrulandı: 02.05.2026)
        city_map = {
            'İstanbul': '1',
            'Ankara':   '5',
            'İzmir':    '2',
            'Adana':    '4',
            'Bursa':    '3',
            'Şanlıurfa':'6',
            'Diyarbakır':'8',
            'Elazığ':   '9',
            'Kocaeli':  '9'
        }

        city = data.get('city', 'İstanbul')
        city_id = city_map.get(city, '1')

        # Bugünün tarihini al
        from datetime import datetime
        today = datetime.now().strftime('%d.%m.%Y')
        today_url = datetime.now().strftime('%d/%m/%Y')   # URL formatı

        # TJK günlük program sayfası — şehir bazlı doğru URL
        url = (
            f"https://www.tjk.org/TR/YarisSever/Info/Sehir/GunlukYarisProgrami"
            f"?SehirId={city_id}&QueryParameter_Tarih={today_url}&SehirAdi={city}&Era=today"
        )

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9",
            "Referer": "https://www.tjk.org/TR/YarisSever/Info/Page/GunlukYarisProgrami"
        }

        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code != 200:
            return jsonify({
                'success': False,
                'error': f'TJK sayfası yüklenemedi. Status: {response.status_code}'
            }), 500

        soup = BeautifulSoup(response.text, 'html.parser')

        # ── Saat bilgisini race_id → time eşlemesi olarak çıkar ──────────────
        time_map = {}
        tabs_ul = soup.find('ul', class_=lambda c: c and 'races-tabs' in (' '.join(c) if isinstance(c, list) else c))
        if tabs_ul:
            for a_tag in tabs_ul.find_all('a', href=True):
                href = a_tag.get('href', '')
                frag_match = re.search(r'#(\d+)', href)
                if frag_match:
                    rid = frag_match.group(1)
                    full_text = a_tag.get_text(separator='\n', strip=True)
                    lines = [l.strip() for l in full_text.splitlines() if l.strip()]
                    if len(lines) >= 2:
                        time_map[rid] = lines[1]

        # ── Her koşu için race-details div'ini parse et ─────────────────────
        races = []
        race_details_list = soup.find_all('div', class_='race-details')

        for rd in race_details_list:
            try:
                race_no_el = rd.find('h3', class_='race-no')
                race_config_el = rd.find('h3', class_='race-config')

                race_number = ''
                race_id = ''
                race_time = ''
                if race_no_el:
                    anchor = race_no_el.find('a', href=True)
                    if anchor:
                        href = anchor.get('href', '')
                        frag = re.search(r'#(\d+)', href)
                        if frag:
                            race_id = frag.group(1)
                            race_time = time_map.get(race_id, '')
                    no_text = race_no_el.get_text(separator=' ', strip=True)
                    no_match = re.search(r'(\d+)\.\s*Ko[şs]u', no_text, re.IGNORECASE)
                    if no_match:
                        race_number = no_match.group(1)

                distance = ''
                track = ''
                race_type = ''
                if race_config_el:
                    config_text = race_config_el.get_text(separator=' ', strip=True)
                    dist_match = re.search(r'\b(\d{3,4})\b', config_text)
                    if dist_match:
                        distance = dist_match.group(1)
                    track_match = re.search(r'\b(Çim|Cim|Kum|Sentetik)\b', config_text, re.IGNORECASE)
                    if track_match:
                        trk = track_match.group(1)
                        if 'im' in trk.lower():
                            track = 'Çim'
                        else:
                            track = trk.capitalize()
                    type_match = re.search(r'^(.+?)\s*,', config_text.strip())
                    if type_match:
                        race_type = type_match.group(1).strip()

                # İkramiye parse
                prize = ''
                parent_pane = rd.find_parent('div', id=True)
                if parent_pane:
                    for h3_el in parent_pane.find_all('h3'):
                        if 'kramiye' in h3_el.get_text():
                            dl_el = h3_el.find_next_sibling('dl')
                            if dl_el:
                                dl_text = dl_el.get_text(separator=' ', strip=True)
                                prize_match = re.search(r'1\.\)\s*([\d.,]+)', dl_text)
                                if prize_match:
                                    prize = prize_match.group(1).replace('.', '').replace(',', '') + ' TL'
                            break
                if race_number:
                    races.append({
                        'raceNumber': race_number,
                        'raceId': race_id,
                        'time': race_time,
                        'distance': distance,
                        'track': track,
                        'raceType': race_type,
                        'prize': prize,
                        'city': city
                    })

            except Exception as e:
                print(f"[daily-races] Koşu parse hatası: {e}")
                continue

        return jsonify({
            'success': True,
            'races': races,
            'city': city,
            'date': today,
            'count': len(races)
        })

    except requests.exceptions.RequestException as e:
        return jsonify({
            'success': False,
            'error': f'İstek hatası: {str(e)}'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Beklenmeyen hata: {str(e)}'
        }), 500


TJK_DAILY_PAGE_URL = "https://www.tjk.org/TR/YarisSever/Info/Page/GunlukYarisProgrami"
TJK_DAILY_CITY_URL = "https://www.tjk.org/TR/YarisSever/Info/Sehir/GunlukYarisProgrami"
TJK_DAILY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": TJK_DAILY_PAGE_URL,
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _clean_daily_text(value):
    return re.sub(r'\s+', ' ', value or '').strip()


def _tjk_daily_get(url, **kwargs):
    """Fetch TJK daily-program HTML through the backend-only scraper path."""
    response = requests.get(
        url,
        headers=TJK_DAILY_HEADERS,
        timeout=kwargs.pop('timeout', 20),
        verify=False,
        **kwargs,
    )
    return response


def _parse_daily_cities(html):
    soup = BeautifulSoup(html, 'html.parser')
    cities = []
    seen = set()

    for link in soup.select('a[href*="SehirId"]'):
        href = link.get('href') or ''
        parsed = urlparse(urljoin(TJK_DAILY_PAGE_URL, href))
        params = parse_qs(parsed.query)
        city_id = (params.get('SehirId') or [''])[0]
        city_name = (params.get('SehirAdi') or [''])[0]
        display_name = city_name or _clean_daily_text(link.get_text(' ', strip=True))

        if not city_id or not display_name or city_id in seen:
            continue

        cities.append({'id': city_id, 'name': _clean_daily_text(display_name)})
        seen.add(city_id)

    return cities


def _parse_daily_horses(table):
    horses = []

    for row in table.select('tr'):
        if row.select('th') or not row.select('td'):
            continue

        no_cell = row.select_one('.gunluk-GunlukYarisProgrami-SiraId')
        name_cell = row.select_one('.gunluk-GunlukYarisProgrami-AtAdi')
        name_anchor = None
        if name_cell:
            for anchor in name_cell.find_all('a', href=True, recursive=False):
                if 'AtKosuBilgileri' in anchor.get('href', ''):
                    name_anchor = anchor
                    break
        no = _clean_daily_text(no_cell.get_text(' ', strip=True)) if no_cell else ''
        if name_anchor:
            name = _clean_daily_text(name_anchor.get_text(' ', strip=True))
        elif name_cell:
            direct_text = ''.join(str(item) for item in name_cell.find_all(string=True, recursive=False))
            name = _clean_daily_text(direct_text)
        else:
            name = ''
        detail_link = name_anchor.get('href', '') if name_anchor else ''

        if not no or not name:
            continue

        def cell_text(selector):
            cell = row.select_one(selector)
            return _clean_daily_text(cell.get_text(' ', strip=True)) if cell else ''

        origin = cell_text('.gunluk-GunlukYarisProgrami-Baba')
        father = origin
        mother = ''
        if ' - ' in origin:
            parts = origin.split(' - ', 1)
            father = parts[0].strip()
            mother = parts[1].split('/')[0].strip()

        best_rating = cell_text('.gunluk-GunlukYarisProgrami-DERECE').split(' ')[0]

        horses.append({
            'no': no,
            'name': name,
            'jockey': cell_text('.gunluk-GunlukYarisProgrami-JokeAdi'),
            'weight': cell_text('.gunluk-GunlukYarisProgrami-Kilo'),
            'age': cell_text('.gunluk-GunlukYarisProgrami-Yas'),
            'owner': cell_text('.gunluk-GunlukYarisProgrami-SahipAdi'),
            'last6': cell_text('.gunluk-GunlukYarisProgrami-Son6Yaris'),
            'father': father,
            'mother': mother,
            'trainer': cell_text('.gunluk-GunlukYarisProgrami-AntronorAdi'),
            'hp': cell_text('.gunluk-GunlukYarisProgrami-Hc'),
            'kgs': cell_text('.gunluk-GunlukYarisProgrami-KGS'),
            's20': cell_text('.gunluk-GunlukYarisProgrami-s20') or cell_text('.gunluk-GunlukYarisProgrami-S20'),
            'bestRating': best_rating,
            'agf': cell_text('.gunluk-GunlukYarisProgrami-AGFORAN'),
            'detailLink': detail_link,
        })

    return horses


def _parse_daily_races(html, city_id, city_name):
    soup = BeautifulSoup(html, 'html.parser')

    time_map = {}
    tabs_ul = soup.find('ul', class_=lambda c: c and 'races-tabs' in (' '.join(c) if isinstance(c, list) else c))
    if tabs_ul:
        for a_tag in tabs_ul.find_all('a', href=True):
            frag_match = re.search(r'#(\d+)', a_tag.get('href', ''))
            if not frag_match:
                continue
            lines = [line.strip() for line in a_tag.get_text(separator='\n', strip=True).splitlines() if line.strip()]
            if len(lines) >= 2:
                time_map[frag_match.group(1)] = lines[1]

    races = []
    for rd in soup.select('div.race-details'):
        race_no_el = rd.select_one('h3.race-no')
        race_config_el = rd.select_one('h3.race-config')

        race_no = ''
        race_id = ''
        race_time = ''
        if race_no_el:
            anchor = race_no_el.find('a', href=True)
            if anchor:
                frag = re.search(r'#(\d+)', anchor.get('href', ''))
                if frag:
                    race_id = frag.group(1)
                    race_time = time_map.get(race_id, '')
            no_match = re.search(r'(\d+)\.', race_no_el.get_text(' ', strip=True))
            if no_match:
                race_no = no_match.group(1)

        race_name = ''
        distance = ''
        track_type = ''
        if race_config_el:
            config_text = _clean_daily_text(race_config_el.get_text(' ', strip=True))
            dist_match = re.search(r'\b(\d{3,4})\b', config_text)
            if dist_match:
                distance = dist_match.group(1)
            track_match = re.search(r'\b(Çim|Cim|Kum|Sentetik)\b', config_text, re.IGNORECASE)
            if track_match:
                track_type = track_match.group(1)
                if track_type.lower().endswith('im'):
                    track_type = 'Çim'
                else:
                    track_type = track_type.capitalize()
            type_match = re.search(r'^(.+?)\s*,', config_text)
            if type_match:
                race_name = type_match.group(1).strip()

        prize = ''
        parent_pane = rd.find_parent('div', id=True)
        if parent_pane:
            for h3_el in parent_pane.find_all('h3'):
                if 'kramiye' in h3_el.get_text(' ', strip=True).lower():
                    dl_el = h3_el.find_next_sibling('dl')
                    if dl_el:
                        prize_match = re.search(r'1\.\)\s*([\d.,]+)', dl_el.get_text(' ', strip=True))
                        if prize_match:
                            prize = prize_match.group(1).replace('.', '').replace(',', '') + ' TL'
                    break

        if race_no:
            races.append({
                'time': race_time,
                'raceNo': race_no,
                'raceNumber': race_no,
                'city': city_name or city_id,
                'raceName': race_name,
                'raceType': race_name,
                'distance': distance,
                'trackType': track_type,
                'track': track_type,
                'prize': prize,
                'raceId': race_id,
                'horses': [],
            })

    horse_tables = [table for table in soup.select('table') if table.select('.gunluk-GunlukYarisProgrami-AtAdi')]
    for index, table in enumerate(horse_tables[:len(races)]):
        races[index]['horses'] = _parse_daily_horses(table)

    return races


def _load_daily_program(date_param, requested_city_id=None, requested_city_name=None):
    main_response = _tjk_daily_get(
        TJK_DAILY_PAGE_URL,
        params={'QueryParameter_Tarih': date_param},
    )
    main_response.raise_for_status()

    cities = _parse_daily_cities(main_response.text)
    if not cities:
        return [], [], requested_city_id or '', requested_city_name or ''

    selected = None
    if requested_city_id:
        selected = next((city for city in cities if city['id'] == requested_city_id), None)
    if selected is None:
        selected = cities[0]

    city_id = selected['id']
    city_name = requested_city_name or selected['name']
    city_response = _tjk_daily_get(
        TJK_DAILY_CITY_URL,
        params={
            'SehirId': city_id,
            'QueryParameter_Tarih': date_param,
            'SehirAdi': city_name,
            'Era': 'today',
        },
    )
    city_response.raise_for_status()

    races = _parse_daily_races(city_response.text, city_id, city_name)
    return cities, races, city_id, city_name


@app.route('/api/compare-horses', methods=['POST'])
def compare_horses():
    """Atları karşılaştır ve kazanma olasılıklarını hesapla"""
    try:
        data = request.json
        horses_to_compare = data.get('horses', [])
        
        if not horses_to_compare:
            return jsonify({
                'success': False,
                'error': 'Karşılaştırılacak at listesi boş'
            }), 400
            
        # Eğer at detayları eksikse (örneğin sadece link varsa), detayları çek
        # Bu örnekte frontend'in detayları zaten gönderdiğini varsayıyoruz, 
        # ancak tam bir implementasyonda burada eksik veriler için fetch yapılabilir.
        # Biz şimdilik frontend'in dolu veri gönderdiğini varsayalım veya
        # prediction_logic içinde eksik verileri handle edelim.
        
        # Olasılıkları hesapla
        compared_horses = prediction_logic.calculate_winning_probability(horses_to_compare)
        
        return jsonify({
            'success': True,
            'horses': compared_horses,
            'count': len(compared_horses)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Hata: {str(e)}'
        }), 500

@app.route('/daily-program', methods=['GET'])
def daily_program():
    """TJK Günlük Yarış Programı"""
    try:
        date_param = request.args.get('date')  # Format: dd/MM/yyyy
        city_id = request.args.get('cityId')
        city_name = request.args.get('cityName')
        
        if not date_param:
            return jsonify({'success': False, 'error': 'Date parameter is required'}), 400

        cities, races, selected_city_id, selected_city_name = _load_daily_program(
            date_param,
            requested_city_id=city_id,
            requested_city_name=city_name,
        )

        return jsonify({
            'success': True,
            'races': races,
            'count': len(races),
            'cities': cities,
            'cityId': selected_city_id,
            'cityName': selected_city_name,
            'date': date_param,
        })

        # TJK Headers
        headers = {
            'sec-ch-ua-platform': '"Windows"',
            'Referer': 'https://www.tjk.org/TR/YarisSever/Info/Page/GunlukYarisProgrami',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'Accept': 'text/html, */*; q=0.01'
        }
        
        # Parameters
        params = {
            'SehirId': city_id,
            'QueryParameter_Tarih': date_param,
            'Era': 'today'
        }
        
        target_url = "https://www.tjk.org/TR/YarisSever/Info/Sehir/GunlukYarisProgrami"
        
        response = requests.get(target_url, headers=headers, params=params, timeout=15)
        
        if response.status_code != 200:
            return jsonify({'success': False, 'error': f'TJK Error: {response.status_code}'}), 500
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        races = []
        
        # Parse logic
        # TJK usually returns a list of races in a specific structure.
        # We need to adapt to the HTML structure returned by this specific endpoint.
        # Based on typical TJK structure:
        
        # Look for race rows or cards
        # The structure might be different from the main page.
        # Let's try to find the main container.
        
        # Common TJK race list structure
        race_rows = soup.find_all('div', class_='row') # Generic
        
        # More specific: Look for "Kosu" containers
        # Since I cannot see the real HTML, I will try to be generic and robust.
        # I'll look for elements that look like race headers.
        
        # Try to find the race table or list
        # Often TJK uses tables for programs
        tables = soup.find_all('table')
        
        for table in tables:
            # Check if this table looks like a race list
            if table.find('thead'):
                # Parse rows
                rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')[1:]
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) > 5:
                        try:
                            # This is a guess at the structure based on typical TJK tables
                            # We might need to adjust this after testing or if the user provides more info.
                            # But the user said "Parse edilen veriyi temiz bir JSON formatına çevir"
                            # I will try to extract as much as possible.
                            
                            # However, the user also said "Kaldır: Ekranın üst kısmında bulunan... widget'ı tamamen kaldır."
                            # and "Kart Tasarımı: ... Koşu Saati, Şehir Adı, Koşu Türü..."
                            
                            # Let's try to find specific classes if possible.
                            # TJK often uses 'd-block' or specific classes for race info.
                            pass
                        except:
                            continue

        # Alternative: The endpoint might return the "City Program" which is often a list of races.
        # Let's look for "accordion" or "card" style elements if it's the mobile view, 
        # or table if desktop. The User-Agent is Windows/Chrome, so likely Desktop view.
        
        # Let's assume it returns the standard program table.
        # I will try to parse the "Program" table.
        
        program_table = soup.find('table', id='programTable') or soup.find('table')
        
        if program_table:
            rows = program_table.find_all('tr')
            current_race = {}
            
            for row in rows:
                # Skip headers
                if row.find('th'):
                    continue
                    
                cells = row.find_all('td')
                if not cells:
                    continue
                    
                # Try to identify columns
                # This is tricky without seeing the HTML.
                # But usually: Race No, Time, Horse Name, etc.
                # Wait, the user wants "Günün Koşuları" (Race List), not "Horse List" for a race.
                # The endpoint `GunlukYarisProgrami` usually lists the RACES (1. Koşu, 2. Koşu...).
                
                # Actually, `GunlukYarisProgrami` page usually has a list of races on the left (or top) 
                # and the details of the selected race.
                # But the endpoint `Info/Sehir/GunlukYarisProgrami` might return the list of races for that city.
                
                # Let's look for elements with class "race-header" or similar.
                pass

        # RE-EVALUATION:
        # The user said "Parse edilen veriyi temiz bir JSON formatına çevir (Örn: Koşu Saati, Şehir, Koşu İsmi, Mesafesi, Bahis Türü vb.)"
        # I'll try to find the race headers.
        
        race_headers = soup.find_all('div', class_='kosu-baslik') # Common TJK class
        if not race_headers:
             race_headers = soup.find_all('div', class_='card-header')
             
        # If we can't find specific classes, let's try to parse the text content of the response
        # to find patterns like "1. Koşu", "13:30", etc.
        
        # Let's try a more robust approach using the structure I saw in `get_daily_races` (lines 430+ in original file)
        # It looked for `race-card` or `kosu-card`.
        
        race_cards = soup.find_all('div', class_='race-card') or soup.find_all('div', class_='kosu-card')
        
        if not race_cards:
            # Try finding the main container
            main_container = soup.find('div', id='main-container') or soup
            
            # Look for race blocks
            # Pattern: "X. Koşu"
            # I'll iterate through all divs and check text
            all_divs = main_container.find_all('div')
            for div in all_divs:
                text = div.get_text(strip=True)
                if 'Koşu' in text and 'Saat' in text:
                    # Potential race header
                    # Parse it
                    # "1. KoşuSaat: 13:30..."
                    pass
        
        # Let's use the logic I wrote in the Dart service (regex) but in Python.
        # It's robust against HTML structure changes.
        
        import re
        text_content = soup.get_text(" | ", strip=True)
        
        # Regex to find races
        # Pattern: 1. Koşu | Saat: 13:30 | ...
        # Or: 1. Koşu 13:30
        
        # Let's try to find the race elements directly.
        # On TJK "GunlukYarisProgrami", races are often in an accordion or list.
        # The endpoint `Info/Sehir/GunlukYarisProgrami` likely returns the partial HTML for the race list.
        
        # I will look for `h5` or `h4` or `div` that contains "Koşu" and "Saat".
        
        found_races = []
        
        # Strategy: Find all elements that might be race headers
        candidates = soup.find_all(['div', 'h3', 'h4', 'h5', 'a'])
        
        for cand in candidates:
            text = cand.get_text(strip=True)
            # Check for "X. Koşu" and "Saat"
            if re.search(r'\d+\.\s*Koşu', text, re.IGNORECASE) and re.search(r'Saat', text, re.IGNORECASE):
                # Found a race header
                # Extract info
                race_num_match = re.search(r'(\d+)\.\s*Koşu', text, re.IGNORECASE)
                time_match = re.search(r'Saat\s*:?\s*(\d{2}:\d{2})', text, re.IGNORECASE)
                
                if race_num_match and time_match:
                    race_num = race_num_match.group(1)
                    time = time_match.group(1)
                    
                    # Try to get distance and track
                    # Usually in the same text or nearby
                    distance_match = re.search(r'(\d{3,4})\s*(?:Metre|m)?\s*(Çim|Kum|Sentetik)', text, re.IGNORECASE)
                    distance = ""
                    track = ""
                    if distance_match:
                        distance = distance_match.group(1)
                        track = distance_match.group(2)
                    
                    # Check if we already added this race (avoid duplicates from nested elements)
                    if not any(r['raceNumber'] == race_num for r in found_races):
                        found_races.append({
                            'raceNumber': race_num,
                            'time': time,
                            'distance': distance,
                            'track': track,
                            'city': city_id, # We don't have city name easily, use ID or map it
                            'info': text[:100] # Summary
                        })
        
        if not found_races:
             # Fallback: Try to parse from the `daily_races` logic I saw earlier
             # Maybe the response is a table?
             pass
             
        return jsonify({
            'success': True,
            'races': found_races,
            'count': len(found_races),
            'cityId': city_id,
            'date': date_param
        })

    except Exception as e:
        return jsonify({'success': False, 'error': f'Server Error: {str(e)}'}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Sunucu sağlık kontrolü"""
    return jsonify({'status': 'ok', 'message': 'TJK API Server çalışıyor'})

def _empty_horse_details(horse_data, reason='no_history'):
    """Return a usable neutral detail payload when TJK history is unavailable."""
    return {
        'name': horse_data.get('name'),
        'jockey': horse_data.get('jockey', ''),
        'weight': horse_data.get('weight', ''),
        'races': [],
        'filteredRaces': [],
        'degreeStats': calculate_degree_stats([]),
        'totalRaceCount': 0,
        'filteredRaceCount': 0,
        'detailFetchStatus': reason,
    }


def fetch_horse_details_safe(horse_data, target_distance=None, race_date_str=None):
    """
    Güvenli bir şekilde at detaylarını çeker (Hata yönetimi ile).
    FAZ 1.1: Tüm yarış geçmişini çeker, mesafe bazlı filtreleme yapar.
    
    TJK At Koşu Bilgileri Tablo Sütunları:
    [0]Tarih [1]Şehir [2]Msf [3]Pist [4]S(sıra) [5]Derece
    [6]Sıklet [7]Takı [8]Jokey [9]St [10]Gny [11]Grup
    [12]K.No-K.Adı [13]Kcins [14]Ant. [15]Sahip [16]HP [17]Ikramiye [18]S20
    """
    try:
        detail_link = horse_data.get('detailLink')
        if not detail_link:
            return _empty_horse_details(horse_data, 'missing_detail_link')
            
        full_url = urljoin(TARGET_URL, detail_link).replace("&amp;", "&")
        
        response = None
        last_error = None
        for attempt in range(3):
            try:
                response = requests.get(full_url, headers=HEADERS, timeout=20, verify=False)
                if response.status_code == 200:
                    break
                last_error = f'http_{response.status_code}'
            except Exception as req_err:
                last_error = f'request_error:{req_err}'
                time.sleep(0.4 * (attempt + 1))

        if response is None or response.status_code != 200:
            return _empty_horse_details(horse_data, last_error or 'http_error')
            
        soup = BeautifulSoup(response.text, 'html.parser')
        data_div = soup.find('div', id='dataDiv')
        if not data_div:
            return _empty_horse_details(horse_data, 'missing_data_div')
            
        race_table = data_div.find('table', id='queryTable')
        if not race_table:
            return _empty_horse_details(horse_data, 'empty_history')
            
        table_body = race_table.find('tbody', id='tbody0')
        if not table_body:
            return _empty_horse_details(horse_data, 'empty_history')
            
        rows = table_body.find_all('tr')
        all_races = []       # Tüm yarışlar
        filtered_races = []  # Mesafe bazlı filtrelenmiş yarışlar
        target_race_date = None
        if race_date_str:
            try:
                target_race_date = datetime.strptime(str(race_date_str).strip(), '%d.%m.%Y')
            except Exception:
                target_race_date = None
        
        # Hedef mesafeyi sayıya çevir (filtreleme için)
        target_dist_num = None
        if target_distance:
            try:
                target_dist_num = int(str(target_distance).replace(' ', '').replace('m', ''))
            except:
                pass
        
        for row in rows:
            if 'hidable' in row.get('class', []):
                continue
                
            cells = row.find_all('td')
            if len(cells) > 17:
                try:
                    race_date = cells[0].text.strip()
                    if target_race_date:
                        try:
                            parsed_race_date = datetime.strptime(race_date, '%d.%m.%Y')
                            if parsed_race_date >= target_race_date:
                                continue
                        except Exception:
                            pass
                    city = cells[1].text.strip()
                    distance = cells[2].text.strip()
                    track = " ".join(cells[3].text.strip().split())  # Pist tipi (Çim/Kum/Sentetik) + durum
                    rank = cells[4].text.strip()     # Sıralama
                    degree = cells[5].text.strip()   # Derece (süre)
                    weight = cells[6].text.strip()   # Sıklet
                    jockey = cells[8].text.strip()   # Jokey
                    group_info = cells[11].text.strip() if len(cells) > 11 else ''  # Grup
                    race_type = cells[13].text.strip() if len(cells) > 13 else ''   # Koşu Cinsi (Kcins)
                    
                    # Derece saniyeye çevir
                    degree_in_seconds = calculate_seconds(degree)
                    
                    # Pist bilgisini ayır: "Kum Normal" -> track_type="Kum", track_condition="Normal"
                    track_parts = track.split()
                    track_type = track_parts[0] if track_parts else track
                    track_condition = ' '.join(track_parts[1:]) if len(track_parts) > 1 else ''
                    
                    race_entry = {
                        'date': race_date,
                        'city': city,
                        'distance': distance,
                        'track': track_type,              # Pist tipi: Kum/Çim/Sentetik
                        'trackCondition': track_condition, # Pist durumu: Normal/Sulu/Islak/Ağır vb.
                        'rank': rank,
                        'weight': weight,
                        'jockey': jockey,
                        'degree': degree,
                        'degreeInSeconds': degree_in_seconds,
                        'group': group_info,               # Grup: Maiden/Şartlı/Handikap vb.
                        'raceType': race_type              # Koşu cinsi detayı
                    }
                    
                    all_races.append(race_entry)
                    
                    # Mesafe filtrelemesi (±100m tolerans)
                    if target_dist_num:
                        try:
                            race_dist = int(distance.replace(' ', ''))
                            if abs(race_dist - target_dist_num) <= 100:
                                # Derece verisi olan yarışları ön planda tut
                                if degree_in_seconds:
                                    filtered_races.append(race_entry)
                        except:
                            pass
                    
                except Exception as e:
                    continue
        
        # FAZ 3.1: Sınıf/Grup Zorluk Çarpanı uygula (tüm yarışlara)
        all_races = apply_class_factor_to_degrees(all_races)
        filtered_races = apply_class_factor_to_degrees(filtered_races)
        
        # Derece istatistikleri hesapla (filtrelenmiş yarışlar üzerinden)
        target_races = filtered_races if filtered_races else all_races
        degree_stats = calculate_degree_stats(target_races)
        
        return {
            'name': horse_data.get('name'),
            'jockey': horse_data.get('jockey', ''),
            'weight': horse_data.get('weight', ''),
            'races': all_races,
            'filteredRaces': filtered_races,
            'degreeStats': degree_stats,
            'totalRaceCount': len(all_races),
            'filteredRaceCount': len(filtered_races),
            'detailFetchStatus': 'ok' if all_races else 'empty_history',
        }
        
    except Exception as e:
        print(f"Error fetching details for {horse_data.get('name')}: {e}")
        return _empty_horse_details(horse_data, f'exception:{type(e).__name__}')


def calculate_degree_stats(races):
    """
    FAZ 1.2 + FAZ 3.1: Yarış listesinden derece istatistikleri hesaplar.
    Class factor uygulanmış adjustedDegreeInSeconds varsa onu kullanır,
    yoksa ham degreeInSeconds değerine düşer.
    
    Returns: {
        avgDegree, bestDegree, worstDegree (saniye),
        avgDegreeFormatted, bestDegreeFormatted, worstDegreeFormatted,
        degreeTrend (pozitif=iyileşme), degreeStdDev (düşük=istikrarlı),
        raceCount, degreeScore (0-100)
    }
    """
    # FAZ 3.1: adjustedDegreeInSeconds varsa onu tercih et
    degrees = [r.get('adjustedDegreeInSeconds') or r.get('degreeInSeconds') 
               for r in races 
               if r.get('adjustedDegreeInSeconds') or r.get('degreeInSeconds')]
    
    if not degrees:
        return {
            'avgDegree': None, 'bestDegree': None, 'worstDegree': None,
            'avgDegreeFormatted': '-', 'bestDegreeFormatted': '-', 'worstDegreeFormatted': '-',
            'degreeTrend': 0, 'degreeStdDev': 0, 'raceCount': 0,
            'degreeScore': 50, 'trendScore': 50, 'stabilityScore': 50
        }
    
    # FAZ B.2: Son yarış ağırlıklı derece ortalaması
    if len(degrees) <= 3:
        recency_weights = [0.45, 0.35, 0.20][:len(degrees)]
        w_total = sum(recency_weights)
        avg_degree = sum(d * w for d, w in zip(degrees, recency_weights)) / w_total
    else:
        recent_3 = degrees[:3]
        older = degrees[3:]
        recent_weights = [0.30, 0.25, 0.15]
        recent_avg = sum(d * w for d, w in zip(recent_3, recent_weights))
        older_weight_each = 0.30 / len(older) if older else 0
        older_avg = sum(d * older_weight_each for d in older)
        avg_degree = recent_avg + older_avg
    best_degree = min(degrees)
    worst_degree = max(degrees)
    std_dev = float(np.std(degrees)) if len(degrees) > 1 else 0

    # FAZ B.2: Son 3 yarışın en iyi derecesi (PASS 2 normalizasyonunda kullanılacak)
    recent_best = min(degrees[:3]) if degrees else None
    
    # Trend hesaplama: Son yarışlardaki iyileşme/kötüleşme
    trend_value = 0
    if len(degrees) >= 2:
        # degrees[0] = en son yarış, degrees[-1] = en eski yarış
        # Düşen süre = iyileşme (pozitif trend)
        y = np.array(degrees[::-1])  # Eski -> yeni sıra
        x = np.arange(len(y))
        if len(x) >= 2:
            slope, _ = np.polyfit(x, y, 1)
            trend_value = -slope  # Negatif slope = süre düşüyor = iyileşme
    
    # Skorlama
    # Derece skoru: Daha düşük ortalama = daha iyi (mesafeye göre normalizasyon gerekir ama burada göreceli)
    # Bu skor yarış grubu içinde normalize edilecek (analyze_race içinde)
    degree_score = 50  # Varsayılan - gruplar arası karşılaştırma gerekir
    
    # Trend skoru: Pozitif trend = iyileşme = yüksek skor
    trend_score = 50 + (trend_value * 10)
    trend_score = max(0, min(100, trend_score))
    
    # İstikrar skoru: Düşük std_dev = yüksek istikrar
    # std_dev 0-5 arası tipik, 0=mükemmel, 5+=çok değişken
    stability_score = max(0, min(100, 100 - (std_dev * 15)))
    
    return {
        'avgDegree': round(avg_degree, 2),
        'bestDegree': round(best_degree, 2),
        'worstDegree': round(worst_degree, 2),
        'recentBestDegree': round(recent_best, 2) if recent_best else None,  # FAZ B.2
        'avgDegreeFormatted': format_seconds_to_degree(avg_degree),
        'bestDegreeFormatted': format_seconds_to_degree(best_degree),
        'worstDegreeFormatted': format_seconds_to_degree(worst_degree),
        'degreeTrend': round(trend_value, 3),
        'degreeStdDev': round(std_dev, 3),
        'raceCount': len(degrees),
        'degreeScore': round(degree_score, 1),
        'trendScore': round(trend_score, 1),
        'stabilityScore': round(stability_score, 1)
    }


def format_seconds_to_degree(seconds):
    """Saniye değerini derece formatına çevirir: 125.34 -> '2.05.34'"""
    if seconds is None:
        return '-'
    try:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        centisecs = int(round((seconds % 1) * 100))
        if minutes > 0:
            return f"{minutes}.{secs:02d}.{centisecs:02d}"
        else:
            return f"{secs}.{centisecs:02d}"
    except:
        return '-'


def calculate_seconds(degree_str):
    """Derece stringini (1.24.50) saniyeye çevirir — iyileştirilmiş parse"""
    try:
        if not degree_str or degree_str.strip() in ('-', '', '0'):
            return None
        
        # Boşlukları temizle
        degree_str = degree_str.strip()
        
        parts = degree_str.split('.')
        if len(parts) == 3:
            # Format: dakika.saniye.salise (örn: 1.24.50 veya 2.05.34)
            minutes = int(parts[0])
            seconds = int(parts[1])
            centisecs = int(parts[2])
            return minutes * 60 + seconds + centisecs / 100
        elif len(parts) == 2:
            # Format: saniye.salise (örn: 24.50)
            seconds = int(parts[0])
            centisecs = int(parts[1])
            return seconds + centisecs / 100
        return None
    except:
        return None

# ============== CLASS FACTOR (SINIF ZORLUK ÇARPANI) ==============

def get_class_multiplier(group_info):
    """
    FAZ 3.1: TJK grup bilgisinden zorluk çarpanı döndürür.
    Daha zorlu gruplarda elde edilen dereceler daha değerli kabul edilir.
    
    Çarpan > 1.0: Derece bölündüğünde daha hızlı (= daha iyi) normalize edilir
    Çarpan < 1.0: Derece bölündüğünde daha yavaş (= daha düşük değer) normalize edilir
    
    Args:
        group_info (str): TJK'dan gelen grup bilgisi (örn: "Maiden", "KV-8", "Şartlı 2")
    
    Returns:
        float: Zorluk çarpanı (0.96 - 1.10 arası)
    """
    if not group_info:
        return 1.00
    
    g = group_info.strip().upper()
    
    # Açık Yarış / Grup yarışları (en zorlu)
    if (
        any(k in g for k in ['GRUP', 'GROUP', 'G1', 'G2', 'G3', 'AÇIK', 'ACIK', 'LİSTED', 'LISTED'])
        or re.search(r'\bG\s*[-/]?\s*[123]\b', g)
    ):
        return 1.10
    
    # Kısa Vade (KV) yarışları — numara bazlı ayrıntı
    if 'KV' in g or 'KISA VADE' in g:
        if '8' in g:
            return 1.08
        elif '7' in g:
            return 1.06
        elif '6' in g:
            return 1.05
        elif '5' in g:
            return 1.04
        return 1.05  # KV varsayılan
    
    # Handikap
    if 'HANDİKAP' in g or 'HANDIKAP' in g or 'HNDİKAP' in g or 'HNDIKAP' in g:
        return 1.02
    
    # Şartlı yarışlar — numara bazlı ayrıntı
    if 'ŞARTLI' in g or 'SARTLI' in g or 'Ş-' in g or 'S-' in g:
        if '4' in g or '5' in g:
            return 1.02
        elif '3' in g:
            return 1.01
        elif '2' in g:
            return 1.00
        elif '1' in g:
            return 0.98
        return 1.00  # Şartlı varsayılan
    
    # Tay / Maiden (en düşük zorluk)
    if 'MAİDEN' in g or 'MAIDEN' in g or 'TAY' in g:
        return 0.96
    
    # Bilinmeyen grup → nötr
    return 1.00


# ============== FAZ 4.1: PİST DURUMU ÇARPANI ==============

def get_track_condition_multiplier(condition):
    """
    FAZ 4.1: Pist durumundan derece normalizasyon çarpanı döndürür.
    
    Mantık: Islak/Ağır pistte atlar daha yavaş koşar. Bu çarpanla
    farklı durumlardaki dereceler karşılaştırılabilir hale gelir.
    Örnek: Ağır pist 2.10 ≈ Normal pist 2.05 → çarpan bunu düzeltir.
    
    Args:
        condition (str): Pist durumu (örn: "Normal", "Sulu", "Islak", "Ağır", "Yumuşak")
    
    Returns:
        float: Düzeltme çarpanı (0.93 - 1.00 arası)
                > Normal pist baz (1.00), ıslak pistte süre uzar → çarpan düşer
    """
    if not condition:
        return 1.00
    
    c = condition.strip().upper()
    
    # Tam eşleşmeler — TJK'nın kullandığı standart ifadeler
    if 'AĞIR' in c or 'AGIR' in c:
        return 0.93   # En yavaş koşulur → en büyük düzeltme
    elif 'ISLAK' in c:
        return 0.96
    elif 'SULU' in c:
        return 0.98
    elif 'YUMUŞAK' in c or 'YUMUSAK' in c:
        return 0.95
    elif 'SERİ' in c or 'SERT' in c:
        return 1.01   # Sert/seri pist → atlar biraz daha hızlı koşabilir
    elif 'NORMAL' in c or 'İYİ' in c or 'IYI' in c:
        return 1.00   # Baz pist
    
    # Bilinmeyen durum → nötr
    return 1.00


def apply_class_factor_to_degrees(races):
    """
    FAZ 3.1 + FAZ 4.1: Yarış listesindeki dereceleri;
      1) Sınıf/grup zorluk çarpanı (classMultiplier)
      2) Pist durumu çarpanı (trackConditionMultiplier) — FAZ 4.1 YENİ
    ile birlikte normalize eder.
    
    Formül:
        adjustedDegreeInSeconds = degreeInSeconds / classMultiplier * trackConditionMultiplier
    
    Örnek:
        Ağır pistte KV-8'de koşulan 2.10 (130sn)
        classMultiplier = 1.08, trackConditionMultiplier = 0.93
        adjusted = 130 / 1.08 * 0.93 ≈ 111.94sn
    
    Args:
        races (list): Yarış dictionaryleri listesi
    
    Returns:
        list: Her yarışa 'adjustedDegreeInSeconds', 'classMultiplier',
              'trackConditionMultiplier' eklenmiş hali
    """
    for race in races:
        # Sınıf çarpanı: raceType (KV-8, Maiden vb.) veya group bilgisinden
        race_type = race.get('raceType', '') or race.get('group', '')
        class_mult = get_class_multiplier(race_type)
        race['classMultiplier'] = class_mult
        
        # FAZ 4.1: Pist durumu çarpanı: trackCondition (Normal/Ağır/Islak vb.)
        track_condition = race.get('trackCondition', '')
        track_cond_mult = get_track_condition_multiplier(track_condition)
        race['trackConditionMultiplier'] = track_cond_mult
        
        # Birleşik normalize edilmiş derece
        degree_seconds = race.get('degreeInSeconds')
        if degree_seconds and degree_seconds > 0:
            # Zor sınıf derecesini bölerek, yavaş pist derecesini çarparak normalize et.
            # 1'in altındaki pist çarpanına bölmek süreyi uzatıp düzeltmeyi tersine çevirir.
            race['adjustedDegreeInSeconds'] = round(
                degree_seconds / class_mult * track_cond_mult, 2
            )
        else:
            race['adjustedDegreeInSeconds'] = None
    
    return races

# ============== TRAINING DATA FUNCTIONS ==============

def fetch_training_data_by_race_id(race_id):
    """
    Koşu ID'sine göre TJK'dan tüm atların idman verilerini çeker.
    KTip=5 parametresi İdman Bilgileri sekmesini getirir.
    Returns: dict mapping horse name to training info
    """
    try:
        if not race_id:
            return {}
            
        # Doğru TJK İdman Bilgileri endpoint'i (KTip=5)
        url = f"https://www.tjk.org/TR/YarisSever/Info/Karsilastirma/Karsilastirma"
        
        params = {
            'KosuKodu': str(race_id),
            'Era': 'today',
            'KTip': '5'  # İdman Bilgileri sekmesi
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'Accept': 'text/html, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://www.tjk.org/TR/YarisSever/Info/Page/GunlukYarisProgrami',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"[TRAINING] Koşu {race_id} için idman verisi alınamadı: {response.status_code}")
            return {}
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tablo gövdesini bul
        table = soup.find('table')
        if not table:
            print(f"[TRAINING] Koşu {race_id} için idman tablosu bulunamadı")
            return {}
            
        tbody = table.find('tbody')
        if not tbody:
            tbody = table  # tbody yoksa table'ı kullan
            
        rows = tbody.find_all('tr')
        if not rows:
            print(f"[TRAINING] Koşu {race_id} için idman satırı bulunamadı")
            return {}
        
        training_map = {}
            
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 10:
                try:
                    # Tablo yapısı: At No, At Adı, mesafe süreleri..., İdman Tarihi, Pist, Hipodrom, İdman Jokeyi
                    horse_name = ''
                    
                    # At adını bul (genellikle link içinde)
                    name_cell = cells[1] if len(cells) > 1 else cells[0]
                    name_link = name_cell.find('a')
                    if name_link:
                        horse_name = name_link.text.strip()
                    else:
                        horse_name = name_cell.text.strip()
                    
                    if not horse_name:
                        continue
                    
                    # Mesafe sürelerini parse et (sütunlar 2-12)
                    times = {}
                    distance_cols = [
                        (2, '2200m'), (3, '2000m'), (4, '1800m'), (5, '1600m'),
                        (6, '1400m'), (7, '1200m'), (8, '1000m'), (9, '800m'),
                        (10, '600m'), (11, '400m'), (12, '200m')
                    ]
                    
                    for col_idx, dist in distance_cols:
                        if col_idx < len(cells):
                            time_val = cells[col_idx].text.strip()
                            if time_val and time_val != '-':
                                times[dist] = time_val
                    
                    # Sabit sütun indeksleri (HTML yapısına göre):
                    # 13: İdman Tarihi (span içinde)
                    # 14: Pist (Kum/Çim/Sentetik)
                    # 15: Pist Durumu (boş olabilir)
                    # 16: İdman Türü (Galop vb.)
                    # 17: İdman Hipodromu (Bursa, Ankara vb.)
                    # 18: İdman Jokeyi
                    
                    training_date = ''
                    track_condition = ''
                    hippodrome = ''
                    training_jockey = ''
                    
                    # İdman Tarihi (index 13) - <span> içinde olabilir
                    if len(cells) > 13:
                        date_cell = cells[13]
                        span = date_cell.find('span')
                        if span:
                            training_date = span.text.strip()
                        else:
                            training_date = date_cell.text.strip()
                        # Tarih formatını düzelt: d.MM.yyyy -> dd.MM.yyyy
                        if training_date and '.' in training_date:
                            parts = training_date.split('.')
                            if len(parts) == 3:
                                # Gün ve ay'ı 2 haneli yap
                                training_date = f"{parts[0].zfill(2)}.{parts[1].zfill(2)}.{parts[2]}"
                    
                    # Pist (index 14)
                    if len(cells) > 14:
                        track_condition = cells[14].text.strip()
                    
                    # Hipodrom (index 17)
                    if len(cells) > 17:
                        hippodrome = cells[17].text.strip()
                    
                    # Jokey (index 18)
                    if len(cells) > 18:
                        training_jockey = cells[18].text.strip()
                    
                    training_data = {
                        'horseName': horse_name,
                        'times': times,
                        'trainingDate': training_date,
                        'hippodrome': hippodrome,
                        'trackCondition': track_condition,
                        'trainingJockey': training_jockey,
                    }
                    
                    # Horse name'i uppercase key olarak kullan (eşleştirme için)
                    training_map[horse_name.upper()] = training_data
                    print(f"[TRAINING] {horse_name}: Tarih={training_date}, Süre={(list(times.values())[0] if times else 'yok')}")
                        
                except Exception as e:
                    print(f"[TRAINING] Satır parse hatası: {e}")
                    continue
        
        print(f"[TRAINING] Koşu {race_id} için {len(training_map)} at idman verisi bulundu")
        return training_map
        
    except Exception as e:
        print(f"[TRAINING ERROR] Koşu {race_id}: {e}")
        return {}

def parse_training_time(time_str):
    """
    İdman süresini (örn: '0.24.50' veya '24.50') saniyeye çevirir.
    """
    try:
        if not time_str or time_str == '-' or time_str.strip() == '':
            return None
            
        time_str = time_str.strip()
        parts = time_str.split('.')
        
        if len(parts) == 3:
            # Format: dakika.saniye.salise (örn: 0.24.50)
            minutes = int(parts[0])
            seconds = int(parts[1])
            centiseconds = int(parts[2])
            return minutes * 60 + seconds + centiseconds / 100
        elif len(parts) == 2:
            # Format: saniye.salise (örn: 24.50)
            seconds = int(parts[0])
            centiseconds = int(parts[1])
            return seconds + centiseconds / 100
        
        return None
    except:
        return None

def calculate_training_fitness(training_data, race_date_str=None):
    """
    İdman verilerinden fitness skoru hesaplar.
    
    Faktörler:
    1. İdman zamanlaması: Yarıştan 2-5 gün önce ideal
    2. İdman süreleri: Hızlı süreler = yüksek skor
    
    Returns: (score: 0-100, label: str, days_since: int or None, best_time: str or None)
    """
    if not training_data:
        return 50.0, "Bilinmiyor", None, None, None
        
    from datetime import datetime, timedelta
    
    score = 50.0  # Başlangıç skoru
    days_since_training = None
    best_time_str = None
    best_distance = None
    
    # 1. İdman tarihi analizi
    training_date_str = training_data.get('trainingDate', '')
    if training_date_str:
        try:
            # TJK tarih formatı: dd.MM.yyyy
            training_date = datetime.strptime(training_date_str, '%d.%m.%Y')
            
            # Yarış tarihi verilmediyse bugünü kullan
            if race_date_str:
                try:
                    race_date = datetime.strptime(race_date_str, '%d.%m.%Y')
                except:
                    race_date = datetime.now()
            else:
                race_date = datetime.now()
            
            days_since_training = (race_date - training_date).days
            
            # İdeal zamanlama: 2-5 gün önce
            if 2 <= days_since_training <= 5:
                score += 25  # Mükemmel zamanlama
            elif 1 <= days_since_training <= 7:
                score += 15  # İyi zamanlama
            elif days_since_training <= 10:
                score += 5   # Kabul edilebilir
            elif days_since_training > 14:
                score -= 10  # Çok eski idman
                
        except Exception as e:
            print(f"[TRAINING] Tarih parse hatası: {e}")
    
    # 2. İdman süreleri analizi
    times = training_data.get('times', {})
    valid_times = []
    
    for distance, time_str in times.items():
        seconds = parse_training_time(time_str)
        if seconds:
            valid_times.append((distance, seconds, time_str))
    
    if valid_times:
        # En hızlı süreyi bul (mesafeye göre normalize edilmiş)
        # 200m için ~12s, 400m için ~24s, 600m için ~38s ideal
        ideal_speeds = {
            '200m': 12.0,
            '400m': 24.0,
            '600m': 37.0,
            '800m': 50.0,
            '1000m': 63.0,
            '1200m': 77.0,
            '1400m': 91.0
        }
        
        speed_scores = []
        for distance, seconds, time_str in valid_times:
            ideal = ideal_speeds.get(distance)
            if ideal:
                # İdeal süreye yakınlık (düşük = iyi)
                ratio = seconds / ideal
                if ratio <= 1.0:
                    speed_score = 100  # İdealden hızlı
                elif ratio <= 1.05:
                    speed_score = 90
                elif ratio <= 1.10:
                    speed_score = 75
                elif ratio <= 1.15:
                    speed_score = 60
                else:
                    speed_score = 40
                speed_scores.append(speed_score)
        
        if speed_scores:
            avg_speed_score = sum(speed_scores) / len(speed_scores)
            score += (avg_speed_score - 50) * 0.5  # -25 ile +25 arası
            
        # En iyi süreyi kaydet (gösterim için)
        best_time_str = valid_times[0][2] if valid_times else None
        best_distance = valid_times[0][0] if valid_times else None
    
    # Skoru 0-100 arasında sınırla
    score = max(0, min(100, score))
    
    # Etiket belirle
    if score >= 80:
        label = "Çok İyi Form"
    elif score >= 65:
        label = "İyi Form"
    elif score >= 50:
        label = "Normal"
    elif score >= 35:
        label = "Orta"
    else:
        label = "Zayıf Form"
    
    return round(score, 1), label, days_since_training, best_time_str, best_distance

def project_training_to_race_distance(training_data, target_distance, avg_race_degree=None):
    """
    FAZ 2.2: İdman verisini yarış mesafesine oranlayarak tahmini yarış derecesi hesaplar.
    
    Returns: dict or None
    """
    if not training_data:
        return None
    
    times = training_data.get('times', {})
    if not times:
        return None
    
    try:
        if isinstance(target_distance, str):
            target_dist = int(target_distance.replace(' ', '').replace('m', ''))
        else:
            target_dist = int(target_distance)
    except:
        return None
    
    if target_dist <= 0:
        return None
    
    best_entry = None
    best_distance_num = 0
    
    for dist_str, time_str in times.items():
        seconds = parse_training_time(time_str)
        if seconds and seconds > 0:
            try:
                dist_num = int(dist_str.replace('m', ''))
                if dist_num > best_distance_num:
                    best_distance_num = dist_num
                    best_entry = (dist_str, seconds, dist_num)
            except:
                continue
    
    if not best_entry or best_distance_num <= 0:
        return None
    
    training_dist_str, training_seconds, training_dist_num = best_entry
    expansion_ratio = target_dist / training_dist_num
    projected_seconds = training_seconds * expansion_ratio
    projected_formatted = format_seconds_to_degree(projected_seconds)
    
    projection_label = "Projeksiyon"
    projection_diff = None
    
    if avg_race_degree and avg_race_degree > 0:
        projection_diff = round(projected_seconds - avg_race_degree, 2)
        tolerance = avg_race_degree * 0.03
        
        if projected_seconds < avg_race_degree - tolerance:
            projection_label = "İdman Hızlı ⚡"
        elif projected_seconds > avg_race_degree + tolerance:
            projection_label = "İdman Yavaş"
        else:
            projection_label = "İdman Uyumlu ✓"
    
    return {
        'projectedDegree': projected_formatted,
        'projectedDegreeSeconds': round(projected_seconds, 2),
        'projectedFromDistance': training_dist_str,
        'expansionRatio': round(expansion_ratio, 1),
        'projectionLabel': projection_label,
        'projectionDiff': projection_diff
    }


# ============== ADVANCED ANALYSIS FUNCTIONS ==============

def calculate_early_speed(races):
    """
    Roket Başlangıç (Early Speed) - İlk 400m performansı
    Son yarışlardaki sıralama ve dereceler üzerinden hesaplanır.
    Mantık: Eğer at genellikle ön sıralarda bitiriyorsa ve hızlı koşuyorsa, erken hızı yüksektir.
    """
    if not races:
        return 50.0, "Bilinmiyor"
    
    early_scores = []
    for i, race in enumerate(races[:5]):
        try:
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
            if rank > 0:
                # Düşük sıralama = yüksek puan (1. = 100, 10. = 10)
                base_score = max(0, 100 - (rank - 1) * 10)
                # Son yarışlara daha fazla ağırlık
                weight = 1.0 - (i * 0.15)
                early_scores.append(base_score * weight)
        except:
            continue
    
    if not early_scores:
        return 50.0, "Bilinmiyor"
    
    score = np.mean(early_scores)
    
    if score >= 80:
        label = "Roket"
    elif score >= 60:
        label = "Hızlı"
    elif score >= 40:
        label = "Orta"
    else:
        label = "Yavaş"
    
    return round(score, 1), label

def calculate_late_kick(races):
    """
    Son Düzlük Canavarı (Late Kick) - Son 400m sprint gücü
    Eğer at genellikle son sıralarda başlayıp ileriye doğru geliyorsa, late kick yüksektir.
    """
    if len(races) < 2:
        return 50.0, "Bilinmiyor"
    
    kick_scores = []
    for i, race in enumerate(races[:5]):
        try:
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
            if rank > 0:
                # Hızlı derece + iyi sıralama = yüksek kick
                seconds = calculate_seconds(race.get('degree', ''))
                distance = int(race.get('distance', '0').replace(' ', '')) if race.get('distance', '').replace(' ', '').isdigit() else 0
                
                if seconds and distance > 0:
                    speed = distance / seconds  # m/s
                    # Normalize hız (15-18 m/s aralığı için)
                    speed_score = min(100, (speed - 14) / 4 * 100)
                    
                    # Düşük sıralamayla birleşim
                    rank_bonus = max(0, (6 - rank) * 10) if rank <= 5 else 0
                    kick_scores.append((speed_score + rank_bonus) / 2)
        except:
            continue
    
    if not kick_scores:
        return 50.0, "Bilinmiyor"
    
    score = np.mean(kick_scores)
    
    if score >= 75:
        label = "Canavar"
    elif score >= 55:
        label = "Güçlü"
    elif score >= 35:
        label = "Normal"
    else:
        label = "Zayıf"
    
    return round(score, 1), label

def calculate_form_trend(races):
    """
    FAZ B.1: Form Skoru — Son yarışlardaki GERÇEK performansı ölçer.

    3 bileşen:
    1. Placement Score: Son yarışlarda field-size'a göre performans (ana sinyal, %60)
    2. Trend Score: İyileşme/kötüleşme eğilimi (trend, %25)
    3. Momentum Bonus: Üst üste iyi/kötü sonuçlar (%15)

    Çıktı aralığı: 0-100 (tam aralık)
    """
    if len(races) < 2:
        return 0.0, 50.0, "Stabil"

    # Son 6 yarıştan sıralama ve alan büyüklüğünü çıkar
    race_data = []
    for race in races[:6]:
        try:
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
            if rank > 0:
                # Alan büyüklüğü: field_size veya runners veya varsayılan 10
                field = int(race.get('field_size') or race.get('runners') or 10)
                field = max(field, rank)  # field en az rank kadar olmalı
                race_data.append({'rank': rank, 'field': field})
        except:
            continue

    if len(race_data) < 2:
        return 0.0, 50.0, "Stabil"

    ranks = [r['rank'] for r in race_data]

    # ── BİLEŞEN 1: Placement Score (%60) ──────────────────────
    # Her yarışta performans = (field - rank) / (field - 1) × 100
    # 1. bitiş = 100, sonuncu bitiş = 0
    # Son yarışlara daha fazla ağırlık ver
    recency_weights = [0.35, 0.25, 0.15, 0.10, 0.10, 0.05][:len(race_data)]
    w_total = sum(recency_weights)

    placement_score = 0.0
    for i, rd in enumerate(race_data):
        if rd['field'] <= 1:
            perf = 50.0
        else:
            perf = ((rd['field'] - rd['rank']) / (rd['field'] - 1)) * 100.0
        placement_score += perf * (recency_weights[i] / w_total)

    # ── BİLEŞEN 2: Trend Score (%25) ──────────────────────────
    # Linear regression: ranks azalıyorsa (iyileşme) pozitif
    y = np.array(ranks[::-1])  # eski -> yeni
    x = np.arange(len(y))
    trend_value = 0.0
    if len(x) >= 2:
        slope, _ = np.polyfit(x, y, 1)
        trend_value = -slope  # düşen sıralama = iyileşme = pozitif

    # Trend'i 0-100'e map'le: -3 → 0, 0 → 50, +3 → 100
    trend_score = 50 + (trend_value * 16.67)
    trend_score = max(0.0, min(100.0, trend_score))

    # ── BİLEŞEN 3: Momentum Bonus (%15) ──────────────────────
    momentum_score = 50.0
    if len(ranks) >= 3:
        last3 = ranks[:3]
        if last3[0] < last3[1] < last3[2]:
            momentum_score = 85.0  # üst üste iyileşme
        elif last3[0] > last3[1] > last3[2]:
            momentum_score = 15.0  # üst üste kötüleşme
        elif last3[0] <= 3:
            momentum_score = 75.0  # son 3 yarışta podyum
        elif last3[0] == 1:
            momentum_score = 90.0  # son yarışta galibiyet

    # Son yarış bonusu
    last_rank = ranks[0]
    if last_rank == 1:
        momentum_score = min(100, momentum_score + 20)
    elif last_rank == 2:
        momentum_score = min(100, momentum_score + 10)
    elif last_rank == 3:
        momentum_score = min(100, momentum_score + 5)

    # ── BİRLEŞTİR ────────────────────────────────────────────
    final_score = placement_score * 0.60 + trend_score * 0.25 + momentum_score * 0.15
    final_score = round(max(0.0, min(100.0, final_score)), 1)

    # Label
    if trend_value > 0.5:
        label = "Yukseliste"
    elif trend_value > 0.1:
        label = "Iyilesiyor"
    elif trend_value < -0.5:
        label = "Dususte"
    elif trend_value < -0.1:
        label = "Geriliyor"
    else:
        label = "Stabil"

    return round(trend_value, 2), final_score, label

def calculate_consistency(races):
    """
    İstikrar Puanı (Consistency) - At ne kadar güvenilir?
    Standart Sapma hesabı - Düşük sapma = yüksek istikrar
    """
    if len(races) < 2:
        return 5.0, "Bilinmiyor"
    
    ranks = []
    for race in races[:6]:
        try:
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
            if rank > 0:
                ranks.append(rank)
        except:
            continue
    
    if len(ranks) < 2:
        return 5.0, "Bilinmiyor"
    
    std_dev = np.std(ranks)
    
    # Düşük std = yüksek istikrar (0-10 arası puan)
    # std_dev: 0 = mükemmel, 5+ = çok istikrarsız
    consistency_score = max(0, 10 - std_dev)
    
    if consistency_score >= 8:
        label = "Çok Güvenilir"
    elif consistency_score >= 6:
        label = "Güvenilir"
    elif consistency_score >= 4:
        label = "Değişken"
    else:
        label = "Sürprizci"
    
    return round(consistency_score, 1), label

def calculate_track_suitability(races, target_track):
    """
    Pist Sevgi Puanı - Kum pistte mi Çim pistte mi daha iyi?
    """
    if not races or not target_track:
        return 50.0, "Bilinmiyor"
    
    target_track_lower = target_track.lower()
    matching_races = []
    other_races = []
    
    for race in races:
        track = race.get('track', '').lower()
        try:
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
            if rank > 0:
                if target_track_lower in track or track in target_track_lower:
                    matching_races.append(rank)
                else:
                    other_races.append(rank)
        except:
            continue
    
    if not matching_races:
        return 50.0, "Veri Yok"
    
    avg_match = np.mean(matching_races)
    avg_other = np.mean(other_races) if other_races else avg_match
    
    # Düşük ortalama sıralama = iyi
    # Hedef pistte ortalamayı diğerleriyle karşılaştır
    if avg_match <= avg_other:
        # Hedef pistte daha iyi
        improvement = (avg_other - avg_match) / max(avg_other, 1) * 100
        score = 50 + min(50, improvement)
    else:
        # Hedef pistte daha kötü
        decline = (avg_match - avg_other) / max(avg_match, 1) * 100
        score = 50 - min(50, decline)
    
    track_type = "Çim" if "çim" in target_track_lower else "Kum" if "kum" in target_track_lower else target_track
    
    if score >= 80:
        label = f"{track_type} Ustası"
    elif score >= 60:
        label = f"{track_type} Uyumlu"
    elif score >= 40:
        label = "Nötr"
    else:
        label = f"{track_type} Zorlanır"
    
    return round(score, 1), label


def _track_key(value):
    folded = _v4_fold_text(value)
    compact = folded.strip()
    if compact.startswith('K:') or compact == 'K':
        return 'kum'
    if compact.startswith('C:') or compact == 'C':
        return 'cim'
    if compact.startswith('S:') or compact == 'S':
        return 'sentetik'
    if compact in ('?IM', 'IM'):
        return 'cim'
    if 'SENTETIK' in folded:
        return 'sentetik'
    if 'KUM' in folded:
        return 'kum'
    if 'CIM' in folded:
        return 'cim'
    return ''


def _rank_average_score(ranks):
    if not ranks:
        return 50.0
    avg_rank = float(np.mean(ranks))
    return max(0.0, min(100.0, 100.0 - (avg_rank - 1.0) * 12.0))


def _confidence_blend(score, sample_size, full_sample=5):
    confidence = min(1.0, max(0.0, float(sample_size or 0) / float(full_sample)))
    return 50.0 + (float(score) - 50.0) * confidence


def _parse_rank_value(race):
    try:
        raw = str((race or {}).get('rank', '')).strip()
        digits = re.sub(r'[^0-9]', '', raw)
        value = int(digits) if digits else 0
        return value if value > 0 else None
    except (ValueError, TypeError):
        return None


def _surface_switch_penalty(source_key, target_key):
    if not source_key or not target_key or source_key == target_key:
        return 0.0
    pair = {source_key, target_key}
    if pair == {'kum', 'sentetik'}:
        return 2.0
    if pair == {'cim', 'sentetik'}:
        return 4.0
    if pair == {'cim', 'kum'}:
        return 7.0
    return 4.0


def calculate_surface_transition_score(races, target_track, track_suit_score=50.0):
    """Score how risky the current surface is versus recent racing surfaces."""
    target_key = _track_key(target_track)
    result = {
        'score': 50.0,
        'targetTrackRaceCount': 0,
        'otherTrackRaceCount': 0,
        'dominantTrack': None,
        'dominantTrackShare': 0.0,
        'lastTrack': None,
        'lastThreeTargetCount': 0,
        'reason': 'neutral',
    }
    if not races or not target_key:
        return result

    recent = []
    for race in (races or [])[:8]:
        key = _track_key(race.get('track', ''))
        if not key:
            continue
        recent.append({'track': key, 'rank': _parse_rank_value(race)})

    if not recent:
        return result

    counts = {}
    for item in recent:
        counts[item['track']] = counts.get(item['track'], 0) + 1
    dominant_track, dominant_count = max(counts.items(), key=lambda item: item[1])
    target_items = [item for item in recent if item['track'] == target_key]
    other_items = [item for item in recent if item['track'] != target_key]
    target_ranks = [item['rank'] for item in target_items if item['rank']]
    other_ranks = [item['rank'] for item in other_items if item['rank']]
    last_track = recent[0]['track']
    last_three = recent[:3]
    last_three_target_count = sum(1 for item in last_three if item['track'] == target_key)

    target_count = len(target_items)
    other_count = len(other_items)
    dominant_share = dominant_count / max(1, len(recent))
    result.update({
        'targetTrackRaceCount': target_count,
        'otherTrackRaceCount': other_count,
        'dominantTrack': dominant_track,
        'dominantTrackShare': round(dominant_share, 3),
        'lastTrack': last_track,
        'lastThreeTargetCount': last_three_target_count,
    })

    if target_count <= 0:
        if other_count >= 5:
            score = 34.0
            reason = 'strong_surface_switch_penalty'
        elif other_count >= 3:
            score = 40.0
            reason = 'surface_switch_penalty'
        else:
            score = 45.0
            reason = 'light_surface_switch_penalty'
        score -= _surface_switch_penalty(dominant_track, target_key)
        if dominant_share >= 0.75:
            score -= 4.0
        if len(last_three) >= 2 and last_three_target_count == 0:
            score -= 3.0
        result['score'] = round(max(20.0, min(88.0, score)), 1)
        result['reason'] = reason
        return result

    target_score = _rank_average_score(target_ranks) if target_ranks else float(track_suit_score or 50.0)
    score = _confidence_blend(target_score, target_count, full_sample=4)
    if target_ranks and other_ranks:
        target_avg = float(np.mean(target_ranks))
        other_avg = float(np.mean(other_ranks))
        if target_avg - other_avg >= 1.5:
            score -= min(18.0, (target_avg - other_avg) * 4.0)
        elif other_avg - target_avg >= 1.5:
            score += min(10.0, (other_avg - target_avg) * 3.0)

    if target_count == 1 and other_count >= 4 and last_three_target_count == 0:
        score = min(score, 58.0)
        score -= _surface_switch_penalty(last_track, target_key) * 0.5
        reason = 'thin_target_surface_history'
    elif last_track != target_key and last_three_target_count == 0:
        score -= _surface_switch_penalty(last_track, target_key) * 0.5
        reason = 'recent_surface_switch'
    elif target_count >= 3 and last_track == target_key:
        score += 4.0
        reason = 'target_surface_recent_and_proven'
    else:
        reason = 'target_surface_history'

    result['score'] = round(max(20.0, min(92.0, score)), 1)
    result['reason'] = reason
    return result


def calculate_track_suitability(races, target_track):
    """Data-backed track suitability score."""
    if not races or not target_track:
        return 50.0, "Bilinmiyor"

    target_key = _track_key(target_track)
    if not target_key:
        return 50.0, "Bilinmiyor"

    matching_races = []
    other_races = []
    for race in races:
        race_track_key = _track_key(race.get('track', ''))
        try:
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
            if rank > 0:
                if race_track_key == target_key:
                    matching_races.append(rank)
                elif race_track_key:
                    other_races.append(rank)
        except Exception:
            continue

    if not matching_races:
        return 50.0, "Veri Yok"

    avg_match = np.mean(matching_races)
    absolute_score = _rank_average_score(matching_races)
    if other_races:
        avg_other = np.mean(other_races)
        if avg_match <= avg_other:
            improvement = (avg_other - avg_match) / max(avg_other, 1) * 100
            relative_score = 50 + min(50, improvement)
        else:
            decline = (avg_match - avg_other) / max(avg_match, 1) * 100
            relative_score = 50 - min(50, decline)
        score = relative_score * 0.65 + absolute_score * 0.35
    else:
        score = absolute_score

    score = _confidence_blend(score, len(matching_races), full_sample=5)
    track_type = "Cim" if target_key == "cim" else "Kum" if target_key == "kum" else "Sentetik"
    if score >= 80:
        label = f"{track_type} Ustasi"
    elif score >= 60:
        label = f"{track_type} Uyumlu"
    elif score >= 40:
        label = "Notr"
    else:
        label = f"{track_type} Zorlanir"

    return round(score, 1), label


def calculate_track_experience_score(races, target_track, track_suit_score=50.0):
    """Penalize unproven surface switches without changing the legacy track score."""
    target_key = _track_key(target_track)
    if not races or not target_key:
        return 50.0, 0, 0, "neutral"

    target_count = 0
    other_count = 0
    for race in races:
        race_track_key = _track_key(race.get('track', ''))
        if not race_track_key:
            continue
        if race_track_key == target_key:
            target_count += 1
        else:
            other_count += 1

    if target_count > 0:
        return float(track_suit_score), target_count, other_count, "target_surface_history"
    if other_count >= 5:
        return 30.0, target_count, other_count, "strong_surface_switch_penalty"
    if other_count >= 3:
        return 35.0, target_count, other_count, "surface_switch_penalty"
    if other_count >= 1:
        return 42.0, target_count, other_count, "light_surface_switch_penalty"
    return 50.0, target_count, other_count, "neutral"


def calculate_distance_transition_score(races, target_distance, distance_suit_score=50.0):
    """Score how risky the current distance is versus recent racing distances."""
    target_dist = _parse_race_distance(target_distance)
    result = {
        'score': 50.0,
        'targetDistance': target_dist,
        'lastDistance': None,
        'avgRecentDistance': None,
        'distanceDelta': None,
        'similarDistanceRaceCount': 0,
        'reason': 'neutral',
    }
    if not races or not target_dist:
        return result

    recent = []
    for race in (races or [])[:8]:
        dist = _parse_race_distance(race.get('distance', ''))
        if not dist:
            continue
        recent.append({'distance': dist, 'rank': _parse_rank_value(race)})

    if not recent:
        return result

    distances = [item['distance'] for item in recent]
    last_distance = distances[0]
    avg_recent = float(np.mean(distances[:5]))
    last_delta = abs(last_distance - target_dist)
    avg_delta = abs(avg_recent - target_dist)
    similar_items = [item for item in recent if abs(item['distance'] - target_dist) <= 200]
    similar_ranks = [item['rank'] for item in similar_items if item['rank']]

    result.update({
        'lastDistance': last_distance,
        'avgRecentDistance': round(avg_recent, 1),
        'distanceDelta': int(last_delta),
        'similarDistanceRaceCount': len(similar_items),
    })

    if similar_items:
        similar_score = _rank_average_score(similar_ranks) if similar_ranks else float(distance_suit_score or 50.0)
        score = _confidence_blend(similar_score, len(similar_items), full_sample=4)
        if len(similar_items) == 1 and len(recent) >= 5:
            score = min(score, 62.0)
        if last_delta > 600:
            score -= 8.0
        elif last_delta > 400:
            score -= 4.0
        if len(similar_items) >= 2 and last_delta <= 400:
            score += 3.0
        reason = 'target_distance_history'
    else:
        score = 48.0
        if last_delta > 800:
            score -= 14.0
        elif last_delta > 600:
            score -= 11.0
        elif last_delta > 400:
            score -= 7.0
        elif last_delta > 200:
            score -= 4.0

        if avg_delta > 800:
            score -= 9.0
        elif avg_delta > 600:
            score -= 7.0
        elif avg_delta > 400:
            score -= 4.0

        if target_dist >= 1800 and avg_recent <= 1400:
            score -= 6.0
        elif target_dist <= 1400 and avg_recent >= 1900:
            score -= 5.0
        reason = 'distance_jump_penalty' if score < 40 else 'unproven_distance'

    result['score'] = round(max(22.0, min(90.0, score)), 1)
    result['reason'] = reason
    return result


def calculate_distance_suitability(races, target_distance):
    """
    Mesafe Uzmanlığı - Bu at 1200m (Sprint) atı mı, 2000m (Uzun) atı mı?
    """
    if not races or not target_distance:
        return 50.0, "Bilinmiyor"
    
    try:
        target_dist = int(target_distance.replace(' ', '').replace('m', ''))
    except:
        return 50.0, "Bilinmiyor"
    
    matching_races = []
    tolerance = 200  # ±200m tolerans
    
    for race in races:
        try:
            dist = int(race.get('distance', '0').replace(' ', ''))
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
            
            if rank > 0 and abs(dist - target_dist) <= tolerance:
                matching_races.append(rank)
        except:
            continue
    
    if not matching_races:
        return 50.0, "Veri Yok"
    
    avg_rank = np.mean(matching_races)
    
    # Düşük ortalama sıralama = iyi
    # 1. = 100, 5. = 50, 10. = 0
    score = max(0, 100 - (avg_rank - 1) * 12)
    
    if target_dist <= 1400:
        dist_type = "Sprint"
    elif target_dist <= 1800:
        dist_type = "Orta"
    else:
        dist_type = "Uzun"
    
    if score >= 75:
        label = f"{dist_type} Uzmanı"
    elif score >= 50:
        label = "Mesafe Uyumlu"
    elif score >= 25:
        label = "Mesafe Zor"
    else:
        label = "Mesafe Uyumsuz"
    
    return round(score, 1), label

def calculate_training_degree_score(training_projection, avg_race_degree):
    """
    Son idman projeksiyonunu ortalama yarış derecesiyle karşılaştırarak skor üretir.
    
    - Projeksiyon daha hızlıysa (düşük süre) → yüksek skor (70-100)
    - Projeksiyon uyumluysa (yakın süre) → orta skor (50-65)
    - Projeksiyon daha yavaşsa → düşük skor (20-45)
    - Veri yoksa → nötr (50)
    
    Returns: float (0-100)
    """
    if not training_projection or not avg_race_degree or avg_race_degree <= 0:
        return 50.0  # Nötr
    
    projected_seconds = training_projection.get('projectedDegreeSeconds')
    if not projected_seconds or projected_seconds <= 0:
        return 50.0
    
    # Fark hesapla: negatif = projeksiyon daha hızlı (iyi)
    diff = projected_seconds - avg_race_degree
    tolerance = avg_race_degree * 0.03  # %3 tolerans
    
    if diff < -tolerance:
        # İdman projeksiyonu yarış ortalamasından hızlı
        # Fark büyüdükçe skor artar (max 100)
        improvement_ratio = abs(diff) / avg_race_degree
        score = 70 + min(30, improvement_ratio * 500)  # 70-100 arası
    elif abs(diff) <= tolerance:
        # Uyumlu — yakın süreler
        closeness = 1 - (abs(diff) / tolerance)  # 0-1 arası
        score = 50 + closeness * 15  # 50-65 arası
    else:
        # İdman projeksiyonu yarış ortalamasından yavaş
        decline_ratio = diff / avg_race_degree
        score = max(20, 50 - decline_ratio * 300)  # 20-50 arası
    
    return round(max(0, min(100, score)), 1)


# ============== FAZ 4.2: SİKLET (KİLO) PERFORMANS ENDİKSİ ==============

def parse_carried_weight(value):
    """Parse TJK carried-weight values such as ``54`` or ``50+2.00Fazla Kilo``."""
    if not value:
        return None
    base_match = re.match(r'(\d+[,.]?\d*)', str(value).strip())
    if not base_match:
        return None
    weight = float(base_match.group(1).replace(',', '.'))
    allowance_match = re.search(r'\+(\d+[,.]?\d*)', str(value))
    if allowance_match:
        weight += float(allowance_match.group(1).replace(',', '.'))
    return weight


def calculate_handicap_efficiency_score(horse_hp, current_weight, all_hps, all_weights):
    """Measure whether carried weight is favorable relative to the horse's HP."""
    if horse_hp is None or current_weight is None or len(all_hps) < 3 or len(all_weights) < 3:
        return 50.0

    hp_min, hp_max = min(all_hps), max(all_hps)
    weight_min, weight_max = min(all_weights), max(all_weights)
    if hp_max <= hp_min or weight_max <= weight_min:
        return 50.0

    hp_percentile = (float(horse_hp) - hp_min) / (hp_max - hp_min)
    weight_percentile = (float(current_weight) - weight_min) / (weight_max - weight_min)
    residual = hp_percentile - weight_percentile
    return round(max(0.0, min(100.0, 50.0 + residual * 50.0)), 1)


def calculate_handicap_weight_relief_score(horse_hp, current_weight, all_hps, all_weights):
    """Score HP/kilo value relative to the current field.

    High HP with a manageable carried weight should be rewarded. A high-HP
    horse that is also top-weighted receives a smaller edge, and a light horse
    with useful HP can still earn value in handicap races.
    """
    if horse_hp is None or current_weight is None or len(all_hps) < 3 or len(all_weights) < 3:
        return 50.0

    hp_min, hp_max = min(all_hps), max(all_hps)
    weight_min, weight_max = min(all_weights), max(all_weights)
    if hp_max <= hp_min or weight_max <= weight_min:
        return 50.0

    hp_pct = (float(horse_hp) - hp_min) / (hp_max - hp_min)
    weight_pct = (float(current_weight) - weight_min) / (weight_max - weight_min)
    hp_vs_weight = hp_pct - weight_pct
    lightness_edge = 0.5 - weight_pct
    practical_value = (hp_pct * 0.65) + ((1.0 - weight_pct) * 0.35)
    score = 50.0 + hp_vs_weight * 34.0 + lightness_edge * 10.0 + (practical_value - 0.5) * 22.0
    return round(max(0.0, min(100.0, score)), 1)


def extract_handicap_level(race_type):
    """Return the numeric Handicap level from values such as ``Handikap 16/H2``."""
    folded = _v4_fold_text(race_type)
    match = re.search(r'(?:HANDIKAP|HNDIKAP)\s*[-/]?\s*(\d+)', folded)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def calculate_handicap_class_transition(races, current_race_type):
    """Describe current Handicap difficulty versus recent Handicap races.

    This is shadow-only. A positive delta means that the current Handicap
    number is higher than the recency-weighted historical average.
    """
    current_level = extract_handicap_level(current_race_type)
    if current_level is None:
        return {
            'hasHistory': False,
            'currentLevel': None,
            'historicalLevel': None,
            'delta': 0.0,
            'score': 50.0,
            'sampleCount': 0,
        }

    recent_levels = []
    for race in (races or [])[:8]:
        past_type = race.get('raceType', '') or race.get('group', '')
        past_level = extract_handicap_level(past_type)
        if past_level is not None:
            recent_levels.append(past_level)
        if len(recent_levels) >= 6:
            break

    if len(recent_levels) < 2:
        return {
            'hasHistory': False,
            'currentLevel': current_level,
            'historicalLevel': recent_levels[0] if recent_levels else None,
            'delta': 0.0,
            'score': 50.0,
            'sampleCount': len(recent_levels),
        }

    weights = list(range(len(recent_levels), 0, -1))
    historical_level = sum(level * weight for level, weight in zip(recent_levels, weights)) / sum(weights)
    delta = float(current_level) - historical_level
    score = max(10.0, min(90.0, 50.0 - delta * 8.0))
    return {
        'hasHistory': True,
        'currentLevel': current_level,
        'historicalLevel': round(historical_level, 2),
        'delta': round(delta, 2),
        'score': round(score, 1),
        'sampleCount': len(recent_levels),
    }


def calculate_weight_impact(current_weight_str, last_weight_str, target_distance):
    """
    FAZ 4.2: Kilo değişimini mesafe÷etkileşimi dâhilinde 0-100 skor üretir.
    
    Mantık:
    - Kilo düşen at hafifleşmiş = avantajlı (+bonus)
    - Kilo artan at ağırlaşmış = dezavantajlı (-ceza)
    - Mesafe uzadıkça kilo etkisi artar (sprint'te önemsiz, uzunda kritik)
    
    Mesafe çarpanı:
        1200m → 1.00x (baz)
        1600m → 1.17x
        2000m → 1.33x
        2400m → 1.50x
    
    Args:
        current_weight_str (str): Bugünkü kilo ("54+2.00Fazla Kilo" formatı olabilir)
        last_weight_str (str): Son yarıştaki kilo
        target_distance (str|int): Hedef mesafe (metre)
    
    Returns:
        float: Skor (0-100), nötr = 50
    """
    cw = parse_carried_weight(current_weight_str)
    lw = parse_carried_weight(last_weight_str)
    
    # Kilo bilgisi yoksa nötr
    if cw is None or lw is None:
        return 50.0
    
    kilo_diff = cw - lw  # Pozitif = arttı, Negatif = düştü
    
    # Mesafe çarpanı
    try:
        mesafe = int(str(target_distance).replace(' ', '').replace('m', ''))
    except:
        mesafe = 1600  # Varsayılan
    
    mesafe_carpani = 1.0 + max(0, (mesafe - 1200)) / 2400
    
    # Etkiyi hesapla
    if kilo_diff < 0:
        # Düşen kilo = avantaj (bonus)
        etki = abs(kilo_diff) * 3 * mesafe_carpani
        score = 50 + etki
    elif kilo_diff > 0:
        # Artan kilo = dezavantaj (ceza daha sert)
        etki = kilo_diff * 4 * mesafe_carpani
        score = 50 - etki
    else:
        score = 50  # Nötr
    
    return round(max(0, min(100, score)), 1)

# ============== FAZ 4.3: GELİŞMİŞ JOKEY ANALİZİ ==============

# ── Jokey adı normalizer (modül düzeyi — hem jockey_match için hem de PASS-1 filtresi için) ──
def normalize_jockey_name(name):
    """
    TJK'da jokey isimleri farklı formatlarda gelebiliyor:
      'H.Karataş'  /  'H. Karataş'  /  'Halis Karataş'  /  'H.KARATAŞ'
    Hepsini karşılaştırılabilir forma getirir:
      → büyük harf, Türkçe → Latin, nokta/boşluk → tek boşluk
    """
    if not name:
        return ""
    name = str(name).strip().upper()
    tr_map = {
        'İ': 'I', 'I': 'I', 'Ğ': 'G', 'Ü': 'U',
        'Ş': 'S', 'Ö': 'O', 'Ç': 'C',
        'ı': 'I', 'ğ': 'G', 'ü': 'U', 'ş': 'S', 'ö': 'O', 'ç': 'C',
    }
    for k, v in tr_map.items():
        name = name.replace(k, v)
    # Nokta ve birden fazla boşluğu tek boşluğa çevir
    name = re.sub(r'[.\s]+', ' ', name).strip()
    return name


def jockey_match(j1, j2):
    """
    İki jokey isminin aynı kişi olup olmadığını kontrol eder.
    Türkçe karakter ve format farklılıklarına karşı dirençli.
      'H.Karataş' == 'Halis Karataş' == 'H KARATAS'  → True
    """
    n1 = normalize_jockey_name(j1)
    n2 = normalize_jockey_name(j2)
    if not n1 or not n2:
        return False
    # 1. Birebir eşleşme
    if n1 == n2:
        return True
    parts1 = n1.split()
    parts2 = n2.split()
    if not parts1 or not parts2:
        return False
    surname1 = parts1[-1]
    surname2 = parts2[-1]
    # 2. Soyad eşleştirme (en az 4 karakter — 'KOC' gibi kısa soyadlarda yanlış match önleme)
    if len(surname1) >= 4 and surname1 == surname2:
        return True
    # 3. İlk harf kısaltması + soyad: 'H KARATAS' ~ 'HALIS KARATAS'
    if surname1 == surname2:
        if (len(parts1[0]) == 1) or (len(parts2[0]) == 1):
            return True
    return False


def calculate_jockey_score(jockey_stats, jockey_changed, training_jockey, race_jockey):
    """
    FAZ 4.3: Jokey-at uyumu, jokey değişimi ve idman jokeyi etkisini 0-100 skor üretir.
    
    Bileşenler:
    1. Jokey-At Uyum Skoru  → Bu jokeyle kaç yarış + kazanma oranı
    2. Jokey Değişim Etkisi → Yeni jokey mi? (nötr — gelecekte jokey genel istatistiği eklenecek)
    3. İdman Jokeyi Bonusu  → Aynı jokey idman yaptıysa +5
    
    Args:
        jockey_stats (dict|None): {'totalRaces': int, 'wins': int, 'winRate': float}
        jockey_changed (bool): Jokey son yarıştan farklı mı?
        training_jockey (str|None): İdman jokeyi adı
        race_jockey (str|None): Yarış jokeyi adı
    
    Returns:
        float: Skor (0-100), nötr = 50
    """
    score = 50.0
    
    # 1. Jokey-At Uyum Skoru
    if jockey_stats:
        total = jockey_stats.get('totalRaces', 0)
        wins = jockey_stats.get('wins', 0)
        
        if total > 0:
            win_rate = wins / total
            # Yarış sayısına göre güven çarpanı (5 yarış = tam güven)
            confidence = min(total / 5.0, 1.0)
            # Win rate 0.3 = mükemmel (30%+), 0.0 = kötü
            uyum_skoru = win_rate * 100 * confidence
            # uyum_skoru: 0-30 aralığı beklenir, 0-100'e normalize
            jockey_uyum = min(100, uyum_skoru * 2.5)
            # Merkeze çek: 50 + (uyum - 50) * 0.6 (30% katkı payı)
            score = 50 + (jockey_uyum - 50) * 0.5
    
    # 2. Jokey Değişimi (şimdilik nötr — Faz 4.7'de jokey genel istatistiğiyle geliştirilecek)
    if jockey_changed:
        score -= 3  # Küçük belirsizlik cezası
    
    # 3. İdman Jokeyi = Yarış Jokeyi Bonusu
    if training_jockey and race_jockey:
        tj = training_jockey.strip().upper()
        rj = race_jockey.strip().upper()
        # Kısmi eşleşme yeterli (soyad kontrolü)
        if tj and rj and (tj in rj or rj in tj or tj.split('.')[-1] == rj.split('.')[-1]):
            score += 5  # Ata alışık jokey bonusu
    
    return round(max(0, min(100, score)), 1)


# ============== FAZ 4.4: BOUNCE EFFECT (DİNLENME ANALİZİ) ==============

def calculate_bounce_score(races, race_date_str=None):
    """
    FAZ 4.4: Son yarıştan bu yana geçen günü ve yarış sıklığını analiz ederek
    atın dinlenme/kondisyon durumunu 0-100 skor üretir.
    
    İdeal dinlenme aralıkları:
    - 14-28 gün  → Mükemmel (100)
    - 10-13 gün  → İyi (85)
    - 29-42 gün  → Kabul Edilebilir (75)
    - 7-9 gün   → Riskli (60) — çok kısa
    - 43-60 gün  → Uzun ara (55) — form kaybı riski
    - 61+ gün   → Çok uzun (35)
    - 0-6 gün   → Çok kısa (40) — fiziksel yorgunluk
    
    Ek cezalar:
    - Son 30 günde 3+ yarış → -15
    - Son yarış 1. + rekor derece → -10 (bounce riski)
    - Hiç yarış yok → nötr 50
    
    Args:
        races (list): Geçmiş yarış listesi (en yeni first). 'date' alanı 'dd.MM.yyyy' formatı
        race_date_str (str|None): Koşu tarihi 'dd.MM.yyyy' (yoksa bugün kullanılır)
    
    Returns:
        float: Skor (0-100), nötr = 50
    """
    from datetime import datetime
    
    if not races:
        return 50.0  # Hiç yarış yok → nötr
    
    # Referans tarih
    try:
        if race_date_str:
            ref_date = datetime.strptime(race_date_str, '%d.%m.%Y')
        else:
            ref_date = datetime.now()
    except:
        ref_date = datetime.now()
    
    # Son yarış tarihi
    last_race_date = None
    for race in races:
        date_str = race.get('date', '')
        if date_str:
            try:
                last_race_date = datetime.strptime(date_str.strip(), '%d.%m.%Y')
                break  # En son yarış (liste en yeni → en eski sıralı)
            except:
                continue
    
    if last_race_date is None:
        return 50.0  # Tarih parse edilemedi → nötr
    
    gun_farki = (ref_date - last_race_date).days
    gun_farki = max(0, gun_farki)
    
    # Dinlenme süresi skoru
    if 14 <= gun_farki <= 28:
        base_score = 100
    elif 10 <= gun_farki <= 13:
        base_score = 85
    elif 29 <= gun_farki <= 42:
        base_score = 75
    elif 7 <= gun_farki <= 9:
        base_score = 60
    elif 43 <= gun_farki <= 60:
        base_score = 55
    elif gun_farki > 60:
        base_score = 35
    elif gun_farki <= 6:
        base_score = 40
    else:
        base_score = 50
    
    penalty = 0
    
    # Bounce Effect: Son yarışı 1. ve olağanüstü hızlıysa → pil bitme riski
    try:
        last_rank = str(races[0].get('rank', '')).strip()
        if last_rank == '1':
            # Son yarış derecesi genel ortalamasından %3'ten fazla hızlıysa
            last_deg = races[0].get('adjustedDegreeInSeconds') or races[0].get('degreeInSeconds')
            all_degs = [
                r.get('adjustedDegreeInSeconds') or r.get('degreeInSeconds')
                for r in races[1:6]
                if r.get('adjustedDegreeInSeconds') or r.get('degreeInSeconds')
            ]
            if last_deg and all_degs:
                avg_deg = sum(all_degs) / len(all_degs)
                if last_deg < avg_deg * 0.97:  # %3+ daha hızlı
                    penalty -= 10  # Bounce riski
    except:
        pass
    
    # Aşırı koşma cezası: Son 30 günde 3+ yarış
    try:
        from datetime import timedelta
        thirty_days_ago = ref_date - timedelta(days=30)
        recent_race_count = 0
        for race in races:
            date_str = race.get('date', '')
            if date_str:
                try:
                    rd = datetime.strptime(date_str.strip(), '%d.%m.%Y')
                    if rd >= thirty_days_ago:
                        recent_race_count += 1
                except:
                    pass
        if recent_race_count >= 3:
            penalty -= 15
    except:
        pass
    
    score = base_score + penalty
    return round(max(0, min(100, score)), 1)


# ============== FAZ 4.5: KOŞU TEMPOSU SENARYOSU (PACE SIMULATION) ==============

def determine_running_style(races):
    """
    FAZ 4.5: Atın koşu stilini geçmiş yarışlardan belirler.
    
    Mantık: Son 5 yarışın sıralamalarına bakarak atın
    genel pozisyon eğilimini çıkarır.
    
    Early Speed Score (ESS):
    - Son 5 yarışta genelde 1-3. bitiriyorsa → KAÇAK  (ESS > 70)
    - Orta sıralarda tutuyorsa            → TAKİPÇİ (40 < ESS <= 70)
    - Genel olarak geride kalıyorsa       → BEKLEME  (ESS <= 40)
    
    Args:
        races (list): Geçmiş yarış listesi (en yeni first)
    
    Returns:
        str: 'KAÇAK', 'TAKİPÇİ', veya 'BEKLEME'
        float: ESS skoru (0-100)
    """
    if not races:
        return 'TAKİPÇİ', 50.0  # Veri yoksa nötr kabul
    
    scores = []
    for i, race in enumerate(races[:5]):
        try:
            rank_str = re.sub(r'[^0-9]', '', str(race.get('rank', '0')))
            rank = int(rank_str) if rank_str else 0
            if rank > 0:
                # Düşük sıralama (1.=en iyi) → yüksek ESS
                # 1.→100, 2.→87, 3.→73, 4.→60, 5.→47, 6+→max(0, 47-(rank-5)*13)
                if rank == 1:
                    base = 100
                elif rank == 2:
                    base = 87
                elif rank == 3:
                    base = 73
                elif rank == 4:
                    base = 60
                elif rank == 5:
                    base = 47
                else:
                    base = max(0, 47 - (rank - 5) * 13)
                
                # Son yarışlara daha fazla ağırlık (azalan)
                weight = 1.0 - (i * 0.12)
                scores.append(base * max(0.4, weight))
        except:
            continue
    
    if not scores:
        return 'TAKİPÇİ', 50.0
    
    ess = sum(scores) / len(scores)
    ess = round(min(100, max(0, ess)), 1)
    
    if ess > 70:
        return 'KAÇAK', ess
    elif ess > 40:
        return 'TAKİPÇİ', ess
    else:
        return 'BEKLEME', ess


def calculate_pace_scenario(horse_styles):
    """
    FAZ 4.5: Yarıştaki tüm atların koşu stillerine göre
    yarışın tempo profilini belirler.
    
    Args:
        horse_styles (list): [{'name': str, 'style': 'KAÇAK'|'TAKİPÇİ'|'BEKLEME'}, ...]
    
    Returns:
        str: 'HIZLI', 'NORMAL', 'YAVAŞ', 'ÇOK_YAVAŞ'
        int: kaçak_sayısı
    """
    kacak_sayisi = sum(1 for h in horse_styles if h.get('style') == 'KAÇAK')
    
    if kacak_sayisi >= 3:
        return 'HIZLI', kacak_sayisi       # Çok fazla kaçak → sert tempo
    elif kacak_sayisi == 2:
        return 'NORMAL', kacak_sayisi      # İki kaçak → dengeli tempo
    elif kacak_sayisi == 1:
        return 'YAVAŞ', kacak_sayisi       # Tek kaçak → o tempoyu kontrol eder
    else:
        return 'ÇOK_YAVAŞ', kacak_sayisi  # Kimse çekmiyor → çok yavaş


def calculate_pace_score(horse_style, pace_scenario):
    """
    FAZ 4.5: Atın koşu stili ile yarış temposu senaryosunun uyumuna göre
    0-100 skor üretir.
    
    Kural tablosu:
    ┌─────────────┬──────────────────────────────────────────────────────┐
    │ Tempo       │ KAÇAK      │ TAKİPÇİ   │ BEKLEME                   │
    ├─────────────┼────────────┼───────────┼───────────────────────────┤
    │ HIZLI       │ -10 (yıpr) │  0 (nötr) │ +15 (gelecek)             │
    │ NORMAL      │  0 (nötr)  │  0 (nötr) │  0 (nötr)                 │
    │ YAVAŞ       │ +15 (krng) │  0 (nötr) │ -5 (geç kalır)            │
    │ ÇOK_YAVAŞ  │ +10 (çek)  │ +5        │ -10 (çok geç)             │
    └─────────────┴────────────┴───────────┴───────────────────────────┘
    
    Args:
        horse_style (str): 'KAÇAK', 'TAKİPÇİ', 'BEKLEME'
        pace_scenario (str): 'HIZLI', 'NORMAL', 'YAVAŞ', 'ÇOK_YAVAŞ'
    
    Returns:
        float: Skor (0-100), nötr = 50
    """
    adjustment = 0
    
    if pace_scenario == 'HIZLI':
        if horse_style == 'KAÇAK':
            adjustment = -10  # Çok sert tempo: kaçaklar yorulur
        elif horse_style == 'BEKLEME':
            adjustment = +15  # Sert tempoda bekleme atı kazanır
        # TAKİPÇİ: nötr
        
    elif pace_scenario == 'YAVAŞ':
        if horse_style == 'KAÇAK':
            adjustment = +15  # Tek kaçak tempoyu kontrol eder
        elif horse_style == 'BEKLEME':
            adjustment = -5   # Geride kalırsa yetişemez
        
    elif pace_scenario == 'ÇOK_YAVAŞ':
        if horse_style == 'KAÇAK':
            adjustment = +10  # Öne geçip tutar
        elif horse_style == 'TAKİPÇİ':
            adjustment = +5   # Öne çıkma şansı
        elif horse_style == 'BEKLEME':
            adjustment = -10  # Çok geride kalır, son düzlük yetmez
    
    # NORMAL tempo → herkese nötr (adjustment = 0)
    
    score = 50 + adjustment
    return round(max(0, min(100, score)), 1)


# ══════════════════════════════════════════════════════════════════
# FAZ 6.2: ANTRENÖR WIN-RATE KATMANI (K13)
# ══════════════════════════════════════════════════════════════════

def calculate_pace_map_edge_score(horse_style, pace_scenario, pace_score=50.0, pace_pressure=0.0):
    """Return a field-context pace edge score."""
    try:
        base = float(pace_score)
    except (TypeError, ValueError):
        base = 50.0
    try:
        pressure = float(pace_pressure)
    except (TypeError, ValueError):
        pressure = 0.0

    style = str(horse_style or '').upper()
    scenario = str(pace_scenario or '').upper()
    bonus = 0.0
    if scenario in {'HIZLI', 'FAST'}:
        if 'BEKLEME' in style:
            bonus += min(12.0, pressure * 0.18)
        elif 'KA' in style:
            bonus -= min(10.0, pressure * 0.14)
    elif scenario in {'YAVAS', 'YAVAŞ', 'SLOW'}:
        if 'KA' in style:
            bonus += 12.0
        elif 'BEKLEME' in style:
            bonus -= 6.0
    elif 'COK' in scenario or 'ÇOK' in scenario:
        if 'TAK' in style:
            bonus += 6.0
        elif 'BEKLEME' in style:
            bonus -= 9.0
    return round(max(0.0, min(100.0, base + bonus)), 1)


def calculate_surface_switch_safety_score(surface_transition, track_experience_score=50.0):
    """Convert surface transition diagnostics into a positive safety score."""
    if not isinstance(surface_transition, dict):
        return 50.0
    try:
        base = float(surface_transition.get('score', 50.0))
    except (TypeError, ValueError):
        base = 50.0
    try:
        experience = float(track_experience_score)
    except (TypeError, ValueError):
        experience = 50.0

    target_count = int(surface_transition.get('targetTrackRaceCount') or 0)
    other_count = int(surface_transition.get('otherTrackRaceCount') or 0)
    last_three_target = int(surface_transition.get('lastThreeTargetCount') or 0)
    dominant_share = float(surface_transition.get('dominantTrackShare') or 0.0)
    last_track = _v4_track_bucket(surface_transition.get('lastTrack') or '')
    dominant_track = _v4_track_bucket(surface_transition.get('dominantTrack') or '')
    has_switch = bool(last_track and dominant_track and last_track != dominant_track)

    score = base * 0.65 + experience * 0.35
    if target_count <= 0 and other_count >= 3:
        score -= 18.0
    elif target_count <= 1 and other_count >= 4:
        score -= 10.0
    if has_switch and target_count <= 1:
        score -= 8.0
    if last_three_target >= 2:
        score += 8.0
    if target_count >= 3:
        score += 5.0
    if dominant_share >= 0.75 and target_count == 0:
        score -= 7.0
    return round(max(0.0, min(100.0, score)), 1)


def _race_class_level(race_type):
    folded = _v4_fold_text(race_type)
    if re.search(r'\bG\s*[-/]?\s*1\b', folded) or 'GRUP 1' in folded or 'GROUP 1' in folded:
        return 5.0
    if re.search(r'\bG\s*[-/]?\s*2\b', folded) or 'GRUP 2' in folded or 'GROUP 2' in folded:
        return 4.4
    if re.search(r'\bG\s*[-/]?\s*3\b', folded) or 'GRUP 3' in folded or 'GROUP 3' in folded:
        return 3.8
    if 'GRUP' in folded or 'GROUP' in folded:
        return 3.4
    if 'LISTED' in folded or 'ACIK' in folded or 'AÇIK' in folded:
        return 3.1
    if 'KV' in folded:
        return 2.4
    if 'SART' in folded:
        return 1.5
    if 'HANDIKAP' in folded or 'HNDIKAP' in folded:
        return 1.4
    if 'MAIDEN' in folded:
        return 1.0
    return 1.2


def calculate_class_peak_score(races, current_race_type):
    """Measure proven class ceiling for elite races without using AGF."""
    current_level = _race_class_level(current_race_type)
    samples = []
    peak_level = None
    for race in (races or [])[:12]:
        past_level = _race_class_level(race.get('raceType', '') or race.get('group', ''))
        peak_level = past_level if peak_level is None else max(peak_level, past_level)
        rank = _to_int(race.get('rank'))
        if rank <= 0:
            continue
        class_fit = max(0.0, min(1.0, past_level / max(current_level, 1.0)))
        if rank == 1:
            finish_score = 1.0
        elif rank == 2:
            finish_score = 0.82
        elif rank == 3:
            finish_score = 0.68
        elif rank <= 5:
            finish_score = 0.48
        else:
            finish_score = 0.25
        recency = max(0.45, 1.0 - len(samples) * 0.06)
        samples.append((45.0 + class_fit * 35.0 + finish_score * 20.0) * recency)
    if not samples:
        return {
            'score': 50.0,
            'hasHistory': False,
            'sampleCount': 0,
            'currentLevel': current_level,
            'peakLevel': peak_level,
        }
    score = max(samples) * 0.70 + (sum(samples) / len(samples)) * 0.30
    return {
        'score': round(max(0.0, min(100.0, score)), 1),
        'hasHistory': True,
        'sampleCount': len(samples),
        'currentLevel': current_level,
        'peakLevel': peak_level,
    }


def calculate_elite_consensus_score(metrics, class_peak_score=50.0):
    """AGF-free consensus for group-class races."""
    if not metrics:
        return 50.0
    pairs = [
        ('hp_score', 0.18),
        ('form_trend', 0.16),
        ('trainer_score', 0.14),
        ('track_experience_score', 0.13),
        ('training_degree_score', 0.12),
        ('surface_transition_score', 0.10),
        ('distance_suit', 0.08),
        ('degree_stability', 0.05),
    ]
    total = 0.0
    weight = 0.0
    for key, w in pairs:
        try:
            value = float(metrics.get(key, 50.0))
        except (TypeError, ValueError):
            value = 50.0
        total += value * w
        weight += w
    try:
        class_value = float(class_peak_score)
    except (TypeError, ValueError):
        class_value = 50.0
    total += class_value * 0.18
    weight += 0.18
    return round(max(0.0, min(100.0, total / max(weight, 0.01))), 1)


def _relative_score(value, values, spread_floor=4.0):
    try:
        clean_values = [float(v) for v in values if v is not None]
        current = float(value)
    except (TypeError, ValueError):
        return 50.0
    if len(clean_values) < 3:
        return 50.0
    spread = float(np.std(clean_values))
    if spread < spread_floor:
        return 50.0
    mean = float(np.mean(clean_values))
    z = (current - mean) / spread
    return round(max(0.0, min(100.0, 50.0 + z * 14.0)), 1)


def apply_v421_contextual_metrics(analyzed_horses, race_type=''):
    """Attach v4.21 contextual scores that need the full race field."""
    if not analyzed_horses:
        return
    profile = extract_v4_race_profile(race_type, '', '', len(analyzed_horses))
    is_handikap = _v417_is_handikap_profile(profile)
    is_grup = profile.get('category') == 'GRUP'
    field_metric_keys = [
        'form_trend', 'hp_score', 'degree_avg', 'distance_suit',
        'surface_transition_score', 'weight_impact',
        'handicap_efficiency_score', 'pace_score',
    ]
    field_values = {
        key: [
            (horse.get('_mf') or {}).get(key)
            for horse in analyzed_horses
            if horse.get('_mf')
        ]
        for key in field_metric_keys
    }

    for horse in analyzed_horses:
        metrics = horse.get('_mf') or {}
        if not metrics:
            continue
        flags = horse.get('metricSourceFlags') or {}
        breakdown = horse.get('scoreBreakdown') or {}

        if is_handikap:
            relative_components = [
                _relative_score(metrics.get(key), field_values.get(key, []))
                for key in field_metric_keys
            ]
            actionable = [score for score in relative_components if abs(score - 50.0) >= 0.5]
            field_relative = round(sum(actionable) / len(actionable), 1) if actionable else 50.0
            metrics['field_relative_value_score'] = field_relative
            metrics['_has_field_relative_value'] = bool(actionable)
            flags['hasFieldRelativeValue'] = bool(actionable)
            breakdown['fieldRelativeValueScore'] = field_relative

            pace_edge = calculate_pace_map_edge_score(
                (horse.get('paceInfo') or {}).get('runningStyle'),
                (horse.get('paceInfo') or {}).get('paceScenario'),
                metrics.get('pace_score', 50.0),
                metrics.get('pace_pressure', 0.0),
            )
            metrics['pace_map_edge_score'] = pace_edge
            metrics['_has_pace_map_edge'] = True
            flags['hasPaceMapEdge'] = True
            breakdown['paceMapEdgeScore'] = pace_edge

            surface_safety = calculate_surface_switch_safety_score(
                {
                    'score': metrics.get('surface_transition_score', 50.0),
                    'targetTrackRaceCount': flags.get('targetTrackRaceCount', 0),
                    'otherTrackRaceCount': flags.get('otherTrackRaceCount', 0),
                    'dominantTrack': flags.get('surfaceTransitionDominantTrack'),
                    'dominantTrackShare': flags.get('surfaceTransitionDominantShare', 0.0),
                    'lastTrack': flags.get('surfaceTransitionLastTrack'),
                    'lastThreeTargetCount': flags.get('surfaceTransitionLastThreeTargetCount', 0),
                },
                metrics.get('track_experience_score', 50.0),
            )
            metrics['surface_switch_safety_score'] = surface_safety
            metrics['_has_surface_switch_safety'] = flags.get('hasSurfaceTransition', False)
            flags['hasSurfaceSwitchSafety'] = flags.get('hasSurfaceTransition', False)
            breakdown['surfaceSwitchSafetyScore'] = surface_safety

            visibility = (
                float(metrics.get('degree_avg', 50.0)) * 0.25
                + float(metrics.get('form_trend', 50.0)) * 0.20
                + float(metrics.get('hp_score', 50.0)) * 0.20
                + float(metrics.get('jockey_score', 50.0)) * 0.15
                + float(metrics.get('agf_score', 50.0)) * 0.20
            )
            real_edge = (
                field_relative * 0.26
                + float(metrics.get('handicap_weight_relief_score', metrics.get('handicap_efficiency_score', 50.0))) * 0.24
                + pace_edge * 0.20
                + surface_safety * 0.18
                + float(metrics.get('distance_transition_score', 50.0)) * 0.12
            )
            risk_gap = max(0.0, visibility - real_edge)
            favorite_guard = round(max(0.0, min(100.0, 70.0 - risk_gap * 0.65 + max(0.0, real_edge - 55.0) * 0.20)), 1)
            metrics['favorite_risk_guard_score'] = favorite_guard
            metrics['_has_favorite_risk_guard'] = True
            flags['hasFavoriteRiskGuard'] = True
            breakdown['favoriteRiskGuardScore'] = favorite_guard
        else:
            for key in [
                'field_relative_value_score', 'pace_map_edge_score',
                'surface_switch_safety_score', 'favorite_risk_guard_score',
            ]:
                metrics.setdefault(key, 50.0)

        if is_grup:
            class_peak = calculate_class_peak_score(metrics.get('_horse_races', []), race_type)
            metrics['class_peak_score'] = class_peak['score']
            metrics['elite_consensus_score'] = calculate_elite_consensus_score(metrics, class_peak['score'])
            metrics['_has_class_peak'] = class_peak['hasHistory']
            metrics['_has_elite_consensus'] = True
            flags['hasClassPeak'] = class_peak['hasHistory']
            flags['classPeakSampleCount'] = class_peak['sampleCount']
            flags['classPeakCurrentLevel'] = class_peak['currentLevel']
            flags['classPeakLevel'] = class_peak['peakLevel']
            flags['hasEliteConsensus'] = True
            breakdown['classPeakScore'] = class_peak['score']
            breakdown['eliteConsensusScore'] = metrics['elite_consensus_score']
        else:
            metrics.setdefault('class_peak_score', 50.0)
            metrics.setdefault('elite_consensus_score', 50.0)

        horse['metricSourceFlags'] = flags
        horse['scoreBreakdown'] = breakdown


_trainer_cache = {}  # { 'ANTRENOR_ADI_UPPER': { ...stats... } }
_trainer_id_cache = {}
_trainer_id_aliases = {
    'H.DOLMAZ': [{'id': 3041, 'name': 'HASAN DOLMAZ'}],
    'R.KARATAS': [{'id': 2376, 'name': 'REŞİT KARATAŞ'}],
    'S.C.GOZUNGU': [{'id': 3085, 'name': 'SEZGİN CAN GÖZÜNGÜ'}],
    'SER.YILDIZ': [{'id': 3039, 'name': 'SERHAT YILDIZ'}],
}


def _to_int(value):
    try:
        return int(re.sub(r'[^0-9]', '', str(value or '')) or 0)
    except (ValueError, TypeError):
        return 0


def _trainer_abbrev_parts(trainer_name):
    folded = _v4_fold_text(trainer_name).replace('.', ' ')
    parts = [p for p in re.split(r'\s+', folded) if p]
    if len(parts) >= 2:
        return _trainer_hint(parts[0]), _trainer_hint(parts[-1])
    return '', _trainer_hint(parts[0]) if parts else ''


def _trainer_hint(value):
    text = re.split(r'[^A-Z]+', _v4_fold_text(value))[0]
    return text.strip()


def _trainer_native_search_parts(trainer_name):
    """Return TJK autocomplete terms without stripping Turkish characters."""
    text = str(trainer_name or '').strip().upper()
    if not text:
        return '', ''
    text = text.replace('Ţ', 'Ş').replace('ţ', 'Ş')
    text = re.sub(r'\b(AP|APRANTI|KG|DB|SK|GKR)\b', ' ', text)
    parts = [
        p for p in re.split(r'[^0-9A-ZÇĞİÖŞÜÂÎÛ]+', text)
        if p
    ]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return '', parts[0] if parts else ''


def _resolve_trainer_ids(trainer_name):
    trainer_key = _v4_fold_text(trainer_name).strip()
    if not trainer_key:
        return []
    if trainer_key in _trainer_id_cache:
        return _trainer_id_cache[trainer_key]
    if trainer_key in _trainer_id_aliases:
        _trainer_id_cache[trainer_key] = _trainer_id_aliases[trainer_key]
        return _trainer_id_cache[trainer_key]

    first_hint, surname_hint = _trainer_abbrev_parts(trainer_name)
    _, native_surname_hint = _trainer_native_search_parts(trainer_name)
    search_terms = []
    if native_surname_hint:
        search_terms.append(native_surname_hint)
    native_key = str(trainer_name or '').strip().upper()
    if native_key:
        search_terms.append(native_key)
    if surname_hint:
        search_terms.append(surname_hint)
    if trainer_key:
        search_terms.append(trainer_key)
    search_terms = list(dict.fromkeys([term for term in search_terms if term]))

    matches = []
    seen = set()
    for term in search_terms:
        try:
            for page in range(1, 6):
                response = requests.get(
                    "https://www.tjk.org/TR/YarisSever/Query/ParameterQuery",
                    params={
                        'parameterName': 'AntronorId',
                        'filter': term,
                        'page': page,
                        'parentParameterName': '',
                        'parentParameterValue': '',
                    },
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "X-Requested-With": "XMLHttpRequest",
                    },
                    timeout=10,
                )
                if response.status_code != 200:
                    continue
                payload = response.json()
                for entity in payload.get('entities', []):
                    entity_id = entity.get('id') or entity.get('Id')
                    entity_text = entity.get('text') or entity.get('Name') or ''
                    folded_text = _v4_fold_text(entity_text)
                    if not entity_id or entity_id in seen:
                        continue
                    text_parts = [p for p in re.split(r'\s+', folded_text) if p]
                    last_name = _trainer_hint(text_parts[-1]) if text_parts else ''
                    if not surname_hint:
                        surname_ok = True
                    elif len(surname_hint) <= 4:
                        surname_ok = last_name == surname_hint
                    else:
                        surname_ok = (
                            last_name == surname_hint
                            or last_name.startswith(surname_hint)
                            or last_name.endswith(surname_hint)
                        )
                    first_ok = not first_hint or any(_trainer_hint(p).startswith(first_hint) for p in text_parts[:-1])
                    exact_ok = trainer_key and trainer_key.replace(' ', '') in folded_text.replace(' ', '')
                    if (surname_ok and first_ok) or exact_ok:
                        matches.append({'id': entity_id, 'name': entity_text})
                        seen.add(entity_id)
                if payload.get('totalCount', 0) <= page * 20:
                    break
        except Exception:
            continue
        if matches:
            break

    unique_names = {}
    for match in matches:
        unique_names.setdefault(_v4_fold_text(match.get('name', '')).strip(), []).append(match)
    if matches:
        # TJK daily program sometimes gives ambiguous abbreviated trainer names
        # such as "A.ATAS". Do not guess a single person; aggregate matching
        # candidates and mark quality as AMBIGUOUS in fetch_trainer_stats().
        _trainer_id_cache[trainer_key] = matches[:5]
        return _trainer_id_cache[trainer_key]
    return []


def fetch_trainer_stats(trainer_name):
    """
    FAZ 6.2: TJK KosuSorgulama sayfasından antrenörün son 2 yılın
    galibiyet istatistiklerini çeker.

    Returns:
        dict: {
            'trainer_name', 'total_races', 'total_wins',
            'win_rate', 'data_quality'
        }
    """
    global _trainer_cache

    if not trainer_name or not trainer_name.strip():
        return None

    trainer_key = trainer_name.strip().upper()

    if trainer_key in _trainer_cache:
        print(f"[TRAINER CACHE] {trainer_key} önbellekten alındı")
        return _trainer_cache[trainer_key]

    print(f"[TRAINER] {trainer_name} için istatistikler çekiliyor...")

    try:
        base_url = "https://www.tjk.org/TR/YarisSever/Query/Page/KosuSorgulama"
        params = {
            'QueryParameter_AntrenorIsmi': trainer_name.strip(),
            'QueryParameter_Tarih_Start': '01.01.2024',
            'QueryParameter_Tarih_End':   '31.12.2025',
            'QueryParameter_SehirId':     '-1',
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9",
            "Referer": "https://www.tjk.org/TR/YarisSever/Query/Page/KosuSorgulama"
        }

        response = requests.get(base_url, params=params, headers=headers, timeout=12)
        if response.status_code != 200:
            print(f"[TRAINER] TJK hatası: {response.status_code}")
            _trainer_cache[trainer_key] = None
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        table = soup.find('table', id='queryTable')
        tbody = None
        if table:
            tbody = table.find('tbody', id='tbody0') or table.find('tbody')
        if not tbody:
            tbody = soup.find('tbody', id='tbody0')

        if not tbody:
            print(f"[TRAINER] {trainer_name} için tablo bulunamadı — veri yok")
            result = {
                'trainer_name': trainer_name,
                'total_races': 0,
                'total_wins':  0,
                'win_rate':    0.0,
                'data_quality': 'NONE'
            }
            _trainer_cache[trainer_key] = result
            return result

        rows = tbody.find_all('tr')
        total_races = 0
        total_wins  = 0

        for row in rows:
            if 'hidable' in row.get('class', []):
                continue
            cells = row.find_all('td')
            if len(cells) < 5:
                continue

            try:
                total_races += 1
                # Sıralama sütunu — index değişkenlik gösterebilir.
                # Yarış sonuçlarında genellikle cells[4] veya cells[5] bitiriş poz.
                # Birden fazla sütunu dene: "1" veya "1." ifadesi = galibiyet
                for col_idx in [4, 5, 3]:
                    pos_txt = cells[col_idx].text.strip() if len(cells) > col_idx else ''
                    pos_clean = pos_txt.replace('.', '').strip()
                    if pos_clean == '1':
                        total_wins += 1
                        break
            except Exception:
                continue

        data_quality = 'NONE' if total_races == 0 else ('LOW' if total_races < 15 else 'HIGH')
        win_rate = round(total_wins / total_races, 3) if total_races else 0.0

        result = {
            'trainer_name': trainer_name,
            'total_races':  total_races,
            'total_wins':   total_wins,
            'win_rate':     win_rate,
            'data_quality': data_quality,
        }
        _trainer_cache[trainer_key] = result
        print(f"[TRAINER] {trainer_name}: {total_races} yarış, {total_wins} galibiyet, oran={win_rate:.1%}")
        return result

    except Exception as e:
        print(f"[TRAINER] Hata ({trainer_name}): {e}")
        _trainer_cache[trainer_key] = None
        return None


def fetch_trainer_stats(trainer_name):
    """
    Fetch trainer yearly stats from TJK AntrenorIstatistikleri.
    Daily programs often use abbreviated names, so we resolve AntronorId first.
    """
    global _trainer_cache

    trainer_key = _v4_fold_text(trainer_name).strip()
    if not trainer_key:
        return None
    if trainer_key in _trainer_cache:
        print(f"[TRAINER CACHE] {trainer_key} onbellekten alindi")
        return _trainer_cache[trainer_key]

    trainer_ids = _resolve_trainer_ids(trainer_name)
    if not trainer_ids:
        return {
            'trainer_name': trainer_name,
            'resolved_name': None,
            'total_races': 0,
            'total_wins': 0,
            'win_rate': 0.0,
            'place_rate': 0.0,
            'data_quality': 'NONE',
        }
    ambiguous_trainer = len({
        _v4_fold_text(item.get('name', '')).strip()
        for item in trainer_ids
        if item.get('name')
    }) > 1

    current_year = time.localtime().tm_year
    year_min = current_year - 2
    total_races = 0
    total_wins = 0
    total_places = 0
    resolved_names = []

    for item in trainer_ids:
        try:
            response = requests.get(
                "https://www.tjk.org/TR/Kurumsal/Query/Grouped/AntrenorIstatistikleri",
                params={'1': '1', 'QueryParameter_AntrenorId': item['id']},
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept-Language": "tr-TR,tr;q=0.9",
                },
                timeout=12,
            )
            if response.status_code != 200:
                continue
            soup = BeautifulSoup(response.text, 'html.parser')
            for row in soup.select('table tbody tr'):
                cells = [' '.join(td.get_text(' ', strip=True).split()) for td in row.find_all('td')]
                if len(cells) < 9:
                    continue
                city = _v4_fold_text(cells[1])
                year = _to_int(cells[2])
                if 'TUM' not in city or year < year_min or year > current_year:
                    continue
                races = _to_int(cells[3])
                wins = _to_int(cells[4])
                second = _to_int(cells[5])
                third = _to_int(cells[6])
                total_races += races
                total_wins += wins
                total_places += wins + second + third
                resolved_names.append(cells[0])
        except Exception as e:
            print(f"[TRAINER] Istatistik hatasi ({trainer_name}): {e}")
            continue

    if total_races == 0:
        data_quality = 'NONE'
    elif ambiguous_trainer:
        data_quality = 'AMBIGUOUS'
    else:
        data_quality = 'LOW' if total_races < 30 else 'HIGH'
    result = {
        'trainer_name': trainer_name,
        'resolved_name': resolved_names[0] if resolved_names else trainer_name,
        'resolved_names': sorted(set(resolved_names)),
        'candidate_count': len(trainer_ids),
        'total_races': total_races,
        'total_wins': total_wins,
        'win_rate': round(total_wins / total_races, 3) if total_races else 0.0,
        'place_rate': round(total_places / total_races, 3) if total_races else 0.0,
        'data_quality': data_quality,
    }
    _trainer_cache[trainer_key] = result
    print(f"[TRAINER] {trainer_name}: {total_races} kosu, {total_wins} birincilik, oran={result['win_rate']:.1%}")
    return result


def calculate_trainer_score(trainer_stats):
    """
    FAZ 6.2: Antrenörün galibiyet oranını 0-100 aralığında puanlar.

    Returns:
        float: 0-100 arası antrenör skoru
    """
    if not trainer_stats or trainer_stats.get('data_quality') == 'NONE':
        return 50.0  # Veri yok → nötr

    win_rate = trainer_stats.get('win_rate', 0.0)
    place_rate = trainer_stats.get('place_rate', 0.0)
    total    = trainer_stats.get('total_races', 0)

    # Az veri → orta skora çek
    if total < 10:
        confidence_factor = 0.6
    elif total < 25:
        confidence_factor = 0.85
    else:
        confidence_factor = 1.0

    # Galibiyet oranına göre taban skor
    if win_rate >= 0.30:
        base = 92
    elif win_rate >= 0.22:
        base = 80
    elif win_rate >= 0.15:
        base = 65
    elif win_rate >= 0.08:
        base = 48
    elif win_rate >= 0.03:
        base = 35
    else:
        base = 28

    # Az veriyse orta (50) değerine doğru çek
    place_bonus = max(-8.0, min(8.0, (place_rate - 0.30) * 35.0))
    score = (base + place_bonus) * confidence_factor + 50.0 * (1 - confidence_factor)
    return round(max(0.0, min(100.0, score)), 1)


# ══════════════════════════════════════════════════════════════════
# FAZ 4.6: PEDİGRİ / KAN HATTI ANALİZİ (KATMAN 11)
# ══════════════════════════════════════════════════════════════════

# Modül düzeyi önbellek — aynı baba için TJK'ya tek istek
_sire_cache = {}  # { 'BABA_ADI_UPPER': { ...stats... } }


def _sire_search_names(sire_name):
    """Return TJK-friendly sire name candidates, stripping country suffixes."""
    raw = str(sire_name or '').strip()
    if not raw:
        return []
    raw = re.sub(r'\s+', ' ', raw)
    cleaned = re.sub(r'\s*\((?:[A-Z]{2,3}|[A-Z]{2,3}\.)\)\s*$', '', raw, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return list(dict.fromkeys([name for name in [cleaned, raw] if name]))


def fetch_sire_offspring_stats(sire_name):
    """
    FAZ 4.6: TJK KosuSorgulama sayfasından babanın yavrularının
    geçmiş yarış istatistiklerini çeker.

    Döndürür:
        dict: {
            'sire_name', 'total_offspring_races',
            'win_rate', 'track_profile', 'distance_profile', 'data_quality'
        }
    """
    global _sire_cache

    if not sire_name or not sire_name.strip():
        return None

    search_names = _sire_search_names(sire_name)
    sire_key = _v4_fold_text(search_names[0] if search_names else sire_name).strip()

    # Önbellekte varsa direkt dön
    if sire_key in _sire_cache:
        print(f"[PEDIGREE CACHE] {sire_key} önbellekten alındı")
        return _sire_cache[sire_key]

    print(f"[PEDIGREE] {sire_name} için yavru istatistikleri çekiliyor...")

    try:
        base_url = "https://www.tjk.org/TR/YarisSever/Query/Page/KosuSorgulama"
        current_year = time.localtime().tm_year
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9",
            "Referer": "https://www.tjk.org/TR/YarisSever/Query/Page/KosuSorgulama"
        }

        tbody = None
        matched_sire_name = None
        for candidate_name in search_names:
            params = {
                'QueryParameter_BabaAdi': candidate_name,
                'QueryParameter_Tarih_Start': f'01.01.{current_year - 2}',
                'QueryParameter_Tarih_End':   f'31.12.{current_year}',
                'QueryParameter_SehirId':     '-1',
            }
            response = requests.get(base_url, params=params, headers=headers, timeout=12)
            if response.status_code != 200:
                print(f"[PEDIGREE] TJK hatası ({candidate_name}): {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # Tablo gövdesini bul
            table = soup.find('table', id='queryTable')
            candidate_tbody = None
            if table:
                candidate_tbody = table.find('tbody', id='tbody0') or table.find('tbody')
            if not candidate_tbody:
                candidate_tbody = soup.find('tbody', id='tbody0')
            rows = candidate_tbody.find_all('tr') if candidate_tbody else []
            has_data = any(
                'hidable' not in row.get('class', []) and len(row.find_all('td')) >= 8
                for row in rows
            )
            if has_data:
                tbody = candidate_tbody
                matched_sire_name = candidate_name
                break

        if not tbody:
            print(f"[PEDIGREE] {sire_name} için tablo bulunamadı — veri yok")
            return {
                'sire_name': sire_name,
                'total_offspring_races': 0,
                'win_rate': 0.0,
                'track_profile': {},
                'distance_profile': {},
                'data_quality': 'NONE'
            }

        rows = tbody.find_all('tr')

        # ── Sayaçlar ──────────────────────────────────────────────
        total_races = 0
        total_wins  = 0
        track_counts = {}
        dist_buckets = {'sprint': [], 'mid': [], 'long': []}

        for row in rows:
            if 'hidable' in row.get('class', []):
                continue
            cells = row.find_all('td')
            if len(cells) < 8:
                continue

            try:
                # Sütun eşlemesi (KosuSorgulama tablosu):
                # [0]Tarih [1]Şehir [2]KoşuNo [3]Grup [4]KoşuCinsi
                # [5]AprKoşCinsi [6]Mesafe [7]Pist ... → sıralama eksik!
                # Detay linkinden at adı / sonuç çekmek yerine
                # kısaca pist + mesafe + yarış varlığını sayıyoruz,
                # birinci gelme bilgisi için koşu detayı çekmemek adına
                # ortalama sıralama yerine win_rate'i atlıyoruz.
                # Yine de pist × mesafe profili için bu satırlar yeterli.

                mesafe_str = cells[6].text.strip() if len(cells) > 6 else ''
                pist_str   = cells[7].text.strip().lower() if len(cells) > 7 else ''

                # Mesafe sayısına çevir
                try:
                    mesafe = int(re.sub(r'[^0-9]', '', mesafe_str))
                except:
                    mesafe = 0

                if mesafe <= 0:
                    continue

                total_races += 1

                # ─ Pist profili ────────────────────────────────
                if 'çim' in pist_str:
                    pist_key = 'çim'
                elif 'kum' in pist_str:
                    pist_key = 'kum'
                elif 'sentetik' in pist_str:
                    pist_key = 'sentetik'
                else:
                    pist_key = 'diger'

                pist_key = _track_key(pist_str) or pist_key
                if pist_key not in track_counts:
                    track_counts[pist_key] = {'races': 0}
                track_counts[pist_key]['races'] += 1

                # ─ Mesafe profili ───────────────────────────────
                if mesafe <= 1400:
                    dist_buckets['sprint'].append(mesafe)
                elif mesafe <= 1800:
                    dist_buckets['mid'].append(mesafe)
                else:
                    dist_buckets['long'].append(mesafe)

            except Exception as row_err:
                print(f"[PEDIGREE] Satır hatası: {row_err}")
                continue

        total_wins = total_races
        data_quality = 'NONE' if total_races == 0 else ('LOW' if total_races < 20 else 'HIGH')

        # Track profile dict oluştur (win_rate = 0 placeholder — detay çekmiyoruz)
        track_profile = {}
        for tk, tv in track_counts.items():
            track_profile[tk] = {
                'races':    tv['races'],
                'share':    round(tv['races'] / total_races, 3) if total_races else 0,
            }

        distance_profile = {
            'sprint': {'races': len(dist_buckets['sprint'])},
            'mid':    {'races': len(dist_buckets['mid'])},
            'long':   {'races': len(dist_buckets['long'])},
        }

        result = {
            'sire_name':             matched_sire_name or sire_name,
            'original_sire_name':    sire_name,
            'total_offspring_races': total_races,
            'win_rate':              round(total_wins / total_races, 3) if total_races else 0.0,
            'track_profile':         track_profile,
            'distance_profile':      distance_profile,
            'data_quality':          data_quality,
        }

        _sire_cache[sire_key] = result
        print(f"[PEDIGREE] {sire_name}: {total_races} yarış, kalite={data_quality}")
        return result

    except Exception as e:
        print(f"[PEDIGREE] fetch_sire_offspring_stats hatası: {e}")
        return None


def calculate_pedigree_score(sire_stats, target_track, target_distance):
    """
    FAZ 4.6: Baba istatistikleri × hedef pist × hedef mesafe → 0-100 arası pedigri skoru.

    Formül:
        PedigriSkoru = GenelPrf(0.40) + PistPrf(0.35) + MesafePrf(0.25)
    """
    if not sire_stats or sire_stats.get('data_quality') == 'NONE':
        return 50.0, 'Veri Yok', 'Bilinmiyor'

    total = sire_stats.get('total_offspring_races', 0)
    data_quality = sire_stats.get('data_quality', 'LOW')

    # ─ 1. Genel Performans ─────────────────────────────────────────
    # Veri varsa genel skor orta-üstü; kalite düşükse nötre çek
    if data_quality == 'HIGH':
        general_score = 60.0  # Yeterli veri: hafif pozitif
    else:
        general_score = 52.0  # Az veri: nötüre yakın

    # ─ 2. Pist Profili ─────────────────────────────────────────────
    general_score = 45.0 + min(25.0, np.log1p(max(total, 0)) * 6.0)
    if data_quality == 'LOW':
        general_score = 45.0 + min(15.0, np.log1p(max(total, 0)) * 4.0)

    track_profile = sire_stats.get('track_profile', {})
    target_track_key = _track_key(target_track)
    target_track_lower = (target_track or '').lower()

    if 'çim' in target_track_lower:
        target_key = 'çim'
    elif 'kum' in target_track_lower:
        target_key = 'kum'
    elif 'sentetik' in target_track_lower:
        target_key = 'sentetik'
    else:
        target_key = None

    pist_score = 50.0  # nötr
    target_key = _track_key(target_track) or target_key
    track_compat_label = 'Bilinmiyor'

    if target_key and track_profile:
        target_pist = track_profile.get(target_key, {})
        target_races = target_pist.get('races', 0)
        total_track_races = sum(v.get('races', 0) for v in track_profile.values())

        if total_track_races > 0:
            share = target_races / total_track_races
            # Payı > %50 → baba bu pistte çok koşmuş → onaylı
            if share >= 0.50:
                pist_score = 75.0
                track_compat_label = f"{target_key.capitalize()} Uyumlu"
            elif share >= 0.30:
                pist_score = 60.0
                track_compat_label = f"{target_key.capitalize()} Uyumlu"
            elif share >= 0.10:
                pist_score = 48.0
                track_compat_label = 'Nötr'
            else:
                pist_score = 35.0
                track_compat_label = f"{target_key.capitalize()} Zayıf"
        else:
            pist_score = 50.0
            track_compat_label = 'Bilinmiyor'
    elif not track_profile:
        track_compat_label = 'Bilinmiyor'

    # ─ 3. Mesafe Profili ───────────────────────────────────────────
    dist_profile = sire_stats.get('distance_profile', {})
    try:
        target_dist = int(re.sub(r'[^0-9]', '', str(target_distance)))
    except:
        target_dist = 0

    mesafe_score = 50.0
    dist_compat_label = 'Bilinmiyor'

    if target_dist > 0 and dist_profile:
        if target_dist <= 1400:
            bucket = 'sprint'
        elif target_dist <= 1800:
            bucket = 'mid'
        else:
            bucket = 'long'

        bucket_races = dist_profile.get(bucket, {}).get('races', 0)
        total_dist_races = sum(v.get('races', 0) for v in dist_profile.values())

        if total_dist_races > 0:
            share = bucket_races / total_dist_races
            if share >= 0.50:
                mesafe_score = 75.0
                dist_compat_label = f"{bucket.capitalize()} Uzmanı"
            elif share >= 0.30:
                mesafe_score = 62.0
                dist_compat_label = 'Mesafe Uyumlu'
            elif share >= 0.10:
                mesafe_score = 48.0
                dist_compat_label = 'Mesafe Nötr'
            else:
                mesafe_score = 35.0
                dist_compat_label = 'Mesafe Zayıf'

    # ─ 4. Ağırlıklı birleşim ─────────────────────────────────────
    score = (general_score * 0.40) + (pist_score * 0.35) + (mesafe_score * 0.25)

    # Az veri → skoru nötre (50) doğru çek
    if data_quality == 'LOW':
        score = score * 0.6 + 50 * 0.4

    return float(round(max(0, min(100, score)), 1)), track_compat_label, dist_compat_label


def calculate_pedigree_weight(horse_races, target_track, target_distance):
    """
    FAZ 4.6: Atın hedef pist ve mesafedeki tecrübesine göre pedigri
    katmanının dinamik ağırlığını hesaplar.

    Returns:
        float: 0.03 ile 0.20 arası pedigri ağırlığı
    """
    if not horse_races:
        return 0.20  # Maiden / veri yok → maksimum pedigri ağırlığı

    target_track_lower = (target_track or '').lower()
    target_track_key = _track_key(target_track)
    try:
        target_dist = int(re.sub(r'[^0-9]', '', str(target_distance)))
    except:
        target_dist = 0

    track_races_count = sum(
        1 for r in horse_races
        if target_track_key and _track_key(r.get('track', '')) == target_track_key
    )

    if track_races_count == 0:
        base_weight = 0.15
    elif track_races_count <= 2:
        base_weight = 0.10
    elif track_races_count <= 5:
        base_weight = 0.06
    else:
        base_weight = 0.03

    if target_dist > 0:
        dist_races_count = sum(
            1 for r in horse_races
            if abs(int(re.sub(r'[^0-9]', '', r.get('distance', '0')) or 0) - target_dist) <= 200
        )
        if dist_races_count == 0:
            base_weight += 0.05

    return round(min(0.20, max(0.03, base_weight)), 3)



def parse_agf_percent(agf_str):
    """Extract the AGF percentage from TJK strings like '%17(3)' or '%7(7) %6(7)'."""
    text = str(agf_str or '').replace(',', '.')
    match = re.search(r'%\s*(\d+(?:\.\d+)?)', text)
    if match:
        try:
            return max(0.0, float(match.group(1)))
        except (ValueError, TypeError):
            return None

    # Fallback for already-normalized numeric AGF values.
    try:
        value = float(text.strip())
        return max(0.0, value)
    except (ValueError, TypeError):
        return None


def calculate_agf_score(agf_str, all_agf_values):
    """
    FAZ 6.2: AGF yüzdesini 0-100 skoruna çevirir.
    Yüksek AGF yüzdesi = piyasanın daha çok tuttuğu at → yüksek skor.

    Args:
        agf_str (str): Atın AGF değeri (str, örn. '%17(3)')
        all_agf_values (list): Koşudaki tüm geçerli AGF değerlerinin listesi

    Returns:
        float: 0-100 arası AGF skoru
    """
    agf_val = parse_agf_percent(agf_str)
    if agf_val is None:
        return 50.0

    if not all_agf_values or len(all_agf_values) < 2:
        return 50.0

    min_agf = min(all_agf_values)
    max_agf = max(all_agf_values)

    agf_range = max_agf - min_agf
    if agf_range <= 0:
        return 50.0

    raw_score = ((agf_val - min_agf) / agf_range) * 100
    return round(max(0.0, min(100.0, raw_score)), 1)


def parse_horse_age(age_str):
    """Parse TJK age strings such as '4y d a' or '3y k d' into an integer age."""
    match = re.search(r'\d+', str(age_str or ''))
    if not match:
        return None
    try:
        return int(match.group(0))
    except (ValueError, TypeError):
        return None


def calculate_age_score(age_value, race_age_values, race_type=''):
    """Return a data-backed 0-100 age score.

    Recent labeled tests showed age only helps when the field has mixed ages.
    In those mixed-age races, older runners gave the most stable small signal
    for Handikap/KV/Sartli. Same-age fields stay neutral and are gated out.
    """
    if age_value is None or not race_age_values or len(race_age_values) < 2:
        return 50.0

    min_age = min(race_age_values)
    max_age = max(race_age_values)
    age_range = max_age - min_age
    if age_range <= 0:
        return 50.0

    race_type_lower = (race_type or '').lower()
    if any(k in race_type_lower for k in ['maiden', 'mdn', 'md']):
        return 50.0

    raw_score = ((age_value - min_age) / age_range) * 100
    return round(max(0.0, min(100.0, raw_score)), 1)


def calculate_dynamic_weights(metrics, race_type='default'):
    """
    FAZ A + 4.7: Her at için veri durumuna göre katmanların ağırlıklarını
    tamamen dinamik hesaplar. Toplam her zaman 1.0 (%100) olur.

    ÖLÜ KATMANLAR: track_suit ve trainer_score devre dışı bırakıldı.

    Args:
        metrics (dict): PASS1'den gelen ham metrikler

    Returns:
        dict: Normalize edilmiş ağırlıklar (toplam = 1.0)
    """
    total_races       = metrics.get('_total_races', 0)
    has_training      = metrics.get('_has_training', False)
    track_races       = metrics.get('_track_races', 0)
    dist_races        = metrics.get('_dist_races', 0)
    has_pedigree_data = metrics.get('_has_pedigree', False)
    has_trainer_data  = metrics.get('_has_trainer', False)
    has_agf_data      = metrics.get('_has_agf', False)
    has_hp_data       = metrics.get('_has_hp', False)
    has_weight_data   = metrics.get('_has_weight', False)
    has_jockey_data   = metrics.get('_has_jockey', False)
    has_training_degree_data = metrics.get('_has_training_projection', False)
    has_age_data      = metrics.get('_has_age', False)
    pedigree_weight   = float(metrics.get('pedigree_weight', 0.03))

    # ══ FAZ A: TEMEL AĞIRLIKLAR (Ölü katmanlar sıfırlandı) ══════════════
    w = {
        'degree_avg':            0.20,
        'degree_trend':          0.08,
        'degree_stability':      0.06,
        'training_fitness':      0.06,
        'training_degree_score': 0.05,
        'track_suit':            0.03,
        'form_trend':            0.18,
        'distance_suit':         0.08,
        'weight_impact':         0.06,
        'jockey_score':          0.07,
        'bounce_score':          0.08,
        'pace_score':            0.03,
        'pedigree':              0.03,
        'hp_score':              0.08,
        'agf_score':             0.00,
        'trainer_score':         0.02,
        'age_score':             0.02,
    }

    # ── KOŞU TİPİNE ÖZEL AĞIRLIK PROFİLLERİ ────────────────────
    race_type_lower = (race_type or 'default').lower()

    if any(k in race_type_lower for k in ['handikap', 'hk', 'handicap']):
        w['hp_score']              = 0.14
        w['weight_impact']         = 0.09
        w['degree_avg']            = 0.18
        w['form_trend']            = 0.20
        w['bounce_score']          = 0.09
        w['distance_suit']         = 0.08
        w['jockey_score']          = 0.08
        w['age_score']             = 0.04
        print(f"[WEIGHTS] Handikap -> HP:%14 Form:%20 Hiz:%18 Kilo:%9")

    elif any(k in race_type_lower for k in ['maiden', 'mdn', 'md']):
        w['pedigree']              = 0.18
        w['training_fitness']      = 0.14
        w['training_degree_score'] = 0.10
        w['agf_score']             = 0.20
        w['degree_avg']            = 0.06
        w['form_trend']            = 0.05
        w['degree_stability']      = 0.02
        w['jockey_score']          = 0.10
        w['age_score']             = 0.00
        print(f"[WEIGHTS] Maiden -> Pedigri:%18 Idman:%24 AGF:%20 Jokey:%10")

    elif any(k in race_type_lower for k in ['sartli', 'conditions']) or 'şartl' in race_type_lower:
        is_sartli_1 = '1' in race_type_lower and not any(c in race_type_lower for c in ['10', '11', '12', '13', '14', '15'])
        if is_sartli_1:
            w['form_trend']            = 0.16
            w['degree_avg']            = 0.16
            w['agf_score']             = 0.15
            w['pedigree']              = 0.10
            w['training_fitness']      = 0.10
            w['hp_score']              = 0.04
            w['jockey_score']          = 0.09
            w['age_score']             = 0.03
            print(f"[WEIGHTS] Sartli 1 -> Form:%16 Hiz:%16 AGF:%15")
        else:
            w['form_trend']            = 0.22
            w['degree_avg']            = 0.20
            w['hp_score']              = 0.05
            w['bounce_score']          = 0.09
            w['distance_suit']         = 0.09
            w['jockey_score']          = 0.08
            w['age_score']             = 0.03
            print(f"[WEIGHTS] Sartli 2+ -> Form:%22 Hiz:%20 Bounce:%9")

    elif 'kv' in race_type_lower:
        w['form_trend']            = 0.22
        w['degree_avg']            = 0.22
        w['hp_score']              = 0.04
        w['bounce_score']          = 0.08
        w['distance_suit']         = 0.08
        w['jockey_score']          = 0.08
        w['age_score']             = 0.04
        print(f"[WEIGHTS] KV -> Form:%22 Hiz:%22 Bounce:%8")

    elif any(k in race_type_lower for k in ['satiş', 'satis', 'claiming']):
        w['form_trend']            = 0.20
        w['degree_avg']            = 0.20
        w['jockey_score']          = 0.08
        w['bounce_score']          = 0.08
        w['age_score']             = 0.02
        print(f"[WEIGHTS] Satis -> Form:%20 Hiz:%20 Bounce:%8")

    is_maiden_for_agf = any(k in race_type_lower for k in ['maiden', 'mdn', 'md'])
    is_sartli_1_for_agf = bool(
        any(k in race_type_lower for k in ['şartl', 'sartl'])
        and re.search(r'\b(?:şartli|şartlı|sartli|sartlı|sartl)\s*[-/]?\s*1\b', race_type_lower)
    )
    if not (is_maiden_for_agf or is_sartli_1_for_agf):
        w['agf_score'] = 0.0

    # ── SENARYO: MAİDEN (İlk koşu — hiç yarış verisi yok) ──────────
    if total_races == 0:
        w['degree_avg']            = 0.0
        w['degree_trend']          = 0.0
        w['degree_stability']      = 0.0
        w['form_trend']            = 0.0
        w['track_suit']            = 0.0
        w['distance_suit']         = 0.0
        w['bounce_score']          = 0.0
        w['hp_score']              *= 0.50
        w['training_fitness']      = 0.25 if has_training else 0.0
        w['training_degree_score'] = 0.15 if has_training else 0.0
        w['jockey_score']          = 0.15
        w['pace_score']            = 0.05
        w['weight_impact']         = 0.05
        w['pedigree']              = 0.20
        w['age_score']             = 0.0

    elif total_races <= 2:
        w['degree_avg']       *= 0.55
        w['form_trend']       *= 0.50
        w['degree_stability'] *= 0.30
        if has_training:
            w['training_fitness']      *= 1.40
            w['training_degree_score'] *= 1.30
        w['pedigree'] = pedigree_weight

    else:
        if dist_races == 0:
            w['distance_suit'] *= 0.3
        w['pedigree'] = pedigree_weight

    # ── İDMAN VERİSİ YOK → idman katmanlarını kapat ────────────────
    if not has_training:
        freed = w['training_fitness'] + w['training_degree_score']
        w['training_fitness']      = 0.0
        w['training_degree_score'] = 0.0
        w['degree_avg']   += freed * 0.50
        w['form_trend']   += freed * 0.25
        w['pedigree']     += freed * 0.15
        w['jockey_score'] += freed * 0.10

    if track_races == 0:
        freed_track = w.get('track_suit', 0.0)
        w['track_suit'] = 0.0
        w['distance_suit'] += freed_track * 0.45
        w['degree_avg'] += freed_track * 0.35
        w['form_trend'] += freed_track * 0.20

    if not has_trainer_data:
        freed_trainer = w.get('trainer_score', 0.0)
        w['trainer_score'] = 0.0
        w['jockey_score'] += freed_trainer * 0.50
        w['form_trend'] += freed_trainer * 0.30
        w['degree_avg'] += freed_trainer * 0.20

    # ── FAZ B.4: AGF VERİSİ YOK → AGF ağırlığını dağıt ─────────────
    # AGF=50 nötr değer demek (veri yok veya koşu başlamadı).
    # Maiden'da %20 ağırlıkla bu çok büyük → sıfırla ve dağıt.
    unavailable = set()
    if total_races == 0:
        unavailable.update([
            'degree_avg', 'degree_trend', 'degree_stability',
            'form_trend', 'track_suit', 'distance_suit',
            'bounce_score', 'pace_score', 'weight_impact',
        ])
    else:
        if track_races == 0:
            unavailable.add('track_suit')
        if dist_races == 0:
            unavailable.add('distance_suit')

    if not has_training:
        unavailable.update(['training_fitness', 'training_degree_score'])
    elif not has_training_degree_data:
        unavailable.add('training_degree_score')
    if not has_pedigree_data:
        unavailable.add('pedigree')
    if not has_trainer_data:
        unavailable.add('trainer_score')
    if not has_agf_data:
        unavailable.add('agf_score')
    if not has_hp_data:
        unavailable.add('hp_score')
    if not has_weight_data:
        unavailable.add('weight_impact')
    if not has_jockey_data and abs(float(metrics.get('jockey_score', 50.0) or 50.0) - 50.0) < 1.0:
        unavailable.add('jockey_score')
    if not has_age_data:
        unavailable.add('age_score')

    agf_val = metrics.get('agf_score', 50.0)
    if not has_agf_data and abs(agf_val - 50.0) < 1.0 and w.get('agf_score', 0) > 0.01:
        freed_agf = w['agf_score']
        w['agf_score'] = 0.0
        # Dağıt: pedigri %40, jokey %30, idman %30
        w['pedigree']     += freed_agf * 0.40
        w['jockey_score'] += freed_agf * 0.30
        if has_training:
            w['training_fitness'] += freed_agf * 0.30
        else:
            w['hp_score']  += freed_agf * 0.15
            w['bounce_score'] += freed_agf * 0.15

    # ── TOPLAMI %100'e normalize et ─────────────────────────────────
    freed_missing = 0.0
    for key in unavailable:
        freed_missing += w.get(key, 0.0)
        w[key] = 0.0

    eligible = [key for key, value in w.items() if value > 0 and key not in unavailable]
    eligible_total = sum(w[key] for key in eligible)
    if freed_missing > 0 and eligible_total > 0:
        for key in eligible:
            w[key] += freed_missing * (w[key] / eligible_total)

    total = sum(w.values())
    if total > 0:
        w = {k: round(v / total, 4) for k, v in w.items()}

    return w


def calculate_data_confidence(metrics):
    """
    FAZ 4.7: Veri doluluk yüzdesini hesaplar.
    Kullanıcıya tahmin doğruluğu hakkında güven sinyali verir.

    Returns:
        float: 0.0 - 1.0 (1.0 = tam veri)
        str:   '🟢 Yüksek' | '🟡 Orta' | '🔴 Düşük'
    """
    total_races  = metrics.get('_total_races', 0)
    has_training = metrics.get('_has_training', False)
    track_races  = metrics.get('_track_races', 0)
    dist_races   = metrics.get('_dist_races', 0)
    has_pedigree = metrics.get('_has_pedigree', False)

    score = 0.0

    # Yarış geçmişi (max 0.40)
    if total_races >= 6:
        score += 0.40
    elif total_races >= 3:
        score += 0.28
    elif total_races >= 1:
        score += 0.12

    # İdman verisi (max 0.20)
    if has_training:
        score += 0.20

    # Pist tecrübesi (max 0.15)
    if track_races >= 3:
        score += 0.15
    elif track_races >= 1:
        score += 0.08

    # Mesafe tecrübesi (max 0.15)
    if dist_races >= 3:
        score += 0.15
    elif dist_races >= 1:
        score += 0.07

    # Pedigri verisi (max 0.10)
    if has_pedigree:
        score += 0.10

    confidence = round(min(1.0, score), 2)

    if confidence >= 0.75:
        label = '🟢 Yüksek'
    elif confidence >= 0.45:
        label = '🟡 Orta'
    else:
        label = '🔴 Düşük'

    return confidence, label


def calculate_group_adjustment(horse_races, current_race_type):
    """
    FAZ B.3: Grup Ayarlaması (Daraltılmış Aralık)
    
    Atın geçmiş koştuğu gruplar ile mevcut koşu grubunu karşılaştır.
    0.94-1.06 aralığında — çok agresif çarpanlar sıralamayı bozuyordu.
    
    Returns:
        float: Çarpan (0.94 - 1.06 arası)
    """
    if not horse_races:
        return 1.0
    
    current_mult = get_class_multiplier(current_race_type)
    
    adjustments = []
    for race in horse_races[:6]:
        race_type = race.get('raceType', '') or race.get('group', '')
        past_mult = get_class_multiplier(race_type)
        try:
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
        except:
            rank = 0
        
        if rank <= 0:
            continue
            
        group_diff = current_mult - past_mult
        
        if group_diff > 0.03:
            # Mevcut koşu geçmişten DAHA ZOR
            if rank <= 3:
                adjustments.append(0.97)  # Kolay grupta top3 → hafif düşür
            else:
                adjustments.append(0.94)  # Kolay grupta kötü → biraz düşür
        elif group_diff < -0.03:
            # Mevcut koşu geçmişten DAHA KOLAY
            if rank <= 3:
                adjustments.append(1.06)  # Zor grupta top3 → ödülllendir
            else:
                adjustments.append(1.0)   # Zor grupta kötü → nötr
        else:
            adjustments.append(1.0)
    
    if not adjustments:
        return 1.0
    
    return round(np.mean(adjustments), 3)


def calculate_master_score(metrics):
    """
    FAZ 4.7 + Grup Ayarlaması: Dinamik ağırlıklı Master Tahmin Skoru.
    
    Konsensüs artık koşu seviyesinde (PASS 2) uygulanıyor.
    Burada sadece:
    - Ağırlıklı ortalama (11 katman)
    - Grup ayarlaması (geçmiş koşu grubu vs mevcut)

    Returns:
        float: 0-100 arası Master AI Skoru
        dict:  Uygulanan dinamik ağırlıklar
        float: Güven yüzde skoru (0-1)
        str:   Güven etiketi
    """
    race_type = metrics.get('_race_type', 'default')
    weights = calculate_dynamic_weights(metrics, race_type=race_type)
    confidence, confidence_label = calculate_data_confidence(metrics)

    weighted_sum  = 0.0
    weight_total  = 0.0

    for key, weight in weights.items():
        if weight <= 0:
            continue
        value = metrics.get(key, 50.0)
        weighted_sum  += value * weight
        weight_total  += weight

    base_score = round(weighted_sum / weight_total, 1) if weight_total > 0 else 50.0
    
    # Grup ayarlaması: geçmiş koşuların grup seviyesi etkisi
    horse_races = metrics.get('_horse_races', [])
    group_adj = calculate_group_adjustment(horse_races, race_type)
    
    final_score = round(base_score * group_adj, 1)
    final_score = max(0, min(100, final_score))
    
    print(f"    [MASTER] base={base_score:.1f} group_adj={group_adj:.3f} final={final_score:.1f}")
    
    return final_score, weights, confidence, confidence_label


# ══════════════════════════════════════════════════════════════════
# FAZ 8: HİBRİT ML BLEND (XGBoost + Kural Tabanlı)
# ══════════════════════════════════════════════════════════════════

# AGF ML'den hariç (kullanıcı kararı)
# ============================================================================
# ALGORITHM V4 SHADOW MODE
# ============================================================================

_V4_VERSION = "4.21"

_V4_METRIC_KEYS = [
    'degree_avg', 'degree_trend', 'degree_stability',
    'form_trend', 'track_suit', 'track_experience_score', 'surface_transition_score',
    'distance_suit', 'distance_transition_score',
    'training_fitness', 'training_degree_score',
    'weight_impact', 'handicap_efficiency_score',
    'handicap_weight_relief_score', 'handicap_class_transition_score',
    'field_relative_value_score', 'pace_map_edge_score',
    'surface_switch_safety_score', 'favorite_risk_guard_score',
    'class_peak_score', 'elite_consensus_score',
    'running_style_proxy_score',
    'jockey_score', 'bounce_score',
    'pace_score', 'pedigree', 'hp_score', 'trainer_score', 'agf_score',
    'age_score',
]

_V4_MIN_SAMPLE_RACES = {
    'exact': 8,
    'subtype_distance_field': 8,
    'subtype_track': 8,
    'subtype': 12,
    'category': 25,
    'global': 0,
}

_V4_WEIGHT_PROFILES = {
    'SART3': {
        'level': 'subtype',
        'sample_races': 21,
        'status': 'eligible_shadow',
        'weights': {
            'pedigree': 23.5,
            'form_trend': 23.1,
            'bounce_score': 12.1,
            'distance_suit': 11.1,
            'degree_avg': 4.5,
            'training_fitness': 4.2,
            'training_degree_score': 4.1,
            'hp_score': 3.9,
            'weight_impact': 3.2,
            'age_score': 2.6,
            'degree_stability': 2.2,
            'jockey_score': 2.0,
            'degree_trend': 1.6,
            'pace_score': 1.6,
        },
    },
    'SART4': {
        'level': 'subtype',
        'sample_races': 21,
        'status': 'eligible_shadow',
        'weights': {
            'training_fitness': 35.0,
            'agf_score': 16.0,
            'pedigree': 8.0,
            'degree_stability': 7.0,
            'jockey_score': 7.0,
            'degree_avg': 6.0,
            'degree_trend': 5.0,
            'training_degree_score': 4.0,
            'form_trend': 4.0,
            'weight_impact': 3.0,
            'hp_score': 2.0,
            'bounce_score': 2.0,
            'distance_suit': 1.0,
            'age_score': 3.0,
        },
    },
    'SART5': {
        'level': 'subtype',
        'sample_races': 12,
        'status': 'eligible_shadow',
        'weights': {
            'jockey_score': 25.2,
            'pedigree': 16.1,
            'degree_avg': 12.3,
            'training_degree_score': 10.7,
            'hp_score': 9.7,
            'weight_impact': 7.1,
            'training_fitness': 6.0,
            'bounce_score': 4.7,
            'degree_stability': 3.0,
            'distance_suit': 2.3,
            'form_trend': 1.8,
            'degree_trend': 1.2,
            'age_score': 4.0,
        },
    },
    'SARTLI': {
        'level': 'category',
        'sample_races': 63,
        'status': 'eligible_shadow',
        'weights': {
            'weight_impact': 18.0,
            'training_degree_score': 14.0,
            'degree_avg': 12.0,
            'pace_score': 12.0,
            'training_fitness': 10.0,
            'bounce_score': 8.0,
            'distance_suit': 7.0,
            'form_trend': 6.0,
            'jockey_score': 4.0,
            'hp_score': 4.0,
            'agf_score': 3.0,
            'degree_trend': 2.0,
            'age_score': 4.0,
        },
    },
    'HANDIKAP': {
        'level': 'category',
        'sample_races': 106,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'HANDIKAP14': {
        'level': 'subtype',
        'sample_races': 12,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'HANDIKAP14|Kum': {
        'level': 'subtype_track',
        'sample_races': 10,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'HANDIKAP15': {
        'level': 'subtype',
        'sample_races': 23,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'HANDIKAP15|Kum': {
        'level': 'subtype_track',
        'sample_races': 12,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'HANDIKAP15|Cim': {
        'level': 'subtype_track',
        'sample_races': 10,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'HANDIKAP16': {
        'level': 'subtype',
        'sample_races': 26,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'HANDIKAP16|Kum': {
        'level': 'subtype_track',
        'sample_races': 11,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'HANDIKAP16|Cim': {
        'level': 'subtype_track',
        'sample_races': 8,
        'status': 'visible_controlled',
        'weights': {
            'pace_score': 28.0,
            'agf_score': 24.0,
            'form_trend': 12.0,
            'degree_trend': 10.0,
            'bounce_score': 8.0,
            'training_fitness': 6.0,
            'training_degree_score': 5.0,
            'pedigree': 5.0,
            'jockey_score': 4.0,
            'trainer_score': 4.0,
            'hp_score': 3.0,
            'distance_suit': 2.0,
            'track_suit': 2.0,
        },
    },
    'MAIDEN': {
        'level': 'category',
        'sample_races': 43,
        'status': 'visible_controlled',
        'weights': {
            'agf_score': 28.6,
            'form_trend': 18.1,
            'pace_score': 10.2,
            'degree_avg': 9.5,
            'hp_score': 9.1,
            'jockey_score': 8.0,
            'degree_stability': 5.7,
            'distance_suit': 4.2,
            'training_fitness': 2.8,
            'pedigree': 1.4,
            'degree_trend': 1.2,
            'training_degree_score': 1.2,
            'bounce_score': 0.0,
            'weight_impact': 0.0,
            'age_score': 0.0,
        },
    },
    'KV': {
        'level': 'category',
        'sample_races': 26,
        'status': 'visible_controlled',
        'weights': {
            'jockey_score': 22.0,
            'form_trend': 18.0,
            'hp_score': 11.0,
            'distance_suit': 10.0,
            'pace_score': 8.0,
            'degree_avg': 8.0,
            'bounce_score': 5.0,
            'training_fitness': 4.0,
            'degree_trend': 4.0,
            'degree_stability': 3.0,
            'agf_score': 3.0,
            'training_degree_score': 2.0,
            'pedigree': 2.0,
            'weight_impact': 1.0,
            'age_score': 4.0,
        },
    },
    'SATIS': {
        'level': 'category',
        'sample_races': 7,
        'status': 'candidate_shadow',
        'weights': {
            'form_trend': 22.0,
            'degree_avg': 20.0,
            'jockey_score': 12.0,
            'bounce_score': 10.0,
            'weight_impact': 8.0,
            'distance_suit': 8.0,
            'pace_score': 7.0,
            'training_fitness': 5.0,
            'training_degree_score': 4.0,
            'hp_score': 4.0,
            'age_score': 2.0,
        },
    },
    'GRUP': {
        'level': 'category',
        'sample_races': 3,
        'status': 'candidate_shadow',
        'weights': {
            'degree_avg': 24.0,
            'form_trend': 20.0,
            'pace_score': 14.0,
            'jockey_score': 12.0,
            'hp_score': 10.0,
            'distance_suit': 8.0,
            'bounce_score': 6.0,
            'training_fitness': 3.0,
            'training_degree_score': 3.0,
            'age_score': 2.0,
        },
    },
    'GLOBAL': {
        'level': 'global',
        'sample_races': 0,
        'status': 'fallback_shadow',
        'weights': {
            'hp_score': 18.6,
            'degree_avg': 15.9,
            'distance_suit': 15.5,
            'pedigree': 12.2,
            'pace_score': 11.3,
            'training_degree_score': 9.8,
            'form_trend': 5.7,
            'jockey_score': 3.9,
            'degree_trend': 3.8,
            'training_fitness': 1.5,
            'weight_impact': 1.0,
            'bounce_score': 0.7,
            'degree_stability': 0.1,
            'age_score': 2.0,
        },
    },
}

_V4_SARTLI_2_PLUS_WEIGHTS = {
    'form_trend': 20.0,
    'degree_avg': 8.0,
    'degree_trend': 5.0,
    'handicap_class_transition_score': 12.0,
    'distance_suit': 8.0,
    'distance_transition_score': 5.0,
    'surface_transition_score': 5.0,
    'pace_score': 7.0,
    'running_style_proxy_score': 3.0,
    'training_fitness': 5.0,
    'training_degree_score': 3.0,
    'jockey_score': 5.0,
    'hp_score': 8.0,
    'weight_impact': 3.0,
    'trainer_score': 1.0,
}

_V4_SART3_PROFILE_WEIGHTS = {
    'form_trend': 28.236,
    'hp_score': 18.056,
    'distance_suit': 15.733,
    'track_experience_score': 15.046,
    'trainer_score': 6.984,
    'degree_stability': 4.302,
    'bounce_score': 3.376,
    'jockey_score': 2.837,
    'training_fitness': 2.069,
    'pace_score': 1.512,
    'degree_avg': 1.414,
    'track_suit': 0.435,
}

_V4_SART4_PROFILE_WEIGHTS = {
    'hp_score': 28.381,
    'trainer_score': 15.908,
    'distance_suit': 15.371,
    'training_fitness': 12.122,
    'bounce_score': 8.761,
    'degree_avg': 4.765,
    'degree_trend': 3.507,
    'degree_stability': 3.417,
    'track_suit': 2.074,
    'pace_score': 1.941,
    'jockey_score': 1.949,
    'form_trend': 1.803,
}

_V4_SART5_PROFILE_WEIGHTS = {
    'form_trend': 30.469,
    'hp_score': 14.190,
    'track_suit': 14.002,
    'degree_stability': 7.867,
    'bounce_score': 6.926,
    'degree_trend': 5.776,
    'training_degree_score': 5.746,
    'degree_avg': 5.649,
    'training_fitness': 4.415,
    'pace_score': 4.366,
    'jockey_score': 0.480,
    'distance_suit': 0.116,
}

_V4_PROFILE_WEIGHT_OVERRIDES = {
    'HANDIKAP': {
        'pace_score': 12.0,
        'running_style_proxy_score': 6.0,
        'handicap_efficiency_score': 12.0,
        'handicap_class_transition_score': 12.0,
        'form_trend': 11.0,
        'distance_suit': 6.0,
        'distance_transition_score': 5.0,
        'surface_transition_score': 4.0,
        'weight_impact': 8.0,
        'degree_avg': 4.0,
        'degree_trend': 3.0,
        'training_fitness': 4.0,
        'training_degree_score': 2.0,
        'jockey_score': 3.0,
        'trainer_score': 1.0,
        'bounce_score': 2.0,
        'hp_score': 4.0,
        'track_experience_score': 1.0,
    },
    'MAIDEN': {
        'agf_score': 16.0,
        'pedigree': 18.0,
        'training_fitness': 8.0,
        'training_degree_score': 17.0,
        'jockey_score': 6.0,
        'trainer_score': 7.0,
        'pace_score': 5.0,
        'running_style_proxy_score': 1.0,
        'hp_score': 4.0,
        'weight_impact': 2.0,
        'form_trend': 4.0,
        'degree_avg': 1.0,
        'degree_stability': 1.0,
        'distance_suit': 1.0,
        'surface_transition_score': 1.0,
        'age_score': 1.0,
    },
    'SART1': {
        'agf_score': 18.0,
        'pedigree': 16.0,
        'training_fitness': 12.0,
        'training_degree_score': 6.0,
        'jockey_score': 8.0,
        'trainer_score': 4.0,
        'pace_score': 7.0,
        'running_style_proxy_score': 3.0,
        'form_trend': 7.0,
        'degree_avg': 5.0,
        'hp_score': 3.0,
        'weight_impact': 3.0,
        'distance_suit': 4.0,
        'surface_transition_score': 2.0,
        'age_score': 2.0,
    },
    'SARTLI': _V4_SARTLI_2_PLUS_WEIGHTS,
    'SART3': _V4_SART3_PROFILE_WEIGHTS,
    'SART4': _V4_SART4_PROFILE_WEIGHTS,
    'SART5': _V4_SART5_PROFILE_WEIGHTS,
    'KV': {
        'degree_avg': 10.0,
        'degree_trend': 6.0,
        'degree_stability': 0.0,
        'form_trend': 18.0,
        'distance_suit': 10.0,
        'distance_transition_score': 4.0,
        'surface_transition_score': 3.0,
        'jockey_score': 8.0,
        'trainer_score': 4.0,
        'pace_score': 7.0,
        'running_style_proxy_score': 3.0,
        'hp_score': 12.0,
        'weight_impact': 0.0,
        'training_fitness': 4.0,
        'training_degree_score': 0.0,
        'bounce_score': 4.0,
        'age_score': 2.0,
    },
    'GRUP': {
        'elite_consensus_score': 24.0,
        'class_peak_score': 18.0,
        'hp_score': 12.0,
        'trainer_score': 10.0,
        'track_experience_score': 10.0,
        'form_trend': 8.0,
        'training_degree_score': 6.0,
        'surface_switch_safety_score': 4.0,
        'distance_suit': 3.0,
        'pace_map_edge_score': 3.0,
        'degree_avg': 2.0,
    },
    'SATIS': {
        'form_trend': 15.0,
        'degree_avg': 13.0,
        'handicap_class_transition_score': 10.0,
        'hp_score': 7.0,
        'weight_impact': 7.0,
        'distance_suit': 8.0,
        'distance_transition_score': 4.0,
        'surface_transition_score': 2.0,
        'pace_score': 7.0,
        'running_style_proxy_score': 3.0,
        'jockey_score': 5.0,
        'trainer_score': 3.0,
        'training_fitness': 4.0,
        'training_degree_score': 2.0,
        'bounce_score': 4.0,
        'age_score': 2.0,
    },
    'GLOBAL': {
        'degree_avg': 12.0,
        'form_trend': 12.0,
        'hp_score': 10.0,
        'distance_suit': 10.0,
        'distance_transition_score': 6.0,
        'surface_transition_score': 6.0,
        'pace_score': 8.0,
        'running_style_proxy_score': 4.0,
        'training_fitness': 5.0,
        'training_degree_score': 5.0,
        'jockey_score': 5.0,
        'weight_impact': 5.0,
        'pedigree': 5.0,
        'bounce_score': 4.0,
        'age_score': 3.0,
    },
}

for _profile_key, _weights in _V4_PROFILE_WEIGHT_OVERRIDES.items():
    if _profile_key not in _V4_WEIGHT_PROFILES:
        _V4_WEIGHT_PROFILES[_profile_key] = {
            'level': 'subtype' if _profile_key.startswith('SART') and _profile_key != 'SARTLI' else 'category',
            'sample_races': 0,
            'status': 'visible_controlled',
            'weights': {},
        }
    _V4_WEIGHT_PROFILES[_profile_key]['weights'] = dict(_weights)

for _profile_key in [
    'HANDIKAP14', 'HANDIKAP14|Kum',
    'HANDIKAP15', 'HANDIKAP15|Kum', 'HANDIKAP15|Cim',
    'HANDIKAP16', 'HANDIKAP16|Kum', 'HANDIKAP16|Cim',
]:
    if _profile_key in _V4_WEIGHT_PROFILES:
        _V4_WEIGHT_PROFILES[_profile_key]['weights'] = dict(_V4_PROFILE_WEIGHT_OVERRIDES['HANDIKAP'])

_V4_HANDIKAP15_KUM_WEIGHTS = {
    'pace_score': 30.0,
    'form_trend': 15.0,
    'running_style_proxy_score': 12.0,
    'handicap_class_transition_score': 9.0,
    'handicap_efficiency_score': 8.0,
    'jockey_score': 5.0,
    'weight_impact': 4.0,
    'hp_score': 4.0,
    'degree_avg': 3.0,
    'distance_suit': 2.0,
    'training_fitness': 2.0,
    'bounce_score': 2.0,
    'surface_transition_score': 1.0,
    'degree_trend': 1.0,
    'track_experience_score': 1.0,
    'trainer_score': 1.0,
}

_V4_HANDIKAP15_CIM_WEIGHTS = {
    'form_trend': 22.0,
    'running_style_proxy_score': 12.0,
    'distance_suit': 10.0,
    'pace_score': 8.0,
    'weight_impact': 8.0,
    'handicap_efficiency_score': 6.0,
    'jockey_score': 5.0,
    'degree_avg': 4.0,
    'hp_score': 4.0,
    'surface_transition_score': 4.0,
    'handicap_class_transition_score': 2.0,
    'track_experience_score': 2.0,
    'trainer_score': 2.0,
    'degree_trend': 1.0,
    'training_fitness': 1.0,
}

_V4_HANDIKAP_CLASS_FLAT_WEIGHTS = dict(_V4_PROFILE_WEIGHT_OVERRIDES['HANDIKAP'])
_V4_HANDIKAP_CLASS_FLAT_WEIGHTS['handicap_class_transition_score'] = 0.0

_V4_HANDIKAP16_PROFILE_WEIGHTS = dict(_V4_PROFILE_WEIGHT_OVERRIDES['HANDIKAP'])
_V4_HANDIKAP16_PROFILE_WEIGHTS.update({
    'handicap_class_transition_score': 0.0,
    'distance_suit': 0.0,
    'pace_score': _V4_HANDIKAP16_PROFILE_WEIGHTS['pace_score'] + 3.0,
    'form_trend': _V4_HANDIKAP16_PROFILE_WEIGHTS['form_trend'] + 3.0,
    'running_style_proxy_score': _V4_HANDIKAP16_PROFILE_WEIGHTS['running_style_proxy_score'] + 2.0,
    'weight_impact': _V4_HANDIKAP16_PROFILE_WEIGHTS['weight_impact'] + 2.0,
})

_V4_HANDIKAP15_CIM_WEIGHTS['handicap_class_transition_score'] = 0.0
_V4_HANDIKAP15_CIM_WEIGHTS['training_fitness'] = 0.0
_V4_HANDIKAP15_CIM_WEIGHTS['hp_score'] *= 0.5
_V4_HANDIKAP15_CIM_WEIGHTS['form_trend'] += 2.0
_V4_HANDIKAP15_CIM_WEIGHTS['surface_transition_score'] += 2.0

for _profile_key in ['HANDIKAP', 'HANDIKAP15']:
    if _profile_key in _V4_WEIGHT_PROFILES:
        _V4_WEIGHT_PROFILES[_profile_key]['weights'] = dict(_V4_HANDIKAP_CLASS_FLAT_WEIGHTS)

for _profile_key in ['HANDIKAP16', 'HANDIKAP16|Kum', 'HANDIKAP16|Cim']:
    if _profile_key in _V4_WEIGHT_PROFILES:
        _V4_WEIGHT_PROFILES[_profile_key]['weights'] = dict(_V4_HANDIKAP16_PROFILE_WEIGHTS)

if 'HANDIKAP15|Kum' in _V4_WEIGHT_PROFILES:
    _V4_WEIGHT_PROFILES['HANDIKAP15|Kum']['sample_races'] = 21
    _V4_WEIGHT_PROFILES['HANDIKAP15|Kum']['weights'] = dict(_V4_HANDIKAP15_KUM_WEIGHTS)

if 'HANDIKAP15|Cim' in _V4_WEIGHT_PROFILES:
    _V4_WEIGHT_PROFILES['HANDIKAP15|Cim']['sample_races'] = 19
    _V4_WEIGHT_PROFILES['HANDIKAP15|Cim']['weights'] = dict(_V4_HANDIKAP15_CIM_WEIGHTS)


def _v4_fold_text(value):
    text = str(value or '').upper()
    replacements = {
        'Ş': 'S', 'İ': 'I', 'İ': 'I', 'Ğ': 'G', 'Ü': 'U', 'Ö': 'O', 'Ç': 'C',
        'Ş': 'S', 'Þ': 'S', 'Åž': 'S', 'ÅŸ': 'S',
        'İ': 'I', 'Ä°': 'I', 'Ä±': 'I',
        'Ğ': 'G', 'Äž': 'G', 'ÄŸ': 'G',
        'Ü': 'U', 'Ãœ': 'U', 'Ã¼': 'U',
        'Ö': 'O', 'Ã–': 'O', 'Ã¶': 'O',
        'Ç': 'C', 'Ã‡': 'C', 'Ã§': 'C',
        'Ţ': 'S', 'ţ': 'S', 'Ț': 'S', 'ț': 'S',
        'Ð': 'D', 'Ý': 'I',
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'\s+', ' ', text).strip()


def _v4_distance_bucket(distance):
    try:
        meters = int(float(str(distance or '').replace(',', '.')))
    except (ValueError, TypeError):
        return 'unknown'
    if meters <= 1200:
        return 'sprint'
    if meters <= 1800:
        return 'mid'
    return 'long'


def _v4_field_bucket(field_size):
    try:
        size = int(field_size or 0)
    except (ValueError, TypeError):
        return 'unknown'
    if size <= 7:
        return 'small'
    if size <= 11:
        return 'medium'
    return 'large'


def _v4_track_bucket(track):
    folded = _v4_fold_text(track)
    if '?IM' in folded or folded.strip() == 'IM':
        return 'Cim'
    if 'KUM' in folded:
        return 'Kum'
    if 'CIM' in folded:
        return 'Cim'
    if 'SENTETIK' in folded:
        return 'Sentetik'
    return 'Unknown'


def extract_v4_race_profile(race_type='', distance='', track='', field_size=0):
    folded = _v4_fold_text(race_type)
    category = 'GLOBAL'
    subtype = 'GLOBAL'

    if any(k in folded for k in ['HANDIKAP', 'HANDICAP', ' HK']):
        category = 'HANDIKAP'
        subtype = 'HANDIKAP'
        import re as _re
        match = _re.search(r'(\d+)', folded)
        if match:
            subtype = f"HANDIKAP{match.group(1)}"
    elif any(k in folded for k in ['MAIDEN', 'MDN']):
        category = 'MAIDEN'
        subtype = 'MAIDEN'
    elif 'SARTLI' in folded or 'SARTL' in folded:
        category = 'SARTLI'
        subtype = 'SARTLI'
        import re as _re
        match = _re.search(r'(\d+)', folded)
        if match:
            subtype = f"SART{match.group(1)}"
    elif 'KV' in folded:
        category = 'KV'
        subtype = 'KV'
        import re as _re
        match = _re.search(r'KV\s*[-/]?\s*(\d+)', folded)
        if match:
            subtype = f"KV{match.group(1)}"
    elif any(k in folded for k in ['SATIS', 'CLAIMING']):
        category = 'SATIS'
        subtype = 'SATIS'
    elif (
        any(k in folded for k in ['GRUP', 'GROUP', ' G1', ' G2', ' G3'])
        or re.search(r'\bG\s*[-/]?\s*[123]\b', folded)
    ):
        category = 'GRUP'
        subtype = 'GRUP'

    distance_bucket = _v4_distance_bucket(distance)
    field_bucket = _v4_field_bucket(field_size)
    track_bucket = _v4_track_bucket(track)

    return {
        'category': category,
        'subtype': subtype,
        'distanceBucket': distance_bucket,
        'fieldBucket': field_bucket,
        'track': track_bucket,
        'profileKey': f"{subtype}|{distance_bucket}|{field_bucket}|{track_bucket}",
    }


def _v4_normalize_weights(raw_weights):
    weights = {k: float(raw_weights.get(k, 0.0)) for k in _V4_METRIC_KEYS}
    total = sum(v for v in weights.values() if v > 0)
    if total <= 0:
        return {k: round(1.0 / len(_V4_METRIC_KEYS), 4) for k in _V4_METRIC_KEYS}
    return {k: round(max(v, 0.0) / total, 4) for k, v in weights.items()}


def _v417_is_handikap_profile(profile):
    return profile.get('category') == 'HANDIKAP' or str(profile.get('subtype', '')).startswith('HANDIKAP')


def _v417_agf_allowed(profile):
    """AGF is a ranking signal only for low-data Maiden and Sartli 1 races."""
    return (
        profile.get('category') == 'MAIDEN'
        or profile.get('subtype') == 'SART1'
    )


def _v417_apply_agf_policy(profile, raw_weights):
    """Remove AGF outside its allowed groups and redistribute its weight."""
    weights = {k: float(raw_weights.get(k, 0.0)) for k in _V4_METRIC_KEYS}
    agf_weight = max(0.0, weights.get('agf_score', 0.0))
    allowed = _v417_agf_allowed(profile)
    if allowed or agf_weight <= 0.0:
        return weights, allowed

    weights['agf_score'] = 0.0
    category = profile.get('category')
    subtype = profile.get('subtype')

    if _v417_is_handikap_profile(profile):
        weights['pace_score'] += agf_weight * 0.35
        weights['handicap_efficiency_score'] += agf_weight * 0.25
        weights['handicap_class_transition_score'] += agf_weight * 0.25
        weights['distance_transition_score'] += agf_weight * 0.15
    elif subtype == 'SART4':
        weights['hp_score'] += agf_weight
    elif category == 'SARTLI':
        weights['degree_stability'] += agf_weight
    elif category == 'KV':
        weights['degree_trend'] += agf_weight * 0.75
        weights['hp_score'] += agf_weight * 0.25
    else:
        # Future profiles must also remain AGF-free unless explicitly allowed.
        weights['degree_stability'] += agf_weight

    return weights, False


def resolve_v4_profile_weights(profile):
    subtype = profile.get('subtype', 'GLOBAL')
    category = profile.get('category', 'GLOBAL')
    distance_bucket = profile.get('distanceBucket', 'unknown')
    field_bucket = profile.get('fieldBucket', 'unknown')
    track_bucket = profile.get('track', 'Unknown')

    candidates = [
        (f"{subtype}|{distance_bucket}|{field_bucket}|{track_bucket}", 'exact'),
        (f"{subtype}|{distance_bucket}|{field_bucket}", 'subtype_distance_field'),
        (f"{subtype}|{track_bucket}", 'subtype_track'),
        (subtype, 'subtype'),
        (category, 'category'),
        ('GLOBAL', 'global'),
    ]

    selected_key = 'GLOBAL'
    fallback_level = 'global'
    selected = _V4_WEIGHT_PROFILES['GLOBAL']
    for key, level in candidates:
        if key in _V4_WEIGHT_PROFILES:
            selected_key = key
            fallback_level = level
            selected = _V4_WEIGHT_PROFILES[key]
            break

    sample_races = int(selected.get('sample_races', 0))
    min_required = _V4_MIN_SAMPLE_RACES.get(fallback_level, 0)
    eligible = sample_races >= min_required
    policy_weights, agf_allowed = _v417_apply_agf_policy(
        profile,
        selected.get('weights', {}),
    )
    weights = _v4_normalize_weights(policy_weights)

    if eligible:
        confidence_score = 0.75 if fallback_level != 'global' else 0.45
        confidence_label = 'eligible-shadow'
    elif sample_races > 0:
        confidence_score = round(max(0.20, min(0.60, sample_races / max(min_required, 1))), 2)
        confidence_label = 'candidate-shadow'
    else:
        confidence_score = 0.20
        confidence_label = 'fallback-shadow'

    return {
        'selectedKey': selected_key,
        'fallbackLevel': fallback_level,
        'sampleRaces': sample_races,
        'minRequired': min_required,
        'eligible': eligible,
        'status': selected.get('status', 'fallback_shadow'),
        'confidenceScore': confidence_score,
        'confidenceLabel': confidence_label,
        'weights': weights,
        'weightsPct': {k: round(v * 100, 1) for k, v in weights.items() if v > 0},
        'agfAllowedForRanking': agf_allowed,
    }


def _parse_race_distance(value):
    try:
        digits = re.sub(r'[^0-9]', '', str(value or ''))
        return int(digits) if digits else None
    except (ValueError, TypeError):
        return None


def calculate_v4_ranking_penalties(races, race_date_str=None, kgs_value=None):
    """Return explicit rest penalties without considering training activity."""
    reference_date = None
    try:
        reference_date = datetime.strptime(str(race_date_str or '').strip(), '%d.%m.%Y')
    except (ValueError, TypeError):
        pass

    last_race = None
    last_race_date = None
    if reference_date:
        dated_races = []
        for race in races or []:
            try:
                parsed_date = datetime.strptime(str(race.get('date', '')).strip(), '%d.%m.%Y')
            except (ValueError, TypeError):
                continue
            if parsed_date < reference_date:
                dated_races.append((parsed_date, race))
        if dated_races:
            last_race_date, last_race = max(dated_races, key=lambda item: item[0])

    days_since_last_race = None
    rest_source = 'unknown'
    if reference_date and last_race_date:
        days_since_last_race = max(0, (reference_date - last_race_date).days)
        rest_source = 'race_history'
    else:
        try:
            parsed_kgs = int(float(str(kgs_value or '').replace(',', '.').strip()))
            if parsed_kgs >= 0:
                days_since_last_race = parsed_kgs
                rest_source = 'kgs'
        except (ValueError, TypeError):
            pass

    if last_race is None and races:
        last_race = races[0]
    last_race_distance = _parse_race_distance(
        last_race.get('distance') if last_race else None
    )

    penalties = []
    if days_since_last_race is not None:
        if days_since_last_race >= 91:
            penalties.append({'code': 'long_layoff_91_plus', 'points': 11.0})
        elif days_since_last_race >= 61:
            penalties.append({'code': 'long_layoff_61_90', 'points': 7.0})
        elif days_since_last_race >= 40:
            penalties.append({'code': 'long_layoff_40_60', 'points': 5.0})

        if (
            days_since_last_race <= 5
            and last_race_distance is not None
            and last_race_distance > 1800
        ):
            penalties.append({'code': 'recent_long_race', 'points': 6.0})

    return {
        'daysSinceLastRace': days_since_last_race,
        'lastRaceDistance': last_race_distance,
        'restDataSource': rest_source,
        'penalties': penalties,
        'totalPenalty': round(sum(item['points'] for item in penalties), 1),
    }


def calculate_v4_shadow_score(metrics, weights):
    weighted_sum = 0.0
    total = 0.0
    source_guards = {
        'agf_score': '_has_agf',
        'hp_score': '_has_hp',
        'weight_impact': '_has_weight',
        'jockey_score': '_has_jockey',
        'trainer_score': '_has_trainer',
        'training_fitness': '_has_training',
        'training_degree_score': '_has_training_times',
        'pedigree': '_has_pedigree',
        'age_score': '_has_age',
        'track_experience_score': '_has_track_experience',
        'surface_transition_score': '_has_surface_transition',
        'distance_transition_score': '_has_distance_transition',
        'handicap_efficiency_score': '_has_handicap_efficiency',
        'handicap_weight_relief_score': '_has_handicap_weight_relief',
        'handicap_class_transition_score': '_has_handicap_class_history',
        'field_relative_value_score': '_has_field_relative_value',
        'pace_map_edge_score': '_has_pace_map_edge',
        'surface_switch_safety_score': '_has_surface_switch_safety',
        'favorite_risk_guard_score': '_has_favorite_risk_guard',
        'class_peak_score': '_has_class_peak',
        'elite_consensus_score': '_has_elite_consensus',
    }
    for key, weight in weights.items():
        if weight <= 0:
            continue
        guard_key = source_guards.get(key)
        if guard_key and guard_key in metrics and not metrics.get(guard_key, False):
            continue
        try:
            value = float(metrics.get(key, 50.0))
        except (ValueError, TypeError):
            value = 50.0
        if key == 'agf_score' and metrics.get('_v4_handikap_agf_capped'):
            value = max(35.0, min(82.0, value))
        weighted_sum += value * weight
        total += weight
    if total <= 0:
        return 50.0
    return round(max(0.0, min(100.0, weighted_sum / total)), 1)


def calculate_v4_data_quality(scored_horses):
    scores = []
    for horse in scored_horses:
        try:
            scores.append(float(horse.get('v4Score', 0.0) or 0.0))
        except (ValueError, TypeError):
            scores.append(0.0)

    runner_count = len(scores)
    zero_count = sum(1 for score in scores if score <= 0.0)
    valid_count = runner_count - zero_count
    all_zero = runner_count > 0 and valid_count == 0
    missing_metrics_count = sum(1 for horse in scored_horses if not horse.get('_mf'))
    detail_fetch_failed_count = sum(
        1 for horse in scored_horses
        if horse.get('detailFetchStatus') not in (None, '', 'ok', 'empty_history')
    )
    source_flags = [horse.get('metricSourceFlags', {}) or {} for horse in scored_horses]
    training_source_count = sum(1 for flags in source_flags if flags.get('hasTraining'))
    agf_source_count = sum(1 for flags in source_flags if flags.get('hasAgf'))
    pedigree_source_count = sum(1 for flags in source_flags if flags.get('hasPedigree'))
    trainer_source_count = sum(1 for flags in source_flags if flags.get('hasTrainer'))
    age_source_count = sum(1 for flags in source_flags if flags.get('hasAgeActionable'))
    track_experience_count = sum(1 for flags in source_flags if flags.get('hasTrackExperience'))
    handicap_efficiency_count = sum(1 for flags in source_flags if flags.get('hasHandicapEfficiency'))
    handicap_weight_relief_count = sum(1 for flags in source_flags if flags.get('hasHandicapWeightRelief'))
    field_relative_count = sum(1 for flags in source_flags if flags.get('hasFieldRelativeValue'))
    pace_map_edge_count = sum(1 for flags in source_flags if flags.get('hasPaceMapEdge'))
    surface_switch_safety_count = sum(1 for flags in source_flags if flags.get('hasSurfaceSwitchSafety'))
    favorite_risk_guard_count = sum(1 for flags in source_flags if flags.get('hasFavoriteRiskGuard'))
    class_peak_count = sum(1 for flags in source_flags if flags.get('hasClassPeak'))
    elite_consensus_count = sum(1 for flags in source_flags if flags.get('hasEliteConsensus'))

    return {
        'zeroScoreCount': zero_count,
        'validRunnerCount': valid_count,
        'missingMetricsCount': missing_metrics_count,
        'detailFetchFailedCount': detail_fetch_failed_count,
        'sourceCoverage': {
            'trainingCount': training_source_count,
            'agfCount': agf_source_count,
            'pedigreeCount': pedigree_source_count,
            'trainerCount': trainer_source_count,
            'ageCount': age_source_count,
            'trackExperienceCount': track_experience_count,
            'handicapEfficiencyCount': handicap_efficiency_count,
            'handicapWeightReliefCount': handicap_weight_relief_count,
            'fieldRelativeValueCount': field_relative_count,
            'paceMapEdgeCount': pace_map_edge_count,
            'surfaceSwitchSafetyCount': surface_switch_safety_count,
            'favoriteRiskGuardCount': favorite_risk_guard_count,
            'classPeakCount': class_peak_count,
            'eliteConsensusCount': elite_consensus_count,
            'runnerCount': runner_count,
        },
        'allZeroRace': all_zero,
        'lowDataRace': all_zero or valid_count < 3 or detail_fetch_failed_count > runner_count * 0.4,
    }


def calculate_softmax_probabilities(scores, temperature=18.0):
    numeric_scores = []
    for score in scores:
        try:
            numeric_scores.append(float(score or 0.0))
        except (ValueError, TypeError):
            numeric_scores.append(0.0)

    if not any(score > 0 for score in numeric_scores):
        return [0.0 for _ in numeric_scores]

    import math
    max_score = max(numeric_scores)
    exp_scores = [math.exp((score - max_score) / temperature) for score in numeric_scores]
    exp_total = sum(exp_scores) or 1.0
    return [round((exp_score / exp_total) * 100, 1) for exp_score in exp_scores]


def attach_sort_metrics(analyzed_horses):
    """Expose data-backed metric values for client-side ranking lenses.

    The scoring engine uses 50 as a neutral fallback when a source is missing.
    Ranking filters should not present that neutral fallback as real data.
    """
    metric_keys = [
        'form', 'degree', 'training', 'trainingFitness', 'pace',
        'distance', 'hp', 'jockey', 'pedigree', 'weight', 'age',
    ]

    def as_float(value):
        try:
            return round(float(value), 1)
        except (ValueError, TypeError):
            return None

    def as_int(value, default=0):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def raw_hp_is_valid(value):
        return str(value or '').strip().isdigit()

    for horse in analyzed_horses:
        metrics = horse.get('_mf', {}) or {}
        training_info = horse.get('trainingInfo') or {}
        pace_info = horse.get('paceInfo') or {}
        pedigree_info = horse.get('pedigreeInfo') or {}
        degree_stats = horse.get('degreeStats') or {}
        jockey_stats = horse.get('jockeyStats') or {}

        race_count = as_int(horse.get('raceCount'))
        filtered_count = as_int(horse.get('filteredRaceCount'), as_int(metrics.get('_dist_races')))
        has_training = isinstance(training_info, dict) and bool(training_info.get('hasData'))
        has_training_times = has_training and bool(training_info.get('times'))
        degree_race_count = as_int(degree_stats.get('raceCount'))
        has_degree = degree_race_count > 0 and bool(
            degree_stats.get('recentBestDegree') or degree_stats.get('avgDegree')
        )
        has_pedigree = (
            isinstance(pedigree_info, dict)
            and pedigree_info.get('dataQuality') != 'NONE'
            and as_int(pedigree_info.get('totalOffspringRaces')) > 0
        )
        has_jockey = isinstance(jockey_stats, dict) and as_int(jockey_stats.get('totalRaces')) > 0
        source_flags = horse.get('metricSourceFlags') or {}
        has_hp = bool(source_flags.get('hasHp')) if 'hasHp' in source_flags else raw_hp_is_valid(horse.get('rawHp'))
        has_weight = horse.get('weightChange') is not None
        has_age = bool(source_flags.get('hasAgeActionable'))

        training_degree = as_float(
            training_info.get('trainingDegreeScore') if isinstance(training_info, dict) else None
        )
        training_fitness = as_float(
            training_info.get('fitnessScore') if isinstance(training_info, dict) else None
        )
        # If race-degree projection is neutral because the horse has no race degree,
        # use the direct fitness signal instead of exposing another fake 50.
        training_sort = training_degree
        if training_sort is None or (abs(training_sort - 50.0) < 0.01 and not has_degree):
            training_sort = training_fitness

        horse['sortMetrics'] = {
            'overall': round(float(horse.get('aiScore', 0) or 0), 1),
            'form': as_float(metrics.get('form_trend')) if race_count >= 2 else None,
            'degree': as_float(degree_stats.get('degreeScore') or metrics.get('degree_avg')) if has_degree else None,
            'training': training_sort if has_training_times else None,
            'trainingFitness': training_fitness if has_training_times else None,
            'pace': as_float(pace_info.get('paceScore') if isinstance(pace_info, dict) else metrics.get('pace_score')) if race_count > 0 else None,
            'distance': as_float(metrics.get('distance_suit')) if filtered_count > 0 else None,
            'hp': as_float(horse.get('hpScore')) if has_hp else None,
            'jockey': as_float(metrics.get('jockey_score')) if has_jockey else None,
            'pedigree': as_float(pedigree_info.get('pedigreeScore')) if has_pedigree else None,
            'weight': as_float(metrics.get('weight_impact')) if has_weight else None,
            'age': as_float(metrics.get('age_score')) if has_age else None,
        }

    for key in metric_keys:
        values = [
            horse.get('sortMetrics', {}).get(key)
            for horse in analyzed_horses
            if horse.get('sortMetrics', {}).get(key) is not None
        ]
        if len(values) >= 2 and max(values) - min(values) < 0.1:
            for horse in analyzed_horses:
                horse.get('sortMetrics', {})[key] = None

    for horse in analyzed_horses:
        metrics = horse.get('sortMetrics', {})
        horse['sortMetricAvailability'] = {
            key: metrics.get(key) is not None
            for key in metric_keys
        }


def resolve_v4_decision(profile, resolved):
    """Classify v4 output now that v4 is the default visible ranking."""
    category = profile.get('category', 'GLOBAL')
    return {
        'mode': 'default_visible',
        'useForRanking': True,
        'reason': (
            f'{category} v{_V4_VERSION} default ranking: visible ranking uses v4 score; '
            'legacy score is no longer calculated for new analyses.'
        ),
    }


def apply_v4_shadow_mode(analyzed_horses, race_type='', distance='', track=''):
    """Attach v4 fields and make v4 the default visible ranking for every group."""
    profile = extract_v4_race_profile(
        race_type=race_type,
        distance=distance,
        track=track,
        field_size=len(analyzed_horses),
    )
    resolved = resolve_v4_profile_weights(profile)
    decision = resolve_v4_decision(profile, resolved)
    weights = resolved['weights']

    scored = []
    is_handikap_profile = _v417_is_handikap_profile(profile)
    for horse in analyzed_horses:
        metrics = horse.get('_mf', {}) or {}
        if metrics:
            metrics['_v4_handikap_agf_capped'] = bool(is_handikap_profile and resolved['agfAllowedForRanking'])
        v4_base_score = calculate_v4_shadow_score(metrics, weights) if metrics else 0.0
        try:
            penalty_total = max(0.0, float(horse.get('v4PenaltyTotal', 0.0) or 0.0))
        except (ValueError, TypeError):
            penalty_total = 0.0
        v4_score = round(max(0.0, min(100.0, v4_base_score - penalty_total)), 1)
        horse['v4Version'] = _V4_VERSION
        horse['v4BaseScore'] = v4_base_score
        horse['v4PenaltyTotal'] = penalty_total
        horse['v4Score'] = v4_score
        horse['agfAllowedForRanking'] = resolved['agfAllowedForRanking']
        horse['v4Mode'] = 'visible'
        horse['v4DecisionMode'] = decision['mode']
        horse['v4UseForRanking'] = decision['useForRanking']
        horse['v4Reason'] = decision['reason']
        horse['v4Profile'] = {
            **profile,
            'selectedKey': resolved['selectedKey'],
            'fallbackLevel': resolved['fallbackLevel'],
        }
        horse['v4Weights'] = resolved['weightsPct']
        horse['v4Confidence'] = {
            'score': resolved['confidenceScore'],
            'label': resolved['confidenceLabel'],
            'sampleRaces': resolved['sampleRaces'],
            'minRequired': resolved['minRequired'],
            'eligible': resolved['eligible'],
            'status': resolved['status'],
        }
        scored.append(horse)

    data_quality = calculate_v4_data_quality(scored)
    for horse in scored:
        horse['v4DataQuality'] = data_quality

    scored.sort(key=lambda h: h.get('v4Score', 0), reverse=True)
    for index, horse in enumerate(scored):
        horse['v4Rank'] = index + 1

    use_visible_v4 = bool(decision['useForRanking'])

    for horse in scored:
        horse['v4AppliedForRanking'] = use_visible_v4
        horse['v4UseForRanking'] = use_visible_v4
        horse['v4Reason'] = decision['reason']
        if use_visible_v4:
            v4_score = horse.get('v4Score', 0)
            horse['aiScore'] = v4_score
            horse['v4Mode'] = 'visible'
            metrics = horse.get('_mf', {}) or {}
            if metrics:
                horse['prediction'] = generate_prediction(v4_score, metrics)
                horse['insight'] = generate_insight(horse.get('name', ''), metrics, v4_score)

    print(
        f"[V4 ROLLOUT] profile={profile.get('profileKey')} "
        f"selected={resolved['selectedKey']} level={resolved['fallbackLevel']} "
        f"sample={resolved['sampleRaces']}/{resolved['minRequired']} "
        f"decision={decision['mode']} visible={use_visible_v4} legacy=disabled version={_V4_VERSION} "
        f"valid={data_quality['validRunnerCount']} zero={data_quality['zeroScoreCount']}"
    )


# AGF is intentionally excluded from the ML feature list.
_ML_FEATURE_KEYS = [
    "degree_avg", "degree_trend", "degree_stability",
    "form_trend", "track_suit", "track_experience_score",
    "surface_transition_score", "distance_suit", "distance_transition_score",
    "training_fitness", "training_degree_score",
    "weight_impact", "handicap_efficiency_score",
    "handicap_weight_relief_score", "handicap_class_transition_score",
    "field_relative_value_score", "pace_map_edge_score",
    "surface_switch_safety_score", "favorite_risk_guard_score",
    "class_peak_score", "elite_consensus_score",
    "running_style_proxy_score",
    "jockey_score", "bounce_score",
    "pace_score", "pedigree", "hp_score", "trainer_score",
]


def predict_ml_score(metrics):
    """
    FAZ 8: Tek bir at için XGBoost ML skoru döndürür.
    Returns: float veya None (model yoksa)
    """
    if _ml_model is None or not _ml_feature_cols:
        return None

    try:
        # Feature vektörü oluştur (eğitim sırasındaki sıra ile)
        feature_vec = []
        for col in _ml_feature_cols:
            if col in metrics:
                feature_vec.append(float(metrics.get(col, 50.0)))
            elif col == "field_size":
                feature_vec.append(float(metrics.get("_field_size", 10)))
            elif col.endswith("_zscore"):
                feature_vec.append(0.0)  # Z-score koşu seviyesinde hesaplanacak
            elif col == "top3_feature_avg":
                vals = sorted([metrics.get(k, 50.0) for k in _ML_FEATURE_KEYS])
                feature_vec.append(float(np.mean(vals[-3:])))
            elif col == "feature_variance":
                vals = [metrics.get(k, 50.0) for k in _ML_FEATURE_KEYS]
                feature_vec.append(float(np.var(vals)))
            elif col.startswith("is_"):
                race_type = (metrics.get("_race_type", "") or "").lower()
                if col == "is_handicap":
                    feature_vec.append(1.0 if any(k in race_type for k in ["handikap", "hk"]) else 0.0)
                elif col == "is_maiden":
                    feature_vec.append(1.0 if any(k in race_type for k in ["maiden", "mdn"]) else 0.0)
                elif col == "is_conditions":
                    feature_vec.append(1.0 if any(k in race_type for k in ["şartlı", "sartli"]) else 0.0)
                elif col == "is_kv":
                    feature_vec.append(1.0 if "kv" in race_type else 0.0)
                else:
                    feature_vec.append(0.0)
            else:
                feature_vec.append(50.0)

        X = np.array([feature_vec], dtype=np.float32)
        raw_score = float(_ml_model.predict(X)[0])
        return raw_score
    except Exception as e:
        print(f"[ML] Tahmin hatası: {e}")
        return None


def calculate_blend_alpha(metrics):
    """
    FAZ 8: Dinamik blend oranı (α=kural, β=ML, α+β=1.0).
    Veri durumuna göre ML'e ne kadar güvenileceğini belirler.
    """
    total_races = metrics.get("_total_races", 0)

    if _ml_model is None:
        return 1.0  # Saf kural tabanlı

    if total_races == 0:
        return 0.90  # Maiden — ML çaresiz

    # Varsayılan: α=0.55 (kural ağırlıklı)
    return 0.55


def calculate_ai_score(metrics):
    """
    FAZ 8: Hibrit Blend Skoru.
    α × master_score + (1-α) × ml_score_normalized
    ML model yoksa saf master_score döner (geriye uyumlu).
    """
    master_score, _, _, _ = calculate_master_score(metrics)

    if _ml_model is None:
        return master_score

    ml_raw = predict_ml_score(metrics)
    if ml_raw is None:
        return master_score

    # ML raw score'u henüz normalize edemeyiz (tek at);
    # Koşu-seviyesinde normalizasyon PASS 2'de yapılacak.
    # Şimdilik sadece master_score döndür, blend PASS 2'de uygulanır.
    return master_score


def _shadow_safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except (ValueError, TypeError):
        return float(default)


def _shadow_feature_dict(metrics, horse=None, field_size=0, race_type='', distance='', track=''):
    horse = horse or {}
    metrics = metrics or {}
    flags = horse.get('metricSourceFlags') or {}
    profile = horse.get('v4Profile') or {}
    race_type_text = str(race_type or metrics.get('_race_type') or profile.get('category') or '')
    folded_type = _v4_fold_text(race_type_text)
    track_bucket = _v4_track_bucket(track or profile.get('track') or '')

    try:
        distance_num = int(re.sub(r'[^0-9]', '', str(distance or 0)) or 0)
    except (ValueError, TypeError):
        distance_num = 0

    score_keys = [
        'degree_avg', 'degree_trend', 'degree_stability', 'form_trend',
        'track_suit', 'track_experience_score', 'surface_transition_score',
        'distance_suit', 'distance_transition_score',
        'training_fitness', 'training_degree_score', 'weight_impact',
        'handicap_efficiency_score', 'handicap_weight_relief_score',
        'field_relative_value_score', 'pace_map_edge_score',
        'surface_switch_safety_score', 'favorite_risk_guard_score',
        'class_peak_score', 'elite_consensus_score',
        'jockey_score', 'bounce_score', 'pace_score', 'pedigree',
        'hp_score', 'trainer_score', 'agf_score', 'age_score',
    ]
    features = {key: _shadow_safe_float(metrics.get(key), 50.0) for key in score_keys}
    agf_allowed = bool(
        horse.get(
            'agfAllowedForRanking',
            profile.get('category') == 'MAIDEN'
            or profile.get('subtype') == 'SART1'
        )
    )
    if not agf_allowed:
        agf_stat = _ml_shadow_feature_stats.get('agf_score', {}) if isinstance(_ml_shadow_feature_stats, dict) else {}
        features['agf_score'] = _shadow_safe_float(agf_stat.get('mean'), 50.0)
    features.update({
        'handicap_class_transition_score': _shadow_safe_float(metrics.get('handicap_class_transition_score'), 50.0),
        'handicap_class_delta': _shadow_safe_float(metrics.get('handicap_class_delta'), 0.0),
        'handicap_weight_relief_score': _shadow_safe_float(metrics.get('handicap_weight_relief_score'), 50.0),
        'field_relative_value_score': _shadow_safe_float(metrics.get('field_relative_value_score'), 50.0),
        'pace_map_edge_score': _shadow_safe_float(metrics.get('pace_map_edge_score'), 50.0),
        'surface_switch_safety_score': _shadow_safe_float(metrics.get('surface_switch_safety_score'), 50.0),
        'favorite_risk_guard_score': _shadow_safe_float(metrics.get('favorite_risk_guard_score'), 50.0),
        'class_peak_score': _shadow_safe_float(metrics.get('class_peak_score'), 50.0),
        'elite_consensus_score': _shadow_safe_float(metrics.get('elite_consensus_score'), 50.0),
        'running_style_proxy_score': _shadow_safe_float(metrics.get('running_style_proxy_score'), 50.0),
        'pace_pressure': _shadow_safe_float(metrics.get('pace_pressure'), 0.0),
    })

    features.update({
        'v4_score': _shadow_safe_float(horse.get('v4Score'), 0.0),
        'v4_rank': _shadow_safe_float(horse.get('v4Rank'), field_size or 0),
        'field_size': _shadow_safe_float(field_size, 0.0),
        'distance_num': _shadow_safe_float(distance_num, 0.0),
        'is_handikap': 1.0 if 'HANDIKAP' in folded_type else 0.0,
        'is_maiden': 1.0 if 'MAIDEN' in folded_type or 'MDN' in folded_type else 0.0,
        'is_sartli': 1.0 if 'SART' in folded_type else 0.0,
        'is_sart1': 1.0 if profile.get('subtype') == 'SART1' else 0.0,
        'is_kv': 1.0 if 'KV' in folded_type else 0.0,
        'is_grup': 1.0 if 'GRUP' in folded_type or re.search(r'\bG\s*[-/]?\s*[123]\b', folded_type) else 0.0,
        'is_satis': 1.0 if 'SATIS' in folded_type else 0.0,
        'track_kum': 1.0 if track_bucket == 'Kum' else 0.0,
        'track_cim': 1.0 if track_bucket == 'Cim' else 0.0,
        'track_sentetik': 1.0 if track_bucket == 'Sentetik' else 0.0,
        'has_training': 1.0 if flags.get('hasTraining') else 0.0,
        'has_agf': 1.0 if agf_allowed and flags.get('hasAgf') else 0.0,
        'has_hp': 1.0 if flags.get('hasHp') else 0.0,
        'has_pedigree': 1.0 if flags.get('hasPedigree') else 0.0,
        'has_trainer': 1.0 if flags.get('hasTrainer') else 0.0,
        'has_age_actionable': 1.0 if flags.get('hasAgeActionable') else 0.0,
        'has_track_experience': 1.0 if flags.get('hasTrackExperience') else 0.0,
        'has_surface_transition': 1.0 if flags.get('hasSurfaceTransition') else 0.0,
        'has_distance_transition': 1.0 if flags.get('hasDistanceTransition') else 0.0,
        'has_handicap_efficiency': 1.0 if flags.get('hasHandicapEfficiency') else 0.0,
        'has_handicap_weight_relief': 1.0 if flags.get('hasHandicapWeightRelief') else 0.0,
        'has_handicap_class_history': 1.0 if flags.get('hasHandicapClassHistory') else 0.0,
        'has_field_relative_value': 1.0 if flags.get('hasFieldRelativeValue') else 0.0,
        'has_pace_map_edge': 1.0 if flags.get('hasPaceMapEdge') else 0.0,
        'has_surface_switch_safety': 1.0 if flags.get('hasSurfaceSwitchSafety') else 0.0,
        'has_favorite_risk_guard': 1.0 if flags.get('hasFavoriteRiskGuard') else 0.0,
        'has_class_peak': 1.0 if flags.get('hasClassPeak') else 0.0,
        'has_elite_consensus': 1.0 if flags.get('hasEliteConsensus') else 0.0,
        'days_since_last_race': _shadow_safe_float(horse.get('daysSinceLastRace'), -1.0),
        'last_race_distance': _shadow_safe_float(horse.get('lastRaceDistance'), 0.0),
        'long_layoff_bucket': _shadow_safe_float(metrics.get('_long_layoff_bucket'), 0.0),
        'recent_long_race_flag': 1.0 if metrics.get('_recent_long_race_flag') else 0.0,
    })

    active_score_keys = [key for key in score_keys if agf_allowed or key != 'agf_score']
    metric_values = [features[key] for key in active_score_keys]
    features['top3_feature_avg'] = float(np.mean(sorted(metric_values)[-3:])) if metric_values else 50.0
    features['feature_variance'] = float(np.var(metric_values)) if metric_values else 0.0
    return features


def _shadow_feature_vector(feature_values):
    vector = []
    for col in _ml_shadow_feature_cols:
        if col in feature_values:
            vector.append(_shadow_safe_float(feature_values.get(col), 0.0))
            continue
        stat = _ml_shadow_feature_stats.get(col, {}) if isinstance(_ml_shadow_feature_stats, dict) else {}
        vector.append(_shadow_safe_float(stat.get('mean'), 0.0))
    return vector


def attach_shadow_ml_predictions(analyzed_horses, race_type='', distance='', track=''):
    """Attach optional ML shadow fields without changing visible v4 ranking."""
    model_version = _ml_shadow_metadata.get('model_version')
    unavailable_reason = None
    if _ml_shadow_model is None or not _ml_shadow_feature_cols:
        unavailable_reason = _ml_shadow_load_error or 'shadow model unavailable'

    raw_scores = []
    field_size = len(analyzed_horses)

    if unavailable_reason is None:
        for horse in analyzed_horses:
            metrics = horse.get('_mf', {}) or {}
            feature_values = _shadow_feature_dict(
                metrics,
                horse=horse,
                field_size=field_size,
                race_type=race_type,
                distance=distance,
                track=track,
            )
            try:
                X = np.array([_shadow_feature_vector(feature_values)], dtype=np.float32)
                raw_scores.append(float(_ml_shadow_model.predict(X)[0]))
            except Exception as err:
                print(f"[SHADOW-ML] Tahmin hatasi: {err}")
                unavailable_reason = str(err)
                break

    if unavailable_reason is not None:
        for horse in analyzed_horses:
            horse['mlShadowMode'] = 'unavailable'
            horse['mlModelVersion'] = model_version
            horse['mlShadowReason'] = unavailable_reason
            horse['mlShadowScore'] = None
            horse['mlShadowRank'] = None
            horse['mlWinProbability'] = None
            horse['mlTop3Probability'] = None
            horse['mlExpectedFinishRank'] = None
        return

    min_score = min(raw_scores)
    max_score = max(raw_scores)
    score_range = max(max_score - min_score, 1e-9)
    normalized_scores = [round((score - min_score) / score_range * 100.0, 1) for score in raw_scores]
    order = sorted(range(field_size), key=lambda idx: normalized_scores[idx], reverse=True)
    rank_by_index = {idx: rank + 1 for rank, idx in enumerate(order)}
    win_probs_by_rank_order = calculate_softmax_probabilities(
        [normalized_scores[idx] for idx in order],
        temperature=18.0,
    )
    win_prob_by_index = {
        idx: win_probs_by_rank_order[pos] if pos < len(win_probs_by_rank_order) else 0.0
        for pos, idx in enumerate(order)
    }

    import math as _math
    for idx, horse in enumerate(analyzed_horses):
        ml_rank = rank_by_index[idx]
        ml_score = normalized_scores[idx]
        expected_from_score = 1.0 + max(field_size - 1, 0) * (1.0 - (ml_score / 100.0))
        expected_finish = round((ml_rank * 0.65) + (expected_from_score * 0.35), 2)
        top3_probability = round(100.0 / (1.0 + _math.exp((ml_rank - 3.5) / 1.6)), 1)
        horse['mlShadowMode'] = _ML_SHADOW_MODE
        horse['mlModelVersion'] = model_version
        horse['mlShadowReason'] = 'shadow model only; visible v4 ranking is unchanged'
        horse['mlShadowScore'] = ml_score
        horse['mlShadowRank'] = ml_rank
        horse['mlWinProbability'] = win_prob_by_index.get(idx, 0.0)
        horse['mlTop3Probability'] = max(0.0, min(99.0, top3_probability))
        horse['mlExpectedFinishRank'] = expected_finish


def generate_prediction(ai_score, metrics):
    """
    FAZ 4.7: Zenginleştirilmiş tahmin etiketi.
    AI skoru + veri güveni + dinamik metrikler üstünden üretilir.
    """
    confidence, _ = calculate_data_confidence(metrics)
    total_races   = metrics.get('_total_races', 0)

    # Veri çok az → tahmin etiketi daha temkinli
    if total_races == 0:
        return "İlk Koşu 🔍"  # Maiden

    if ai_score >= 87:
        return "Favori ⭐"
    elif ai_score >= 78:
        if confidence >= 0.70:
            return "Güçlü Aday 🥇"
        else:
            return "Plase Adayı"
    elif ai_score >= 68:
        if metrics.get('form_trend_value', 0) > 0.5:
            return "Formda 📈"
        elif metrics.get('pedigree', 50) >= 70 and metrics.get('_track_races', 1) == 0:
            return "Pedigri Vaadi 🧬"
        else:
            return "Plase Adayı"
    elif ai_score >= 55:
        if metrics.get('bounce_score', 50) >= 70:
            return "Kondisyonda ✨"
        elif metrics.get('track_suit', 50) >= 75:
            return "Pist Uzmanı"
        elif metrics.get('pace_score', 50) >= 70:
            return "Tempo Avantajlı"
        else:
            return "İzlenmeli"
    else:
        if metrics.get('jockey_score', 50) >= 75:
            return "Jokey Faktörü"
        return "Zayıf Aday"


def generate_insight(name, metrics, ai_score):
    """
    FAZ 4.7: Zenginleştirilmiş, çok katmanlı Türkçe insight metni.
    11 katmandan en kritik 2 sinyali seçer.
    """
    insights = []
    total_races = metrics.get('_total_races', 0)

    # ─ Maiden (veri yok) ──────────────────────────────────────
    if total_races == 0:
        pedigree_s = metrics.get('pedigree', 50)
        has_t = metrics.get('_has_training', False)
        if pedigree_s >= 65:
            insights.append("Pedigri profili bu koşu için umut veriyor")
        if has_t:
            insights.append("İdman verileri tek somut referans")
        else:
            insights.append("Yarış geçmişi ve idman verisi yok")
        return " • ".join(insights[:2])

    # ─ K1: Hız / Derece ───────────────────────────────────────
    degree_avg = metrics.get('degree_avg', 50)
    if degree_avg >= 80:
        insights.append("Derecesi rakiplerine göre üstün")
    elif degree_avg <= 30:
        insights.append("Genel derecesi rakiplerine göre düşük")

    # ─ K4: Form & Momentum ─────────────────────────────────
    form_trend_v = metrics.get('form_trend_value', 0)
    if form_trend_v > 0.5:
        insights.append("Son yarışlarda güçlü yükseliş eğilimi")
    elif form_trend_v < -0.5:
        insights.append("Son yarışlarda düşüş eğilimi var")

    # ─ K5: İdman ─────────────────────────────────────────────
    training_deg = metrics.get('training_degree_score', 50)
    if training_deg >= 75 and metrics.get('_has_training', False):
        insights.append("İdman projeksiyonu yarış ortalamasının üzerinde")
    elif training_deg <= 30 and metrics.get('_has_training', False):
        insights.append("İdman projeksiyonu yarış ortalamasının altında")

    # ─ K3: Pist Uyumu ───────────────────────────────────────
    track_suit = metrics.get('track_suit', 50)
    track_races = metrics.get('_track_races', 0)
    if track_races == 0:
        insights.append("Bu pistte ilk kez koşuyor (belirsizlik)")
    elif track_suit >= 78:
        insights.append("Bu pist tipinde yüksek performans geçmişi")
    elif track_suit <= 30:
        insights.append("Bu pistte tarihî başarısı zayıf")

    # ─ K8: Bounce / Dinlenme ────────────────────────────────
    bounce = metrics.get('bounce_score', 50)
    if bounce <= 30:
        insights.append("Dinlenme süresi yetersiz veya çok uzun ara")
    elif bounce >= 78:
        insights.append("Optimal dinlenme süresinde")

    # ─ K9: Tempo ─────────────────────────────────────────────
    pace = metrics.get('pace_score', 50)
    if pace >= 70:
        insights.append("Koşu temposu koşu stiline uygun")
    elif pace <= 30:
        insights.append("Koşu temposu stiline karşı çıkıyor")

    # ─ K11: Pedigri ────────────────────────────────────────────
    pedigree = metrics.get('pedigree', 50)
    if pedigree >= 70 and track_races == 0:
        insights.append("Baba profili bu pistte umut vaat ediyor")

    if not insights:
        if ai_score >= 70:
            insights.append("Genel metriklerinde dengeli görünüyor")
        else:
            insights.append("Rakiplerine göre dezavantajlı konumda")

    return " • ".join(insights[:2])




@app.route('/api/analyze-race', methods=['POST'])
def analyze_race():
    """🧠 Gelişmiş Yarış Analizi ve Tahmin Modülü"""
    try:
        start_time = time.time()
        data = request.json
        horses = data.get('horses', [])
        target_distance = data.get('targetDistance', '')
        target_track = data.get('targetTrack', '')
        race_id   = data.get('raceId', '')    # İdman bilgileri için koşu ID'si
        race_type = data.get('raceType', '')   # FAZ 6.2: Koşu tipi (Handikap/Maiden/Şartlı...)
        race_date = data.get('raceDate', '')   # FAZ 7: ML log için koşu tarihi (dd.MM.yyyy)
        race_no   = data.get('raceNo', '')     # FAZ 7: ML log için koşu numarası
        
        if not horses:
            return jsonify({'success': False, 'error': 'At listesi boş'}), 400
        
        print(f"[ANALYZE] {len(horses)} at için analiz başlatıldı. Mesafe: {target_distance}, Pist: {target_track}, RaceId: {race_id}")
            
        # 1. İdman Verilerini Koşu ID'sine Göre Çek (Tek İstek)
        training_data_map = {}
        if race_id:
            print(f"[ANALYZE] Koşu {race_id} için idman verileri çekiliyor...")
            training_data_map = fetch_training_data_by_race_id(race_id)
        else:
            print(f"[ANALYZE] RaceId belirtilmedi, idman verileri çekilemeyecek")
        
        print(f"[ANALYZE] {len(training_data_map)} at için idman verisi bulundu")
        
        # FAZ 4.5: 2-PASS MİMARİSİ
        # ─────────────────────────────────────────────────────────────
        # PASS 1: Tüm atların yarış geçmişini paralel çek + koşu stillerini belirle
        # PASS 2: Koşu temposunu hesapla + her ata pace_score uygula → final AI Score
        # ─────────────────────────────────────────────────────────────
        
        # FAZ 5.5: Pedigri Hız Optimizasyonu (Paralel Çekim)
        # Sequential döngüyü bloklamaması için benzersiz babaları önden ThreadPool ile önbelleğe al!
        unique_sires = list(set([h.get('father', '').strip() for h in horses if h.get('father', '').strip()]))
        if unique_sires:
            print(f"[ANALYZE] {len(unique_sires)} farklı aygır (baba) paralel sorgulanıyor...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=7) as sire_executor:
                sire_futures = [sire_executor.submit(fetch_sire_offspring_stats, sire) for sire in unique_sires]
                concurrent.futures.wait(sire_futures)
            print(f"[ANALYZE] Pedigri verileri başarıyla önbelleğe alındı.")

        # FAZ 6.2: Antrenör Win-Rate Hız Optimizasyonu (Paralel Çekim)
        unique_trainers = list(set([h.get('trainer', '').strip() for h in horses if h.get('trainer', '').strip()]))
        if unique_trainers:
            print(f"[ANALYZE] {len(unique_trainers)} farklı antrenör paralel sorgulanıyor...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=7) as trainer_executor:
                trainer_futures = [trainer_executor.submit(fetch_trainer_stats, t) for t in unique_trainers]
                concurrent.futures.wait(trainer_futures)
            print(f"[ANALYZE] Antrenör verileri başarıyla önbelleğe alındı.")


        # FAZ 5.2: HP (Handikap) Normalizasyonu (Pass 1 öncesi hazırlık)
        valid_hps = []
        for h in horses:
            hp_str = str(h.get('hp', '')).strip()
            if hp_str.isdigit():
                valid_hps.append(int(hp_str))

        valid_current_weights = []
        for h in horses:
            parsed_weight = parse_carried_weight(h.get('weight', ''))
            if parsed_weight is not None:
                valid_current_weights.append(parsed_weight)

        folded_race_type = _v4_fold_text(race_type)
        is_handicap_race = 'HANDIKAP' in folded_race_type or 'HANDICAP' in folded_race_type
        
        race_max_hp = max(valid_hps) if valid_hps else 50
        race_min_hp = min(valid_hps) if valid_hps else 50
        hp_range = race_max_hp - race_min_hp if race_max_hp > race_min_hp else 1

        # FAZ 6.3: Yaş metriği (koşu içi normalize, aynı yaşta pasif)
        valid_age_values = []
        for h in horses:
            age_val = parse_horse_age(h.get('age', ''))
            if age_val is not None:
                valid_age_values.append(age_val)
        race_max_age = max(valid_age_values) if valid_age_values else None
        race_min_age = min(valid_age_values) if valid_age_values else None
        race_age_range = (
            race_max_age - race_min_age
            if race_max_age is not None and race_min_age is not None and race_max_age > race_min_age
            else 0
        )
        race_type_lower_for_age = (race_type or '').lower()
        age_supported_for_ranking = not any(k in race_type_lower_for_age for k in ['maiden', 'mdn', 'md'])
        print(f"[AGE] {len(valid_age_values)} at için yaş verisi bulundu, range={race_age_range}")

        # FAZ 6.2: AGF Normalizasyonu (Pass 1 öncesi hazırlık)
        valid_agf_values = []
        for h in horses:
            agf_val = parse_agf_percent(h.get('agf', ''))
            if agf_val is not None:
                valid_agf_values.append(agf_val)
        print(f"[AGF] {len(valid_agf_values)} at için geçerli AGF verisi bulundu")

        # PASS 1: Paralel veri çekme + stil belirleme
        intermediate_horses = []  # [{ original_horse, horse_data, style, ess, ... }]
        

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_horse = {executor.submit(fetch_horse_details_safe, horse, target_distance, race_date): horse for horse in horses}
            
            for future in concurrent.futures.as_completed(future_to_horse):
                original_horse = future_to_horse[future]
                horse_data = future.result()
                horse_name = original_horse.get('name', '')

                
                # İdman verisini al (Türkçe karakter uyumlu eşleştirme)
                import unicodedata
                import re
                
                def clean_horse_name(name):
                    """At ismini temizle - newline, boşluk, yarış no kaldır"""
                    if not name:
                        return ""
                    # Newline ve fazla boşlukları tek boşluğa çevir
                    name = re.sub(r'[\n\r\t]+', ' ', name)
                    name = re.sub(r'\s+', ' ', name).strip()
                    # Sondaki (X) yarış numarasını kaldır
                    name = re.sub(r'\s*\(\d+\)\s*$', '', name).strip()
                    return name
                
                def normalize_name(name):
                    """Türkçe karakterleri normalize et - BÜYÜK HARFE çevir"""
                    name = clean_horse_name(name)
                    if not name:
                        return ""
                    # Unicode normalize et
                    normalized = unicodedata.normalize('NFKC', name)
                    # Türkçe karakterleri standartlaştır
                    tr_map = {
                        'ı': 'I', 'i': 'I', 'İ': 'I',
                        'ğ': 'G', 'Ğ': 'G',
                        'ü': 'U', 'Ü': 'U',
                        'ş': 'S', 'Ş': 'S',
                        'ö': 'O', 'Ö': 'O',
                        'ç': 'C', 'Ç': 'C'
                    }
                    for tr_char, en_char in tr_map.items():
                        normalized = normalized.replace(tr_char, en_char)
                    return normalized.upper()
                
                horse_name_clean = clean_horse_name(horse_name)
                horse_name_normalized = normalize_name(horse_name)
                training_data = None
                
                # Debug: İlk at için karşılaştırma göster
                if len(intermediate_horses) == 0:
                    print(f"[DEBUG] Aranan (temiz): '{horse_name_clean}' -> '{horse_name_normalized}'")
                
                # Eşleşen anahtarı bul
                for key, value in training_data_map.items():
                    key_normalized = normalize_name(key)
                    if key_normalized == horse_name_normalized:
                        training_data = value
                        break
                
                if training_data:
                    print(f"[DEBUG] EŞLEŞME: {horse_name_clean} -> Training VAR")
                else:
                    print(f"[DEBUG] At: {horse_name_clean}, Training: YOK")
                training_fitness, training_label, days_since, training_best_time, training_best_distance = calculate_training_fitness(training_data, race_date)
                
                if horse_data is not None:
                    races = horse_data.get('races', [])
                    filtered_races = horse_data.get('filteredRaces', [])
                    degree_stats = horse_data.get('degreeStats', {})
                    detail_fetch_status = horse_data.get('detailFetchStatus', 'ok')
                    
                    # 2. Gelişmiş Metrikler — Derece bazlı
                    trend_value, trend_score, trend_label = calculate_form_trend(races)
                    consistency, consistency_label = calculate_consistency(races)
                    track_suit, track_label = calculate_track_suitability(races, target_track)
                    distance_suit, distance_label = calculate_distance_suitability(races, target_distance)
                    
                    # FAZ 2.2: İdman projeksiyonu hesapla (AI score'dan ÖNCE)
                    training_projection = None
                    training_deg_score = 50.0
                    if training_data:
                        avg_race_deg = degree_stats.get('avgDegree') if degree_stats else None
                        training_projection = project_training_to_race_distance(training_data, target_distance, avg_race_deg)
                        training_deg_score = calculate_training_degree_score(training_projection, avg_race_deg)
                    
                    # FAZ 4.2: Kilo değişimi — AI score'dan ÖNCE hesapla
                    current_weight = original_horse.get('weight', '').strip()
                    current_weight_value = parse_carried_weight(current_weight)
                    last_weight = races[0].get('weight', '').strip() if races else ''
                    weight_impact_score = calculate_weight_impact(current_weight, last_weight, target_distance)
                    
                    # FAZ 4.3+4.4: Jokey ve Bounce skorlarını metrics'ten ÖNCE hesapla
                    # (Jokey bilgisi aşağıda tam hesaplanacak; şimdi hızlı bir ön hesap)
                    _cur_jockey = original_horse.get('jockey', '').strip()
                    _jockey_races = [r for r in races if jockey_match(r.get('jockey', ''), _cur_jockey)]
                    _jockey_wins = sum(1 for r in _jockey_races if r.get('rank') == '1')
                    _jockey_stats_pre = {
                        'totalRaces': len(_jockey_races),
                        'wins': _jockey_wins,
                    } if _cur_jockey and _jockey_races else None
                    _last_jockey_race = races[0].get('jockey', '').strip() if races else ''
                    _jockey_changed_pre = bool(_cur_jockey and _last_jockey_race and not jockey_match(_cur_jockey, _last_jockey_race))
                    _training_jockey = training_data.get('trainingJockey', '') if training_data else ''
                    _jockey_training_match = bool(_cur_jockey and _training_jockey and jockey_match(_training_jockey, _cur_jockey))
                    has_jockey_source = bool(_jockey_stats_pre or _jockey_changed_pre or _jockey_training_match)
                    jockey_score_val = calculate_jockey_score(_jockey_stats_pre, _jockey_changed_pre, _training_jockey, _cur_jockey)
                    
                    bounce_score_val = calculate_bounce_score(races, race_date)
                    ranking_penalty_info = calculate_v4_ranking_penalties(
                        races,
                        race_date_str=race_date,
                        kgs_value=original_horse.get('kgs'),
                    )
                    
                    # ═══ FAZ A.2: HP Puanı — KOŞU-İÇİ GÖRELİ (TAM ARALIK 0-100) ═══
                    # Yüksek HP = güçlü at → doğrusal ödüllendirme
                    # Eski dar [15-90] aralığı kaldırıldı → tam [0-100] normalizasyon
                    raw_hp = str(original_horse.get('hp', '')).strip()
                    has_valid_hp = raw_hp.isdigit()
                    has_hp_source = has_valid_hp and len(valid_hps) >= 2 and race_max_hp > race_min_hp
                    horse_hp = int(raw_hp) if raw_hp.isdigit() else (race_min_hp if valid_hps else 50)
                    if not valid_hps or race_max_hp == race_min_hp:
                        hp_score_val = 50.0
                    else:
                        # TAM ARALIK: en düşük HP=0, en yüksek HP=100
                        hp_score_val = round(((horse_hp - race_min_hp) / hp_range) * 100.0, 1)
                        hp_score_val = max(0.0, min(100.0, hp_score_val))

                    has_handicap_efficiency = bool(
                        is_handicap_race
                        and has_valid_hp
                        and current_weight_value is not None
                        and len(valid_hps) >= 3
                        and len(valid_current_weights) >= 3
                    )
                    handicap_efficiency_score = calculate_handicap_efficiency_score(
                        horse_hp if has_valid_hp else None,
                        current_weight_value,
                        valid_hps,
                        valid_current_weights,
                    ) if has_handicap_efficiency else 50.0
                    handicap_weight_relief_score = calculate_handicap_weight_relief_score(
                        horse_hp if has_valid_hp else None,
                        current_weight_value,
                        valid_hps,
                        valid_current_weights,
                    ) if has_handicap_efficiency else 50.0

                    # Arka plan loglarına ve frontend'e dönmesi için original_horse içine yedekle
                    original_horse['_raw_hp'] = raw_hp if raw_hp else '-';
                    raw_agf = str(original_horse.get('agf', '')).strip()
                    has_valid_agf = parse_agf_percent(raw_agf) is not None
                    agf_score_val = calculate_agf_score(original_horse.get('agf', ''), valid_agf_values)
                    has_weight_source = bool(current_weight and last_weight)
                    raw_age = str(original_horse.get('age', '')).strip()
                    horse_age = parse_horse_age(raw_age)
                    has_age_source = horse_age is not None
                    has_age_actionable = bool(
                        has_age_source
                        and len(valid_age_values) >= 2
                        and race_age_range > 0
                        and age_supported_for_ranking
                    )
                    age_score_val = calculate_age_score(horse_age, valid_age_values, race_type)
                    track_experience_score_val, target_track_race_count, other_track_race_count, track_experience_reason = calculate_track_experience_score(
                        races,
                        target_track,
                        track_suit,
                    )
                    has_track_experience = bool(target_track_race_count > 0 or other_track_race_count > 0)
                    surface_transition = calculate_surface_transition_score(
                        races,
                        target_track,
                        track_suit,
                    )
                    has_surface_transition = bool(
                        surface_transition['targetTrackRaceCount'] > 0
                        or surface_transition['otherTrackRaceCount'] > 0
                    )
                    distance_transition = calculate_distance_transition_score(
                        races,
                        target_distance,
                        distance_suit,
                    )
                    has_distance_transition = bool(distance_transition['lastDistance'] is not None)
                    handicap_class_transition = calculate_handicap_class_transition(races, race_type)

                    metrics_pass1 = {
                        'degree_avg': degree_stats.get('degreeScore', 50),
                        'degree_trend': degree_stats.get('trendScore', 50),
                        'degree_stability': degree_stats.get('stabilityScore', 50),
                        'form_trend': trend_score,
                        'form_trend_value': trend_value,
                        'consistency': consistency,
                        'track_suit': track_suit,
                        'track_experience_score': track_experience_score_val,
                        'surface_transition_score': surface_transition['score'],
                        'distance_suit': distance_suit,
                        'distance_transition_score': distance_transition['score'],
                        'training_fitness': training_fitness,
                        'training_degree_score': training_deg_score,
                        'weight_impact': weight_impact_score,   # FAZ 4.2
                        # Shadow feature: HP ile taşınan kiloyu birlikte değerlendirir.
                        'handicap_efficiency_score': handicap_efficiency_score,
                        'handicap_weight_relief_score': handicap_weight_relief_score,
                        'handicap_class_transition_score': handicap_class_transition['score'],
                        'handicap_class_delta': handicap_class_transition['delta'],
                        'running_style_proxy_score': 50.0,
                        'field_relative_value_score': 50.0,
                        'pace_map_edge_score': 50.0,
                        'surface_switch_safety_score': 50.0,
                        'favorite_risk_guard_score': 50.0,
                        'class_peak_score': 50.0,
                        'elite_consensus_score': 50.0,
                        'pace_pressure': 0.0,
                        'jockey_score': jockey_score_val,       # FAZ 4.3
                        'bounce_score': bounce_score_val,       # FAZ 4.4
                        'pace_score': 50.0,                     # FAZ 4.5: PASS 2'de güncellenecek (nötr placeholder)
                        'pedigree': 50.0,                       # FAZ 4.6: pedigri skoru (placeholder)
                        'pedigree_weight': 0.03,                # FAZ 4.6: dinamik ağırlık (placeholder)
                        'hp_score': hp_score_val,               # FAZ 5.2: Handikap Puanı normalizasyonu
                        'agf_score': agf_score_val,             # FAZ 6.2: AGF piyasa sinyali
                        'trainer_score': 50.0,                  # FAZ 6.2: Antrenör skoru (aşağıda güncellenecek)
                        'age_score': age_score_val,             # FAZ 6.3: Yaş sinyali
                        # FAZ 4.7: calculate_dynamic_weights için meta alanlar
                        '_total_races':   len(races),
                        '_track_races':   sum(1 for r in races if _track_key(r.get('track', '')) == _track_key(target_track)) if target_track else 0,
                        '_dist_races':    len(filtered_races),
                        '_has_training':  training_data is not None,
                        '_has_training_times': bool(training_data and training_data.get('times')),
                        '_has_training_projection': training_projection is not None,
                        '_has_pedigree':  False,  # Pe4.6 sonrası güncellenecek
                        '_has_agf':       has_valid_agf,
                        '_has_hp':        has_hp_source,
                        '_has_weight':    has_weight_source,
                        '_has_handicap_efficiency': has_handicap_efficiency,
                        '_has_handicap_weight_relief': has_handicap_efficiency,
                        '_has_handicap_class_history': handicap_class_transition['hasHistory'],
                        '_has_field_relative_value': False,
                        '_has_pace_map_edge': False,
                        '_has_surface_switch_safety': False,
                        '_has_favorite_risk_guard': False,
                        '_has_class_peak': False,
                        '_has_elite_consensus': False,
                        '_has_jockey':    has_jockey_source,
                        '_has_age':       has_age_actionable,
                        '_has_track_experience': has_track_experience,
                        '_has_surface_transition': has_surface_transition,
                        '_has_distance_transition': has_distance_transition,
                        '_days_since_last_race': ranking_penalty_info['daysSinceLastRace'],
                        '_last_race_distance': ranking_penalty_info['lastRaceDistance'],
                        '_long_layoff_bucket': (
                            3 if (ranking_penalty_info['daysSinceLastRace'] or 0) >= 91
                            else 2 if (ranking_penalty_info['daysSinceLastRace'] or 0) >= 61
                            else 1 if (ranking_penalty_info['daysSinceLastRace'] or 0) >= 40
                            else 0
                        ),
                        '_recent_long_race_flag': any(
                            item.get('code') == 'recent_long_race'
                            for item in ranking_penalty_info['penalties']
                        ),
                        '_race_type':     race_type,  # FAZ 6.2: Koşu tipine özel ağırlık profili
                        '_horse_races':   races,       # Konsensüs: grup ayarlaması için geçmiş yarışlar
                    }
                    # FAZ 4.6: Pedigri (baba) skoru — cache'li TJK çekimi
                    sire_name = original_horse.get('father', '').strip()
                    sire_stats = fetch_sire_offspring_stats(sire_name) if sire_name else None
                    pedigree_score_val, track_compat, dist_compat = calculate_pedigree_score(
                        sire_stats, target_track, target_distance
                    )
                    pedigree_weight_val = calculate_pedigree_weight(races, target_track, target_distance)

                    # metrics_pass1 güncellemesi (pedigri + antrenör + meta)
                    metrics_pass1['pedigree']        = pedigree_score_val
                    metrics_pass1['pedigree_weight'] = pedigree_weight_val
                    metrics_pass1['_has_pedigree']   = (sire_stats is not None and sire_stats.get('data_quality') != 'NONE')

                    # FAZ 6.2: Antrenör skoru — önbellekten al (paralel prefetch tamamlandı)
                    trainer_name_val = original_horse.get('trainer', '').strip()
                    trainer_stats_val = fetch_trainer_stats(trainer_name_val) if trainer_name_val else None
                    trainer_score_val = calculate_trainer_score(trainer_stats_val)
                    metrics_pass1['trainer_score'] = trainer_score_val
                    metrics_pass1['_has_trainer'] = bool(trainer_stats_val and trainer_stats_val.get('data_quality') != 'NONE')
                    metric_source_flags = {
                        'hasTraining': training_data is not None,
                        'hasTrainingTimes': bool(training_data and training_data.get('times')),
                        'hasTrainingProjection': training_projection is not None,
                        'trainingMatchedName': training_data.get('horseName') if training_data else None,
                        'trainingDate': training_data.get('trainingDate') if training_data else None,
                        'hasAgf': has_valid_agf,
                        'rawAgf': raw_agf or None,
                        'validAgfCountInRace': len(valid_agf_values),
                        'agfNeutral': abs(float(agf_score_val) - 50.0) < 1.0,
                        'hasHp': has_hp_source,
                        'rawHp': raw_hp or None,
                        'validHpCountInRace': len(valid_hps),
                        'hasHandicapEfficiency': has_handicap_efficiency,
                        'hasHandicapWeightRelief': has_handicap_efficiency,
                        'rawCurrentWeight': current_weight or None,
                        'parsedCurrentWeight': current_weight_value,
                        'validWeightCountInRace': len(valid_current_weights),
                        'handicapEfficiencyScore': handicap_efficiency_score,
                        'handicapWeightReliefScore': handicap_weight_relief_score,
                        'hasHandicapClassHistory': handicap_class_transition['hasHistory'],
                        'handicapClassCurrentLevel': handicap_class_transition['currentLevel'],
                        'handicapClassHistoricalLevel': handicap_class_transition['historicalLevel'],
                        'handicapClassDelta': handicap_class_transition['delta'],
                        'handicapClassSampleCount': handicap_class_transition['sampleCount'],
                        'handicapClassTransitionScore': handicap_class_transition['score'],
                        'hasAge': has_age_source,
                        'hasAgeActionable': has_age_actionable,
                        'rawAge': raw_age or None,
                        'parsedAge': horse_age,
                        'validAgeCountInRace': len(valid_age_values),
                        'ageRangeInRace': race_age_range,
                        'ageScoreDirection': 'older_relative' if age_supported_for_ranking else 'neutral',
                        'hasTrackExperience': has_track_experience,
                        'targetTrackRaceCount': target_track_race_count,
                        'otherTrackRaceCount': other_track_race_count,
                        'trackExperienceReason': track_experience_reason,
                        'trackExperienceScore': track_experience_score_val,
                        'hasSurfaceTransition': has_surface_transition,
                        'surfaceTransitionScore': surface_transition['score'],
                        'surfaceTransitionReason': surface_transition['reason'],
                        'surfaceTransitionDominantTrack': surface_transition['dominantTrack'],
                        'surfaceTransitionDominantShare': surface_transition['dominantTrackShare'],
                        'surfaceTransitionLastTrack': surface_transition['lastTrack'],
                        'surfaceTransitionLastThreeTargetCount': surface_transition['lastThreeTargetCount'],
                        'hasDistanceTransition': has_distance_transition,
                        'distanceTransitionScore': distance_transition['score'],
                        'distanceTransitionReason': distance_transition['reason'],
                        'lastDistanceForTransition': distance_transition['lastDistance'],
                        'avgRecentDistanceForTransition': distance_transition['avgRecentDistance'],
                        'distanceDeltaForTransition': distance_transition['distanceDelta'],
                        'similarDistanceRaceCount': distance_transition['similarDistanceRaceCount'],
                        'hasSireName': bool(sire_name),
                        'hasPedigree': bool(sire_stats and sire_stats.get('data_quality') != 'NONE'),
                        'pedigreeDataQuality': sire_stats.get('data_quality', 'NONE') if sire_stats else 'NONE',
                        'pedigreeOffspringRaces': sire_stats.get('total_offspring_races', 0) if sire_stats else 0,
                        'hasTrainerName': bool(trainer_name_val),
                        'hasTrainer': bool(trainer_stats_val and trainer_stats_val.get('data_quality') != 'NONE'),
                        'trainerDataQuality': trainer_stats_val.get('data_quality', 'NONE') if trainer_stats_val else 'NONE',
                        'trainerRaceCount': trainer_stats_val.get('total_races', 0) if trainer_stats_val else 0,
                    }

                    ai_score_pass1 = calculate_ai_score(metrics_pass1)

                    # FAZ 4.5: PASS 1 — koşu stilini belirle (diğer atlar bitmeden pace_scenario hesaplanamaz)
                    horse_style, ess_score = determine_running_style(races)
                    
                    # === TEMEL İSTATİSTİKLER ===
                    ranks = [int(r['rank']) for r in races if r.get('rank', '').isdigit()]
                    wins = sum(1 for r in ranks if r == 1)
                    podiums = sum(1 for r in ranks if r <= 3)
                    avg_rank = sum(ranks) / len(ranks) if ranks else 0
                    
                    # Pist ve mesafe galibiyetleri
                    track_wins = sum(1 for r in races if r.get('rank') == '1' and target_track.lower() in r.get('track', '').lower())
                    distance_wins = sum(1 for r in races if r.get('rank') == '1' and target_distance in r.get('distance', ''))
                    
                    # === GELİŞMİŞ BAHİS İSTATİSTİKLERİ ===
                    
                    # 1. Jokey Performansı
                    current_jockey = original_horse.get('jockey', '').strip()
                    print(f"[DEBUG] At: {horse_data['name']}, Mevcut Jokey: '{current_jockey}'")
                    print(f"[DEBUG] Yarış geçmişindeki jokeyler: {[r.get('jockey', '') for r in races]}")
                    
                    # jockey_match() modül düzeyinde tanımlandı (normalize_jockey_name ile Türkçe uyumlu)
                    jockey_races = [r for r in races if jockey_match(r.get('jockey', ''), current_jockey)]


                    jockey_wins = sum(1 for r in jockey_races if r.get('rank') == '1')
                    jockey_stats = {
                        'name': _cur_jockey,
                        'totalRaces': len(jockey_races),
                        'wins': jockey_wins,
                        'winRate': round(jockey_wins / len(jockey_races) * 100) if jockey_races else 0
                    } if _cur_jockey and jockey_races else None
                    
                    last_jockey = races[0].get('jockey', '').strip() if races else ''
                    jockey_changed = _cur_jockey and last_jockey and not jockey_match(_cur_jockey, last_jockey)
                    
                    weight_change = None
                    try:
                        def _parse_w_display(w_str):
                            if not w_str: return None
                            m = re.match(r'(\d+[,.]?\d*)', str(w_str).strip())
                            if not m: return None
                            base = float(m.group(1).replace(',', '.'))
                            bm = re.search(r'\+(\d+[,.]?\d*)', str(w_str))
                            if bm: base += float(bm.group(1).replace(',', '.'))
                            return base
                        cw = _parse_w_display(current_weight)
                        lw = _parse_w_display(last_weight)
                        if cw is not None and lw is not None:
                            weight_change = round(cw - lw, 1) or None
                    except: pass
                    
                    best_time = degree_stats.get('bestDegreeFormatted', training_best_time)
                    
                    # PASS 1: intermediate_horses'a kaydet (metrics de dahil)
                    intermediate_horses.append({
                        'name': horse_data['name'],
                        'no': original_horse.get('no', ''),
                        'rawAge': raw_age,
                        'ageScore': age_score_val,
                        'rawHp': original_horse.get('_raw_hp', ''),  # FAZ 5.2 (UI İÇİN)
                        'hpScore': hp_score_val,                     # FAZ 5.2 (UI İÇİN)
                        'aiScore': ai_score_pass1,   # geçici, PASS 2'de güncellenecek
                        'formIndex': {
                            'trend': 'UP' if trend_value > 0 else 'DOWN' if trend_value < 0 else 'STABLE',
                            'trendValue': trend_value,
                        },
                        'raceHistory': races,
                        'filteredRaces': filtered_races,
                        'degreeStats': degree_stats,
                        'stats': {
                            'avgRank': round(avg_rank, 1) if avg_rank > 0 else None,
                            'winRate': round(wins / len(ranks) * 100) if ranks else None,
                            'podiumRate': round(podiums / len(ranks) * 100) if ranks else None,
                            'trackWins': track_wins if track_wins > 0 else None,
                            'distanceWins': distance_wins if distance_wins > 0 else None,
                        },
                        'jockeyStats': jockey_stats,
                        'jockeyChanged': jockey_changed,
                        'weightChange': weight_change,
                        'daysSinceLastRace': ranking_penalty_info['daysSinceLastRace'],
                        'lastRaceDistance': ranking_penalty_info['lastRaceDistance'],
                        'restDataSource': ranking_penalty_info['restDataSource'],
                        'rankingPenalties': ranking_penalty_info['penalties'],
                        'v4PenaltyTotal': ranking_penalty_info['totalPenalty'],
                        'bestTime': best_time,
                        'raceCount': len(races),
                        'filteredRaceCount': len(filtered_races),
                        'detailFetchStatus': detail_fetch_status,
                        'featuresReliable': True,
                        'metricSourceFlags': metric_source_flags,
                        'scoreBreakdown': {
                            'weightImpactScore': weight_impact_score,
                            'handicapEfficiencyScore': handicap_efficiency_score,
                            'handicapWeightReliefScore': handicap_weight_relief_score,
                            'handicapClassTransitionScore': handicap_class_transition['score'],
                            'handicapClassDelta': handicap_class_transition['delta'],
                            'jockeyScore': jockey_score_val,
                            'bounceScore': bounce_score_val,
                            'paceScore': 50.0,          # PASS 2'de güncellenecek
                            'pedigreeScore': pedigree_score_val,   # FAZ 4.6
                            'trackSuitScore': track_suit,
                            'trackExperienceScore': track_experience_score_val,
                            'surfaceTransitionScore': surface_transition['score'],
                            'distanceSuitScore': distance_suit,
                            'distanceTransitionScore': distance_transition['score'],
                            'formTrendScore': trend_score,
                            'degreeAvgScore': degree_stats.get('degreeScore', 50),
                            'trainingFitnessScore': training_fitness,
                            'trainingDegreeScore': training_deg_score,
                            'hpScore': hp_score_val,
                            'agfScore': agf_score_val,
                            'trainerScore': trainer_score_val,
                            'ageScore': age_score_val,
                        },
                        # FAZ 4.5+4.6: PASS 1 ara değerleri (PASS 2 için gerekli)
                        '_runningStyle': horse_style,
                        '_essScore': ess_score,
                        '_metrics_pass1': metrics_pass1,  # PASS 2'de pace_score + pedigree güncel
                        # FAZ 4.6: Pedigri bilgileri (API response için korunur)
                        '_pedigreeInfo': {
                            'sireName':            sire_name,
                            'pedigreeScore':       pedigree_score_val,
                            'pedigreeWeight':      pedigree_weight_val,
                            'trackCompatibility':  track_compat,
                            'distanceCompatibility': dist_compat,
                            'dataQuality':         sire_stats.get('data_quality', 'NONE') if sire_stats else 'NONE',
                            'totalOffspringRaces': sire_stats.get('total_offspring_races', 0) if sire_stats else 0,
                        },
                        # İDMAN
                        'trainingInfo': {
                            'hasData': training_data is not None,
                            'fitnessScore': training_fitness,
                            'fitnessLabel': training_label,
                            'daysSinceTraining': days_since,
                            'trainingDate': training_data.get('trainingDate', '') if training_data else None,
                            'hippodrome': training_data.get('hippodrome', '') if training_data else None,
                            'trackCondition': training_data.get('trackCondition', '') if training_data else None,
                            'trainingJockey': training_data.get('trainingJockey', '') if training_data else None,
                            'times': training_data.get('times', {}) if training_data else {},
                            'bestTrainingTime': training_best_time,
                            'bestTrainingDistance': training_best_distance,
                            'bestTrainingTimeSeconds': parse_training_time(training_best_time) if training_best_time else None,
                            # FAZ 2.2: Projeksiyon verileri
                            'projectedDegree': training_projection.get('projectedDegree') if training_projection else None,
                            'projectedDegreeSeconds': training_projection.get('projectedDegreeSeconds') if training_projection else None,
                            'projectedFromDistance': training_projection.get('projectedFromDistance') if training_projection else None,
                            'expansionRatio': training_projection.get('expansionRatio') if training_projection else None,
                            'projectionLabel': training_projection.get('projectionLabel') if training_projection else None,
                            'projectionDiff': training_projection.get('projectionDiff') if training_projection else None,
                            # Yeni: İdman projeksiyon derecesi skoru
                            'trainingDegreeScore': training_deg_score,
                        } if training_data else None
                    })
                else:
                    # Veri çekilemediyse
                    intermediate_horses.append({
                        'name': original_horse.get('name', 'Bilinmiyor'),
                        'no': original_horse.get('no', ''),
                        'aiScore': 0,
                        'formIndex': {'trend': '-', 'trendValue': 0},
                        'raceHistory': [],
                        'filteredRaces': [],
                        'degreeStats': {},
                        'stats': {},
                        'raceCount': 0,
                        'filteredRaceCount': 0,
                        'daysSinceLastRace': None,
                        'lastRaceDistance': None,
                        'restDataSource': 'unknown',
                        'rankingPenalties': [],
                        'v4PenaltyTotal': 0.0,
                        'detailFetchStatus': 'unrecoverable',
                        'featuresReliable': False,
                        'metricSourceFlags': {
                            'hasTraining': False,
                            'hasTrainingTimes': False,
                            'hasTrainingProjection': False,
                            'hasAgf': False,
                            'validAgfCountInRace': len(valid_agf_values),
                            'hasAge': parse_horse_age(original_horse.get('age', '')) is not None,
                            'hasAgeActionable': False,
                            'rawAge': original_horse.get('age', '') or None,
                            'validAgeCountInRace': len(valid_age_values),
                            'ageRangeInRace': race_age_range,
                            'hasSireName': bool(original_horse.get('father', '').strip()),
                            'hasPedigree': False,
                            'pedigreeDataQuality': 'NONE',
                            'hasTrainerName': bool(original_horse.get('trainer', '').strip()),
                            'hasTrainer': False,
                            'trainerDataQuality': 'NONE',
                        },
                        '_runningStyle': 'TAKİPÇİ',
                        '_essScore': 50.0,
                        '_metrics_pass1': {},
                    })

        # FAZ 4.5: PASS 2 — Tempo Senaryosu + Final AI Score
        # ─────────────────────────────────────────────────────────────
        # Tüm atların koşu stilleri artık belli → pace_scenario hesaplanabilir
        horse_styles_list = [
            {'name': h['name'], 'style': h.get('_runningStyle', 'TAKİPÇİ')}
            for h in intermediate_horses
        ]
        pace_scenario, kacak_count = calculate_pace_scenario(horse_styles_list)
        pace_pressure = round((kacak_count / max(1, len(intermediate_horses))) * 100.0, 1)
        print(f"[FAZ 4.5] Tempo senaryosu: {pace_scenario} ({kacak_count} kaçak at)")
        
        analyzed_horses = []
        for h in intermediate_horses:
            horse_name = h['name']
            horse_style = h.get('_runningStyle', 'TAKİPÇİ')
            metrics_p1 = h.get('_metrics_pass1', {})
            running_style_proxy_score = float(h.get('_essScore', 50.0) or 50.0)
            
            if metrics_p1:
                # pace_score hesapla ve metrics'e ekle
                pace_score_val = calculate_pace_score(horse_style, pace_scenario)
                metrics_p1['pace_score'] = pace_score_val
                metrics_p1['running_style_proxy_score'] = running_style_proxy_score
                metrics_p1['pace_pressure'] = pace_pressure
                final_ai_score = calculate_ai_score(metrics_p1)
                # FAZ 4.7: Veri güven skoru
                confidence_val, confidence_label = calculate_data_confidence(metrics_p1)
                # FAZ 4.7: Tahmin etiketi + insight
                prediction_label = generate_prediction(final_ai_score, metrics_p1)
                insight_text     = generate_insight(horse_name, metrics_p1, final_ai_score)
            else:
                pace_score_val     = 50.0
                final_ai_score     = h.get('aiScore', 0)
                confidence_val     = 0.0
                confidence_label   = '🔴 Düşük'
                prediction_label   = 'İzlenmeli'
                insight_text       = 'Yeterli veri bulunamadı.'
            
            # scoreBreakdown güncelle
            if 'scoreBreakdown' in h:
                h['scoreBreakdown']['paceScore'] = pace_score_val
                h['scoreBreakdown']['runningStyleProxyScore'] = running_style_proxy_score
                h['scoreBreakdown']['pacePressure'] = pace_pressure
            
            # Temizle: PASS 1 private alanlarını kaldır, pedigreeInfo'yu kalıcıya taşı
            pedigree_info = h.pop('_pedigreeInfo', None)
            h.pop('_runningStyle', None)
            h.pop('_essScore', None)
            h.pop('_metrics_pass1', None)

            # FAZ 4.7 Bug Fix: Derece normalizasyonu için metrics'i geici sakla
            # (_mf = metrics_final; normalizasyon sonrası degree_avg güncellenip
            #  calculate_ai_score() yeniden çağrılacak, üste yazma olmayacak)
            if metrics_p1:
                h['_mf'] = metrics_p1

            if pedigree_info:
                h['pedigreeInfo'] = pedigree_info
            
            # Final AI score, güven ve tahmin ekle
            h['aiScore']          = final_ai_score
            h['prediction']       = prediction_label      # FAZ 4.7
            h['insight']          = insight_text          # FAZ 4.7
            h['dataConfidence']   = {                     # FAZ 4.7
                'score': confidence_val,
                'label': confidence_label,
            }
            h['paceInfo'] = {
                'runningStyle': horse_style,
                'paceScenario': pace_scenario,
                'paceScore':    pace_score_val,
                'kacakCount':   kacak_count,
                'pacePressure': pace_pressure,
                'styleProxyScore': running_style_proxy_score,
                'styleSource': 'recent_finish_position_proxy',
            }
            
            analyzed_horses.append(h)

        # ═══ FAZ B.2: DERECE NORMALİZASYONU — RECENT BEST DEGREE ═══
        # Kariyer avgDegree yerine son 3 yarışın en iyi derecesi (recentBestDegree)
        # kullanılır. Bu, "deneyimli ama yavaşlayan" atların haksız avantajını kaldırır.
        # Koşu içi göreceli: en hızlı recent = 100, en yavaş = 0
        recent_degrees = []
        for h in analyzed_horses:
            ds = h.get('degreeStats', {})
            rb = ds.get('recentBestDegree') or ds.get('avgDegree')
            if rb:
                recent_degrees.append(rb)

        if recent_degrees:
            best_recent  = min(recent_degrees)
            worst_recent = max(recent_degrees)
            degree_range = worst_recent - best_recent if worst_recent > best_recent else 1

            for h in analyzed_horses:
                ds = h.get('degreeStats', {})
                rb = ds.get('recentBestDegree') or ds.get('avgDegree')
                if rb:
                    # 0-100 normalize: düşük derece (hızlı) = yüksek skor
                    normalized = 100 - ((rb - best_recent) / degree_range * 100)
                    normalized = round(max(0, min(100, normalized)), 1)
                    h['degreeStats']['degreeScore'] = normalized

                    mf = h.get('_mf')
                    if mf:
                        mf['degree_avg'] = normalized
                        if h.get('scoreBreakdown') is not None:
                            h['scoreBreakdown']['degreeAvgScore'] = normalized
                        new_score = calculate_ai_score(mf)
                        h['aiScore']    = new_score
                        h['prediction'] = generate_prediction(new_score, mf)
                        h['insight']    = generate_insight(h.get('name', ''), mf, new_score)
                        conf_v, conf_l  = calculate_data_confidence(mf)
                        h['dataConfidence'] = {'score': conf_v, 'label': conf_l}

        # NOT: _mf burada temizlenmez! FAZ 7 ML log'u için kullanılacak.
        # _mf temizleme FAZ 7 upsert'ten SONRA yapılır (aşağıda).

        # ═══════════════════════════════════════════════════════════
        # FAZ 8: ML BLEND (XGBoost + Kural Tabanlı Hibrit Skor)
        # ═══════════════════════════════════════════════════════════
        apply_v421_contextual_metrics(analyzed_horses, race_type=race_type)

        blend_mode = 'rules_only'
        if _ml_model is not None:
            try:
                ml_raw_scores = []
                for h in analyzed_horses:
                    mf = h.get('_mf', {})
                    if mf:
                        mf['_field_size'] = len(analyzed_horses)
                        raw = predict_ml_score(mf)
                        ml_raw_scores.append(raw if raw is not None else 0.0)
                    else:
                        ml_raw_scores.append(0.0)

                # ML raw score'ları 0-100'e normalize et (koşu içi min-max)
                ml_min = min(ml_raw_scores)
                ml_max = max(ml_raw_scores)
                ml_range = ml_max - ml_min if ml_max > ml_min else 1.0
                ml_norm = [round((s - ml_min) / ml_range * 100, 1) for s in ml_raw_scores]

                for h, ml_s in zip(analyzed_horses, ml_norm):
                    mf = h.get('_mf', {})
                    alpha = calculate_blend_alpha(mf) if mf else 1.0
                    master_s = h.get('aiScore', 50)
                    blended = round(alpha * master_s + (1 - alpha) * ml_s, 1)
                    h['aiScore'] = blended
                    h['mlScore'] = ml_s
                    h['blendAlpha'] = round(alpha, 2)

                blend_mode = 'hybrid'
                print(f"[FAZ 8] ML Blend uygulandı: {len(analyzed_horses)} at, α={alpha:.2f}")
            except Exception as ml_err:
                print(f"[FAZ 8] ML Blend hatası, saf kural tabanlı devam: {ml_err}")
                blend_mode = 'rules_only'

        # ═══════════════════════════════════════════════════════════
        # KOŞU-SEVİYESİ KONSENSÜS: Her katmanda atları sırala,
        # kaç katmanda Top-N'de olduğunu say → çoklu katmanda güçlü
        # olan atlar ödüllendirilir, tek katmanda parlayan cezalandırılır
        # ═══════════════════════════════════════════════════════════
        # ═══ FAZ A.1: KONSENSÜS — Ölü katmanlar çıkarıldı, std kontrolü eklendi ═══
        blend_mode = 'v4_default_shadow_ml'
        _CONSENSUS_LAYERS = [
            'degree_avg', 'form_trend', 'hp_score', 'distance_suit',
            'training_fitness', 'jockey_score',
            'weight_impact', 'bounce_score', 'degree_stability',
            'track_suit', 'trainer_score', 'age_score',
            # track_suit ÇIKARILDI (std=0, hep 50)
            # trainer_score ÇIKARILDI (veri güvenilir değil)
        ]
        n_horses = len(analyzed_horses)
        top_n = max(3, n_horses // 3)

        if n_horses >= 3:
            consensus_counts = {}
            active_layer_count = 0
            consensus_weight_cache = {
                h.get('name', ''): calculate_dynamic_weights(
                    h.get('_mf', {}), race_type=h.get('_mf', {}).get('_race_type', 'default')
                )
                for h in analyzed_horses
                if h.get('_mf')
            }

            for layer in _CONSENSUS_LAYERS:
                layer_scores = []
                for h in analyzed_horses:
                    mf = h.get('_mf', {})
                    if not mf:
                        continue
                    layer_weights = consensus_weight_cache.get(h.get('name', ''), {})
                    if layer_weights.get(layer, 0.0) <= 0:
                        continue
                    score = mf.get(layer, 50.0)
                    layer_scores.append((h.get('name', ''), score))

                # std < 1 → bu katman atları ayırt edemiyor → ATLA
                vals = [s for _, s in layer_scores]
                if len(vals) < 2:
                    print(f"    [CONSENSUS] {layer} ATLANACAK (aktif veri yok)")
                    continue
                layer_std = float(np.std(vals)) if len(vals) > 1 else 0.0
                if layer_std < 1.0:
                    print(f"    [CONSENSUS] {layer} ATLANACAK (std={layer_std:.2f})")
                    continue

                active_layer_count += 1
                layer_scores.sort(key=lambda x: x[1], reverse=True)

                for i, (name, _) in enumerate(layer_scores):
                    if name not in consensus_counts:
                        consensus_counts[name] = 0
                    if i < top_n:
                        consensus_counts[name] += 1

            # Genişletilmiş çarpan: 0 katman → x0.75, tümü → x1.20
            max_possible = max(active_layer_count, 1)
            for h in analyzed_horses:
                name = h.get('name', '')
                count = consensus_counts.get(name, 0)
                ratio = count / max_possible
                consensus_mult = 0.75 + ratio * 0.45

                old_score = h.get('aiScore', 50)
                new_score = round(old_score * consensus_mult, 1)
                new_score = max(0, min(100, new_score))
                h['aiScore'] = new_score
                h['_consensus'] = {
                    'topN_count': count,
                    'total_layers': max_possible,
                    'ratio': round(ratio, 2),
                    'multiplier': round(consensus_mult, 3),
                }
                print(f"    [CONSENSUS] {name}: {count}/{max_possible} aktif katmanda Top-{top_n} "
                      f"-> x{consensus_mult:.3f} ({old_score:.1f} -> {new_score:.1f})")

        # 5. Sıralama (Yüksek AI puanından düşüğe)
        try:
            apply_v4_shadow_mode(
                analyzed_horses,
                race_type=race_type,
                distance=target_distance,
                track=target_track,
            )
        except Exception as _v4_err:
            print(f"[V4 SHADOW] Hesaplama hatasi, mevcut algoritma ile devam: {_v4_err}")

        try:
            attach_shadow_ml_predictions(
                analyzed_horses,
                race_type=race_type,
                distance=target_distance,
                track=target_track,
            )
        except Exception as _shadow_ml_err:
            print(f"[SHADOW-ML] Hesaplama hatasi, v4 siralama korunuyor: {_shadow_ml_err}")
            for _h in analyzed_horses:
                _h['mlShadowMode'] = 'unavailable'
                _h['mlShadowReason'] = str(_shadow_ml_err)

        attach_sort_metrics(analyzed_horses)

        analyzed_horses.sort(key=lambda x: x['aiScore'], reverse=True)
        
        # 6. Sıralama numaraları ekle
        for i, horse in enumerate(analyzed_horses):
            horse['rank'] = i + 1

        # === FAZ 5.1: SOFTMAX KAZANMA OLASILIGI ===
        # AI skorlarını softmax ile olasılığa çevir.
        # Temperature parametresi ayrışımı kontrol eder:
        #   Düşük T → kazanan daha net öne çıkar
        #   Yüksek T → dağılım daha eşit
        _scores = [h.get('aiScore', 0) for h in analyzed_horses]
        _win_probs = calculate_softmax_probabilities(_scores, temperature=18.0)
        if any(prob > 0 for prob in _win_probs):
            for h, win_prob in zip(analyzed_horses, _win_probs):
                h['winProbability'] = win_prob          # %  kazanma ihtimali
                h['winProbabilityLabel'] = (
                    f'%{win_prob:.1f} kazanma ihtimali'
                )

        
        # 6. Yarış insight'ı oluştur
        top_horses = [h['name'] for h in analyzed_horses[:3] if h['aiScore'] > 0]
        if len(top_horses) >= 2:
            race_insight = f"Bu yarışta {', '.join(top_horses[:-1])} ve {top_horses[-1]} ön plana çıkıyor."
        elif len(top_horses) == 1:
            race_insight = f"{top_horses[0]} bu yarışta favori görünüyor."
        else:
            race_insight = "Yeterli veri bulunamadı."
        
        process_time = round(time.time() - start_time, 2)
        print(f"[ANALYZE] Tamamlandı: {len(analyzed_horses)} at, {process_time}s")
        
        # === FAZ 7: ORGANİK ML LOG (predictions.jsonl) — UPSERT ===
        # Aynı race_id + horse_name varsa GÜNCELLE, yoksa EKLE.
        # Etiketlenmiş (finish_pos != None) kayıtların label bilgisi korunur.
        try:
            import json as _json
            import os as _os
            _log_path = _os.path.join(_os.path.dirname(__file__), 'predictions.jsonl')
            _current_race_id = race_id or f"{target_distance}_{target_track}_{int(time.time())}"

            def _prediction_log_name_key(value):
                text = str(value or '').split('\n')[0].strip().upper()
                text = re.sub(r'\s*\(\s*\d+\s*\)\s*$', '', text)
                return re.sub(r'\s+', ' ', text).strip()

            # 1. Mevcut dosyayı oku → dict'e çevir
            _existing = {}   # key = (race_id, horse_name_upper) → entry
            _other_lines = []  # Bu koşuya ait OLMAYAN satırlar
            if _os.path.exists(_log_path):
                with open(_log_path, 'r', encoding='utf-8') as _rf:
                    for _line in _rf:
                        _line = _line.strip()
                        if not _line:
                            continue
                        try:
                            _old = _json.loads(_line)
                            _old_rid = str(_old.get('race_id', ''))
                            _old_name = _prediction_log_name_key(_old.get('horse_name', ''))
                            if _old_rid == str(_current_race_id):
                                _existing[(_old_rid, _old_name)] = _old
                            else:
                                _other_lines.append(_line)
                        except Exception:
                            _other_lines.append(_line)

            # 2. Yeni entry'leri hazırla (upsert)
            _new_entries = []
            for _h in analyzed_horses:
                # FAZ 7 Bug Fix: _metrics_pass1 zaten pop() ile silindi → _mf kullan
                _m = _h.get('_mf', {})
                _h_name = _h.get('name', '')
                _key = (str(_current_race_id), _prediction_log_name_key(_h_name))

                _entry = {
                    'race_id':    _current_race_id,
                    'race_date':  race_date or '',   # FAZ 7: Lookup için
                    'race_no':    race_no or '',     # FAZ 7: Lookup için
                    'horse_name': _h_name,
                    'horse_no':   _h.get('no', ''),
                    'ai_score':   _h.get('aiScore', 0),
                    'rank_pred':  _h.get('rank', 0),
                    'legacy_score': _h.get('legacyScore'),
                    'legacy_rank': _h.get('legacyRank'),
                    'legacy_win_probability': _h.get('legacyWinProbability'),
                    'v4_score':   _h.get('v4Score', 0),
                    'v4_base_score': _h.get('v4BaseScore', 0),
                    'v4_penalty_total': _h.get('v4PenaltyTotal', 0),
                    'v4_rank':    _h.get('v4Rank', 0),
                    'v4_version': _h.get('v4Version', _V4_VERSION),
                    'v4_mode':    _h.get('v4Mode', 'visible'),
                    'v4_decision_mode': _h.get('v4DecisionMode', 'default_visible'),
                    'v4_use_for_ranking': _h.get('v4UseForRanking', False),
                    'v4_applied_for_ranking': _h.get('v4AppliedForRanking', False),
                    'v4_reason': _h.get('v4Reason', ''),
                    'v4_profile': _h.get('v4Profile', {}),
                    'v4_weights': _h.get('v4Weights', {}),
                    'v4_confidence': _h.get('v4Confidence', {}),
                    'v4_data_quality': _h.get('v4DataQuality', {}),
                    'days_since_last_race': _h.get('daysSinceLastRace'),
                    'last_race_distance': _h.get('lastRaceDistance'),
                    'race_count': _h.get('raceCount'),
                    'filtered_race_count': _h.get('filteredRaceCount'),
                    'rest_data_source': _h.get('restDataSource', 'unknown'),
                    'ranking_penalties': _h.get('rankingPenalties', []),
                    'agf_allowed_for_ranking': _h.get('agfAllowedForRanking', False),
                    'ml_shadow_score': _h.get('mlShadowScore'),
                    'ml_shadow_rank': _h.get('mlShadowRank'),
                    'ml_win_probability': _h.get('mlWinProbability'),
                    'ml_top3_probability': _h.get('mlTop3Probability'),
                    'ml_expected_finish_rank': _h.get('mlExpectedFinishRank'),
                    'ml_model_version': _h.get('mlModelVersion'),
                    'ml_shadow_mode': _h.get('mlShadowMode'),
                    'ml_shadow_reason': _h.get('mlShadowReason'),
                    'sort_metrics': _h.get('sortMetrics', {}),
                    'race_type':  race_type or '',
                    'distance':   target_distance or '',
                    'track':      target_track or '',
                    'field_size': len(analyzed_horses),
                    'detail_fetch_status': _h.get('detailFetchStatus', ''),
                    'features_reliable': bool(_m),
                    'metric_source_flags': _h.get('metricSourceFlags', {}),
                    'finish_pos': None,
                    'is_winner':  None,
                    'ts':         int(time.time()),
                    'features': {
                        k: _m.get(k) if _m else None
                        for k in [
                            'degree_avg','degree_trend','degree_stability',
                            'form_trend','track_suit','track_experience_score',
                            'surface_transition_score','distance_suit','distance_transition_score',
                            'training_fitness','training_degree_score',
                            'weight_impact','handicap_efficiency_score',
                            'handicap_weight_relief_score',
                            'handicap_class_transition_score','handicap_class_delta',
                            'field_relative_value_score','pace_map_edge_score',
                            'surface_switch_safety_score','favorite_risk_guard_score',
                            'class_peak_score','elite_consensus_score',
                            'jockey_score','bounce_score','pace_score',
                            'running_style_proxy_score','pace_pressure','pedigree','hp_score',
                            'agf_score','trainer_score','age_score',
                        ]
                    }
                }

                # Eğer bu at daha önce logllanmış VE etiketlenmişse → label'ı koru
                if _key in _existing:
                    _prev = _existing[_key]
                    if _prev.get('finish_pos') is not None:
                        _entry['finish_pos'] = _prev['finish_pos']
                        _entry['is_winner']  = _prev.get('is_winner')

                _new_entries.append(_json.dumps(_entry, ensure_ascii=False))

            # 3. Dosyayı yeniden yaz (diğer koşular + güncel koşu)
            with open(_log_path, 'w', encoding='utf-8') as _wf:
                for _ol in _other_lines:
                    _wf.write(_ol + '\n')
                for _ne in _new_entries:
                    _wf.write(_ne + '\n')

            print(f"[PRED LOG] Upsert: {_current_race_id} → {len(_new_entries)} at (mevcut {len(_existing)} güncellendi)")
            github_backup()  # FAZ 7.2: GitHub'a yedekle
        except Exception as _le:
            print(f"[PRED LOG] Loglama hatası: {_le}")
        finally:
            # FAZ 7 tamamlandı — şimdi _mf geçici alanını temizle (API response'a karışmasın)
            for _h in analyzed_horses:
                _h.pop('_mf', None)

        return jsonify({
            'success': True,
            'results': analyzed_horses,
            'raceInsight': race_insight,
            'targetDistance': target_distance,
            'targetTrack': target_track,
            'paceScenario': pace_scenario,
            'blendMode': blend_mode,
            'processTime': process_time
        })
        
    except Exception as e:
        print(f"[ANALYZE ERROR] {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ══════════════════════════════════════════════════════════════════
# FAZ 7: SONUÇ GÖNDERME (ML Label)
# ══════════════════════════════════════════════════════════════════

@app.route('/api/submit-results', methods=['POST'])
def submit_results():
    """
    Kullanıcı gerçek yarış sonuçlarını girinceye kadar predictions.jsonl'daki
    finish_pos=None satırlarını günceller.

    Body:
      {
        "race_id": "12345",
        "race_date": "28.04.2026",   (opsiyonel — fallback eşleşme için)
        "race_no":   "3",            (opsiyonel — fallback eşleşme için)
        "results": [
          {"horse_name": "ERDEK", "finish_pos": 1},
          {"horse_name": "SİMSEK YELELI", "finish_pos": 2},
          ...
        ]
      }
    """
    try:
        import json as _json, os as _os
        data = request.json
        def _clean_name(s):
            """Normalize horse names for result matching.

            TJK result sources can collapse spaces (``SUPERCHIRON``) while
            analysis records keep them (``SUPER CHIRON``). Race-id matching
            already limits the scope, so punctuation and whitespace can be
            removed safely from both sides before comparing names.
            """
            text = str(s).split('\n')[0].strip().upper()
            text = re.sub(r'\s*\(\s*\d+\s*\)\s*$', '', text)
            return re.sub(r'[\W_]+', '', text, flags=re.UNICODE)

        race_id_in  = str(data.get('race_id', '')).strip()
        race_date   = str(data.get('race_date', '')).strip()   # FAZ 7.4: fallback
        race_no_in  = str(data.get('race_no', '')).strip()     # FAZ 7.4: fallback
        incoming    = {_clean_name(r['horse_name']): r['finish_pos'] for r in data.get('results', [])}

        if not race_id_in or not incoming:
            return jsonify({'success': False, 'error': 'race_id ve results zorunlu'}), 400

        log_path = _os.path.join(_os.path.dirname(__file__), 'predictions.jsonl')
        if not _os.path.exists(log_path):
            return jsonify({'success': False, 'error': 'predictions.jsonl bulunamadı'}), 404

        # ── PASS 1: race_id ile eşleştir ──────────────────────────────
        lines = []
        updated = 0
        race_id_hits = 0  # race_id eşleşen satır sayısı (horse_name'den bağımsız)
        resolved_race_id = None  # Eğer tarih-format ID geldiyse, gerçek numeric ID'yi bul
        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entry = _json.loads(line)
                    if str(entry.get('race_id', '')) == race_id_in:
                        race_id_hits += 1
                        name_key = _clean_name(entry.get('horse_name', ''))
                        if name_key in incoming:
                            pos = incoming[name_key]
                            entry['finish_pos'] = pos
                            entry['is_winner']  = 1 if pos == 1 else 0
                            updated += 1
                    lines.append(_json.dumps(entry, ensure_ascii=False))
                except Exception:
                    lines.append(line.strip())

        print(f"[SUBMIT] PASS 1: race_id={race_id_in} → {race_id_hits} kayıt bulundu, {updated} at güncellendi")

        # ── PASS 2 (FALLBACK): race_id hiç bulunamadıysa → race_date + horse_name ──
        # NOT: race_id_hits > 0 ama updated == 0 ise at isimleri eşleşmedi demek.
        # Bu durumda PASS 2/3'e geçme — race_id doğru koşuyu buldu, at isimleri sorun.
        if race_id_hits == 0 and race_date:
            print(f"[SUBMIT] PASS 1 race_id bulunamadı, PASS 2: race_date={race_date} ile deneniyor...")
            lines = []
            # Tarih eşleşen koşuları bul — eğer horse_name de eşleşiyorsa güncelle
            with open(log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = _json.loads(line)
                        entry_date = str(entry.get('race_date', ''))
                        entry_no   = str(entry.get('race_no', ''))
                        name_key   = _clean_name(entry.get('horse_name', ''))

                        # race_date eşleşiyor VE (race_no eşleşiyor VEYA boş) VE horse_name listede
                        date_match = entry_date == race_date
                        no_match   = (not race_no_in or not entry_no or entry_no == race_no_in)
                        name_match = name_key in incoming

                        if date_match and no_match and name_match:
                            pos = incoming[name_key]
                            entry['finish_pos'] = pos
                            entry['is_winner']  = 1 if pos == 1 else 0
                            updated += 1
                            if not resolved_race_id:
                                resolved_race_id = str(entry.get('race_id', ''))

                        lines.append(_json.dumps(entry, ensure_ascii=False))
                    except Exception:
                        lines.append(line.strip())

            if updated > 0:
                print(f"[SUBMIT] PASS 2 başarılı: {updated} at güncellendi (resolved race_id={resolved_race_id})")

        # ── PASS 3 (SON ÇARE): race_date alanı olmayan eski kayıtlar ──
        # Eski analizlerdeki predictions.jsonl entries'de race_date yok.
        # Horse_name set eşleşmesi ile doğru koşuyu bul.
        # SADECE race_id hiç bulunamadığında (race_id_hits==0) devreye girer.
        if race_id_hits == 0 and updated == 0 and incoming:
            print(f"[SUBMIT] PASS 3: horse_name set eşleşmesi deneniyor...")
            lines = []
            # race_id'lere göre grupla ve horse_name set overlap'i en yüksek olanı bul
            race_groups = {}  # race_id → {names: set, count: int}
            all_entries = []
            with open(log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = _json.loads(line)
                        all_entries.append(entry)
                        rid = str(entry.get('race_id', ''))
                        name = _clean_name(entry.get('horse_name', ''))
                        if rid not in race_groups:
                            race_groups[rid] = {'names': set(), 'count': 0}
                        race_groups[rid]['names'].add(name)
                        race_groups[rid]['count'] += 1
                    except Exception:
                        all_entries.append(line.strip())

            # En yüksek overlap'li race_id'yi bul
            incoming_names = set(incoming.keys())
            best_rid = None
            best_overlap = 0
            for rid, info in race_groups.items():
                overlap = len(incoming_names & info['names'])
                # En az %50 eşleşme gerekli (yanlış koşuyla eşleşmeyi önle)
                if overlap > best_overlap and overlap >= len(incoming_names) * 0.5:
                    best_overlap = overlap
                    best_rid = rid

            if best_rid:
                for entry in all_entries:
                    if isinstance(entry, dict):
                        if str(entry.get('race_id', '')) == best_rid:
                            name_key = _clean_name(entry.get('horse_name', ''))
                            if name_key in incoming:
                                pos = incoming[name_key]
                                entry['finish_pos'] = pos
                                entry['is_winner']  = 1 if pos == 1 else 0
                                updated += 1
                        lines.append(_json.dumps(entry, ensure_ascii=False))
                    else:
                        lines.append(entry)
                resolved_race_id = best_rid
                print(f"[SUBMIT] PASS 3 başarılı: {updated} at güncellendi (best_rid={best_rid}, overlap={best_overlap}/{len(incoming_names)})")
            else:
                # Hiçbir yöntemle eşleşme bulunamadı
                for entry in all_entries:
                    if isinstance(entry, dict):
                        lines.append(_json.dumps(entry, ensure_ascii=False))
                    else:
                        lines.append(entry)

        with open(log_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')

        final_id = resolved_race_id or race_id_in
        print(f"[SUBMIT] {final_id}: {updated} at güncellendi")

        if updated > 0:
            github_backup()  # FAZ 7.2: GitHub'a yedekle
            return jsonify({'success': True, 'updated': updated, 'race_id': final_id})
        elif race_id_hits > 0:
            # race_id bulundu ama hiçbir at ismi eşleşmedi
            # Bu, fetch-race-results'tan gelen at isimleri predictions.jsonl'dakiyle uyuşmadığında olur
            return jsonify({
                'success': True,
                'updated': 0,
                'race_id_hits': race_id_hits,
                'incoming_horses': list(incoming.keys()),
                'warning': f'race_id={race_id_in} bulundu ({race_id_hits} at) ama hiçbir at ismi eşleşmedi. '
                           f'Gönderilen atlar: {list(incoming.keys())[:5]}'
            })
        else:
            # race_id hiç bulunamadı
            return jsonify({
                'success': True,
                'updated': 0,
                'warning': f'race_id={race_id_in} ile eşleşen kayıt bulunamadı. Bu koşuyu önce analiz ettiğinizden emin olun.'
            })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ══════════════════════════════════════════════════════════════════
# FAZ 7.1: ML VERİ TEMİZLEME (Duplikasyon Giderme)
# ══════════════════════════════════════════════════════════════════

@app.route('/api/ml-cleanup', methods=['POST'])
def ml_cleanup():
    """
    predictions.jsonl içindeki duplike satırları temizler.
    Aynı race_id + horse_name için sadece en son kaydı tutar.
    Etiketlenmiş (finish_pos != None) kayıtlar önceliklidir.

    POST /api/ml-cleanup
    """
    try:
        import json as _json, os as _os
        log_path = _os.path.join(_os.path.dirname(__file__), 'predictions.jsonl')
        if not _os.path.exists(log_path):
            return jsonify({'success': False, 'error': 'predictions.jsonl bulunamadı'}), 404

        entries = {}  # key=(race_id, horse_name_upper) → entry
        total_before = 0
        duplicates_removed = 0

        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = _json.loads(line)
                    total_before += 1
                    rid = str(entry.get('race_id', ''))
                    name = entry.get('horse_name', '').strip().upper()
                    key = (rid, name)

                    if key in entries:
                        prev = entries[key]
                        # Etiketli kayıt varsa onu koru, yoksa yenisini al
                        if prev.get('finish_pos') is not None:
                            # Zaten etiketli → sadece feature'ları güncelle, label koru
                            entry['finish_pos'] = prev['finish_pos']
                            entry['is_winner'] = prev.get('is_winner')
                        duplicates_removed += 1
                    entries[key] = entry
                except Exception:
                    continue

        # Yeniden yaz
        total_after = len(entries)
        with open(log_path, 'w', encoding='utf-8') as f:
            for entry in entries.values():
                f.write(_json.dumps(entry, ensure_ascii=False) + '\n')

        print(f"[ML-CLEANUP] {total_before} → {total_after} ({duplicates_removed} duplike silindi)")
        return jsonify({
            'success': True,
            'before': total_before,
            'after': total_after,
            'duplicates_removed': duplicates_removed,
            'message': f'{duplicates_removed} duplike kayıt temizlendi. {total_after} kayıt kaldı.'
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ══════════════════════════════════════════════════════════════════
# FAZ 7.3: RACE-ID ÇÖZÜMLEME (date+raceNo → numeric race_id)
# ══════════════════════════════════════════════════════════════════

@app.route('/api/resolve-race-id', methods=['GET'])
def resolve_race_id():
    """
    predictions.jsonl'dan date + raceNo ile numeric race_id döndürür.
    Yarış bittiğinde TJK HTML değiştiği için Flutter scraper race_id'yi
    boş parse edebiliyor. Bu endpoint o durumda fallback olarak kullanılır.

    GET /api/resolve-race-id?date=27.04.2026&raceNo=3
    Yanıt: {"race_id": "224638", "source": "jsonl"}
    """
    try:
        import json as _json, os as _os
        date_param = request.args.get('date', '').strip()    # dd.MM.yyyy
        race_no_param = request.args.get('raceNo', '').strip()

        if not date_param or not race_no_param:
            return jsonify({'success': False, 'error': 'date ve raceNo zorunlu'}), 400

        log_path = _os.path.join(_os.path.dirname(__file__), 'predictions.jsonl')
        if _os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = _json.loads(line)
                        if (str(entry.get('race_date', '')) == date_param and
                                str(entry.get('race_no', '')) == str(race_no_param)):
                            rid = str(entry.get('race_id', ''))
                            if rid:
                                return jsonify({'success': True, 'race_id': rid, 'source': 'jsonl'})
                    except Exception:
                        continue

        return jsonify({'success': False, 'error': 'Bu tarih/koşu için kayıt bulunamadı'}), 404

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ══════════════════════════════════════════════════════════════════
# FAZ 7: ML EĞİTİM VERİ İSTATİSTİKLERİ
# ══════════════════════════════════════════════════════════════════

@app.route('/api/ml-stats', methods=['GET'])
def ml_stats():
    """
    predictions.jsonl hakkında özet istatistikler döner.
    Tarayıcıdan doğrudan açılabilir:
      https://atistik-backend.onrender.com/api/ml-stats
    """
    try:
        import json as _json, os as _os
        log_path = _os.path.join(_os.path.dirname(__file__), 'predictions.jsonl')
        if not _os.path.exists(log_path):
            return jsonify({
                'success': True,
                'total': 0,
                'labeled': 0,
                'unlabeled': 0,
                'races': [],
                'message': 'Henüz hiç analiz yapılmamış. predictions.jsonl yok.'
            })

        total     = 0
        labeled   = 0
        unlabeled = 0
        races     = {}   # race_id → {horses, labeled}

        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = _json.loads(line)
                    total += 1
                    rid = entry.get('race_id', 'bilinmiyor')
                    if rid not in races:
                        races[rid] = {'horses': 0, 'labeled': 0, 'sample_horse': entry.get('horse_name', '')}
                    races[rid]['horses'] += 1

                    if entry.get('finish_pos') is not None:
                        labeled += 1
                        races[rid]['labeled'] += 1
                    else:
                        unlabeled += 1
                except Exception:
                    continue

        analyzed_race_count = len(races)
        labeled_race_count = sum(1 for values in races.values() if values['labeled'] > 0)
        fully_labeled_race_count = sum(
            1 for values in races.values()
            if values['horses'] > 0 and values['labeled'] == values['horses']
        )
        partially_labeled_race_count = sum(
            1 for values in races.values()
            if 0 < values['labeled'] < values['horses']
        )
        unlabeled_race_count = analyzed_race_count - labeled_race_count

        trained_race_count = 0
        try:
            trained_race_count = int(_ml_shadow_metadata.get('train_races', 0) or 0) + int(
                _ml_shadow_metadata.get('validation_races', 0) or 0
            )
        except (TypeError, ValueError):
            trained_race_count = 0

        retrain_interval_races = 50
        new_labeled_races = max(0, labeled_race_count - trained_race_count)
        races_until_retrain = max(0, retrain_interval_races - new_labeled_races)
        retrain_due = trained_race_count > 0 and races_until_retrain == 0

        race_list = [
            {
                'race_id': rid,
                'horses':  v['horses'],
                'labeled': v['labeled'],
                'done':    v['labeled'] == v['horses'],
            }
            for rid, v in sorted(races.items(), reverse=True)
        ]

        return jsonify({
            'success':   True,
            'total':     total,
            'labeled':   labeled,
            'unlabeled': unlabeled,
            # ML sayacinda race_count yalnizca egitime girebilen sonuclu kosulari ifade eder.
            'race_count': labeled_race_count,
            'analyzed_race_count': analyzed_race_count,
            'labeled_race_count': labeled_race_count,
            'fully_labeled_race_count': fully_labeled_race_count,
            'partially_labeled_race_count': partially_labeled_race_count,
            'unlabeled_race_count': unlabeled_race_count,
            'model_training_race_count': trained_race_count,
            'new_labeled_races_since_training': new_labeled_races,
            'retrain_interval_races': retrain_interval_races,
            'races_until_retrain': races_until_retrain,
            'retrain_due': retrain_due,
            'training_ready': labeled >= 50,
            'races':     race_list[:20],   # Son 20 koşu
            'message': (
                f'{labeled_race_count} sonuçlu yarış / {labeled} etiketlenmiş at var. '
                + (
                    f'Yeniden eğitim eşiği doldu (+{new_labeled_races} yarış).'
                    if retrain_due
                    else f'Yeniden eğitim için {races_until_retrain} sonuçlu yarış daha gerekli.'
                )
            )
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ══════════════════════════════════════════════════════════════════
# FAZ 8: ML EĞİTİM VERİSİ EXPORT (predictions.jsonl → JSON)
# ══════════════════════════════════════════════════════════════════

@app.route('/api/ml-export', methods=['GET'])
def ml_export():
    """
    predictions.jsonl'ın tamamını JSON array olarak döner.
    Lokal ML eğitimi için kullanılır.

    Opsiyonel parametre:
      ?labeled_only=true  → sadece finish_pos != null kayıtları

    GET /api/ml-export
    GET /api/ml-export?labeled_only=true
    """
    try:
        import json as _json, os as _os
        log_path = _os.path.join(_os.path.dirname(__file__), 'predictions.jsonl')
        if not _os.path.exists(log_path):
            return jsonify({'success': False, 'error': 'predictions.jsonl bulunamadı'}), 404

        labeled_only = request.args.get('labeled_only', 'false').lower() == 'true'
        entries = []

        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = _json.loads(line)
                    if labeled_only and entry.get('finish_pos') is None:
                        continue
                    entries.append(entry)
                except Exception:
                    continue

        return jsonify({
            'success': True,
            'count': len(entries),
            'entries': entries,
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


    port = int(os.environ.get('PORT', 5000))
    print("TJK API Server başlatılıyor...")
    print("Endpoint'ler:")
    print("  POST /api/search-horses - At arama")
    print("  POST /api/horse-details - At detayları")
    print("  POST /api/search-races - Yarış arama")
    print("  POST /api/daily-races - Günün koşuları")
    print("  GET  /daily-program - Günün Yarış Programı (Yeni)")
    print("  GET  /health - Sağlık kontrolü")
    print(f"Port: {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
