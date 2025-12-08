from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import prediction_logic
import concurrent.futures
import pandas as pd
import numpy as np
import time
import re

app = Flask(__name__)
CORS(app)  # Flutter'dan gelen isteklere izin ver

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
        
        response = requests.get(detail_url, headers=HEADERS, timeout=10)
        
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
        # Şehir ID mapping
        city_map = {
            'İstanbul': '1',
            'Ankara': '2',
            'İzmir': '3',
            'Adana': '4',
            'Bursa': '5',
            'Şanlıurfa': '6',
            'Diyarbakır': '7',
            'Elazığ': '8',
            'Kocaeli': '9'
        }
        
        city = data.get('city', 'İstanbul')
        city_id = city_map.get(city, '1')
        
        # Bugünün tarihini al
        from datetime import datetime
        today = datetime.now().strftime('%d.%m.%Y')
        
        # TJK günlük program sayfası
        url = f"https://www.tjk.org/TR/YarisSever/Info/Page/GunlukYarisProgrami?SehirId={city_id}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9"
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return jsonify({
                'success': False,
                'error': f'TJK sayfası yüklenemedi. Status: {response.status_code}'
            }), 500
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Koşu bilgilerini bul
        races = []
        race_cards = soup.find_all('div', class_='race-card') or soup.find_all('div', class_='kosu-card')
        
        # Alternatif: Tablo formatında koşular
        if not race_cards:
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # İlk satır başlık
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        try:
                            race = {
                                'raceNumber': cells[0].text.strip(),
                                'time': cells[1].text.strip(),
                                'distance': cells[2].text.strip(),
                                'track': cells[3].text.strip(),
                                'city': city
                            }
                            races.append(race)
                        except:
                            continue
        else:
            for card in race_cards:
                try:
                    race = {
                        'raceNumber': card.find('span', class_='race-number').text.strip() if card.find('span', class_='race-number') else '',
                        'time': card.find('span', class_='race-time').text.strip() if card.find('span', class_='race-time') else '',
                        'distance': card.find('span', class_='distance').text.strip() if card.find('span', class_='distance') else '',
                        'track': card.find('span', class_='track').text.strip() if card.find('span', class_='track') else '',
                        'city': city
                    }
                    races.append(race)
                except:
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
        city_id = request.args.get('cityId', '1') # Default İstanbul
        
        if not date_param:
            return jsonify({'success': False, 'error': 'Date parameter is required'}), 400

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

def fetch_horse_details_safe(horse_data):
    """Güvenli bir şekilde at detaylarını çeker (Hata yönetimi ile)"""
    try:
        detail_link = horse_data.get('detailLink')
        if not detail_link:
            return None
            
        full_url = urljoin(TARGET_URL, detail_link).replace("&amp;", "&")
        
        # Rastgele bekleme (Anti-bot önlemi)
        # time.sleep(0.1) 
        
        response = requests.get(full_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        data_div = soup.find('div', id='dataDiv')
        if not data_div:
            return None
            
        race_table = data_div.find('table', id='queryTable')
        if not race_table:
            return None
            
        table_body = race_table.find('tbody', id='tbody0')
        if not table_body:
            return None
            
        rows = table_body.find_all('tr')
        races = []
        
        # Son 5 yarışı al
        count = 0
        for row in rows:
            if 'hidable' in row.get('class', []):
                continue
            
            if count >= 5:
                break
                
            cells = row.find_all('td')
            if len(cells) > 17:
                try:
                    race_date = cells[0].text.strip()
                    city = cells[1].text.strip()
                    distance = cells[2].text.strip()
                    track = cells[3].text.strip()  # Çim/Kum
                    rank = cells[4].text.strip()   # Sıralama
                    weight = cells[6].text.strip() # Kilo
                    jockey = cells[7].text.strip() # Jokey
                    degree = cells[12].text.strip() # Derece (süre)
                    
                    races.append({
                        'date': race_date,
                        'city': city,
                        'distance': distance,
                        'track': track,
                        'rank': rank,
                        'weight': weight,
                        'jockey': jockey,
                        'degree': degree
                    })
                    count += 1
                except:
                    continue
        
        return {
            'name': horse_data.get('name'),
            'jockey': horse_data.get('jockey', ''),  # Mevcut jokey
            'weight': horse_data.get('weight', ''),  # Mevcut kilo
            'races': races
        }
        
    except Exception as e:
        print(f"Error fetching details for {horse_data.get('name')}: {e}")
        return None

def calculate_seconds(degree_str):
    """Derece stringini (1.24.50) saniyeye çevirir"""
    try:
        if not degree_str or degree_str == '-':
            return None
        parts = degree_str.split('.')
        if len(parts) == 3:
            return int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) / 100
        elif len(parts) == 2: # Sadece saniye.salise
             return int(parts[0]) + int(parts[1]) / 100
        return None
    except:
        return None

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
        return 50.0, "Bilinmiyor", None, None
        
    from datetime import datetime, timedelta
    
    score = 50.0  # Başlangıç skoru
    days_since_training = None
    best_time_str = None
    
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
    
    return round(score, 1), label, days_since_training, best_time_str

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
    Form Grafiği (Form Trend) - At gelişiyor mu, geriliyor mu?
    Son 4 yarıştaki sıralamaların ağırlıklı ortalaması
    """
    if len(races) < 2:
        return 0.0, 50.0, "Stabil"
    
    ranks = []
    for race in races[:4]:
        try:
            rank = int(re.sub(r'[^0-9]', '', race.get('rank', '0')) or 0)
            if rank > 0:
                ranks.append(rank)
        except:
            continue
    
    if len(ranks) < 2:
        return 0.0, 50.0, "Stabil"
    
    # Trend hesaplama: ranks[0] = en son yarış
    # Eğer son yarışlar daha iyi (düşük rank) ise trend pozitif
    y = np.array(ranks[::-1])  # Tersine çevir (eski -> yeni)
    x = np.arange(len(y))
    
    if len(x) >= 2:
        slope, _ = np.polyfit(x, y, 1)
        # Negatif slope = sıralama düşüyor = performans artıyor
        trend_value = -slope
    else:
        trend_value = 0

    # Trend skorunu 0-100 aralığına normalize et
    # trend_value: -3 ile +3 arası olabilir
    trend_score = 50 + (trend_value * 15)
    trend_score = max(0, min(100, trend_score))
    
    if trend_value > 0.5:
        label = "Yükselişte 📈"
    elif trend_value > 0.1:
        label = "İyileşiyor"
    elif trend_value < -0.5:
        label = "Düşüşte 📉"
    elif trend_value < -0.1:
        label = "Geriliyor"
    else:
        label = "Stabil"
    
    return round(trend_value, 2), round(trend_score, 1), label

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

def calculate_ai_score(metrics):
    """
    Tüm metriklerin ağırlıklı birleşimi - Nihai AI Skoru (0-100)
    İdman fitness skoru da dahil edildi.
    """
    weights = {
        'early_speed': 0.12,
        'late_kick': 0.18,
        'form_trend': 0.18,
        'consistency': 0.12,
        'track_suit': 0.13,
        'distance_suit': 0.12,
        'training_fitness': 0.15
    }
    
    weighted_sum = 0
    weight_total = 0
    
    for key, weight in weights.items():
        value = metrics.get(key, 50)
        if key == 'consistency':
            value = value * 10
        weighted_sum += value * weight
        weight_total += weight
    
    return round(weighted_sum / weight_total, 1) if weight_total > 0 else 50.0

def generate_prediction(ai_score, metrics):
    """Tahmin etiketi oluştur"""
    if ai_score >= 85:
        return "Favori ⭐"
    elif ai_score >= 75:
        return "Plase Adayı"
    elif metrics.get('form_trend_value', 0) > 0.5:
        return "Formda 📈"
    elif metrics.get('late_kick', 50) >= 70:
        return "Sprinter"
    elif metrics.get('early_speed', 50) >= 70:
        return "Kaçak"
    elif metrics.get('consistency', 5) >= 7:
        return "Güvenilir"
    else:
        return "İzlenmeli"

def generate_insight(name, metrics, ai_score):
    """At için Türkçe insight metni oluştur"""
    insights = []
    
    if metrics.get('form_trend_value', 0) > 0.3:
        insights.append(f"Son yarışlarda yükselen performans")
    elif metrics.get('form_trend_value', 0) < -0.3:
        insights.append(f"Son yarışlarda düşüş eğilimi")
    
    if metrics.get('track_suit', 50) >= 75:
        insights.append(f"Bu pist tipinde başarılı geçmişi var")
    
    if metrics.get('late_kick', 50) >= 70:
        insights.append(f"Son düzlükte güçlü sprint kapasitesi")
    
    if metrics.get('consistency', 5) >= 7:
        insights.append(f"Tutarlı performans sergiliyor")
    elif metrics.get('consistency', 5) <= 3:
        insights.append(f"Performansı değişken, sürpriz yapabilir")
    
    if not insights:
        if ai_score >= 70:
            insights.append("Genel metriklerinde dengeli görünüyor")
        else:
            insights.append("Rakiplerine göre dezavantajlı konumda")
    
    return " • ".join(insights[:2])  # Max 2 insight

@app.route('/api/analyze-race', methods=['POST'])
def analyze_race():
    """🧠 Gelişmiş Yarış Analizi ve Tahmin Modülü"""
    try:
        start_time = time.time()
        data = request.json
        horses = data.get('horses', [])
        target_distance = data.get('targetDistance', '')
        target_track = data.get('targetTrack', '')
        race_id = data.get('raceId', '')  # YENİ: Koşu ID'si
        
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
        
        # 2. Paralel Yarış Geçmişi ve Analiz
        analyzed_horses = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_horse = {executor.submit(fetch_horse_details_safe, horse): horse for horse in horses}
            
            for future in concurrent.futures.as_completed(future_to_horse):
                original_horse = future_to_horse[future]
                horse_data = future.result()
                horse_name = original_horse.get('name', '')
                
                # İdman verisini al (Türkçe karakter uyumlu eşleştirme)
                import unicodedata
                def normalize_name(name):
                    """Türkçe karakterleri normalize et ve küçük harfe çevir"""
                    if not name:
                        return ""
                    # Unicode normalize et
                    normalized = unicodedata.normalize('NFKC', name.strip())
                    # Türkçe-uyumlu küçük harf (casefold daha iyi çalışır)
                    return normalized.casefold()
                
                horse_name_normalized = normalize_name(horse_name)
                training_data = None
                
                # Debug: İlk at için key'leri göster
                if len(analyzed_horses) == 0:
                    print(f"[DEBUG] Training map keys: {list(training_data_map.keys())}")
                    print(f"[DEBUG] Aranan at (raw): '{horse_name}'")
                    print(f"[DEBUG] Aranan at (norm): '{horse_name_normalized}'")
                    for key in training_data_map.keys():
                        print(f"[DEBUG] Map key (raw): '{key}' -> (norm): '{normalize_name(key)}'")
                
                # Eşleşen anahtarı bul
                for key, value in training_data_map.items():
                    if normalize_name(key) == horse_name_normalized:
                        training_data = value
                        print(f"[DEBUG] EŞLEŞME BULUNDU: '{horse_name}' == '{key}'")
                        break
                
                print(f"[DEBUG] At: {horse_name}, Training data: {'VAR' if training_data else 'YOK'}")
                training_fitness, training_label, days_since, training_best_time = calculate_training_fitness(training_data)
                
                if horse_data and horse_data.get('races'):
                    races = horse_data['races']
                    
                    # 2. Gelişmiş Metrikler
                    early_speed, early_label = calculate_early_speed(races)
                    late_kick, late_label = calculate_late_kick(races)
                    trend_value, trend_score, trend_label = calculate_form_trend(races)
                    consistency, consistency_label = calculate_consistency(races)
                    track_suit, track_label = calculate_track_suitability(races, target_track)
                    distance_suit, distance_label = calculate_distance_suitability(races, target_distance)
                    
                    # 3. AI Score hesaplama - İdman verisi dahil
                    metrics = {
                        'early_speed': early_speed,
                        'late_kick': late_kick,
                        'form_trend': trend_score,
                        'form_trend_value': trend_value,
                        'consistency': consistency,
                        'track_suit': track_suit,
                        'distance_suit': distance_suit,
                        'training_fitness': training_fitness  # İdman fitness skoru
                    }
                    
                    ai_score = calculate_ai_score(metrics)
                    
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
                    
                    # Jokey eşleştirmesi - kısmi eşleşme kullan (isim baş harfleri farklı olabilir)
                    def jockey_match(j1, j2):
                        if not j1 or not j2:
                            return False
                        j1 = j1.strip().upper()
                        j2 = j2.strip().upper()
                        # Birebir eşleşme
                        if j1 == j2:
                            return True
                        # Kısmi eşleşme (soyad aynı mı?)
                        parts1 = j1.split('.')
                        parts2 = j2.split('.')
                        if len(parts1) > 1 and len(parts2) > 1:
                            return parts1[-1].strip() == parts2[-1].strip()
                        return False
                    
                    jockey_races = [r for r in races if jockey_match(r.get('jockey', ''), current_jockey)]
                    jockey_wins = sum(1 for r in jockey_races if r.get('rank') == '1')
                    
                    # Eğer jokey eşleşmesi bulunamadıysa, tüm yarışları say
                    if len(jockey_races) == 0 and current_jockey:
                        jockey_races = races
                        jockey_wins = sum(1 for r in races if r.get('rank') == '1')
                    
                    jockey_stats = {
                        'name': current_jockey,
                        'totalRaces': len(jockey_races),
                        'wins': jockey_wins,
                        'winRate': round(jockey_wins / len(jockey_races) * 100) if jockey_races else 0
                    } if current_jockey else None
                    
                    # 2. Jokey Değişimi - daha akıllı karşılaştırma
                    last_jockey = races[0].get('jockey', '').strip() if races else ''
                    jockey_changed = current_jockey and last_jockey and not jockey_match(current_jockey, last_jockey)
                    print(f"[DEBUG] Son jokey: '{last_jockey}', Değişti mi: {jockey_changed}")
                    
                    # 3. Kilo Değişimi - Fazla kilo dahil toplam hesapla
                    current_weight = original_horse.get('weight', '').strip()
                    last_weight = races[0].get('weight', '').strip() if races else ''
                    weight_change = None
                    
                    def parse_weight(w_str):
                        """Parse weight string like '50+2.00Fazla Kilo' -> 52.0"""
                        if not w_str:
                            return None
                        import re
                        # İlk sayı (temel kilo)
                        base_match = re.match(r'(\d+[,.]?\d*)', w_str)
                        if not base_match:
                            return None
                        base = float(base_match.group(1).replace(',', '.'))
                        # + işaretinden sonraki bonus kilo
                        bonus_match = re.search(r'\+(\d+[,.]?\d*)', w_str)
                        if bonus_match:
                            bonus = float(bonus_match.group(1).replace(',', '.'))
                            return base + bonus
                        return base
                    
                    try:
                        cw = parse_weight(current_weight)
                        lw = parse_weight(last_weight)
                        print(f"[DEBUG] Current weight parsed: {cw}, Last weight parsed: {lw}")
                        if cw is not None and lw is not None:
                            weight_change = round(cw - lw, 1)
                            if weight_change == 0:
                                weight_change = None  # 0 ise gösterme
                            print(f"[DEBUG] Kilo değişimi: {weight_change}")
                    except Exception as e:
                        print(f"[DEBUG] Kilo parse hatası: {e}")
                    
                    # 4. En İyi Derece - İdman süresinden alınıyor
                    best_time = training_best_time  # İdman verisinden
                    
                    analyzed_horses.append({
                        'name': horse_data['name'],
                        'no': original_horse.get('no', ''),
                        'aiScore': ai_score,
                        'formIndex': {
                            'trend': 'UP' if trend_value > 0 else 'DOWN' if trend_value < 0 else 'STABLE',
                            'trendValue': trend_value,
                        },
                        'raceHistory': races,
                        'stats': {
                            'avgRank': round(avg_rank, 1) if avg_rank > 0 else None,
                            'winRate': round(wins / len(ranks) * 100) if ranks else None,
                            'podiumRate': round(podiums / len(ranks) * 100) if ranks else None,
                            'trackWins': track_wins if track_wins > 0 else None,
                            'distanceWins': distance_wins if distance_wins > 0 else None,
                        },
                        # Gelişmiş bahis istatistikleri
                        'jockeyStats': jockey_stats,
                        'jockeyChanged': jockey_changed,
                        'weightChange': weight_change,
                        'bestTime': best_time,
                        'raceCount': len(races),
                        # === İDMAN BİLGİLERİ ===
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
                        } if training_data else None
                    })
                else:
                    # Veri çekilemediyse
                    analyzed_horses.append({
                        'name': original_horse.get('name', 'Bilinmiyor'),
                        'no': original_horse.get('no', ''),
                        'aiScore': 0,
                        'paceAnalysis': {
                            'earlySpeed': 0,
                            'earlySpeedLabel': 'Veri Yok',
                            'lateKick': 0,
                            'lateKickLabel': 'Veri Yok'
                        },
                        'formIndex': {
                            'trend': '-',
                            'trendValue': 0,
                            'trendScore': 0,
                            'trendLabel': 'Veri Yok'
                        },
                        'consistency': {
                            'score': 0,
                            'label': 'Veri Yok'
                        },
                        'suitability': {
                            'trackScore': 0,
                            'trackLabel': 'Veri Yok',
                            'distanceScore': 0,
                            'distanceLabel': 'Veri Yok'
                        },
                        'prediction': 'Veri Yok',
                        'insight': 'Geçmiş yarış verisi bulunamadı',
                        'raceCount': 0
                    })

        # 4. Sıralama (Yüksek AI puanından düşüğe)
        analyzed_horses.sort(key=lambda x: x['aiScore'], reverse=True)
        
        # 5. Sıralama numaraları ekle
        for i, horse in enumerate(analyzed_horses):
            horse['rank'] = i + 1
        
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
        
        return jsonify({
            'success': True,
            'results': analyzed_horses,
            'raceInsight': race_insight,
            'targetDistance': target_distance,
            'targetTrack': target_track,
            'processTime': process_time
        })
        
    except Exception as e:
        print(f"[ANALYZE ERROR] {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    import os
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
