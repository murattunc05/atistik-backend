# Handikap ML Inceleme Notu - 2026-06-16

## Model

- Yeni shadow model: `shadow-20260616-0113`
- Egitim kaynagi: canli `/api/ml-export?labeled_only=true`
- Labeled entries: `3994`
- Train/validation race split: `323 / 81`
- Gorunur siralama etkisi: yok, sistem `shadow_only`

## Genel ML Sonucu

| Model | Races | Top1 | Winner Top3 | Winner Top5 | Rho | MAE | NDCG@5 |
|---|---:|---:|---:|---:|---:|---:|---:|
| v4.14/v4.15 validation | 81 | 19/81 | 49/81 | 66/81 | 0.375 | 2.864 | 0.816 |
| ML + AGF | 81 | 21/81 | 50/81 | 63/81 | 0.427 | 2.775 | 0.829 |
| ML - AGF | 81 | 23/81 | 48/81 | 64/81 | 0.419 | 2.790 | 0.820 |
| AGF only | 81 | 26/81 | 53/81 | 70/81 | 0.513 | 2.423 | 0.862 |

Genel havuzda ML, v4'e karsi kucuk ama gercek bir iyilesme veriyor. Buna ragmen
AGF only halen cok guclu bir benchmark; bu, AGF'nin bazi profillerde piyasa
bilgisi olarak ciddi sinyal tasidigini gosteriyor.

## Handikap Sonucu

| Model | Races | Top1 | Winner Top3 | Winner Top5 | Rho | MAE | NDCG@5 | Avg winner rank |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| v4.15 | 25 | 5/25 | 13/25 | 18/25 | 0.264 | 3.385 | 0.752 | 4.160 |
| ML + AGF | 25 | 6/25 | 11/25 | 17/25 | 0.269 | 3.341 | 0.749 | 4.160 |
| ML - AGF | 25 | 8/25 | 12/25 | 17/25 | 0.255 | 3.304 | 0.738 | 4.040 |
| AGF only | 25 | 8/25 | 18/25 | 20/25 | 0.415 | 2.769 | 0.803 | 3.040 |

Handikapta yeni ML gorunur aday degil. Top1 tarafinda ML - AGF iyi gorunse de
kazananin ilk 3'e yerlestirilmesi, Rho ve NDCG AGF only tarafinda belirgin daha
iyi. v4.15 de Handikapta zayif, fakat ML bu zayifligi tek basina cozmuyor.

## Son Donem Handikap Metrikleri

| Metrik | Tum Handikap WTop3 | 12.06+ WTop3 | v4.15 WTop3 | Not |
|---|---:|---:|---:|---|
| `agf_score` | 65/107 | 15/19 | 9/10 | En guclu canli sinyal |
| `pace_score` | 58/107 | 13/19 | 7/10 | AGF disi en tutarli aday |
| `form_trend` | 55/107 | 7/19 | 5/10 | Genel havuzda iyi, son donemde zayif |
| `training_fitness` | 47/107 | 10/19 | 5/10 | Son donemde orta sinyal |
| `distance_suit` | 47/107 | 12/19 | 6/10 | Top1 katkisi var, Rho zayif |
| `running_style_proxy_score` | 53/107 | 9/19 | 4/10 | Tek basina yeterli degil |

AGF kaldirildiktan sonra Handikap kaybinin ana nedeni, AGF payinin
`degree_avg/training_fitness` tarafina dagitilmesinin ayni siralama sinyalini
uretememesi. Son 19 Handikapta AGF, kazanani ilk 3'e yerlestirmede 15/19
basari vermis; v4.15 agirliklari bu seviyeye yaklasmiyor.

## Karar

- Yeni ML modeli shadow olarak deploy edilsin; gorunur v4.15 siralamasi
  degismeyecek.
- Handikap metrikleri hemen guncellenmesin. Bu rapor, AGF'siz Handikap
  formulunun yeterince guclu olmadigini gosteriyor.
- Handikap icin bir sonraki calisma, iki ayri adayla yapilmali:
  - `AGF capped`: AGF tamamen yasak degil, dusuk tavanli piyasa sinyali.
  - `AGF-free`: `pace_score`, `distance_suit`, `training_fitness`,
    `running_style_proxy_score` ve `handicap_class_transition_score` agirlikli
    shadow aday.
- Alt profil ayrimi zorunlu gorunuyor. Ozellikle `HANDIKAP15|Cim`,
  `HANDIKAP15|Kum`, `HANDIKAP16|Kum` ayni agirlikla iyi davranmiyor.

## Bir Sonraki Adim

Handikap icin canliya dokunmadan iki shadow v4 aday uretilmeli:

1. `handikap_agf_capped_shadow`
2. `handikap_agf_free_shadow`

Bu iki aday, mevcut v4.15 ve yeni ML ile ayni export uzerinde en az `+15`
Handikap sonucu daha izlendikten sonra karsilastirilacak.
