# 🤖 BIST Multi-Bot Sistemi

> **6 Telegram botu tek Render servisinde** — CEO Bot üzerinden tam orkestrasyon

---

## 📐 Sistem Mimarisi

```
Sen
 └─► CeoWarrenBot  (/tara, /check, /analiz, /haber, /al, /sat ...)
       │
       ├─► ScannerAgentBot   → Tarama Odası (topic: 754)
       ├─► NewsScanAgentBot  → Haber Odası  (topic: 9)
       ├─► TradeBookAgentBot → İşlem Odası  (topic: 229)
       ├─► ProfGenBot        → ProfGen Odası (topic: 17)
       └─► AnalysisBot       → Finance Check Odası (topic: 13) [yakında]

CEO Odası (topic: 7) — Tüm özetler ve raporlar
Hata Ayıklama Odası (topic: 15) — Sistem logları
```

---

## 📁 Dosya Yapısı

```
├── bot.py                  # Ana bot kodu (tüm mantık burada)
├── multi_bot_layer.py      # Çok-bot katmanı (bot.py'ye entegre edilir)
├── requirements.txt        # Python bağımlılıkları
├── Procfile                # Render başlatma komutu
├── render.yaml             # Render otomatik deploy konfigürasyonu
├── .env.example            # Ortam değişkenleri şablonu
├── .gitignore              # Git'e gönderilemeyecek dosyalar
└── README.md               # Bu dosya
```

---

## 🚀 Kurulum — Adım Adım

### Adım 1: GitHub Repo Hazırlama

```bash
# 1. Yeni repo oluştur → github.com/new
# 2. Klonla
git clone https://github.com/KULLANICI_ADIN/bist-multi-bot.git
cd bist-multi-bot

# 3. Dosyaları kopyala
cp /dosyalar/* .

# 4. İlk commit
git add .
git commit -m "feat: initial multi-bot setup"
git push origin main
```

### Adım 2: bot.py'ye Multi-Bot Katmanını Entegre Etme

`bot.py` dosyasını açın ve `load_dotenv()` çağrısından **hemen sonra**,
`TOKEN = os.getenv(...)` satırından **hemen önce** şunu ekleyin:

```python
# multi_bot_layer.py içeriğini buraya yapıştırın
# VEYA import edin:
from multi_bot_layer import (
    CHANNEL_ID, TOPIC_CEO, TOPIC_SCANNER, TOPIC_NEWS,
    TOPIC_TRADEBOOK, TOPIC_PROF, TOPIC_DEBUG, TOPIC_ANALYSIS,
    send_to_topic, ceo_dispatch, register_sub_bot_webhooks, set_all_webhooks,
    scanner_bot, news_bot, tradebook_bot, prof_bot
)
```

`set_webhook()` fonksiyonunu bulun ve sonuna şunu ekleyin:

```python
def set_webhook():
    # ... mevcut kod ...
    bot.set_webhook(url=f"{RENDER_URL}/webhook/{TOKEN}")
    set_all_webhooks(RENDER_URL)              # ← YENİ: Alt-bot webhook'ları
    setbotcommands()
```

`app = Flask(__name__)` satırından **sonra** ekleyin:

```python
register_sub_bot_webhooks(app, RENDER_URL)   # ← YENİ: Alt-bot route'ları
```

### Adım 3: Render PostgreSQL Veritabanı

1. render.com → Dashboard → **New +** → **PostgreSQL**
2. Name: `bist-bot-db` | Plan: **Free** (1 GB)
3. Oluştur → **External Database URL**'i kopyala

### Adım 4: Render Web Service

1. render.com → Dashboard → **New +** → **Web Service**
2. GitHub reponuzu bağlayın
3. **Environment Variables** bölümüne aşağıdaki değişkenleri girin:

| Değişken | Değer |
|---|---|
| `TELEGRAM_TOKEN` | CeoWarrenBot token (BotFather) |
| `SCANNER_BOT_TOKEN` | ScannerAgentBot token |
| `NEWS_BOT_TOKEN` | NewsScanAgentBot token |
| `TRADEBOOK_BOT_TOKEN` | TradeBookAgentBot token |
| `PROF_BOT_TOKEN` | ProfGenBot token |
| `ANALYSIS_BOT_TOKEN` | (boş bırakın) |
| `DATABASE_URL` | Render PostgreSQL URL |
| `CHANNEL_ID` | `-1003827065867` |
| `TOPIC_CEO` | `7` |
| `TOPIC_SCANNER` | `754` |
| `TOPIC_ANALYSIS` | `13` |
| `TOPIC_NEWS` | `9` |
| `TOPIC_TRADEBOOK` | `229` |
| `TOPIC_PROF` | `17` |
| `TOPIC_DEBUG` | `15` |
| `RENDER_URL` | `https://bist-multi-bot.onrender.com` |
| `GEMINI_KEY` | Google AI Studio API Key |
| `GROQ_KEY` | Groq API Key |
| `WATCHDOG_CHAT_ID` | Kendi chat ID'niz |
| `TD_DELAY` | `1.5` |

4. **Deploy** → Log'ları izleyin

### Adım 5: Telegram Kanal Ayarları

Kanalın botları admin olarak eklendiğinden emin olun:
- Kanal → Yöneticiler → Her botu ekle
- İzinler: **Mesaj Gönder** ✅ | **Başlık Gönder** ✅

### Adım 6: Test

CeoWarrenBot'a mesaj gönderin:
```
/kontrolbot
```
Tüm sistemler ✅ gelirse kurulum tamamdır.

---

## 🔧 Kanal Topic ID Referansı

| Oda | Topic ID | URL |
|---|---|---|
| CEO Odası | `7` | t.me/c/3827065867/7 |
| Tarama Odası | `754` | t.me/c/3827065867/754 |
| Finance Check | `13` | t.me/c/3827065867/13 |
| Haber Odası | `9` | t.me/c/3827065867/9 |
| İşlem Odası | `229` | t.me/c/3827065867/229 |
| ProfGen Odası | `17` | t.me/c/3827065867/17 |
| Hata Ayıklama | `15` | t.me/c/3827065867/15 |

---

## 📊 Bot Görev Tablosu

| Bot | Görev | Otomatik Görev |
|---|---|---|
| **CeoWarrenBot** 👑 | Tüm komutları al, yönlendir, raporla | — |
| **ScannerAgentBot** 🔍 | 18 strateji BIST taraması, MTF skor | 18:05 günlük tarama |
| **NewsScanAgentBot** 📰 | RSS haber, piyasa özeti, kriz alarmı | 09:05 sabah / 18:30 akşam bülteni |
| **TradeBookAgentBot** 📒 | Trade kayıt, kar/zarar, pozisyon takibi | — |
| **ProfGenBot** 📈 | Backtest, EMA optimize, Gemini analiz | Cuma 17:00 strateji |
| **AnalysisBot** 🧠 | Derin analiz (yakında) | — |

---

## ⚠️ Sorun Giderme

**Bot cevap vermiyorsa:**
```
/status → bot.py çalışıyor mu?
/kontrolbot → tüm sistemleri test eder
```

**Webhook hatası:**
```python
# Render loglarında bakın:
# [WEBHOOK-SET] scanner: https://...
# [WEBHOOK] /news → /webhook/... kaydedildi
```

**DB bağlantı hatası:**
- `DATABASE_URL` değişkeninde `sslmode=require` olduğundan emin olun

---

## 📝 Lisans

Özel kullanım. Ticari dağıtım yasaktır.
