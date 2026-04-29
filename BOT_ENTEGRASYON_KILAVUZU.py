# ════════════════════════════════════════════════════════════════════════════
#  BOT.PY ENTEGRASYON KILAVUZU
#  Bu dosya bot.py'ye hangi satırların nereye ekleneceğini gösterir.
#  Sırası ÇOK ÖNEMLİDİR, lütfen aşağıdaki adımları sırayla uygulayın.
# ════════════════════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────────────────
# DEĞİŞİKLİK 1 — Dosyanın EN BAŞI (load_dotenv() sonrası, TOKEN öncesi)
# ──────────────────────────────────────────────────────────────────────────
# MEVCUT KOD:
#   from dotenv import load_dotenv
#   load_dotenv()
#   TOKEN = os.getenv("TELEGRAM_TOKEN") or "".strip()
#
# YENİ DURUM:
#   from dotenv import load_dotenv
#   load_dotenv()
#
#   from multi_bot_layer import (                           # ← EKLE
#       CHANNEL_ID, TOPIC_CEO, TOPIC_SCANNER, TOPIC_NEWS,  # ← EKLE
#       TOPIC_TRADEBOOK, TOPIC_PROF, TOPIC_DEBUG,          # ← EKLE
#       TOPIC_ANALYSIS, send_to_topic, ceo_dispatch,       # ← EKLE
#       register_sub_bot_webhooks, set_all_webhooks,       # ← EKLE
#       scanner_bot, news_bot, tradebook_bot, prof_bot     # ← EKLE
#   )                                                       # ← EKLE
#
#   TOKEN = os.getenv("TELEGRAM_TOKEN") or "".strip()
#

# ──────────────────────────────────────────────────────────────────────────
# DEĞİŞİKLİK 2 — set_webhook() fonksiyonunun sonuna ekle
# ──────────────────────────────────────────────────────────────────────────
# MEVCUT KOD:
#   def set_webhook():
#       if not RENDER_URL:
#           print("HATA: RENDER_URL eksik!")
#           return
#       bot.remove_webhook()
#       time.sleep(1)
#       print(f"Webhook: {bot.set_webhook(url=f'{RENDER_URL}/webhook/{TOKEN}')}")
#       set_bot_commands()
#
# YENİ DURUM: (son iki satırın arasına ekle)
#   def set_webhook():
#       if not RENDER_URL:
#           print("HATA: RENDER_URL eksik!")
#           return
#       bot.remove_webhook()
#       time.sleep(1)
#       print(f"Webhook: {bot.set_webhook(url=f'{RENDER_URL}/webhook/{TOKEN}')}")
#       set_all_webhooks(RENDER_URL)   # ← EKLE — alt-bot webhook'ları
#       set_bot_commands()
#

# ──────────────────────────────────────────────────────────────────────────
# DEĞİŞİKLİK 3 — app = Flask(__name__) satırından HEMEN SONRA
# ──────────────────────────────────────────────────────────────────────────
# MEVCUT KOD:
#   app = Flask(__name__)
#
# YENİ DURUM:
#   app = Flask(__name__)
#   register_sub_bot_webhooks(app, RENDER_URL)   # ← EKLE
#

# ──────────────────────────────────────────────────────────────────────────
# DEĞİŞİKLİK 4 — Kanal'a mesaj gönderim örnekleri
#   (mevcut bot.send_message çağrılarının yanına opsiyonel ekleme)
# ──────────────────────────────────────────────────────────────────────────
# Tarama tamamlandığında Scanner Topic'e kayıt at:
#   send_to_topic(TOPIC_SCANNER, f"✅ Tarama tamamlandı: {len(eslesen)} eşleşme")
#
# Haber geldiğinde News Topic'e at:
#   send_to_topic(TOPIC_NEWS, formatted_news_text)
#
# Trade kaydedildiğinde İşlem Topic'e at:
#   send_to_topic(TOPIC_TRADEBOOK, f"📒 {ticker} AL kaydedildi — {fiyat:.2f}")
#
# Hata varsa Debug Topic'e at:
#   send_to_topic(TOPIC_DEBUG, f"⚠️ Hata: {str(e)[:200]}")
#
# CEO raporu:
#   send_to_topic(TOPIC_CEO, f"📊 CEO Raporu:\n{ozet}")
#

# ════════════════════════════════════════════════════════════════════════════
#  TOPLAM 3 SATIR EKLENİYOR, 0 SATIR DEĞİŞTİRİLİYOR
#  Mevcut bot.py kodu tamamen korunuyor.
# ════════════════════════════════════════════════════════════════════════════
