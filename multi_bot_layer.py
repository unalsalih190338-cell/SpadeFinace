# ════════════════════════════════════════════════════════════════════════════════
#  MULTI-BOT KATMANI — bu bloğu bot.py'de load_dotenv() çağrısından hemen
#  SONRA, TOKEN satırının ÜSTÜNE yapıştırın.
# ════════════════════════════════════════════════════════════════════════════════
import os
import threading
import telebot

# ── 1. Alt-Bot Tokenleri ────────────────────────────────────────────────────────
SCANNER_BOT_TOKEN   = (os.getenv("SCANNER_BOT_TOKEN")   or "").strip()
NEWS_BOT_TOKEN      = (os.getenv("NEWS_BOT_TOKEN")      or "").strip()
TRADEBOOK_BOT_TOKEN = (os.getenv("TRADEBOOK_BOT_TOKEN") or "").strip()
PROF_BOT_TOKEN      = (os.getenv("PROF_BOT_TOKEN")      or "").strip()
ANALYSIS_BOT_TOKEN  = (os.getenv("ANALYSIS_BOT_TOKEN")  or "").strip()

# ── 2. Kanal & Topic ID Sabitleri ───────────────────────────────────────────────
CHANNEL_ID      = int(os.getenv("CHANNEL_ID",      "-1003827065867"))
TOPIC_CEO       = int(os.getenv("TOPIC_CEO",       "7"))
TOPIC_SCANNER   = int(os.getenv("TOPIC_SCANNER",   "754"))
TOPIC_ANALYSIS  = int(os.getenv("TOPIC_ANALYSIS",  "13"))
TOPIC_NEWS      = int(os.getenv("TOPIC_NEWS",       "9"))
TOPIC_TRADEBOOK = int(os.getenv("TOPIC_TRADEBOOK", "229"))
TOPIC_PROF      = int(os.getenv("TOPIC_PROF",       "17"))
TOPIC_DEBUG     = int(os.getenv("TOPIC_DEBUG",      "15"))

# ── 3. Alt-Bot pyTelegramBotAPI Örnekleri ───────────────────────────────────────
# Her bot kendi token'ı ile ayrı bir TeleBot nesnesidir.
# CEO Bot (ana bot) zaten aşağıda TOKEN ile oluşturulacak.
scanner_bot   = telebot.TeleBot(SCANNER_BOT_TOKEN,   threaded=False) if SCANNER_BOT_TOKEN   else None
news_bot      = telebot.TeleBot(NEWS_BOT_TOKEN,      threaded=False) if NEWS_BOT_TOKEN      else None
tradebook_bot = telebot.TeleBot(TRADEBOOK_BOT_TOKEN, threaded=False) if TRADEBOOK_BOT_TOKEN else None
prof_bot      = telebot.TeleBot(PROF_BOT_TOKEN,      threaded=False) if PROF_BOT_TOKEN      else None
analysis_bot  = telebot.TeleBot(ANALYSIS_BOT_TOKEN,  threaded=False) if ANALYSIS_BOT_TOKEN  else None

# ── 4. Kanalda Topic'e Mesaj Gönderme Yardımcısı ────────────────────────────────
def send_to_topic(topic_id: int, text: str, parse_mode: str = "Markdown") -> bool:
    """
    CEO Bot'u kullanarak kanalın belirtilen topic'ine mesaj gönderir.
    bot nesnesi bu fonksiyonun altında tanımlandığı için geç bağlama (late-binding)
    yapısında çalışır — modül tamamen yüklendikten sonra çağrılmalıdır.
    """
    try:
        # 'bot' CEO bot'tur; mevcut bot.py'de global olarak tanımlı
        bot.send_message(
            CHANNEL_ID,
            text,
            message_thread_id=topic_id,
            parse_mode=parse_mode
        )
        return True
    except Exception as e:
        print(f"[TOPIC-SEND] topic={topic_id} hata: {e}")
        return False

def topic_for_bot(bot_name: str) -> int:
    """Bot adına göre ilgili topic ID'sini döner."""
    mapping = {
        "scanner":   TOPIC_SCANNER,
        "news":      TOPIC_NEWS,
        "tradebook": TOPIC_TRADEBOOK,
        "prof":      TOPIC_PROF,
        "analysis":  TOPIC_ANALYSIS,
        "ceo":       TOPIC_CEO,
        "debug":     TOPIC_DEBUG,
    }
    return mapping.get(bot_name.lower(), TOPIC_CEO)

# ── 5. CEO Orkestrasyon Katmanı ──────────────────────────────────────────────────
# Kullanıcı → CEO Bot → ilgili fonksiyon → Kanal Topic → CEO Bot → Kullanıcıya rapor

CEO_COMMAND_MAP = {
    # Komut adı        : (hedef bot adı,  yönlendirilecek fonksiyon adı)
    "tara":             ("scanner",    "tara_single_or_all"),
    "check":            ("scanner",    "scan_all_stocks"),
    "spade":            ("scanner",    "tara_spade"),
    "haber":            ("news",       "cmd_haber"),
    "bulten":           ("news",       "send_bulten"),
    "al":               ("tradebook",  "cmd_kitap_al"),
    "sat":              ("tradebook",  "cmd_kitap_sat"),
    "kitap":            ("tradebook",  "show_kitap"),
    "karzarar":         ("tradebook",  "cmd_karzarar"),
    "analiz":           ("analysis",   "cmd_analiz"),
    "backtest":         ("prof",       "cmd_backtest"),
    "optimize":         ("prof",       "run_optimize"),
    "optimizeall":      ("prof",       "run_optimize_all"),
}

def ceo_dispatch(chat_id: int, command: str, params: str = "", message=None):
    """
    CEO Agent merkezi yönlendirici.
    Komutu ilgili bota yönlendirir, tamamlanınca CEO odasına rapor gönderir.

    Akış:
        Kullanıcı Komutu
            → CEO Bot bu fonksiyonu çağırır
                → İlgili bot fonksiyonu çalışır (kendi topic'ine mesaj atar)
                    → CEO CEO odasına özet rapor atar
                        → Kullanıcıya DM ile bildirim gönderilir
    """
    entry = CEO_COMMAND_MAP.get(command.lower())
    if not entry:
        return  # Bilinmeyen komut — normal handler devam eder

    bot_name, func_name = entry
    topic_id = topic_for_bot(bot_name)

    # Kanalda "komut işleniyor" bildirimi
    send_to_topic(
        topic_id,
        f"⚙️ *{bot_name.upper()} BOT* işleme aldı\n"
        f"Komut: `/{command} {params}`\n"
        f"İstekte Bulunan: `{chat_id}`"
    )

# ── 6. Alt-Bot Webhook Route'ları ────────────────────────────────────────────────
# CEO Bot'un Flask app'ine alt-bot webhook yolları eklenir.
# Bu fonksiyon bot.py'de Flask app nesnesi oluşturulduktan SONRA çağrılmalıdır.

def register_sub_bot_webhooks(app_instance, render_url: str):
    """
    Her alt-bot için ayrı webhook route kaydeder.
    bot.py'deki app nesnesi oluşturulduktan hemen sonra çağırın:

        register_sub_bot_webhooks(app, RENDER_URL)
    """
    from flask import request, abort

    bots_config = [
        ("scanner",   scanner_bot,   SCANNER_BOT_TOKEN),
        ("news",      news_bot,      NEWS_BOT_TOKEN),
        ("tradebook", tradebook_bot, TRADEBOOK_BOT_TOKEN),
        ("prof",      prof_bot,      PROF_BOT_TOKEN),
        ("analysis",  analysis_bot,  ANALYSIS_BOT_TOKEN),
    ]

    for bot_name, bot_obj, token in bots_config:
        if not bot_obj or not token:
            print(f"[WEBHOOK] {bot_name} token eksik — atlandı.")
            continue

        # Closure için default argüman hilesini kullanıyoruz
        def make_handler(b=bot_obj, t=token, n=bot_name):
            def handler():
                if request.headers.get("content-type") == "application/json":
                    update = telebot.types.Update.de_json(
                        request.get_data(as_text=True)
                    )
                    threading.Thread(
                        target=b.process_new_updates,
                        args=([update],),
                        daemon=True
                    ).start()
                    return "OK", 200
                abort(403)
            handler.__name__ = f"webhook_{n}"
            return handler

        route = f"/webhook/{token}"
        app_instance.add_url_rule(
            route,
            view_func=make_handler(),
            methods=["POST"]
        )
        print(f"[WEBHOOK] /{bot_name} → {route[:30]}... kaydedildi")

def set_all_webhooks(render_url: str):
    """Tüm alt-botların webhook'larını Telegram'a kaydeder."""
    bots_config = [
        ("scanner",   scanner_bot,   SCANNER_BOT_TOKEN),
        ("news",      news_bot,      NEWS_BOT_TOKEN),
        ("tradebook", tradebook_bot, TRADEBOOK_BOT_TOKEN),
        ("prof",      prof_bot,      PROF_BOT_TOKEN),
        ("analysis",  analysis_bot,  ANALYSIS_BOT_TOKEN),
    ]
    import time
    for bot_name, bot_obj, token in bots_config:
        if not bot_obj or not token:
            continue
        try:
            bot_obj.remove_webhook()
            time.sleep(0.5)
            webhook_url = f"{render_url}/webhook/{token}"
            bot_obj.set_webhook(url=webhook_url)
            print(f"[WEBHOOK-SET] {bot_name}: {webhook_url[:50]}...")
        except Exception as e:
            print(f"[WEBHOOK-SET] {bot_name} hata: {e}")

# ════════════════════════════════════════════════════════════════════════════════
#  MEVCUT bot.py KODU BURADAN DEVAM EDER
# ════════════════════════════════════════════════════════════════════════════════
