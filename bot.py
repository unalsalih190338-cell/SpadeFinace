import os
import collections
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import telebot
import pandas as pd
import numpy as np
import traceback
import requests
from dotenv import load_dotenv
import pytz
import psycopg2
import psycopg2.pool
import xml.etree.ElementTree as ET
from flask import Flask, request, abort

load_dotenv()
TOKEN        = (os.getenv('TELEGRAM_TOKEN') or '').strip()
RENDER_URL   = (os.getenv('RENDER_URL') or '').strip()
DATABASE_URL = (os.getenv('DATABASE_URL') or '').strip()
PORT         = int(os.getenv('PORT', 10000))
_wdog_raw    = (os.getenv('WATCHDOG_CHAT_ID') or '').strip()
WATCHDOG_CHAT_ID = int(_wdog_raw) if _wdog_raw.isdigit() else None
GEMINI_KEY   = (os.getenv('GEMINI_KEY')  or '').strip()
GEMINI_KEY2  = (os.getenv('GEMINI_KEY2') or '').strip()
GEMINI_KEY3  = (os.getenv('GEMINI_KEY3') or '').strip()
GROQ_KEY     = (os.getenv('GROQ_KEY')  or '').strip()
GROQ_KEY2    = (os.getenv('GROQ_KEY2') or '').strip()
GROQ_KEY3    = (os.getenv('GROQ_KEY3') or '').strip()


# ═══════════════════════════════════════════════════════════════════════
# MULTI-BOT KATMANI
# ═══════════════════════════════════════════════════════════════════════
SCANNER_BOT_TOKEN   = (os.getenv('SCANNER_BOT_TOKEN')   or '').strip()
NEWS_BOT_TOKEN      = (os.getenv('NEWS_BOT_TOKEN')      or '').strip()
TRADEBOOK_BOT_TOKEN = (os.getenv('TRADEBOOK_BOT_TOKEN') or '').strip()
PROF_BOT_TOKEN      = (os.getenv('PROF_BOT_TOKEN')      or '').strip()
ANALYSIS_BOT_TOKEN  = (os.getenv('ANALYSIS_BOT_TOKEN')  or '').strip()
GEMINI_KEY4  = (os.getenv('GEMINI_KEY4') or '').strip()
GEMINI_KEY5  = (os.getenv('GEMINI_KEY5') or '').strip()
GEMINI_PRO_KEY = (os.getenv('GEMINI_PRO_KEY') or '').strip()
GROQ_KEY4    = (os.getenv('GROQ_KEY4') or '').strip()
GROQ_KEY5    = (os.getenv('GROQ_KEY5') or '').strip()

CHANNEL_ID      = int(os.getenv('CHANNEL_ID',      '0') or '0')
TOPIC_CEO       = int(os.getenv('TOPIC_CEO',        '7'))
TOPIC_SCANNER   = int(os.getenv('TOPIC_SCANNER',    '754'))
TOPIC_ANALYSIS  = int(os.getenv('TOPIC_ANALYSIS',   '13'))
TOPIC_NEWS      = int(os.getenv('TOPIC_NEWS',        '9'))
TOPIC_TRADEBOOK = int(os.getenv('TOPIC_TRADEBOOK',  '229'))
TOPIC_PROF      = int(os.getenv('TOPIC_PROF',        '17'))
TOPIC_DEBUG     = int(os.getenv('TOPIC_DEBUG',       '15'))

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# ── Sub-bot örnekleri ─────────────────────────────────────────────────────────
_scanner_bot   = telebot.TeleBot(SCANNER_BOT_TOKEN,   threaded=False) if SCANNER_BOT_TOKEN   else None
_news_bot      = telebot.TeleBot(NEWS_BOT_TOKEN,      threaded=False) if NEWS_BOT_TOKEN      else None
_tradebook_bot = telebot.TeleBot(TRADEBOOK_BOT_TOKEN, threaded=False) if TRADEBOOK_BOT_TOKEN else None
_prof_bot      = telebot.TeleBot(PROF_BOT_TOKEN,      threaded=False) if PROF_BOT_TOKEN      else None
_analysis_bot  = telebot.TeleBot(ANALYSIS_BOT_TOKEN,  threaded=False) if ANALYSIS_BOT_TOKEN  else None

def agent_send(agent_bot, topic_id, text, parse_mode='Markdown'):
    """Sub-bot ile kanal topic'e mesaj gönder. Yoksa CEO bot kullanır."""
    _bot = agent_bot if agent_bot else bot
    try:
        _bot.send_message(CHANNEL_ID, text,
                          message_thread_id=topic_id,
                          parse_mode=parse_mode)
    except Exception:
        clean = text.replace('*','').replace('_','').replace('`','')
        try:
            _bot.send_message(CHANNEL_ID, clean,
                              message_thread_id=topic_id)
        except Exception as e:
            print(f'[agent_send] hata topic={topic_id}: {e}')

def set_all_webhooks(render_url):
    """Tüm alt-bot webhook'larını ayarla."""
    for name, b, token in [
        ('scanner',   _scanner_bot,   SCANNER_BOT_TOKEN),
        ('news',      _news_bot,      NEWS_BOT_TOKEN),
        ('tradebook', _tradebook_bot, TRADEBOOK_BOT_TOKEN),
        ('prof',      _prof_bot,      PROF_BOT_TOKEN),
        ('analysis',  _analysis_bot,  ANALYSIS_BOT_TOKEN),
    ]:
        if not b or not token: continue
        try:
            b.remove_webhook(); time.sleep(0.4)
            b.set_webhook(url=f'{render_url}/webhook/{token}')
            print(f'[WEBHOOK-SET] {name} OK')
        except Exception as e:
            print(f'[WEBHOOK-SET] {name} hata: {e}')

def register_sub_webhooks(app_obj, render_url):
    """Alt-bot Flask route'larını kaydet."""
    for name, b, token in [
        ('scanner',   _scanner_bot,   SCANNER_BOT_TOKEN),
        ('news',      _news_bot,      NEWS_BOT_TOKEN),
        ('tradebook', _tradebook_bot, TRADEBOOK_BOT_TOKEN),
        ('prof',      _prof_bot,      PROF_BOT_TOKEN),
        ('analysis',  _analysis_bot,  ANALYSIS_BOT_TOKEN),
    ]:
        if not b or not token: continue
        def _make_view(bb=b, nn=name):
            def _view():
                if request.headers.get('content-type') == 'application/json':
                    upd = telebot.types.Update.de_json(request.get_data(as_text=True))
                    threading.Thread(target=bb.process_new_updates, args=([upd],), daemon=True).start()
                    return 'OK', 200
                abort(403)
            _view.__name__ = f'webhook_{nn}'
            return _view
        app_obj.add_url_rule(f'/webhook/{token}', view_func=_make_view(), methods=['POST'])
        print(f'[WEBHOOK] {name} route kaydedildi.')


# ═══════════════════════════════════════════════
# GLOBAL LOCK & THREAD POOL
# ═══════════════════════════════════════════════
_yahoo_lock = threading.Lock()
_task_executor = ThreadPoolExecutor(max_workers=4)
_heavy_task_lock = threading.Lock()

_cancel_flags = {}
def get_cancel_flag(chat_id, op):
    chat_id = str(chat_id)
    if chat_id not in _cancel_flags:
        _cancel_flags[chat_id] = {}
    if op not in _cancel_flags[chat_id]:
        _cancel_flags[chat_id][op] = threading.Event()
    return _cancel_flags[chat_id][op]

def reset_cancel_flag(chat_id, op):
    flag = get_cancel_flag(chat_id, op)
    flag.clear()
    return flag

def is_cancelled(chat_id, op):
    return get_cancel_flag(chat_id, op).is_set()

TD_DELAY = float(os.getenv("TD_DELAY", "1.5"))

# ═══════════════════════════════════════════════
# SEKTÖR HARİTASI
# ═══════════════════════════════════════════════
SEKTOR_HISSELER = {
    "BANKACILIK": ["AKBNK","GARAN","HALKB","ISCTR","VAKBN","YKBNK","SKBNK","ALBRK","QNBFB","TSKB","KLNMA"],
    "HOLDINGLER": ["SAHOL","KCHOL","DOHOL","AGHOL","ECZYT","GLYHO","TAVHL","TKFEN"],
    "DEMIR_CELIK": ["EREGL","KRDMD","KRDMB","KRDMA","BRSAN"],
    "OTOMOTIV": ["TOASO","FROTO","DOAS","OTKAR","ASUZU","BRISA"],
    "HAVACILIK": ["THYAO","PGSUS","CLEBI","TAVHL"],
    "ENERJI": ["TUPRS","AKSEN","AYEN","ENKAI","ENJSA","AKENR","AYDEM","ZOREN","CWENE","EUPWR"],
    "PERAKENDE": ["BIMAS","SOKM","MGROS","MAVI","VAKKO","ADESE"],
    "TELEKOM": ["TCELL","TTKOM","NETAS"],
    "SIGORTA": ["AKGRT","ANHYT","AGESA","ANSGR","TURSG"],
    "TEKNOLOJI": ["ASELS","LOGO","ARENA","INDES","FONET","PAPIL","HUBVC"],
    "GMYO": ["EKGYO","ISGYO","HLGYO","TSGYO","RYGYO","MSGYO","DZGYO"],
    "KIMYA": ["PETKM","SASA","GUBRF","BAGFS","AKSA","SODA","NUHCM"],
    "GIDA": ["ULKER","BANVT","CCOLA","TATGD","AEFES","KERVT","FRIGO","PETUN"],
    "SAVUNMA": ["ASELS","KATMR","OTKAR"],
    "MADENCILIK": ["KOZAL","KOZAA","IPEKE"],
}
_TICKER_SEKTOR = {}
for _sek, _ticks in SEKTOR_HISSELER.items():
    for _t in _ticks:
        _TICKER_SEKTOR[_t] = _sek

# ═══════════════════════════════════════════════
# HABER ÖNCELİK
# ═══════════════════════════════════════════════
KRIZ_ANAHTAR = [
    "fed","faiz","rate","interest rate","inflation","enflasyon",
    "recession","resesyon","war","savaş","kriz","crisis",
    "sanctions","yaptırım","tcmb","merkez bankası","central bank",
    "pandemic","salgın","deprem","earthquake",
]
YUKSEK_ANAHTAR = [
    "oil","petrol","gold","altın","dollar","dolar","euro",
    "bist","borsa","endeks","trump","china","çin","rusya","ukraine","israel",
]
ORTA_ANAHTAR = [
    "piyasa","market","stock","hisse","ekonomi","economy",
    "büyüme","growth","istihdam","employment",
]

def rank_news_priority(news_item):
    text = (news_item.get("title","") + " " + news_item.get("desc","")).lower()
    for kw in KRIZ_ANAHTAR:
        if kw in text:
            return "kritik"
    for kw in YUKSEK_ANAHTAR:
        if kw in text:
            return "yuksek"
    for kw in ORTA_ANAHTAR:
        if kw in text:
            return "orta"
    return "dusuk"

def format_news_card(i, n, priority="orta"):
    emoji_map = {"kritik":"🔴","yuksek":"🟠","orta":"🟡","dusuk":"🟢"}
    em = emoji_map.get(priority, "🟡")
    lines = [f"{em} {i}. [{n.get('source','?')}] {n.get('title','').strip()}"]
    if n.get("desc"):
        lines.append(f"📝 {n['desc'][:160].strip()}")
    if n.get("pub"):
        lines.append(f"🕒 {n['pub'][:25]}")
    if n.get("link"):
        lines.append(f"🔗 {n['link']}")
    return "\n".join(lines)

def format_news_block(title, items, block_emoji="📰"):
    lines = [f"{block_emoji} {title}", "━━━━━━━━━━━━━━━━━━━"]
    for idx, item in enumerate(items[:5], 1):
        prio = item.get("priority", rank_news_priority(item))
        item["priority"] = prio
        lines.append(format_news_card(idx, item, prio))
        lines.append("")
    return "\n".join(lines)

def extract_sector_from_news(news_text):
    text = news_text.lower()
    found = []
    sector_keywords = {
        "BANKACILIK": ["banka","bank","faiz","kredi"],
        "HAVACILIK": ["havayolu","uçak","havacılık","thy","airline"],
        "ENERJI": ["enerji","petrol","doğalgaz","elektrik","energy"],
        "OTOMOTIV": ["otomobil","araç","otomotiv","car","automotive"],
        "DEMIR_CELIK": ["demir","çelik","steel","iron"],
        "TEKNOLOJI": ["teknoloji","yazılım","tech","software"],
        "PERAKENDE": ["perakende","mağaza","retail"],
        "SAVUNMA": ["savunma","silah","defense","military"],
        "MADENCILIK": ["altın","maden","gold","mining"],
    }
    for sek, keywords in sector_keywords.items():
        for kw in keywords:
            if kw in text:
                found.append(sek)
                break
    return list(set(found))

def get_related_tickers(sector):
    return SEKTOR_HISSELER.get(sector, [])

# ═══════════════════════════════════════════════
# BIST TAM LİSTE
# ═══════════════════════════════════════════════
_BIST_RAW = [
    "A1CAP","A1YEN","ACSEL","ADEL","ADANA","ADESE","ADGYO","AEFES","AFYON","AGESA",
    "AGHOL","AGROT","AHGAZ","AHSGY","AKENR","AKBNK","AKCNS","AKFGY","AKFIN","AKFIS",
    "AKFYE","AKGRT","AKHAN","AKMGY","AKPAZ","AKSA","AKSEN","AKSGY","AKSUE","AKTIF",
    "AKYHO","ALARK","ALBRK","ALCAR","ALCTL","ALFAS","ALGYO","ALKA","ALKIM","ALKLC",
    "ALMAD","ALNTF","ALPAY","ALTES","ALTIN","ALTNY","ALVES","AMBRA","AMTEK","ANACM",
    "ANELE","ANGEN","ANHYT","ANSGR","ANTKS","ARASE","ARCLK","ARDYZ","ARENA","ARFYE",
    "ARFYO","ARMGD","ARSAN","ARTMS","ARZUM","ASCEL","ASGYO","ASELS","ASKLR","ASPILSAN",
    "ASRNC","ASTOR","ATAGY","ATAKP","ATATP","ATATR","ATEKS","ATLAS","ATPET","ATSYH",
    "AVGYO","AVHOL","AVOD","AVPGY","AVTUR","AYCES","AYDEM","AYEN","AYNES","AYES",
    "AYGAZ","AZTEK","ASUZU",
    "BAGFS","BAHKM","BAKAB","BAKLK","BALAT","BALSU","BANBO","BANVT","BARMA","BASCM",
    "BASGZ","BATISOKE","BAYRK","BEGYO","BERA","BESLR","BESTE","BEYAZ","BFREN","BIMAS",
    "BIOEN","BIRTO","BJKAS","BKFIN","BLCYT","BMELK","BMSTL","BNTAS","BOSSA","BOYP",
    "BOZK","BRLSM","BRISA","BRKSN","BRSAN","BRSHP","BRYAT","BRYYH","BSOKE","BTCIM",
    "BUCIM","BURCE","BURVA","BVSAN","BYDNR","BYME","BYNDR","BIGGS",
    "CAFER","CANTE","CARFA","CARSA","CASA","CATES","CCOLA","CEMAS","CEMTS","CEPHE",
    "CEREN","CGCAM","CGINR","CGMYO","CHEFS","CIMSA","CLEBI","COFOR","CRDFA","CRFSA",
    "CRPCA","CUSAN","CVKMD","CWENE",
    "DAGHL","DARDL","DATNS","DENGE","DERI","DERHL","DERIM","DESA","DESAS","DESPC",
    "DEVA","DGATE","DIRIT","DITAS","DMSAS","DNISI","DOAS","DOBUR","DOGUB","DOHOL","DOKTA",
    "DORAY","DORTS","DPENS","DRTST","DURDO","DYOBY","DZGYO",
    "EAPRO","EBEBK","ECILC","ECZYT","EDIP","EFOR","EGGUB","EGEEN","EGPRO","EGSER",
    "EKGYO","EKIZ","EKOS","ELITE","EMKEL","EMNIS","EMPA","ENCTR","ENDL","ENERY",
    "ENJSA","ENKAI","EPLAS","EREGL","ERSU","ESCOM","ESEN","ESKYP","ETYAT","EUHOL",
    "EUPWR","EYGYO","EZRMK",
    "FADE","FENER","FLAP","FMIZP","FONET","FORTE","FRIGO","FROTO","FZLGY",
    "GARAN","GARFA","GBOOK","GEDIK","GEDZA","GENIL","GENTS","GEREL","GESAN","GIMAT",
    "GLBMD","GLRYH","GLYHO","GMTAS","GNDUZ","GOKNR","GOLDS","GOLTS","GOODY","GRSEL",
    "GRTRK","GRTHO","GSDDE","GSDHO","GSRAY","GUBRF","GWIND","GZNMI",
    "HALKB","HATEK","HEDEF","HEKTS","HKTM","HLGYO","HOROZ","HRKET","HTTBT","HUBVC",
    "HURGZ","HZNDR",
    "ICBCT","IDEAS","IDGYO","IEYHO","IHEVA","IHGZT","IHLAS","IHLGM","IHYAY","IMASM",
    "IMBAT","INDES","INFO","INGRM","INTEM","INVEO","IPEKE","ISATR","ISBIR","ISDMR",
    "ISFIN","ISGSY","ISGYO","ISCTR","ISMEN","ISYAT","ITTFK","IZFAS","IZMDC","IZTAR",
    "JANTS",
    "KAPLM","KAREL","KARSN","KARTN","KATMR","KAYSE","KBORU","KCAER","KENT","KERVN",
    "KERVT","KCHOL","KIMMR","KLKIM","KLMSN","KLNMA","KNFRT","KONKA","KONTR","KOPOL",
    "KONYA","KORDS","KOZAA","KOZAL","KRDMA","KRDMB","KRDMD","KRONT","KRPLS","KRSAN",
    "KSTUR","KTLEV","KUYAS","KZBGY",
    "LIDER","LIDFA","LKMNH","LMKDC","LOGO","LUKSK",
    "MACKO","MAGEN","MAKIM","MANAS","MARTI","MAVI","MEDTR","MEPET","MERCN","MERKO",
    "METRO","METUR","MGROS","MIATK","MIPAZ","MNDRS","MNVRL","MOBTL","MOGAN","MPARK",
    "MRGYO","MRDIN","MRSHL","MSGYO","MTRKS","MZHLD",
    "NATEN","NBORU","NETAS","NTGAZ","NTHOL","NTTUR","NUGYO","NUHCM",
    "OBAMS","OBASE","ODAS","ODEAB","OFSYM","ONCSM","ORCAY","ORGE","ORMA","OSMEN","OSTIM",
    "OTKAR","OTTO","OYAKC","OYAYO","OZKGY",
    "PAMEL","PAPIL","PARSN","PASEU","PCILT","PEKGY","PENGD","PENTA","PETKM","PETUN",
    "PGSUS","PINSU","PKART","PLTUR","PNLSN","POLHO","POLTK","PRZMA","PSDTC",
    "QNBFB","QNBFL",
    "RALYH","RAYSG","RCAST","REEDR","RHEAG","RNPOL","RODRG","ROYAL","RUBNS","RYGYO",
    "RYSAS",
    "SAFKR","SAHOL","SAMAT","SANEL","SANFM","SANKO","SARKY","SASA","SAYAS","SDTTR",
    "SEGYO","SEKFK","SEKUR","SELEC","SELGD","SELVA","SEYKM","SILVR","SISE","SKBNK",
    "SMART","SMRTG","SMILE","SNPAM","SODA","SOKM","SONME","SUWEN",
    "TABGD","TARKM","TATEN","TATGD","TAVHL","TBORG","TCELL","TDGYO","TEDU","TEKTU",
    "TEZOL","TFAC","TGSAS","THYAO","TIRE","TKFEN","TMSN","TOASO","TPVST","TRALT",
    "TRCAS","TRENJ","TRMET","TRILC","TSGYO","TSKB","TSPOR","TTKOM","TTRAK","TUCLK",
    "TUPRS","TUREX","TURGG","TURSG","TUYAP",
    "UCAK","ULKER","ULAS","ULUSE","ULUFA","ULUUN","UNLU","USAK","USDMR","UZERB",
    "VAKBN","VAKFN","VAKKO","VANGD","VBTYZ","VESTL","VERUS","VERTU","VKFYO","VKGYO",
    "WISD","WNDYR",
    "YAPRK","YKBNK","YATAS","YBTAS","YESIL","YEOTK","YGGYO","YGYO","YKSLN","YUNSA",
    "YYAPI",
    "ZEDUR","ZOREN","ZRGYO",
]
BIST_FALLBACK = sorted(list(set(_BIST_RAW)))

def find_peaks(arr, distance=1):
    arr = np.asarray(arr)
    peaks = []
    n = len(arr)
    i = 1
    while i < n - 1:
        if arr[i] > arr[i-1] and arr[i] > arr[i+1]:
            if not peaks or (i - peaks[-1]) >= distance:
                peaks.append(i)
        i += 1
    return np.array(peaks), {}

# ═══════════════════════════════════════════════
# VERİTABANI — Connection Pooling
# ═══════════════════════════════════════════════
_db_pool = None

def db_pool_init():
    global _db_pool
    if not DATABASE_URL:
        return
    try:
        _db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2, maxconn=8,
            dsn=DATABASE_URL, sslmode='require'
        )
        print("DB pool hazır (2-8).")
    except Exception as e:
        print(f"DB pool hata: {e}")
        _db_pool = None

def db_connect():
    if _db_pool:
        try:
            return _db_pool.getconn()
        except Exception:
            pass
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL, sslmode='require')
    except Exception as e:
        print(f"DB hata: {e}")
        return None

def db_release(conn):
    if _db_pool and conn:
        try:
            _db_pool.putconn(conn)
        except Exception:
            try: conn.close()
            except: pass
    elif conn:
        try: conn.close()
        except: pass

def db_init():
    conn = db_connect()
    if not conn:
        print("DB yok – in-memory mod.")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS store (
                key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS price_cache (
                ticker TEXT PRIMARY KEY, fetched_at DATE NOT NULL, data TEXT NOT NULL)""")
        conn.commit()
        print("DB hazır.")
    except Exception as e:
        print(f"DB init hata: {e}")
    finally:
        db_release(conn)

def db_get(key, default=None):
    conn = db_connect()
    if not conn: return default
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM store WHERE key=%s", (key,))
            row = cur.fetchone()
            return json.loads(row[0]) if row else default
    except Exception: return default
    finally: db_release(conn)

def db_set(key, value):
    conn = db_connect()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO store(key,value) VALUES(%s,%s)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value""",
                (key, json.dumps(value)))
        conn.commit()
    except Exception as e: print(f"DB set hata: {e}")
    finally: db_release(conn)

def db_del(key):
    conn = db_connect()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM store WHERE key=%s", (key,))
        conn.commit()
    except Exception as e: print(f"DB del hata: {e}")
    finally: db_release(conn)

def pc_save(ticker, df):
    conn = db_connect()
    if not conn: return
    try:
        data = df.reset_index()
        if 'datetime' not in data.columns:
            data = data.rename(columns={data.columns[0]: 'datetime'})
        data['datetime'] = data['datetime'].astype(str)
        payload = data.to_json(orient='records')
        today_tr = datetime.now(pytz.timezone('Europe/Istanbul')).date()
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO price_cache(ticker, fetched_at, data) VALUES(%s,%s,%s)
                ON CONFLICT(ticker) DO UPDATE SET fetched_at=EXCLUDED.fetched_at, data=EXCLUDED.data""",
                (ticker, today_tr, payload))
        conn.commit()
    except Exception as e: print(f"pc_save hata {ticker}: {e}")
    finally: db_release(conn)

def pc_load(ticker):
    conn = db_connect()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data, fetched_at FROM price_cache WHERE ticker=%s", (ticker,))
            row = cur.fetchone()
            if not row: return None
            fetched_at = row[1]
            today = datetime.now(pytz.timezone('Europe/Istanbul')).date()
            if fetched_at < today: return None
            records = json.loads(row[0])
            df = pd.DataFrame(records)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime').sort_index()
            for col in ['Open','High','Low','Close','Volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
    except Exception as e:
        print(f"pc_load hata {ticker}: {e}")
        return None
    finally: db_release(conn)

def pc_count_today():
    conn = db_connect()
    if not conn: return 0
    try:
        today = datetime.now(pytz.timezone('Europe/Istanbul')).date()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM price_cache WHERE fetched_at=%s", (today,))
            return cur.fetchone()[0]
    except Exception: return 0
    finally: db_release(conn)

# ─── Watchlist ───
_mem_watchlist = {}
_mem_emas      = {}

def wl_get(chat_id):
    return db_get(f"wl:{chat_id}", []) if DATABASE_URL else _mem_watchlist.get(str(chat_id), [])

def wl_set(chat_id, tickers):
    if DATABASE_URL: db_set(f"wl:{chat_id}", tickers)
    else: _mem_watchlist[str(chat_id)] = tickers

def wl_all_ids():
    if not DATABASE_URL: return list(_mem_watchlist.keys())
    conn = db_connect()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT key FROM store WHERE key LIKE 'wl:%'")
            return [r[0].replace('wl:','') for r in cur.fetchall()]
    except Exception: return []
    finally: db_release(conn)

def ema_get(ticker):
    val = db_get(f"ema:{ticker}") if DATABASE_URL else _mem_emas.get(ticker)
    if isinstance(val, dict):
        return {"daily": tuple(val.get("daily",[9,21])), "weekly": tuple(val.get("weekly",[9,21]))}
    if isinstance(val, (list, tuple)) and len(val) == 2:
        return {"daily": tuple(val), "weekly": tuple(val)}
    return {"daily": (9, 21), "weekly": (9, 21)}

def ema_set(ticker, pairs_dict):
    payload = {"daily": list(pairs_dict.get("daily",(9,21))), "weekly": list(pairs_dict.get("weekly",(9,21)))}
    if DATABASE_URL: db_set(f"ema:{ticker}", payload)
    else: _mem_emas[ticker] = payload

# ═══════════════════════════════════════════════
# Flask – Webhook
# ═══════════════════════════════════════════════
@app.route('/')
def home():
    return "BIST Bot calisiyor.", 200

@app.route('/health')
def health():
    return "OK", 200

@app.route(f'/webhook/{TOKEN}', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        update = telebot.types.Update.de_json(request.get_data(as_text=True))
        threading.Thread(target=bot.process_new_updates, args=([update],), daemon=True).start()
        return 'OK', 200
    abort(403)

def set_webhook():
    if not RENDER_URL:
        print("HATA: RENDER_URL eksik!")
        return
    bot.remove_webhook()
    time.sleep(1)
    print(f"Webhook: {bot.set_webhook(url=f'{RENDER_URL}/webhook/{TOKEN}')}")
    register_sub_webhooks(app, RENDER_URL)
    set_all_webhooks(RENDER_URL)
    _set_bot_commands()

def _set_bot_commands():
    commands = [
        telebot.types.BotCommand("tara","16 strateji: /tara 1..14 | /tara A/B/C | /tara all"),
        telebot.types.BotCommand("al","Trade kitabı — alış: /al THYAO 145.50 100 sebep"),
        telebot.types.BotCommand("sat","Trade kitabı — satış + AI yorum: /sat THYAO 162.00 100 sebep"),
        telebot.types.BotCommand("kitap","Trade geçmişi: /kitap | /kitap ozet | /kitap acik"),
        telebot.types.BotCommand("karzarar","Kar/Zarar hesapla: /karzarar THYAO 145 162 100"),
        telebot.types.BotCommand("check","Tara: /check all | /check 50 | /check THYAO"),
        telebot.types.BotCommand("backtest","Strateji backtest: /backtest 1 THYAO"),
        telebot.types.BotCommand("addall","BIST hisselerini ekle"),
        telebot.types.BotCommand("refreshlist","Yahoo'dan güncel listeyi çek"),
        telebot.types.BotCommand("add","Tek hisse ekle: /add THYAO"),
        telebot.types.BotCommand("remove","Tek hisse çıkar: /remove THYAO"),
        telebot.types.BotCommand("watchlist","İzleme listesini gör"),
        telebot.types.BotCommand("sinyal","Bugünkü sinyaller: /sinyal al | /sinyal sat"),
        telebot.types.BotCommand("analiz","Gemini AI analizi: /analiz THYAO"),
        telebot.types.BotCommand("kredi","AI kullanım ve kredi durumu"),
        telebot.types.BotCommand("haber","Haberler: /haber | /haber THYAO"),
        telebot.types.BotCommand("bulten","Bülten: /bulten sabah | /bulten aksam"),
        telebot.types.BotCommand("optimize","Tek hisse EMA optimize"),
        telebot.types.BotCommand("optimizeall","Tüm listeyi optimize et"),
        telebot.types.BotCommand("backup","Veriyi JSON olarak yedekle"),
        telebot.types.BotCommand("loadbackup","JSON yedekten geri yükle"),
        telebot.types.BotCommand("kontrolbot","Tüm sistemleri test et"),
        telebot.types.BotCommand("status","Bot sağlık durumu"),
        telebot.types.BotCommand("iptal","İşlemi durdur: /iptal tara | /iptal check"),
        telebot.types.BotCommand("help","Tüm komutları göster"),
    ]
    try:
        bot.set_my_commands(commands)
    except Exception as e:
        print(f"Komut listesi güncellenemedi: {e}")

def keep_alive():
    if not RENDER_URL: return
    while True:
        try: requests.get(f"{RENDER_URL}/health", timeout=10)
        except Exception: pass
        time.sleep(14 * 60)# ═══════════════════════════════════════════════
# HESAPLAMALAR
# ═══════════════════════════════════════════════
def calc_rsi(series, length=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=length-1, min_periods=length).mean()
    avg_loss = loss.ewm(com=length-1, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def calc_sma(series, length):
    return series.rolling(window=length).mean()

def calc_macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line

def calc_adx(df, period=14):
    high = df["High"]; low = df["Low"]; close = df["Close"]
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    up = high - high.shift()
    down = low.shift() - low
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    plus_di = 100 * plus_dm.rolling(period).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.rolling(period).mean() / atr.replace(0, np.nan)
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    adx = dx.rolling(period).mean()
    return adx

def calc_atr(df, period=14):
    high = df["High"]; low = df["Low"]; close = df["Close"]
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calc_bollinger_width(series, period=20, std_mult=2):
    sma = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = sma + std_mult * std
    lower = sma - std_mult * std
    width = (upper - lower) / sma.replace(0, np.nan) * 100
    return width

def perf_pct(series, days):
    if len(series) < days + 1:
        return None
    base = series.iloc[-(days+1)]
    if base == 0 or pd.isna(base):
        return None
    result = (series.iloc[-1] - base) / base * 100
    return None if pd.isna(result) else float(result)

# ═══════════════════════════════════════════════
# MARKET REJİM TESPİTİ
# ═══════════════════════════════════════════════
def detect_market_regime(df, period=50):
    """
    Piyasa rejimini tespit et.
    TREND_UP / TREND_DOWN / RANGE / HIGH_VOL
    """
    try:
        if df is None or df.empty or len(df) < period + 10:
            return "UNKNOWN", {}

        close = df["Close"]
        # SMA eğimi
        sma50 = calc_sma(close, period)
        if sma50.isna().iloc[-1]:
            return "UNKNOWN", {}

        sma_slope = (float(sma50.iloc[-1]) - float(sma50.iloc[-10])) / float(sma50.iloc[-10]) * 100
        # ADX
        adx_s = calc_adx(df)
        adx_val = float(adx_s.iloc[-1]) if not adx_s.isna().iloc[-1] else 15
        # Volatilite
        returns = close.pct_change().dropna()
        vol_20 = float(returns.iloc[-20:].std() * 100) if len(returns) >= 20 else 1.0
        vol_60 = float(returns.iloc[-60:].std() * 100) if len(returns) >= 60 else vol_20
        vol_ratio = vol_20 / vol_60 if vol_60 > 0 else 1.0
        # Fiyat SMA üstünde/altında
        price = float(close.iloc[-1])
        sma_val = float(sma50.iloc[-1])
        above_sma = price > sma_val

        details = {
            "sma_slope": round(sma_slope, 2),
            "adx": round(adx_val, 1),
            "vol_20": round(vol_20, 2),
            "vol_ratio": round(vol_ratio, 2),
            "above_sma": above_sma,
        }

        # Karar
        if vol_ratio > 1.5 and vol_20 > 2.5:
            regime = "HIGH_VOL"
        elif adx_val > 25 and sma_slope > 1.0 and above_sma:
            regime = "TREND_UP"
        elif adx_val > 25 and sma_slope < -1.0 and not above_sma:
            regime = "TREND_DOWN"
        else:
            regime = "RANGE"

        return regime, details
    except Exception:
        return "UNKNOWN", {}

def regime_strategy_weight(regime):
    """Rejime göre strateji ağırlıkları."""
    weights = {
        "TREND_UP":  {"momentum": 1.3, "breakout": 1.2, "mean_rev": 0.6, "squeeze": 0.8},
        "TREND_DOWN":{"momentum": 0.5, "breakout": 0.7, "mean_rev": 1.2, "squeeze": 0.9},
        "RANGE":     {"momentum": 0.7, "breakout": 0.8, "mean_rev": 1.3, "squeeze": 1.4},
        "HIGH_VOL":  {"momentum": 1.1, "breakout": 1.0, "mean_rev": 0.5, "squeeze": 0.6},
        "UNKNOWN":   {"momentum": 1.0, "breakout": 1.0, "mean_rev": 1.0, "squeeze": 1.0},
    }
    return weights.get(regime, weights["UNKNOWN"])

# ═══════════════════════════════════════════════
# RİSK YÖNETİMİ — Stop Loss, Hedef Fiyat, R/R
# ═══════════════════════════════════════════════
def calc_risk_management(df, signal_type="AL"):
    """
    ATR bazlı stop-loss, hedef fiyat ve risk/reward hesapla.
    signal_type: "AL" veya "SAT"
    """
    try:
        if df is None or df.empty or len(df) < 20:
            return None

        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        price = float(close.iloc[-1])

        atr_s = calc_atr(df, 14)
        atr = float(atr_s.iloc[-1]) if not atr_s.isna().iloc[-1] else price * 0.02

        # Son 20 günün destek/direnç seviyeleri
        recent_high = float(high.iloc[-20:].max())
        recent_low = float(low.iloc[-20:].min())
        # Son 5 günün en düşük/yüksek noktası (yakın destek/direnç)
        near_high = float(high.iloc[-5:].max())
        near_low = float(low.iloc[-5:].min())

        if signal_type == "AL":
            # Stop: ATR x 1.5 altında veya yakın destek altında (hangisi yakınsa)
            atr_stop = price - (atr * 1.5)
            support_stop = near_low - (atr * 0.3)
            stop_loss = max(atr_stop, support_stop)  # Daha yakın olanı seç
            stop_loss = round(stop_loss, 2)

            # Hedefler: ATR bazlı
            hedef_1 = round(price + (atr * 2.0), 2)
            hedef_2 = round(price + (atr * 3.5), 2)
            hedef_3 = round(price + (atr * 5.0), 2)

            # Direnç seviyesi yakınsa hedefi ayarla
            if recent_high > price and recent_high < hedef_1:
                hedef_1 = round(recent_high, 2)

            risk = price - stop_loss
            reward_1 = hedef_1 - price
            rr_1 = round(reward_1 / risk, 1) if risk > 0 else 0
            reward_2 = hedef_2 - price
            rr_2 = round(reward_2 / risk, 1) if risk > 0 else 0

        else:  # SAT
            atr_stop = price + (atr * 1.5)
            resist_stop = near_high + (atr * 0.3)
            stop_loss = min(atr_stop, resist_stop)
            stop_loss = round(stop_loss, 2)

            hedef_1 = round(price - (atr * 2.0), 2)
            hedef_2 = round(price - (atr * 3.5), 2)
            hedef_3 = round(price - (atr * 5.0), 2)

            if recent_low < price and recent_low > hedef_1:
                hedef_1 = round(recent_low, 2)

            risk = stop_loss - price
            reward_1 = price - hedef_1
            rr_1 = round(reward_1 / risk, 1) if risk > 0 else 0
            reward_2 = price - hedef_2
            rr_2 = round(reward_2 / risk, 1) if risk > 0 else 0

        stop_pct = round(abs(price - stop_loss) / price * 100, 1)

        return {
            "price": price,
            "atr": round(atr, 2),
            "stop_loss": stop_loss,
            "stop_pct": stop_pct,
            "hedef_1": hedef_1,
            "hedef_2": hedef_2,
            "hedef_3": hedef_3,
            "rr_1": rr_1,
            "rr_2": rr_2,
            "destek": round(recent_low, 2),
            "direnc": round(recent_high, 2),
        }
    except Exception:
        return None

def format_risk_text(rm, signal_type="AL"):
    """Risk yönetimi sonucunu formatlı metin olarak döndür."""
    if not rm:
        return ""
    emoji = "🟢" if signal_type == "AL" else "🔴"
    lines = [
        f"🎯 *TRADE PLANI*",
        f"├ {emoji} Giriş: {rm['price']:.2f}₺",
        f"├ 🛑 Stop: {rm['stop_loss']:.2f}₺ (-%{rm['stop_pct']})",
        f"├ 🎯 Hedef 1: {rm['hedef_1']:.2f}₺ (R/R: {rm['rr_1']})",
        f"├ 🎯 Hedef 2: {rm['hedef_2']:.2f}₺ (R/R: {rm['rr_2']})",
        f"├ 📏 ATR(14): {rm['atr']:.2f}₺",
        f"└ 📐 Destek: {rm['destek']:.2f}₺ | Direnç: {rm['direnc']:.2f}₺",
    ]
    return "\n".join(lines)

# ═══════════════════════════════════════════════
# GAP ANALİZİ
# ═══════════════════════════════════════════════
def calc_gap_analysis(df):
    """Açılış vs önceki kapanış gap analizi."""
    try:
        if df is None or df.empty or len(df) < 2:
            return None
        today_open = float(df["Open"].iloc[-1])
        prev_close = float(df["Close"].iloc[-2])
        today_close = float(df["Close"].iloc[-1])
        today_high = float(df["High"].iloc[-1])
        today_low = float(df["Low"].iloc[-1])

        if prev_close == 0 or pd.isna(prev_close):
            return None

        gap_pct = (today_open - prev_close) / prev_close * 100
        day_change_pct = (today_close - prev_close) / prev_close * 100

        # Gap tipi
        if gap_pct > 1.5:
            gap_type = "GAP UP 🟢⬆️"
        elif gap_pct > 0.5:
            gap_type = "GAP UP 🟢"
        elif gap_pct < -1.5:
            gap_type = "GAP DOWN 🔴⬇️"
        elif gap_pct < -0.5:
            gap_type = "GAP DOWN 🔴"
        else:
            gap_type = "FLAT ➡️"

        # Gap kapandı mı? (Gap up ise gün içinde açılış altına indi mi)
        gap_filled = False
        if gap_pct > 0.5 and today_low <= prev_close:
            gap_filled = True
        elif gap_pct < -0.5 and today_high >= prev_close:
            gap_filled = True

        # Açılış sonrası trend
        if today_close > today_open:
            open_trend = "📈 Açılıştan YUKARI"
        elif today_close < today_open:
            open_trend = "📉 Açılıştan AŞAĞI"
        else:
            open_trend = "➡️ Değişimsiz"

        return {
            "prev_close": round(prev_close, 2),
            "today_open": round(today_open, 2),
            "today_close": round(today_close, 2),
            "gap_pct": round(gap_pct, 2),
            "gap_type": gap_type,
            "gap_filled": gap_filled,
            "day_change_pct": round(day_change_pct, 2),
            "open_trend": open_trend,
        }
    except Exception:
        return None

def format_gap_text(gap):
    """Gap analizi formatlı metin."""
    if not gap:
        return ""
    filled_str = "✅ Gap kapandı" if gap["gap_filled"] else "⏳ Gap açık"
    lines = [
        f"📊 *GAP ANALİZİ*",
        f"├ Önceki Kapanış: {gap['prev_close']:.2f}₺",
        f"├ Bugün Açılış: {gap['today_open']:.2f}₺ ({gap['gap_type']}  {gap['gap_pct']:+.1f}%)",
        f"├ Bugün Kapanış: {gap['today_close']:.2f}₺ ({gap['day_change_pct']:+.1f}%)",
        f"├ {gap['open_trend']}",
        f"└ {filled_str}",
    ]
    return "\n".join(lines)

# ═══════════════════════════════════════════════
# MULTI-TIMEFRAME SKOR ENGINE
# ═══════════════════════════════════════════════
def calc_mtf_score(df_d, df_w, ticker):
    """
    Günlük + haftalık + hacim + momentum birleşik skor.
    0-100 arası toplam puan döner.
    """
    try:
        scores = {}
        total = 0
        max_total = 0

        # ── GÜNLÜK TREND SKORU (max 25) ──
        if df_d is not None and not df_d.empty and len(df_d) >= 50:
            d_close = df_d["Close"]
            d_sma20 = calc_sma(d_close, 20)
            d_sma50 = calc_sma(d_close, 50)
            d_rsi = calc_rsi(d_close, 14)
            price = float(d_close.iloc[-1])

            d_score = 0
            # Fiyat SMA20 üstünde (+5)
            if not d_sma20.isna().iloc[-1] and price > float(d_sma20.iloc[-1]):
                d_score += 5
            # Fiyat SMA50 üstünde (+5)
            if not d_sma50.isna().iloc[-1] and price > float(d_sma50.iloc[-1]):
                d_score += 5
            # SMA20 > SMA50 (golden cross durumu) (+5)
            if (not d_sma20.isna().iloc[-1] and not d_sma50.isna().iloc[-1] and
                float(d_sma20.iloc[-1]) > float(d_sma50.iloc[-1])):
                d_score += 5
            # RSI 40-70 bandında (+5)
            rsi_val = float(d_rsi.iloc[-1]) if not d_rsi.isna().iloc[-1] else 50
            if 40 <= rsi_val <= 70:
                d_score += 5
            elif 30 <= rsi_val < 40:
                d_score += 3
            # MACD pozitif (+5)
            macd_l, macd_s = calc_macd(d_close)
            if not macd_l.isna().iloc[-1] and float(macd_l.iloc[-1]) > float(macd_s.iloc[-1]):
                d_score += 5

            scores["daily_trend"] = d_score
            total += d_score
            max_total += 25

        # ── HAFTALIK TREND SKORU (max 25) ──
        if df_w is not None and not df_w.empty and len(df_w) >= 20:
            w_close = df_w["Close"]
            w_sma20 = calc_sma(w_close, 20)
            w_rsi = calc_rsi(w_close, 14)
            w_price = float(w_close.iloc[-1])

            w_score = 0
            if not w_sma20.isna().iloc[-1] and w_price > float(w_sma20.iloc[-1]):
                w_score += 8
            w_rsi_val = float(w_rsi.iloc[-1]) if not w_rsi.isna().iloc[-1] else 50
            if 40 <= w_rsi_val <= 70:
                w_score += 7
            elif 30 <= w_rsi_val < 40:
                w_score += 4
            w_macd_l, w_macd_s = calc_macd(w_close)
            if not w_macd_l.isna().iloc[-1] and float(w_macd_l.iloc[-1]) > float(w_macd_s.iloc[-1]):
                w_score += 10

            scores["weekly_trend"] = w_score
            total += w_score
            max_total += 25

        # ── HACİM SKORU (max 25) ──
        if df_d is not None and not df_d.empty and len(df_d) >= 25:
            volume = df_d["Volume"]
            d_close = df_d["Close"]
            vol_avg = volume.rolling(20).mean()

            v_score = 0
            last_vol = float(volume.iloc[-1]) if not pd.isna(volume.iloc[-1]) else 0
            avg_vol = float(vol_avg.iloc[-1]) if not vol_avg.isna().iloc[-1] else 1
            rel_vol = last_vol / avg_vol if avg_vol > 0 else 0

            # Hacim ortalamanın üstünde
            if rel_vol >= 2.0:
                v_score += 10
            elif rel_vol >= 1.5:
                v_score += 7
            elif rel_vol >= 1.0:
                v_score += 4

            # OBV trendi
            obv = (np.sign(d_close.diff()) * volume).fillna(0).cumsum()
            obv_recent = obv.iloc[-10:]
            if len(obv_recent) >= 5:
                x = np.arange(len(obv_recent))
                obv_slope = np.polyfit(x, obv_recent.values, 1)[0]
                obv_mean = abs(obv_recent.mean())
                if obv_mean > 0:
                    obv_pct = obv_slope / obv_mean * 100
                    if obv_pct > 3:
                        v_score += 8
                    elif obv_pct > 0:
                        v_score += 4

            # Alım baskısı
            last10 = df_d.iloc[-10:].copy()
            last10["up"] = last10["Close"] > last10["Close"].shift(1)
            up_vol = last10.loc[last10["up"], "Volume"].sum()
            total_vol = last10["Volume"].sum()
            if total_vol > 0:
                bp = up_vol / total_vol
                if bp >= 0.60:
                    v_score += 7
                elif bp >= 0.50:
                    v_score += 4

            scores["volume"] = v_score
            total += v_score
            max_total += 25

        # ── MOMENTUM SKORU (max 25) ──
        if df_d is not None and not df_d.empty and len(df_d) >= 25:
            d_close = df_d["Close"]
            m_score = 0

            perf_5 = perf_pct(d_close, 5)
            perf_21 = perf_pct(d_close, 21)
            adx_s = calc_adx(df_d)
            adx_val = float(adx_s.iloc[-1]) if not adx_s.isna().iloc[-1] else 15

            if perf_5 is not None and perf_5 > 3:
                m_score += 6
            elif perf_5 is not None and perf_5 > 0:
                m_score += 3

            if perf_21 is not None and perf_21 > 8:
                m_score += 6
            elif perf_21 is not None and perf_21 > 0:
                m_score += 3

            if adx_val > 30:
                m_score += 7
            elif adx_val > 20:
                m_score += 4

            # ROC momentum
            if len(d_close) > 10:
                roc = (float(d_close.iloc[-1]) / float(d_close.iloc[-10]) - 1) * 100
                if roc > 5:
                    m_score += 6
                elif roc > 0:
                    m_score += 3

            scores["momentum"] = m_score
            total += m_score
            max_total += 25

        # Normalize 0-100
        final_score = int(total / max_total * 100) if max_total > 0 else 0
        scores["total"] = final_score

        return final_score, scores
    except Exception:
        return 0, {}

def score_to_label(score):
    """Skoru etiketle."""
    if score >= 80:
        return "GÜÇLÜ AL++ 🟢🟢"
    elif score >= 65:
        return "AL+ 🟢"
    elif score >= 50:
        return "ORTA AL 🟡"
    elif score >= 35:
        return "ZAYIF / BEKLE ⚪"
    elif score >= 20:
        return "ZAYIF SAT 🟠"
    else:
        return "GÜÇLÜ SAT 🔴"

# ═══════════════════════════════════════════════
# SİNYAL GÜVEN SEVİYESİ (Confidence)
# ═══════════════════════════════════════════════
def calc_signal_confidence(ind, kod):
    """
    Strateji filtrelerindeki her koşulun yüzde kaçının sağlandığını hesapla.
    0-100 arası confidence skoru döner.
    """
    p = ind
    price = p["price"]

    def gt(val, thr): return val is not None and val > thr
    def lt(val, thr): return val is not None and val < thr
    def between(val, lo, hi): return val is not None and lo <= val <= hi
    def near(val, ref, pct): return val is not None and ref is not None and ref > 0 and abs(val - ref) / ref * 100 <= pct

    checks = []  # (True/False, ağırlık)

    if kod == "1":
        checks = [
            (gt(price, p["sma200"] or 0), 2),
            (near(price, p["sma50"], 5), 1),
            (between(p["rsi"], 40, 68), 1.5),
            (gt(p["rel_vol"], 1.8), 1.5),
            (p.get("bb_width_low60", False), 1),
            (between(p.get("perf_5d"), 0, 6), 1),
            (gt(p.get("perf_21d"), 11), 1),
            (gt(p.get("adx"), 25), 1.5),
            (p.get("price_above_vwap", False), 1),
            (p.get("kapanis_yukari", False), 0.5),
            (p.get("macd_hist_artiyor", False), 1),
        ]
    elif kod == "2":
        checks = [
            (gt(price, p["sma20"] or 0), 1.5),
            (gt(price, p["sma50"] or 0), 1.5),
            (between(p["rsi"], 50, 74), 1.5),
            (gt(p.get("macd"), p.get("macd_sig") if p.get("macd_sig") is not None else -999), 1.5),
            (p.get("macd_hist_pozitif", False), 1),
            (p.get("macd_hist_artiyor", False), 1),
            (gt(p["rel_vol"], 1.8), 1.5),
            (gt(p.get("perf_1d"), 2), 1),
            (gt(p.get("perf_5d"), 5), 1),
            (gt(p.get("adx"), 25), 1.5),
            (p.get("price_above_vwap", False), 1),
        ]
    elif kod == "4":
        checks = [
            (gt(price, p["sma50"] or 0), 1.5),
            (gt(price, p["sma200"] or 0), 2),
            (between(p["rsi"], 56, 74), 1.5),
            (gt(p.get("macd"), p.get("macd_sig") if p.get("macd_sig") is not None else -999), 1.5),
            (p.get("macd_hist_artiyor", False), 1),
            (gt(p["rel_vol"], 1.9), 1.5),
            (gt(p.get("perf_5d"), 8), 1),
            (gt(p.get("perf_21d"), 13), 1),
            (gt(p.get("adx"), 28), 1.5),
        ]
    else:
        # Genel confidence: temel göstergeler
        checks = [
            (gt(price, p.get("sma50") or 0), 2),
            (gt(price, p.get("sma200") or 0), 2),
            (between(p.get("rsi"), 40, 70), 1.5),
            (gt(p.get("rel_vol"), 1.5), 1.5),
            (p.get("macd_hist_pozitif", False), 1),
            (gt(p.get("adx"), 25), 1.5),
            (p.get("price_above_vwap", False), 1),
        ]

    if not checks:
        return 50

    total_weight = sum(w for _, w in checks)
    earned_weight = sum(w for passed, w in checks if passed)

    confidence = int(earned_weight / total_weight * 100) if total_weight > 0 else 50
    return confidence

def confidence_label(conf):
    if conf >= 80:
        return "GÜÇLÜ 🔥🔥"
    elif conf >= 65:
        return "İYİ 🔥"
    elif conf >= 50:
        return "ORTA 🟡"
    else:
        return "ZAYIF ⚪"

# ═══════════════════════════════════════════════
# HACİM & MOMENTUM ANALİZİ
# ═══════════════════════════════════════════════
def calc_volume_momentum(df, vol_period=20, mom_period_fast=5, mom_period_slow=10):
    result = {
        "vol_ratio": None, "vol_trend": "NOTR",
        "buy_pressure": None, "mom_fast": None, "mom_slow": None,
        "obv_trend": "NOTR", "confirm_buy": False, "confirm_sell": False,
        "summary": "❓ Hacim verisi yok"
    }
    try:
        if "Volume" not in df.columns or len(df) < vol_period + 2:
            return result
        close = df["Close"]
        volume = df["Volume"].replace(0, np.nan)

        avg_vol = volume.iloc[-(vol_period+1):-1].mean()
        last_vol = volume.iloc[-1]
        vol_ratio = last_vol / avg_vol if (avg_vol and avg_vol > 0) else None
        result["vol_ratio"] = vol_ratio

        recent_vol = volume.dropna().iloc[-5:]
        if len(recent_vol) >= 3:
            x = np.arange(len(recent_vol))
            slope = np.polyfit(x, recent_vol.values, 1)[0]
            norm = recent_vol.mean()
            if norm > 0:
                slope_pct = slope / norm * 100
                if slope_pct > 5: result["vol_trend"] = "YUKARI"
                elif slope_pct < -5: result["vol_trend"] = "ASAGI"

        last10 = df.iloc[-10:].copy()
        last10["up"] = last10["Close"] > last10["Close"].shift(1)
        up_vol = last10.loc[last10["up"], "Volume"].sum()
        down_vol = last10.loc[~last10["up"], "Volume"].sum()
        total_vol = up_vol + down_vol
        if total_vol > 0:
            result["buy_pressure"] = up_vol / total_vol

        if len(close) > mom_period_slow + 1:
            result["mom_fast"] = round((close.iloc[-1] / close.iloc[-mom_period_fast] - 1) * 100, 2)
            result["mom_slow"] = round((close.iloc[-1] / close.iloc[-mom_period_slow] - 1) * 100, 2)

        obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
        obv_recent = obv.iloc[-10:]
        if len(obv_recent) >= 5:
            x = np.arange(len(obv_recent))
            obv_slope = np.polyfit(x, obv_recent.values, 1)[0]
            obv_mean = abs(obv_recent.mean())
            if obv_mean > 0:
                obv_slope_pct = obv_slope / obv_mean * 100
                if obv_slope_pct > 2: result["obv_trend"] = "YUKARI"
                elif obv_slope_pct < -2: result["obv_trend"] = "ASAGI"

        vr = vol_ratio or 0
        bp = result["buy_pressure"] or 0.5
        mf = result["mom_fast"] or 0
        ms = result["mom_slow"] or 0
        obv_t = result["obv_trend"]

        buy_score = ((1 if vr >= 1.2 else 0) + (1 if bp >= 0.55 else 0) +
                     (1 if mf > 0 else 0) + (1 if ms > 0 else 0) +
                     (1 if obv_t == "YUKARI" else 0))
        sell_score = ((1 if vr >= 1.2 else 0) + (1 if bp <= 0.45 else 0) +
                      (1 if mf < 0 else 0) + (1 if ms < 0 else 0) +
                      (1 if obv_t == "ASAGI" else 0))
        result["confirm_buy"] = buy_score >= 3
        result["confirm_sell"] = sell_score >= 3

        vol_str = f"{vr:.1f}x ort" if vr else "?"
        vol_icon = "🔥" if vr and vr >= 1.5 else ("📊" if vr and vr >= 0.8 else "🔇")
        bp_str = f"%{bp*100:.0f} alım" if result["buy_pressure"] is not None else "?"
        mf_str = f"{mf:+.1f}%" if result["mom_fast"] is not None else "?"
        ms_str = f"{ms:+.1f}%" if result["mom_slow"] is not None else "?"
        obv_str = "📈OBV" if obv_t == "YUKARI" else ("📉OBV" if obv_t == "ASAGI" else "➡️OBV")
        result["summary"] = f"{vol_icon} Hacim:{vol_str} | {bp_str} | Mom:{mf_str}(5g)/{ms_str}(10g) | {obv_str}"
    except Exception as e:
        result["summary"] = f"❓ Hacim/momentum hata: {e}"
    return result

# ═══════════════════════════════════════════════
# DİVERJANS TESPİTİ
# ═══════════════════════════════════════════════
def detect_divergence(df, window=60, min_bars=5, max_bars=40):
    try:
        if "RSI" not in df.columns:
            df = df.copy()
            df["RSI"] = calc_rsi(df["Close"], 14)
        df = df.dropna(subset=["Close", "RSI"]).tail(window)
        if len(df) < min_bars + 5:
            return None, None
        closes = df["Close"].values
        rsis = df["RSI"].values
        last_idx = len(closes) - 1

        price_lows, _ = find_peaks(-closes, distance=min_bars)
        rsi_lows, _ = find_peaks(-rsis, distance=min_bars)
        price_highs, _ = find_peaks(closes, distance=min_bars)
        rsi_highs, _ = find_peaks(rsis, distance=min_bars)

        recent_price_lows = price_lows[price_lows >= last_idx - max_bars]
        recent_rsi_lows = rsi_lows[rsi_lows >= last_idx - max_bars]

        if len(recent_price_lows) >= 2 and len(recent_rsi_lows) >= 2:
            p1, p2 = recent_price_lows[-2], recent_price_lows[-1]
            r1, r2 = recent_rsi_lows[-2], recent_rsi_lows[-1]
            if closes[p2] < closes[p1] and rsis[r2] > rsis[r1]:
                strength = "GÜÇLÜ 🔥" if rsis[r2] < 40 else "ORTA"
                return ("Bullish Diverjans",
                    f"📈 POZİTİF UYUMSUZLUK [{strength}]\n"
                    f"   Fiyat dip: {closes[p1]:.2f} → {closes[p2]:.2f} ↘\n"
                    f"   RSI  dip: {rsis[r1]:.1f} → {rsis[r2]:.1f} ↗\n"
                    f"   ⚡ Yukarı dönüş sinyali!")
        elif len(recent_price_lows) >= 1 and len(recent_rsi_lows) >= 1:
            p2 = recent_price_lows[-1]
            r2 = recent_rsi_lows[-1]
            if p2 >= last_idx - 10 and rsis[r2] < 35:
                return ("Bullish Diverjans",
                    f"📈 POZİTİF UYUMSUZLUK [ZAYIF]\n"
                    f"   RSI dip: {rsis[r2]:.1f} — Aşırı Satım bölgesi\n"
                    f"   ⚡ Toparlanma ihtimali")

        recent_price_highs = price_highs[price_highs >= last_idx - max_bars]
        recent_rsi_highs = rsi_highs[rsi_highs >= last_idx - max_bars]

        if len(recent_price_highs) >= 2 and len(recent_rsi_highs) >= 2:
            p1, p2 = recent_price_highs[-2], recent_price_highs[-1]
            r1, r2 = recent_rsi_highs[-2], recent_rsi_highs[-1]
            if closes[p2] > closes[p1] and rsis[r2] < rsis[r1]:
                strength = "GÜÇLÜ 🔥" if rsis[r2] > 60 else "ORTA"
                return ("Bearish Diverjans",
                    f"📉 NEGATİF UYUMSUZLUK [{strength}]\n"
                    f"   Fiyat tepe: {closes[p1]:.2f} → {closes[p2]:.2f} ↗\n"
                    f"   RSI  tepe: {rsis[r1]:.1f} → {rsis[r2]:.1f} ↘\n"
                    f"   ⚡ Aşağı dönüş sinyali!")
        elif len(recent_price_highs) >= 1 and len(recent_rsi_highs) >= 1:
            p2 = recent_price_highs[-1]
            r2 = recent_rsi_highs[-1]
            if p2 >= last_idx - 10 and rsis[r2] > 65:
                return ("Bearish Diverjans",
                    f"📉 NEGATİF UYUMSUZLUK [ZAYIF]\n"
                    f"   RSI tepe: {rsis[r2]:.1f} — Aşırı Alım bölgesi\n"
                    f"   ⚡ Düzeltme ihtimali")
    except Exception as e:
        print(f"Diverjans hata: {e}")
    return None, None

# ═══════════════════════════════════════════════
# VERİ ÇEKME — Yahoo Finance (Global Lock)
# ═══════════════════════════════════════════════
def fetch_bist_tickers_yahoo():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8",
        "Referer": "https://finance.yahoo.com/",
    }
    results = set()
    letters = "ABCDEFGHIJKLMNOPRSTUVYZ"
    for letter in letters:
        try:
            url = (f"https://query1.finance.yahoo.com/v1/finance/search"
                   f"?q={letter}.IS&lang=en-US&region=TR"
                   f"&quotesCount=20&newsCount=0&enableFuzzyQuery=false")
            with _yahoo_lock:
                resp = requests.get(url, headers=headers, timeout=8)
            if resp.status_code == 200:
                quotes = resp.json().get("quotes", [])
                batch = [q["symbol"].replace(".IS","") for q in quotes
                         if q.get("symbol","").endswith(".IS") and q.get("quoteType")=="EQUITY"]
                results.update(batch)
        except Exception: pass
        time.sleep(0.2)

    try:
        chunk_size = 50
        all_known = sorted(list(set(list(results) + BIST_FALLBACK)))
        for i in range(0, len(all_known), chunk_size):
            chunk = all_known[i:i+chunk_size]
            symbols = ",".join([f"{t}.IS" for t in chunk])
            url2 = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}&lang=en-US&region=TR"
            with _yahoo_lock:
                resp2 = requests.get(url2, headers=headers, timeout=12)
            if resp2.status_code == 200:
                qr = resp2.json().get("quoteResponse",{}).get("result",[])
                batch = [q["symbol"].replace(".IS","") for q in qr if q.get("symbol","").endswith(".IS")]
                results.update(batch)
            time.sleep(0.3)
    except Exception as e:
        print(f"Yahoo quote bulk hata: {e}")
    return sorted(results)

def get_all_bist_tickers():
    if DATABASE_URL:
        saved = db_get("master_ticker_list")
        if saved and isinstance(saved, list) and len(saved) > 100:
            return saved
    live = fetch_bist_tickers_yahoo()
    if len(live) > 100:
        if DATABASE_URL: db_set("master_ticker_list", live)
        return live
    return BIST_FALLBACK

def _run_refreshlist(chat_id):
    bot.send_message(chat_id,
        "🔄 *Liste Yenileniyor...*\n"
        "3 kaynak deneniyor:\n"
        "1️⃣ Yahoo Finance query1\n"
        "2️⃣ Yahoo Finance toplu quote\n"
        "⏱ ~30-45 saniye sürebilir...")
    try:
        live = fetch_bist_tickers_yahoo()
        merged_master = sorted(list(set(live) | set(BIST_FALLBACK)))
        if len(live) < 50:
            bot.send_message(chat_id,
                f"⚠️ Yahoo az sonuç ({len(live)}).\n"
                f"📋 Dahili liste ile birleştirildi: *{len(merged_master)} hisse*")
        else:
            if DATABASE_URL: db_set("master_ticker_list", merged_master)
            bot.send_message(chat_id,
                f"✅ Yahoo: *{len(live)} hisse*\n"
                f"📋 Birleştirildi: *{len(merged_master)} hisse*")

        current = wl_get(chat_id)
        current_set = set(current)
        new_ones = [t for t in merged_master if t not in current_set]
        if new_ones:
            final = sorted(list(current_set | set(merged_master)))
            wl_set(chat_id, final)
            bot.send_message(chat_id,
                f"📥 *{len(new_ones)} yeni hisse* eklendi!\n"
                f"📋 Toplam: *{len(final)} hisse*\n\n"
                f"Yeni (ilk 30):\n" + ", ".join(new_ones[:30]) +
                (f"\n...ve {len(new_ones)-30} daha" if len(new_ones)>30 else ""))
        else:
            bot.send_message(chat_id, f"✅ Güncel! {len(current)} hisse ekli.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Hata: {e}")

def fetch_yahoo_direct(ticker, interval="1d", range_="2y"):
    try:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}.IS?interval={interval}&range={range_}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "tr-TR,tr;q=0.9",
            "Referer": "https://finance.yahoo.com/",
        }
        with _yahoo_lock:
            resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            return pd.DataFrame()
        data = resp.json()
        result = data.get("chart",{}).get("result",[])
        if not result:
            return pd.DataFrame()
        r = result[0]
        timestamps = r.get("timestamp",[])
        ohlcv = r.get("indicators",{}).get("quote",[{}])[0]
        if not timestamps:
            return pd.DataFrame()
        df = pd.DataFrame({
            "datetime": pd.to_datetime(timestamps, unit="s"),
            "Open": ohlcv.get("open",[]),
            "High": ohlcv.get("high",[]),
            "Low": ohlcv.get("low",[]),
            "Close": ohlcv.get("close",[]),
            "Volume": ohlcv.get("volume",[]),
        })
        df = df.set_index("datetime").sort_index()
        df.index = df.index.tz_localize(None)
        for col in ["Open","High","Low","Close","Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna(subset=["Close"])
    except Exception as e:
        print(f"Yahoo direct hata {ticker}: {e}")
        return pd.DataFrame()

def resample_weekly(df_daily):
    if df_daily.empty:
        return pd.DataFrame()
    df = df_daily.resample("W").agg({
        "Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"
    }).dropna(subset=["Close"])
    df.index.name = "datetime"
    return df

def get_data(ticker):
    df_daily = pd.DataFrame()
    df_weekly = pd.DataFrame()
    if DATABASE_URL:
        c = pc_load(f"d:{ticker}")
        if c is not None and not c.empty:
            df_daily = c
        c = pc_load(f"w:{ticker}")
        if c is not None and not c.empty:
            df_weekly = c
    if df_daily.empty:
        r = fetch_yahoo_direct(ticker, interval="1d", range_="2y")
        if isinstance(r, pd.DataFrame) and not r.empty:
            df_daily = r
            if DATABASE_URL: pc_save(f"d:{ticker}", r)
    if df_weekly.empty:
        r = fetch_yahoo_direct(ticker, interval="1wk", range_="5y")
        if isinstance(r, pd.DataFrame) and not r.empty:
            df_weekly = r
            if DATABASE_URL: pc_save(f"w:{ticker}", r)
    if df_weekly.empty and not df_daily.empty:
        df_weekly = resample_weekly(df_daily)
    return df_daily, df_weekly

_cached_today_set = None
_cached_today_date = None

def _get_cached_tickers_today():
    global _cached_today_set, _cached_today_date
    today = datetime.now(pytz.timezone('Europe/Istanbul')).date()
    if _cached_today_set is not None and _cached_today_date == today:
        return _cached_today_set
    if not DATABASE_URL:
        return set()
    conn = db_connect()
    if not conn:
        return set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT REPLACE(ticker, 'd:', '') FROM price_cache WHERE fetched_at=%s AND ticker LIKE 'd:%'",
                (today,))
            rows = cur.fetchall()
        _cached_today_set = {r[0] for r in rows}
        _cached_today_date = today
        return _cached_today_set
    except Exception:
        return set()
    finally:
        db_release(conn)

def _invalidate_cache_set():
    global _cached_today_set
    _cached_today_set = None

# ═══════════════════════════════════════════════
# BACKTEST MODÜLÜ
# ═══════════════════════════════════════════════
def backtest_strategy(ticker, kod, lookback_days=252):
    """
    Strateji kodunu geçmiş veride test et.
    Son lookback_days gün üzerinden çalışır.
    """
    try:
        df_d, _ = get_data(ticker)
        if df_d is None or df_d.empty or len(df_d) < lookback_days:
            return None

        close = df_d["Close"]
        trades = []
        in_trade = False
        entry_price = 0
        entry_date = None

        # Her gün için indikatör hesapla ve sinyali kontrol et
        # Performans için: son lookback_days kadar pencere kaydır
        start_idx = max(60, len(df_d) - lookback_days)

        for i in range(start_idx, len(df_d) - 1):
            window = df_d.iloc[:i+1]
            if len(window) < 60:
                continue

            try:
                ind = _backtest_indicators(window)
                if ind is None:
                    continue
            except Exception:
                continue

            if not in_trade:
                # Giriş sinyali
                if strateji_filtre(ind, kod):
                    in_trade = True
                    entry_price = float(close.iloc[i+1])  # Ertesi gün açılış
                    entry_date = df_d.index[i+1]
            else:
                # Çıkış: RSI 70 üstü veya %10 zarar veya 20 gün sonra
                cur_price = float(close.iloc[i])
                days_in = (df_d.index[i] - entry_date).days if entry_date else 0
                rsi_val = float(calc_rsi(window["Close"], 14).iloc[-1]) if len(window) > 14 else 50

                exit_signal = False
                exit_reason = ""

                if rsi_val > 75:
                    exit_signal = True
                    exit_reason = "RSI>75"
                elif entry_price > 0 and (cur_price - entry_price) / entry_price < -0.10:
                    exit_signal = True
                    exit_reason = "Stop-%10"
                elif days_in > 20:
                    exit_signal = True
                    exit_reason = "20gün"

                if exit_signal:
                    pct = (cur_price - entry_price) / entry_price * 100
                    trades.append({
                        "entry": entry_price,
                        "exit": cur_price,
                        "pct": round(pct, 2),
                        "days": days_in,
                        "reason": exit_reason,
                    })
                    in_trade = False

        # Açık pozisyon varsa kapat
        if in_trade and entry_price > 0:
            cur_price = float(close.iloc[-1])
            pct = (cur_price - entry_price) / entry_price * 100
            days_in = (df_d.index[-1] - entry_date).days if entry_date else 0
            trades.append({
                "entry": entry_price, "exit": cur_price,
                "pct": round(pct, 2), "days": days_in, "reason": "Açık",
            })

        if not trades:
            return {"ticker": ticker, "kod": kod, "trades": 0, "msg": "Sinyal bulunamadı"}

        wins = [t for t in trades if t["pct"] > 0]
        losses = [t for t in trades if t["pct"] <= 0]
        total_pct = sum(t["pct"] for t in trades)
        avg_pct = total_pct / len(trades)
        max_win = max(t["pct"] for t in trades) if trades else 0
        max_loss = min(t["pct"] for t in trades) if trades else 0
        win_rate = len(wins) / len(trades) * 100 if trades else 0
        avg_days = sum(t["days"] for t in trades) / len(trades) if trades else 0

        # Max drawdown
        equity = [100]
        for t in trades:
            equity.append(equity[-1] * (1 + t["pct"]/100))
        peak = equity[0]
        max_dd = 0
        for e in equity:
            if e > peak: peak = e
            dd = (peak - e) / peak * 100
            if dd > max_dd: max_dd = dd

        # Profit factor
        gross_profit = sum(t["pct"] for t in wins) if wins else 0
        gross_loss = abs(sum(t["pct"] for t in losses)) if losses else 0.01
        profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 999

        return {
            "ticker": ticker,
            "kod": kod,
            "trades": len(trades),
            "win_rate": round(win_rate, 1),
            "avg_return": round(avg_pct, 2),
            "total_return": round(total_pct, 2),
            "max_win": round(max_win, 2),
            "max_loss": round(max_loss, 2),
            "max_drawdown": round(max_dd, 2),
            "profit_factor": profit_factor,
            "avg_days": round(avg_days, 1),
            "wins": len(wins),
            "losses": len(losses),
        }
    except Exception as e:
        return {"ticker": ticker, "kod": kod, "trades": 0, "msg": f"Hata: {str(e)[:80]}"}

def _backtest_indicators(df):
    """Backtest için hafif indikatör hesabı (get_data çağırmaz)."""
    if len(df) < 60:
        return None
    d = df.copy()
    close = d["Close"]; high = d["High"]; low = d["Low"]
    volume = d["Volume"]; open_ = d.get("Open", close)
    price = float(close.iloc[-1])

    _close_last = close.iloc[-1]
    _open_last = open_.iloc[-1]
    kapanis_yukari = (not pd.isna(_close_last) and not pd.isna(_open_last) and float(_close_last) > float(_open_last))

    sma20 = calc_sma(close, 20); sma50 = calc_sma(close, 50); sma200 = calc_sma(close, 200)
    s20 = float(sma20.iloc[-1]) if not sma20.isna().iloc[-1] else None
    s50 = float(sma50.iloc[-1]) if not sma50.isna().iloc[-1] else None
    s200 = float(sma200.iloc[-1]) if not sma200.isna().iloc[-1] else None

    rsi_s = calc_rsi(close, 14).dropna()
    rsi = float(rsi_s.iloc[-1]) if len(rsi_s) >= 1 else None
    rsi_prev = float(rsi_s.iloc[-2]) if len(rsi_s) >= 2 else None
    rsi_yukari_3g = (len(rsi_s) >= 3 and float(rsi_s.iloc[-1]) > float(rsi_s.iloc[-2]) > float(rsi_s.iloc[-3]))

    macd_line, macd_sig_line = calc_macd(close)
    macd_val = float(macd_line.iloc[-1]) if not macd_line.isna().iloc[-1] else None
    macd_sig_val = float(macd_sig_line.iloc[-1]) if not macd_sig_line.isna().iloc[-1] else None
    macd_prev = float(macd_line.iloc[-2]) if len(macd_line) >= 2 and not pd.isna(macd_line.iloc[-2]) else None
    macd_sig_prev = float(macd_sig_line.iloc[-2]) if len(macd_sig_line) >= 2 and not pd.isna(macd_sig_line.iloc[-2]) else None

    if macd_val is not None and macd_sig_val is not None:
        hist_cur = macd_val - macd_sig_val
        hist_prev = (macd_prev - macd_sig_prev) if (macd_prev is not None and macd_sig_prev is not None) else None
        macd_hist_pozitif = hist_cur > 0
        macd_hist_artiyor = hist_prev is not None and hist_cur > hist_prev
        macd_hist_pozitif_kesim = (hist_prev is not None and hist_prev <= 0 and hist_cur > 0)
    else:
        macd_hist_pozitif = False; macd_hist_artiyor = False; macd_hist_pozitif_kesim = False

    macd_fresh_cross = (macd_val is not None and macd_sig_val is not None and
                        macd_prev is not None and macd_sig_prev is not None and
                        macd_val > macd_sig_val and macd_prev <= macd_sig_prev)

    adx_s = calc_adx(d)
    adx = float(adx_s.iloc[-1]) if not adx_s.isna().iloc[-1] else None

    bb_width_s = calc_bollinger_width(close)
    bb_width = float(bb_width_s.iloc[-1]) if not bb_width_s.isna().iloc[-1] else None
    _bb_avg20_raw = bb_width_s.rolling(20).mean().iloc[-1]
    bb_avg20 = float(_bb_avg20_raw) if len(bb_width_s) > 20 and not pd.isna(_bb_avg20_raw) else None
    bb_width_low = (bb_width is not None and bb_avg20 is not None and bb_width < bb_avg20)
    bb_avg60_raw = bb_width_s.rolling(60).mean().iloc[-1]
    bb_avg60 = float(bb_avg60_raw) if (len(bb_width_s) > 60 and not pd.isna(bb_avg60_raw)) else bb_avg20
    bb_width_low60 = (bb_width is not None and bb_avg60 is not None and bb_avg60 > 0 and bb_width < bb_avg60 * 0.70)
    bb_width_low65 = (bb_width is not None and bb_avg60 is not None and bb_avg60 > 0 and bb_width < bb_avg60 * 0.65)

    typical_price = (high + low + close) / 3
    vwap_num = (typical_price * volume).rolling(20).sum()
    vwap_den = volume.rolling(20).sum()
    vwap_s = vwap_num / vwap_den.replace(0, float("nan"))
    vwap_raw = vwap_s.iloc[-1]
    vwap = float(vwap_raw) if (vwap_raw is not None and not pd.isna(vwap_raw)) else None
    price_above_vwap = (vwap is not None and price > vwap)

    vol_avg20_raw = volume.rolling(20).mean().iloc[-1]
    vol_avg20 = float(vol_avg20_raw) if not pd.isna(vol_avg20_raw) else 0.0
    vol_cur = float(volume.iloc[-1]) if not pd.isna(volume.iloc[-1]) else 0.0
    rel_vol = vol_cur / vol_avg20 if vol_avg20 > 0 else 0

    return {
        "ticker": "BT", "price": price, "kapanis_yukari": kapanis_yukari,
        "sma20": s20, "sma50": s50, "sma200": s200,
        "rsi": rsi, "rsi_prev": rsi_prev, "rsi_yukari_3g": rsi_yukari_3g,
        "macd": macd_val, "macd_sig": macd_sig_val,
        "macd_fresh_cross": macd_fresh_cross,
        "macd_hist_pozitif": macd_hist_pozitif,
        "macd_hist_artiyor": macd_hist_artiyor,
        "macd_hist_pozitif_kesim": macd_hist_pozitif_kesim,
        "adx": adx, "bb_width": bb_width, "bb_width_low": bb_width_low,
        "bb_width_low60": bb_width_low60, "bb_width_low65": bb_width_low65,
        "vwap": vwap, "price_above_vwap": price_above_vwap,
        "vol_avg20": vol_avg20, "vol_cur": vol_cur, "rel_vol": rel_vol,
        "perf_1d": perf_pct(close, 1), "perf_5d": perf_pct(close, 5),
        "perf_21d": perf_pct(close, 21), "perf_63d": perf_pct(close, 63),
        "perf_252d": perf_pct(close, 252),
    }
    # ═══════════════════════════════════════════════
# EMA OPTİMİZASYON
# ═══════════════════════════════════════════════
def find_best_ema_pair(ticker, chat_id=None):
    df_daily, df_weekly = get_data(ticker)
    pairs = [
        (3,8),(4,9),(5,10),(5,13),(6,13),
        (8,21),(9,21),(10,21),(10,26),(12,26),
        (13,34),(14,28),(10,30),(15,30),
        (20,50),(21,55),(20,60),(21,89),(50,200),
        (8,13),(13,21),
    ]
    result = {}
    for label, df in [("daily", df_daily), ("weekly", df_weekly)]:
        if chat_id and is_cancelled(chat_id, "optimize"):
            return None
        min_bars = 50 if label == "weekly" else 100
        if not isinstance(df, pd.DataFrame) or df.empty or len(df) < min_bars:
            result[label] = (9, 21)
            continue
        best_profit, best_pair = -999.0, (9, 21)
        for short, long_ in pairs:
            if chat_id and is_cancelled(chat_id, "optimize"):
                return None
            tmp = df.copy()
            tmp["es"] = calc_ema(tmp["Close"], short)
            tmp["el"] = calc_ema(tmp["Close"], long_)
            tmp = tmp.dropna(subset=["es","el"])
            profit = 0.0; in_pos = False; entry = 0.0
            for i in range(1, len(tmp)):
                ep = tmp["es"].iloc[i-1]; lp = tmp["el"].iloc[i-1]
                ec = tmp["es"].iloc[i]; lc = tmp["el"].iloc[i]
                if not in_pos and ec > lc and ep <= lp:
                    in_pos = True; entry = tmp["Close"].iloc[i]
                elif in_pos and ec < lc and ep >= lp:
                    profit += (tmp["Close"].iloc[i] - entry) / entry * 100
                    in_pos = False
            if in_pos:
                profit += (tmp["Close"].iloc[-1] - entry) / entry * 100
            if profit > best_profit:
                best_profit, best_pair = profit, (short, long_)
        result[label] = best_pair
    return result

# ═══════════════════════════════════════════════
# TARA İNDİKATÖRLER — df_d parametreli (cache dostu)
# ═══════════════════════════════════════════════
def tara_indicators(ticker, df_d=None):
    """
    Bir hisse için /tara stratejilerinde kullanılacak göstergeleri hesapla.
    df_d verilmişse get_data() çağırmaz (performans).
    """
    if df_d is None:
        df_d, _ = get_data(ticker)
    if not isinstance(df_d, pd.DataFrame) or df_d.empty or len(df_d) < 60:
        return None

    d = df_d.copy()
    close = d["Close"]; high = d["High"]; low = d["Low"]
    volume = d["Volume"]
    open_ = d["Open"] if "Open" in d.columns else close
    price = float(close.iloc[-1])

    _close_last = close.iloc[-1]; _open_last = open_.iloc[-1]
    kapanis_yukari = (not pd.isna(_close_last) and not pd.isna(_open_last) and float(_close_last) > float(_open_last))

    # ── SMA / EMA ──
    sma20 = calc_sma(close, 20); sma50 = calc_sma(close, 50); sma200 = calc_sma(close, 200)
    s20 = float(sma20.iloc[-1]) if not sma20.isna().iloc[-1] else None
    s50 = float(sma50.iloc[-1]) if not sma50.isna().iloc[-1] else None
    s200 = float(sma200.iloc[-1]) if not sma200.isna().iloc[-1] else None
    # EMA 9 ve 21 — strateji 16 icin
    ema9_s  = calc_ema(close, 9)
    ema21_s = calc_ema(close, 21)
    e9  = float(ema9_s.iloc[-1])  if not ema9_s.isna().iloc[-1]  else None
    e21 = float(ema21_s.iloc[-1]) if not ema21_s.isna().iloc[-1] else None
    ema_dizilimi = (e9 is not None and e21 is not None and s50 is not None and e9 > e21 > s50)

    # ── RSI ──
    rsi_s = calc_rsi(close, 14).dropna()
    rsi = float(rsi_s.iloc[-1]) if len(rsi_s) >= 1 else None
    rsi_prev = float(rsi_s.iloc[-2]) if len(rsi_s) >= 2 else None
    rsi_prev2 = float(rsi_s.iloc[-3]) if len(rsi_s) >= 3 else None
    rsi_yukari_3g = (rsi is not None and rsi_prev is not None and rsi_prev2 is not None and
                     rsi > rsi_prev > rsi_prev2)
    # RSI 30 yukarı kesim (strateji 3 için)
    rsi_30_cross_up = (rsi_prev is not None and rsi is not None and rsi_prev <= 30 and rsi > 30)
    # RSI 50 yukarı kesim
    rsi_50_cross_up = (rsi_prev is not None and rsi is not None and rsi_prev <= 50 and rsi > 50)

    # ── MACD ──
    macd_line, macd_sig_line = calc_macd(close)
    macd_val = float(macd_line.iloc[-1]) if not macd_line.isna().iloc[-1] else None
    macd_sig_val = float(macd_sig_line.iloc[-1]) if not macd_sig_line.isna().iloc[-1] else None
    macd_prev = float(macd_line.iloc[-2]) if len(macd_line) >= 2 and not pd.isna(macd_line.iloc[-2]) else None
    macd_sig_prev = float(macd_sig_line.iloc[-2]) if len(macd_sig_line) >= 2 and not pd.isna(macd_sig_line.iloc[-2]) else None

    if macd_val is not None and macd_sig_val is not None:
        hist_cur = macd_val - macd_sig_val
        hist_prev = (macd_prev - macd_sig_prev) if (macd_prev is not None and macd_sig_prev is not None) else None
        macd_hist_pozitif = hist_cur > 0
        macd_hist_artiyor = hist_prev is not None and hist_cur > hist_prev
        macd_hist_pozitif_kesim = (hist_prev is not None and hist_prev <= 0 and hist_cur > 0)
    else:
        macd_hist_pozitif = False; macd_hist_artiyor = False; macd_hist_pozitif_kesim = False

    macd_above_signal = (macd_val is not None and macd_sig_val is not None and macd_val > macd_sig_val)
    macd_fresh_cross = (macd_val is not None and macd_sig_val is not None and
                        macd_prev is not None and macd_sig_prev is not None and
                        macd_val > macd_sig_val and macd_prev <= macd_sig_prev)

    # ── ADX ──
    adx_s = calc_adx(d)
    adx = float(adx_s.iloc[-1]) if not adx_s.isna().iloc[-1] else None

    # ── ATR ──
    atr_s = calc_atr(d, 14)
    atr = float(atr_s.iloc[-1]) if not atr_s.isna().iloc[-1] else None
    atr_pct = (atr / price * 100) if (atr and price > 0) else None

    # ── Bollinger ──
    bb_width_s = calc_bollinger_width(close)
    bb_width = float(bb_width_s.iloc[-1]) if not bb_width_s.isna().iloc[-1] else None
    _bb_avg20_raw = bb_width_s.rolling(20).mean().iloc[-1]
    bb_avg20 = float(_bb_avg20_raw) if len(bb_width_s) > 20 and not pd.isna(_bb_avg20_raw) else None
    bb_width_low = (bb_width is not None and bb_avg20 is not None and bb_width < bb_avg20)
    bb_avg60_raw = bb_width_s.rolling(60).mean().iloc[-1]
    bb_avg60 = float(bb_avg60_raw) if (len(bb_width_s) > 60 and not pd.isna(bb_avg60_raw)) else bb_avg20
    bb_width_low60 = (bb_width is not None and bb_avg60 is not None and bb_avg60 > 0 and bb_width < bb_avg60 * 0.70)
    bb_width_low65 = (bb_width is not None and bb_avg60 is not None and bb_avg60 > 0 and bb_width < bb_avg60 * 0.65)
    # Bollinger pozisyonu
    bb_sma = calc_sma(close, 20)
    bb_std = close.rolling(20).std()
    bb_upper = bb_sma + 2 * bb_std
    bb_lower = bb_sma - 2 * bb_std
    bb_pct_b = None
    if not bb_upper.isna().iloc[-1] and not bb_lower.isna().iloc[-1]:
        bw = float(bb_upper.iloc[-1]) - float(bb_lower.iloc[-1])
        if bw > 0:
            bb_pct_b = (price - float(bb_lower.iloc[-1])) / bw

    # ── VWAP ──
    typical_price = (high + low + close) / 3
    vwap_num = (typical_price * volume).rolling(20).sum()
    vwap_den = volume.rolling(20).sum()
    vwap_s = vwap_num / vwap_den.replace(0, float("nan"))
    vwap_raw = vwap_s.iloc[-1]
    vwap = float(vwap_raw) if (vwap_raw is not None and not pd.isna(vwap_raw)) else None
    price_above_vwap = (vwap is not None and price > vwap)

    # ── HACİM ──
    vol_avg20_raw = volume.rolling(20).mean().iloc[-1]
    vol_avg20 = float(vol_avg20_raw) if not pd.isna(vol_avg20_raw) else 0.0
    vol_cur = float(volume.iloc[-1]) if not pd.isna(volume.iloc[-1]) else 0.0
    rel_vol = vol_cur / vol_avg20 if vol_avg20 > 0 else 0
    vol_above_avg = vol_cur > vol_avg20

    # Hacim trendi (son 3 gün artan)
    vol_trend_up = False
    if len(volume) >= 4:
        v1 = float(volume.iloc[-3]) if not pd.isna(volume.iloc[-3]) else 0
        v2 = float(volume.iloc[-2]) if not pd.isna(volume.iloc[-2]) else 0
        v3 = float(volume.iloc[-1]) if not pd.isna(volume.iloc[-1]) else 0
        vol_trend_up = (v3 > v2 > v1 > 0)

    # ── OBV ──
    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    obv_ema21 = obv.ewm(span=21, adjust=False).mean()
    obv_bullish = float(obv.iloc[-1]) > float(obv_ema21.iloc[-1]) if not obv.isna().iloc[-1] else False

    # Alım baskısı (son 10 bar)
    buy_pressure = None
    if len(d) >= 10:
        last10 = d.iloc[-10:].copy()
        last10["up"] = last10["Close"] > last10["Close"].shift(1)
        up_vol = last10.loc[last10["up"], "Volume"].sum()
        total_vol = last10["Volume"].sum()
        if total_vol > 0:
            buy_pressure = up_vol / total_vol

    # ── PERFORMANS ──
    perf_1d = perf_pct(close, 1); perf_5d = perf_pct(close, 5)
    perf_21d = perf_pct(close, 21); perf_63d = perf_pct(close, 63); perf_252d = perf_pct(close, 252)

    # ── DİVERJANS (basit) ──
    has_bullish_div = False; has_bearish_div = False
    try:
        d_copy = d.copy(); d_copy["RSI"] = calc_rsi(d_copy["Close"], 14)
        dt, _ = detect_divergence(d_copy, window=60)
        if dt == "Bullish Diverjans": has_bullish_div = True
        elif dt == "Bearish Diverjans": has_bearish_div = True
    except: pass

    # ── DESTEK/DİRENÇ YAKINLIĞI ──
    recent_low_20 = float(low.iloc[-20:].min()) if len(low) >= 20 else None
    recent_high_20 = float(high.iloc[-20:].max()) if len(high) >= 20 else None
    near_support = (recent_low_20 is not None and price > 0 and
                    abs(price - recent_low_20) / price * 100 < 3)
    near_resistance = (recent_high_20 is not None and price > 0 and
                       abs(price - recent_high_20) / price * 100 < 3)

    # ── SMA50 YAKINLIĞI ──
    near_sma50 = (s50 is not None and s50 > 0 and abs(price - s50) / s50 * 100 <= 5)

    # ── Üst üste yeşil mum sayısı ──
    green_streak = 0
    for j in range(1, min(11, len(d))):
        c_j = float(close.iloc[-j]); o_j = float(open_.iloc[-j])
        if not pd.isna(c_j) and not pd.isna(o_j) and c_j > o_j:
            green_streak += 1
        else: break

    # ── Kâr marjı proxy (son çeyrek vs önceki yıl) ──
    earnings_momentum = False
    if perf_63d is not None and perf_252d is not None:
        if perf_63d > 15 and perf_252d > 30:
            earnings_momentum = True

    return {
        "ticker": ticker, "price": price, "kapanis_yukari": kapanis_yukari,
        "sma20": s20, "sma50": s50, "sma200": s200, "near_sma50": near_sma50,
        "rsi": rsi, "rsi_prev": rsi_prev, "rsi_yukari_3g": rsi_yukari_3g,
        "rsi_30_cross_up": rsi_30_cross_up, "rsi_50_cross_up": rsi_50_cross_up,
        "macd": macd_val, "macd_sig": macd_sig_val,
        "macd_above_signal": macd_above_signal,
        "macd_fresh_cross": macd_fresh_cross,
        "macd_hist_pozitif": macd_hist_pozitif,
        "macd_hist_artiyor": macd_hist_artiyor,
        "macd_hist_pozitif_kesim": macd_hist_pozitif_kesim,
        "adx": adx, "atr": atr, "atr_pct": atr_pct,
        "bb_width": bb_width, "bb_width_low": bb_width_low,
        "bb_width_low60": bb_width_low60, "bb_width_low65": bb_width_low65,
        "bb_pct_b": bb_pct_b,
        "vwap": vwap, "price_above_vwap": price_above_vwap,
        "vol_avg20": vol_avg20, "vol_cur": vol_cur, "rel_vol": rel_vol,
        "vol_above_avg": vol_above_avg, "vol_trend_up": vol_trend_up,
        "obv_bullish": obv_bullish, "buy_pressure": buy_pressure,
        "perf_1d": perf_1d, "perf_5d": perf_5d,
        "perf_21d": perf_21d, "perf_63d": perf_63d, "perf_252d": perf_252d,
        "has_bullish_div": has_bullish_div, "has_bearish_div": has_bearish_div,
        "near_support": near_support, "near_resistance": near_resistance,
        "green_streak": green_streak, "earnings_momentum": earnings_momentum,
        "ema9": e9, "ema21": e21, "ema_dizilimi": ema_dizilimi,
    }

# ═══════════════════════════════════════════════
# STRATEJİ FİLTRELERİ
# ═══════════════════════════════════════════════
TARA_STRATEJILER = {
    "1": ("🧠 Smart Money Birikim","Patlama öncesi sessiz birikim"),
    "2": ("📈 Düşen Trend Kırılımı","Yeni yükseliş başlangıcı"),
    "3": ("🔄 Güçlü Dipten Dönüş","Destekten güçlü RSI dönüşü"),
    "4": ("🚀 Momentum Patlaması","Sert yükseliş — hacim+RSI+MACD"),
    "6": ("🔥 Tavan Serisi","Ardışık tavan yapabilecek hisseler"),
    "7": ("💎 Akümülasyon Çıkışı","Büyük hareket başlangıcı"),
    "8": ("🌱 Erken Trend Doğumu","Trend başlıyor, SMA dizilimi oluştu"),
    "9": ("🏦 Kurumsal Para Girişi","Gizli kurumsal toplama"),
    "10":("⚖️ Güçlü Konsolidasyon","Sıkışma — yakında büyük hareket"),
    "11":("⚡ Volatilite Patlaması","Ani hacim + fiyat hareketi"),
    "12":("👑 Sektör Lideri","Güçlü hisseler — trend liderliği"),
    "13":("🌅 Dipten Lider Doğuşu","Derin düşüşten güçlü dönüş"),
    "14":("💰 Büyük Ralli","+%50 potansiyel — tüm göstergeler uyumlu"),
    "A": ("💪 Piyasadan Güçlü","Relative strength — endeksten üstün"),
    "B": ("💸 Büyük Para Girişi","Günlük trade fırsatı"),
    "C": ("🛡️ Endeks Düşerken Güçlü","Düşen piyasada ayakta"),
    "15":("📊 Bilançodan Önce Hareket","Güçlü büyüme + teknik uyum"),
    "16":("🎯 Baz Kırılımı","3ay yatay/düşen + EMA dizilimi + hacim artışı"),
}

# Strateji -> rejim kategorisi eşleşmesi
_STRATEJI_REJIM_TIPI = {
    "1":"squeeze","2":"breakout","3":"mean_rev","4":"momentum",
    "6":"momentum","7":"breakout","8":"breakout","9":"squeeze",
    "10":"squeeze","11":"momentum","12":"momentum","13":"mean_rev",
    "14":"momentum","A":"momentum","B":"momentum","C":"mean_rev","15":"breakout",
}

def strateji_filtre(ind, kod):
    p = ind; price = p["price"]
    def gt(val, thr): return val is not None and val > thr
    def lt(val, thr): return val is not None and val < thr
    def between(val, lo, hi): return val is not None and lo <= val <= hi
    def near(val, ref, pct): return val is not None and ref is not None and ref > 0 and abs(val-ref)/ref*100 <= pct

    if kod == "1":
        return (gt(price, p["sma200"] or 0) and near(price, p["sma50"], 5) and
                between(p["rsi"], 40, 68) and gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.8) and
                gt(p["rel_vol"], 1.8) and p["bb_width_low60"] and
                between(p["perf_5d"], 0, 6) and gt(p["perf_21d"], 11) and
                gt(p["adx"], 25) and p["price_above_vwap"] and
                p["kapanis_yukari"] and p["macd_hist_artiyor"])
    elif kod == "2":
        return (gt(price, p["sma20"] or 0) and gt(price, p["sma50"] or 0) and
                between(p["rsi"], 50, 74) and
                gt(p["macd"], p["macd_sig"] if p["macd_sig"] is not None else -999) and
                p["macd_hist_pozitif"] and p["macd_hist_artiyor"] and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.8) and gt(p["rel_vol"], 1.8) and
                gt(p["perf_1d"], 2) and gt(p["perf_5d"], 5) and lt(p["perf_21d"], 11) and
                gt(p["adx"], 25) and p["price_above_vwap"] and p["rsi_yukari_3g"])
    elif kod == "3":
        rsi3_yukari = (p["rsi"] is not None and p["rsi_prev"] is not None and p["rsi"] > p["rsi_prev"])
        return (lt(p["rsi"], 45) and rsi3_yukari and gt(price, p["sma20"] or 0) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.8) and gt(p["rel_vol"], 1.8) and
                gt(p["perf_1d"], 2) and lt(p["perf_5d"], 0) and lt(p["perf_21d"], 0) and
                gt(p["adx"], 25) and p["price_above_vwap"] and p["macd_hist_pozitif_kesim"])
    elif kod == "4":
        return (gt(price, p["sma50"] or 0) and gt(price, p["sma200"] or 0) and
                between(p["rsi"], 56, 74) and
                gt(p["macd"], p["macd_sig"] if p["macd_sig"] is not None else -999) and
                p["macd_hist_pozitif"] and p["macd_hist_artiyor"] and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*2.0) and gt(p["rel_vol"], 1.9) and
                gt(p["perf_5d"], 8) and gt(p["perf_21d"], 13) and gt(p["adx"], 28))
    elif kod == "6":
        return (gt(price, p["sma20"] or 0) and gt(price, p["sma50"] or 0) and
                between(p["rsi"], 64, 82) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*2.2) and gt(p["rel_vol"], 2.2) and
                gt(p["perf_1d"], 5) and gt(p["perf_5d"], 16) and gt(p["perf_21d"], 26) and
                gt(p["adx"], 32) and p["macd_hist_artiyor"] and p["price_above_vwap"])
    elif kod == "7":
        return (gt(price, p["sma50"] or 0) and gt(price, p["sma200"] or 0) and
                between(p["rsi"], 54, 70) and p["macd_fresh_cross"] and
                p["macd_hist_pozitif"] and p["macd_hist_artiyor"] and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.9) and gt(p["rel_vol"], 1.8) and
                gt(p["perf_1d"], 3) and gt(p["perf_5d"], 9) and
                between(p["perf_21d"], 0, 11) and gt(p["adx"], 25) and p["price_above_vwap"])
    elif kod == "8":
        return (gt(price, p["sma20"] or 0) and gt(price, p["sma50"] or 0) and
                gt(p["sma20"] or 0, p["sma50"] or 0) and between(p["rsi"], 48, 66) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.8) and gt(p["rel_vol"], 1.8) and
                between(p["perf_5d"], 0, 7) and between(p["perf_21d"], 0, 13) and
                gt(p["adx"], 25) and p["price_above_vwap"])
    elif kod == "9":
        return (gt(price, p["sma50"] or 0) and between(p["rsi"], 44, 62) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.9) and gt(p["rel_vol"], 1.9) and
                between(p["perf_5d"], 0, 7) and between(p["perf_21d"], 6, 22) and
                gt(p["adx"], 25) and p["price_above_vwap"] and p["macd_hist_pozitif"])
    elif kod == "10":
        return (near(price, p["sma50"], 5) and between(p["rsi"], 38, 56) and
                between(p["perf_1d"], -1.5, 1.5) and between(p["perf_5d"], -4, 4) and
                lt(p["vol_cur"], (p["vol_avg20"] or 0)*0.7) and p["bb_width_low65"] and lt(p["adx"], 20))
    elif kod == "11":
        return (gt(price, p["sma20"] or 0) and gt(p["rsi"], 56) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*2.2) and gt(p["rel_vol"], 2.2) and
                gt(p["perf_1d"], 3) and gt(p["perf_5d"], 9) and
                gt(p["adx"], 28) and p["price_above_vwap"])
    elif kod == "12":
        return (gt(price, p["sma50"] or 0) and gt(price, p["sma200"] or 0) and
                gt(p["rsi"], 59) and gt(p["perf_5d"], 11) and gt(p["perf_21d"], 19) and
                gt(p["perf_63d"], 32) and gt(p["adx"], 26))
    elif kod == "13":
        return (lt(p["perf_252d"], -28) and lt(p["rsi"], 43) and gt(price, p["sma20"] or 0) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.8) and gt(p["perf_1d"], 2) and
                gt(p["rel_vol"], 1.8) and gt(p["adx"], 22))
    elif kod == "14":
        return (gt(price, p["sma50"] or 0) and gt(price, p["sma200"] or 0) and
                between(p["rsi"], 61, 78) and gt(p["adx"], 32) and
                gt(p["macd"], p["macd_sig"] if p["macd_sig"] is not None else -999) and
                p["macd_hist_artiyor"] and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*2.2) and
                gt(p["perf_5d"], 13) and gt(p["perf_21d"], 21) and p["price_above_vwap"])
    elif kod == "15":
        return (gt(price, p["sma50"] or 0) and gt(p["rsi"], 56) and
                between(p["perf_5d"], 6, 30) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.8) and gt(p["adx"], 25) and
                p["macd_hist_pozitif"] and p["price_above_vwap"])
    elif kod == "A":
        return (gt(price, p["sma50"] or 0) and gt(price, p["sma200"] or 0) and
                gt(p["perf_5d"], 5) and gt(p["perf_21d"], 10) and gt(p["rsi"], 50))
    elif kod == "B":
        return (gt(p["vol_cur"], (p["vol_avg20"] or 0)*1.5) and gt(p["rel_vol"], 1.5) and
                gt(p["perf_1d"], 1) and gt(p["rsi"], 45) and gt(price, p["sma20"] or 0))
    elif kod == "C":
        return (gt(p["perf_1d"], 0) and gt(p["perf_5d"], 3) and
                gt(p["rsi"], 50) and gt(price, p["sma50"] or 0))
    elif kod == "16":
        # Baz Kırılımı: EMA9>EMA21>SMA50 dizilimi + MACD pozitif + RSI uygun
        return (gt(price, p["sma50"] or 0) and gt(price, p["sma200"] or 0) and
                p.get("ema_dizilimi", False) and
                p["macd_hist_pozitif"] and p["macd_above_signal"] and
                between(p["rsi"], 50, 70) and
                gt(p["adx"], 18))
    return False

# ═══════════════════════════════════════════════
# TARA KAYIT / YÜKLEME / GEÇMİŞ KARŞILAŞTIRMA
# ═══════════════════════════════════════════════
def tara_save(chat_id, kod, eslesen, ai_yorum=""):
    tr_tz = pytz.timezone("Europe/Istanbul")
    simdi = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    veri = {
        "tarih": simdi,
        "eslesen": [e["ticker"].replace(".IS","") for e in eslesen],
        "detay": [{"ticker":e["ticker"].replace(".IS",""),
                   "rsi":round(e.get("rsi") or 0,1), "rel_vol":round(e.get("rel_vol") or 0,2),
                   "perf_1d":round(e.get("perf_1d") or 0,2), "perf_5d":round(e.get("perf_5d") or 0,2),
                   "perf_21d":round(e.get("perf_21d") or 0,2)} for e in eslesen],
        "ai_yorum": ai_yorum,
    }
    db_set(f"tara_sonuc_{chat_id}_{kod}", veri)
    ozet = db_get(f"tara_ozet_{chat_id}", {})
    isim, _ = TARA_STRATEJILER.get(kod, (kod,""))
    ozet[kod] = {"isim":isim, "tarih":simdi, "sayi":len(eslesen)}
    db_set(f"tara_ozet_{chat_id}", ozet)

def tara_load(chat_id, kod):
    return db_get(f"tara_sonuc_{chat_id}_{kod}")

def tara_load_ozet(chat_id):
    return db_get(f"tara_ozet_{chat_id}", {})

def gecmis_karsilastir(chat_id, kod, yeni_eslesen):
    try:
        tr_tz = pytz.timezone("Europe/Istanbul")
        simdi = datetime.now(tr_tz)
        veri = tara_load(chat_id, kod)
        if not veri: return
        tarih_str = veri.get("tarih","")
        if not tarih_str: return
        try:
            gecmis_tarih = datetime.strptime(tarih_str, "%d.%m.%Y %H:%M")
            gecmis_tarih = pytz.timezone("Europe/Istanbul").localize(gecmis_tarih)
        except Exception: return
        gun_farki = (simdi - gecmis_tarih).days
        if gun_farki < 1 or gun_farki > 14: return
        gecmis_setl = set(veri.get("eslesen",[]))
        yeni_setl = {ind["ticker"].replace(".IS","") for ind in yeni_eslesen}
        devam_eden = yeni_setl & gecmis_setl
        yeni_giren = yeni_setl - gecmis_setl
        cikan = gecmis_setl - yeni_setl
        if not devam_eden and not yeni_giren: return
        isim = TARA_STRATEJILER.get(kod, (kod,""))[0]
        satirlar = [f"📅 GEÇMİŞ KARŞILAŞTIRMA — {isim} ({gun_farki} gün önce)", ""]
        if yeni_giren:
            satirlar.append(f"🆕 YENİ GİREN ({len(yeni_giren)}):")
            satirlar.append("  " + ", ".join(sorted(yeni_giren)))
        if devam_eden:
            satirlar.append(f"🔄 DEVAM EDEN ({len(devam_eden)}):")
            satirlar.append("  " + ", ".join(sorted(devam_eden)))
        if cikan:
            satirlar.append(f"⬇️ ÇIKAN ({len(cikan)}):")
            satirlar.append("  " + ", ".join(sorted(cikan)))
        bot.send_message(chat_id, "\n".join(satirlar))
    except Exception as e:
        debug_log("WARN","gecmis_karsilastir",str(e)[:100])

def coklu_strateji_kontrol(chat_id, tum_eslesen):
    try:
        sayac = {}; hangi = {}
        for kod, eslesen_list in tum_eslesen.items():
            for ind in eslesen_list:
                t = ind["ticker"].replace(".IS","")
                sayac[t] = sayac.get(t,0) + 1
                hangi.setdefault(t,[]).append(TARA_STRATEJILER.get(kod,(kod,""))[0])
        super_sinyaller = {t:(sayac[t],hangi[t]) for t in sayac if sayac[t] >= 3}
        if not super_sinyaller: return
        satirlar = ["🚀 SÜPER SİNYAL — Çoklu Strateji Uyarısı!", "━━━━━━━━━━━━━━━━━━━",
                    "Bu hisseler 3+ stratejide eşleşti:", ""]
        for t, (sayi, stratejiler) in sorted(super_sinyaller.items(), key=lambda x: -x[1][0]):
            satirlar.append(f"⭐ *{t}* — {sayi} strateji")
            for s in stratejiler:
                satirlar.append(f"   ✓ {s}")
            satirlar.append(f"   📊 https://tr.tradingview.com/chart/?symbol=BIST:{t}")
            satirlar.append("")
        satirlar.append("⚡ /analiz HISSE ile AI yorumu al!")
        safe_send(chat_id, "\n".join(satirlar))
    except Exception as e:
        debug_log("WARN","coklu_strateji",str(e)[:100])

# ═══════════════════════════════════════════════
# TARA FORMAT + CONFIDENCE
# ═══════════════════════════════════════════════
def tara_format_results(kod, eslesen):
    isim, aciklama = TARA_STRATEJILER.get(kod, (kod,""))
    satirlar = [f"{isim}", f"{aciklama}", f"Eşleşme: {len(eslesen)} hisse", "━━━━━━━━━━━━━━━━━━━"]
    if not eslesen:
        satirlar.append("Bu kriterle eşleyen hisse bulunamadı.")
        return "\n".join(satirlar), []

    eslesen_sorted = sorted(eslesen, key=lambda x: x.get("rsi") or 0, reverse=True)
    for ind in eslesen_sorted[:20]:
        t = ind["ticker"].replace(".IS","")
        rsi = ind["rsi"]; rv = ind["rel_vol"]
        p1d = ind["perf_1d"]; p5d = ind["perf_5d"]; p21d = ind["perf_21d"]
        fiyat = ind.get("price")

        # Confidence hesapla
        conf = calc_signal_confidence(ind, kod)
        conf_lbl = confidence_label(conf)

        tv_link = f"https://tr.tradingview.com/chart/?symbol=BIST:{t}"
        if rsi is not None and rv is not None and p1d is not None and p5d is not None and p21d is not None:
            fiyat_str = f"{fiyat:.2f}₺ | " if fiyat else ""
            satirlar.append(
                f"{t} | {fiyat_str}RSI:{rsi:.0f} | RV:{rv:.1f}x | "
                f"G:{'+' if p1d>0 else ''}{p1d:.1f}% "
                f"H:{'+' if p5d>0 else ''}{p5d:.1f}% "
                f"A:{'+' if p21d>0 else ''}{p21d:.1f}% | "
                f"Güven:{conf}% {conf_lbl} | 📊 {tv_link}")
        else:
            satirlar.append(f"{t} | 📊 {tv_link}")
    if len(eslesen) > 20:
        satirlar.append(f"... ve {len(eslesen)-20} hisse daha")
    return "\n".join(satirlar), eslesen_sorted[:5]

def tara_single_strategy(chat_id, tickers, kod):
    isim, aciklama = TARA_STRATEJILER.get(kod, (kod,""))
    eslesen = []; hata = 0

    cached_set = _get_cached_tickers_today()
    missing = [t for t in tickers if t not in cached_set]

    if missing:
        bot.send_message(chat_id,
            f"📥 {len(tickers)-len(missing)} hazır | {len(missing)} indiriliyor\nİptal: /iptal tara")
        for i, ticker in enumerate(missing):
            if is_cancelled(chat_id, "tara"):
                bot.send_message(chat_id, "🚫 İptal edildi."); return None
            get_data(ticker)
            if (i+1)%20 == 0:
                pct = int((i+1)/len(missing)*100)
                bot.send_message(chat_id, f"📥 {i+1}/{len(missing)} ({pct}%)")
            time.sleep(TD_DELAY)
        _invalidate_cache_set()
        bot.send_message(chat_id, "✅ Veri hazır, filtreler uygulanıyor...")

    filtre_baslangic = time.time()
    for i, ticker in enumerate(tickers):
        if is_cancelled(chat_id, "tara"):
            bot.send_message(chat_id, f"🚫 İptal ({len(eslesen)} eşleşme)"); return None
        try:
            ind = tara_indicators(ticker)
            if ind is None: hata += 1; continue
            if strateji_filtre(ind, kod): eslesen.append(ind)
        except Exception as e:
            hata += 1; debug_log("WARN",f"tara/{kod}",f"{ticker}: {str(e)[:60]}")
        if (i+1)%50 == 0:
            gecen = max(1, int(time.time()-filtre_baslangic))
            kalan = int((len(tickers)-(i+1))*(gecen/(i+1)))
            bot.send_message(chat_id, f"📊 {i+1}/{len(tickers)} ({len(eslesen)} eşleşme)")
    return eslesen

# ═══════════════════════════════════════════════
# /tara KOMUTU
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['tara'])
def cmd_tara(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("tara", chat_id, parts[1] if len(parts)>1 else "menu")

    if len(parts) < 2:
        menu = [
            "📊 TARA — Strateji Seçimi", "━━━━━━━━━━━━━━━━━━━", "Kullanım: /tara [numara]", "",
            "🧠 /tara 1  — Smart Money Birikim", "📈 /tara 2  — Düşen Trend Kırılımı",
            "🔄 /tara 3  — Güçlü Dipten Dönüş", "🚀 /tara 4  — Momentum Patlaması",
            "🔥 /tara 6  — Tavan Serisi", "💎 /tara 7  — Akümülasyon Çıkışı",
            "🌱 /tara 8  — Erken Trend Doğumu", "🏦 /tara 9  — Kurumsal Para Girişi",
            "⚖️ /tara 10 — Güçlü Konsolidasyon", "⚡ /tara 11 — Volatilite Patlaması",
            "👑 /tara 12 — Sektör Lideri", "🌅 /tara 13 — Dipten Lider Doğuşu",
            "💰 /tara 14 — Büyük Ralli", "",
            "⭐ EKSTRA:", "💪 /tara A  — Piyasadan Güçlü", "💸 /tara B  — Büyük Para Girişi",
            "🛡️ /tara C  — Endeks Düşerken Güçlü", "📊 /tara 15 — Bilançodan Önce Hareket", "",
            "🎯 /tara 16 — Baz Kırılımı (YENİ)",
            "🔍 /tara all — Tüm 18 strateji", "♠️ /tara spade — SpadeHunter",
        ]
        bot.send_message(chat_id, "\n".join(menu)); return

    kod = parts[1].upper()
    if kod == "ALL":
        threading.Thread(target=_tara_all, args=(chat_id,), daemon=True).start(); return
    if kod == "SPADE":
        threading.Thread(target=_tara_spade, args=(chat_id,), daemon=True).start(); return

    kod = parts[1]
    for k in TARA_STRATEJILER:
        if k.upper() == kod.upper():
            kod = k; break
    if kod in TARA_STRATEJILER:
        threading.Thread(target=_tara_single, args=(chat_id, kod), daemon=True).start()
    else:
        bot.send_message(chat_id, f"❌ Geçersiz: {kod}\n/tara ile listeyi gör.")

def _tara_single(chat_id, kod):
    bot.send_message(chat_id, f"🔍 *Scanner Agent* {kod} analiz ediyor...", parse_mode='Markdown')
    try:
        isim, aciklama = TARA_STRATEJILER[kod]
        tickers = wl_get(chat_id)
        if not tickers: bot.send_message(chat_id, "Watchlist boş! /addall yaz."); return
        tr_tz = pytz.timezone("Europe/Istanbul")
        reset_cancel_flag(chat_id, "tara")
        bot.send_message(chat_id, f"Strateji: {isim}\nToplam: {len(tickers)} hisse\nİptal: /iptal tara")

        eslesen = tara_single_strategy(chat_id, tickers, kod)
        if eslesen is None: return

        mesaj, top5 = tara_format_results(kod, eslesen)
        bot.send_message(chat_id, mesaj)
        tara_save(chat_id, kod, eslesen)
        if eslesen: gecmis_karsilastir(chat_id, kod, eslesen)

        simdi = datetime.now(tr_tz).strftime("%H:%M")
        bot.send_message(chat_id, f"Tarama tamamlandı ({simdi}) — {len(eslesen)} eşleşme")
    except Exception as e:
        debug_log("ERROR","_tara_single",str(e)[:150])
        bot.send_message(chat_id, f"Tara hatası: {str(e)[:100]}")

def _tara_all(chat_id):
    bot.send_message(chat_id, "🔍 *Scanner Agent* tarama başlatıldı...", parse_mode='Markdown')
    """Tüm stratejiler — veri BİR KEZ indir, indikatörler BİR KEZ hesapla."""
    tickers = wl_get(chat_id)
    if not tickers: bot.send_message(chat_id, "📭 Watchlist boş! /addall yaz."); return

    tr_tz = pytz.timezone("Europe/Istanbul")
    simdi = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    kodlar = list(TARA_STRATEJILER.keys())

    reset_cancel_flag(chat_id, "tara")

    # Market rejim tespiti
    try:
        xu100_d, _ = get_data("XU100")
        regime, regime_details = detect_market_regime(xu100_d)
    except Exception:
        regime, regime_details = "UNKNOWN", {}

    regime_emoji = {"TREND_UP":"📈","TREND_DOWN":"📉","RANGE":"↔️","HIGH_VOL":"🌊","UNKNOWN":"❓"}
    bot.send_message(chat_id,
        f"📊 TAM STRATEJİ TARAMASI\n{simdi}\n"
        f"{len(tickers)} hisse x {len(kodlar)} strateji\n"
        f"{regime_emoji.get(regime,'❓')} Piyasa Rejimi: {regime}\n"
        f"🚫 İptal: /iptal tara\n━━━━━━━━━━━━━━━━━━━")

    # Eksik verileri indir
    cached_set = _get_cached_tickers_today()
    missing = [t for t in tickers if t not in cached_set]
    if missing:
        bot.send_message(chat_id, f"📥 {len(cached_set)} hazır | {len(missing)} indiriliyor")
        for i, ticker in enumerate(missing):
            if is_cancelled(chat_id, "tara"):
                bot.send_message(chat_id, "🚫 İptal."); return
            get_data(ticker)
            if (i+1)%20 == 0:
                bot.send_message(chat_id, f"📥 {i+1}/{len(missing)} ({int((i+1)/len(missing)*100)}%)")
            time.sleep(TD_DELAY)
        _invalidate_cache_set()
        bot.send_message(chat_id, f"✅ Veri hazır. Göstergeler hesaplanıyor...")
    else:
        bot.send_message(chat_id, f"⚡ Cache hazır. Göstergeler hesaplanıyor...")

    if is_cancelled(chat_id, "tara"):
        bot.send_message(chat_id, "🚫 İptal."); return

    # İndikatörleri BİR KEZ hesapla — df_d parametreli
    tum_ind = {}; df_cache = {}; hata = 0
    for i, ticker in enumerate(tickers):
        if is_cancelled(chat_id, "tara"):
            bot.send_message(chat_id, "🚫 İptal."); return
        try:
            df_d, _ = get_data(ticker)
            if isinstance(df_d, pd.DataFrame) and not df_d.empty:
                df_cache[ticker] = df_d
                ind = tara_indicators(ticker, df_d=df_d)
                if ind is not None:
                    tum_ind[ticker] = ind
                else:
                    hata += 1
            else:
                hata += 1
        except Exception as e:
            hata += 1
            debug_log("WARN","tara_all/ind",f"{ticker}: {str(e)[:60]}")
        if (i+1)%50 == 0:
            bot.send_message(chat_id, f"📊 Göstergeler: {i+1}/{len(tickers)} ({len(tum_ind)} geçerli)")

    bot.send_message(chat_id,
        f"✅ Göstergeler: {len(tum_ind)}/{len(tickers)} ({hata} veri yok)\n17 strateji filtreleniyor...")

    # Stratejileri filtrele — rejim ağırlıklandırması ile
    regime_weights = regime_strategy_weight(regime)
    ozet_satirlar = [f"📊 TARAMA ÖZETİ — {simdi}", f"{regime_emoji.get(regime,'❓')} Rejim: {regime}", ""]
    tum_eslesen = {}

    for i, kod in enumerate(kodlar):
        if is_cancelled(chat_id, "tara"):
            bot.send_message(chat_id, "🚫 İptal."); return
        isim, _ = TARA_STRATEJILER[kod]
        eslesen = []
        for ticker, ind in tum_ind.items():
            try:
                if strateji_filtre(ind, kod): eslesen.append(ind)
            except Exception: pass

        tum_eslesen[kod] = eslesen

        # Rejim ağırlığı göster
        rejim_tipi = _STRATEJI_REJIM_TIPI.get(kod, "momentum")
        weight = regime_weights.get(rejim_tipi, 1.0)
        weight_str = ""
        if weight >= 1.2: weight_str = " ⬆️"
        elif weight <= 0.7: weight_str = " ⬇️"

        ozet_satirlar.append(f"{isim}: {len(eslesen)} hisse{weight_str}")

        mesaj, top5 = tara_format_results(kod, eslesen)
        bot.send_message(chat_id, mesaj)
        tara_save(chat_id, kod, eslesen)
        if eslesen: gecmis_karsilastir(chat_id, kod, eslesen)
        time.sleep(0.3)

    # Özet
    en_cok = sorted(tum_eslesen.items(), key=lambda x: len(x[1]), reverse=True)[:3]
    ozet_satirlar += ["","━━━━━━━━━━━━━━━━━━━","EN FAZLA EŞLEŞEN:"]
    for kod, eslesen in en_cok:
        isim, _ = TARA_STRATEJILER[kod]
        ozet_satirlar.append(f"  {isim}: {len(eslesen)} hisse")
    bot.send_message(chat_id, "\n".join(ozet_satirlar))
    coklu_strateji_kontrol(chat_id, tum_eslesen)
    bot.send_message(chat_id, "✅ Tüm stratejiler tamamlandı!")

# ═══════════════════════════════════════════════
# SPADE HUNTER
# ═══════════════════════════════════════════════
SPADE_BUY_LIMIT = 0; SPADE_VOL_MULT = 1.8; SPADE_VOL_EARLY = 1.3
SPADE_ADX_THRESH = 25; SPADE_RSI_LOWER = 40; SPADE_RSI_UPPER = 68
SPADE_M1 = 5; SPADE_M2 = 5

def spade_calc_obv(close, volume):
    direction = np.sign(close.diff().fillna(0))
    return (direction * volume).cumsum()

def spade_calc_cmf(high, low, close, volume, period=21):
    denom = (high - low).replace(0, np.nan)
    mfm = ((close - low) - (high - close)) / denom
    mfv = mfm * volume
    return mfv.rolling(period).sum() / volume.rolling(period).sum().replace(0, np.nan)

def spade_indicators(ticker):
    df_d, df_w = get_data(ticker)
    if df_w is None or df_w.empty:
        if df_d is not None and not df_d.empty: df_w = resample_weekly(df_d)
        else: return None
    if len(df_w) < 60: return None

    d = df_w.copy()
    tr_now = datetime.now(pytz.timezone("Europe/Istanbul"))
    if tr_now.weekday() < 5 and len(d) > 1:
        d = d.iloc[:-1]

    close = d["Close"]; high = d["High"]; low = d["Low"]; volume = d["Volume"]
    open_ = d["Open"] if "Open" in d.columns else close
    price = float(close.iloc[-1])

    sma20 = calc_sma(close,20); sma50 = calc_sma(close,50); sma200 = calc_sma(close,200)
    rsi14 = calc_rsi(close,14)
    macd_line, macd_sig = calc_macd(close)
    macd_hist = macd_line - macd_sig
    macd_hist_ris = (macd_hist > macd_hist.shift(1))
    macd_full_bull = (macd_line > macd_sig) & (macd_hist > 0) & macd_hist_ris
    macd_hist_pos = macd_hist > 0

    vma20 = volume.rolling(20).mean()
    rvol = volume / vma20.replace(0, np.nan)
    vol_trend3 = (volume > volume.shift(1)) & (volume.shift(1) > volume.shift(2))
    early_vol = (rvol >= SPADE_VOL_EARLY) & vol_trend3
    high_vol = rvol >= SPADE_VOL_MULT

    adx_s = calc_adx(d)
    strong_trend = adx_s > SPADE_ADX_THRESH

    hlc3 = (high + low + close) / 3
    cum_tp_vol = (hlc3 * volume).rolling(252, min_periods=1).sum()
    cum_vol = volume.rolling(252, min_periods=1).sum().replace(0, np.nan)
    vwap_s = cum_tp_vol / cum_vol
    above_vwap = close > vwap_s

    bb_width = calc_bollinger_width(close, 20, 2)
    bb_wma60 = bb_width.rolling(60).mean()
    bb_sqz = bb_width < bb_wma60 * 0.70
    bb_tsqz = bb_width < bb_wma60 * 0.65

    cmf21 = spade_calc_cmf(high, low, close, volume, 21)
    cmf_pos = cmf21 > 0.05; cmf_strong = cmf21 > 0.15
    obv_val = spade_calc_obv(close, volume)
    obv_ema = obv_val.ewm(span=21, adjust=False).mean()
    obv_bull = obv_val > obv_ema

    perf_d = (close / close.shift(1) - 1.0) * 100.0
    perf_w = (close / close.shift(5) - 1.0) * 100.0
    perf_m = (close / close.shift(21) - 1.0) * 100.0

    rsi_x50 = (((rsi14.shift(2)<=50)&(rsi14.shift(1)>50)) | ((rsi14.shift(1)<=50)&(rsi14>50)))

    # Weekly onay
    _sma50_v = float(sma50.iloc[-1]) if not pd.isna(sma50.iloc[-1]) else None
    _sma200_v = float(sma200.iloc[-1]) if not pd.isna(sma200.iloc[-1]) else None
    _rsi_wok = float(rsi14.iloc[-1]) if not pd.isna(rsi14.iloc[-1]) else None
    _mh_wok = float((macd_line - macd_sig).iloc[-1])
    _cmf_wok = float(cmf21.iloc[-1]) if not pd.isna(cmf21.iloc[-1]) else 0.0

    _wk1 = (_sma50_v is not None) and price > _sma50_v
    _wk2 = (_rsi_wok is not None) and 35.0 <= _rsi_wok <= 75.0
    _wk3 = not pd.isna(_mh_wok) and _mh_wok > 0.0
    _wk4 = _cmf_wok > 0.0
    _wk5 = float(obv_val.iloc[-1]) > float(obv_ema.iloc[-1])
    _wk6 = (_sma200_v is not None) and price > _sma200_v
    weekly_ok = sum([_wk1,_wk2,_wk3,_wk4,_wk5,_wk6]) >= 4

    # Composite
    c1 = pd.Series(0.0, index=close.index)
    c1 += (close > sma200).fillna(False)*12.0
    c1 += ((sma50>0)&((close-sma50).abs()/sma50.replace(0,np.nan)*100<5.0)).fillna(False)*10.0
    c1 += ((rsi14>=SPADE_RSI_LOWER)&(rsi14<=SPADE_RSI_UPPER)).fillna(False)*8.0
    c1 += early_vol.fillna(False)*12.0; c1 += bb_sqz.fillna(False)*12.0
    c1 += ((perf_w>=0.0)&(perf_w<=6.0)).fillna(False)*5.0
    c1 += (perf_m>11.0).fillna(False)*8.0; c1 += strong_trend.fillna(False)*8.0
    c1 += above_vwap.fillna(False)*8.0; c1 += (close>open_).fillna(False)*5.0
    c1 += macd_hist_ris.fillna(False)*5.0; c1 += cmf_pos.fillna(False)*4.0
    c1 += obv_bull.fillna(False)*3.0; n1 = c1.clip(upper=100.0)

    c2 = pd.Series(0.0, index=close.index)
    c2 += (close>sma20).fillna(False)*8.0; c2 += (close>sma50).fillna(False)*8.0
    c2 += ((rsi14>=50.0)&(rsi14<=74.0)).fillna(False)*12.0
    c2 += macd_full_bull.fillna(False)*16.0; c2 += early_vol.fillna(False)*12.0
    c2 += (perf_d>2.0).fillna(False)*8.0; c2 += (perf_w>5.0).fillna(False)*8.0
    c2 += (perf_m<11.0).fillna(False)*3.0; c2 += strong_trend.fillna(False)*8.0
    c2 += above_vwap.fillna(False)*8.0; c2 += rsi_x50.fillna(False)*5.0
    c2 += (cmf21>0.1).fillna(False)*4.0; n2 = c2.clip(upper=100.0)

    c3 = pd.Series(0.0, index=close.index)
    c3 += (close>sma50).fillna(False)*15.0
    c3 += ((rsi14>=44.0)&(rsi14<=62.0)).fillna(False)*15.0
    c3 += (rvol>1.9).fillna(False)*18.0
    c3 += ((perf_w>=0.0)&(perf_w<=7.0)).fillna(False)*8.0
    c3 += ((perf_m>=6.0)&(perf_m<=22.0)).fillna(False)*8.0
    c3 += strong_trend.fillna(False)*10.0; c3 += above_vwap.fillna(False)*10.0
    c3 += macd_hist_pos.fillna(False)*8.0; c3 += cmf_strong.fillna(False)*8.0
    n3 = c3.clip(upper=100.0)

    c4 = pd.Series(0.0, index=close.index)
    c4 += ((sma50>0)&((close-sma50).abs()/sma50.replace(0,np.nan)*100<5.0)).fillna(False)*18.0
    c4 += ((rsi14>=38.0)&(rsi14<=56.0)).fillna(False)*18.0
    c4 += (perf_d.abs()<=1.5).fillna(False)*14.0; c4 += (perf_w.abs()<=4.0).fillna(False)*14.0
    c4 += (volume<0.7*vma20).fillna(False)*14.0; c4 += bb_tsqz.fillna(False)*14.0
    c4 += (adx_s<20.0).fillna(False)*8.0; n4 = c4.clip(upper=100.0)

    composite = n1*0.30 + n2*0.25 + n3*0.25 + n4*0.20
    brk_event = ((close>sma20)&(close.shift(1)<=sma20))|((close>sma50)&(close.shift(1)<=sma50))
    fake_break = brk_event & (~high_vol | ~strong_trend | ~above_vwap)
    adj_comp = composite.where(~fake_break, composite*0.60)
    main_line = adj_comp.ewm(span=SPADE_M1, adjust=False).mean()
    sig_line = main_line.ewm(span=SPADE_M2, adjust=False).mean()
    hist_val = main_line - sig_line

    def sv(s):
        v = s.iloc[-1]; return None if pd.isna(v) else float(v)

    ml=sv(main_line); sl=sv(sig_line); hv=sv(hist_val)
    n1v=sv(n1); n2v=sv(n2); n3v=sv(n3); n4v=sv(n4); comp=sv(composite)
    adx_v=sv(adx_s); rsi_v=sv(rsi14); rvol_v=sv(rvol)
    fb=bool(fake_break.iloc[-1]); ev=bool(early_vol.iloc[-1]); st=bool(strong_trend.iloc[-1])
    mhr=bool(macd_hist_ris.iloc[-1]); mfb=bool(macd_full_bull.iloc[-1])

    if ml is None or sl is None or hv is None: return None

    ml_prev = sv(main_line.shift(1))
    ml_cross_up = (ml_prev is not None) and (ml_prev <= SPADE_BUY_LIMIT) and (ml > SPADE_BUY_LIMIT)
    ml_rising = (ml_prev is not None) and (ml > ml_prev)

    sig_b = (n1v is not None and n1v>=65.0) and ml_cross_up and not fb
    sig_k = (n2v is not None and n2v>=65.0) and ev and st and bool(above_vwap.iloc[-1]) and mfb and not fb
    sig_kp = (n3v is not None and n3v>=65.0) and ml_rising and bool(cmf_strong.iloc[-1]) and not fb
    sig_s = (n4v is not None and n4v>=60.0) and bool(bb_sqz.iloc[-1]) and (adx_v is not None and adx_v<20.0)

    sig_count = sum([sig_b, sig_k, sig_kp])
    master_buy = (sig_count>=1 and ml>sl and hv>0.0 and ml>SPADE_BUY_LIMIT and not fb and mhr)

    t1=weekly_ok; t2=not fb; t3=ev; t4=st
    t5=bool(cmf_pos.iloc[-1]); t6=bool(above_vwap.iloc[-1]); t7=mhr
    t8=(rsi_v is not None) and 35.0<=rsi_v<=75.0
    onay_count = sum([t1,t2,t3,t4,t5,t6,t7,t8])
    tam_onay = (onay_count>=7) and master_buy

    if not master_buy: return None

    sinyaller = []
    if sig_b: sinyaller.append("B")
    if sig_k: sinyaller.append("K")
    if sig_kp: sinyaller.append("KP")
    if sig_s: sinyaller.append("S")

    cmf_v = float(cmf21.iloc[-1]) if not pd.isna(cmf21.iloc[-1]) else 0.0
    vwap_v = float(vwap_s.iloc[-1]) if not pd.isna(vwap_s.iloc[-1]) else 0.0

    return {
        "ticker":ticker,"price":price,"tam_onay":tam_onay,"master_buy":master_buy,
        "onay_count":onay_count,"composite":round(comp,1) if comp else 0,
        "sinyaller":"+".join(sinyaller) if sinyaller else "MB",
        "rsi":round(rsi_v,1) if rsi_v else 0,"adx":round(adx_v,1) if adx_v else 0,
        "rvol":round(rvol_v,2) if rvol_v else 0,"weekly_ok":t1,"fake_break":fb,
        "n1":round(n1v,1) if n1v else 0,"n2":round(n2v,1) if n2v else 0,
        "n3":round(n3v,1) if n3v else 0,"n4":round(n4v,1) if n4v else 0,
        "ml":round(ml,2),"sl":round(sl,2),"hv":round(hv,2),
        "ml_prev":round(ml_prev,2) if ml_prev else 0,
        "t1_weekly":t1,"t2_fake":t2,"t3_vol":t3,"t4_adx":t4,
        "t5_cmf":t5,"t6_vwap":t6,"t7_macd":t7,"t8_rsi":t8,
        "sig_b":sig_b,"sig_k":sig_k,"sig_kp":sig_kp,"sig_s":sig_s,
        "vwap":round(vwap_v,2),"cmf":round(cmf_v,3),
        "early_vol":ev,"strong_trend":st,
    }

def _tara_spade(chat_id):
    tickers = wl_get(chat_id)
    if not tickers: bot.send_message(chat_id, "📭 Watchlist boş!"); return
    tr_tz = pytz.timezone("Europe/Istanbul")
    simdi = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    reset_cancel_flag(chat_id, "tara")

    cached_set = _get_cached_tickers_today()
    missing = [t for t in tickers if t not in cached_set]

    bot.send_message(chat_id,
        f"♠️ *SPADE HUNTER*\n━━━━━━━━━━━━━━━━━━━\n{simdi}\n"
        f"📋 {len(tickers)} hisse | 💾 {len(tickers)-len(missing)} cache | 📥 {len(missing)} indir\n"
        f"🚫 İptal: /iptal tara")

    if missing:
        for i, ticker in enumerate(missing):
            if is_cancelled(chat_id, "tara"):
                bot.send_message(chat_id, "🚫 İptal."); return
            get_data(ticker)
            if (i+1)%20==0:
                bot.send_message(chat_id, f"📥 {i+1}/{len(missing)} ({int((i+1)/len(missing)*100)}%)")
            time.sleep(TD_DELAY)
        _invalidate_cache_set()
        bot.send_message(chat_id, "✅ Veri hazır, SpadeHunter hesaplanıyor...")
    else:
        bot.send_message(chat_id, "⚡ Cache hazır. SpadeHunter hesaplanıyor...")

    tam_onay_list = []; master_buy_list = []; hata = 0
    baslangic = time.time()
    for i, ticker in enumerate(tickers):
        if is_cancelled(chat_id, "tara"):
            bot.send_message(chat_id, "🚫 İptal."); return
        try:
            sonuc = spade_indicators(ticker)
            if sonuc is None: continue
            if sonuc["tam_onay"]: tam_onay_list.append(sonuc)
            elif sonuc["master_buy"]: master_buy_list.append(sonuc)
        except Exception as e:
            hata += 1; debug_log("WARN","spade",f"{ticker}: {str(e)[:80]}")
        if (i+1)%10==0:
            bot.send_message(chat_id,
                f"📊 {i+1}/{len(tickers)} | 🏆 {len(tam_onay_list)} tam | ⚡ {len(master_buy_list)} master")

    tam_onay_list.sort(key=lambda x: x["composite"], reverse=True)
    master_buy_list.sort(key=lambda x: x["composite"], reverse=True)

    if tam_onay_list:
        satirlar = [f"🏆 *TAM ONAY* — {len(tam_onay_list)} hisse", "━━━━━━━━━━━━━━━━━━━"]
        for s in tam_onay_list:
            h = "✅W" if s["weekly_ok"] else "❌W"
            satirlar.append(f"*{s['ticker']}* {s['price']:.2f}₺ Skor:{s['composite']:.0f} [{s['sinyaller']}] RSI:{s['rsi']:.0f} Vol:{s['rvol']:.1f}x {h} {s['onay_count']}/8")
        bot.send_message(chat_id, "\n".join(satirlar), parse_mode='Markdown')
    else:
        bot.send_message(chat_id, "🏆 TAM ONAY: Bugün eşleşme yok.")

    if master_buy_list:
        satirlar = [f"⚡ *MASTER BUY* — {len(master_buy_list)} hisse", "━━━━━━━━━━━━━━━━━━━"]
        for s in master_buy_list[:20]:
            h = "✅W" if s["weekly_ok"] else "❌W"
            satirlar.append(f"*{s['ticker']}* {s['price']:.2f}₺ Skor:{s['composite']:.0f} [{s['sinyaller']}] RSI:{s['rsi']:.0f} Vol:{s['rvol']:.1f}x {h} {s['onay_count']}/8")
        bot.send_message(chat_id, "\n".join(satirlar), parse_mode='Markdown')
    else:
        bot.send_message(chat_id, "⚡ MASTER BUY: Bugün eşleşme yok.")

    bot.send_message(chat_id,
        f"♠️ SpadeHunter tamamlandı\n🏆 Tam:{len(tam_onay_list)} ⚡ Master:{len(master_buy_list)}" +
        (f"\n⚠️ {hata} hesaplanamadı" if hata else ""))
    # ═══════════════════════════════════════════════
# MESAJ YARDIMCILARI
# ═══════════════════════════════════════════════
def safe_send(chat_id, text, parse_mode='Markdown'):
    try:
        bot.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception:
        clean = text.replace('*','').replace('_','').replace('`','').replace('[','').replace(']','')
        try: bot.send_message(chat_id, clean)
        except Exception as e: print(f"safe_send hata: {e}")

def send_long_message(chat_id, text):
    if len(text) <= 4000:
        bot.send_message(chat_id, text, parse_mode='Markdown'); return
    parts=[]; current=""
    for block in text.split('\n\n'):
        if len(current)+len(block)+2>4000:
            parts.append(current.strip()); current=block
        else: current+=block+'\n\n'
    if current: parts.append(current.strip())
    for part in parts:
        bot.send_message(chat_id, part, parse_mode='Markdown')
        time.sleep(0.5)

# ═══════════════════════════════════════════════
# /check KOMUTU — Yeni: Gap, MTF Skor, Risk, Sadece AL default
# ═══════════════════════════════════════════════
def scan_all_stocks(chat_id, limit=None, ticker_list=None, show_sells=False):
    chat_id = str(chat_id)
    if ticker_list:
        tickers = ticker_list
        show_sells = True  # Tek hisse sorgusu ise her şeyi göster
    else:
        tickers = wl_get(chat_id)
        if not tickers:
            bot.send_message(chat_id, "📭 Liste boş! /addall yaz."); return
        if limit and limit < len(tickers):
            import random
            tickers = random.sample(tickers, limit)

    total = len(tickers)
    _cached_set = _get_cached_tickers_today()
    cached = [t for t in tickers if t in _cached_set]
    missing = [t for t in tickers if t not in _cached_set]

    bot.send_message(chat_id,
        f"🔍 *{total} hisse taranacak*\n"
        f"💾 Cache: {len(cached)} | 📡 İndir: {len(missing)}\n"
        f"{'⏱ ~'+str(int(len(missing)*TD_DELAY//60))+' dk' if missing else '⚡ Anında!'}\n"
        f"🚫 İptal: /iptal check")

    for i, ticker in enumerate(missing):
        if is_cancelled(chat_id, "check"):
            bot.send_message(chat_id, "İndirme iptal edildi."); return
        get_data(ticker)
        if (i+1)%20==0:
            bot.send_message(chat_id, f"📥 {i+1}/{len(missing)}")
        time.sleep(TD_DELAY)

    bot.send_message(chat_id, "✅ Veri hazır, analiz başlıyor...")

    messages = []; no_data = 0

    for ticker in tickers:
        if is_cancelled(chat_id, "check"):
            bot.send_message(chat_id, f"🚫 İptal. ({len(messages)} sinyal)")
            if messages:
                for msg in messages:
                    safe_send(chat_id, msg); time.sleep(0.3)
            return
        try:
            df_d, df_w = get_data(ticker)
            has_daily = isinstance(df_d, pd.DataFrame) and not df_d.empty and len(df_d)>=10
            has_weekly = isinstance(df_w, pd.DataFrame) and not df_w.empty and len(df_w)>=10
            if not has_daily and not has_weekly:
                no_data += 1; continue

            ep = ema_get(ticker); ep_d = ep["daily"]; ep_w = ep["weekly"]
            signals = []; rsi_lines = []

            # ── GÜNLÜK ──
            if has_daily:
                d = df_d.copy()
                rsi_series = calc_rsi(d["Close"], 14).dropna()
                rsi_d = float(rsi_series.iloc[-1]) if len(rsi_series)>0 else None

                if rsi_d is not None:
                    if rsi_d >= 70: rsi_lines.append(f"RSI-G: {rsi_d:.1f} ⚠️ AŞIRI ALIM")
                    elif rsi_d <= 30: rsi_lines.append(f"RSI-G: {rsi_d:.1f} 💡 AŞIRI SATIM")
                    else: rsi_lines.append(f"RSI-G: {rsi_d:.1f}")

                ema_s = calc_ema(d["Close"], ep_d[0])
                ema_l = calc_ema(d["Close"], ep_d[1])
                ef = pd.DataFrame({"s":ema_s,"l":ema_l}).dropna()

                if len(ef) >= 2:
                    cross_up = ef["s"].iloc[-2]<=ef["l"].iloc[-2] and ef["s"].iloc[-1]>ef["l"].iloc[-1]
                    cross_down = ef["s"].iloc[-2]>=ef["l"].iloc[-2] and ef["s"].iloc[-1]<ef["l"].iloc[-1]
                    trend_up = ef["s"].iloc[-1] > ef["l"].iloc[-1]
                    trend_down = ef["s"].iloc[-1] < ef["l"].iloc[-1]

                    if cross_up:
                        signals.append(f"🟢↑ Günlük AL — EMA Taze Kesişim ({ep_d[0]}/{ep_d[1]})")
                    elif cross_down:
                        signals.append(f"🔴↓ Günlük SAT — EMA Taze Kesişim ({ep_d[0]}/{ep_d[1]})")
                    elif trend_up and rsi_d and 40<=rsi_d<=65:
                        signals.append(f"📈 Günlük YUKARI TREND — EMA({ep_d[0]}>{ep_d[1]}) RSI Uyumlu")
                    elif trend_down and rsi_d and 35<=rsi_d<=60:
                        signals.append(f"📉 Günlük AŞAĞI TREND — EMA({ep_d[0]}<{ep_d[1]})")

                d["RSI"] = calc_rsi(d["Close"], 14)
                dt, dm = detect_divergence(d)
                if dt: signals.append(dm)

            # ── HAFTALIK ──
            if has_weekly:
                w = df_w.copy()
                rsi_series_w = calc_rsi(w["Close"], 14).dropna()
                rsi_w = float(rsi_series_w.iloc[-1]) if len(rsi_series_w)>0 else None

                ema_sw = calc_ema(w["Close"], ep_w[0])
                ema_lw = calc_ema(w["Close"], ep_w[1])
                wf = pd.DataFrame({"s":ema_sw,"l":ema_lw}).dropna()
                trend_str = "YUKARI" if (len(wf)>0 and wf["s"].iloc[-1]>wf["l"].iloc[-1]) else "ASAGI"

                if rsi_w is not None:
                    if rsi_w >= 70: rsi_lines.append(f"RSI-H: {rsi_w:.1f} ⚠️ AŞIRI ALIM | Trend:{trend_str}")
                    elif rsi_w <= 30: rsi_lines.append(f"RSI-H: {rsi_w:.1f} 💡 AŞIRI SATIM | Trend:{trend_str}")
                    else: rsi_lines.append(f"RSI-H: {rsi_w:.1f} | Trend:{trend_str}")

                if len(wf) >= 2:
                    cross_up_w = wf["s"].iloc[-2]<=wf["l"].iloc[-2] and wf["s"].iloc[-1]>wf["l"].iloc[-1]
                    cross_down_w = wf["s"].iloc[-2]>=wf["l"].iloc[-2] and wf["s"].iloc[-1]<wf["l"].iloc[-1]
                    trend_up_w = wf["s"].iloc[-1] > wf["l"].iloc[-1]
                    trend_down_w = wf["s"].iloc[-1] < wf["l"].iloc[-1]

                    if cross_up_w:
                        signals.append(f"🟢🟢↑↑ Haftalık AL — Taze Kesişim GÜÇLÜ ({ep_w[0]}/{ep_w[1]})")
                    elif cross_down_w:
                        signals.append(f"🔴🔴↓↓ Haftalık SAT — Taze Kesişim GÜÇLÜ ({ep_w[0]}/{ep_w[1]})")
                    elif trend_up_w and rsi_w and 40<=rsi_w<=65:
                        signals.append(f"📈📈 Haftalık YUKARI TREND (GÜÇLÜ)")
                    elif trend_down_w and rsi_w and 35<=rsi_w<=60:
                        signals.append(f"📉📉 Haftalık AŞAĞI TREND (GÜÇLÜ)")

                w["RSI"] = calc_rsi(w["Close"], 14)
                dt_w, dm_w = detect_divergence(w, window=80, min_bars=3, max_bars=60)
                if dt_w: signals.append(f"[HAFTALIK] {dm_w}")

            # ── HACİM & MOMENTUM ──
            vm = calc_volume_momentum(df_d) if has_daily else {}

            # ── GÖSTER KOŞULU ──
            rsi_extreme = any("AŞIRI" in r for r in rsi_lines)
            has_signal = bool(signals)
            force_show = ticker_list is not None

            # Sinyal tipini belirle
            buy_sigs = []; sell_sigs = []; other_sigs = []
            for sig in signals:
                is_buy = any(k in sig for k in ["AL","YUKARI","POZİTİF"])
                is_sell = any(k in sig for k in ["SAT","AŞAĞI","NEGATİF"])
                if is_buy: buy_sigs.append(sig)
                elif is_sell: sell_sigs.append(sig)
                else: other_sigs.append(sig)

            # Default: sadece AL sinyallerini göster (show_sells=False ise SAT'ları gizle)
            if not show_sells and not buy_sigs and not force_show and not rsi_extreme:
                continue

            show = has_signal or rsi_extreme or force_show
            if not show: continue

            today_str = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%d %b %Y')

            # ── MTF SKOR ──
            mtf_score, mtf_details = calc_mtf_score(df_d, df_w, ticker)
            score_lbl = score_to_label(mtf_score)

            # ── GAP ANALİZİ ──
            gap = calc_gap_analysis(df_d) if has_daily else None

            # ── RİSK YÖNETİMİ ──
            sig_type_main = "AL" if buy_sigs else "SAT"
            risk_mgmt = calc_risk_management(df_d, sig_type_main) if has_daily else None

            # ── SEKTÖR ──
            sektor = _TICKER_SEKTOR.get(ticker, "")
            sektor_str = f" | 🏭 {sektor}" if sektor else ""

            # ── MESAJ OLUŞTUR ──
            vol = df_d["Close"].pct_change().std()*100 if has_daily else 0
            vol_tag = " 🔥 Yüksek Vol" if vol>2 else ""
            header = f"{'🔥' if vol>2 else '📌'} *{ticker}*{vol_tag}{sektor_str} | {today_str}"

            parts_msg = [header, f"📊 MTF Skor: {mtf_score}/100 — {score_lbl}", ""]

            # Gap
            if gap:
                parts_msg.append(format_gap_text(gap))
                parts_msg.append("")

            # RSI
            if rsi_lines:
                parts_msg.append("📊 *ANALİZ*")
                for i_r, r in enumerate(rsi_lines):
                    prefix = "└" if i_r==len(rsi_lines)-1 else "├"
                    parts_msg.append(f"{prefix} {r}")
                parts_msg.append("")

            # AL sinyalleri + hacim onayı
            if buy_sigs:
                parts_msg.append("🟢 *AL SİNYALLERİ*")
                for sig in buy_sigs:
                    parts_msg.append(f"├ {sig}")
                    if vm and vm.get("confirm_buy"):
                        parts_msg.append("│  ✅ Hacim & Momentum Destekliyor")
                    elif vm:
                        warns = []
                        if vm.get("vol_ratio") is not None and vm["vol_ratio"]<0.8: warns.append("hacim düşük")
                        if vm.get("buy_pressure") is not None and vm["buy_pressure"]<0.5: warns.append("satım baskısı")
                        if vm.get("mom_fast") is not None and vm["mom_fast"]<0: warns.append(f"momentum negatif")
                        if vm.get("obv_trend")=="ASAGI": warns.append("OBV aşağı")
                        if warns: parts_msg.append(f"│  ⚠️ Zayıf: {', '.join(warns)}")
                        else: parts_msg.append("│  🔶 Hacim/Momentum nötr")
                parts_msg.append("")

            # SAT sinyalleri
            if sell_sigs and (show_sells or force_show):
                parts_msg.append("🔴 *SAT SİNYALLERİ*")
                for sig in sell_sigs:
                    parts_msg.append(f"├ {sig}")
                parts_msg.append("")

            # Diğer
            if other_sigs:
                parts_msg.append("🔎 *DİĞER*")
                for sig in other_sigs:
                    parts_msg.append(f"├ {sig}")
                parts_msg.append("")

            # Risk yönetimi (sadece AL sinyali varsa veya tek hisse sorgusu)
            if risk_mgmt and (buy_sigs or force_show):
                parts_msg.append(format_risk_text(risk_mgmt, sig_type_main))
                parts_msg.append("")

            parts_msg.append(f"📐 EMA → G:{ep_d[0]}-{ep_d[1]} | H:{ep_w[0]}-{ep_w[1]}")
            parts_msg.append(f"📈 [TradingView](https://tr.tradingview.com/chart/?symbol=BIST:{ticker})")

            final_msg = "\n".join(parts_msg)

            has_buy = bool(buy_sigs); has_sell = bool(sell_sigs)
            sig_type = "KARISIK" if (has_buy and has_sell) else ("AL" if has_buy else ("SAT" if has_sell else "DIGER"))

            try:
                today_key = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%Y-%m-%d')
                db_set(f"sinyal:{today_key}:{ticker}", {"msg":final_msg,"type":sig_type,"chat_id":chat_id})
            except Exception: pass

            messages.append(final_msg)
        except Exception as e:
            print(f"Scan hata {ticker}: {e}"); continue

    if messages:
        bot.send_message(chat_id, f"🔔 *{len(messages)} sinyal!*", parse_mode='Markdown')
        for msg in messages:
            if is_cancelled(chat_id, "check"): break
            safe_send(chat_id, msg); time.sleep(0.3)
        bot.send_message(chat_id, f"✅ {total} hisse tarandı, {len(messages)} sinyal.")
    else:
        bot.send_message(chat_id, f"✅ {total} hisse tarandı ({no_data} veri yok). Sinyal yok.")
    _invalidate_cache_set()

# ═══════════════════════════════════════════════
# TRADE KİTABI
# ═══════════════════════════════════════════════
def kitap_get(chat_id): return db_get(f"kitap:{chat_id}", [])
def kitap_set(chat_id, islemler): db_set(f"kitap:{chat_id}", islemler)
def kitap_acik_get(chat_id): return db_get(f"kitap_acik:{chat_id}", {})
def kitap_acik_set(chat_id, acik): db_set(f"kitap_acik:{chat_id}", acik)

@bot.message_handler(commands=['al'])
def cmd_kitap_al(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split(None, 4)
    if len(parts) < 4:
        bot.reply_to(message,
            "📗 TRADE KİTABI — Alış\n━━━━━━━━━━━━━━━━━━━\n"
            "Kullanım: /al THYAO 145.50 100 sebebi\n\n"
            "Örnek: /al THYAO 145.50 100 SMA200 üstüne geçti"); return
    try:
        ticker = parts[1].upper().replace(".IS","")
        fiyat = float(parts[2].replace(",","."))
        adet = float(parts[3].replace(",","."))
        neden = parts[4].strip() if len(parts)>4 else ""
    except (ValueError, IndexError):
        bot.reply_to(message, "❌ Format hatalı.\nÖrnek: /al THYAO 145.50 100"); return
    if fiyat<=0 or adet<=0:
        bot.reply_to(message, "❌ Fiyat ve adet sıfırdan büyük olmalı."); return

    tr_tz = pytz.timezone("Europe/Istanbul")
    tarih = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    toplam = fiyat * adet

    acik = kitap_acik_get(chat_id)
    if ticker in acik:
        m = acik[ticker]
        eski_adet = float(m["adet"]); eski_fiyat = float(m["fiyat"])
        yeni_adet = eski_adet + adet
        yeni_fiyat = (eski_adet*eski_fiyat + adet*fiyat) / yeni_adet
        acik[ticker]["adet"] = round(yeni_adet,2)
        acik[ticker]["fiyat"] = round(yeni_fiyat,4)
        acik[ticker]["neden"] = neden or acik[ticker].get("neden","")
    else:
        acik[ticker] = {"fiyat":round(fiyat,4),"adet":round(adet,2),"tarih":tarih,"neden":neden}
    kitap_acik_set(chat_id, acik)

    islemler = kitap_get(chat_id)
    islem_id = len(islemler) + 1
    islemler.append({"id":islem_id,"yon":"AL","ticker":ticker,"fiyat":round(fiyat,4),
                     "adet":round(adet,2),"tarih":tarih,"neden":neden,"ai_yorum":""})
    kitap_set(chat_id, islemler)

    acik_bilgi = ""
    if ticker in acik and float(acik[ticker]["adet"]) > adet:
        acik_bilgi = f"\nOrt. maliyet: {acik[ticker]['fiyat']:.2f}₺ | Toplam: {acik[ticker]['adet']:.0f} adet"

    bot.reply_to(message,
        f"📗 *{ticker}* alış kaydedildi (#{islem_id})\n━━━━━━━━━━━━━━━━━━━\n"
        f"💰 {adet:.0f} adet × {fiyat:.2f}₺ = {toplam:,.0f}₺\n📅 {tarih}\n"
        f"📝 Sebep: {neden if neden else '(belirtilmedi)'}{acik_bilgi}\n\n"
        f"Satış için: /sat {ticker} FIYAT ADET sebep")

@bot.message_handler(commands=['sat'])
def cmd_kitap_sat(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split(None, 4)
    if len(parts) < 4:
        bot.reply_to(message,
            "📕 TRADE KİTABI — Satış\n━━━━━━━━━━━━━━━━━━━\n"
            "Kullanım: /sat THYAO 162.00 100 sebebi"); return
    try:
        ticker = parts[1].upper().replace(".IS","")
        fiyat = float(parts[2].replace(",","."))
        adet = float(parts[3].replace(",","."))
        neden = parts[4].strip() if len(parts)>4 else ""
    except (ValueError, IndexError):
        bot.reply_to(message, "❌ Format hatalı."); return
    if fiyat<=0 or adet<=0:
        bot.reply_to(message, "❌ Fiyat ve adet sıfırdan büyük olmalı."); return

    tr_tz = pytz.timezone("Europe/Istanbul")
    tarih = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    acik = kitap_acik_get(chat_id)

    if ticker in acik:
        alis_fiyat = float(acik[ticker]["fiyat"])
        alis_tarih = acik[ticker].get("tarih","?")
        alis_neden = acik[ticker].get("neden","")
        sure_str = "?"
        try:
            alis_dt = datetime.strptime(alis_tarih, "%d.%m.%Y %H:%M")
            sat_dt = datetime.strptime(tarih, "%d.%m.%Y %H:%M")
            sure_str = f"{(sat_dt-alis_dt).days} gün"
        except Exception: pass
        kalan_adet = float(acik[ticker]["adet"]) - adet
        if kalan_adet <= 0.001: del acik[ticker]
        else: acik[ticker]["adet"] = round(kalan_adet,2)
        kitap_acik_set(chat_id, acik)
    else:
        alis_fiyat = None; alis_tarih = "?"; alis_neden = ""; sure_str = "?"

    if alis_fiyat is not None and alis_fiyat > 0:
        kaz_tl = (fiyat-alis_fiyat)*adet
        kaz_pct = (fiyat-alis_fiyat)/alis_fiyat*100
    else:
        kaz_tl = None; kaz_pct = None

    kaz_str = ""
    if kaz_tl is not None:
        emoji_kaz = "🟢" if kaz_tl>=0 else "🔴"
        kaz_str = f"\n{emoji_kaz} KÂR/ZARAR: {'+' if kaz_tl>=0 else ''}{kaz_tl:,.0f}₺ ({'+' if kaz_pct>=0 else ''}{kaz_pct:.1f}%)\n📅 Süre: {sure_str}"

    islemler = kitap_get(chat_id)
    islem_id = len(islemler) + 1
    islemler.append({"id":islem_id,"yon":"SAT","ticker":ticker,"fiyat":round(fiyat,4),
                     "adet":round(adet,2),"tarih":tarih,"neden":neden,
                     "alis_fiyat":round(alis_fiyat,4) if alis_fiyat else None,
                     "kaz_tl":round(kaz_tl,2) if kaz_tl is not None else None,
                     "kaz_pct":round(kaz_pct,2) if kaz_pct is not None else None,
                     "sure":sure_str,"ai_yorum":""})
    kitap_set(chat_id, islemler)

    bot.reply_to(message,
        f"📕 *{ticker}* satış kaydedildi (#{islem_id})\n━━━━━━━━━━━━━━━━━━━\n"
        f"💰 {adet:.0f} adet × {fiyat:.2f}₺ = {adet*fiyat:,.0f}₺\n📅 {tarih}\n"
        f"📝 Sebep: {neden if neden else '(belirtilmedi)'}{kaz_str}\n\n🤖 AI yorumu hazırlanıyor...")

    def _ai_yorum():
        try:
            alis_bilgi = f"Alış: {alis_fiyat:.2f}₺ ({alis_tarih})\nSebep: {alis_neden or 'belirtilmemiş'}\n" if alis_fiyat else "Alış kaydı yok.\n"
            kaz_bilgi = f"Kâr/Zarar: {'+' if kaz_tl>=0 else ''}{kaz_tl:,.0f}₺ ({'+' if kaz_pct>=0 else ''}{kaz_pct:.1f}%) — {sure_str}" if kaz_tl is not None else "Hesaplanamadı"
            prompt = f"""Deneyimli BIST teknik analisti ve trade koçu olarak işlemi değerlendir.
Hisse: {ticker}
{alis_bilgi}Satış: {fiyat:.2f}₺ ({tarih})
Sebep: {neden or 'belirtilmemiş'}
{kaz_bilgi}

Türkçe, kısa:
📊 DEĞERLENDİRME: (2 cümle)
✅ DOĞRU: (en güçlü karar)
⚠️ GELİŞTİR: (ne farklı yapılabilir)
🧠 DERS: (tek cümle)"""
            yorum = gemini_ask(prompt, max_tokens=350)
            if not yorum or yorum.startswith("⚠️"): yorum = groq_ask(prompt, max_tokens=350)
            islemler2 = kitap_get(chat_id)
            for isl in islemler2:
                if isl["id"]==islem_id: isl["ai_yorum"]=yorum; break
            kitap_set(chat_id, islemler2)
            safe_send(chat_id, f"🤖 *{ticker} Trade Analizi* (#{islem_id})\n━━━━━━━━━━━━━━━━━━━\n{yorum}")
        except Exception as e:
            debug_log("WARN","kitap_ai",str(e)[:100])
    threading.Thread(target=_ai_yorum, daemon=True).start()

@bot.message_handler(commands=['kitap'])
def cmd_kitap(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("kitap", chat_id, parts[1] if len(parts)>1 else "liste")
    islemler = kitap_get(chat_id)

    if len(parts)>1 and parts[1].lower()=="ozet":
        if not islemler: bot.reply_to(message, "📒 Henüz işlem yok.\n/al ve /sat ile kaydet."); return
        satislar = [i for i in islemler if i["yon"]=="SAT" and i.get("kaz_pct") is not None]
        alislar = [i for i in islemler if i["yon"]=="AL"]
        karlar = [i for i in satislar if (i.get("kaz_tl") or 0)>=0]
        zararlar = [i for i in satislar if (i.get("kaz_tl") or 0)<0]
        toplam_kaz = sum(i.get("kaz_tl") or 0 for i in satislar)
        ort_pct = (sum(i.get("kaz_pct") or 0 for i in satislar)/len(satislar)) if satislar else 0
        acik = kitap_acik_get(chat_id)
        satirlar = ["📒 TRADE KİTABI — ÖZET","━━━━━━━━━━━━━━━━━━━",
            f"📋 Toplam: {len(islemler)} ({len(alislar)} alış, {len(satislar)} satış)",
            f"🟢 Kârlı: {len(karlar)}", f"🔴 Zararlı: {len(zararlar)}",
            f"📊 Başarı: {len(karlar)/len(satislar)*100:.0f}%" if satislar else "📊 -",
            f"💰 Net: {'+' if toplam_kaz>=0 else ''}{toplam_kaz:,.0f}₺",
            f"📈 Ort: {'+' if ort_pct>=0 else ''}{ort_pct:.1f}%"]
        if acik: satirlar.append(f"\n📂 Açık: {', '.join(acik.keys())}")
        bot.reply_to(message, "\n".join(satirlar)); return

    if len(parts)>1 and parts[1].lower()=="acik":
        acik = kitap_acik_get(chat_id)
        if not acik: bot.reply_to(message, "📂 Açık pozisyon yok."); return
        satirlar = ["📂 AÇIK POZİSYONLAR","━━━━━━━━━━━━━━━━━━━"]
        for tick, poz in acik.items():
            satirlar.append(f"📌 *{tick}*\n   {poz['adet']:.0f} × {poz['fiyat']:.2f}₺ = {float(poz['adet'])*float(poz['fiyat']):,.0f}₺\n   {poz.get('tarih','?')}")
        safe_send(chat_id, "\n".join(satirlar)); return

    if len(parts)>1 and parts[1].lower() not in ("ozet","acik"):
        ticker = parts[1].upper().replace(".IS","")
        filtre = [i for i in islemler if i["ticker"]==ticker]
        if not filtre: bot.reply_to(message, f"📒 {ticker} için kayıt yok."); return
        satirlar = [f"📒 {ticker} — {len(filtre)} işlem","━━━━━━━━━━━━━━━━━━━"]
        for isl in filtre[-15:]:
            yon_e = "📗" if isl["yon"]=="AL" else "📕"
            kaz = ""
            if isl["yon"]=="SAT" and isl.get("kaz_pct") is not None:
                kaz = f" → {'+' if isl['kaz_pct']>=0 else ''}{isl['kaz_pct']:.1f}%"
            satirlar.append(f"{yon_e} #{isl['id']} {isl['yon']} {isl['fiyat']:.2f}₺ × {isl['adet']:.0f}{kaz}\n   {isl['tarih']}")
        safe_send(chat_id, "\n".join(satirlar)); return

    if not islemler:
        bot.reply_to(message, "📒 Boş.\n/al THYAO 145.50 100 sebep\n/sat THYAO 162.00 100 sebep\n/kitap ozet | /kitap acik"); return
    son20 = islemler[-20:]
    satirlar = [f"📒 Son {len(son20)} İşlem","━━━━━━━━━━━━━━━━━━━"]
    for isl in reversed(son20):
        yon_e = "📗" if isl["yon"]=="AL" else "📕"
        kaz = ""
        if isl["yon"]=="SAT" and isl.get("kaz_pct") is not None:
            kaz = f"  {'+' if isl['kaz_pct']>=0 else ''}{isl['kaz_pct']:.1f}%"
        satirlar.append(f"{yon_e} #{isl['id']} *{isl['ticker']}* {isl['yon']} {isl['fiyat']:.2f}₺ × {isl['adet']:.0f}{kaz}\n   {isl['tarih']}")
    safe_send(chat_id, "\n".join(satirlar))

# ═══════════════════════════════════════════════
# KAR ZARAR HESAPLAYICI
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['karzarar'])
def cmd_karzarar(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("karzarar", chat_id)

    if len(parts) < 2:
        bot.reply_to(message,
            "💰 *KAR/ZARAR HESAPLAYICI*\n━━━━━━━━━━━━━━━━━━━\n\n"
            "*1) Trade Hesabı:*\n"
            "/karzarar THYAO 145.50 162.00 100\n"
            "  Ticker AlısFiyatı SatışFiyatı Adet\n\n"
            "*2) Risk Hesabı:*\n"
            "/karzarar risk 5000 5\n"
            "  risk Portföy RiskOrani%\n\n"
            "*3) Açık Pozisyon:*\n"
            "/karzarar acik THYAO 162.00\n"
            "  acik Ticker GuncelFiyat\n\n"
            "*4) Pozisyon Boyutu:*\n"
            "/karzarar pozisyon 10000 2 145.50 140.00\n"
            "  pozisyon Portföy Risk% GirisFiyati StopLoss",
            parse_mode='Markdown')
        return

    mod = parts[1].lower()

    # MOD 1: Trade Hesabı — /karzarar THYAO 145.50 162.00 100
    if mod not in ("risk", "acik", "pozisyon"):
        try:
            ticker = parts[1].upper().replace(".IS","")
            alis   = float(parts[2].replace(",","."))
            satis  = float(parts[3].replace(",","."))
            adet   = float(parts[4].replace(",","."))
        except (IndexError, ValueError):
            bot.reply_to(message, "❌ Format hatali!\nOrnek: /karzarar THYAO 145.50 162.00 100"); return

        if alis <= 0 or satis <= 0 or adet <= 0:
            bot.reply_to(message, "❌ Degerler sifirdan buyuk olmali."); return

        alis_top   = alis * adet
        satis_top  = satis * adet
        kaz_tl     = satis_top - alis_top
        kaz_pct    = (satis - alis) / alis * 100
        komisyon   = alis_top * 0.001 + satis_top * 0.001
        net_kaz    = kaz_tl - komisyon
        emoji      = "🟢" if kaz_tl >= 0 else "🔴"
        yon        = "KAR" if kaz_tl >= 0 else "ZARAR"

        bot.reply_to(message,
            f"💰 *{ticker} — Trade Hesabi*\n━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Alis:  {adet:.0f} x {alis:.2f}TL = *{alis_top:,.0f}TL*\n"
            f"📤 Satis: {adet:.0f} x {satis:.2f}TL = *{satis_top:,.0f}TL*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji} *{yon}: {'+' if kaz_tl>=0 else ''}{kaz_tl:,.0f}TL "
            f"({'+' if kaz_pct>=0 else ''}{kaz_pct:.2f}%)*\n"
            f"💸 Tahmini komisyon: ~{komisyon:,.0f}TL\n"
            f"💎 *Net {yon}: {'+' if net_kaz>=0 else ''}{net_kaz:,.0f}TL*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Alis degeri:  {alis_top:,.0f}TL\n"
            f"📊 Satis degeri: {satis_top:,.0f}TL\n"
            f"📈 Getiri: %{kaz_pct:.2f}",
            parse_mode='Markdown')
        return

    # MOD 2: Risk Hesabı — /karzarar risk 5000 5
    if mod == "risk":
        try:
            portfolio = float(parts[2].replace(",","."))
            risk_pct  = float(parts[3].replace(",","."))
        except (IndexError, ValueError):
            bot.reply_to(message, "❌ Format hatali!\nOrnek: /karzarar risk 5000 5"); return

        if portfolio <= 0 or risk_pct <= 0:
            bot.reply_to(message, "❌ Degerler sifirdan buyuk olmali."); return

        max_risk    = portfolio * risk_pct / 100
        cok_agresif = portfolio * 0.10
        agresif     = portfolio * 0.05
        orta        = portfolio * 0.02
        muhafazakar = portfolio * 0.01

        bot.reply_to(message,
            f"📊 *Risk Hesabi*\n━━━━━━━━━━━━━━━━━━━\n"
            f"💼 Portfoy: *{portfolio:,.0f}TL*\n"
            f"⚠️ Risk Orani: *%{risk_pct}*\n"
            f"🔴 *Max Kayip: {max_risk:,.0f}TL*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📉 *Risk Seviyeleri:*\n"
            f"  🟥 Cok Agresif (%10): {cok_agresif:,.0f}TL\n"
            f"  🟧 Agresif (%5):      {agresif:,.0f}TL\n"
            f"  🟨 Orta (%2):         {orta:,.0f}TL\n"
            f"  🟩 Muhafazakar (%1):  {muhafazakar:,.0f}TL\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Altin Kural: Tek islemde portfoyun\n"
            f"   max %2-5 riske at.\n"
            f"   Secimin: %{risk_pct} = {max_risk:,.0f}TL\n\n"
            f"📌 Pozisyon boyutu icin:\n"
            f"/karzarar pozisyon {portfolio:.0f} {risk_pct} GirisFiyati StopLoss",
            parse_mode='Markdown')
        return

    # MOD 3: Açık Pozisyon — /karzarar acik THYAO 162.00
    if mod == "acik":
        try:
            ticker = parts[2].upper().replace(".IS","")
            guncel = float(parts[3].replace(",","."))
        except (IndexError, ValueError):
            bot.reply_to(message, "❌ Format hatali!\nOrnek: /karzarar acik THYAO 162.00"); return

        acik = kitap_acik_get(chat_id)
        if ticker not in acik:
            bot.reply_to(message,
                f"❌ *{ticker}* acik pozisyon kaydi yok.\n"
                f"/al {ticker} FIYAT ADET ile kaydet.",
                parse_mode='Markdown'); return

        poz       = acik[ticker]
        alis      = float(poz["fiyat"])
        adet      = float(poz["adet"])
        alis_top  = alis * adet
        gun_top   = guncel * adet
        kaz_tl    = gun_top - alis_top
        kaz_pct   = (guncel - alis) / alis * 100
        emoji     = "🟢" if kaz_tl >= 0 else "🔴"
        yon       = "KAR" if kaz_tl >= 0 else "ZARAR"
        hedef1    = alis * 1.10
        hedef2    = alis * 1.20
        stop      = alis * 0.97

        bot.reply_to(message,
            f"📂 *{ticker} — Acik Pozisyon*\n━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Alis:   {adet:.0f} x {alis:.2f}TL = {alis_top:,.0f}TL\n"
            f"💹 Guncel: {adet:.0f} x {guncel:.2f}TL = {gun_top:,.0f}TL\n"
            f"📅 Tarih:  {poz.get('tarih','?')}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji} *Anlik {yon}: {'+' if kaz_tl>=0 else ''}{kaz_tl:,.0f}TL "
            f"({'+' if kaz_pct>=0 else ''}{kaz_pct:.2f}%)*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 *Hedefler:*\n"
            f"  +%10 => {hedef1:.2f}TL ({(hedef1-guncel)/guncel*100:+.1f}% uzakta)\n"
            f"  +%20 => {hedef2:.2f}TL ({(hedef2-guncel)/guncel*100:+.1f}% uzakta)\n"
            f"🛑 *Stop Loss:*\n"
            f"  -%3  => {stop:.2f}TL ({(stop-guncel)/guncel*100:+.1f}% uzakta)",
            parse_mode='Markdown')
        return

    # MOD 4: Pozisyon Boyutu — /karzarar pozisyon 10000 2 145.50 140.00
    if mod == "pozisyon":
        try:
            portfolio = float(parts[2].replace(",","."))
            risk_pct  = float(parts[3].replace(",","."))
            giris     = float(parts[4].replace(",","."))
            stop      = float(parts[5].replace(",","."))
        except (IndexError, ValueError):
            bot.reply_to(message,
                "❌ Format hatali!\n"
                "Ornek: /karzarar pozisyon 10000 2 145.50 140.00"); return

        if giris <= stop:
            bot.reply_to(message, "❌ Giris fiyati stop loss'tan yuksek olmali!"); return
        if portfolio <= 0 or risk_pct <= 0:
            bot.reply_to(message, "❌ Degerler sifirdan buyuk olmali."); return

        max_risk   = portfolio * risk_pct / 100
        risk_per   = giris - stop
        risk_pct_p = risk_per / giris * 100
        adet       = max_risk / risk_per
        pozisyon   = adet * giris
        hedef1     = giris + risk_per * 2
        hedef2     = giris + risk_per * 3
        kar1       = (hedef1 - giris) * adet
        kar2       = (hedef2 - giris) * adet

        bot.reply_to(message,
            f"📐 *Pozisyon Boyutu Hesabi*\n━━━━━━━━━━━━━━━━━━━\n"
            f"💼 Portfoy:    *{portfolio:,.0f}TL*\n"
            f"⚠️ Risk Orani: *%{risk_pct}*\n"
            f"🔴 Max Kayip:  *{max_risk:,.0f}TL*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Giris: {giris:.2f}TL\n"
            f"🛑 Stop:  {stop:.2f}TL (-%{risk_pct_p:.1f})\n"
            f"📊 Hisse basi risk: {risk_per:.2f}TL\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Ideal Pozisyon:*\n"
            f"  📦 Adet:    *{adet:.0f} hisse*\n"
            f"  💰 Deger:   *{pozisyon:,.0f}TL*\n"
            f"  📊 Portfoy: *%{pozisyon/portfolio*100:.1f}*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 *Hedefler (Risk/Odul):*\n"
            f"  1:2 => {hedef1:.2f}TL = +{kar1:,.0f}TL\n"
            f"  1:3 => {hedef2:.2f}TL = +{kar2:,.0f}TL",
            parse_mode='Markdown')
        return

# ═══════════════════════════════════════════════
# AI MODÜLÜ — Gemini + Groq
# ═══════════════════════════════════════════════
RSS_FEEDS = {
    "global": [
        ("Reuters","https://feeds.reuters.com/reuters/businessNews"),
        ("CNBC","https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135"),
        ("MarketWatch","https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ],
    "bist": [
        ("Google Borsa","https://news.google.com/rss/search?q=borsa+istanbul+BIST+hisse&hl=tr&gl=TR&ceid=TR:tr"),
        ("Google Ekonomi","https://news.google.com/rss/search?q=türkiye+ekonomi+piyasa&hl=tr&gl=TR&ceid=TR:tr"),
        ("Google TCMB","https://news.google.com/rss/search?q=TCMB+merkez+bankası+faiz&hl=tr&gl=TR&ceid=TR:tr"),
        ("Bloomberg HT","https://www.bloomberght.com/rss"),
    ],
    "macro": [
        ("Google Fed","https://news.google.com/rss/search?q=fed+interest+rate+inflation&hl=en&gl=US&ceid=US:en"),
        ("Google Dolar","https://news.google.com/rss/search?q=dolar+türk+lirası+kur&hl=tr&gl=TR&ceid=TR:tr"),
    ],
    "kap": [
        ("KAP","https://www.kap.org.tr/tr/rss/bildirim"),
    ],
}

def fetch_rss(url, max_items=5, timeout=8):
    try:
        headers = {"User-Agent":"Mozilla/5.0 (compatible; BISTBot/1.0)"}
        resp = requests.get(url, timeout=timeout, headers=headers)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall(".//item")[:max_items]:
            title = item.findtext("title","").strip()
            desc = item.findtext("description","").strip()
            pub = item.findtext("pubDate","").strip()
            link = item.findtext("link","").strip()
            if title: items.append({"title":title,"desc":desc[:200],"pub":pub,"link":link})
        return items
    except Exception: return []

def collect_news(categories=None, max_per_feed=5, ticker=None):
    if categories is None: categories = ["global","bist","macro"]
    all_news = []
    for cat in categories:
        for name, url in RSS_FEEDS.get(cat, []):
            items = fetch_rss(url, max_items=max_per_feed)
            for item in items:
                item["source"] = name; item["category"] = cat
                item["priority"] = rank_news_priority(item)
                all_news.append(item)
    if ticker:
        filtered = [n for n in all_news if ticker.lower() in (n["title"]+n["desc"]).lower()]
        return filtered if filtered else []
    # Önem sırasına göre sırala
    prio_order = {"kritik":0,"yuksek":1,"orta":2,"dusuk":3}
    all_news.sort(key=lambda x: prio_order.get(x.get("priority","dusuk"),3))
    return all_news

def fetch_kap_news(ticker=None, max_items=5):
    """KAP RSS'den bildirim çek. ticker varsa filtrele."""
    try:
        items = fetch_rss("https://www.kap.org.tr/tr/rss/bildirim", max_items=20)
        for item in items:
            item["source"] = "KAP"; item["category"] = "kap"
            item["priority"] = "yuksek"
        if ticker:
            filtered = [n for n in items if ticker.upper() in n.get("title","").upper()]
            return filtered[:max_items]
        return items[:max_items]
    except Exception: return []

def news_to_text(news_list, max_items=10):
    lines = []
    for i, n in enumerate(news_list[:max_items]):
        lines.append(f"{i+1}. [{n['source']}] {n['title']}")
        if n.get("desc"): lines.append(f"   {n['desc'][:120]}...")
    return "\n".join(lines) if lines else "Haber bulunamadı."

# ── Gemini ──
_ai_usage = {
    "gemini_today":0,"gemini_date":"","gemini_last_model":"henuz yok",
    "groq_today":0,"groq_date":"","groq_remaining_req":"?","groq_remaining_tokens":"?",
    "groq_reset_req":"?","groq_reset_tokens":"?",
    "gemini_last_error":None,"gemini_quota_date":"","gemini_active_key":1,
    "groq_active_key":1,"groq_last_error":None,
}

def _ai_count(service):
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    if _ai_usage[f"{service}_date"] != today:
        _ai_usage[f"{service}_today"] = 0; _ai_usage[f"{service}_date"] = today
    _ai_usage[f"{service}_today"] += 1

GEMINI_MODELS = ["gemini-2.0-flash-lite","gemini-2.0-flash","gemini-2.0-flash-001"]

def _gemini_keys():
    keys = []
    if GEMINI_KEY:  keys.append((1,GEMINI_KEY))
    if GEMINI_KEY2: keys.append((2,GEMINI_KEY2))
    if GEMINI_KEY3: keys.append((3,GEMINI_KEY3))
    if GEMINI_KEY4: keys.append((4,GEMINI_KEY4))
    if GEMINI_KEY5: keys.append((5,GEMINI_KEY5))
    return keys

def _gemini_keys_pro():
    if GEMINI_PRO_KEY: return [(0, GEMINI_PRO_KEY)]
    return _gemini_keys()

def _groq_keys():
    keys = []
    if GROQ_KEY:  keys.append((1,GROQ_KEY))
    if GROQ_KEY2: keys.append((2,GROQ_KEY2))
    if GROQ_KEY3: keys.append((3,GROQ_KEY3))
    if GROQ_KEY4: keys.append((4,GROQ_KEY4))
    if GROQ_KEY5: keys.append((5,GROQ_KEY5))
    return keys

def _gemini_key_exhausted(key_no):
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    try: return db_get(f"gemini_quota_exhausted_{key_no}") == today
    except: return False

def _gemini_mark_exhausted(key_no):
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    try: db_set(f"gemini_quota_exhausted_{key_no}", today)
    except: pass

def gemini_ask(prompt, max_tokens=600):
    keys = _gemini_keys()
    if not keys:
        if GROQ_KEY: return groq_ask(prompt, max_tokens)
        return "⚠️ GEMINI_KEY yok."
    payload = {"contents":[{"parts":[{"text":prompt}]}],
               "generationConfig":{"maxOutputTokens":max_tokens,"temperature":0.4}}
    for key_no, api_key in keys:
        if _gemini_key_exhausted(key_no): continue
        for model in GEMINI_MODELS:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
            try:
                resp = requests.post(url, json=payload, timeout=20)
                if resp.status_code==404: continue
                if resp.status_code==429: _gemini_mark_exhausted(key_no); break
                if resp.status_code==403: break
                resp.raise_for_status()
                _ai_count("gemini"); _ai_usage["gemini_last_model"]=f"Key{key_no}/{model}"
                _ai_usage["gemini_active_key"]=key_no; _ai_usage["gemini_last_error"]=None
                return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            except Exception: continue
    if GROQ_KEY:
        r = groq_ask(prompt, max_tokens)
        if not r.startswith("⚠️"): return "[Groq]\n"+r
        return r
    return "⚠️ Tüm AI servisleri yanıt vermedi."

def _groq_key_exhausted(key_no):
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    try: return db_get(f"groq_ratelimit_{key_no}") == today
    except: return False

def _groq_mark_exhausted(key_no):
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    try: db_set(f"groq_ratelimit_{key_no}", today)
    except: pass

def groq_ask(prompt, max_tokens=800, model="llama-3.1-8b-instant"):
    keys = _groq_keys()
    if not keys: return "⚠️ GROQ_KEY yok."
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {"model":model,"messages":[{"role":"user","content":prompt}],"max_tokens":max_tokens,"temperature":0.4}
    for key_no, api_key in keys:
        if _groq_key_exhausted(key_no): continue
        headers = {"Authorization":f"Bearer {api_key}","Content-Type":"application/json"}
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=20)
            if resp.status_code==429: _groq_mark_exhausted(key_no); continue
            if resp.status_code==401: continue
            resp.raise_for_status()
            h = resp.headers
            _ai_usage["groq_remaining_req"]=h.get("x-ratelimit-remaining-requests","?")
            _ai_usage["groq_remaining_tokens"]=h.get("x-ratelimit-remaining-tokens","?")
            _ai_usage["groq_active_key"]=key_no; _ai_count("groq"); _ai_usage["groq_last_error"]=None
            return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            _ai_usage["groq_last_error"]=str(e)[:100]; continue
    return "⚠️ Tüm Groq keyleri yanıt vermedi."

def groq_news_summary(news_text, context="genel piyasa"):
    prompt = f"""BIST ve global piyasa uzmanısın. Haberleri analiz et, BIST yatırımcısına net sinyal ver.
Konu: {context}
Haberler:
{news_text}

Türkçe, emojili:
📰 ÖZET: (1-2 gelişme)
📊 BIST ETKİSİ: 🟢 POZİTİF / 🔴 NEGATİF / ⚪ NÖTR — (neden)
⚡ SİNYAL: 🟢 AL / 🔴 SAT / ⏸ BEKLE
⚠️ RİSK: (varsa)
💡 TAKTİK: (1 cümle)"""
    return groq_ask(prompt, max_tokens=350)

def groq_ticker_news(ticker, news_text):
    prompt = f"""{ticker} hissesi haber analizi. Net AL/SAT/BEKLE önerisi ver.
Haberler:
{news_text}

Türkçe, kısa:
📰 HABER: (kritik gelişme)
📊 ETKİ: 🟢/🔴/⚪ — (neden)
⚡ ÖNERİ: 🟢 AL / 🔴 SAT / ⏸ BEKLE
💡 NOT: (1 dikkat noktası)"""
    return groq_ask(prompt, max_tokens=280)

def groq_crisis_check(news_text):
    prompt = f"""Global haberleri incele. Çok kritik kriz/alarm var mı?
Haberler:
{news_text}

Kritik kriz varsa:
🚨 KRİZ ALARMI: (ne oldu)
📉 BIST ETKİSİ: (beklenti)
🔴 ÖNERİ: (ne yapmalı)

Önemli kriz yoksa sadece: YOK"""
    result = groq_ask(prompt, max_tokens=250)
    if result.strip().upper().startswith("YOK"): return None
    return result

# ═══════════════════════════════════════════════
# /haber — Profesyonel Format + KAP + Sektör Alarm
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['haber'])
def cmd_haber(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("haber", chat_id, parts[1] if len(parts)>1 else "genel")
    if not _groq_keys():
        bot.reply_to(message, "❌ GROQ_KEY yok."); return

    def _run_haber(ticker=None):
        try:
            now_str = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%d.%m.%Y %H:%M')

            if ticker:
                # ── Tek hisse ──
                bot.send_message(chat_id, f"📰 *{ticker}* haberleri çekiliyor...")

                # KAP önce
                kap_news = fetch_kap_news(ticker, max_items=3)
                all_news = collect_news(["bist","global"], max_per_feed=5, ticker=ticker)
                genel = False
                if not all_news and not kap_news:
                    all_news = collect_news(["bist"], max_per_feed=3)
                    genel = True

                msg_parts = [
                    f"📰 *{ticker} HABER DOSYASI*",
                    "━━━━━━━━━━━━━━━━━━━",
                    f"🕒 {now_str}",
                    f"📌 Sembol: {ticker}",
                    f"🧭 Kaynaklar: KAP + Haber", ""
                ]

                if kap_news:
                    msg_parts.append("📢 *KAP BİLDİRİMLERİ*")
                    msg_parts.append("━━━━━━━━━━━━━━━━━━━")
                    for i, n in enumerate(kap_news, 1):
                        msg_parts.append(format_news_card(i, n, "yuksek"))
                        msg_parts.append("")

                if all_news:
                    title = "Genel BIST" if genel else "İlgili"
                    msg_parts.append(f"📋 *{title} Haberler*")
                    msg_parts.append("━━━━━━━━━━━━━━━━━━━")
                    for i, n in enumerate(all_news[:4], 1):
                        msg_parts.append(format_news_card(i, n, n.get("priority","orta")))
                        msg_parts.append("")

                safe_send(chat_id, "\n".join(msg_parts))

                # AI yorumu
                time.sleep(0.5)
                combined = kap_news + all_news
                if combined:
                    try:
                        yorum = groq_ticker_news(ticker, news_to_text(combined, 6))
                        safe_send(chat_id,
                            f"🤖 *{ticker} — AI Yorumu*\n━━━━━━━━━━━━━━━━━━━\n{yorum}\n🕐 {now_str}")
                    except Exception:
                        safe_send(chat_id, f"⚠️ AI yorumu atlandı — haber akışı yukarıda.")
            else:
                # ── Genel haber bülteni ──
                bot.send_message(chat_id,
                    f"📰 GÜNLÜK PİYASA HABERLERİ\n━━━━━━━━━━━━━━━━━━━\n🕒 {now_str}\n🌍 Global + BIST + Makro\n\n📡 Çekiliyor...")

                # BIST
                bist_news = collect_news(["bist"], max_per_feed=4)
                if bist_news:
                    kritik_bist = [n for n in bist_news if n.get("priority") in ("kritik","yuksek")]
                    diger_bist = [n for n in bist_news if n.get("priority") not in ("kritik","yuksek")]

                    if kritik_bist:
                        safe_send(chat_id, format_news_block("🔥 ÖNEMLİ BIST HABERLERİ", kritik_bist, "🇹🇷"))
                    if diger_bist:
                        safe_send(chat_id, format_news_block("📊 BIST & Ekonomi", diger_bist[:3], "🇹🇷"))

                    time.sleep(0.5)
                    try:
                        bist_yorum = groq_news_summary(news_to_text(bist_news, 6), "BIST ve Türk piyasaları")
                        safe_send(chat_id, f"🤖 *BIST Yorumu*\n━━━━━━━━━━━━━━━━━━━\n{bist_yorum}")
                    except Exception:
                        pass  # Sessiz fallback
                    time.sleep(0.5)

                # Global
                global_news = collect_news(["global","macro"], max_per_feed=4)
                if global_news:
                    kritik_global = [n for n in global_news if n.get("priority") in ("kritik","yuksek")]
                    if kritik_global:
                        safe_send(chat_id, format_news_block("🔥 KRİTİK GLOBAL", kritik_global, "🌍"))
                    else:
                        safe_send(chat_id, format_news_block("🌍 Global Piyasa", global_news[:4], "🌍"))

                    time.sleep(0.5)
                    # Kriz kontrolü
                    try:
                        crisis = groq_crisis_check(news_to_text(global_news, 8))
                        if crisis:
                            safe_send(chat_id, f"🚨 *GLOBAL KRİZ ALARMI*\n━━━━━━━━━━━━━━━━━━━\n{crisis}")
                            # Sektör alarm
                            sectors = extract_sector_from_news(news_to_text(global_news, 5))
                            if sectors:
                                alarm_lines = ["🏭 *SEKTÖR ETKİSİ*","━━━━━━━━━━━━━━━━━━━"]
                                for sek in sectors:
                                    ticks = get_related_tickers(sek)
                                    if ticks:
                                        alarm_lines.append(f"⚠️ {sek}: {', '.join(ticks[:8])}")
                                safe_send(chat_id, "\n".join(alarm_lines))
                    except Exception:
                        pass  # Sessiz fallback

                    time.sleep(0.5)
                    try:
                        global_yorum = groq_news_summary(news_to_text(global_news, 6), "global piyasalar, Fed, dolar")
                        safe_send(chat_id, f"🤖 *Global Yorum*\n━━━━━━━━━━━━━━━━━━━\n{global_yorum}")
                    except Exception:
                        pass

                # Sinyal hisseleri haberleri
                today_key = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%Y-%m-%d')
                try:
                    conn = db_connect()
                    if conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT key FROM store WHERE key LIKE %s", (f"sinyal:{today_key}:%",))
                            rows = cur.fetchall()
                        db_release(conn)
                        signal_tickers = [r[0].split(":")[-1] for r in rows]
                        if signal_tickers:
                            bot.send_message(chat_id, f"🔍 *{len(signal_tickers)} sinyal hissesi haberleri...*", parse_mode='Markdown')
                            for t in signal_tickers[:6]:
                                t_news = collect_news(["bist"], max_per_feed=3, ticker=t)
                                t_kap = fetch_kap_news(t, 2)
                                combined = t_kap + t_news
                                if combined:
                                    try:
                                        yorum = groq_ticker_news(t, news_to_text(combined, 4))
                                        safe_send(chat_id, f"📰 *{t}*\n{yorum}")
                                    except Exception: pass
                                    time.sleep(0.8)
                except Exception: pass

        except Exception as e:
            bot.send_message(chat_id, f"❌ Haber hatası: {e}")
        # News Agent kanalına özet bildir
        try:
            now_s = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%d.%m.%Y %H:%M')
            agent_send(_news_bot, TOPIC_NEWS,
                f"📰 *Haber Raporu* — {now_s}\n"
                f"CEO tarafından tetiklendi — haberler yukarıda.")
        except Exception: pass

    ticker = parts[1].upper().replace(".IS","") if len(parts)>1 else None
    threading.Thread(target=_run_haber, args=(ticker,), daemon=True).start()

# ═══════════════════════════════════════════════
# /backtest KOMUTU
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['backtest'])
def cmd_backtest(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("backtest", chat_id, " ".join(parts[1:]) if len(parts)>1 else "")

    if len(parts) < 3:
        bot.reply_to(message,
            "📊 BACKTEST\n━━━━━━━━━━━━━━━━━━━\n"
            "Kullanım: /backtest STRATEJI HISSE\n\n"
            "Örnekler:\n"
            "/backtest 1 THYAO\n"
            "/backtest 4 EREGL\n"
            "/backtest A AKBNK\n\n"
            "Stratejiler: 1-14, A, B, C, 15"); return

    kod = parts[1]
    for k in TARA_STRATEJILER:
        if k.upper() == kod.upper(): kod = k; break
    if kod not in TARA_STRATEJILER:
        bot.reply_to(message, f"❌ Geçersiz strateji: {kod}"); return

    ticker = parts[2].upper().replace(".IS","")
    isim, _ = TARA_STRATEJILER[kod]
    bot.send_message(chat_id, f"📊 Backtest başlıyor...\n{isim} → {ticker}\n⏱ ~10-30 saniye")

    def _run():
        try:
            result = backtest_strategy(ticker, kod)
            if result is None:
                bot.send_message(chat_id, f"❌ {ticker} için yeterli veri yok."); return
            if result.get("msg"):
                bot.send_message(chat_id, f"📊 {ticker} — {isim}\n{result['msg']}"); return

            # Değerlendirme
            if result["win_rate"] >= 60 and result["profit_factor"] >= 1.5:
                verdict = "🟢 GÜÇLÜ STRATEJİ"
            elif result["win_rate"] >= 50 and result["profit_factor"] >= 1.0:
                verdict = "🟡 ORTA STRATEJİ"
            else:
                verdict = "🔴 ZAYIF STRATEJİ"

            lines = [
                f"📊 *BACKTEST SONUCU*",
                f"━━━━━━━━━━━━━━━━━━━",
                f"📌 {ticker} — {isim}",
                f"📅 Son ~1 yıl verisi",
                f"",
                f"📋 *İSTATİSTİKLER*",
                f"├ Toplam işlem: {result['trades']}",
                f"├ Kazanan: {result['wins']} | Kaybeden: {result['losses']}",
                f"├ Başarı oranı: %{result['win_rate']}",
                f"├ Ort. getiri: %{result['avg_return']}",
                f"├ Toplam getiri: %{result['total_return']}",
                f"├ En iyi: %{result['max_win']} | En kötü: %{result['max_loss']}",
                f"├ Max drawdown: %{result['max_drawdown']}",
                f"├ Profit factor: {result['profit_factor']}",
                f"└ Ort. pozisyon süresi: {result['avg_days']} gün",
                f"",
                f"🏆 *DEĞERLENDİRME: {verdict}*",
            ]
            safe_send(chat_id, "\n".join(lines))
        except Exception as e:
            bot.send_message(chat_id, f"❌ Backtest hatası: {str(e)[:120]}")

    threading.Thread(target=_run, daemon=True).start()

# ═══════════════════════════════════════════════
# /analiz /spade /sinyal /bulten
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['analiz'])
def cmd_analiz(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("analiz", chat_id, parts[1] if len(parts)>1 else "")
    if len(parts)<2: bot.reply_to(message, "Kullanım: /analiz THYAO"); return
    ticker = parts[1].upper().replace(".IS","")
    if not _gemini_keys(): bot.reply_to(message, "❌ GEMINI_KEY yok."); return
    bot.send_message(chat_id, f"🤖 *{ticker}* analiz ediliyor...", parse_mode='Markdown')

    def _run():
        try:
            df_d, df_w = get_data(ticker)
            if df_d is None or df_d.empty or len(df_d)<30:
                bot.send_message(chat_id, f"❌ {ticker} veri yok."); return
            ep = ema_get(ticker); ep_d=ep["daily"]; ep_w=ep["weekly"]
            close_price = float(df_d["Close"].iloc[-1])
            rsi_d = float(calc_rsi(df_d["Close"],14).iloc[-1]) if len(df_d)>14 else 50.0
            rsi_w = 50.0
            if df_w is not None and len(df_w)>14:
                rsi_w = float(calc_rsi(df_w["Close"],14).iloc[-1])

            # MTF skor
            mtf_score, _ = calc_mtf_score(df_d, df_w, ticker)
            score_lbl = score_to_label(mtf_score)

            # Gap
            gap = calc_gap_analysis(df_d)
            gap_str = f"\nGap: {gap['gap_type']} {gap['gap_pct']:+.1f}%" if gap else ""

            # Risk
            risk = calc_risk_management(df_d, "AL")
            risk_str = ""
            if risk:
                risk_str = f"\nStop: {risk['stop_loss']:.2f}₺ | Hedef1: {risk['hedef_1']:.2f}₺ | R/R: {risk['rr_1']}"

            signals = []
            ema_s = calc_ema(df_d["Close"], ep_d[0]); ema_l = calc_ema(df_d["Close"], ep_d[1])
            if ema_s is not None and ema_l is not None:
                if ema_s.iloc[-1] > ema_l.iloc[-1]:
                    signals.append(f"Günlük YUKARI TREND EMA({ep_d[0]}>{ep_d[1]})")
                else:
                    signals.append(f"Günlük AŞAĞI TREND EMA({ep_d[0]}<{ep_d[1]})")

            sig_text = "\n".join(signals) if signals else "Sinyal yok"
            prompt = f"""Profesyonel BIST teknik analisti olarak kısa ve net yorum yap.
Hisse: {ticker}
Fiyat: {close_price:.2f} TL
MTF Skor: {mtf_score}/100 ({score_lbl})
EMA G:{ep_d[0]}/{ep_d[1]} H:{ep_w[0]}/{ep_w[1]}
RSI G:{rsi_d:.1f} H:{rsi_w:.1f}{gap_str}{risk_str}
Sinyaller: {sig_text}

Format:
📊 YORUM: (2 cümle)
🎯 DESTEK: X.XX TL / X.XX TL
🚀 DİRENÇ: X.XX TL / X.XX TL
🧠 KARAKTERİ: (1 cümle)
⚡ ÖZET: AL / SAT / BEKLE + neden"""

            result = gemini_ask(prompt, max_tokens=400)
            msg = (f"🤖 *{ticker} — AI Analizi*\n━━━━━━━━━━━━━━━━━━━\n"
                   f"📊 MTF Skor: {mtf_score}/100 — {score_lbl}\n\n"
                   f"{result}\n━━━━━━━━━━━━━━━━━━━\n"
                   f"📈 [TradingView](https://tr.tradingview.com/chart/?symbol=BIST:{ticker})")
            safe_send(chat_id, msg)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Hata: {e}")
    threading.Thread(target=_run, daemon=True).start()

@bot.message_handler(commands=['spade'])
def cmd_spade_debug(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    if len(parts)<2: bot.reply_to(message, "Kullanım: /spade THYAO"); return
    ticker = parts[1].upper().replace(".IS","")
    bot.send_message(chat_id, f"♠️ {ticker} SpadeHunter analizi...")
    def _run():
        try:
            r = spade_indicators(ticker)
            if r is None:
                bot.send_message(chat_id, f"⚪ {ticker}: MasterBuy yok."); return
            onay = "✅" if r["tam_onay"] else ("⚡" if r["master_buy"] else "⚪")
            satirlar = [
                f"♠️ *{ticker} — SpadeHunter*","━━━━━━━━━━━━━━━━━━━",
                f"Fiyat: {r['price']:.2f}₺ Skor: {r['composite']:.0f} {onay}",
                f"Sinyal: {r['sinyaller']} Onay: {r['onay_count']}/8","",
                f"{'✅' if r['t1_weekly'] else '❌'} Haftalık | {'✅' if r['t2_fake'] else '❌'} FakeYok",
                f"{'✅' if r['t3_vol'] else '❌'} Hacim:{r['rvol']:.1f}x | {'✅' if r['t4_adx'] else '❌'} ADX:{r['adx']:.0f}",
                f"{'✅' if r['t5_cmf'] else '❌'} CMF:{r['cmf']:.3f} | {'✅' if r['t6_vwap'] else '❌'} VWAP",
                f"{'✅' if r['t7_macd'] else '❌'} MACD | {'✅' if r['t8_rsi'] else '❌'} RSI:{r['rsi']:.0f}",
            ]
            bot.send_message(chat_id, "\n".join(satirlar), parse_mode="Markdown")
        except Exception as e:
            bot.send_message(chat_id, f"❌ Hata: {str(e)[:150]}")
    threading.Thread(target=_run, daemon=True).start()

@bot.message_handler(commands=['sinyal'])
def sinyal_handler(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    if len(parts)<2 or parts[1].lower() not in ("al","sat"):
        bot.reply_to(message, "/sinyal al — AL sinyalleri\n/sinyal sat — SAT sinyalleri"); return
    filtre = parts[1].upper()
    today_key = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%Y-%m-%d')
    try:
        conn = db_connect()
        if not conn: bot.reply_to(message, "❌ DB yok."); return
        with conn.cursor() as cur:
            cur.execute("SELECT key, value FROM store WHERE key LIKE %s", (f"sinyal:{today_key}:%",))
            rows = cur.fetchall()
        db_release(conn)
    except Exception as e:
        bot.reply_to(message, f"❌ {e}"); return

    bulunanlar = []
    for key, val in rows:
        try:
            data = json.loads(val)
            tip = data.get("type","")
            if filtre=="AL" and tip in ("AL","KARISIK"): bulunanlar.append(data["msg"])
            elif filtre=="SAT" and tip in ("SAT","KARISIK"): bulunanlar.append(data["msg"])
        except Exception: continue
    if not bulunanlar:
        bot.reply_to(message, f"{'🟢' if filtre=='AL' else '🔴'} Bugün {filtre} sinyali yok.\n/check all ile tara.", parse_mode='Markdown'); return
    bot.send_message(chat_id, f"{'🟢' if filtre=='AL' else '🔴'} *{len(bulunanlar)} {filtre} sinyali*", parse_mode='Markdown')
    for msg in bulunanlar:
        safe_send(chat_id, msg); time.sleep(0.3)

@bot.message_handler(commands=['bulten'])
def cmd_bulten(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    tip = parts[1].lower() if len(parts)>1 else "sabah"
    call_log("bulten", chat_id, tip)
    if not GROQ_KEY: bot.reply_to(message, "❌ GROQ_KEY yok."); return
    def _run():
        try: _send_bulten(chat_id, tip)
        except Exception as e: bot.send_message(chat_id, f"❌ {e}")
    threading.Thread(target=_run, daemon=True).start()

def _send_bulten(chat_id, tip="sabah"):
    tr_tz = pytz.timezone('Europe/Istanbul')
    now_str = datetime.now(tr_tz).strftime('%d.%m.%Y %H:%M')
    all_news = collect_news(["global","bist","macro"], max_per_feed=5)
    news_text = news_to_text(all_news, 12)

    if tip == "sabah":
        prompt = f"""BIST uzmanı analist. {now_str} sabah bülteni.
Haberler: {news_text}

🌅 SABAH BÜLTENİ — {now_str}
━━━━━━━━━━━━━━━━━━━
📰 GÜNÜN ÖNE ÇIKANLARI: (2-3 madde)
🌍 GLOBAL: (kısa)
📊 BIST BEKLENTİSİ: (bugün)
⚠️ DİKKAT: (risk)
💡 STRATEJİ: (öneri)"""
    else:
        signal_summary = ""
        try:
            today_key = datetime.now(tr_tz).strftime('%Y-%m-%d')
            conn = db_connect()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT key,value FROM store WHERE key LIKE %s",(f"sinyal:{today_key}:%",))
                    rows = cur.fetchall()
                db_release(conn)
                al_l = [r[0].split(":")[-1] for r in rows if json.loads(r[1]).get("type") in ("AL","KARISIK")]
                sat_l = [r[0].split(":")[-1] for r in rows if json.loads(r[1]).get("type") in ("SAT","KARISIK")]
                if al_l: signal_summary += f"AL: {', '.join(al_l[:10])}\n"
                if sat_l: signal_summary += f"SAT: {', '.join(sat_l[:10])}\n"
        except Exception: pass

        prompt = f"""BIST uzmanı. {now_str} akşam bülteni.
Haberler: {news_text}
Sinyaller: {signal_summary or 'Tarama yapılmadı'}

🌆 AKŞAM BÜLTENİ — {now_str}
━━━━━━━━━━━━━━━━━━━
📊 GÜNÜN ÖZETİ: (kapanış)
📰 HABERLER: (2-3 madde)
🔍 TEKNİK: (sinyallere göre)
🔮 YARIN: (öngörü)
💡 STRATEJİ: (yarın)"""

    result = groq_ask(prompt, max_tokens=600)
    safe_send(chat_id, result)

@bot.message_handler(commands=['tarasonuc'])
def cmd_tarasonuc(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("tarasonuc", chat_id, parts[1] if len(parts)>1 else "ozet")

    if len(parts)<2:
        ozet = tara_load_ozet(chat_id)
        if not ozet: bot.reply_to(message, "Henüz tarama yok.\n/tara all ile başlat."); return
        satirlar = ["📊 SON TARAMA SONUÇLARI",""]
        for kod in sorted(ozet.keys()):
            v = ozet[kod]
            emoji = "🟢" if v.get("sayi",0)>0 else "⬜"
            satirlar.append(f"{emoji} /{kod} — {v.get('isim',kod)}: {v.get('sayi',0)} ({v.get('tarih','?')})")
        satirlar += ["","Detay: /tarasonuc 2"]
        bot.reply_to(message, "\n".join(satirlar))
    else:
        kod = parts[1].upper()
        if kod not in TARA_STRATEJILER: bot.reply_to(message, f"Geçersiz: {kod}"); return
        veri = tara_load(chat_id, kod)
        if not veri: bot.reply_to(message, f"{kod} için sonuç yok. /tara {kod}"); return
        isim, aciklama = TARA_STRATEJILER[kod]
        detay = veri.get("detay",[])
        satirlar = [f"📊 {isim}",f"{aciklama}",f"{veri.get('tarih','?')} — {len(detay)} eşleşme","━━━━━━━━━━━━━━━━━━━"]
        if not detay: satirlar.append("Eşleşme yok.")
        else:
            for e in detay[:20]:
                satirlar.append(f"{e.get('ticker','?')} | RSI:{e.get('rsi',0):.1f} | RV:{e.get('rel_vol',0):.2f}x")
        bot.reply_to(message, "\n".join(satirlar))
        # ═══════════════════════════════════════════════
# BOT KOMUTLARI — Liste, Optimize, Check, Sıra, İptal
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    metin = (
        "📊 *BIST Teknik Analiz Botu v2.0*\n\n"
        "🔍 *── Strateji Tarama ──*\n"
        "/tara 1 — Tek strateji\n"
        "/tara all — Tüm 17 strateji ⭐\n"
        "/tara spade — SpadeHunter\n"
        "/tarasonuc — Son tarama özeti\n\n"
        "📈 *── EMA/RSI Tarama ──*\n"
        "/check all — Tüm liste (sadece AL) ⭐\n"
        "/check THYAO — Tek hisse (AL+SAT)\n"
        "/check 50 — Rastgele 50 hisse\n\n"
        "📊 *── Backtest ──*\n"
        "/backtest 1 THYAO — Strateji backtest\n\n"
        "📡 *── Sinyaller ──*\n"
        "/sinyal al — Bugünkü AL sinyalleri 🟢\n"
        "/sinyal sat — Bugünkü SAT sinyalleri 🔴\n\n"
        "🤖 *── AI & Haberler ──*\n"
        "/analiz THYAO — Gemini AI + MTF skor + risk\n"
        "/haber — Günlük piyasa haberleri (KAP+RSS) 📰\n"
        "/haber THYAO — Tek hisse haberleri + KAP\n"
        "/bulten sabah — Sabah bülteni 🌅\n"
        "/bulten aksam — Akşam bülteni 🌆\n\n"
        "⚡ *── Sıralı Komut ──*\n"
        "/sira cachesil|check all|tara all\n\n"
        "📋 *── Liste ──*\n"
        "/addall — BIST hisselerini ekle\n"
        "/refreshlist — Yahoo'dan güncel liste ⭐\n"
        "/add THYAO | /remove THYAO\n"
        "/watchlist — Listeyi gör\n\n"
        "🔧 *── Optimize ──*\n"
        "/optimize THYAO | /optimizeall\n\n"
        "📒 *── Trade Kitabı ──*\n"
        "/al THYAO 145.50 100 sebep\n"
        "/sat THYAO 162.00 100 sebep\n"
        "/kitap | /kitap ozet | /kitap acik\n"
        "/karzarar THYAO 145 162 100 — Trade hesabı\n"
        "/karzarar risk 5000 5 — Risk hesabı\n"
        "/karzarar acik THYAO 162 — Açık pozisyon\n"
        "/karzarar pozisyon 10000 2 145 140 — Pozisyon boyutu\n\n"
        "🛠 *── Sistem ──*\n"
        "/kontrolbot — Sağlık testi\n"
        "/status — Bot durumu\n"
        "/kredi — AI kota\n"
        "/cachesil — Cache temizle\n"
        "/resetgemini — Gemini kota sıfırla\n\n"
        "🚫 /iptal tara|check|sira|optimize\n"
        "💾 /backup | /loadbackup"
    )
    bot.reply_to(message, metin, parse_mode="Markdown")

@bot.message_handler(commands=['check'])
def manual_check(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()

    if len(parts)>1 and parts[1].upper() not in ("ALL","TUM","TÜMÜ") and not parts[1].isdigit():
        ticker = parts[1].upper().replace(".IS","")
        reset_cancel_flag(chat_id, "check")
        bot.reply_to(message, f"🔍 *{ticker}* analiz ediliyor...")
        threading.Thread(target=scan_all_stocks, args=(chat_id, None, [ticker], True), daemon=True).start()
        return

    if not wl_get(chat_id):
        bot.reply_to(message, "📭 Liste boş! /addall yaz."); return

    if len(parts)==1 or parts[1].upper() in ("ALL","TUM","TÜMÜ"):
        total = len(wl_get(chat_id))
        # show_sells parametresini kontrol et
        show_sells = len(parts)>2 and parts[2].lower() in ("all","hepsi","sat")
        bot.reply_to(message,
            f"🔍 *{total} hisse taranıyor*\n"
            f"{'📋 AL+SAT gösterilecek' if show_sells else '📋 Sadece AL sinyalleri'}\n"
            f"🚫 İptal: /iptal check")
        reset_cancel_flag(chat_id, "check")
        threading.Thread(target=scan_all_stocks, args=(chat_id, None, None, show_sells), daemon=True).start()
        return

    try:
        limit = int(parts[1])
        reset_cancel_flag(chat_id, "check")
        threading.Thread(target=scan_all_stocks, args=(chat_id, limit), daemon=True).start()
    except ValueError:
        bot.reply_to(message, "/check all | /check 50 | /check THYAO")

@bot.message_handler(commands=['addall'])
def add_all(message):
    chat_id = str(message.chat.id)
    tickers = get_all_bist_tickers()
    wl_set(chat_id, tickers)
    bot.reply_to(message, f"✅ {len(tickers)} hisse eklendi.\n💡 /refreshlist ile güncelleyebilirsin.")

@bot.message_handler(commands=['refreshlist'])
def refreshlist(message):
    chat_id = str(message.chat.id)
    threading.Thread(target=_run_refreshlist, args=(chat_id,), daemon=True).start()

@bot.message_handler(commands=['add'])
def add_ticker(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    if len(parts)<2: bot.reply_to(message, "/add THYAO"); return
    ticker = parts[1].upper().replace(".IS","")
    lst = wl_get(chat_id)
    if ticker not in lst:
        lst.append(ticker); wl_set(chat_id, lst)
        bot.reply_to(message, f"✅ {ticker} eklendi. ({len(lst)})")
    else:
        bot.reply_to(message, f"ℹ️ {ticker} zaten listede.")

@bot.message_handler(commands=['remove'])
def remove_ticker(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    if len(parts)<2: bot.reply_to(message, "/remove THYAO"); return
    ticker = parts[1].upper().replace(".IS","")
    lst = wl_get(chat_id)
    if ticker in lst:
        lst.remove(ticker); wl_set(chat_id, lst)
        bot.reply_to(message, f"🗑 {ticker} silindi. ({len(lst)})")
    else:
        bot.reply_to(message, f"ℹ️ {ticker} listede yok.")

@bot.message_handler(commands=['watchlist'])
def show_list(message):
    chat_id = str(message.chat.id)
    lst = wl_get(chat_id)
    if lst: send_long_message(chat_id, f"📋 *İzleme Listen* ({len(lst)}):\n" + "\n".join(lst))
    else: bot.reply_to(message, "📭 Boş — /addall yaz")

@bot.message_handler(commands=['optimize'])
def optimize(message):
    try:
        ticker = message.text.split()[1].upper().replace('.IS','')
        chat_id = str(message.chat.id)
        bot.reply_to(message, f"⚙️ *{ticker}* optimize başlıyor...\n🚫 İptal: /iptal optimize")
        threading.Thread(target=_run_optimize, args=(chat_id, ticker), daemon=True).start()
    except IndexError: bot.reply_to(message, "/optimize THYAO")
    except Exception as e: bot.reply_to(message, f"Hata: {e}")

def _run_optimize(chat_id, ticker):
    reset_cancel_flag(chat_id, "optimize")
    try:
        pairs = find_best_ema_pair(ticker, chat_id=chat_id)
        if pairs is None: bot.send_message(chat_id, f"{ticker} iptal."); return
        if not pairs or (pairs["daily"]==(9,21) and pairs["weekly"]==(9,21)):
            bot.send_message(chat_id, f"{ticker}: Yeterli veri yok, 9-21.")
        else:
            ema_set(ticker, pairs); d=pairs["daily"]; w=pairs["weekly"]
            bot.send_message(chat_id, f"✅ *{ticker}*\n📅 G: {d[0]}-{d[1]}\n📆 H: {w[0]}-{w[1]}")
    except Exception as e: bot.send_message(chat_id, f"Hata: {e}")

@bot.message_handler(commands=['optimizeall'])
def optimizeall(message):
    chat_id = str(message.chat.id)
    lst = wl_get(chat_id)
    if not lst: bot.reply_to(message, "📭 Boş!"); return
    bot.reply_to(message, f"⚙️ {len(lst)} hisse optimize edilecek\n🚫 /iptal optimizeall")
    threading.Thread(target=_run_optimizeall, args=(chat_id,), daemon=True).start()

def _run_optimizeall(chat_id):
    reset_cancel_flag(chat_id, "optimizeall")
    tickers = wl_get(chat_id)
    if not tickers: return
    total=len(tickers); done=0; skipped=0; improved=0

    bot.send_message(chat_id, f"⚙️ *Toplu Optimize*\n📋 {total} hisse\n⏱ ~{total*25//3600}sa {(total*25%3600)//60}dk")

    for i, ticker in enumerate(tickers):
        if is_cancelled(chat_id, "optimizeall"):
            bot.send_message(chat_id, f"🚫 Durduruldu. ✅{done} 📈{improved} ⏭{skipped}"); return
        try:
            pairs = find_best_ema_pair(ticker, chat_id=chat_id)
            if pairs is None: bot.send_message(chat_id, "🚫 İptal."); return
            if pairs:
                ema_set(ticker, pairs)
                if pairs["daily"]!=(9,21) or pairs["weekly"]!=(9,21): improved+=1
                done+=1
            else: skipped+=1
        except Exception: skipped+=1
        if (i+1)%10==0:
            pct=int((i+1)/total*100)
            bot.send_message(chat_id, f"⚙️ {i+1}/{total} ({pct}%)\n✅{done} 📈{improved} ⏭{skipped}")
        time.sleep(0.5)

    bot.send_message(chat_id,
        f"🎉 *Toplu Optimize Bitti!*\n━━━━━━━━━━━━━━━━━━━\n"
        f"📋 {total} | ✅ {done} | 📈 {improved} | ⏭ {skipped}")

# ── /cachesil ──
@bot.message_handler(commands=['cachesil'])
def cmd_cachesil(message):
    chat_id = str(message.chat.id)
    conn = db_connect()
    if not conn: bot.reply_to(message, "❌ DB yok."); return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM price_cache"); silinen=cur.rowcount
        conn.commit(); _invalidate_cache_set()
        bot.reply_to(message, f"✅ {silinen} kayıt silindi.")
    except Exception as e: bot.reply_to(message, f"❌ {e}")
    finally: db_release(conn)

# ── /sira ──
_sira_running = {}
SIRA_KOMUTLAR = {
    "check all": lambda cid: scan_all_stocks(cid),
    "tara all": lambda cid: _tara_all(cid),
    "cachesil": lambda cid: _cachesil_sync(cid),
    "optimizeall": lambda cid: _run_optimizeall(cid),
}

def _cachesil_sync(chat_id):
    conn = db_connect()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM price_cache"); s=cur.rowcount
        conn.commit(); _invalidate_cache_set()
        bot.send_message(chat_id, f"✅ Cache: {s} silindi.")
    except Exception as e: bot.send_message(chat_id, f"❌ {e}")
    finally: db_release(conn)

def _run_sira(chat_id, komutlar):
    _sira_running[chat_id] = True
    toplam = len(komutlar)
    bot.send_message(chat_id, f"⚙️ SIRA — {toplam} komut\n" +
        "\n".join([f"  {i+1}. {k}" for i,k in enumerate(komutlar)]))
    try:
        for i, komut in enumerate(komutlar):
            if not _sira_running.get(chat_id, False):
                bot.send_message(chat_id, f"🚫 Sıra iptal. ({i}/{toplam})"); return
            fn = SIRA_KOMUTLAR.get(komut)
            if not fn: bot.send_message(chat_id, f"⚠️ Bilinmeyen: '{komut}'"); continue
            bot.send_message(chat_id, f"▶️ [{i+1}/{toplam}] {komut}...")
            try:
                fn(chat_id)
                bot.send_message(chat_id, f"✅ [{i+1}/{toplam}] {komut} tamam.")
            except Exception as e:
                bot.send_message(chat_id, f"❌ [{i+1}/{toplam}] {komut}: {str(e)[:80]}")
    finally: _sira_running[chat_id] = False
    bot.send_message(chat_id, "🏁 Sıra tamamlandı!")

@bot.message_handler(commands=['sira'])
def cmd_sira(message):
    chat_id = str(message.chat.id)
    after = message.text.strip()[len("/sira"):].strip()
    if not after:
        bot.reply_to(message,
            "⚙️ SIRA\n━━━━━━━━━━━━━━━━━━━\n"
            "Kullanım: /sira komut1 | komut2\n\n"
            "Komutlar: check all, tara all, cachesil, optimizeall\n\n"
            "Örnek: /sira cachesil | check all | tara all\n"
            "İptal: /iptal sira"); return
    if _sira_running.get(chat_id, False):
        bot.reply_to(message, "⚠️ Zaten çalışıyor.\n/iptal sira"); return
    komutlar = [k.strip().lower() for k in after.split("|") if k.strip()]
    gecersiz = [k for k in komutlar if k not in SIRA_KOMUTLAR]
    if gecersiz:
        bot.reply_to(message, f"❌ Bilinmeyen: {', '.join(gecersiz)}"); return
    call_log("sira", chat_id, " | ".join(komutlar))
    threading.Thread(target=_run_sira, args=(chat_id, komutlar), daemon=True).start()

@bot.message_handler(commands=['iptal'])
def iptal(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    op = parts[1].lower() if len(parts)>1 else "check"
    if op == "sira":
        _sira_running[chat_id] = False
        bot.reply_to(message, "🚫 Sıra iptal.")
    else:
        get_cancel_flag(chat_id, op).set()
        bot.reply_to(message, f"🚫 '{op}' iptal sinyali gönderildi.")

# ── /kredi ──
@bot.message_handler(commands=['resetgemini'])
def cmd_resetgemini(message):
    try:
        db_del("gemini_quota_exhausted_1"); db_del("gemini_quota_exhausted_2"); db_del("gemini_quota_exhausted_3")
        bot.reply_to(message, "✅ Gemini kota sıfırlandı.\n/kredi ile kontrol et.")
    except Exception as e: bot.reply_to(message, f"❌ {e}")

@bot.message_handler(commands=['kredi'])
def cmd_kredi(message):
    chat_id = str(message.chat.id)
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    now_str = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%d.%m.%Y %H:%M")
    g_keys = _gemini_keys()
    g_used = _ai_usage.get("gemini_today",0)

    g_lines = []
    for kno in (1,2,3):
        kval = [GEMINI_KEY,GEMINI_KEY2,GEMINI_KEY3][kno-1]
        if not kval: g_lines.append(f"  Key{kno}: ❌ Yok"); continue
        doldu = db_get(f"gemini_quota_exhausted_{kno}") == today
        aktif = "← AKTİF" if kno==_ai_usage.get("gemini_active_key") else ""
        g_lines.append(f"  Key{kno}: {'🔴 DOLU' if doldu else '🟢 OK'} {aktif}")

    gr_lines = []
    for kno in (1,2,3):
        kval = [GROQ_KEY,GROQ_KEY2,GROQ_KEY3][kno-1]
        if not kval: gr_lines.append(f"  Key{kno}: ❌ Yok"); continue
        doldu = db_get(f"groq_ratelimit_{kno}") == today
        aktif = "← AKTİF" if kno==_ai_usage.get("groq_active_key") else ""
        gr_lines.append(f"  Key{kno}: {'🔴 LIMIT' if doldu else '🟢 OK'} {aktif}")

    lines = [
        f"🤖 AI Durumu — {now_str}","━━━━━━━━━━━━━━━━━━━","",
        f"🟣 GEMINI ({len(g_keys)}/3)",
        f"  Bugün: {g_used} istek",
        f"  Model: {_ai_usage.get('gemini_last_model','?')}",
    ] + g_lines + ["",
        f"🟠 GROQ ({len(_groq_keys())}/3)",
        f"  Bugün: {_ai_usage.get('groq_today',0)} istek",
        f"  Kalan: {_ai_usage.get('groq_remaining_req','?')} req",
    ] + gr_lines + ["",
        f"📋 Watchlist: {len(wl_get(chat_id))} hisse"
    ]
    bot.send_message(chat_id, "\n".join(lines))

# ── /backup /loadbackup ──
@bot.message_handler(commands=['backup'])
def backup(message):
    chat_id = str(message.chat.id)
    try:
        wl = wl_get(chat_id)
        if not wl: bot.reply_to(message, "📭 Boş."); return
        ema_data = {}
        for ticker in wl:
            ep = ema_get(ticker)
            if ep != {"daily":(9,21),"weekly":(9,21)}:
                ema_data[ticker] = {"daily":list(ep["daily"]),"weekly":list(ep["weekly"])}
        backup_payload = {
            "version":2,
            "date":datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%Y-%m-%d %H:%M'),
            "chat_id":chat_id, "watchlist":wl, "ema_custom":ema_data,
        }
        import io
        file_obj = io.BytesIO(json.dumps(backup_payload, ensure_ascii=False).encode('utf-8'))
        file_obj.name = f"bist_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        bot.send_message(chat_id, f"💾 Yedek: {len(wl)} hisse, {len(ema_data)} özel EMA")
        bot.send_document(chat_id, file_obj, caption="📦 Geri yükle: dosyayı gönder + /loadbackup")
    except Exception as e: bot.reply_to(message, f"❌ {e}")

@bot.message_handler(commands=['loadbackup'])
def loadbackup(message):
    chat_id = str(message.chat.id)
    if not message.reply_to_message:
        bot.reply_to(message, "📂 JSON dosyasını gönder, sonra yanıtla /loadbackup yaz."); return
    _process_backup_file(message, chat_id)

@bot.message_handler(content_types=['document'])
def handle_document(message):
    chat_id = str(message.chat.id)
    try:
        fname = message.document.file_name or ""
        if fname.startswith("bist_backup") and fname.endswith(".json"):
            if message.caption and "/loadbackup" in message.caption:
                _process_backup_file(message, chat_id, doc_message=message)
            else:
                bot.reply_to(message, "📦 Yedek algılandı! Yanıtla /loadbackup yaz.")
    except Exception: pass

def _process_backup_file(message, chat_id, doc_message=None):
    try:
        target = doc_message or message.reply_to_message
        if not target or not target.document:
            bot.send_message(chat_id, "❌ Dosya bulunamadı."); return
        file_info = bot.get_file(target.document.file_id)
        downloaded = bot.download_file(file_info.file_path)
        payload = json.loads(downloaded.decode('utf-8'))
        watchlist = payload.get("watchlist",[])
        ema_data = payload.get("ema_custom",{})
        if not watchlist: bot.send_message(chat_id, "❌ Watchlist boş."); return
        wl_set(chat_id, watchlist)
        ema_loaded = 0
        for ticker, ep in ema_data.items():
            try:
                ema_set(ticker, {"daily":tuple(ep.get("daily",[9,21])),"weekly":tuple(ep.get("weekly",[9,21]))})
                ema_loaded += 1
            except Exception: pass
        bot.send_message(chat_id,
            f"✅ *Yedek Yüklendi!*\n📋 {len(watchlist)} hisse\n⚙️ {ema_loaded} EMA\n🕐 {payload.get('date','?')}")
    except json.JSONDecodeError: bot.send_message(chat_id, "❌ Geçersiz JSON.")
    except Exception as e: bot.send_message(chat_id, f"❌ {e}")

# ═══════════════════════════════════════════════
# DEBUG & KONTROL
# ═══════════════════════════════════════════════
_debug_log = collections.deque(maxlen=30)
_call_log = collections.deque(maxlen=20)

def debug_log(seviye, kaynak, mesaj, extra=None):
    tr_tz = pytz.timezone('Europe/Istanbul')
    zaman = datetime.now(tr_tz).strftime('%H:%M:%S')
    _debug_log.append({"t":zaman,"lvl":seviye,"src":kaynak,"msg":mesaj[:300],"extra":str(extra)[:200] if extra else None})
    if seviye in ("ERROR","CRASH"): print(f"[{seviye}] {kaynak}: {mesaj}")

def call_log(komut, chat_id, params=""):
    tr_tz = pytz.timezone('Europe/Istanbul')
    _call_log.append({"t":datetime.now(tr_tz).strftime('%H:%M:%S'),"cmd":komut,"uid":str(chat_id)[-4:],"p":params[:40]})

@bot.message_handler(commands=['kontrolbot'])
def cmd_kontrolbot(message):
    chat_id = str(message.chat.id)
    call_log("kontrolbot", chat_id)
    bot.send_message(chat_id, "🔬 Tanılama başlıyor...")

    def _run_kontrol():
        sonuclar = []
        def test(ad, fn):
            try: ok, detay, tam = fn()
            except Exception as e: ok, detay, tam = False, str(e)[:120], traceback.format_exc()
            sonuclar.append((ad, ok, detay, tam))

        def t_telegram():
            info = bot.get_me(); wh = bot.get_webhook_info()
            return True, f"@{info.username} | Webhook: {'OK' if wh.url else 'YOK'} | Kuyruk: {wh.pending_update_count}", None
        test("Telegram", t_telegram)

        def t_db():
            if not DATABASE_URL: return False, "DATABASE_URL YOK", None
            conn = db_connect()
            if not conn: return False, "Bağlantı yok", None
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM store"); cnt=cur.fetchone()[0]
            db_release(conn)
            pool_str = f"Pool: {'Aktif' if _db_pool else 'Yok'}"
            return True, f"store: {cnt} | {pool_str}", None
        test("PostgreSQL", t_db)

        def t_wl():
            wl = wl_get(chat_id)
            if not wl: return False, "Boş → /addall", None
            return True, f"{len(wl)} hisse", None
        test("Watchlist", t_wl)

        def t_yahoo():
            df_d, _ = get_data("THYAO")
            if df_d is None or df_d.empty: return False, "THYAO gelmedi", None
            return True, f"THYAO {df_d['Close'].iloc[-1]:.2f}₺ | {len(df_d)} bar", None
        test("Yahoo", t_yahoo)

        def t_gemini():
            keys = _gemini_keys()
            if not keys: return False, "Key yok", None
            y = gemini_ask("1+1=? Sadece rakam.", max_tokens=5)
            if y.startswith("⚠️"): return False, y[:80], None
            return True, f"{_ai_usage.get('gemini_last_model','?')} | '{y[:15]}'", None
        test("Gemini", t_gemini)

        def t_groq():
            if not GROQ_KEY: return False, "Key yok", None
            y = groq_ask("1+1=?", max_tokens=5)
            if y.startswith("⚠️"): return False, y[:80], None
            return True, f"Key{_ai_usage.get('groq_active_key','?')} | Kalan: {_ai_usage.get('groq_remaining_req','?')}", None
        test("Groq", t_groq)

        def t_rss():
            ok_c=[]; fail_c=[]
            for cat, feeds in RSS_FEEDS.items():
                for name, url in feeds:
                    items = fetch_rss(url, max_items=1, timeout=5)
                    if items: ok_c.append(name)
                    else: fail_c.append(name)
            return len(ok_c)>=len(fail_c), f"{len(ok_c)} aktif / {len(fail_c)} bozuk", None
        test("RSS", t_rss)

        tr_tz = pytz.timezone('Europe/Istanbul')
        simdi = datetime.now(tr_tz).strftime('%d.%m.%Y %H:%M:%S')
        hatali = [(a,d,t) for a,ok,d,t in sonuclar if not ok]

        ozet = [f"🔬 KONTROLBOT — {simdi}",""]
        for ad, ok, detay, _ in sonuclar:
            ozet.append(f"{'✅' if ok else '❌'} {ad}: {detay}")
        ozet.append(f"\n{'🟢 TÜM SİSTEMLER OK' if not hatali else f'🔴 {len(hatali)} SORUN'}")
        ozet.append(f"Uptime: {str(datetime.now(tr_tz)-_bot_start_time).split('.')[0]}")
        bot.send_message(chat_id, "\n".join(ozet))

        if hatali:
            debug_m = ["━━━━━━━━━━━━━━━━━━━","🤖 DEBUG RAPORU","━━━━━━━━━━━━━━━━━━━"]
            for ad, detay, tam in hatali:
                debug_m.append(f"[HATA] {ad}: {detay}")
                if tam: debug_m.append(f"  {tam[:300]}")
            for g in list(_debug_log)[-8:]:
                debug_m.append(f"{g['t']} [{g['lvl']}] {g['src']}: {g['msg']}")
            bot.send_message(chat_id, "\n".join(debug_m))

    threading.Thread(target=_run_kontrol, daemon=True).start()

# ═══════════════════════════════════════════════
# ÇÖKÜŞ KORUMA
# ═══════════════════════════════════════════════
_bot_start_time = datetime.now(pytz.timezone('Europe/Istanbul'))
_error_count = 0; _last_error = None; _last_error_time = None; _webhook_failures = 0

def _notify_admin(msg):
    try:
        for chat_id in wl_all_ids():
            try: bot.send_message(chat_id, f"⚠️ *Bot*\n{msg}", parse_mode='Markdown')
            except: pass
    except: pass

@bot.message_handler(commands=['status'])
def bot_status(message):
    tr_tz = pytz.timezone('Europe/Istanbul')
    now = datetime.now(tr_tz)
    uptime = now - _bot_start_time
    hours = int(uptime.total_seconds()//3600)
    minutes = int((uptime.total_seconds()%3600)//60)

    thread_names = [t.name for t in threading.enumerate()]
    ka_ok = "keep_alive" in thread_names
    as_ok = "auto_scan" in thread_names
    wd_ok = "watchdog" in thread_names

    db_ok = False
    if DATABASE_URL:
        try:
            conn = db_connect()
            if conn: db_release(conn); db_ok = True
        except: pass

    lines = [
        "🤖 *Bot Durumu*","━━━━━━━━━━━━━━━━━━━",
        f"⏱ Uptime: {hours}sa {minutes}dk",
        f"🕐 Başlangıç: {_bot_start_time.strftime('%d.%m.%Y %H:%M')}","",
        f"{'✅' if ka_ok else '❌'} Keep-alive",
        f"{'✅' if as_ok else '❌'} Auto-scan",
        f"{'✅' if wd_ok else '❌'} Watchdog",
        f"{'✅' if db_ok else '⚠️'} DB {'(Pool)' if _db_pool else '(Direct)'}","",
        f"📋 Watchlist: {len(wl_get(str(message.chat.id)))}",
        f"💾 Cache: {pc_count_today()}",
        f"🔢 Hata: {_error_count}",
    ]
    if _last_error:
        lines.append(f"🔴 Son: {_last_error[:80]}")
    else: lines.append("✅ Hata yok")
    bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')

# ═══════════════════════════════════════════════
# AUTO-SCAN (18:05) — Cuma optimize KALDIRILDI
# ═══════════════════════════════════════════════
def auto_scan():
    tr_tz = pytz.timezone('Europe/Istanbul')
    check_date = None; cuma_date = None
    bulten_s_date = None; bulten_a_date = None; kriz_date = None

    while True:
        try:
            now = datetime.now(tr_tz)

            # 09:00 — Sabah bülteni
            if now.hour==9 and now.minute<5 and bulten_s_date!=now.date():
                bulten_s_date = now.date()
                if GROQ_KEY:
                    for cid in wl_all_ids():
                        try: _send_bulten(cid, "sabah")
                        except: pass

            # 10:00, 14:00, 17:30 — Kriz kontrolü
            if now.minute<2 and now.hour in (10,14,17) and kriz_date!=(now.date(),now.hour):
                kriz_date = (now.date(), now.hour)
                if GROQ_KEY:
                    def _kriz():
                        try:
                            gn = collect_news(["global","macro"], max_per_feed=5)
                            crisis = groq_crisis_check(news_to_text(gn, 10))
                            if crisis:
                                sectors = extract_sector_from_news(news_to_text(gn, 5))
                                for cid in wl_all_ids():
                                    try:
                                        safe_send(cid, f"🚨 *KRİZ ALARMI*\n━━━━━━━━━━━━━━━━━━━\n{crisis}")
                                        if sectors:
                                            alarm = ["🏭 *ETKİLENEN SEKTÖRLER*"]
                                            for s in sectors:
                                                ts = get_related_tickers(s)
                                                if ts: alarm.append(f"⚠️ {s}: {', '.join(ts[:8])}")
                                            safe_send(cid, "\n".join(alarm))
                                    except: pass
                        except: pass
                    threading.Thread(target=_kriz, daemon=True).start()

            # Cuma 17:00 — Strateji taraması (optimize KALDIRILDI)
            if now.weekday()==4 and now.hour==17 and now.minute<10 and cuma_date!=now.date():
                cuma_date = now.date()
                for cid in wl_all_ids():
                    try:
                        bot.send_message(cid, "📊 CUMA STRATEJİ TARAMASI\n17 strateji çalışıyor...")
                        threading.Thread(target=_tara_all, args=(cid,), daemon=True).start()
                    except: pass

            # 18:05 — Teknik tarama
            if now.hour==18 and 5<=now.minute<45 and check_date!=now.date():
                check_date = now.date()
                for cid in wl_all_ids():
                    try:
                        bot.send_message(cid, "🕕 *18:05 Otomatik Tarama*\n🚫 /iptal check", parse_mode='Markdown')
                        scan_all_stocks(cid)
                    except Exception as e:
                        debug_log("ERROR","auto_scan",str(e)[:150])

            # 18:30 — Akşam bülteni
            if now.hour==18 and 30<=now.minute<35 and bulten_a_date!=now.date():
                bulten_a_date = now.date()
                if GROQ_KEY:
                    for cid in wl_all_ids():
                        try: _send_bulten(cid, "aksam")
                        except: pass

        except Exception as e:
            print(f"auto_scan hata: {e}")
        time.sleep(60)

# ═══════════════════════════════════════════════
# WATCHDOG
# ═══════════════════════════════════════════════
def watchdog():
    global _webhook_failures
    critical_threads = {}
    def ensure_thread(name, target, args=()):
        t = critical_threads.get(name)
        if t is None or not t.is_alive():
            print(f"[WATCHDOG] {name} yeniden başlatılıyor...")
            new_t = threading.Thread(target=target, args=args, name=name, daemon=True)
            new_t.start(); critical_threads[name] = new_t; return True
        return False

    while True:
        try:
            restarted = []
            if ensure_thread("keep_alive", keep_alive): restarted.append("keep_alive")
            if ensure_thread("auto_scan", auto_scan): restarted.append("auto_scan")

            if RENDER_URL:
                try:
                    r = requests.get(f"{RENDER_URL}/health", timeout=10)
                    if r.status_code==200: _webhook_failures=0
                    else: _webhook_failures+=1
                except: _webhook_failures+=1

                if _webhook_failures>=3:
                    try: set_webhook(); _webhook_failures=0
                    except: pass

            if restarted:
                _notify_admin(f"♻️ Yeniden başlatılan: {', '.join(restarted)}")
        except Exception as e:
            print(f"[WATCHDOG] {e}")
        time.sleep(5*60)

def safe_thread(target, args=(), name="thread", notify_on_crash=False):
    global _error_count, _last_error, _last_error_time
    def wrapper():
        while True:
            try:
                target(*args); break
            except Exception as e:
                _error_count+=1; _last_error=str(e)
                _last_error_time = datetime.now(pytz.timezone('Europe/Istanbul'))
                print(f"[CRASH] {name}: {e}")
                if notify_on_crash:
                    _notify_admin(f"🔴 {name} çöktü: {str(e)[:120]}\n♻️ 30sn sonra yeniden başlatılıyor...")
                time.sleep(30)
                if name in ("auto_scan","keep_alive","watchdog"): continue
                break
    t = threading.Thread(target=wrapper, name=name, daemon=True)
    t.start(); return t

def global_exception_handler(exc_type, exc_value, exc_traceback):
    global _error_count, _last_error, _last_error_time
    _error_count+=1; _last_error=str(exc_value)
    _last_error_time = datetime.now(pytz.timezone('Europe/Istanbul'))
    print(f"[GLOBAL EXCEPTION]\n{''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))}")

# ═══════════════════════════════════════════════
# ANA BAŞLATICI
# ═══════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# UYGULAMA BAŞLANGIÇ — gunicorn "bot:app" import'unda çalışır
# ═══════════════════════════════════════════════════════════════════════════════
def _app_startup():
    import sys
    sys.excepthook = global_exception_handler
    print(f"BIST Bot v2.0 başlatılıyor - PORT={PORT}")
    try: db_pool_init()
    except Exception as e: print(f"Pool hata (devam): {e}")
    try: db_init()
    except Exception as e: print(f"DB init hata (devam): {e}")
    try:
        today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
        for kno in (1,2,3,4,5):
            if db_get(f"gemini_quota_exhausted_{kno}") == today:
                print(f"[STARTUP] Gemini Key{kno} bugün dolmuş")
    except: pass
    try: set_webhook()
    except Exception as e: print(f"Webhook hata (devam): {e}")
    safe_thread(keep_alive, name="keep_alive", notify_on_crash=True)
    safe_thread(auto_scan,  name="auto_scan",  notify_on_crash=True)
    safe_thread(watchdog,   name="watchdog",   notify_on_crash=True)


# gunicorn bot:app → modül import'unda çalışır
_app_startup()


def main():
    _app_startup()
    if not RENDER_URL:
        print("RENDER_URL tanımlı değil, polling modunda başlatılıyor...")
        bot.infinity_polling(skip_pending=True, timeout=30)
        return
    flask_crash = 0
    while True:
        try:
            app.run(host="0.0.0.0", port=PORT)
        except Exception as e:
            flask_crash += 1
            print(f"Flask çöktü ({flask_crash}): {e}")
            _notify_admin(f"🔴 Flask çöktü: {str(e)[:120]}\n♻️ 10sn sonra...")
            time.sleep(10)


if __name__ == "__main__":
    main()
