# catalyst_watch.py
import os, json, time, logging, math, requests
from datetime import datetime, timedelta, timezone
from typing import Iterable, Set, List, Tuple
from bs4 import BeautifulSoup
import yfinance as yf
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ==== ENV ====
PRICE_MIN = float(os.getenv("CAT_PRICE_MIN", "3.0"))
PRICE_MAX = float(os.getenv("CAT_PRICE_MAX", "15.0"))
VOL_SPIKE_MULT = float(os.getenv("CAT_VOL_SPIKE_MULT", "3.0"))   # во сколько раз выше среднего 10дн
PRICE_MOVE_PCT = float(os.getenv("CAT_PRICE_MOVE_PCT", "8.0"))   # % за день
NEWS_MIN = int(os.getenv("CAT_NEWS_MIN", "1"))                   # мин. число свежих новостей/файлингов
YF_SLEEP = float(os.getenv("CAT_YF_SLEEP", "0.6"))
SEC_DAYS_BACK = int(os.getenv("SEC_DAYS_BACK", "14"))            # глубина для 10-12B

GCS_BUCKET = os.getenv("GCS_BUCKET")
CAT_WATCHLIST_GCS_BLOB = os.getenv("CAT_WATCHLIST_GCS_BLOB", "catalyst/watchlist.txt")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ==== GCS ====
_gcs = None
def _get_gcs():
    global _gcs
    if _gcs: return _gcs
    key_str = os.getenv("GCS_KEY_JSON") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_str:
        raise ValueError("❌ Нет ключа GCS (GCS_KEY_JSON или GOOGLE_APPLICATION_CREDENTIALS)")
    with open("gcs_key.json", "w", encoding="utf-8") as f:
        json.dump(json.loads(key_str), f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcs_key.json"
    _gcs = storage.Client()
    return _gcs

def gcs_save_text(bucket: str, key: str, text: str):
    if not bucket: return
    _get_gcs().bucket(bucket).blob(key).upload_from_string(text, content_type="text/plain; charset=utf-8")
    logging.info(f"Saved to gs://{bucket}/{key} ({len(text)} bytes)")

def gcs_load_text(bucket: str, key: str, default: str = "") -> str:
    try:
        blob = _get_gcs().bucket(bucket).blob(key)
        return blob.download_as_text(encoding="utf-8") if blob.exists() else default
    except Exception:
        return default

# ==== Telegram ====
def tg_send(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("[TG MOCK] " + text)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=20,
        )
    except Exception as e:
        logging.warning(f"TG send error: {e}")

# ==== Источник 1: IPO (Nasdaq) ====
def fetch_nasdaq_ipos() -> Set[str]:
    """
    Тянем таблицы с https://www.nasdaq.com/market-activity/ipos (recent/upcoming).
    Берём первый столбец (тикер). Сайт иногда меняет вёрстку — держим парсинг максимально устойчивым.
    """
    url = "https://www.nasdaq.com/market-activity/ipos"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    tickers: Set[str] = set()

    # Любая таблица на странице: ищем ячейки, похожие на тикер
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if not cols: continue
            t = cols[0].get_text(strip=True).upper()
            # тикер: 1-5 латинских букв (допускаем .U/.W для юнитов/варрантов, но обрежем до базы)
            base = t.replace(".U", "").replace(".W", "")
            if 1 <= len(base) <= 5 and base.isalpha():
                tickers.add(base)
    logging.info(f"Nasdaq IPO: {len(tickers)}")
    return tickers

# ==== Источник 2: Spin-offs (SEC 10-12B) ====
def fetch_sec_spinoffs(days_back: int = 14) -> Set[str]:
    """
    Официальный SEC search-index (без сторонних сервисов).
    Берём последние формы 10-12B за days_back дней и собираем тикеры.
    """
    # API индекса: https://efts.sec.gov/LATEST/search-index
    # Ограничим размер, чтобы не злоупотреблять.
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    body = {
        "query": {"query_string": {"query": f'formType:"10-12B" AND filedAt:>={since}'}},
        "from": 0,
        "size": 200,
        "sort": [{"filedAt": {"order": "desc"}}],
        "highlight": False,
    }
    r = requests.post("https://efts.sec.gov/LATEST/search-index", json=body, timeout=30,
                      headers={"User-Agent":"Mozilla/5.0"})
    if r.status_code != 200:
        logging.warning(f"SEC 10-12B request failed: {r.status_code} {r.text[:200]}")
        return set()
    data = r.json()
    hits = data.get("hits", {}).get("hits", [])
    res: Set[str] = set()
    for h in hits:
        tick = (h.get("_source", {}).get("ticker") or "").upper().strip()
        if tick and tick != "N/A":
            base = tick.replace(".U","").replace(".W","")
            if 1 <= len(base) <= 5 and base.isalpha():
                res.add(base)
    logging.info(f"SEC 10-12B (spin-offs): {len(res)}")
    return res

# ==== Метрики активности ====
def get_hist(ticker: str):
    t = yf.Ticker(ticker)
    hist = t.history(period="15d", interval="1d", auto_adjust=False)
    return hist if hist is not None and not hist.empty else None

def volume_spike(hist) -> Tuple[bool, float, float]:
    """
    Сравниваем последний объём с SMA(10) предыдущих дней.
    """
    vol = hist["Volume"].dropna()
    if len(vol) < 12:  # нужен хотя бы ~10 дней истории + текущий
        return (False, 0.0, 0.0)
    last = float(vol.iloc[-1])
    base = float(vol.iloc[-11:-1].mean())
    if base <= 0:
        return (False, last, base)
    return (last >= VOL_SPIKE_MULT * base, last, base)

def price_move(hist) -> Tuple[bool, float]:
    """
    % изменения: последний Close vs предыдущий Close.
    """
    close = hist["Close"].dropna()
    if len(close) < 2:
        return (False, 0.0)
    pct = (float(close.iloc[-1]) / float(close.iloc[-2]) - 1.0) * 100.0
    return (pct >= PRICE_MOVE_PCT, pct)

def in_price_range(hist) -> Tuple[bool, float]:
    last_close = float(hist["Close"].dropna().iloc[-1])
    return (PRICE_MIN <= last_close <= PRICE_MAX, last_close)

def recent_news_count_yf(ticker: str, hours: int = 24) -> int:
    """
    Пытаемся взять свежие новости через yfinance (если есть).
    Считаем записи за последние 'hours' часов.
    """
    try:
        t = yf.Ticker(ticker)
        # yfinance>=0.2.40: t.news -> список словарей с providerPublishTime (unix)
        news = getattr(t, "news", []) or []
        if not isinstance(news, list):
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        cnt = 0
        for n in news:
            ts = n.get("providerPublishTime")
            if ts is None:  # иногда поле другое
                ts = n.get("providerPublishTimeUtc")
            if ts is None:
                continue
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            if dt >= cutoff:
                cnt += 1
        return cnt
    except Exception:
        return 0

def recent_filings_count_sec(ticker: str, hours: int = 24) -> int:
    """
    Считаем свежие 8-K/PR-похожие файлинги за последние 'hours' часов через SEC search-index.
    """
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    q = {
        "query": {"query_string": {"query": f'ticker:"{ticker}" AND filedAt:>={cutoff} AND (formType:"8-K" OR formType:"6-K")'}},
        "from": 0, "size": 50,
        "sort": [{"filedAt": {"order": "desc"}}],
        "highlight": False,
    }
    try:
        r = requests.post("https://efts.sec.gov/LATEST/search-index", json=q, timeout=25,
                          headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code != 200:
            return 0
        data = r.json()
        return int(data.get("hits", {}).get("total", {}).get("value", 0))
    except Exception:
        return 0

# ==== Финальный проход ====
def analyze_ticker(ticker: str):
    hist = get_hist(ticker)
    if hist is None: return None

    in_rng, last_px = in_price_range(hist)
    if not in_rng:
        return None

    v_spike, v_last, v_base = volume_spike(hist)
    p_move, pct = price_move(hist)

    news_yf = recent_news_count_yf(ticker, hours=24)
    filings = recent_filings_count_sec(ticker, hours=24)
    news_total = news_yf + filings
    news_hit = news_total >= NEWS_MIN

    hit = v_spike or p_move or news_hit
    if not hit:
        return None

    return {
        "ticker": ticker,
        "price": round(last_px, 2),
        "vol_spike": v_spike,
        "vol_last": int(v_last),
        "vol_base": int(v_base),
        "price_move_pct": round(pct, 2),
        "news_24h": int(news_total),
        "news_yf": int(news_yf),
        "filings_24h": int(filings),
    }

def fetch_watchlist() -> List[str]:
    # Собираем свежий список: IPO (Nasdaq) + спин-оффы (SEC 10-12B)
    ipos = fetch_nasdaq_ipos()
    spins = fetch_sec_spinoffs(days_back=SEC_DAYS_BACK)
    all_tick = sorted(ipos.union(spins))
    # Сохраняем «источник истины» в GCS
    if GCS_BUCKET and all_tick:
        gcs_save_text(GCS_BUCKET, CAT_WATCHLIST_GCS_BLOB, "\n".join(all_tick))
    # Если вдруг страница упала — берём прошлый список из GCS
    if not all_tick and GCS_BUCKET:
        cached = gcs_load_text(GCS_BUCKET, CAT_WATCHLIST_GCS_BLOB, "")
        all_tick = [t.strip().upper() for t in cached.splitlines() if t.strip()]
    return all_tick

def main():
    watch = fetch_watchlist()
    logging.info(f"Watchlist size: {len(watch)}")

    hits = []
    for t in watch:
        try:
            res = analyze_ticker(t)
            if res:
                hits.append(res)
            time.sleep(YF_SLEEP)
        except Exception as e:
            logging.info(f"{t}: skip ({e})")

    if not hits:
        logging.info("No catalysts today in range.")
        return

    # Формируем краткое уведомление и подробный хвост
    lines = []
    for h in hits[:60]:
        flags = []
        if h["vol_spike"]: flags.append(f"Vol≥{VOL_SPIKE_MULT}× ({h['vol_last']}/{max(1,h['vol_base'])})")
        if abs(h["price_move_pct"]) >= PRICE_MOVE_PCT: flags.append(f"Δ{h['price_move_pct']}%")
        if h["news_24h"] >= NEWS_MIN: flags.append(f"News:{h['news_24h']} (YF {h['news_yf']}/SEC {h['filings_24h']})")
        lines.append(f"{h['ticker']} (${h['price']}) — " + ", ".join(flags) if flags else f"{h['ticker']} (${h['price']})")

    msg = (
        f"🚀 <b>Catalyst Watch</b> ${PRICE_MIN:.0f}–${PRICE_MAX:.0f}\n"
        f"Триггеры: Vol≥{VOL_SPIKE_MULT}×, |Δ|≥{PRICE_MOVE_PCT}%, News≥{NEWS_MIN}\n"
        + "\n".join(lines)
    )
    tg_send(msg)
    logging.info(f"Alerts: {len(hits)} tickers")

if __name__ == "__main__":
    main()
