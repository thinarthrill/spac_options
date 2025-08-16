# catalyst_watch.py (resilient)
import os, json, time, logging, requests, math
from datetime import datetime, timedelta, timezone
from typing import Set, List, Tuple
from bs4 import BeautifulSoup
import yfinance as yf
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ==== ENV ====
PRICE_MIN = float(os.getenv("CAT_PRICE_MIN", "3.0"))
PRICE_MAX = float(os.getenv("CAT_PRICE_MAX", "15.0"))
VOL_SPIKE_MULT = float(os.getenv("CAT_VOL_SPIKE_MULT", "3.0"))
PRICE_MOVE_PCT = float(os.getenv("CAT_PRICE_MOVE_PCT", "8.0"))
NEWS_MIN = int(os.getenv("CAT_NEWS_MIN", "1"))
YF_SLEEP = float(os.getenv("CAT_YF_SLEEP", "0.6"))
SEC_DAYS_BACK = int(os.getenv("SEC_DAYS_BACK", "14"))
CAT_TEST_LIMIT = int(os.getenv("CAT_TEST_LIMIT", "0"))  # 0 = без лимита

# сеть/таймауты
REQ_TIMEOUT = int(os.getenv("REQ_TIMEOUT", "25"))
REQ_RETRIES = int(os.getenv("REQ_RETRIES", "3"))
REQ_BACKOFF = float(os.getenv("REQ_BACKOFF", "1.8"))
NASDAQ_DISABLED = os.getenv("NASDAQ_DISABLED", "0") == "1"

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
        raise ValueError("No GCS key (GCS_KEY_JSON or GOOGLE_APPLICATION_CREDENTIALS)")
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
            timeout=REQ_TIMEOUT,
        )
    except Exception as e:
        logging.warning(f"TG send error: {e}")

# ==== сетевой helper с ретраями ====
def http_get(url: str, headers: dict = None, params: dict = None) -> requests.Response:
    headers = {"User-Agent": "Mozilla/5.0"} | (headers or {})
    last_err = None
    for i in range(REQ_RETRIES):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=REQ_TIMEOUT)
            if r.status_code == 200:
                return r
            last_err = Exception(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e
        sleep_s = REQ_BACKOFF ** i
        logging.info(f"[GET retry {i+1}/{REQ_RETRIES}] {url} ({last_err}); sleep {sleep_s:.1f}s")
        time.sleep(sleep_s)
    raise last_err or Exception("request failed")

def http_post_json(url: str, body: dict, headers: dict = None) -> requests.Response:
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"} | (headers or {})
    last_err = None
    for i in range(REQ_RETRIES):
        try:
            r = requests.post(url, json=body, headers=headers, timeout=REQ_TIMEOUT)
            if r.status_code == 200:
                return r
            last_err = Exception(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e
        sleep_s = REQ_BACKOFF ** i
        logging.info(f"[POST retry {i+1}/{REQ_RETRIES}] {url} ({last_err}); sleep {sleep_s:.1f}s")
        time.sleep(sleep_s)
    raise last_err or Exception("request failed")

# ==== Источник 1: IPO (Nasdaq) + запасной (StockAnalysis) ====
def fetch_nasdaq_ipos() -> Set[str]:
    if NASDAQ_DISABLED:
        logging.info("Nasdaq IPO fetch disabled by env")
        return set()
    url = "https://www.nasdaq.com/market-activity/ipos"
    r = http_get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    tickers: Set[str] = set()
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            tds = row.find_all("td")
            if not tds: continue
            t = tds[0].get_text(strip=True).upper()
            base = t.replace(".U","").replace(".W","")
            if 1 <= len(base) <= 5 and base.isalpha():
                tickers.add(base)
    logging.info(f"Nasdaq IPO: {len(tickers)}")
    return tickers

def fetch_stockanalysis_ipos() -> Set[str]:
    # запасной источник: простая таблица
    url = "https://stockanalysis.com/ipos/"
    r = http_get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    tickers: Set[str] = set()
    for a in soup.select("table a[href*='/ipos/']"):
        t = a.get_text(strip=True).upper()
        base = t.replace(".U","").replace(".W","")
        if 1 <= len(base) <= 5 and base.isalpha():
            tickers.add(base)
    logging.info(f"StockAnalysis IPO: {len(tickers)}")
    return tickers

# ==== Источник 2: Spin-offs (SEC 10-12B) ====
def fetch_sec_spinoffs(days_back: int = 14) -> Set[str]:
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    body = {
        "query": {"query_string": {"query": f'formType:"10-12B" AND filedAt:>={since}'}},
        "from": 0, "size": 200,
        "sort": [{"filedAt": {"order": "desc"}}],
        "highlight": False,
    }
    r = http_post_json("https://efts.sec.gov/LATEST/search-index", body)
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
    vol = hist["Volume"].dropna()
    if len(vol) < 12:
        return (False, 0.0, 0.0)
    last = float(vol.iloc[-1])
    base = float(vol.iloc[-11:-1].mean())
    if base <= 0:
        return (False, last, base)
    return (last >= VOL_SPIKE_MULT * base, last, base)

def price_move(hist) -> Tuple[bool, float]:
    close = hist["Close"].dropna()
    if len(close) < 2:
        return (False, 0.0)
    pct = (float(close.iloc[-1]) / float(close.iloc[-2]) - 1.0) * 100.0
    return (pct >= PRICE_MOVE_PCT, pct)

def in_price_range(hist) -> Tuple[bool, float]:
    last_close = float(hist["Close"].dropna().iloc[-1])
    return (PRICE_MIN <= last_close <= PRICE_MAX, last_close)

def recent_news_count_yf(ticker: str, hours: int = 24) -> int:
    try:
        t = yf.Ticker(ticker)
        news = getattr(t, "news", []) or []
        if not isinstance(news, list):
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        cnt = 0
        for n in news:
            ts = n.get("providerPublishTime") or n.get("providerPublishTimeUtc")
            if ts is None: continue
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            if dt >= cutoff:
                cnt += 1
        return cnt
    except Exception:
        return 0

def recent_filings_count_sec(ticker: str, hours: int = 24) -> int:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    q = {
        "query": {"query_string": {"query": f'ticker:"{ticker}" AND filedAt:>={cutoff} AND (formType:"8-K" OR formType:"6-K")'}},
        "from": 0, "size": 50,
        "sort": [{"filedAt": {"order": "desc"}}], "highlight": False,
    }
    try:
        r = http_post_json("https://efts.sec.gov/LATEST/search-index", q)
        data = r.json()
        return int(data.get("hits", {}).get("total", {}).get("value", 0))
    except Exception:
        return 0

def analyze_ticker(ticker: str):
    hist = get_hist(ticker)
    if hist is None: return None
    in_rng, last_px = in_price_range(hist)
    if not in_rng: return None

    v_spike, v_last, v_base = volume_spike(hist)
    p_move, pct = price_move(hist)
    news_yf = recent_news_count_yf(ticker, hours=24)
    filings = recent_filings_count_sec(ticker, hours=24)
    news_total = news_yf + filings

    hit = v_spike or p_move or (news_total >= NEWS_MIN)
    if not hit: return None

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

# ==== Watchlist сбор с fallback ====
def fetch_watchlist() -> List[str]:
    tickers: Set[str] = set()
    # 1) Nasdaq (с ретраями)
    try:
        if not NASDAQ_DISABLED:
            tickers |= fetch_nasdaq_ipos()
    except Exception as e:
        logging.warning(f"Nasdaq IPO failed: {e}")
    # 2) StockAnalysis как запасной
    if not tickers:
        try:
            tickers |= fetch_stockanalysis_ipos()
        except Exception as e:
            logging.warning(f"StockAnalysis IPO failed: {e}")
    # 3) SEC 10-12B
    try:
        tickers |= fetch_sec_spinoffs(days_back=SEC_DAYS_BACK)
    except Exception as e:
        logging.warning(f"SEC 10-12B failed: {e}")

    all_tick = sorted(tickers)
    logging.info(f"Watchlist collected: {len(all_tick)} tickers")

    # Сохраняем в GCS; если пусто — берём кэш
    if all_tick and GCS_BUCKET:
        gcs_save_text(GCS_BUCKET, CAT_WATCHLIST_GCS_BLOB, "\n".join(all_tick))
    if not all_tick and GCS_BUCKET:
        cached = gcs_load_text(GCS_BUCKET, CAT_WATCHLIST_GCS_BLOB, "")
        all_tick = [t.strip().upper() for t in cached.splitlines() if t.strip()]
        logging.info(f"Loaded cached watchlist: {len(all_tick)} from GCS")

    # тест-лимит
    if CAT_TEST_LIMIT and len(all_tick) > CAT_TEST_LIMIT:
        all_tick = all_tick[:CAT_TEST_LIMIT]
        logging.info(f"TEST MODE: limiting to {CAT_TEST_LIMIT} tickers")

    return all_tick

def main():
    watch = fetch_watchlist()
    logging.info(f"Processing {len(watch)} tickers...")

    hits = []
    for i, t in enumerate(watch, 1):
        try:
            logging.info(f"[{i}/{len(watch)}] {t}")
            res = analyze_ticker(t)
            if res: hits.append(res)
        except Exception as e:
            logging.info(f"{t}: skip ({e})")
        time.sleep(YF_SLEEP)

    if not hits:
        logging.info("No catalysts today in range.")
        return

    lines = []
    for h in hits[:60]:
        flags = []
        if h["vol_spike"]: flags.append(f"Vol≥{VOL_SPIKE_MULT}× ({h['vol_last']}/{max(1,h['vol_base'])})")
        if abs(h["price_move_pct"]) >= PRICE_MOVE_PCT: flags.append(f"Δ{h['price_move_pct']}%")
        if h["news_24h"] >= NEWS_MIN: flags.append(f"News:{h['news_24h']} (YF {h['news_yf']}/SEC {h['filings_24h']})")
        lines.append(f"{h['ticker']} (${h['price']}) — " + ", ".join(flags) if flags else f"{h['ticker']} (${h['price']})")

    msg = (
        f"🚀 <b>Catalyst Watch</b> ${PRICE_MIN:.0f}–${PRICE_MAX:.0f}\n"
        f"Triggers: Vol≥{VOL_SPIKE_MULT}×, |Δ|≥{PRICE_MOVE_PCT}%, News≥{NEWS_MIN}\n"
        + "\n".join(lines)
    )
    tg_send(msg)
    logging.info(f"Alerts: {len(hits)} tickers")

if __name__ == "__main__":
    main()


