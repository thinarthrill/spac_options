# spac_options_bot_gcs.py
import os
import time
import json
import math
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import yfinance as yf
import requests
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ========= ПАРАМЕТРЫ =========
# Список тикеров: через запятую в .env (например: "IPOF,IPOD,KVSA")
WATCHLIST_ENV = os.getenv("WATCHLIST", "")
WATCHLIST = [t.strip().upper() for t in WATCHLIST_ENV.split(",") if t.strip()] or [
    # Резервный пример (замените на актуальные SPAC)
    "IPOF", "KVSA"
]

PRICE_MIN = float(os.getenv("PRICE_MIN", "9.0"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "11.0"))
AVG_VOL_MAX = int(os.getenv("AVG_VOL_MAX", "500000"))  # «тихий» по среднему дневному объёму

NEAREST_EXPIRIES_TO_CHECK = int(os.getenv("NEAREST_EXPIRIES_TO_CHECK", "2"))  # сколько ближайших экспираций суммировать
SPIKE_MULTIPLIER = float(os.getenv("SPIKE_MULTIPLIER", "3.0"))  # «в разы»: 3x по умолчанию
EMA_ALPHA = float(os.getenv("EMA_ALPHA", "0.2"))  # сглаживание для средней опционного объёма

CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "900"))  # 15 минут по умолчанию
YF_SLEEP_BETWEEN_TICKERS = float(os.getenv("YF_SLEEP_BETWEEN_TICKERS", "1.0"))

# ========= GCS =========
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_STATE_BLOB = os.getenv("GCS_STATE_BLOB", "spac/state.json")
GCS_LOG_PREFIX = os.getenv("GCS_LOG_PREFIX", "spac/logs")

_gcs_client = None
def _get_gcs():
    global _gcs_client
    if _gcs_client is None:
        key_str = os.getenv("GCS_KEY_JSON")
        if not key_str:
            raise ValueError("❌ GCS_KEY_JSON не установлена или пуста")

        # Преобразуем строку обратно в dict
        try:
            key_dict = json.loads(key_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"❌ Ошибка парсинга GCS_KEY_JSON: {e}")

        # Сохраняем как файл
        with open("gcs_key.json", "w", encoding="utf-8") as f:
            json.dump(key_dict, f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcs_key.json"
        _gcs_client = storage.Client()
        logging.info("✅ GCS клиент создан через ключ из GCS_KEY_JSON")
    return _gcs_client


def gcs_blob_exists(bucket_name: str, key: str) -> bool:
    client = _get_gcs()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(key)
    return blob.exists()

def gcs_load_json(default=None):
    default = {} if default is None else default
    if not GCS_BUCKET:
        logging.error("GCS_BUCKET не задан. Укажите его в .env")
        return default
    try:
        client = _get_gcs()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(GCS_STATE_BLOB)
        if not blob.exists():
            logging.info(f"GCS: {GCS_STATE_BLOB} отсутствует — стартуем с пустого состояния.")
            return default
        data = blob.download_as_text(encoding="utf-8")
        return json.loads(data)
    except Exception as e:
        logging.warning(f"GCS load error: {e} — возвращаю default.")
        return default

def gcs_save_json(obj: dict):
    if not GCS_BUCKET:
        logging.error("GCS_BUCKET не задан — пропускаю сохранение.")
        return
    try:
        client = _get_gcs()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(GCS_STATE_BLOB)
        payload = json.dumps(obj, ensure_ascii=False, indent=2)
        blob.upload_from_string(payload, content_type="application/json; charset=utf-8")
        logging.info(f"GCS: сохранено состояние {GCS_STATE_BLOB} ({len(payload)} байт).")
    except Exception as e:
        logging.error(f"GCS save error: {e}")

def gcs_append_daily_log(ticker: str, payload: dict):
    if not GCS_BUCKET:
        return
    try:
        client = _get_gcs()
        bucket = client.bucket(GCS_BUCKET)
        day = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"{GCS_LOG_PREFIX}/{ticker}/{day}.json"
        blob = bucket.blob(key)

        if blob.exists():
            try:
                arr = json.loads(blob.download_as_text(encoding="utf-8"))
                if not isinstance(arr, list):
                    arr = [arr]
            except Exception:
                arr = []
        else:
            arr = []

        arr.append(payload)
        blob.upload_from_string(json.dumps(arr, ensure_ascii=False), content_type="application/json; charset=utf-8")
    except Exception as e:
        logging.warning(f"GCS daily log error for {ticker}: {e}")

# ========= TELEGRAM =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def tg_send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info(f"[TG MOCK] {text}")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=15
        )
        if resp.status_code != 200:
            logging.warning(f"TG send failed: {resp.status_code} {resp.text}")
    except Exception as e:
        logging.warning(f"TG send error: {e}")

# ========= ДАННЫЕ ПО АКЦИИ/ОПЦИОНАМ =========
def get_price_and_avg_volume(ticker: str) -> Tuple[float, float]:
    """
    Возвращает (last_close, avg_volume_30d).
    """
    t = yf.Ticker(ticker)
    hist = t.history(period="30d", interval="1d", auto_adjust=False)
    if hist is None or hist.empty:
        raise ValueError("Нет дневной истории")
    last_close = float(hist["Close"].dropna().iloc[-1])
    vol_series = hist["Volume"].dropna()
    avg_vol = float(vol_series.mean()) if not vol_series.empty else 0.0
    return last_close, avg_vol

def get_options_snapshot(ticker: str, nearest_n: int) -> Dict[str, Any]:
    """
    Возвращает сводку по опционам:
    {
      'has_options': bool,
      'expiries': [list of str],
      'total_option_volume': int,   # суммарный volume calls+puts для ближайших N экспираций
      'by_expiry': {expiry: {'calls_volume': int, 'puts_volume': int}}
    }
    """
    t = yf.Ticker(ticker)
    expiries: List[str] = t.options or []
    has_options = len(expiries) > 0

    snapshot = {
        "has_options": has_options,
        "expiries": expiries,
        "total_option_volume": 0,
        "by_expiry": {}
    }
    if not has_options:
        return snapshot

    for expiry in expiries[:nearest_n]:
        try:
            chain = t.option_chain(expiry=expiry)
            calls = chain.calls if hasattr(chain, "calls") else None
            puts = chain.puts if hasattr(chain, "puts") else None

            calls_vol = int(calls["volume"].fillna(0).sum()) if calls is not None and "volume" in calls.columns else 0
            puts_vol = int(puts["volume"].fillna(0).sum()) if puts is not None and "volume" in puts.columns else 0

            snapshot["by_expiry"][expiry] = {"calls_volume": calls_vol, "puts_volume": puts_vol}
            snapshot["total_option_volume"] += calls_vol + puts_vol
        except Exception as e:
            logging.warning(f"{ticker}: не удалось загрузить опционный слой {expiry}: {e}")
            continue

    return snapshot

# ========= ЛОГИКА =========
def is_quiet_spac(price: float, avg_vol: float) -> bool:
    return (PRICE_MIN <= price <= PRICE_MAX) and (avg_vol <= AVG_VOL_MAX)

def process_ticker(ticker: str, state: Dict[str, Any]) -> None:
    # Инициализация узла состояния
    node = state.get(ticker, {
        "had_options_before": False,
        "known_expiries": [],
        "option_volume_ema": None,  # EMA по суммарному объёму опционов
        "last_alert_ts": None
    })

    try:
        price, avg_vol = get_price_and_avg_volume(ticker)
    except Exception as e:
        logging.info(f"{ticker}: нет цен/объёма ({e})")
        state[ticker] = node
        return

    if not is_quiet_spac(price, avg_vol):
        logging.info(f"{ticker}: не «тихий SPAC» (price={price:.2f}, avgVol={int(avg_vol)})")
        state[ticker] = node
        return

    snapshot = get_options_snapshot(ticker, NEAREST_EXPIRIES_TO_CHECK)
    has_options_now = snapshot["has_options"]
    expiries_now = snapshot["expiries"]
    total_opt_vol_now = snapshot["total_option_volume"]

    # 1) Событие: опционы появились впервые
    if has_options_now and not node.get("had_options_before", False):
        node["had_options_before"] = True
        node["known_expiries"] = expiries_now
        msg = (
            f"🟢 <b>{ticker}</b>: впервые появились опционы\n"
            f"Цена: ${price:.2f}, средн. объём акции: {int(avg_vol)}\n"
            f"Экспирации (первые {NEAREST_EXPIRIES_TO_CHECK}): {', '.join(expiries_now[:NEAREST_EXPIRIES_TO_CHECK]) or '—'}"
        )
        tg_send(msg)

    # 2) EMA и всплеск
    prev_ema = node.get("option_volume_ema", None)
    new_ema = float(total_opt_vol_now) if prev_ema is None else (EMA_ALPHA * total_opt_vol_now + (1 - EMA_ALPHA) * prev_ema)
    node["option_volume_ema"] = new_ema

    is_spike = False
    if prev_ema is not None and total_opt_vol_now > 0:
        if total_opt_vol_now >= SPIKE_MULTIPLIER * max(prev_ema, 1.0):
            is_spike = True

    if is_spike:
        by_expiry_lines = []
        for exp, d in snapshot["by_expiry"].items():
            by_expiry_lines.append(f"{exp}: C={d['calls_volume']}, P={d['puts_volume']}")
        details = "\n".join(by_expiry_lines) if by_expiry_lines else "нет данных"

        msg = (
            f"🔥 <b>{ticker}</b>: всплеск объёма опционов (≥{SPIKE_MULTIPLIER:.1f}×)\n"
            f"Цена: ${price:.2f}, средн. объём акции: {int(avg_vol)}\n"
            f"Текущий opt volume: {int(total_opt_vol_now)}, EMA: {int(prev_ema or 0)} → {int(new_ema)}\n"
            f"По экспирациям:\n{details}"
        )
        tg_send(msg)

    # 3) Дневной лог (GCS)
    try:
        gcs_append_daily_log(ticker, {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "price": round(price, 4),
            "avg_volume_30d": int(avg_vol),
            "total_option_volume": int(total_opt_vol_now),
            "ema": float(round(new_ema, 2)),
            "expiries_checked": expiries_now[:NEAREST_EXPIRIES_TO_CHECK],
        })
    except Exception as e:
        logging.warning(f"{ticker}: ошибка записи дневного лога: {e}")

    # Сохраняем
    state[ticker] = node

def load_state() -> dict:
    return gcs_load_json(default={})

def save_state(state: dict) -> None:
    gcs_save_json(state)

if __name__ == "__main__":
    state = load_state()
    logging.info(f"Запуск проверки {len(WATCHLIST)} тикеров...")
    for tkr in WATCHLIST:
        try:
            process_ticker(tkr, state)
        except Exception as e:
            logging.warning(f"{tkr}: ошибка обработки: {e}")
        time.sleep(YF_SLEEP_BETWEEN_TICKERS)
    save_state(state)
    logging.info("✅ Проверка завершена")

