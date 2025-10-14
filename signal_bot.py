#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, csv, math, logging, asyncio, random
from datetime import datetime, timezone
from copy import deepcopy

import numpy as np
import pandas as pd
import aiohttp

# aiogram 3.7+
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ===================== CONFIG =====================

VERSION = "v14.1 hotfix6 (NG feed backoff+fallback)"

BOT_TOKEN = "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"
OWNER_ID  = 6784470762

USE_5M_CONFIRM = True
STRUCT_BARS    = 1

# Минимальные буферы (порог шума)
MIN_SPREAD = {"BTC": 277.5, "NG": 0.004, "XAU": 0.25}

# Динамика буфера
ATR_K   = {"BTC": 7.0, "NG": 0.40, "XAU": 0.55}
RANGE_K = {"BTC": 0.90, "NG": 0.85, "XAU": 0.90}

# Доп. подушка к уровням TP/SL (поверх buffer)
BUF_K_TO_LEVEL = {"BTC": 1.15, "NG": 0.25, "XAU": 0.30}

# Минимальный RR и ограничение ширины стопа
RR_MIN     = 1.30
MAX_SL_ATR = 1.40

# Порог допуска по «уверенности»
CONF_MIN = 0.70

SYMBOLS = {
    "BTC": {"name": "BTC-USD",   "tf": "1m"},
    "NG":  {"name": "NG=F",      "tf": "1m"},
    "XAU": {"name": "XAUUSD=X",  "tf": "1m"},
}

# Периоды
ATR_PERIOD = 14
EMA_FAST   = 12
EMA_SLOW   = 26

# Петля
POLL_SEC         = 5
COOLDOWN_SEC     = 20
GUARD_AFTER_SL_S = 15 * 60
BOOT_COOLDOWN_S  = 60

STALE_FEED_SEC   = 600

# Сеть/ретраи
HTTP_TIMEOUT = 10
HTTP_RETRIES = 3
HTTP_RETRY_SLEEP = 0.8

COMMON_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"),
    "Accept": "*/*",
    "Connection": "keep-alive",
}
YA_URLS = [
    "https://query1.finance.yahoo.com",
    "https://query2.finance.yahoo.com",
]

# Файлы
STATS_JSON = "gv_stats.json"
STATE_JSON = "gv_state.json"
TRADES_CSV = "gv_trades.csv"

# Источники цен
CC_URL     = "https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=200"
STOOQ_TPL  = "https://stooq.com/q/d/l/?s={ticker}&i=1"

# ===================== STATE =====================

mode = "BTC"
requested_mode = "BTC"
last_mode_switch_ts = 0.0
MODE_SWITCH_DEBOUNCE = 2.0

trade = {"BTC": None, "NG": None, "XAU": None}
cooldown_until = {"BTC": 0, "NG": 0, "XAU": 0}
last_candle_close_ts = {"BTC": 0, "NG": 0, "XAU": 0}
boot_ts = time.time()

last_seen_idx   = {"BTC": -1, "NG": -1, "XAU": -1}
last_signal_idx = {"BTC": -1, "NG": -1, "XAU": -1}

# Анти-лимит Yahoo (когда снова можно пробовать Yahoo)
YAHOO_BACKOFF_UNTIL = {"NG": 0, "XAU": 0, "BTC": 0}

DEFAULT_PARAMS = {
    "tp_mul": 1.80, "sl_mul": 1.20, "risk": 1.00,
    "win": 0, "loss": 0, "streak_win": 0, "streak_loss": 0,
    "last_sig": None, "last_sig_ts": 0, "last_outcome": None,
}

def load_json(path, default):
    if not os.path.exists(path): return deepcopy(default)
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except: return deepcopy(default)

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"save_json({path}) -> {e}")

stats = load_json(STATS_JSON, {})
state  = load_json(STATE_JSON, {"mode": mode})

def save_stats(): save_json(STATS_JSON, stats)
def save_state():
    state["mode"] = mode
    save_json(STATE_JSON, state)

def append_trade(row):
    newf = not os.path.exists(TRADES_CSV)
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if newf: w.writeheader()
        w.writerow(row)

# ===================== TECH =====================

def now_iso():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="seconds")

def ema(series: pd.Series, span: int):
    return series.ewm(span=span, adjust=False).mean()

def true_range(h, l, c):
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr

def atr(df, n):
    return true_range(df["High"], df["Low"], df["Close"]).rolling(n).mean()

def rnd(sym: str, x: float):
    return round(x, 1 if sym == "BTC" else (4 if sym == "NG" else 2))

def sig_id(symbol, side, trend, atr_val, tf):
    return f"{symbol}|{side}|{trend}|{round(float(atr_val),1)}|{tf}"

def allow_after_sl(symbol, signature, ts):
    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    return not (p.get("last_outcome") == "SL" and p.get("last_sig") == signature and ts - float(p.get("last_sig_ts",0)) < GUARD_AFTER_SL_S)

# ===================== PRICE =====================

async def _http_get_text(session: aiohttp.ClientSession, url: str) -> str:
    for i in range(HTTP_RETRIES):
        try:
            await asyncio.sleep(random.uniform(0.08, 0.20))
            async with session.get(url, headers=COMMON_HEADERS, timeout=HTTP_TIMEOUT) as r:
                if r.status == 200:
                    return await r.text()
                if r.status in (429, 503):
                    logging.warning(f"429/503 on GET text: {url}")
                    await asyncio.sleep(1.0 * (i + 1))
                    continue
        except Exception as e:
            logging.warning(f"GET text fail [{i+1}/{HTTP_RETRIES}] {url}: {e}")
        await asyncio.sleep(HTTP_RETRY_SLEEP)
    return ""

async def _http_get_json(session: aiohttp.ClientSession, url: str) -> dict:
    for i in range(HTTP_RETRIES):
        try:
            await asyncio.sleep(random.uniform(0.08, 0.20))
            async with session.get(url, headers=COMMON_HEADERS, timeout=HTTP_TIMEOUT) as r:
                if r.status == 200:
                    return await r.json()
                if r.status in (429, 503):
                    logging.warning(f"429/503 on GET json: {url}")
                    await asyncio.sleep(1.0 * (i + 1))
                    continue
        except Exception as e:
            logging.warning(f"GET json fail [{i+1}/{HTTP_RETRIES}] {url}: {e}")
        await asyncio.sleep(HTTP_RETRY_SLEEP)
    return {}

# ===================== PRICE (robust NG/XAU) =====================

# --- новые настройки для фетчей ---
ROBUST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
YAHOO_TIMEOUT_S = 12
YAHOO_RETRIES   = 4           # максимум попыток на один запрос
YAHOO_BACKOFF0  = 0.9         # стартовый бэкофф (сек)
YAHOO_JITTER    = 0.35        # случайная примесь

# Кэш, чтобы не ддосить источники и не ловить 429
# хранит: {symbol: {"ts": last_fetch_time, "df": DataFrame, "feed":"yahoo/stooq"}}
_prices_cache = {}

def _now():
    return time.time()

async def _http_get_text(session: aiohttp.ClientSession, url: str) -> str:
    # (оставь как было) — нужен для stooq
    for i in range(HTTP_RETRIES):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
                if r.status == 200:
                    return await r.text()
                await asyncio.sleep(0.25)
        except Exception as e:
            logging.warning(f"GET text fail [{i+1}/{HTTP_RETRIES}] {url}: {e}")
        await asyncio.sleep(HTTP_RETRY_SLEEP)
    return ""

async def _http_get_json_robust(session: aiohttp.ClientSession, url: str) -> dict:
    """Yahoo v8 с нормальными заголовками, бэкоффом и ретраями."""
    backoff = YAHOO_BACKOFF0
    for i in range(YAHOO_RETRIES):
        try:
            async with session.get(url, timeout=YAHOO_TIMEOUT_S, headers=ROBUST_HEADERS) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                # 429/503 — подождём и повторим
                if r.status in (429, 503):
                    logging.warning(f"YAHOO {r.status} on GET json: {url}")
                    await asyncio.sleep(backoff + (random.random() * YAHOO_JITTER))
                    backoff *= 1.7
                    continue
                # прочие статусы — бессмысленно ретраить
                logging.warning(f"YAHOO status {r.status}: {url}")
                return {}
        except Exception as e:
            logging.warning(f"YAHOO GET json fail [{i+1}/{YAHOO_RETRIES}] {url}: {e}")
        await asyncio.sleep(backoff + (random.random() * YAHOO_JITTER))
        backoff *= 1.6
    return {}

def _df_from_yahoo_v8(payload: dict) -> pd.DataFrame:
    """Парсит ответ chart/v8: берём timestamps + indicators.quote[*]."""
    try:
        r = payload.get("chart", {}).get("result", [])[0]
        ts = r.get("timestamp", [])
        q  = r.get("indicators", {}).get("quote", [])[0]
        if not ts or not q: 
            return pd.DataFrame()
        df = pd.DataFrame({
            "Open":  q.get("open",  []),
            "High":  q.get("high",  []),
            "Low":   q.get("low",   []),
            "Close": q.get("close", []),
        }, index=pd.to_datetime(ts, unit="s"))
        # убираем «дыры» — ffill/bfill, затем дроп нулей, хвост 300
        df = df.replace([None, np.nan], method="ffill").replace([None, np.nan], method="bfill")
        df = df.dropna().tail(300).reset_index(drop=True)
        # фильтр нулевых/абсурдных
        for col in ("Open","High","Low","Close"):
            df = df[df[col] > 0]
        return df.tail(300).reset_index(drop=True)
    except Exception as e:
        logging.warning(f"Yahoo v8 parse error: {e}")
        return pd.DataFrame()

def df_from_stooq_csv(text: str):
    """Оставь как было, но чуть надёжнее на пустых ответах."""
    try:
        from io import StringIO
        if not text or "Date,Open,High,Low,Close" not in text:
            return pd.DataFrame()
        df = pd.read_csv(StringIO(text))
        if not {"Open","High","Low","Close"}.issubset(df.columns):
            return pd.DataFrame()
        return df.tail(300).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

async def _get_df_ng_yahoo(session: aiohttp.ClientSession) -> pd.DataFrame:
    # NG=F, 1m/1d — как у тебя, но с robust get + парсером
    url = "https://query1.finance.yahoo.com/v8/finance/chart/NG%3DF?interval=1m&range=1d"
    data = await _http_get_json_robust(session, url)
    return _df_from_yahoo_v8(data)

async def _get_df_xau_yahoo(session: aiohttp.ClientSession) -> pd.DataFrame:
    url = "https://query1.finance.yahoo.com/v8/finance/chart/XAUUSD%3DX?interval=1m&range=1d"
    data = await _http_get_json_robust(session, url)
    return _df_from_yahoo_v8(data)

async def _get_df_ng_stooq(session: aiohttp.ClientSession) -> pd.DataFrame:
    txt = await _http_get_text(session, "https://stooq.com/q/d/l/?s=ng.f&i=1")
    return df_from_stooq_csv(txt)

async def _get_df_xau_stooq(session: aiohttp.ClientSession) -> pd.DataFrame:
    txt = await _http_get_text(session, "https://stooq.com/q/d/l/?s=xauusd&i=1")
    return df_from_stooq_csv(txt)

async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
    """
    ЕДИНСТВЕННОЕ место, которое дергает движок.
    Тут добавлен кэш (10–15 c для NG/XAU), robust Yahoo и stooq-фолбэк.
    BTC оставляю как было (CryptoCompare), но тоже подхватываю headers/hb.
    """
    global last_candle_close_ts, _prices_cache

    # кэширование для NG/XAU (чтобы не ловить 429)
    cache_ttl = 12.0 if symbol in ("NG", "XAU") else 2.0
    c = _prices_cache.get(symbol)
    now_ts = _now()
    if c and (now_ts - c["ts"] < cache_ttl) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]

    if symbol == "BTC":
        data = await _http_get_json_robust(session, CC_URL)  # можно оставить твой _http_get_json; я просто унифицировал
        try:
            if data.get("Response") != "Success":
                return pd.DataFrame()
            arr = data["Data"]["Data"]
            df = pd.DataFrame(arr)
            if "time" in df.columns:
                last_ts = int(df["time"].iloc[-1])
                last_candle_close_ts["BTC"] = last_ts
            df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
            df = df[["Open","High","Low","Close"]].tail(300).reset_index(drop=True)
            _prices_cache["BTC"] = {"ts": now_ts, "df": df, "feed":"cc"}
            return df
        except Exception as e:
            logging.warning(f"BTC df parse error: {e}")
            return pd.DataFrame()

    if symbol == "NG":
        # 1) пытаемся Yahoo
        df = await _get_df_ng_yahoo(session)
        if not df.empty:
            last_candle_close_ts["NG"] = time.time()  # статус «живёт»
            _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
            return df
        # 2) фолбэк stooq
        df = await _get_df_ng_stooq(session)
        if not df.empty:
            last_candle_close_ts["NG"] = time.time()
            _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"stooq"}
            return df
        # 3) последний удачный кэш — лучше, чем пустота
        if c and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
            return c["df"]
        return pd.DataFrame()

    if symbol == "XAU":
        df = await _get_df_xau_yahoo(session)
        if not df.empty:
            last_candle_close_ts["XAU"] = time.time()
            _prices_cache["XAU"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
            return df
        df = await _get_df_xau_stooq(session)
        if not df.empty:
            last_candle_close_ts["XAU"] = time.time()
            _prices_cache["XAU"] = {"ts": now_ts, "df": df, "feed":"stooq"}
            return df
        if c and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
            return c["df"]
        return pd.DataFrame()

    # неизвестный символ
    return pd.DataFrame()

def dynamic_buffer(symbol: str, df: pd.DataFrame, atr_now: float) -> float:
    try:
        rng = float(df["High"].iloc[-2] - df["Low"].iloc[-2])
    except Exception:
        rng = 0.0
    base = MIN_SPREAD.get(symbol, 0.0)
    from_atr   = float(atr_now) * ATR_K.get(symbol, 1.0)
    from_range = float(rng)     * RANGE_K.get(symbol, 1.0)
    return max(base, from_atr, from_range)

# ===================== STRATEGY =====================

def trend_side(df: pd.DataFrame) -> str:
    c = df["Close"]
    return "UP" if ema(c, EMA_FAST).iloc[-1] > ema(c, EMA_SLOW).iloc[-1] else "DOWN"

def _confidence(rr: float, trend_ok: bool, atr_now: float, symbol: str) -> float:
    rr_part = max(0.0, min(1.0, (rr - 1.0) / 1.2))
    trend_part = 0.1 if trend_ok else 0.0
    tiny_atr_penalty = 0.05 if atr_now < (MIN_SPREAD.get(symbol, 0.0) * 0.2) else 0.0
    conf = rr_part + trend_part - tiny_atr_penalty
    return max(0.0, min(1.0, conf))

def build_setup(df: pd.DataFrame, symbol: str, tf_label: str):
    if df is None or df.empty or len(df) < max(ATR_PERIOD, EMA_SLOW) + 3: return None

    df = df.copy()
    df["ATR"] = atr(df, ATR_PERIOD)
    atr_now = float(df["ATR"].iloc[-1])
    if not np.isfinite(atr_now) or atr_now <= 0: return None

    side_tr = trend_side(df)
    last = df.iloc[-2]                    # закрытый бар
    entry = float(last["Close"])

    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    tp_mul = float(p["tp_mul"]) * float(p["risk"])
    sl_mul = float(p["sl_mul"]) * float(1.0 / p["risk"])

    if side_tr == "UP":
        side = "BUY"
        raw_tp = entry + tp_mul * atr_now
        raw_sl = entry - sl_mul * atr_now
        sl_cap = entry - MAX_SL_ATR * atr_now
        sl = max(raw_sl, sl_cap)
        tp = raw_tp
    else:
        side = "SELL"
        raw_tp = entry - tp_mul * atr_now
        raw_sl = entry + sl_mul * atr_now
        sl_cap = entry + MAX_SL_ATR * atr_now
        sl = min(raw_sl, sl_cap)
        tp = raw_tp

    rr = abs(tp - entry) / max(abs(entry - sl), 1e-9)
    if rr < RR_MIN: return None

    conf = _confidence(rr, True, atr_now, symbol)
    if conf < CONF_MIN:
        return None

    signature = sig_id(symbol, side, side_tr, atr_now, tf_label)
    return {
        "symbol": symbol, "tf": tf_label,
        "side": side, "trend": side_tr,
        "entry": entry, "tp": tp, "sl": sl,
        "atr": atr_now, "rr": rr, "conf": conf, "sig": signature
    }

def format_signal(setup, buffer):
    sym = setup["symbol"]; side = setup["side"]; tf = setup["tf"]
    add = BUF_K_TO_LEVEL.get(sym, 1.0) * buffer
    tp = setup["tp"] + (add if side == "BUY" else -add)
    sl = setup["sl"] - (add if side == "BUY" else -add)
    lines = [
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}",
        f"✅ TP: **{rnd(sym, tp)}**",
        f"🟥 SL: **{rnd(sym, sl)}**",
        f"Entry: {rnd(sym, setup['entry'])}  Spread≈{rnd(sym, buffer)}  ATR(14)≈{rnd(sym, setup['atr'])}  Conf: {int(setup['conf']*100)}%  Trend: {setup['trend']}"
    ]
    return "\n".join(lines)

# ===================== LEARNING =====================

def learn(symbol: str, outcome: str, sess: dict):
    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    if sess and "sig" in sess:
        p["last_sig"] = sess["sig"]; p["last_sig_ts"] = time.time(); p["last_outcome"] = outcome

    if outcome == "TP":
        p["win"] += 1; p["streak_win"] += 1; p["streak_loss"] = 0
        p["risk"]   = float(min(1.30, p["risk"] + 0.04))
        p["tp_mul"] = float(min(2.10, p["tp_mul"] * 1.02))
        p["sl_mul"] = float(max(1.10, p["sl_mul"] * 0.995))
    else:
        p["loss"] += 1; p["streak_loss"] += 1; p["streak_win"] = 0
        p["sl_mul"] = float(min(1.60, p["sl_mul"] * 1.04))
        p["risk"]   = float(max(0.80, p["risk"] - 0.04))
        if p["streak_loss"] >= 3:
            cooldown_until[symbol] = max(cooldown_until[symbol], time.time() + 60)

    stats[symbol] = p; save_stats()

def finish_trade(symbol: str, outcome: str, price_now: float):
    sess = trade[symbol]
    trade[symbol] = None
    cooldown_until[symbol] = time.time() + COOLDOWN_SEC
    if not sess: return
    try:
        rr = (sess["tp"]-sess["entry"]) if sess["side"]=="BUY" else (sess["entry"]-sess["tp"])
        rl = (sess["entry"]-sess["sl"]) if sess["side"]=="BUY" else (sess["sl"]-sess["entry"])
        append_trade({
            "ts_close": now_iso(), "symbol": symbol, "side": sess["side"],
            "entry": rnd(symbol, sess["entry"]), "tp": rnd(symbol, sess["tp"]),
            "sl": rnd(symbol, sess["sl"]), "outcome": outcome,
            "rr_ratio": round(float(rr)/max(float(rl),1e-9), 3),
            "life_sec": int(time.time()-sess.get("opened_at", time.time())),
            "sig": sess.get("sig","")
        })
    except Exception as e:
        logging.error(f"log append error: {e}")
    learn(symbol, outcome, sess)

# ===================== TELEGRAM =====================

router = Router()
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
dp  = Dispatcher()
dp.include_router(router)

def mode_title(m):
    return {"BTC":"BITCOIN (BTC-USD)","NG":"NATGAS (NG=F)","XAU":"GOLD (XAUUSD)","AUTO":"NATGAS+GOLD (AUTO)"}\
        .get(m,m)

async def _request_mode(new_mode: str, m: Message = None):
    global requested_mode
    requested_mode = new_mode
    save_state()
    if m:
        await m.answer(f"✅ Режим {new_mode}: слежу за {mode_title(new_mode)}.")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot is alive ({VERSION}).\nНапиши 'команды' чтобы увидеть список.\nПо умолчанию режим BTC.")
    await m.answer(f"✅ Режим {mode}: слежу за {mode_title(mode)}.")

@router.message(F.text.lower() == "команды")
async def cmd_help(m: Message):
    await m.answer(
        "📋 Команды:\n"
        "• /start — запуск\n"
        "• команды — список\n"
        "• биток / газ / золото / авто — выбор рынка\n"
        "• стоп — стоп и короткий кулдаун\n"
        "• статус — диагностика\n"
        "• отчет — 10 последних закрытий (только владелец)\n"
        "• тест — вывести тест-сигнал (диагностика форматирования)"
    )

@router.message(F.text.lower() == "биток")
async def set_btc(m: Message): await _request_mode("BTC", m)

@router.message(F.text.lower() == "газ")
async def set_ng(m: Message):  await _request_mode("NG", m)

@router.message(F.text.lower() == "золото")
async def set_xau(m: Message): await _request_mode("XAU", m)

@router.message(F.text.lower() == "авто")
async def set_auto(m: Message): await _request_mode("AUTO", m)

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    for s in trade.keys(): trade[s] = None; cooldown_until[s] = time.time() + 5
    await m.answer("🛑 Остановил. Открытых нет, короткий кулдаун.")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    lines = [f"mode: {mode} (requested: {requested_mode})"]
    now = time.time()
    for s in ["BTC","NG","XAU"]:
        opened = bool(trade[s])
        age = int(now - last_candle_close_ts[s]) if last_candle_close_ts[s] else -1
        atrtxt = state.get(f"atr_{s}", "—")
        lines.append(f"{SYMBOLS[s]['name']}: open={opened}  cooldown={max(0,int(cooldown_until[s]-now))}  ATR≈{atrtxt}  last_close_age={age}s")
    await m.answer("```\n"+ "\n".join(lines) + "\n```")

@router.message(F.text.lower() == "отчет")
async def cmd_report(m: Message):
    if m.from_user.id != OWNER_ID: return await m.answer("Доступно только владельцу.")
    if not os.path.exists(TRADES_CSV): return await m.answer("Пока нет закрытых сделок.")
    rows = list(csv.DictReader(open(TRADES_CSV,encoding="utf-8")))[-10:]
    if not rows: return await m.answer("Пусто.")
    txt = "Последние 10 закрытий:\n"
    for r in rows:
        txt += (f"{r['ts_close']}  {r['symbol']}  {r['side']}  {r['outcome']}  "
                f"entry:{r['entry']} tp:{r['tp']} sl:{r['sl']} rr:{r['rr_ratio']}\n")
    await m.answer("```\n"+txt+"```")

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    text = (
        "🔥 BUY BTC-USD | 1m\n"
        "✅ TP: **114999.9**\n"
        "🟥 SL: **114111.1**\n"
        "Entry: 114555.5  Spread≈350.0  ATR(14)≈45.0  Conf: 72%  Trend: UP"
    )
    await m.answer(text)

# ===================== NOTIFY =====================

async def send_signal(symbol: str, setup: dict, buffer: float):
    try: await bot.send_message(OWNER_ID, format_signal(setup, buffer))
    except: pass

async def notify_hit(symbol: str, outcome: str, price: float):
    name = SYMBOLS[symbol]["name"]; p = rnd(symbol, price)
    text = f"✅ TP hit on {name} @ {p}" if outcome=="TP" else f"🟥 SL hit on {name} @ {p}"
    try: await bot.send_message(OWNER_ID, text)
    except: pass

# ===================== ENGINE =====================

def _active_symbols_for_mode(md: str):
    if md == "AUTO": return ["NG","XAU"]
    if md == "BTC":  return ["BTC"]
    if md == "NG":   return ["NG"]
    if md == "XAU":  return ["XAU"]
    return ["BTC"]

def _apply_mode_change_if_needed():
    global mode, last_mode_switch_ts
    now = time.time()
    if requested_mode != mode and (now - last_mode_switch_ts) >= MODE_SWITCH_DEBOUNCE:
        old = mode
        mode = requested_mode
        last_mode_switch_ts = now
        logging.info(f"MODE SWITCH: {old} -> {mode}")

async def handle_symbol(session: aiohttp.ClientSession, symbol: str):
    global last_candle_close_ts, last_seen_idx, last_signal_idx

    if mode != "AUTO" and mode != symbol: return

    df = await get_df(session, symbol)
    if df.empty or len(df) < max(ATR_PERIOD, EMA_SLOW) + 3:
        return

    last_candle_close_ts[symbol] = time.time()
    atr_now = float(atr(df, ATR_PERIOD).iloc[-1])
    state[f"atr_{symbol}"] = rnd(symbol, atr_now)

    logging.info(f"HB {symbol}: last_close={rnd(symbol, float(df['Close'].iloc[-1]))} ATR≈{rnd(symbol, atr_now)}")

    cur_idx = len(df) - 1
    closed_idx = cur_idx - 1
    if closed_idx <= last_seen_idx[symbol]:
        return
    last_seen_idx[symbol] = closed_idx

    sess = trade[symbol]
    if sess:
        start_i = int(sess.get("entry_bar_idx", cur_idx))
        post = df.iloc[(start_i + 1):]
        if not post.empty:
            side = sess["side"]; tp = sess["tp"]; sl = sess["sl"]
            if side == "BUY":
                hit_tp = post["High"].max() >= tp
                hit_sl = post["Low"].min()  <= sl
            else:
                hit_tp = post["Low"].min()  <= tp
                hit_sl = post["High"].max() >= sl
            if hit_tp:
                price_now = float(post["Close"].iloc[-1])
                await notify_hit(symbol, "TP", price_now)
                finish_trade(symbol, "TP", price_now)
                return
            if hit_sl:
                price_now = float(post["Close"].iloc[-1])
                await notify_hit(symbol, "SL", price_now)
                finish_trade(symbol, "SL", price_now)
                return
        return

    if time.time() - boot_ts < BOOT_COOLDOWN_S: return
    if time.time() < cooldown_until[symbol]: return

    setup = build_setup(df, symbol, SYMBOLS[symbol]["tf"])
    if not setup: return

    if last_signal_idx[symbol] == closed_idx:
        return
    last_signal_idx[symbol] = closed_idx

    if not allow_after_sl(symbol, setup["sig"], time.time()): return

    base_buffer = dynamic_buffer(symbol, df, setup["atr"])
    add = BUF_K_TO_LEVEL.get(symbol, 1.0) * base_buffer
    side = setup["side"]
    tp = setup["tp"] + (add if side=="BUY" else -add)
    sl = setup["sl"] - (add if side=="BUY" else -add)

    await send_signal(symbol, setup, base_buffer)
    trade[symbol] = {
        "side": side,
        "entry": float(setup["entry"]),
        "tp": float(tp),
        "sl": float(sl),
        "sig": setup["sig"],
        "opened_at": time.time(),
        "entry_bar_idx": cur_idx
    }

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                _apply_mode_change_if_needed()
                for s in _active_symbols_for_mode(mode):
                    await handle_symbol(session, s)
                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(2)

# ===================== MAIN =====================

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    global mode, requested_mode, last_mode_switch_ts
    mode = state.get("mode","BTC")
    requested_mode = mode
    last_mode_switch_ts = time.time()
    asyncio.create_task(engine_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass

