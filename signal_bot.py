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

VERSION = "v15.2 SMC V3.5 (struct SL+0.002, SMC-TP min10p, RR≥1.25, BE alert, no-chase)"

BOT_TOKEN = "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"
OWNER_ID  = 6784470762

USE_5M_CONFIRM = True
STRUCT_BARS    = 1

# Порог шума
MIN_SPREAD = {"BTC": 277.5, "NG": 0.0020, "XAU": 0.25}

# ATR — не блокируем (V3.5 свобода)
ATR_MIN = {"BTC": 0.0, "NG": 0.0, "XAU": 0.0}

# Динамика буфера
ATR_K   = {"BTC": 7.0, "NG": 0.30, "XAU": 0.55}
RANGE_K = {"BTC": 0.90, "NG": 0.75, "XAU": 0.90}

# Подушка к TP/SL поверх buffer (оставляем)
BUF_K_TO_LEVEL = {"BTC": 1.15, "NG": 0.20, "XAU": 0.30}

# Глобальные пороги (fallback)
RR_MIN     = 1.10
MAX_SL_ATR = 1.40

# ---- Идеи / боевые ----
CONF_MIN_IDEA   = 0.20
CONF_MIN_TRADE  = 0.55   # активнее, как просил

SEND_IDEAS          = True
IDEA_COOLDOWN_SEC   = 60
MAX_IDEAS_PER_HOUR  = 30

CONF_MIN = 0.55

SYMBOLS = {
    "BTC": {"name": "BTC-USD",   "tf": "1m"},
    "NG":  {"name": "NG=F",      "tf": "1m"},
    "XAU": {"name": "XAUUSD=X",  "tf": "1m"},
}

# Периоды
ATR_PERIOD = 10
EMA_FAST   = 9
EMA_SLOW   = 21

# Петля
POLL_SEC         = 5
COOLDOWN_SEC     = 12
GUARD_AFTER_SL_S = 15 * 60
BOOT_COOLDOWN_S  = 60

STALE_FEED_SEC   = 600

# Сеть/ретраи
HTTP_TIMEOUT = 10
HTTP_RETRIES = 3
HTTP_RETRY_SLEEP = 0.8

ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/119.0 Safari/537.36"),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# Файлы
STATS_JSON = "gv_stats.json"
STATE_JSON = "gv_state.json"
TRADES_CSV = "gv_trades.csv"

# Источники
CC_URL = "https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=200"

# Локальные NG
RR_MIN_NG         = 1.30
CONF_MIN_NG       = 0.50
COOLDOWN_SEC_NG   = 8
IMPULSE_PIPS_NG   = 0.010
LOOKBACK_BREAK_NG = 6

# NG strategy pack (оставлено)
NG_STRAT_ENABLED        = True
NG_STRAT_MIN_SCORE      = 2
NG_RSI_OB               = 70
NG_RSI_OS               = 30
NG_BB_WINDOW            = 20
NG_BB_K                 = 2.0
NG_MACD_FAST            = 12
NG_MACD_SLOW            = 26
NG_MACD_SIGNAL          = 9
NG_EIA_BLACKOUT_MIN_BEFORE = 30
NG_EIA_BLACKOUT_MIN_AFTER  = 30
NG_EIA_UTC_HOUR         = 14
NG_EIA_UTC_MIN          = 30
NG_EIA_WEEKDAY          = 3
NG_SEASONAL_MONTHS_BULL = {11,12,1,2,3}

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

_last_idea_ts = {"BTC": 0.0, "NG": 0.0, "XAU": 0.0}
_ideas_count_hour = {"BTC": 0, "NG": 0, "XAU": 0}
_ideas_count_hour_ts = {"BTC": 0.0, "NG": 0.0, "XAU": 0.0}

_prices_cache = {}

# Интеррынок (оставляем как в v15.1)
INTERMARKET = {"DXY": None, "WTI": None, "ts": 0.0}
INTER_REFRESH_SEC = 120.0

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
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
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

def _sigmoid(x: float) -> float:
    try: return 1.0 / (1.0 + math.exp(-float(x)))
    except Exception: return 0.5

def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))

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

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = (delta.clip(lower=0)).ewm(alpha=1/period, adjust=False).mean()
    down = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    rs = up / (down + 1e-12)
    return 100 - (100 / (1 + rs))

def rnd(sym: str, x: float):
    return round(x, 1 if sym == "BTC" else (4 if sym == "NG" else 2))

def sig_id(symbol, side, trend, atr_val, tf):
    return f"{symbol}|{side}|{trend}|{round(float(atr_val),1)}|{tf}"

def allow_after_sl(symbol, signature, ts):
    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    return not (p.get("last_outcome") == "SL" and
                p.get("last_sig") == signature and
                ts - float(p.get("last_sig_ts",0)) < GUARD_AFTER_SL_S)

# ===================== PRICE HELPERS =====================

async def _http_get_json_robust(session: aiohttp.ClientSession, url: str) -> dict:
    YAHOO_TIMEOUT_S = 12
    YAHOO_RETRIES   = 4
    YAHOO_BACKOFF0  = 0.9
    YAHOO_JITTER    = 0.35
    backoff = YAHOO_BACKOFF0
    for i in range(YAHOO_RETRIES):
        try:
            async with session.get(url, timeout=YAHOO_TIMEOUT_S, headers=ROBUST_HEADERS) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429, 503):
                    await asyncio.sleep(backoff + (random.random() * YAHOO_JITTER))
                    backoff *= 1.7
                    continue
                return {}
        except Exception:
            pass
        await asyncio.sleep(backoff + (random.random() * YAHOO_JITTER))
        backoff *= 1.6
    return {}

def _df_from_yahoo_v8(payload: dict) -> pd.DataFrame:
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
        df = df.replace([None, np.nan], method="ffill").replace([None, np.nan], method="bfill")
        df = df.dropna().tail(3000).reset_index(drop=True)
        for col in ("Open","High","Low","Close"):
            df = df[df[col] > 0]
        return df.tail(3000).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

def df_from_stooq_csv(text: str):
    try:
        from io import StringIO
        if not text or "Date,Open,High,Low,Close" not in text:
            return pd.DataFrame()
        df = pd.read_csv(StringIO(text))
        if not {"Open","High","Low","Close"}.issubset(df.columns):
            return pd.DataFrame()
        return df.tail(3000).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

async def _get_df_yahoo_range(session, ticker_enc: str, interval: str, range_s: str) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_enc}?interval={interval}&range={range_s}"
    data = await _http_get_json_robust(session, url)
    return _df_from_yahoo_v8(data)

async def _get_df_ng_yahoo(session):  return await _get_df_yahoo_range(session, "NG%3DF", "1m", "5d")
async def _get_df_xau_yahoo(session):
    for t in ("XAUUSD%3DX", "GC%3DF"):
        df = await _get_df_yahoo_range(session, t, "1m", "5d")
        if not df.empty: return df
    return pd.DataFrame()

async def _get_df_ng_stooq(session):
    try:
        async with session.get("https://stooq.com/q/d/l/?s=ng.f&i=1", timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
            if r.status == 200:
                return df_from_stooq_csv(await r.text())
    except Exception: pass
    return pd.DataFrame()

async def _get_df_xau_stooq(session):
    for t in ("xauusd","gc.f"):
        try:
            async with session.get(f"https://stooq.com/q/d/l/?s={t}&i=1", timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
                if r.status == 200:
                    df = df_from_stooq_csv(await r.text())
                    if not df.empty: return df
        except Exception: pass
    return pd.DataFrame()

# Интеррынок (обновление)
async def _refresh_intermarket(session):
    now = time.time()
    if now - INTERMARKET["ts"] < INTER_REFRESH_SEC:
        return
    try:
        dxy = await _get_df_yahoo_range(session, "%5EDXY", "1m", "5d")
        wti = await _get_df_yahoo_range(session, "CL%3DF", "1m", "5d")
        if not dxy.empty: INTERMARKET["DXY"] = dxy.copy()
        if not wti.empty: INTERMARKET["WTI"] = wti.copy()
        INTERMARKET["ts"] = now
    except Exception as e:
        logging.warning(f"intermarket refresh fail: {e}")

async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
    global last_candle_close_ts, _prices_cache
    await _refresh_intermarket(session)

    now_ts = time.time()
    cache_ttl = 12.0 if symbol in ("NG", "XAU") else 2.0
    c = _prices_cache.get(symbol)
    if c and (now_ts - c["ts"] < cache_ttl) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]

    if symbol == "BTC":
        try:
            async with aiohttp.ClientSession() as s2:
                async with s2.get(CC_URL, timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
                    if r.status != 200:
                        return pd.DataFrame()
                    data = await r.json()
        except Exception:
            return pd.DataFrame()
        try:
            if data.get("Response") != "Success": return pd.DataFrame()
            arr = data["Data"]["Data"]
            df = pd.DataFrame(arr)
            if "time" in df.columns:
                last_candle_close_ts["BTC"] = int(df["time"].iloc[-1])
            df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
            df = df[["Open","High","Low","Close"]].tail(3000).reset_index(drop=True)
            _prices_cache["BTC"] = {"ts": now_ts, "df": df, "feed":"cc"}
            return df
        except Exception:
            return pd.DataFrame()

    if symbol == "NG":
        df = await _get_df_ng_yahoo(session)
        if not df.empty:
            last_candle_close_ts["NG"] = time.time()
            _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
            return df
        df = await _get_df_ng_stooq(session)
        if not df.empty:
            last_candle_close_ts["NG"] = time.time()
            _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"stooq"}
            return df
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

    return pd.DataFrame()

# ===================== IDEAS anti-spam =====================

def _reset_hour_if_needed(sym: str):
    now = time.time()
    start = _ideas_count_hour_ts.get(sym, 0.0) or 0.0
    if now - start >= 3600:
        _ideas_count_hour_ts[sym] = now
        _ideas_count_hour[sym] = 0

def can_send_idea(sym: str) -> bool:
    if not SEND_IDEAS: return False
    now = time.time()
    if now - _last_idea_ts.get(sym, 0.0) < IDEA_COOLDOWN_SEC:
        return False
    _reset_hour_if_needed(sym)
    if _ideas_count_hour.get(sym, 0) >= MAX_IDEAS_PER_HOUR:
        return False
    return True

async def send_idea_message(symbol: str, setup: dict, buffer: float, note: str = ""):
    sym = setup["symbol"]
    side = setup["side"]
    tf = setup.get("tf", "1m")
    add = BUF_K_TO_LEVEL.get(sym, 1.0) * buffer
    tp = setup["tp"] + (add if side == "BUY" else -add)
    sl = setup["sl"] - (add if side == "BUY" else -add)
    conf = int(setup.get("conf", 0.0) * 100)
    why  = setup.get("why","")
    text = (
        f"💡 IDEA {sym} | {tf}\n"
        f"{side}  Conf: {conf}%\n"
        f"{why}\n"
        f"Entry: {rnd(sym, setup['entry'])}\n"
        f"TP: {rnd(sym, tp)}  SL: {rnd(sym, sl)}  ATR(14)≈{rnd(sym, setup.get('atr',0))}"
    )
    try: await bot.send_message(OWNER_ID, text)
    except: pass
    _last_idea_ts[symbol] = time.time()
    _ideas_count_hour[symbol] = _ideas_count_hour.get(symbol, 0) + 1
    if _ideas_count_hour_ts.get(symbol, 0.0) == 0.0:
        _ideas_count_hour_ts[symbol] = time.time()

# ===================== STRATEGY HELPERS =====================

def dynamic_buffer(symbol: str, df: pd.DataFrame, atr_now: float) -> float:
    try:
        rng = float(df["High"].iloc[-2] - df["Low"].iloc[-2])
    except Exception:
        rng = 0.0
    base = MIN_SPREAD.get(symbol, 0.0)
    from_atr   = float(atr_now) * ATR_K.get(symbol, 1.0)
    from_range = float(rng)     * RANGE_K.get(symbol, 1.0)
    return max(base, from_atr, from_range)

def trend_side(df: pd.DataFrame) -> str:
    c = df["Close"]
    return "UP" if ema(c, EMA_FAST).iloc[-1] > ema(c, EMA_SLOW).iloc[-1] else "DOWN"

def _confidence(rr: float, trend_ok: bool, atr_now: float, symbol: str, feats: dict | None = None) -> float:
    rr_part    = max(0.0, min(1.0, (rr - 1.0) / 1.2))
    trend_part = 0.1 if trend_ok else 0.0
    conf = rr_part + trend_part
    if symbol == "NG" and feats:
        if feats.get("impulse_ok"): conf += 0.12
        if feats.get("broke_up") or feats.get("broke_dn"): conf += 0.10
        if (feats.get("ema_up") and trend_ok) or (feats.get("ema_dn") and not trend_ok):
            conf += 0.08
    return max(0.0, min(1.0, conf))

# --- MTF ресемплинг ---
def _resample_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    x = df.copy()
    idx = pd.to_datetime(x.index) if not isinstance(x.index, pd.DatetimeIndex) else x.index
    x.index = idx
    o = x["Open"].resample(rule).first()
    h = x["High"].resample(rule).max()
    l = x["Low"].resample(rule).min()
    c = x["Close"].resample(rule).last()
    out = pd.concat([o,h,l,c], axis=1)
    out.columns = ["Open","High","Low","Close"]
    out = out.dropna()
    return out

def _swing_points(series: pd.Series, left=2, right=2):
    highs, lows = [], []
    vals = series.values
    for i in range(left, len(vals)-right):
        seg = vals[i-left:i+right+1]
        if vals[i] == seg.max(): highs.append(i)
        if vals[i] == seg.min(): lows.append(i)
    return highs, lows

def _bos_bias(h4: pd.DataFrame, h1: pd.DataFrame) -> str:
    if len(h4) < 50 or len(h1) < 100:
        return "UP" if ema(h1["Close"], 20).iloc[-1] > ema(h1["Close"], 50).iloc[-1] else "DOWN"
    highs, lows = _swing_points(h4["Close"], 2, 2)
    bias = "UP"
    if len(highs) >= 2 and len(lows) >= 2:
        last_hh = h4["Close"].iloc[highs[-1]]
        prev_hh = h4["Close"].iloc[highs[-2]]
        last_ll = h4["Close"].iloc[lows[-1]]
        prev_ll = h4["Close"].iloc[lows[-2]]
        up = last_hh > prev_hh and last_ll >= prev_ll
        down = last_ll < prev_ll and last_hh <= prev_hh
        if up: bias = "UP"
        elif down: bias = "DOWN"
        else:
            bias = "UP" if ema(h4["Close"], 20).iloc[-1] > ema(h4["Close"], 50).iloc[-1] else "DOWN"
    h1_up = ema(h1["Close"], 20).iloc[-1] > ema(h1["Close"], 50).iloc[-1]
    if bias == "UP" and not h1_up: bias = "DOWN" if ema(h1["Close"], 20).iloc[-1] < ema(h1["Close"], 50).iloc[-1] else "UP"
    if bias == "DOWN" and h1_up:   bias = "UP"
    return bias

def _find_liquidity_m15(m15: pd.DataFrame, hours=4):
    bars = max(1, int(hours*60/15))
    look = m15.tail(bars)
    return float(look["High"].max()), float(look["Low"].min())

def _find_fvg(df: pd.DataFrame, depth=60):
    out = []
    n = min(depth, len(df)-2)
    H, L = df["High"].values, df["Low"].values
    for i in range(2, 2+n):
        if L[-i] > H[-i-2]:   out.append(("bull", float(H[-i-2]), float(L[-i])))
        if H[-i] < L[-i-2]:   out.append(("bear", float(H[-i]), float(L[-i-2])))
    return out[:5]

def _choch_trigger(df: pd.DataFrame, direction: str) -> bool:
    if len(df) < 20: return False
    c = df["Close"]
    ema_fast = ema(c, 5); ema_slow = ema(c, 13)
    trend_up = ema_fast.iloc[-1] > ema_slow.iloc[-1]
    highs, lows = _swing_points(c, 2, 2)
    if not highs or not lows: return False
    last_high = c.iloc[highs[-1]]
    last_low  = c.iloc[lows[-1]]
    if direction == "BUY":
        return trend_up and (c.iloc[-1] > last_high)
    else:
        return (not trend_up) and (c.iloc[-1] < last_low)

# -------- Fibonacci OTE (62%..79%) ----------
def _ote_band(a: float, b: float):
    lo, hi = (a, b) if a < b else (b, a)
    rng = hi - lo
    return (hi - 0.79*rng, hi - 0.62*rng)  # зона отката

def _last_impulse_for_bos(m15: pd.DataFrame, bias: str):
    highs, lows = _swing_points(m15["Close"], 2, 2)
    if bias == "UP" and len(highs)>=1 and len(lows)>=1:
        start = float(m15["Low"].iloc[lows[-1]])
        end   = float(m15["High"].iloc[highs[-1]])
        if end > start: return start, end
    if bias == "DOWN" and len(highs)>=1 and len(lows)>=1:
        start = float(m15["High"].iloc[highs[-1]])
        end   = float(m15["Low"].iloc[lows[-1]])
        if start > end: return start, end
    return float(m15["Low"].tail(30).min()), float(m15["High"].tail(30).max())

def _overlaps(a1,a2,b1,b2):
    lo1, hi1 = (a1,a2) if a1<=a2 else (a2,a1)
    lo2, hi2 = (b1,b2) if b1<=b2 else (b2,b1)
    return not (hi1 < lo2 or hi2 < lo1)

# -------- SL/TP (V3.5): структурный SL + 0.002 NG buffer; TP = SMC цель; min TP 0.010 --------
def _structural_sl(entry: float, side: str, m15: pd.DataFrame, m5: pd.DataFrame, fvg_zone: tuple | None, symbol: str):
    # базовый структурный SL по свингу M5 (CHoCH/локальный экстремум)
    highs5, lows5 = _swing_points(m5["Close"], 2, 2)
    sl_candidates = []
    if side == "BUY" and lows5:
        sl_candidates.append(float(m5["Low"].iloc[lows5[-1]]))
    if side == "SELL" and highs5:
        sl_candidates.append(float(m5["High"].iloc[highs5[-1]]))
    # FVG-граница
    if fvg_zone:
        ftype, a, b = fvg_zone
        if side == "BUY" and ftype == "bull":
            sl_candidates.append(min(a,b))
        if side == "SELL" and ftype == "bear":
            sl_candidates.append(max(a,b))
    if not sl_candidates:
        # подстраховка: 1*ATR(M15)
        a15 = float(atr(m15, 14).iloc[-1])
        if side == "BUY":  return entry - a15
        else:              return entry + a15

    if side == "BUY":   sl0 = min(sl_candidates)
    else:               sl0 = max(sl_candidates)

    # V3.5: никакого минимума по ATR — добавляем только спред-буфер
    buf = 0.002 if symbol == "NG" else 0.0
    if side == "BUY":   return sl0 - buf
    else:               return sl0 + buf

def _smc_tp(entry: float, side: str, m15: pd.DataFrame, liq_hi: float, liq_lo: float, fvgs15: list):
    # TP = ближайший разумный LP (high/low) или ближайший противоположный FVG
    target_candidates = []
    if side == "BUY":
        target_candidates.append(liq_hi)
        opp = [f for f in fvgs15 if f[0]=="bear"]
        if opp:  # использовать нижнюю границу медвежьего FVG как «стенку»
            target_candidates.append(max(opp[0][1], opp[0][2]))
        tp = max(target_candidates) if target_candidates else entry + 0.02
    else:
        target_candidates.append(liq_lo)
        opp = [f for f in fvgs15 if f[0]=="bull"]
        if opp:
            target_candidates.append(min(opp[0][1], opp[0][2]))
        tp = min(target_candidates) if target_candidates else entry - 0.02
    return float(tp)

def _no_chase_rule(entry: float, side: str, fvg_zone: tuple | None, tp: float) -> bool:
    # Запрет на погоню: если уже пройдено >50% от FVG до TP — не входим
    if not fvg_zone: return False
    _, a, b = fvg_zone
    f_mid = (a + b) / 2.0
    dist_total = abs(tp - f_mid)
    if dist_total <= 1e-9: return True
    prog = abs(entry - f_mid) / dist_total
    return prog > 0.5

def _impulse_bonus(m5: pd.DataFrame):
    if len(m5) < 20: return 0, ""
    body = abs(float(m5["Close"].iloc[-2]) - float(m5["Open"].iloc[-2]))
    a5 = float(atr(m5, 14).iloc[-1])
    if a5 > 0 and body >= 1.5 * a5:
        return 5, "Impulse +5"
    return 0, ""

# -------- Межрыночный штраф (как было) --------
def _trend_up(df: pd.DataFrame, rule="1H"):
    if df is None or df.empty: return None
    x = _resample_ohlc(df.set_index(pd.to_datetime(df.index)), rule)
    if len(x) < 60: return None
    return bool(ema(x["Close"], 20).iloc[-1] > ema(x["Close"], 50).iloc[-1])

def _inter_penalty(side: str):
    penalty = 0
    dxy = INTERMARKET.get("DXY"); wti = INTERMARKET.get("WTI")
    dxy_up = _trend_up(dxy, "1H") if isinstance(dxy, pd.DataFrame) else None
    wti_up = _trend_up(wti, "1H") if isinstance(wti, pd.DataFrame) else None
    try:
        if dxy_up is not None:
            if side == "BUY" and dxy_up: penalty -= 5
            if side == "SELL" and (not dxy_up): penalty -= 5
        if wti_up is not None:
            if side == "BUY" and (not wti_up): penalty -= 5
            if side == "SELL" and wti_up:      penalty -= 5
    except Exception:
        pass
    return penalty

# -------- SMC core (с V3.5 изменениями) --------
def _smc_score(symbol: str, df1m: pd.DataFrame) -> dict:
    out = {"ok": False}
    if df1m is None or df1m.empty or len(df1m) < 300:
        return out

    m5  = _resample_ohlc(df1m.set_index(pd.to_datetime(df1m.index)), "5min")
    m15 = _resample_ohlc(df1m.set_index(pd.to_datetime(df1m.index)), "15min")
    h1  = _resample_ohlc(df1m.set_index(pd.to_datetime(df1m.index)), "1H")
    h4  = _resample_ohlc(df1m.set_index(pd.to_datetime(df1m.index)), "4H")
    if min(len(m5),len(m15),len(h1),len(h4)) < 20:
        return out

    bias = _bos_bias(h4, h1)
    liq_hi, liq_lo = _find_liquidity_m15(m15, hours=4)
    fvgs15 = _find_fvg(m15, depth=40)
    bull_fvg = next((f for f in fvgs15 if f[0]=="bull"), None)
    bear_fvg = next((f for f in fvgs15 if f[0]=="bear"), None)

    last_m5 = float(m5["Close"].iloc[-1])
    fake_out = False
    if bias == "UP":
        fake_out = (float(m5["Low"].iloc[-1]) <= liq_lo and last_m5 > liq_lo)
    else:
        fake_out = (float(m5["High"].iloc[-1]) >= liq_hi and last_m5 < liq_hi)

    side = "BUY" if bias == "UP" else "SELL"
    choch = _choch_trigger(m5, side)

    score = 0
    why = []

    # База: FVG по bias
    if side == "BUY" and bull_fvg: score += 50; why.append("FVG↑ M15")
    if side == "SELL" and bear_fvg: score += 50; why.append("FVG↓ M15")

    # Stop-hunt
    if fake_out: score += 30; why.append("Stop-hunt")

    # Совпадение с Bias
    score += 20; why.append(f"Bias {bias}")

    # Фаза тренда (vol proxy)
    a_now = float(atr(h1, 14).iloc[-1]); a_ref = float(atr(h1, 100).iloc[-1]) if len(h1) >= 110 else a_now
    if a_ref > 0 and a_now >= 1.2 * a_ref:
        score += 10; why.append("Trend phase (vol↑)")

    # CHoCH
    if choch: score += 20; why.append("CHoCH M5")

    # --- OTE бонус ---
    try:
        imp_a, imp_b = _last_impulse_for_bos(m15, bias)
        ote_lo, ote_hi = _ote_band(imp_a, imp_b)
        if side == "BUY" and bull_fvg:
            f_lo, f_hi = bull_fvg[1], bull_fvg[2]
            if _overlaps(f_lo, f_hi, ote_lo, ote_hi):
                score += 10; why.append("OTE 62-79%")
        if side == "SELL" and bear_fvg:
            f_lo, f_hi = bear_fvg[2], bear_fvg[1]
            if _overlaps(f_lo, f_hi, ote_lo, ote_hi):
                score += 10; why.append("OTE 62-79%")
    except Exception:
        pass

    # --- Межрыночный штраф ---
    penalty = _inter_penalty(side)
    if penalty != 0:
        score += penalty; why.append("Intermarket -5")

    # --- Импульс +5 ---
    imp_pts, imp_txt = _impulse_bonus(m5)
    score += imp_pts
    if imp_pts: why.append(imp_txt)

    score = max(0, min(100, score))
    if score < 20:
        out["ok"] = False
        return out

    # ENTRY по закрытию последней 1m свечи
    entry = float(df1m["Close"].iloc[-2])

    # Выбор FVG в сторону входа
    use_fvg = bull_fvg if side=="BUY" else bear_fvg

    # TP по SMC: LP или противоположный FVG
    tp = _smc_tp(entry, side, m15, liq_hi, liq_lo, fvgs15)

    # SL строго структурный + 0.002 (NG) буфер. НИКАКОГО min ATR.
    sl = _structural_sl(entry, side, m15, m5, use_fvg, symbol="NG" if symbol=="NG" else symbol)

    # RR расчёт + no-chase
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = reward / max(risk, 1e-12)

    # no-chase: если уже пройдено >50% от FVG->TP — не входим
    if _no_chase_rule(entry, side, use_fvg, tp):
        out["ok"] = False
        return out

    # Минимальная гарантия TP: >= 0.010 (NG)
    if symbol == "NG":
        if reward < 0.010 - 1e-9:
            out["ok"] = False
            return out

    # Готово
    out.update({
        "ok": True,
        "score": score,
        "side": side,
        "bias": bias,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "why": ", ".join(why + (["SMC-TP"])),
        "fvg": use_fvg
    })
    return out

# ===================== NG nowcast (fallback) =====================

def _ng_nowcast(df: pd.DataFrame) -> dict:
    feats = {"p_up": 0.5, "impulse_ok": False, "broke_up": False, "broke_dn": False, "ema_up": False, "ema_dn": False}
    try:
        if df is None or df.empty or len(df) < max(ATR_PERIOD, EMA_SLOW) + 5:
            return feats
        closed = len(df) - 2
        if closed < 4: return feats

        c = df["Close"].astype(float).values
        o = df["Open"].astype(float).values if "Open" in df.columns else c
        h = df["High"].astype(float).values if "High" in df.columns else c
        l = df["Low"].astype(float).values  if "Low"  in df.columns else c

        ser_c = pd.Series(c)
        ema_fast = ser_c.ewm(span=EMA_FAST, adjust=False).mean().values
        ema_slow = ser_c.ewm(span=EMA_SLOW, adjust=False).mean().values

        df_tmp = df.copy()
        df_tmp["ATR"] = atr(df_tmp, ATR_PERIOD)
        atr_now = float(df_tmp["ATR"].iloc[closed])
        if not np.isfinite(atr_now) or atr_now <= 0: return feats

        r1 = c[closed]   - c[closed-1]
        r2 = c[closed-1] - c[closed-2]
        accel = (r1 - r2) / max(atr_now, 1e-12)

        slope_fast = (ema_fast[closed] - ema_fast[closed-2]) / max(atr_now, 1e-12)
        slope_slow = (ema_slow[closed] - ema_slow[closed-2]) / max(atr_now, 1e-12)
        ema_combo = slope_fast + 0.7 * slope_slow

        atr_ref = float(pd.Series(df_tmp["ATR"]).rolling(20).mean().iloc[closed])
        vol_expand = 0.0
        if np.isfinite(atr_ref) and atr_ref > 0:
            vol_expand = (atr_now / atr_ref) - 1.0

        N = 12
        window_c = c[closed-N:closed+1]
        prebreak_up   = (c[closed] - window_c.min()) / max(atr_now, 1e-12)
        prebreak_down = (window_c.max() - c[closed]) / max(atr_now, 1e-12)
        prebreak = prebreak_up - prebreak_down

        body = abs(c[closed-1] - o[closed-1])
        rng  = max(h[closed-1] - l[closed-1], 1e-12)
        upper = h[closed-1] - max(c[closed-1], o[closed-1])
        lower = min(c[closed-1], o[closed-1]) - l[closed-1]
        candle_bias = ((lower - upper) / rng) + ((c[closed-1] - l[closed-1]) / rng - 0.5)

        feats["impulse_ok"] = abs(c[closed] - c[closed-1]) >= IMPULSE_PIPS_NG
        Nbr = LOOKBACK_BREAK_NG
        hi = float(np.max(h[closed-(Nbr+1):closed-1])) if closed-(Nbr+1) >= 0 else np.nan
        lo = float(np.min(l[closed-(Nbr+1):closed-1])) if closed-(Nbr+1) >= 0 else np.nan
        feats["broke_up"] = np.isfinite(hi) and (c[closed] > hi)
        feats["broke_dn"] = np.isfinite(lo) and (c[closed] < lo)
        feats["ema_up"]   = bool(ema_fast[closed] > ema_slow[closed])
        feats["ema_dn"]   = bool(ema_fast[closed] < ema_slow[closed])

        w1, w2, w3, w4, w5 = 2.4, 2.0, 1.8, 1.2, 0.8
        score = (w1 * ( (r1 - r2) / max(atr_now, 1e-12) ) +
                 w2 * ema_combo +
                 w3 * prebreak +
                 w4 * vol_expand +
                 w5 * candle_bias)
        feats["p_up"] = _clip01(_sigmoid(score))
        return feats
    except Exception:
        return feats

# ===================== BUILD SETUP =====================

def build_setup(df: pd.DataFrame, symbol: str, tf_label: str):
    if df is None or df.empty or len(df) < max(ATR_PERIOD, EMA_SLOW) + 3:
        return None

    df = df.copy()
    df["ATR"] = atr(df, ATR_PERIOD)
    atr_now = float(df["ATR"].iloc[-1])
    if not np.isfinite(atr_now) or atr_now <= 0:
        return None

    # === SMC приоритет для NG/XAU ===
    if symbol in ("NG", "XAU"):
        smc = _smc_score(symbol, df.set_index(pd.to_datetime(df.index)))
        if smc.get("ok", False):
            side = smc["side"]; entry = smc["entry"]; sl = smc["sl"]; tp = smc["tp"]
            rr = float(smc["rr"]); conf = smc["score"] / 100.0
            signature = sig_id(symbol, side, side, atr_now, tf_label)
            return {
                "symbol": symbol, "tf": tf_label,
                "side": side, "trend": smc["bias"],
                "entry": entry, "tp": tp, "sl": sl,
                "atr": atr_now, "rr": rr, "conf": conf,
                "sig": signature, "why": smc.get("why","")
            }
        # если SMC не готов — падение на fallback ниже

    # ---- Fallback (и для BTC) ----
    side_tr = trend_side(df)
    last    = df.iloc[-2]
    entry   = float(last["Close"])

    feats = _ng_nowcast(df) if symbol == "NG" else None
    rr_min   = RR_MIN_NG   if symbol == "NG" else RR_MIN

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

    nowcast_extra = 0.0
    if symbol == "NG" and feats:
        p_up = float(feats.get("p_up", 0.5))
        if side == "BUY" and p_up >= 0.58:
            nowcast_extra = 0.10 + (0.05 if p_up >= 0.66 else 0.0)
        elif side == "SELL" and p_up <= 0.42:
            nowcast_extra = 0.10 + (0.05 if p_up <= 0.34 else 0.0)

    conf_base = _confidence(rr, True, atr_now, symbol, feats)
    conf = max(0.0, min(1.0, conf_base + nowcast_extra))

    signature = sig_id(symbol, side, side_tr, atr_now, tf_label)
    return {
        "symbol": symbol, "tf": tf_label,
        "side": side, "trend": side_tr,
        "entry": entry, "tp": tp, "sl": sl,
        "atr": atr_now, "rr": rr, "conf": conf, "sig": signature,
        "why": "fallback"
    }

def format_signal(setup, buffer):
    sym = setup["symbol"]; side = setup["side"]; tf = setup["tf"]
    add = BUF_K_TO_LEVEL.get(sym, 1.0) * buffer
    tp = setup["tp"] + (add if side == "BUY" else -add)
    sl = setup["sl"] - (add if side == "BUY" else -add)
    why = setup.get("why","")
    lines = [
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}",
        f"✅ TP: **{rnd(sym, tp)}**",
        f"🟥 SL: **{rnd(sym, sl)}**",
        f"Entry: {rnd(sym, setup['entry'])}  Spread≈{rnd(sym, buffer)}  ATR(14)≈{rnd(sym, setup['atr'])}  Conf: {int(setup['conf']*100)}%  Trend: {setup.get('trend','')}",
    ]
    if why:
        lines.append(f"📌 {why}")
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
    cooldown_until[symbol] = time.time() + (COOLDOWN_SEC_NG if symbol == "NG" else COOLDOWN_SEC)
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

async def send_text(txt: str):
    try: await bot.send_message(OWNER_ID, txt)
    except: pass

async def notify_hit(symbol: str, outcome: str, price: float):
    name = SYMBOLS[symbol]["name"]; p = rnd(symbol, price)
    text = f"✅ TP hit on {name} @ {p}" if outcome=="TP" else f"🟥 SL hit on {name} @ {p}"
    try: await bot.send_message(OWNER_ID, text)
    except: pass

async def notify_be(symbol: str):
    name = SYMBOLS[symbol]["name"]
    try:
        await bot.send_message(OWNER_ID, f"🔔 ALERT: БЕЗУБЫТОК {name}. Реком.: перевести SL в BE (+0.001).")
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

    # сопровождение открытой сессии + BE alert
    sess = trade[symbol]
    if sess:
        start_i = int(sess.get("entry_bar_idx", cur_idx))
        post = df.iloc[(start_i + 1):]
        if not post.empty:
            side = sess["side"]; tp = sess["tp"]; sl = sess["sl"]
            entry = sess["entry"]
            # BE-алерт: достижение 1:1
            if not sess.get("be_alerted", False):
                risk = abs(entry - sl)
                if side == "BUY":
                    fe = float(post["High"].max()) - entry
                else:
                    fe = entry - float(post["Low"].min())
                if fe >= risk - 1e-9:
                    await notify_be(symbol)
                    sess["be_alerted"] = True

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

    base_buffer = dynamic_buffer(symbol, df, setup["atr"])
    add = BUF_K_TO_LEVEL.get(symbol, 1.0) * base_buffer
    side = setup["side"]
    tp_show = setup["tp"] + (add if side == "BUY" else -add)
    sl_show = setup["sl"] - (add if side == "BUY" else -add)

    conf = float(setup.get("conf", 0.0))
    pct  = int(round(conf * 100))
    rr_v = round(float(setup.get("rr", 0.0)), 3)
    entry = rnd(symbol, setup["entry"])
    atr_v = rnd(symbol, setup["atr"])
    name = SYMBOLS[symbol]["name"]
    why  = setup.get("why","")

    # ИДЕЯ
    if conf >= CONF_MIN_IDEA and can_send_idea(symbol):
        txt = (
            f"{'⚡️ СДЕЛКА' if (conf >= CONF_MIN_TRADE and rr_v >= 1.25) else '💡 ИДЕЯ'} {name} | {SYMBOLS[symbol]['tf']}\n"
            f"{side} | Conf: {pct}%  RR≈{rr_v}  ATR≈{atr_v}\n"
            f"{('📌 '+why) if why else ''}\n"
            f"Entry: {entry}\n"
            f"TP: {rnd(symbol, tp_show)}   SL: {rnd(symbol, sl_show)}"
        )
        await send_text(txt)

    # БОЕВОЙ — строго RR ≥ 1.25 (V3.5)
    if conf >= CONF_MIN_TRADE and float(setup.get("rr", 0.0)) >= 1.25:
        if not allow_after_sl(symbol, setup["sig"], time.time()): return
        pretty = dict(setup)
        pretty["tp"] = tp_show
        pretty["sl"] = sl_show
        await send_signal(symbol, pretty, base_buffer)
        trade[symbol] = {
            "side": setup["side"],
            "entry": float(setup["entry"]),
            "tp": float(tp_show),
            "sl": float(sl_show),
            "sig": setup["sig"],
            "opened_at": time.time(),
            "entry_bar_idx": cur_idx,
            "be_alerted": False
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

router = Router()
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
