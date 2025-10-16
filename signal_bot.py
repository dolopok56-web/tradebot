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

VERSION = "v16.0 SMC V4.5 (pure SMC; RR≥1.0; ideas≥0.05; BE alert; ALIVE via log-bot)"

# --- Telegram ---
BOT_TOKEN = "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"     # основной бот (сигналы)
LOG_BOT_TOKEN = "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw" # отдельный бот только для ALIVE
OWNER_ID  = 6784470762

# --- Рынки и таймфрейм мин. баров (фид остаётся твой) ---
SYMBOLS = {
    "BTC": {"name": "BTC-USD",   "tf": "1m"},
    "NG":  {"name": "NG=F",      "tf": "1m"},
    "XAU": {"name": "XAUUSD=X",  "tf": "1m"},
}

# --- Фиксированный спред-буфер к уровням TP/SL (чистый SMC) ---
SPREAD_BUFFER = {"NG": 0.0020, "XAU": 0.20, "BTC": 0.0}
BUF_K_TO_LEVEL = {"BTC": 1.15, "NG": 1.00, "XAU": 1.00}  # множитель к спред-буферу при показе

# --- Пороговые значения активности ---
CONF_MIN_IDEA   = 0.05   # идеи с низкой уверенностью, чтобы «не молчал»
CONF_MIN_TRADE  = 0.55   # боевые сигналы
RR_TRADE_MIN    = 1.00   # минимум RR для TRADE
TP_MIN_TRADE    = {"NG": 0.005, "XAU": 0.005, "BTC": 10.0}  # BTC здесь не используем, но оставлен

# --- Анти-спам идей ---
SEND_IDEAS         = True
IDEA_COOLDOWN_SEC  = 60
MAX_IDEAS_PER_HOUR = 30

# --- Служебные периоды/петли ---
POLL_SEC         = 5
COOLDOWN_SEC     = 12
GUARD_AFTER_SL_S = 15 * 60
BOOT_COOLDOWN_S  = 60
STALE_FEED_SEC   = 600

# --- HTTP ---
HTTP_TIMEOUT = 10
ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/119.0 Safari/537.36"),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# --- Логи/состояние ---
STATS_JSON = "gv_stats.json"
STATE_JSON = "gv_state.json"
TRADES_CSV = "gv_trades.csv"

# --- Внешние источники ---
CC_URL = "https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=200"

# --- Интеррынок (для DXY) ---
INTERMARKET = {"DXY": None, "WTI": None, "ts": 0.0}
INTER_REFRESH_SEC = 120.0

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

DEFAULT_PARAMS = {
    "win": 0, "loss": 0, "streak_win": 0, "streak_loss": 0,
    "last_sig": None, "last_sig_ts": 0, "last_outcome": None,
}

# ===================== IO HELPERS =====================

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

# ===================== MATH/UTIL =====================

def now_iso():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="seconds")

def rnd(sym: str, x: float):
    return round(float(x), 1 if sym == "BTC" else (4 if sym == "NG" else 2))

def sig_id(symbol, side, trend, tf):
    return f"{symbol}|{side}|{trend}|{tf}"

def allow_after_sl(symbol, signature, ts):
    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    return not (p.get("last_outcome") == "SL" and
                p.get("last_sig") == signature and
                ts - float(p.get("last_sig_ts",0)) < GUARD_AFTER_SL_S)

# ===================== ATR (Только для ALIVE/статуса) =====================

def true_range(h, l, c):
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr

def atr(df, n=14):
    try:
        return true_range(df["High"], df["Low"], df["Close"]).rolling(n).mean()
    except Exception:
        return pd.Series([0.0]*len(df))

# ===================== PRICE HELPERS =====================

async def _http_get_json_robust(session: aiohttp.ClientSession, url: str) -> dict:
    YAHOO_TIMEOUT_S = 12
    YAHOO_RETRIES   = 4
    YAHOO_BACKOFF0  = 0.9
    YAHOO_JITTER    = 0.35
    backoff = YAHOO_BACKOFF0
    for _ in range(YAHOO_RETRIES):
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
        df = df.dropna().tail(3000)
        return df
    except Exception:
        return pd.DataFrame()

def df_from_stooq_csv(text: str):
    try:
        from io import StringIO
        if not text or "Date,Open,High,Low,Close" not in text:
            return pd.DataFrame()
        df = pd.read_csv(StringIO(text))
        # если Date есть — ставим индекс датой
        if "Date" in df.columns:
            try:
                idx = pd.to_datetime(df["Date"])
                df = df[["Open","High","Low","Close"]]
                df.index = idx
            except Exception:
                df = df[["Open","High","Low","Close"]]
        df = df.dropna().tail(3000)
        return df
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

async def _refresh_intermarket(session):
    now = time.time()
    if now - INTERMARKET["ts"] < INTER_REFRESH_SEC:
        return
    try:
        dxy = await _get_df_yahoo_range(session, "%5EDXY", "1m", "5d")
        if not dxy.empty: INTERMARKET["DXY"] = dxy.copy()
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
            df = df[["Open","High","Low","Close"]].tail(3000)
            df.index = pd.to_datetime(df.index, unit="s", origin="unix", utc=True)  # псевдо-время
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
        f"IDEA {sym} | {tf}\n"
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

# ===================== SMC CORE =====================

def _resample_ohlc_uniform(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Ресемплим на равномерную сетку от текущего момента назад — твой прежний подход."""
    if df is None or df.empty: return pd.DataFrame()
    base = pd.date_range(end=pd.Timestamp.utcnow(), periods=len(df), freq="1min")
    z = df.copy()
    z.index = base
    o = z["Open"].resample(f"{minutes}min").first()
    h = z["High"].resample(f"{minutes}min").max()
    l = z["Low"].resample(f"{minutes}min").min()
    c = z["Close"].resample(f"{minutes}min").last()
    r = pd.concat([o,h,l,c], axis=1).dropna()
    r.columns = ["Open","High","Low","Close"]
    return r

def _swing_high(df, lookback=20):
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["High"].iloc[L:i+1].max()) if len(df) > 0 else np.nan

def _swing_low(df, lookback=20):
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["Low"].iloc[L:i+1].min()) if len(df) > 0 else np.nan

def _bias_bos_higher(df60, df240):
    """Чистый BOS: сравниваем текущий close с экстремумами; без EMA/RSI."""
    if len(df60) < 10 or len(df240) < 10:
        return "UP"
    c1 = float(df60["Close"].iloc[-2])
    hh4 = _swing_high(df240, 20)
    ll4 = _swing_low(df240, 20)
    if c1 > hh4: return "UP"
    if c1 < ll4: return "DOWN"
    hh1 = _swing_high(df60, 20)
    ll1 = _swing_low(df60, 20)
    if c1 > hh1: return "UP"
    if c1 < ll1: return "DOWN"
    return "UP"

def _had_liquidity_sweep(df, lookback=20):
    i = len(df) - 2
    if i < 2: return (False, "")
    hh = _swing_high(df, lookback)
    ll = _swing_low(df, lookback)
    H = float(df["High"].iloc[i])
    L = float(df["Low"].iloc[i])
    C = float(df["Close"].iloc[i])
    if H > hh and C < hh: return True, "DOWN"  # сняли хай и вернулись
    if L < ll and C > ll: return True, "UP"    # сняли лоу и вернулись
    return False, ""

def _fvg_last(df):
    if len(df) < 4: return False,"",0.0,0.0
    i = len(df)-2
    h2,l2 = float(df["High"].iloc[i-2]),float(df["Low"].iloc[i-2])
    h0,l0 = float(df["High"].iloc[i]),float(df["Low"].iloc[i])
    if l0 > h2: return True,"bull",l0,h2
    if h0 < l2: return True,"bear",h0,l2
    return False,"",0.0,0.0

def _choch(df, want):
    c = df["Close"].values
    if len(c) < 12: return False
    if want == "UP": return c[-2] > max(c[-9:-2])
    else:            return c[-2] < min(c[-9:-2])

def _in_session_utc():
    h = pd.Timestamp.utcnow().hour
    return (h in range(7,15)) or (h in range(12,21))  # Лондон/Нью-Йорк

def _is_consolidation_break(df):
    if len(df)<20: return False
    i=len(df)-2
    window=df.iloc[i-12:i]
    rng=float(window["High"].max()-window["Low"].min())
    base=float(window["Close"].iloc[-1])
    if base<=0: return False
    if rng/base<=0.003:
        H=float(df["High"].iloc[i]); L=float(df["Low"].iloc[i])
        return H>window["High"].max() or L<window["Low"].min()
    return False

def _inside_higher_ob(df_low, df_high):
    if len(df_low)<5 or len(df_high)<5: return False
    cl=float(df_low["Close"].iloc[-2])
    body=df_high.iloc[-2]
    top=max(body["Open"],body["Close"])
    bot=min(body["Open"],body["Close"])
    return bot<=cl<=top

def _fib_ote_ok(a,b,price):
    if a==b: return False
    lo,hi=sorted([a,b])
    lvl62=hi-0.62*(hi-lo)
    lvl79=hi-0.79*(hi-lo)
    loZ,hiZ=min(lvl62,lvl79),max(lvl62,lvl79)
    return loZ<=price<=hiZ

def _ote_band(a: float, b: float):
    lo, hi = (a, b) if a < b else (b, a)
    rng = hi - lo
    return (hi - 0.79*rng, hi - 0.62*rng)

def _last_impulse_for_bos(m15: pd.DataFrame, bias: str):
    # простая импульсная нога от последнего свинга
    try:
        i = len(m15) - 2
        if i < 5: return float(m15["Low"].min()), float(m15["High"].max())
        if bias == "UP":
            lo = float(m15["Low"].iloc[i-10:i].min())
            hi = float(m15["High"].iloc[i-10:i].max())
            if hi > lo: return lo, hi
        else:
            hi = float(m15["High"].iloc[i-10:i].max())
            lo = float(m15["Low"].iloc[i-10:i].min())
            if hi > lo: return hi, lo
    except Exception:
        pass
    return float(m15["Low"].tail(30).min()), float(m15["High"].tail(30).max())

def _overlaps(a1,a2,b1,b2):
    lo1, hi1 = (a1,a2) if a1<=a2 else (a2,a1)
    lo2, hi2 = (b1,b2) if b1<=b2 else (b2,a1)
    return not (hi1 < lo2 or hi2 < lo1)

def _structural_sl(entry: float, side: str, m15: pd.DataFrame, m5: pd.DataFrame, fvg_zone: tuple | None, symbol: str):
    # кандидаты: последний свинг M5 + граница FVG по направлению
    highs5, lows5 = [], []
    vals = m5["Close"].values
    for i in range(2, len(vals)-2):
        seg = vals[i-2:i+3]
        if vals[i] == seg.max(): highs5.append(i)
        if vals[i] == seg.min(): lows5.append(i)
    sl_candidates = []
    if side == "BUY" and lows5:
        sl_candidates.append(float(m5["Low"].iloc[lows5[-1]]))
    if side == "SELL" and highs5:
        sl_candidates.append(float(m5["High"].iloc[highs5[-1]]))
    if fvg_zone:
        ftype, a, b = fvg_zone
        if side == "BUY" and ftype == "bull":
            sl_candidates.append(min(a,b))
        if side == "SELL" and ftype == "bear":
            sl_candidates.append(max(a,b))
    if not sl_candidates:
        # без ATR fallback: минимальный технический буфер
        raw = entry - 2*SPREAD_BUFFER.get(symbol, 0.0) if side=="BUY" else entry + 2*SPREAD_BUFFER.get(symbol, 0.0)
        return float(raw)
    sl0 = min(sl_candidates) if side=="BUY" else max(sl_candidates)
    buf = SPREAD_BUFFER.get(symbol, 0.0)
    return float(sl0 - buf) if side=="BUY" else float(sl0 + buf)

def _smc_tp(entry: float, side: str, m15: pd.DataFrame, liq_hi: float, liq_lo: float, fvgs15: list):
    # цель: ближайший LP либо первая противоположная FVG
    target_candidates = []
    if side == "BUY":
        target_candidates.append(liq_hi)
        opp = [f for f in fvgs15 if f[0]=="bear"]
        if opp:
            target_candidates.append(max(opp[0][1], opp[0][2]))
        tp = max(target_candidates) if target_candidates else entry + 0.02
    else:
        target_candidates.append(liq_lo)
        opp = [f for f in fvgs15 if f[0]=="bull"]
        if opp:
            target_candidates.append(min(opp[0][1], opp[0][2]))
        tp = min(target_candidates) if target_candidates else entry - 0.02
    return float(tp)

def _no_chase_rule(entry: float, fvg_zone: tuple | None, tp: float) -> bool:
    if not fvg_zone: return False
    _, a, b = fvg_zone
    f_mid = (a + b) / 2.0
    dist_total = abs(tp - f_mid)
    if dist_total <= 1e-12: return True
    prog = abs(entry - f_mid) / dist_total
    return prog > 0.5

def _smc_score(symbol: str, df1m: pd.DataFrame) -> dict:
    out = {"ok": False}
    if df1m is None or df1m.empty or len(df1m) < 300:
        return out

    m5  = _resample_ohlc_uniform(df1m, 5)
    m15 = _resample_ohlc_uniform(df1m, 15)
    h1  = _resample_ohlc_uniform(df1m, 60)
    h4  = _resample_ohlc_uniform(df1m, 240)
    if min(len(m5),len(m15),len(h1),len(h4)) < 20:
        return out

    bias = _bias_bos_higher(h1, h4)
    liq_hi, liq_lo = _swing_high(m15, 20), _swing_low(m15, 20)
    fvgs15 = []
    # найдём последние несколько FVG
    if len(m15) >= 6:
        for k in range(2, min(40, len(m15)-2)):
            h2,l2 = float(m15["High"].iloc[-k-2]), float(m15["Low"].iloc[-k-2])
            h0,l0 = float(m15["High"].iloc[-k]),   float(m15["Low"].iloc[-k])
            if l0 > h2: fvgs15.append(("bull", l0, h2))
            if h0 < l2: fvgs15.append(("bear", h0, l2))
    bull_fvg = next((f for f in fvgs15 if f[0]=="bull"), None)
    bear_fvg = next((f for f in fvgs15 if f[0]=="bear"), None)

    # sweep M15
    sweep15, sweep_dir15 = _had_liquidity_sweep(m15)

    side = "BUY" if bias == "UP" else "SELL"
    if sweep15:
        if sweep_dir15=="UP": side="BUY"
        if sweep_dir15=="DOWN": side="SELL"

    choch5 = _choch(m5, "UP" if side=="BUY" else "DOWN")
    entry  = float(m5["Close"].iloc[-2])

    # SL / TP
    use_fvg = bull_fvg if side=="BUY" else bear_fvg
    sl = _structural_sl(entry, side, m15, m5, use_fvg, symbol)
    tp = _smc_tp(entry, side, m15, liq_hi, liq_lo, fvgs15)

    risk   = abs(entry - sl)
    reward = abs(tp - entry)
    if risk <= 1e-12:
        return out
    rr = reward / risk

    # no-chase
    if _no_chase_rule(entry, use_fvg, tp):
        return out

    # --- СКОPИНГ (7 стратегий) ---
    score = 0
    why = []

    # База
    base_ok = bool((side=="BUY" and bull_fvg) or (side=="SELL" and bear_fvg)) and bool(choch5)
    score += 40 if base_ok else 20
    if base_ok: why.append("FVG+CHoCH")

    # 1) OTE 62..79%
    try:
        imp_a, imp_b = _last_impulse_for_bos(m15, bias)
        if _fib_ote_ok(imp_a, imp_b, entry):
            score += 10; why.append("OTE 62-79%")
    except Exception:
        pass

    # 2) Stop-hunt
    if sweep15:
        score += 10; why.append("StopHunt M15")

    # 3) Сила FVG (больше средней ширины)
    try:
        if use_fvg:
            f_top, f_bot = (use_fvg[1], use_fvg[2])
            f_w = abs(f_top - f_bot)
            rng = float((m15["High"] - m15["Low"]).tail(20).mean())
            if rng > 0 and f_w >= 1.5 * rng:
                score += 7; why.append("Strong FVG")
    except Exception:
        pass

    # 4) Сессия
    if _in_session_utc():
        score += 5; why.append("Session")

    # 5) Выход из консолидации
    if _is_consolidation_break(m5):
        score += 12; why.append("Breakout")

    # 6) Mitigation: попадание в тело последнего H1/H4 бара
    if _inside_higher_ob(m5, h1) or _inside_higher_ob(m5, h4):
        score += 15; why.append("Mitigation H1/H4")

    # 7) DXY корреляция для XAU
    try:
        dxy_df = INTERMARKET.get("DXY")
        if symbol == "XAU" and isinstance(dxy_df, pd.DataFrame) and not dxy_df.empty:
            dxy60 = _resample_ohlc_uniform(dxy_df, 60)
            dxy240 = _resample_ohlc_uniform(dxy_df, 240)
            dxy_bias = _bias_bos_higher(dxy60, dxy240) if min(len(dxy60),len(dxy240))>=10 else None
            if dxy_bias:
                if side=="BUY" and dxy_bias=="DOWN":
                    score += 15; why.append("DXY↓ +15")
                if side=="SELL" and dxy_bias=="UP":
                    score += 15; why.append("DXY↑ +15")
    except Exception:
        pass

    # бонус за RR>=1.25 (более вкусные)
    if rr >= 1.25:
        score += 10; why.append("RR≥1.25 +10")

    score = max(0, min(100, score))
    if score < 5:
        return out

    out.update({
        "ok": True, "score": score, "side": side, "bias": bias,
        "entry": entry, "sl": sl, "tp": tp, "rr": rr,
        "why": ", ".join(why) if why else "SMC"
    })
    return out

# ===================== BUILD SETUP (ЧИСТЫЙ SMC) =====================

def build_setup(df: pd.DataFrame, symbol: str, tf_label: str):
    """
    Чистый SMC: только _smc_score. Никаких ATR/EMA/RSI в логике входа.
    Доп.проверки: RR ≥ 1.0 и TP_abs ≥ TP_MIN_TRADE[symbol] для TRADE.
    """
    if df is None or df.empty or len(df) < 300:
        return None

    df_local = df.copy()
    # ATR — только для статуса/показа
    try:
        df_local["ATR"] = atr(df_local, 14)
        atr_now = float(df_local["ATR"].iloc[-1]) if pd.notna(df_local["ATR"].iloc[-1]) else 0.0
    except Exception:
        atr_now = 0.0

    smc = _smc_score(symbol, df_local)
    if not smc.get("ok", False):
        return None

    side  = smc["side"]
    entry = float(smc["entry"])
    sl    = float(smc["sl"])
    tp    = float(smc["tp"])
    rr    = float(smc["rr"])
    conf  = float(smc["score"]) / 100.0
    tp_abs = abs(tp - entry)

    sig = sig_id(symbol, side, smc["bias"], tf_label)

    return {
        "symbol": symbol, "tf": tf_label,
        "side": side, "trend": smc["bias"],
        "entry": entry, "tp": tp, "sl": sl,
        "atr": atr_now, "rr": rr, "conf": conf, "sig": sig,
        "tp_abs": tp_abs, "tp_min": TP_MIN_TRADE.get(symbol, 0.0),
        "why": smc.get("why", "SMC")
    }

def format_signal(setup, buffer):
    sym = setup["symbol"]; side = setup["side"]; tf = setup["tf"]
    add = BUF_K_TO_LEVEL.get(sym, 1.0) * buffer
    tp = setup["tp"] + (add if side == "BUY" else -add)
    sl = setup["sl"] - (add if side == "BUY" else -add)
    why = setup.get("why","")
    return (
        f"{side} {SYMBOLS[sym]['name']} | {tf}\n"
        f"TP: {rnd(sym, tp)}\n"
        f"SL: {rnd(sym, sl)}\n"
        f"Entry: {rnd(sym, setup['entry'])}  Spread≈{rnd(sym, buffer)}  "
        f"ATR(14)≈{rnd(sym, setup.get('atr',0))}  Conf: {int(setup['conf']*100)}%  Bias: {setup.get('trend','')}\n"
        f"{('Reason: '+why) if why else ''}"
    )

# ===================== LEARNING =====================

def learn(symbol: str, outcome: str, sess: dict):
    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    if sess and "sig" in sess:
        p["last_sig"] = sess["sig"]; p["last_sig_ts"] = time.time(); p["last_outcome"] = outcome
    if outcome == "TP":
        p["win"] += 1; p["streak_win"] += 1; p["streak_loss"] = 0
    else:
        p["loss"] += 1; p["streak_loss"] += 1; p["streak_win"] = 0
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
bot     = Bot(BOT_TOKEN,    default=DefaultBotProperties(parse_mode=None))
log_bot = Bot(LOG_BOT_TOKEN,default=DefaultBotProperties(parse_mode=None))
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
        await m.answer(f"Режим {new_mode}: слежу за {mode_title(new_mode)}.")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"Bot alive ({VERSION}). Напиши 'команды' чтобы увидеть список. По умолчанию режим BTC.")
    await m.answer(f"Режим {mode}: слежу за {mode_title(mode)}.")

@router.message(F.text.lower() == "команды")
async def cmd_help(m: Message):
    await m.answer(
        "Команды:\n"
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
    await m.answer("Остановил. Открытых нет, короткий кулдаун.")

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
        "BUY BTC-USD | 1m\n"
        "TP: 114999.9\n"
        "SL: 114111.1\n"
        "Entry: 114555.5  Spread≈350.0  ATR(14)≈45.0  Conf: 72%  Bias: UP"
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
    text = f"TP hit on {name} @ {p}" if outcome=="TP" else f"SL hit on {name} @ {p}"
    try: await bot.send_message(OWNER_ID, text)
    except: pass

async def notify_be(symbol: str):
    name = SYMBOLS[symbol]["name"]
    try:
        await bot.send_message(OWNER_ID, f"ALERT: БЕЗУБЫТОК {name}. Реком.: перевести SL в BE (+0.001).")
    except: pass

# ===================== ENGINE =====================

def dynamic_buffer(symbol: str, _df: pd.DataFrame, _atr_unused: float) -> float:
    return SPREAD_BUFFER.get(symbol, 0.0)

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
    if df.empty or len(df) < 300:
        return

    # статус (ATR только информативно)
    last_candle_close_ts[symbol] = time.time()
    atr_now_series = atr(df, 14)
    atr_now = float(atr_now_series.iloc[-1]) if len(atr_now_series) else 0.0
    state[f"atr_{symbol}"] = rnd(symbol, atr_now)

    logging.info(f"HB {symbol}: last_close={rnd(symbol, float(df['Close'].iloc[-1]))} ATR≈{rnd(symbol, atr_now)}")

    # только по закрытию нового бара
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
            side = sess["side"]; tp = sess["tp"]; sl = sess["sl"]; entry = sess["entry"]
            # BE-алерт: достижение 1:1
            if not sess.get("be_alerted", False):
                risk = abs(entry - sl)
                fe = (float(post["High"].max()) - entry) if side=="BUY" else (entry - float(post["Low"].min()))
                if fe >= risk - 1e-9:
                    await notify_be(symbol)
                    sess["be_alerted"] = True
            # фиксация
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

    base_buffer = dynamic_buffer(symbol, df, setup.get("atr",0.0))
    add = BUF_K_TO_LEVEL.get(symbol, 1.0) * base_buffer
    side = setup["side"]
    tp_show = setup["tp"] + (add if side == "BUY" else -add)
    sl_show = setup["sl"] - (add if side == "BUY" else -add)

    conf = float(setup.get("conf", 0.0))
    pct  = int(round(conf * 100))
    rr_v = round(float(setup.get("rr", 0.0)), 3)
    entry = rnd(symbol, setup["entry"])
    atr_v = rnd(symbol, setup.get("atr", 0.0))
    name = SYMBOLS[symbol]["name"]
    why  = setup.get("why","")

    # ИДЕЯ (всегда, если ≥ 0.05)
    if conf >= CONF_MIN_IDEA and can_send_idea(symbol):
        txt = (
            f"{'СДЕЛКА' if (conf >= CONF_MIN_TRADE and rr_v >= RR_TRADE_MIN and setup['tp_abs'] >= setup['tp_min']) else 'ИДЕЯ'} {name} | {SYMBOLS[symbol]['tf']}\n"
            f"{side} | Conf: {pct}%  RR≈{rr_v}  ATR≈{atr_v}\n"
            f"{('Reason: '+why) if why else ''}\n"
            f"Entry: {entry}\n"
            f"TP: {rnd(symbol, tp_show)}   SL: {rnd(symbol, sl_show)}"
        )
        await send_text(txt)

    # БОЕВОЙ: CONF ≥ 0.55, RR ≥ 1.0, TP_abs ≥ 0.005
    if conf >= CONF_MIN_TRADE and float(setup.get("rr", 0.0)) >= RR_TRADE_MIN and float(setup.get("tp_abs", 0.0)) >= float(setup.get("tp_min", 0.0)):
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

# ===================== ALIVE LOOP (лог-бот) =====================

async def alive_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                df_ng  = await get_df(s,"NG")
                df_xau = await get_df(s,"XAU")

            def _atr_m15(df):
                if df is None or df.empty: return 0.0
                x = df.copy()
                base = pd.date_range(end=pd.Timestamp.utcnow(), periods=len(x), freq="1min")
                x.index = base
                m15 = x.resample("15min").agg({"Open":"first","High":"max","Low":"min","Close":"last"}).dropna()
                if len(m15) < 15: return 0.0
                tr = (m15["High"] - m15["Low"]).rolling(14).mean()
                return float(tr.iloc[-1]) if pd.notna(tr.iloc[-1]) else 0.0

            c_ng  = float(df_ng["Close"].iloc[-1])  if not df_ng.empty  else 0.0
            a_ng  = _atr_m15(df_ng)                 if not df_ng.empty  else 0.0
            c_xau = float(df_xau["Close"].iloc[-1]) if not df_xau.empty else 0.0
            a_xau = _atr_m15(df_xau)                if not df_xau.empty else 0.0

            msg = f"[ALIVE] NG: {rnd('NG',c_ng)}, ATR15: {rnd('NG',a_ng)} | XAU: {rnd('XAU',c_xau)}, ATR15: {rnd('XAU',a_xau)}. Status: OK."
            await log_bot.send_message(OWNER_ID, msg)
        except Exception as e:
            try:
                await log_bot.send_message(OWNER_ID, f"[ALIVE ERROR] {e}")
            except:
                pass
        await asyncio.sleep(300)

# ===================== MAIN =====================

router = Router()  # harmless duplicate
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    global mode, requested_mode, last_mode_switch_ts
    mode = state.get("mode","BTC")
    requested_mode = mode
    last_mode_switch_ts = time.time()
    asyncio.create_task(engine_loop())
    asyncio.create_task(alive_loop())  # лог-бот
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
