#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TradeBot V5.0 — «ЧЕЛОВЕК-ТРЕЙДЕР»
— Чистый SMC-аналитик без душных фильтров: ATR/EMA/RSI не влияют на входы
— Мультитаймфрейм (H4/H1/M15/M5/M1) через ресемпл из 1m свечей
— «Мягкие глаза»: FVG максимально ослаблен (ищет микрогэпы), CHoCH мягкий
— 7-факторный скоринг (Stop-hunt, OTE, дисбаланс FVG, сессия, консолидация, OB-митигация, DXY для золота)
— ИДЕИ: CONF>=0.05 ; ТРЕЙДЫ: CONF>=0.55 и RR>=1.0 и TP>=0.005
— Буфер спреда: NG=0.0020, XAU=0.20
— Живой отчёт (ALIVE) каждые 5 минут — во второй лог-бот
"""

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

VERSION = "V5.0 Human-Trader (pure SMC, soft FVG/CHoCH, 7factors, alive)"

# --- Telegram
MAIN_BOT_TOKEN = "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"  # сигналы/идеи
LOG_BOT_TOKEN  = "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw"  # ALIVE/диагностика
OWNER_ID  = 6784470762

# --- Инструменты
SYMBOLS = {
    "NG":  {"name": "NG=F",      "tf": "1m"},     # Natural Gas futures (Yahoo)
    "XAU": {"name": "XAUUSD=X",  "tf": "1m"},     # spot gold; фолбэк GC=F
}
DXY_TICKERS = ("DX-Y.NYB", "DX=F")

# --- Порог шума/буфер на уровни
SPREAD_BUFFER = {"NG": 0.0020, "XAU": 0.20}

# --- Пороги активности
CONF_MIN_IDEA   = 0.05     # шлём ИДЕИ от 5%
CONF_MIN_TRADE  = 0.55     # боевой сигнал от 55%
RR_TRADE_MIN    = 1.00     # трейд, только если TP>=SL (RR>=1.0)
TP_MIN_TRADE    = {"NG": 0.005, "XAU": 0.005}

# --- Антиспам ИДЕЙ
SEND_IDEAS           = True
IDEA_COOLDOWN_SEC    = 90
MAX_IDEAS_PER_HOUR   = 20

# --- Сессии (UTC)
LONDON_HOURS = range(7, 15)
NY_HOURS     = range(12, 21)

# --- Лупы/времена
POLL_SEC           = 6     # разбор рынка
ALIVE_EVERY_SEC    = 300   # раз в 5 минут — отчёт
BOOT_COOLDOWN_S    = 30
COOLDOWN_SEC       = 10
COOLDOWN_SEC_NG    = 7

# --- Файлы лога сделок
TRADES_CSV   = "gv_trades.csv"

# --- HTTP
HTTP_TIMEOUT = 12
YAHOO_RETRIES = 4
YAHOO_BACKOFF0 = 0.9
YAHOO_JITTER = 0.35

ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ===================== STATE =====================

boot_ts = time.time()

# открытая «сессия» по каждому символу (1 активная)
trade = {"NG": None, "XAU": None}
cooldown_until = {"NG": 0.0, "XAU": 0.0}
last_candle_close_ts = {"NG": 0.0, "XAU": 0.0}

# антиспам для идей
_last_idea_ts = {"NG": 0.0, "XAU": 0.0}
_ideas_count_hour = {"NG": 0, "XAU": 0}
_ideas_count_hour_ts = {"NG": 0.0, "XAU": 0.0}

# индекс обработанного закрытого бара
last_seen_idx   = {"NG": -1, "XAU": -1}
last_signal_idx = {"NG": -1, "XAU": -1}

# кэш прайсов
_prices_cache = {}

# ===================== TELEGRAM =====================

router = Router()
main_bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
log_bot  = Bot(LOG_BOT_TOKEN,  default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

async def send_main(text: str):
    try: await main_bot.send_message(OWNER_ID, text)
    except: pass

async def send_log(text: str):
    try: await log_bot.send_message(OWNER_ID, text)
    except: pass

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ TradeBot {VERSION} запущен.\nИдеи от {int(CONF_MIN_IDEA*100)}%, трейды RR≥{RR_TRADE_MIN:.2f}, CONF≥{int(CONF_MIN_TRADE*100)}%.")

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    for s in trade.keys(): trade[s] = None; cooldown_until[s] = time.time() + 5
    await m.answer("🛑 Остановил. Открытых нет, короткий кулдаун.")

# ===================== IO: Price Feeds (Yahoo/Stooq) =====================

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
        df = df.dropna()
        for col in ("Open","High","Low","Close"):
            df = df[df[col] > 0]
        return df.tail(1000).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

async def _yahoo_json(session: aiohttp.ClientSession, url: str) -> dict:
    backoff = YAHOO_BACKOFF0
    for _ in range(YAHOO_RETRIES):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429, 503):
                    await asyncio.sleep(backoff + (random.random()*YAHOO_JITTER))
                    backoff *= 1.7
                    continue
                return {}
        except:
            await asyncio.sleep(backoff + (random.random()*YAHOO_JITTER))
            backoff *= 1.6
    return {}

async def _get_df_yahoo_1m(session, ticker: str) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
    data = await _yahoo_json(session, url)
    return _df_from_yahoo_v8(data)

def _df_from_stooq_csv(text: str):
    try:
        from io import StringIO
        if not text or "Date,Open,High,Low,Close" not in text:
            return pd.DataFrame()
        df = pd.read_csv(StringIO(text))
        if not {"Open","High","Low","Close"}.issubset(df.columns):
            return pd.DataFrame()
        return df.tail(1000).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

async def _get_df_stooq_1m(session, stooq_code: str) -> pd.DataFrame:
    url = f"https://stooq.com/q/d/l/?s={stooq_code}&i=1"
    try:
        async with session.get(url, timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
            if r.status == 200:
                txt = await r.text()
                return _df_from_stooq_csv(txt)
    except:
        pass
    return pd.DataFrame()

async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
    """
    NG => Yahoo NG=F (фолбэк stooq: ng.f)
    XAU => Yahoo XAUUSD=X (фолбэк GC=F, stooq: xauusd/gc.f)
    """
    now_ts = time.time()
    c = _prices_cache.get(symbol)
    cache_ttl = 10.0
    if c and (now_ts - c["ts"] < cache_ttl) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]

    if symbol == "NG":
        for t in ("NG%3DF",):
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
            if not df.empty:
                last_candle_close_ts["NG"] = time.time()
                _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                return df
        df = await _get_df_stooq_1m(session, "ng.f")
        if not df.empty:
            last_candle_close_ts["NG"] = time.time()
            _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"stooq"}
            return df
        return pd.DataFrame()

    if symbol == "XAU":
        # пробуем спот и фьюч
        for t in ("XAUUSD%3DX", "GC%3DF"):
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
            if not df.empty:
                last_candle_close_ts["XAU"] = time.time()
                _prices_cache["XAU"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                return df
        for s in ("xauusd","gc.f"):
            df = await _get_df_stooq_1m(session, s)
            if not df.empty:
                last_candle_close_ts["XAU"] = time.time()
                _prices_cache["XAU"] = {"ts": now_ts, "df": df, "feed":"stooq"}
                return df
        return pd.DataFrame()

    # для DXY вызываем отдельную функцию
    return pd.DataFrame()

async def get_dxy_df(session: aiohttp.ClientSession) -> pd.DataFrame:
    for t in DXY_TICKERS:
        df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
        if not df.empty:
            return df
    return pd.DataFrame()

# ===================== UTILS =====================

def rnd(sym: str, x: float) -> float:
    if sym == "NG":  return round(float(x), 4)
    if sym == "XAU": return round(float(x), 2)
    return round(float(x), 4)

def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Ресемпл по фактическому временному индексу (мягко к дыркам)."""
    if df is None or df.empty:
        return pd.DataFrame()
    # создадим временной индекс с 1m шагом от первого до последнего таймстемпа
    idx = pd.date_range(start=pd.Timestamp.utcnow().floor('D'), periods=len(df), freq="1min")
    z = df.copy()
    z.index = idx
    o = z["Open"].resample(f"{minutes}min").first()
    h = z["High"].resample(f"{minutes}min").max()
    l = z["Low"].resample(f"{minutes}min").min()
    c = z["Close"].resample(f"{minutes}min").last()
    r = pd.concat([o,h,l,c], axis=1).dropna()
    r.columns = ["Open","High","Low","Close"]
    return r.reset_index(drop=True)

def _swing_high(df, lookback=20):
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["High"].iloc[L:i+1].max())

def _swing_low(df, lookback=20):
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["Low"].iloc[L:i+1].min())

def _in_session_utc():
    h = pd.Timestamp.utcnow().hour
    return (h in LONDON_HOURS) or (h in NY_HOURS)

# ===================== FVG / CHoCH (SOFT) =====================

def fvg_last_soft(df: pd.DataFrame, lookback: int = 20, use_bodies: bool = True,
                  min_abs: float = 0.0, min_rel_to_avg: float = 0.0):
    """
    МАКСИМАЛЬНО МЯГКИЙ поиск FVG за последние `lookback` баров.
    — Допускаем микрогэпы (min_abs=0) по телам (use_bodies=True)
    — Можно дополнительно требовать ширину как долю средней свечи (min_rel_to_avg), по умолчанию = 0
    Возвращает: (has_fvg, dir, top, bot, width)
    """
    n = len(df)
    if n < 4:
        return False, "", 0.0, 0.0, 0.0

    avg_rng = float((df["High"] - df["Low"]).tail(max(lookback, 12)).mean() or 0.0)

    # пробегаем от свежего к старому
    for i in range(n-2, max(1, n - lookback) - 1, -1):
        if use_bodies:
            h2 = max(float(df["Open"].iloc[i-2]), float(df["Close"].iloc[i-2]))
            l2 = min(float(df["Open"].iloc[i-2]), float(df["Close"].iloc[i-2]))
            h0 = max(float(df["Open"].iloc[i]),   float(df["Close"].iloc[i]))
            l0 = min(float(df["Open"].iloc[i]),   float(df["Close"].iloc[i]))
        else:
            h2 = float(df["High"].iloc[i-2]); l2 = float(df["Low"].iloc[i-2])
            h0 = float(df["High"].iloc[i]);   l0 = float(df["Low"].iloc[i])

        # BULL FVG: нижняя граница текущего бара выше верхней границы бара i-2
        if l0 > h2:
            top, bot = l0, h2
            width = abs(top - bot)
            if width >= min_abs and (min_rel_to_avg <= 0.0 or (avg_rng > 0 and width >= min_rel_to_avg * avg_rng)):
                return True, "BULL", top, bot, width

        # BEAR FVG
        if h0 < l2:
            top, bot = h2, l0
            width = abs(top - bot)
            if width >= min_abs and (min_rel_to_avg <= 0.0 or (avg_rng > 0 and width >= min_rel_to_avg * avg_rng)):
                return True, "BEAR", top, bot, width

    return False, "", 0.0, 0.0, 0.0

def choch_soft(df: pd.DataFrame, want: str, swing_lookback: int = 8, confirm_break: bool = False):
    """
    Мягкий CHoCH: берём high/low за окно `swing_lookback`, допускаем пробой на текущем или предыдущем close.
    want: "UP" или "DOWN"
    """
    n = len(df)
    if n < swing_lookback + 3: return False

    i = n - 2
    local_high = float(df["High"].iloc[i - swing_lookback:i].max())
    local_low  = float(df["Low"].iloc[i - swing_lookback:i].min())

    c_prev = float(df["Close"].iloc[i-1])
    c_now  = float(df["Close"].iloc[i])

    if want == "UP":
        return (c_now > local_high) or (not confirm_break and c_prev > local_high)
    else:
        return (c_now < local_low)  or (not confirm_break and c_prev < local_low)

# ===================== Остальной SMC =====================

def bias_bos_higher(df60, df240) -> str:
    """Грубый H1/H4 bias: если текущий close выше swing-high — UP; ниже swing-low — DOWN; иначе — UP по умолчанию."""
    if df60 is None or df60.empty or df240 is None or df240.empty: return "UP"
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

def had_liquidity_sweep(df, lookback=20):
    """Ложный прокол локального high/low M15/M5"""
    if df is None or df.empty or len(df) < lookback+3: return (False,"")
    i = len(df) - 2
    hh = _swing_high(df, lookback)
    ll = _swing_low(df, lookback)
    H = float(df["High"].iloc[i])
    L = float(df["Low"].iloc[i])
    C = float(df["Close"].iloc[i])
    if H > hh and C < hh: return True, "DOWN"
    if L < ll and C > ll: return True, "UP"
    return False, ""

def is_consolidation_break(df):
    """Выход из узкой консолидации (узкий диапазон, затем пробой)"""
    if df is None or df.empty or len(df) < 20: return False
    i = len(df) - 2
    window = df.iloc[i-12:i].copy()
    rng = float((window["High"].max() - window["Low"].min()) or 0.0)
    base = float(window["Close"].iloc[-1])
    if base <= 0: return False
    # «узкая» консолидация
    if (rng / base) <= 0.003:
        H = float(df["High"].iloc[i]); L = float(df["Low"].iloc[i])
        return H > window["High"].max() or L < window["Low"].min()
    return False

def inside_higher_ob(df_low, df_high):
    """Грубое попадание текущей цены в тело последней H1/H4 свечи (как суррогат OB-митигации)."""
    if df_low is None or df_low.empty or df_high is None or df_high.empty: return False
    if len(df_low) < 5 or len(df_high) < 5: return False
    cl  = float(df_low["Close"].iloc[-2])
    body = df_high.iloc[-2]
    top = max(float(body["Open"]), float(body["Close"]))
    bot = min(float(body["Open"]), float(body["Close"]))
    return bot <= cl <= top

def fib_ote_ok(a, b, price):
    """OTE зона 62–79% импульса [a..b]"""
    if a == b: return False
    lo, hi = sorted([float(a), float(b)])
    lvl62 = hi - 0.62*(hi-lo)
    lvl79 = hi - 0.79*(hi-lo)
    loZ, hiZ = min(lvl62, lvl79), max(lvl62, lvl79)
    return loZ <= float(price) <= hiZ

# ===================== DXY для золота =====================

def dxy_bias_from_df(dxy_1m: pd.DataFrame) -> str|None:
    if dxy_1m is None or dxy_1m.empty: return None
    df60 = _resample(dxy_1m, 60)
    df240 = _resample(dxy_1m, 240)
    if df60.empty or df240.empty: return None
    return bias_bos_higher(df60, df240)

# ===================== BUILD SETUP =====================

def format_signal(setup, buffer):
    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]
    return (
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}\n"
        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
        f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}  "
        f"RR≈{round(setup['rr'],2)}  Conf: {int(setup['conf']*100)}%  Bias: {setup['trend']}"
    )

def dynamic_buffer(symbol: str) -> float:
    return SPREAD_BUFFER.get(symbol, 0.0)

def build_setup(df1m: pd.DataFrame, symbol: str, tf_label: str, dxy_bias: str | None = None):
    if df1m is None or df1m.empty or len(df1m) < 200:
        return None

    # MTF
    df5   = _resample(df1m, 5)
    df15  = _resample(df1m, 15)
    df60  = _resample(df1m, 60)
    df240 = _resample(df1m, 240)
    if df5.empty or df15.empty or df60.empty or df240.empty: return None

    bias = bias_bos_higher(df60, df240)

    # базовые «глаза»: максимально мягкие
    fvg_ok, fvg_dir, fvg_top, fvg_bot, fvg_w = fvg_last_soft(df15, lookback=20, use_bodies=True, min_abs=0.0, min_rel_to_avg=0.0)
    choch_ok = choch_soft(df5, "UP" if bias=="UP" else "DOWN", swing_lookback=8, confirm_break=False)

    # стоп-хант
    sweep15, sweep_dir15 = had_liquidity_sweep(df15, lookback=20)

    # сторона
    side = "BUY" if bias=="UP" else "SELL"
    if sweep15:
        if sweep_dir15=="UP": side="BUY"
        if sweep_dir15=="DOWN": side="SELL"

    entry = float(df5["Close"].iloc[-2])
    hi15  = _swing_high(df15, 20)
    lo15  = _swing_low(df15, 20)
    buf   = dynamic_buffer(symbol)

    if side == "BUY":
        sl = min(entry, lo15 - buf)       # SL только по структуре + буфер
        # TP — следующая явная LP: минимум цель — ближайший swing-high
        tp = hi15 if hi15 > entry else entry + max(entry-(sl), 1e-9)
    else:
        sl = max(entry, hi15 + buf)
        tp = lo15 if lo15 < entry else entry - max((sl)-entry, 1e-9)

    rr     = abs(tp - entry) / max(abs(entry - sl), 1e-9)
    tp_abs = abs(tp - entry)
    tp_min = TP_MIN_TRADE.get(symbol, 0.0)

    # ===== 7-ФАКТОРНЫЙ СКОРИНГ =====
    score = 0

    # (0) БАЗА: FVG/CHoCH — если что-то есть, даём широкую базу
    base_ok = (fvg_ok or choch_ok)
    score += 40 if base_ok else 10  # очень мягко: даже 10, если «почти пусто»

    # (1) OTE (Фибо 62–79) — +10
    last15 = df15.iloc[-2]
    if fib_ote_ok(float(last15["Open"]), float(last15["Close"]), entry):
        score += 10

    # (2) Stop-hunt — +10
    if sweep15: score += 10

    # (3) Дисбаланс/сила FVG — +7 (если FVG заметно шире средней свечи)
    if fvg_ok:
        avg_rng = float((df15["High"] - df15["Low"]).tail(20).mean() or 0.0)
        if avg_rng > 0 and fvg_w >= 1.5 * avg_rng:
            score += 7

    # (4) Активная сессия — +5
    if _in_session_utc(): score += 5

    # (5) Выход из консолидации — +12
    if is_consolidation_break(df5): score += 12

    # (6) OB-митигация (H1/H4 суррогат телом свечи) — +15
    if inside_higher_ob(df5, df60) or inside_higher_ob(df5, df240): score += 15

    # (7) DXY корреляция — ТОЛЬКО для XAU — +15
    if symbol == "XAU" and dxy_bias:
        if side == "BUY"  and dxy_bias == "DOWN": score += 15
        if side == "SELL" and dxy_bias == "UP":   score += 15

    # бонус за RR≥1.25 — +10 (но не обязательное условие)
    if rr >= 1.25: score += 10

    score = max(0, min(100, score))
    conf  = score / 100.0
    if conf < CONF_MIN_IDEA:
        return None

    return {
        "symbol": symbol, "tf": tf_label,
        "side": side, "trend": bias,
        "entry": entry, "tp": tp, "sl": sl,
        "rr": rr, "conf": conf, "tp_abs": tp_abs, "tp_min": tp_min
    }

# ===================== EXECUTION / LIFECYCLE =====================

def append_trade(row):
    newf = not os.path.exists(TRADES_CSV)
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if newf: w.writeheader()
        w.writerow(row)

async def notify_outcome(symbol: str, outcome: str, price: float):
    name = SYMBOLS[symbol]["name"]; p = rnd(symbol, price)
    text = f"✅ TP hit on {name} @ {p}" if outcome=="TP" else f"🟥 SL hit on {name} @ {p}"
    await send_main(text)

def finish_trade(symbol: str, outcome: str, price_now: float):
    sess = trade[symbol]
    trade[symbol] = None
    cooldown_until[symbol] = time.time() + (COOLDOWN_SEC_NG if symbol == "NG" else COOLDOWN_SEC)
    if not sess: return
    try:
        rr = (sess["tp"]-sess["entry"]) if sess["side"]=="BUY" else (sess["entry"]-sess["tp"])
        rl = (sess["entry"]-sess["sl"]) if sess["side"]=="BUY" else (sess["sl"]-sess["entry"])
        append_trade({
            "ts_close": datetime.utcnow().isoformat(timespec="seconds"),
            "symbol": symbol, "side": sess["side"],
            "entry": rnd(symbol, sess["entry"]), "tp": rnd(symbol, sess["tp"]),
            "sl": rnd(symbol, sess["sl"]), "outcome": outcome,
            "rr_ratio": round(float(rr)/max(float(rl),1e-9), 3),
            "life_sec": int(time.time()-sess.get("opened_at", time.time())),
        })
    except Exception as e:
        logging.error(f"log append error: {e}")

# ===================== ENGINE =====================

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

async def handle_symbol(session: aiohttp.ClientSession, symbol: str, dxy_df: pd.DataFrame | None):
    global last_seen_idx, last_signal_idx

    df = await get_df(session, symbol)
    if df.empty or len(df) < 200:
        return

    # новый закрытый бар?
    cur_idx = len(df) - 1
    closed_idx = cur_idx - 1
    if closed_idx <= last_seen_idx[symbol]:  # уже обработан
        return
    last_seen_idx[symbol] = closed_idx

    # сопровождение открытой сделки
    sess = trade[symbol]
    if sess:
        start_i = int(sess.get("entry_bar_idx", cur_idx))
        post = df.iloc[(start_i + 1):]
        if not post.empty:
            side = sess["side"]; tp = sess["tp"]; sl = sess["sl"]
            hit_tp = (post["High"].max() >= tp) if side=="BUY" else (post["Low"].min() <= tp)
            hit_sl = (post["Low"].min()  <= sl) if side=="BUY" else (post["High"].max() >= sl)
            if hit_tp:
                price_now = float(post["Close"].iloc[-1])
                await notify_outcome(symbol, "TP", price_now)
                finish_trade(symbol, "TP", price_now)
                return
            if hit_sl:
                price_now = float(post["Close"].iloc[-1])
                await notify_outcome(symbol, "SL", price_now)
                finish_trade(symbol, "SL", price_now)
                return
        return  # активная — новых входов не ищем

    # глобальные кулдауны
    if time.time() - boot_ts < BOOT_COOLDOWN_S: return
    if time.time() < cooldown_until[symbol]:   return

    # DXY bias (для золота)
    dxy_bias = dxy_bias_from_df(dxy_df) if symbol=="XAU" and dxy_df is not None and not dxy_df.empty else None

    setup = build_setup(df, symbol, SYMBOLS[symbol]["tf"], dxy_bias=dxy_bias)
    if not setup:
        return

    # защита от повтора на том же закрытом баре
    if last_signal_idx[symbol] == closed_idx:
        return
    last_signal_idx[symbol] = closed_idx

    # форматы/отправка
    buffer = dynamic_buffer(symbol)
    conf = float(setup["conf"])
    rr   = float(setup["rr"])

    # IDEA
    if conf >= CONF_MIN_IDEA and can_send_idea(symbol):
        await send_main("🧠 IDEA:\n" + format_signal(setup, buffer))
        _last_idea_ts[symbol] = time.time()
        _ideas_count_hour[symbol] = _ideas_count_hour.get(symbol, 0) + 1
        if _ideas_count_hour_ts.get(symbol, 0.0) == 0.0:
            _ideas_count_hour_ts[symbol] = time.time()

    # TRADE
    if (conf >= CONF_MIN_TRADE) and (rr >= RR_TRADE_MIN) and (setup["tp_abs"] >= setup["tp_min"]):
        await send_main(format_signal(setup, buffer))
        trade[symbol] = {
            "side": setup["side"],
            "entry": float(setup["entry"]),
            "tp": float(setup["tp"]),
            "sl": float(setup["sl"]),
            "opened_at": time.time(),
            "entry_bar_idx": cur_idx,
        }

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        dxy_df = None
        dxy_ts = 0.0
        while True:
            try:
                # обновим DXY иногда (каждые ~25 сек)
                if time.time() - dxy_ts > 25:
                    dxy_df = await get_dxy_df(session)
                    dxy_ts = time.time()

                for s in ("NG", "XAU"):
                    await handle_symbol(session, s, dxy_df)
                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(2)

# ===================== ALIVE LOOP =====================

def _atr_m15(df: pd.DataFrame) -> float:
    d = _resample(df, 15)
    if d.empty: return 0.0
    tr = (d["High"] - d["Low"]).rolling(14).mean()
    return float(tr.iloc[-1]) if not tr.empty and pd.notna(tr.iloc[-1]) else 0.0

async def alive_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                df_ng  = await get_df(s, "NG")
                df_xau = await get_df(s, "XAU")
            c_ng  = float(df_ng["Close"].iloc[-1])  if not df_ng.empty else 0.0
            c_xau = float(df_xau["Close"].iloc[-1]) if not df_xau.empty else 0.0
            a_ng  = _atr_m15(df_ng)  if not df_ng.empty else 0.0
            a_xau = _atr_m15(df_xau) if not df_xau.empty else 0.0
            msg = f"[ALIVE] NG: {rnd('NG',c_ng)}, ATR15: {rnd('NG',a_ng)} | XAU: {rnd('XAU',c_xau)}, ATR15: {rnd('XAU',a_xau)}. Status: OK."
            await send_log(msg)
        except Exception as e:
            await send_log(f"[ALIVE ERROR] {e}")
        await asyncio.sleep(ALIVE_EVERY_SEC)

# ===================== MAIN =====================

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    # запускаем движок + alive-логгер
    asyncio.create_task(engine_loop())
    asyncio.create_task(alive_loop())
    # телеграм поллинг для /start и "стоп"
    await dp.start_polling(main_bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
