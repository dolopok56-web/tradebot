#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, csv, logging, asyncio, random, math
from datetime import datetime
from copy import deepcopy

import numpy as np
import pandas as pd
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ===================== VERSION =====================
VERSION = "V7.2 Human-Like Analyst (MTF 1m→1D, structural TP/SL, reasons, no auto-trade, ATR only in logs)"

# ===================== TOKENS / OWNER =====================
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# ===================== MARKETS / SETTINGS =====================
SYMBOLS = {
    "BTC": {"name": "BTC-USD",   "tf": "1m"},
    "NG":  {"name": "NG=F",      "tf": "1m"},
    "XAU": {"name": "XAUUSD=X",  "tf": "1m"},
}
DXY_TICKERS = ("DX-Y.NYB", "DX=F")

# Спред/буферы (используются для отступа от уровней, НЕ для расчёта TP по ATR)
SPREAD_BUFFER   = {"NG": 0.0020, "XAU": 0.20, "BTC": 5.0}    # минимальный отступ/компенсация
TP_EXTRA_BUFFER = {"NG": 0.0100, "XAU": 0.30, "BTC": 15.0}   # минимальный смысловой ход поверх спреда
SL_MIN_GAP      = {"NG": 0.0040, "XAU": 0.25, "BTC": 12.0}   # не ставить сверхблизко к входу

# Конф/скоринг
CONF_MIN_IDEA   = 0.25   # идея
CONF_MIN_TRADE  = 0.55   # “боевой” сигнал (по сути — сильная идея). Торговли всё равно нет.
RR_TRADE_MIN    = 1.20   # ориентир: не сигналим мусор
RR_MIN_IDEA     = 1.00

# Антиспам
SEND_IDEAS         = True
IDEA_COOLDOWN_SEC  = 180
MAX_IDEAS_PER_HOUR = 20

# Сессии (только как мягкий бонус к уверенности)
LONDON_HOURS = range(7, 15)   # UTC
NY_HOURS     = range(12, 21)  # UTC

# Скорость
POLL_SEC        = 1
ALIVE_EVERY_SEC = 300
BOOT_COOLDOWN_S = 15

TRADES_CSV = "gv_trades.csv"  # остаётся на будущее (лог закрытий если когда-то включишь автоторг)

HTTP_TIMEOUT   = 12
YAHOO_RETRIES  = 4
YAHOO_BACKOFF0 = 0.9
YAHOO_JITTER   = 0.35

ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ===================== STATE =====================
boot_ts = time.time()

# мы НЕ открываем сделки автоматически. Ниже — только защита от повторов.
_last_signal_idx = {"NG": -1, "XAU": -1, "BTC": -1}
_last_signal_fingerprint = {"NG": "", "XAU": "", "BTC": ""}

_prices_cache = {}
state = {}
mode = "AUTO"            # AUTO: NG+XAU; BTC — по команде “биток”
requested_mode = "AUTO"

# ===================== TELEGRAM =====================
router = Router()
bot_main = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
bot_log  = Bot(LOG_BOT_TOKEN,  default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

async def send_main(text: str):
    try:
        await bot_main.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.error(f"send_main error: {e}")

async def send_log(text: str):
    try:
        await bot_log.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.error(f"send_log error: {e}")

def mode_title(m: str) -> str:
    return {"BTC": "BITCOIN (BTC-USD)",
            "NG": "NATGAS (NG=F)",
            "XAU": "GOLD (XAUUSD)",
            "AUTO": "NATGAS+GOLD (AUTO)"} .get(m, m)

async def _request_mode(new_mode: str, m: Message | None = None):
    global requested_mode, mode
    requested_mode = new_mode
    mode = new_mode
    if m:
        await m.answer(f"✅ Режим {new_mode}: слежу за {mode_title(new_mode)}.")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot is alive ({VERSION}).\nНапиши 'команды' чтобы увидеть список.")
    await m.answer(f"✅ Текущий режим: {mode} — {mode_title(mode)}.")

@router.message(F.text.lower() == "команды")
async def cmd_help(m: Message):
    await m.answer(
        "📋 Команды:\n"
        "• /start — запуск\n"
        "• команды — список\n"
        "• биток / газ / золото / авто — выбор рынка\n"
        "• статус — диагностика\n"
        "• тест — тестовый сигнал (пример формата)\n"
    )

@router.message(F.text.lower() == "биток")
async def set_btc(m: Message): await _request_mode("BTC", m)

@router.message(F.text.lower() == "газ")
async def set_ng(m: Message):  await _request_mode("NG", m)

@router.message(F.text.lower() == "золото")
async def set_xau(m: Message): await _request_mode("XAU", m)

@router.message(F.text.lower() == "авто")
async def set_auto(m: Message): await _request_mode("AUTO", m)

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    lines = [f"mode: {mode} (requested: {requested_mode})",
             f"alive: OK | idea_cooldown={IDEA_COOLDOWN_SEC}s | poll={POLL_SEC}s"]
    for s in ["BTC","NG","XAU"]:
        atrtxt = state.get(f"atr_{s}", "—")
        lines.append(f"{SYMBOLS[s]['name']}: ATR15≈{atrtxt}")
    await m.answer("```\n"+ "\n".join(lines) + "\n```")

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    text = (
        "🧠 IDEA (пример):\n"
        "🔥 BUY NG=F | 1m\n"
        "✅ TP: **2.9150**\n"
        "🟥 SL: **2.9020**\n"
        "Entry: 2.9080  SpreadBuf≈0.0020  RR≈1.6  Conf: 68%  Bias: UP\n"
        "🧠 Почему:\n"
        "• H1/H4 структура указывает вверх\n"
        "• Забрали ликвидность на M15, быстрый возврат\n"
        "• TP перед ближайшей ликвидностью, SL за локальным минимумом\n"
        "• Хороший микро-импульс по 1m"
    )
    await m.answer(text)

# ===================== PRICE FEEDS =====================
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
        df = df.ffill().bfill().dropna()
        for col in ("Open","High","Low","Close"):
            df = df[df[col] > 0]
        return df.tail(1500).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

def _df_from_stooq_csv(text: str):
    try:
        from io import StringIO
        if not text or "Date,Open,High,Low,Close" not in text:
            return pd.DataFrame()
        df = pd.read_csv(StringIO(text))
        if not {"Open","High","Low","Close"}.issubset(set(df.columns)):
            return pd.DataFrame()
        return df.tail(1500).reset_index(drop=True)
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
    now_ts = time.time()
    c = _prices_cache.get(symbol)
    cache_ttl = 1.0
    if c and (now_ts - c["ts"] < cache_ttl) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]

    if symbol == "NG":
        for t in ("NG%3DF",):
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
            if not df.empty:
                _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                return df
        df = await _get_df_stooq_1m(session, "ng.f")
        if not df.empty:
            _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"stooq"}
            return df
        return pd.DataFrame()

    if symbol == "XAU":
        for t in ("XAUUSD%3DX", "GC%3DF"):
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
            if not df.empty:
                _prices_cache["XAU"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                return df
        for s in ("xauusd","gc.f"):
            df = await _get_df_stooq_1m(session, s)
            if not df.empty:
                _prices_cache["XAU"] = {"ts": now_ts, "df": df, "feed":"stooq"}
                return df
        return pd.DataFrame()

    if symbol == "BTC":
        for t in ("BTC-USD",):
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
            if not df.empty:
                _prices_cache["BTC"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                return df
        return pd.DataFrame()

    return pd.DataFrame()

async def get_dxy_df(session: aiohttp.ClientSession) -> pd.DataFrame:
    for t in DXY_TICKERS:
        df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
        if not df.empty:
            return df
    return pd.DataFrame()

# ===================== UTILS / ANALYTICS =====================
def rnd(sym: str, x: float) -> float:
    if sym == "NG":  return round(float(x), 4)
    if sym == "XAU": return round(float(x), 2)
    if sym == "BTC": return round(float(x), 2)
    return round(float(x), 4)

def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
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

def fvg_last_soft(df: pd.DataFrame, lookback: int = 20, use_bodies: bool = True,
                  min_abs: float = 0.0, min_rel_to_avg: float = 0.0):
    n = len(df)
    if n < 4:
        return False, "", 0.0, 0.0, 0.0
    avg_rng = float((df["High"] - df["Low"]).tail(max(lookback, 12)).mean() or 0.0)
    for i in range(n-2, max(1, n - lookback) - 1, -1):
        if use_bodies:
            h2 = max(float(df["Open"].iloc[i-2]), float(df["Close"].iloc[i-2]))
            l2 = min(float(df["Open"].iloc[i-2]), float(df["Close"].iloc[i-2]))
            h0 = max(float(df["Open"].iloc[i]),   float(df["Close"].iloc[i]))
            l0 = min(float(df["Open"].iloc[i]),   float(df["Close"].iloc[i]))
        else:
            h2 = float(df["High"].iloc[i-2]); l2 = float(df["Low"].iloc[i-2])
            h0 = float(df["High"].iloc[i]);   l0 = float(df["Low"].iloc[i])
        if l0 > h2:
            top, bot = l0, h2
            width = abs(top - bot)
            if width >= min_abs and (min_rel_to_avg <= 0.0 or (avg_rng > 0 and width >= min_rel_to_avg * avg_rng)):
                return True, "BULL", top, bot, width
        if h0 < l2:
            top, bot = h2, l0
            width = abs(top - bot)
            if width >= min_abs and (min_rel_to_avg <= 0.0 or (avg_rng > 0 and width >= min_rel_to_avg * avg_rng)):
                return True, "BEAR", top, bot, width
    return False, "", 0.0, 0.0, 0.0

def choch_soft(df: pd.DataFrame, want: str, swing_lookback: int = 8, confirm_break: bool = False):
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

def bias_bos_higher(df60, df240) -> str:
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
    if df is None or df.empty or len(df) < lookback+3: return (False,"")
    i = len(df) - 2
    hh = _swing_high(df, lookback)
    ll = _swing_low(df, lookback)
    H = float(df["High"].iloc[i]); L = float(df["Low"].iloc[i]); C = float(df["Close"].iloc[i])
    if H > hh and C < hh: return True, "DOWN"
    if L < ll and C > ll: return True, "UP"
    return False, ""

def dynamic_buffer(symbol: str) -> float:
    return SPREAD_BUFFER.get(symbol, 0.0)

# --- локальные экстремумы на 1m (pivot'ы) ---
def _local_extrema_1m(df1m: pd.DataFrame, lookback: int, mode: str):
    if df1m is None or df1m.empty:
        return []
    data = df1m.tail(max(lookback, 50)).reset_index(drop=True)
    if len(data) < 7:
        return []
    highs = data["High"].astype(float).to_numpy()
    lows  = data["Low"].astype(float).to_numpy()
    res = []
    for i in range(3, len(data)-3):
        if mode == "high":
            window = highs[i-3:i+4]
            price = float(highs[i])
            if price >= float(window.max()):
                res.append(("H", price))
        else:
            window = lows[i-3:i+4]
            price = float(lows[i])
            if price <= float(window.min()):
                res.append(("L", price))
    return res

# собрать уровни разного масштаба из 1m
def _collect_levels_multiscale(df1m: pd.DataFrame, bias: str):
    # набор окон в барах (мин)
    scales = [60, 120, 180, 360, 720]
    ups, downs = set(), set()
    for win in scales:
        hs = _local_extrema_1m(df1m, win, "high")
        ls = _local_extrema_1m(df1m, win, "low")
        for _, p in hs: ups.add(round(float(p), 8))
        for _, p in ls: downs.add(round(float(p), 8))
    # вернём списки
    return sorted(list(ups)), sorted(list(downs))

def momentum_confirmation_simple(df1m: pd.DataFrame, side: str):
    # без ATR: проверяем 4 последних тела и суммарное направление
    if df1m is None or df1m.empty or len(df1m) < 6:
        return False, ""
    recent = df1m.iloc[-5:-1].copy()
    bodies = (recent["Close"] - recent["Open"]).astype(float)
    aligned = int((bodies > 0).sum() if side=="BUY" else (bodies < 0).sum())
    net_move = float(recent["Close"].iloc[-1] - recent["Open"].iloc[0])
    direction_ok = net_move > 0 if side=="BUY" else net_move < 0
    # не хотим входить после “слишком большого” импульса: сравним с средним H-L последних 20 баров
    rng = (df1m["High"] - df1m["Low"]).astype(float)
    avg_rng = float(rng.tail(20).mean() or 0.0)
    too_late = abs(net_move) > 1.2 * avg_rng if avg_rng > 0 else False
    ok = (aligned >= 2) and direction_ok and (not too_late)
    if not ok:
        return False, ""
    reason = f"Импульс подтверждён {aligned}/4 свечами 1m, вход не поздний"
    return True, reason

def dxy_bias_from_df(dxy_1m: pd.DataFrame) -> str|None:
    if dxy_1m is None or dxy_1m.empty: return None
    df60 = _resample(dxy_1m, 60)
    df240 = _resample(dxy_1m, 240)
    if df60.empty or df240.empty: return None
    return bias_bos_higher(df60, df240)

def format_signal(setup, buffer):
    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]
    header = (
        f"🧠 IDEA:\n"
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}\n"
        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
        f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}  "
        f"RR≈{round(setup['rr'],2)}  Conf: {int(setup['conf']*100)}%  Bias: {setup['trend']}"
    )
    reasons = setup.get("reasons", [])
    if reasons:
        header += "\n🧠 Почему:\n" + "\n".join(f"• {r}" for r in reasons)
    return header

# ===================== BUILD SETUP =====================
def build_setup(df1m: pd.DataFrame, symbol: str, tf_label: str, dxy_bias: str | None = None):
    if df1m is None or df1m.empty or len(df1m) < 200:
        return None

    # MTF (из 1m): используем для направления и контекста, НЕ для жёстких правил
    df5    = _resample(df1m, 5)
    df15   = _resample(df1m, 15)
    df60   = _resample(df1m, 60)
    df240  = _resample(df1m, 240)
    df1440 = _resample(df1m, 1440)
    if df5.empty or df15.empty or df60.empty or df240.empty:
        return None

    bias = bias_bos_higher(df60, df240)  # ориентир с H1/H4
    reasons = [f"H1/H4 структура → {('вверх' if bias=='UP' else 'вниз')}"]

    # глазам нужен контекст ликвидности
    fvg_ok, fvg_dir, _, _, fvg_w = fvg_last_soft(df15, lookback=20, use_bodies=True, min_abs=0.0, min_rel_to_avg=0.0)
    if fvg_ok:
        reasons.append(f"Свежий {('бычий' if fvg_dir=='BULL' else 'медвежий')} FVG на M15")

    sweep15, sweep_dir15 = had_liquidity_sweep(df15, lookback=20)
    if sweep15:
        reasons.append("Вынос ликвидности на M15 перед входом")

    side = "BUY" if bias == "UP" else "SELL"
    if sweep15:
        if sweep_dir15 == "UP": side = "BUY"
        if sweep_dir15 == "DOWN": side = "SELL"

    entry = float(df1m["Close"].iloc[-2])
    buf   = dynamic_buffer(symbol)

    # уровни из многомасштабной 1m-структуры
    ups, downs = _collect_levels_multiscale(df1m, bias)

    # SL — прячем за ближайшую противоположную ликвидность, но не слишком близко
    sl_price = None
    if side == "BUY":
        lower = [p for p in downs if p < entry]; lower.sort(reverse=True)
        for a in lower:
            c = float(a) - buf
            if (entry - c) >= SL_MIN_GAP.get(symbol, buf):
                sl_price = c
                reasons.append(f"SL под локальным минимумом ≈ {rnd(symbol, a)}")
                break
    else:
        upper = [p for p in ups if p > entry]; upper.sort()
        for a in upper:
            c = float(a) + buf
            if (c - entry) >= SL_MIN_GAP.get(symbol, buf):
                sl_price = c
                reasons.append(f"SL над локальным максимумом ≈ {rnd(symbol, a)}")
                break
    if sl_price is None:
        return None

    # TP — ближайшая по ходу ликвидность (не ставим “внутрь” уровня, даём отступ на спред)
    tp_price = None
    if side == "BUY":
        forward = [u for u in ups if u > entry]; forward.sort()
        for a in forward:
            t = float(a) - buf
            if t > entry and (t - entry) >= (SPREAD_BUFFER.get(symbol,0.0) + TP_EXTRA_BUFFER.get(symbol,0.0)):
                tp_price = t
                reasons.append(f"TP перед ликвидностью ≈ {rnd(symbol, a)}")
                break
    else:
        forward = [d for d in downs if d < entry]; forward.sort(reverse=True)
        for a in forward:
            t = float(a) + buf
            if t < entry and (entry - t) >= (SPREAD_BUFFER.get(symbol,0.0) + TP_EXTRA_BUFFER.get(symbol,0.0)):
                tp_price = t
                reasons.append(f"TP до ликвидности ≈ {rnd(symbol, a)}")
                break
    if tp_price is None:
        return None

    # микро-импульс 1m — чтоб не заходить в самый хвост
    ok_mom, mom_reason = momentum_confirmation_simple(df1m, side)
    if not ok_mom:
        return None
    reasons.append(mom_reason)

    # лёгкие бонусы
    if _in_session_utc(): reasons.append("Активная сессия добавляет ликвидности")
    if symbol == "XAU" and dxy_bias:
        if side == "BUY" and dxy_bias == "DOWN":
            reasons.append("DXY ↓ поддерживает лонг по золоту")
        if side == "SELL" and dxy_bias == "UP":
            reasons.append("DXY ↑ поддерживает шорт по золоту")

    rr = abs(tp_price - entry) / max(abs(entry - sl_price), 1e-9)

    # скоринг (простой и понятный)
    score = 10
    if fvg_ok:               score += 12
    if sweep15:              score += 14
    if rr >= 1.2:            score += 8
    if rr >= 1.6:            score += 6
    if _in_session_utc():    score += 4
    conf = max(0.0, min(1.0, score/100.0))
    if conf < CONF_MIN_IDEA:
        return None

    return {
        "symbol": symbol, "tf": tf_label,
        "side": side, "trend": bias,
        "entry": entry, "tp": float(tp_price), "sl": float(sl_price),
        "rr": rr, "conf": conf, "tp_abs": abs(tp_price - entry),
        "tp_min": SPREAD_BUFFER.get(symbol,0.0) + TP_EXTRA_BUFFER.get(symbol,0.0),
        "reasons": reasons
    }

# ===================== ENGINE =====================
_last_idea_ts = {"NG": 0.0, "XAU": 0.0, "BTC": 0.0}
_ideas_count_hour = {"NG": 0, "XAU": 0, "BTC": 0}
_ideas_count_hour_ts = {"NG": 0.0, "XAU": 0.0, "BTC": 0.0}

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

def is_fresh_enough(symbol: str, entry: float, close_now: float) -> bool:
    # сигналим только если цена не убежала дальше “свежести”
    lim = 10.0 * SPREAD_BUFFER.get(symbol, 0.0)
    return abs(float(entry) - float(close_now)) <= lim

async def handle_symbol(session: aiohttp.ClientSession, symbol: str, dxy_df: pd.DataFrame | None):
    # режим: если один рынок выбран, не трогаем другие
    if mode != "AUTO" and symbol not in (mode,):
        return

    df = await get_df(session, symbol)
    if df.empty or len(df) < 200:
        return

    cur_idx = len(df) - 1
    closed_idx = cur_idx - 1
    if closed_idx <= _last_signal_idx[symbol]:
        return

    # dxy только для XAU
    dxy_bias = dxy_bias_from_df(dxy_df) if symbol=="XAU" and dxy_df is not None and not dxy_df.empty else None

    setup = build_setup(df, symbol, SYMBOLS[symbol]["tf"], dxy_bias=dxy_bias)
    if not setup:
        _last_signal_idx[symbol] = closed_idx
        return

    # свежесть/антидубль
    close_now = float(df["Close"].iloc[-1])
    if not is_fresh_enough(symbol, float(setup["entry"]), close_now):
        _last_signal_idx[symbol] = closed_idx
        return

    fingerprint = f"{setup['side']}|{rnd(symbol,setup['entry'])}|{rnd(symbol,setup['tp'])}|{rnd(symbol,setup['sl'])}"
    if fingerprint == _last_signal_fingerprint.get(symbol, ""):
        _last_signal_idx[symbol] = closed_idx
        return

    # IDEA
    if setup["conf"] >= CONF_MIN_IDEA and setup["rr"] >= RR_MIN_IDEA and can_send_idea(symbol):
        txt = format_signal(setup, dynamic_buffer(symbol))
        await send_main(txt)
        _last_signal_idx[symbol] = closed_idx
        _last_signal_fingerprint[symbol] = fingerprint
        _last_idea_ts[symbol] = time.time()
        _ideas_count_hour[symbol] = _ideas_count_hour.get(symbol, 0) + 1
        if _ideas_count_hour_ts.get(symbol, 0.0) == 0.0:
            _ideas_count_hour_ts[symbol] = time.time()
        return

    # “боевой” (просто более сильная идея — ТЫ всё равно решаешь руками)
    if setup["conf"] >= CONF_MIN_TRADE and setup["rr"] >= RR_TRADE_MIN:
        txt = format_signal(setup, dynamic_buffer(symbol))
        await send_main(txt)
        _last_signal_idx[symbol] = closed_idx
        _last_signal_fingerprint[symbol] = fingerprint
        return

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        dxy_df = None
        dxy_ts = 0.0
        while True:
            try:
                if time.time() - dxy_ts > 25:
                    dxy_df = await get_dxy_df(session)
                    dxy_ts = time.time()
                symbols_to_run = ("NG","XAU") if mode == "AUTO" else (mode,)
                for s in symbols_to_run:
                    await handle_symbol(session, s, dxy_df)
                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(2)

# ===================== ALIVE LOOP (ATR только в логах) =====================
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
                df_btc = await get_df(s, "BTC")
            c_ng  = float(df_ng["Close"].iloc[-1])  if not df_ng.empty else 0.0
            c_xau = float(df_xau["Close"].iloc[-1]) if not df_xau.empty else 0.0
            c_btc = float(df_btc["Close"].iloc[-1]) if not df_btc.empty else 0.0
            a_ng  = _atr_m15(df_ng)  if not df_ng.empty else 0.0
            a_xau = _atr_m15(df_xau) if not df_xau.empty else 0.0
            a_btc = _atr_m15(df_btc) if not df_btc.empty else 0.0
            state["atr_NG"]  = rnd("NG", a_ng)
            state["atr_XAU"] = rnd("XAU", a_xau)
            state["atr_BTC"] = rnd("BTC", a_btc)
            msg = (
                f"[ALIVE] "
                f"NG: {rnd('NG',c_ng)}, ATR15: {rnd('NG',a_ng)} | "
                f"XAU: {rnd('XAU',c_xau)}, ATR15: {rnd('XAU',a_xau)} | "
                f"BTC: {rnd('BTC',c_btc)}, ATR15: {rnd('BTC',a_btc)}. Status: OK."
            )
            await send_log(msg)
        except Exception as e:
            await send_log(f"[ALIVE ERROR] {e}")
        await asyncio.sleep(ALIVE_EVERY_SEC)

# ===================== MAIN =====================
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine_loop())
    asyncio.create_task(alive_loop())
    await dp.start_polling(bot_main)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
