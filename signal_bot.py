#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, csv, logging, asyncio, random
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
VERSION = "V7.1 Human-Pro (SMC/MTF, memory of levels, no-ATR logic, structure TP/SL, anti-dup, 1s speed, BTC manual)"

# ===================== TOKENS / OWNER =====================
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# ===================== MARKETS / SETTINGS =====================
# tf в подписи сигнала; сами данные берём 1m и ресемплим
SYMBOLS = {
    "BTC": {"name": "BTC-USD",          "tf": "5m"},   # BTC включаешь вручную /биток
    "NG":  {"name": "NATGAS (NG=F)",    "tf": "1m"},
    "XAU": {"name": "GOLD (XAUUSD=X)",  "tf": "1m"},
}
DXY_TICKERS = ("DX-Y.NYB", "DX=F")

# Спред-буферы (для SL и компенсации TP)
SPREAD_BUFFER = {"NG": 0.0020, "XAU": 0.20, "BTC": 5.0}

# Минимальная реальная дистанция TP (без микродвижений)
TP_MIN_ABS = {"NG": 0.0100, "XAU": 0.80, "BTC": 25.0}   # как просил: NG минимум 0.010

# Порог уверенности (BTC можешь поднять до 0.70)
CONF_MIN_TRADE = {"NG": 0.45, "XAU": 0.55, "BTC": 0.55}
CONF_MIN_IDEA  = 0.05

# RR-фильтров нет (оставлены 0 — только для отображения)
RR_TRADE_MIN = 0.0
RR_MIN_IDEA  = 0.0

# Антиспам идей отключён; новый сигнал только при отсутствии позиции
SEND_IDEAS         = True
IDEA_COOLDOWN_SEC  = 0
MAX_IDEAS_PER_HOUR = 120

# Торговые окна — лёгкий бонус к скору
LONDON_HOURS = range(7, 15)
NY_HOURS     = range(12, 21)

# Скорость и интервалы
POLL_SEC        = 1
ALIVE_EVERY_SEC = 300
BOOT_COOLDOWN_S = 20
COOLDOWN_SEC    = 0    # как просил — без паузы после SL

TRADES_CSV = "gv_trades.csv"

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

trade = {"NG": None, "XAU": None, "BTC": None}
cooldown_until = {"NG": 0.0, "XAU": 0.0, "BTC": 0.0}
last_candle_close_ts = {"NG": 0.0, "XAU": 0.0, "BTC": 0.0}

_last_idea_ts = {"NG": 0.0, "XAU": 0.0, "BTC": 0.0}
_ideas_count_hour = {"NG": 0, "XAU": 0, "BTC": 0}
_ideas_count_hour_ts = {"NG": 0.0, "XAU": 0.0, "BTC": 0.0}

last_seen_idx   = {"NG": -1, "XAU": -1, "BTC": -1}
last_signal_idx = {"NG": -1, "XAU": -1, "BTC": -1}
_last_signal_price = {"NG": None, "XAU": None, "BTC": None}

_prices_cache = {}
state = {
    "levels": {"NG": [], "XAU": [], "BTC": []},  # память уровней
}
mode = "AUTO"          # AUTO: NG + XAU; BTC — вручную
requested_mode = "AUTO"

# Параметры памяти уровней (сколько часов смотрим на ТФ)
LEVEL_MEMORY_HOURS = {"5m": 72, "15m": 72, "60m": 120}
LEVEL_DEDUP_TOL    = {"NG": 0.003, "XAU": 0.30, "BTC": 8.0}   # как слипать близкие уровни (по инструменту)
LEVEL_EXPIRE_SEC   = 48 * 3600                                 # старше 48ч — выкидываем

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
            "XAU": "GOLD (XAUUSD=X)",
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
        "• стоп — стоп и короткий кулдаун\n"
        "• статус — диагностика\n"
        "• отчет — 10 последних закрытий (только владелец)\n"
        "• тест — тестовый сигнал"
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
    now = time.time()
    for s in trade.keys():
        trade[s] = None
        cooldown_until[s] = now + 3
    await m.answer("🛑 Остановил. Открытых нет, короткий кулдаун.")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    now = time.time()
    lines = [f"mode: {mode} (requested: {requested_mode})",
             f"alive: OK | idea_cooldown={IDEA_COOLDOWN_SEC}s | poll={POLL_SEC}s"]
    for s in ["BTC","NG","XAU"]:
        opened = bool(trade[s])
        age = int(now - last_candle_close_ts[s]) if last_candle_close_ts[s] else -1
        atrtxt = state.get(f"atr_{s}", "—")
        nm = SYMBOLS[s]["name"]
        cd = max(0, int(cooldown_until[s]-now))
        L = len(state["levels"][s]) if isinstance(state.get("levels",{}).get(s), list) else 0
        lines.append(f"{nm}: ATR15≈{atrtxt}  open={opened}  cooldown={cd}  last_close_age={age}s  levels_mem={L}")
    await m.answer("```\n"+ "\n".join(lines) + "\n```")

@router.message(F.text.lower() == "отчет")
async def cmd_report(m: Message):
    if m.from_user.id != OWNER_ID:
        return await m.answer("Доступно только владельцу.")
    if not os.path.exists(TRADES_CSV):
        return await m.answer("Пока нет закрытых сделок.")
    rows = list(csv.DictReader(open(TRADES_CSV,encoding="utf-8")))[-10:]
    if not rows:
        return await m.answer("Пусто.")
    txt = "Последние 10 закрытий:\n"
    for r in rows:
        txt += (f"{r['ts_close']}  {r['symbol']}  {r['side']}  {r['outcome']}  "
                f"entry:{r['entry']} tp:{r['tp']} sl:{r['sl']} rr:{r['rr_ratio']}\n")
    await m.answer("```\n"+txt+"```")

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    text = (
        "🔥 BUY BTC-USD | 5m\n"
        "✅ TP: **114999.9**\n"
        "🟥 SL: **114111.1**\n"
        "Entry: 114555.5  Spread≈5.0  Conf: 72%  Bias: UP"
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
        return df.tail(2000).reset_index(drop=True)
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
        return df.tail(2000).reset_index(drop=True)
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
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d"))
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
        for t in ("XAUUSD%3DX", "GC%3DF"):
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d"))
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

    if symbol == "BTC":
        for t in ("BTC-USD",):
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d"))
            if not df.empty:
                last_candle_close_ts["BTC"] = time.time()
                _prices_cache["BTC"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                return df
        return pd.DataFrame()

    return pd.DataFrame()

async def get_dxy_df(session: aiohttp.ClientSession) -> pd.DataFrame:
    for t in DXY_TICKERS:
        df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d"))
        if not df.empty:
            return df
    return pd.DataFrame()

# ===================== SMC / MTF UTILS =====================
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

def fvg_last_soft(df: pd.DataFrame, lookback: int = 20, use_bodies: bool = True):
    n = len(df)
    if n < 4:
        return False, "", 0.0, 0.0, 0.0
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
            return True, "BULL", l0, h2, abs(l0-h2)
        if h0 < l2:
            return True, "BEAR", h2, l0, abs(h2-l0)
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

def had_liquidity_sweep(df, lookback=20):
    if df is None or df.empty or len(df) < lookback+3: return (False,"")
    i = len(df) - 2
    hh = _swing_high(df, lookback)
    ll = _swing_low(df, lookback)
    H = float(df["High"].iloc[i]); L = float(df["Low"].iloc[i]); C = float(df["Close"].iloc[i])
    if H > hh and C < hh: return True, "DOWN"
    if L < ll and C > ll: return True, "UP"
    return False, ""

def is_consolidation_break(df):
    if df is None or df.empty or len(df) < 20: return False
    i = len(df) - 2
    window = df.iloc[i-12:i].copy()
    rng = float((window["High"].max() - window["Low"].min()) or 0.0)
    base = float(window["Close"].iloc[-1])
    if base <= 0: return False
    if (rng / base) <= 0.003:
        H = float(df["High"].iloc[i]); L = float(df["Low"].iloc[i])
        return H > window["High"].max() or L < window["Low"].min()
    return False

def inside_higher_ob(df_low, df_high):
    if df_low is None or df_low.empty or df_high is None or df_high.empty: return False
    if len(df_low) < 5 or len(df_high) < 5: return False
    cl  = float(df_low["Close"].iloc[-2])
    body = df_high.iloc[-2]
    top = max(float(body["Open"]), float(body["Close"]))
    bot = min(float(body["Open"]), float(body["Close"]))
    return bot <= cl <= top

def _bars_for_hours(tf: str, hours: int) -> int:
    if tf == "5m":  return hours * 12
    if tf == "15m": return hours * 4
    if tf == "60m": return hours * 1
    return hours * 12

def _dedup_level_list(levels: list, tol: float) -> list:
    out = []
    for L in sorted(levels, key=lambda x: x["price"]):
        if not out: 
            out.append(L); continue
        if abs(L["price"] - out[-1]["price"]) <= tol:
            # усиливаем «сильный» уровень — оставляем с бОльшим весом
            if L.get("strength",1) > out[-1].get("strength",1):
                out[-1] = L
        else:
            out.append(L)
    return out

def extract_levels(df: pd.DataFrame, tf_label: str, lookback_hours: int, now_ts: float, kind: str) -> list:
    """kind in {'HH','LL'} — берём локальные экстремумы в заданном окне."""
    if df is None or df.empty: return []
    bars = _bars_for_hours(tf_label, lookback_hours)
    d = df.tail(max(bars, 30)).copy()
    out = []
    n = len(d)
    if n < 10: return out
    rng = (d["High"].max() - d["Low"].min()) or 0.0
    if rng <= 0: return out

    # локальные swing по окну k
    k = 3
    for i in range(k, n-k):
        hi = float(d["High"].iloc[i]); lo = float(d["Low"].iloc[i])
        if kind == "HH":
            if hi == max(d["High"].iloc[i-k:i+k+1]):
                out.append({"price": hi, "tf": tf_label, "ts": now_ts, "kind": "HH", "strength": 1})
        else:
            if lo == min(d["Low"].iloc[i-k:i+k+1]):
                out.append({"price": lo, "tf": tf_label, "ts": now_ts, "kind": "LL", "strength": 1})
    return out

def build_level_memory(symbol: str, df1m: pd.DataFrame):
    """Обновляем память уровней: HH/LL с 5m/15m/60m, дедуп, чистка по времени."""
    if df1m is None or df1m.empty: return
    now_ts = time.time()

    df5   = _resample(df1m, 5)
    df15  = _resample(df1m, 15)
    df60  = _resample(df1m, 60)

    mem = state["levels"].get(symbol, [])
    # удаляем старое
    mem = [L for L in mem if now_ts - L.get("ts", now_ts) <= LEVEL_EXPIRE_SEC]

    # наращиваем из свежих данных
    for tf, d, hours in (("5m", df5, LEVEL_MEMORY_HOURS["5m"]),
                         ("15m", df15, LEVEL_MEMORY_HOURS["15m"]),
                         ("60m", df60, LEVEL_MEMORY_HOURS["60m"])):
        mem += extract_levels(d, tf, hours, now_ts, "HH")
        mem += extract_levels(d, tf, hours, now_ts, "LL")

    # дедуп по близости цены
    tol = LEVEL_DEDUP_TOL.get(symbol, 0.003)
    mem = _dedup_level_list(mem, tol)
    state["levels"][symbol] = mem

def nearest_level_from_memory(symbol: str, side: str, price: float) -> float | None:
    """Берём ближайший уровень из памяти в сторону TP."""
    mem = state["levels"].get(symbol, []) or []
    if not mem: return None
    above = [L["price"] for L in mem if L["price"] > price]
    below = [L["price"] for L in mem if L["price"] < price]
    if side == "BUY":
        return min(above) if above else None
    else:
        return max(below) if below else None

def dxy_bias_from_df(dxy_1m: pd.DataFrame) -> str|None:
    if dxy_1m is None or dxy_1m.empty: return None
    df60 = _resample(dxy_1m, 60)
    df240 = _resample(dxy_1m, 240)
    if df60.empty or df240.empty: return None
    c1 = float(df60["Close"].iloc[-2])
    hh4 = _swing_high(df240, 20); ll4=_swing_low(df240,20)
    if c1 > hh4: return "UP"
    if c1 < ll4: return "DOWN"
    hh1 = _swing_high(df60, 20); ll1=_swing_low(df60,20)
    if c1 > hh1: return "UP"
    if c1 < ll1: return "DOWN"
    return None

def dynamic_buffer(symbol: str) -> float:
    return SPREAD_BUFFER.get(symbol, 0.0)

def format_signal(setup, buffer):
    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]
    rr = max(setup.get('rr',0.0), 0.0)
    return (
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}\n"
        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
        f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}  "
        f"RR≈{round(rr,2)}  Conf: {int(setup['conf']*100)}%  Bias: {setup['trend']}"
    )

# ===================== BUILD SETUP (MEMORY-LEVEL TP/SL) =====================
def build_setup(df1m: pd.DataFrame, symbol: str, tf_label: str, dxy_bias: str | None = None):
    if df1m is None or df1m.empty or len(df1m) < 240:
        return None

    # Обновим память уровней для символа (на фоне 1m)
    build_level_memory(symbol, df1m)

    # MTF
    df5   = _resample(df1m, 5)
    df15  = _resample(df1m, 15)
    df60  = _resample(df1m, 60)
    df240 = _resample(df1m, 240)
    if df5.empty or df15.empty or df60.empty or df240.empty: return None

    # bias по H1/H4
    c1 = float(df60["Close"].iloc[-2])
    hh4 = _swing_high(df240, 20); ll4=_swing_low(df240,20)
    if   c1 > hh4:  bias = "UP"
    elif c1 < ll4:  bias = "DOWN"
    else:
        hh1 = _swing_high(df60, 20); ll1=_swing_low(df60,20)
        bias = "UP" if c1 > hh1 else ("DOWN" if c1 < ll1 else "UP")

    # локальные факторы
    fvg_ok, fvg_dir, fvg_top, fvg_bot, fvg_w = fvg_last_soft(df15, lookback=24, use_bodies=True)
    choch_up   = choch_soft(df5, "UP",   8, False)
    choch_down = choch_soft(df5, "DOWN", 8, False)
    sweep15, sweep_dir15 = had_liquidity_sweep(df15, lookback=20)
    cons_break = is_consolidation_break(df5)

    # сторона
    side = "BUY" if bias=="UP" else "SELL"
    if sweep15:
        if sweep_dir15=="UP": side="BUY"
        if sweep_dir15=="DOWN": side="SELL"

    entry = float(df5["Close"].iloc[-2])
    buf   = dynamic_buffer(symbol)

    # SL: за ближайший структурный уровень (15m swing) + буфер спреда
    lo15  = _swing_low(df15, 20)
    hi15  = _swing_high(df15, 20)
    if side == "BUY":
        sl = min(entry, lo15 - buf)
    else:
        sl = max(entry, hi15 + buf)

    # ===== TP: ближайший уровень из памяти уровней =====
    mem_target = nearest_level_from_memory(symbol, side, entry)
    if mem_target is None:
        # fallback: ближайший уровень на 5m/15m
        def nearest_level_above(df: pd.DataFrame, price: float, lookback_bars: int) -> float | None:
            highs = df["High"].tail(lookback_bars)
            c = highs[highs > price]
            return float(c.min()) if not c.empty else None
        def nearest_level_below(df: pd.DataFrame, price: float, lookback_bars: int) -> float | None:
            lows = df["Low"].tail(lookback_bars)
            c = lows[lows < price]
            return float(c.max()) if not c.empty else None

        lb5  = _bars_for_hours("5m", 72)
        lb15 = _bars_for_hours("15m", 72)

        if side == "BUY":
            lvl5  = nearest_level_above(df5,  entry,  lb5)
            lvl15 = nearest_level_above(df15, entry,  lb15)
            if lvl5 is not None and lvl15 is not None:
                mem_target = min(lvl5, lvl15)
            else:
                mem_target = lvl5 if lvl5 is not None else lvl15
        else:
            lvl5  = nearest_level_below(df5,  entry,  lb5)
            lvl15 = nearest_level_below(df15, entry,  lb15)
            if lvl5 is not None and lvl15 is not None:
                mem_target = max(lvl5, lvl15)
            else:
                mem_target = lvl5 if lvl5 is not None else lvl15

    if side == "BUY":
        if mem_target is None or mem_target <= entry:
            # fallback — половина расстояния до hi15, но не ниже минимального TP
            target = max(entry + max(entry - sl, 1e-9)*0.8, entry + TP_MIN_ABS.get(symbol,0.0))
        else:
            target = mem_target
        tp = target + buf  # компенсация спреда
    else:
        if mem_target is None or mem_target >= entry:
            target = min(entry - max(sl - entry, 1e-9)*0.8, entry - TP_MIN_ABS.get(symbol,0.0))
        else:
            target = mem_target
        tp = target - buf

    tp_abs = abs(tp - entry)
    tp_min = TP_MIN_ABS.get(symbol, 0.0)
    if tp_abs < tp_min:
        return None

    rr = abs(tp - entry) / max(abs(entry - sl), 1e-9)

    # ===== Уверенность (Conf) =====
    score = 0
    if fvg_ok or cons_break: score += 20
    if sweep15:              score += 20
    if (side=="BUY" and choch_up) or (side=="SELL" and choch_down): score += 15
    if inside_higher_ob(df5, df60) or inside_higher_ob(df5, df240): score += 10
    if _in_session_utc(): score += 5
    if rr <= 1.0: score += 10

    # бонус за «совпадение»: если TP близко к swing 15m/60m — уровень сильнее
    if side == "BUY":
        if abs(tp - hi15) <= 2*buf: score += 10
    else:
        if abs(tp - lo15) <= 2*buf: score += 10

    # бонус за «плотность памяти» вокруг TP — много уровней в коридоре => цель реальнее
    mem = state["levels"].get(symbol, [])
    if mem:
        near = [L for L in mem if abs(L["price"] - tp) <= (3*LEVEL_DEDUP_TOL.get(symbol,0.003))]
        score += min(15, 3 * len(near))

    # золото и DXY противофаза
    if symbol == "XAU" and dxy_bias:
        if side == "BUY"  and dxy_bias == "DOWN": score += 10
        if side == "SELL" and dxy_bias == "UP":   score += 10

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

# ===================== EXECUTION / LOGGING =====================
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
    cooldown_until[symbol] = time.time() + COOLDOWN_SEC
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
    if IDEA_COOLDOWN_SEC > 0 and (now - _last_idea_ts.get(sym, 0.0) < IDEA_COOLDOWN_SEC):
        return False
    _reset_hour_if_needed(sym)
    if _ideas_count_hour.get(sym, 0) >= MAX_IDEAS_PER_HOUR:
        return False
    return True

def is_fresh_enough(symbol: str, entry: float, close_now: float) -> bool:
    # антистарьё
    buf = SPREAD_BUFFER.get(symbol, 0.0)
    lim = 15.0 * buf
    return abs(float(entry) - float(close_now)) <= lim

def is_duplicate_signal(symbol: str, entry: float) -> bool:
    """Антидубли: не спамим сигналами «+пара пунктов от того же места»."""
    lastp = _last_signal_price.get(symbol)
    if lastp is None: return False
    tol = 8.0 * SPREAD_BUFFER.get(symbol, 0.0)
    return abs(float(entry) - float(lastp)) <= tol

async def handle_symbol(session: aiohttp.ClientSession, symbol: str, dxy_df: pd.DataFrame | None):
    global last_seen_idx, last_signal_idx, _last_signal_price

    # AUTO: NG+XAU; ручной — только выбранный
    if mode != "AUTO" and symbol not in (mode,):
        return

    df = await get_df(session, symbol)
    if df.empty or len(df) < 240:
        return

    cur_idx = len(df) - 1
    closed_idx = cur_idx - 1
    if closed_idx <= last_seen_idx[symbol]:
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
                asyncio.create_task(notify_outcome(symbol, "TP", price_now))
                finish_trade(symbol, "TP", price_now); return
            if hit_sl:
                price_now = float(post["Close"].iloc[-1])
                asyncio.create_task(notify_outcome(symbol, "SL", price_now))
                finish_trade(symbol, "SL", price_now); return
        return

    # глобальные кулдауны
    if time.time() - boot_ts < BOOT_COOLDOWN_S: return
    if time.time() < cooldown_until[symbol]:   return

    # DXY только для XAU
    dxy_bias = dxy_bias_from_df(dxy_df) if symbol=="XAU" and dxy_df is not None and not dxy_df.empty else None

    setup = build_setup(df, symbol, SYMBOLS[symbol]["tf"], dxy_bias=dxy_bias)
    if not setup:
        return

    if last_signal_idx[symbol] == closed_idx:
        return

    buffer    = dynamic_buffer(symbol)
    conf_thr  = CONF_MIN_TRADE.get(symbol, 0.55)
    conf      = float(setup["conf"])
    close_now = float(df["Close"].iloc[-1])
    entry     = float(setup["entry"])

    if not is_fresh_enough(symbol, entry, close_now):
        return
    if is_duplicate_signal(symbol, entry):
        return

    # IDEA
    if conf >= CONF_MIN_IDEA and can_send_idea(symbol):
        await send_main("🧠 IDEA:\n" + format_signal(setup, buffer))
        _last_idea_ts[symbol] = time.time()
        _ideas_count_hour[symbol] = _ideas_count_hour.get(symbol, 0) + 1
        if _ideas_count_hour_ts.get(symbol, 0.0) == 0.0:
            _ideas_count_hour_ts[symbol] = time.time()

    # TRADE
    if conf >= conf_thr and (setup["tp_abs"] >= setup["tp_min"]):
        await send_main(format_signal(setup, buffer))
        trade[symbol] = {
            "side": setup["side"],
            "entry": float(setup["entry"]),
            "tp": float(setup["tp"]),
            "sl": float(setup["sl"]),
            "opened_at": time.time(),
            "entry_bar_idx": cur_idx,
        }
        last_signal_idx[symbol] = closed_idx
        _last_signal_price[symbol] = entry

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        dxy_df = None; dxy_ts = 0.0
        while True:
            try:
                if time.time() - dxy_ts > 25:
                    dxy_df = await get_dxy_df(session); dxy_ts = time.time()
                symbols_to_run = ("NG","XAU") if mode == "AUTO" else (mode,)
                for s in symbols_to_run:
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

            # уровень памяти (для диагностики): сколько уровней держим по каждому символу
            Lng = len(state["levels"]["NG"])
            Lxa = len(state["levels"]["XAU"])
            Lbt = len(state["levels"]["BTC"])

            msg = (f"[ALIVE] NG: {rnd('NG',c_ng)}, ATR15: {rnd('NG',a_ng)} (mem:{Lng}) | "
                   f"XAU: {rnd('XAU',c_xau)}, ATR15: {rnd('XAU',a_xau)} (mem:{Lxa}) | "
                   f"BTC: {rnd('BTC',c_btc)}, ATR15: {rnd('BTC',a_btc)} (mem:{Lbt}). Status: OK.")
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
