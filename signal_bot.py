#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ========= GranVex — V8.1 NG =========
# Trend + Human + Assist (NG-only, dynamic TP/SL, Yahoo 1m)
# Анти-пила, мягкие пороги, автосид уровней, дедуп по цене расширен

import os, time, csv, logging, asyncio, random
from datetime import datetime
import pandas as pd
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

VERSION = "V8.1 NG — Trend+Human+Assist (antichop, seeded levels)"

# ===================== TOKENS / OWNER =====================
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# ===================== MARKETS / SETTINGS =====================
SYMBOLS = {"NG": {"name": "NATGAS (NG=F)", "tf": "1m"}}
SPREAD_BUFFER = {"NG": 0.0040}

TP_MIN_ABS = {"NG": 0.0120}       # нижняя граница для HUMAN

CONF_MIN_TRADE = {"NG": 0.45}
CONF_MIN_IDEA  = 0.0120

SEND_IDEAS         = True
IDEA_COOLDOWN_SEC  = 0
MAX_IDEAS_PER_HOUR = 60

# Сессии (UTC)
LONDON_HOURS = range(7, 15)
NY_HOURS     = range(12, 21)

# Частоты
POLL_SEC        = 0.25
ALIVE_EVERY_SEC = 300
BOOT_COOLDOWN_S = 10
COOLDOWN_SEC    = 0

TRADES_CSV = "gv_trades.csv"

HTTP_TIMEOUT   = 12
YAHOO_RETRIES  = 4
YAHOO_BACKOFF0 = 0.9
YAHOO_JITTER   = 0.35

ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
    "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9", "Connection": "keep-alive",
}

# ====== ASSIST (мягкий фоллбек по NG 1m) ======
SCALP_ASSIST_ENABLED = True
SCALP_ATR1_MIN     = 0.0040
SCALP_MIN_IMPULSE  = 0.0020
SCALP_MIN_BODY     = 0.0010
SCALP_NEAR_BREAK   = 0.0015
SCALP_TP_ABS       = 0.0200
SCALP_SL_ABS       = 0.0120
SCALP_COOLDOWN_SEC = 1
SCALP_MAX_PER_HOUR = 20

# ====== TREND (включён по умолчанию, пороги смягчены) ======
TREND_ENABLED        = True
TREND_ATR1_MIN       = 0.0
TREND_LOOK_MIN       = 8            # было 10
TREND_MIN_MOVE       = 0.0080       # было 0.0100
TREND_MIN_GAP_MIN    = 1            # пауза между тренд-входами
TREND_MAX_PER_DAY    = 8            # было 5
TREND_TP_ABS         = 0.0200
TREND_SL_ABS         = 0.0120

# ===================== STATE =====================
boot_ts = time.time()

trade = {"NG": None}
cooldown_until = {"NG": 0.0}
last_candle_close_ts = {"NG": 0.0}

_last_idea_ts = {"NG": 0.0}
_ideas_count_hour = {"NG": 0}
_ideas_count_hour_ts = {"NG": 0.0}

last_seen_idx   = {"NG": -1}
last_signal_idx = {"NG": -1}
_last_signal_price = {"NG": None}

_prices_cache = {}
state = {
    "levels": {"NG": []},
    "atr_NG": 0.0,
    "trend_day": {"date": None, "count": 0, "last_ts": 0.0},
}
mode = "NG"
requested_mode = "NG"

LEVEL_MEMORY_HOURS = {"5m": 72, "15m": 72, "60m": 120}
LEVEL_DEDUP_TOL    = {"NG": 0.003}
LEVEL_EXPIRE_SEC   = 48 * 3600

scalp_cooldown_until = 0.0
scalp_trades_hour_ts = 0.0
scalp_trades_hour_ct = 0

# ===================== TELEGRAM =====================
router = Router()
bot_main = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
bot_log  = Bot(LOG_BOT_TOKEN,  default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

async def send_main(text: str):
    try: await bot_main.send_message(TARGET_CHAT_ID, text)
    except Exception as e: logging.error(f"send_main error: {e}")

async def send_log(text: str):
    try: await bot_log.send_message(TARGET_CHAT_ID, text)
    except Exception as e: logging.error(f"send_log error: {e}")

def mode_title(m: str) -> str: return "NATGAS (NG=F)"

async def _request_mode(new_mode: str, m: Message | None = None):
    global requested_mode, mode
    requested_mode = new_mode; mode = new_mode
    if m: await m.answer(f"✅ Режим: {mode_title(new_mode)}.")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot is alive ({VERSION}).\nНапиши 'команды' чтобы увидеть список.")
    await m.answer(f"✅ Текущий режим: {mode_title(mode)}.")

@router.message(F.text.lower() == "команды")
async def cmd_help(m: Message):
    await m.answer(
        "📋 Команды:\n"
        "• /start — запуск\n"
        "• газ — следить только за NG\n"
        "• скальп вкл / скальп выкл — ассист-фоллбек\n"
        "• тренд вкл / тренд выкл — слой тренда\n"
        "• стоп — стоп и короткий кулдаун\n"
        "• статус — диагностика\n"
        "• отчет — 10 последних закрытий (только владелец)\n"
        "• тест — тестовый сигнал"
    )

@router.message(F.text.lower() == "газ")
async def set_ng(m: Message):  await _request_mode("NG", m)

@router.message(F.text.lower() == "скальп вкл")
async def scalp_on(m: Message):
    global SCALP_ASSIST_ENABLED
    SCALP_ASSIST_ENABLED = True
    await m.answer("✅ Скальп-ассист включён (NG, TP 0.020 / SL 0.012, ATR1≥0.004).")

@router.message(F.text.lower() == "скальп выкл")
async def scalp_off(m: Message):
    global SCALP_ASSIST_ENABLED
    SCALP_ASSIST_ENABLED = False
    await m.answer("⛔ Скальп-ассист выключен.")

@router.message(F.text.lower() == "тренд вкл")
async def trend_on(m: Message):
    global TREND_ENABLED
    TREND_ENABLED = True
    await m.answer("✅ Слой TREND включён.")

@router.message(F.text.lower() == "тренд выкл")
async def trend_off(m: Message):
    global TREND_ENABLED
    TREND_ENABLED = False
    await m.answer("⛔ Слой TREND выключен.")

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    now = time.time()
    trade["NG"] = None
    cooldown_until["NG"] = now + 3
    global scalp_cooldown_until
    scalp_cooldown_until = now + 10
    await m.answer("🛑 Остановил. Открытых нет, короткий кулдаун.")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    now = time.time()
    lines = [f"mode: NG (requested: {requested_mode})",
             f"alive: OK | poll={POLL_SEC}s"]
    s = "NG"
    opened = bool(trade[s])
    age = int(now - last_candle_close_ts[s]) if last_candle_close_ts[s] else -1
    atrtxt = state.get("atr_NG", "—")
    nm = SYMBOLS[s]["name"]
    cd = max(0, int(cooldown_until[s]-now))
    L = len(state["levels"][s]) if isinstance(state["levels"].get(s), list) else 0
    sample = [round(x["price"],3) for x in (state["levels"][s][-4:] if L else [])]
    lines.append(f"{nm}: ATR15≈{atrtxt}  open={opened}  cooldown={cd}  last_close_age={age}s  levels_mem={L}")
    lines.append(f"levels_sample: {sample if sample else '[]'}")
    lines.append(f"Trend: {'ON' if TREND_ENABLED else 'OFF'}  Assist: {'ON' if SCALP_ASSIST_ENABLED else 'OFF'}")
    scd = max(0, int(scalp_cooldown_until - now))
    lines.append(f"Assist stats: cooldown={scd}s  per_hour={scalp_trades_hour_ct}")
    await m.answer("```\n"+ "\n".join(lines) + "\n```")

@router.message(F.text.lower() == "отчет")
async def cmd_report(m: Message):
    if m.from_user.id != OWNER_ID:
        return await m.answer("Доступно только владельцу.")
    if not os.path.exists(TRADES_CSV):
        return await m.answer("Пока нет закрытых сделок.")
    rows = list(csv.DictReader(open(TRADES_CSV,encoding="utf-8")))[-10:]
    if not rows: return await m.answer("Пусто.")
    txt = "Последние 10 закрытий:\n"
    for r in rows:
        txt += (f"{r['ts_close']}  {r['symbol']}  {r['side']}  {r['outcome']}  "
                f"entry:{r['entry']} tp:{r['tp']} sl:{r['sl']} rr:{r['rr_ratio']}\n")
    await m.answer("```\n"+txt+"```")

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    text = ("🔥 BUY NATGAS (NG=F) | 1m\n"
            "✅ TP: **2.9999**\n"
            "🟥 SL: **2.9700**\n"
            "Entry: 2.9699  Spread≈0.0040  Conf: 60%  Bias: UP")
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
        if not ts or not q: return pd.DataFrame()
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

async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
    now_ts = time.time()
    c = _prices_cache.get(symbol)
    cache_ttl = 0.35
    if c and (now_ts - c["ts"] < cache_ttl) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]
    if symbol == "NG":
        for t in ("NG=F",):
            df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d"))
            if not df.empty:
                last_candle_close_ts["NG"] = time.time()
                _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                return df
        return pd.DataFrame()
    return pd.DataFrame()

# ===================== UTILS / SMC =====================
def rnd(sym: str, x: float) -> float:
    return round(float(x), 4)

def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    end = pd.Timestamp.utcnow().floor("min")
    idx = pd.date_range(end - pd.Timedelta(minutes=len(df)-1), periods=len(df), freq="1min")
    z = df.copy(); z.index = idx
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

def _atr_m1(df1m: pd.DataFrame, period: int = 14) -> float:
    d = df1m
    if d is None or d.empty or len(d) < period+2: return 0.0
    highs = d["High"].values; lows = d["Low"].values; closes = d["Close"].values
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)
    return float(sum(trs[-period:]) / period) if len(trs) >= period else 0.0

def fvg_last_soft(df: pd.DataFrame, lookback: int = 20, use_bodies: bool = True):
    n = len(df)
    if n < 4: return False, "", 0.0, 0.0, 0.0
    for i in range(n-2, max(1, n - lookback) - 1, -1):
        if use_bodies:
            h2 = max(float(df["Open"].iloc[i-2]), float(df["Close"].iloc[i-2]))
            l2 = min(float(df["Open"].iloc[i-2]), float(df["Close"].iloc[i-2]))
            h0 = max(float(df["Open"].iloc[i]),   float(df["Close"].iloc[i]))
            l0 = min(float(df["Open"].iloc[i]),   float(df["Close"].iloc[i]))
        else:
            h2 = float(df["High"].iloc[i-2]); l2 = float(df["Low"].iloc[i-2])
            h0 = float(df["High"].iloc[i]);   l0 = float(df["Low"].iloc[i])
        if l0 > h2:  return True, "BULL", l0, h2, abs(l0-h2)
        if h0 < l2:  return True, "BEAR", h2, l0, abs(h2-l0)
    return False, "", 0.0, 0.0, 0.0

def choch_soft(df: pd.DataFrame, want: str, swing_lookback: int = 8, confirm_break: bool = False):
    n = len(df)
    if n < swing_lookback + 3: return False
    i = n - 2
    local_high = float(df["High"].iloc[i - swing_lookback:i].max())
    local_low  = float(df["Low"].iloc[i - swing_lookback:i].min())
    c_prev = float(df["Close"].iloc[i-1]); c_now  = float(df["Close"].iloc[i])
    if want == "UP":   return (c_now > local_high) or (not confirm_break and c_prev > local_high)
    else:              return (c_now < local_low)  or (not confirm_break and c_prev < local_low)

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
        if not out: out.append(L); continue
        if abs(L["price"] - out[-1]["price"]) <= tol:
            if L.get("strength",1) > out[-1].get("strength",1):
                out[-1] = L
        else:
            out.append(L)
    return out

def extract_levels(df: pd.DataFrame, tf_label: str, lookback_hours: int, now_ts: float, kind: str) -> list:
    if df is None or df.empty: return []
    bars = _bars_for_hours(tf_label, lookback_hours)
    d = df.tail(max(bars, 30)).copy()
    out = []; n = len(d); 
    if n < 10: return out
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
    if df1m is None or df1m.empty: return
    now_ts = time.time()
    df5   = _resample(df1m, 5)
    df15  = _resample(df1m, 15)
    df60  = _resample(df1m, 60)

    mem = state["levels"].get(symbol, []) or []
    mem = [L for L in mem if now_ts - L.get("ts", now_ts) <= LEVEL_EXPIRE_SEC]

    for tf, d, hours in (("5m", df5, LEVEL_MEMORY_HOURS["5m"]),
                         ("15m", df15, LEVEL_MEMORY_HOURS["15m"]),
                         ("60m", df60, LEVEL_MEMORY_HOURS["60m"])):
        mem += extract_levels(d, tf, hours, now_ts, "HH")
        mem += extract_levels(d, tf, hours, now_ts, "LL")

    if len(mem) < 20 and (df1m is not None and not df1m.empty):
        d = df1m.tail(400); k = 3
        for i in range(k, len(d)-k):
            hi = float(d["High"].iloc[i]); lo = float(d["Low"].iloc[i])
            if hi == max(d["High"].iloc[i-k:i+k+1]):
                mem.append({"price": hi, "tf": "seed", "ts": now_ts, "kind": "HH", "strength": 1})
            if lo == min(d["Low"].iloc[i-k:i+k+1]):
                mem.append({"price": lo, "tf": "seed", "ts": now_ts, "kind": "LL", "strength": 1})

    tol = LEVEL_DEDUP_TOL.get(symbol, 0.003)
    mem = _dedup_level_list(mem, tol)
    state["levels"][symbol] = mem

def nearest_level_from_memory(symbol: str, side: str, price: float) -> float | None:
    mem = state["levels"].get(symbol, []) or []
    if not mem: return None
    above = [L["price"] for L in mem if L["price"] > price]
    below = [L["price"] for L in mem if L["price"] < price]
    if side == "BUY":  return min(above) if above else None
    else:              return max(below) if below else None

def dynamic_buffer(symbol: str) -> float: return SPREAD_BUFFER.get(symbol, 0.0)

def format_signal(setup, buffer):
    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]
    rr = max(setup.get('rr',0.0), 0.0)
    tag = setup.get("kind","")
    extra = f"  ({tag})" if tag else ""
    return (
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}{extra}\n"
        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
        f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}  "
        f"RR≈{round(rr,2)}  Conf: {int(setup['conf']*100)}%  Bias: {setup['trend']}"
    )

# ========== Лёгкий «разум» ==========
def _ema(arr, n):
    k = 2.0/(n+1.0); ema = None; out=[]
    for v in arr:
        ema = v if ema is None else (v*k + ema*(1-k))
        out.append(ema)
    return out

def _trend_strength(df1m):
    """Сила тренда по 5m: EMA5 vs EMA20 + импульс последних 8 баров."""
    d5 = _resample(df1m, 5)
    if d5.empty or len(d5) < 25: return 0.0
    c = d5["Close"].tolist()
    e5 = _ema(c, 5)[-1]; e20 = _ema(c, 20)[-1]
    slope = (c[-1]-c[-9])  # импульс за ~40 минут
    sign  = 1 if e5>e20 else (-1 if e5<e20 else 0)
    return sign * (abs(e5-e20) + slope)

def _chop_guard(df1m):
    """Антипила на 1m: 3 из 4 последних закрытий в одну сторону,
    и последний бар не с огромными тенями (body/(high-low) >= 0.4)."""
    d = df1m
    if d is None or d.empty or len(d) < 6: return False
    closes = d["Close"].values
    opens  = d["Open"].values
    highs  = d["High"].values
    lows   = d["Low"].values
    last = -1
    seq = [(closes[i]-closes[i-1]) for i in range(last-3, last+1)]
    up = sum(1 for x in seq if x>0)
    dn = sum(1 for x in seq if x<0)
    body = abs(closes[last]-opens[last]); rng = max(1e-9, highs[last]-lows[last])
    good_body = (body/rng) >= 0.4
    return (max(up,dn) >= 3) and good_body

def _rr_ok(entry, tp, sl):
    risk = abs(entry-sl); reward = abs(tp-entry)
    return (risk >= 0.004) and (reward/max(risk,1e-9) >= 1.2)

# ===================== HUMAN =====================
def build_setup(df1m: pd.DataFrame, symbol: str, tf_label: str, dxy_bias=None):
    if df1m is None or df1m.empty or len(df1m) < 240: return None
    build_level_memory(symbol, df1m)

    df5   = _resample(df1m, 5)
    df15  = _resample(df1m, 15)
    df60  = _resample(df1m, 60)
    df240 = _resample(df1m, 240)
    if df5.empty or df15.empty or df60.empty or df240.empty: return None

    c1 = float(df60["Close"].iloc[-2])
    hh4 = _swing_high(df240, 20); ll4=_swing_low(df240,20)
    if   c1 > hh4:  bias = "UP"
    elif c1 < ll4:  bias = "DOWN"
    else:
        hh1 = _swing_high(df60, 20); ll1=_swing_low(df60,20)
        bias = "UP" if c1 > hh1 else ("DOWN" if c1 < ll1 else "UP")

    fvg_ok, _, _, _, _ = fvg_last_soft(df15, lookback=24, use_bodies=True)
    choch_up   = choch_soft(df5, "UP",   8, False)
    choch_down = choch_soft(df5, "DOWN", 8, False)

    side = "BUY" if bias=="UP" else "SELL"
    entry = float(df5["Close"].iloc[-2])
    buf   = dynamic_buffer(symbol)

    lo15  = _swing_low(df15, 20)
    hi15  = _swing_high(df15, 20)
    if side == "BUY": sl = min(entry, lo15 - buf)
    else:             sl = max(entry, hi15 + buf)

    mem_target = nearest_level_from_memory(symbol, side, entry)
    if side == "BUY":
        if mem_target is None or mem_target <= entry:
            target = entry + max(abs(entry - sl)*0.8, TP_MIN_ABS.get(symbol,0.0))
        else:
            target = mem_target
        tp = target + buf
    else:
        if mem_target is None or mem_target >= entry:
            target = entry - max(abs(entry - sl)*0.8, TP_MIN_ABS.get(symbol,0.0))
        else:
            target = mem_target
        tp = target - buf

    tp_abs = abs(tp - entry)
    if tp_abs < TP_MIN_ABS.get(symbol, 0.0): return None

    rr = abs(tp - entry) / max(abs(entry - sl), 1e-9)
    score = 0
    if fvg_ok: score += 20
    if (side=="BUY" and choch_up) or (side=="SELL" and choch_down): score += 15
    if _in_session_utc(): score += 5
    score = max(0, min(100, score)); conf = score/100.0
    if conf < CONF_MIN_IDEA: return None

    if not _rr_ok(entry, tp, sl): return None

    return {"symbol": symbol, "tf": tf_label, "side": side, "trend": bias,
            "entry": entry, "tp": tp, "sl": sl, "rr": rr, "conf": conf,
            "tp_abs": tp_abs, "tp_min": TP_MIN_ABS.get(symbol,0.0), "kind":"HUMAN"}

# ===================== TREND =====================
def _trend_reset_if_new_day():
    d = datetime.utcnow().date().isoformat()
    if state["trend_day"]["date"] != d:
        state["trend_day"] = {"date": d, "count": 0, "last_ts": 0.0}

def build_trend_setup_ng(df1m: pd.DataFrame) -> dict | None:
    if not TREND_ENABLED: return None
    if df1m is None or df1m.empty or len(df1m) < TREND_LOOK_MIN + 30:
        return None

    # Антипила: последние бары согласованы и тело адекватное
    if not _chop_guard(df1m):
        return None

    # импульс за LOOK_MIN
    close_now  = float(df1m["Close"].iloc[-1])
    close_look = float(df1m["Close"].iloc[-(TREND_LOOK_MIN+1)])
    delta = close_now - close_look
    if abs(delta) < TREND_MIN_MOVE:
        return None
    side = "BUY" if delta > 0 else "SELL"

    # Сила тренда на 5m
    ts = _trend_strength(df1m)
    if (side == "BUY" and ts <= 0) or (side == "SELL" and ts >= 0):
        return None

    # анти-спам: лимит на день и пауза
    _trend_reset_if_new_day()
    td = state["trend_day"]
    if td["count"] >= TREND_MAX_PER_DAY: return None
    if time.time() - float(td.get("last_ts",0.0)) < TREND_MIN_GAP_MIN*60: return None

    df5 = _resample(df1m, 5)
    if df5 is None or df5.empty or len(df5) < 30: return None

    buf   = SPREAD_BUFFER.get("NG", 0.0)
    entry = close_now

    # SL по свингу
    if side == "BUY":
        swing_lo = _swing_low(df5, 20)
        sl = min(entry - 1e-6, swing_lo - buf)
        min_sl = entry - TREND_SL_ABS - buf
        if sl > entry - 0.004: sl = min_sl
    else:
        swing_hi = _swing_high(df5, 20)
        sl = max(entry + 1e-6, swing_hi + buf)
        min_sl = entry + TREND_SL_ABS + buf
        if sl < entry + 0.004: sl = min_sl

    risk = abs(entry - sl)
    if risk < 0.004 or risk > 0.025: return None

    # TP — ближайший уровень или RR≈1.6*risk
    build_level_memory("NG", df1m)
    mem_target = nearest_level_from_memory("NG", side, entry)

    rr_target = 1.6
    if side == "BUY":
        tp_rr = entry + rr_target * risk
        tp = max(tp_rr, entry + 0.012)
        if mem_target is not None and mem_target > entry:
            tp = max(tp, mem_target + buf)
        tp = min(tp, entry + 0.030)
    else:
        tp_rr = entry - rr_target * risk
        tp = min(tp_rr, entry - 0.012)
        if mem_target is not None and mem_target < entry:
            tp = min(tp, mem_target - buf)
        tp = max(tp, entry - 0.030)

    tp_abs = abs(tp - entry)
    if tp_abs < 0.010: return None

    if not _rr_ok(entry, tp, sl): return None

    rr = tp_abs / max(risk,1e-9)
    base = 0.62
    if _in_session_utc(): base += 0.04
    if abs(delta) >= TREND_MIN_MOVE*1.5: base += 0.03
    conf = min(0.9, base)

    setup = {"symbol":"NG","tf":"1m","side":side,"trend": "UP" if side=="BUY" else "DOWN",
             "entry": entry,"tp": tp,"sl": sl,"rr": rr,"conf": conf,
             "tp_abs": tp_abs,"tp_min": 0.010, "kind":"TREND"}
    state["trend_day"]["last_ts"] = time.time()
    return setup

# ===================== ASSIST =====================
def _reset_scalp_hour():
    global scalp_trades_hour_ts, scalp_trades_hour_ct
    now = time.time()
    if now - (scalp_trades_hour_ts or 0.0) >= 3600:
        scalp_trades_hour_ts = now
        scalp_trades_hour_ct = 0

def _ok_scalp_frequency() -> bool:
    _reset_scalp_hour()
    return scalp_trades_hour_ct < SCALP_MAX_PER_HOUR

def build_scalp_setup_ng(df1m: pd.DataFrame) -> dict | None:
    if df1m is None or df1m.empty or len(df1m) < 30: return None
    atr1 = _atr_m1(df1m, 14)
    if atr1 < SCALP_ATR1_MIN: return None

    i = len(df1m) - 1
    H = float(df1m["High"].iloc[i]); L = float(df1m["Low"].iloc[i])
    O = float(df1m["Open"].iloc[i]); C = float(df1m["Close"].iloc[i])
    rng  = H - L; body = abs(C - O)
    if (rng < SCALP_MIN_IMPULSE) or (body < SCALP_MIN_BODY): return None

    cur = float(df1m["Close"].iloc[-1]); buf = SPREAD_BUFFER.get("NG", 0.0)
    near_up   = (H - cur) <= SCALP_NEAR_BREAK
    near_down = (cur - L) <= SCALP_NEAR_BREAK

    side = None
    if near_up and C >= O: side = "BUY"
    elif near_down and C <= O: side = "SELL"
    else: return None

    entry = float(df1m["Close"].iloc[-1])
    if side == "BUY":
        tp = entry + SCALP_TP_ABS + buf
        sl = entry - SCALP_SL_ABS - buf
        sl = min(entry - 1e-6, sl)
    else:
        tp = entry - SCALP_TP_ABS - buf
        sl = entry + SCALP_SL_ABS + buf
        sl = max(entry + 1e-6, sl)

    rr = abs(tp - entry) / max(abs(entry - sl), 1e-9)
    conf = 0.58 + (0.05 if _in_session_utc() else 0.0)

    return {"symbol":"NG","tf":"1m","side":side,"trend":"UP" if side=='BUY' else 'DOWN',
            "entry":entry,"tp":tp,"sl":sl,"rr":rr,"conf":conf,
            "tp_abs":abs(tp-entry),"tp_min":SCALP_TP_ABS,"kind":"ASSIST"}

# ===================== LOGGING / OUTCOMES =====================
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
    sess = trade[symbol]; trade[symbol] = None
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

async def handle_symbol(session: aiohttp.ClientSession, symbol: str):
    global last_seen_idx, last_signal_idx, _last_signal_price
    global scalp_cooldown_until, scalp_trades_hour_ct

    if symbol != "NG": return
    df = await get_df(session, symbol)
    if df.empty or len(df) < 240: return

    # гарантированно обновим уровни, чтобы HUMAN/TREND не молчали в прогреве
    build_level_memory("NG", df)

    cur_idx = len(df) - 1
    closed_idx = cur_idx - 1
    if closed_idx <= last_seen_idx[symbol]: return
    last_seen_idx[symbol] = closed_idx

    # есть открытая — проверяем TP/SL
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

    if time.time() - boot_ts < BOOT_COOLDOWN_S: return
    if time.time() < cooldown_until[symbol]:   return

    setup = None

    # 0) TREND
    setup = build_trend_setup_ng(df)
    # 1) HUMAN (если тренд не дал)
    if setup is None:
        setup = build_setup(df, "NG", SYMBOLS["NG"]["tf"], dxy_bias=None)
    # 2) ASSIST (если и HUMAN не дал)
    if setup is None and SCALP_ASSIST_ENABLED and _ok_scalp_frequency() and time.time() >= scalp_cooldown_until:
        setup = build_scalp_setup_ng(df)

    if not setup: return
    if last_signal_idx[symbol] == closed_idx: return

    buffer    = SPREAD_BUFFER.get(symbol, 0.0)
    conf_thr  = CONF_MIN_TRADE.get(symbol, 0.55)
    conf      = float(setup["conf"])
    close_now = float(df["Close"].iloc[-1])
    entry     = float(setup["entry"])

    # расширили допуск по близости цены и дедуп по цене
    if abs(entry - close_now) > 15.0 * buffer: return
    if _last_signal_price[symbol] is not None and abs(entry - _last_signal_price[symbol]) <= 12.0 * buffer:
        return

    if conf >= CONF_MIN_IDEA and SEND_IDEAS and can_send_idea(symbol):
        now = time.time()
        await send_main("🧠 IDEA:\n" + format_signal(setup, buffer))
        _last_idea_ts[symbol] = now

    if conf >= conf_thr and (setup["tp_abs"] >= setup["tp_min"]):
        await send_main(format_signal(setup, buffer))
        trade[symbol] = {
            "side": setup["side"], "entry": float(setup["entry"]),
            "tp": float(setup["tp"]), "sl": float(setup["sl"]),
            "opened_at": time.time(), "entry_bar_idx": cur_idx,
        }
        last_signal_idx[symbol] = closed_idx
        _last_signal_price[symbol] = entry

        if setup.get("kind") == "TREND":
            _trend_reset_if_new_day()
            state["trend_day"]["count"] += 1
            state["trend_day"]["last_ts"] = time.time()

        scalp_trades_hour_ct += 1
        scalp_cooldown_until = time.time() + SCALP_COOLDOWN_SEC
        return

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await handle_symbol(session, "NG")
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
            # сидируем уровни сразу, чтобы levels_mem не был 0 на старте
            if not df_ng.empty:
                build_level_memory("NG", df_ng)

            c_ng  = float(df_ng["Close"].iloc[-1])  if not df_ng.empty else 0.0
            a_ng  = _atr_m15(df_ng)  if not df_ng.empty else 0.0
            state["atr_NG"]  = rnd("NG", a_ng)
            Lng = len(state["levels"]["NG"])
            sample = [round(x["price"],3) for x in (state["levels"]["NG"][-4:] if Lng else [])]
            msg = (f"[ALIVE] NG: {rnd('NG',c_ng)}, ATR15: {rnd('NG',a_ng)} (mem:{Lng}). "
                   f"levels_sample: {sample if sample else '[]'}. Status: OK.")
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



