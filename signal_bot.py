#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ========= GranVex — V9.0 GOLD =========
# XAUUSD-only, Yahoo 1m feed, simple breakout + levels.
# No ATR/RR gating. Sends Telegram signals only.

import os, time, csv, logging, asyncio, random
from datetime import datetime
import pandas as pd
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

VERSION = "V9.0 GOLD — breakout+levels, min TP 4p"

# ===================== TOKENS / OWNER =====================

MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# ===================== MARKETS / SETTINGS =====================

SYMBOLS = {"XAU": {"name": "GOLD (XAUUSD)", "tf": "1m"}}

SPREAD_BUFFER = {"XAU": 0.05}   # tune to your broker
TP_MIN_ABS    = {"XAU": 4.0}    # your requirement: minimum 4 points

CONF_MIN_TRADE = {"XAU": 0.50}
CONF_MIN_IDEA  = 0.00

SEND_IDEAS         = True
IDEA_COOLDOWN_SEC  = 0
MAX_IDEAS_PER_HOUR = 120

LONDON_HOURS = range(7, 15)   # UTC sessions boost
NY_HOURS     = range(12, 21)

POLL_SEC        = 0.35
ALIVE_EVERY_SEC = 300
BOOT_COOLDOWN_S = 8
COOLDOWN_SEC    = 0

TRADES_CSV = "gv_trades_gold.csv"

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

trade = {"XAU": None}
cooldown_until = {"XAU": 0.0}
last_candle_close_ts = {"XAU": 0.0}

_last_idea_ts = {"XAU": 0.0}
_ideas_count_hour = {"XAU": 0}
_ideas_count_hour_ts = {"XAU": 0.0}

last_seen_idx   = {"XAU": -1}
last_signal_idx = {"XAU": -1}
_last_signal_price = {"XAU": None}

_prices_cache = {}
state = {
    "levels": {"XAU": []},
}
mode = "XAU"
requested_mode = "XAU"

LEVEL_MEMORY_HOURS = {"5m": 72, "15m": 72, "60m": 120}
LEVEL_DEDUP_TOL    = {"XAU": 0.30}
LEVEL_EXPIRE_SEC   = 48 * 3600

# Signal frequency knobs
BREAK_LOOKBACK_N = 15
RETEST_ALLOW     = True
RETEST_TOL       = 0.25
MAX_TP_CAP       = 50.0
MIN_SL_ABS       = 3.0
MAX_RISK_ABS     = 30.0
ENTRY_PROX_MULT  = 10.0
DEDUP_PROX_MULT  = 8.0

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

def mode_title(_: str) -> str:
    return "GOLD (XAUUSD)"

async def _request_mode(new_mode: str, m: Message | None = None):
    global requested_mode, mode
    requested_mode = new_mode
    mode = new_mode
    if m:
        await m.answer(f"✅ Mode: {mode_title(new_mode)}.")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot is alive ({VERSION}).\nType 'команды' to see commands.")
    await m.answer(f"✅ Current mode: {mode_title(mode)}.")

@router.message(F.text.lower() == "команды")
async def cmd_help(m: Message):
    await m.answer(
        "📋 Команды:\n"
        "• /start — запуск\n"
        "• золото — режим XAUUSD\n"
        "• стоп — стоп и короткий кулдаун\n"
        "• статус — диагностика\n"
        "• отчет — 10 последних закрытий (только владелец)\n"
        "• тест — тестовый сигнал"
    )

@router.message(F.text.lower() == "золото")
async def set_xau(m: Message):
    await _request_mode("XAU", m)

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    now = time.time()
    trade["XAU"] = None
    cooldown_until["XAU"] = now + 3
    await m.answer("🛑 Stopped. No open position. Short cooldown set.")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    now = time.time()
    s = "XAU"
    opened = bool(trade[s])
    age = int(now - last_candle_close_ts[s]) if last_candle_close_ts[s] else -1
    nm = SYMBOLS[s]["name"]
    cd = max(0, int(cooldown_until[s]-now))
    L = len(state["levels"][s]) if isinstance(state["levels"].get(s), list) else 0
    sample = [round(x["price"],2) for x in (state["levels"][s][-4:] if L else [])]
    lines = [
        f"mode: XAU (requested: {requested_mode})",
        f"{nm}: open={opened} cooldown={cd}s last_close_age={age}s levels_mem={L}",
        f"levels_sample: {sample if sample else '[]'}",
        f"breakout N={BREAK_LOOKBACK_N} retest={RETEST_ALLOW} tp_min={TP_MIN_ABS['XAU']} cap={MAX_TP_CAP}"
    ]
    await m.answer("`\n"+ "\n".join(lines) + "\n`")

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
                f"entry:{r['entry']} tp:{r['tp']} sl:{r['sl']}\n")
    await m.answer("`\n"+txt+"`")

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    text = ("🔥 BUY GOLD (XAUUSD) | 1m\n"
            "✅ TP: **2392.00**\n"
            "🟥 SL: **2384.00**\n"
            "Entry: 2388.00  Spread≈0.05")
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

async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
    now_ts = time.time()
    c = _prices_cache.get(symbol)
    cache_ttl = 0.40
    if c and (now_ts - c["ts"] < cache_ttl) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]
    if symbol == "XAU":
        for t in ("XAUUSD=X",):
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d"
            df = _df_from_yahoo_v8(await _yahoo_json(session, url))
            if not df.empty:
                last_candle_close_ts["XAU"] = time.time()
                _prices_cache["XAU"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                return df
    return pd.DataFrame()

# ===================== UTILS / LEVELS =====================

DECIMALS = {"XAU": 2}
def rnd(sym: str, x: float) -> float:
    return round(float(x), DECIMALS.get(sym, 4))

def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    end = pd.Timestamp.utcnow().floor("min")
    idx = pd.date_range(end - pd.Timedelta(minutes=len(df)-1), periods=len(df), freq="1min")
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

def _bars_for_hours(tf: str, hours: int) -> int:
    if tf == "5m":
        return hours * 12
    if tf == "15m":
        return hours * 4
    if tf == "60m":
        return hours * 1
    return hours * 12

def _dedup_level_list(levels: list, tol: float) -> list:
    out = []
    for L in sorted(levels, key=lambda x: x["price"]):
        if not out:
            out.append(L)
            continue
        if abs(L["price"] - out[-1]["price"]) <= tol:
            if L.get("strength",1) > out[-1].get("strength",1):
                out[-1] = L
        else:
            out.append(L)
    return out

def extract_levels(df: pd.DataFrame, tf_label: str, lookback_hours: int, now_ts: float, kind: str) -> list:
    if df is None or df.empty:
        return []
    bars = _bars_for_hours(tf_label, lookback_hours)
    d = df.tail(max(bars, 30)).copy()
    out = []
    n = len(d)
    if n < 10:
        return out
    k = 3
    for i in range(k, n-k):
        hi = float(d["High"].iloc[i])
        lo = float(d["Low"].iloc[i])
        if kind == "HH":
            if hi == max(d["High"].iloc[i-k:i+k+1]):
                out.append({"price": hi, "tf": tf_label, "ts": now_ts, "kind": "HH", "strength": 1})
        else:
            if lo == min(d["Low"].iloc[i-k:i+k+1]):
                out.append({"price": lo, "tf": tf_label, "ts": now_ts, "kind": "LL", "strength": 1})
    return out

def build_level_memory(symbol: str, df1m: pd.DataFrame):
    if df1m is None or df1m.empty:
        return
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
        d = df1m.tail(400)
        k = 3
        for i in range(k, len(d)-k):
            hi = float(d["High"].iloc[i])
            lo = float(d["Low"].iloc[i])
            if hi == max(d["High"].iloc[i-k:i+k+1]):
                mem.append({"price": hi, "tf": "seed", "ts": now_ts, "kind": "HH", "strength": 1})
            if lo == min(d["Low"].iloc[i-k:i+k+1]):
                mem.append({"price": lo, "tf": "seed", "ts": now_ts, "kind": "LL", "strength": 1})

    tol = LEVEL_DEDUP_TOL.get(symbol, 0.30)
    mem = _dedup_level_list(mem, tol)
    state["levels"][symbol] = mem

def nearest_level_from_memory(symbol: str, side: str, price: float) -> float | None:
    mem = state["levels"].get(symbol, []) or []
    if not mem:
        return None
    above = [L["price"] for L in mem if L["price"] > price]
    below = [L["price"] for L in mem if L["price"] < price]
    if side == "BUY":
        return min(above) if above else None
    else:
        return max(below) if below else None

def dynamic_buffer(symbol: str) -> float:
    return SPREAD_BUFFER.get(symbol, 0.0)

def format_signal(setup, buffer):
    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]
    tag = setup.get("kind","")
    extra = f"  ({tag})" if tag else ""
    return (
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}{extra}\n"
        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
        f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}"
    )

# ========== Gold logic (no ATR/RR gating) ==========

def build_setup_xau(df1m: pd.DataFrame) -> dict | None:
    # Breakout of last N bars' high/low + optional retest.
    if df1m is None or df1m.empty or len(df1m) < max(60, BREAK_LOOKBACK_N+5):
        return None

    build_level_memory("XAU", df1m)

    df5   = _resample(df1m, 5)
    df15  = _resample(df1m, 15)
    if df5.empty or df15.empty:
        return None

    i_close = len(df1m) - 1
    close_now = float(df1m["Close"].iloc[i_close])
    buf = dynamic_buffer("XAU")

    # Use last CLOSED bar for breakout check
    closed = df1m.iloc[:-1]
    if len(closed) < BREAK_LOOKBACK_N + 2:
        return None
    hiN = float(closed["High"].tail(BREAK_LOOKBACK_N).max())
    loN = float(closed["Low"].tail(BREAK_LOOKBACK_N).min())
    last_close = float(closed["Close"].iloc[-1])

    side = None
    entry = None

    if last_close > hiN:
        if RETEST_ALLOW:
            if abs(close_now - hiN) <= RETEST_TOL:
                side = "BUY"; entry = close_now
        else:
            side = "BUY"; entry = last_close
    elif last_close < loN:
        if RETEST_ALLOW:
            if abs(close_now - loN) <= RETEST_TOL:
                side = "SELL"; entry = close_now
        else:
            side = "SELL"; entry = last_close

    if side is None:
        return None

    # SL via 15m swing, bounded
    if side == "BUY":
        swing_lo = _swing_low(df15, 20)
        sl = min(entry - 1e-6, swing_lo - buf)
        risk = max(MIN_SL_ABS, entry - sl)
        sl = entry - risk
    else:
        swing_hi = _swing_high(df15, 20)
        sl = max(entry + 1e-6, swing_hi + buf)
        risk = max(MIN_SL_ABS, sl - entry)
        sl = entry + risk

    if risk < MIN_SL_ABS or risk > MAX_RISK_ABS:
        return None

    # TP by nearest memory level or risk-based, with caps
    mem_target = nearest_level_from_memory("XAU", side, entry)
    if side == "BUY":
        if mem_target is None or mem_target <= entry:
            tp_raw = entry + max(TP_MIN_ABS["XAU"], 0.8 * risk)
        else:
            tp_raw = max(mem_target, entry + TP_MIN_ABS["XAU"])
        tp = min(tp_raw, entry + MAX_TP_CAP)
    else:
        if mem_target is None or mem_target >= entry:
            tp_raw = entry - max(TP_MIN_ABS["XAU"], 0.8 * risk)
        else:
            tp_raw = min(mem_target, entry - TP_MIN_ABS["XAU"])
        tp = max(tp_raw, entry - MAX_TP_CAP)

    # proximity + dedup
    if abs(entry - close_now) > ENTRY_PROX_MULT * buf:
        return None

    conf = 0.55 + (0.05 if _in_session_utc() else 0.0)

    return {
        "symbol":"XAU","tf":"1m","side":side,"trend": "UP" if side=="BUY" else "DOWN",
        "entry": float(entry), "tp": float(tp), "sl": float(sl),
        "conf": conf, "tp_abs": abs(tp-entry), "tp_min": TP_MIN_ABS["XAU"], "kind":"BREAK"
    }

# ===================== LOGGING / OUTCOMES =====================

def append_trade(row):
    newf = not os.path.exists(TRADES_CSV)
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if newf:
            w.writeheader()
        w.writerow(row)

async def notify_outcome(symbol: str, outcome: str, price: float):
    name = SYMBOLS[symbol]["name"]
    p = rnd(symbol, price)
    text = f"✅ TP hit on {name} @ {p}" if outcome=="TP" else f"🟥 SL hit on {name} @ {p}"
    await send_main(text)

def finish_trade(symbol: str, outcome: str, price_now: float):
    sess = trade[symbol]
    trade[symbol] = None
    cooldown_until[symbol] = time.time() + COOLDOWN_SEC
    if not sess:
        return
    try:
        append_trade({
            "ts_close": datetime.utcnow().isoformat(timespec="seconds"),
            "symbol": symbol,
            "side": sess["side"],
            "entry": rnd(symbol, sess["entry"]),
            "tp": rnd(symbol, sess["tp"]),
            "sl": rnd(symbol, sess["sl"]),
            "outcome": outcome,
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
    if not SEND_IDEAS:
        return False
    now = time.time()
    if IDEA_COOLDOWN_SEC > 0 and (now - _last_idea_ts.get(sym, 0.0) < IDEA_COOLDOWN_SEC):
        return False
    _reset_hour_if_needed(sym)
    if _ideas_count_hour.get(sym, 0) >= MAX_IDEAS_PER_HOUR:
        return False
    return True

async def handle_symbol(session: aiohttp.ClientSession, symbol: str):
    global last_seen_idx, last_signal_idx, _last_signal_price

    if symbol != "XAU":
        return
    df = await get_df(session, symbol)
    if df.empty or len(df) < 240:
        return

    build_level_memory("XAU", df)

    cur_idx = len(df) - 1
    closed_idx = cur_idx - 1
    if closed_idx <= last_seen_idx[symbol]:
        return
    last_seen_idx[symbol] = closed_idx

    # check TP/SL for open idea
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
                finish_trade(symbol, "TP", price_now)
                return
            if hit_sl:
                price_now = float(post["Close"].iloc[-1])
                asyncio.create_task(notify_outcome(symbol, "SL", price_now))
                finish_trade(symbol, "SL", price_now)
                return
        return

    if time.time() - boot_ts < BOOT_COOLDOWN_S:
        return
    if time.time() < cooldown_until[symbol]:
        return

    setup = build_setup_xau(df)
    if not setup:
        return
    if last_signal_idx[symbol] == closed_idx:
        return

    buffer    = SPREAD_BUFFER.get(symbol, 0.0)
    conf_thr  = CONF_MIN_TRADE.get(symbol, 0.50)
    conf      = float(setup["conf"])
    close_now = float(df["Close"].iloc[-1])
    entry     = float(setup["entry"])

    if abs(entry - close_now) > ENTRY_PROX_MULT * buffer:
        return
    if _last_signal_price[symbol] is not None and abs(entry - _last_signal_price[symbol]) <= DEDUP_PROX_MULT * buffer:
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
        return

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await handle_symbol(session, "XAU")
                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(2)

# ===================== ALIVE LOOP =====================

async def alive_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                df_xau = await get_df(s, "XAU")
                if not df_xau.empty:
                    build_level_memory("XAU", df_xau)
                c_xau = float(df_xau["Close"].iloc[-1]) if not df_xau.empty else 0.0
                Lx = len(state["levels"]["XAU"])
                sample = [round(x["price"],2) for x in (state["levels"]["XAU"][-4:] if Lx else [])]
                msg = (f"[ALIVE] XAU: {rnd('XAU',c_xau)} (mem:{Lx}). "
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
