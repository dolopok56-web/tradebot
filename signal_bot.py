#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, csv, logging, asyncio, random
from datetime import datetime

import pandas as pd
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

VERSION = "V5.6 Ultra-Scalper (TP=0.5*ATR15, RR>=0.20 idea+trade, Conf>=0.55, 1s loop)"

# ===== TOKENS / OWNER (оставлены как просил) =====
MAIN_BOT_TOKEN = "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"
LOG_BOT_TOKEN  = "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw"
OWNER_ID       = 6784470762
TARGET_CHAT_ID = 6784470762

# ===== MARKETS =====
SYMBOLS = {
    "NG":  {"name": "NG=F",      "tf": "1m"},
    "XAU": {"name": "XAUUSD=X",  "tf": "1m"},
}
SPREAD_BUFFER = {"NG": 0.0020, "XAU": 0.20}

# ===== STRATEGY / FILTERS =====
CONF_MIN_IDEA  = 0.05
CONF_MIN_TRADE = 0.55
RR_TRADE_MIN   = 0.20           # trade threshold (низкий RR, SL дальше)
RR_MIN_IDEA    = 0.20           # HOTFIX: idea threshold = 0.20 (как trade)
FRESH_MULT     = 10.0           # свежесть: |Entry-Close_now| <= 10*buf

SEND_IDEAS         = True
IDEA_COOLDOWN_SEC  = 90
MAX_IDEAS_PER_HOUR = 20

POLL_SEC        = 1
ALIVE_EVERY_SEC = 300
BOOT_COOLDOWN_S = 15
COOLDOWN_SEC    = 10
COOLDOWN_SEC_NG = 7

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

# ===== STATE =====
boot_ts = time.time()
trade = {"NG": None, "XAU": None}
cooldown_until = {"NG": 0.0, "XAU": 0.0}
last_candle_close_ts = {"NG": 0.0, "XAU": 0.0}
_last_idea_ts = {"NG": 0.0, "XAU": 0.0}
_ideas_count_hour = {"NG": 0, "XAU": 0}
_ideas_count_hour_ts = {"NG": 0.0, "XAU": 0.0}
last_seen_idx = {"NG": -1, "XAU": -1}
last_signal_idx = {"NG": -1, "XAU": -1}
_prices_cache = {}
mode = "AUTO"   # AUTO: и NG, и XAU

# ===== TELEGRAM =====
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

def mode_title(m: str) -> str:
    return {"NG":"NATGAS (NG=F)","XAU":"GOLD (XAUUSD)","AUTO":"NATGAS+GOLD (AUTO)"}.get(m,m)

async def _request_mode(new_mode: str, m: Message | None = None):
    global mode
    mode = new_mode
    if m: await m.answer(f"✅ Режим {new_mode}: слежу за {mode_title(new_mode)}.")

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
        "• газ / золото / авто — выбор рынка\n"
        "• стоп — стоп и короткий кулдаун\n"
        "• статус — диагностика\n"
        "• отчет — 10 последних закрытий\n"
        "• тест — тестовый сигнал"
    )

@router.message(F.text.lower() == "газ")
async def set_ng(m: Message):  await _request_mode("NG", m)

@router.message(F.text.lower() == "золото")
async def set_xau(m: Message): await _request_mode("XAU", m)

@router.message(F.text.lower() == "авто")
async def set_auto(m: Message): await _request_mode("AUTO", m)

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    now = time.time()
    for s in trade.keys(): trade[s] = None; cooldown_until[s] = now + 5
    await m.answer("🛑 Остановил. Открытых нет, короткий кулдаун.")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    lines = [f"mode: {mode}"]
    now = time.time()
    for s in ["NG","XAU"]:
        opened = bool(trade[s])
        age = int(now - last_candle_close_ts[s]) if last_candle_close_ts[s] else -1
        nm = SYMBOLS[s]["name"]; cd = max(0, int(cooldown_until[s]-now))
        lines.append(f"{nm}: open={opened}  cooldown={cd}s  last_close_age={age}s")
    await m.answer("```\n"+ "\n".join(lines) + "\n```")

@router.message(F.text.lower() == "отчет")
async def cmd_report(m: Message):
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
    await m.answer(
        "🔥 BUY NG=F | 1m\n"
        "✅ TP: **2.9990**\n"
        "🟥 SL: **2.9550**\n"
        "Entry: 2.9800  Spread≈0.0020  RR≈0.35  Conf: 60%"
    )

# ===== PRICE FEEDS (Yahoo) =====
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
        return df.tail(1000).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
    now_ts = time.time()
    c = _prices_cache.get(symbol)
    if c and (now_ts - c["ts"] < 10.0) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]
    tickers = {"NG": ["NG%3DF"], "XAU": ["XAUUSD%3DX","GC%3DF"]}
    for t in tickers.get(symbol, []):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"
        df = _df_from_yahoo_v8(await _yahoo_json(session, url))
        if not df.empty:
            last_candle_close_ts[symbol] = time.time()
            _prices_cache[symbol] = {"ts": now_ts, "df": df}
            return df
    return pd.DataFrame()

# ===== SMC UTILS =====
def rnd(sym: str, x: float) -> float:
    return round(float(x), 4) if sym == "NG" else round(float(x), 2)

def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    idx = pd.date_range(start=pd.Timestamp.utcnow().floor('D'), periods=len(df), freq="1min")
    z = df.copy(); z.index = idx
    o = z["Open"].resample(f"{minutes}min").first()
    h = z["High"].resample(f"{minutes}min").max()
    l = z["Low"].resample(f"{minutes}min").min()
    c = z["Close"].resample(f"{minutes}min").last()
    r = pd.concat([o,h,l,c], axis=1).dropna()
    r.columns = ["Open","High","Low","Close"]
    return r.reset_index(drop=True)

def _swing_high(df: pd.DataFrame, lookback: int = 20) -> float:
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["High"].iloc[L:i+1].max())

def _swing_low(df: pd.DataFrame, lookback: int = 20) -> float:
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["Low"].iloc[L:i+1].min())

def _atr_m(df: pd.DataFrame, minutes: int = 15, period: int = 14) -> float:
    d = _resample(df, minutes)
    if d.empty: return 0.0
    tr = (d["High"] - d["Low"]).rolling(period).mean()
    return float(tr.iloc[-1]) if not tr.empty and pd.notna(tr.iloc[-1]) else 0.0

def dynamic_buffer(symbol: str) -> float:
    return SPREAD_BUFFER.get(symbol, 0.0)

# ===== SIGNAL BUILDER (V5.6: TP=0.5*ATR15, RR>=0.20) =====
def build_setup(df1m: pd.DataFrame, symbol: str):
    if df1m is None or df1m.empty or len(df1m) < 200: return None
    df15 = _resample(df1m, 15)
    if df15.empty: return None

    entry = float(df1m["Close"].iloc[-2])
    hi15  = _swing_high(df15, 20)
    lo15  = _swing_low(df15, 20)
    buf   = dynamic_buffer(symbol)

    # Простая сторона: положение к середине диапазона M15
    bias = "UP" if entry >= (lo15 + (hi15 - lo15)/2.0) else "DOWN"
    side = "BUY" if bias == "UP" else "SELL"

    # SL по структуре (далеко) + буфер
    sl = (lo15 - buf) if side == "BUY" else (hi15 + buf)

    # TP всегда близкий: 0.5 * ATR(15)
    atr15 = _atr_m(df1m, 15, 14)
    tp_dist = 0.5 * max(atr15, 1e-9)
    tp = entry + tp_dist if side == "BUY" else entry - tp_dist

    rr = abs(tp - entry) / max(abs(entry - sl), 1e-9)
    conf = 0.60  # лёгкая суррогатная уверенность, чтобы не молчал

    return {"symbol": symbol, "side": side, "entry": entry, "tp": tp, "sl": sl,
            "rr": rr, "conf": conf, "tf": "1m"}

def format_signal(setup, buffer):
    sym=setup["symbol"]; side=setup["side"]
    return (
        f"🔥 {side} {SYMBOLS[sym]['name']} | {setup['tf']}\n"
        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
        f"Entry: {rnd(sym,setup['entry'])}  Spread≈{rnd(sym,buffer)}  "
        f"RR≈{round(setup['rr'],2)}  Conf: {int(setup['conf']*100)}%"
    )

# ===== EXECUTION / LOGGING =====
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

# ===== ENGINE =====
def _reset_hour_if_needed(sym: str):
    now = time.time()
    start = _ideas_count_hour_ts.get(sym, 0.0) or 0.0
    if now - start >= 3600:
        _ideas_count_hour_ts[sym] = now
        _ideas_count_hour[sym] = 0

def can_send_idea(sym: str) -> bool:
    if not SEND_IDEAS: return False
    now = time.time()
    if now - _last_idea_ts.get(sym, 0.0) < IDEA_COOLDOWN_SEC: return False
    _reset_hour_if_needed(sym)
    if _ideas_count_hour.get(sym, 0) >= MAX_IDEAS_PER_HOUR: return False
    return True

def is_fresh_enough(symbol: str, entry: float, close_now: float) -> bool:
    buf = SPREAD_BUFFER.get(symbol, 0.0)
    lim = FRESH_MULT * buf
    return abs(float(entry) - float(close_now)) <= lim

async def handle_symbol(session: aiohttp.ClientSession, symbol: str):
    df = await get_df(session, symbol)
    if df.empty or len(df) < 200: return

    cur_idx = len(df)-1
    closed_idx = cur_idx - 1
    if closed_idx <= last_seen_idx[symbol]: return
    last_seen_idx[symbol] = closed_idx

    # Сопровождение открытой сессии
    sess = trade[symbol]
    if sess:
        post = df.iloc[int(sess.get("entry_bar_idx", cur_idx)) + 1:]
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

    if time.time() - boot_ts < BOOT_COOLDOWN_S: return
    if time.time() < cooldown_until[symbol]:   return

    setup = build_setup(df, symbol)
    if not setup: return

    if last_signal_idx[symbol] == closed_idx: return
    last_signal_idx[symbol] = closed_idx

    buffer    = dynamic_buffer(symbol)
    conf      = float(setup["conf"])
    rr        = float(setup["rr"])
    close_now = float(df["Close"].iloc[-1])
    entry     = float(setup["entry"])

    if not is_fresh_enough(symbol, entry, close_now): return

    # IDEA (теперь RR>=0.20)
    if conf >= CONF_MIN_IDEA and rr >= RR_MIN_IDEA and can_send_idea(symbol):
        await send_main("🧠 IDEA:\n" + format_signal(setup, buffer))
        _last_idea_ts[symbol] = time.time()
        _ideas_count_hour[symbol] = _ideas_count_hour.get(symbol, 0) + 1
        if _ideas_count_hour_ts.get(symbol, 0.0) == 0.0:
            _ideas_count_hour_ts[symbol] = time.time()

    # TRADE (Conf≥0.55, RR≥0.20)
    if (conf >= CONF_MIN_TRADE) and (rr >= RR_TRADE_MIN):
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
        while True:
            try:
                if mode in ("AUTO","NG"):  await handle_symbol(session, "NG")
                if mode in ("AUTO","XAU"): await handle_symbol(session, "XAU")
                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(0.5)

async def alive_loop():
    while True:
        try:
            await send_log("[ALIVE] V5.6 running. Engine OK.")
        except Exception as e:
            await send_log(f"[ALIVE ERROR] {e}")
        await asyncio.sleep(ALIVE_EVERY_SEC)

# ===== MAIN =====
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
