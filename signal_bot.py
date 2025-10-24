#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gold_bot_final.py
GOLD (XAUUSD) — агрессивный stream: сигналы по текущей свече.
TP 10п, SL 8п, кулдаун 2с. Приоритет фида GC=F.
"""

import os, time, csv, logging, asyncio, random
from datetime import datetime
import pandas as pd
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

VERSION = "GOLD STREAM v2 (no-ATR, no-wait-close)"

# ====== TOKENS / OWNER ======
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# ====== SETTINGS ======
SYMBOLS  = {"XAU": {"name": "GOLD (XAUUSD)", "tf": "1m"}}
DECIMALS = {"XAU": 2}

# STREAM (без ожиданий, максимум реактивности)
ENABLE_STREAM_MODE   = True
TP_STREAM_PIPS       = 10.0   # цель
SL_STREAM_PIPS       = 8.0    # стоп
STREAM_COOLDOWN_SEC  = 2.0    # антиспам сигналов

ONLY_ACTIVE_HOURS    = False  # если True — стрелять только Лондон/НЙ

# Базовые
SPREAD_BUFFER = {"XAU": 0.05}
TP_MIN_ABS    = {"XAU": 4.0}
POLL_SEC        = 0.25
ALIVE_EVERY_SEC = 300
COOLDOWN_SEC    = 0
BOOT_COOLDOWN_S = 0.2
TRADES_CSV = "gv_trades_gold.csv"

# HTTP/Yahoo
HTTP_TIMEOUT   = 12
YAHOO_RETRIES  = 4
YAHOO_BACKOFF0 = 0.9
YAHOO_JITTER   = 0.35
ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
    "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9", "Connection": "keep-alive",
}

# ====== STATE ======
boot_ts = time.time()
trade = {"XAU": None}
cooldown_until = {"XAU": 0.0}
last_candle_close_ts = {"XAU": 0.0}
_prices_cache = {}
_last_stream_ts = {"XAU": 0.0}

state = {"levels": {"XAU": []}}

# ====== TELEGRAM ======
router = Router()
bot_main = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
bot_log  = Bot(LOG_BOT_TOKEN,  default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher(); dp.include_router(router)

def rnd(sym: str, x: float) -> float: return round(float(x), DECIMALS.get(sym, 2))

async def send_main(text: str):
    try: await bot_main.send_message(TARGET_CHAT_ID, text)
    except Exception as e: logging.error(f"send_main error: {e}")

async def send_log(text: str):
    try: await bot_log.send_message(TARGET_CHAT_ID, text)
    except Exception as e: logging.error(f"send_log error: {e}")

def format_signal(setup, buffer):
    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]; tag=setup.get("kind","")
    extra=f"  ({tag})" if tag else ""
    return (f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}{extra}\n"
            f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
            f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
            f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot is alive ({VERSION}).")
    await m.answer("Команды: статус / тест")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    now = time.time(); s="XAU"
    opened = bool(trade[s])
    age = int(now - last_candle_close_ts[s]) if last_candle_close_ts[s] else -1
    L = len(state["levels"][s]) if isinstance(state["levels"].get(s), list) else 0
    sample = [round(x["price"],2) for x in (state["levels"][s][-6:] if L else [])]
    lines = [
        f"{SYMBOLS[s]['name']}: open={opened} cooldown={max(0,int(cooldown_until[s]-now))}s last_close_age={age}s",
        f"stream: {'ON' if ENABLE_STREAM_MODE else 'OFF'} tp={TP_STREAM_PIPS} sl={SL_STREAM_PIPS} cool={STREAM_COOLDOWN_SEC}s",
        f"levels_mem={L} sample={sample if sample else '[]'}",
    ]
    await m.answer("`\n"+ "\n".join(lines) + "\n`")

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    await m.answer("🔥 BUY GOLD (XAUUSD) | 1m\n✅ TP: **4142.0**\n🟥 SL: **4116.0**\nEntry: 4128.0  Spread≈0.05")

# ====== FEED ======
async def _yahoo_json(session: aiohttp.ClientSession, url: str) -> dict:
    backoff = YAHOO_BACKOFF0
    for _ in range(YAHOO_RETRIES):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
                if r.status == 200: return await r.json(content_type=None)
                if r.status in (429,503):
                    await asyncio.sleep(backoff + (random.random()*YAHOO_JITTER)); backoff*=1.7; continue
                return {}
        except:
            await asyncio.sleep(backoff + (random.random()*YAHOO_JITTER)); backoff*=1.6
    return {}

def _df_from_yahoo_v8(payload: dict) -> pd.DataFrame:
    try:
        r = payload.get("chart", {}).get("result", [])[0]
        ts = r.get("timestamp", []); q  = r.get("indicators", {}).get("quote", [])[0]
        if not ts or not q: return pd.DataFrame()
        df = pd.DataFrame({"Open":q.get("open",[]),"High":q.get("high",[]),
                           "Low":q.get("low",[]),"Close":q.get("close",[])},
                          index=pd.to_datetime(ts, unit="s"))
        df = df.ffill().bfill().dropna()
        for c in ("Open","High","Low","Close"): df = df[df[c]>0]
        return df.tail(2000).reset_index(drop=True)
    except: return pd.DataFrame()

async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
    now_ts = time.time(); c = _prices_cache.get(symbol); cache_ttl = 0.30
    if c and (now_ts - c["ts"] < cache_ttl) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]
    if symbol != "XAU": return pd.DataFrame()

    # приоритет — FUTURES, потом SPOT
    candidates = [("GC=F","futures"), ("XAUUSD=X","spot-main"), ("XAU=X","spot-alt")]
    current_feed = c.get("feed") if c else None

    for t, tag in candidates:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d&includePrePost=true"
        payload = await _yahoo_json(session, url)
        df = _df_from_yahoo_v8(payload)
        if not df.empty:
            last_candle_close_ts["XAU"] = time.time()
            new_feed = f"yahoo:{tag}:{t}"
            _prices_cache["XAU"] = {"ts": now_ts, "df": df, "feed": new_feed}
            if new_feed != current_feed:
                try: last = float(df["Close"].iloc[-1])
                except: last = 0.0
                await send_log(f"[FEED] ✅ switched to {tag} ({t}) last={round(last,2)} rows={len(df)}")
            return df

    last_empty = _prices_cache.get("_last_empty_log", 0)
    if now_ts - last_empty > 15:
        await send_log("[FEED] ❌ all empty — check network")
        _prices_cache["_last_empty_log"] = now_ts
    return pd.DataFrame()

# ====== LEVELS (для инфо) ======
def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    end = pd.Timestamp.utcnow().floor("min")
    idx = pd.date_range(end - pd.Timedelta(minutes=len(df)-1), periods=len(df), freq="1min")
    z = df.copy(); z.index = idx
    o=z["Open"].resample(f"{minutes}min").first()
    h=z["High"].resample(f"{minutes}min").max()
    l=z["Low"].resample(f"{minutes}min").min()
    c=z["Close"].resample(f"{minutes}min").last()
    r=pd.concat([o,h,l,c],axis=1).dropna(); r.columns=["Open","High","Low","Close"]
    return r.reset_index(drop=True)

def build_level_memory(symbol: str, df1m: pd.DataFrame):
    if df1m is None or df1m.empty: return
    now_ts=time.time(); df5=_resample(df1m,5); df15=_resample(df1m,15); df60=_resample(df1m,60)
    mem=state["levels"].get(symbol,[]) or []; mem=[L for L in mem if now_ts-L.get("ts",now_ts)<=48*3600]
    def add(d,tf):
        if d is None or d.empty: return
        k=3
        for i in range(k, len(d)-k):
            hi=float(d["High"].iloc[i]); lo=float(d["Low"].iloc[i])
            if hi==max(d["High"].iloc[i-k:i+k+1]): mem.append({"price":hi,"tf":tf,"ts":now_ts,"kind":"HH"})
            if lo==min(d["Low"].iloc[i-k:i+k+1]): mem.append({"price":lo,"tf":tf,"ts":now_ts,"kind":"LL"})
    add(df5,"5m"); add(df15,"15m"); add(df60,"60m")
    mem = sorted({round(x["price"],2):x for x in mem}.values(), key=lambda x:x["price"])
    state["levels"][symbol]=mem

# ====== LOGGING / OUTCOMES ======
def append_trade(row):
    newf=not os.path.exists(TRADES_CSV)
    with open(TRADES_CSV,"a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=list(row.keys()))
        if newf: w.writeheader()
        w.writerow(row)

async def notify_outcome(symbol: str, outcome: str, price: float):
    name=SYMBOLS[symbol]["name"]; p=rnd(symbol, price)
    await send_main(f"✅ TP hit on {name} @ {p}" if outcome=="TP" else f"🟥 SL hit on {name} @ {p}")

def finish_trade(symbol: str, outcome: str, price_now: float):
    sess=trade[symbol]; trade[symbol]=None; cooldown_until[symbol]=time.time()+COOLDOWN_SEC
    if not sess: return
    try:
        append_trade({"ts_close":datetime.utcnow().isoformat(timespec="seconds"),
                      "symbol":symbol,"side":sess["side"],
                      "entry":rnd(symbol,sess["entry"]),"tp":rnd(symbol,sess["tp"]),
                      "sl":rnd(symbol,sess["sl"]),"outcome":outcome,
                      "life_sec":int(time.time()-sess.get("opened_at",time.time()))})
    except Exception as e: logging.error(f"log append error: {e}")

# ====== ENGINE ======
async def handle_symbol(session: aiohttp.ClientSession, symbol: str):
    if symbol!="XAU": return
    df=await get_df(session, symbol)
    if df.empty or len(df)<60: return

    # поддерживаем уровни «для вида»
    build_level_memory("XAU", df)

    # Проверка TP/SL по открытой
    sess=trade[symbol]
    if sess:
        start_i=int(sess.get("entry_bar_idx", len(df)-1))
        post=df.iloc[(start_i + 1):]
        if not post.empty:
            side=sess["side"]; tp=sess["tp"]; sl=sess["sl"]
            hit_tp=(post["High"].max()>=tp) if side=="BUY" else (post["Low"].min()<=tp)
            hit_sl=(post["Low"].min()<=sl) if side=="BUY" else (post["High"].max()>=sl)
            if hit_tp: price_now=float(post["Close"].iloc[-1]); asyncio.create_task(notify_outcome(symbol,"TP",price_now)); finish_trade(symbol,"TP",price_now); return
            if hit_sl: price_now=float(post["Close"].iloc[-1]); asyncio.create_task(notify_outcome(symbol,"SL",price_now)); finish_trade(symbol,"SL",price_now); return
        return  # ждём исхода открытой

    if time.time()-boot_ts < BOOT_COOLDOWN_S: return
    if time.time() < cooldown_until[symbol]: return

    # ===== STREAM: вход по текущей свече каждые N секунд =====
        # ===== STREAM (динамические TP/SL по графику) =====
    if ENABLE_STREAM_MODE:
        now_ts = time.time()
        if ONLY_ACTIVE_HOURS:
            h = pd.Timestamp.utcnow().hour
            if not ((7 <= h <= 14) or (12 <= h <= 20)):
                return
        if now_ts - _last_stream_ts[symbol] < STREAM_COOLDOWN_SEC:
            return

        i = len(df) - 1
        O = float(df["Open"].iloc[i])
        C = float(df["Close"].iloc[i])
        side  = "BUY" if C >= O else "SELL"
        entry = float(df["Close"].iloc[i])
        buf   = SPREAD_BUFFER.get(symbol, 0.0)

        # попробуем взять ближайший уровень из памяти (если он логичен)
        mem_target = nearest_level_from_memory(symbol, side, entry)

        # свинг 15m как опора для SL/TP
        df15 = _resample(df, 15)
        swing_ok = False
        if not df15.empty:
            if side == "BUY":
                swing_lo = _swing_low(df15, 20)
                swing_ok = True
            else:
                swing_hi = _swing_high(df15, 20)
                swing_ok = True

        # вычислим SL: по свингу если есть, иначе небольшая страховка
        if side == "BUY":
            if swing_ok:
                sl = min(entry - 1e-6, swing_lo - buf)
                # не делаем стоп слишком близко
                if entry - sl < 2.0:
                    sl = entry - max(2.0, SL_STREAM_PIPS) 
            else:
                sl = entry - SL_STREAM_PIPS
        else:
            if swing_ok:
                sl = max(entry + 1e-6, swing_hi + buf)
                if sl - entry < 2.0:
                    sl = entry + max(2.0, SL_STREAM_PIPS)
            else:
                sl = entry + SL_STREAM_PIPS

        # вычислим TP: предпочтение ближайшему уровню, иначе RR*случай/кап
        tp_min = TP_MIN_ABS.get(symbol, 4.0)
        tp_cap = MAX_TP_CAP if 'MAX_TP_CAP' in globals() else 30.0

        cand_tps = []
        # если есть уровень в нужном направлении и адекватен — используем
        if mem_target is not None:
            if side == "BUY" and mem_target > entry + 0.5:
                cand_tps.append(min(mem_target, entry + tp_cap))
            if side == "SELL" and mem_target < entry - 0.5:
                cand_tps.append(max(mem_target, entry - tp_cap))

        # базовый TP по соотношению RR ≈ 1.25
        risk = abs(entry - sl)
        if risk <= 0:
            base_rr = tp_min
        else:
            base_rr = entry + (1.25 * risk) if side == "BUY" else entry - (1.25 * risk)
            # ограничим в разумных пределах
            if side == "BUY":
                base_rr = min(base_rr, entry + tp_cap)
            else:
                base_rr = max(base_rr, entry - tp_cap)
        cand_tps.append(base_rr)

        # ещё можно учесть локальный диапазон (5m) для усиления цели
        df5 = _resample(df, 5)
        if not df5.empty:
            h5 = float(df5["High"].tail(6).max()); l5 = float(df5["Low"].tail(6).min())
            rng5 = max(0.0, h5 - l5)
            vol_tp = entry + (0.5 * rng5) if side == "BUY" else entry - (0.5 * rng5)
            cand_tps.append(vol_tp)

        # финальный TP: выбираем наиболее консервативный/адекватный вариант
        if side == "BUY":
            tp = max([x for x in cand_tps if x>entry], default=entry+tp_min)
            tp = min(tp, entry + tp_cap)
            tp = max(tp, entry + tp_min)
        else:
            tp = min([x for x in cand_tps if x<entry], default=entry-tp_min)
            tp = max(tp, entry - tp_cap)
            tp = min(tp, entry - tp_min)

        # safety: приводим tp/sl к 2 знакам
        tp = round(float(tp), 2)
        sl = round(float(sl), 2)

        setup = {
            "symbol": symbol, "tf": "1m", "side": side,
            "trend": "UP" if side == "BUY" else "DOWN",
            "entry": entry, "tp": tp, "sl": sl,
            "tp_abs": abs(tp-entry), "tp_min": tp_min,
            "kind": "STREAM_DYNAMIC"
        }

        await send_main("🧠 IDEA:\n" + format_signal(setup, buf))
        await send_main(format_signal(setup, buf))

        cur_idx = len(df) - 1
        trade[symbol] = {
            "side": side, "entry": entry, "tp": tp, "sl": sl,
            "opened_at": time.time(), "entry_bar_idx": cur_idx,
        }
        _last_stream_ts[symbol] = now_ts
        return

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await handle_symbol(session,"XAU"); await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}"); await asyncio.sleep(2)

# ====== ALIVE ======
async def alive_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                df_xau=await get_df(s,"XAU")
                if not df_xau.empty: build_level_memory("XAU", df_xau)
                c_xau=float(df_xau["Close"].iloc[-1]) if not df_xau.empty else 0.0
                Lx=len(state["levels"]["XAU"]); sample=[round(x["price"],2) for x in (state["levels"]["XAU"][-6:] if Lx else [])]
                await send_log(f"[ALIVE] XAU: {rnd('XAU',c_xau)} (mem:{Lx}) levels_sample:{sample if sample else '[]'} OK")
        except Exception as e:
            await send_log(f"[ALIVE ERROR] {e}")
        await asyncio.sleep(ALIVE_EVERY_SEC)

# ====== MAIN ======
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine_loop()); asyncio.create_task(alive_loop())
    await dp.start_polling(bot_main)

if __name__=="__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass

