#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GranVex HUMAN: уровни → TP/SL с графика, без жёсткого RR.
1–5 сигналов/день, анти-пила, Yahoo 1m. NG по умолчанию.
"""

import os, time, random, asyncio, logging
from datetime import datetime
import aiohttp
import pandas as pd

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ---------------- CFG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_TELEGRAM_BOT_TOKEN")
CHAT_ID   = int(os.getenv("CHAT_ID", "123456789"))

SYMBOLS = {
    "NG":   {"yahoo":"NG=F",    "name":"NATGAS (NG=F)", "pip":0.001, "buf":0.0040, "tp_min":0.015, "sl_min":0.006},
    "GOLD": {"yahoo":"GC=F",    "name":"GOLD (FUT)",    "pip":0.05,  "buf":0.30,   "tp_min":1.50,  "sl_min":0.80},
}
DEFAULT = "NG"

POLL_SEC        = 0.35
ALIVE_EVERY_SEC = 300
MAX_PER_DAY     = 5
MIN_GAP_MIN     = 8

IDEAS_ENABLED   = True
CONF_TRADE_MIN  = 0.55
CONF_IDEA_MIN   = 0.10

LONDON = range(7, 15)   # UTC
NY     = range(12, 21)

HTTP_TIMEOUT   = 12
RETRIES        = 4
BACKOFF0       = 0.9
JITTER         = 0.35

router = Router()
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp  = Dispatcher()
dp.include_router(router)

mode = DEFAULT
day_ct = 0
day_date = None
last_signal_ts = 0.0
last_signal_price = None
last_seen_bar = -1

_prices_cache = {}

# ------------- helpers -------------
def rnd(sym, x): return round(float(x), 4 if sym=="NG" else 2)
def in_session():
    h = pd.Timestamp.utcnow().hour
    return (h in LONDON) or (h in NY)

async def yahoo_json(s, url):
    back = BACKOFF0
    for _ in range(RETRIES):
        try:
            async with s.get(url, timeout=HTTP_TIMEOUT, headers={
                "User-Agent":"Mozilla/5.0","Accept":"*/*"
            }) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429,503):
                    await asyncio.sleep(back + random.random()*JITTER)
                    back *= 1.6
                    continue
                return {}
        except:
            await asyncio.sleep(back + random.random()*JITTER)
            back *= 1.5
    return {}

def df_from_payload(p):
    try:
        r = p.get("chart",{}).get("result",[])[0]
        ts = r.get("timestamp",[])
        q  = r.get("indicators",{}).get("quote",[{}])[0]
        if not ts or not q: return pd.DataFrame()
        df = pd.DataFrame({
            "Open":q["open"],"High":q["high"],"Low":q["low"],"Close":q["close"]
        }, index=pd.to_datetime(ts, unit="s"))
        df = df.ffill().bfill().dropna()
        for c in ("Open","High","Low","Close"): df = df[df[c] > 0]
        return df.tail(2000).reset_index(drop=True)
    except: return pd.DataFrame()

async def get_df(s, sym):
    now = time.time()
    c = _prices_cache.get(sym)
    if c and now - c["ts"] < 0.35: return c["df"]
    t = SYMBOLS[sym]["yahoo"]
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d"
    df = df_from_payload(await yahoo_json(s, url))
    if not df.empty: _prices_cache[sym] = {"ts":now,"df":df}
    return df

def resample(df, m):
    if df.empty: return df
    end = pd.Timestamp.utcnow().floor("min")
    idx = pd.date_range(end - pd.Timedelta(minutes=len(df)-1), periods=len(df), freq="1min")
    z = df.copy(); z.index = idx
    o = z["Open"].resample(f"{m}min").first()
    h = z["High"].resample(f"{m}min").max()
    l = z["Low"].resample(f"{m}min").min()
    c = z["Close"].resample(f"{m}min").last()
    r = pd.concat([o,h,l,c], axis=1).dropna()
    r.columns = ["Open","High","Low","Close"]
    return r.reset_index(drop=True)

def swing_high(d, lb=20): i=len(d)-2; L=max(0,i-lb+1); return float(d["High"].iloc[L:i+1].max())
def swing_low (d, lb=20): i=len(d)-2; L=max(0,i-lb+1); return float(d["Low"].iloc [L:i+1].min())

def ema(arr,n):
    k=2/(n+1); e=None; out=[]
    for v in arr:
        e=v if e is None else (v*k + e*(1-k)); out.append(e)
    return out

def trend_strength(df1m):
    d5=resample(df1m,5)
    if d5.empty or len(d5)<25: return 0.0
    c=d5["Close"].tolist()
    e5=ema(c,5)[-1]; e20=ema(c,20)[-1]
    slope=c[-1]-c[-9]
    sign=1 if e5>e20 else (-1 if e5<e20 else 0)
    return sign*(abs(e5-e20)+slope)

def anti_chop(df1m):
    if df1m.empty or len(df1m)<6: return False
    C=df1m["Close"].values; O=df1m["Open"].values; H=df1m["High"].values; L=df1m["Low"].values
    last=-1
    seq=[C[i]-C[i-1] for i in range(last-3,last+1)]
    up=sum(1 for x in seq if x>0); dn=sum(1 for x in seq if x<0)
    body=abs(C[last]-O[last]); rng=max(1e-9, H[last]-L[last])
    return (max(up,dn)>=3) and (body/rng>=0.4)

def dedup_levels(levels, tol):
    out=[]
    for L in sorted(levels, key=lambda x: x["p"]):
        if not out: out.append(L); continue
        if abs(L["p"]-out[-1]["p"])<=tol:
            if L["w"]>out[-1]["w"]: out[-1]=L
        else: out.append(L)
    return out

def build_levels(sym, df1m):
    now=time.time()
    out=[]
    for tf,mins,hours in (("5m",5,72),("15m",15,72),("60m",60,120)):
        d=resample(df1m, mins).tail(max(30,int(hours*60/mins)))
        if d.empty: continue
        k=3
        for i in range(k,len(d)-k):
            hi=float(d["High"].iloc[i]); lo=float(d["Low"].iloc[i])
            if hi==max(d["High"].iloc[i-k:i+k+1]): out.append({"p":hi,"tf":tf,"w":1,"ts":now,"k":"HH"})
            if lo==min(d["Low"].iloc [i-k:i+k+1]): out.append({"p":lo,"tf":tf,"w":1,"ts":now,"k":"LL"})
    # легкий seed с 1m
    d1=df1m.tail(400); k=3
    for i in range(k,len(d1)-k):
        hi=float(d1["High"].iloc[i]); lo=float(d1["Low"].iloc[i])
        if hi==max(d1["High"].iloc[i-k:i+k+1]): out.append({"p":hi,"tf":"seed","w":1,"ts":now,"k":"HH"})
        if lo==min(d1["Low"].iloc [i-k:i+k+1]): out.append({"p":lo,"tf":"seed","w":1,"ts":now,"k":"LL"})
    return dedup_levels(out, tol=0.003 if sym=="NG" else 0.5)

def nearest(mem, side, price):
    ab=[L["p"] for L in mem if L["p"]>price]
    bl=[L["p"] for L in mem if L["p"]<price]
    return (min(ab) if side=="BUY" else max(bl)) if (ab if side=="BUY" else bl) else None

def format_signal(sym, setup):
    name=SYMBOLS[sym]["name"]; buf=SYMBOLS[sym]["buf"]
    return (f"🔥 {setup['side']} {name} | 1m\n"
            f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
            f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
            f"Entry: {rnd(sym,setup['entry'])}  "
            f"SpreadBuf≈{rnd(sym,buf)}  Conf: {int(setup['conf']*100)}%  Bias: {setup['bias']}")

# ---------- core decision ----------
def build_setup(sym, df1m):
    if df1m.empty or len(df1m)<240: return None
    d5   = resample(df1m,5)
    d15  = resample(df1m,15)
    d60  = resample(df1m,60)
    if d5.empty or d15.empty or d60.empty: return None

    # тренд/смещение
    bias = "UP" if trend_strength(df1m)>0 else "DOWN"
    if not anti_chop(df1m): return None  # защита от пилы

    C=float(df1m["Close"].iloc[-1]); O=float(df1m["Open"].iloc[-1])
    side = "BUY" if (C>=O and bias=="UP") else ("SELL" if (C<=O and bias=="DOWN") else None)
    if side is None: return None

    mem = build_levels(sym, df1m)
    buf = SYMBOLS[sym]["buf"]

    # SL за ближайший свинг 5m
    if side=="BUY":
        sl = min(C-1e-6, swing_low(d5,20)-buf)
        sl = min(sl, C - max(SYMBOLS[sym]["sl_min"], 4*SYMBOLS[sym]["pip"]))
    else:
        sl = max(C+1e-6, swing_high(d5,20)+buf)
        sl = max(sl, C + max(SYMBOLS[sym]["sl_min"], 4*SYMBOLS[sym]["pip"]))

    # TP на ближайший противоположный уровень
    tgt = nearest(mem, side, C)
    if tgt is None:
        # «страховочный» минимум, если уровней рядом нет
        tgt = C + (SYMBOLS[sym]["tp_min"] if side=="BUY" else -SYMBOLS[sym]["tp_min"])

    tp = (tgt + buf) if side=="BUY" else (tgt - buf)

    # здравый смысл
    risk = abs(C-sl); reward = abs(tp-C)
    if risk < max(4*SYMBOLS[sym]["pip"], 0.004 if sym=="NG" else 0.3): return None
    if reward < SYMBOLS[sym]["tp_min"]*0.8: return None
    if reward/risk < 1.1: return None

    conf = 0.6
    if in_session(): conf += 0.05
    return {"side":side,"bias":bias,"entry":C,"tp":tp,"sl":sl,"conf":min(conf,0.9)}

# ------------- engine -------------
async def send(text): 
    try: await bot.send_message(CHAT_ID, text)
    except Exception as e: logging.error(f"tg send: {e}")

def reset_day():
    global day_ct, day_date
    d = datetime.utcnow().date().isoformat()
    if day_date != d:
        day_date = d; day_ct = 0

async def handle_symbol(s, sym):
    global last_seen_bar, last_signal_ts, last_signal_price, day_ct
    df = await get_df(s, sym)
    if df.empty: return

    cur = len(df)-1
    closed = cur-1
    if closed <= last_seen_bar: return
    last_seen_bar = closed

    reset_day()
    if day_ct >= MAX_PER_DAY: return
    if time.time() - last_signal_ts < MIN_GAP_MIN*60: return

    st = build_setup(sym, df)
    if not st: return

    price_now = float(df["Close"].iloc[-1])
    if abs(st["entry"] - price_now) > 15.0*SYMBOLS[sym]["buf"]:
        return
    if last_signal_price is not None and abs(st["entry"]-last_signal_price) <= 12.0*SYMBOLS[sym]["buf"]:
        return

    if IDEAS_ENABLED and st["conf"] >= CONF_IDEA_MIN:
        await send("🧠 IDEA:\n"+format_signal(sym, st))
    if st["conf"] >= CONF_TRADE_MIN:
        await send(format_signal(sym, st))
        last_signal_ts = time.time()
        last_signal_price = st["entry"]
        day_ct += 1

async def engine():
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                await handle_symbol(s, mode)
            except Exception as e:
                logging.error(f"engine: {e}")
            await asyncio.sleep(POLL_SEC)

async def alive():
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                df = await get_df(s, mode)
                c  = float(df["Close"].iloc[-1]) if not df.empty else 0.0
                await send(f"[ALIVE] {SYMBOLS[mode]['name']}: {rnd(mode,c)} | day_ct={day_ct}/{MAX_PER_DAY}")
            except Exception as e:
                logging.error(f"alive: {e}")
            await asyncio.sleep(ALIVE_EVERY_SEC)

# ------------- commands -------------
@router.message(Command("start"))
async def start(m: Message):
    await m.answer("✅ GranVex HUMAN online.\nКоманды: газ, золото, статус, стоп")

@router.message(F.text.lower() == "газ")
async def set_ng(m: Message):
    global mode; mode="NG"; await m.answer("NG включён.")

@router.message(F.text.lower() == "золото")
async def set_gold(m: Message):
    global mode; mode="GOLD"; await m.answer("GOLD включено.")

@router.message(F.text.lower() == "статус")
async def status(m: Message):
    await m.answer(f"mode={mode}  day_ct={day_ct}/{MAX_PER_DAY}  gap>={MIN_GAP_MIN}m")

@router.message(F.text.lower() == "стоп")
async def stop(m: Message):
    global last_signal_ts; last_signal_ts = time.time()
    await m.answer("🛑 стоп: включён временный кулдаун на MIN_GAP_MIN.")

# ------------- main -------------
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine())
    asyncio.create_task(alive())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
