#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, random, asyncio, logging
from datetime import datetime
import pandas as pd
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ====== CONFIG ======
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = OWNER_ID

SYMBOL = "NG"
YAHOO_SYMBOL = "NG=F"
SPREAD_BUF = 0.004
TP_ABS = 0.020
SL_ABS = 0.012

# ====== sensitivity ======
SCALP_ATR1_MIN     = 0.0020
SCALP_MIN_IMPULSE  = 0.0010
SCALP_MIN_BODY     = 0.0008
SCALP_NEAR_BREAK   = 0.0010
SCALP_COOLDOWN_SEC = 1

TREND_ENABLED = True
TREND_MIN_MOVE = 0.0040

POLL_SEC        = 0.25
ALIVE_EVERY_SEC = 300
HTTP_TIMEOUT    = 12

router = Router()
bot_main = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
bot_log  = Bot(LOG_BOT_TOKEN,  default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

trade = None
last_signal_price = None
last_seen_idx = -1
prices_cache = {}

# ====== helpers ======
def rnd(x): return round(float(x), 4)

def in_session():
    h = pd.Timestamp.utcnow().hour
    return (7 <= h < 15) or (12 <= h < 21)

async def send_main(t): 
    try: await bot_main.send_message(TARGET_CHAT_ID, t)
    except Exception as e: logging.error(f"send_main: {e}")

async def send_log(t): 
    try: await bot_log.send_message(TARGET_CHAT_ID, t)
    except Exception as e: logging.error(f"send_log: {e}")

# ====== data ======
async def yahoo_json(s, url):
    backoff=0.9
    for _ in range(4):
        try:
            async with s.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent":"Mozilla/5.0"}) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429,503):
                    await asyncio.sleep(backoff+random.random()*0.3)
                    backoff*=1.6
                    continue
                return {}
        except:
            await asyncio.sleep(backoff+random.random()*0.3)
            backoff*=1.5
    return {}

def df_from_payload(p):
    try:
        r = p.get("chart",{}).get("result",[])[0]
        ts = r.get("timestamp",[])
        q  = r.get("indicators",{}).get("quote",[{}])[0]
        if not ts or not q: return pd.DataFrame()
        df = pd.DataFrame({"Open":q["open"],"High":q["high"],"Low":q["low"],"Close":q["close"]}, index=pd.to_datetime(ts, unit="s"))
        df = df.ffill().bfill().dropna()
        for c in ("Open","High","Low","Close"):
            df = df[df[c] > 0]
        return df.tail(2000).reset_index(drop=True)
    except:
        return pd.DataFrame()

async def get_df(s):
    now = time.time()
    c = prices_cache.get(SYMBOL)
    if c and now - c["ts"] < 0.35: return c["df"]
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{YAHOO_SYMBOL}?interval=1m&range=5d"
    df = df_from_payload(await yahoo_json(s, url))
    if not df.empty: prices_cache[SYMBOL] = {"ts":now,"df":df}
    return df

# ====== ATR ======
def atr_m1(df, period=14):
    if df.empty or len(df) < period+2: return 0.0
    h = df["High"].values; l = df["Low"].values; c = df["Close"].values
    trs=[]
    for i in range(1,len(c)):
        tr = max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
        trs.append(tr)
    return float(sum(trs[-period:])/period) if len(trs)>=period else 0.0

# ====== scalp core ======
def build_scalp(df):
    atr1 = atr_m1(df)
    if atr1 < SCALP_ATR1_MIN: return None

    i = len(df)-1
    H = float(df["High"].iloc[i])
    L = float(df["Low"].iloc[i])
    O = float(df["Open"].iloc[i])
    C = float(df["Close"].iloc[i])
    rng = H - L
    body = abs(C - O)

    if rng < SCALP_MIN_IMPULSE or body < SCALP_MIN_BODY:
        return None

    near_up = (H - C) <= SCALP_NEAR_BREAK
    near_down = (C - L) <= SCALP_NEAR_BREAK

    side = None
    if near_up and C >= O: side = "BUY"
    elif near_down and C <= O: side = "SELL"
    else: return None

    entry = C
    if side == "BUY":
        tp = entry + TP_ABS + SPREAD_BUF
        sl = entry - SL_ABS - SPREAD_BUF
    else:
        tp = entry - TP_ABS - SPREAD_BUF
        sl = entry + SL_ABS + SPREAD_BUF

    conf = 0.6 + (0.05 if in_session() else 0.0)
    return {"side":side,"entry":entry,"tp":tp,"sl":sl,"conf":min(conf,0.9)}

def format_signal(st):
    return (f"🔥 {st['side']} NATGAS (NG=F) | 1m\n"
            f"✅ TP: {rnd(st['tp'])}\n"
            f"🟥 SL: {rnd(st['sl'])}\n"
            f"Entry: {rnd(st['entry'])}  Spread≈{SPREAD_BUF}  Conf: {int(st['conf']*100)}%")

# ====== ENGINE ======
async def engine_loop():
    global trade, last_signal_price, last_seen_idx
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                df = await get_df(s)
                if df.empty or len(df) < 30:
                    await asyncio.sleep(POLL_SEC); continue

                cur = len(df)-1
                closed = cur-1
                if closed <= last_seen_idx:
                    await asyncio.sleep(POLL_SEC); continue
                last_seen_idx = closed

                # check active trade TP/SL
                if trade:
                    post = df.iloc[trade["idx"]+1:]
                    if not post.empty:
                        side = trade["side"]
                        if side=="BUY":
                            if post["High"].max() >= trade["tp"]:
                                await send_main(f"✅ TP hit @ {rnd(trade['tp'])}")
                                trade = None
                            elif post["Low"].min() <= trade["sl"]:
                                await send_main(f"🟥 SL hit @ {rnd(trade['sl'])}")
                                trade = None
                        else:
                            if post["Low"].min() <= trade["tp"]:
                                await send_main(f"✅ TP hit @ {rnd(trade['tp'])}")
                                trade = None
                            elif post["High"].max() >= trade["sl"]:
                                await send_main(f"🟥 SL hit @ {rnd(trade['sl'])}")
                                trade = None
                    await asyncio.sleep(POLL_SEC); continue

                setup = build_scalp(df)
                if not setup:
                    await asyncio.sleep(POLL_SEC); continue

                price_now = float(df["Close"].iloc[-1])
                if abs(setup["entry"] - price_now) > 15.0*SPREAD_BUF:
                    await asyncio.sleep(POLL_SEC); continue
                if last_signal_price and abs(setup["entry"]-last_signal_price) <= 12.0*SPREAD_BUF:
                    await asyncio.sleep(POLL_SEC); continue

                await send_main(format_signal(setup))
                trade = {
                    "side": setup["side"],
                    "entry": setup["entry"],
                    "tp": setup["tp"],
                    "sl": setup["sl"],
                    "idx": closed,
                }
                last_signal_price = setup["entry"]

            except Exception as e:
                logging.error(f"engine: {e}")
            await asyncio.sleep(POLL_SEC)

# ====== ALIVE ======
async def alive_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                df = await get_df(s)
                c = float(df["Close"].iloc[-1]) if not df.empty else 0.0
                await send_log(f"[ALIVE] NG={rnd(c)} | Status OK")
        except Exception as e:
            await send_log(f"[ALIVE ERROR] {e}")
        await asyncio.sleep(ALIVE_EVERY_SEC)

# ====== TG CMDS ======
@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ GranVex V8.1-LITE (SCALP) запущен.\nРежим: NATGAS\nTP {TP_ABS} | SL {SL_ABS}")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    await m.answer(f"mode: NG  last_price={last_signal_price}  active={bool(trade)}")

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    global trade
    trade = None
    await m.answer("🛑 Все позиции сброшены.")

# ====== MAIN ======
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
