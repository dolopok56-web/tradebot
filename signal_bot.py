#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ====== GOLD SCREAMER (простая версия) ======
# • Источник: Yahoo (GC=F приор., XAUUSD=X — запасной)
# • Каждая закрытая M1 => сигнал
# • Направление: цвет последней M5 (простой тренд-бар, БЕЗ индикаторов)
# • SL: за экстремумом сигнальной свечи / свинга + буфер
# • TP: ближайший свинг-уровень или RR≈1.35*risk
# • Ограничения TP: min 4 пп, max 30 пп
# • Никаких блокировок «открытой» — стреляет всегда
# • Мини-фильтр от мусора: range>=3 пп, body>=1 пп

import os, asyncio, time, random, logging
from datetime import datetime
import aiohttp
import pandas as pd

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ========= CONFIG =========
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "6784470762")))

SYMBOL_NAME = "GOLD (GC=F/XAU)"
POLL_SEC = 1.0

# Порог/ограничения для TP/SL (в «пунктах» твоего брокера)
TP_MIN = 4.0
TP_CAP = 30.0
SL_MIN = 2.0
SL_BUFER = 0.2

# Мини-фильтр свечи
MIN_RANGE = 3.0
MIN_BODY  = 1.0

# ====== Telegram ======
router = Router()
bot_main = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

async def send(text: str):
    try:
        await bot_main.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.error(f"send error: {e}")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer("✅ GOLD Screamer онлайн. Каждая M1 закрылась — прилетит сигнал.\nКоманда: статус")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    info = (f"mode: XAU | poll={POLL_SEC}s | tp_min={TP_MIN} tp_cap={TP_CAP}\n"
            f"range>= {MIN_RANGE}  body>= {MIN_BODY}\n"
            f"Источник: Yahoo GC=F->XAUUSD=X")
    await m.answer("`\n"+info+"\n`")

# ====== Yahoo fetch ======
UA = {"User-Agent": "Mozilla/5.0"}
YURL = "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1m&range=5d"

async def _yahoo_json(session, symbol):
    url = YURL.format(symbol)
    backoff = 0.6
    for _ in range(5):
        try:
            async with session.get(url, headers=UA, timeout=10) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429, 503):
                    await asyncio.sleep(backoff + random.random()*0.3)
                    backoff *= 1.7
                    continue
                return {}
        except:
            await asyncio.sleep(backoff)
            backoff *= 1.5
    return {}

def _df_from_yahoo(payload: dict) -> pd.DataFrame:
    try:
        res = payload.get("chart", {}).get("result", [])[0]
        ts = res.get("timestamp", [])
        q  = res.get("indicators", {}).get("quote", [])[0]
        if not ts or not q: return pd.DataFrame()
        df = pd.DataFrame({
            "Open":  q.get("open",  []),
            "High":  q.get("high",  []),
            "Low":   q.get("low",   []),
            "Close": q.get("close", []),
        }, index=pd.to_datetime(ts, unit="s"))
        df = df.ffill().bfill().dropna()
        for c in ("Open","High","Low","Close"):
            df = df[df[c] > 0]
        return df.tail(2000).reset_index(drop=True)
    except:
        return pd.DataFrame()

async def get_m1(session) -> pd.DataFrame:
    # приоритет GC=F (фьюч), запасной XAUUSD=X
    for sym in ("GC=F", "XAUUSD=X"):
        d = _df_from_yahoo(await _yahoo_json(session, sym))
        if not d.empty: return d
    return pd.DataFrame()

# ====== utils ======
def resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    end = pd.Timestamp.utcnow().floor("min")
    idx = pd.date_range(end - pd.Timedelta(minutes=len(df)-1),
                        periods=len(df), freq="1min")
    z = df.copy(); z.index = idx
    o = z["Open"].resample(f"{minutes}min").first()
    h = z["High"].resample(f"{minutes}min").max()
    l = z["Low"].resample(f"{minutes}min").min()
    c = z["Close"].resample(f"{minutes}min").last()
    r = pd.concat([o,h,l,c], axis=1).dropna()
    r.columns = ["Open","High","Low","Close"]
    return r.reset_index(drop=True)

def swing_high(df: pd.DataFrame, look=20) -> float:
    i = len(df) - 2
    L = max(0, i - look + 1)
    return float(df["High"].iloc[L:i+1].max())

def swing_low(df: pd.DataFrame, look=20) -> float:
    i = len(df) - 2
    L = max(0, i - look + 1)
    return float(df["Low"].iloc[L:i+1].min())

def format_signal(side, entry, tp, sl, tf="1m", tag="FLOW"):
    rr = abs(tp-entry)/max(abs(entry-sl), 1e-9)
    return (f"🔥 {side} {SYMBOL_NAME} | {tf} ({tag})\n"
            f"✅ TP: **{tp:.2f}**\n"
            f"🟥 SL: **{sl:.2f}**\n"
            f"Entry: {entry:.2f}  RR≈{rr:.2f}")

last_sent_idx = -1

# ====== CORE (каждая закрытая M1) ======
async def engine():
    global last_sent_idx
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                df = await get_m1(s)
                if df.empty or len(df) < 60:
                    await asyncio.sleep(POLL_SEC); continue

                cur = len(df) - 1
                i   = cur - 1  # закрытая M1
                if i <= last_sent_idx:
                    await asyncio.sleep(POLL_SEC); continue

                O = float(df["Open"].iloc[i]); C = float(df["Close"].iloc[i])
                H = float(df["High"].iloc[i]); L = float(df["Low"].iloc[i])
                rng = H - L; body = abs(C - O)

                # мини-фильтр «не пшик»
                if (rng < MIN_RANGE) or (body < MIN_BODY):
                    last_sent_idx = i
                    await asyncio.sleep(POLL_SEC); continue

                # Направление по М5 (цвет последней закрытой М5)
                df5 = resample(df, 5)
                if df5.empty or len(df5) < 3:
                    last_sent_idx = i
                    await asyncio.sleep(POLL_SEC); continue
                O5 = float(df5["Open"].iloc[-2]); C5 = float(df5["Close"].iloc[-2])
                m5_up = C5 >= O5

                side = "BUY" if m5_up else "SELL"
                # если свеча M1 идёт ПРОТИВ М5 — ждём следующую (чтобы не лезть в контртренд)
                if (side == "BUY" and C < O) or (side == "SELL" and C > O):
                    last_sent_idx = i
                    await asyncio.sleep(POLL_SEC); continue

                entry = C

                # SL: за экстремумом сигнальной свечи ИЛИ свингом М5 (что дальше)
                if side == "BUY":
                    sl_bar = L - SL_BUFER
                    sl_swg = swing_low(df5, 20) - SL_BUFER
                    sl = round(min(sl_bar, sl_swg), 2)
                    if entry - sl < SL_MIN: sl = round(entry - SL_MIN, 2)
                else:
                    sl_bar = H + SL_BUFER
                    sl_swg = swing_high(df5, 20) + SL_BUFER
                    sl = round(max(sl_bar, sl_swg), 2)
                    if sl - entry < SL_MIN: sl = round(entry + SL_MIN, 2)

                risk = abs(entry - sl) or 1.0

                # TP кандидат 1: RR≈1.35*risk
                tp_rr = entry + 1.35*risk if side=="BUY" else entry - 1.35*risk

                # TP кандидат 2: ближайший свинг по М5
                if side == "BUY":
                    swg = swing_high(df5, 20)
                    tp_lvl = swg if swg > entry else entry + TP_MIN
                else:
                    swg = swing_low(df5, 20)
                    tp_lvl = swg if swg < entry else entry - TP_MIN

                # финальный TP: берём консервативней и ограничиваем 4..30 пп
                tp_raw = (tp_rr + tp_lvl)/2.0
                if side == "BUY":
                    tp = max(entry + TP_MIN, min(tp_raw, entry + TP_CAP))
                else:
                    tp = min(entry - TP_MIN, max(tp_raw, entry - TP_CAP))

                text = format_signal(side, entry, round(tp,2), round(sl,2), tf="1m", tag="FLOW")
                await send(text)

                last_sent_idx = i
                await asyncio.sleep(POLL_SEC)

            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(2.0)

# ====== MAIN ======
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine())
    await dp.start_polling(bot_main)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass



