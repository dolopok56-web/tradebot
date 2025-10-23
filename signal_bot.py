#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NG SIGNAL BOT — IMPULSE+TREND (NO XTB API)
- Источник цен: Yahoo Finance (NG=F), 1m, с ретраями
- Сдвиг под брокера: /ref 4.040 (или "реф 4.040") -> сохраняется в config.json
- Сигналы: 1) импульс + подтверждение, 2) тренд-подтягивание, 3) мягкий ассист
- TP/SL: TP 0.015–0.040, SL за ближайший свинг 5m (минимум ~0.008)
- Частотный лимит: по умолчанию 8/день, пауза 5 минут между сигналами
- Команды: /start, статус, газ, лимит N, турбо вкл/выкл, реф X.XXX, тест
"""

import os, json, time, math, random, asyncio, logging
from datetime import datetime, date
import aiohttp, pandas as pd

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ------------- CONFIG / STATE -------------
VERSION = "NG-IMPULSE v1.0 (Yahoo 1m, TP15-40, RR~1.4-1.8)"
SYMBOL = "NG"             # внутр. имя
YAHOO_TICKER = "NG=F"     # источник
SPREAD_BUF = 0.0040       # буфер к TP/SL
TP_MIN = 0.0150
TP_MAX = 0.0400
SL_MIN = 0.0080
RR_TARGET = 1.5

# Частота
DAY_LIMIT_DEFAULT = 8
COOLDOWN_MIN = 5*60

# Турбо-фильтры (чтобы не молчал)
IMP_LOOK_MIN = 5          # минут смотреть дельту
IMP_MOVE_MIN = 0.0100     # импульс за окно
BODY_MIN = 0.0060         # тело последней свечи 1m
ATR1_MIN_ASSIST = 0.0040  # ассист включается когда хоть как-то шевелится

CONFIG_FILE = "config.json"

# ------------- TOKENS -------------
MAIN_BOT_TOKEN = os.getenv("7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs", "")
TARGET_CHAT_ID = int(os.getenv("6784470762", "0"))

if not MAIN_BOT_TOKEN or TARGET_CHAT_ID == 0:
    raise SystemExit("⚠️ Укажи MAIN_BOT_TOKEN и TARGET_CHAT_ID в переменных окружения.")

# ------------- TELEGRAM -------------
router = Router()
bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

async def say(text: str):
    try: await bot.send_message(TARGET_CHAT_ID, text)
    except Exception as e: logging.error(f"send err: {e}")

# ------------- PERSIST CONFIG -------------
def load_config():
    if os.path.exists(CONFIG_FILE):
        try: return json.load(open(CONFIG_FILE,"r",encoding="utf-8"))
        except: pass
    return {
        "broker_offset": 0.0,
        "day_limit": DAY_LIMIT_DEFAULT,
        "turbo": True,
        "signals_today": 0,
        "day": date.today().isoformat(),
        "last_signal_ts": 0.0
    }

def save_config(cfg: dict):
    json.dump(cfg, open(CONFIG_FILE,"w",encoding="utf-8"), ensure_ascii=False, indent=2)

CFG = load_config()

def reset_day_if_needed():
    d = date.today().isoformat()
    if CFG.get("day") != d:
        CFG["day"] = d
        CFG["signals_today"] = 0
        save_config(CFG)

# ------------- PRICE FEED -------------
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/119.0 Safari/537.36)")

async def yahoo_json(session: aiohttp.ClientSession, url: str, retries=4) -> dict:
    back = 0.8
    for _ in range(retries):
        try:
            async with session.get(url, headers={"User-Agent":UA}, timeout=15) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429,503):
                    await asyncio.sleep(back + random.random()*0.3); back *= 1.6
                else:
                    await asyncio.sleep(0.5)
        except:
            await asyncio.sleep(back + random.random()*0.3); back *= 1.5
    return {}

def df_from_yahoo(payload: dict) -> pd.DataFrame:
    try:
        r = payload.get("chart",{}).get("result",[{}])[0]
        ts = r.get("timestamp",[])
        q  = r.get("indicators",{}).get("quote",[{}])[0]
        if not ts or not q: return pd.DataFrame()
        df = pd.DataFrame({
            "Open":  q.get("open",[]),
            "High":  q.get("high",[]),
            "Low":   q.get("low",[]),
            "Close": q.get("close",[]),
        })
        df = df.dropna().reset_index(drop=True)
        for col in ("Open","High","Low","Close"):
            df = df[df[col] > 0]
        return df.tail(1200).reset_index(drop=True)
    except:
        return pd.DataFrame()

async def get_df(session) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{YAHOO_TICKER}?interval=1m&range=5d"
    return df_from_yahoo(await yahoo_json(session, url))

# ------------- TECH -------------
def resample(df: pd.DataFrame, minutes:int) -> pd.DataFrame:
    if df.empty: return df
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

def swing_high(df, look=20):
    i = len(df)-2; L = max(0, i-look+1); 
    return float(df["High"].iloc[L:i+1].max())

def swing_low(df, look=20):
    i = len(df)-2; L = max(0, i-look+1); 
    return float(df["Low"].iloc[L:i+1].min())

def atr1m(df: pd.DataFrame, period=14) -> float:
    if df is None or df.empty or len(df) < period+2: return 0.0
    H = df["High"].values; L = df["Low"].values; C = df["Close"].values
    trs = []
    for i in range(1,len(C)):
        tr = max(H[i]-L[i], abs(H[i]-C[i-1]), abs(L[i]-C[i-1]))
        trs.append(tr)
    return float(sum(trs[-period:]) / period) if len(trs)>=period else 0.0

# ------------- SIGNAL LOGIC -------------
def fmt(x: float) -> str:
    return f"{x:.4f}"

def format_signal(setup: dict) -> str:
    rr = setup.get("rr",0.0)
    return (
        f"🔥 {setup['side']} NATGAS (NG=F) | 1m {setup['kind']}\n"
        f"✅ TP: **{fmt(setup['tp'])}**\n"
        f"🟥 SL: **{fmt(setup['sl'])}**\n"
        f"Entry: {fmt(setup['entry'])}  Spread≈{fmt(SPREAD_BUF)}  RR≈{rr:.2f}"
    )

def build_setup_impulse(df1m: pd.DataFrame) -> dict|None:
    if df1m.empty or len(df1m) < max(IMP_LOOK_MIN,20)+3: return None
    # импульс за окно
    c_now = float(df1m["Close"].iloc[-1])
    c_was = float(df1m["Close"].iloc[-(IMP_LOOK_MIN+1)])
    delta = c_now - c_was
    if abs(delta) < IMP_MOVE_MIN: return None

    # последняя свеча должна быть "реальной"
    i = len(df1m)-1
    body = abs(df1m["Close"].iloc[i] - df1m["Open"].iloc[i])
    if body < BODY_MIN: return None

    side = "BUY" if delta>0 else "SELL"

    # SL за свинг 5m
    df5 = resample(df1m, 5)
    entry = c_now + CFG.get("broker_offset",0.0)
    if side=="BUY":
        sl = min(entry-1e-6, swing_low(df5, 20) - SPREAD_BUF)
        risk = max(SL_MIN, entry - sl)
        tp  = min(entry + max(TP_MIN, RR_TARGET*risk), entry + TP_MAX)
    else:
        sl = max(entry+1e-6, swing_high(df5, 20) + SPREAD_BUF)
        risk = max(SL_MIN, sl - entry)
        tp  = max(entry - max(TP_MIN, RR_TARGET*risk), entry - TP_MAX)

    rr = abs(tp-entry)/max(abs(entry-sl),1e-9)
    return {"kind":"(Impulse)", "side":side, "entry":entry, "tp":tp, "sl":sl, "rr":rr}

def build_setup_trend(df1m: pd.DataFrame) -> dict|None:
    if df1m.empty or len(df1m) < 40: return None
    df5 = resample(df1m,5)
    if df5.empty or len(df5)<25: return None

    # простая тенденция: close выше/ниже SMA20 на 5m и 10m-дельта того же знака
    sma = df5["Close"].rolling(20).mean().iloc[-2]
    c5  = float(df5["Close"].iloc[-2])
    c10 = float(df1m["Close"].iloc[-11])
    c1  = float(df1m["Close"].iloc[-1])
    bias_up = (c5 > sma) and (c1 - c10 > 0.0)
    bias_dn = (c5 < sma) and (c1 - c10 < 0.0)
    if not (bias_up or bias_dn): return None

    side = "BUY" if bias_up else "SELL"
    entry = c1 + CFG.get("broker_offset",0.0)

    if side=="BUY":
        sl = min(entry-1e-6, swing_low(df5, 15) - SPREAD_BUF)
        risk = max(SL_MIN, entry-sl)
        tp  = min(entry + max(TP_MIN, RR_TARGET*risk), entry + TP_MAX)
    else:
        sl = max(entry+1e-6, swing_high(df5,15) + SPREAD_BUF)
        risk = max(SL_MIN, sl-entry)
        tp  = max(entry - max(TP_MIN, RR_TARGET*risk), entry - TP_MAX)

    rr = abs(tp-entry)/max(abs(entry-sl),1e-9)
    return {"kind":"(Trend)", "side":side, "entry":entry, "tp":tp, "sl":sl, "rr":rr}

def build_setup_assist(df1m: pd.DataFrame) -> dict|None:
    if df1m.empty or len(df1m)<25: return None
    if atr1m(df1m,14) < ATR1_MIN_ASSIST: return None
    # простое «вблизи пробоя» по 1m
    i = len(df1m)-1
    H = float(df1m["High"].iloc[i]); L = float(df1m["Low"].iloc[i])
    O = float(df1m["Open"].iloc[i]); C = float(df1m["Close"].iloc[i])
    entry = C + CFG.get("broker_offset",0.0)

    if (H - C) <= (SPREAD_BUF*1.5) and C >= O:   # возле хая, зелёная
        side = "BUY"
        sl = min(entry-1e-6, entry - max(SL_MIN, 0.010))
        tp = entry + max(TP_MIN, min(TP_MAX, 0.020 + SPREAD_BUF))
    elif (C - L) <= (SPREAD_BUF*1.5) and C <= O: # возле лоу, красная
        side = "SELL"
        sl = max(entry+1e-6, entry + max(SL_MIN, 0.010))
        tp = entry - max(TP_MIN, min(TP_MAX, 0.020 + SPREAD_BUF))
    else:
        return None

    rr = abs(tp-entry)/max(abs(entry-sl),1e-9)
    return {"kind":"(Assist)", "side":side, "entry":entry, "tp":tp, "sl":sl, "rr":rr}

# ------------- ENGINE -------------
async def engine():
    await say(f"✅ Стартую: {VERSION}\n"
              f"Лимит сегодня: {CFG.get('day_limit',DAY_LIMIT_DEFAULT)}  | Турбо: {'ON' if CFG.get('turbo',True) else 'OFF'}")
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                reset_day_if_needed()
                # кулдаун между сигналами
                if time.time() - float(CFG.get("last_signal_ts",0)) < COOLDOWN_MIN:
                    await asyncio.sleep(2); continue
                if CFG.get("signals_today",0) >= CFG.get("day_limit",DAY_LIMIT_DEFAULT):
                    await asyncio.sleep(5); continue

                df = await get_df(session)
                if df.empty or len(df)<40:
                    await asyncio.sleep(1.2); continue

                setup = None
                # 1) Импульс
                if CFG.get("turbo",True):
                    setup = build_setup_impulse(df)
                # 2) Тренд
                if setup is None:
                    setup = build_setup_trend(df)
                # 3) Ассист
                if setup is None and CFG.get("turbo",True):
                    setup = build_setup_assist(df)

                if setup:
                    CFG["signals_today"] = int(CFG.get("signals_today",0)) + 1
                    CFG["last_signal_ts"] = time.time()
                    save_config(CFG)
                    await say(format_signal(setup))

                await asyncio.sleep(0.6 if CFG.get("turbo",True) else 1.0)

            except Exception as e:
                logging.exception(e)
                await asyncio.sleep(2.0)

# ------------- COMMANDS -------------
@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Бот жив. {VERSION}\n"
                   "Команды: статус | газ | лимит 5 | турбо вкл/выкл | /ref 4.040 | тест")

@router.message(F.text.lower().in_({"статус","status"}))
async def cmd_status(m: Message):
    reset_day_if_needed()
    await m.answer(
        "```\n"
        f"mode: NG (Impulse)\n"
        f"turbo: {'ON' if CFG.get('turbo',True) else 'OFF'}\n"
        f"day: {CFG.get('day')}  signals: {CFG.get('signals_today',0)}/{CFG.get('day_limit',DAY_LIMIT_DEFAULT)}\n"
        f"cooldown_left: {max(0,int(COOLDOWN_MIN - (time.time()-float(CFG.get('last_signal_ts',0))))) }s\n"
        f"broker_offset: {CFG.get('broker_offset',0.0):.4f}\n"
        "```"
    )

@router.message(F.text.lower().in_({"газ","ng","натгаз"}))
async def cmd_ng(m: Message):
    await m.answer("✅ Режим: NATGAS (NG=F) — активен.")

@router.message(F.text.lower().regexp(r"^(лимит|limit)\s+(\d+)$"))
async def cmd_limit(m: Message):
    try:
        n = int(m.text.split()[-1]); n = max(1, min(20, n))
        CFG["day_limit"] = n; save_config(CFG)
        await m.answer(f"✅ Лимит сигналов на сегодня: {n}")
    except:
        await m.answer("Формат: лимит 5")

@router.message(F.text.lower().in_({"турбо вкл","turbo on"}))
async def turbo_on(m: Message):
    CFG["turbo"] = True; save_config(CFG)
    await m.answer("⚡ Турбо: ON")

@router.message(F.text.lower().in_({"турбо выкл","turbo off"}))
async def turbo_off(m: Message):
    CFG["turbo"] = False; save_config(CFG)
    await m.answer("⛔ Турбо: OFF")

@router.message(F.text.lower().regexp(r"^(/?ref|реф|смещение)\s+([0-9\.,]+)$"))
async def cmd_ref(m: Message):
    try:
        # пользователь присылает цену с платформы, мы считаем offset = broker - yahoo
        val = m.text.split()[-1].replace(",",".")
        broker_px = float(val)
        async with aiohttp.ClientSession() as s:
            df = await get_df(s)
        if df.empty: return await m.answer("Не получил цену с Yahoo. Попробуй позже.")
        yahoo_px = float(df["Close"].iloc[-1])
        CFG["broker_offset"] = broker_px - yahoo_px
        save_config(CFG)
        await m.answer(f"✅ Смещение сохранено: {CFG['broker_offset']:+.4f} (broker {broker_px:.4f} vs yahoo {yahoo_px:.4f})")
    except Exception as e:
        await m.answer(f"Не смог разобрать число. Пример: /ref 4.040")

@router.message(F.text.lower().in_({"тест","/test"}))
async def cmd_test(m: Message):
    sample = {"kind":"(Test)", "side":"BUY", "entry":4.0000, "tp":4.0200, "sl":3.9880, "rr":1.67}
    await m.answer(format_signal(sample))

# ------------- MAIN -------------
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
