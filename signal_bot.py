#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GranVex NG — Stable Impulse v1.0
— только NATGAS (NG=F), поток Yahoo (1m)
— сигнал = импульс за 3–7 мин + направленный контекст
— TP/SL динамика: 1.5R (минимум 0.015, максимум 0.040)
— анти-спам: окна, лимиты, охлаждение
— только СИГНАЛЫ в Telegram (никаких "рефов" и лишних режимов)
"""

import os, time, asyncio, random, logging, math
from datetime import datetime, timedelta

import pandas as pd
import aiohttp
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message

# ================== BASIC CONFIG ==================
VERSION = "GranVex NG — Stable Impulse v1.0"

# set these 2 (или через переменные окружения)
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "<7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs>")
TARGET_CHAT_ID = int(os.getenv("6784470762", "0"))   # твой Telegram ID (или чат)

SYMBOL = "NG"                 # единственный рынок
Y_SYMBOL = "NG=F"            # тикер Yahoo
TF = "1m"

# небольшая корректировка разницы фьюч/CFD, если нужно (можно оставить 0.0)
OFFSET = float(os.getenv("NG_OFFSET", "0.0000"))

# частота опроса и “сердцебиение”
POLL_SEC        = 0.35
ALIVE_EVERY_SEC = 300

# лимиты сигналов
DAILY_MAX_SIGNALS = 8
MIN_GAP_MIN       = 4          # пауза между сигналами (мин)

# буфер спреда для округлений (чтобы не попадать в bid/ask)
SPREAD_BUF = 0.0040

# ============ IMPULSE RULES (мягкие, без ATR-задушки) ============
IMP_LOOK_MIN   = 3     # минимум минут для импульса
IMP_LOOK_MAX   = 7     # максимум минут для импульса
IMP_MOVE_MIN   = 0.010 # 10 пипсов — этого достаточно, не жадничаем

# дополнительный фильтр “контекста” (чтобы не брать в стену):
SWING_LKB_5M   = 20    # свинг на 5m для SL
RR_TARGET      = 1.5   # целевой RR
TP_MIN_ABS     = 0.015
TP_MAX_ABS     = 0.040
RISK_MIN       = 0.006 # чтобы SL не был микроскопическим
RISK_MAX       = 0.030 # и не был конским

# fallback, если долго тишина — разрешим сигнал при чуть меньшем импульсе
QUIET_MINUTES_FOR_FALLBACK = 20
FALLBACK_MOVE_MIN          = 0.008  # 8 пипсов через 20 мин молчания

# ================== TELEGRAM ==================
router = Router()
bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

async def say(text: str):
    if TARGET_CHAT_ID == 0: return
    try:
        await bot.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.error(f"telegram send error: {e}")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ {VERSION}\nРаботаю по {Y_SYMBOL} (Yahoo 1m).")
    await m.answer("Команды: статус, стоп, тест, оффсет <число>")

@router.message(lambda m: m.text and m.text.lower().startswith("оффсет"))
async def cmd_offset(m: Message):
    global OFFSET
    try:
        parts = m.text.split(maxsplit=1)
        if len(parts) == 2:
            OFFSET = float(parts[1].replace(",", "."))
            await m.answer(f"OFFSET обновлён: {OFFSET:+.4f}")
        else:
            await m.answer(f"Текущий OFFSET: {OFFSET:+.4f}")
    except:
        await m.answer("Формат: оффсет 0.0123")

@router.message(lambda m: m.text and m.text.lower() == "стоп")
async def cmd_stop(m: Message):
    state["cooldown_until"] = time.time() + 5
    await m.answer("🛑 Остановил на 5 сек. Открытых нет (мы только шлём сигналы).")

@router.message(lambda m: m.text and m.text.lower() == "статус")
async def cmd_status(m: Message):
    s = state
    last_age = int(time.time() - s["last_close_ts"]) if s["last_close_ts"] else -1
    last_sig_age = int(time.time() - s["last_signal_ts"]) if s["last_signal_ts"] else -1
    lines = [
        f"mode: NG-only ({Y_SYMBOL})",
        f"alive: OK | poll={POLL_SEC}s",
        f"signals_today: {s['signals_today']}/{DAILY_MAX_SIGNALS}",
        f"last_close_age={last_age}s last_signal_age={last_sig_age}s",
        f"cooldown_left={max(0,int(s['cooldown_until']-time.time()))}s",
        f"OFFSET={OFFSET:+.4f}",
    ]
    await m.answer("```\n" + "\n".join(lines) + "\n```")

@router.message(lambda m: m.text and m.text.lower() == "тест")
async def cmd_test(m: Message):
    await m.answer("🔥 BUY NATGAS (NG=F) | 1m\n✅ TP: **4.1234**\n🟥 SL: **4.1111**\nEntry: 4.1155  RR≈1.5  (DEMO)")

# ================== DATA FEED (Yahoo v8) ==================
ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
    "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9", "Connection": "keep-alive",
}
HTTP_TIMEOUT   = 12
Y_RETRIES      = 5
Y_BACKOFF0     = 0.8
Y_JITTER       = 0.35

_prices_cache = {"ts": 0.0, "df": pd.DataFrame()}

async def _yjson(session: aiohttp.ClientSession, url: str) -> dict:
    backoff = Y_BACKOFF0
    for _ in range(Y_RETRIES):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429, 503):
                    await asyncio.sleep(backoff + random.random()*Y_JITTER)
                    backoff *= 1.7
                    continue
                return {}
        except:
            await asyncio.sleep(backoff + random.random()*Y_JITTER)
            backoff *= 1.6
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

async def get_df(session: aiohttp.ClientSession) -> pd.DataFrame:
    now = time.time()
    if now - _prices_cache["ts"] < 0.35 and not _prices_cache["df"].empty:
        return _prices_cache["df"]
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{Y_SYMBOL}?interval=1m&range=5d"
    payload = await _yjson(session, url)
    df = _df_from_yahoo(payload)
    if not df.empty:
        _prices_cache["ts"] = now
        _prices_cache["df"] = df
    return df

# ================== TOOLS ==================
def rnd(x: float) -> float:
    return round(float(x), 4)

def resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
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

def swing_high(df: pd.DataFrame, lookback: int = 20) -> float:
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["High"].iloc[L:i+1].max())

def swing_low(df: pd.DataFrame, lookback: int = 20) -> float:
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["Low"].iloc[L:i+1].min())

# ================== STATE ==================
state = {
    "last_close_ts": 0.0,
    "last_signal_ts": 0.0,
    "signals_today": 0,
    "day": datetime.utcnow().date().isoformat(),
    "cooldown_until": 0.0,
}

def reset_if_new_day():
    d = datetime.utcnow().date().isoformat()
    if state["day"] != d:
        state["day"] = d
        state["signals_today"] = 0
        state["last_signal_ts"] = 0.0

# ================== SIGNAL LOGIC ==================
def build_signal(df1m: pd.DataFrame) -> dict | None:
    """Импульс за 3–7 минут + SL по свингу 5m, TP≈1.5R, рамки 0.015..0.040"""

    if df1m is None or df1m.empty or len(df1m) < 60:
        return None

    # 1) ищем импульс
    last_close = float(df1m["Close"].iloc[-1])
    chosen_look = None
    delta = 0.0
    now_ts = time.time()

    # если давно тишина — разрешаем чуть меньший порог
    quiet = (now_ts - (state["last_signal_ts"] or 0)) >= QUIET_MINUTES_FOR_FALLBACK*60
    min_move = FALLBACK_MOVE_MIN if quiet else IMP_MOVE_MIN

    for look in range(IMP_LOOK_MIN, IMP_LOOK_MAX+1):
        c_look = float(df1m["Close"].iloc[-(look+1)])
        d = last_close - c_look
        if abs(d) >= min_move:
            chosen_look = look
            delta = d
            break

    if chosen_look is None:
        return None

    side = "BUY" if delta > 0 else "SELL"
    entry = last_close + OFFSET   # при необходимости можно подровнять CFD

    # 2) строим 5m для свинга
    df5 = resample(df1m, 5)
    if df5.empty or len(df5) < SWING_LKB_5M + 5:
        return None

    if side == "BUY":
        sl = min(entry - 1e-6, swing_low(df5, SWING_LKB_5M) - SPREAD_BUF)
    else:
        sl = max(entry + 1e-6, swing_high(df5, SWING_LKB_5M) + SPREAD_BUF)

    risk = abs(entry - sl)
    if risk < RISK_MIN or risk > RISK_MAX:
        return None

    # 3) TP ≈ 1.5R в рамках 0.015..0.040
    if side == "BUY":
        tp = entry + max(TP_MIN_ABS, min(TP_MAX_ABS, RR_TARGET * risk))
    else:
        tp = entry - max(TP_MIN_ABS, min(TP_MAX_ABS, RR_TARGET * risk))

    rr = abs(tp - entry) / max(risk, 1e-9)
    if abs(tp - entry) < TP_MIN_ABS:
        return None

    return {
        "side": side, "entry": entry, "tp": tp, "sl": sl,
        "rr": rr, "kind": f"IMP{chosen_look}m",
    }

def fmt_signal(s: dict) -> str:
    side = s["side"]
    return (
        f"🔥 {side} NATGAS (NG=F) | {TF}\n"
        f"✅ TP: **{rnd(s['tp'])}**\n"
        f"🟥 SL: **{rnd(s['sl'])}**\n"
        f"Entry: {rnd(s['entry'])}  RR≈{round(s['rr'],2)}  "
        f"Buf≈{SPREAD_BUF}  {s['kind']}"
    )

# ================== ENGINE ==================
async def engine_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                reset_if_new_day()

                if time.time() < state["cooldown_until"]:
                    await asyncio.sleep(POLL_SEC); continue

                df = await get_df(session)
                if df.empty or len(df) < 80:
                    await asyncio.sleep(POLL_SEC); continue

                # закрылась новая свеча?
                state["last_close_ts"] = time.time()

                # лимит на день / пауза между сигналами
                if state["signals_today"] >= DAILY_MAX_SIGNALS:
                    await asyncio.sleep(POLL_SEC); continue
                if (time.time() - (state["last_signal_ts"] or 0)) < MIN_GAP_MIN*60:
                    await asyncio.sleep(POLL_SEC); continue

                sig = build_signal(df)
                if not sig:
                    await asyncio.sleep(POLL_SEC); continue

                # отправляем
                await say(fmt_signal(sig))
                state["signals_today"] += 1
                state["last_signal_ts"] = time.time()
                state["cooldown_until"] = time.time() + 2.0  # небольшой кулдаун от дублей

                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(1.2)

async def alive_loop():
    while True:
        try:
            msg = (f"[ALIVE] {VERSION}\n"
                   f"signals_today: {state['signals_today']}/{DAILY_MAX_SIGNALS}  "
                   f"cooldown_left={max(0,int(state['cooldown_until']-time.time()))}s  "
                   f"OFFSET={OFFSET:+.4f}")
            await say(msg)
        except Exception as e:
            logging.error(f"alive error: {e}")
        await asyncio.sleep(ALIVE_EVERY_SEC)

# ================== MAIN ==================
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine_loop())
    asyncio.create_task(alive_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
