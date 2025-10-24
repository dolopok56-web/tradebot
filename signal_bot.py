#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =================== XAU M1 SIGNAL BOT (LOUD) ===================
# Частые сигналы по золоту XAU (GC=F / XAUUSD=X, 1m Yahoo)
# Без ATR/ RR/ мудрёных фильтров. Простая логика:
# - работа по закрытой свече М1
# - свеча не должна быть "пылью": диапазон >= MIN_RANGE_PIPS и тело >= MIN_BODY_PIPS
# - направление: BREAKOUT (закрытие у экстремума свечи) или PULLBACK по короткому наклону
# - TP динамический: max(свеча*коэф, TP_MIN_PIPS), но не выше TP_CAP_PIPS
# - SL короткий за хвост/локальный экстремум
# - анти-спам: не повторяем тот же сайд два раза подряд на одной и той же цене

import os
import time
import math
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
import pandas as pd

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# =================== SETTINGS ===================

VERSION = "XAU-M1 Loud v1.0"

# Токены/чаты (можно переопределить через ENV)
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# Символы и фиды
SYMBOL  = "XAU"                    # логическое имя
NAME    = "GOLD (XAU)"             # подпись в сообщениях
TICKERS = ["GC=F", "XAUUSD=X"]     # порядок опроса на Yahoo, берём первый, где есть данные

# Частота и режим
POLL_SEC        = 0.30             # опрос ленты
PER_BAR_MODE    = True             # работать по закрытию каждой М1, если свеча нормальная
COOLDOWN_BARS   = 0                # 0 = можно каждый бар; 2 = раз в ~3 минуты
MAX_SIGNALS_DAY = 50               # страховка на день

# Порог «не пыль»
MIN_RANGE_PIPS  = 3.0              # минимальный High-Low у свечи, чтобы говорить
MIN_BODY_PIPS   = 1.0              # минимальное тело |Close-Open|

# TP/SL
TP_MIN_PIPS     = 4.0              # минимум, как просил
TP_CAP_PIPS     = 30.0             # не ставим космос
TP_COEF         = 1.4              # TP = max(range*коэф, TP_MIN), но ≤ TP_CAP
SL_TAIL_PAD     = 0.8              # SL за тенью на ~0.8 пункта
SL_MIN_PIPS     = 5.0              # нижняя планка для SL (чуть шире, чтобы не сдувало)
SL_MAX_PIPS     = 18.0             # верхняя (страховка)

# Мелочи
HTTP_TIMEOUT    = 12
ROBUST_HEADERS  = {"User-Agent": "Mozilla/5.0"}
LOG_EVERY_SEC   = 300

# =================== TELEGRAM ===================

router    = Router()
bot_main  = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp        = Dispatcher()
dp.include_router(router)

async def send(text: str):
    try:
        await bot_main.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.error(f"send error: {e}")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ {VERSION}\nРежим: {NAME} M1.\nНапиши: статус / золото")

@router.message(F.text.casefold() == "золото")
async def cmd_gold(m: Message):
    await m.answer("✅ Режим уже: GOLD (XAU) M1.")

@router.message(F.text.casefold() == "статус")
async def cmd_status(m: Message):
    s = state
    lines = [
        f"mode: XAU (requested: XAU)",
        f"alive: OK | poll={POLL_SEC}s",
        f"cooldown_bars={COOLDOWN_BARS} last_close_age={int(time.time()-s.get('last_close_ts',0))}s",
        f"signals_today={s.get('signals_today',0)} (limit={MAX_SIGNALS_DAY})",
        f"last_side={s.get('last_side','-')} last_price={s.get('last_price','-')}",
        f"tp_min={TP_MIN_PIPS} cap={TP_CAP_PIPS} min_range={MIN_RANGE_PIPS} min_body={MIN_BODY_PIPS}",
    ]
    await m.answer("`\n" + "\n".join(lines) + "\n`")

# =================== PRICE FEED ===================

_prices_cache = {"df": None, "ts": 0.0, "src": ""}

async def yahoo_json(session: aiohttp.ClientSession, url: str) -> dict:
    try:
        async with session.get(url, timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
            if r.status == 200:
                return await r.json(content_type=None)
    except:
        pass
    return {}

def df_from_yahoo(payload: dict) -> pd.DataFrame:
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
        for c in ("Open","High","Low","Close"):
            df = df[df[c] > 0]
        return df.tail(2000).reset_index(drop=True)
    except:
        return pd.DataFrame()

async def get_df(session: aiohttp.ClientSession) -> pd.DataFrame:
    now = time.time()
    if _prices_cache["df"] is not None and now - _prices_cache["ts"] < 0.5:
        return _prices_cache["df"]

    # пробуем тики по тикерам по очереди
    for t in TICKERS:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d"
        df = df_from_yahoo(await yahoo_json(session, url))
        if not df.empty:
            _prices_cache.update({"df": df, "ts": now, "src": t})
            return df
    return pd.DataFrame()

# =================== LOGIC ===================

state = {
    "last_bar_idx": -1,
    "last_close_ts": 0.0,
    "last_signal_bar": -999,
    "last_side": None,
    "last_price": None,
    "signals_today": 0,
    "day": None,
}

def reset_day_if_needed():
    d = datetime.utcnow().date().isoformat()
    if state["day"] != d:
        state["day"] = d
        state["signals_today"] = 0

def pips(x: float) -> float:
    return float(x)

def fmt(x: float) -> str:
    return f"{x:.2f}"

def candle_signal(o: float, h: float, l: float, c: float, prev_closes: list) -> dict|None:
    rng  = h - l
    body = abs(c - o)
    if rng < MIN_RANGE_PIPS or body < MIN_BODY_PIPS:
        return None

    # breakout, если закрытие рядом с экстремумом свечи
    near_top = (h - c) <= 0.6
    near_bot = (c - l) <= 0.6

    # простой наклон: последние 8 закрытий
    slope = 0.0
    if len(prev_closes) >= 8:
        slope = prev_closes[-1] - prev_closes[-8]

    side = None
    reason = ""
    if near_top or slope > 0:
        side = "BUY"; reason = "BREAKOUT" if near_top else "PULLBACK-UP"
    if near_bot or slope < 0:
        # если оба сработали — берём что ближе к экстремуму
        if side is None:
            side = "SELL"; reason = "BREAKOUT" if near_bot else "PULLBACK-DOWN"
        else:
            # выбрать более очевидный: сравним расстояние до экстремума
            dist_top = h - c
            dist_bot = c - l
            if dist_bot < dist_top:
                side = "SELL"; reason = "BREAKOUT" if near_bot else "PULLBACK-DOWN"

    if side is None:
        return None

    # entry — закрытие бара
    entry = c

    # TP динамический
    raw_tp = max(rng * TP_COEF, TP_MIN_PIPS)
    raw_tp = min(raw_tp, TP_CAP_PIPS)

    if side == "BUY":
        tp = entry + raw_tp
        # SL — за тенью или минимумом бара
        sl = min(entry - SL_MIN_PIPS, l - SL_TAIL_PAD)
        sl = max(entry - SL_MAX_PIPS, sl)  # ограничить глубину
    else:
        tp = entry - raw_tp
        sl = max(entry + SL_MIN_PIPS, h + SL_TAIL_PAD)
        sl = min(entry + SL_MAX_PIPS, sl)

    if abs(tp - entry) < TP_MIN_PIPS:
        return None

    return {
        "side": side,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "rng": rng,
        "body": body,
        "reason": reason,
    }

def text_signal(sig: dict) -> str:
    side = "BUY" if sig["side"] == "BUY" else "SELL"
    rr   = abs(sig["tp"] - sig["entry"]) / max(abs(sig["entry"] - sig["sl"]), 1e-9)
    return (
        f"🔥 {side} {NAME} | M1 ({sig['reason']})\n"
        f"Entry: {fmt(sig['entry'])}\n"
        f"✅ TP: **{fmt(sig['tp'])}**\n"
        f"🟥 SL: **{fmt(sig['sl'])}**\n"
        f"rng={fmt(sig['rng'])} body={fmt(sig['body'])}  RR≈{rr:.2f}"
    )

# =================== ENGINE ===================

async def engine_loop():
    last_log = 0.0
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                reset_day_if_needed()
                if state["signals_today"] >= MAX_SIGNALS_DAY:
                    if time.time() - last_log > 60:
                        await send("⚠️ Дневной лимит сигналов достигнут.")
                        last_log = time.time()
                    await asyncio.sleep(POLL_SEC)
                    continue

                df = await get_df(session)
                if df.empty or len(df) < 20:
                    await asyncio.sleep(POLL_SEC)
                    continue

                i = len(df) - 1         # текущий бар формируется
                cbar = i - 1            # закрытый бар
                if cbar <= state["last_bar_idx"]:
                    await asyncio.sleep(POLL_SEC)
                    continue

                o = float(df["Open"].iloc[cbar])
                h = float(df["High"].iloc[cbar])
                l = float(df["Low"].iloc[cbar])
                c = float(df["Close"].iloc[cbar])
                prev_closes = df["Close"].iloc[max(0, cbar-50):cbar].tolist()

                state["last_bar_idx"] = cbar
                state["last_close_ts"] = time.time()

                # анти-спам по барам
                if COOLDOWN_BARS > 0 and (cbar - state["last_signal_bar"] < COOLDOWN_BARS):
                    continue

                sig = candle_signal(o, h, l, c, prev_closes)
                if not sig:
                    # периодический "жив"
                    if time.time() - last_log > LOG_EVERY_SEC:
                        await send(f"[ALIVE] feed={_prices_cache['src']} last={fmt(c)} OK")
                        last_log = time.time()
                    continue

                # не дублировать тот же сайд на почти той же цене
                if state["last_side"] == sig["side"] and state["last_price"] is not None:
                    if abs(sig["entry"] - state["last_price"]) <= 1.0:
                        continue

                # отправка сигнала
                await send(text_signal(sig))

                # обновить состояние
                state["last_signal_bar"] = cbar
                state["signals_today"]  += 1
                state["last_side"]       = sig["side"]
                state["last_price"]      = sig["entry"]

            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(1.0)
            await asyncio.sleep(POLL_SEC)

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine_loop())
    await dp.start_polling(bot_main)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
