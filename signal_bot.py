#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import logging
from dataclasses import dataclass
from time import time

import pandas as pd
import yfinance as yf
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_polling

# ----------------------- ЛОГИ -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("xau-bot")

# ----------------------- КОНФИГ -----------------------
# Токен/чаты: читаем из ENV, но даём дефолты, чтобы работало сразу
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN") or "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "6784470762"))
OWNER_ID       = int(os.getenv("OWNER_ID",       "6784470762"))

# Рынок
SYMBOL_YF   = "XAUUSD=X"   # золото спот в Yahoo
INTERVAL    = "1m"         # минутные свечи
POLL_SEC    = 0.3          # тик цикла
HIST_BARS   = 120          # сколько минутных баров подгружать

# Фильтры/ограничители
SINGLE_TRADE_MODE = True   # только 1 сделка единовременно
TP_MIN_PIPS       = 4.0    # минимум цели
CAP_PIPS          = 30.0   # максимум цели (ограничитель)
MIN_RANGE_PIPS    = 3.0    # минимальный размер свечи (high-low)
MIN_BODY_PIPS     = 1.0    # минимальное тело

# Сигнальная логика (простая и отзывчивая)
BREAK_BODY_PCT    = 0.6    # насколько полным должно быть тело (0..1)
PULLBACK_TRIGGER  = 2.0    # мин. размер отката в пипсах, чтобы взять ретест

# ----------------------- ТЕЛЕГРАМ -----------------------
bot = Bot(MAIN_BOT_TOKEN, parse_mode="HTML")
dp  = Dispatcher(bot)

def pips(a: float, b: float) -> float:
    return abs(a - b)

@dataclass
class Trade:
    side: str         # "BUY"/"SELL"
    entry: float
    tp: float
    sl: float
    created_at: float

active_trade: Trade | None = None
last_close_ts: float = 0.0  # защита от двойной обработки одной и той же свечи

# ----------------------- ДАННЫЕ -----------------------
async def load_history():
    """
    Грузим минутные свечи за сегодня с Yahoo. Возвращаем pd.DataFrame с колонками:
    ['open','high','low','close'] и индексом datetime (UTC).
    """
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, lambda: yf.download(
        tickers=SYMBOL_YF,
        interval=INTERVAL,
        period="1d",
        progress=False,
        auto_adjust=False,
        threads=False
    ))
    if df is None or df.empty:
        raise RuntimeError("Empty data from Yahoo")
    df = df.rename(columns=str.lower)[["open", "high", "low", "close"]]
    return df

# ----------------------- СИГНАЛЫ -----------------------
def detect_signal(df: pd.DataFrame) -> tuple[str, float, float, float, str] | None:
    """
    Простая логика:
      1) Breakout: большая свеча-тренд (полное тело), пробой тела в сторону
      2) Pullback: откат >= PULLBACK_TRIGGER и возврат в сторону тренда
    Возвращает (side, entry, tp, sl, reason) или None.
    """
    if len(df) < 5:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    rng = last["high"] - last["low"]
    body = abs(last["close"] - last["open"])

    if rng < MIN_RANGE_PIPS or body < MIN_BODY_PIPS:
        return None

    body_ok = (body / rng) >= BREAK_BODY_PCT

    # Определяем направление (по телу)
    direction_up   = last["close"] > last["open"]
    direction_down = last["close"] < last["open"]

    entry = float(last["close"])

    # --- Breakout ---
    if body_ok:
        if direction_up:
            # TP/SL на CAP/2 по умолчанию
            tp = min(entry + max(TP_MIN_PIPS, body), entry + CAP_PIPS)
            sl = max(entry - max(MIN_BODY_PIPS, body/2), entry - CAP_PIPS)
            if (tp - entry) >= TP_MIN_PIPS:
                return ("BUY", entry, tp, sl, "BREAKOUT")
        if direction_down:
            tp = max(entry - max(TP_MIN_PIPS, body), entry - CAP_PIPS)
            sl = min(entry + max(MIN_BODY_PIPS, body/2), entry + CAP_PIPS)
            if (entry - tp) >= TP_MIN_PIPS:
                return ("SELL", entry, tp, sl, "BREAKOUT")

    # --- Pullback ---
    # если текущая свеча вернулась внутрь тела предыдущей на PULLBACK_TRIGGER
    prev_mid = (prev["open"] + prev["close"]) / 2
    if direction_up and (prev_mid - last["open"] >= PULLBACK_TRIGGER):
        tp = min(entry + max(TP_MIN_PIPS, rng), entry + CAP_PIPS)
        sl = max(entry - max(MIN_BODY_PIPS, rng/2), entry - CAP_PIPS)
        if (tp - entry) >= TP_MIN_PIPS:
            return ("BUY", entry, tp, sl, "PULLBACK-UP")

    if direction_down and (last["open"] - prev_mid >= PULLBACK_TRIGGER):
        tp = max(entry - max(TP_MIN_PIPS, rng), entry - CAP_PIPS)
        sl = min(entry + max(MIN_BODY_PIPS, rng/2), entry + CAP_PIPS)
        if (entry - tp) >= TP_MIN_PIPS:
            return ("SELL", entry, tp, sl, "PULLBACK-DOWN")

    return None

# ----------------------- ОТПРАВКИ -----------------------
async def send_signal(side: str, entry: float, tp: float, sl: float, reason: str):
    global active_trade
    if SINGLE_TRADE_MODE and active_trade is not None:
        return
    active_trade = Trade(side=side, entry=entry, tp=tp, sl=sl, created_at=time())

    tp_p = pips(tp, entry)
    sl_p = pips(sl, entry)
    emoji = "🟢 BUY" if side == "BUY" else "🔴 SELL"
    text = (
        f"🔥 {emoji} GOLD (XAU) | M1 ({reason})\n"
        f"Entry: <b>{entry:.2f}</b>\n"
        f"✅ TP: <b>{tp:.2f}</b> (+{tp_p:.2f} pips)\n"
        f"🟥 SL: <b>{sl:.2f}</b> (−{sl_p:.2f} pips)\n"
    )
    await bot.send_message(TARGET_CHAT_ID, text)

async def close_deal(kind: str, level: float):
    global active_trade
    if not active_trade:
        return
    t = active_trade
    pnl = pips(level, t.entry)
    await bot.send_message(
        TARGET_CHAT_ID,
        f"✅ DEAL CLOSED: <b>{kind}</b> at <b>{level:.2f}</b> "
        f"(+{pnl:.2f} pips)\nSide: {t.side}, Entry: {t.entry:.2f}"
    )
    active_trade = None

async def check_active_trade_hit(cur_price: float):
    if not active_trade:
        return
    t = active_trade
    if t.side == "BUY":
        if cur_price >= t.tp:
            await close_deal("TP", t.tp)
        elif cur_price <= t.sl:
            await close_deal("SL", t.sl)
    else:  # SELL
        if cur_price <= t.tp:
            await close_deal("TP", t.tp)
        elif cur_price >= t.sl:
            await close_deal("SL", t.sl)

# ----------------------- ОСНОВНОЙ ЦИКЛ -----------------------
async def engine_loop():
    global last_close_ts
    logger.info("Engine loop started")
    while True:
        try:
            df = await load_history()
            # берём последнюю закрытую свечу
            last_bar = df.iloc[-1]
            # индекс — это pandas.Timestamp
            ts = float(pd.Timestamp(last_bar.name).timestamp())

            close_price = float(last_bar["close"])

            # сначала проверяем открытую сделку
            await check_active_trade_hit(close_price)

            # чтобы не генерить несколько раз по одной и той же свече
            if ts > last_close_ts and (active_trade is None):
                sig = detect_signal(df)
                if sig:
                    side, entry, tp, sl, reason = sig
                    if pips(tp, entry) >= TP_MIN_PIPS:
                        await send_signal(side, entry, tp, sl, reason)
                last_close_ts = ts

        except Exception as e:
            logger.exception("Engine error: %s", e)

        await asyncio.sleep(POLL_SEC)

# ----------------------- КОМАНДЫ -----------------------
@dp.message_handler(commands=["start"])
async def cmd_start(m: types.Message):
    await m.reply("Онлайн. Режим: GOLD (XAU) M1. Команды: /status, /gold")

@dp.message_handler(commands=["status", "статус"])
async def cmd_status(m: types.Message):
    t = active_trade
    if t:
        age = int(time() - t.created_at)
        msg = (f"mode: XAU M1 | open=YES side={t.side} "
               f"entry={t.entry:.2f} tp={t.tp:.2f} sl={t.sl:.2f} age={age}s")
    else:
        msg = "mode: XAU M1 | open=NO"
    await m.reply(f"<code>\n{msg}\n</code>")

@dp.message_handler(commands=["gold", "золото"])
async def cmd_gold(m: types.Message):
    await m.reply("✅ Режим уже: GOLD (XAU) M1.")

# ----------------------- MAIN -----------------------
async def main():
    # параллельно запускаем движок и телеграм
    loop_task = asyncio.create_task(engine_loop())
    try:
        await start_polling(dp, skip_updates=True)
    finally:
        loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop_task

if __name__ == "__main__":
    import contextlib
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
