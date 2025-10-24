# gold_bot_final.py
# Полный рабочий бот: XAU (Gold) M1, без спама.
# Зависимости: aiogram, aiohttp, yfinance, pandas, numpy
# pip install aiogram aiohttp yfinance pandas numpy

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional, Callable

import pandas as pd
import numpy as np
import yfinance as yf
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command

# ==========[ НАСТРОЙКИ ]==========

# ТВОИ ДАННЫЕ (можно оставить так — работает из коробки)
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
OWNER_ID = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))  # куда летят сигналы
LOG_CHAT_ID = int(os.getenv("LOG_CHAT_ID", str(OWNER_ID)))        # лог (alive/техтекст)

# Рынок/фид
SYMBOL_NAME = "GOLD (XAU)"
YF_TICKER = os.getenv("YF_TICKER", "GC=F")   # золото фьюч; можно "XAUUSD=X"
TIMEFRAME = "1m"                             # M1
POLL_SEC = 3                                 # опрос каждые ~3с
PER_BAR_MODE = True                          # анализ только по закрытой свече

# Условия свечи
MIN_RANGE = 3.0      # минимальный диапазон свечи (в пунктах цены)
MIN_BODY = 1.0       # минимальное тело свечи (в пунктах)
TP_MIN = 4.0         # минимум ТП (в пунктах)
CAP_TP = 30.0        # ограничение ТП (чтоб не улетало)
SL_BUFFER = 3.0      # запас к SL (для SELL SL=entry+max(body,MIN_RANGE)+SL_BUFFER)
# Анти-спам:
EXCLUSIVE_MODE = True           # один активный сигнал
LOCK_SAME_BAR = True            # не дублировать в тот же бар
MAX_TRADE_BARS = 6              # "срок годности" идеи (6 баров M1 ≈ 6 мин)
COOLDOWN_AFTER_CLOSE_SEC = 60   # пауза после TP/SL/expire
ALIVE_TO_MAIN = False           # alive только в лог, не в основной чат

# ==========[ СЕРВИС ]==========

def now_ts() -> int:
    return int(time.time())

async def aprint_log(bot: Bot, text: str):
    try:
        if ALIVE_TO_MAIN:
            await bot.send_message(TARGET_CHAT_ID, text)
        else:
            await bot.send_message(LOG_CHAT_ID, text)
    except Exception:
        pass

@dataclass
class TradeState:
    side: str          # "BUY" / "SELL"
    entry: float
    tp: float
    sl: float
    bar_ts: pd.Timestamp   # ts бара, по которому открыт сигнал
    opened_ts: int

active: Optional[TradeState] = None
cooldown_until = 0
last_signal_bar_id: Optional[pd.Timestamp] = None

def price_hit(side: str, p: float, tp: float, sl: float) -> Optional[str]:
    if side == "BUY":
        if p >= tp: return "TP"
        if p <= sl: return "SL"
    else:
        if p <= tp: return "TP"
        if p >= sl: return "SL"
    return None

def bars_since(bar_ts: pd.Timestamp, last_bar_ts: pd.Timestamp) -> int:
    # сколько закрытых баров прошло от bar_ts до last_bar_ts
    # оба — pandas Timestamp с минутной частотой
    if bar_ts is None or last_bar_ts is None:
        return 0
    dif = (last_bar_ts - bar_ts) / pd.Timedelta(minutes=1)
    try:
        return int(dif)
    except Exception:
        return 0

def last_price_from_df(df: pd.DataFrame) -> float:
    return float(df["Close"].iloc[-1])

# ==========[ ДАННЫЕ ]==========

async def fetch_bars() -> pd.DataFrame:
    """
    Грузим последние 90 минут M1. yfinance работает синхронно — завернём в тред.
    """
    loop = asyncio.get_running_loop()
    def _load():
        return yf.Ticker(YF_TICKER).history(period="90m", interval=TIMEFRAME)

    df = await loop.run_in_executor(None, _load)
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise RuntimeError("Empty data from yfinance")
    df = df[["Open", "High", "Low", "Close"]].copy()
    df = df.dropna()
    return df

def detect_signal(df: pd.DataFrame):
    """
    Простая реактивная логика без ATR:
    - если закрылась большая красная свеча (range>=MIN_RANGE & body>=MIN_BODY) → SELL
    - если зелёная с теми же условиями → BUY
    """
    row = df.iloc[-1]  # последний ЗАКРЫТЫЙ бар (мы работаем per-bar)
    o, h, l, c = float(row.Open), float(row.High), float(row.Low), float(row.Close)
    rng = h - l
    body = abs(c - o)
    if rng < MIN_RANGE or body < MIN_BODY:
        return None

    if c < o:  # медвежья
        side = "SELL"
        entry = c
        tp = max(entry - max(TP_MIN, body), entry - TP_MIN)
        tp = max(tp, entry - CAP_TP)
        sl = entry + max(body, MIN_RANGE) + SL_BUFFER
    else:      # бычья
        side = "BUY"
        entry = c
        tp = min(entry + max(TP_MIN, body), entry + CAP_TP)
        sl = entry - max(body, MIN_RANGE) - SL_BUFFER

    # RR чисто информативно
    rr = abs(tp - entry) / max(1e-6, abs(entry - sl))
    return {
        "side": side,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "rng": rng,
        "body": body,
        "rr": rr
    }

async def send_signal(bot: Bot, sig: dict):
    global active, last_signal_bar_id
    msg = (
        f"🔥 {sig['side']} {SYMBOL_NAME} | M1\n"
        f"Entry: {sig['entry']:.2f}\n"
        f"✅ TP: **{sig['tp']:.2f}**\n"
        f"🧱 SL: **{sig['sl']:.2f}**\n"
        f"rng={sig['rng']:.2f} body={sig['body']:.2f}  RR≈{sig['rr']:.2f}"
    )
    await bot.send_message(TARGET_CHAT_ID, msg, parse_mode=ParseMode.MARKDOWN)
    active = TradeState(
        side=sig["side"], entry=sig["entry"], tp=sig["tp"], sl=sig["sl"],
        bar_ts=last_signal_bar_id, opened_ts=now_ts()
    )

async def on_tp_sl(bot: Bot, hit: str, price: float):
    global active, cooldown_until
    side = active.side
    await bot.send_message(
        TARGET_CHAT_ID,
        ("✅ TP hit" if hit == "TP" else "🛑 SL hit") + f" @ {price:.2f} ({side})"
    )
    active = None
    cooldown_until = now_ts() + COOLDOWN_AFTER_CLOSE_SEC

# ==========[ ТЕЛЕГРАМ ]==========

bot = Bot(token=MAIN_BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    await m.answer("Готов. Режим: GOLD M1. Один сигнал за раз, без спама. Команда: /status")

@dp.message(Command("status"))
async def cmd_status(m: types.Message):
    global active, cooldown_until
    cool = max(0, cooldown_until - now_ts())
    act = (
        f"{active.side} entry={active.entry:.2f} tp={active.tp:.2f} sl={active.sl:.2f}"
        if active else "нет"
    )
    await m.answer(
        f"<b>mode:</b> XAU M1\n"
        f"cooldown: {cool}s\n"
        f"active: {act}\n"
        f"min_range={MIN_RANGE} min_body={MIN_BODY} tp_min={TP_MIN} cap={CAP_TP}\n"
        f"per_bar={PER_BAR_MODE} poll={POLL_SEC}s"
    )

@dp.message(Command("cancel"))
async def cmd_cancel(m: types.Message):
    global active, cooldown_until
    active = None
    cooldown_until = now_ts() + 5
    await m.answer("Окей, текущая идея сброшена.")

# ==========[ ДВИЖОК ]==========

async def engine_loop():
    global last_signal_bar_id, cooldown_until, active

    await aprint_log(bot, "[engine] start")
    last_closed_index: Optional[pd.Timestamp] = None

    while True:
        try:
            df = await fetch_bars()

            # последний закрытый бар — это последний в df
            closed_ts = df.index[-1]

            # мониторинг активной идеи
            if active:
                last_price = float(df["Close"].iloc[-1])
                hit = price_hit(active.side, last_price, active.tp, active.sl)
                if hit:
                    await on_tp_sl(bot, hit, last_price)
                else:
                    # истечение по количеству баров
                    bars_passed = bars_since(active.bar_ts, closed_ts)
                    if bars_passed >= MAX_TRADE_BARS:
                        await aprint_log(bot, "Idea expired (no TP/SL). Unlock.")
                        active = None
                        cooldown_until = now_ts() + COOLDOWN_AFTER_CLOSE_SEC

            # новые сигналы — только если:
            # 1) нет активной сделки
            # 2) нет кулдауна
            # 3) новый бар закрыт
            if (not active) and (now_ts() >= cooldown_until):
                new_bar = (last_closed_index is None) or (closed_ts != last_closed_index)
                if PER_BAR_MODE and new_bar:
                    # запоминаем бар
                    last_closed_index = closed_ts
                    last_signal_bar_id = closed_ts

                    sig = detect_signal(df)
                    if sig:
                        # антидубликат в том же баре
                        if LOCK_SAME_BAR and last_signal_bar_id == closed_ts:
                            # last_signal_bar_id сейчас как раз обновили выше — так что
                            # проверку оставим для совместимости с будущими изменениями
                            pass
                        # отсылка
                        if EXCLUSIVE_MODE:
                            await send_signal(bot, sig)

            # alive в лог раз в ~60с
            if int(time.time()) % 60 == 0:
                await aprint_log(bot, f"[alive] {SYMBOL_NAME} poll={POLL_SEC}s ok")

        except Exception as e:
            try:
                await aprint_log(bot, f"[engine] error: {e}")
            except Exception:
                pass

        await asyncio.sleep(POLL_SEC)

# ==========[ MAIN ]==========

async def main():
    asyncio.create_task(engine_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
