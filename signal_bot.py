# signal_bot.py
# -*- coding: utf-8 -*-

import os
import asyncio
import logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher.filters import BoundFilter

# =============== НАСТРОЙКИ ===============

SYMBOL = "GC=F"           # Gold futures в Yahoo
TF = "1m"                 # базовый таймфрейм
PIP = 1.0                 # 1 пипс = 1.0 пункта (как у твоего брокера)
POLL_SEC = 0.6            # опрос фида (сек)
HISTORY_BARS = 300        # сколько М1 берём для работы

# фильтры свечей, чтобы не брать «пыль»
MIN_RANGE_PIPS = 6.0      # минимум (High-Low) в пипсах
MIN_BODY_PIPS = 2.0       # минимум тело свечи в пипсах
TP_MIN_PIPS = 8.0         # мин. TP
TP_CAP_PIPS = 50.0        # макс. TP (ограничитель)
RR = 1.5                  # целевой RR

# тренд М5: сколько М5-баров берём для наклона
M5_TREND_BARS = 6         # ~30 минут тренда
M5_MIN_SLOPE_PIPS = 2.0   # минимальный наклон тренда в пипсах, чтобы считать направленным

# Лимиты
DAILY_LIMIT = 30          # сигналы/день максимум

# Токены (можно задать через переменные среды или вписать сюда)
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
OWNER_ID = int(os.getenv("OWNER_ID", "6784470762") or 0)
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "0") or 0)

# =========================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)

if not MAIN_BOT_TOKEN or not TARGET_CHAT_ID or not OWNER_ID:
    logging.error("Задай MAIN_BOT_TOKEN / OWNER_ID / TARGET_CHAT_ID (env или в коде).")
    raise SystemExit(1)

bot = Bot(token=MAIN_BOT_TOKEN)
dp = Dispatcher(bot)

# --- доступ только владельцу/цели
class AllowedChat(BoundFilter):
    key = "allowed"

    def __init__(self, allowed: bool):
        self.allowed = allowed

    async def check(self, msg: types.Message) -> bool:
        if msg.chat and msg.chat.id == TARGET_CHAT_ID:
            return True
        if msg.from_user and msg.from_user.id == OWNER_ID:
            return True
        return False

dp.filters_factory.bind(AllowedChat)

# --- состояние
class EngineState:
    def __init__(self):
        self.last_bar_ts = None              # время последней закрытой М1
        self.last_price = None
        self.signals_today = 0
        self.active_trade = None             # dict | None
        self.started_utc_date = datetime.now(timezone.utc).date()

ES = EngineState()

# ========= утилиты =========

def pips_from_move(dpts: float) -> float:
    return abs(dpts) / PIP

def price_from_pips(pips_val: float) -> float:
    return pips_val * PIP

def fmt_price(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")

def load_m1_history(symbol: str, bars: int) -> pd.DataFrame:
    """
    История М1. Последняя строка у Yahoo часто незакрытая — поэтому берём предпоследнюю,
    если таймстамп совпадает с текущей минутой.
    """
    df = yf.download(
        tickers=symbol,
        period="2d",
        interval="1m",
        auto_adjust=True,
        progress=False,
        threads=False,
    )
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise RuntimeError("Empty history")

    df = df.tail(bars + 5).copy()
    df.index = df.index.tz_convert("UTC")
    return df

def pick_last_closed(df: pd.DataFrame):
    now_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ts = df.index[-1].to_pydatetime()
    if ts >= now_min:
        row = df.iloc[-2]
        ts = df.index[-2].to_pydatetime()
    else:
        row = df.iloc[-1]
    return ts, float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"])

def m5_trend_from_m1(df_m1: pd.DataFrame):
    """
    Делаем М5 из М1 и оцениваем наклон за последние M5_TREND_BARS:
    > 0 — восходящий тренд, < 0 — нисходящий, около 0 — флэт.
    Возвращаем: 'UP' | 'DOWN' | 'FLAT'
    """
    df = df_m1.resample("5T").agg({"Open":"first","High":"max","Low":"min","Close":"last"}).dropna()
    if len(df) < M5_TREND_BARS + 1:
        return "FLAT"

    tail = df.tail(M5_TREND_BARS)
    start = float(tail["Close"].iloc[0])
    end = float(tail["Close"].iloc[-1])
    move_pips = pips_from_move(end - start)

    if move_pips < M5_MIN_SLOPE_PIPS:
        return "FLAT"
    return "UP" if end > start else "DOWN"

def build_signal(side: str, entry: float, rng_pips: float, body_pips: float):
    # TP как max(MIN, 0.7*rng) и не больше CAP
    base_tp = max(TP_MIN_PIPS, 0.7 * rng_pips)
    tp_pips = min(base_tp, TP_CAP_PIPS)

    sl_pips = max(tp_pips / RR, tp_pips * 0.6)

    if side == "SELL":
        tp = entry - price_from_pips(tp_pips)
        sl = entry + price_from_pips(sl_pips)
    else:
        tp = entry + price_from_pips(tp_pips)
        sl = entry - price_from_pips(sl_pips)

    return {
        "side": side,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "tp_pips": tp_pips,
        "sl_pips": sl_pips,
        "rng_pips": rng_pips,
        "body_pips": body_pips,
        "bar_ts": None,
        "active": True,
    }

def crossed(value: float, target: float, side: str, kind: str) -> bool:
    # kind: 'tp' / 'sl'
    if kind == "tp":
        return value >= target if side == "BUY" else value <= target
    else:
        return value <= target if side == "BUY" else value >= target

async def send_signal(sig: dict):
    txt = (
        f"🔥 {sig['side']} GOLD (XAU) | M1\n"
        f"Entry: **{fmt_price(sig['entry'])}**\n"
        f"✅ TP: **{fmt_price(sig['tp'])}**  (~{sig['tp_pips']:.2f} pips)\n"
        f"🟥 SL: **{fmt_price(sig['sl'])}**  (~{sig['sl_pips']:.2f} pips)\n"
        f"rng={sig['rng_pips']:.2f} body={sig['body_pips']:.2f}  RR≈{sig['tp_pips']/sig['sl_pips']:.2f}"
    )
    await bot.send_message(TARGET_CHAT_ID, txt, parse_mode="Markdown")

async def send_result(kind: str, sig: dict, fill: float):
    emoji = "✅" if kind == "TP" else "🟥"
    key = "tp" if kind == "TP" else "sl"
    txt = (
        f"{emoji} {kind} hit | {sig['side']} GOLD (XAU) | M1\n"
        f"Fill: **{fmt_price(fill)}**  vs target **{fmt_price(sig[key])}**\n"
        f"TP={sig['tp_pips']:.2f}p, SL={sig['sl_pips']:.2f}p  (RR≈{sig['tp_pips']/sig['sl_pips']:.2f})"
    )
    await bot.send_message(TARGET_CHAT_ID, txt, parse_mode="Markdown")

# ========= основной цикл =========

async def engine():
    logging.info("Engine started.")
    while True:
        try:
            # сброс дневного лимита по UTC-датe
            today = datetime.now(timezone.utc).date()
            if today != ES.started_utc_date:
                ES.started_utc_date = today
                ES.signals_today = 0

            # если есть активная сделка — отслеживаем TP/SL
            if ES.active_trade and ES.active_trade.get("active"):
                df = load_m1_history(SYMBOL, 5)
                _, o, h, l, c = pick_last_closed(df)
                ES.last_price = c

                if crossed(h, ES.active_trade["tp"], ES.active_trade["side"], "tp") or crossed(c, ES.active_trade["tp"], ES.active_trade["side"], "tp"):
                    ES.active_trade["active"] = False
                    await send_result("TP", ES.active_trade, c)
                    ES.active_trade = None

                elif crossed(l, ES.active_trade["sl"], ES.active_trade["side"], "sl") or crossed(c, ES.active_trade["sl"], ES.active_trade["side"], "sl"):
                    ES.active_trade["active"] = False
                    await send_result("SL", ES.active_trade, c)
                    ES.active_trade = None

                await asyncio.sleep(POLL_SEC)
                continue

            # нет активной — ищем новый сигнал только на НОВОЙ закрытой М1
            df = load_m1_history(SYMBOL, HISTORY_BARS)
            bar_ts, o, h, l, c = pick_last_closed(df)
            ES.last_price = c

            # антиспам: только по новой свече
            if ES.last_bar_ts is not None and bar_ts <= ES.last_bar_ts:
                await asyncio.sleep(POLL_SEC)
                continue
            ES.last_bar_ts = bar_ts

            if ES.signals_today >= DAILY_LIMIT:
                await asyncio.sleep(POLL_SEC)
                continue

            # фильтр по М5-тренду
            trend = m5_trend_from_m1(df)
            rng_p = pips_from_move(h - l)
            body_p = pips_from_move(c - o)

            # базовые фильтры свечи
            if rng_p < MIN_RANGE_PIPS or body_p < MIN_BODY_PIPS or trend == "FLAT":
                await asyncio.sleep(POLL_SEC)
                continue

            # выбираем сторону в сторону тренда
            side = None
            if trend == "UP" and c > o:
                side = "BUY"
            elif trend == "DOWN" and c < o:
                side = "SELL"

            if not side:
                await asyncio.sleep(POLL_SEC)
                continue

            sig = build_signal(side, c, rng_p, body_p)
            sig["bar_ts"] = bar_ts
            ES.active_trade = sig
            ES.signals_today += 1
            await send_signal(sig)

            await asyncio.sleep(POLL_SEC)

        except Exception as e:
            logging.exception(f"Engine error: {e}")
            await asyncio.sleep(1.0)

# ========= команды TG =========

@dp.message_handler(commands=["start"], allowed=True)
async def _start(m: types.Message):
    await m.answer("Готов. Даю **один** сигнал за раз по XAU (М1) в сторону тренда М5. Команды: /status, «Золото», «Команды».")

@dp.message_handler(commands=["status"], allowed=True)
async def _status(m: types.Message):
    last_age = "-"
    if ES.last_bar_ts:
        last_age = f"{int((datetime.now(timezone.utc)-ES.last_bar_ts).total_seconds())}s"
    act = ES.active_trade["side"] if ES.active_trade else "None"
    await m.answer(
        "```\n"
        f"mode: XAU\n"
        f"alive: OK | poll~{POLL_SEC:.1f}s\n"
        f"last_close_age={last_age}  signals_today={ES.signals_today}/{DAILY_LIMIT}\n"
        f"last_price={fmt_price(ES.last_price) if ES.last_price else 'None'}  active={act}\n"
        f"filters: rng>={MIN_RANGE_PIPS}p body>={MIN_BODY_PIPS}p  RR={RR}\n"
        f"tp_min={TP_MIN_PIPS}p cap={TP_CAP_PIPS}p  PIP={PIP}\n"
        "```",
        parse_mode="Markdown",
    )

@dp.message_handler(lambda m: m.text and m.text.lower().strip() in ("золото","xau","gold"), allowed=True)
async def _gold(m: types.Message):
    await m.answer("✅ Режим: GOLD (XAU) M1. Фильтр тренда М5 активен.")

@dp.message_handler(lambda m: m.text and "команд" in m.text.lower(), allowed=True)
async def _help(m: types.Message):
    await m.answer("Команды: /start, /status, «Золото». Бот шлёт новый сигнал только после TP/SL предыдущего.")

@dp.message_handler()
async def _ignore(m: types.Message):
    pass

# ========= запуск =========

async def on_startup(_):
    asyncio.create_task(engine())
    logging.info("Bot started.")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
