# signal_bot.py
# -*- coding: utf-8 -*-

import os
import time
import logging
import asyncio
from datetime import datetime, timezone, timedelta

import pandas as pd
import yfinance as yf

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher.filters import BoundFilter

# ==================== CONFIG ====================

SYMBOL = "GC=F"          # GOLD futures у Yahoo
TF = "1m"                # M1
PIP = 0.1                # 1 "пип" для золота = 0.1
POLL_SEC = 0.3           # частота опроса фида
HISTORY_BARS = 120       # сколько свеч берём из истории

# Фильтры свечей (чтобы не брать «пыль»)
MIN_RANGE_PIPS = 3.0     # min High-Low в пипсах
MIN_BODY_PIPS = 1.0      # min |Close-Open| в пипсах
TP_MIN_PIPS = 4.0        # минимум ТР
TP_CAP_PIPS = 30.0       # максимум ТР (ограничитель)
RR_DEFAULT = 1.5         # RR если считаем SL от TP (ниже уточняем)

# Переменные среды (обязательно задать)
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
OWNER_ID = int(os.getenv("OWNER_ID", "6784470762") or 0)
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "6784470762") or 0)

# =================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)

if not MAIN_BOT_TOKEN or not TARGET_CHAT_ID or not OWNER_ID:
    logging.error("Нет переменных среды MAIN_BOT_TOKEN / OWNER_ID / TARGET_CHAT_ID")
    raise SystemExit(1)

bot = Bot(token=MAIN_BOT_TOKEN)
dp = Dispatcher(bot)

# --- доступ только владельцу/цели ---
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

# --- состояние движка и активной сделки ---
class EngineState:
    def __init__(self):
        self.mode = "XAU"                 # один инструмент
        self.last_bar_ts = None           # время последней закрытой свечи (UTC)
        self.last_price = None
        self.signals_today = 0
        self.limit_per_day = 50
        self.cooldown_bars = 0            # 0 = сигнал можно на каждую закрытую свечу
        self.cooldown_left = 0
        self.active_trade = None          # dict | None
        self.poll_ok = True
        self.last_fetch_ok = None

    def reset_day_counters_if_new_day(self):
        now_utc = datetime.now(timezone.utc)
        # обнулим счётчики в 00:00 UTC
        if self.last_bar_ts and now_utc.date() != self.last_bar_ts.date():
            self.signals_today = 0
            self.cooldown_left = 0

ES = EngineState()

# ---------- Вспомогалки ----------

def pips(value: float) -> float:
    """сколько пипсов в абсолютном движении цены"""
    return abs(value) / PIP

def pips_to_price(pips_count: float) -> float:
    return pips_count * PIP

def fmt_price(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")

def load_m1_history(symbol: str, bars: int) -> pd.DataFrame:
    """
    Берём историю M1. yfinance иногда отдаёт незакрытую последнюю минуту —
    поэтому считаем закрытой предпоследнюю строку, если индекс совпадает с текущей минутой.
    """
    df = yf.download(
        tickers=symbol,
        period="2d",
        interval="1m",
        progress=False,
        threads=False,
    )
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise RuntimeError("Empty history")

    df = df.tail(bars + 3).copy()
    # нормализуем индекс в UTC
    df.index = df.index.tz_convert("UTC")
    return df

def last_closed_candle(df: pd.DataFrame):
    """
    Возвращает последнюю закрытую свечу (ts, o, h, l, c).
    Если последняя строка == текущая незакрытая минута — берём предыдущую.
    """
    now_utc_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ts = df.index[-1].to_pydatetime()
    if ts >= now_utc_min:
        # незакрытая — берём -2
        ts = df.index[-2].to_pydatetime()
        row = df.iloc[-2]
    else:
        row = df.iloc[-1]

    return ts, float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"])

def build_signal_from_candle(o, h, l, c):
    """
    Простая логика:
    - если тело вниз и диапазон достаточный → SELL (Pullback-Down)
    - если тело вверх и диапазон достаточный → BUY  (Pullback-Up)
    Варианты BREAKOUT/PULLBACK только как метка — чисто косметика.
    """
    rng_pips = pips(h - l)
    body_pips = pips(c - o)

    if rng_pips < MIN_RANGE_PIPS or abs(body_pips) < MIN_BODY_PIPS:
        return None

    if c < o:
        side = "SELL"
        style = "PULLBACK-DOWN"
        entry = c
    else:
        side = "BUY"
        style = "PULLBACK-UP"
        entry = c

    # TP/SL:
    # базовый TP — max(MIN, 0.9 * rng) но capped
    base_tp = max(TP_MIN_PIPS, 0.9 * rng_pips)
    tp_pips = min(base_tp, TP_CAP_PIPS)

    # SL под RR≈1.5 (минимум 0.6*TP чтобы не слишком узкий)
    sl_pips = max(tp_pips / RR_DEFAULT, tp_pips * 0.6)

    if side == "SELL":
        tp = entry - pips_to_price(tp_pips)
        sl = entry + pips_to_price(sl_pips)
    else:
        tp = entry + pips_to_price(tp_pips)
        sl = entry - pips_to_price(sl_pips)

    rr = tp_pips / sl_pips

    return {
        "side": side,
        "style": style,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "tp_pips": tp_pips,
        "sl_pips": sl_pips,
        "rng_pips": rng_pips,
        "body_pips": abs(body_pips),
        "opened_ts": None,       # заполним, когда «зафиксируем» сигнал
        "bar_ts": None,          # штамп бара, на котором он родился
        "active": True,
    }

async def send_signal_msg(sig: dict):
    text = (
        f"🔥 {sig['side']} GOLD (XAU) | M1 ({sig['style']})\n"
        f"Entry: **{fmt_price(sig['entry'])}**\n"
        f"✅ TP: **{fmt_price(sig['tp'])}**  _(≈{sig['tp_pips']:.2f} pips)_\n"
        f"🟥 SL: **{fmt_price(sig['sl'])}**  _(≈{sig['sl_pips']:.2f} pips)_\n"
        f"rng={sig['rng_pips']:.2f} body={sig['body_pips']:.2f}  RR≈{sig['tp_pips']/sig['sl_pips']:.2f}"
    )
    await bot.send_message(TARGET_CHAT_ID, text, parse_mode="Markdown")

async def send_result_msg(result: str, sig: dict, price: float):
    emoji = "✅" if result == "TP" else "🟥"
    text = (
        f"{emoji} {result} hit | {sig['side']} GOLD (XAU) | M1\n"
        f"Fill: **{fmt_price(price)}**  vs target **{fmt_price(sig[result.lower()])}**\n"
        f"TP={sig['tp_pips']:.2f}p, SL={sig['sl_pips']:.2f}p  (RR≈{sig['tp_pips']/sig['sl_pips']:.2f})"
    )
    await bot.send_message(TARGET_CHAT_ID, text, parse_mode="Markdown")

def price_crossed(value: float, target: float, side: str, kind: str) -> bool:
    """
    Проверка достижения TP/SL баром (по Close/High/Low):
    kind = 'tp'/'sl'
    """
    if kind == "tp":
        if side == "BUY":
            return value >= target
        else:
            return value <= target
    else:  # sl
        if side == "BUY":
            return value <= target
        else:
            return value >= target

# ---------- Основной цикл движка ----------

async def engine_loop():
    logging.info("Engine loop started")
    while True:
        try:
            ES.reset_day_counters_if_new_day()

            # Активная сделка? — следим за достижением TP/SL и только потом ищем новую
            if ES.active_trade and ES.active_trade.get("active", False):
                df = load_m1_history(SYMBOL, 4)
                _, o, h, l, c = last_closed_candle(df)
                ES.last_price = c

                if price_crossed(h, ES.active_trade["tp"], ES.active_trade["side"], "tp") or \
                   price_crossed(c, ES.active_trade["tp"], ES.active_trade["side"], "tp"):
                    ES.active_trade["active"] = False
                    await send_result_msg("TP", ES.active_trade, c)
                    ES.active_trade = None
                    # Бар-кулдаун опционально
                    ES.cooldown_left = ES.cooldown_bars

                elif price_crossed(l, ES.active_trade["sl"], ES.active_trade["side"], "sl") or \
                     price_crossed(c, ES.active_trade["sl"], ES.active_trade["side"], "sl"):
                    ES.active_trade["active"] = False
                    await send_result_msg("SL", ES.active_trade, c)
                    ES.active_trade = None
                    ES.cooldown_left = ES.cooldown_bars

                await asyncio.sleep(POLL_SEC)
                continue

            # Нет активной — ищем новую, но только по НОВОЙ закрытой свече
            df = load_m1_history(SYMBOL, HISTORY_BARS)
            bar_ts, o, h, l, c = last_closed_candle(df)
            ES.last_price = c

            if ES.cooldown_left > 0:
                # ждём N баров
                if ES.last_bar_ts is None or bar_ts > ES.last_bar_ts:
                    ES.cooldown_left -= 1
                    ES.last_bar_ts = bar_ts
                await asyncio.sleep(POLL_SEC)
                continue

            if ES.last_bar_ts is None or bar_ts > ES.last_bar_ts:
                ES.last_bar_ts = bar_ts

                if ES.signals_today >= ES.limit_per_day:
                    await asyncio.sleep(POLL_SEC)
                    continue

                sig = build_signal_from_candle(o, h, l, c)
                if sig:
                    sig["opened_ts"] = datetime.now(timezone.utc)
                    sig["bar_ts"] = bar_ts
                    ES.active_trade = sig
                    ES.signals_today += 1
                    await send_signal_msg(sig)

            await asyncio.sleep(POLL_SEC)

        except Exception as e:
            logging.exception(f"Engine error: {e}")
            # маленькая пауза, чтобы не крутить ошибки безостановочно
            await asyncio.sleep(1.0)

# ---------- Команды TG ----------

@dp.message_handler(commands=["start"], allowed=True)
async def cmd_start(m: types.Message):
    await m.answer(
        "Я здесь. Отправляю один сигнал за раз по GOLD (M1) и жду исход (TP/SL).\n"
        "Команды: /status, «Золото», «Команды».",
    )

@dp.message_handler(commands=["status"], allowed=True)
async def cmd_status(m: types.Message):
    last_age = "-"
    if ES.last_bar_ts:
        last_age = f"{int((datetime.now(timezone.utc)-ES.last_bar_ts).total_seconds())}s"
    act = ES.active_trade["side"] if ES.active_trade else "None"
    await m.answer(
        "```\n"
        f"mode: XAU\n"
        f"alive: OK | poll~{POLL_SEC:.1f}s\n"
        f"cooldown_bars={ES.cooldown_bars} last_close_age={last_age}\n"
        f"signals_today={ES.signals_today} (limit={ES.limit_per_day})\n"
        f"last_price={fmt_price(ES.last_price) if ES.last_price else 'None'}\n"
        f"active={act}\n"
        f"tp_min={TP_MIN_PIPS} cap={TP_CAP_PIPS} min_range={MIN_RANGE_PIPS} min_body={MIN_BODY_PIPS}\n"
        "```",
        parse_mode="Markdown",
    )

@dp.message_handler(lambda m: m.text and m.text.lower().strip() in ("золото", "xau", "gold"), allowed=True)
async def set_gold(m: types.Message):
    ES.mode = "XAU"
    await m.answer("✅ Режим уже: GOLD (XAU) M1.")

@dp.message_handler(lambda m: m.text and "команд" in m.text.lower(), allowed=True)
async def cmd_help(m: types.Message):
    await m.answer(
        "Команды:\n"
        "• /start — приветствие\n"
        "• /status — состояние\n"
        "• «Золото» — режим XAU M1\n\n"
        "Бот шлёт новый сигнал только когда завершится предыдущий (TP/SL).",
    )

@dp.message_handler()
async def ignore(m: types.Message):
    # игнор всего остального, чтобы не засорять чат
    pass

# ---------- Запуск ----------

async def on_startup(_):
    # отдельная задача с движком
    asyncio.create_task(engine_loop())
    logging.info("Bot started, engine scheduled.")

if __name__ == "__main__":
    # ВАЖНО: НИКАКИХ asyncio.run(...) !
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)

