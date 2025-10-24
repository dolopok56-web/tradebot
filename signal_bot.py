# signal_bot.py
# -*- coding: utf-8 -*-

import asyncio
import logging
import time
from datetime import datetime, timezone
import json
import socket

import pandas as pd
import yfinance as yf
from requests import RequestException

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher.filters import BoundFilter

# ==================== ЖЁСТКИЕ НАСТРОЙКИ ====================

# Твои данные — ВШИТЫ. Если когда-нибудь сменишь — поменяй тут.
MAIN_BOT_TOKEN = "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"
OWNER_ID = 6784470762
TARGET_CHAT_ID = 6784470762

# Инструмент и таймфрейм
SYMBOL = "GC=F"     # Yahoo Gold Futures
TF = "1m"           # 1 минута

# Пипсы / частоты
PIP = 1.0
POLL_SEC = 1.0          # частота опроса (сек)
HISTORY_BARS = 120

# Фильтры свечи
MIN_RANGE_PIPS = 3.0
MIN_BODY_PIPS = 1.0

# TP/SL и RR
TP_MIN_PIPS = 4.0
TP_CAP_PIPS = 30.0
RR_DEFAULT = 1.5

# Анти-спам
SIGNALS_PER_DAY_LIMIT = 50
COOLDOWN_BARS = 0      # 0=без кулдауна после сделки, можно менять

# ===========================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)

bot = Bot(token=MAIN_BOT_TOKEN)
dp = Dispatcher(bot)


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


class EngineState:
    def __init__(self):
        self.last_bar_ts = None       # datetime UTC последней закрытой свечи
        self.last_price = None
        self.signals_today = 0
        self.limit_per_day = SIGNALS_PER_DAY_LIMIT
        self.cooldown_bars = COOLDOWN_BARS
        self.cooldown_left = 0
        self.active_trade = None      # dict | None

    def reset_day_if_needed(self, new_bar_ts):
        if self.last_bar_ts and new_bar_ts.date() != self.last_bar_ts.date():
            self.signals_today = 0
            self.cooldown_left = 0


ES = EngineState()

# -------------------- Утилиты --------------------

def pips(value: float) -> float:
    return abs(value) / PIP


def pips_to_price(pips_count: float) -> float:
    return pips_count * PIP


def fmt_price(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")


def load_m1_history(symbol: str, bars: int) -> pd.DataFrame:
    """
    Надёжный загрузчик 1m истории с retry и fallback.
    """
    tries = 0
    max_tries = 5
    delay = 1.0
    while tries < max_tries:
        tries += 1
        try:
            df = yf.download(
                tickers=symbol,
                period="2d",
                interval="1m",
                progress=False,
                threads=False,
            )
            if isinstance(df, pd.DataFrame) and not df.empty:
                df = df.tail(bars + 3).copy()
                try:
                    df.index = df.index.tz_convert("UTC")
                except Exception:
                    df.index = df.index.tz_localize("UTC")
                return df

            logging.warning("yfinance empty DF, fallback to Ticker.history (try %d/%d)", tries, max_tries)
            t = yf.Ticker(symbol)
            df2 = t.history(period="2d", interval="1m", auto_adjust=False)
            if isinstance(df2, pd.DataFrame) and not df2.empty:
                df2 = df2.tail(bars + 3).copy()
                try:
                    df2.index = df2.index.tz_convert("UTC")
                except Exception:
                    df2.index = df2.index.tz_localize("UTC")
                return df2

            logging.warning("Fallback empty (try %d/%d). Sleep %.1fs", tries, max_tries, delay)
            time.sleep(delay)
            delay *= 1.8

        except (json.JSONDecodeError, RequestException, socket.timeout) as e:
            logging.warning("Network/JSON error: %s (try %d/%d). Sleep %.1fs", e, tries, max_tries, delay)
            time.sleep(delay)
            delay *= 1.8
        except Exception as e:
            logging.exception("load_m1_history unexpected (try %d/%d): %s", tries, max_tries, e)
            time.sleep(delay)
            delay *= 1.8

    raise RuntimeError("Empty history")


def last_closed_candle(df: pd.DataFrame):
    """
    Вернуть последнюю ЗАКРЫТУЮ свечу (ts, o, h, l, c).
    Если последняя строка совпадает с текущей минутой — берём предыдущую.
    """
    now_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ts = df.index[-1].to_pydatetime()
    if ts >= now_min:
        row = df.iloc[-2]
        ts = df.index[-2].to_pydatetime()
    else:
        row = df.iloc[-1]
    return ts, float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"])


def build_signal(o, h, l, c):
    rng_p = pips(h - l)
    body_p = pips(c - o)
    if rng_p < MIN_RANGE_PIPS or abs(body_p) < MIN_BODY_PIPS:
        return None

    side = "SELL" if c < o else "BUY"
    style = "PULLBACK-DOWN" if side == "SELL" else "PULLBACK-UP"
    entry = c

    base_tp = max(TP_MIN_PIPS, 0.9 * rng_p)
    tp_p = min(base_tp, TP_CAP_PIPS)
    sl_p = max(tp_p / RR_DEFAULT, tp_p * 0.6)

    if side == "SELL":
        tp = entry - pips_to_price(tp_p)
        sl = entry + pips_to_price(sl_p)
    else:
        tp = entry + pips_to_price(tp_p)
        sl = entry - pips_to_price(sl_p)

    return {
        "side": side,
        "style": style,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "tp_pips": tp_p,
        "sl_pips": sl_p,
        "rng_pips": rng_p,
        "body_pips": abs(body_p),
        "bar_ts": None,
        "opened_ts": None,
        "active": True,
    }


async def send_signal(sig: dict):
    txt = (
        f"🔥 {sig['side']} GOLD (XAU) | M1 ({sig['style']})\n"
        f"Entry: **{fmt_price(sig['entry'])}**\n"
        f"✅ TP: **{fmt_price(sig['tp'])}**  _(≈{sig['tp_pips']:.2f} pips)_\n"
        f"🟥 SL: **{fmt_price(sig['sl'])}**  _(≈{sig['sl_pips']:.2f} pips)_\n"
        f"rng={sig['rng_pips']:.2f} body={sig['body_pips']:.2f}  RR≈{sig['tp_pips']/sig['sl_pips']:.2f}"
    )
    await bot.send_message(TARGET_CHAT_ID, txt, parse_mode="Markdown")


async def send_result(result: str, sig: dict, price: float):
    emoji = "✅" if result == "TP" else "🟥"
    txt = (
        f"{emoji} {result} hit | {sig['side']} GOLD (XAU) | M1\n"
        f"Fill: **{fmt_price(price)}**  vs target **{fmt_price(sig[result.lower()])}**\n"
        f"TP={sig['tp_pips']:.2f}p, SL={sig['sl_pips']:.2f}p  (RR≈{sig['tp_pips']/sig['sl_pips']:.2f})"
    )
    await bot.send_message(TARGET_CHAT_ID, txt, parse_mode="Markdown")


def crossed(val: float, target: float, side: str, kind: str) -> bool:
    if kind == "tp":
        return val >= target if side == "BUY" else val <= target
    else:
        return val <= target if side == "BUY" else val >= target


# -------------------- ДВИЖОК --------------------

async def engine_loop():
    logging.info("Engine loop started")
    while True:
        try:
            # если есть активная — следим только за ней
            if ES.active_trade and ES.active_trade.get("active", False):
                df = load_m1_history(SYMBOL, 4)
                _, o, h, l, c = last_closed_candle(df)
                ES.last_price = c

                if crossed(h, ES.active_trade["tp"], ES.active_trade["side"], "tp") or \
                   crossed(c, ES.active_trade["tp"], ES.active_trade["side"], "tp"):
                    ES.active_trade["active"] = False
                    await send_result("TP", ES.active_trade, c)
                    ES.active_trade = None
                    ES.cooldown_left = ES.cooldown_bars
                    await asyncio.sleep(POLL_SEC)
                    continue

                if crossed(l, ES.active_trade["sl"], ES.active_trade["side"], "sl") or \
                   crossed(c, ES.active_trade["sl"], ES.active_trade["side"], "sl"):
                    ES.active_trade["active"] = False
                    await send_result("SL", ES.active_trade, c)
                    ES.active_trade = None
                    ES.cooldown_left = ES.cooldown_bars
                    await asyncio.sleep(POLL_SEC)
                    continue

                await asyncio.sleep(POLL_SEC)
                continue

            # Ищем новую — только на НОВОЙ закрытой свече
            df = load_m1_history(SYMBOL, HISTORY_BARS)
            bar_ts, o, h, l, c = last_closed_candle(df)
            ES.last_price = c
            ES.reset_day_if_needed(bar_ts)

            if ES.cooldown_left > 0:
                # отсчитываем кулдаун по закрытым свечам
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

                sig = build_signal(o, h, l, c)
                if sig:
                    sig["bar_ts"] = bar_ts
                    sig["opened_ts"] = datetime.now(timezone.utc)
                    ES.active_trade = sig
                    ES.signals_today += 1
                    await send_signal(sig)

            await asyncio.sleep(POLL_SEC)

        except Exception as e:
            logging.exception("Engine error: %s", e)
            await asyncio.sleep(5.0)  # пауза при проблемах, чтобы не спамить


# -------------------- КОМАНДЫ TG --------------------

@dp.message_handler(commands=["start"], allowed=True)
async def cmd_start(m: types.Message):
    await m.answer(
        "Готов. Даю ОДИН сигнал по GOLD (M1) и жду исход (TP/SL). "
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
    await m.answer("✅ Режим: GOLD (XAU) M1.")


@dp.message_handler(lambda m: m.text and "команд" in m.text.lower(), allowed=True)
async def cmd_help(m: types.Message):
    await m.answer(
        "Команды:\n"
        "• /start — приветствие\n"
        "• /status — состояние\n"
        "• «Золото» — режим XAU M1\n\n"
        "Новый сигнал появляется только ПОСЛЕ TP/SL предыдущего."
    )


@dp.message_handler()
async def ignore(m: types.Message):
    pass


# -------------------- ЗАПУСК --------------------

async def on_startup(_):
    asyncio.create_task(engine_loop())
    logging.info("Bot started, engine scheduled.")


if __name__ == "__main__":
    # aiogram v2.x
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)

