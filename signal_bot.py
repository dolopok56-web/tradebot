#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Signal bot NG-only — HUMAN/TREND style, no scalping, offset support for broker prices.
Copy into signal_bot.py, set MAIN_BOT_TOKEN and TARGET_CHAT_ID, run.
"""

import os, time, csv, logging, asyncio, math
from datetime import datetime, date
import aiohttp
import pandas as pd

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message
from aiogram.filters import Command

# ---------------- CONFIG ----------------
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "6784470762"))  # поставь свой chat id
OWNER_ID = int(os.getenv("OWNER_ID", "6784470762"))

VERSION = "NG-HUMAN-READY v1.0"

SYMBOL_YAHOO = "NG=F"
SYMBOL_NAME  = "NATGAS (NG=F)"

# Default thresholds — подгони при необходимости
IMP_LOOK_MIN   = 5        # окно импульса (минут)
IMP_MOVE_MIN   = 0.0100   # 0.0100 ~ 10 пипсов за IMP_LOOK_MIN (подстрой под рынок)
TAIL_MAX_FRAC  = 0.80     # не стрелять если текущ свеча уже покрыла >80% импульса (в хвост)
BREAK_BUFFER   = 0.0010   # буфер при сравнении (анти-микрошум)

# TP / SL / RR
SL_MIN         = 0.0100   # минимум SL (10 пипсов)
TP_MIN         = 0.0150   # минимум TP
TP_MAX         = 0.0400   # максимум TP (30-40 ппс)
RR_TARGET      = 1.6      # целевой RR = TP/risk

SPREAD_BUF     = 0.0040   # добавочный буфер при отображении уровней (примерно твой спред)

# Rate limiting
COOLDOWN_SEC         = 60       # пауза между сигналами
MAX_SIGNALS_PER_DAY  = 8        # мягкий лимит в день (поднимай если нужно)

# Data fetch
YAHOO_RETRIES = 3
POLL_SEC      = 5.0     # частота цикла (сек) — низкая, но достаточно частая

TRADES_CSV = "ng_trades.csv"

# ---------------- STATE ----------------
boot_ts = time.time()

bot = Bot(MAIN_BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# runtime state
_last_prices = None
_last_prices_ts = 0.0
_offset = 0.0   # broker_price = yahoo_price + _offset ; can be set by /ref
_signals_today = 0
_signals_day_date = date.today()
_last_signal_ts = 0.0
_last_signal_price = None
_last_impulse = {"side": None, "ts": 0.0, "price": None}
_running = True

# ---------------- HELPERS ----------------
def rnd(x: float) -> float:
    return round(float(x), 4)

def reset_day_if_needed():
    global _signals_today, _signals_day_date
    today = date.today()
    if today != _signals_day_date:
        _signals_today = 0
        _signals_day_date = today

def append_trade_csv(row: dict):
    try:
        newf = not os.path.exists(TRADES_CSV)
        with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            if newf: w.writeheader()
            w.writerow(row)
    except Exception as e:
        logging.error("append_trade_csv error: %s", e)

async def send_main(text: str):
    try:
        await bot.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.exception("send_main err: %s", e)

# ---------------- TELEGRAM CMDS ----------------
@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot alive — {VERSION}\nКоманды: статус, тест, стоп, агро, норм, лайт, ref <price>, отчет")

@router.message(lambda msg: msg.text and msg.text.lower() == "статус")
async def cmd_status(m: Message):
    reset_day_if_needed()
    lines = [
        f"VER: {VERSION}",
        f"Market: {SYMBOL_NAME}",
        f"Offset(broker - yahoo): {_offset:.4f}",
        f"Signals today: {_signals_today}/{MAX_SIGNALS_PER_DAY}",
        f"Last signal (ref price): {rnd(_last_signal_price) if _last_signal_price else '—'}",
        f"Cooldown left: {max(0,int(_last_signal_ts + COOLDOWN_SEC - time.time()))}s",
        f"Thresholds: IMP_LOOK_MIN={IMP_LOOK_MIN}m IMP_MOVE_MIN={IMP_MOVE_MIN} TAIL_MAX_FRAC={TAIL_MAX_FRAC}",
        f"SLmin={SL_MIN} TPmin={TP_MIN} TPmax={TP_MAX} RRtarget={RR_TARGET}"
    ]
    await m.answer("```\n" + "\n".join(lines) + "\n```")

@router.message(lambda msg: msg.text and msg.text.lower().startswith("ref "))
async def cmd_ref(m: Message):
    global _offset
    try:
        parts = m.text.split()
        if len(parts) >= 2:
            broker_price = float(parts[1].replace(',', '.'))
            # fetch yahoo last price quickly
            yahoo_last = 0.0
            async with aiohttp.ClientSession() as s:
                for _ in range(2):
                    try:
                        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{SYMBOL_YAHOO}?interval=1m&range=1d"
                        async with s.get(url, timeout=8) as r:
                            if r.status == 200:
                                j = await r.json(content_type=None)
                                res = j.get("chart", {}).get("result", [])
                                if res:
                                    meta = res[0].get("meta", {})
                                    yahoo_last = float(meta.get("regularMarketPrice") or 0.0)
                                    break
                    except: await asyncio.sleep(0.4)
            if yahoo_last <= 0:
                await m.answer("Не удалось получить реальную цену Yahoo для калибровки.")
                return
            _offset = broker_price - yahoo_last
            await m.answer(f"OK. Установлен OFFSET = {_offset:.4f} (broker {broker_price:.4f} vs yahoo {yahoo_last:.4f})")
        else:
            await m.answer("Формат: ref <broker_last_price>. Пример: ref 4.029")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@router.message(lambda msg: msg.text and msg.text.lower() == "тест")
async def cmd_test(m: Message):
    entry = 3.2000
    entry_b = entry + _offset
    tp, sl = build_tp_sl("BUY", entry)
    tp_b, sl_b = tp + _offset, sl + _offset
    txt = (f"🔥 TEST BUY {SYMBOL_NAME}\nEntry: {rnd(entry_b)} (ref:{rnd(entry)})\n"
           f"TP: {rnd(tp_b)}  SL: {rnd(sl_b)}")
    await m.answer(txt)

@router.message(lambda msg: msg.text and msg.text.lower() == "стоп")
async def cmd_stop(m: Message):
    global _last_signal_ts
    _last_signal_ts = time.time() + 5
    await m.answer("🛑 Локальный стоп: 5s кулдаун.")

@router.message(lambda msg: msg.text and msg.text.lower() == "агро")
async def cmd_agro(m: Message):
    global IMP_LOOK_MIN, IMP_MOVE_MIN, COOLDOWN_SEC
    IMP_LOOK_MIN = 3; IMP_MOVE_MIN = 0.0060; COOLDOWN_SEC = 40
    await m.answer("Профиль: АГРО (больше сигналов).")

@router.message(lambda msg: msg.text and msg.text.lower() == "норм")
async def cmd_norm(m: Message):
    global IMP_LOOK_MIN, IMP_MOVE_MIN, COOLDOWN_SEC
    IMP_LOOK_MIN = 5; IMP_MOVE_MIN = 0.0100; COOLDOWN_SEC = 60
    await m.answer("Профиль: НОРМ (по умолчанию).")

@router.message(lambda msg: msg.text and msg.text.lower() == "лайт")
async def cmd_light(m: Message):
    global IMP_LOOK_MIN, IMP_MOVE_MIN, COOLDOWN_SEC
    IMP_LOOK_MIN = 7; IMP_MOVE_MIN = 0.0140; COOLDOWN_SEC = 90
    await m.answer("Профиль: ЛАЙТ (меньше сигналов).")

@router.message(lambda msg: msg.text and msg.text.lower() == "отчет")
async def cmd_report(m: Message):
    if m.from_user.id != OWNER_ID:
        return await m.answer("Доступно только владельцу.")
    if not os.path.exists(TRADES_CSV):
        return await m.answer("Пока нет закрытых сигналов.")
    rows = list(csv.DictReader(open(TRADES_CSV, encoding="utf-8")))[-20:]
    if not rows:
        return await m.answer("Пусто.")
    txt = "Последние закрытия:\n"
    for r in rows:
        txt += f"{r.get('ts_close')} {r.get('side')} entry:{r.get('entry')} tp:{r.get('tp')} sl:{r.get('sl')} outcome:{r.get('outcome')}\n"
    await m.answer("```\n"+txt+"```")

# ---------------- PRICE FETCH ----------------
async def fetch_yahoo(session: aiohttp.ClientSession):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{SYMBOL_YAHOO}?interval=1m&range=1d"
    for _ in range(YAHOO_RETRIES):
        try:
            async with session.get(url, timeout=8) as r:
                if r.status != 200:
                    await asyncio.sleep(0.6); continue
                j = await r.json(content_type=None)
                res = j.get("chart", {}).get("result", [])
                if not res: return [], 0.0
                r0 = res[0]
                ts = r0.get("timestamp", [])
                q = r0.get("indicators", {}).get("quote", [{}])[0]
                closes = q.get("close", [])
                # filter none/0
                closes = [c for c in closes if c is not None and c > 0]
                meta = r0.get("meta", {})
                last_price = float(meta.get("regularMarketPrice") or (closes[-1] if closes else 0.0))
                return closes, last_price
        except Exception:
            await asyncio.sleep(0.6)
    return [], 0.0

# ---------------- SIGNAL BUILD / CHECKS ----------------
def build_tp_sl(side: str, entry: float):
    """Return tp, sl (ref-scale)"""
    if side == "BUY":
        sl = entry - SL_MIN - SPREAD_BUF
        risk = entry - sl
        tp = entry + min(max(RR_TARGET * risk, TP_MIN), TP_MAX)
        # clamp
        tp = min(tp, entry + TP_MAX)
    else:
        sl = entry + SL_MIN + SPREAD_BUF
        risk = sl - entry
        tp = entry - min(max(RR_TARGET * risk, TP_MIN), TP_MAX)
        tp = max(tp, entry - TP_MAX)
    return rnd(tp), rnd(sl)

def should_fire_impulse(closes: list, look_min: int, move_min: float):
    """
    closes: list of closes (in chronological order)
    look_min: L
    move_min: threshold
    returns (side, entry, reason) or (None,...)
    """
    if len(closes) < look_min + 2:
        return None, None, None
    price_now = float(closes[-1])
    price_look = float(closes[-(look_min+1)])
    delta = price_now - price_look
    # check impulse
    if delta >= (move_min + BREAK_BUFFER):
        side = "BUY"
        impulse = delta
    elif delta <= -(move_min + BREAK_BUFFER):
        side = "SELL"
        impulse = abs(delta)
    else:
        return None, None, None

    # tail check: how much of impulse is already done in last candle movement?
    # approximate last candle movement
    last_candle_move = abs(float(closes[-1]) - float(closes[-2])) if len(closes) >= 2 else 0.0
    if last_candle_move >= impulse * TAIL_MAX_FRAC:
        # it's already almost done -> skip (tail)
        return None, None, "TAIL"
    # passed -> entry = price_now
    return side, price_now, "IMPULSE"

# ---------------- ENGINE LOOP ----------------
async def engine_loop():
    global _last_prices, _last_prices_ts, _last_impulse, _signals_today, _last_signal_ts, _last_signal_price
    async with aiohttp.ClientSession() as session:
        while _running:
            try:
                reset_day_if_needed()
                # fetch prices
                closes, last_price = await fetch_yahoo(session)
                if not closes:
                    await asyncio.sleep(POLL_SEC); continue

                _last_prices = closes
                _last_prices_ts = time.time()

                # housekeeping: rate limits
                if _signals_today >= MAX_SIGNALS_PER_DAY:
                    await asyncio.sleep(POLL_SEC); continue
                if time.time() - _last_signal_ts < COOLDOWN_SEC:
                    await asyncio.sleep(POLL_SEC); continue

                # build impulse decision
                side, entry_ref, reason = should_fire_impulse(closes, IMP_LOOK_MIN, IMP_MOVE_MIN)
                if side:
                    # avoid duplicate price signals
                    if _last_signal_price is not None and abs(entry_ref - _last_signal_price) <= (8.0 * SPREAD_BUF):
                        # too close to last signal -> skip
                        await asyncio.sleep(POLL_SEC); continue

                    # entry mapped to broker scale
                    entry_broker = entry_ref + _offset
                    # build tp/sl (in ref scale), then map
                    tp_ref, sl_ref = build_tp_sl(side, entry_ref)
                    tp_b, sl_b = tp_ref + _offset, sl_ref + _offset

                    # send idea (short), then signal
                    text = (f"🔥 {side} {SYMBOL_NAME} | 1m  ({reason})\n"
                            f"Entry: {rnd(entry_broker)} (ref:{rnd(entry_ref)})\n"
                            f"✅ TP: **{rnd(tp_b)}**\n"
                            f"🟥 SL: **{rnd(sl_b)}**\n"
                            f"RR≈{round((abs(tp_ref-entry_ref)/max(abs(entry_ref-sl_ref),1e-9)),2)} "
                            f" Conf: ~{int(0.6*100)}%")
                    await send_main(text)

                    # log/trade meta
                    _signals_today += 1
                    _last_signal_ts = time.time()
                    _last_signal_price = entry_ref

                    # write to csv as opened (for tracking) — outcome empty until closed
                    append_trade_csv({
                        "ts_open": datetime.utcnow().isoformat(timespec="seconds"),
                        "symbol": "NG",
                        "side": side,
                        "entry": rnd(entry_ref),
                        "tp": rnd(tp_ref),
                        "sl": rnd(sl_ref),
                        "outcome": "",
                        "ts_close": ""
                    })
                else:
                    # maybe tail or nothing
                    # (we don't message tail silence)
                    pass

                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.exception("engine loop error: %s", e)
                await asyncio.sleep(1.5)

# ---------------- MAIN ----------------
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    # start engine
    asyncio.create_task(engine_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
