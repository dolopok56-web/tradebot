#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, logging, asyncio, random
from datetime import date
import pandas as pd
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ===================== VERSION =====================
VERSION = "Turbo Impuls V1 — NG signals-only (Yahoo 1m, 3–8m impulse)"

# ===================== TOKENS / OWNER =====================
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# ===================== MARKET =====================
SYMBOLS = {"NG": {"name": "NATGAS (NG=F)", "tf": "1m"}}

# ===================== ENGINE / LIMITS =====================
POLL_SEC        = 0.30         # опрос цены
BOOT_COOLDOWN_S = 6            # пауза после старта
ALIVE_EVERY_SEC = 300          # лог-пинг

# анти-спам сигналов
MAX_SIGNALS_PER_DAY = 12
MIN_GAP_SECONDS     = 60       # минимум 1 мин между сигналами

# ===================== IMPULSE LOGIC (минимум «ума») =====================
# окна импульса (минуты)
LOOK_MINS = (3, 4, 5, 6, 7, 8)

# адаптивный шум (медиана последних 24 диапазонов 1m)
NOISE_BARS      = 24
NOISE_FLOOR     = 0.0006       # нижняя планка шума для NG
NOISE_CEIL      = 0.0120       # верхняя планка шума для NG

# триггеры импульса (абсолют/множитель шума)
IMPULSE_ABS_MIN = 0.0080       # 8 пипсов — абсолютный минимум
IMPULSE_K_NOISE = 1.6          # или 1.6 * noise (обычно 8–20 пипсов)

# «окей, можно входить» — хотим увидеть крошечное продолжение, не стоим в противоположный тик
CONT_PROX       = 0.0005       # как близко к хай/лою импульса должна быть текущая цена

# Рекомендации SL/TP (не исполняем сами — только в тексте сигнала)
MIN_RISK        = 0.0040       # рекомендуемый min SL (4 пипса)
MAX_RISK        = 0.0250       # рекомендуемый max SL (25 пипсов)
RR_TARGET       = 1.60         # рекомендуемый RR
TP_MIN          = 0.0100       # минимум TP (10 пипсов)
TP_MAX          = 0.0600       # максимум TP (60 пипсов)

# ===================== HTTP / Yahoo =====================
HTTP_TIMEOUT   = 12
YAHOO_RETRIES  = 4
YAHOO_BACKOFF0 = 0.9
YAHOO_JITTER   = 0.35

ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
    "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9", "Connection": "keep-alive",
}

# ===================== STATE =====================
boot_ts = time.time()

router = Router()
bot_main = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
bot_log  = Bot(LOG_BOT_TOKEN,  default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

last_candle_close_ts = {"NG": 0.0}
cooldown_until = {"NG": 0.0}

_prices_cache = {}
_last_signal_ts = 0.0
_daily = {"date": None, "count": 0}

# ===================== TELEGRAM =====================
async def send_main(text: str):
    try: await bot_main.send_message(TARGET_CHAT_ID, text)
    except Exception as e: logging.error(f"send_main error: {e}")

async def send_log(text: str):
    try: await bot_log.send_message(TARGET_CHAT_ID, text)
    except Exception as e: logging.error(f"send_log error: {e}")

def rnd(x: float, p=4) -> float:
    return round(float(x), p)

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot is alive ({VERSION}).\nНапиши 'команды' чтобы увидеть список.")
    await m.answer(f"✅ Режим: NATGAS (NG=F) — только сигналы по импульсу.")

@router.message(F.text.lower() == "команды")
async def cmd_help(m: Message):
    await m.answer(
        "📋 Команды:\n"
        "• /start — запуск\n"
        "• команды — список\n"
        "• стоп — короткий кулдаун (1 мин)\n"
        "• статус — диагностика\n"
        "• тест — тестовый сигнал (демо)"
    )

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    cooldown_until["NG"] = time.time() + 60
    await m.answer("🛑 Остановил. Кулдаун 60 сек.")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    now = time.time()
    age = int(now - last_candle_close_ts["NG"]) if last_candle_close_ts["NG"] else -1
    cd  = max(0, int(cooldown_until["NG"] - now))
    await m.answer(
        "```\n"
        f"mode: NG (Turbo Impuls)\n"
        f"alive: OK | poll={POLL_SEC}s\n"
        f"cooldown={cd}s  last_close_age={age}s\n"
        f"signals_today={_daily['count']} (limit={MAX_SIGNALS_PER_DAY})\n"
        "```"
    )

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    await m.answer(
        "🔥 BUY NATGAS (NG=F) | 1m (IMP)\n"
        "✅ TP: **3.5120**\n"
        "🟥 SL: **3.5020**\n"
        "Entry: 3.5050  RR≈1.6\n"
        "(signals-only — вход/выход руками)"
    )

# ===================== PRICE: Yahoo 1m =====================
async def _yahoo_json(session: aiohttp.ClientSession, url: str) -> dict:
    backoff = YAHOO_BACKOFF0
    for _ in range(YAHOO_RETRIES):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT, headers=ROBUST_HEADERS) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429, 503):
                    await asyncio.sleep(backoff + (random.random()*YAHOO_JITTER))
                    backoff *= 1.7
                    continue
                return {}
        except:
            await asyncio.sleep(backoff + (random.random()*YAHOO_JITTER))
            backoff *= 1.6
    return {}

def _df_from_yahoo_v8(payload: dict) -> pd.DataFrame:
    try:
        r = payload.get("chart", {}).get("result", [])[0]
        ts = r.get("timestamp", [])
        q  = r.get("indicators", {}).get("quote", [])[0]
        if not ts or not q: return pd.DataFrame()
        df = pd.DataFrame({
            "Open":  q.get("open",  []),
            "High":  q.get("high",  []),
            "Low":   q.get("low",   []),
            "Close": q.get("close", []),
        }, index=pd.to_datetime(ts, unit="s"))
        df = df.ffill().bfill().dropna()
        for col in ("Open","High","Low","Close"):
            df = df[df[col] > 0]
        return df.tail(2000).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

async def get_df(session: aiohttp.ClientSession) -> pd.DataFrame:
    now_ts = time.time()
    c = _prices_cache.get("NG")
    cache_ttl = 0.35
    if c and (now_ts - c["ts"] < cache_ttl) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]
    df = pd.DataFrame()
    for t in ("NG=F",):
        data = await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d")
        df = _df_from_yahoo_v8(data)
        if not df.empty:
            last_candle_close_ts["NG"] = time.time()
            _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
            return df
    return pd.DataFrame()

# ===================== UTILS =====================
def _median(lst):
    s = sorted([x for x in lst if x == x])
    n = len(s)
    if n == 0: return 0.0
    mid = n//2
    return s[mid] if n%2 else (s[mid-1]+s[mid])/2.0

def _noise_med(df1m: pd.DataFrame, bars: int = NOISE_BARS) -> float:
    if df1m is None or df1m.empty or len(df1m) < bars+2: return NOISE_FLOOR
    rngs = (df1m["High"] - df1m["Low"]).tail(bars).astype(float).tolist()
    md = _median(rngs)
    return max(NOISE_FLOOR, min(md, NOISE_CEIL))

# ===================== TURBO IMPULSE =====================
def _find_impulse_setup(df1m: pd.DataFrame) -> dict | None:
    """
    Простая логика:
      1) считаем текущий шум (медиана диапазона 1m)
      2) ищем импульс за одно из окон 3–8 минут: |C_now - C_look| >= max(ABS_MIN, K*noise)
      3) направление = знак дельты
      4) проверка: текущая цена близко к high/low импульсного окна (CONT_PROX)
      5) SL — за недавний локальный экстремум окна, clamp в [MIN_RISK, MAX_RISK]
      6) TP = clamp(RR_TARGET * risk, TP_MIN..TP_MAX)
    """
    if df1m is None or df1m.empty or len(df1m) < max(LOOK_MINS)+10:
        return None

    noise = _noise_med(df1m, NOISE_BARS)
    need = max(IMPULSE_ABS_MIN, IMPULSE_K_NOISE * noise)

    # берём close[-2] как «закрытую» свечу для устойчивости
    c_now_closed = float(df1m["Close"].iloc[-2])
    cur          = float(df1m["Close"].iloc[-1])

    for look in LOOK_MINS:
        c_look = float(df1m["Close"].iloc[-(look+2)])
        delta  = c_now_closed - c_look
        if abs(delta) < need:
            continue

        window = df1m.tail(look+2).copy()
        H = float(window["High"].max())
        L = float(window["Low"].min())

        if delta > 0:
            # BUY: хотим, чтобы текущая цена была недалеко от хая импульса
            if (H - cur) > max(CONT_PROX, noise*0.4):
                continue
            entry = max(cur, H - 0.0001)
            # SL под минимум окна
            sl = L - max(noise*0.6, MIN_RISK - 0.001)
            risk = entry - sl
            if risk < MIN_RISK:
                sl = entry - MIN_RISK; risk = MIN_RISK
            if risk > MAX_RISK:
                continue
            tp = entry + max(TP_MIN, min(TP_MAX, RR_TARGET * risk))
            rr = (tp - entry) / max(risk, 1e-9)
            return {"side":"BUY","entry":entry,"sl":sl,"tp":tp,"rr":rr,"kind":f"IMP{look}m","noise":noise,"need":need}

        else:
            # SELL: хотим быть недалеко от лоя импульса
            if (cur - L) > max(CONT_PROX, noise*0.4):
                continue
            entry = min(cur, L + 0.0001)
            sl = H + max(noise*0.6, MIN_RISK - 0.001)
            risk = sl - entry
            if risk < MIN_RISK:
                sl = entry + MIN_RISK; risk = MIN_RISK
            if risk > MAX_RISK:
                continue
            tp = entry - max(TP_MIN, min(TP_MAX, RR_TARGET * risk))
            rr = (entry - tp) / max(risk, 1e-9)
            return {"side":"SELL","entry":entry,"sl":sl,"tp":tp,"rr":rr,"kind":f"IMP{look}m","noise":noise,"need":need}

    return None

def _format_signal(setup: dict) -> str:
    side = setup["side"]; kind = setup.get("kind","IMP")
    name = SYMBOLS["NG"]["name"]
    return (
        f"🔥 {side} {name} | 1m ({kind})\n"
        f"✅ TP: **{rnd(setup['tp'])}**\n"
        f"🟥 SL: **{rnd(setup['sl'])}**\n"
        f"Entry: {rnd(setup['entry'])}  RR≈{round(setup['rr'],2)}\n"
        f"(signals-only — вход/выход руками)"
    )

# ===================== ENGINE =====================
def _daily_reset_if_needed():
    d = date.today().isoformat()
    if _daily["date"] != d:
        _daily["date"] = d
        _daily["count"] = 0

async def handle_symbol(session: aiohttp.ClientSession):
    global _last_signal_ts

    df = await get_df(session)
    if df.empty or len(df) < max(LOOK_MINS)+10:
        return
    last_candle_close_ts["NG"] = time.time()

    _daily_reset_if_needed()
    now = time.time()
    if _daily["count"] >= MAX_SIGNALS_PER_DAY: return
    if now - boot_ts < BOOT_COOLDOWN_S:        return
    if now < cooldown_until["NG"]:             return
    if now - _last_signal_ts < MIN_GAP_SECONDS:return

    setup = _find_impulse_setup(df)
    if not setup:
        return

    await send_main(_format_signal(setup))
    _daily["count"] += 1
    _last_signal_ts = now

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await handle_symbol(session)
                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(2)

# ===================== ALIVE =====================
async def alive_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                df = await get_df(s)
            c = float(df["Close"].iloc[-1]) if not df.empty else 0.0
            msg = f"[ALIVE] NG: {rnd(c)}  signals_today={_daily['count']} (limit={MAX_SIGNALS_PER_DAY}). OK."
            await send_log(msg)
        except Exception as e:
            await send_log(f"[ALIVE ERROR] {e}")
        await asyncio.sleep(ALIVE_EVERY_SEC)

# ===================== MAIN =====================
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine_loop())
    asyncio.create_task(alive_loop())
    await dp.start_polling(bot_main)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
