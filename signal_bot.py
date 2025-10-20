#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, csv, logging, asyncio, random
from datetime import datetime

import pandas as pd
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ===================== VERSION =====================
VERSION = "V7.4-FULL Scalp NG (1m, dynamic TP 10–30, fast poll, auto TP/SL notify & continue)"

# ===================== TOKENS / OWNER =====================
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# ===================== MARKETS / SETTINGS =====================
SYMBOLS = {
    "NG":  {"name": "NATGAS (NG=F)", "tf": "1m"},
}
# спред-буфер под твой спред ~0.004 на NG
SPREAD_BUFFER = {"NG": 0.0040}

# скорость
POLL_SEC        = 0.25   # быстрый опрос — меньше задержки входа
ALIVE_EVERY_SEC = 300
BOOT_COOLDOWN_S = 8
COOLDOWN_SEC    = 0

TRADES_CSV = "gv_trades.csv"

HTTP_TIMEOUT   = 10
YAHOO_RETRIES  = 4
YAHOO_BACKOFF0 = 0.8
YAHOO_JITTER   = 0.35
ROBUST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
    "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9", "Connection": "keep-alive",
}

# ====== SCALP (NG, динамика) ======
SCALP_MODE_ENABLED = True
SCALP_SYMBOL       = "NG"
SCALP_TF_LABEL     = "1m"

# динамический TP/SL
SCALP_TP_MIN_ABS   = 0.0100      # 10 пипсов
SCALP_TP_MAX_ABS   = 0.0300      # 30 пипсов
SCALP_RR_TARGET    = 1.30        # целевой RR (TP/SL) ~1.3

# импульсные фильтры (ослаблены для частоты)
SCALP_MIN_IMPULSE  = 0.0050      # минимум High-Low у «толчковой» свечи
SCALP_MIN_BODY     = 0.0028      # минимум |Close-Open|
SCALP_NEAR_BREAK   = 0.0018      # близость к High/Low толчковой свечи для входа

SCALP_COOLDOWN_SEC = 6           # короткая пауза после сделки
SCALP_MAX_PER_HOUR = 40          # ограничение частоты (антиразгон)

# ===================== STATE =====================
boot_ts = time.time()

trade = {"NG": None}
cooldown_until = {"NG": 0.0}
last_candle_close_ts = {"NG": 0.0}

_prices_cache = {}
mode = "SCALP"      # сразу скальп; есть команды переключения на будущее
requested_mode = "SCALP"

# скальп — частотные лимиты
scalp_cooldown_until = 0.0
scalp_trades_hour_ts = 0.0
scalp_trades_hour_ct = 0

# ===================== TELEGRAM =====================
router = Router()
bot_main = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
bot_log  = Bot(LOG_BOT_TOKEN,  default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()
dp.include_router(router)

async def send_main(text: str):
    try:
        await bot_main.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.error(f"send_main error: {e}")

async def send_log(text: str):
    try:
        await bot_log.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.error(f"send_log error: {e}")

def mode_title(m: str) -> str:
    return {"SCALP": "SCALP NG (1m)"} .get(m, m)

async def _request_mode(new_mode: str, m: Message | None = None):
    global requested_mode, mode
    requested_mode = new_mode
    mode = new_mode
    if m:
        await m.answer(f"✅ Режим {new_mode}: {mode_title(new_mode)}.")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot is alive ({VERSION}).\nНапиши 'команды' чтобы увидеть список.")
    await m.answer(f"✅ Текущий режим: {mode} — {mode_title(mode)}.")

@router.message(F.text.lower() == "команды")
async def cmd_help(m: Message):
    await m.answer(
        "📋 Команды:\n"
        "• /start — запуск\n"
        "• команды — список\n"
        "• скальп — скальпинг NG (1m, динамич. TP 10–30, SL по RR≈1.3)\n"
        "• стоп — закрыть/сбросить и дать короткий кулдаун\n"
        "• статус — диагностика скальпа\n"
        "• тест — тестовый сигнал"
    )

@router.message(F.text.lower() == "скальп")
async def set_scalp(m: Message):
    await _request_mode("SCALP", m)
    await m.answer("🟢 SCALP включен: NG 1m, динамич. TP 10–30, SL по RR≈1.3, быстрый опрос 0.25s.")

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    now = time.time()
    for s in trade.keys():
        trade[s] = None
        cooldown_until[s] = now + 2
    global scalp_cooldown_until
    scalp_cooldown_until = now + 6
    await m.answer("🛑 Остановил. Открытых нет, короткий кулдаун.")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    now = time.time()
    opened = bool(trade["NG"])
    age = int(now - last_candle_close_ts["NG"]) if last_candle_close_ts["NG"] else -1
    scd = max(0, int(scalp_cooldown_until - now))
    lines = [
        f"mode: {mode} (requested: {requested_mode})",
        f"alive: OK | poll={POLL_SEC}s",
        f"{SYMBOLS['NG']['name']}: open={opened}  last_close_age={age}s",
        f"SCALP: TP∈[{SCALP_TP_MIN_ABS:.3f}..{SCALP_TP_MAX_ABS:.3f}]  RR≈{SCALP_RR_TARGET:.2f}  "
        f"cooldown={scd}s  per_hour={scalp_trades_hour_ct}"
    ]
    await m.answer("```\n"+ "\n".join(lines) + "\n```")

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    text = (
        "🔥 BUY NATGAS (NG=F) | 1m\n"
        "✅ TP: **2.9990**\n"
        "🟥 SL: **2.9900**\n"
        "Entry: 2.9945  SpreadBuf≈0.0040  RR≈1.3  Conf: 70%  Bias: UP"
    )
    await m.answer(text)

# ===================== PRICE FEEDS =====================
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
        if not ts or not q:
            return pd.DataFrame()
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

async def get_df_ng(session: aiohttp.ClientSession) -> pd.DataFrame:
    now_ts = time.time()
    c = _prices_cache.get("NG")
    if c and (now_ts - c["ts"] < 1.0) and isinstance(c.get("df"), pd.DataFrame) and not c["df"].empty:
        return c["df"]
    for t in ("NG%3DF",):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d"
        df = _df_from_yahoo_v8(await _yahoo_json(session, url))
        if not df.empty:
            last_candle_close_ts["NG"] = time.time()
            _prices_cache["NG"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
            return df
    return pd.DataFrame()

# ===================== HELPERS =====================
def rnd(x: float) -> float:
    return round(float(x), 4)

def dynamic_tp_sl(prev_range: float) -> tuple[float, float]:
    """
    На базе «силы» прошлой (закрытой) 1m свечи:
    - целевой TP = clamp(prev_range * 0.7, 0.010..0.030)
    - SL из RR: SL = TP / RR
    """
    tp_abs = max(SCALP_TP_MIN_ABS, min(SCALP_TP_MAX_ABS, prev_range * 0.7))
    sl_abs = max(0.0040, tp_abs / max(SCALP_RR_TARGET, 1.01))  # не меньше 4 пипсов
    return tp_abs, sl_abs

def format_signal(side: str, entry: float, tp: float, sl: float, rr: float, conf: float) -> str:
    return (
        f"🔥 {side} {SYMBOLS['NG']['name']} | {SCALP_TF_LABEL}\n"
        f"✅ TP: **{rnd(tp)}**\n"
        f"🟥 SL: **{rnd(sl)}**\n"
        f"Entry: {rnd(entry)}  SpreadBuf≈{rnd(SPREAD_BUFFER['NG'])}  "
        f"RR≈{round(rr,2)}  Conf: {int(conf*100)}%"
    )

def append_trade(row: dict):
    newf = not os.path.exists(TRADES_CSV)
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if newf: w.writeheader()
        w.writerow(row)

async def notify_outcome(outcome: str, price: float):
    name = SYMBOLS["NG"]["name"]; p = rnd(price)
    text = f"✅ TP hit on {name} @ {p}" if outcome=="TP" else f"🟥 SL hit on {name} @ {p}"
    await send_main(text)

def finish_trade(outcome: str, price_now: float):
    sess = trade["NG"]
    trade["NG"] = None
    cooldown_until["NG"] = time.time() + COOLDOWN_SEC
    if not sess: return
    try:
        rr = (sess["tp"]-sess["entry"]) if sess["side"]=="BUY" else (sess["entry"]-sess["tp"])
        rl = (sess["entry"]-sess["sl"]) if sess["side"]=="BUY" else (sess["sl"]-sess["entry"])
        rr_ratio = round(float(rr)/max(float(rl),1e-9), 3)
        append_trade({
            "ts_close": datetime.utcnow().isoformat(timespec="seconds"),
            "symbol": "NG", "side": sess["side"],
            "entry": rnd(sess["entry"]), "tp": rnd(sess["tp"]),
            "sl": rnd(sess["sl"]), "outcome": outcome,
            "rr_ratio": rr_ratio, "life_sec": int(time.time()-sess.get("opened_at", time.time())),
        })
    except Exception as e:
        logging.error(f"log append error: {e}")

# ===================== SCALP BUILDER =====================
def _reset_scalp_hour():
    global scalp_trades_hour_ts, scalp_trades_hour_ct
    now = time.time()
    if now - (scalp_trades_hour_ts or 0.0) >= 3600:
        scalp_trades_hour_ts = now
        scalp_trades_hour_ct = 0

def _ok_scalp_frequency() -> bool:
    _reset_scalp_hour()
    return scalp_trades_hour_ct < SCALP_MAX_PER_HOUR

def build_scalp_setup_ng(df1m: pd.DataFrame) -> dict | None:
    """
    Импульсный скальп:
    — берём последнюю закрытую 1m свечу (i = -2),
    — проверяем импульс (High-Low) и тело,
    — текущая цена должна быть близко к пробою High/Low этой свечи,
    — TP/SL динамически из силы свечи и RR.
    """
    if df1m is None or df1m.empty or len(df1m) < 30:
        return None

    i = len(df1m) - 2
    H = float(df1m["High"].iloc[i])
    L = float(df1m["Low"].iloc[i])
    O = float(df1m["Open"].iloc[i])
    C = float(df1m["Close"].iloc[i])
    rng  = H - L
    body = abs(C - O)
    if (rng < SCALP_MIN_IMPULSE) or (body < SCALP_MIN_BODY):
        return None

    cur = float(df1m["Close"].iloc[-1])
    buf = SPREAD_BUFFER["NG"]

    near_up   = (H - cur) <= SCALP_NEAR_BREAK
    near_down = (cur - L) <= SCALP_NEAR_BREAK

    side = None
    # в лонг — если бычья толчковая и на пробой high; в шорт — если медвежья и на пробой low
    if near_up and C >= O:
        side = "BUY"
    elif near_down and C <= O:
        side = "SELL"
    else:
        return None

    # динамический TP/SL
    tp_abs, sl_abs = dynamic_tp_sl(rng)
    entry = cur

    if side == "BUY":
        tp = entry + tp_abs + buf
        sl = entry - sl_abs - buf
        sl = min(entry - 1e-6, sl)
    else:
        tp = entry - tp_abs - buf
        sl = entry + sl_abs + buf
        sl = max(entry + 1e-6, sl)

    rr = abs(tp - entry) / max(abs(entry - sl), 1e-9)

    # простая уверенность на базе силы свечи
    score = 0
    if body >= SCALP_MIN_BODY:   score += 40
    if rng  >= SCALP_MIN_IMPULSE:score += 40
    if rr <= SCALP_RR_TARGET+0.2:score += 10
    conf = max(0, min(100, score)) / 100.0

    return {
        "side": side, "entry": entry, "tp": tp, "sl": sl,
        "rr": rr, "conf": conf, "tp_abs": abs(tp - entry)
    }

# ===================== ENGINE =====================
async def handle_scalp_ng(session: aiohttp.ClientSession):
    global trade, scalp_cooldown_until, scalp_trades_hour_ct

    if mode != "SCALP" or not SCALP_MODE_ENABLED:
        return

    df = await get_df_ng(session)
    if df.empty or len(df) < 30:
        return

    # сопровождение открытой сделки — проверяем TP/SL на массиве баров после входа
    if trade["NG"]:
        sess = trade["NG"]
        start_i = int(sess.get("entry_bar_idx", len(df)-1))
        post = df.iloc[(start_i + 1):]
        if not post.empty:
            side = sess["side"]; tp = sess["tp"]; sl = sess["sl"]
            hit_tp = (post["High"].max() >= tp) if side=="BUY" else (post["Low"].min() <= tp)
            hit_sl = (post["Low"].min()  <= sl) if side=="BUY" else (post["High"].max() >= sl)
            if hit_tp:
                price_now = float(post["Close"].iloc[-1])
                await notify_outcome("TP", price_now)
                finish_trade("TP", price_now)
                scalp_cooldown_until = time.time() + SCALP_COOLDOWN_SEC
                return
            if hit_sl:
                price_now = float(post["Close"].iloc[-1])
                await notify_outcome("SL", price_now)
                finish_trade("SL", price_now)
                scalp_cooldown_until = time.time() + SCALP_COOLDOWN_SEC
                return
        return

    # нет открытой — можно искать новый вход
    now = time.time()
    if now - boot_ts < BOOT_COOLDOWN_S: return
    if now < scalp_cooldown_until:      return
    if not _ok_scalp_frequency():       return

    setup = build_scalp_setup_ng(df)
    if not setup:
        return

    # защита от «устарело»: текущая цена не должна уйти далеко от entry
    buf = SPREAD_BUFFER["NG"]
    close_now = float(df["Close"].iloc[-1])
    if abs(setup["entry"] - close_now) > 12.0 * buf:
        return

    # шлём вход
    await send_main(format_signal(setup["side"], setup["entry"], setup["tp"], setup["sl"], setup["rr"], setup["conf"]))
    trade["NG"] = {
        "side": setup["side"],
        "entry": float(setup["entry"]),
        "tp": float(setup["tp"]),
        "sl": float(setup["sl"]),
        "opened_at": time.time(),
        "entry_bar_idx": len(df)-1,
    }
    scalp_trades_hour_ct += 1
    # небольшой локальный кулдаун сразу после открытия — от заспама
    scalp_cooldown_until = time.time() + SCALP_COOLDOWN_SEC

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await handle_scalp_ng(session)
                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(1)

# ===================== ALIVE LOOP =====================
async def alive_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                df_ng = await get_df_ng(s)
            c_ng  = float(df_ng["Close"].iloc[-1]) if not df_ng.empty else 0.0
            msg = f"[ALIVE] NG: {rnd(c_ng)}. Mode={mode}. OK."
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
