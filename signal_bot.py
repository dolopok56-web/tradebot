#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, csv, math, logging, asyncio, re
from datetime import datetime, timezone, timedelta
from copy import deepcopy

import numpy as np
import pandas as pd
import aiohttp

# aiogram 3.7+
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ===================== CONFIG =====================

VERSION = "v14.2 ng-upgrade (stable)"

# !!! не менял твои токены/ID — оставь свои:
BOT_TOKEN = "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"
OWNER_ID  = 6784470762

# подтверждение тренда / пробой структуры — резерв
USE_5M_CONFIRM = True
STRUCT_BARS    = 1

# Минимальные буферы (порог шума)
# (оставил твои базовые + не трогал BTC/XAU)
MIN_SPREAD = {"BTC": 277.5, "NG": 0.006, "XAU": 0.25}

# Динамика буфера: max(MIN, ATR*ATR_K, range(last_closed)*RANGE_K)
ATR_K   = {"BTC": 7.0, "NG": 0.40, "XAU": 0.55}
RANGE_K = {"BTC": 0.90, "NG": 0.85, "XAU": 0.90}

# Доп. подушка к уровням TP/SL (поверх buffer)
BUF_K_TO_LEVEL = {"BTC": 1.15, "NG": 0.25, "XAU": 0.30}

# Базовый минимальный RR и ограничение ширины стопа
RR_MIN     = 1.30
MAX_SL_ATR = 1.40

# === Локальные NG-параметры (BTC/XAU не трогаем) ===
RR_MIN_NG        = 1.10   # разрешить больше сделок по NG
CONF_MIN_NG      = 0.55   # базовый порог уверенности по NG
COOLDOWN_NG      = 8      # сек между перезаходами по NG
IMPULSE_PIPS_NG  = 0.010  # минимальный микродвиж за бар (≈пунктов)
LOOKBACK_BREAK_NG= 6      # пробой max/min за N баров
# Сезонный сдвиг (декабрь—март): допускаем ещё ниже Conf
CONF_MIN_NG_WINTER = 0.50
# «чёрное окно» EIA: по четвергам 14:00 UTC ±30m -> no-trade
EIA_BLACKOUT_MIN  = 30

SYMBOLS = {
    "BTC": {"name": "BTC-USD",   "tf": "1m"},
    "NG":  {"name": "NG=F",      "tf": "1m"},
    "XAU": {"name": "XAUUSD=X",  "tf": "1m"},
}

# Периоды
ATR_PERIOD = 14
EMA_FAST   = 12
EMA_SLOW   = 26

# Тик-цикл
POLL_SEC         = 5
COOLDOWN_SEC     = 20
GUARD_AFTER_SL_S = 15 * 60
BOOT_COOLDOWN_S  = 60

# Сеть/ретраи
HTTP_TIMEOUT = 10
HTTP_RETRIES = 3
HTTP_RETRY_SLEEP = 0.8

# Файлы
STATS_JSON = "gv_stats.json"
STATE_JSON = "gv_state.json"
TRADES_CSV = "gv_trades.csv"

# Источники цен
CC_URL           = "https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=200"
YF_CHART_TPL     = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=6h"
STOOQ_TPL        = "https://stooq.com/q/d/l/?s={ticker}&i=1"

# ===================== STATE =====================

mode = "BTC"
requested_mode = "BTC"
last_mode_switch_ts = 0.0
MODE_SWITCH_DEBOUNCE = 2.0

trade = {"BTC": None, "NG": None, "XAU": None}
cooldown_until = {"BTC": 0, "NG": 0, "XAU": 0}
last_candle_close_ts = {"BTC": 0, "NG": 0, "XAU": 0}
boot_ts = time.time()

last_seen_idx   = {"BTC": -1, "NG": -1, "XAU": -1}
last_signal_idx = {"BTC": -1, "NG": -1, "XAU": -1}

DEFAULT_PARAMS = {
    "tp_mul": 1.80, "sl_mul": 1.20, "risk": 1.00,
    "win": 0, "loss": 0, "streak_win": 0, "streak_loss": 0,
    "last_sig": None, "last_sig_ts": 0, "last_outcome": None,
}

def load_json(path, default):
    if not os.path.exists(path): return deepcopy(default)
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except: return deepcopy(default)

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"save_json({path}) -> {e}")

stats = load_json(STATS_JSON, {})
state  = load_json(STATE_JSON, {"mode": mode})

def save_stats(): save_json(STATS_JSON, stats)
def save_state():
    state["mode"] = mode
    save_json(STATE_JSON, state)

def append_trade(row):
    newf = not os.path.exists(TRADES_CSV)
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if newf: w.writeheader()
        w.writerow(row)

# ===================== TECH =====================

def now_iso():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="seconds")

def ema(series: pd.Series, span: int):
    return series.ewm(span=span, adjust=False).mean()

def true_range(h, l, c):
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr

def atr(df, n):
    return true_range(df["High"], df["Low"], df["Close"]).rolling(n).mean()

def rnd(sym: str, x: float):
    return round(float(x), 1 if sym == "BTC" else (4 if sym == "NG" else 2))

def sig_id(symbol, side, trend, atr_val, tf):
    return f"{symbol}|{side}|{trend}|{round(float(atr_val),4)}|{tf}"

def allow_after_sl(symbol, signature, ts):
    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    return not (p.get("last_outcome") == "SL" and p.get("last_sig") == signature and ts - float(p.get("last_sig_ts",0)) < GUARD_AFTER_SL_S)

# ===================== PRICE FETCH =====================

async def _http_get_text(session: aiohttp.ClientSession, url: str) -> str:
    for i in range(HTTP_RETRIES):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT) as r:
                if r.status == 200:
                    return await r.text()
        except Exception as e:
            logging.warning(f"GET text fail [{i+1}/{HTTP_RETRIES}] {url}: {e}")
        await asyncio.sleep(HTTP_RETRY_SLEEP)
    return ""

async def _http_get_json(session: aiohttp.ClientSession, url: str) -> dict:
    for i in range(HTTP_RETRIES):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT) as r:
                if r.status == 200:
                    return await r.json()
        except Exception as e:
            logging.warning(f"GET json fail [{i+1}/{HTTP_RETRIES}] {url}: {e}")
        await asyncio.sleep(HTTP_RETRY_SLEEP)
    return {}

def df_from_stooq_csv(text: str):
    try:
        from io import StringIO
        df = pd.read_csv(StringIO(text))
        # stooq столбцы: Date,Time,Open,High,Low,Close,Volume
        if not {"Open","High","Low","Close"}.issubset(df.columns): return pd.DataFrame()
        return df.tail(300).reset_index(drop=True)
    except:
        return pd.DataFrame()

def df_from_yahoo_chart(js: dict):
    try:
        res = js.get("chart", {}).get("result", [])
        if not res: return pd.DataFrame()
        res = res[0]
        ts  = res.get("timestamp", [])
        ind = res.get("indicators", {}).get("quote", [])
        if not ts or not ind: return pd.DataFrame()
        q = ind[0]
        df = pd.DataFrame({
            "Open":  q.get("open", []),
            "High":  q.get("high", []),
            "Low":   q.get("low", []),
            "Close": q.get("close", []),
        })
        # убрать None
        df = df.replace({None: np.nan}).fillna(method="ffill").fillna(method="bfill")
        df = df[["Open","High","Low","Close"]].tail(300).reset_index(drop=True)
        return df
    except Exception as e:
        logging.warning(f"Yahoo parse fail: {e}")
        return pd.DataFrame()

async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
    if symbol == "BTC":
        data = await _http_get_json(session, CC_URL)
        try:
            if data.get("Response") != "Success": return pd.DataFrame()
            arr = data["Data"]["Data"]
            df = pd.DataFrame(arr)
            if "time" in df.columns:
                last_ts = int(df["time"].iloc[-1])
                last_candle_close_ts["BTC"] = last_ts
            df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
            return df[["Open","High","Low","Close"]].tail(300).reset_index(drop=True)
        except Exception as e:
            logging.warning(f"BTC df parse error: {e}")
            return pd.DataFrame()

    if symbol == "NG":
        # 1) Yahoo v8
        js = await _http_get_json(session, YF_CHART_TPL.format(ticker="NG%3DF"))
        df = df_from_yahoo_chart(js)
        if not df.empty:
            last_candle_close_ts["NG"] = int(time.time())
            return df
        # 2) fallback: stooq
        txt = await _http_get_text(session, STOOQ_TPL.format(ticker="ng.f"))
        df = df_from_stooq_csv(txt)
        if not df.empty:
            last_candle_close_ts["NG"] = int(time.time())
        return df

    if symbol == "XAU":
        # 1) Yahoo v8 для XAUUSD=X
        js = await _http_get_json(session, YF_CHART_TPL.format(ticker="XAUUSD%3DX"))
        df = df_from_yahoo_chart(js)
        if not df.empty:
            last_candle_close_ts["XAU"] = int(time.time())
            return df
        # 2) fallback stooq
        txt = await _http_get_text(session, STOOQ_TPL.format(ticker="xauusd"))
        df = df_from_stooq_csv(txt)
        if not df.empty:
            last_candle_close_ts["XAU"] = int(time.time())
        return df

    return pd.DataFrame()

def dynamic_buffer(symbol: str, df: pd.DataFrame, atr_now: float) -> float:
    try:
        rng = float(df["High"].iloc[-2] - df["Low"].iloc[-2])  # закрытый бар
    except Exception:
        rng = 0.0
    base = MIN_SPREAD.get(symbol, 0.0)
    from_atr   = float(atr_now) * ATR_K.get(symbol, 1.0)
    from_range = float(rng)     * RANGE_K.get(symbol, 1.0)
    return max(base, from_atr, from_range)

# ===================== STRATEGY =====================

def trend_side(df: pd.DataFrame) -> str:
    c = df["Close"]
    return "UP" if ema(c, EMA_FAST).iloc[-1] > ema(c, EMA_SLOW).iloc[-1] else "DOWN"

def _confidence(rr: float, trend_ok: bool, atr_now: float, symbol: str) -> float:
    rr_part = max(0.0, min(1.0, (rr - 1.0) / 1.2))  # RR 1.0->0 ; 2.2->1
    trend_part = 0.1 if trend_ok else 0.0
    tiny_atr_penalty = 0.05 if atr_now < (MIN_SPREAD.get(symbol, 0.0) * 0.2) else 0.0
    conf = rr_part + trend_part - tiny_atr_penalty
    return max(0.0, min(1.0, conf))

def _is_winter_month(utc_dt: datetime) -> bool:
    return utc_dt.month in (12,1,2,3)

def _in_eia_blackout(utc_dt: datetime) -> bool:
    # Четверг, 14:00 UTC ±30 минут
    if utc_dt.weekday() != 3:  # 0=Mon ... 3=Thu
        return False
    target = utc_dt.replace(hour=14, minute=0, second=0, microsecond=0)
    return abs((utc_dt - target).total_seconds()) <= EIA_BLACKOUT_MIN * 60

def _ng_filters(df: pd.DataFrame, symbol: str) -> bool:
    """
    Доп. NG-фильтры:
    - минимальный импульс за закрытый бар
    - пробой локального high/low за LOOKBACK_BREAK_NG
    """
    if symbol != "NG": return True
    if len(df) < LOOKBACK_BREAK_NG + 3: return False
    last = df.iloc[-2]  # закрытый
    prev = df.iloc[-(LOOKBACK_BREAK_NG+2):-2]
    impulse = abs(float(last["Close"]) - float(last["Open"]))
    if impulse < IMPULSE_PIPS_NG:
        return False
    # пробой
    hh = prev["High"].max()
    ll = prev["Low"].min()
    broke_up   = float(last["High"]) >= hh
    broke_down = float(last["Low"])  <= ll
    return broke_up or broke_down

def build_setup(df: pd.DataFrame, symbol: str, tf_label: str):
    if df is None or df.empty or len(df) < max(ATR_PERIOD, EMA_SLOW) + 3: return None

    # NG спец-фильтр по импульсу/пробою
    if not _ng_filters(df, symbol):
        return None

    df = df.copy()
    df["ATR"] = atr(df, ATR_PERIOD)
    atr_now = float(df["ATR"].iloc[-1])
    if not np.isfinite(atr_now) or atr_now <= 0: return None

    side_tr = trend_side(df)
    last = df.iloc[-2]                    # закрытый бар
    entry = float(last["Close"])

    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    tp_mul = float(p["tp_mul"]) * float(p["risk"])
    sl_mul = float(p["sl_mul"]) * float(1.0 / p["risk"])

    if side_tr == "UP":
        side = "BUY"
        raw_tp = entry + tp_mul * atr_now
        raw_sl = entry - sl_mul * atr_now
        sl_cap = entry - MAX_SL_ATR * atr_now
        sl = max(raw_sl, sl_cap)
        tp = raw_tp
    else:
        side = "SELL"
        raw_tp = entry - tp_mul * atr_now
        raw_sl = entry + sl_mul * atr_now
        sl_cap = entry + MAX_SL_ATR * atr_now
        sl = min(raw_sl, sl_cap)
        tp = raw_tp

    rr = abs(tp - entry) / max(abs(entry - sl), 1e-9)

    # Базовый RR
    rr_required = RR_MIN_NG if symbol == "NG" else RR_MIN
    if rr < rr_required: return None

    # Уверенность: для NG порог динамический (зима мягче)
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc)
    conf_min = (CONF_MIN_NG_WINTER if (symbol=="NG" and _is_winter_month(utc_now))
                else (CONF_MIN_NG if symbol=="NG" else 0.70))
    conf = _confidence(rr, True, atr_now, symbol)
    if conf < conf_min:
        return None

    signature = sig_id(symbol, side, side_tr, atr_now, tf_label)
    return {
        "symbol": symbol, "tf": tf_label,
        "side": side, "trend": side_tr,
        "entry": entry, "tp": tp, "sl": sl,
        "atr": atr_now, "rr": rr, "conf": conf, "sig": signature
    }

def format_signal(setup, buffer):
    sym = setup["symbol"]; side = setup["side"]; tf = setup["tf"]
    add = BUF_K_TO_LEVEL.get(sym, 1.0) * buffer
    tp = setup["tp"] + (add if side == "BUY" else -add)
    sl = setup["sl"] - (add if side == "BUY" else -add)
    lines = [
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}",
        f"✅ TP: **{rnd(sym, tp)}**",
        f"🟥 SL: **{rnd(sym, sl)}**",
        f"Entry: {rnd(sym, setup['entry'])}  Spread≈{rnd(sym, buffer)}  ATR(14)≈{rnd(sym, setup['atr'])}  Conf: {int(setup['conf']*100)}%  Trend: {setup['trend']}"
    ]
    return "\n".join(lines)

# ===================== LEARNING =====================

def learn(symbol: str, outcome: str, sess: dict):
    p = stats.get(symbol, deepcopy(DEFAULT_PARAMS))
    if sess and "sig" in sess:
        p["last_sig"] = sess["sig"]; p["last_sig_ts"] = time.time(); p["last_outcome"] = outcome

    if outcome == "TP":
        p["win"] += 1; p["streak_win"] += 1; p["streak_loss"] = 0
        p["risk"]   = float(min(1.30, p["risk"] + 0.04))
        p["tp_mul"] = float(min(2.10, p["tp_mul"] * 1.02))
        p["sl_mul"] = float(max(1.10, p["sl_mul"] * 0.995))
    else:
        p["loss"] += 1; p["streak_loss"] += 1; p["streak_win"] = 0
        p["sl_mul"] = float(min(1.60, p["sl_mul"] * 1.04))
        p["risk"]   = float(max(0.80, p["risk"] - 0.04))
        if p["streak_loss"] >= 3:
            cooldown_until[symbol] = max(cooldown_until[symbol], time.time() + 60)

    stats[symbol] = p; save_stats()

def finish_trade(symbol: str, outcome: str, price_now: float):
    sess = trade[symbol]
    trade[symbol] = None
    # локальный кулдаун для NG
    if symbol == "NG":
        cooldown_until[symbol] = time.time() + COOLDOWN_NG
    else:
        cooldown_until[symbol] = time.time() + COOLDOWN_SEC

    if not sess: return
    try:
        rr = (sess["tp"]-sess["entry"]) if sess["side"]=="BUY" else (sess["entry"]-sess["tp"])
        rl = (sess["entry"]-sess["sl"]) if sess["side"]=="BUY" else (sess["sl"]-sess["entry"])
        append_trade({
            "ts_close": now_iso(), "symbol": symbol, "side": sess["side"],
            "entry": rnd(symbol, sess["entry"]), "tp": rnd(symbol, sess["tp"]),
            "sl": rnd(symbol, sess["sl"]), "outcome": outcome,
            "rr_ratio": round(float(rr)/max(float(rl),1e-9), 3),
            "life_sec": int(time.time()-sess.get("opened_at", time.time())),
            "sig": sess.get("sig","")
        })
    except Exception as e:
        logging.error(f"log append error: {e}")
    learn(symbol, outcome, sess)

# ===================== TELEGRAM =====================

router = Router()
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
dp  = Dispatcher()
dp.include_router(router)

def mode_title(m):
    return {"BTC":"BITCOIN (BTC-USD)","NG":"NATGAS (NG=F)","XAU":"GOLD (XAUUSD)","AUTO":"NATGAS+GOLD (AUTO)"}\
        .get(m,m)

async def _request_mode(new_mode: str, m: Message = None):
    global requested_mode
    requested_mode = new_mode
    save_state()
    if m:
        await m.answer(f"✅ Режим {new_mode}: слежу за {mode_title(new_mode)}.")

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"✅ Bot is alive ({VERSION}).\nНапиши 'команды' чтобы увидеть список.\nПо умолчанию режим BTC.")
    await m.answer(f"✅ Режим {mode}: слежу за {mode_title(mode)}.")

@router.message(F.text.lower() == "команды")
async def cmd_help(m: Message):
    await m.answer(
        "📋 Команды:\n"
        "• /start — запуск\n"
        "• команды — список\n"
        "• биток / газ / золото / авто — выбор рынка\n"
        "• стоп — стоп и короткий кулдаун\n"
        "• статус — диагностика\n"
        "• отчет — 10 последних закрытий (только владелец)\n"
        "• тест — вывести тест-сигнал (диагностика форматирования)"
    )

@router.message(F.text.lower() == "биток")
async def set_btc(m: Message): await _request_mode("BTC", m)

@router.message(F.text.lower() == "газ")
async def set_ng(m: Message):  await _request_mode("NG", m)

@router.message(F.text.lower() == "золото")
async def set_xau(m: Message): await _request_mode("XAU", m)

@router.message(F.text.lower() == "авто")
async def set_auto(m: Message): await _request_mode("AUTO", m)

@router.message(F.text.lower() == "стоп")
async def cmd_stop(m: Message):
    for s in trade.keys(): trade[s] = None; cooldown_until[s] = time.time() + 5
    await m.answer("🛑 Остановил. Открытых нет, короткий кулдаун.")

@router.message(F.text.lower() == "статус")
async def cmd_status(m: Message):
    lines = [f"mode: {mode} (requested: {requested_mode})"]
    now = time.time()
    for s in ["BTC","NG","XAU"]:
        opened = bool(trade[s])
        age = int(now - last_candle_close_ts[s]) if last_candle_close_ts[s] else -1
        atrtxt = state.get(f"atr_{s}", "—")
        lines.append(f"{SYMBOLS[s]['name']}: open={opened}  cooldown={max(0,int(cooldown_until[s]-now))}  ATR≈{atrtxt}  last_close_age={age}s")
    await m.answer("```\n"+ "\n".join(lines) + "\n```")

@router.message(F.text.lower() == "отчет")
async def cmd_report(m: Message):
    if m.from_user.id != OWNER_ID: return await m.answer("Доступно только владельцу.")
    if not os.path.exists(TRADES_CSV): return await m.answer("Пока нет закрытых сделок.")
    rows = list(csv.DictReader(open(TRADES_CSV,encoding="utf-8")))[-10:]
    if not rows: return await m.answer("Пусто.")
    txt = "Последние 10 закрытий:\n"
    for r in rows:
        txt += (f"{r['ts_close']}  {r['symbol']}  {r['side']}  {r['outcome']}  "
                f"entry:{r['entry']} tp:{r['tp']} sl:{r['sl']} rr:{r['rr_ratio']}\n")
    await m.answer("```\n"+txt+"```")

@router.message(F.text.lower() == "тест")
async def cmd_test(m: Message):
    text = (
        "🔥 BUY BTC-USD | 1m\n"
        "✅ TP: **114999.9**\n"
        "🟥 SL: **114111.1**\n"
        "Entry: 114555.5  Spread≈350.0  ATR(14)≈45.0  Conf: 72%  Trend: UP"
    )
    await m.answer(text)

# ===================== NOTIFY =====================

async def send_signal(symbol: str, setup: dict, buffer: float):
    try: await bot.send_message(OWNER_ID, format_signal(setup, buffer))
    except Exception as e: logging.warning(f"send_signal fail: {e}")

async def notify_hit(symbol: str, outcome: str, price: float):
    name = SYMBOLS[symbol]["name"]; p = rnd(symbol, price)
    text = f"✅ TP hit on {name} @ {p}" if outcome=="TP" else f"🟥 SL hit on {name} @ {p}"
    try: await bot.send_message(OWNER_ID, text)
    except Exception as e: logging.warning(f"notify fail: {e}")

# ===================== ENGINE =====================

def _active_symbols_for_mode(md: str):
    if md == "AUTO":
        return ["NG","XAU"]
    if md == "BTC":
        return ["BTC"]
    if md == "NG":
        return ["NG"]
    if md == "XAU":
        return ["XAU"]
    return ["BTC"]

def _apply_mode_change_if_needed():
    global mode, last_mode_switch_ts
    now = time.time()
    if requested_mode != mode and (now - last_mode_switch_ts) >= MODE_SWITCH_DEBOUNCE:
        old = mode
        mode = requested_mode
        last_mode_switch_ts = now
        logging.info(f"MODE SWITCH: {old} -> {mode}")

async def handle_symbol(session: aiohttp.ClientSession, symbol: str):
    global last_candle_close_ts, last_seen_idx, last_signal_idx

    if mode != "AUTO" and mode != symbol: return

    # NG: EIA blackout
    if symbol == "NG" and _in_eia_blackout(datetime.utcnow().replace(tzinfo=timezone.utc)):
        return

    df = await get_df(session, symbol)
    if df.empty or len(df) < max(ATR_PERIOD, EMA_SLOW) + 3:
        return

    last_candle_close_ts[symbol] = time.time()
    atr_now = float(atr(df, ATR_PERIOD).iloc[-1])
    state[f"atr_{symbol}"] = rnd(symbol, atr_now)

    # Heartbeat
    logging.info(f"HB {symbol}: last_close={rnd(symbol, float(df['Close'].iloc[-1]))} ATR≈{rnd(symbol, atr_now)}")

    # ===== Анти «конец свечи»: обрабатываем только НОВЫЙ закрытый бар =====
    cur_idx = len(df) - 1
    closed_idx = cur_idx - 1
    if closed_idx <= last_seen_idx[symbol]:
        return
    last_seen_idx[symbol] = closed_idx

    # === если сделка открыта — TP/SL по High/Low ПОСЛЕ входа ===
    sess = trade[symbol]
    if sess:
        start_i = int(sess.get("entry_bar_idx", cur_idx))
        post = df.iloc[(start_i + 1):]
        if not post.empty:
            side = sess["side"]; tp = sess["tp"]; sl = sess["sl"]
            if side == "BUY":
                hit_tp = post["High"].max() >= tp
                hit_sl = post["Low"].min()  <= sl
            else:
                hit_tp = post["Low"].min()  <= tp
                hit_sl = post["High"].max() >= sl
            if hit_tp:
                price_now = float(post["Close"].iloc[-1])
                await notify_hit(symbol, "TP", price_now)
                finish_trade(symbol, "TP", price_now)
                return
            if hit_sl:
                price_now = float(post["Close"].iloc[-1])
                await notify_hit(symbol, "SL", price_now)
                finish_trade(symbol, "SL", price_now)
                return
        return

    # анти вход сразу после старта + кулдаун
    if time.time() - boot_ts < BOOT_COOLDOWN_S: return
    if time.time() < cooldown_until[symbol]: return

    setup = build_setup(df, symbol, SYMBOLS[symbol]["tf"])
    if not setup: return

    if last_signal_idx[symbol] == closed_idx:
        return
    last_signal_idx[symbol] = closed_idx

    if not allow_after_sl(symbol, setup["sig"], time.time()): return

    # Динамический buffer + подушка
    base_buffer = dynamic_buffer(symbol, df, setup["atr"])
    add = BUF_K_TO_LEVEL.get(symbol, 1.0) * base_buffer
    side = setup["side"]
    tp = setup["tp"] + (add if side=="BUY" else -add)
    sl = setup["sl"] - (add if side=="BUY" else -add)

    # отправить сигнал и открыть «сессию»
    await send_signal(symbol, setup, base_buffer)
    trade[symbol] = {
        "side": side,
        "entry": float(setup["entry"]),
        "tp": float(tp),
        "sl": float(sl),
        "sig": setup["sig"],
        "opened_at": time.time(),
        "entry_bar_idx": cur_idx
    }

async def engine_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                _apply_mode_change_if_needed()
                for s in _active_symbols_for_mode(mode):
                    await handle_symbol(session, s)
                await asyncio.sleep(POLL_SEC)
            except Exception as e:
                logging.error(f"engine error: {e}")
                await asyncio.sleep(2)

# ===================== MAIN =====================

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    global mode, requested_mode, last_mode_switch_ts
    mode = state.get("mode","BTC")
    requested_mode = mode
    last_mode_switch_ts = time.time()
    asyncio.create_task(engine_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
