sudo systemctl stop signaler.service || true
mkdir -p /root/tradebot
cat > /root/tradebot/signal_bot.py <<'PY'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, asyncio, time, json, logging, aiohttp, math
from datetime import datetime, timezone
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command

# ================== НАСТРОЙКИ ==================
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  MAIN_BOT_TOKEN)  # можно одним ботом
OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))

# только золото
NAME     = "GOLD"
Y_SYMS   = ("GC=F","XAUUSD=X")  # фьюч и спот (бэкап)
POLL_SEC = 0.35

# анти-«молчун»: простые пороги для M1
MIN_RANGE_PIPS = 3.0   # свеча должна иметь диапазон >= 3 пунктов
MIN_BODY_PIPS  = 1.0   # тело >= 1 пункт
COOLDOWN_BARS  = 2     # минимум 2 закрытых бара между сигналами
DAY_LIMIT      = 20    # макс сигналов в сутки

# рамки для целей
TP_MIN = 4.0
TP_MAX = 30.0
SL_MIN = 6.0
SL_MAX = 12.0

# ================== ТГ ==================
router = Router()
bot_main = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
bot_log  = Bot(LOG_BOT_TOKEN,  default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher(); dp.include_router(router)

async def send(text:str):
    try:
        await bot_main.send_message(TARGET_CHAT_ID, text)
    except Exception as e:
        logging.error(f"send error: {e}")

# ================== ПРАЙСЫ (Yahoo) ==================
ROBUST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "*/*",
}

async def y_json(session: aiohttp.ClientSession, url: str) -> dict:
    backoff = 0.9
    for _ in range(4):
        try:
            async with session.get(url, headers=ROBUST_HEADERS, timeout=12) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                if r.status in (429,503):
                    await asyncio.sleep(backoff); backoff *= 1.6; continue
                return {}
        except:
            await asyncio.sleep(backoff); backoff *= 1.6
    return {}

async def get_m1(session: aiohttp.ClientSession):
    """Вернёт OHLC списками по закрытым барам (макс 2000), либо None."""
    for sym in Y_SYMS:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1m&range=5d"
        p = await y_json(session, url)
        try:
            r = p["chart"]["result"][0]
            ts = r["timestamp"]; q = r["indicators"]["quote"][0]
            o, h, l, c = q["open"], q["high"], q["low"], q["close"]
            # чистим None и нули, берём последние 2000
            o2=h2=l2=c2=[]; idx=[]
            for i in range(len(c)):
                if all(v is not None and v>0 for v in (o[i],h[i],l[i],c[i])):
                    idx.append(ts[i]); o2.append(o[i]); h2.append(h[i]); l2.append(l[i]); c2.append(c[i])
            if len(c2) > 5:
                return {"t": idx[-2000:], "o": o2[-2000:], "h": h2[-2000:], "l": l2[-2000:], "c": c2[-2000:]}
        except Exception:
            pass
    return None

# ================== ЛОГИКА СИГНАЛОВ ==================
state = {
    "last_closed_idx": -1,
    "last_signal_idx": -9999,
    "day": None,
    "count": 0,
}

def new_day():
    d = datetime.now(timezone.utc).date().isoformat()
    if state["day"] != d:
        state["day"] = d; state["count"] = 0

def pips(a,b):  # в твоём терминале 1 пункт = 1.00 цены
    return abs(float(a)-float(b))

def make_setup(o,h,l,c):
    """Возвращает dict сигнала или None. Очень простой импульс."""
    rng  = pips(h,l)
    body = pips(o,c)
    if rng < MIN_RANGE_PIPS or body < MIN_BODY_PIPS:
        return None

    side = "BUY" if c>o else "SELL"
    entry = c  # вход по цене закрытия бара

    # динамический TP/SL из «силы» свечи, но в границах
    base = max(MIN_RANGE_PIPS, min(12.0, rng))  # ограничим влияние
    tp_size = max(TP_MIN, min(TP_MAX, base*2.2))  # 4..30
    sl_size = max(SL_MIN, min(SL_MAX, base*1.2))  # 6..12

    if side == "BUY":
        tp = entry + tp_size
        sl = entry - sl_size
    else:
        tp = entry - tp_size
        sl = entry + sl_size

    rr = (tp_size / max(sl_size,1e-9))
    return {
        "side": side, "entry": entry, "tp": tp, "sl": sl,
        "tp_size": tp_size, "sl_size": sl_size, "rr": rr
    }

def fmt(s):
    return (f"🔥 {NAME} {s['side']}\n"
            f"Entry: {s['entry']:.2f}\n"
            f"TP: **{s['tp']:.2f}** (+{s['tp_size']:.1f} пп)\n"
            f"SL: **{s['sl']:.2f}** (-{s['sl_size']:.1f} пп)\n"
            f"RR≈{s['rr']:.2f}")

# ================== БОЕВОЙ ЦИКЛ ==================
async def engine():
    await send(f"✅ {NAME} bot online. Порог: body≥{MIN_BODY_PIPS} / range≥{MIN_RANGE_PIPS}, "
               f"cooldown={COOLDOWN_BARS}b, day_limit={DAY_LIMIT}.")
    async with aiohttp.ClientSession() as sess:
        while True:
            try:
                d = await get_m1(sess)
                if not d:
                    await asyncio.sleep(POLL_SEC); continue

                new_day()
                i_last = len(d["c"]) - 1          # индекс текущего формирующегося
                i_closed = i_last - 1             # последний закрытый

                # чтобы не дублить
                if i_closed <= state["last_closed_idx"]:
                    await asyncio.sleep(POLL_SEC); continue
                state["last_closed_idx"] = i_closed

                # кулдаун по барам
                if (i_closed - state["last_signal_idx"]) < COOLDOWN_BARS:
                    await asyncio.sleep(POLL_SEC); continue

                if state["count"] >= DAY_LIMIT:
                    await asyncio.sleep(POLL_SEC); continue

                # берём закрытый бар
                o = d["o"][i_closed]; h = d["h"][i_closed]
                l = d["l"][i_closed]; c = d["c"][i_closed]

                setup = make_setup(o,h,l,c)
                if setup:
                    await send(fmt(setup))
                    state["last_signal_idx"] = i_closed
                    state["count"] += 1

            except Exception as e:
                logging.exception(f"engine error: {e}")
                await asyncio.sleep(1.5)
            await asyncio.sleep(POLL_SEC)

# ================== КОМАНДЫ ==================
@router.message(Command("start"))
async def start(m: Message):
    await m.answer(f"✅ {NAME} bot готов. /status — проверить.")

@router.message(F.text.lower() == "статус")
@router.message(F.text.lower() == "status")
async def st(m: Message):
    await m.answer("`\n"
                   f"mode: {NAME}\n"
                   f"poll={POLL_SEC}s cooldown_bars={COOLDOWN_BARS}\n"
                   f"min_range={MIN_RANGE_PIPS} min_body={MIN_BODY_PIPS}\n"
                   f"signals_today={state['count']} (limit={DAY_LIMIT})\n"
                   "`")

# ================== MAIN ==================
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.create_task(engine())
    await dp.start_polling(bot_main)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
PY

# Проверка синтаксиса
python3 -m py_compile /root/tradebot/signal_bot.py || { echo "Syntax error"; exit 1; }

# Обновим/создадим unit (если у тебя уже есть, просто убедись в пути ExecStart)
cat | sudo tee /etc/systemd/system/signaler.service >/dev/null <<'UNIT'
[Unit]
Description=Signal Bot
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 /root/tradebot/signal_bot.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl start signaler.service
sudo systemctl enable signaler.service
sleep 1
journalctl -u signaler.service -n 30 --no-pager
