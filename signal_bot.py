 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/signal_bot.py b/signal_bot.py
index 590fd3707d6cedd385a2c7671d661047ddc96f28..2a5027efbc37261e502f57dbaed8b3042e16a95a 100644
--- a/signal_bot.py
+++ b/signal_bot.py
@@ -14,68 +14,73 @@ VERSION = "Turbo Impuls V2 — NG-only, Impulse + Pullback, signals-only"
 
 # ===================== TOKENS / OWNER =====================
 MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
 OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
 TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))
 
 # ===================== MARKET =====================
 SYMBOL_NAME = "NATGAS (NG=F)"
 YAHOO_CODE  = "NG=F"
 
 # ===================== IMPULSE SETTINGS (агро по умолчанию) =====================
 # Смотрим импульс за последние L минут и отдаём сигнал, если дельта >= MOVE_MIN
 IMP_LOOK_MIN   = 5          # окно импульса, минут
 IMP_MOVE_MIN   = 0.0060     # ~6 пипсов — агрессивно. Если пусто, опусти до 0.0050
 # Допуск на «отскок» после импульса: если была сильная дельта вниз,
 # и цена отскакивает на PULLBACK_MIN — даём контр-сигнал.
 PULLBACK_MIN   = 0.0020     # 2 пипса быстрым рывком в обратку
 PULLBACK_LAG_S = 120        # смотреть отскок в течение 2 минут после импульса
 # Буфер пробоя (чтобы не триггерить на микро-тика): используем как анти-микрошум
 BREAK_BUFFER   = 0.0015
 
 # ===================== RISK / TARGET =====================
 SPREAD_BUF     = 0.0040     # реальный спред по NG, учтём в SL/TP
 SL_MIN         = 0.0100     # 10 пипсов — минимум, чтобы не срывало пылью
 RR_TARGET      = 1.6        # целевой RR ~1.6
-TP_CAP         = 0.0300     # не гонимся выше 30 пипсов по умолчанию
+TP_MIN         = 0.0110     # минимум 11 пипсов, чтобы бот всегда брал ощутимый профит
+TP_CAP         = 0.0300     # не гонимся выше 30 пипсов по умолчанию
 
 # ===================== ANTI-SPAM =====================
 POLL_SEC             = 0.8   # частота цикла
 COOLDOWN_SEC         = 45    # пауза между сигналами
-MAX_SIGNALS_PER_DAY  = 16    # мягкий дневной лимит
-DUP_TOL              = 8.0 * SPREAD_BUF   # не дублируем сигналы рядом по цене
+MAX_SIGNALS_PER_DAY  = 16    # мягкий дневной лимит
+DUP_TOL              = 8.0 * SPREAD_BUF   # не дублируем сигналы рядом по цене
+
+# ===================== NOTIFICATIONS =====================
+HEARTBEAT_INTERVAL   = 600   # раз в 10 минут отчёт, что бот жив
 
 # ===================== STATE =====================
 boot_ts = time.time()
 router = Router()
 bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
 dp = Dispatcher()
 dp.include_router(router)
 
 signals_today = 0
 last_signal_ts = 0.0
-last_signal_price = None
+last_signal_price = None
+last_signal_side = None
 
 last_impulse_side = None         # "BUY"/"SELL"
 last_impulse_ts   = 0.0
 last_impulse_price= None
 last_seen_minute  = None         # для ресета дневного лимита
 
 # кэш минутных данных
 _last_df = None
 _last_df_ts = 0.0
 _df_cache_ttl = 3.0  # секунды
 
 # ===================== TELEGRAM =====================
 async def send_msg(text: str):
     try:
         await bot.send_message(TARGET_CHAT_ID, text)
     except Exception as e:
         logging.error(f"send_msg error: {e}")
 
 @router.message(Command("start"))
 async def cmd_start(m: Message):
     await m.answer(f"✅ Bot is alive ({VERSION}).\nКоманды: статус, агро, норм, лайт, стоп, тест")
 
 @router.message(F.text.lower() == "стоп")
 async def cmd_stop(m: Message):
     global last_signal_ts
@@ -148,106 +153,121 @@ async def fetch_yahoo_1m(session: aiohttp.ClientSession):
     """ Возвращает (list_of_closes, last_price, last_timestamp) за последние ~90 минут. """
     url = f"https://query1.finance.yahoo.com/v8/finance/chart/{YAHOO_CODE}?interval=1m&range=90m"
     for _ in range(3):
         try:
             async with session.get(url, headers=Y_HEADERS, timeout=10) as r:
                 if r.status != 200:
                     await asyncio.sleep(0.6); continue
                 j = await r.json(content_type=None)
                 res = j["chart"]["result"][0]
                 closes = res["indicators"]["quote"][0]["close"]
                 ts = res["timestamp"]
                 meta = res.get("meta", {})
                 last_price = meta.get("regularMarketPrice", None)
                 if not closes or not ts: 
                     await asyncio.sleep(0.6); continue
                 # чистим нули
                 closes = [c for c in closes if c is not None and c > 0]
                 if not closes: 
                     await asyncio.sleep(0.6); continue
                 return closes, float(last_price or closes[-1]), ts[-1]
         except Exception:
             await asyncio.sleep(0.6)
     return [], 0.0, 0
 
 # ===================== SIGNAL BUILDER =====================
-def build_tp_sl(side: str, entry: float) -> tuple[float, float]:
-    """ SL – минимум 10 пипсов + спред; TP – RR≈1.6 c верхним колпаком. """
-    if side == "BUY":
-        sl = entry - SL_MIN - SPREAD_BUF
-        sl = min(sl, entry - 1e-6)
-        risk = entry - sl
-        tp  = entry + RR_TARGET * risk
-        tp  = min(tp, entry + TP_CAP)
-    else:
-        sl = entry + SL_MIN + SPREAD_BUF
-        sl = max(sl, entry + 1e-6)
-        risk = sl - entry
-        tp  = entry - RR_TARGET * risk
-        tp  = max(tp, entry - TP_CAP)
-    return tp, sl
-
-def fmt(p: float) -> float:
-    return round(float(p), 4)
-
-async def fire_signal(side: str, entry: float):
-    global signals_today, last_signal_ts, last_signal_price
+def build_tp_sl(side: str, entry: float) -> tuple[float, float]:
+    """ SL – минимум 10 пипсов + спред; TP – RR≈1.6 c верхним колпаком. """
+    if side == "BUY":
+        sl = entry - SL_MIN - SPREAD_BUF
+        sl = min(sl, entry - 1e-6)
+        risk = entry - sl
+        tp  = entry + RR_TARGET * risk
+        tp  = min(tp, entry + TP_CAP)
+        tp  = max(tp, entry + TP_MIN)
+    else:
+        sl = entry + SL_MIN + SPREAD_BUF
+        sl = max(sl, entry + 1e-6)
+        risk = sl - entry
+        tp  = entry - RR_TARGET * risk
+        tp  = max(tp, entry - TP_CAP)
+        tp  = min(tp, entry - TP_MIN)
+    return tp, sl
+
+def fmt(p: float) -> float:
+    return round(float(p), 4)
+
+def fmt_since(ts: float) -> str:
+    if ts <= 0:
+        return "никогда"
+    delta = max(0, int(time.time() - ts))
+    minutes, seconds = divmod(delta, 60)
+    hours, minutes = divmod(minutes, 60)
+    if hours:
+        return f"{hours}ч {minutes}м {seconds}с назад"
+    if minutes:
+        return f"{minutes}м {seconds}с назад"
+    return f"{seconds}с назад"
+
+async def fire_signal(side: str, entry: float):
+    global signals_today, last_signal_ts, last_signal_price, last_signal_side
     signals_today += 1
     last_signal_ts = time.time()
-    last_signal_price = entry
+    last_signal_price = entry
+    last_signal_side = side
 
     tp, sl = build_tp_sl(side, entry)
     text = (
         f"🔥 {side} {SYMBOL_NAME} | 1m (Turbo)\n"
         f"✅ TP: **{fmt(tp)}**\n"
         f"🟥 SL: **{fmt(sl)}**\n"
         f"Entry: {fmt(entry)}  SpreadBuf≈{fmt(SPREAD_BUF)}  "
         f"day_ct={signals_today}/{MAX_SIGNALS_PER_DAY}"
     )
     await send_msg(text)
 
 def reset_day_if_needed(last_ts: int):
     global signals_today, last_impulse_side, last_impulse_ts, last_impulse_price, last_seen_minute
     # ts у Yahoo — в секундах UTC; определим день
     cur_day = datetime.datetime.utcnow().date()
     if last_seen_minute is None:
         last_seen_minute = cur_day
         return
     if cur_day != last_seen_minute:
         # новый день — сброс лимита и импульс-контекста
         signals_today = 0
         last_impulse_side = None
         last_impulse_ts   = 0.0
         last_impulse_price= None
         last_seen_minute  = cur_day
 
 # ===================== ENGINE =====================
-async def engine_loop():
-    global _last_df, _last_df_ts, last_impulse_side, last_impulse_ts, last_impulse_price
-
-    async with aiohttp.ClientSession() as session:
-        while True:
+async def engine_loop():
+    global _last_df, _last_df_ts, last_impulse_side, last_impulse_ts, last_impulse_price
+
+    async with aiohttp.ClientSession() as session:
+        while True:
             try:
                 # 1) фетчим минутные цены (кратно минуте) + last price (почти realtime)
                 now = time.time()
                 need_fetch = (_last_df is None) or (now - _last_df_ts > _df_cache_ttl)
                 if need_fetch:
                     closes, last_price, last_ts = await fetch_yahoo_1m(session)
                     if closes:
                         _last_df = closes
                         _last_df_ts = now
                         reset_day_if_needed(last_ts)
                 else:
                     closes = _last_df
                     last_price = closes[-1] if closes else 0.0
                     last_ts = int(now)
 
                 if not closes or last_price <= 0:
                     await asyncio.sleep(POLL_SEC); continue
 
                 # 2) анти-спам
                 if signals_today >= MAX_SIGNALS_PER_DAY:
                     await asyncio.sleep(POLL_SEC); continue
                 if time.time() - last_signal_ts < COOLDOWN_SEC:
                     await asyncio.sleep(POLL_SEC); continue
 
                 # 3) импульс за L минут
@@ -279,40 +299,62 @@ async def engine_loop():
 
                     # 4) отскок после последнего импульса в противоположную сторону
                     if last_impulse_side is not None and (time.time() - last_impulse_ts <= PULLBACK_LAG_S):
                         # если был импульс вниз — ждём быстрый аптик
                         if last_impulse_side == "SELL":
                             if (price_now - last_impulse_price) >= (PULLBACK_MIN + BREAK_BUFFER):
                                 if (last_signal_price is None) or (abs(price_now - last_signal_price) > DUP_TOL):
                                     await fire_signal("BUY", price_now)
                                     last_impulse_side  = "BUY"
                                     last_impulse_ts    = time.time()
                                     last_impulse_price = price_now
                                     await asyncio.sleep(POLL_SEC); continue
                         # если был импульс вверх — ждём быстрый даунтик
                         elif last_impulse_side == "BUY":
                             if (last_impulse_price - price_now) >= (PULLBACK_MIN + BREAK_BUFFER):
                                 if (last_signal_price is None) or (abs(price_now - last_signal_price) > DUP_TOL):
                                     await fire_signal("SELL", price_now)
                                     last_impulse_side  = "SELL"
                                     last_impulse_ts    = time.time()
                                     last_impulse_price = price_now
                                     await asyncio.sleep(POLL_SEC); continue
 
                 await asyncio.sleep(POLL_SEC)
 
             except Exception as e:
-                logging.error(f"engine error: {e}")
-                await asyncio.sleep(1.2)
-
-# ===================== MAIN =====================
-async def main():
-    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
-    asyncio.create_task(engine_loop())
-    await dp.start_polling(bot)
+                logging.error(f"engine error: {e}")
+                await asyncio.sleep(1.2)
+
+# ===================== HEARTBEAT =====================
+async def heartbeat_loop():
+    global signals_today, last_signal_side, last_signal_price, last_signal_ts
+    while True:
+        try:
+            await asyncio.sleep(HEARTBEAT_INTERVAL)
+            profile = (f"look={IMP_LOOK_MIN}m move_min={IMP_MOVE_MIN} "
+                       f"pullback_min={PULLBACK_MIN} coold={COOLDOWN_SEC}s")
+            last_side = last_signal_side or "нет"
+            last_price = fmt(last_signal_price) if last_signal_price else "—"
+            text = (
+                "📡 Heartbeat: бот в строю.\n"
+                f"Сигналы сегодня: {signals_today}/{MAX_SIGNALS_PER_DAY}.\n"
+                f"Последний сигнал: {last_side} @ {last_price} ({fmt_since(last_signal_ts)}).\n"
+                f"Профиль: {profile}."
+            )
+            await send_msg(text)
+        except Exception as e:
+            logging.error(f"heartbeat error: {e}")
+            await asyncio.sleep(5)
+
+# ===================== MAIN =====================
+async def main():
+    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
+    asyncio.create_task(engine_loop())
+    asyncio.create_task(heartbeat_loop())
+    await dp.start_polling(bot)
 
 if __name__ == "__main__":
     try:
         asyncio.run(main())
     except (KeyboardInterrupt, SystemExit):
         pass
 
