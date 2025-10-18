 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/signal_bot.py b/signal_bot.py
index b2b165b3b7b8b394cb67d520db4c7871346f41c0..310b99bb1e15f70f13a4e396166a369a87120667 100644
--- a/signal_bot.py
+++ b/signal_bot.py
@@ -1,66 +1,68 @@
 #!/usr/bin/env python3
 # -*- coding: utf-8 -*-
 
 import os, time, json, csv, logging, asyncio, random, math
 from datetime import datetime
 from copy import deepcopy
 
 import numpy as np
 import pandas as pd
 import aiohttp
 
 from aiogram import Bot, Dispatcher, Router, F
 from aiogram.client.default import DefaultBotProperties
 from aiogram.types import Message
 from aiogram.filters import Command
 
-# ===================== VERSION =====================
-VERSION = "V5.8 Ultra-Scalper (V5.1 base + speed 1s + TP=0.5*ATR15 + 3m idea cooldown + BTC only on demand + no RR filters)"
+# ===================== VERSION =====================
+VERSION = "V7.0 Smart Trader (structural TP/SL, multi-timeframe brain, single-position discipline)"
 
 # ===================== TOKENS / OWNER =====================
 MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs")
 LOG_BOT_TOKEN  = os.getenv("LOG_BOT_TOKEN",  "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw")
 OWNER_ID       = int(os.getenv("OWNER_ID", "6784470762"))
 TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", str(OWNER_ID)))
 
 # ===================== MARKETS / SETTINGS =====================
 SYMBOLS = {
     "BTC": {"name": "BTC-USD",   "tf": "1m"},
     "NG":  {"name": "NG=F",      "tf": "1m"},
     "XAU": {"name": "XAUUSD=X",  "tf": "1m"},
 }
 DXY_TICKERS = ("DX-Y.NYB", "DX=F")
 
-SPREAD_BUFFER   = {"NG": 0.0020, "XAU": 0.20, "BTC": 5.0}
-CONF_MIN_IDEA   = 0.05
-CONF_MIN_TRADE  = 0.55
-
-# V5.8: RR фильтры отключены (оставлены как значения, но НЕ используются)
-RR_TRADE_MIN    = 0.00
-TP_MIN_TRADE    = {"NG": 0.005, "XAU": 0.005, "BTC": 50.0}
-RR_MIN_IDEA     = 0.00
+SPREAD_BUFFER   = {"NG": 0.0020, "XAU": 0.20, "BTC": 5.0}
+TP_EXTRA_BUFFER = {"NG": 0.0100, "XAU": 0.30, "BTC": 15.0}
+SL_MIN_GAP      = {"NG": 0.0040, "XAU": 0.25, "BTC": 12.0}
+TF_TITLES = {"1m": "M1", "5m": "M5", "15m": "M15", "60m": "H1", "240m": "H4", "1440m": "D1"}
+STRUCTURAL_LOOKBACKS = {"1m": 180, "5m": 180, "15m": 200, "60m": 220, "240m": 260, "1440m": 90}
+CONF_MIN_IDEA   = 0.25
+CONF_MIN_TRADE  = 0.55
+
+RR_TRADE_MIN    = 2.0
+RR_MIN_IDEA     = 2.0
 
 # фильтр свежести (оставляем)
 FRESH_MULT      = 10.0
 
 # Антиспам ИДЕЙ (V5.8: 3 минуты кулдаун)
 SEND_IDEAS         = True
 IDEA_COOLDOWN_SEC  = 180
 MAX_IDEAS_PER_HOUR = 20
 
 # Сессии (UTC)
 LONDON_HOURS = range(7, 15)
 NY_HOURS     = range(12, 21)
 
 # Скорость и интервалы
 POLL_SEC        = 1          # V5.8 скорость
 ALIVE_EVERY_SEC = 300
 BOOT_COOLDOWN_S = 30
 COOLDOWN_SEC    = 10
 COOLDOWN_SEC_NG = 7
 
 TRADES_CSV = "gv_trades.csv"
 
 HTTP_TIMEOUT   = 12
 YAHOO_RETRIES  = 4
 YAHOO_BACKOFF0 = 0.9
@@ -302,63 +304,175 @@ async def get_df(session: aiohttp.ClientSession, symbol: str) -> pd.DataFrame:
     if symbol == "BTC":
         for t in ("BTC-USD",):
             df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
             if not df.empty:
                 last_candle_close_ts["BTC"] = time.time()
                 _prices_cache["BTC"] = {"ts": now_ts, "df": df, "feed":"yahoo"}
                 return df
         return pd.DataFrame()
 
     return pd.DataFrame()
 
 async def get_dxy_df(session: aiohttp.ClientSession) -> pd.DataFrame:
     for t in DXY_TICKERS:
         df = _df_from_yahoo_v8(await _yahoo_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d"))
         if not df.empty:
             return df
     return pd.DataFrame()
 
 # ===================== UTILS / SMC =====================
 def rnd(sym: str, x: float) -> float:
     if sym == "NG":  return round(float(x), 4)
     if sym == "XAU": return round(float(x), 2)
     if sym == "BTC": return round(float(x), 2)
     return round(float(x), 4)
 
-def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
-    if df is None or df.empty:
-        return pd.DataFrame()
-    idx = pd.date_range(start=pd.Timestamp.utcnow().floor('D'), periods=len(df), freq="1min")
-    z = df.copy()
-    z.index = idx
-    o = z["Open"].resample(f"{minutes}min").first()
-    h = z["High"].resample(f"{minutes}min").max()
-    l = z["Low"].resample(f"{minutes}min").min()
-    c = z["Close"].resample(f"{minutes}min").last()
-    r = pd.concat([o,h,l,c], axis=1).dropna()
-    r.columns = ["Open","High","Low","Close"]
-    return r.reset_index(drop=True)
+def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
+    if df is None or df.empty:
+        return pd.DataFrame()
+    idx = pd.date_range(start=pd.Timestamp.utcnow().floor('D'), periods=len(df), freq="1min")
+    z = df.copy()
+    z.index = idx
+    o = z["Open"].resample(f"{minutes}min").first()
+    h = z["High"].resample(f"{minutes}min").max()
+    l = z["Low"].resample(f"{minutes}min").min()
+    c = z["Close"].resample(f"{minutes}min").last()
+    r = pd.concat([o,h,l,c], axis=1).dropna()
+    r.columns = ["Open","High","Low","Close"]
+    return r.reset_index(drop=True)
+
+
+def _local_extrema(df: pd.DataFrame, lookback: int, mode: str) -> list[float]:
+    if df is None or df.empty:
+        return []
+    data = df.tail(max(lookback, 10)).reset_index(drop=True)
+    if len(data) < 5:
+        return []
+    highs = data["High"].astype(float).to_numpy()
+    lows = data["Low"].astype(float).to_numpy()
+    res: list[float] = []
+    for i in range(2, len(data) - 2):
+        if mode == "high":
+            window = highs[i-2:i+3]
+            price = float(highs[i])
+            if price >= float(window.max()):
+                res.append(price)
+        else:
+            window = lows[i-2:i+3]
+            price = float(lows[i])
+            if price <= float(window.min()):
+                res.append(price)
+    # remove duplicates while keeping order by distance from the end
+    unique = []
+    seen = set()
+    for price in reversed(res):
+        key = round(price, 8)
+        if key in seen:
+            continue
+        seen.add(key)
+        unique.append(price)
+    return list(reversed(unique))
+
+
+def _tf_title(tf_key: str) -> str:
+    return TF_TITLES.get(tf_key, tf_key)
+
+
+def _collect_structural_levels(frames: dict[str, pd.DataFrame], mode: str, lookbacks: dict[str, int]) -> list[tuple[float, str]]:
+    levels: list[tuple[float, str]] = []
+    for tf_key, frame in frames.items():
+        if frame is None or frame.empty:
+            continue
+        lookback = lookbacks.get(tf_key, 80)
+        extrema = _local_extrema(frame, lookback, "high" if mode == "high" else "low")
+        for level in extrema:
+            levels.append((float(level), tf_key))
+        try:
+            if mode == "high":
+                swing = _swing_high(frame, min(lookback, len(frame) - 1))
+            else:
+                swing = _swing_low(frame, min(lookback, len(frame) - 1))
+            if swing and not math.isnan(swing):
+                levels.append((float(swing), tf_key))
+        except Exception:
+            pass
+    return levels
+
+
+def choose_structural_stop(entry: float, side: str, symbol: str,
+                           frames: dict[str, pd.DataFrame], lookbacks: dict[str, int]) -> dict | None:
+    buffer = dynamic_buffer(symbol)
+    min_gap = SL_MIN_GAP.get(symbol, buffer)
+    mode = "low" if side == "BUY" else "high"
+    anchors = _collect_structural_levels(frames, mode, lookbacks)
+    if not anchors:
+        return None
+    # sort by proximity to entry while keeping protective distance requirement
+    if side == "BUY":
+        anchors = sorted([(entry - lvl, lvl, tf) for lvl, tf in anchors if lvl < entry], key=lambda x: x[0])
+        for gap, anchor, tf in anchors:
+            if gap <= 0:
+                continue
+            sl_price = anchor - buffer
+            if entry - sl_price >= min_gap:
+                return {"price": float(sl_price), "anchor": float(anchor), "tf": tf}
+    else:
+        anchors = sorted([(lvl - entry, lvl, tf) for lvl, tf in anchors if lvl > entry], key=lambda x: x[0])
+        for gap, anchor, tf in anchors:
+            if gap <= 0:
+                continue
+            sl_price = anchor + buffer
+            if sl_price - entry >= min_gap:
+                return {"price": float(sl_price), "anchor": float(anchor), "tf": tf}
+    return None
+
+
+def choose_structural_target(entry: float, side: str, symbol: str,
+                             frames: dict[str, pd.DataFrame], lookbacks: dict[str, int]) -> dict | None:
+    buffer = dynamic_buffer(symbol)
+    min_gap = SPREAD_BUFFER.get(symbol, 0.0) + TP_EXTRA_BUFFER.get(symbol, 0.0)
+    mode = "high" if side == "BUY" else "low"
+    anchors = _collect_structural_levels(frames, mode, lookbacks)
+    if not anchors:
+        return None
+    if side == "BUY":
+        ordered = sorted([(lvl - entry, lvl, tf) for lvl, tf in anchors if lvl > entry], key=lambda x: x[0])
+        for gap, anchor, tf in ordered:
+            tp_candidate = anchor - buffer
+            if tp_candidate <= entry:
+                continue
+            if (tp_candidate - entry) >= min_gap:
+                return {"price": float(tp_candidate), "anchor": float(anchor), "tf": tf}
+    else:
+        ordered = sorted([(entry - lvl, lvl, tf) for lvl, tf in anchors if lvl < entry], key=lambda x: x[0])
+        for gap, anchor, tf in ordered:
+            tp_candidate = anchor + buffer
+            if tp_candidate >= entry:
+                continue
+            if (entry - tp_candidate) >= min_gap:
+                return {"price": float(tp_candidate), "anchor": float(anchor), "tf": tf}
+    return None
 
 def _swing_high(df, lookback=20):
     i = len(df) - 2
     L = max(0, i - lookback + 1)
     return float(df["High"].iloc[L:i+1].max())
 
 def _swing_low(df, lookback=20):
     i = len(df) - 2
     L = max(0, i - lookback + 1)
     return float(df["Low"].iloc[L:i+1].min())
 
 def _in_session_utc():
     h = pd.Timestamp.utcnow().hour
     return (h in LONDON_HOURS) or (h in NY_HOURS)
 
 def fvg_last_soft(df: pd.DataFrame, lookback: int = 20, use_bodies: bool = True,
                   min_abs: float = 0.0, min_rel_to_avg: float = 0.0):
     n = len(df)
     if n < 4:
         return False, "", 0.0, 0.0, 0.0
     avg_rng = float((df["High"] - df["Low"]).tail(max(lookback, 12)).mean() or 0.0)
     for i in range(n-2, max(1, n - lookback) - 1, -1):
         if use_bodies:
             h2 = max(float(df["Open"].iloc[i-2]), float(df["Close"].iloc[i-2]))
             l2 = min(float(df["Open"].iloc[i-2]), float(df["Close"].iloc[i-2]))
@@ -429,145 +543,280 @@ def is_consolidation_break(df):
 
 def inside_higher_ob(df_low, df_high):
     if df_low is None or df_low.empty or df_high is None or df_high.empty: return False
     if len(df_low) < 5 or len(df_high) < 5: return False
     cl  = float(df_low["Close"].iloc[-2])
     body = df_high.iloc[-2]
     top = max(float(body["Open"]), float(body["Close"]))
     bot = min(float(body["Open"]), float(body["Close"]))
     return bot <= cl <= top
 
 def fib_ote_ok(a, b, price):
     if a == b: return False
     lo, hi = sorted([float(a), float(b)])
     lvl62 = hi - 0.62*(hi-lo)
     lvl79 = hi - 0.79*(hi-lo)
     loZ, hiZ = min(lvl62, lvl79), max(lvl62, lvl79)
     return loZ <= float(price) <= hiZ
 
 def dxy_bias_from_df(dxy_1m: pd.DataFrame) -> str|None:
     if dxy_1m is None or dxy_1m.empty: return None
     df60 = _resample(dxy_1m, 60)
     df240 = _resample(dxy_1m, 240)
     if df60.empty or df240.empty: return None
     return bias_bos_higher(df60, df240)
 
-def dynamic_buffer(symbol: str) -> float:
-    return SPREAD_BUFFER.get(symbol, 0.0)
-
-def format_signal(setup, buffer):
-    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]
-    return (
-        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}\n"
-        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
-        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
-        f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}  "
-        f"RR≈{round(setup['rr'],2)}  Conf: {int(setup['conf']*100)}%  Bias: {setup['trend']}"
-    )
+def dynamic_buffer(symbol: str) -> float:
+    return SPREAD_BUFFER.get(symbol, 0.0)
+
+def momentum_confirmation(df1m: pd.DataFrame, symbol: str, side: str, atr15: float):
+    if df1m is None or df1m.empty or len(df1m) < 6:
+        return False, "", 0, 0.0
+    recent = df1m.iloc[-5:-1].copy()
+    bodies = recent["Close"] - recent["Open"]
+    if side == "BUY":
+        aligned = int((bodies > 0).sum())
+        net_move = float(recent["Close"].iloc[-1] - recent["Open"].iloc[0])
+        direction_ok = net_move > 0
+    else:
+        aligned = int((bodies < 0).sum())
+        net_move = float(recent["Close"].iloc[-1] - recent["Open"].iloc[0])
+        direction_ok = net_move < 0
+    impulse = abs(net_move)
+    if atr15 <= 0:
+        atr_ratio = 0.0
+    else:
+        atr_ratio = impulse / atr15
+    early_enough = atr_ratio <= 0.75
+    ok = (aligned >= 2) and direction_ok and early_enough
+    if not ok:
+        return False, "", aligned, atr_ratio
+    abs_move = rnd(symbol, abs(net_move))
+    reason = (
+        f"Импульс подтверждён {aligned}/4 свечами 1m, движение ≈ {abs_move}"
+        f" ({'вверх' if side=='BUY' else 'вниз'})"
+    )
+    return True, reason, aligned, atr_ratio
+
+def format_signal(setup, buffer):
+    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]
+    header = (
+        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}\n"
+        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
+        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
+        f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}  "
+        f"RR≈{round(setup['rr'],2)}  Conf: {int(setup['conf']*100)}%  Bias: {setup['trend']}"
+    )
+    reasons = setup.get("reasons", [])
+    if reasons:
+        header += "\n🧠 Почему:\n" + "\n".join(f"• {r}" for r in reasons)
+    return header
 
 # ===================== BUILD SETUP (V5.8 изменения внутри) =====================
-def build_setup(df1m: pd.DataFrame, symbol: str, tf_label: str, dxy_bias: str | None = None):
-    if df1m is None or df1m.empty or len(df1m) < 200:
-        return None
-
-    # MTF
-    df5   = _resample(df1m, 5)
-    df15  = _resample(df1m, 15)
-    df60  = _resample(df1m, 60)
-    df240 = _resample(df1m, 240)
-    if df5.empty or df15.empty or df60.empty or df240.empty: return None
-
-    bias = bias_bos_higher(df60, df240)
-
-    # базовые «глаза»: максимально мягкие
-    fvg_ok, fvg_dir, fvg_top, fvg_bot, fvg_w = fvg_last_soft(df15, lookback=20, use_bodies=True, min_abs=0.0, min_rel_to_avg=0.0)
-    choch_ok = choch_soft(df5, "UP" if bias=="UP" else "DOWN", swing_lookback=8, confirm_break=False)
-
-    # стоп-хант
-    sweep15, sweep_dir15 = had_liquidity_sweep(df15, lookback=20)
-
-    # сторона
-    side = "BUY" if bias=="UP" else "SELL"
-    if sweep15:
-        if sweep_dir15=="UP": side="BUY"
-        if sweep_dir15=="DOWN": side="SELL"
-
-    entry = float(df5["Close"].iloc[-2])
-    hi15  = _swing_high(df15, 20)
-    lo15  = _swing_low(df15, 20)
-    buf   = dynamic_buffer(symbol)
-
-    # SL как в V5.1: структура + буфер
-    if side == "BUY":
-        sl = min(entry, lo15 - buf)
-    else:
-        sl = max(entry, hi15 + buf)
-
-    # === V5.8: TP = entry ± 0.5 * ATR(15) === (локально считаем ATR15)
-    d15 = _resample(df1m, 15)
-    if d15.empty or len(d15) < 20:
-        return None
-    tr = (d15["High"] - d15["Low"]).rolling(14).mean()
-    atr15 = float(tr.iloc[-1]) if not tr.empty and pd.notna(tr.iloc[-1]) else 0.0
-    if atr15 <= 0:
-        return None
-    tp_dist = 0.5 * atr15
-    tp = entry + tp_dist if side == "BUY" else entry - tp_dist
-
-    rr     = abs(tp - entry) / max(abs(entry - sl), 1e-9)
-    tp_abs = abs(tp - entry)
-    tp_min = TP_MIN_TRADE.get(symbol, 0.0)
-
-    # ===== скоринг (как было) =====
-    score = 0
-    base_ok = (fvg_ok or choch_ok)
-    score += 40 if base_ok else 10
-    last15 = df15.iloc[-2]
-    if fib_ote_ok(float(last15["Open"]), float(last15["Close"]), entry): score += 10
-    if sweep15: score += 10
-    if fvg_ok:
-        avg_rng = float((df15["High"] - df15["Low"]).tail(20).mean() or 0.0)
-        if avg_rng > 0 and fvg_w >= 1.5 * avg_rng: score += 7
-    if _in_session_utc(): score += 5
-    if is_consolidation_break(df5): score += 12
-    if inside_higher_ob(df5, df60) or inside_higher_ob(df5, df240): score += 15
-    if symbol == "XAU" and dxy_bias:
-        if side == "BUY"  and dxy_bias == "DOWN": score += 15
-        if side == "SELL" and dxy_bias == "UP":   score += 15
-    if rr >= 1.25: score += 10
-
-    score = max(0, min(100, score))
-    conf  = score / 100.0
-    if conf < CONF_MIN_IDEA:
-        return None
-
-    return {
-        "symbol": symbol, "tf": tf_label,
-        "side": side, "trend": bias,
-        "entry": entry, "tp": tp, "sl": sl,
-        "rr": rr, "conf": conf, "tp_abs": tp_abs, "tp_min": tp_min
-    }
+def build_setup(df1m: pd.DataFrame, symbol: str, tf_label: str, dxy_bias: str | None = None):
+    if df1m is None or df1m.empty or len(df1m) < 200:
+        return None
+
+    # multi timeframe view (1m → 1D)
+    df5    = _resample(df1m, 5)
+    df15   = _resample(df1m, 15)
+    df60   = _resample(df1m, 60)
+    df240  = _resample(df1m, 240)
+    df1440 = _resample(df1m, 1440)
+    if df5.empty or df15.empty or df60.empty or df240.empty:
+        return None
+
+    frames = {
+        "1m": df1m,
+        "5m": df5,
+        "15m": df15,
+        "60m": df60,
+        "240m": df240,
+    }
+    if not df1440.empty:
+        frames["1440m"] = df1440
+
+    bias = bias_bos_higher(df60, df240)
+
+    reasons = []
+    score_breakdown = []
+    score = 10
+    reasons.append("H1/H4 структура указывает " + ("вверх" if bias == "UP" else "вниз"))
+    score_breakdown.append(("Trend bias", 10))
+    core_signals = 0
+
+    if not df1440.empty and len(df1440) >= 2:
+        daily = df1440.iloc[-2]
+        daily_bias = "UP" if float(daily["Close"]) >= float(daily["Open"]) else "DOWN"
+        if daily_bias == bias:
+            reasons.append("D1 свеча поддерживает направление")
+            score += 8
+            score_breakdown.append(("Daily alignment", 8))
+        else:
+            reasons.append("D1 идёт против, держим риск под контролем")
+
+    # базовые «глаза»: структура и импульс
+    fvg_ok, fvg_dir, _, _, fvg_w = fvg_last_soft(df15, lookback=20, use_bodies=True, min_abs=0.0, min_rel_to_avg=0.0)
+    if fvg_ok:
+        reasons.append(f"Свежий {('бычий' if fvg_dir=='BULL' else 'медвежий')} FVG на M15")
+        score += 20
+        score_breakdown.append(("FVG M15", 20))
+        core_signals += 1
+    choch_ok = choch_soft(df5, "UP" if bias == "UP" else "DOWN", swing_lookback=8, confirm_break=False)
+    if choch_ok:
+        reasons.append("CHoCH на M5 подтверждает смену структуры")
+        score += 18
+        score_breakdown.append(("CHOCH M5", 18))
+        core_signals += 1
+    if not (fvg_ok or choch_ok):
+        return None
+
+    sweep15, sweep_dir15 = had_liquidity_sweep(df15, lookback=20)
+    if sweep15:
+        reasons.append("Перед входом забрали ликвидность на M15")
+        score += 14
+        score_breakdown.append(("Liquidity sweep", 14))
+        core_signals += 1
+
+    side = "BUY" if bias == "UP" else "SELL"
+    if sweep15:
+        if sweep_dir15 == "UP":
+            side = "BUY"
+        if sweep_dir15 == "DOWN":
+            side = "SELL"
+
+    entry = float(df1m["Close"].iloc[-2])
+
+    structural_frames = {k: v for k, v in frames.items() if not v.empty}
+    sl_info = choose_structural_stop(entry, side, symbol, structural_frames, STRUCTURAL_LOOKBACKS)
+    if not sl_info:
+        return None
+    tp_info = choose_structural_target(entry, side, symbol, structural_frames, STRUCTURAL_LOOKBACKS)
+    if not tp_info:
+        return None
+
+    sl = float(sl_info["price"])
+    tp = float(tp_info["price"])
+
+    tp_min = SPREAD_BUFFER.get(symbol, 0.0) + TP_EXTRA_BUFFER.get(symbol, 0.0)
+    tp_abs = abs(tp - entry)
+    if tp_abs < tp_min:
+        return None
+
+    rr = abs(tp - entry) / max(abs(entry - sl), 1e-9)
+
+    reasons.append(
+        f"SL спрятан за {_tf_title(sl_info['tf'])} минимумом {rnd(symbol, sl_info['anchor'])}"
+        if side == "BUY" else
+        f"SL над {_tf_title(sl_info['tf'])} максимумом {rnd(symbol, sl_info['anchor'])}"
+    )
+    score += 12
+    score_breakdown.append(("Structural SL", 12))
+    reasons.append(
+        f"TP перед {_tf_title(tp_info['tf'])} ликвидностью {rnd(symbol, tp_info['anchor'])}"
+        if side == "BUY" else
+        f"TP до {_tf_title(tp_info['tf'])} ликвидности {rnd(symbol, tp_info['anchor'])}"
+    )
+    score += 14
+    score_breakdown.append(("Structural TP", 14))
+
+    atr_series = (df15["High"] - df15["Low"]).rolling(14).mean()
+    atr15 = float(atr_series.iloc[-1]) if not atr_series.empty and pd.notna(atr_series.iloc[-1]) else 0.0
+    if atr15 <= 0:
+        return None
+
+    momentum_ok, momentum_reason, momentum_aligned, momentum_ratio = momentum_confirmation(df1m, symbol, side, atr15)
+    if not momentum_ok:
+        return None
+    reasons.append(momentum_reason)
+    score += 15
+    score_breakdown.append(("Momentum confirm", 15))
+    core_signals += 1
+
+    last15 = df15.iloc[-2]
+    if fib_ote_ok(float(last15["Open"]), float(last15["Close"]), entry):
+        reasons.append("Цена в зоне OTE 62-79% на M15")
+        score += 9
+        score_breakdown.append(("OTE", 9))
+    if fvg_ok:
+        avg_rng = float((df15["High"] - df15["Low"]).tail(20).mean() or 0.0)
+        if avg_rng > 0 and fvg_w >= 1.5 * avg_rng:
+            reasons.append("FVG шире среднего диапазона")
+            score += 6
+            score_breakdown.append(("Wide FVG", 6))
+    if is_consolidation_break(df5):
+        reasons.append("Выход из консолидации на M5")
+        score += 10
+        score_breakdown.append(("Consolidation break", 10))
+        core_signals += 1
+    if inside_higher_ob(df5, df60) or inside_higher_ob(df5, df240):
+        reasons.append("Цена внутри Order Block старшего ТФ")
+        score += 12
+        score_breakdown.append(("HTF OB", 12))
+    if symbol == "XAU" and dxy_bias:
+        if side == "BUY" and dxy_bias == "DOWN":
+            reasons.append("DXY падает, золото получает поддержку")
+            score += 12
+            score_breakdown.append(("DXY correlation", 12))
+        if side == "SELL" and dxy_bias == "UP":
+            reasons.append("DXY растёт, давит на золото")
+            score += 12
+            score_breakdown.append(("DXY correlation", 12))
+
+    if rr >= RR_TRADE_MIN:
+        reasons.append(f"Риск/прибыль ≈ 1:{round(rr, 2)}")
+        score += 10
+        score_breakdown.append(("Risk/Reward", 10))
+
+    if core_signals < 2:
+        return None
+
+    score = max(0, min(100, score))
+    conf = score / 100.0
+    if conf < CONF_MIN_IDEA:
+        return None
+
+    return {
+        "symbol": symbol,
+        "tf": tf_label,
+        "side": side,
+        "trend": bias,
+        "entry": entry,
+        "tp": tp,
+        "sl": sl,
+        "rr": rr,
+        "conf": conf,
+        "tp_abs": tp_abs,
+        "tp_min": tp_min,
+        "reasons": reasons,
+        "score_breakdown": score_breakdown,
+        "core_signals": core_signals,
+        "atr15": atr15,
+        "momentum_ratio": momentum_ratio,
+        "momentum_aligned": momentum_aligned,
+        "dxy_bias": dxy_bias,
+    }
 
 # ===================== EXECUTION / LOGGING =====================
 def append_trade(row):
     newf = not os.path.exists(TRADES_CSV)
     with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
         w = csv.DictWriter(f, fieldnames=list(row.keys()))
         if newf: w.writeheader()
         w.writerow(row)
 
 async def notify_outcome(symbol: str, outcome: str, price: float):
     name = SYMBOLS[symbol]["name"]; p = rnd(symbol, price)
     text = f"✅ TP hit on {name} @ {p}" if outcome=="TP" else f"🟥 SL hit on {name} @ {p}"
     await send_main(text)
 
 def finish_trade(symbol: str, outcome: str, price_now: float):
     sess = trade[symbol]
     trade[symbol] = None
     cooldown_until[symbol] = time.time() + (COOLDOWN_SEC_NG if symbol == "NG" else COOLDOWN_SEC)
     if not sess: return
     try:
         rr = (sess["tp"]-sess["entry"]) if sess["side"]=="BUY" else (sess["entry"]-sess["tp"])
         rl = (sess["entry"]-sess["sl"]) if sess["side"]=="BUY" else (sess["sl"]-sess["entry"])
         append_trade({
             "ts_close": datetime.utcnow().isoformat(timespec="seconds"),
             "symbol": symbol, "side": sess["side"],
@@ -598,103 +847,107 @@ def can_send_idea(sym: str) -> bool:
     return True
 
 def is_fresh_enough(symbol: str, entry: float, close_now: float) -> bool:
     buf = SPREAD_BUFFER.get(symbol, 0.0)
     lim = FRESH_MULT * buf
     return abs(float(entry) - float(close_now)) <= lim
 
 async def handle_symbol(session: aiohttp.ClientSession, symbol: str, dxy_df: pd.DataFrame | None):
     global last_seen_idx, last_signal_idx
 
     # Режим: если не AUTO, обрабатываем только выбранный символ (в т.ч. BTC)
     if mode != "AUTO" and symbol not in (mode,):
         return
 
     df = await get_df(session, symbol)
     if df.empty or len(df) < 200:
         return
 
     cur_idx = len(df) - 1
     closed_idx = cur_idx - 1
     if closed_idx <= last_seen_idx[symbol]:
         return
     last_seen_idx[symbol] = closed_idx
 
     # сопровождение открытой сделки
-    sess = trade[symbol]
-    if sess:
-        start_i = int(sess.get("entry_bar_idx", cur_idx))
-        post = df.iloc[(start_i + 1):]
-        if not post.empty:
+    sess = trade[symbol]
+    if sess:
+        start_i = int(sess.get("entry_bar_idx", cur_idx))
+        post = df.iloc[(start_i + 1):]
+        if not post.empty:
             side = sess["side"]; tp = sess["tp"]; sl = sess["sl"]
             hit_tp = (post["High"].max() >= tp) if side=="BUY" else (post["Low"].min() <= tp)
             hit_sl = (post["Low"].min()  <= sl) if side=="BUY" else (post["High"].max() >= sl)
             if hit_tp:
                 price_now = float(post["Close"].iloc[-1])
                 asyncio.create_task(notify_outcome(symbol, "TP", price_now))
                 finish_trade(symbol, "TP", price_now)
                 return
             if hit_sl:
                 price_now = float(post["Close"].iloc[-1])
                 asyncio.create_task(notify_outcome(symbol, "SL", price_now))
                 finish_trade(symbol, "SL", price_now)
                 return
-        return
-
-    # глобальные кулдауны
-    if time.time() - boot_ts < BOOT_COOLDOWN_S: return
+        return
+
+    # дисциплина: не открываем новую позицию, пока предыдущая не завершена на любом рынке
+    if any(trade.values()):
+        return
+
+    # глобальные кулдауны
+    if time.time() - boot_ts < BOOT_COOLDOWN_S: return
     if time.time() < cooldown_until[symbol]:   return
 
     # DXY bias (только для золота)
     dxy_bias = dxy_bias_from_df(dxy_df) if symbol=="XAU" and dxy_df is not None and not dxy_df.empty else None
 
     setup = build_setup(df, symbol, SYMBOLS[symbol]["tf"], dxy_bias=dxy_bias)
     if not setup:
         return
 
     if last_signal_idx[symbol] == closed_idx:
         return
     last_signal_idx[symbol] = closed_idx
 
     buffer    = dynamic_buffer(symbol)
     conf      = float(setup["conf"])
     close_now = float(df["Close"].iloc[-1])
     entry     = float(setup["entry"])
 
     if not is_fresh_enough(symbol, entry, close_now):
         return
 
     # IDEA (V5.8: без RR-фильтра)
-    if conf >= CONF_MIN_IDEA and can_send_idea(symbol):
+    if conf >= CONF_MIN_IDEA and setup["rr"] >= RR_MIN_IDEA and can_send_idea(symbol):
         await send_main("🧠 IDEA:\n" + format_signal(setup, buffer))
         _last_idea_ts[symbol] = time.time()
         _ideas_count_hour[symbol] = _ideas_count_hour.get(symbol, 0) + 1
         if _ideas_count_hour_ts.get(symbol, 0.0) == 0.0:
             _ideas_count_hour_ts[symbol] = time.time()
 
     # TRADE (V5.8: RR-фильтр удалён; оставлен порог по Conf и минимальная амплитуда TP)
-    if conf >= CONF_MIN_TRADE and (setup["tp_abs"] >= setup["tp_min"]):
+    if conf >= CONF_MIN_TRADE and setup["rr"] >= RR_TRADE_MIN and (setup["tp_abs"] >= setup["tp_min"]):
         await send_main(format_signal(setup, buffer))
         trade[symbol] = {
             "side": setup["side"],
             "entry": float(setup["entry"]),
             "tp": float(setup["tp"]),
             "sl": float(setup["sl"]),
             "opened_at": time.time(),
             "entry_bar_idx": cur_idx,
         }
 
 async def engine_loop():
     async with aiohttp.ClientSession() as session:
         dxy_df = None
         dxy_ts = 0.0
         while True:
             try:
                 # DXY обновляем иногда (для XAU)
                 if time.time() - dxy_ts > 25:
                     dxy_df = await get_dxy_df(session)
                     dxy_ts = time.time()
 
                 # V5.8: AUTO = NG+XAU; BTC торгуется ТОЛЬКО при выборе режима "биток"
                 symbols_to_run = ("NG","XAU") if mode == "AUTO" else (mode,)
 
                 for s in symbols_to_run: 
EOF
)
