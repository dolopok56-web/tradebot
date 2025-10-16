import asyncio
import aiohttp
import pandas as pd
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
import time
from copy import deepcopy

# ===================== CONFIG =====================
# Твои параметры инструментов
SYMBOLS = {
    "NG": {"name": "Natural Gas", "tf": "M5"},
    "XAU": {"name": "Gold", "tf": "M5"}
}

# Фиксированный буфер для TP/SL
SPREAD_BUFFER = {"NG": 0.0020, "XAU": 0.20}

# Пороговые значения
CONF_MIN_IDEA  = 0.05
CONF_MIN_TRADE = 0.55
RR_TRADE_MIN   = 1.00
TP_MIN_TRADE   = {"NG": 0.005, "XAU": 0.005}

# Сессии
LONDON_HOURS = range(7, 15)
NY_HOURS     = range(12, 21)

# Telegram Tokens
MAIN_BOT_TOKEN = "7930269505:AAEBq25Gc4XLksdelqmAMfZnyRdyD_KUzSs"
LOG_BOT_TOKEN  = "8073073724:AAHGuUPg9s_oRsH24CpLUu-5udWagAB4eaw"
OWNER_ID = 6784470762

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
log_bot = Bot(LOG_BOT_TOKEN, default=DefaultBotProperties((parse_mode=None))

# ===================== УТИЛИТЫ =====================
def rnd(sym, val): return round(float(val), 4)

def dynamic_buffer(symbol, df, atr_now):  # ATR игнорируется
    return SPREAD_BUFFER.get(symbol, 0.0)

def _resample(df, minutes):
    base = pd.date_range(end=pd.Timestamp.utcnow(), periods=len(df), freq="1min")
    z = df.copy(); z.index = base
    o = z["Open"].resample(f"{minutes}min").first()
    h = z["High"].resample(f"{minutes}min").max()
    l = z["Low"].resample(f"{minutes}min").min()
    c = z["Close"].resample(f"{minutes}min").last()
    r = pd.concat([o,h,l,c], axis=1).dropna()
    r.columns = ["Open","High","Low","Close"]
    return r.reset_index(drop=True)

def _swing_high(df, lookback=20):
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["High"].iloc[L:i+1].max())

def _swing_low(df, lookback=20):
    i = len(df) - 2
    L = max(0, i - lookback + 1)
    return float(df["Low"].iloc[L:i+1].min())

# ===================== SMC Core =====================
def _bias_bos_higher(df60, df240):
    if len(df60) < 10 or len(df240) < 10: return "UP"
    c1 = float(df60["Close"].iloc[-2])
    hh4 = _swing_high(df240, 20)
    ll4 = _swing_low(df240, 20)
    if c1 > hh4: return "UP"
    if c1 < ll4: return "DOWN"
    hh1 = _swing_high(df60, 20)
    ll1 = _swing_low(df60, 20)
    if c1 > hh1: return "UP"
    if c1 < ll1: return "DOWN"
    return "UP"

def _had_liquidity_sweep(df, lookback=20):
    i = len(df) - 2
    hh = _swing_high(df, lookback)
    ll = _swing_low(df, lookback)
    H = float(df["High"].iloc[i])
    L = float(df["Low"].iloc[i])
    C = float(df["Close"].iloc[i])
    if H > hh and C < hh: return True, "DOWN"
    if L < ll and C > ll: return True, "UP"
    return False, ""

def _fvg_last(df):
    if len(df) < 4: return False,"",0.0,0.0
    i = len(df)-2
    h2,l2 = float(df["High"].iloc[i-2]),float(df["Low"].iloc[i-2])
    h0,l0 = float(df["High"].iloc[i]),float(df["Low"].iloc[i])
    if l0 > h2: return True,"BULL",l0,h2
    if h0 < l2: return True,"BEAR",h0,l2
    return False,"",0.0,0.0

def _choch(df, want):
    c = df["Close"].values
    if len(c) < 12: return False
    if want == "UP": return c[-2] > max(c[-9:-2])
    else: return c[-2] < min(c[-9:-2])

def _in_session_utc():
    h = pd.Timestamp.utcnow().hour
    return (h in LONDON_HOURS) or (h in NY_HOURS)

def _is_consolidation_break(df):
    if len(df)<20: return False
    i=len(df)-2
    window=df.iloc[i-12:i]
    rng=float(window["High"].max()-window["Low"].min())
    base=float(window["Close"].iloc[-1])
    if base<=0: return False
    if rng/base<=0.003:
        H=float(df["High"].iloc[i])
        L=float(df["Low"].iloc[i])
        return H>window["High"].max() or L<window["Low"].min()
    return False

def _inside_higher_ob(df_low, df_high):
    if len(df_low)<5 or len(df_high)<5: return False
    cl=float(df_low["Close"].iloc[-2])
    body=df_high.iloc[-2]
    top=max(body["Open"],body["Close"])
    bot=min(body["Open"],body["Close"])
    return bot<=cl<=top

def _fib_ote_ok(a,b,price):
    if a==b: return False
    lo,hi=sorted([a,b])
    lvl62=hi-0.62*(hi-lo)
    lvl79=hi-0.79*(hi-lo)
    loZ,hiZ=min(lvl62,lvl79),max(lvl62,lvl79)
    return loZ<=price<=hiZ

# ===================== DXY Bias для золота =====================
async def get_dxy_bias(session):
    try:
        df = t get_df(session,"DXY")  # <-- если DXY подключён как отдельный инструмент
        df60 = _resample(df,60)
        df240 = _resample(df,240)
        return _bias_bos_higher(df60,df240)
    except:
        return None

# ===================== BUILD SETUP =====================
def build_setup(df1m, symbol, tf_label, dxy_bias=None):
    if df1m is None or df1m.empty or len(df1m)<120: return None

    df5=_resample(df1m,5)
    df15=_resample(df1m,15)
    df60=_resample(df1m,60)
    df240=_resample(df1m,240)

    bias=_bias_bos_higher(df60,df240)
    sweep15,sweep_dir15=_had_liquidity_sweep(df15)
    fvg15,fvg15_dir,fvg15_top,fvg15_bot=_fvg_last(df15)
    choch5=_choch(df5,"UP" if bias=="UP" else "DOWN")

    side="BUY" if bias=="UP" else "SELL"
    if sweep15:
        if sweep_dir15=="UP": side="BUY"
        if sweep_dir15=="DOWN": side="SELL"

    entry=float(df5["Close"].iloc[-2])
    hi15=_swing_high(df15,20)
    lo15=_swing_low(df15,20)
    buf=SPREAD_BUFFER.get(symbol,0.0)

    if side=="BUY":
        sl=min(entry,lo15-buf)
        tp=hi15 if hi15>entry else entry+(entry-sl)
    else:
        sl=max(entry,hi15+buf)
        tp=lo15 if lo15<entry else entry-(sl-entry)

    rr=abs(tp-entry)/max(abs(entry-sl),1e-9)
    tp_abs=abs(tp-entry)
    tp_min=TP_MIN_TRADE.get(symbol,0.0)

    # --- SCORE ---
    score=0
    # базовый CHoCH/FVG
    base_ok = choch5 or fvg15
    score += 40 if base_ok else 20
    # 1 OTE
    last15=df15.iloc[-2]
    if _fib_ote_ok(float(last15["Open"]),float(last15["Close"]),entry):
        score+=10
    # 2 StopHunt
    if sweep15: score+=10
    # 3 Сила FVG
    if fvg15:
        fvg_w=abs(fvg15_top-fvg15_bot)
        rng=(df15["High"]-df15["Low"]).tail(20).mean()
        if pd.notna(rng) and rng>0 and fvg_w>=1.5*float(rng):
            score+=7
    # 4 Сессия
    if _in_session_utc(): score+=5
    # 5 Консолидация
    if _is_consolidation_break(df5): score+=12
    # 6 OB Mitigation
    if _inside_higher_ob(df5,df60) or _inside_higher_ob(df5,df240): score+=15
    # 7 DXY корреляция (ТОЛЬКО для золота)
    if symbol=="XAU" and dxy_bias:
        if side=="BUY" and dxy_bias=="DOWN": score+=15
        if side=="SELL" and dxy_bias=="UP": score+=15
    # бонус за RR>=1.25
    if rr>=1.25: score+=10

    score=max(0,min(100,score))
    conf=score/100.0

    if conf<CONF_MIN_IDEA:
        return None

    return {
        "symbol":symbol,"tf":tf_label,"side":side,"trend":bias,
        "entry":entry,"tp":tp,"sl":sl,"rr":rr,"conf":conf,
        "tp_abs":tp_abs,"tp_min":tp_min
    }

# ===================== SIGNAL FORMAT =====================
def format_signal(setup, buffer):
    sym=setup["symbol"]; side=setup["side"]; tf=setup["tf"]
    return (
        f"🔥 {side} {SYMBOLS[sym]['name']} | {tf}\n"
        f"✅ TP: **{rnd(sym,setup['tp'])}**\n"
        f"🟥 SL: **{rnd(sym,setup['sl'])}**\n"
        f"Entry: {rnd(sym,setup['entry'])}  SpreadBuf≈{rnd(sym,buffer)}  "
        f"RR≈{round(setup['rr'],2)}  Conf: {int(setup['conf']*100)}%  Bias: {setup['trend']}"
    )

# ===================== MAIN HANDLE =====================
async def handle_symbol(session, symbol):
    df = t get_df(session, symbol)
    dxy_bias = None
    if symbol=="XAU":
        dxy_bias = t get_dxy_bias(session)

    setup = build_setup(df, symbol, SYMBOLS[symbol]["tf"], dxy_bias=dxy_bias)
    if not setup: return

    buffer = dynamic_buffer(symbol, df, 0.0)
    conf = setup["conf"]
    rr = setup["rr"]

    # IDEA
    if conf >= CONF_MIN_IDEA:
        t bot.send_message(OWNER_ID, "🧠 IDEA:\n"+format_signal(setup,buffer))

    # TRADE
    if conf >= CONF_MIN_TRADE and rr >= RR_TRADE_MIN and setup["tp_abs"] >= setup["tp_min"]:
        t bot.send_message(OWNER_ID, format_signal(setup, buffer))

# ===================== ALIVE LOOP =====================
async def alive_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                df_ng = t get_df(s,"NG")
                df_xau = t get_df(s,"XAU")
            def _atr_m15(df):
                d=_resample(df,15)
                tr=(d["High"]-d["Low"]).rolling(14).mean()
                return float(tr.iloc[-1])
            c_ng=float(df_ng["Close"].iloc[-1]) if not df_ng.empty else 0
            c_xau=float(df_xau["Close"].iloc[-1]) if not df_xau.empty else 0
            a_ng=_atr_m15(df_ng) if not df_ng.empty else 0
            a_xau=_atr_m15(df_xau) if not df_xau.empty else 0
            msg=f"[ALIVE] NG: {rnd('NG',c_ng)}, ATR15: {rnd('NG',a_ng)} | XAU: {rnd('XAU',c_xau)}, ATR15: {rnd('XAU',a_xau)}. Status: OK."
            t log_bot.send_message(OWNER_ID,msg)
        except Exception as e:
            t log_bot.send_message(OWNER_ID,f"[ALIVE ERROR] {e}")
        t asyncio.sleep(300)

# ===================== MAIN LOOP =====================
async def main_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                t handle_symbol(s,"NG")
                t handle_symbol(s,"XAU")
        except Exception as e:
            t log_bot.send_message(OWNER_ID,f"[ERROR] {e}")
        t asyncio.sleep(60)  # проверка каждую минуту

# ===================== RUN =====================
async def main():
    asyncio.create_task(alive_loop())
    t main_loop()

if __name__ == "__main__":
    asyncio.run(main())
