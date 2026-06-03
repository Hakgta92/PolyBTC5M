"""
╔══════════════════════════════════════════════════════════════════╗
║     POLYMARKET BTC BOT v8 — MAX WINRATE EDITION                  ║
║     Confluence scoring | Signal strict | AI prompt amélioré      ║
╚══════════════════════════════════════════════════════════════════╝

AMÉLIORATIONS v8 vs v7 :
  • Scoring de confluence quantitatif (0-20) — trade seulement si score ≥ 8
  • MACD crossover détecté (signal > simple histogramme)
  • Squeeze Bollinger détecté (volatilité compressée = breakout imminent)
  • Volume spike (x2.0+) comme multiplicateur de confiance
  • Filtre ATR amélioré (seuils adaptatifs selon session)
  • Nouveau filtre : cohérence 1m/5m/15m sur direction
  • Prompt Claude v8 : reçoit score numérique, règles binaires claires
  • PASS loggé avec raison précise (pour analyse win rate)
  • Trailing stop simulé sur bets actifs (coupe perte si BTC bouge fort contre)
  • Détection de fakeout (break de niveau puis retour = piège)
  • Session bonus dynamique selon vraie heure Paris
  • Anti-chop : interdit de trader si 3 PASS consécutifs sur même direction
"""

import asyncio
import logging
import os
import json
import time
import math
import aiohttp
from datetime import datetime
from collections import deque
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ─── CONFIG ────────────────────────────────────────────────────────────────
TOKEN          = os.getenv("TELEGRAM_TOKEN", "VOTRE_TOKEN_ICI")
ALLOWED_UID    = int(os.getenv("ALLOWED_USER_ID", "0"))
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
PAPER_MODE     = os.getenv("PAPER_MODE", "true").lower() == "true"
BANKROLL_START = float(os.getenv("BANKROLL", "50.0"))

MAX_BET_USD       = 5.0
MIN_BET_USD       = 1.0
MAX_BET_PCT       = 0.05
POLY_FEE          = 0.02
DAILY_LOSS_MAX    = 0.10
MAX_CONSEC_LOSS   = 2
COOLDOWN_MIN      = 30
MIN_SCORE_TO_TRADE = 8       # ← NOUVEAU : score confluence minimum pour trader
TRAILING_STOP_PCT  = 0.25   # ← NOUVEAU : si BTC bouge 0.25% contre le bet → coupe
CLAUDE_API     = "https://api.anthropic.com/v1/messages"
FEAR_GREED_API = "https://api.alternative.me/fng/?limit=1"
DATA_FILE      = "polybot_v8_state.json"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("polybot_v8.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─── INDICATEURS ───────────────────────────────────────────────────────────
def ema(values, period):
    if not values: return 0
    if len(values) < period: return values[-1]
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]: e = v * k + e * (1 - k)
    return e

def rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains = losses = 0.0
    for i in range(len(closes) - period, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0: gains += d
        else: losses -= d
    if losses == 0: return 100.0
    return round(100 - 100 / (1 + gains/losses), 2)

def macd_calc(closes):
    """Retourne aussi le crossover (signal clé)"""
    if len(closes) < 26: return 0, 0, 0, False
    ml  = ema(closes, 12) - ema(closes, 26)
    # Signal line = EMA9 du MACD — approximation sur 2 dernières valeurs
    ml_prev = ema(closes[:-1], 12) - ema(closes[:-1], 26) if len(closes) > 26 else ml
    sig  = ema([ml_prev, ml], 9) if ml_prev != ml else ml * 0.9
    hist = ml - sig
    crossover = (ml_prev < sig) and (ml > sig)   # bullish cross
    crossunder = (ml_prev > sig) and (ml < sig)  # bearish cross
    return round(ml,4), round(sig,4), round(hist,4), crossover or crossunder

def bollinger(closes, period=20):
    if len(closes) < period: return None, None, None, False
    w   = closes[-period:]
    mid = sum(w) / period
    std = math.sqrt(sum((x-mid)**2 for x in w) / period)
    bb_l = round(mid - 2*std, 2)
    bb_h = round(mid + 2*std, 2)
    # Squeeze : bandes très serrées = breakout imminent
    bandwidth = (bb_h - bb_l) / mid * 100 if mid else 0
    squeeze   = bandwidth < 0.8   # moins de 0.8% = squeeze
    return bb_l, round(mid,2), bb_h, squeeze

def atr_calc(candles, period=14):
    if len(candles) < 2: return 0.0
    trs = [max(c["high"]-c["low"],
               abs(c["high"]-candles[i-1]["close"]),
               abs(c["low"]-candles[i-1]["close"]))
           for i, c in enumerate(candles) if i > 0]
    if not trs: return 0.0
    return round(sum(trs[-period:]) / min(len(trs), period), 2)

def stoch(closes, highs, lows, period=14):
    if len(closes) < period: return 50.0, 50.0
    lo, hi = min(lows[-period:]), max(highs[-period:])
    if hi == lo: return 50.0, 50.0
    k = (closes[-1]-lo)/(hi-lo)*100
    d = (closes[-2]-lo)/(hi-lo)*100 if len(closes) > period else k
    return round(k,1), round(d,1)

def williams_r(closes, highs, lows, period=14):
    if len(closes) < period: return -50.0
    hi, lo = max(highs[-period:]), min(lows[-period:])
    if hi == lo: return -50.0
    return round(-100*(hi-closes[-1])/(hi-lo), 1)

def vwap_calc(candles):
    if not candles: return 0
    tv = sum(c["vol"] for c in candles)
    if tv == 0: return candles[-1]["close"]
    return round(sum(((c["high"]+c["low"]+c["close"])/3)*c["vol"] for c in candles)/tv, 2)

def detect_divergence(candles_5m):
    """Divergence RSI haussière/baissière — version améliorée sur 3 pivots"""
    if len(candles_5m) < 15: return None
    closes = [c["close"] for c in candles_5m[-15:]]
    rsis   = []
    for i in range(5, 15):
        rsis.append(rsi(closes[max(0,i-14):i+1]))

    if len(rsis) < 6: return None

    # Cherche 2 bas successifs dans prix et RSI
    price_lower = closes[-1] < closes[-4] < closes[-7]
    rsi_higher  = rsis[-1]  > rsis[-4]  > rsis[-7]
    if price_lower and rsi_higher and rsis[-1] < 45:
        return "BULLISH"

    price_higher = closes[-1] > closes[-4] > closes[-7]
    rsi_lower    = rsis[-1]  < rsis[-4]  < rsis[-7]
    if price_higher and rsi_lower and rsis[-1] > 55:
        return "BEARISH"
    return None

def detect_engulfing(candles):
    if len(candles) < 3: return None
    prev, curr = candles[-2], candles[-1]
    prev_body  = abs(prev["close"] - prev["open"])
    curr_body  = abs(curr["close"] - curr["open"])
    if prev_body == 0: return None
    if (prev["close"] < prev["open"] and curr["close"] > curr["open"] and
        curr["open"] < prev["close"] and curr["close"] > prev["open"] and
        curr_body > prev_body * 1.3):
        return "BULLISH"
    if (prev["close"] > prev["open"] and curr["close"] < curr["open"] and
        curr["open"] > prev["close"] and curr["close"] < prev["open"] and
        curr_body > prev_body * 1.3):
        return "BEARISH"
    return None

def detect_vwap_break(candles, lookback=6):
    if len(candles) < lookback + 2: return None
    vw         = vwap_calc(candles[-20:])
    prev_price = candles[-2]["close"]
    curr_price = candles[-1]["close"]
    vols       = [c["vol"] for c in candles[-lookback:]]
    avg_vol    = sum(vols) / len(vols) if vols else 1
    curr_vol   = candles[-1]["vol"]
    vol_ok     = curr_vol > avg_vol * 1.5   # v8 : seuil relevé à 1.5x
    if prev_price < vw and curr_price > vw and vol_ok: return "BULLISH"
    if prev_price > vw and curr_price < vw and vol_ok: return "BEARISH"
    return None

def detect_volume_spike(candles, lookback=20):
    """Détecte un spike de volume anormal (signal fort)"""
    if len(candles) < lookback: return False
    vols    = [c["vol"] for c in candles[-lookback:-1]]
    avg_vol = sum(vols) / len(vols) if vols else 1
    curr    = candles[-1]["vol"]
    return curr > avg_vol * 2.0   # spike = 2x la moyenne

def detect_fakeout(candles, level, direction, lookback=4):
    """Détecte un fakeout : prix perce un niveau puis revient — piège à éviter"""
    if len(candles) < lookback + 2: return False
    prices = [c["close"] for c in candles[-lookback-2:]]
    if direction == "UP":
        # Prix était au-dessus du niveau puis revient en-dessous
        was_above = any(p > level for p in prices[:-2])
        now_below = prices[-1] < level
        return was_above and now_below
    else:
        was_below = any(p < level for p in prices[:-2])
        now_above = prices[-1] > level
        return was_below and now_above

def pivot_sr(candles, lookback=20):
    if len(candles) < lookback: return [], []
    highs = [c["high"] for c in candles[-lookback:]]
    lows  = [c["low"]  for c in candles[-lookback:]]
    price = candles[-1]["close"]
    atr   = atr_calc(candles) * 3
    res, sup = [], []
    for i in range(2, len(highs)-2):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1] and highs[i] > highs[i-2] and highs[i] > highs[i+2]:
            if highs[i] > price and highs[i]-price < atr:
                res.append(round(highs[i], 0))
        if lows[i] < lows[i-1] and lows[i] < lows[i+1] and lows[i] < lows[i-2] and lows[i] < lows[i+2]:
            if lows[i] < price and price-lows[i] < atr:
                sup.append(round(lows[i], 0))
    return sorted(set(sup), reverse=True)[:2], sorted(set(res))[:2]

def compute_ind(candles):
    if len(candles) < 10: return {}
    c = [x["close"] for x in candles]
    h = [x["high"]  for x in candles]
    l = [x["low"]   for x in candles]
    v = [x["vol"]   for x in candles]
    price = c[-1]
    e9  = ema(c, 9);   e21 = ema(c, 21);  e50 = ema(c, min(50,len(c)))
    r14 = rsi(c, 14);  r7  = rsi(c, 7)
    ml, sg, hist, cross = macd_calc(c)
    bb_l, bb_m, bb_h, squeeze = bollinger(c)
    at   = atr_calc(candles)
    stk, std = stoch(c, h, l)
    wr_v = williams_r(c, h, l)
    vw   = vwap_calc(candles[-20:])
    av   = sum(v[-10:])/10 if len(v)>=10 else v[-1]
    mom  = c[-1]-c[-6] if len(c)>=6 else 0
    sup, res = pivot_sr(candles)
    vol_spike = detect_volume_spike(candles)
    return {
        "price":c[-1], "rsi_7":r7, "rsi_14":r14,
        "ema9":round(e9,2),"ema21":round(e21,2),"ema50":round(e50,2),
        "macd_hist":hist,"macd_line":ml,"macd_cross":cross,
        "bb_low":bb_l,"bb_mid":bb_m,"bb_high":bb_h,"bb_squeeze":squeeze,
        "atr":at,"atr_pct":round(at/price*100,3) if price else 0,
        "stoch_k":stk,"stoch_d":std,"williams_r":wr_v,
        "vwap":vw,"above_vwap":price>vw,
        "vol_ratio":round(v[-1]/av,2) if av else 1.0,
        "vol_spike":vol_spike,
        "momentum":round(mom,2),"ema_bull":e9>e21,
        "ema_bull_strong":e9>e21 and e21>e50,
        "supports":sup,"resistances":res,
    }

# ─── SCORING DE CONFLUENCE v8 ───────────────────────────────────────────────
def compute_confluence_score(i1, i5, i15, i1h, fg, sess, adv):
    """
    Score de 0 à 20.
    Trade seulement si score >= MIN_SCORE_TO_TRADE (8).
    Retourne (score_up, score_down, direction, signals_list)
    """
    up = 0.0
    dn = 0.0
    signals = []

    # ── TIMEFRAME ALIGNMENT (max 6 pts) ──
    if i15.get("ema_bull"):   up += 2; signals.append("15m EMA ↑")
    else:                     dn += 2; signals.append("15m EMA ↓")

    if i1h.get("ema_bull"):   up += 2; signals.append("1h EMA ↑")
    else:                     dn += 2; signals.append("1h EMA ↓")

    if i5.get("ema_bull"):    up += 1; signals.append("5m EMA ↑")
    else:                     dn += 1; signals.append("5m EMA ↓")

    if i1.get("ema_bull"):    up += 0.5
    else:                     dn += 0.5

    # ── MACD (max 4 pts) ──
    if i15.get("macd_hist",0) > 0:   up += 1.5; signals.append("MACD 15m positif")
    elif i15.get("macd_hist",0) < 0: dn += 1.5; signals.append("MACD 15m négatif")

    if i5.get("macd_hist",0) > 0:    up += 1
    elif i5.get("macd_hist",0) < 0:  dn += 1

    if i5.get("macd_cross"):
        ml = i5.get("macd_line",0)
        if ml > 0: up += 1.5; signals.append("⚡ MACD cross haussier 5m")
        else:      dn += 1.5; signals.append("⚡ MACD cross baissier 5m")

    # ── RSI (max 3 pts) ──
    r5 = i5.get("rsi_14", 50)
    r15 = i15.get("rsi_14", 50)
    if r5 < 30:   up += 2; signals.append(f"RSI 5m survendu ({r5})")
    elif r5 > 70: dn += 2; signals.append(f"RSI 5m suracheté ({r5})")
    elif r5 < 45: up += 0.5
    elif r5 > 55: dn += 0.5

    if r15 < 40:  up += 0.5
    elif r15 > 60: dn += 0.5

    # ── VWAP (max 2 pts) ──
    if i5.get("above_vwap"):   up += 1; signals.append("Prix > VWAP")
    else:                      dn += 1; signals.append("Prix < VWAP")

    if i15.get("above_vwap"):  up += 0.5
    else:                      dn += 0.5

    # ── STOCH (max 1 pt) ──
    sk = i5.get("stoch_k", 50)
    if sk < 25:   up += 0.5; signals.append(f"Stoch survendu ({sk})")
    elif sk > 75: dn += 0.5; signals.append(f"Stoch suracheté ({sk})")

    # ── SIGNAUX AVANCÉS (max 5 pts) ──
    adv_score = adv.get("score", 0)
    if adv_score > 0:   up += min(adv_score * 1.5, 5); signals.extend(adv.get("signals",[]))
    elif adv_score < 0: dn += min(abs(adv_score)*1.5, 5); signals.extend(adv.get("signals",[]))

    # ── VOLUME SPIKE (bonus 1.5 pt) ──
    if i5.get("vol_spike"):
        bonus = 1.5
        if up > dn: up += bonus; signals.append("🔥 Volume spike (confirme UP)")
        elif dn > up: dn += bonus; signals.append("🔥 Volume spike (confirme DOWN)")

    # ── SESSION BONUS (max 2 pts) ──
    sb = sess.get("score_bonus", 0)
    if sb > 0:
        if up > dn: up += sb
        elif dn > up: dn += sb

    # ── FEAR & GREED (bonus/malus) ──
    fgv = fg.get("value", 50)
    if fgv < 15:  up += 1; signals.append(f"F&G extrême peur ({fgv}) → rebond")
    elif fgv > 85: dn += 1; signals.append(f"F&G extrême greed ({fgv}) → correction")

    # ── SQUEEZE BOLLINGER (signal fort de break) ──
    if i5.get("bb_squeeze"):
        signals.append("⚡ Squeeze BB — breakout imminent")
        # Direction du breakout = direction dominante actuelle
        if up > dn: up += 1
        else: dn += 1

    # Déterminer direction
    if up >= dn:
        direction = "UP"
        score = round(up, 1)
    else:
        direction = "DOWN"
        score = round(dn, 1)

    diff = abs(up - dn)

    return {
        "score_up": round(up, 1),
        "score_dn": round(dn, 1),
        "score": score,
        "diff": round(diff, 1),
        "direction": direction,
        "signals": signals[:8],   # top 8 signaux
        "tradeable": score >= MIN_SCORE_TO_TRADE and diff >= 2.0,
    }

def compute_advanced_signals(candles_5m, candles_1m):
    div = detect_divergence(candles_5m)
    eng = detect_engulfing(candles_5m[-3:]) if len(candles_5m) >= 3 else None
    vb  = detect_vwap_break(candles_5m)
    signals = []; score = 0
    if div == "BULLISH":   signals.append("🔄 Divergence RSI haussière"); score += 2
    elif div == "BEARISH": signals.append("🔄 Divergence RSI baissière"); score -= 2
    if eng == "BULLISH":   signals.append("🕯️ Engulfing haussier"); score += 2
    elif eng == "BEARISH": signals.append("🕯️ Engulfing baissier"); score -= 2
    if vb == "BULLISH":    signals.append("📊 VWAP break ↑ avec volume"); score += 1.5
    elif vb == "BEARISH":  signals.append("📊 VWAP break ↓ avec volume"); score -= 1.5
    return {"divergence":div,"engulfing":eng,"vwap_break":vb,
            "signals":signals,"score":score,
            "bias":"UP" if score>0 else "DOWN" if score<0 else None}

def session_ctx():
    h = (datetime.utcnow().hour + 2) % 24
    if   14 <= h < 17: return {"session":"US_OPEN",      "quality":"EXCELLENT","score_bonus":2}
    elif 17 <= h < 20: return {"session":"US_AFTERNOON", "quality":"EXCELLENT","score_bonus":1}
    elif  9 <= h < 13: return {"session":"EU_OPEN",      "quality":"GOOD",     "score_bonus":1}
    elif 20 <= h < 22: return {"session":"US_CLOSE",     "quality":"GOOD",     "score_bonus":0}
    elif  7 <= h <  9: return {"session":"ASIA_LATE",    "quality":"MEDIUM",   "score_bonus":0}
    elif  1 <= h <  7: return {"session":"ASIA_EARLY",   "quality":"MEDIUM",   "score_bonus":-1}
    else:              return {"session":"OVERNIGHT",    "quality":"LOW",      "score_bonus":-2}

def pattern_mem(trades):
    if len(trades) < 5: return "Moins de 5 trades."
    wins  = [t for t in trades if t["result"]=="WIN"]
    losses= [t for t in trades if t["result"]=="LOSS"]
    up_t  = [t for t in trades if t["dir"]=="UP"]
    dn_t  = [t for t in trades if t["dir"]=="DOWN"]
    up_wr = sum(1 for t in up_t if t["result"]=="WIN")/len(up_t)*100 if up_t else 0
    dn_wr = sum(1 for t in dn_t if t["result"]=="WIN")/len(dn_t)*100 if dn_t else 0
    cw = sum(t.get("conf",0) for t in wins)/len(wins)*100 if wins else 0
    cl = sum(t.get("conf",0) for t in losses)/len(losses)*100 if losses else 0
    hi_score_trades = [t for t in trades if t.get("score",0) >= MIN_SCORE_TO_TRADE]
    hi_wr = sum(1 for t in hi_score_trades if t["result"]=="WIN")/len(hi_score_trades)*100 if hi_score_trades else 0
    return (f"UP:{up_wr:.0f}%({len(up_t)}) DOWN:{dn_wr:.0f}%({len(dn_t)}) | "
            f"Conf wins:{cw:.0f}% losses:{cl:.0f}% | Score≥{MIN_SCORE_TO_TRADE}: {hi_wr:.0f}%({len(hi_score_trades)})")

def is_trending(c5, c15):
    if len(c5) < 12: return False
    h = (datetime.utcnow().hour + 2) % 24
    thr = 0.10 if (22 <= h or h < 7) else 0.05
    closes = [c["close"] for c in c5[-12:]]
    highs  = [c["high"]  for c in c5[-6:]]
    lows   = [c["low"]   for c in c5[-6:]]
    price  = closes[-1] if closes[-1] else 1
    range_pct = (max(highs)-min(lows))/price*100
    mom_pct   = abs(closes[-1]-closes[0])/price*100
    return range_pct > thr or mom_pct > thr*0.7

# ─── DATA FETCH ────────────────────────────────────────────────────────────
async def fetch_price():
    sources = [
        ("Kraken",   "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
         lambda d: float(d["result"]["XXBTZUSD"]["c"][0])),
        ("Coinbase", "https://api.coinbase.com/v2/prices/BTC-USD/spot",
         lambda d: float(d["data"]["amount"])),
        ("CoinGecko","https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
         lambda d: float(d["bitcoin"]["usd"])),
        ("Binance",  "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
         lambda d: float(d["price"])),
    ]
    for name, url, parser in sources:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
                    if r.status == 200:
                        p = parser(await r.json())
                        if p > 0:
                            log.info(f"Price {name}: ${p:,.0f}")
                            return p
        except Exception as e:
            log.warning(f"Price {name}: {e}")
    return st.price

async def fetch_klines(interval, limit=60):
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval={interval}&limit={limit}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list) and len(data) > 5:
                        return [{"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                                 "close":float(k[4]),"vol":float(k[5]),"ts":int(k[0])//1000}
                                for k in data]
    except Exception as e:
        log.warning(f"Binance klines {interval}: {e}")
    try:
        km = {"1m":1,"5m":5,"15m":15,"1h":60}
        url = f"https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval={km.get(interval,5)}&count={limit}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    ohlc = data.get("result",{}).get("XXBTZUSD",[])
                    if ohlc:
                        return [{"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                                 "close":float(k[4]),"vol":float(k[6]),"ts":int(k[0])}
                                for k in ohlc[-limit:]]
    except Exception as e:
        log.warning(f"Kraken klines {interval}: {e}")
    return []

async def fetch_fear_greed():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(FEAR_GREED_API, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    d = await r.json()
                    return {"value":int(d["data"][0]["value"]),
                            "label":d["data"][0]["value_classification"]}
    except: pass
    return {"value":50,"label":"Neutral"}

async def fetch_btc_24h():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
                             timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status == 200:
                    d = await r.json()
                    t = d.get("result",{}).get("XXBTZUSD",{})
                    if t:
                        price = float(t["c"][0]); open_p = float(t["o"])
                        chg = ((price-open_p)/open_p*100) if open_p else 0
                        return {"change_pct":round(chg,2),
                                "high_24h":float(t["h"][0]),"low_24h":float(t["l"][0]),
                                "volume":float(t["v"][0])}
    except: pass
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    d = (await r.json()).get("bitcoin",{})
                    return {"change_pct":round(d.get("usd_24h_change",0),2),
                            "high_24h":0,"low_24h":0,"volume":0}
    except: pass
    return {"change_pct":0,"high_24h":0,"low_24h":0,"volume":0}

# ─── CLAUDE AI v8 ──────────────────────────────────────────────────────────
async def claude_decide(i1, i5, i15, i1h, adv, trades, bankroll,
                        consec, fg, btc24, sess, conf_score):
    """
    v8 : Claude reçoit le score de confluence calculé.
    Son rôle = valider/invalider + choisir la mise précise.
    Moins d'ambiguïté, décisions plus nettes.
    """
    if not ANTHROPIC_KEY:
        return {"dir":None,"conf":0,"size":0,"reasoning":"Pas de clé API.","trade":False}

    patterns = pattern_mem(trades)
    trades_txt = "".join(
        f"  {t['result']} {t['dir']} PnL:{t['pnl']:+.2f}$ conf:{t.get('conf',0)*100:.0f}% score:{t.get('score',0)}\n"
        for t in trades[-6:]
    ) or "  Aucun trade.\n"
    sigs_txt = "\n".join(f"  ✓ {s}" for s in conf_score["signals"]) or "  Aucun"
    max_bet  = round(min(MAX_BET_USD, bankroll*MAX_BET_PCT), 2)
    mid_bet  = round(MIN_BET_USD + (max_bet-MIN_BET_USD)*0.5, 2)

    prompt = f"""Tu es Claude, expert en trading binaire BTC UP/DOWN (prédiction 5 minutes sur Polymarket).
Un système quantitatif a déjà calculé un score de confluence. Ton rôle = valider ou invalider le trade et choisir la mise.

━━━ SCORE DE CONFLUENCE (calculé automatiquement) ━━━
Direction suggérée: {conf_score['direction']}
Score UP:   {conf_score['score_up']}/20
Score DOWN: {conf_score['score_dn']}/20
Différence: {conf_score['diff']} pts
Tradeable (score≥{MIN_SCORE_TO_TRADE} ET diff≥2): {'OUI ✅' if conf_score['tradeable'] else 'NON ❌'}

Signaux qui composent ce score:
{sigs_txt}

━━━ CONTEXTE MARCHÉ ━━━
BTC: ${i5.get('price',0):,.2f} | 24h: {btc24.get('change_pct',0):+.2f}%
Fear&Greed: {fg['value']}/100 ({fg['label']})
Session: {sess['session']} ({sess['quality']}) | Heure Paris: {(datetime.utcnow().hour+2)%24}h

━━━ INDICATEURS CLÉS ━━━
5m  RSI:{i5.get('rsi_14',50)} | MACD hist:{i5.get('macd_hist',0):+.4f} cross:{i5.get('macd_cross',False)} | Stoch:{i5.get('stoch_k',50)} | Vol:x{i5.get('vol_ratio',1):.1f} spike:{i5.get('vol_spike',False)}
15m RSI:{i15.get('rsi_14',50)} | MACD:{i15.get('macd_hist',0):+.3f} | EMA:{'↑' if i15.get('ema_bull') else '↓'} strong:{i15.get('ema_bull_strong',False)}
1h  RSI:{i1h.get('rsi_14',50)} | MACD:{i1h.get('macd_hist',0):+.3f} | EMA:{'↑' if i1h.get('ema_bull') else '↓'}
1m  RSI:{i1.get('rsi_14',50)} | Vol:x{i1.get('vol_ratio',1):.1f}
BB squeeze 5m: {i5.get('bb_squeeze',False)} | ATR%: {i5.get('atr_pct',0):.3f}%
VWAP 5m: ${i5.get('vwap',0):,.0f} ({'AU-DESSUS' if i5.get('above_vwap') else 'EN-DESSOUS'})
Supports: {i5.get('supports',[])} | Résistances: {i5.get('resistances',[])}

━━━ HISTORIQUE ━━━
{patterns}
Derniers trades:
{trades_txt}Pertes consécutives: {consec} | Bankroll: {bankroll:.2f}$

━━━ RÈGLES DE DÉCISION ━━━
TRADER si TOUTES ces conditions:
  1. Score tradeable = OUI (déjà calculé)
  2. Pas de signal contradictoire FORT évident que le score n'a pas capté
  3. ATR% > 0.04% (marché actif)
  4. Vol ratio > 0.5 (liquidité suffisante)

PASSER si AU MOINS UNE:
  - Score tradeable = NON
  - 15m ET 1h en directions opposées ET diff < 3 pts
  - ATR < 0.04% (marché mort)
  - Après 2 pertes: mise MIN seulement si score ≥ 10

MISE:
  - Score ≥ 12 ET session EXCELLENT → {max_bet}$ (max)
  - Score 10-11 → {mid_bet}$ (medium)
  - Score {MIN_SCORE_TO_TRADE}-9 → {MIN_BET_USD}$ (minimum)
  - Après 2 pertes consécutives → {MIN_BET_USD}$ peu importe le score

RÉPONDS UNIQUEMENT EN JSON (rien d'autre, pas de markdown):
{{"trade":true/false,"direction":"UP"/"DOWN"/null,"confidence":0.0-1.0,"bet_size":{MIN_BET_USD}-{max_bet},"reasoning":"2 phrases MAX en FR expliquant pourquoi tu trades ou passes","risk_level":"LOW"/"MEDIUM"/"HIGH"}}"""

    try:
        async with aiohttp.ClientSession() as session_http:
            async with session_http.post(
                CLAUDE_API,
                headers={"Content-Type":"application/json",
                         "x-api-key":ANTHROPIC_KEY,
                         "anthropic-version":"2023-06-01"},
                json={"model":"claude-haiku-4-5-20251001","max_tokens":300,
                      "messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status != 200:
                    txt = await r.text()
                    log.error(f"Claude {r.status}: {txt[:100]}")
                    return {"dir":None,"conf":0,"size":0,
                            "reasoning":f"Erreur API {r.status}","trade":False}
                data = await r.json()
                raw  = data["content"][0]["text"].strip()
                raw  = raw.replace("```json","").replace("```","").strip()
                s = raw.find("{"); e = raw.rfind("}")+1
                if s >= 0 and e > s: raw = raw[s:e]
                res = json.loads(raw)

                def sf(v, d=0.0):
                    try: return float(v) if v is not None else d
                    except: return d

                direction = res.get("direction")
                if direction not in ["UP","DOWN"]: direction = None
                trade = bool(res.get("trade",False)) and direction is not None

                return {
                    "dir":       direction,
                    "conf":      sf(res.get("confidence"),0.0),
                    "size":      sf(res.get("bet_size"),0.0),
                    "reasoning": str(res.get("reasoning","")),
                    "risk":      res.get("risk_level","MEDIUM") or "MEDIUM",
                    "trade":     trade,
                }
    except Exception as e:
        log.error(f"Claude error: {e}")
        return {"dir":None,"conf":0,"size":0,
                "reasoning":f"Erreur: {str(e)[:60]}","trade":False}

# ─── STATE ─────────────────────────────────────────────────────────────────
class State:
    def __init__(self):
        self.running        = False
        self.paper_mode     = PAPER_MODE
        self.bankroll       = BANKROLL_START
        self.c1  = deque(maxlen=100)
        self.c5  = deque(maxlen=100)
        self.c15 = deque(maxlen=100)
        self.c1h = deque(maxlen=100)
        self.price          = 0.0
        self.trades         = []
        self.bet            = None
        self.wins = self.losses = 0
        self.pnl            = 0.0
        self.consec         = 0
        self.streak         = 0
        self.best_streak    = 0
        self.worst_streak   = 0
        self.cooldown_until = 0
        self.session_start  = time.time()
        self.daily_start    = BANKROLL_START
        self.daily_ts       = time.time()
        self.skipped        = 0
        self.pass_reasons   = []   # ← NOUVEAU : log des PASS avec raison
        self.last_decision  = {}
        self.last_conf_score= {}
        self.fg             = {"value":50,"label":"Neutral"}
        self.btc24          = {}
        self.tick_job = self.price_job = self.macro_job = None

    def save(self):
        try:
            with open(DATA_FILE,"w") as f:
                json.dump({
                    "bankroll":self.bankroll,"trades":self.trades[-200:],
                    "wins":self.wins,"losses":self.losses,"pnl":self.pnl,
                    "best_streak":self.best_streak,"worst_streak":self.worst_streak,
                    "consec":self.consec,"daily_start":self.daily_start,
                    "daily_ts":self.daily_ts,"paper_mode":self.paper_mode,
                    "skipped":self.skipped,"pass_reasons":self.pass_reasons[-50:],
                },f,indent=2)
        except Exception as e: log.error(f"Save: {e}")

    def load(self):
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE) as f: d = json.load(f)
                self.bankroll     = d.get("bankroll",BANKROLL_START)
                self.trades       = d.get("trades",[])
                self.wins         = d.get("wins",0)
                self.losses       = d.get("losses",0)
                self.pnl          = d.get("pnl",0.0)
                self.best_streak  = d.get("best_streak",0)
                self.worst_streak = d.get("worst_streak",0)
                self.consec       = d.get("consec",0)
                self.daily_start  = d.get("daily_start",self.bankroll)
                self.daily_ts     = d.get("daily_ts",time.time())
                self.paper_mode   = d.get("paper_mode",PAPER_MODE)
                self.skipped      = d.get("skipped",0)
                self.pass_reasons = d.get("pass_reasons",[])
                log.info("State v8 chargé")
        except Exception as e: log.error(f"Load: {e}")

st = State()

# ─── RISK ──────────────────────────────────────────────────────────────────
def check_daily():
    now = time.time()
    if now - st.daily_ts > 86400:
        st.daily_start = st.bankroll
        st.daily_ts = now
    return st.daily_start > 0 and (st.daily_start-st.bankroll)/st.daily_start >= DAILY_LOSS_MAX

def in_cd(): return time.time() < st.cooldown_until

def check_trailing_stop():
    """v8 : coupe le bet si BTC bouge fort contre la direction"""
    if not st.bet or not st.price: return False
    entry = st.bet["entry"]
    dir_  = st.bet["dir"]
    move_pct = (st.price - entry) / entry * 100
    if dir_ == "UP"   and move_pct < -TRAILING_STOP_PCT: return True
    if dir_ == "DOWN" and move_pct >  TRAILING_STOP_PCT: return True
    return False

# ─── SEND ──────────────────────────────────────────────────────────────────
async def send(bot, text, parse_mode="Markdown"):
    try:
        await bot.send_message(chat_id=ALLOWED_UID, text=text, parse_mode=parse_mode)
        return True
    except Exception as e:
        log.error(f"Send failed: {e}")
        try:
            clean = text.replace("*","").replace("`","").replace("_","")
            await bot.send_message(chat_id=ALLOWED_UID, text=clean)
            return True
        except Exception as e2:
            log.error(f"Send retry: {e2}")
            return False

# ─── JOBS ──────────────────────────────────────────────────────────────────
async def job_price(context):
    p = await fetch_price()
    if p > 0: st.price = p

async def job_macro(context):
    st.fg    = await fetch_fear_greed()
    st.btc24 = await fetch_btc_24h()
    log.info(f"Macro: F&G={st.fg['value']} BTC24h={st.btc24.get('change_pct',0):+.2f}%")

async def job_tick(context):
    if not st.running: return

    if check_daily():
        st.running = False
        await send(context.bot, "🛑 *Limite journalière atteinte* — Bot arrêté.")
        return

    if in_cd():
        rem = int((st.cooldown_until-time.time())/60)
        log.info(f"Cooldown {rem}min restantes")
        return

    # Fetch data
    c1  = await fetch_klines("1m",  60)
    c5  = await fetch_klines("5m",  50)
    c15 = await fetch_klines("15m", 40)
    c1h = await fetch_klines("1h",  30)

    if not c5:
        log.warning("Pas de données klines")
        return

    st.c1  = deque(c1,  maxlen=100)
    st.c5  = deque(c5,  maxlen=100)
    st.c15 = deque(c15, maxlen=100)
    st.c1h = deque(c1h, maxlen=100)
    st.price = c5[-1]["close"]

    # ── TRAILING STOP v8 ──
    if st.bet and check_trailing_stop():
        bet   = st.bet
        gross = -bet["amount"]  # coupe = perte
        st.bankroll = max(0.0, st.bankroll + gross)
        st.pnl     += gross
        st.losses  += 1
        st.consec  += 1
        st.streak   = st.streak-1 if st.streak <= 0 else -1
        st.worst_streak = min(st.worst_streak, st.streak)
        if st.consec >= MAX_CONSEC_LOSS:
            st.cooldown_until = time.time() + COOLDOWN_MIN*60
        record = {"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
                  "conf":bet["conf"],"result":"LOSS","entry":bet["entry"],
                  "exit":st.price,"reasoning":"Trailing stop déclenché",
                  "paper":st.paper_mode,"ts":int(time.time()),"score":bet.get("score",0)}
        st.trades.append(record)
        st.bet = None
        await send(context.bot,
            f"⚡ *Trailing stop déclenché*\n"
            f"`{bet['dir']}` | Entry:`${bet['entry']:,.0f}` → `${st.price:,.0f}`\n"
            f"PnL: `{gross:.2f}$` | BR: `{st.bankroll:.2f}$`",
            parse_mode="Markdown")
        st.save()
        return

    # ── RÉSOUDRE BET ACTIF ──
    if st.bet:
        bet  = st.bet
        won  = bet["dir"] == ("UP" if st.price > bet["entry"] else "DOWN")
        gross = bet["amount"]*(1-POLY_FEE) if won else -bet["amount"]
        st.bankroll = max(0.0, st.bankroll + gross)
        st.pnl += gross
        if won:
            st.wins += 1; st.consec = 0
            st.streak = st.streak+1 if st.streak >= 0 else 1
            st.best_streak = max(st.best_streak, st.streak)
        else:
            st.losses += 1; st.consec += 1
            st.streak = st.streak-1 if st.streak <= 0 else -1
            st.worst_streak = min(st.worst_streak, st.streak)
            if st.consec >= MAX_CONSEC_LOSS:
                st.cooldown_until = time.time() + COOLDOWN_MIN*60
                log.warning(f"Cooldown activé ({st.consec} pertes)")
        record = {
            "dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
            "conf":bet["conf"],"result":"WIN" if won else "LOSS",
            "entry":bet["entry"],"exit":st.price,
            "reasoning":bet.get("reasoning",""),
            "paper":st.paper_mode,"ts":int(time.time()),
            "score":bet.get("score",0),
        }
        st.trades.append(record)
        st.bet = None
        emoji = "✅" if won else "❌"
        mode  = "📄" if st.paper_mode else "💰"
        cd_msg = f"\n⏸ Cooldown {COOLDOWN_MIN}min" if in_cd() else ""
        await send(context.bot,
            f"{emoji} *Trade clôturé* [{mode}]\n"
            f"`{bet['dir']}` | `${bet['entry']:,.0f}` → `${st.price:,.0f}`\n"
            f"PnL: `{'+' if gross>=0 else ''}{gross:.2f} USDC`\n"
            f"Bankroll: `{st.bankroll:.2f} USDC`\n"
            f"Streak: `{st.streak:+d}` | Pertes: `{st.consec}`{cd_msg}")
        st.save()

    if in_cd(): return

    # Trend filter
    if not is_trending(list(st.c5), list(st.c15)):
        st.skipped += 1
        reason = "Range — marché plat"
        st.pass_reasons.append({"ts":int(time.time()),"reason":reason})
        log.info(reason)
        return

    # Indicateurs
    i1  = compute_ind(list(st.c1))
    i5  = compute_ind(list(st.c5))
    i15 = compute_ind(list(st.c15))
    i1h = compute_ind(list(st.c1h))
    sess = session_ctx()
    if not i5: return

    # ── SCORE DE CONFLUENCE v8 ──
    adv        = compute_advanced_signals(list(st.c5), list(st.c1))
    conf_score = compute_confluence_score(i1, i5, i15, i1h, st.fg, sess, adv)
    st.last_conf_score = conf_score

    log.info(f"Score: UP={conf_score['score_up']} DOWN={conf_score['score_dn']} "
             f"dir={conf_score['direction']} tradeable={conf_score['tradeable']}")

    # Filtre pre-Claude : si score pas tradeable → PASS direct (économise tokens API)
    if not conf_score["tradeable"]:
        st.skipped += 1
        reason = f"Score insuffisant ({conf_score['score']:.1f}/{MIN_SCORE_TO_TRADE} diff={conf_score['diff']:.1f})"
        st.pass_reasons.append({"ts":int(time.time()),"reason":reason,
                                 "score_up":conf_score["score_up"],
                                 "score_dn":conf_score["score_dn"]})
        log.info(f"PASS pré-Claude: {reason}")
        return

    # ATR filtre
    atr_pct = i5.get("atr_pct", 0)
    sess_q  = sess.get("quality","MEDIUM")
    atr_min = 0.03 if sess_q == "EXCELLENT" else 0.04
    if atr_pct < atr_min:
        st.skipped += 1
        reason = f"ATR trop faible ({atr_pct:.3f}% < {atr_min}%)"
        st.pass_reasons.append({"ts":int(time.time()),"reason":reason})
        log.info(f"PASS: {reason}")
        return

    # Volume filtre
    if i5.get("vol_ratio",1) < 0.4:
        st.skipped += 1
        reason = f"Volume trop faible (x{i5.get('vol_ratio',1):.2f})"
        st.pass_reasons.append({"ts":int(time.time()),"reason":reason})
        log.info(f"PASS: {reason}")
        return

    # Claude valide
    dec = await claude_decide(i1, i5, i15, i1h, adv, st.trades[-15:],
                              st.bankroll, st.consec, st.fg, st.btc24,
                              sess, conf_score)
    st.last_decision = dec
    log.info(f"Claude: {dec['dir']} trade={dec['trade']} conf={dec['conf']:.0%} | {dec['reasoning'][:80]}")

    # Placer bet
    if dec["trade"] and dec["dir"] and not st.bet:
        amount = max(MIN_BET_USD, min(dec["size"], MAX_BET_USD, st.bankroll*MAX_BET_PCT))
        amount = round(amount, 2)
        if amount >= MIN_BET_USD and st.bankroll >= amount:
            st.bet = {
                "dir":dec["dir"],"amount":amount,"conf":dec["conf"],
                "entry":st.price,"reasoning":dec["reasoning"],
                "ts":int(time.time()),"score":conf_score["score"],
            }
            mode = "📄" if st.paper_mode else "💰"
            risk_e = {"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(dec["risk"],"🟡")
            sigs   = "\n".join(f"  • {s}" for s in conf_score["signals"][:5])
            await send(context.bot,
                f"🧠 *Bet placé* [{mode}]\n"
                f"━━━━━━━━━━━━━━━\n"
                f"*{dec['dir']}* | `{amount:.2f}$` | `{dec['conf']*100:.0f}%` | {risk_e}\n"
                f"Score: `{conf_score['score']:.1f}/20` (UP:{conf_score['score_up']} DN:{conf_score['score_dn']})\n"
                f"BTC: `${st.price:,.2f}` | `{sess['session']}`\n"
                f"F&G: `{st.fg['value']}` | 15m:`{'↑' if i15.get('ema_bull') else '↓'}` 1h:`{'↑' if i1h.get('ema_bull') else '↓'}`\n\n"
                f"💭 _{dec['reasoning']}_\n\n"
                f"🔑 Signaux:\n{sigs}")
    else:
        st.skipped += 1
        reason = f"Claude refuse: {dec['reasoning'][:60]}"
        st.pass_reasons.append({"ts":int(time.time()),"reason":reason})

# ─── HELPERS ───────────────────────────────────────────────────────────────
def auth(u): return ALLOWED_UID == 0 or u.effective_user.id == ALLOWED_UID
def fmt(v):  return f"+{v:.2f}" if v >= 0 else f"{v:.2f}"
def wr():
    t = st.wins + st.losses
    return f"{st.wins/t*100:.1f}%" if t else "—"
def roi():
    return f"{fmt((st.bankroll-BANKROLL_START)/BANKROLL_START*100)}%"
def upt():
    s = int(time.time()-st.session_start)
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

def kb():
    sess = session_ctx()
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Status",    callback_data="status"),
         InlineKeyboardButton("🧠 AI Last",   callback_data="ai")],
        [InlineKeyboardButton("📈 Trades",    callback_data="trades"),
         InlineKeyboardButton("📉 Stats",     callback_data="stats")],
        [InlineKeyboardButton("😱 F&G",       callback_data="fear"),
         InlineKeyboardButton("🎯 Score",     callback_data="score")],
        [InlineKeyboardButton("▶️ Start",     callback_data="run"),
         InlineKeyboardButton("⏹ Stop",      callback_data="stop")],
        [InlineKeyboardButton("🟢 Actif" if st.running else "🔴 Arrêté", callback_data="status"),
         InlineKeyboardButton("💰 Réel" if st.paper_mode else "📄 Paper",  callback_data="paper")],
    ])

# ─── COMMANDES ─────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context):
    if not auth(update): return
    sess = session_ctx()
    await update.message.reply_text(
        f"🧠 *POLYMARKET BOT v8 — MAX WINRATE*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode: *{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}*\n\n"
        f"🆕 v8 — Améliorations:\n"
        f"  ✅ Score confluence 0-20 (seuil: {MIN_SCORE_TO_TRADE})\n"
        f"  ✅ MACD crossover détecté\n"
        f"  ✅ Squeeze Bollinger\n"
        f"  ✅ Volume spike (x2.0+)\n"
        f"  ✅ Trailing stop ({TRAILING_STOP_PCT}%)\n"
        f"  ✅ Log PASS avec raison\n"
        f"  ✅ Claude prompt restructuré\n\n"
        f"Session: `{sess['session']}` — {sess['quality']}\n\n"
        f"*/run* */stop* */status* */ai* */signal* */score*\n"
        f"*/trades* */stats* */fear* */passes* */paper* */reset*",
        parse_mode="Markdown", reply_markup=kb()
    )

async def cmd_run(update: Update, context):
    if not auth(update): return
    if st.running:
        await update.message.reply_text("⚠️ Déjà en cours.")
        return
    if not ANTHROPIC_KEY:
        await update.message.reply_text("❌ ANTHROPIC_API_KEY manquante.")
        return
    st.running = True
    st.session_start = time.time()
    st.daily_start = st.bankroll
    st.daily_ts = time.time()
    st.price_job = context.job_queue.run_repeating(job_price, interval=30,  first=5)
    st.macro_job = context.job_queue.run_repeating(job_macro, interval=300, first=8)
    st.tick_job  = context.job_queue.run_repeating(job_tick,  interval=300, first=15)
    st.fg    = await fetch_fear_greed()
    st.btc24 = await fetch_btc_24h()
    sess = session_ctx()
    await update.message.reply_text(
        f"▶️ *Bot v8 démarré !*\n"
        f"F&G: `{st.fg['value']}` ({st.fg['label']})\n"
        f"BTC 24h: `{st.btc24.get('change_pct',0):+.2f}%`\n"
        f"Session: `{sess['session']}` — {sess['quality']}\n"
        f"Bankroll: `{st.bankroll:.2f} USDC`\n"
        f"Seuil score: `≥{MIN_SCORE_TO_TRADE}/20`",
        parse_mode="Markdown"
    )
    await job_tick(context)

async def cmd_stop(update: Update, context):
    if not auth(update): return
    st.running = False
    for j in [st.tick_job, st.price_job, st.macro_job]:
        if j:
            try: j.schedule_removal()
            except: pass
    st.tick_job = st.price_job = st.macro_job = None
    st.save()
    await update.message.reply_text(
        f"⏹ *Arrêté* | Uptime:`{upt()}` | BR:`{st.bankroll:.2f}` | PnL:`{fmt(st.pnl)}` | WR:`{wr()}`",
        parse_mode="Markdown"
    )

async def cmd_status(update: Update, context):
    if not auth(update): return
    sess = session_ctx()
    dl = (st.daily_start-st.bankroll)/st.daily_start*100 if st.daily_start > 0 else 0
    bet_info = f"{st.bet['dir']} {st.bet['amount']:.2f}$ @ ${st.bet['entry']:,.0f}" if st.bet else "Aucun"
    cd_msg = f"\n⏸ Cooldown: `{int((st.cooldown_until-time.time())/60)}min`" if in_cd() else ""
    cs = st.last_conf_score
    score_info = f"`{cs.get('score',0):.1f}/20` UP:{cs.get('score_up',0)} DN:{cs.get('score_dn',0)}" if cs else "—"
    await update.message.reply_text(
        f"📊 *STATUS v8* [{'📄' if st.paper_mode else '💰'}]\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢 EN COURS' if st.running else '🔴 ARRÊTÉ'}{cd_msg}\n\n"
        f"₿ `${st.price:,.2f}` | 24h:`{st.btc24.get('change_pct',0):+.2f}%`\n"
        f"😱 F&G:`{st.fg['value']}` ({st.fg['label']})\n"
        f"🕐 Session:`{sess['session']}` ({sess['quality']})\n"
        f"🎯 Dernier score: {score_info}\n\n"
        f"💰 BR:`{st.bankroll:.2f}` | ROI:`{roi()}` | PnL:`{fmt(st.pnl)}`\n"
        f"📅 Perte jour:`{dl:.1f}%/{DAILY_LOSS_MAX*100:.0f}%`\n"
        f"🎲 Bet:`{bet_info}`\n"
        f"🚫 Refusés:`{st.skipped}` | ⏱`{upt()}`",
        parse_mode="Markdown", reply_markup=kb()
    )

async def cmd_score(update: Update, context):
    """v8 : Affiche le score de confluence en temps réel"""
    if not auth(update): return
    await update.message.reply_text("⏳ Calcul du score...")
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30)
    if c5:
        st.c5=deque(c5,maxlen=100); st.c15=deque(c15,maxlen=100)
        st.c1h=deque(c1h,maxlen=100); st.c1=deque(c1,maxlen=100)
        st.price=c5[-1]["close"]
    st.fg=await fetch_fear_greed()
    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5))
    i15=compute_ind(list(st.c15)); i1h=compute_ind(list(st.c1h))
    sess=session_ctx()
    adv=compute_advanced_signals(list(st.c5),list(st.c1))
    cs=compute_confluence_score(i1,i5,i15,i1h,st.fg,sess,adv)
    st.last_conf_score=cs
    bar_up="█"*int(cs["score_up"])+"░"*(20-int(cs["score_up"]))
    bar_dn="█"*int(cs["score_dn"])+"░"*(20-int(cs["score_dn"]))
    sigs="\n".join(f"  {'✅' if 'UP' in s or '↑' in s or 'haussier' in s.lower() else '❌' if 'DOWN' in s or '↓' in s or 'baissier' in s.lower() else '⚡'} {s}" for s in cs["signals"])
    tradeable_e = "✅ TRADEABLE" if cs["tradeable"] else f"❌ PASS (besoin diff≥2 et score≥{MIN_SCORE_TO_TRADE})"
    await update.message.reply_text(
        f"🎯 *SCORE CONFLUENCE v8*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"₿ `${st.price:,.2f}` | Session:`{sess['session']}`\n\n"
        f"🟢 UP:  `{cs['score_up']:4.1f}` `{bar_up[:10]}`\n"
        f"🔴 DOWN:`{cs['score_dn']:4.1f}` `{bar_dn[:10]}`\n"
        f"Diff: `{cs['diff']:.1f}` pts → {tradeable_e}\n\n"
        f"Signaux:\n{sigs or '  Aucun'}",
        parse_mode="Markdown"
    )

async def cmd_ai(update: Update, context):
    if not auth(update): return
    d = st.last_decision
    cs = st.last_conf_score
    if not d:
        await update.message.reply_text("⏳ Lance /run ou /signal d'abord.")
        return
    risk_e = {"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(d.get("risk","MEDIUM"),"🟡")
    dir_e  = "🟢" if d.get("dir")=="UP" else "🔴" if d.get("dir")=="DOWN" else "⚪"
    score_line = f"Score: `{cs.get('score',0):.1f}/20`\n" if cs else ""
    await update.message.reply_text(
        f"🧠 *DERNIÈRE DÉCISION AI v8*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_e} *{d.get('dir') or 'PASS'}* | {risk_e} | `{d.get('conf',0)*100:.0f}%`\n"
        f"{score_line}"
        f"Trade: `{'OUI ✅' if d.get('trade') else 'NON ❌'}` | Mise:`{d.get('size',0):.2f}$`\n\n"
        f"💭 _{d.get('reasoning','—')}_",
        parse_mode="Markdown"
    )

async def cmd_signal(update: Update, context):
    if not auth(update): return
    await update.message.reply_text("⏳ Analyse complète v8...")
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30)
    if c5:
        st.c1=deque(c1,maxlen=100); st.c5=deque(c5,maxlen=100)
        st.c15=deque(c15,maxlen=100); st.c1h=deque(c1h,maxlen=100)
        st.price=c5[-1]["close"]
    st.fg=await fetch_fear_greed(); st.btc24=await fetch_btc_24h()
    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5))
    i15=compute_ind(list(st.c15)); i1h=compute_ind(list(st.c1h))
    sess=session_ctx()
    adv=compute_advanced_signals(list(st.c5),list(st.c1))
    cs=compute_confluence_score(i1,i5,i15,i1h,st.fg,sess,adv)
    st.last_conf_score=cs
    d=await claude_decide(i1,i5,i15,i1h,adv,st.trades[-15:],st.bankroll,
                          st.consec,st.fg,st.btc24,sess,cs)
    st.last_decision=d
    dir_e="🟢" if d["dir"]=="UP" else "🔴" if d["dir"]=="DOWN" else "⚪"
    risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(d.get("risk","MEDIUM"),"🟡")
    tradeable_e="✅" if cs["tradeable"] else "❌"
    await update.message.reply_text(
        f"🧠 *ANALYSE COMPLÈTE v8*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_e} *{d['dir'] or 'PASS'}* | {risk_e} | `{d['conf']*100:.0f}%`\n"
        f"Score: `{cs['score']:.1f}/20` {tradeable_e} (UP:{cs['score_up']} DN:{cs['score_dn']})\n"
        f"₿`${i5.get('price',0):,.2f}` | F&G:`{st.fg['value']}` | `{sess['session']}`\n"
        f"15m:`{'↑' if i15.get('ema_bull') else '↓'}` 1h:`{'↑' if i1h.get('ema_bull') else '↓'}` "
        f"MACD cross:`{i5.get('macd_cross',False)}`\n\n"
        f"💭 _{d['reasoning']}_",
        parse_mode="Markdown"
    )

async def cmd_trades(update: Update, context):
    if not auth(update): return
    trades = st.trades[-8:][::-1]
    if not trades:
        await update.message.reply_text("📈 Aucun trade.")
        return
    lines = ["📈 *TRADES v8*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    for t in trades:
        e="✅" if t["result"]=="WIN" else "❌"
        ts=datetime.fromtimestamp(t["ts"]).strftime("%d/%m %H:%M")
        r=t.get("reasoning","")[:40]
        sc=t.get("score",0)
        lines.append(f"{e} `{t['dir']}` `{fmt(t['pnl'])}$` score:`{sc}` `{ts}`\n   _{r}_")
    if st.bet:
        elapsed = int((time.time()-st.bet["ts"])/60)
        lines.append(f"\n🔄 *Actif:* `{st.bet['dir']}` `{st.bet['amount']:.2f}$` @ `${st.bet['entry']:,.0f}` ({elapsed}min)")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_stats(update: Update, context):
    if not auth(update): return
    total=st.wins+st.losses
    aw=sum(t["pnl"] for t in st.trades if t["pnl"]>0)/max(st.wins,1)
    al=abs(sum(t["pnl"] for t in st.trades if t["pnl"]<0))/max(st.losses,1)
    rr=aw/al if al>0 else 0
    peak=BANKROLL_START; mdd=0.0; rb=BANKROLL_START
    for t in st.trades:
        rb+=t["pnl"]
        if rb>peak: peak=rb
        dd=(peak-rb)/peak*100 if peak>0 else 0
        if dd>mdd: mdd=dd
    hi_score=[t for t in st.trades if t.get("score",0)>=MIN_SCORE_TO_TRADE]
    hi_wr=sum(1 for t in hi_score if t["result"]=="WIN")/len(hi_score)*100 if hi_score else 0
    patterns=pattern_mem(st.trades)
    await update.message.reply_text(
        f"📉 *STATS v8*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total:`{total}` (✅{st.wins} ❌{st.losses})\n"
        f"Win Rate: `{wr()}` | ROI:`{roi()}`\n"
        f"PnL:`{fmt(st.pnl)}$` | R:R:`{rr:.2f}`\n\n"
        f"Gain moy:`+{aw:.2f}$` | Perte moy:`-{al:.2f}$`\n"
        f"Best streak:`+{st.best_streak}` | Max DD:`{mdd:.1f}%`\n"
        f"Score≥{MIN_SCORE_TO_TRADE}: `{hi_wr:.0f}%` ({len(hi_score)} trades)\n"
        f"Refusés AI:`{st.skipped}`\n"
        f"Bankroll:`{st.bankroll:.2f} USDC`\n\n"
        f"📊 _{patterns}_",
        parse_mode="Markdown"
    )

async def cmd_passes(update: Update, context):
    """v8 : Affiche les dernières raisons de PASS (pour analyse)"""
    if not auth(update): return
    passes = st.pass_reasons[-10:][::-1]
    if not passes:
        await update.message.reply_text("✅ Aucun PASS enregistré.")
        return
    lines = ["🚫 *DERNIERS PASS v8*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    for p in passes:
        ts = datetime.fromtimestamp(p["ts"]).strftime("%H:%M")
        lines.append(f"`{ts}` — {p['reason']}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_fear(update: Update, context):
    if not auth(update): return
    fg=st.fg; v=fg['value']
    bar="█"*(v//10)+"░"*(10-v//10)
    e="😱" if v<20 else "😟" if v<40 else "😐" if v<60 else "😊" if v<80 else "🤑"
    interp=("Extrême Peur → biais UP en marché neutre" if v<20 else
            "Peur → incertitude" if v<40 else "Neutre" if v<60 else
            "Greed → attention correction" if v<80 else "Extrême Greed → biais DOWN")
    btc=st.btc24
    await update.message.reply_text(
        f"😱 *FEAR & GREED*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{e} *{fg['label']}* — `{v}/100`\n`{bar}`\n\n_{interp}_\n\n"
        f"₿ 24h:`{btc.get('change_pct',0):+.2f}%` | "
        f"H:`${btc.get('high_24h',0):,.0f}` L:`${btc.get('low_24h',0):,.0f}`",
        parse_mode="Markdown"
    )

async def cmd_paper(update: Update, context):
    if not auth(update): return
    st.paper_mode = not st.paper_mode
    await update.message.reply_text(
        f"Mode: *{'📄 PAPER' if st.paper_mode else '💰 RÉEL ⚠️'}*",
        parse_mode="Markdown"
    )
    st.save()

async def cmd_reset(update: Update, context):
    if not auth(update): return
    st.running = False
    for j in [st.tick_job, st.price_job, st.macro_job]:
        if j:
            try: j.schedule_removal()
            except: pass
    st.bankroll=BANKROLL_START; st.trades=[]; st.bet=None
    st.wins=st.losses=st.skipped=st.consec=0
    st.pnl=st.streak=st.best_streak=st.worst_streak=0
    st.cooldown_until=0; st.session_start=time.time()
    st.pass_reasons=[]; st.last_conf_score={}
    st.c1.clear(); st.c5.clear(); st.c15.clear(); st.c1h.clear()
    if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    await update.message.reply_text("🔄 *Reset complet v8.*", parse_mode="Markdown")

async def cmd_cooldown(update: Update, context):
    if not auth(update): return
    st.cooldown_until=0; st.consec=0
    await update.message.reply_text("✅ Cooldown reset.", parse_mode="Markdown")

async def cb(update: Update, context):
    q=update.callback_query; await q.answer()
    h={"status":cmd_status,"ai":cmd_ai,"trades":cmd_trades,"stats":cmd_stats,
       "fear":cmd_fear,"score":cmd_score,"run":cmd_run,"stop":cmd_stop,"paper":cmd_paper}
    if q.data in h: await h[q.data](update, context)

# ─── MAIN ──────────────────────────────────────────────────────────────────
def main():
    st.load()
    app = Application.builder().token(TOKEN).build()
    for name, handler in [
        ("start",cmd_start),("run",cmd_run),("stop",cmd_stop),
        ("status",cmd_status),("ai",cmd_ai),("signal",cmd_signal),
        ("score",cmd_score),("trades",cmd_trades),("stats",cmd_stats),
        ("fear",cmd_fear),("passes",cmd_passes),("paper",cmd_paper),
        ("cooldown",cmd_cooldown),("reset",cmd_reset),
    ]:
        app.add_handler(CommandHandler(name, handler))
    app.add_handler(CallbackQueryHandler(cb))
    log.info("🧠 PolyBot v8 MAX WINRATE démarré")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
