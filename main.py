"""
╔══════════════════════════════════════════════════════════════╗
║     POLYMARKET BTC BOT v6 — ENHANCED AI EDITION             ║
║     Fear&Greed | Macro | Sessions | S/R | Pattern Memory    ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import logging
import os
import json
import time
import math
import aiohttp
from datetime import datetime, timedelta
from collections import deque
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)

# ─── CONFIG ────────────────────────────────────────────────────────────────
TOKEN           = os.getenv("TELEGRAM_TOKEN", "VOTRE_TOKEN_ICI")
ALLOWED_UID     = int(os.getenv("ALLOWED_USER_ID", "0"))
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
PAPER_MODE      = os.getenv("PAPER_MODE", "true").lower() == "true"
BANKROLL_START  = float(os.getenv("BANKROLL", "50.0"))

MAX_BET_USD     = 5.0
MIN_BET_USD     = 1.0
MAX_BET_PCT     = 0.05
POLY_FEE        = 0.02
DAILY_LOSS_MAX  = 0.10
MAX_CONSEC_LOSS = 3
COOLDOWN_MIN    = 30

BINANCE_KLINES  = "https://api.binance.com/api/v3/klines"
CLAUDE_API      = "https://api.anthropic.com/v1/messages"
FEAR_GREED_API  = "https://api.alternative.me/fng/?limit=1"
POLY_MARKETS    = "https://clob.polymarket.com/markets"
DATA_FILE       = "polybot_v6_state.json"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("polybot_v6.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─── INDICATEURS ───────────────────────────────────────────────────────────
def ema(values, period):
    if len(values) < period: return values[-1] if values else 0
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]: e = v * k + e * (1 - k)
    return e

def rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains, losses = 0.0, 0.0
    for i in range(len(closes) - period, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0: gains += d
        else: losses -= d
    if losses == 0: return 100.0
    return round(100 - 100 / (1 + gains/losses), 2)

def macd_calc(closes):
    if len(closes) < 26: return 0, 0, 0
    ml = ema(closes, 12) - ema(closes, 26)
    sig = ml * 0.9
    return round(ml, 4), round(sig, 4), round(ml - sig, 4)

def bollinger(closes, period=20):
    if len(closes) < period: return None, None, None
    w = closes[-period:]
    mid = sum(w) / period
    std = math.sqrt(sum((x-mid)**2 for x in w) / period)
    return round(mid-2*std, 2), round(mid, 2), round(mid+2*std, 2)

def atr_calc(candles, period=14):
    if len(candles) < period+1: return 0.0
    trs = [max(c["high"]-c["low"], abs(c["high"]-candles[i-1]["close"]),
               abs(c["low"]-candles[i-1]["close"]))
           for i, c in enumerate(candles) if i > 0]
    return round(sum(trs[-period:]) / min(len(trs), period), 2)

def stoch(closes, highs, lows, period=14):
    if len(closes) < period: return 50.0, 50.0
    lo, hi = min(lows[-period:]), max(highs[-period:])
    if hi == lo: return 50.0, 50.0
    k = (closes[-1]-lo)/(hi-lo)*100
    d = (closes[-2]-lo)/(hi-lo)*100 if len(closes) >= period+1 else k
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

def support_resistance(candles, lookback=20):
    """Détecte les niveaux S/R par clustering des pivots"""
    if len(candles) < lookback: return [], []
    highs = [c["high"] for c in candles[-lookback:]]
    lows  = [c["low"]  for c in candles[-lookback:]]
    price = candles[-1]["close"]
    atr   = atr_calc(candles)

    # Pivots hauts et bas
    resistances, supports = [], []
    for i in range(2, len(highs)-2):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1] and highs[i] > highs[i-2] and highs[i] > highs[i+2]:
            resistances.append(round(highs[i], 2))
        if lows[i] < lows[i-1] and lows[i] < lows[i+1] and lows[i] < lows[i-2] and lows[i] < lows[i+2]:
            supports.append(round(lows[i], 2))

    # Filtrer : proches du prix actuel (dans 2x ATR)
    threshold = atr * 2
    near_r = sorted([r for r in resistances if r > price and r - price < threshold])[:3]
    near_s = sorted([s for s in supports if s < price and price - s < threshold], reverse=True)[:3]
    return near_s, near_r

def is_trending_market(candles_5m, candles_15m, threshold_pct=0.08):
    """Retourne True si le marché est en tendance (pas en range)"""
    if len(candles_5m) < 12 or len(candles_15m) < 6:
        return False  # pas assez de données, on attend

    hour = datetime.utcnow().hour
    # Session nuit (22h-7h UTC = 0h-9h Paris) : seuil plus strict
    if 22 <= hour or hour < 7:
        threshold_pct = 0.15  # besoin de plus de mouvement la nuit

    # Vérifier le mouvement sur les 30 dernières minutes (6 bougies 5m)
    recent = candles_5m[-6:]
    high = max(c["high"] for c in recent)
    low  = min(c["low"]  for c in recent)
    price = candles_5m[-1]["close"]
    if price == 0: return False
    range_pct = (high - low) / price * 100

    # Vérifier la pente des EMA sur 15m
    closes_15m = [c["close"] for c in candles_15m[-8:]]
    ema_slope = abs(closes_15m[-1] - closes_15m[0]) / closes_15m[0] * 100

    # Vérifier le momentum sur 5m (12 dernières bougies = 1h)
    closes_5m = [c["close"] for c in candles_5m[-12:]]
    momentum = abs(closes_5m[-1] - closes_5m[0]) / closes_5m[0] * 100

    # Volume moyen suffisant
    vols = [c["vol"] for c in candles_5m[-6:]]
    avg_vol = sum(vols) / len(vols) if vols else 0
    good_volume = avg_vol > 500

    # Marché en tendance si mouvement + volume suffisants
    is_trending = (range_pct > threshold_pct or momentum > threshold_pct * 0.8) and good_volume
    
    if not is_trending:
        log.info(f"Range détecté: range={range_pct:.3f}% mom={momentum:.3f}% vol={avg_vol:.0f}")
    
    return is_trending

def compute_indicators(candles):
    if len(candles) < 10: return {}
    closes = [c["close"] for c in candles]
    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    vols   = [c["vol"]   for c in candles]
    price  = closes[-1]

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, min(50, len(closes)))
    r7  = rsi(closes, 7)
    r14 = rsi(closes, 14)
    ml, sig, hist = macd_calc(closes)
    bb_l, bb_m, bb_h = bollinger(closes)
    atr = atr_calc(candles)
    stk, std = stoch(closes, highs, lows)
    wr  = williams_r(closes, highs, lows)
    vw  = vwap_calc(candles[-20:])
    avg_vol = sum(vols[-10:])/10 if len(vols) >= 10 else vols[-1]
    mom = closes[-1] - closes[-6] if len(closes) >= 6 else 0
    atr_pct = (atr/price*100) if price > 0 else 0
    sup, res = support_resistance(candles)

    return {
        "price": round(price, 2),
        "rsi_7": r7, "rsi_14": r14,
        "ema9": round(e9,2), "ema21": round(e21,2), "ema50": round(e50,2),
        "macd_line": ml, "macd_signal": sig, "macd_hist": hist,
        "bb_low": bb_l, "bb_mid": bb_m, "bb_high": bb_h,
        "atr": atr, "atr_pct": round(atr_pct, 3),
        "stoch_k": stk, "stoch_d": std,
        "williams_r": wr,
        "vwap": vw, "above_vwap": price > vw,
        "vol_ratio": round(vols[-1]/avg_vol, 2) if avg_vol > 0 else 1.0,
        "momentum": round(mom, 2),
        "ema_bull": e9 > e21,
        "supports": sup,
        "resistances": res,
        "candles_count": len(candles),
    }

# ─── SESSION TIMING ────────────────────────────────────────────────────────
def get_session_context():
    """Retourne le contexte de la session de trading actuelle (heure UTC)"""
    hour = datetime.utcnow().hour
    if 0 <= hour < 6:
        session = "ASIA_EARLY"
        quality = "MEDIUM — Asie early, peut trader si signal fort"
    elif 6 <= hour < 8:
        session = "ASIA_LATE"
        quality = "GOOD — Fin session Asie, bonne activité"
    elif 8 <= hour < 12:
        session = "EUROPE_OPEN"
        quality = "EXCELLENT — Ouverture Europe, très bonne liquidité"
    elif 12 <= hour < 14:
        session = "LUNCH"
        quality = "MEDIUM — Déjeuner, trader si signal très fort"
    elif 14 <= hour < 17:
        session = "US_OPEN"
        quality = "EXCELLENT — Ouverture US, meilleure liquidité"
    elif 17 <= hour < 20:
        session = "US_AFTERNOON"
        quality = "EXCELLENT — Après-midi US, tendances fortes"
    elif 20 <= hour < 22:
        session = "US_CLOSE"
        quality = "GOOD — Clôture US, encore actif"
    else:
        session = "OVERNIGHT"
        quality = "MEDIUM — Nuit, trader si signal clair"

    return {
        "session": session,
        "quality": quality,
        "hour_utc": hour,
        "hour_paris": (hour + 2) % 24,
    }

# ─── PATTERN MEMORY ────────────────────────────────────────────────────────
def analyze_patterns(trades):
    """Analyse les patterns gagnants/perdants dans l'historique"""
    if len(trades) < 5:
        return "Pas assez de trades pour analyser les patterns."

    wins  = [t for t in trades if t["result"] == "WIN"]
    losses= [t for t in trades if t["result"] == "LOSS"]

    # Win rate par direction
    up_trades   = [t for t in trades if t["dir"] == "UP"]
    down_trades = [t for t in trades if t["dir"] == "DOWN"]
    up_wr   = sum(1 for t in up_trades   if t["result"]=="WIN") / len(up_trades)   * 100 if up_trades   else 0
    down_wr = sum(1 for t in down_trades if t["result"]=="WIN") / len(down_trades) * 100 if down_trades else 0

    # Win rate par heure (si disponible)
    hour_stats = {}
    for t in trades:
        h = datetime.fromtimestamp(t["ts"]).hour
        if h not in hour_stats: hour_stats[h] = {"w":0,"l":0}
        if t["result"] == "WIN": hour_stats[h]["w"] += 1
        else: hour_stats[h]["l"] += 1

    best_hours = sorted(hour_stats.items(), key=lambda x: x[1]["w"]/(x[1]["w"]+x[1]["l"]), reverse=True)[:3]
    best_hours_str = ", ".join([f"{h}h ({s['w']}/{s['w']+s['l']})" for h, s in best_hours])

    # Confiance moyenne wins vs losses
    avg_conf_win  = sum(t.get("conf",0) for t in wins)  / len(wins)  * 100 if wins  else 0
    avg_conf_loss = sum(t.get("conf",0) for t in losses) / len(losses)* 100 if losses else 0

    return (
        f"Win rate UP: {up_wr:.0f}% ({len(up_trades)} trades) | "
        f"DOWN: {down_wr:.0f}% ({len(down_trades)} trades)\n"
        f"Confiance moy. wins: {avg_conf_win:.0f}% | losses: {avg_conf_loss:.0f}%\n"
        f"Meilleures heures: {best_hours_str}"
    )

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
                            log.info(f"Price {name}: ${p:,.2f}")
                            return p
        except Exception as e:
            log.warning(f"{name}: {e}")
    return st.current_price

async def fetch_klines(interval, limit=60):
    try:
        url = f"{BINANCE_KLINES}?symbol=BTCUSDT&interval={interval}&limit={limit}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list) and len(data) > 5:
                        return [{"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                                 "close":float(k[4]),"vol":float(k[5]),"ts":int(k[0])//1000}
                                for k in data]
    except Exception as e:
        log.warning(f"Binance {interval}: {e}")
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
        log.warning(f"Kraken {interval}: {e}")
    return []

async def fetch_fear_greed():
    """Fear & Greed Index (0=Extreme Fear, 100=Extreme Greed)"""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(FEAR_GREED_API, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    val  = int(data["data"][0]["value"])
                    name = data["data"][0]["value_classification"]
                    return {"value": val, "label": name}
    except Exception as e:
        log.warning(f"Fear&Greed: {e}")
    return {"value": 50, "label": "Neutral"}

async def fetch_btc_24h():
    """Variation BTC sur 24h"""
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    d = await r.json()
                    return {
                        "change_pct": round(float(d["priceChangePercent"]), 2),
                        "high_24h": float(d["highPrice"]),
                        "low_24h": float(d["lowPrice"]),
                        "volume_24h": round(float(d["volume"]), 2),
                    }
    except:
        pass
    return {"change_pct": 0, "high_24h": 0, "low_24h": 0, "volume_24h": 0}

# ─── POLYMARKET SENTIMENT ──────────────────────────────────────────────────
async def fetch_poly_btc_sentiment():
    """
    Récupère le sentiment du marché Polymarket BTC UP/DOWN.
    Si 80% parient DOWN → signal fort DOWN (et vice versa).
    """
    try:
        # Chercher marchés BTC actifs
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://clob.polymarket.com/markets",
                params={"active": "true", "closed": "false", "limit": "100"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                markets = data.get("data", []) if isinstance(data, dict) else data

        # Filtrer marchés BTC UP/DOWN 5min
        btc_markets = []
        for m in markets:
            if not isinstance(m, dict): continue
            q = m.get("question", "").lower()
            desc = m.get("description", "").lower()
            if ("bitcoin" in q or "btc" in q) and ("up" in q or "down" in q or "higher" in q or "lower" in q):
                btc_markets.append(m)

        if not btc_markets:
            return None

        # Prendre le marché le plus récent avec volume
        market = btc_markets[0]
        tokens = market.get("tokens", [])
        
        if len(tokens) < 2:
            return None

        # Trouver UP et DOWN
        up_price = down_price = None
        for token in tokens:
            outcome = token.get("outcome", "").upper()
            price = float(token.get("price", 0.5))
            if "UP" in outcome or "YES" in outcome or "HIGHER" in outcome:
                up_price = price
            elif "DOWN" in outcome or "NO" in outcome or "LOWER" in outcome:
                down_price = price

        if up_price is None or down_price is None:
            # Fallback: premier token = UP, deuxième = DOWN
            up_price   = float(tokens[0].get("price", 0.5))
            down_price = float(tokens[1].get("price", 0.5))

        # Sentiment: prix = probabilité implicite
        # up_price = 0.30 signifie 30% de chance UP → marché pense DOWN
        up_pct   = round(up_price   * 100, 1)
        down_pct = round(down_price * 100, 1)

        # Signal fort si déséquilibre > 65/35
        bias = None
        if up_pct >= 65:   bias = "UP"
        elif down_pct >= 65: bias = "DOWN"

        return {
            "up_pct":    up_pct,
            "down_pct":  down_pct,
            "bias":       bias,
            "market_q":  market.get("question", "")[:60],
            "confidence": max(up_pct, down_pct),
        }

    except Exception as e:
        log.warning(f"Poly sentiment: {e}")
        return None

# ─── CLAUDE AI BRAIN v6 ────────────────────────────────────────────────────
async def claude_decide(ind_1m, ind_5m, ind_15m, ind_1h, recent_trades, poly_sentiment,
                        bankroll, consec_losses, fear_greed, btc_24h, session):
    if not ANTHROPIC_KEY:
        return {"dir": None, "confidence": 0, "bet_size": 0,
                "reasoning": "Pas de clé API Claude.", "trade": False}

    # Polymarket sentiment string
    if poly_sentiment:
        poly_str = (f"UP: {poly_sentiment['up_pct']}% | DOWN: {poly_sentiment['down_pct']}% | "
                   f"Biais marché: {poly_sentiment['bias'] or 'NEUTRE'} | "
                   f"Marché: {poly_sentiment['market_q']}")
    else:
        poly_str = "Données Polymarket non disponibles — se baser sur TF uniquement."

    # Pattern memory
    pattern_analysis = analyze_patterns(recent_trades) if len(recent_trades) >= 5 else "Moins de 5 trades — pas encore de pattern."

    # Résumé trades récents
    trades_summary = ""
    for t in recent_trades[-8:]:
        ts = datetime.fromtimestamp(t["ts"]).strftime("%H:%M")
        trades_summary += f"- {t['result']} {t['dir']} | PnL:{t['pnl']:+.2f}$ | conf:{t.get('conf',0)*100:.0f}% | {ts}\n"
    if not trades_summary: trades_summary = "Aucun trade."

    # Support/Résistance
    sup_str = str(ind_5m.get("supports",  [])) if ind_5m.get("supports")  else "Aucun détecté"
    res_str = str(ind_5m.get("resistances",[])) if ind_5m.get("resistances") else "Aucun détecté"

    prompt = f"""Tu es un expert en trading de prédiction binaire Polymarket (BTC UP/DOWN 5 minutes).
Tu dois analyser TOUS les éléments et prendre la MEILLEURE décision possible.

═══════════════════════════════
CONTEXTE MACRO
═══════════════════════════════
Fear & Greed Index: {fear_greed['value']}/100 ({fear_greed['label']})
BTC 24h: {btc_24h['change_pct']:+.2f}% | High: ${btc_24h['high_24h']:,.0f} | Low: ${btc_24h['low_24h']:,.0f}
Session actuelle: {session['session']} ({session['quality']})
Heure Paris: {session['hour_paris']}h

═══════════════════════════════
SENTIMENT POLYMARKET (CRUCIAL)
═══════════════════════════════
{poly_str}

⚡ Le sentiment Polymarket = ce que les VRAIS traders parient avec du vrai argent.
C'est le signal le plus fiable disponible. Accorde-lui un poids MAJEUR.
Si Poly dit DOWN à 75%+ → forte probabilité DOWN, même si TF mitigés.
Si Poly dit UP à 75%+ → forte probabilité UP, même si TF mitigés.
Si Poly est neutre (45-55%) → se baser uniquement sur les TF.

═══════════════════════════════
PRIX & NIVEAUX CLÉS
═══════════════════════════════
Prix actuel: ${ind_5m.get('price',0):,.2f}
Supports proches: {sup_str}
Résistances proches: {res_str}
VWAP 5m: ${ind_5m.get('vwap',0):,.0f} ({'AU-DESSUS' if ind_5m.get('above_vwap') else 'EN-DESSOUS'})

═══════════════════════════════
INDICATEURS TECHNIQUES
═══════════════════════════════
── 1 MINUTE ──
RSI: {ind_1m.get('rsi_14',50)} | Stoch K/D: {ind_1m.get('stoch_k',50)}/{ind_1m.get('stoch_d',50)}
EMA9/21: {'HAUSSIER' if ind_1m.get('ema_bull') else 'BAISSIER'} | MACD: {ind_1m.get('macd_hist',0):.4f}
Williams %R: {ind_1m.get('williams_r',-50)} | Vol ratio: x{ind_1m.get('vol_ratio',1):.2f}
Momentum: {ind_1m.get('momentum',0):+.2f}

── 5 MINUTES (principal) ──
RSI 7/14: {ind_5m.get('rsi_7',50)}/{ind_5m.get('rsi_14',50)}
EMA9/21/50: {ind_5m.get('ema9',0):.0f}/{ind_5m.get('ema21',0):.0f}/{ind_5m.get('ema50',0):.0f} ({'HAUSSIER' if ind_5m.get('ema_bull') else 'BAISSIER'})
MACD hist: {ind_5m.get('macd_hist',0):.4f}
Bollinger: {ind_5m.get('bb_low',0):.0f} / {ind_5m.get('bb_mid',0):.0f} / {ind_5m.get('bb_high',0):.0f}
Stoch: {ind_5m.get('stoch_k',50)}/{ind_5m.get('stoch_d',50)} | Williams: {ind_5m.get('williams_r',-50)}
ATR: ${ind_5m.get('atr',0):.0f} ({ind_5m.get('atr_pct',0):.3f}%)

── 15 MINUTES ──
RSI: {ind_15m.get('rsi_14',50)} | EMA: {'HAUSSIER' if ind_15m.get('ema_bull') else 'BAISSIER'}
MACD: {ind_15m.get('macd_hist',0):.4f} | Momentum: {ind_15m.get('momentum',0):+.2f}

── 1 HEURE (tendance long terme) ──
RSI: {ind_1h.get('rsi_14',50)} | EMA: {'HAUSSIER' if ind_1h.get('ema_bull') else 'BAISSIER'}
MACD: {ind_1h.get('macd_hist',0):.4f} | Momentum: {ind_1h.get('momentum',0):+.2f}

═══════════════════════════════
MÉMOIRE & PATTERNS
═══════════════════════════════
{pattern_analysis}

Derniers trades:
{trades_summary}
Pertes consécutives: {consec_losses}
Bankroll: {bankroll:.2f} USDC

═══════════════════════════════
RÈGLES DE DÉCISION
═══════════════════════════════
OBJECTIF: Maximiser le win rate. Ne trader QUE les setups à haute probabilité.

STRATÉGIE HAUTE PROBABILITÉ:
1. Signal FORT (trader avec mise normale):
   - 3+ timeframes alignés dans la même direction
   - RSI < 25 (UP) ou RSI > 75 (DOWN) sur le 5m
   - MACD confirme sur au moins 2 TF
   - Volume ratio > 1.3 (confirmation institutionnelle)

2. Signal MOYEN (trader avec mise réduite 50%):
   - 2 timeframes alignés + RSI < 35 ou > 65
   - Rebond clair sur support/résistance avec confirmation
   - Session EXCELLENT ou GOOD uniquement

3. NE PAS TRADER si:
   - Tous les TF contradictoires
   - ATR < 0.05% (marché vraiment mort)
   - Prix exactement sur S/R sans direction claire
   - 3 pertes consécutives sur le même setup (attendre changement)

4. RÈGLES AVANCÉES:
   - RSI divergence: RSI monte mais prix baisse → signal UP fort
   - Après rebond RSI extrême (< 20) → chercher confirmation UP sur 1m
   - Fear&Greed < 20: favoriser UP (marché survendu globalement)
   - Fear&Greed > 80: favoriser DOWN (marché suracheté globalement)
   - Session US_OPEN/US_AFTERNOON: augmenter confiance de 10%

5. MISE:
   - Signal FORT: {min(MAX_BET_USD, bankroll*MAX_BET_PCT):.2f}$ max
   - Signal MOYEN: {MIN_BET_USD + (min(MAX_BET_USD, bankroll*MAX_BET_PCT)-MIN_BET_USD)*0.5:.2f}$
   - Après 2 pertes: {MIN_BET_USD}$ minimum seulement

RÉPONDS UNIQUEMENT EN JSON:
{{
  "trade": true/false,
  "direction": "UP" ou "DOWN" ou null,
  "confidence": 0.0-1.0,
  "bet_size": montant USDC,
  "reasoning": "Explication 2-3 phrases en français",
  "key_signals": ["signal1", "signal2", "signal3", "signal4"],
  "risk_level": "LOW" ou "MEDIUM" ou "HIGH",
  "session_ok": true/false,
  "main_concern": "principale préoccupation si pas de trade"
}}"""

    try:
        async with aiohttp.ClientSession() as session_http:
            async with session_http.post(
                CLAUDE_API,
                headers={"Content-Type":"application/json",
                         "x-api-key": ANTHROPIC_KEY,
                         "anthropic-version":"2023-06-01"},
                json={"model":"claude-haiku-4-5-20251001",
                      "max_tokens":600,
                      "messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status != 200:
                    log.error(f"Claude API {r.status}")
                    return {"dir":None,"confidence":0,"bet_size":0,
                            "reasoning":f"Erreur API ({r.status})","trade":False}
                data = await r.json()
                raw  = data["content"][0]["text"].strip()
                # Robust JSON extraction
                raw = raw.replace("```json","").replace("```","").strip()
                # Extract JSON object even if extra text around it
                start = raw.find("{")
                end   = raw.rfind("}") + 1
                if start >= 0 and end > start:
                    raw = raw[start:end]
                res  = json.loads(raw)
                def safe_float(val, default=0.0):
                    try: return float(val) if val is not None else default
                    except: return default

                direction = res.get("direction")
                if direction not in ["UP", "DOWN"]: direction = None

                return {
                    "dir":         direction,
                    "confidence":  safe_float(res.get("confidence"), 0.0),
                    "bet_size":    safe_float(res.get("bet_size"), 0.0),
                    "reasoning":   str(res.get("reasoning","")),
                    "key_signals": res.get("key_signals",[]) or [],
                    "risk_level":  res.get("risk_level","MEDIUM") or "MEDIUM",
                    "session_ok":  bool(res.get("session_ok", True)),
                    "main_concern":str(res.get("main_concern","")),
                    "trade":       bool(res.get("trade", False)) and direction is not None,
                }
    except Exception as e:
        log.error(f"Claude: {e}")
        return {"dir":None,"confidence":0,"bet_size":0,
                "reasoning":f"Erreur: {str(e)[:80]}","trade":False}

# ─── STATE ─────────────────────────────────────────────────────────────────
class State:
    def __init__(self):
        self.running          = False
        self.paper_mode       = PAPER_MODE
        self.bankroll         = BANKROLL_START
        self.candles_1m       = deque(maxlen=100)
        self.candles_5m       = deque(maxlen=100)
        self.candles_15m      = deque(maxlen=100)
        self.candles_1h       = deque(maxlen=100)
        self.current_price    = 0.0
        self.trades           = []
        self.active_bet       = None
        self.wins             = 0
        self.losses           = 0
        self.total_pnl        = 0.0
        self.alerts           = True
        self.daily_start_br   = BANKROLL_START
        self.daily_reset_ts   = time.time()
        self.streak           = 0
        self.best_streak      = 0
        self.worst_streak     = 0
        self.consec_losses    = 0
        self.cooldown_until   = 0
        self.session_start    = time.time()
        self.last_ai_decision = {}
        self.skipped_trades   = 0
        self.fear_greed       = {"value": 50, "label": "Neutral"}
        self.btc_24h          = {}
        self.poly_sentiment   = None
        self.tick_job         = None
        self.price_job        = None
        self.macro_job        = None

    def save(self):
        try:
            with open(DATA_FILE, "w") as f:
                json.dump({
                    "bankroll": self.bankroll,
                    "trades": self.trades[-200:],
                    "wins": self.wins, "losses": self.losses,
                    "total_pnl": self.total_pnl,
                    "best_streak": self.best_streak,
                    "worst_streak": self.worst_streak,
                    "consec_losses": self.consec_losses,
                    "daily_start_br": self.daily_start_br,
                    "daily_reset_ts": self.daily_reset_ts,
                    "paper_mode": self.paper_mode,
                    "skipped_trades": self.skipped_trades,
                }, f, indent=2)
        except Exception as e: log.error(f"Save: {e}")

    def load(self):
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE) as f: d = json.load(f)
                self.bankroll       = d.get("bankroll", BANKROLL_START)
                self.trades         = d.get("trades", [])
                self.wins           = d.get("wins", 0)
                self.losses         = d.get("losses", 0)
                self.total_pnl      = d.get("total_pnl", 0.0)
                self.best_streak    = d.get("best_streak", 0)
                self.worst_streak   = d.get("worst_streak", 0)
                self.consec_losses  = d.get("consec_losses", 0)
                self.daily_start_br = d.get("daily_start_br", self.bankroll)
                self.daily_reset_ts = d.get("daily_reset_ts", time.time())
                self.paper_mode     = d.get("paper_mode", PAPER_MODE)
                self.skipped_trades = d.get("skipped_trades", 0)
                log.info("State chargé")
        except Exception as e: log.error(f"Load: {e}")

st = State()

# ─── RISK ──────────────────────────────────────────────────────────────────
def check_daily_limit():
    now = time.time()
    if now - st.daily_reset_ts > 86400:
        st.daily_start_br = st.bankroll
        st.daily_reset_ts = now
    if st.daily_start_br == 0: return False
    return (st.daily_start_br - st.bankroll) / st.daily_start_br >= DAILY_LOSS_MAX

def in_cooldown(): return time.time() < st.cooldown_until

# ─── JOBS ──────────────────────────────────────────────────────────────────
async def price_update(context):
    p = await fetch_price()
    if p > 0: st.current_price = p

async def macro_update(context):
    """Met à jour Fear&Greed, données 24h et sentiment Polymarket toutes les 5 min"""
    st.fear_greed     = await fetch_fear_greed()
    st.btc_24h        = await fetch_btc_24h()
    st.poly_sentiment = await fetch_poly_btc_sentiment()
    poly_str = f"Poly={st.poly_sentiment['bias']} {st.poly_sentiment['confidence']:.0f}%" if st.poly_sentiment else "Poly=N/A"
    log.info(f"Macro: F&G={st.fear_greed['value']} BTC24h={st.btc_24h.get('change_pct',0):+.2f}% {poly_str}")

async def tick(context: ContextTypes.DEFAULT_TYPE):
    if not st.running: return
    if check_daily_limit():
        st.running = False
        await context.bot.send_message(chat_id=ALLOWED_UID,
            text="🛑 *Limite journalière atteinte* — Bot arrêté.", parse_mode="Markdown")
        return
    if in_cooldown():
        log.info(f"Cooldown {int((st.cooldown_until-time.time())/60)}min")
        return

    # Fetch 4 timeframes
    c1  = await fetch_klines("1m",  60)
    c5  = await fetch_klines("5m",  50)
    c15 = await fetch_klines("15m", 40)
    c1h = await fetch_klines("1h",  30)

    if not c5: log.warning("Pas de données"); return

    st.candles_1m  = deque(c1,  maxlen=100)
    st.candles_5m  = deque(c5,  maxlen=100)
    st.candles_15m = deque(c15, maxlen=100)
    st.candles_1h  = deque(c1h, maxlen=100)
    st.current_price = c5[-1]["close"]

    # Résoudre bet actif
    if st.active_bet:
        bet   = st.active_bet
        won   = bet["dir"] == ("UP" if st.current_price > bet["entry_price"] else "DOWN")
        gross = bet["amount"] * (1-POLY_FEE) if won else -bet["amount"]

        st.bankroll   = max(0.0, st.bankroll + gross)
        st.total_pnl += gross

        if won:
            st.wins += 1; st.consec_losses = 0
            st.streak = st.streak+1 if st.streak >= 0 else 1
            st.best_streak = max(st.best_streak, st.streak)
        else:
            st.losses += 1; st.consec_losses += 1
            st.streak = st.streak-1 if st.streak <= 0 else -1
            st.worst_streak = min(st.worst_streak, st.streak)
            if st.consec_losses >= MAX_CONSEC_LOSS:
                st.cooldown_until = time.time() + COOLDOWN_MIN*60

        record = {"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
                  "conf":bet["conf"],"result":"WIN" if won else "LOSS",
                  "entry":bet["entry_price"],"exit":st.current_price,
                  "ai_reasoning":bet.get("ai_reasoning",""),
                  "paper":st.paper_mode,"ts":int(time.time())}
        st.trades.append(record)
        st.active_bet = None

        if st.alerts:
            emoji = "✅" if won else "❌"
            cd_msg = f"\n⏸ Cooldown {COOLDOWN_MIN}min activé" if in_cooldown() else ""
            await context.bot.send_message(chat_id=ALLOWED_UID,
                text=(f"{emoji} *Trade clôturé* [{'📄' if st.paper_mode else '💰'}]\n"
                      f"`{bet['dir']}` | Entrée `${bet['entry_price']:,.0f}` → `${st.current_price:,.0f}`\n"
                      f"PnL: `{'+' if gross>=0 else ''}{gross:.2f} USDC` | Bankroll: `{st.bankroll:.2f}`\n"
                      f"Streak: `{st.streak:+d}` | F&G: `{st.fear_greed['value']}`{cd_msg}"),
                parse_mode="Markdown")
        st.save()

    if in_cooldown(): return

    # Indicateurs
    ind_1m  = compute_indicators(list(st.candles_1m))
    ind_5m  = compute_indicators(list(st.candles_5m))
    ind_15m = compute_indicators(list(st.candles_15m))
    ind_1h  = compute_indicators(list(st.candles_1h))
    session = get_session_context()

    if not ind_5m: return

    # Filtre tendance — pause si marché en range
    if not is_trending_market(list(st.candles_5m), list(st.candles_15m)):
        st.skipped_trades += 1
        log.info("Marché en range — pause automatique")
        return

    # Claude AI décide
    decision = await claude_decide(
        ind_1m, ind_5m, ind_15m, ind_1h,
        st.trades[-15:], st.poly_sentiment, st.bankroll, st.consec_losses,
        st.fear_greed, st.btc_24h, session
    )
    st.last_ai_decision = decision

    if decision["trade"] and decision["dir"] and not st.active_bet:
        amount = max(MIN_BET_USD, min(decision["bet_size"], MAX_BET_USD, st.bankroll*MAX_BET_PCT))
        amount = round(amount, 2)
        if amount >= MIN_BET_USD and st.bankroll >= amount:
            st.active_bet = {
                "dir": decision["dir"], "amount": amount,
                "conf": decision["confidence"],
                "entry_price": st.current_price,
                "ai_reasoning": decision["reasoning"],
                "ts": int(time.time()),
            }
            if st.alerts:
                risk_e = {"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(decision.get("risk_level","MEDIUM"),"🟡")
                sig_txt = "\n".join(f"  • {s}" for s in decision.get("key_signals",[])[:4])
                await context.bot.send_message(chat_id=ALLOWED_UID,
                    text=(f"🧠 *Claude AI Bet* [{'📄' if st.paper_mode else '💰'}]\n"
                          f"━━━━━━━━━━━━━━━━━\n"
                          f"*{decision['dir']}* | `{amount:.2f}$` | `{decision['confidence']*100:.0f}%` | {risk_e}\n"
                          f"BTC: `${st.current_price:,.2f}` | Session: `{session['session']}`\n"
                          f"F&G: `{st.fear_greed['value']}` ({st.fear_greed['label']})\n\n"
                          f"💭 _{decision['reasoning']}_\n\n"
                          f"🔑 Signaux:\n{sig_txt}"),
                    parse_mode="Markdown")
    else:
        st.skipped_trades += 1
        log.info(f"PASS: {decision.get('main_concern','') or decision.get('reasoning','')[:80]}")

# ─── HELPERS ───────────────────────────────────────────────────────────────
def auth(u): return ALLOWED_UID == 0 or u.effective_user.id == ALLOWED_UID
def fmt(v): return f"+{v:.2f}" if v >= 0 else f"{v:.2f}"
def uptime():
    s = int(time.time() - st.session_start)
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"
def win_rate():
    t = st.wins + st.losses
    return f"{st.wins/t*100:.1f}%" if t > 0 else "—"
def roi():
    r = (st.bankroll - BANKROLL_START) / BANKROLL_START * 100
    return f"{fmt(r)}%"

def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Status",    callback_data="status"),
         InlineKeyboardButton("🧠 AI Last",   callback_data="ai_last")],
        [InlineKeyboardButton("📈 Trades",    callback_data="trades"),
         InlineKeyboardButton("📉 Stats",     callback_data="stats")],
        [InlineKeyboardButton("😱 Fear&Greed",callback_data="fear"),
         InlineKeyboardButton("🕐 Session",   callback_data="session")],
        [InlineKeyboardButton("▶️ Start",     callback_data="run"),
         InlineKeyboardButton("⏹ Stop",      callback_data="stop")],
        [InlineKeyboardButton("🟢 Actif" if st.running else "🔴 Arrêté", callback_data="status"),
         InlineKeyboardButton("💰 Passer Réel" if st.paper_mode else "📄 Paper", callback_data="toggle_paper")],
    ])

# ─── COMMANDES ─────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    session = get_session_context()
    await update.message.reply_text(
        f"🧠 *POLYMARKET BOT v6 — AI ENHANCED*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode: *{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}*\n\n"
        f"✅ 4 timeframes (1m/5m/15m/1h)\n"
        f"✅ Fear & Greed Index\n"
        f"✅ Sessions de trading\n"
        f"✅ Support/Résistance\n"
        f"✅ Mémoire des patterns\n"
        f"✅ Contexte macro 24h\n\n"
        f"Session: `{session['session']}` — {session['quality']}\n\n"
        f"*/run* */stop* */status* */ai* */signal*\n"
        f"*/trades* */stats* */fear* */session* */reset*",
        parse_mode="Markdown", reply_markup=main_kb()
    )

async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    if st.running:
        await update.message.reply_text("⚠️ Déjà en cours.")
        return
    if not ANTHROPIC_KEY:
        await update.message.reply_text("❌ ANTHROPIC_API_KEY manquante dans Railway.")
        return

    st.running = True
    st.session_start = time.time()
    st.daily_start_br = st.bankroll

    st.price_job = context.job_queue.run_repeating(price_update, interval=30,   first=5)
    st.macro_job = context.job_queue.run_repeating(macro_update, interval=300,  first=10)
    st.tick_job  = context.job_queue.run_repeating(tick,         interval=300,  first=15)

    # Fetch macro immédiat
    st.fear_greed     = await fetch_fear_greed()
    st.btc_24h        = await fetch_btc_24h()
    st.poly_sentiment = await fetch_poly_btc_sentiment()
    session           = get_session_context()

    await update.message.reply_text(
        f"▶️ *Bot v6 AI démarré !*\n"
        f"F&G: `{st.fear_greed['value']}` ({st.fear_greed['label']})\n"
        f"BTC 24h: `{st.btc_24h.get('change_pct',0):+.2f}%`\n"
        f"Session: `{session['session']}` — {session['quality']}\n"
        f"Bankroll: `{st.bankroll:.2f} USDC`",
        parse_mode="Markdown"
    )
    await tick(context)

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    st.running = False
    for j in [st.tick_job, st.price_job, st.macro_job]:
        if j: j.schedule_removal()
    st.tick_job = st.price_job = st.macro_job = None
    st.save()
    await update.message.reply_text(
        f"⏹ *Bot arrêté*\n"
        f"Uptime: `{uptime()}` | Bankroll: `{st.bankroll:.2f}` | PnL: `{fmt(st.total_pnl)}` | WR: `{win_rate()}`",
        parse_mode="Markdown"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    session = get_session_context()
    daily_loss = (st.daily_start_br-st.bankroll)/st.daily_start_br*100 if st.daily_start_br > 0 else 0
    bet_info = f"{st.active_bet['dir']} {st.active_bet['amount']:.2f}$ @ ${st.active_bet['entry_price']:,.0f}" if st.active_bet else "Aucun"
    cd_msg = f"\n⏸ Cooldown: `{int((st.cooldown_until-time.time())/60)}min`" if in_cooldown() else ""

    await update.message.reply_text(
        f"📊 *STATUS v6* [{'📄' if st.paper_mode else '💰'}]\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢 EN COURS' if st.running else '🔴 ARRÊTÉ'}{cd_msg}\n\n"
        f"₿ `${st.current_price:,.2f}` | 24h: `{st.btc_24h.get('change_pct',0):+.2f}%`\n"
        f"😱 F&G: `{st.fear_greed['value']}` ({st.fear_greed['label']})\n"
        f"🕐 Session: `{session['session']}`\n\n"
        f"💰 Bankroll: `{st.bankroll:.2f}` | ROI: `{roi()}`\n"
        f"📅 Perte jour: `{daily_loss:.1f}%/{DAILY_LOSS_MAX*100:.0f}%`\n"
        f"🎯 Bet actif: `{bet_info}`\n"
        f"🚫 Refusés: `{st.skipped_trades}` | Uptime: `{uptime()}`",
        parse_mode="Markdown", reply_markup=main_kb()
    )

async def cmd_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    d = st.last_ai_decision
    if not d:
        await update.message.reply_text("⏳ Lance /run d'abord.")
        return
    risk_e = {"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(d.get("risk_level","MEDIUM"),"🟡")
    dir_e  = "🟢" if d.get("dir")=="UP" else "🔴" if d.get("dir")=="DOWN" else "⚪"
    sigs   = "\n".join(f"  • {s}" for s in d.get("key_signals",[]))
    await update.message.reply_text(
        f"🧠 *DERNIÈRE DÉCISION AI*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_e} *{d.get('dir') or 'PASS'}* | {risk_e} `{d.get('risk_level','?')}`\n"
        f"Confiance: `{d.get('confidence',0)*100:.0f}%` | Mise: `{d.get('bet_size',0):.2f}$`\n"
        f"Trade: `{'OUI ✅' if d.get('trade') else 'NON ❌'}`\n\n"
        f"💭 _{d.get('reasoning','—')}_\n\n"
        f"🔑 Signaux:\n{sigs or '  —'}\n\n"
        f"⚠️ _{d.get('main_concern','')}_",
        parse_mode="Markdown"
    )

async def cmd_fear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    fg = st.fear_greed
    val = fg['value']
    bar = "█" * (val//10) + "░" * (10 - val//10)
    emoji = "😱" if val<20 else "😟" if val<40 else "😐" if val<60 else "😊" if val<80 else "🤑"
    interp = ("Extrême Peur — souvent bon moment pour acheter" if val<20 else
              "Peur — marché incertain" if val<40 else
              "Neutre — pas de biais fort" if val<60 else
              "Greed — attention aux retournements" if val<80 else
              "Extrême Greed — risque élevé de correction")
    btc = st.btc_24h
    await update.message.reply_text(
        f"😱 *FEAR & GREED INDEX*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} *{fg['label']}* — `{val}/100`\n"
        f"`{bar}`\n\n"
        f"_{interp}_\n\n"
        f"₿ *BTC 24h:*\n"
        f"Variation: `{btc.get('change_pct',0):+.2f}%`\n"
        f"High: `${btc.get('high_24h',0):,.0f}` | Low: `${btc.get('low_24h',0):,.0f}`",
        parse_mode="Markdown"
    )

async def cmd_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    s = get_session_context()
    sessions_info = (
        "🌏 Asia (0-8h UTC): Volatilité modérée\n"
        "🌍 Europe Open (8-12h): Bonne liquidité\n"
        "😴 Lunch (12-14h): Éviter\n"
        "🇺🇸 US Open (14-17h): MEILLEUR moment\n"
        "🌆 US Afternoon (17-20h): Bonnes tendances\n"
        "🌙 Overnight (22-0h): Éviter"
    )
    await update.message.reply_text(
        f"🕐 *SESSION ACTUELLE*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Session: *{s['session']}*\n"
        f"Heure Paris: `{s['hour_paris']}h`\n"
        f"Qualité: _{s['quality']}_\n\n"
        f"*Calendrier des sessions:*\n{sessions_info}",
        parse_mode="Markdown"
    )

async def cmd_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    await update.message.reply_text("⏳ Analyse complète en cours...")
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30)
    if c5:
        st.candles_1m=deque(c1,maxlen=100); st.candles_5m=deque(c5,maxlen=100)
        st.candles_15m=deque(c15,maxlen=100); st.candles_1h=deque(c1h,maxlen=100)
        st.current_price=c5[-1]["close"]
    fg = await fetch_fear_greed(); st.fear_greed = fg
    btc24 = await fetch_btc_24h(); st.btc_24h = btc24
    session = get_session_context()
    ind_1m=compute_indicators(list(st.candles_1m))
    ind_5m=compute_indicators(list(st.candles_5m))
    ind_15m=compute_indicators(list(st.candles_15m))
    ind_1h=compute_indicators(list(st.candles_1h))
    d = await claude_decide(ind_1m,ind_5m,ind_15m,ind_1h,st.trades[-15:],
                            st.poly_sentiment,st.bankroll,st.consec_losses,fg,btc24,session)
    st.last_ai_decision = d
    dir_e="🟢" if d["dir"]=="UP" else "🔴" if d["dir"]=="DOWN" else "⚪"
    risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(d.get("risk_level","MEDIUM"),"🟡")
    sigs="\n".join(f"  • {s}" for s in d.get("key_signals",[])[:5])
    await update.message.reply_text(
        f"🧠 *ANALYSE CLAUDE AI v6*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_e} *{d['dir'] or 'PASS'}* | {risk_e} | `{d['confidence']*100:.0f}%`\n"
        f"₿ `${ind_5m.get('price',0):,.2f}` | F&G: `{fg['value']}` | Session: `{session['session']}`\n\n"
        f"💭 _{d['reasoning']}_\n\n"
        f"🔑 Signaux:\n{sigs or '  Aucun'}",
        parse_mode="Markdown"
    )

async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    trades = st.trades[-8:][::-1]
    if not trades:
        await update.message.reply_text("📈 Aucun trade.")
        return
    lines = ["📈 *TRADES*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    for t in trades:
        e="✅" if t["result"]=="WIN" else "❌"
        ts=datetime.fromtimestamp(t["ts"]).strftime("%d/%m %H:%M")
        r=t.get("ai_reasoning","")[:45]
        lines.append(f"{e} `{t['dir']}` `{fmt(t['pnl'])}$` `{ts}`\n   _{r}_")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    total=st.wins+st.losses
    avg_win=sum(t["pnl"] for t in st.trades if t["pnl"]>0)/max(st.wins,1)
    avg_loss=abs(sum(t["pnl"] for t in st.trades if t["pnl"]<0))/max(st.losses,1)
    rr=avg_win/avg_loss if avg_loss>0 else 0
    peak,max_dd,rbr=BANKROLL_START,0.0,BANKROLL_START
    for t in st.trades:
        rbr+=t["pnl"]
        if rbr>peak: peak=rbr
        dd=(peak-rbr)/peak*100 if peak>0 else 0
        if dd>max_dd: max_dd=dd
    patterns = analyze_patterns(st.trades) if len(st.trades)>=5 else "Pas assez de trades"
    await update.message.reply_text(
        f"📉 *STATS AI BOT v6*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total: `{total}` (✅{st.wins} ❌{st.losses})\n"
        f"Win Rate: `{win_rate()}` | ROI: `{roi()}`\n"
        f"PnL: `{fmt(st.total_pnl)}$` | R:R: `{rr:.2f}`\n\n"
        f"Gain moyen: `+{avg_win:.2f}$` | Perte: `-{avg_loss:.2f}$`\n"
        f"Best streak: `+{st.best_streak}` | Max DD: `{max_dd:.1f}%`\n"
        f"Refusés AI: `{st.skipped_trades}`\n"
        f"Bankroll: `{st.bankroll:.2f} USDC`\n\n"
        f"📊 *Patterns:*\n_{patterns}_",
        parse_mode="Markdown"
    )

async def cmd_cooldown(update, context):
    if not auth(update): return
    st.cooldown_until=0; st.consec_losses=0
    await update.message.reply_text("✅ Cooldown reset.")

async def cmd_paper(update, context):
    if not auth(update): return
    st.paper_mode = not st.paper_mode
    await update.message.reply_text(f"Mode: *{'📄 PAPER' if st.paper_mode else '💰 RÉEL ⚠️'}*", parse_mode="Markdown")
    st.save()

async def cmd_reset(update, context):
    if not auth(update): return
    st.running=False
    for j in [st.tick_job,st.price_job,st.macro_job]:
        if j: j.schedule_removal()
    st.bankroll=BANKROLL_START; st.trades=[]; st.active_bet=None
    st.wins=st.losses=st.skipped_trades=st.consec_losses=0
    st.total_pnl=st.streak=st.best_streak=st.worst_streak=0
    st.cooldown_until=0; st.session_start=time.time()
    st.candles_1m.clear(); st.candles_5m.clear(); st.candles_15m.clear(); st.candles_1h.clear()
    if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    await update.message.reply_text("🔄 Reset complet.", parse_mode="Markdown")

async def callback_handler(update, context):
    q=update.callback_query; await q.answer()
    h={"status":cmd_status,"ai_last":cmd_ai,"trades":cmd_trades,"stats":cmd_stats,
       "fear":cmd_fear,"session":cmd_session,"run":cmd_run,"stop":cmd_stop,
       "toggle_paper":cmd_paper,"indicators":cmd_signal}
    if q.data in h: await h[q.data](update, context)

def main():
    st.load()
    app = Application.builder().token(TOKEN).build()
    for name, handler in [
        ("start",cmd_start),("run",cmd_run),("stop",cmd_stop),
        ("status",cmd_status),("ai",cmd_ai),("signal",cmd_signal),
        ("trades",cmd_trades),("stats",cmd_stats),("fear",cmd_fear),
        ("session",cmd_session),("paper",cmd_paper),
        ("cooldown",cmd_cooldown),("reset",cmd_reset),
    ]:
        app.add_handler(CommandHandler(name, handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    log.info("🧠 PolyBot v6 Enhanced AI démarré")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
