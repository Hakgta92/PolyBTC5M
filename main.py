"""
╔══════════════════════════════════════════════════════════════════╗
║     POLYMARKET BTC BOT v10 — FULLY AUTOMATED TRADING             ║
║     Placement réel | Take profit auto | API CLOB Polymarket      ║
╚══════════════════════════════════════════════════════════════════╝

NOUVEAUTÉS v10 :
  • Placement automatique des bets sur Polymarket via py-clob-client
  • Recherche automatique du marché BTC 5min actif
  • Take profit automatique si token x2.0+ avant expiration
  • Surveillance prix token en temps réel (toutes les 30s)
  • Annulation et revente de position possible
  • Fallback paper mode si API indisponible
  • Wallet Magic.link supporté (signature_type=1)

VARIABLES RAILWAY REQUISES :
  TELEGRAM_TOKEN, ALLOWED_USER_ID, ANTHROPIC_API_KEY
  POLY_PRIVATE_KEY     = clé privée Magic.link wallet
  POLY_PROXY_WALLET    = 0xa56554e... (adresse proxy Polymarket)
  POLY_RELAYER_KEY     = clé API relayer
  PAPER_MODE           = false (pour trader en réel)
  BANKROLL             = montant USDC disponible

INSTALLATION RAILWAY :
  Ajouter dans requirements.txt ou Nixpacks :
  py-clob-client>=0.18.0
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

# ─── Charge .env si présent ────────────────────────────────────────────────
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, val = line.partition('=')
                    os.environ.setdefault(key.strip(), val.strip())
load_env()

# ─── CONFIG ────────────────────────────────────────────────────────────────
TOKEN           = os.getenv("TELEGRAM_TOKEN", "")
ALLOWED_UID     = int(os.getenv("ALLOWED_USER_ID", "0"))
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
PAPER_MODE      = os.getenv("PAPER_MODE", "true").lower() == "true"
BANKROLL_START  = float(os.getenv("BANKROLL", "50.0"))

# ── Polymarket CLOB ──
POLY_PRIVATE_KEY  = os.getenv("POLY_PRIVATE_KEY", "")
POLY_PROXY_WALLET = os.getenv("POLY_PROXY_WALLET", "")   # adresse proxy (funder)
POLY_RELAYER_KEY  = os.getenv("POLY_RELAYER_KEY", "")
POLY_HOST         = "https://clob.polymarket.com"
POLY_GAMMA        = "https://gamma-api.polymarket.com"
POLY_CHAIN_ID     = 137   # Polygon

# ── Mises (identiques v9) ──
MIN_BET_USD       = 2.0
MID_BET_USD       = 5.0
MAX_BET_USD       = 8.0
MAX_BET_PCT       = 0.06

# ── Take profit ──
TAKE_PROFIT_MULT  = 2.0   # revendre si token x2.0 de la mise initiale
TAKE_PROFIT_CHECK = 30    # vérifier toutes les 30 secondes

# ── Filtres ──
POLY_FEE          = 0.02
DAILY_LOSS_MAX    = 0.12
MAX_CONSEC_LOSS   = 2
COOLDOWN_MIN      = 25
MIN_SCORE_EXCEL   = 8
MIN_SCORE_GOOD    = 9
MIN_SCORE_LOW     = 10
TRAILING_STOP_PCT = 0.30
MAX_TRADES_PER_H  = 3

CLAUDE_API      = "https://api.anthropic.com/v1/messages"
FEAR_GREED_API  = "https://api.alternative.me/fng/?limit=1"
DATA_FILE       = "polybot_v10_state.json"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("polybot_v10.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─── POLYMARKET CLIENT ──────────────────────────────────────────────────────
class PolyClient:
    """Wrapper async pour l'API CLOB Polymarket"""
    
    def __init__(self):
        self.client = None
        self.ready  = False
        self.api_key = None
        self.api_secret = None
        self.api_passphrase = None

    def init_client(self):
        """Initialise le client py-clob-client"""
        if not POLY_PRIVATE_KEY or not POLY_PROXY_WALLET:
            log.warning("Clés Polymarket manquantes — mode paper forcé")
            return False
        try:
            from py_clob_client.client import ClobClient
            self.client = ClobClient(
                POLY_HOST,
                key=POLY_PRIVATE_KEY,
                chain_id=POLY_CHAIN_ID,
                signature_type=1,      # Magic.link / email wallet
                funder=POLY_PROXY_WALLET
            )
            # Dérive ou crée les credentials API
            creds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)
            self.ready = True
            log.info("✅ Polymarket CLOB client initialisé")
            return True
        except ImportError:
            log.error("py-clob-client non installé — pip install py-clob-client")
            return False
        except Exception as e:
            log.error(f"Polymarket init error: {e}")
            return False

    async def find_btc_5min_market(self):
        """
        Calcule le slug BTC 5min mathématiquement — pas besoin de l'API Gamma.
        Pattern: btc-updown-5m-TIMESTAMP (arrondi aux 5 minutes UTC)
        Essaie le marché actuel ET le suivant pour fiabilité.
        """
        now = int(time.time())
        # Arrondi aux 5 minutes (300 secondes)
        current_ts = (now // 300) * 300
        next_ts    = current_ts + 300
        prev_ts    = current_ts - 300

        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
            "Accept": "application/json",
            "Referer": "https://polymarket.com/",
            "Origin": "https://polymarket.com",
        }

        for ts in [current_ts, next_ts, prev_ts]:
            slug = f"btc-updown-5m-{ts}"
            log.info(f"Recherche marché: {slug}")
            try:
                async with aiohttp.ClientSession(headers=headers) as s:
                    # Méthode 1 : API events avec slug exact
                    async with s.get(f"{POLY_GAMMA}/events",
                                     params={"slug": slug},
                                     timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            data = await r.json()
                            events = data if isinstance(data, list) else data.get("events", [])
                            for ev in events:
                                if slug in ev.get("slug", ""):
                                    markets = ev.get("markets", [])
                                    for m in markets:
                                        clob_ids = m.get("clobTokenIds", "[]")
                                        if isinstance(clob_ids, str):
                                            try: clob_ids = json.loads(clob_ids)
                                            except: clob_ids = []
                                        if len(clob_ids) >= 2:
                                            log.info(f"✅ Marché trouvé: {slug}")
                                            return {
                                                "token_up":    clob_ids[0],
                                                "token_down":  clob_ids[1],
                                                "question":    ev.get("title", slug),
                                                "condition_id":m.get("conditionId", ""),
                                                "end_date":    m.get("endDate", ""),
                                                "market_slug": slug,
                                            }
            except Exception as e:
                log.warning(f"Slug {slug}: {e}")

            try:
                async with aiohttp.ClientSession(headers=headers) as s:
                    # Méthode 2 : API markets avec slug
                    async with s.get(f"{POLY_GAMMA}/markets",
                                     params={"slug": slug},
                                     timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            data = await r.json()
                            markets = data if isinstance(data, list) else data.get("markets", [])
                            for m in markets:
                                clob_ids = m.get("clobTokenIds", "[]")
                                if isinstance(clob_ids, str):
                                    try: clob_ids = json.loads(clob_ids)
                                    except: clob_ids = []
                                if len(clob_ids) >= 2:
                                    log.info(f"✅ Marché via /markets: {slug}")
                                    return {
                                        "token_up":    clob_ids[0],
                                        "token_down":  clob_ids[1],
                                        "question":    m.get("question", slug),
                                        "condition_id":m.get("conditionId", ""),
                                        "end_date":    m.get("endDate", ""),
                                        "market_slug": slug,
                                    }
            except Exception as e:
                log.warning(f"Markets {slug}: {e}")

        log.warning("Aucun marché BTC 5min trouvé")
        return None

    async def get_token_price(self, token_id):
        """Récupère le prix actuel d'un token (0 à 1)"""
        headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15","Referer":"https://polymarket.com/"}
        try:
            async with aiohttp.ClientSession(headers=headers) as s:
                async with s.get(f"{POLY_HOST}/price",
                                 params={"token_id": token_id, "side": "buy"},
                                 timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        d = await r.json()
                        return float(d.get("price", 0.5))
        except: pass
        return 0.5

    async def place_market_order(self, token_id, amount_usdc, side="BUY"):
        """
        Place un ordre marché sur Polymarket.
        amount_usdc = montant en USDC à dépenser.
        Retourne l'ID de l'ordre ou None si échec.
        """
        if not self.ready or not self.client:
            log.warning("Client non initialisé")
            return None
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL
            
            side_const = BUY if side == "BUY" else SELL
            mo = MarketOrderArgs(
                token_id=token_id,
                amount=amount_usdc,
                side=side_const,
                order_type=OrderType.FOK   # Fill or Kill
            )
            signed = self.client.create_market_order(mo)
            resp   = self.client.post_order(signed, OrderType.FOK)
            
            if resp and resp.get("success"):
                order_id = resp.get("orderID", resp.get("id", "unknown"))
                log.info(f"✅ Ordre placé: {order_id} | {side} {amount_usdc}$ token={token_id[:16]}...")
                return order_id
            else:
                log.error(f"Ordre refusé: {resp}")
                return None
        except Exception as e:
            log.error(f"place_order error: {e}")
            return None

    async def sell_position(self, token_id, shares_amount):
        """Revend une position (take profit ou stop loss)"""
        if not self.ready or not self.client:
            return None
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL
            
            mo = MarketOrderArgs(
                token_id=token_id,
                amount=shares_amount,
                side=SELL,
                order_type=OrderType.FOK
            )
            signed = self.client.create_market_order(mo)
            resp   = self.client.post_order(signed, OrderType.FOK)
            if resp and resp.get("success"):
                log.info(f"✅ Position vendue: {shares_amount:.2f} shares")
                return resp
            return None
        except Exception as e:
            log.error(f"sell_position error: {e}")
            return None

    async def get_balance(self):
        """Récupère le solde USDC via API publique Polymarket"""
        if not POLY_PROXY_WALLET:
            return None
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
            "Accept": "application/json",
            "Referer": "https://polymarket.com/",
        }
        try:
            # API publique — pas besoin d'auth
            async with aiohttp.ClientSession(headers=headers) as s:
                async with s.get(
                    f"https://data-api.polymarket.com/portfolio?user={POLY_PROXY_WALLET}",
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        if isinstance(d, dict):
                            for k in ["cash","balance","usdc","available","portfolioValue"]:
                                if k in d and d[k] is not None:
                                    return round(float(d[k]), 2)
                        elif isinstance(d, (int, float)):
                            return round(float(d), 2)
        except Exception as e:
            log.warning(f"Balance portfolio: {e}")

        try:
            async with aiohttp.ClientSession(headers=headers) as s:
                async with s.get(
                    f"https://data-api.polymarket.com/positions?user={POLY_PROXY_WALLET}",
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        if isinstance(d, dict):
                            for k in ["cash","balance","usdc"]:
                                if k in d and d[k] is not None:
                                    return round(float(d[k]), 2)
        except Exception as e:
            log.warning(f"Balance positions: {e}")

        return None

poly = PolyClient()

# ─── INDICATEURS (identiques v9) ───────────────────────────────────────────
def ema(values, period):
    if not values: return 0
    if len(values) < period: return values[-1]
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]: e = v * k + e * (1 - k)
    return e

def ema_slope(values, period, lookback=3):
    if len(values) < period + lookback: return 0.0
    e_now  = ema(values, period)
    e_prev = ema(values[:-lookback], period)
    return round((e_now - e_prev) / e_prev * 100, 4) if e_prev else 0.0

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
    if len(closes) < 26: return 0, 0, 0, False
    ml      = ema(closes, 12) - ema(closes, 26)
    ml_prev = ema(closes[:-1], 12) - ema(closes[:-1], 26) if len(closes) > 26 else ml
    sig     = ema([ml_prev, ml], 9) if ml_prev != ml else ml * 0.9
    hist    = ml - sig
    cross   = ((ml_prev < sig) and (ml > sig)) or ((ml_prev > sig) and (ml < sig))
    return round(ml,4), round(sig,4), round(hist,4), cross

def bollinger(closes, period=20):
    if len(closes) < period: return None, None, None, False
    w = closes[-period:]; mid = sum(w)/period
    std = math.sqrt(sum((x-mid)**2 for x in w)/period)
    bb_l = round(mid-2*std,2); bb_h = round(mid+2*std,2)
    return bb_l, round(mid,2), bb_h, (bb_h-bb_l)/mid*100 < 0.8 if mid else False

def atr_calc(candles, period=14):
    if len(candles) < 2: return 0.0
    trs = [max(c["high"]-c["low"],abs(c["high"]-candles[i-1]["close"]),
               abs(c["low"]-candles[i-1]["close"])) for i,c in enumerate(candles) if i>0]
    return round(sum(trs[-period:])/min(len(trs),period),2) if trs else 0.0

def stoch(closes, highs, lows, period=14):
    if len(closes) < period: return 50.0, 50.0
    lo,hi = min(lows[-period:]),max(highs[-period:])
    if hi==lo: return 50.0,50.0
    k=(closes[-1]-lo)/(hi-lo)*100; d=(closes[-2]-lo)/(hi-lo)*100 if len(closes)>period else k
    return round(k,1),round(d,1)

def williams_r(closes, highs, lows, period=14):
    if len(closes)<period: return -50.0
    hi,lo=max(highs[-period:]),min(lows[-period:])
    return round(-100*(hi-closes[-1])/(hi-lo),1) if hi!=lo else -50.0

def vwap_calc(candles):
    if not candles: return 0
    tv=sum(c["vol"] for c in candles)
    return round(sum(((c["high"]+c["low"]+c["close"])/3)*c["vol"] for c in candles)/tv,2) if tv else candles[-1]["close"]

def detect_volume_spike(candles, lookback=20):
    if len(candles)<lookback: return False
    vols=[c["vol"] for c in candles[-lookback:-1]]
    avg=sum(vols)/len(vols) if vols else 1
    return candles[-1]["vol"]>avg*2.0

def detect_consolidation(candles, period=6):
    if len(candles)<period: return False
    highs=[c["high"] for c in candles[-period:]]
    lows=[c["low"] for c in candles[-period:]]
    price=candles[-1]["close"] or 1
    return (max(highs)-min(lows))/price*100 < 0.15

def detect_divergence(candles_5m):
    if len(candles_5m)<15: return None
    closes=[c["close"] for c in candles_5m[-15:]]
    rsis=[rsi(closes[max(0,i-14):i+1]) for i in range(5,15)]
    if len(rsis)<6: return None
    if closes[-1]<closes[-4]<closes[-7] and rsis[-1]>rsis[-4]>rsis[-7] and rsis[-1]<45: return "BULLISH"
    if closes[-1]>closes[-4]>closes[-7] and rsis[-1]<rsis[-4]<rsis[-7] and rsis[-1]>55: return "BEARISH"
    return None

def detect_engulfing(candles):
    if len(candles)<3: return None
    prev,curr=candles[-2],candles[-1]
    pb=abs(prev["close"]-prev["open"]); cb=abs(curr["close"]-curr["open"])
    if pb==0: return None
    if prev["close"]<prev["open"] and curr["close"]>curr["open"] and curr["open"]<prev["close"] and curr["close"]>prev["open"] and cb>pb*1.3: return "BULLISH"
    if prev["close"]>prev["open"] and curr["close"]<curr["open"] and curr["open"]>prev["close"] and curr["close"]<prev["open"] and cb>pb*1.3: return "BEARISH"
    return None

def detect_vwap_break(candles, lookback=6):
    if len(candles)<lookback+2: return None
    vw=vwap_calc(candles[-20:]); pp,cp=candles[-2]["close"],candles[-1]["close"]
    vols=[c["vol"] for c in candles[-lookback:]]
    avg_v=sum(vols)/len(vols) if vols else 1
    vol_ok=candles[-1]["vol"]>avg_v*1.5
    if pp<vw and cp>vw and vol_ok: return "BULLISH"
    if pp>vw and cp<vw and vol_ok: return "BEARISH"
    return None

def pivot_sr(candles, lookback=20):
    if len(candles)<lookback: return [],[]
    highs=[c["high"] for c in candles[-lookback:]]; lows=[c["low"] for c in candles[-lookback:]]
    price=candles[-1]["close"]; atr=atr_calc(candles)*3
    res,sup=[],[]
    for i in range(2,len(highs)-2):
        if highs[i]>highs[i-1] and highs[i]>highs[i+1] and highs[i]>highs[i-2] and highs[i]>highs[i+2]:
            if highs[i]>price and highs[i]-price<atr: res.append(round(highs[i],0))
        if lows[i]<lows[i-1] and lows[i]<lows[i+1] and lows[i]<lows[i-2] and lows[i]<lows[i+2]:
            if lows[i]<price and price-lows[i]<atr: sup.append(round(lows[i],0))
    return sorted(set(sup),reverse=True)[:2],sorted(set(res))[:2]

def compute_ind(candles):
    if len(candles)<10: return {}
    c=[x["close"] for x in candles]; h=[x["high"] for x in candles]
    l=[x["low"] for x in candles]; v=[x["vol"] for x in candles]
    price=c[-1]
    e9=ema(c,9); e21=ema(c,21); e50=ema(c,min(50,len(c)))
    r14=rsi(c,14); r7=rsi(c,7)
    ml,sg,hist,cross=macd_calc(c)
    bb_l,bb_m,bb_h,squeeze=bollinger(c)
    at=atr_calc(candles); stk,std=stoch(c,h,l)
    wr_v=williams_r(c,h,l); vw=vwap_calc(candles[-20:])
    av=sum(v[-10:])/10 if len(v)>=10 else v[-1]
    mom=c[-1]-c[-6] if len(c)>=6 else 0
    sup,res=pivot_sr(candles)
    return {
        "price":round(price,2),"rsi_7":r7,"rsi_14":r14,
        "ema9":round(e9,2),"ema21":round(e21,2),"ema50":round(e50,2),
        "slope_e9":ema_slope(c,9),"slope_e21":ema_slope(c,21),
        "macd_hist":hist,"macd_line":ml,"macd_cross":cross,
        "bb_low":bb_l,"bb_mid":bb_m,"bb_high":bb_h,"bb_squeeze":squeeze,
        "atr":at,"atr_pct":round(at/price*100,3) if price else 0,
        "stoch_k":stk,"stoch_d":std,"williams_r":wr_v,
        "vwap":vw,"above_vwap":price>vw,
        "vol_ratio":round(v[-1]/av,2) if av else 1.0,
        "vol_spike":detect_volume_spike(candles),
        "consolidation":detect_consolidation(candles),
        "momentum":round(mom,2),"ema_bull":e9>e21,"ema_bull_strong":e9>e21 and e21>e50,
        "supports":sup,"resistances":res,
    }

def compute_advanced_signals(candles_5m, candles_1m):
    div=detect_divergence(candles_5m)
    eng=detect_engulfing(candles_5m[-3:]) if len(candles_5m)>=3 else None
    vb=detect_vwap_break(candles_5m)
    signals=[]; score=0
    if div=="BULLISH":   signals.append("🔄 Divergence RSI haussière"); score+=2
    elif div=="BEARISH": signals.append("🔄 Divergence RSI baissière"); score-=2
    if eng=="BULLISH":   signals.append("🕯️ Engulfing haussier"); score+=2
    elif eng=="BEARISH": signals.append("🕯️ Engulfing baissier"); score-=2
    if vb=="BULLISH":    signals.append("📊 VWAP break ↑"); score+=1.5
    elif vb=="BEARISH":  signals.append("📊 VWAP break ↓"); score-=1.5
    return {"divergence":div,"engulfing":eng,"vwap_break":vb,"signals":signals,"score":score,
            "bias":"UP" if score>0 else "DOWN" if score<0 else None}

def session_ctx():
    h=(datetime.utcnow().hour+2)%24
    if   14<=h<17: return {"session":"US_OPEN",     "quality":"EXCELLENT","score_bonus":2}
    elif 17<=h<20: return {"session":"US_AFTERNOON","quality":"EXCELLENT","score_bonus":1}
    elif  9<=h<13: return {"session":"EU_OPEN",     "quality":"GOOD",     "score_bonus":1}
    elif 20<=h<22: return {"session":"US_CLOSE",    "quality":"GOOD",     "score_bonus":0}
    elif  7<=h< 9: return {"session":"ASIA_LATE",   "quality":"MEDIUM",   "score_bonus":0}
    elif  1<=h< 7: return {"session":"ASIA_EARLY",  "quality":"MEDIUM",   "score_bonus":-1}
    else:          return {"session":"OVERNIGHT",   "quality":"LOW",      "score_bonus":-2}

def compute_confluence_score(i1,i5,i15,i1h,i4h,fg,sess,adv):
    up=0.0; dn=0.0; signals=[]
    if i4h:
        if i4h.get("ema_bull"): up+=2.0; signals.append("4h EMA ↑")
        else:                   dn+=2.0; signals.append("4h EMA ↓")
        r4=i4h.get("rsi_14",50)
        if r4>55: up+=0.5
        elif r4<45: dn+=0.5
    if i15.get("ema_bull"): up+=2.0; signals.append("15m EMA ↑")
    else:                   dn+=2.0; signals.append("15m EMA ↓")
    if i1h.get("ema_bull"): up+=1.5; signals.append("1h EMA ↑")
    else:                   dn+=1.5; signals.append("1h EMA ↓")
    if i5.get("ema_bull"):  up+=1.0; signals.append("5m EMA ↑")
    else:                   dn+=1.0; signals.append("5m EMA ↓")
    if i1.get("ema_bull"):  up+=0.5
    else:                   dn+=0.5
    s9=i5.get("slope_e9",0)
    if s9>0.03:   up+=1.0; signals.append(f"EMA slope ↑ ({s9:+.3f}%)")
    elif s9<-0.03: dn+=1.0; signals.append(f"EMA slope ↓ ({s9:+.3f}%)")
    if i15.get("macd_hist",0)>0:   up+=1.5; signals.append("MACD 15m +")
    elif i15.get("macd_hist",0)<0: dn+=1.5; signals.append("MACD 15m -")
    if i5.get("macd_hist",0)>0:    up+=1.0
    elif i5.get("macd_hist",0)<0:  dn+=1.0
    if i5.get("macd_cross"):
        ml=i5.get("macd_line",0)
        if ml>0: up+=1.5; signals.append("⚡ MACD cross ↑")
        else:    dn+=1.5; signals.append("⚡ MACD cross ↓")
    r5=i5.get("rsi_14",50); r15=i15.get("rsi_14",50)
    if r5<25:    up+=2.5; signals.append(f"RSI survendu extrême ({r5})")
    elif r5<35:  up+=1.5; signals.append(f"RSI survendu ({r5})")
    elif r5>75:  dn+=2.5; signals.append(f"RSI suracheté extrême ({r5})")
    elif r5>65:  dn+=1.5; signals.append(f"RSI suracheté ({r5})")
    elif r5<45:  up+=0.5
    elif r5>55:  dn+=0.5
    if r15<40: up+=0.5
    elif r15>60: dn+=0.5
    if i5.get("above_vwap"):   up+=1.0; signals.append("Prix > VWAP")
    else:                      dn+=1.0; signals.append("Prix < VWAP")
    if i15.get("above_vwap"):  up+=0.5
    else:                      dn+=0.5
    sk=i5.get("stoch_k",50)
    if sk<15:    up+=1.5; signals.append(f"Stoch survendu ({sk})")
    elif sk<25:  up+=0.8
    elif sk>85:  dn+=1.5; signals.append(f"Stoch suracheté ({sk})")
    elif sk>75:  dn+=0.8
    adv_s=adv.get("score",0)
    if adv_s>0:   up+=min(adv_s*1.5,5); signals.extend(adv.get("signals",[]))
    elif adv_s<0: dn+=min(abs(adv_s)*1.5,5); signals.extend(adv.get("signals",[]))
    if i5.get("vol_spike"):
        if up>dn: up+=1.5; signals.append("🔥 Volume spike UP")
        else:     dn+=1.5; signals.append("🔥 Volume spike DOWN")
    sb=sess.get("score_bonus",0)
    if sb>0:
        if up>dn: up+=sb
        else: dn+=sb
    fgv=fg.get("value",50)
    if fgv<15:   up+=1.0; signals.append(f"F&G peur extrême ({fgv})")
    elif fgv>85: dn+=1.0; signals.append(f"F&G greed extrême ({fgv})")
    if i5.get("bb_squeeze"):
        signals.append("⚡ Squeeze BB")
        if up>dn: up+=0.5
        else: dn+=0.5
    if i5.get("consolidation"):
        up*=0.8; dn*=0.8; signals.append("⚠️ Consolidation")
    direction="UP" if up>=dn else "DOWN"
    score=round(up if up>=dn else dn,1); diff=round(abs(up-dn),1)
    sess_q=sess.get("quality","MEDIUM")
    min_score=(MIN_SCORE_EXCEL if sess_q=="EXCELLENT" else
               MIN_SCORE_GOOD  if sess_q=="GOOD"      else MIN_SCORE_LOW)
    return {"score_up":round(up,1),"score_dn":round(dn,1),"score":score,"diff":diff,
            "direction":direction,"signals":signals[:8],"min_score":min_score,
            "tradeable":score>=min_score and diff>=2.5}

def compute_momentum_score(i1,i5,i15):
    score=0.0
    r5=i5.get("rsi_14",50)
    if r5<25 or r5>75: score+=3.0
    elif r5<35 or r5>65: score+=1.5
    elif r5<40 or r5>60: score+=0.5
    s9=abs(i5.get("slope_e9",0)); s21=abs(i5.get("slope_e21",0))
    if s9>0.05: score+=2.0
    elif s9>0.02: score+=1.0
    if s21>0.03: score+=1.0
    vr=i5.get("vol_ratio",1.0)
    if vr>2.0: score+=2.0
    elif vr>1.5: score+=1.0
    elif vr>1.2: score+=0.5
    if i5.get("macd_cross"): score+=2.0
    if i1.get("ema_bull")==i5.get("ema_bull"): score+=0.5
    return round(min(score,10.0),1)

def analyze_losses(trades):
    losses=[t for t in trades[-20:] if t["result"]=="LOSS"]
    if not losses: return "Aucune perte récente."
    patterns=[]
    if sum(1 for t in losses if t.get("score",0)<9)>=2:
        patterns.append("⚠️ Pertes sur score <9 — éviter setups limites")
    if sum(1 for t in losses if t["dir"]=="DOWN" and t.get("fg_value",50)<15):
        patterns.append("⚠️ Pertes DOWN avec F&G<15 — F&G extrême peur → rebond UP")
    up_l=sum(1 for t in losses if t["dir"]=="UP")
    dn_l=sum(1 for t in losses if t["dir"]=="DOWN")
    if up_l>dn_l*2: patterns.append(f"⚠️ Trop pertes UP ({up_l}) — prudence UP")
    elif dn_l>up_l*2: patterns.append(f"⚠️ Trop pertes DOWN ({dn_l}) — prudence DOWN")
    return "\n".join(patterns) if patterns else f"{len(losses)} perte(s) sans pattern clair."

def recent_same_setup_loss(trades,direction,lookback=3):
    recent=trades[-lookback:] if len(trades)>=lookback else trades
    return sum(1 for t in recent if t["dir"]==direction and t["result"]=="LOSS")>=1

def trades_last_hour(trades):
    now=time.time()
    return sum(1 for t in trades if now-t.get("ts",0)<3600)

def pattern_mem(trades):
    if len(trades)<5: return "Moins de 5 trades."
    wins=[t for t in trades if t["result"]=="WIN"]
    losses=[t for t in trades if t["result"]=="LOSS"]
    up_t=[t for t in trades if t["dir"]=="UP"]; dn_t=[t for t in trades if t["dir"]=="DOWN"]
    up_wr=sum(1 for t in up_t if t["result"]=="WIN")/len(up_t)*100 if up_t else 0
    dn_wr=sum(1 for t in dn_t if t["result"]=="WIN")/len(dn_t)*100 if dn_t else 0
    return f"UP:{up_wr:.0f}%({len(up_t)}) DOWN:{dn_wr:.0f}%({len(dn_t)})"

def is_trending(c5,c15):
    if len(c5)<12: return False
    h=(datetime.utcnow().hour+2)%24; thr=0.10 if (22<=h or h<7) else 0.05
    closes=[c["close"] for c in c5[-12:]]; highs=[c["high"] for c in c5[-6:]]
    lows=[c["low"] for c in c5[-6:]]; price=closes[-1] if closes[-1] else 1
    return (max(highs)-min(lows))/price*100>thr or abs(closes[-1]-closes[0])/price*100>thr*0.7

# ─── DATA FETCH ────────────────────────────────────────────────────────────
async def fetch_price():
    sources=[
        ("Kraken","https://api.kraken.com/0/public/Ticker?pair=XBTUSD",lambda d:float(d["result"]["XXBTZUSD"]["c"][0])),
        ("Binance","https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",lambda d:float(d["price"])),
        ("CoinGecko","https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",lambda d:float(d["bitcoin"]["usd"])),
    ]
    for name,url,parser in sources:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url,timeout=aiohttp.ClientTimeout(total=6)) as r:
                    if r.status==200:
                        p=parser(await r.json())
                        if p>0: return p
        except: pass
    return st.price

async def fetch_klines(interval,limit=60):
    try:
        url=f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval={interval}&limit={limit}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url,timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status==200:
                    data=await r.json()
                    if isinstance(data,list) and len(data)>5:
                        return [{"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                                 "close":float(k[4]),"vol":float(k[5]),"ts":int(k[0])//1000} for k in data]
    except: pass
    try:
        km={"1m":1,"5m":5,"15m":15,"1h":60,"4h":240}
        url=f"https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval={km.get(interval,5)}&count={limit}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url,timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status==200:
                    data=await r.json(); ohlc=data.get("result",{}).get("XXBTZUSD",[])
                    if ohlc:
                        return [{"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                                 "close":float(k[4]),"vol":float(k[6]),"ts":int(k[0])} for k in ohlc[-limit:]]
    except: pass
    return []

async def fetch_fear_greed():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(FEAR_GREED_API,timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status==200:
                    d=await r.json()
                    return {"value":int(d["data"][0]["value"]),"label":d["data"][0]["value_classification"]}
    except: pass
    return {"value":50,"label":"Neutral"}

async def fetch_btc_24h():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD",timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status==200:
                    d=await r.json(); t=d.get("result",{}).get("XXBTZUSD",{})
                    if t:
                        price=float(t["c"][0]); open_p=float(t["o"])
                        return {"change_pct":round((price-open_p)/open_p*100,2) if open_p else 0,
                                "high_24h":float(t["h"][0]),"low_24h":float(t["l"][0]),"volume":float(t["v"][0])}
    except: pass
    return {"change_pct":0,"high_24h":0,"low_24h":0,"volume":0}

# ─── CLAUDE AI v10 ─────────────────────────────────────────────────────────
async def claude_decide(i1,i5,i15,i1h,i4h,adv,trades,bankroll,consec,fg,btc24,sess,conf_score,mom_score,token_price_up,token_price_dn):
    if not ANTHROPIC_KEY:
        return {"dir":None,"conf":0,"size":0,"reasoning":"Pas de clé API.","trade":False}

    loss_analysis=analyze_losses(trades)
    patterns=pattern_mem(trades)
    same_up=recent_same_setup_loss(trades,"UP")
    same_dn=recent_same_setup_loss(trades,"DOWN")

    trades_txt="".join(
        f"  {'✅' if t['result']=='WIN' else '❌'} {t['dir']} PnL:{t['pnl']:+.2f}$ "
        f"score:{t.get('score',0)} {'[REAL]' if not t.get('paper',True) else '[paper]'}\n"
        for t in trades[-6:]
    ) or "  Aucun.\n"

    sigs_txt="\n".join(f"  ✓ {s}" for s in conf_score["signals"]) or "  Aucun"

    if conf_score["score"]>=13 and sess.get("quality")=="EXCELLENT": suggested=MAX_BET_USD; bet_r="Score élevé+EXCELLENT→MAX"
    elif conf_score["score"]>=11: suggested=MID_BET_USD; bet_r="Score bon→MEDIUM"
    else: suggested=MIN_BET_USD; bet_r="Score limite→MIN"
    if consec>=1: suggested=MIN_BET_USD; bet_r=f"{consec} perte(s)→MIN"

    # Calcul payout réel selon prix du token
    payout_up = round(1/token_price_up, 2) if token_price_up > 0 else 2.0
    payout_dn = round(1/token_price_dn, 2) if token_price_dn > 0 else 2.0

    i4h_txt=f"4h RSI:{i4h.get('rsi_14',50)} EMA:{'↑' if i4h.get('ema_bull') else '↓'}" if i4h else ""
    h_paris=(datetime.utcnow().hour+2)%24

    prompt=f"""Tu es expert en trading binaire BTC UP/DOWN 5min sur Polymarket.
Les bets sont maintenant RÉELS — chaque erreur coûte de l'argent réel.

━━━ CONTEXTE MARCHÉ ━━━
BTC: ${i5.get('price',0):,.2f} | 24h: {btc24.get('change_pct',0):+.2f}%
Fear&Greed: {fg['value']}/100 ({fg['label']})
Session: {sess['session']} ({sess['quality']}) | {h_paris}h Paris

━━━ PRIX TOKENS POLYMARKET (CRUCIAL) ━━━
Token UP:   {token_price_up:.3f}$ → payout x{payout_up} si gagne
Token DOWN: {token_price_dn:.3f}$ → payout x{payout_dn} si gagne
→ Préfère le token avec le meilleur payout ET le bon signal technique
→ Token < 0.35$ = marché pense que c'est peu probable (mais bon payout si correct)
→ Token > 0.65$ = marché pense que c'est probable (faible payout)

━━━ SCORE CONFLUENCE ━━━
Direction: {conf_score['direction']} | Score: {conf_score['score']:.1f}/{conf_score['min_score']}
UP:{conf_score['score_up']} vs DOWN:{conf_score['score_dn']} | Diff:{conf_score['diff']} | Tradeable:{'OUI' if conf_score['tradeable'] else 'NON'}
Momentum: {mom_score}/10

Signaux:
{sigs_txt}

5m  RSI:{i5.get('rsi_14',50)} MACD:{i5.get('macd_hist',0):+.4f} cross:{i5.get('macd_cross',False)} Stoch:{i5.get('stoch_k',50)} Vol:x{i5.get('vol_ratio',1):.1f}
15m RSI:{i15.get('rsi_14',50)} EMA:{'↑' if i15.get('ema_bull') else '↓'} MACD:{i15.get('macd_hist',0):+.3f}
1h  RSI:{i1h.get('rsi_14',50)} EMA:{'↑' if i1h.get('ema_bull') else '↓'}
{i4h_txt}

━━━ APPRENTISSAGE ━━━
{patterns}
Analyse erreurs: {loss_analysis}
UP perdu récemment: {'⚠️ OUI' if same_up else 'Non'} | DOWN perdu récemment: {'⚠️ OUI' if same_dn else 'Non'}
Derniers trades:
{trades_txt}
Pertes consécutives: {consec} | Bankroll: {bankroll:.2f}$

━━━ DÉCISION ━━━
Mise suggérée: {suggested:.2f}$ ({bet_r})

RÈGLES ABSOLUES (ARGENT RÉEL) :
✅ TRADER: score tradeable + momentum≥4 + pas consolidation + payout≥1.8
❌ PASSER: score non tradeable, momentum<3, consolidation, même setup a perdu + momentum<6
❌ PASSER: payout<1.5 (mauvaise valeur)
⚠️ MIN si: consec≥1 ou diff<3.5 ou momentum<5

RÉPONDS UNIQUEMENT EN JSON:
{{"trade":true/false,"direction":"UP"/"DOWN"/null,"confidence":0.0-1.0,"bet_size":{MIN_BET_USD}-{MAX_BET_USD},"reasoning":"2 phrases MAX en FR","risk_level":"LOW"/"MEDIUM"/"HIGH"}}"""

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(CLAUDE_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                json={"model":"claude-haiku-4-5-20251001","max_tokens":300,"messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status!=200: return {"dir":None,"conf":0,"size":0,"reasoning":f"Erreur {r.status}","trade":False}
                data=await r.json(); raw=data["content"][0]["text"].strip()
                raw=raw.replace("```json","").replace("```","").strip()
                s2=raw.find("{"); e=raw.rfind("}")+1
                if s2>=0 and e>s2: raw=raw[s2:e]
                res=json.loads(raw)
                def sf(v,d=0.0):
                    try: return float(v) if v is not None else d
                    except: return d
                direction=res.get("direction")
                if direction not in ["UP","DOWN"]: direction=None
                trade=bool(res.get("trade",False)) and direction is not None
                return {"dir":direction,"conf":sf(res.get("confidence"),0.0),
                        "size":sf(res.get("bet_size"),0.0),
                        "reasoning":str(res.get("reasoning","")),"risk":res.get("risk_level","MEDIUM"),"trade":trade}
    except Exception as e:
        log.error(f"Claude: {e}")
        return {"dir":None,"conf":0,"size":0,"reasoning":f"Erreur: {str(e)[:60]}","trade":False}

# ─── STATE ─────────────────────────────────────────────────────────────────
class State:
    def __init__(self):
        self.running=False; self.paper_mode=PAPER_MODE; self.bankroll=BANKROLL_START
        self.c1=deque(maxlen=100); self.c5=deque(maxlen=100)
        self.c15=deque(maxlen=100); self.c1h=deque(maxlen=100); self.c4h=deque(maxlen=50)
        self.price=0.0; self.trades=[]; self.bet=None
        self.wins=self.losses=0; self.pnl=0.0; self.consec=0
        self.streak=self.best_streak=self.worst_streak=0
        self.cooldown_until=0; self.session_start=time.time()
        self.daily_start=BANKROLL_START; self.daily_ts=time.time()
        self.skipped=0; self.pass_reasons=[]
        self.last_decision={}; self.last_conf_score={}; self.last_mom_score=0
        self.fg={"value":50,"label":"Neutral"}; self.btc24={}
        self.tick_job=self.price_job=self.macro_job=self.tp_job=None
        # Polymarket
        self.current_market=None   # marché BTC 5min actif
        self.active_order_id=None  # ID ordre Polymarket en cours
        self.active_token_id=None  # token ID de la position
        self.entry_token_price=0.0 # prix du token à l'achat
        self.shares_bought=0.0     # nombre de shares achetés

    def save(self):
        try:
            with open(DATA_FILE,"w") as f:
                json.dump({"bankroll":self.bankroll,"trades":self.trades[-200:],
                    "wins":self.wins,"losses":self.losses,"pnl":self.pnl,
                    "best_streak":self.best_streak,"worst_streak":self.worst_streak,
                    "consec":self.consec,"daily_start":self.daily_start,"daily_ts":self.daily_ts,
                    "paper_mode":self.paper_mode,"skipped":self.skipped,
                    "pass_reasons":self.pass_reasons[-50:]},f,indent=2)
        except Exception as e: log.error(f"Save: {e}")

    def load(self):
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE) as f: d=json.load(f)
                self.bankroll=d.get("bankroll",BANKROLL_START)
                self.trades=d.get("trades",[]); self.wins=d.get("wins",0)
                self.losses=d.get("losses",0); self.pnl=d.get("pnl",0.0)
                self.best_streak=d.get("best_streak",0); self.worst_streak=d.get("worst_streak",0)
                self.consec=d.get("consec",0); self.daily_start=d.get("daily_start",self.bankroll)
                self.daily_ts=d.get("daily_ts",time.time())
                self.paper_mode=d.get("paper_mode",PAPER_MODE)
                self.skipped=d.get("skipped",0); self.pass_reasons=d.get("pass_reasons",[])
                log.info("State v10 chargé")
        except Exception as e: log.error(f"Load: {e}")

st=State()

def check_daily():
    now=time.time()
    if now-st.daily_ts>86400: st.daily_start=st.bankroll; st.daily_ts=now
    return st.daily_start>0 and (st.daily_start-st.bankroll)/st.daily_start>=DAILY_LOSS_MAX

def in_cd(): return time.time()<st.cooldown_until

async def send(bot,text,parse_mode="Markdown"):
    try: await bot.send_message(chat_id=ALLOWED_UID,text=text,parse_mode=parse_mode); return True
    except Exception as e:
        log.error(f"Send: {e}")
        try:
            clean=text.replace("*","").replace("`","").replace("_","")
            await bot.send_message(chat_id=ALLOWED_UID,text=clean); return True
        except: return False

# ─── JOB TAKE PROFIT (nouveau v10) ─────────────────────────────────────────
async def job_take_profit(context):
    """
    Vérifie toutes les 30s si la position a atteint x2.0.
    Si oui, revend automatiquement avant expiration.
    """
    if not st.bet or not st.active_token_id: return
    if st.paper_mode: return  # pas de TP en paper

    try:
        current_price = await poly.get_token_price(st.active_token_id)
        if current_price <= 0 or st.entry_token_price <= 0: return

        # Calcul du gain potentiel
        gain_mult = current_price / st.entry_token_price
        potential_pnl = round((current_price - st.entry_token_price) * st.shares_bought, 2)

        log.info(f"TP check: entry={st.entry_token_price:.3f} current={current_price:.3f} x{gain_mult:.2f}")

        if gain_mult >= TAKE_PROFIT_MULT:
            # 🎯 TAKE PROFIT !
            log.info(f"🎯 Take profit déclenché! x{gain_mult:.2f}")
            result = await poly.sell_position(st.active_token_id, st.shares_bought)

            if result:
                gross = potential_pnl
                st.bankroll = max(0.0, st.bankroll + gross)
                st.pnl += gross
                st.wins += 1; st.consec = 0
                st.streak = st.streak+1 if st.streak>=0 else 1
                st.best_streak = max(st.best_streak, st.streak)

                bet = st.bet
                st.trades.append({
                    "dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
                    "conf":bet["conf"],"result":"WIN","entry":bet["entry"],"exit":st.price,
                    "reasoning":f"Take profit x{gain_mult:.2f}",
                    "paper":False,"ts":int(time.time()),"score":bet.get("score",0),
                    "fg_value":st.fg.get("value",50),"aligned_15h1h":True
                })
                st.bet=None; st.active_token_id=None; st.active_order_id=None
                st.shares_bought=0; st.entry_token_price=0

                await send(context.bot,
                    f"🎯 *TAKE PROFIT* x{gain_mult:.2f}\n"
                    f"`{bet['dir']}` | `+{gross:.2f} USDC`\n"
                    f"BR:`{st.bankroll:.2f}` | Streak:`{st.streak:+d}`")
                st.save()

    except Exception as e:
        log.error(f"job_take_profit: {e}")

# ─── JOBS PRINCIPAUX ────────────────────────────────────────────────────────
async def job_price(context):
    p=await fetch_price()
    if p>0: st.price=p

async def job_macro(context):
    st.fg=await fetch_fear_greed(); st.btc24=await fetch_btc_24h()

async def job_tick(context):
    if not st.running: return
    if check_daily():
        st.running=False
        await send(context.bot,"🛑 *Limite journalière atteinte* — Bot arrêté."); return
    if in_cd(): return

    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
    if not c5: return

    st.c1=deque(c1,maxlen=100); st.c5=deque(c5,maxlen=100)
    st.c15=deque(c15,maxlen=100); st.c1h=deque(c1h,maxlen=100); st.c4h=deque(c4h,maxlen=50)
    st.price=c5[-1]["close"]

    # Anti-overtrading
    if trades_last_hour(st.trades)>=MAX_TRADES_PER_H: return

    # Résoudre bet expiré (paper mode ou si pas de TP déclenché)
    if st.bet:
        bet=st.bet
        won=bet["dir"]==("UP" if st.price>bet["entry"] else "DOWN")
        gross=bet["amount"]*(1-POLY_FEE) if won else -bet["amount"]

        if st.paper_mode:
            # Paper mode : résolution automatique
            st.bankroll=max(0.0,st.bankroll+gross); st.pnl+=gross
            if won:
                st.wins+=1; st.consec=0
                st.streak=st.streak+1 if st.streak>=0 else 1
                st.best_streak=max(st.best_streak,st.streak)
            else:
                st.losses+=1; st.consec+=1
                st.streak=st.streak-1 if st.streak<=0 else -1
                st.worst_streak=min(st.worst_streak,st.streak)
                if st.consec>=MAX_CONSEC_LOSS: st.cooldown_until=time.time()+COOLDOWN_MIN*60
            i15_n=compute_ind(list(st.c15)); i1h_n=compute_ind(list(st.c1h))
            st.trades.append({"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
                "conf":bet["conf"],"result":"WIN" if won else "LOSS","entry":bet["entry"],"exit":st.price,
                "reasoning":bet.get("reasoning",""),"paper":True,"ts":int(time.time()),
                "score":bet.get("score",0),"fg_value":st.fg.get("value",50),
                "aligned_15h1h":i15_n.get("ema_bull")==i1h_n.get("ema_bull")})
            st.bet=None
            emoji="✅" if won else "❌"
            cd_msg=f"\n⏸ Cooldown {COOLDOWN_MIN}min" if in_cd() else ""
            await send(context.bot,
                f"{emoji} *Trade clôturé* [📄]\n"
                f"`{bet['dir']}` `${bet['entry']:,.0f}`→`${st.price:,.0f}`\n"
                f"PnL:`{'+' if gross>=0 else ''}{gross:.2f}$` BR:`{st.bankroll:.2f}`"
                f" Streak:`{st.streak:+d}`{cd_msg}")
            st.save()

    if in_cd(): return
    if not is_trending(list(st.c5),list(st.c15)):
        st.skipped+=1; return

    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5))
    i15=compute_ind(list(st.c15)); i1h=compute_ind(list(st.c1h))
    i4h=compute_ind(list(st.c4h)) if st.c4h else {}
    sess=session_ctx()
    if not i5: return

    adv=compute_advanced_signals(list(st.c5),list(st.c1))
    conf_score=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv)
    mom_score=compute_momentum_score(i1,i5,i15)
    st.last_conf_score=conf_score; st.last_mom_score=mom_score

    if not conf_score["tradeable"]:
        st.skipped+=1
        st.pass_reasons.append({"ts":int(time.time()),
            "reason":f"Score {conf_score['score']:.1f}<{conf_score['min_score']}"}); return

    if mom_score<3:
        st.skipped+=1
        st.pass_reasons.append({"ts":int(time.time()),"reason":f"Momentum faible ({mom_score}/10)"}); return

    if i5.get("atr_pct",0)<0.03:
        st.skipped+=1; return

    if i5.get("vol_ratio",1)<0.4:
        st.skipped+=1; return

    # Récupère prix tokens Polymarket (réel)
    token_up_price=0.5; token_dn_price=0.5
    if not st.paper_mode:
        market=await poly.find_btc_5min_market()
        if market:
            st.current_market=market
            token_up_price=await poly.get_token_price(market["token_up"])
            token_dn_price=await poly.get_token_price(market["token_down"])
        else:
            log.warning("Pas de marché BTC 5min trouvé — PASS")
            st.skipped+=1
            st.pass_reasons.append({"ts":int(time.time()),"reason":"Aucun marché Polymarket actif"}); return

    dec=await claude_decide(i1,i5,i15,i1h,i4h,adv,st.trades[-15:],st.bankroll,
                            st.consec,st.fg,st.btc24,sess,conf_score,mom_score,
                            token_up_price,token_dn_price)
    st.last_decision=dec

    if dec["trade"] and dec["dir"] and not st.bet:
        amount=max(MIN_BET_USD,min(dec["size"],MAX_BET_USD,st.bankroll*MAX_BET_PCT))
        amount=round(amount,2)
        if amount<MIN_BET_USD or st.bankroll<amount: return

        order_id=None
        token_used=None
        entry_token_price=0.5

        if not st.paper_mode and st.current_market:
            # ── PLACEMENT RÉEL ──
            if dec["dir"]=="UP":
                token_used=st.current_market["token_up"]
                entry_token_price=token_up_price
            else:
                token_used=st.current_market["token_down"]
                entry_token_price=token_dn_price

            order_id=await poly.place_market_order(token_used, amount, "BUY")
            if not order_id:
                log.error("Ordre Polymarket refusé — PASS")
                await send(context.bot,"⚠️ *Ordre Polymarket refusé* — vérifier solde/connexion")
                return

            shares=round(amount/entry_token_price,4) if entry_token_price>0 else 0
            st.active_order_id=order_id
            st.active_token_id=token_used
            st.entry_token_price=entry_token_price
            st.shares_bought=shares

        st.bet={
            "dir":dec["dir"],"amount":amount,"conf":dec["conf"],
            "entry":st.price,"reasoning":dec["reasoning"],
            "ts":int(time.time()),"score":conf_score["score"],"session":sess["session"]
        }

        mode="💰 RÉEL" if not st.paper_mode else "📄 paper"
        risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(dec["risk"],"🟡")
        sigs="\n".join(f"  • {s}" for s in conf_score["signals"][:4])
        payout_info=""
        if not st.paper_mode:
            tp=round(1/entry_token_price,2) if entry_token_price>0 else 2.0
            payout_info=f"\nToken:`{entry_token_price:.3f}$` → payout x`{tp}` | TP auto si x`{TAKE_PROFIT_MULT}`"

        await send(context.bot,
            f"🧠 *Bet placé* [{mode}]\n━━━━━━━━━━━━━━━\n"
            f"*{dec['dir']}* | `{amount:.2f}$` | `{dec['conf']*100:.0f}%` | {risk_e}\n"
            f"Score:`{conf_score['score']:.1f}` Mom:`{mom_score}/10`{payout_info}\n"
            f"BTC:`${st.price:,.2f}` | `{sess['session']}`\n"
            f"F&G:`{st.fg['value']}` 4h:`{'↑' if i4h.get('ema_bull') else '↓' if i4h else '?'}` "
            f"15m:`{'↑' if i15.get('ema_bull') else '↓'}` 1h:`{'↑' if i1h.get('ema_bull') else '↓'}`\n\n"
            f"💭 _{dec['reasoning']}_\n\n🔑 Signaux:\n{sigs}")
    else:
        st.skipped+=1
        st.pass_reasons.append({"ts":int(time.time()),"reason":f"Claude PASS: {dec['reasoning'][:50]}"})

# ─── HELPERS ───────────────────────────────────────────────────────────────
def auth(u): return ALLOWED_UID==0 or u.effective_user.id==ALLOWED_UID
def fmt(v): return f"+{v:.2f}" if v>=0 else f"{v:.2f}"
def wr():
    t=st.wins+st.losses; return f"{st.wins/t*100:.1f}%" if t else "—"
def roi(): return f"{fmt((st.bankroll-BANKROLL_START)/BANKROLL_START*100)}%"
def upt():
    s=int(time.time()-st.session_start)
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

def kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Status",  callback_data="status"),
         InlineKeyboardButton("🧠 AI Last", callback_data="ai")],
        [InlineKeyboardButton("📈 Trades",  callback_data="trades"),
         InlineKeyboardButton("📉 Stats",   callback_data="stats")],
        [InlineKeyboardButton("😱 F&G",     callback_data="fear"),
         InlineKeyboardButton("🎯 Score",   callback_data="score")],
        [InlineKeyboardButton("▶️ Start",   callback_data="run"),
         InlineKeyboardButton("⏹ Stop",    callback_data="stop")],
        [InlineKeyboardButton("🟢 Actif" if st.running else "🔴 Arrêté",callback_data="status"),
         InlineKeyboardButton("💰 Réel" if st.paper_mode else "📄 Paper",callback_data="paper")],
    ])

# ─── COMMANDES ─────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context):
    if not auth(update): return
    poly_ok="✅" if poly.ready else "❌"
    await update.message.reply_text(
        f"🧠 *POLYMARKET BOT v10 — FULLY AUTOMATED*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode: *{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}*\n"
        f"Polymarket API: {poly_ok}\n\n"
        f"🆕 v10:\n"
        f"  ✅ Placement automatique Polymarket\n"
        f"  ✅ Take profit x{TAKE_PROFIT_MULT} automatique\n"
        f"  ✅ Prix token UP/DOWN en temps réel\n"
        f"  ✅ Payout calculé avant chaque bet\n"
        f"  ✅ Wallet Magic.link supporté\n\n"
        f"*/run* */stop* */status* */signal* */score*\n"
        f"*/market* */balance* */trades* */stats* */paper*",
        parse_mode="Markdown",reply_markup=kb())

async def cmd_run(update: Update, context):
    if not auth(update): return
    if st.running: await update.message.reply_text("⚠️ Déjà en cours."); return
    if not ANTHROPIC_KEY: await update.message.reply_text("❌ ANTHROPIC_API_KEY manquante."); return

    # Init Polymarket si mode réel
    if not st.paper_mode:
        ok=poly.init_client()
        if not ok:
            await update.message.reply_text(
                "⚠️ *Polymarket API non disponible* — passage en paper mode automatique\n"
                "Vérifie que `py-clob-client` est installé et les variables POLY_* sont correctes.",
                parse_mode="Markdown")
            st.paper_mode=True

    st.running=True; st.session_start=time.time()
    st.daily_start=st.bankroll; st.daily_ts=time.time()
    st.price_job=context.job_queue.run_repeating(job_price,interval=30,first=5)
    st.macro_job=context.job_queue.run_repeating(job_macro,interval=300,first=8)
    st.tick_job =context.job_queue.run_repeating(job_tick, interval=300,first=15)
    # Take profit check toutes les 30s
    st.tp_job=context.job_queue.run_repeating(job_take_profit,interval=TAKE_PROFIT_CHECK,first=10)

    st.fg=await fetch_fear_greed(); st.btc24=await fetch_btc_24h()
    sess=session_ctx()

    # Solde réel
    balance_txt=""
    if not st.paper_mode and poly.ready:
        bal=await poly.get_balance()
        if bal: balance_txt=f"\nSolde Polymarket: `{bal:.2f} USDC`"

    await update.message.reply_text(
        f"▶️ *Bot v10 démarré !*\n"
        f"Mode: *{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}*\n"
        f"F&G:`{st.fg['value']}` | BTC:`{st.btc24.get('change_pct',0):+.2f}%`\n"
        f"Session:`{sess['session']}` — {sess['quality']}\n"
        f"BR:`{st.bankroll:.2f}$` | TP auto: x{TAKE_PROFIT_MULT}{balance_txt}",
        parse_mode="Markdown")
    await job_tick(context)

async def cmd_stop(update: Update, context):
    if not auth(update): return
    st.running=False
    for j in [st.tick_job,st.price_job,st.macro_job,st.tp_job]:
        if j:
            try: j.schedule_removal()
            except: pass
    st.tick_job=st.price_job=st.macro_job=st.tp_job=None; st.save()
    await update.message.reply_text(
        f"⏹ *Arrêté* | `{upt()}` | BR:`{st.bankroll:.2f}` | PnL:`{fmt(st.pnl)}` | WR:`{wr()}`",
        parse_mode="Markdown")

async def cmd_status(update: Update, context):
    if not auth(update): return
    sess=session_ctx()
    dl=(st.daily_start-st.bankroll)/st.daily_start*100 if st.daily_start>0 else 0
    cs=st.last_conf_score
    score_info=f"`{cs.get('score',0):.1f}/20` Mom:`{st.last_mom_score}/10`" if cs else "—"
    bet_info="Aucun"
    if st.bet:
        elapsed=int((time.time()-st.bet["ts"])/60)
        tp_info=f" | TP token@{st.entry_token_price:.3f}" if st.entry_token_price>0 else ""
        bet_info=f"{st.bet['dir']} {st.bet['amount']:.2f}$ ({elapsed}min){tp_info}"
    poly_status="✅ Connecté" if poly.ready else "❌ Non connecté"
    await update.message.reply_text(
        f"📊 *STATUS v10* [{'📄' if st.paper_mode else '💰'}]\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢 EN COURS' if st.running else '🔴 ARRÊTÉ'}\n"
        f"Polymarket: {poly_status}\n\n"
        f"₿`${st.price:,.2f}` | F&G:`{st.fg['value']}` | `{sess['session']}`\n"
        f"🎯 Score: {score_info}\n\n"
        f"💰 BR:`{st.bankroll:.2f}` | ROI:`{roi()}` | PnL:`{fmt(st.pnl)}`\n"
        f"📅 Perte jour:`{dl:.1f}%/{DAILY_LOSS_MAX*100:.0f}%`\n"
        f"🎲 Bet:`{bet_info}`\n"
        f"🚫 Refusés:`{st.skipped}` | ⏱`{upt()}`",
        parse_mode="Markdown",reply_markup=kb())

async def cmd_market(update: Update, context):
    """Affiche le marché BTC 5min actif"""
    if not auth(update): return
    await update.message.reply_text("⏳ Recherche marché...")
    market=await poly.find_btc_5min_market()
    if not market:
        await update.message.reply_text("❌ Aucun marché BTC 5min actif trouvé."); return
    tu=await poly.get_token_price(market["token_up"])
    td=await poly.get_token_price(market["token_down"])
    pu=round(1/tu,2) if tu>0 else 0; pd=round(1/td,2) if td>0 else 0
    await update.message.reply_text(
        f"🎯 *MARCHÉ ACTIF*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_{market['question']}_\n\n"
        f"🟢 UP:   `{tu:.3f}$` → payout x`{pu}`\n"
        f"🔴 DOWN: `{td:.3f}$` → payout x`{pd}`\n\n"
        f"Token UP: `{market['token_up'][:20]}...`\n"
        f"Fin: `{market.get('end_date','?')}`",
        parse_mode="Markdown")

async def cmd_balance(update: Update, context):
    """Affiche le solde Polymarket réel"""
    if not auth(update): return
    if st.paper_mode:
        await update.message.reply_text(f"📄 Paper mode | BR simulé: `{st.bankroll:.2f}$`",parse_mode="Markdown"); return
    if not poly.ready:
        await update.message.reply_text("❌ Polymarket non connecté."); return
    bal=await poly.get_balance()
    if bal is not None:
        await update.message.reply_text(f"💰 *Solde Polymarket*\n`{bal:.2f} USDC`",parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Impossible de récupérer le solde.")

async def cmd_score(update: Update, context):
    if not auth(update): return
    await update.message.reply_text("⏳ Calcul score v10...")
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
    if c5:
        st.c5=deque(c5,maxlen=100); st.c15=deque(c15,maxlen=100)
        st.c1h=deque(c1h,maxlen=100); st.c1=deque(c1,maxlen=100); st.c4h=deque(c4h,maxlen=50)
        st.price=c5[-1]["close"]
    st.fg=await fetch_fear_greed()
    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5))
    i15=compute_ind(list(st.c15)); i1h=compute_ind(list(st.c1h))
    i4h=compute_ind(list(st.c4h)) if st.c4h else {}
    sess=session_ctx()
    adv=compute_advanced_signals(list(st.c5),list(st.c1))
    cs=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv)
    mom=compute_momentum_score(i1,i5,i15)
    st.last_conf_score=cs; st.last_mom_score=mom
    # Prix tokens si disponible
    token_txt=""
    if not st.paper_mode and poly.ready:
        m=await poly.find_btc_5min_market()
        if m:
            tu=await poly.get_token_price(m["token_up"])
            td=await poly.get_token_price(m["token_down"])
            token_txt=f"\n🟢 UP:`{tu:.3f}$` x{round(1/tu,2) if tu>0 else '?'} | 🔴 DOWN:`{td:.3f}$` x{round(1/td,2) if td>0 else '?'}"
    tradeable_e="✅ TRADEABLE" if cs["tradeable"] else f"❌ PASS"
    mom_e="🔥" if mom>=7 else "⚡" if mom>=4 else "💤"
    sigs="\n".join(f"  • {s}" for s in cs["signals"])
    await update.message.reply_text(
        f"🎯 *SCORE v10*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"₿`${st.price:,.2f}` | `{sess['session']}`{token_txt}\n\n"
        f"🟢 UP:`{cs['score_up']:.1f}` 🔴 DOWN:`{cs['score_dn']:.1f}`\n"
        f"Diff:`{cs['diff']:.1f}` → {tradeable_e}\n"
        f"⚡ Momentum:`{mom}/10` {mom_e}\n\n"
        f"Signaux:\n{sigs or '  Aucun'}",
        parse_mode="Markdown")

async def cmd_signal(update: Update, context):
    if not auth(update): return
    await update.message.reply_text("⏳ Analyse complète v10...")
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
    if c5:
        st.c1=deque(c1,maxlen=100); st.c5=deque(c5,maxlen=100)
        st.c15=deque(c15,maxlen=100); st.c1h=deque(c1h,maxlen=100); st.c4h=deque(c4h,maxlen=50)
        st.price=c5[-1]["close"]
    st.fg=await fetch_fear_greed(); st.btc24=await fetch_btc_24h()
    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5))
    i15=compute_ind(list(st.c15)); i1h=compute_ind(list(st.c1h))
    i4h=compute_ind(list(st.c4h)) if st.c4h else {}
    sess=session_ctx()
    adv=compute_advanced_signals(list(st.c5),list(st.c1))
    cs=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv)
    mom=compute_momentum_score(i1,i5,i15)
    st.last_conf_score=cs; st.last_mom_score=mom
    tu=0.5; td=0.5
    if not st.paper_mode and poly.ready:
        m=await poly.find_btc_5min_market()
        if m: tu=await poly.get_token_price(m["token_up"]); td=await poly.get_token_price(m["token_down"])
    d=await claude_decide(i1,i5,i15,i1h,i4h,adv,st.trades[-15:],st.bankroll,
                          st.consec,st.fg,st.btc24,sess,cs,mom,tu,td)
    st.last_decision=d
    dir_e="🟢" if d["dir"]=="UP" else "🔴" if d["dir"]=="DOWN" else "⚪"
    risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(d.get("risk","MEDIUM"),"🟡")
    payout=round(1/(tu if d["dir"]=="UP" else td),2) if d["dir"] else 0
    await update.message.reply_text(
        f"🧠 *ANALYSE v10*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_e} *{d['dir'] or 'PASS'}* | {risk_e} | `{d['conf']*100:.0f}%`\n"
        f"Score:`{cs['score']:.1f}` Mom:`{mom}/10` Payout:x`{payout}`\n"
        f"₿`${i5.get('price',0):,.2f}` | F&G:`{st.fg['value']}` | `{sess['session']}`\n\n"
        f"💭 _{d['reasoning']}_",
        parse_mode="Markdown")

async def cmd_ai(update: Update, context):
    if not auth(update): return
    d=st.last_decision; cs=st.last_conf_score
    if not d: await update.message.reply_text("⏳ Lance /signal d'abord."); return
    dir_e="🟢" if d.get("dir")=="UP" else "🔴" if d.get("dir")=="DOWN" else "⚪"
    risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(d.get("risk","MEDIUM"),"🟡")
    await update.message.reply_text(
        f"🧠 *DERNIÈRE DÉCISION v10*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_e} *{d.get('dir') or 'PASS'}* | {risk_e} | `{d.get('conf',0)*100:.0f}%`\n"
        f"Score:`{cs.get('score',0):.1f}` Mom:`{st.last_mom_score}/10`\n"
        f"Trade:`{'OUI ✅' if d.get('trade') else 'NON ❌'}` | Mise:`{d.get('size',0):.2f}$`\n\n"
        f"💭 _{d.get('reasoning','—')}_",
        parse_mode="Markdown")

async def cmd_trades(update: Update, context):
    if not auth(update): return
    trades=st.trades[-8:][::-1]
    if not trades: await update.message.reply_text("📈 Aucun trade."); return
    lines=["📈 *TRADES v10*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    for t in trades:
        e="✅" if t["result"]=="WIN" else "❌"
        ts=datetime.fromtimestamp(t["ts"]).strftime("%d/%m %H:%M")
        mode="💰" if not t.get("paper",True) else "📄"
        lines.append(f"{e}{mode} `{t['dir']}` `{fmt(t['pnl'])}$` sc:`{t.get('score',0):.0f}` `{ts}`")
    if st.bet:
        elapsed=int((time.time()-st.bet["ts"])/60)
        lines.append(f"\n🔄 *Actif:* `{st.bet['dir']}` `{st.bet['amount']:.2f}$` ({elapsed}min)")
    await update.message.reply_text("\n".join(lines),parse_mode="Markdown")

async def cmd_stats(update: Update, context):
    if not auth(update): return
    total=st.wins+st.losses
    aw=sum(t["pnl"] for t in st.trades if t["pnl"]>0)/max(st.wins,1)
    al=abs(sum(t["pnl"] for t in st.trades if t["pnl"]<0))/max(st.losses,1)
    rr=aw/al if al>0 else 0
    real_trades=[t for t in st.trades if not t.get("paper",True)]
    real_wr=sum(1 for t in real_trades if t["result"]=="WIN")/len(real_trades)*100 if real_trades else 0
    loss_analysis=analyze_losses(st.trades)
    await update.message.reply_text(
        f"📉 *STATS v10*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total:`{total}` (✅{st.wins} ❌{st.losses})\n"
        f"WR:`{wr()}` | ROI:`{roi()}` | R:R:`{rr:.2f}`\n"
        f"PnL:`{fmt(st.pnl)}$` | BR:`{st.bankroll:.2f}$`\n\n"
        f"💰 Trades réels: `{len(real_trades)}` WR:`{real_wr:.0f}%`\n"
        f"Gain moy:`+{aw:.2f}$` | Perte moy:`-{al:.2f}$`\n\n"
        f"🔍 _{loss_analysis}_",
        parse_mode="Markdown")

async def cmd_passes(update: Update, context):
    if not auth(update): return
    passes=st.pass_reasons[-10:][::-1]
    if not passes: await update.message.reply_text("✅ Aucun PASS."); return
    lines=["🚫 *DERNIERS PASS*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    for p in passes:
        ts=datetime.fromtimestamp(p["ts"]).strftime("%H:%M")
        lines.append(f"`{ts}` {p['reason']}")
    await update.message.reply_text("\n".join(lines),parse_mode="Markdown")

async def cmd_fear(update: Update, context):
    if not auth(update): return
    fg=st.fg; v=fg['value']
    bar="█"*(v//10)+"░"*(10-v//10)
    e="😱" if v<20 else "😟" if v<40 else "😐" if v<60 else "😊" if v<80 else "🤑"
    interp=("Extrême Peur → biais UP" if v<20 else "Peur" if v<40 else
            "Neutre" if v<60 else "Greed" if v<80 else "Extrême Greed → biais DOWN")
    btc=st.btc24
    await update.message.reply_text(
        f"😱 *FEAR & GREED*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{e} *{fg['label']}* — `{v}/100`\n`{bar}`\n\n_{interp}_\n\n"
        f"₿ 24h:`{btc.get('change_pct',0):+.2f}%`",
        parse_mode="Markdown")

async def cmd_paper(update: Update, context):
    if not auth(update): return
    st.paper_mode=not st.paper_mode
    if not st.paper_mode and not poly.ready:
        poly.init_client()
    await update.message.reply_text(
        f"Mode: *{'📄 PAPER' if st.paper_mode else '💰 RÉEL ⚠️'}*\n"
        f"Polymarket API: {'✅' if poly.ready else '❌ non connecté'}",
        parse_mode="Markdown")
    st.save()

async def cmd_reset(update: Update, context):
    if not auth(update): return
    st.running=False
    for j in [st.tick_job,st.price_job,st.macro_job,st.tp_job]:
        if j:
            try: j.schedule_removal()
            except: pass
    st.bankroll=BANKROLL_START; st.trades=[]; st.bet=None
    st.wins=st.losses=st.skipped=st.consec=0
    st.pnl=st.streak=st.best_streak=st.worst_streak=0
    st.cooldown_until=0; st.session_start=time.time()
    st.pass_reasons=[]; st.last_conf_score={}; st.last_mom_score=0
    st.active_order_id=None; st.active_token_id=None
    st.shares_bought=0; st.entry_token_price=0
    st.c1.clear(); st.c5.clear(); st.c15.clear(); st.c1h.clear(); st.c4h.clear()
    if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    await update.message.reply_text("🔄 *Reset complet v10.*",parse_mode="Markdown")

async def cmd_cooldown(update: Update, context):
    if not auth(update): return
    st.cooldown_until=0; st.consec=0
    await update.message.reply_text("✅ Cooldown reset.",parse_mode="Markdown")


async def cmd_debug(update: Update, context):
    """Debug: affiche la réponse brute de l'API Gamma"""
    if not auth(update): return
    await update.message.reply_text("⏳ Debug API Gamma...")
    results = []
    try:
        async with aiohttp.ClientSession() as s:
            # Test 1: events avec slug
            async with s.get("https://gamma-api.polymarket.com/events",
                             params={"slug": "btc-updown-5m", "limit": 3},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                results.append(f"T1 status:{r.status} type:{type(data).__name__} len:{len(data) if isinstance(data,list) else 'dict'}")
                if isinstance(data, list) and data:
                    results.append(f"T1 slug:{data[0].get('slug','?')} title:{data[0].get('title','?')[:30]}")

            # Test 2: events sans filtre
            async with s.get("https://gamma-api.polymarket.com/events",
                             params={"active": "true", "limit": 5},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                results.append(f"T2 status:{r.status} len:{len(data) if isinstance(data,list) else '?'}")
                if isinstance(data, list):
                    for ev in data[:3]:
                        sl = ev.get("slug","?")
                        if "btc" in sl.lower():
                            results.append(f"BTC found: {sl}")

            # Test 3: markets avec recherche
            async with s.get("https://gamma-api.polymarket.com/markets",
                             params={"active": "true", "limit": 5},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                results.append(f"T3 status:{r.status} type:{type(data).__name__}")
                items = data if isinstance(data,list) else data.get("markets",[])
                for m in items[:3]:
                    sl = m.get("slug","?")
                    if "btc" in sl.lower() or "updown" in sl.lower():
                        results.append(f"BTC market: {sl[:40]}")

    except Exception as e:
        results.append(f"Error: {e}")

    await update.message.reply_text(
        "🔍 *DEBUG API*\n" + "\n".join(results),
        parse_mode="Markdown")

async def cb(update: Update, context):
    q=update.callback_query; await q.answer()
    h={"status":cmd_status,"ai":cmd_ai,"trades":cmd_trades,"stats":cmd_stats,
       "fear":cmd_fear,"score":cmd_score,"run":cmd_run,"stop":cmd_stop,"paper":cmd_paper}
    if q.data in h: await h[q.data](update,context)

def main():
    st.load()
    # Init Polymarket au démarrage si mode réel
    if not st.paper_mode and POLY_PRIVATE_KEY:
        poly.init_client()
    app=Application.builder().token(TOKEN).build()
    for name,handler in [
        ("start",cmd_start),("run",cmd_run),("stop",cmd_stop),("status",cmd_status),
        ("ai",cmd_ai),("signal",cmd_signal),("score",cmd_score),("trades",cmd_trades),
        ("stats",cmd_stats),("fear",cmd_fear),("passes",cmd_passes),("market",cmd_market),
        ("balance",cmd_balance),("paper",cmd_paper),("cooldown",cmd_cooldown),("reset",cmd_reset),
    ]:
        app.add_handler(CommandHandler(name,handler))
    app.add_handler(CallbackQueryHandler(cb))
    log.info("🧠 PolyBot v10 FULLY AUTOMATED démarré")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
