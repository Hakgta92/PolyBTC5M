async def ws_oracle_sol_loop():
    """✅ v12.4 — Remplacé par ws_oracle_loop unifié"""
    pass  # Géré par ws_oracle_loop()

"""
POLYMARKET BOT v12.1 — BTC/ETH/SOL Oracle Lag | TA RSI+EMA | Multi-asset
NOUVEAUTÉS v10.29 — CORRECTIONS MAJEURES:

SOURCES VÉRIFIÉES (juin 2026):
  • Formule frais officielle: fee = shares × feeRate × p × (1-p)
    feeRate crypto = 0.07 (source: docs Polymarket + startpolymarket.com)
    NOTRE ANCIENNE FORMULE ÉTAIT FAUSSE: 0.25*(p*(1-p))²
    Écart à p=0.65$: ancien 0.53¢ vs réel 1.07¢ (x2 sous-estimé!)
  • Maker orders: zéro frais + rebate 100% des frais taker (source: luckylobster.io)
  • Filtre fee_pct>0.5% SUPPRIMÉ: redondant avec EV gate, tuait la zone 0.55-0.75$
  • Fee max crypto = 1.80% à p=0.50$ (source: startpolymarket.com)

NOUVEAUTÉS v10.33 — ARCHITECTURE ORACLE CORRIGÉE (source: blockeden.xyz/forum):

RÉVÉLATIONS SOURCES JUIN 2026:
  1. Chainlink Data Streams = PULL-BASED sub-seconde (pas push 10-30s)
     Notre flux RTDS = exactement la source de settlement. Pas de délai entre
     oracle qu on trace et prix de résolution.
  2. TIES résolus en UP (smart contract): "end price >= start price → UP wins"
     → Bonus UP de +0.01 sur les slots quasi-plats (EV asymétrique)
  3. Settlement delay = 64 blocs Polygon (~2min) APRÈS la fin du slot
     → Pas d impact sur notre trade mais confirme que T-6s est le dernier moment

IMPACT SUR LA STRATÉGIE:
  • Le gap spot↔oracle EST immédiat (sub-sec), pas un lag de 30-55s
  • L edge réel = spot consensus (Binance+CB+Kraken) vs oracle multi-exchange
    Binance bouge d abord → CB/Kraken suivent → oracle aggregate suit
    Pendant cette cascade de 1-5s, le gap est exploitable
  • Seuil gap abaissé: 0.02% → 0.01% (le lag est plus court, seuil doit être fin)
  • cmd_oracle mis à jour: affiche signal réel + recommandation trade

NOUVEAUTÉS v10.28 — R:R FIX (diagnostic sur 20 trades réels):

PROBLÈME IDENTIFIÉ sur v10.27:
  Token 0.80-0.96$ → R:R catastrophique même à 70% WR
  Preuve: gain moy +0.74$ / perte moy -3.87$ = R:R 0.19
  Math: à token 0.88$ il faut WR > 88% pour être à l'équilibre.
  70% WR à 0.88$ = EV -18% par dollar misé → perte inévitable.

CORRECTIFS v10.28:
  • SNIPE_TOKEN_MIN: 0.80 → 0.55$ (R:R viable: 70% WR profitable dès token <0.70$)
  • SNIPE_TOKEN_MAX: 0.96 → 0.75$ (zone où 70% WR = EV positif)
  • BPS_CURRENT_MAX: 10 → 22 (trop strict: 6/6 skips auraient gagné)
  • BPS_CURRENT_MIN: 5 → 2  (idem: bloquait des trades directionnels valides)
  • BPS_TOTAL_MAX: 12 → 30  (élargi — le polybacktest ne tient pas compte du R:R)
  • BPS_TOTAL_MIN: 5 → 2   (idem)
  • SNIPE_EDGE_MIN: 0.04 → 0.10 (garde-fou EV plus strict pour compenser la zone élargie)
  • SNIPE_MIN_PROB: 0.76 → 0.72 (compensé par l'EV gate plus strict)
  • VOL_SAFETY: 2.5 → 3.0 (le modèle était trop confiant — calibration empirique)

MATH DE VALIDATION:
  Token 0.65$, WR réel 70%: EV = 0.70×(1/0.65-1) - 0.30×1 = +7.7% ✅ POSITIF
  Token 0.72$, WR réel 70%: EV = 0.70×0.39 - 0.30×1 = +2.7% ✅ POSITIF
  Token 0.88$, WR réel 70%: EV = 0.70×0.14 - 0.30×1 = -18%  ❌ v10.27 PROBLÈME
"""

import asyncio, math, logging, os, json, time, aiohttp
from datetime import datetime, timedelta
from collections import deque
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

BOT_VERSION = "12.4"

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

TOKEN           = os.getenv("TELEGRAM_TOKEN", "")
ALLOWED_UID     = int(os.getenv("ALLOWED_USER_ID", "0"))
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
PAPER_MODE      = os.getenv("PAPER_MODE", "true").lower() == "true"
POLY_PRIVATE_KEY   = os.getenv("POLY_PRIVATE_KEY", "")
POLY_PROXY_WALLET  = os.getenv("POLY_PROXY_WALLET", "")
POLY_FUNDER_WALLET = os.getenv("POLY_FUNDER_WALLET", "")
POLY_API_KEY       = os.getenv("POLY_API_KEY", "")
POLY_API_SECRET    = os.getenv("POLY_API_SECRET", "")
POLY_API_PASSPHRASE= os.getenv("POLY_API_PASSPHRASE", "")
POLY_HOST          = "https://clob.polymarket.com"
POLY_GAMMA         = "https://gamma-api.polymarket.com"
POLY_CHAIN_ID      = 137

MIN_BET_USD     = 2.0   # ✅ v11.9i — 2$ minimum (Kelly sur BR 28$)
FAIR_EDGE_MIN   = 0.08
MAX_BET_USD     = 4.0   # ✅ v11.9i — 4$ max (14% BR max sur 28$)
MAX_BET_PCT     = 0.05  # ✅ v11.9i — 5% BR max (conservateur)
KELLY_FRACTION  = 0.25

# ✅ v10.27 — Paramètres validés sur 29,060 trades réels (polybacktest.com)
ENTRY_LAST_SECONDS = 60   # Entrée jusqu'à T-60s (polybacktest: pas trop tard)
SNIPE_LAST_MIN     = 30   # Fenêtre: T-4min → T-60s (entrée entre T+30s et T-60s)
SNIPE_MIN_PROB     = 0.72 # ✅ v10.28 — abaissé (compensé par EV gate plus strict)
SNIPE_EDGE_MIN     = 0.10 # ✅ v10.28/29 — EV net après vrais frais ≥10% (ex: token 0.65$ → p_dir≥0.77)
SNIPE_TOKEN_MIN    = 0.55 # ✅ v10.28 — R:R FIX: besoin token<0.70$ pour EV>0 à 70% WR
SNIPE_TOKEN_MAX    = 0.75 # ✅ v10.28 — Cap: à 0.75$ avec 70% WR → EV +2.7%

# ✅ v10.27 — Filtres BPS (basis points) validés sur 29,060 trades
BPS_CURRENT_MIN    = 2    # ✅ v10.28 FIX — était 5: bloquait trades gagnants (WR skips 100%)
BPS_CURRENT_MAX    = 22   # ✅ v10.28 FIX — était 10: idem (bpscurrent 11.2 et 12.0 auraient gagné)
BPS_TOTAL_MIN      = 2    # ✅ v10.28 FIX — était 5: idem
BPS_TOTAL_MAX      = 30   # ✅ v10.28 FIX — élargi (le polybacktest mesure l ordre de grandeur, pas le cap exact)

# ✅ v10.24 — Stop loss réintroduit
STOP_LOSS_MULT     = 0.01  # ✅ v12.1 — désactivé (binaire: pas de stop)

# ═══════════ v10.23 — NOUVELLES CONSTANTES ═══════════
# Oracle lag (le meilleur edge: l'oracle bouge en <1s, l'orderbook met ~55s)
ORACLE_LAG_MIN_PCT  = 0.03   # Divergence oracle vs orderbook mini pour signaler un lag exploitable
ORACLE_FRESH_S      = 3.0    # Tick Chainlink considéré frais si <3s
# Entrée étagée
STAGED_ENTRY        = True   # Splitter la mise en 2 tranches
STAGED_FRACTIONS    = [0.6, 0.4]   # 60% à la 1re entrée, 40% à la 2e si signal tient
# Maker order (presque gratuit: tout est limite sur Polymarket de toute façon)
USE_MAKER_ORDERS    = True   # ✅ Ordre GTC maker = ZÉRO frais + rebate USDC quotidien
MAKER_UNDERCUT      = 0.01   # ✅ v11.9k — 1¢ sous le prix (moins agressif = meilleur fill maker)
# Calibration sigma (auto-correction de VOL_SAFETY après N trades)
CALIB_MIN_TRADES    = 30     # Trades mini avant d'auto-calibrer
# Auto-tuning seuils via WR théorique des skips
AUTOTUNE_MIN_SKIPS  = 25     # Skips résolus mini avant de proposer un ajustement
# Kill-switch drawdown
KILL_SWITCH_LOSSES  = 5      # Pertes consécutives → arrêt total (au-delà du cooldown)

# ✅ v10.30 — ORACLE LAG STRATEGY (source: medium.com/mountain-movers, dev.to/fatherson)
# Edge documenté: l'oracle Chainlink (qui RÈGLE le marché) bouge en <1s
# L'orderbook Polymarket met 30-55s à suivre → fenêtre d'arb
# Strategy: si oracle a bougé X% depuis slot open ET token gagnant encore pas cher → BUY
ORACLE_ENTRY_DELTA  = 0.02  # ✅ v12.1 — 0.02% min delta
ORACLE_TOKEN_MAX    = 0.68  # ✅ v12.4 — 0.68$ max (EV +0.02$/trade à 70% WR)
ORACLE_TOKEN_MIN    = 0.51  # Token min (trop proche de 0.50$ = incertitude trop haute)
ORACLE_EDGE_MIN     = 0.15  # ✅ v12.1 — EV min 15%
ORACLE_WINDOW_START = 25    # ✅ v12.1 — T-25s→T-5s BTC
ORACLE_WINDOW_END   = 5     # ✅ v12.1 — T-5s
# ✅ v10.32 — Mode T-10s (source: github.com/Archetapp — T-10s "direction quasi lockée")
ORACLE_ULTRA_WINDOW = 12    # Passe ultra-précise si T-12s→T-6s ET EV exceptionnelle
ORACLE_ULTRA_EV_MIN = 0.05  # EV min pour passe ultra (moins strict car WR > 95% à T-10s)

# ✅ v10.36 — Filtres WR validés par étude live (medium.com/@gwrx2005, mars 2026)
# Source: filtre 10min → -93% pertes, seuils relevés → -73% fréquence = bien meilleur WR
ORACLE_DELTA_CONTRA_MAX = 0.032 # ✅ v11.9g Haiku — WIN tous delta≥+0.031% LOSS tous delta≤-0.001%  # Si votes=1/3, delta contre doit être < 0.03% sinon skip
ORACLE_GAP_MIN_STRONG   = 0.05  # Gap "fort" = au-delà de ce seuil, même votes=1/3 accepté
ORACLE_TREND_10MIN      = 0.08  # Filtre tendance 10min: si BTC contre-tendance de 0.08%, skip
ORACLE_GAP_CONFIRM_RET  = 0.01  # Return 3s minimum pour confirmer la direction du gap (0.01%=1bps)
ORACLE_MIN_FRESH_S  = 2.0   # Tick oracle doit être frais (<2s) pour trader
EXCH_STALE_S        = 3.0   # Prix exchange ignoré si plus vieux que 3s (consensus_price)


TAKE_PROFIT_MULT    = 2.0
TRAILING_PEAK_MULT  = 99.0  # ✅ v11.9l — désactivé en réel (binaire: tenir jusqu'à résolution)
TRAILING_STOP_MULT  = 0.01  # ✅ v11.9l — désactivé (seuil impossible = jamais déclenché)
TAKE_PROFIT_CHECK   = 15   # ✅ v10.22 — 15s (avant: 30s, trop lent sur du 5min)
POLY_FEE            = 0.02 # Legacy: estimation flat pour le paper mode uniquement
MAX_CONSEC_LOSS     = 2
COOLDOWN_MIN        = 0    # ✅ v12.1 — pas de cooldown (h24)24)
MAX_TRADES_PER_H    = 6   # ✅ v11.9j — 6/h    # ✅ v10.26 — Max 3/heure (supprimé la limite 1, garde-fou à 3)
CONSERVATIVE_AFTER_LOSSES = 2
BOOST_AFTER_WINS    = 999
DAILY_LOSS_MAX      = 0.99  # ✅ v12.1 — pause journalière désactivée (h24)
DAILY_PAUSE_H       = 3

# ✅ v10.21 — Seuils relevés (+2 partout): -73% de trades = 7x moins de pertes (source v3 testée réel)
SESSION_THRESHOLDS = {
    "US_OPEN":      (10, 3.0, 4),
    "US_AFTERNOON": (10, 3.0, 4),
    "EU_OPEN":      (11, 3.5, 4),
    "US_CLOSE":     (11, 3.5, 4),
    "ASIA_LATE":    (12, 4.0, 5),
    "ASIA_EARLY":   (13, 4.5, 5),
    "OVERNIGHT":    (14, 5.0, 6),
}

# ✅ v10.12f — Seuil momentum réduit si score très élevé
SCORE_MOMENTUM_BONUS = {13: 1, 15: 2}

CLAUDE_API    = "https://api.anthropic.com/v1/messages"
FEAR_GREED_API= "https://api.alternative.me/fng/?limit=1"
DATA_FILE     = "polybot_v10_state.json"
BACKUP_FILE   = "polybot_v10_backup.json"
DASHBOARD_FILE= "/tmp/polybot_dashboard.html"

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO,
    handlers=[logging.FileHandler("polybot_v10.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)

def expected_token_price(delta_pct: float, time_remaining: float) -> float:
    """
    ✅ v11.10 — Modèle token price attendu (source: github.com/Archetapp, observation live)
    delta_pct: % de move depuis slot open (abs value)
    time_remaining: secondes restantes
    Retourne le prix attendu du token gagnant selon le delta.
    Si marché < expected → mispricing exploitable.
    """
    # Ajustement temporel: plus on est proche de la fin, plus le token est pricé
    time_factor = max(0.5, min(1.0, time_remaining / 60))  # 1.0 à T-60s, 0.5 à T-30s
    if delta_pct < 0.005:   return 0.50  # coin flip
    elif delta_pct < 0.020: return 0.50 + (delta_pct - 0.005) / 0.015 * 0.05  # 0.50-0.55
    elif delta_pct < 0.050: return 0.55 + (delta_pct - 0.020) / 0.030 * 0.10  # 0.55-0.65
    elif delta_pct < 0.100: return 0.65 + (delta_pct - 0.050) / 0.050 * 0.15  # 0.65-0.80
    elif delta_pct < 0.150: return 0.80 + (delta_pct - 0.100) / 0.050 * 0.12  # 0.80-0.92
    else:                    return min(0.97, 0.92 + (delta_pct - 0.150) * 1.0) # 0.92-0.97


def taker_fee_per_share(p):
    """
    ✅ v10.29 — FORMULE CORRIGÉE (source: startpolymarket.com, docs Polymarket juin 2026)
    fee = shares × feeRate × p × (1-p)
    → par share: FEE_RATE_CRYPTO × p × (1-p)
    FEE_RATE_CRYPTO = 0.07 (crypto 5min/15min uniquement)
    p=0.50 → 1.75¢/share (max) | p=0.65 → 1.59¢ | p=0.75 → 1.31¢ | p=0.90 → 0.63¢
    Maker orders: frais=0 + rebate (USE_MAKER_ORDERS=True dans place_bet)
    ANCIENNE FORMULE ÉTAIT FAUSSE: 0.25*(p*(1-p))² sous-estimait les frais x2
    """
    if p <= 0 or p >= 1: return 0.0
    return FEE_RATE_CRYPTO * p * (1.0 - p)

FEE_RATE_CRYPTO = 0.07  # ✅ v10.29 — taux officiel crypto Polymarket (0.07 = max 1.75¢/share à p=0.50)

def delta_to_weight(pct):
    """✅ v10.22 — Mapping window delta % → poids score (centralisé, 3 usages)"""
    if pct > 0.15: return 6.0
    if pct > 0.05: return 4.0
    if pct > 0.01: return 2.0
    if pct < -0.15: return -6.0
    if pct < -0.05: return -4.0
    if pct < -0.01: return -2.0
    return 0.0

def kelly_bet(bankroll, win_prob, payout_mult, token_price=0.5, ev_bonus=False):
    """
    ✅ v10.26 — Kelly adaptatif 3 tiers selon qualité du setup:

    TIER 1 — NORMAL      (EV 5-10%,  P 78-85%): fraction 0.25 → ~5%  BR
    TIER 2 — FORT        (EV 10-15%, P 85-92%): fraction 0.40 → ~10% BR
    TIER 3 — EXCEPTIONNEL(EV >15%,   P >92%):   fraction 0.55 → ~15% BR

    ev_bonus=True = setup fort ou exceptionnel (oracle confirmé ou EV>15%)
    Jamais retourner MIN_BET si edge nul — retourner 0
    """
    if win_prob <= 0 or payout_mult <= 1:
        return 0.0
    b = payout_mult - 1
    q = 1 - win_prob
    kp = (win_prob * b - q) / b
    if kp <= 0:
        return 0.0  # Edge négatif → ne pas trader

    # Liquidity factor: réduire sur tokens extrêmes
    liquidity_factor = 1.0
    if token_price < 0.15 or token_price > 0.92:
        liquidity_factor = 0.8

    # ✅ v10.26 — 3 tiers selon EV réelle
    ev_real = win_prob - token_price  # EV approximative
    if ev_real >= 0.15 or win_prob >= 0.92:
        # TIER 3 — EXCEPTIONNEL: 15% BR max
        fraction = 0.55
        tier_pct = 0.15
        tier_name = "EXCEPTIONNEL"
    elif ev_real >= 0.10 or win_prob >= 0.85:
        # TIER 2 — FORT: 10% BR max
        fraction = 0.40
        tier_pct = 0.10
        tier_name = "FORT"
    else:
        # TIER 1 — NORMAL: 5% BR max
        fraction = 0.25
        tier_pct = 0.05
        tier_name = "NORMAL"

    # ✅ v11.9k — tier_pct adaptatif: si BR faible, réduire proportionnellement
    effective_tier = min(tier_pct, 0.10)  # jamais plus de 10% BR
    raw_bet = bankroll * min(kp * fraction * liquidity_factor, effective_tier)
    # ✅ v11.9k — 5% BR minimum (source: pros recommandent 1-2%, on met 5% plancher)
    dynamic_min = max(MIN_BET_USD, round(bankroll * 0.05, 2))
    # MAX adaptatif: 10% BR absolu (protège en cas de bankroll faible)
    dynamic_max = min(MAX_BET_USD, round(bankroll * 0.10, 2))
    result = round(max(dynamic_min, min(raw_bet, dynamic_max)), 2)
    log.debug(f"Kelly tier={tier_name} EV={ev_real:.2f} P={win_prob:.2f} → {result:.2f}$")
    return result

# ─── DONNÉES AVANCÉES ──────────────────────────────────────────────────────
async def fetch_orderbook_imbalance():
    """
    ✅ v10.12c — Kraken spread + ticker comme proxy OB.
    """
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(
                "https://api.kraken.com/0/public/Ticker",
                params={"pair": "XBTUSD"},
                timeout=aiohttp.ClientTimeout(total=6)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    t = data.get("result", {}).get("XXBTZUSD", {})
                    if t:
                        bid = float(t["b"][0])
                        ask = float(t["a"][0])
                        bid_vol = float(t["b"][2])
                        ask_vol = float(t["a"][2])
                        spread_pct = (ask - bid) / bid * 100
                        vol_24h = float(t["v"][1])
                        vwap_24h = float(t["p"][1])
                        price = float(t["c"][0])
                        total_vol = bid_vol + ask_vol if (bid_vol + ask_vol) > 0 else 1
                        ratio = round(bid_vol / total_vol, 3)
                        above_vwap = price > vwap_24h
                        if above_vwap and ratio > 0.5:
                            return {"bias": "UP", "ratio": ratio, "desc": f"📗 Kraken OB↑ spread:{spread_pct:.3f}%"}
                        elif not above_vwap and ratio < 0.5:
                            return {"bias": "DOWN", "ratio": ratio, "desc": f"📕 Kraken OB↓ spread:{spread_pct:.3f}%"}
                        else:
                            return {"bias": None, "ratio": ratio, "desc": f"Kraken OB neutre spread:{spread_pct:.3f}%"}
    except Exception as e:
        log.warning(f"OB Kraken: {e}")
    return {"bias": None, "ratio": 0.5, "desc": "OB N/A"}

async def fetch_liquidations():
    """
    ✅ v10.12c — Kraken 24h stats pour détecter excès directionnel.
    """
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(
                "https://api.kraken.com/0/public/Ticker",
                params={"pair": "XBTUSD"},
                timeout=aiohttp.ClientTimeout(total=6)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    t = data.get("result", {}).get("XXBTZUSD", {})
                    if t:
                        price = float(t["c"][0])
                        high_24h = float(t["h"][1])
                        low_24h = float(t["l"][1])
                        vwap_24h = float(t["p"][1])
                        trades_24h = int(t["t"][1])
                        vol_24h = float(t["v"][1])
                        open_price = float(t["o"])
                        change_pct = (price - open_price) / open_price * 100 if open_price > 0 else 0
                        range_pct = (high_24h - low_24h) / low_24h * 100 if low_24h > 0 else 0
                        if (high_24h - low_24h) > 0:
                            pos_in_range = (price - low_24h) / (high_24h - low_24h)
                        else:
                            pos_in_range = 0.5
                        if pos_in_range > 0.85 and change_pct > 2.0:
                            return {"bias": "DOWN", "desc": f"💸 Suracheté {pos_in_range*100:.0f}% range +{change_pct:.1f}%"}
                        elif pos_in_range < 0.15 and change_pct < -2.0:
                            return {"bias": "UP", "desc": f"💸 Survendu {pos_in_range*100:.0f}% range {change_pct:.1f}%"}
                        else:
                            bias = None
                            if change_pct > 1.0: bias = "DOWN"
                            elif change_pct < -1.0: bias = "UP"
                            return {"bias": bias, "desc": f"Kraken {change_pct:+.2f}% pos:{pos_in_range*100:.0f}%range"}
    except Exception as e:
        log.warning(f"Liq Kraken: {e}")
    return {"bias": None, "desc": "Liq N/A"}


async def fetch_eth_klines(interval="5m", limit=30):
    """✅ v10.12d — Kraken ETH avec toutes les clés possibles"""
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    km = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240}
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(
                "https://api.kraken.com/0/public/OHLC",
                params={"pair": "ETHUSD", "interval": km.get(interval, 5), "count": limit},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    result = data.get("result", {})
                    ohlc = None
                    for key in ["XETHUSD", "ETHUSD", "ETHUSDT"]:
                        if key in result:
                            ohlc = result[key]
                            break
                    if not ohlc:
                        for key, val in result.items():
                            if key != "last" and isinstance(val, list) and len(val) > 5:
                                ohlc = val
                                break
                    if ohlc:
                        candles = [{"close": float(k[4]), "open": float(k[1]),
                                   "high": float(k[2]), "low": float(k[3]), "vol": float(k[6])}
                                   for k in ohlc[-limit:]]
                        log.info(f"ETH klines OK: {len(candles)} candles, last close={candles[-1]['close']:.2f}")
                        return candles
                    else:
                        log.warning(f"ETH klines: keys={list(result.keys())}")
    except Exception as e:
        log.warning(f"ETH klines Kraken: {e}")
    return []

def compute_eth_correlation(eth_klines, btc_direction):
    if not eth_klines or len(eth_klines) < 5:
        return 0, "ETH N/A"
    closes = [c["close"] for c in eth_klines]
    e9 = sum(closes[-9:]) / min(9, len(closes))
    e21 = sum(closes[-21:]) / min(21, len(closes)) if len(closes) >= 21 else closes[0]
    eth_dir = "UP" if e9 > e21 else "DOWN"
    change = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] > 0 else 0
    if eth_dir == btc_direction:
        return 1.5, f"Ξ confirme {eth_dir} ({change:+.2f}%)"
    else:
        return -1.0, f"Ξ diverge {eth_dir} ({change:+.2f}%)"

# ─── DASHBOARD HTML ────────────────────────────────────────────────────────
def generate_dashboard(trades, bankroll, bankroll_ref, pnl):
    """Génère un dashboard HTML avec graphique PnL et stats"""
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    roi = round((bankroll - bankroll_ref) / bankroll_ref * 100, 2) if bankroll_ref > 0 else 0

    cumul = 0; pnl_points = []
    for t in sorted(trades, key=lambda x: x.get("ts", 0)):
        cumul += t["pnl"]
        ts = datetime.fromtimestamp(t.get("ts", 0)).strftime("%d/%m %H:%M")
        pnl_points.append({"x": ts, "y": round(cumul, 2)})

    sessions = {}
    for t in trades:
        s = t.get("session", "?")
        if s not in sessions: sessions[s] = {"w": 0, "l": 0}
        if t["result"] == "WIN": sessions[s]["w"] += 1
        else: sessions[s]["l"] += 1

    sess_rows = ""
    for s, v in sessions.items():
        total_s = v["w"] + v["l"]
        wr_s = v["w"] / total_s * 100 if total_s > 0 else 0
        color = "#4CAF50" if wr_s >= 50 else "#f44336"
        sess_rows += f'<tr><td>{s}</td><td>{v["w"]}</td><td>{v["l"]}</td><td style="color:{color}">{wr_s:.0f}%</td></tr>'

    trade_rows = ""
    for t in sorted(trades, key=lambda x: x.get("ts", 0), reverse=True)[:10]:
        ts = datetime.fromtimestamp(t.get("ts", 0)).strftime("%d/%m %H:%M")
        color = "#4CAF50" if t["pnl"] >= 0 else "#f44336"
        emoji = "✅" if t["result"] == "WIN" else "❌"
        trade_rows += f'<tr><td>{emoji}</td><td>{t["dir"]}</td><td style="color:{color}">{t["pnl"]:+.2f}$</td><td>{ts}</td></tr>'

    labels = json.dumps([p["x"] for p in pnl_points])
    data_vals = json.dumps([p["y"] for p in pnl_points])
    total = len(trades)
    wins = sum(1 for t in trades if t["result"] == "WIN")
    wr = round(wins / total * 100, 1) if total > 0 else 0

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PolyBot v{BOT_VERSION} Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  body{{font-family:Arial,sans-serif;background:#1a1a2e;color:#eee;margin:0;padding:20px}}
  .card{{background:#16213e;border-radius:12px;padding:20px;margin:10px 0}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}}
  .stat{{background:#0f3460;border-radius:8px;padding:15px;text-align:center}}
  .stat .val{{font-size:24px;font-weight:bold;color:#e94560}}
  .stat .lbl{{font-size:12px;color:#aaa;margin-top:5px}}
  table{{width:100%;border-collapse:collapse}}
  th,td{{padding:8px;border-bottom:1px solid #333;text-align:left;font-size:13px}}
  th{{color:#aaa}}
  h2{{color:#e94560;margin-top:0}}
  .positive{{color:#4CAF50}} .negative{{color:#f44336}}
</style>
</head>
<body>
<h1>🧠 PolyBot v{BOT_VERSION} — Dashboard</h1>
<p style="color:#aaa">Généré le {now}</p>

<div class="card">
<div class="grid">
  <div class="stat"><div class="val {'positive' if roi>=0 else 'negative'}">{roi:+.2f}%</div><div class="lbl">ROI</div></div>
  <div class="stat"><div class="val">{bankroll:.2f}$</div><div class="lbl">Bankroll</div></div>
  <div class="stat"><div class="val {'positive' if pnl>=0 else 'negative'}">{pnl:+.2f}$</div><div class="lbl">PnL Session</div></div>
  <div class="stat"><div class="val">{wr}%</div><div class="lbl">Win Rate</div></div>
  <div class="stat"><div class="val">{total}</div><div class="lbl">Trades</div></div>
  <div class="stat"><div class="val">{wins}</div><div class="lbl">Wins</div></div>
</div>
</div>

<div class="card">
<h2>📈 PnL Cumulé</h2>
<canvas id="pnlChart" height="100"></canvas>
</div>

<div class="card">
<h2>📊 WR par Session</h2>
<table>
<tr><th>Session</th><th>✅ Wins</th><th>❌ Losses</th><th>WR</th></tr>
{sess_rows}
</table>
</div>

<div class="card">
<h2>📋 Derniers Trades</h2>
<table>
<tr><th></th><th>Dir</th><th>PnL</th><th>Date</th></tr>
{trade_rows}
</table>
</div>

<script>
const ctx = document.getElementById('pnlChart').getContext('2d');
new Chart(ctx, {{
  type: 'line',
  data: {{
    labels: {labels},
    datasets: [{{
      label: 'PnL Cumulé ($)',
      data: {data_vals},
      borderColor: '#e94560',
      backgroundColor: 'rgba(233,69,96,0.1)',
      fill: true,
      tension: 0.4,
      pointRadius: 3
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ labels: {{ color: '#eee' }} }} }},
    scales: {{
      x: {{ ticks: {{ color: '#aaa', maxTicksLimit: 10 }}, grid: {{ color: '#333' }} }},
      y: {{ ticks: {{ color: '#aaa' }}, grid: {{ color: '#333' }} }}
    }}
  }}
}});
</script>
</body>
</html>"""
    return html

# ─── POLYMARKET CLIENT ─────────────────────────────────────────────────────
class PolyClient:
    def __init__(self):
        self.client=None; self.ready=False; self.client_version="v1"

    def init_client(self):
        if not POLY_PRIVATE_KEY or not POLY_PROXY_WALLET:
            log.warning("Clés Polymarket manquantes"); return False
        # ✅ v10.14 — Migration vers py-clob-client-v2 (CLOB V2 depuis avril 2026)
        try:
            from py_clob_client_v2 import ClobClient as ClobClientV2, ApiCreds
            # ✅ v10.14l — signature_type=3 (POLY_1271) + funder=deposit wallet
            deposit_wallet = POLY_PROXY_WALLET
            self.client = ClobClientV2(
                host=POLY_HOST,
                key=POLY_PRIVATE_KEY,
                chain_id=POLY_CHAIN_ID,
                signature_type=3,
                funder=deposit_wallet
            )
            creds = self.client.create_or_derive_api_key()
            self.client = ClobClientV2(
                host=POLY_HOST,
                key=POLY_PRIVATE_KEY,
                chain_id=POLY_CHAIN_ID,
                signature_type=3,
                funder=deposit_wallet,
                creds=creds
            )
            self.ready = True
            self.client_version = "v2"
            log.info(f"✅ Polymarket CLOB V2 initialisé (sig_type=3, deposit={deposit_wallet[:10]}...)"); return True
        except ImportError:
            log.warning("py-clob-client-v2 non installé, fallback v1")
        except Exception as e:
            log.warning(f"CLOB V2 init: {e}, fallback v1")
        # Fallback v1
        try:
            from py_clob_client.client import ClobClient
            self.client=ClobClient(POLY_HOST,key=POLY_PRIVATE_KEY,chain_id=POLY_CHAIN_ID,
                signature_type=1,funder=POLY_PROXY_WALLET)
            creds=self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)
            self.ready=True
            self.client_version = "v1"
            log.info("✅ Polymarket CLOB V1 initialisé"); return True
        except Exception as e: log.error(f"Polymarket init: {e}"); return False

    async def get_market_by_slug(self, slug: str):
        """✅ v12.2 — Récupère un marché par slug (BTC/ETH/SOL)"""
        headers={"User-Agent":"Mozilla/5.0","Accept":"application/json",
                 "Referer":"https://polymarket.com/","Origin":"https://polymarket.com"}
        for endpoint in ["/events","/markets"]:
            try:
                async with aiohttp.ClientSession(headers=headers) as s:
                    async with s.get(f"{POLY_GAMMA}{endpoint}",params={"slug":slug},
                                     timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status==200:
                            data=await r.json()
                            items=data if isinstance(data,list) else data.get("events",data.get("markets",[]))
                            for item in items:
                                if slug in item.get("slug",""):
                                    markets=item.get("markets",[item])
                                    for m in markets:
                                        ids=m.get("clobTokenIds","[]")
                                        if isinstance(ids,str):
                                            try: ids=json.loads(ids)
                                            except: ids=[]
                                        if len(ids)>=2:
                                            return {"token_up":ids[0],"token_down":ids[1],
                                                "question":item.get("title",item.get("question",slug)),
                                                "condition_id":m.get("conditionId",""),
                                                "end_date":m.get("endDate",""),"market_slug":slug}
            except Exception as e: log.warning(f"get_market_by_slug {slug}{endpoint}: {e}")
        return None

    async def find_btc_5min_market(self):
        now=int(time.time()); current_ts=(now//300)*300
        headers={"User-Agent":"Mozilla/5.0","Accept":"application/json",
                 "Referer":"https://polymarket.com/","Origin":"https://polymarket.com"}
        for ts in [current_ts,current_ts+300,current_ts-300]:
            slug=f"btc-updown-5m-{ts}"
            for endpoint in ["/events","/markets"]:
                try:
                    async with aiohttp.ClientSession(headers=headers) as s:
                        async with s.get(f"{POLY_GAMMA}{endpoint}",params={"slug":slug},
                                         timeout=aiohttp.ClientTimeout(total=10)) as r:
                            if r.status==200:
                                data=await r.json()
                                items=data if isinstance(data,list) else data.get("events",data.get("markets",[]))
                                for item in items:
                                    if slug in item.get("slug",""):
                                        markets=item.get("markets",[item])
                                        for m in markets:
                                            ids=m.get("clobTokenIds","[]")
                                            if isinstance(ids,str):
                                                try: ids=json.loads(ids)
                                                except: ids=[]
                                            if len(ids)>=2:
                                                return {"token_up":ids[0],"token_down":ids[1],
                                                    "question":item.get("title",item.get("question",slug)),
                                                    "condition_id":m.get("conditionId",""),
                                                    "end_date":m.get("endDate",""),"market_slug":slug}
                except Exception as e: log.warning(f"{slug}{endpoint}: {e}")
        return None

    async def get_token_price(self,token_id):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{POLY_HOST}/price",params={"token_id":token_id,"side":"buy"},
                                 timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status==200:
                        return float((await r.json()).get("price",0.5))
        except: pass
        return 0.5

    async def place_order(self, token_id, amount_usdc, ref_price, side="BUY"):
        """
        ✅ v10.23 — Ordre LIMITE maker. Sur Polymarket tout est limite de toute façon;
        on pose à ref_price - MAKER_UNDERCUT pour viser le rebate/zéro frais.
        Si non rempli rapidement, le client retombe sur un FAK proche du marché.
        """
        if not self.ready or not self.client: return None
        if not USE_MAKER_ORDERS:
            return await self.place_market_order(token_id, amount_usdc, side)
        client_version = getattr(self, "client_version", "v1")
        amount_float=float(amount_usdc)
        if client_version=="v2":
            try:
                from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
                side_v2 = Side.BUY if side=="BUY" else Side.SELL
                size_val=round(max(5.0,amount_float),2)
                # Maker: undercut léger (BUY → un peu plus bas; on reste sous l'ask)
                maker_price=round(max(0.01,min(0.99, ref_price - MAKER_UNDERCUT)),2)
                # 1) Tente GTC (maker, peut obtenir rebate)
                for price_val, otype in [(maker_price, OrderType.GTC), (round(min(0.99,ref_price*1.02),2), OrderType.FAK)]:
                    try:
                        resp=self.client.create_and_post_order(
                            order_args=OrderArgs(token_id=token_id, price=price_val, side=side_v2, size=size_val),
                            options=PartialCreateOrderOptions(tick_size="0.01"),
                            order_type=otype)
                        log.info(f"place_order {otype} @{price_val}: {resp}")
                        if resp and (resp.get("success") or resp.get("orderID")):
                            return resp.get("orderID", resp.get("id","unknown"))
                    except Exception as e:
                        log.warning(f"place_order {otype}: {e}")
            except Exception as e:
                log.error(f"place_order v2: {e}")
            return None
        # v1 fallback: market
        return await self.place_market_order(token_id, amount_usdc, side)

    async def place_market_order(self,token_id,amount_usdc,side="BUY"):
        if not self.ready or not self.client: return None

        amount_float = float(amount_usdc)
        client_version = getattr(self, "client_version", "v1")

        # ✅ v10.14 — CLOB V2 API
        if client_version == "v2":
            try:
                from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
                side_v2 = Side.BUY if side == "BUY" else Side.SELL
                size_val = round(max(5.0, amount_float), 2)  # min 5$

                # ✅ v10.19 — Prix dynamique avec slippage adaptatif
                try:
                    token_price_resp = await self.get_token_price(token_id)
                    if token_price_resp > 0 and token_price_resp < 1.0:
                        if token_price_resp < 0.2 or token_price_resp > 0.8:
                            slippage = 0.05
                        else:
                            slippage = 0.02
                        price_val = round(min(0.99, token_price_resp * (1 + slippage)), 2)
                    else:
                        price_val = 0.50
                except:
                    price_val = 0.50

                log.info(f"V2 order: token={token_id[:10]} price={price_val} size={size_val}")

                for order_type_v2 in [OrderType.FAK, OrderType.GTC]:
                    try:
                        resp = self.client.create_and_post_order(
                            order_args=OrderArgs(
                                token_id=token_id,
                                price=price_val,
                                side=side_v2,
                                size=size_val,
                            ),
                            options=PartialCreateOrderOptions(tick_size="0.01"),
                            order_type=order_type_v2,
                        )
                        log.info(f"V2 {order_type_v2} réponse: {resp}")
                        if resp and resp.get("success"):
                            oid = resp.get("orderID", resp.get("id", "unknown"))
                            log.info(f"✅ Ordre V2 {order_type_v2} placé: {oid}")
                            return oid
                        log.warning(f"V2 {order_type_v2} refusé: {resp}")
                    except Exception as e:
                        log.warning(f"V2 {order_type_v2} erreur: {e}")
            except Exception as e:
                log.error(f"V2 order erreur: {e}")
            return None

        # Fallback V1
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            amount_float = float(amount_usdc)
            side_str = "BUY" if side == "BUY" else "SELL"
            for order_type in [OrderType.FOK, OrderType.GTC]:
                try:
                    mo = MarketOrderArgs(token_id=token_id, amount=amount_float,
                        side=side_str, order_type=order_type)
                    signed = self.client.create_market_order(mo)
                    resp = self.client.post_order(signed, order_type)
                    if resp and resp.get("success"):
                        return resp.get("orderID", resp.get("id", "unknown"))
                    log.warning(f"V1 {order_type} refusé: {resp}")
                except Exception as e:
                    log.warning(f"V1 {order_type} erreur: {e}")
        except Exception as e:
            log.error(f"V1 import erreur: {e}")
        return None

    async def place_limit_maker(self, token_id, amount_usdc, price, side="BUY"):
        if not self.ready or not self.client: return None
        if getattr(self, "client_version", "v1") != "v2": return None
        try:
            from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
            size_val = round(max(5.0, float(amount_usdc)), 2)
            price_val = round(min(0.99, max(0.01, price)), 2)
            resp = self.client.create_and_post_order(
                order_args=OrderArgs(token_id=token_id, price=price_val,
                                     side=Side.BUY if side=="BUY" else Side.SELL, size=size_val),
                options=PartialCreateOrderOptions(tick_size="0.01"),
                order_type=OrderType.GTC)
            log.info(f"maker GTC: {resp}")
            if resp and (resp.get("success") or resp.get("orderID")):
                return resp.get("orderID", resp.get("id", "maker"))
        except Exception as e:
            log.warning(f"place_limit_maker: {e}")
        return None

    async def order_filled(self, token_id):
        if not self.ready or getattr(self,"client_version","v1")!="v2": return False
        try:
            from py_clob_client_v2 import BalanceAllowanceParams
            from py_clob_client_v2.clob_types import AssetType
            resp = self.client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id))
            if resp:
                bal = resp.get("balance", resp.get("amount", 0))
                return float(bal) > 0
        except Exception as e:
            log.warning(f"order_filled: {e}")
        return False

    async def sell_position(self, token_id, shares, opposite_token_id=None, current_price=0.5):
        """
        ✅ v10.20k — Vente via negative risk Polymarket
        """
        if not self.ready or not self.client: return None
        try:
            from py_clob_client_v2 import OrderArgs, OrderType, Side, PartialCreateOrderOptions

            # Méthode 1: SELL direct du token (FAK)
            try:
                sell_price = round(max(0.01, current_price - 0.02), 2)
                resp = self.client.create_and_post_order(
                    order_args=OrderArgs(token_id=token_id, price=sell_price, side=Side.SELL, size=round(float(shares), 2)),
                    options=PartialCreateOrderOptions(tick_size="0.01"),
                    order_type=OrderType.FAK,
                )
                log.info(f"sell_position FAK: {resp}")
                if resp and resp.get("success"):
                    return resp
            except Exception as e1:
                log.warning(f"sell FAK échoué: {e1}")

            # Méthode 2: GTC limite (reste dans l'orderbook)
            try:
                sell_price = round(max(0.01, current_price - 0.01), 2)
                resp2 = self.client.create_and_post_order(
                    order_args=OrderArgs(token_id=token_id, price=sell_price, side=Side.SELL, size=round(float(shares), 2)),
                    options=PartialCreateOrderOptions(tick_size="0.01"),
                    order_type=OrderType.GTC,
                )
                log.info(f"sell_position GTC: {resp2}")
                if resp2 and (resp2.get("success") or resp2.get("orderID")):
                    return resp2
            except Exception as e2:
                log.warning(f"sell GTC échoué: {e2}")

            # Méthode 3: Acheter le token opposé (negative risk)
            if opposite_token_id:
                try:
                    buy_price = round(min(0.99, 1.0 - current_price + 0.02), 2)
                    resp3 = self.client.create_and_post_order(
                        order_args=OrderArgs(token_id=opposite_token_id, price=buy_price, side=Side.BUY, size=round(float(shares), 2)),
                        options=PartialCreateOrderOptions(tick_size="0.01"),
                        order_type=OrderType.FAK,
                    )
                    log.info(f"sell via opposite token FAK: {resp3}")
                    if resp3 and resp3.get("success"):
                        return resp3
                except Exception as e3:
                    log.warning(f"sell opposite échoué: {e3}")

        except Exception as e:
            err = str(e)
            if "No orderbook" in err or "404" in err:
                log.info("sell_position: slot expiré, résolution auto")
                return {"success": True, "auto_resolved": True}
            log.error(f"sell_position: {e}")
        return None

poly=PolyClient()

# ─── INDICATEURS ───────────────────────────────────────────────────────────
def ema(values,period):
    if not values: return 0
    if len(values)<period: return values[-1]
    k=2/(period+1); e=sum(values[:period])/period
    for v in values[period:]: e=v*k+e*(1-k)
    return e

def ema_slope(values,period,lookback=3):
    if len(values)<period+lookback: return 0.0
    e_now=ema(values,period); e_prev=ema(values[:-lookback],period)
    return round((e_now-e_prev)/e_prev*100,4) if e_prev else 0.0

def rsi(closes,period=14):
    if len(closes)<period+1: return 50.0
    gains=losses=0.0
    for i in range(len(closes)-period,len(closes)):
        d=closes[i]-closes[i-1]
        if d>0: gains+=d
        else: losses-=d
    if losses==0: return 100.0
    return round(100-100/(1+gains/losses),2)

def macd_calc(closes):
    if len(closes)<26: return 0,0,0,False
    ml=ema(closes,12)-ema(closes,26)
    ml_prev=ema(closes[:-1],12)-ema(closes[:-1],26) if len(closes)>26 else ml
    sig=ema([ml_prev,ml],9) if ml_prev!=ml else ml*0.9
    hist=ml-sig
    cross=((ml_prev<sig)and(ml>sig))or((ml_prev>sig)and(ml<sig))
    return round(ml,4),round(sig,4),round(hist,4),cross

def bollinger(closes,period=20):
    if len(closes)<period: return None,None,None,False
    w=closes[-period:]; mid=sum(w)/period
    std=math.sqrt(sum((x-mid)**2 for x in w)/period)
    bb_l=round(mid-2*std,2); bb_h=round(mid+2*std,2)
    return bb_l,round(mid,2),bb_h,(bb_h-bb_l)/mid*100<0.8 if mid else False

def atr_calc(candles,period=14):
    if len(candles)<2: return 0.0
    trs=[max(c["high"]-c["low"],abs(c["high"]-candles[i-1]["close"]),
             abs(c["low"]-candles[i-1]["close"])) for i,c in enumerate(candles) if i>0]
    return round(sum(trs[-period:])/min(len(trs),period),2) if trs else 0.0

def stoch(closes,highs,lows,period=14):
    if len(closes)<period: return 50.0,50.0
    lo,hi=min(lows[-period:]),max(highs[-period:])
    if hi==lo: return 50.0,50.0
    k=(closes[-1]-lo)/(hi-lo)*100; d=(closes[-2]-lo)/(hi-lo)*100 if len(closes)>period else k
    return round(k,1),round(d,1)

def williams_r(closes,highs,lows,period=14):
    if len(closes)<period: return -50.0
    hi,lo=max(highs[-period:]),min(lows[-period:])
    return round(-100*(hi-closes[-1])/(hi-lo),1) if hi!=lo else -50.0

def adx_calc(candles, period=14):
    """✅ v10.20 — ADX (Average Directional Index)"""
    if len(candles) < period + 2: return 20.0, 0.0, 0.0
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]

    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(candles)):
        h_diff = highs[i] - highs[i-1]
        l_diff = lows[i-1] - lows[i]
        plus_dm.append(max(h_diff, 0) if h_diff > l_diff else 0)
        minus_dm.append(max(l_diff, 0) if l_diff > h_diff else 0)
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        tr_list.append(tr)

    def smooth(values, p):
        s = sum(values[:p])
        result = [s]
        for v in values[p:]:
            s = s - s/p + v
            result.append(s)
        return result

    atr_s = smooth(tr_list, period)
    pdm_s = smooth(plus_dm, period)
    mdm_s = smooth(minus_dm, period)

    pdi = [100*p/a if a>0 else 0 for p,a in zip(pdm_s, atr_s)]
    mdi = [100*m/a if a>0 else 0 for m,a in zip(mdm_s, atr_s)]
    dx = [100*abs(p-m)/(p+m) if (p+m)>0 else 0 for p,m in zip(pdi, mdi)]

    if len(dx) < period: return 20.0, pdi[-1] if pdi else 0, mdi[-1] if mdi else 0
    adx_val = sum(dx[-period:]) / period
    return round(adx_val, 1), round(pdi[-1], 1), round(mdi[-1], 1)

def vwap_calc(candles):
    if not candles: return 0
    tv=sum(c["vol"] for c in candles)
    return round(sum(((c["high"]+c["low"]+c["close"])/3)*c["vol"] for c in candles)/tv,2) if tv else candles[-1]["close"]

def detect_volume_spike(candles,lookback=20):
    if len(candles)<lookback: return False
    vols=[c["vol"] for c in candles[-lookback:-1]]; avg=sum(vols)/len(vols) if vols else 1
    return candles[-1]["vol"]>avg*2.0

def detect_consolidation(candles,period=6):
    """✅ v10.19 — Détection range serré améliorée"""
    if len(candles)<period: return False
    highs=[c["high"] for c in candles[-period:]]; lows=[c["low"] for c in candles[-period:]]
    price=candles[-1]["close"] or 1
    range_pct = (max(highs)-min(lows))/price*100
    if range_pct < 0.15: return True
    if len(candles) >= 12:
        highs12=[c["high"] for c in candles[-12:]]; lows12=[c["low"] for c in candles[-12:]]
        range12 = (max(highs12)-min(lows12))/price*100
        if range12 < 0.25: return True
    return False

def detect_divergence(candles_5m):
    if len(candles_5m)<15: return None
    closes=[c["close"] for c in candles_5m[-15:]]
    rsis=[rsi(closes[max(0,i-14):i+1]) for i in range(5,15)]
    if len(rsis)<6: return None
    if closes[-1]<closes[-4]<closes[-7] and rsis[-1]>rsis[-4]>rsis[-7] and rsis[-1]<45: return "BULLISH"
    if closes[-1]>closes[-4]>closes[-7] and rsis[-1]<rsis[-4]<rsis[-7] and rsis[-1]>55: return "BEARISH"
    return None

def detect_rsi_divergence_4h(candles_4h):
    """✅ v10.20b — Divergence RSI sur 4h — signal fort de retournement"""
    if len(candles_4h) < 10: return None
    closes = [c["close"] for c in candles_4h[-10:]]
    rsis = [rsi(closes[max(0,i-7):i+1]) for i in range(3, 10)]
    if len(rsis) < 4: return None
    if closes[-1] < closes[-4] and rsis[-1] > rsis[-4] and rsis[-1] < 40:
        return "BULLISH"
    if closes[-1] > closes[-4] and rsis[-1] < rsis[-4] and rsis[-1] > 60:
        return "BEARISH"
    return None

def detect_engulfing(candles):
    if len(candles)<3: return None
    prev,curr=candles[-2],candles[-1]
    pb=abs(prev["close"]-prev["open"]); cb=abs(curr["close"]-curr["open"])
    if pb==0: return None
    if prev["close"]<prev["open"] and curr["close"]>curr["open"] and curr["open"]<prev["close"] and curr["close"]>prev["open"] and cb>pb*1.3: return "BULLISH"
    if prev["close"]>prev["open"] and curr["close"]<curr["open"] and curr["open"]>prev["close"] and curr["close"]<prev["open"] and cb>pb*1.3: return "BEARISH"
    return None

def detect_vwap_break(candles,lookback=6):
    if len(candles)<lookback+2: return None
    vw=vwap_calc(candles[-20:]); pp,cp=candles[-2]["close"],candles[-1]["close"]
    vols=[c["vol"] for c in candles[-lookback:]]; avg_v=sum(vols)/len(vols) if vols else 1
    vol_ok=candles[-1]["vol"]>avg_v*1.5
    if pp<vw and cp>vw and vol_ok: return "BULLISH"
    if pp>vw and cp<vw and vol_ok: return "BEARISH"
    return None

def pivot_sr(candles,lookback=20):
    if len(candles)<lookback: return [],[]
    highs=[c["high"] for c in candles[-lookback:]]; lows=[c["low"] for c in candles[-lookback:]]
    price=candles[-1]["close"]; atr=atr_calc(candles)*3; res,sup=[],[]
    for i in range(2,len(highs)-2):
        if highs[i]>highs[i-1] and highs[i]>highs[i+1] and highs[i]>highs[i-2] and highs[i]>highs[i+2]:
            if highs[i]>price and highs[i]-price<atr: res.append(round(highs[i],0))
        if lows[i]<lows[i-1] and lows[i]<lows[i+1] and lows[i]<lows[i-2] and lows[i]<lows[i+2]:
            if lows[i]<price and price-lows[i]<atr: sup.append(round(lows[i],0))
    return sorted(set(sup),reverse=True)[:2],sorted(set(res))[:2]

def compute_ind(candles):
    if len(candles)<10: return {}
    c=[x["close"] for x in candles]; h=[x["high"] for x in candles]
    l=[x["low"] for x in candles]; v=[x["vol"] for x in candles]; price=c[-1]
    e9=ema(c,9); e21=ema(c,21); e50=ema(c,min(50,len(c)))
    r14=rsi(c,14); r7=rsi(c,7); ml,sg,hist,cross=macd_calc(c)
    bb_l,bb_m,bb_h,squeeze=bollinger(c); at=atr_calc(candles)
    stk,std=stoch(c,h,l); wr_v=williams_r(c,h,l); vw=vwap_calc(candles[-20:])
    av=sum(v[-10:])/10 if len(v)>=10 else v[-1]; mom=c[-1]-c[-6] if len(c)>=6 else 0
    sup,res=pivot_sr(candles)
    adx_v, pdi_v, mdi_v = adx_calc(candles)
    return {"price":round(price,2),"rsi_7":r7,"rsi_14":r14,"ema9":round(e9,2),"ema21":round(e21,2),
        "ema50":round(e50,2),"slope_e9":ema_slope(c,9),"slope_e21":ema_slope(c,21),
        "macd_hist":hist,"macd_line":ml,"macd_cross":cross,"bb_low":bb_l,"bb_mid":bb_m,
        "bb_high":bb_h,"bb_squeeze":squeeze,"atr":at,"atr_pct":round(at/price*100,3) if price else 0,
        "stoch_k":stk,"stoch_d":std,"williams_r":wr_v,"vwap":vw,"above_vwap":price>vw,
        "vol_ratio":round(v[-1]/av,2) if av else 1.0,"vol_spike":detect_volume_spike(candles),
        "consolidation":detect_consolidation(candles),"momentum":round(mom,2),
        "ema_bull":e9>e21,"ema_bull_strong":e9>e21 and e21>e50,"supports":sup,"resistances":res,
        "adx":adx_v,"pdi":pdi_v,"mdi":mdi_v}

def compute_rsi(prices, period=7):
    """RSI rapide sur période courte pour 5min trading."""
    if len(prices) < period + 1: return 50.0
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = prices[-i] - prices[-i-1]
        if d > 0: gains += d
        else: losses -= d
    if losses == 0: return 100.0
    rs = (gains/period) / (losses/period)
    return 100 - (100 / (1 + rs))

def compute_ema(prices, period):
    """EMA sur liste de prix."""
    if len(prices) < period: return prices[-1] if prices else 0
    k = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]: ema = p * k + ema * (1 - k)
    return ema

def compute_ta_score(price_history, asset="BTC"):
    """
    ✅ v11.10z — Score TA 5 indicateurs calibrés pour marchés binaires 5min.
    Source: Archetapp/PolymarketBot + BTCC RSI 7 research.
    Retourne: (score, direction, details)
    score > 0 = UP, score < 0 = DOWN
    """
    if len(price_history) < 8: return 0, None, {}
    prices = [x["price"] for x in sorted(price_history, key=lambda x: x["ts"])]
    now = price_history[-1]["ts"] if price_history else 0
    score = 0; details = {}

    # 1) RSI(7) — <35=oversold=UP signal | >65=overbought=DOWN signal
    rsi = compute_rsi(prices, 7)
    details["rsi"] = round(rsi, 1)
    if rsi < 35: score += 2    # forte oversold = rebond UP probable
    elif rsi < 45: score += 1  # légère oversold
    elif rsi > 65: score -= 2  # forte overbought = retournement DOWN
    elif rsi > 55: score -= 1  # légère overbought

    # 2) EMA9 vs EMA21 — crossover = signal tendance
    if len(prices) >= 21:
        ema9 = compute_ema(prices[-21:], 9)
        ema21 = compute_ema(prices[-21:], 21)
        details["ema9"] = round(ema9, 2)
        details["ema21"] = round(ema21, 2)
        gap_pct = (ema9 - ema21) / ema21 * 100
        if gap_pct > 0.02: score += 1    # EMA9 > EMA21 = haussier
        elif gap_pct < -0.02: score -= 1 # EMA9 < EMA21 = baissier

    # 3) Momentum 3min — ROC des 3 dernières minutes
    pts_3min = [x for x in price_history if now - x["ts"] <= 180]
    if len(pts_3min) >= 2:
        roc = (pts_3min[-1]["price"] - pts_3min[0]["price"]) / pts_3min[0]["price"] * 100
        details["mom3m"] = round(roc, 4)
        if roc > 0.03: score += 1   # momentum positif fort
        elif roc > 0.01: score += 0 # momentum léger
        elif roc < -0.03: score -= 1

    # 4) Volatilité — si prix très volatil, signal moins fiable
    if len(prices) >= 10:
        recent = prices[-10:]
        avg = sum(recent)/len(recent)
        std = (sum((p-avg)**2 for p in recent)/len(recent))**0.5
        vol_pct = std/avg*100
        details["vol"] = round(vol_pct, 4)
        if vol_pct > 0.1: score = int(score * 0.7)  # réduire confiance si trop volatil

    direction = "UP" if score > 0 else ("DOWN" if score < 0 else None)
    return score, direction, details

def compute_advanced_signals(candles_5m,candles_1m,candles_4h=None):
    div=detect_divergence(candles_5m)
    div_4h=detect_rsi_divergence_4h(candles_4h) if candles_4h else None
    eng=detect_engulfing(candles_5m[-3:]) if len(candles_5m)>=3 else None
    vb=detect_vwap_break(candles_5m)
    signals=[]; score=0
    if div=="BULLISH": signals.append("🔄 Divergence RSI haussière"); score+=2
    elif div=="BEARISH": signals.append("🔄 Divergence RSI baissière"); score-=2
    if eng=="BULLISH": signals.append("🕯️ Engulfing haussier"); score+=2
    elif eng=="BEARISH": signals.append("🕯️ Engulfing baissier"); score-=2
    if vb=="BULLISH": signals.append("📊 VWAP break ↑"); score+=1.5
    elif vb=="BEARISH": signals.append("📊 VWAP break ↓"); score-=1.5
    if div_4h=="BULLISH": signals.append("🔄 Div RSI 4h haussière ⚡"); score+=3.0
    elif div_4h=="BEARISH": signals.append("🔄 Div RSI 4h baissière ⚡"); score-=3.0
    return {"divergence":div,"divergence_4h":div_4h,"engulfing":eng,"vwap_break":vb,"signals":signals,"score":score,
            "bias":"UP" if score>0 else "DOWN" if score<0 else None}

# ✅ v10.16 — Watchdog: timestamp du dernier tick actif
_last_tick_ts = 0

def session_ctx():
    h=(datetime.utcnow().hour+2)%24
    if   14<=h<17: return {"session":"US_OPEN",     "quality":"EXCELLENT","score_bonus":2}
    elif 17<=h<20: return {"session":"US_AFTERNOON","quality":"EXCELLENT","score_bonus":1}
    elif  9<=h<14: return {"session":"EU_OPEN",     "quality":"GOOD",     "score_bonus":1}
    elif 20<=h<22: return {"session":"US_CLOSE",    "quality":"GOOD",     "score_bonus":0}
    elif  7<=h< 9: return {"session":"ASIA_LATE",   "quality":"MEDIUM",   "score_bonus":0}
    elif  1<=h< 7: return {"session":"ASIA_EARLY",  "quality":"MEDIUM",   "score_bonus":-1}
    else:          return {"session":"OVERNIGHT",   "quality":"LOW",      "score_bonus":-2}

def get_session_thresholds(session_name, score=0):
    """
    ✅ v10.12f — Seuil momentum adaptatif selon le score.
    ✅ v10.17 — Mode turbo: seuils réduits si actif
    """
    min_score, min_diff, min_mom = SESSION_THRESHOLDS.get(session_name, (10, 3.5, 4))
    if hasattr(st, 'conservative_until') and time.time() < st.conservative_until:
        min_score = min_score + 2
        min_mom = min_mom + 1
        min_diff = min_diff + 0.5
    elif hasattr(st, 'turbo_until') and time.time() < st.turbo_until:
        min_score = max(7, min_score - 2)
        min_mom = max(2, min_mom - 1)
        min_diff = max(1.5, min_diff - 0.5)
    elif score >= 15:
        min_mom = max(2, min_mom - 2)
    elif score >= 13:
        min_mom = max(2, min_mom - 1)
    return min_score, min_diff, min_mom

def compute_confluence_score(i1,i5,i15,i1h,i4h,fg,sess,adv,ob=None,liq=None,eth_bonus=0,eth_desc="",btc24=None,window_delta=0.0,window_delta_pct=0.0):
    up=0.0; dn=0.0; signals=[]

    # ✅ v10.20g — WINDOW DELTA: signal dominant (poids x6)
    if window_delta > 0:
        up += abs(window_delta)
        signals.append(f"📈 Window delta +{window_delta_pct:+.3f}% (score +{abs(window_delta):.0f})")
    elif window_delta < 0:
        dn += abs(window_delta)
        signals.append(f"📉 Window delta {window_delta_pct:+.3f}% (score +{abs(window_delta):.0f})")
    else:
        signals.append(f"↔️ Window delta ~0% (indécis)")

    if i5.get("ema_bull"): up+=1.0; signals.append("5m EMA ↑")
    else: dn+=1.0; signals.append("5m EMA ↓")
    if i1.get("ema_bull"): up+=0.5
    else: dn+=0.5

    if i15.get("ema_bull"): up+=1.0; signals.append("15m EMA ↑")
    else: dn+=1.0; signals.append("15m EMA ↓")

    if i1h.get("ema_bull"): up+=0.5; signals.append("1h EMA ↑")
    else: dn+=0.5; signals.append("1h EMA ↓")
    if i4h:
        if i4h.get("ema_bull"): up+=0.5; signals.append("4h EMA ↑")
        else: dn+=0.5; signals.append("4h EMA ↓")
    s9=i5.get("slope_e9",0)
    if s9>0.03: up+=1.0; signals.append(f"EMA slope ↑ ({s9:+.3f}%)")
    elif s9<-0.03: dn+=1.0; signals.append(f"EMA slope ↓ ({s9:+.3f}%)")
    if i15.get("macd_hist",0)>0: up+=1.5; signals.append("MACD 15m +")
    elif i15.get("macd_hist",0)<0: dn+=1.5; signals.append("MACD 15m -")
    if i5.get("macd_hist",0)>0: up+=1.0
    elif i5.get("macd_hist",0)<0: dn+=1.0
    if i5.get("macd_cross"):
        ml=i5.get("macd_line",0)
        if ml>0: up+=1.5; signals.append("⚡ MACD cross ↑")
        else: dn+=1.5; signals.append("⚡ MACD cross ↓")
    r5=i5.get("rsi_14",50); r15=i15.get("rsi_14",50)
    if r5<25: up+=2.5; signals.append(f"RSI survendu extrême ({r5})")
    elif r5<35: up+=1.5; signals.append(f"RSI survendu ({r5})")
    elif r5>75: dn+=2.5; signals.append(f"RSI suracheté extrême ({r5})")
    elif r5>65: dn+=1.5; signals.append(f"RSI suracheté ({r5})")
    elif r5<45: up+=0.5
    elif r5>55: dn+=0.5
    if r15<40: up+=0.5
    elif r15>60: dn+=0.5
    if i5.get("above_vwap"): up+=1.0; signals.append("Prix > VWAP")
    else: dn+=1.0; signals.append("Prix < VWAP")
    if i15.get("above_vwap"): up+=0.5
    else: dn+=0.5
    sk=i5.get("stoch_k",50)
    if sk<15: up+=1.5; signals.append(f"Stoch survendu ({sk})")
    elif sk<25: up+=0.8
    elif sk>85: dn+=1.5; signals.append(f"Stoch suracheté ({sk})")
    elif sk>75: dn+=0.8
    adv_s=adv.get("score",0)
    if adv_s>0: up+=min(adv_s*1.5,5); signals.extend(adv.get("signals",[]))
    elif adv_s<0: dn+=min(abs(adv_s)*1.5,5); signals.extend(adv.get("signals",[]))
    if i5.get("vol_spike"):
        if up>dn: up+=1.5; signals.append("🔥 Volume spike UP")
        else: dn+=1.5; signals.append("🔥 Volume spike DOWN")
    sb=sess.get("score_bonus",0)
    if sb>0:
        if up>dn: up+=sb
        else: dn+=sb
    fgv=fg.get("value",50)
    if fgv<15: up+=1.0; signals.append(f"F&G peur extrême ({fgv})")
    elif fgv>85: dn+=1.0; signals.append(f"F&G greed extrême ({fgv})")
    # ✅ v10.15 — Filtre tendance BTC 24h
    btc_change=btc24.get("change_pct",0) if btc24 else 0
    if btc_change < -3.0: dn+=2.0; signals.append(f"⚠️ BTC {btc_change:.1f}% tendance baissière forte")
    elif btc_change > 3.0: up+=2.0; signals.append(f"⚠️ BTC +{btc_change:.1f}% tendance haussière forte")
    if i5.get("bb_squeeze"):
        signals.append("⚡ Squeeze BB")
        if up>dn: up+=0.5
        else: dn+=0.5
    if i5.get("consolidation"):
        up*=0.8; dn*=0.8; signals.append("⚠️ Consolidation")
    if ob and ob.get("bias"):
        if ob["bias"]=="UP": up+=1.5; signals.append(ob["desc"])
        elif ob["bias"]=="DOWN": dn+=1.5; signals.append(ob["desc"])
    if liq and liq.get("bias"):
        if liq["bias"]=="UP": up+=2.0; signals.append(liq["desc"])
        elif liq["bias"]=="DOWN": dn+=2.0; signals.append(liq["desc"])
    if eth_bonus!=0:
        if eth_bonus>0:
            if up>dn: up+=eth_bonus
            else: dn+=eth_bonus
        else:
            if up>dn: up+=eth_bonus
            else: dn+=eth_bonus
        if eth_desc: signals.append(eth_desc)
    direction="UP" if up>=dn else "DOWN"
    score=round(up if up>=dn else dn,1); diff=round(abs(up-dn),1)
    direction_tmp="UP" if up>=dn else "DOWN"
    score_tmp=round(up if up>=dn else dn,1)
    # ✅ v10.20 — Probabilité implicite calculée
    total_score = up + dn
    prob_up = round(up/total_score, 3) if total_score > 0 else 0.5
    prob_dn = round(dn/total_score, 3) if total_score > 0 else 0.5
    min_score,min_diff,min_mom=get_session_thresholds(sess.get("session","OVERNIGHT"), score_tmp)
    return {"score_up":round(up,1),"score_dn":round(dn,1),"score":score,"diff":diff,
            "direction":direction,"signals":signals[:10],"min_score":min_score,
            "min_diff":min_diff,"min_mom":min_mom,
            "tradeable":score>=min_score and diff>=min_diff,
            "prob_up":prob_up,"prob_dn":prob_dn}

def compute_momentum_score(i1,i5,i15):
    score=0.0; r5=i5.get("rsi_14",50)
    if r5<25 or r5>75: score+=3.0
    elif r5<35 or r5>65: score+=1.5
    elif r5<40 or r5>60: score+=0.5
    s9=abs(i5.get("slope_e9",0))
    if s9>0.05: score+=2.0
    elif s9>0.02: score+=1.0
    if abs(i5.get("slope_e21",0))>0.03: score+=1.0
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
    if sum(1 for t in losses if t.get("score",0)<9)>=2: patterns.append("⚠️ Pertes sur score <9")
    up_l=sum(1 for t in losses if t["dir"]=="UP"); dn_l=sum(1 for t in losses if t["dir"]=="DOWN")
    if up_l>dn_l*2: patterns.append(f"⚠️ Trop pertes UP ({up_l})")
    elif dn_l>up_l*2: patterns.append(f"⚠️ Trop pertes DOWN ({dn_l})")
    return "\n".join(patterns) if patterns else f"{len(losses)} perte(s) sans pattern."

def recent_same_setup_loss(trades,direction,lookback=3):
    recent=trades[-lookback:] if len(trades)>=lookback else trades
    return sum(1 for t in recent if t["dir"]==direction and t["result"]=="LOSS")>=1

def trades_last_hour(trades):
    now=time.time(); return sum(1 for t in trades if now-t.get("ts",0)<3600)

def pattern_mem(trades):
    """✅ v10.18 — Mémoire patterns par direction ET par session"""
    if len(trades)<5: return "Moins de 5 trades."
    up_t=[t for t in trades if t["dir"]=="UP"]; dn_t=[t for t in trades if t["dir"]=="DOWN"]
    up_wr=sum(1 for t in up_t if t["result"]=="WIN")/len(up_t)*100 if up_t else 0
    dn_wr=sum(1 for t in dn_t if t["result"]=="WIN")/len(dn_t)*100 if dn_t else 0
    recent=trades[-30:]
    sessions={}
    for t in recent:
        s=t.get("session","?")
        if s not in sessions: sessions[s]={"w":0,"l":0}
        if t["result"]=="WIN": sessions[s]["w"]+=1
        else: sessions[s]["l"]+=1
    best_sess=worst_sess=""
    best_wr=0; worst_wr=100
    for s,v in sessions.items():
        total=v["w"]+v["l"]
        if total>=2:
            wr=v["w"]/total*100
            if wr>best_wr: best_wr=wr; best_sess=s
            if wr<worst_wr: worst_wr=wr; worst_sess=s
    sess_info=""
    if best_sess: sess_info=f" | Best:{best_sess}({best_wr:.0f}%)"
    if worst_sess and worst_sess!=best_sess: sess_info+=f" Worst:{worst_sess}({worst_wr:.0f}%)"
    return f"UP:{up_wr:.0f}%({len(up_t)}) DOWN:{dn_wr:.0f}%({len(dn_t)}){sess_info}"

def is_trending(c5,c15):
    if len(c5)<12: return False
    h=(datetime.utcnow().hour+2)%24
    # ✅ v10.24 — Seuil relevé 0.05%→0.10% (évite les entrées sur bruit de marché plat)
    thr=0.15 if (22<=h or h<7) else 0.10
    closes=[c["close"] for c in c5[-12:]]; highs=[c["high"] for c in c5[-6:]]
    lows=[c["low"] for c in c5[-6:]]; price=closes[-1] if closes[-1] else 1
    return (max(highs)-min(lows))/price*100>thr or abs(closes[-1]-closes[0])/price*100>thr*0.7

def wr_by_session(trades, days=7):
    """WR par session sur les N derniers jours"""
    cutoff=time.time()-days*86400
    recent=[t for t in trades if t.get("ts",0)>=cutoff]
    sessions={}
    for t in recent:
        s=t.get("session","?")
        if s not in sessions: sessions[s]={"w":0,"l":0,"pnl":0}
        if t["result"]=="WIN": sessions[s]["w"]+=1
        else: sessions[s]["l"]+=1
        sessions[s]["pnl"]+=t["pnl"]
    return sessions

def wr_by_hour(trades, days=30):
    """✅ v10.20b — WR par heure Paris sur les N derniers jours"""
    cutoff=time.time()-days*86400
    recent=[t for t in trades if t.get("ts",0)>=cutoff]
    hours={}
    for t in recent:
        h=(datetime.fromtimestamp(t["ts"]).hour+2)%24
        if h not in hours: hours[h]={"w":0,"l":0}
        if t["result"]=="WIN": hours[h]["w"]+=1
        else: hours[h]["l"]+=1
    best_h=worst_h=None; best_wr=0; worst_wr=100
    for h,v in hours.items():
        total=v["w"]+v["l"]
        if total>=3:
            wr=v["w"]/total*100
            if wr>best_wr: best_wr=wr; best_h=h
            if wr<worst_wr: worst_wr=wr; worst_h=h
    return hours, best_h, worst_h, best_wr, worst_wr

async def fetch_clob_balance():
    """✅ v10.15c — Lit le solde réel depuis Polymarket CLOB V2"""
    if not poly.ready or poly.client_version != "v2":
        return None
    try:
        from py_clob_client_v2 import BalanceAllowanceParams
        from py_clob_client_v2.clob_types import AssetType
        resp = poly.client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        if resp:
            bal = resp.get("balance", resp.get("amount", None))
            if bal is not None:
                return round(float(bal) / 1e6, 2)
    except Exception as e:
        log.warning(f"fetch_clob_balance: {e}")
    return None

async def fetch_price():
    sources=[("Kraken","https://api.kraken.com/0/public/Ticker?pair=XBTUSD",lambda d:float(d["result"]["XXBTZUSD"]["c"][0])),
             ("Binance","https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",lambda d:float(d["price"]))]
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
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval={interval}&limit={limit}",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status==200:
                    data=await r.json()
                    if isinstance(data,list) and len(data)>5:
                        return [{"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                                 "close":float(k[4]),"vol":float(k[5]),"ts":int(k[0])//1000} for k in data]
    except: pass
    try:
        km={"1m":1,"5m":5,"15m":15,"1h":60,"4h":240}
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval={km.get(interval,5)}&count={limit}",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
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

async def fetch_btc_news():
    """✅ v10.18 — News BTC en temps réel via CryptoPanic"""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://cryptopanic.com/api/free/v1/posts/",
                params={"auth_token":"free","currencies":"BTC","filter":"hot","public":"true"},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    results = data.get("results", [])
                    if not results:
                        return {"sentiment": "neutral", "score": 0, "news": []}
                    positive_words = ["bull", "surge", "rally", "pump", "ath", "break", "high", "gain", "up", "buy"]
                    negative_words = ["bear", "crash", "dump", "fall", "low", "drop", "down", "sell", "fear", "ban"]
                    pos = neg = 0
                    recent_news = []
                    for item in results[:5]:
                        title = item.get("title", "").lower()
                        votes = item.get("votes", {})
                        bullish = votes.get("positive", 0)
                        bearish = votes.get("negative", 0)
                        pos += bullish
                        neg += bearish
                        for w in positive_words:
                            if w in title: pos += 2
                        for w in negative_words:
                            if w in title: neg += 2
                        recent_news.append(item.get("title", "")[:60])
                    total = pos + neg
                    if total == 0:
                        sentiment = "neutral"
                        score = 0
                    elif pos > neg * 1.5:
                        sentiment = "bullish"
                        score = min(3, round((pos - neg) / max(total, 1) * 5, 1))
                    elif neg > pos * 1.5:
                        sentiment = "bearish"
                        score = -min(3, round((neg - pos) / max(total, 1) * 5, 1))
                    else:
                        sentiment = "neutral"
                        score = 0
                    return {"sentiment": sentiment, "score": score, "news": recent_news[:3]}
    except Exception as e:
        log.warning(f"fetch_btc_news: {e}")
    return {"sentiment": "neutral", "score": 0, "news": []}

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

async def claude_decide(i1,i5,i15,i1h,i4h,adv,trades,bankroll,consec,fg,btc24,sess,conf_score,mom_score,tpu,tpd,ob=None,liq=None,eth_desc=""):
    """
    ✅ v10.22 — Claude n'est PLUS appelé dans le chemin chaud (job_tick/job_snipe).
    Latence 10-25s = prix d'entrée périmé sur un marché 5min.
    Reste utilisé uniquement par /signal pour l'analyse manuelle détaillée.
    """
    if not ANTHROPIC_KEY: return {"dir":None,"conf":0,"size":0,"reasoning":"Pas de clé API.","trade":False}
    loss_analysis=analyze_losses(trades); patterns=pattern_mem(trades)
    same_up=recent_same_setup_loss(trades,"UP"); same_dn=recent_same_setup_loss(trades,"DOWN")
    trades_txt="".join(f"  {'✅' if t['result']=='WIN' else '❌'} {t['dir']} PnL:{t['pnl']:+.2f}$ score:{t.get('score',0)}\n" for t in trades[-6:]) or "  Aucun.\n"
    sigs_txt="\n".join(f"  ✓ {s}" for s in conf_score["signals"]) or "  Aucun"
    ppu=round(1/tpu,2) if tpu>0 else 2.0; ppd=round(1/tpd,2) if tpd>0 else 2.0
    kelly_up=kelly_bet(bankroll,0.6,ppu); kelly_dn=kelly_bet(bankroll,0.6,ppd)
    i4h_txt=f"4h RSI:{i4h.get('rsi_14',50)} EMA:{'↑' if i4h.get('ema_bull') else '↓'}" if i4h else ""
    h_paris=(datetime.utcnow().hour+2)%24
    min_score,min_diff,min_mom=get_session_thresholds(sess.get("session","OVERNIGHT"))
    ob_txt=ob["desc"] if ob else "OB N/A"
    liq_txt=liq["desc"] if liq else "Liq N/A"
    news_data=st.last_news if hasattr(st,'last_news') else {"sentiment":"neutral","score":0,"news":[]}
    news_txt=f"News:{news_data['sentiment']}(score:{news_data['score']:+.1f})" if news_data['news'] else "News:N/A"
    if news_data['news']: news_txt+=f" [{news_data['news'][0][:40]}...]"
    prompt=f"""Expert trading binaire BTC UP/DOWN 5min Polymarket. Bets RÉELS.
BTC:${i5.get('price',0):,.2f} | 24h:{btc24.get('change_pct',0):+.2f}% | F&G:{fg['value']}/100 | {sess['session']} {h_paris}h | {news_txt}
UP:{tpu:.3f}$→x{ppu}(Kelly≈{kelly_up:.2f}$) | DOWN:{tpd:.3f}$→x{ppd}(Kelly≈{kelly_dn:.2f}$)
Score:{conf_score['direction']} {conf_score['score']:.1f}/{min_score} Diff:{conf_score['diff']}/{min_diff} Tradeable:{'OUI' if conf_score['tradeable'] else 'NON'}
EdgeUP:{round((conf_score.get('prob_up',0.5)-tpu)*100,1)}% EdgeDN:{round((conf_score.get('prob_dn',0.5)-tpd)*100,1)}%
Mom:{mom_score}/10(seuil:{min_mom}) | ETH:{eth_desc} | {ob_txt} | {liq_txt}
Signaux:{sigs_txt}
5m RSI:{i5.get('rsi_14',50)} MACD:{i5.get('macd_hist',0):+.4f} Stoch:{i5.get('stoch_k',50)} Vol:x{i5.get('vol_ratio',1):.1f}
15m RSI:{i15.get('rsi_14',50)} EMA:{'↑' if i15.get('ema_bull') else '↓'} | 1h:{'↑' if i1h.get('ema_bull') else '↓'} | {i4h_txt}
{patterns} | {loss_analysis}
{trades_txt}Consec:{consec} | BR:{bankroll:.2f}$
RÈGLES STRICTES ET NON NÉGOCIABLES:
✅ TRADER OBLIGATOIREMENT si: tradeable=OUI ET mom≥{min_mom} ET 1.3≤payout≤5.0
❌ PASSER UNIQUEMENT si: tradeable=NON OU mom<{min_mom} OU payout<1.3 OU payout>5.0
🚫 INTERDIT de trader si payout>5.0 (token<0.20$) = marché pense >80% que tu perds
🚫 INTERDIT d'inventer des raisons supplémentaires
⚠️ mom={min_mom} exactement = VALIDE sans exception
⚠️ Si les 3 conditions ✅ sont remplies → trade=true OBLIGATOIRE
JSON:{{"trade":true/false,"direction":"UP"/"DOWN"/null,"confidence":0.0-1.0,"bet_size":{MIN_BET_USD}-{MAX_BET_USD},"reasoning":"2 phrases FR","risk_level":"LOW"/"MEDIUM"/"HIGH"}}"""
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
                conf=sf(res.get("confidence"),0.0)
                payout=ppu if direction=="UP" else ppd if direction=="DOWN" else 2.0
                kelly_size=kelly_bet(bankroll,conf,payout)
                return {"dir":direction,"conf":conf,"size":kelly_size,
                        "reasoning":str(res.get("reasoning","")),"risk":res.get("risk_level","MEDIUM"),
                        "trade":bool(res.get("trade",False)) and direction is not None,
                        "kelly_pct":round(kelly_size/bankroll*100,1) if bankroll>0 else 0}
    except Exception as e:
        log.error(f"Claude: {e}")
        return {"dir":None,"conf":0,"size":0,"reasoning":f"Erreur:{str(e)[:60]}","trade":False}

# ─── STATE ─────────────────────────────────────────────────────────────────
class State:
    def __init__(self):
        self.running=False; self.paper_mode=PAPER_MODE
        self.bankroll=50.0; self.bankroll_ref=50.0
        self.c1=deque(maxlen=100); self.c5=deque(maxlen=100); self.c15=deque(maxlen=100)
        self.c1h=deque(maxlen=100); self.c4h=deque(maxlen=50)
        self.price=0.0; self.trades=[]; self.bet=None
        self.wins=self.losses=0; self.pnl=0.0; self.consec=0
        self.streak=self.best_streak=self.worst_streak=0
        self.cooldown_until=0; self.session_start=time.time()
        self.daily_start=50.0; self.daily_ts=time.time()
        self.daily_pause_until=0
        self.skipped=0; self.pass_reasons=[]
        # ✅ v10.37 — Auto-apprentissage
        self.oracle_patterns=[]          # [{gap,delta,ret3s,votes,dir,result,ts}]
        self.calibration_log=[]          # historique des ajustements auto
        self.haiku_insights=[]           # insights Claude Haiku horaires
        self.last_haiku_ts=0
        self.turbo_until=0
        self.conservative_until=0
        self.win_streak_count=0
        self.window_delta_pct=0.0
        self.window_delta=0.0
        # ✅ v10.21 — WebSocket Binance temps réel
        self.ws_prices=deque(); self.ws_volumes=deque()  # ✅ v12.3
        self.ws_price=0.0
        self.ws_connected=False
        self.ws_task=None
        self.slot_open_price=0.0
        self.slot_open_ts=0
        self.last_fair={}
        self.last_decision={}; self.last_conf_score={}; self.last_mom_score=0
        self.fg={"value":50,"label":"Neutral"}; self.btc24={}
        self.tick_job=self.price_job=self.macro_job=self.tp_job=self.backup_job=self.recap_job=None
        self.snipe_job=None  # ✅ v10.22
        self.current_market=None; self.active_order_id=None; self.active_token_id=None
        self.entry_token_price=0.0; self.shares_bought=0.0
        self.token_price_peak=0.0; self.trailing_active=False
        # ✅ v11.9m — Orderbook imbalance (Strategy 2)
        self.ob_imbalance=0.0; self.ob_ts=0.0; self.ob_asset_id=""; self.clob_ws_task=None
        # ✅ v12.3 — OB ETH/SOL
        self.eth_ob_imbalance=0.0; self.eth_ob_ts=0.0; self.eth_ob_asset_id=""; self.eth_clob_ws_task=None
        self.sol_ob_imbalance=0.0; self.sol_ob_ts=0.0; self.sol_ob_asset_id=""; self.sol_clob_ws_task=None
        # ✅ v12.2 — Multi-asset: ETH + SOL
        # ETH
        self.eth_price=0.0; self.eth_ts=0; self.eth_ws_task=None
        self.eth_ws_prices=deque()  # historique prix ETH pour ret_over
        self.eth_oracle_price=0.0; self.eth_oracle_ts=0.0
        self.eth_oracle_slot_open=0.0; self.eth_oracle_slot_ts=0
        self.eth_last_trade_slot=0
        # SOL
        self.sol_price=0.0; self.sol_ts=0; self.sol_ws_task=None
        self.sol_ws_prices=deque()  # historique prix SOL pour ret_over
        self.sol_oracle_price=0.0; self.sol_oracle_ts=0.0
        self.sol_oracle_slot_open=0.0; self.sol_oracle_slot_ts=0
        self.sol_last_trade_slot=0
        self.bet_expiry=0
        self.last_ob=None; self.last_liq=None; self.last_eth_klines=[]
        self.last_news={"sentiment":"neutral","score":0,"news":[]}
        self.price_history=[]
        # ✅ v10.23 — Multi-exchange WS (Coinbase + Kraken en plus de Binance)
        self.cb_price=0.0; self.kr_price=0.0; self.bs_price=0.0
        self.cb_ts=0; self.kr_ts=0; self.bs_ts=0
        self.cb_task=None; self.kr_task=None; self.bs_task=None  # ✅ v11.9k Bitstamp
        # ✅ v10.23 — Oracle Chainlink (le feed qui RÈGLE le marché)
        self.oracle_price=0.0; self.oracle_ts=0
        self.oracle_slot_open=0.0; self.oracle_slot_ts=0
        self.oracle_task=None; self.oracle_connected=False
        self.eth_oracle_task=None; self.sol_oracle_task=None  # ✅ v11.10y
        self.oracle_chainlink_ts=0.0  # ✅ v12.3 — timestamp Chainlink réel
        self.oracle_lag_signal=None  # {"bias","desc","div_pct"}
        # ✅ v10.23 — Calibration sigma
        self.calib_factor=1.0  # Multiplie VOL_SAFETY (1.0 = pas de correction)
        # ✅ v10.23 — Kill switch
        self.killed=False
        self.last_trade_slot=0  # ✅ v10.23 dédup: 1 seul trade par slot 5min

    def save(self):
        # ✅ v10.19 — Export CSV des trades
        try:
            import csv
            csv_path = "polybot_trades.csv"
            if self.trades:
                fieldnames = ["ts","dir","amount","pnl","result","entry","exit","score","session","conf","paper"]
                write_header = not os.path.exists(csv_path)
                with open(csv_path, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if write_header:
                        writer.writeheader()
                    for t in self.trades[-5:]:
                        writer.writerow({k: t.get(k,"") for k in fieldnames})
        except Exception as e:
            log.warning(f"CSV export: {e}")
        data={"bankroll":self.bankroll,"bankroll_ref":self.bankroll_ref,
            "trades":self.trades,
            "wins":self.wins,"losses":self.losses,"pnl":self.pnl,
            "best_streak":self.best_streak,"worst_streak":self.worst_streak,"consec":self.consec,
            "daily_start":self.daily_start,"daily_ts":self.daily_ts,
            "daily_pause_until":self.daily_pause_until,"paper_mode":self.paper_mode,
            "skipped":self.skipped,
            "calib_factor":self.calib_factor,"killed":self.killed,
            "version":BOT_VERSION,"saved_at":int(time.time()),
            "delta_contra_max":ORACLE_DELTA_CONTRA_MAX,
            "oracle_patterns":self.oracle_patterns,
            "pass_reasons":self.pass_reasons[-500:],
            "calibration_log":self.calibration_log,
            "haiku_insights":self.haiku_insights[-50:]}
        try:
            with open(DATA_FILE,"w") as f: json.dump(data,f,indent=2)
        except Exception as e: log.error(f"Save: {e}")
        return data

    def backup(self):
        try:
            data=self.save()
            with open(BACKUP_FILE,"w") as f: json.dump(data,f,indent=2)
            log.info(f"✅ Backup BR:{self.bankroll:.2f}")
            # ✅ v11.10i — GitHub push déplacé dans job_backup (async, plus stable)
            return True
        except Exception as e: log.error(f"Backup: {e}"); return False

    def load(self):
        for filepath in [DATA_FILE,BACKUP_FILE]:
            try:
                if os.path.exists(filepath):
                    with open(filepath) as f: d=json.load(f)
                    self.bankroll=d.get("bankroll",50.0)
                    self.bankroll_ref=d.get("bankroll_ref",self.bankroll)
                    self.trades=d.get("trades",[]); self.wins=d.get("wins",0)
                    self.losses=d.get("losses",0); self.pnl=d.get("pnl",0.0)
                    self.best_streak=d.get("best_streak",0); self.worst_streak=d.get("worst_streak",0)
                    self.consec=d.get("consec",0); self.daily_start=d.get("daily_start",self.bankroll)
                    self.daily_ts=d.get("daily_ts",time.time())
                    self.daily_pause_until=d.get("daily_pause_until",0)
                    self.paper_mode=d.get("paper_mode",PAPER_MODE)
                    self.skipped=d.get("skipped",0); self.pass_reasons=d.get("pass_reasons",[])
                    self.calib_factor=d.get("calib_factor",1.0); self.killed=d.get("killed",False)
                    # ✅ v11.10q — Charger patterns + calibrations depuis JSON
                    self.oracle_patterns=d.get("oracle_patterns",[])
                    # ✅ v11.10v — Restaurer les seuils auto-calibrés
                    global ORACLE_DELTA_CONTRA_MAX
                    saved_dcm = d.get("delta_contra_max", 0)
                    if saved_dcm > 0 and saved_dcm < 0.050:
                        ORACLE_DELTA_CONTRA_MAX = saved_dcm
                        log.info(f"✅ delta_contra_max restauré: {saved_dcm:.3f}%")
                    self.calibration_log=d.get("calibration_log",[])
                    self.haiku_insights=d.get("haiku_insights",[])
                    age=int((time.time()-d.get("saved_at",0))/60)
                    log.info(f"✅ State {filepath} ({age}min) BR:{self.bankroll:.2f} patterns:{len(self.oracle_patterns)}"); return
            except Exception as e: log.error(f"Load {filepath}: {e}")

st=State()

async def init_state_from_github():
    """✅ v11.10m — Appelé au premier /run pour charger le state depuis GitHub."""
    pulled = await pull_state_from_github()
    if pulled:
        st.load()
        log.info(f"✅ State restauré depuis GitHub: BR={st.bankroll:.2f}$")


# ─── HELPERS v10.22 ────────────────────────────────────────────────────────
def log_skip(reason, direction=None, features=None):
    """✅ v10.37 — Log skip + features oracle pour auto-calibration."""
    st.skipped += 1
    now = int(time.time())
    entry = {"ts": now, "reason": reason, "dir": direction,
             "slot_end": (now // 300) * 300 + 300,
             "open_px": st.slot_open_price if st.slot_open_price > 0 else st.price,
             "resolved": None}
    st.pass_reasons.append(entry)
    if features and direction:
        # ✅ v12.2 — Détecter l'asset depuis le reason ou features
        asset_tag = "BTC"
        if reason.startswith("ETH:") or reason.startswith("[ETH]") or "ETH:" in reason[:5]:
            asset_tag = "ETH"
        elif reason.startswith("SOL:") or reason.startswith("[SOL]") or "SOL:" in reason[:5]:
            asset_tag = "SOL"
        st.oracle_patterns.append({**features, "direction": direction,
                                    "result": None, "ts": now, "slot_end": entry["slot_end"],
                                    "open_px": entry["open_px"], "v": BOT_VERSION,
                                    "asset": asset_tag})

def live_window_delta():
    """✅ v10.22 — Delta du slot en TEMPS RÉEL (WS prioritaire, fallback dernier tick)"""
    cur_slot = int(time.time() // 300) * 300
    if st.ws_price > 0 and st.slot_open_price > 0 and st.slot_open_ts == cur_slot:
        pct = (st.ws_price - st.slot_open_price) / st.slot_open_price * 100
        return delta_to_weight(pct), pct
    return st.window_delta, st.window_delta_pct

def roi():
    if st.bankroll_ref<=0: return "+0.00%"
    pct=(st.bankroll-st.bankroll_ref)/st.bankroll_ref*100
    return f"+{pct:.2f}%" if pct>=0 else f"{pct:.2f}%"

def fmt(v): return f"+{v:.2f}" if v>=0 else f"{v:.2f}"
def wr():
    t=st.wins+st.losses; return f"{st.wins/t*100:.1f}%" if t else "—"
def upt():
    s=int(time.time()-st.session_start); return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

def check_daily():
    now=time.time()
    if now-st.daily_ts>86400:
        st.daily_start=st.bankroll; st.daily_ts=now; st.daily_pause_until=0; return False
    if st.daily_pause_until>0 and now<st.daily_pause_until: return True
    if st.daily_pause_until>0 and now>=st.daily_pause_until:
        st.daily_pause_until=0; st.daily_start=st.bankroll; return False
    if st.daily_start>0 and (st.daily_start-st.bankroll)/st.daily_start>=DAILY_LOSS_MAX:
        st.daily_pause_until=now+(DAILY_PAUSE_H*3600); return True
    return False

def in_cd(): return time.time()<st.cooldown_until

def register_trade_result(won):
    """✅ v10.22 — Centralise streaks/conservateur/boost (paper ET réel)"""
    if won:
        st.wins+=1; st.consec=0
        st.streak=st.streak+1 if st.streak>=0 else 1
        st.best_streak=max(st.best_streak,st.streak)
        st.win_streak_count+=1
    else:
        st.losses+=1; st.consec+=1
        st.streak=st.streak-1 if st.streak<=0 else -1
        st.worst_streak=min(st.worst_streak,st.streak)
        st.win_streak_count=0
        if st.consec>=MAX_CONSEC_LOSS: st.cooldown_until=time.time()+COOLDOWN_MIN*60
        # ✅ v11.10u — mode conservateur SUPPRIMÉ définitivement
        if st.consec>=KILL_SWITCH_LOSSES:  # ✅ v10.23 — arrêt total
            st.killed=True; st.running=False

async def send(bot,text,parse_mode="Markdown"):
    try: await bot.send_message(chat_id=ALLOWED_UID,text=text,parse_mode=parse_mode); return True
    except Exception as e:
        log.error(f"Send: {e}")
        try: await bot.send_message(chat_id=ALLOWED_UID,text=text.replace("*","").replace("`","").replace("_","")); return True
        except: return False

# ─── JOBS ──────────────────────────────────────────────────────────────────
async def job_backup(context):
    # ✅ v10.23 — Auto-calibration sigma à chaque backup
    factor, _ = calibrate_sigma()
    st.backup()
    # ✅ v11.10i — Push GitHub depuis contexte async (stable)
    try:
        gh_token = os.getenv("GITHUB_TOKEN","")
        gh_repo = os.getenv("GITHUB_REPO","")
        if gh_token and gh_repo:
            await push_state_to_github()
    except Exception as e:
        log.warning(f"job_backup github: {e}")

async def job_sync_balance(context):
    """✅ v11.9 — Sync auto BR avec solde CLOB réel toutes les 30min (h24 sans intervention)"""
    if st.paper_mode or not poly.ready or st.bet: return  # pas pendant un trade
    try:
        clob_bal = await fetch_clob_balance()
        if clob_bal and clob_bal > 0 and abs(clob_bal - st.bankroll) > 0.10:
            old_br = st.bankroll
            st.bankroll = round(clob_bal, 2)
            log.info(f"Sync BR: {old_br:.2f}$ → {clob_bal:.2f}$ (CLOB réel)")
    except Exception as e:
        log.warning(f"job_sync_balance: {e}")

async def job_daily_recap(context):
    """✅ v10.16 — Résumé 22h + rapport hebdo dimanche + alerte bot arrêté"""
    h_paris=(datetime.utcnow().hour+2)%24
    if _last_tick_ts > 0 and (time.time() - _last_tick_ts) > 600:
        await send(context.bot, f"⚠️ *Alerte* — Dernier tick il y a `{int((time.time()-_last_tick_ts)/60)}min`. Bot potentiellement bloqué!")
    if h_paris!=22: return
    now=time.time(); cutoff=now-86400
    trades_24h=[t for t in st.trades if t.get("ts",0)>=cutoff]
    if not trades_24h:
        is_sunday = datetime.utcnow().weekday() == 6
        if is_sunday:
            trades_7d = [t for t in st.trades if t.get("ts",0) >= time.time()-7*86400]
            wins_7d = [t for t in trades_7d if t["result"]=="WIN"]
            pnl_7d = sum(t["pnl"] for t in trades_7d)
            wr_7d = len(wins_7d)/len(trades_7d)*100 if trades_7d else 0
            await send(context.bot,
                f"📅 *BILAN HEBDOMADAIRE*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Trades:`{len(trades_7d)}` | WR:`{wr_7d:.1f}%` | PnL:`{fmt(pnl_7d)}$`\n"
                f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`")
        else:
            await send(context.bot,f"📊 *Récap 22h* — Aucun trade aujourd'hui.\nBR:`{st.bankroll:.2f}$` | ROI:`{roi()}`")
        return
    wins=[t for t in trades_24h if t["result"]=="WIN"]
    losses=[t for t in trades_24h if t["result"]=="LOSS"]
    pnl_24h=sum(t["pnl"] for t in trades_24h)
    wr_24h=len(wins)/len(trades_24h)*100
    sessions_wr=wr_by_session(trades_24h,1)
    best_sess=max(sessions_wr.items(),key=lambda x:x[1]["w"]/(x[1]["w"]+x[1]["l"]) if (x[1]["w"]+x[1]["l"])>0 else 0)[0] if sessions_wr else "?"
    await send(context.bot,
        f"📊 *RÉCAP JOURNALIER 22h*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Trades:`{len(trades_24h)}` (✅{len(wins)} ❌{len(losses)})\n"
        f"WR:`{wr_24h:.1f}%` | PnL:`{fmt(pnl_24h)}$`\n"
        f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`\n"
        f"Meilleure session: `{best_sess}`\n\n"
        f"_Bot continue demain — bonne nuit 🌙_")

async def job_check_expiry(context):
    """✅ v10.18b — Alerte + clôture automatique quand slot expiré"""
    if not st.bet or st.paper_mode: return
    now = time.time()

    if st.bet_expiry > 0:
        remaining = st.bet_expiry - now
        if 50 <= remaining <= 70:
            current_price = await poly.get_token_price(st.active_token_id) if st.active_token_id else 0
            gain_mult = current_price/st.entry_token_price if st.entry_token_price>0 and current_price>0 else 0
            await send(context.bot,
                f"⏰ *Position expire dans ~1min*\n"
                f"`{st.bet['dir']}` | Token:`{current_price:.3f}$` | x`{gain_mult:.2f}`\n"
                f"BTC:`${st.price:,.2f}`")

        # ✅ Clôture automatique 60s après expiration
        if remaining < -60:
            log.info("Slot expiré depuis >60s — clôture automatique")
            # ✅ v11.10d — attendre settlement Polygon avant lecture solde réel
            await asyncio.sleep(5)
            prev_bal = st.bankroll
            clob_bal = None
            for _i in range(4):
                clob_bal = await fetch_clob_balance()
                if clob_bal and clob_bal > 0: break
                await asyncio.sleep(3)
            bet = st.bet
            if clob_bal and clob_bal > 0:
                gross = round(clob_bal - prev_bal, 2)
                won = gross >= -0.05
                st.bankroll = clob_bal
            else:
                gross = 0.0; won = False
            st.pnl += gross
            register_trade_result(won)  # ✅ v10.22 — streaks + conservateur aussi en réel
            result_txt = "WIN" if won else "LOSS"
            if not won and st.consec >= CONSERVATIVE_AFTER_LOSSES:
                await send(context.bot, f"⚠️ *Mode conservateur activé 2h* — {st.consec} pertes consécutives")
            st.trades.append({"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
                "conf":bet["conf"],"result":result_txt,"entry":bet["entry"],"exit":st.price,
                "reasoning":"Résolution auto slot expiré","paper":False,"ts":int(now),
                "score":bet.get("score",0),"fg_value":st.fg.get("value",50),
                "session":bet.get("session","?"),"aligned_15h1h":True})
            st.bet=None; st.active_token_id=None; st.active_order_id=None
            st.shares_bought=0; st.entry_token_price=0
            st.token_price_peak=0; st.trailing_active=False; st.bet_expiry=0
            emoji="✅" if won else "❌"
            await send(context.bot,
                f"{emoji} *Trade résolu* (slot expiré)\n"
                f"`{bet['dir']}` | PnL:`{fmt(gross)}$`\n"
                f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`")
            st.backup()

async def job_take_profit(context):
    """✅ v10.16 — Vente anticipée si x2/x3/x4 avant résolution du slot"""
    if not st.bet or not st.active_token_id or st.paper_mode: return
    try:
        current_price = await poly.get_token_price(st.active_token_id)
        if current_price <= 0 or st.entry_token_price <= 0: return

        gain_mult = current_price / st.entry_token_price

        if gain_mult > st.token_price_peak:
            st.token_price_peak = gain_mult
            if gain_mult >= TRAILING_PEAK_MULT and not st.trailing_active:
                st.trailing_active = True
                await send(context.bot,
                    f"🎯 *Trailing stop activé* x`{gain_mult:.2f}`\n"
                    f"Vente auto si retombe sous x`{TRAILING_STOP_MULT:.1f}`")

        sell_reason = None
        sell_pct = 100

        # ✅ v10.24 — STOP LOSS réintroduit: si token perd >55% de l'entrée → vendre
        # (v10.21 l'avait supprimé car "panique sur micro-rebonds" — mais sans SL on rend 100% sur chaque perte)
        if gain_mult < STOP_LOSS_MULT:
            sell_reason = f"🛑 Stop loss x{gain_mult:.2f} (<{STOP_LOSS_MULT})"
            sell_pct = 100

        if current_price >= 0.95:
            sell_reason = f"✅ Résolution imminente (token={current_price:.2f}$)"
            sell_pct = 100
        elif gain_mult >= 4.0:
            sell_reason = f"🚀 x{gain_mult:.1f} — Take profit x4"
            sell_pct = 100
        elif gain_mult >= 3.0 and st.token_price_peak >= 3.0:
            sell_reason = f"💰 x{gain_mult:.1f} — Take profit x3"
            sell_pct = 80
        elif gain_mult >= 2.0:
            sell_reason = f"💰 x{gain_mult:.1f} — Take profit x2"
            sell_pct = 60
        elif gain_mult >= TAKE_PROFIT_MULT:
            sell_reason = f"Take profit x{gain_mult:.2f}"
            sell_pct = 100
        elif st.trailing_active and st.token_price_peak > 0:
            trail_threshold = max(TRAILING_STOP_MULT, st.token_price_peak * 0.87)
            if gain_mult < trail_threshold:
                sell_reason = f"Trailing stop (peak x{st.token_price_peak:.2f}→x{gain_mult:.2f})"
                sell_pct = 100

        if sell_reason:
            shares_to_sell = round(st.shares_bought * sell_pct / 100, 4)
            opp_token = None
            if st.current_market:
                opp_token = st.current_market.get("token_up") if st.bet.get("dir")=="DOWN" else st.current_market.get("token_down")
            result = await poly.sell_position(st.active_token_id, shares_to_sell, opp_token, current_price)
            if result:
                gross_est = round((current_price - st.entry_token_price) * shares_to_sell, 2)
                # ✅ v11.10d — attendre settlement Polygon (5s) pour vrai solde
                prev_bal = st.bankroll
                await asyncio.sleep(5)
                clob_bal = None
                for _i in range(4):
                    clob_bal = await fetch_clob_balance()
                    if clob_bal and clob_bal > 0: break
                    await asyncio.sleep(3)
                if clob_bal and clob_bal > 0:
                    gross = round(clob_bal - prev_bal, 2)
                    st.bankroll = clob_bal
                else:
                    gross = gross_est
                    st.bankroll = max(0.0, st.bankroll + gross)
                st.pnl += gross
                bet = st.bet

                if sell_pct == 100:
                    register_trade_result(True)
                    st.trades.append({"dir": bet["dir"], "amount": bet["amount"],
                        "pnl": round(gross, 4), "conf": bet["conf"], "result": "WIN",
                        "entry": bet["entry"], "exit": st.price, "reasoning": sell_reason,
                        "paper": False, "ts": int(time.time()), "score": bet.get("score", 0),
                        "fg_value": st.fg.get("value", 50), "aligned_15h1h": True,
                        "session": bet.get("session", "?")})
                    st.bet = None; st.active_token_id = None; st.active_order_id = None
                    st.shares_bought = 0; st.entry_token_price = 0
                    st.token_price_peak = 0; st.trailing_active = False; st.bet_expiry = 0
                else:
                    st.shares_bought = round(st.shares_bought - shares_to_sell, 4)
                    st.trailing_active = True

                await send(context.bot,
                    f"🎯 *VENTE {sell_pct}%* — {sell_reason}\n"
                    f"`{bet['dir']}` | `+{gross:.2f} USDC`\n"
                    f"BR:`{st.bankroll:.2f}` | ROI:`{roi()}`")
                st.backup()
    except Exception as e: log.error(f"job_take_profit: {e}")

# ═══════════ ✅ v10.21 — WEBSOCKET BINANCE + FAIR VALUE (modèle Brownien) ═══════════
async def ws_binance_loop():
    """Flux temps réel BTC via WebSocket Binance aggTrade (public, sans clé)"""
    url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    st.ws_connected = True
                    log.info("✅ WS Binance connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            p = float(d.get("p", 0))
                            q = float(d.get("q", 0))  # ✅ v12.3 — volume du trade
                            if p > 0:
                                now = time.time()
                                st.ws_price = p
                                st.ws_prices.append((now, p))
                                while st.ws_prices and now - st.ws_prices[0][0] > 120:
                                    st.ws_prices.popleft()
                                # ✅ v12.3 — Volume spike: tracker les volumes
                                if q > 0:
                                    st.ws_volumes.append((now, q))
                                    while st.ws_volumes and now - st.ws_volumes[0][0] > 60:
                                        st.ws_volumes.popleft()
                                slot_start = int(now // 300) * 300
                                if st.slot_open_ts != slot_start:
                                    st.slot_open_ts = slot_start
                                    st.slot_open_price = p
                                    log.info(f"📌 Slot open: ${p:,.2f}")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except Exception as e:
            log.warning(f"WS Binance déconnecté: {e}")
        st.ws_connected = False
        await asyncio.sleep(5)

async def job_ws_watchdog(context):
    """Garde le WebSocket en vie"""
    t = st.ws_task
    if t is None or t.done():
        st.ws_task = asyncio.create_task(ws_binance_loop())

# ═══════════ v10.23 — MULTI-EXCHANGE WS + ORACLE CHAINLINK ═══════════
async def ws_coinbase_loop():
    """Flux temps réel BTC via Coinbase (public, gratuit)"""
    url = "wss://ws-feed.exchange.coinbase.com"
    sub = {"type":"subscribe","product_ids":["BTC-USD"],"channels":["ticker"]}
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    await ws.send_json(sub)
                    log.info("✅ WS Coinbase connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if d.get("type")=="ticker" and d.get("price"):
                                st.cb_price=float(d["price"]); st.cb_ts=time.time()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except Exception as e:
            log.warning(f"WS Coinbase: {e}")
        await asyncio.sleep(5)

async def ws_kraken_loop():
    """Flux temps réel BTC via Kraken (public, gratuit)"""
    url = "wss://ws.kraken.com/v2"
    sub = {"method":"subscribe","params":{"channel":"ticker","symbol":["BTC/USD"]}}
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    await ws.send_json(sub)
                    log.info("✅ WS Kraken connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if d.get("channel")=="ticker" and d.get("data"):
                                px=d["data"][0].get("last")
                                if px: st.kr_price=float(px); st.kr_ts=time.time()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except Exception as e:
            log.warning(f"WS Kraken: {e}")
        await asyncio.sleep(5)

async def ws_bitstamp_loop():
    """✅ v11.9k — Flux temps réel BTC via Bitstamp (utilisé par PolyCryptoBot)"""
    url = "wss://ws.bitstamp.net"
    sub = {"event":"bts:subscribe","data":{"channel":"live_trades_btcusd"}}
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    await ws.send_json(sub)
                    log.info("✅ WS Bitstamp connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if d.get("event") == "trade" and d.get("data",{}).get("price"):
                                st.bs_price = float(d["data"]["price"])
                                st.bs_ts = time.time()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except Exception as e:
            log.warning(f"WS Bitstamp: {e}")
        await asyncio.sleep(5)


async def ws_clob_loop(asset_id_up: str):
    """
    ✅ v11.9m — Orderbook imbalance via CLOB WebSocket Polymarket.
    Source: benjamincup.substack.com — Strategy 2: 80-90% WR sur imbalance forte.
    Si côté UP a beaucoup plus d'acheteurs que de vendeurs → signal UP (smart money).
    """
    if not asset_id_up:
        return
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sub = {"assets_ids": [asset_id_up], "type": "market"}
    st.ob_asset_id = asset_id_up
    st.ob_imbalance = 0.0
    log.info(f"✅ WS CLOB OB démarré pour {asset_id_up[:12]}...")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(url, heartbeat=10, timeout=aiohttp.ClientTimeout(total=300)) as ws:
                await ws.send_json(sub)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            raw = json.loads(msg.data)
                            # Polymarket envoie soit une liste soit un dict
                            msgs = raw if isinstance(raw, list) else [raw]
                            for m in msgs:
                                if not isinstance(m, dict): continue
                                etype = m.get("event_type") or m.get("type", "")
                                if etype not in ("book", "price_change", "tick_size_change"):
                                    continue
                                bids = m.get("bids") or []
                                asks = m.get("asks") or []
                                if not isinstance(bids, list) or not isinstance(asks, list):
                                    continue
                                bid_vol = ask_vol = 0.0
                                for b in bids:
                                    try:
                                        if isinstance(b, dict): bid_vol += float(b.get("size", 0))
                                        elif isinstance(b, (list, tuple)) and len(b) >= 2: bid_vol += float(b[1])
                                    except: pass
                                for a in asks:
                                    try:
                                        if isinstance(a, dict): ask_vol += float(a.get("size", 0))
                                        elif isinstance(a, (list, tuple)) and len(a) >= 2: ask_vol += float(a[1])
                                    except: pass
                                total = bid_vol + ask_vol
                                if total > 0:
                                    st.ob_imbalance = round((bid_vol - ask_vol) / total, 3)
                                    st.ob_ts = time.time()
                        except Exception as pe:
                            log.debug(f"WS CLOB OB parse: {pe}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
    except Exception as e:
        log.warning(f"WS CLOB OB: {e}")
    st.ob_imbalance = 0.0


async def ws_clob_loop_asset(asset_id_up: str, asset: str):
    """✅ v12.3 — OB imbalance ETH/SOL via CLOB WS (même logique que BTC)"""
    if not asset_id_up: return
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sub = {"assets_ids": [asset_id_up], "type": "market"}
    log.info(f"✅ WS CLOB OB {asset} démarré pour {asset_id_up[:12]}...")
    if asset == "ETH":
        st.eth_ob_asset_id = asset_id_up; st.eth_ob_imbalance = 0.0
    else:
        st.sol_ob_asset_id = asset_id_up; st.sol_ob_imbalance = 0.0
    try:
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(url, heartbeat=10, timeout=aiohttp.ClientTimeout(total=300)) as ws:
                await ws.send_json(sub)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            raw = json.loads(msg.data)
                            msgs = raw if isinstance(raw, list) else [raw]
                            for m in msgs:
                                if not isinstance(m, dict): continue
                                etype = m.get("event_type") or m.get("type", "")
                                if etype not in ("book", "price_change", "tick_size_change"): continue
                                bids = m.get("bids") or []
                                asks = m.get("asks") or []
                                bid_vol = ask_vol = 0.0
                                for b in bids:
                                    try:
                                        if isinstance(b, dict): bid_vol += float(b.get("size", 0))
                                        elif isinstance(b, (list,tuple)) and len(b)>=2: bid_vol += float(b[1])
                                    except: pass
                                for a in asks:
                                    try:
                                        if isinstance(a, dict): ask_vol += float(a.get("size", 0))
                                        elif isinstance(a, (list,tuple)) and len(a)>=2: ask_vol += float(a[1])
                                    except: pass
                                total = bid_vol + ask_vol
                                if total > 0:
                                    imb = round((bid_vol - ask_vol) / total, 3)
                                    if asset == "ETH":
                                        st.eth_ob_imbalance = imb; st.eth_ob_ts = time.time()
                                    else:
                                        st.sol_ob_imbalance = imb; st.sol_ob_ts = time.time()
                        except Exception as pe:
                            log.debug(f"WS CLOB OB {asset} parse: {pe}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
    except Exception as e:
        log.warning(f"WS CLOB OB {asset}: {e}")
    if asset == "ETH": st.eth_ob_imbalance = 0.0
    else: st.sol_ob_imbalance = 0.0


async def ws_eth_loop():
    """✅ v11.10y — Prix ETH temps réel (Binance aggTrade)"""
    url = "wss://stream.binance.com:9443/ws/ethusdt@aggTrade"
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    log.info("✅ WS ETH Binance connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if "p" in d:
                                st.eth_price = float(d["p"])
                                st.eth_ts = time.time()
                                now_e = time.time()
                                st.eth_ws_prices.append((now_e, float(d["p"])))
                                while st.eth_ws_prices and now_e - st.eth_ws_prices[0][0] > 120:
                                    st.eth_ws_prices.popleft()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
        except Exception as e: log.warning(f"WS ETH: {e}")
        await asyncio.sleep(5)

async def ws_sol_loop():
    """✅ v11.10y — Prix SOL temps réel (Binance aggTrade)"""
    url = "wss://stream.binance.com:9443/ws/solusdt@aggTrade"
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    log.info("✅ WS SOL Binance connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if "p" in d:
                                st.sol_price = float(d["p"])
                                st.sol_ts = time.time()
                                now_s = time.time()
                                st.sol_ws_prices.append((now_s, float(d["p"])))
                                while st.sol_ws_prices and now_s - st.sol_ws_prices[0][0] > 120:
                                    st.sol_ws_prices.popleft()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
        except Exception as e: log.warning(f"WS SOL: {e}")
        await asyncio.sleep(5)


async def ws_oracle_loop():
    """
    ✅ v12.4 — UNE seule connexion WS Chainlink pour BTC + ETH + SOL.
    Avant: 3 connexions séparées → rate limiting → BTC tick périmé.
    Maintenant: 1 connexion, subscribe à tous les symboles, dispatch par symbol.
    """
    url = "wss://ws-live-data.polymarket.com"
    # Subscribe à TOUS les symboles Chainlink en une seule connexion
    sub = {"action":"subscribe","subscriptions":[
        {"topic":"crypto_prices_chainlink","type":"*","filters":""}]}
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=10) as ws:  # 10s = raisonnable
                    await ws.send_json(sub)
                    st.oracle_connected = True
                    log.info("✅ WS Oracle Chainlink unifié (BTC+ETH+SOL) connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try: d = json.loads(msg.data)
                            except: continue
                            payload = d.get("payload", {})
                            symbol = payload.get("symbol", "").lower()
                            val = payload.get("value")
                            ts_ms = payload.get("timestamp", 0)
                            if not val or float(val) <= 0: continue
                            p = float(val); now = time.time()
                            cl_ts = ts_ms / 1000 if ts_ms > 0 else now
                            # Rejeter données périmées >30s
                            if ts_ms > 0 and (now - cl_ts) > 30: continue
                            slot_start = int(now // 300) * 300

                            if symbol == "btc/usd":
                                st.oracle_price = p; st.oracle_ts = now
                                st.oracle_chainlink_ts = cl_ts
                                if st.oracle_slot_ts != slot_start:
                                    st.oracle_slot_ts = slot_start
                                    st.oracle_slot_open = p
                                    log.info(f"📌 BTC slot open: ${p:,.2f}")

                            elif symbol == "eth/usd" and float(val) > 100:
                                st.eth_oracle_price = p; st.eth_oracle_ts = now
                                if st.eth_oracle_slot_ts != slot_start:
                                    st.eth_oracle_slot_ts = slot_start
                                    st.eth_oracle_slot_open = p

                            elif symbol == "sol/usd" and float(val) > 5:
                                st.sol_oracle_price = p; st.sol_oracle_ts = now
                                if st.sol_oracle_slot_ts != slot_start:
                                    st.sol_oracle_slot_ts = slot_start
                                    st.sol_oracle_slot_open = p

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except Exception as e:
            log.warning(f"WS Oracle unifié: {e}")
        st.oracle_connected = False
        await asyncio.sleep(5)

async def ws_oracle_eth_loop():
    """✅ v12.4 — Remplacé par ws_oracle_loop unifié (BTC+ETH+SOL en une connexion)"""
    pass  # Géré par ws_oracle_loop()


async def ws_oracle_sol_loop():
    """✅ v12.4 — Remplacé par ws_oracle_loop unifié"""
    pass  # Géré par ws_oracle_loop()


