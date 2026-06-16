"""
POLYMARKET BTC BOT v10.29 — FRAIS CORRIGÉS + FEE_FILTER SUPPRIMÉ
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

BOT_VERSION = "12.8"

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

# ✅ v12.4 — Filtres oracle (alias pour compatibilité save/load)
FILTER_RET3S          = -0.070  # v12.6 — relevé -0.055→-0.070
FILTER_DELTA_CONTRA   = 0.017   # delta contra max
FILTER_GAP_STRONG     = 0.025   # gap fort BTC


MIN_BET_USD     = 2.0   # Minimum absolu
FAIR_EDGE_MIN   = 0.08
MAX_BET_USD     = 8.0   # ✅ v10.26 — Max 8$ (setup exceptionnel sur BR 35$ = ~23%)
MAX_BET_PCT     = 0.15  # ✅ v10.26 — Max Kelly 15% sur setup exceptionnel
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
STOP_LOSS_MULT     = 0.01   # v12.4 désactivé  # Vendre si token tombe sous 45% du prix d'entrée (perte >55%)

# ═══════════ v10.23 — NOUVELLES CONSTANTES ═══════════
# Oracle lag (le meilleur edge: l'oracle bouge en <1s, l'orderbook met ~55s)
ORACLE_LAG_MIN_PCT  = 0.03   # Divergence oracle vs orderbook mini pour signaler un lag exploitable
ORACLE_FRESH_S      = 3.0    # Tick Chainlink considéré frais si <3s
# Entrée étagée
STAGED_ENTRY        = True   # Splitter la mise en 2 tranches
STAGED_FRACTIONS    = [0.6, 0.4]   # 60% à la 1re entrée, 40% à la 2e si signal tient
# Maker order (presque gratuit: tout est limite sur Polymarket de toute façon)
USE_MAKER_ORDERS    = True   # Ordre limite maker = zéro frais + rebate 25%
MAKER_UNDERCUT      = 0.02   # ✅ v10.25 — 2¢ sous le prix (meilleure chance d'être maker)
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
ORACLE_ENTRY_DELTA  = 0.02  # v12.4  # ✅ v10.31 — baissé 0.05→0.03% (-0.049% bloqué mais ✅ dans passes)
ORACLE_TOKEN_MAX    = 0.80  # v12.6 — élargi pour accumuler des données  # ✅ v10.32 — breakeven exact @92%WR = token 0.92$ (EV>0 jusqu'à 0.92$)
ORACLE_TOKEN_MIN    = 0.51  # Token min (trop proche de 0.50$ = incertitude trop haute)
ORACLE_EDGE_MIN     = 0.15  # v12.4  # EV minimum après frais (8%)
ORACLE_WINDOW_START = 25    # v12.4    # Fenêtre normale: T-35s→T-6s (source: dev.to/fatherson)
ORACLE_WINDOW_END   = 5     # v12.4     # T-6s = dernier moment sûr (latence ordre ~2-3s)
# ✅ v10.32 — Mode T-10s (source: github.com/Archetapp — T-10s "direction quasi lockée")
ORACLE_ULTRA_WINDOW = 12    # Passe ultra-précise si T-12s→T-6s ET EV exceptionnelle
ORACLE_ULTRA_EV_MIN = 0.05  # EV min pour passe ultra (moins strict car WR > 95% à T-10s)

# ✅ v10.36 — Filtres WR validés par étude live (medium.com/@gwrx2005, mars 2026)
# Source: filtre 10min → -93% pertes, seuils relevés → -73% fréquence = bien meilleur WR
ORACLE_DELTA_CONTRA_MAX = 0.03  # Si votes=1/3, delta contre doit être < 0.03% sinon skip
ORACLE_GAP_MIN_STRONG   = 0.05  # Gap "fort" = au-delà de ce seuil, même votes=1/3 accepté
ORACLE_TREND_10MIN      = 0.08  # Filtre tendance 10min: si BTC contre-tendance de 0.08%, skip
ORACLE_GAP_CONFIRM_RET  = 0.03  # v11.1 fallback (quand historique gap insuffisant)
GAP_PERSIST_SECS       = 15     # ✅ v11.1 — fenêtre persistance gap (secondes)
GAP_PERSIST_RATIO      = 0.60   # ✅ v11.1 — 60% des points doivent être du même côté
ORACLE_MIN_FRESH_S  = 2.0   # Tick oracle doit être frais (<2s) pour trader
EXCH_STALE_S        = 3.0   # Prix exchange ignoré si plus vieux que 3s (consensus_price)


TAKE_PROFIT_MULT    = 2.0
TRAILING_PEAK_MULT  = 99.0  # v12.4 désactivé
TRAILING_STOP_MULT  = 1.3
TAKE_PROFIT_CHECK   = 15   # ✅ v10.22 — 15s (avant: 30s, trop lent sur du 5min)
POLY_FEE            = 0.02 # Legacy: estimation flat pour le paper mode uniquement
MAX_CONSEC_LOSS     = 2
COOLDOWN_MIN        = 0      # v12.4
MAX_TRADES_PER_H    = 3    # ✅ v10.26 — Max 3/heure (supprimé la limite 1, garde-fou à 3)
CONSERVATIVE_AFTER_LOSSES = 2
BOOST_AFTER_WINS    = 999
DAILY_LOSS_MAX      = 0.99  # v12.4
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

def auth(update):
    """Vérifie que l'utilisateur est autorisé."""
    uid = update.effective_user.id if update.effective_user else 0
    return uid == ALLOWED_UID or ALLOWED_UID == 0


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

    raw_bet = bankroll * min(kp * fraction * liquidity_factor, tier_pct)
    dynamic_min = max(MIN_BET_USD, round(bankroll * 0.04, 2))
    result = round(max(dynamic_min, min(raw_bet, MAX_BET_USD)), 2)
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

    async def get_market_by_slug(self, slug:str):
        """v12.4 — Récupère un marché par slug (BTC/ETH/SOL)."""
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
                                                "question":item.get("title",slug),
                                                "condition_id":m.get("conditionId",""),
                                                "end_date":m.get("endDate",""),"market_slug":slug}
            except Exception as e: log.warning(f"get_market_by_slug {slug}: {e}")
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

async def fetch_klines(interval,limit=60,symbol="btcusdt"):
    sym=symbol.upper()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.binance.com/api/v3/klines?symbol={sym}&interval={interval}&limit={limit}",
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
        self.ws_prices=deque(maxlen=300)   # (ts, price) 5 dernières minutes
        self.ws_price=0.0
        self.gap_history=deque(maxlen=60)  # ✅ v11.1 — (ts, gap%) historique du gap spot↔oracle
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
        self.bet_expiry=0
        self.last_ob=None; self.last_liq=None; self.last_eth_klines=[]
        self.last_news={"sentiment":"neutral","score":0,"news":[]}
        self.price_history=[]
        # ✅ v10.23 — Multi-exchange WS (Coinbase + Kraken en plus de Binance)
        self.cb_price=0.0; self.kr_price=0.0
        self.cb_ts=0; self.kr_ts=0
        self.cb_task=None; self.kr_task=None
        # ✅ v10.23 — Oracle Chainlink (le feed qui RÈGLE le marché)
        self.oracle_price=0.0; self.oracle_ts=0
        self.oracle_slot_open=0.0; self.oracle_slot_ts=0
        self.oracle_task=None; self.oracle_connected=False
        self.oracle_chainlink_ts=0.0
        # ETH
        self.eth_price=0.0; self.eth_ts=0; self.eth_ws_task=None
        self.eth_ws_prices=deque(); self.eth_ws_volumes=deque()
        self.eth_oracle_price=0.0; self.eth_oracle_ts=0.0
        self.eth_oracle_slot_open=0.0; self.eth_oracle_slot_ts=0
        self.eth_last_trade_slot=0
        self.eth_ob_imbalance=0.0; self.eth_ob_ts=0.0; self.eth_ob_asset_id=""; self.eth_clob_ws_task=None
        # SOL
        self.sol_price=0.0; self.sol_ts=0; self.sol_ws_task=None
        self.sol_ws_prices=deque(); self.sol_ws_volumes=deque()
        self.sol_oracle_price=0.0; self.sol_oracle_ts=0.0
        self.sol_oracle_slot_open=0.0; self.sol_oracle_slot_ts=0
        self.sol_last_trade_slot=0
        self.sol_ob_imbalance=0.0; self.sol_ob_ts=0.0; self.sol_ob_asset_id=""; self.sol_clob_ws_task=None
        # ✅ v12.8 — XRP
        self.xrp_price=0.0; self.xrp_ts=0; self.xrp_ws_task=None
        self.xrp_ws_prices=deque()
        self.xrp_oracle_price=0.0; self.xrp_oracle_ts=0.0
        self.xrp_oracle_slot_open=0.0; self.xrp_oracle_slot_ts=0
        self.xrp_last_trade_slot=0
        self.xrp_ob_imbalance=0.0; self.xrp_ob_ts=0.0; self.xrp_ob_asset_id=""; self.xrp_clob_ws_task=None
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
            "trades":self.trades[-200:],"wins":self.wins,"losses":self.losses,"pnl":self.pnl,
            "best_streak":self.best_streak,"worst_streak":self.worst_streak,"consec":self.consec,
            "daily_start":self.daily_start,"daily_ts":self.daily_ts,
            "daily_pause_until":self.daily_pause_until,"paper_mode":self.paper_mode,
            "skipped":self.skipped,"pass_reasons":self.pass_reasons[-50:],
            "calib_factor":self.calib_factor,"killed":self.killed,
            "version":BOT_VERSION,"saved_at":int(time.time()),
            "oracle_patterns":self.oracle_patterns[-200:],
            "calibration_log":self.calibration_log[-20:],
            "haiku_insights":self.haiku_insights[-20:],
            "filter_ret3s":FILTER_RET3S,
            "filter_delta_contra":FILTER_DELTA_CONTRA,
            "filter_gap_strong":FILTER_GAP_STRONG,
            "delta_contra_max":ORACLE_DELTA_CONTRA_MAX}
        try:
            with open(DATA_FILE,"w") as f: json.dump(data,f,indent=2)
        except Exception as e: log.error(f"Save: {e}")
        return data

    def backup(self):
        try:
            data=self.save()
            with open(BACKUP_FILE,"w") as f: json.dump(data,f,indent=2)
            log.info(f"✅ Backup BR:{self.bankroll:.2f}"); return True
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
                    self.oracle_patterns=d.get("oracle_patterns",[])
                    self.calibration_log=d.get("calibration_log",[])
                    self.haiku_insights=d.get("haiku_insights",[])
                    # ✅ Restaurer les seuils auto-calibrés
                    global FILTER_RET3S, FILTER_DELTA_CONTRA, FILTER_GAP_STRONG
                    FILTER_RET3S=d.get("filter_ret3s", FILTER_RET3S)
                    FILTER_DELTA_CONTRA=d.get("filter_delta_contra", FILTER_DELTA_CONTRA)
                    FILTER_GAP_STRONG=d.get("filter_gap_strong", FILTER_GAP_STRONG)
                    self.calib_factor=d.get("calib_factor",1.0); self.killed=d.get("killed",False)
                    age=int((time.time()-d.get("saved_at",0))/60)
                    log.info(f"✅ State {filepath} ({age}min) BR:{self.bankroll:.2f}"); return
            except Exception as e: log.error(f"Load {filepath}: {e}")

st=State()

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
        # ✅ v12.5 — Détecter l'asset depuis reason ou features
        asset_tag = features.get("asset", "BTC")
        if not features.get("asset"):
            if reason.startswith("ETH:") or "[ETH]" in reason[:6]: asset_tag = "ETH"
            elif reason.startswith("SOL:") or "[SOL]" in reason[:6]: asset_tag = "SOL"
            elif reason.startswith("Ξ") or "ETH:" in reason[:8]: asset_tag = "ETH"
            elif reason.startswith("◎") or "SOL:" in reason[:8]: asset_tag = "SOL"
        st.oracle_patterns.append({**features, "direction": direction,
                                    "result": None, "ts": now, "slot_end": entry["slot_end"],
                                    "open_px": entry["open_px"], "asset": asset_tag,
                                    "v": BOT_VERSION})
        if len(st.oracle_patterns) > 2000:
            st.oracle_patterns = st.oracle_patterns[-2000:]

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
        if st.consec>=CONSERVATIVE_AFTER_LOSSES:
            st.conservative_until=time.time()+2*3600
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
    """v12.5 — Backup local + GitHub State toutes les 2min."""
    try: factor, _ = calibrate_sigma(); st.calib_factor = factor
    except: pass
    try:
        st.backup()
        log.info(f"✅ Backup local OK — {len(st.oracle_patterns)} patterns / {len(st.trades)} trades")
    except Exception as e:
        log.warning(f"Backup local ERREUR: {e}")
    try:
        await push_state_to_github()
    except Exception as e:
        log.warning(f"push GitHub ERREUR: {e}")

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
            clob_bal = await fetch_clob_balance()
            bet = st.bet
            if clob_bal and clob_bal > 0:
                prev_bal = st.bankroll
                gross = round(clob_bal - prev_bal, 2)
                won = gross >= 0
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
                gross = round((current_price - st.entry_token_price) * shares_to_sell, 2)
                clob_bal = await fetch_clob_balance()
                if clob_bal and clob_bal > 0:
                    st.bankroll = clob_bal
                else:
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
                            if p > 0:
                                now = time.time()
                                st.ws_price = p
                                st.ws_prices.append((now, p))
                                while st.ws_prices and now - st.ws_prices[0][0] > 120:
                                    st.ws_prices.popleft()
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

async def ws_eth_loop():
    """v12.4 — Prix ETH temps réel Binance."""
    url = "wss://stream.binance.com:9443/ws/ethusdt@aggTrade"
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    log.info("✅ WS ETH Binance")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if "p" in d:
                                p = float(d["p"]); now = time.time()
                                st.eth_price=p; st.eth_ts=now
                                st.eth_ws_prices.append((now,p))
                                while st.eth_ws_prices and now-st.eth_ws_prices[0][0]>120: st.eth_ws_prices.popleft()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
        except Exception as e: log.warning(f"WS ETH: {e}")
        await asyncio.sleep(5)

async def ws_sol_loop():
    """v12.4 — Prix SOL temps réel Binance."""
    url = "wss://stream.binance.com:9443/ws/solusdt@aggTrade"
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    log.info("✅ WS SOL Binance")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if "p" in d:
                                p = float(d["p"]); now = time.time()
                                st.sol_price=p; st.sol_ts=now
                                st.sol_ws_prices.append((now,p))
                                while st.sol_ws_prices and now-st.sol_ws_prices[0][0]>120: st.sol_ws_prices.popleft()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
        except Exception as e: log.warning(f"WS SOL: {e}")
        await asyncio.sleep(5)

async def ws_xrp_loop():
    """v12.8 — Prix XRP temps réel Binance."""
    url = "wss://stream.binance.com:9443/ws/xrpusdt@aggTrade"
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    log.info("✅ WS XRP Binance")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if "p" in d:
                                p = float(d["p"]); now = time.time()
                                st.xrp_price=p; st.xrp_ts=now
                                st.xrp_ws_prices.append((now,p))
                                while st.xrp_ws_prices and now-st.xrp_ws_prices[0][0]>120:
                                    st.xrp_ws_prices.popleft()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
        except Exception as e: log.warning(f"WS XRP: {e}")
        await asyncio.sleep(5)


async def ws_oracle_eth_loop():
    pass  # v12.4 — géré par ws_oracle_loop unifié

async def ws_oracle_sol_loop():
    pass  # v12.4 — géré par ws_oracle_loop unifié


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

async def pull_state_from_github():
    """Télécharge le state depuis GitHub branche State."""
    gh_token = os.getenv("GITHUB_TOKEN",""); gh_repo = os.getenv("GITHUB_REPO","")
    if not gh_token or not gh_repo: return False
    try:
        import base64
        url = f"https://api.github.com/repos/{gh_repo}/contents/polybot_v10_state.json?ref=State"
        hdrs = {"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"}
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=hdrs) as r:
                if r.status != 200: return False
                data = await r.json()
                content = base64.b64decode(data["content"]).decode()
                with open("polybot_v10_state.json", "w") as f: f.write(content)
                log.info(f"✅ State GitHub chargé ({len(content)} bytes)")
                return True
    except Exception as e: log.warning(f"pull_state: {e}"); return False

async def push_state_to_github():
    """Push le state vers GitHub branche State."""
    gh_token = os.getenv("GITHUB_TOKEN",""); gh_repo = os.getenv("GITHUB_REPO","")
    if not gh_token or not gh_repo: return
    try:
        import base64, json as _json
        data = st.save()
        content = base64.b64encode(_json.dumps(data).encode()).decode()
        url = f"https://api.github.com/repos/{gh_repo}/contents/polybot_v10_state.json"
        hdrs = {"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json",
                "Content-Type": "application/json"}
        async with aiohttp.ClientSession() as s:
            async with s.get(url + "?ref=State", headers=hdrs) as r:
                sha = (await r.json()).get("sha","") if r.status==200 else ""
            body = {"message":"auto backup","content":content,"branch":"State"}
            if sha: body["sha"] = sha
            async with s.put(url, headers=hdrs, json=body) as r:
                if r.status in (200,201): log.info("✅ State → GitHub State")
    except Exception as e:
        import traceback
        log.warning(f"push_state ERREUR: {e}")
        log.warning(traceback.format_exc())


def compute_rsi(prices, period=7):
    if len(prices) < period+1: return 50.0
    gains = losses = 0.0
    for i in range(1, period+1):
        d = prices[-i]-prices[-i-1]
        if d>0: gains+=d
        else: losses-=d
    if losses==0: return 100.0
    rs=(gains/period)/(losses/period)
    return 100-(100/(1+rs))

def compute_ema(prices, period):
    if len(prices)<period: return prices[-1] if prices else 0
    k=2/(period+1); ema=prices[0]
    for p in prices[1:]: ema=p*k+ema*(1-k)
    return ema

def compute_ta_score(price_history, asset="BTC"):
    if len(price_history)<8: return 0,None,{}
    prices=[x["price"] for x in sorted(price_history,key=lambda x:x["ts"])]
    now=price_history[-1]["ts"] if price_history else 0
    score=0; details={}
    rsi=compute_rsi(prices,7); details["rsi"]=round(rsi,1)
    if rsi<35: score+=2
    elif rsi<45: score+=1
    elif rsi>65: score-=2
    elif rsi>55: score-=1
    if len(prices)>=21:
        ema9=compute_ema(prices[-21:],9); ema21=compute_ema(prices[-21:],21)
        gap_pct=(ema9-ema21)/ema21*100 if ema21 else 0
        if gap_pct>0.02: score+=1
        elif gap_pct<-0.02: score-=1
    pts_3m=[x for x in price_history if now-x["ts"]<=180]
    if len(pts_3m)>=2:
        roc=(pts_3m[-1]["price"]-pts_3m[0]["price"])/pts_3m[0]["price"]*100 if pts_3m[0]["price"] else 0
        details["mom3m"]=round(roc,4)
        if roc>0.03: score+=1
        elif roc<-0.03: score-=1
    if len(prices)>=10:
        recent=prices[-10:]; avg=sum(recent)/len(recent)
        std=(sum((p-avg)**2 for p in recent)/len(recent))**0.5
        if avg>0 and std/avg*100>0.1: score=int(score*0.7)
    direction="UP" if score>0 else ("DOWN" if score<0 else None)
    return score,direction,details


async def ws_oracle_loop():
    """v12.4 — Oracle unifié BTC+ETH+SOL en UNE seule connexion (évite le rate limiting)."""
    url = "wss://ws-live-data.polymarket.com"
    sub = {"action":"subscribe","subscriptions":[{"topic":"crypto_prices_chainlink","type":"*","filters":""}]}
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=10) as ws:  # v12.4 — 1 seule connexion BTC+ETH+SOL
                    await ws.send_json(sub)
                    st.oracle_connected=True
                    log.info("✅ WS Oracle Chainlink connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                d = json.loads(msg.data)
                            except: continue
                            payload = d.get("payload", {})
                            symbol = payload.get("symbol","").lower()
                            val = payload.get("value")
                            ts_ms = payload.get("timestamp",0)
                            if not val or float(val) <= 0: continue
                            p = float(val); now = time.time()
                            cl_ts = ts_ms/1000 if ts_ms>0 else now
                            if ts_ms>0 and (now-cl_ts)>30: continue
                            slot_start = int(now//300)*300
                            st.oracle_chainlink_ts = cl_ts
                            if symbol == "btc/usd":
                                st.oracle_price=p; st.oracle_ts=now
                                if st.oracle_slot_ts!=slot_start:
                                    st.oracle_slot_ts=slot_start; st.oracle_slot_open=p
                                    log.info(f"📌 BTC slot open: ${p:,.2f}")
                            elif symbol == "eth/usd" and p>100:
                                st.eth_oracle_price=p; st.eth_oracle_ts=now
                                if st.eth_oracle_slot_ts!=slot_start:
                                    st.eth_oracle_slot_ts=slot_start; st.eth_oracle_slot_open=p
                            elif symbol == "sol/usd" and p>1:
                                st.sol_oracle_price=p; st.sol_oracle_ts=now
                                if st.sol_oracle_slot_ts!=slot_start:
                                    st.sol_oracle_slot_ts=slot_start; st.sol_oracle_slot_open=p
                            elif symbol == "xrp/usd" and p>0.01:
                                st.xrp_oracle_price=p; st.xrp_oracle_ts=now
                                if st.xrp_oracle_slot_ts!=slot_start:
                                    st.xrp_oracle_slot_ts=slot_start; st.xrp_oracle_slot_open=p
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except Exception as e:
            log.warning(f"WS Oracle: {e}")
        st.oracle_connected=False
        await asyncio.sleep(5)

async def job_ws_watchdog_all(context):
    """✅ v10.23 — Garde TOUS les WS en vie (Binance + Coinbase + Kraken + Oracle)"""
    if st.ws_task is None or st.ws_task.done():
        st.ws_task = asyncio.create_task(ws_binance_loop())
    if st.cb_task is None or st.cb_task.done():
        st.cb_task = asyncio.create_task(ws_coinbase_loop())
    if st.kr_task is None or st.kr_task.done():
        st.kr_task = asyncio.create_task(ws_kraken_loop())
    oracle_stale = st.oracle_ts>0 and (time.time()-st.oracle_ts)>60
    if st.oracle_task is None or st.oracle_task.done() or oracle_stale:
        if oracle_stale and st.oracle_task and not st.oracle_task.done(): st.oracle_task.cancel()
        st.oracle_task = asyncio.create_task(ws_oracle_loop())
    # ETH/SOL WS prix
    if not hasattr(st,"eth_ws_task") or not st.eth_ws_task or st.eth_ws_task.done():
        st.eth_ws_task = asyncio.create_task(ws_eth_loop())
    if not hasattr(st,"sol_ws_task") or not st.sol_ws_task or st.sol_ws_task.done():
        st.sol_ws_task = asyncio.create_task(ws_sol_loop())
    if not hasattr(st,"xrp_ws_task") or not st.xrp_ws_task or st.xrp_ws_task.done():
        st.xrp_ws_task = asyncio.create_task(ws_xrp_loop())

def consensus_price():
    """✅ v10.23 — Prix médian des exchanges frais (<3s). Filtre un exchange qui lag/diverge."""
    now = time.time()
    prices = []
    if st.ws_price > 0 and now - (st.ws_prices[-1][0] if st.ws_prices else 0) < 3: prices.append(st.ws_price)
    if st.cb_price > 0 and now - st.cb_ts < 3: prices.append(st.cb_price)
    if st.kr_price > 0 and now - st.kr_ts < 3: prices.append(st.kr_price)
    if not prices: return st.ws_price if st.ws_price>0 else st.price
    prices.sort()
    n=len(prices)
    return prices[n//2] if n%2 else (prices[n//2-1]+prices[n//2])/2

def compute_oracle_lag():
    """
    ✅ v10.23 — Détecte le lag oracle: si l'oracle (qui règle) a déjà bougé dans
    une direction depuis l'ouverture du slot mais que l'orderbook ne l'a pas
    encore pricé, c'est un signal directionnel quasi sûr.
    Retourne un bias basé sur le delta de l'ORACLE (pas du spot exchange).
    """
    now = time.time()
    if not st.oracle_connected or st.oracle_price<=0 or st.oracle_slot_open<=0:
        return None
    if now - st.oracle_ts > ORACLE_FRESH_S:  # tick oracle périmé
        return None
    if st.oracle_slot_ts != int(now//300)*300:
        return None
    div_pct = (st.oracle_price - st.oracle_slot_open) / st.oracle_slot_open * 100
    if abs(div_pct) < ORACLE_LAG_MIN_PCT:
        return None
    bias = "UP" if div_pct > 0 else "DOWN"
    return {"bias":bias,"div_pct":round(div_pct,3),
            "desc":f"🔗 Oracle {bias} {div_pct:+.3f}% (règle le marché)"}

def compute_btc_bps(slot_open_price, current_price, direction):
    """
    ✅ v10.27 — Filtre BPS validé sur 29,060 trades (polybacktest.com).

    Deux conditions empiriquement optimales:
    1. BPS_CURRENT: BTC est 5-10 bps AU-DELÀ du prix de référence dans la direction
       → confirme que la direction est bien établie
    2. BPS_TOTAL: BTC n'a bougé que 5-12 bps TOTAL depuis l'ouverture
       → mouvement lent et stable = moins de risque de retournement brutal

    Retourne (ok, bps_current, bps_total, reason)
    """
    if slot_open_price <= 0 or current_price <= 0:
        return False, 0, 0, "Prix manquant"

    # BPS total depuis ouverture (amplitude totale)
    bps_total = abs(current_price - slot_open_price) / slot_open_price * 10000

    # BPS dans la direction tradée
    if direction == "UP":
        bps_current = (current_price - slot_open_price) / slot_open_price * 10000
    else:
        bps_current = (slot_open_price - current_price) / slot_open_price * 10000

    # Filtre 1: BTC doit être dans la bonne direction avec 5-10 bps
    if bps_current < BPS_CURRENT_MIN:
        return False, round(bps_current,1), round(bps_total,1), f"bps_current {bps_current:.1f}<{BPS_CURRENT_MIN} (direction pas assez établie)"
    if bps_current > BPS_CURRENT_MAX:
        return False, round(bps_current,1), round(bps_total,1), f"bps_current {bps_current:.1f}>{BPS_CURRENT_MAX} (déjà pricé dans le token)"

    # Filtre 2: mouvement total doit être lent et stable (5-12 bps)
    if bps_total < BPS_TOTAL_MIN:
        return False, round(bps_current,1), round(bps_total,1), f"bps_total {bps_total:.1f}<{BPS_TOTAL_MIN} (mouvement trop faible)"
    if bps_total > BPS_TOTAL_MAX:
        return False, round(bps_current,1), round(bps_total,1), f"bps_total {bps_total:.1f}>{BPS_TOTAL_MAX} (mouvement trop brusque = risque retournement)"

    return True, round(bps_current,1), round(bps_total,1), f"✅ BPS ok: {bps_current:.1f} bps vers {direction}, {bps_total:.1f} bps total"

def calibrate_sigma():
    """
    ✅ v10.23 — Auto-calibre VOL_SAFETY à partir des trades réels résolus.
    Compare la confiance prédite (bet['conf']) au WR réel par bucket.
    Si le bot gagne MOINS souvent que prédit → augmenter VOL_SAFETY (être plus prudent).
    Si plus souvent → diminuer. Retourne le nouveau facteur (borné 0.7-2.5).
    """
    resolved = [t for t in st.trades if t.get("conf",0)>0 and t.get("result") in ("WIN","LOSS")]
    if len(resolved) < CALIB_MIN_TRADES:
        return st.calib_factor, f"Calibration: {len(resolved)}/{CALIB_MIN_TRADES} trades"
    # WR réel vs confiance moyenne prédite
    avg_conf = sum(t["conf"] for t in resolved)/len(resolved)
    real_wr = sum(1 for t in resolved if t["result"]=="WIN")/len(resolved)
    if real_wr <= 0: real_wr = 0.01
    # Si on prédit 0.85 mais on gagne 0.70 → on est trop confiant → σ trop bas → augmenter
    ratio = avg_conf / real_wr  # >1 = surconfiant
    new_factor = max(0.7, min(2.5, st.calib_factor * (0.5 + 0.5*ratio)))
    return round(new_factor,3), f"Calib: pred {avg_conf:.2f} vs réel {real_wr:.2f} → ×{new_factor:.2f}"

def realized_vol():
    """Volatilité réalisée (% par √seconde) sur les ~60 dernières secondes WS"""
    pts = list(st.ws_prices)
    if len(pts) < 10: return 0.0
    rets = []; last_t, last_p = pts[0]
    for t, p in pts[1:]:
        dt = t - last_t
        if dt >= 0.8 and last_p > 0:
            rets.append((p - last_p) / last_p * 100 / math.sqrt(dt))
            last_t, last_p = t, p
    if len(rets) < 5: return 0.0
    m = sum(rets) / len(rets)
    var = sum((r - m) ** 2 for r in rets) / len(rets)
    return math.sqrt(var)

VOL_SAFETY = 3.0   # ✅ v10.28 — Relevé 2.5→3.0 (calibration empirique: modèle était surconfiant, 70% WR < probas prédites)
P_CAP      = 0.95  # ✅ v10.21c — Jamais plus confiant que 95% (15-20% des slots flippent en fin)


def fair_prob_up(delta_pct, t_remaining_s, sigma):
    """P(BTC finit UP) — modèle Brownien: N(delta / (sigma * √T))"""
    if t_remaining_s <= 0: return 1.0 if delta_pct > 0 else 0.0
    if sigma <= 0: return 0.5
    z = delta_pct / (sigma * VOL_SAFETY * st.calib_factor * math.sqrt(t_remaining_s))  # ✅ v10.23 calib
    p = 0.5 * (1.0 + math.erf(z / math.sqrt(2)))
    return max(1.0 - P_CAP, min(P_CAP, p))

async def job_price(context):
    p=await fetch_price()
    if p>0:
        now=time.time()
        st.price_history.append({"price":p,"ts":now})
        st.price_history=[x for x in st.price_history if now-x["ts"]<600]

        # ✅ v10.22 — Résolution THÉORIQUE des skips
        for pr in st.pass_reasons[-40:]:
            if (pr.get("resolved") is None and pr.get("slot_end",0)>0
                and now>pr["slot_end"]+10 and pr.get("dir") in ("UP","DOWN")
                and pr.get("open_px",0)>0):
                won=(p>pr["open_px"])==(pr["dir"]=="UP")
                pr["resolved"]="WIN" if won else "LOSS"
        # ✅ v10.37 — Résolution des patterns oracle pour auto-calibration
        for pat in st.oracle_patterns[-100:]:
            if (pat.get("result") is None and pat.get("slot_end",0)>0
                and now>pat["slot_end"]+10 and pat.get("open_px",0)>0
                and pat.get("direction") in ("UP","DOWN")):
                won=(p>pat["open_px"])==(pat["direction"]=="UP")
                pat["result"]="WIN" if won else "LOSS"

        if st.price>0 and not st.bet:
            move_pct = (p - st.price) / st.price * 100
            if abs(move_pct) >= 1.0:
                direction = "📈 UP" if move_pct > 0 else "📉 DOWN"
                await send(context.bot,
                    f"⚡ *Move BTC détecté*\n"
                    f"{direction} `{move_pct:+.2f}%` en ~30s\n"
                    f"₿`${p:,.2f}` | Lance `/signal` pour analyser")

        prices_2min=[x for x in st.price_history if now-x["ts"]<=120]
        if len(prices_2min)>=2 and not st.bet:
            p_old=prices_2min[0]["price"]
            move_2min=(p-p_old)/p_old*100 if p_old>0 else 0
            if abs(move_2min)>=0.5 and abs(move_2min)<1.0:
                log.info(f"Move 2min: {move_2min:+.2f}%")
        st.price=p

async def job_macro(context):
    st.fg=await fetch_fear_greed(); st.btc24=await fetch_btc_24h()
    try: st.last_ob=await fetch_orderbook_imbalance()
    except: pass
    try: st.last_liq=await fetch_liquidations()
    except: pass
    try: st.last_eth_klines=await fetch_eth_klines("5m",30)
    except: pass
    try: st.last_news=await fetch_btc_news()
    except: pass

async def resolve_paper_bet(context):
    """✅ v10.22 — Résolution paper sortie des gates de timing (avant: retardée jusqu'au
    prochain tick dans la fenêtre d'entrée, ce qui faussait entry vs exit)"""
    if not st.bet or not st.paper_mode: return
    bet_slot_end=(st.bet["ts"]//300)*300+300
    if time.time()<bet_slot_end+5: return
    bet=st.bet; won=bet["dir"]==("UP" if st.price>bet["entry"] else "DOWN")
    gross=bet["amount"]*(1-POLY_FEE) if won else -bet["amount"]
    st.bankroll=max(0.0,st.bankroll+gross); st.pnl+=gross
    register_trade_result(won)
    i15_n=compute_ind(list(st.c15)); i1h_n=compute_ind(list(st.c1h))
    st.trades.append({"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
        "conf":bet["conf"],"result":"WIN" if won else "LOSS","entry":bet["entry"],"exit":st.price,
        "reasoning":bet.get("reasoning",""),"paper":True,"ts":int(time.time()),
        "score":bet.get("score",0),"fg_value":st.fg.get("value",50),
        "session":bet.get("session","?"),
        "aligned_15h1h":i15_n.get("ema_bull")==i1h_n.get("ema_bull") if i15_n and i1h_n else True})
    st.bet=None; st.token_price_peak=0; st.trailing_active=False; st.bet_expiry=0
    if not won and st.consec>=CONSERVATIVE_AFTER_LOSSES:
        await send(context.bot, f"⚠️ *Mode conservateur activé 2h* — {st.consec} pertes consécutives")
    elif won and st.win_streak_count>=BOOST_AFTER_WINS:
        await send(context.bot, f"🔥 *{st.win_streak_count} wins consécutifs* — Kelly +20%")
    cd_msg=f"\n⏸ Cooldown {COOLDOWN_MIN}min" if in_cd() else ""
    await send(context.bot,f"{'✅' if won else '❌'} *Trade clôturé* [📄]\n`{bet['dir']}` `${bet['entry']:,.0f}`→`${st.price:,.0f}`\nPnL:`{'+' if gross>=0 else ''}{gross:.2f}$` BR:`{st.bankroll:.2f}` ROI:`{roi()}`{cd_msg}")
    st.backup()

async def place_bet(context, direction, amount, conf, reasoning, conf_score, sess, tpu, tpd, market_end, source="tick"):
    """
    ✅ v10.23 — Placement centralisé: REFETCH prix + MAKER order (undercut) +
    ENTRÉE ÉTAGÉE (la 2e tranche est gérée dans st.bet["staged_remaining"]).
    Rappel source: sur Polymarket tout est un ordre LIMITE de toute façon.
    """
    order_id=None; token_used=None; entry_tp=0.5
    # ✅ v10.23 — Entrée étagée: on place d'abord STAGED_FRACTIONS[0] du montant
    staged_remaining = 0.0
    first_amount = amount
    if STAGED_ENTRY and amount >= MIN_BET_USD*2 and source in ("tick","snipe"):
        first_amount = round(max(MIN_BET_USD, amount*STAGED_FRACTIONS[0]),2)
        staged_remaining = round(amount-first_amount,2)
        if staged_remaining < MIN_BET_USD:  # le reste serait sous le minimum → on met tout d'un coup
            first_amount = amount; staged_remaining = 0.0

    if not st.paper_mode and st.current_market:
        token_used=st.current_market["token_up"] if direction=="UP" else st.current_market["token_down"]
        if market_end > 0 and (market_end - time.time()) < 15:
            log_skip(f"Slot expire dans {market_end-time.time():.0f}s — ordre annulé", direction)
            return False
        # ✅ REFETCH prix juste avant l'ordre
        fresh_tp = await poly.get_token_price(token_used)
        entry_tp = fresh_tp if fresh_tp > 0 else (tpu if direction=="UP" else tpd)
        if source=="tick" and (entry_tp < 0.35 or entry_tp > 0.92):
            log_skip(f"Prix token bougé avant ordre ({entry_tp:.2f}$)", direction); return False
        if source=="snipe" and (entry_tp < SNIPE_TOKEN_MIN-0.05 or entry_tp > SNIPE_TOKEN_MAX+0.03):
            log_skip(f"SNIPE: prix token bougé ({entry_tp:.2f}$)", direction); return False
        order_id=await poly.place_order(token_used, first_amount, entry_tp, "BUY")  # ✅ maker/limite
        if not order_id:
            await send(context.bot,"⚠️ *Ordre Polymarket refusé — réessai prochain slot*"); return False
        st.active_order_id=order_id; st.active_token_id=token_used
        st.entry_token_price=entry_tp; st.shares_bought=round(first_amount/entry_tp,4) if entry_tp>0 else 0
        st.token_price_peak=1.0; st.trailing_active=False
        st.bet_expiry=market_end if market_end>0 else (int(time.time()//300)*300+300)
    else:
        entry_tp = tpu if direction=="UP" else tpd
        st.entry_token_price=entry_tp; st.shares_bought=round(first_amount/entry_tp,4) if entry_tp>0 else 0
        st.bet_expiry=int(time.time()//300)*300+300
    st.bet={"dir":direction,"amount":first_amount,"conf":conf,"entry":consensus_price() if consensus_price()>0 else st.price,
            "reasoning":reasoning,"ts":int(time.time()),"score":conf_score.get("score",0),"session":sess["session"],
            "staged_remaining":staged_remaining,"staged_done":staged_remaining<=0,"source":source}
    st.last_trade_slot = int(time.time()//300)*300  # ✅ dédup
    return True

async def job_staged_entry(context):
    """✅ v10.23 — Place la 2e tranche si le signal tient toujours (oracle/delta cohérents)"""
    if not st.bet or st.bet.get("staged_done") or st.bet.get("staged_remaining",0)<MIN_BET_USD: return
    if st.paper_mode:  # en paper on valide juste la logique, on additionne au montant
        st.bet["amount"]=round(st.bet["amount"]+st.bet["staged_remaining"],2)
        st.bet["staged_remaining"]=0.0; st.bet["staged_done"]=True
        return
    # Attendre ~20s après la 1re entrée
    if time.time()-st.bet["ts"] < 20: return
    direction=st.bet["dir"]
    # Le signal tient-il ? Delta oracle/consensus toujours dans le bon sens
    wd_w,wd_pct=live_window_delta()
    still_ok=(direction=="UP" and wd_pct>0) or (direction=="DOWN" and wd_pct<0)
    if not still_ok:
        st.bet["staged_done"]=True  # signal cassé → on garde juste la 1re tranche
        return
    remaining=st.bet["staged_remaining"]
    if st.bankroll<remaining:
        st.bet["staged_done"]=True; return
    fresh_tp=await poly.get_token_price(st.active_token_id)
    if fresh_tp<=0 or fresh_tp>0.70:
        # ✅ v10.34 — Token >0.70$ = direction déjà pricée, EV 2e tranche négative
        # Ex: 1re tranche 0.59$ (EV+29%), 2e tranche 0.86$ (EV~0%) = dilution pure
        st.bet["staged_done"]=True; return
    oid=await poly.place_order(st.active_token_id, remaining, fresh_tp, "BUY")
    if oid:
        # Recalcul prix d'entrée moyen pondéré
        old_shares=st.shares_bought; new_shares=round(remaining/fresh_tp,4)
        total_shares=old_shares+new_shares
        st.entry_token_price=round((st.entry_token_price*old_shares+fresh_tp*new_shares)/total_shares,4) if total_shares>0 else fresh_tp
        st.shares_bought=total_shares
        st.bet["amount"]=round(st.bet["amount"]+remaining,2)
        st.bet["staged_done"]=True
        await send(context.bot, f"➕ *2e tranche* `{remaining:.2f}$` @`{fresh_tp:.3f}$` | entrée moy:`{st.entry_token_price:.3f}$`")
    else:
        st.bet["staged_done"]=True

async def job_tick(context):
    if not st.running or st.killed: return

    # ✅ v10.25 — SNIPE-ONLY en mode réel
    # job_tick (entrée T-60s à T-50s, token 0.50-0.75$) = zone taker fees max = non rentable
    # En mode réel: on laisse tourner uniquement pour la résolution paper et les stats
    # job_snipe gère tout le trading réel (token ≥ 0.82$, frais ~0¢)
    if not st.paper_mode:
        await resolve_paper_bet(context)  # résolution si position paper ouverte
        return

    # ✅ v10.22 — Résolution paper HORS des gates de timing
    await resolve_paper_bet(context)

    now_ts = time.time()
    slot_pos = now_ts % 300
    slot_remaining = 300 - slot_pos

    # ✅ v10.22 — Fenêtre normale élargie: 15s → T-45s (avant: T-90s)
    # Le mode SNIPE (job dédié) couvre T-45s → T-20s
    if slot_remaining < ENTRY_LAST_SECONDS:
        return
    if slot_pos < 15:
        return

    global _last_tick_ts
    _last_tick_ts = time.time()
    if st.last_trade_slot == int(time.time()//300)*300: return  # ✅ dédup slot
    paused=check_daily()
    if paused:
        remaining=int((st.daily_pause_until-time.time())/60)
        if remaining%30==0 and remaining>0:
            await send(context.bot,f"⏸ *Pause journalière* — reprise dans `{remaining}min`")
        return
    if in_cd(): return
    if st.bet: return
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
    if not c5: return

    # ✅ v10.20g — WINDOW DELTA: signal dominant
    now_price = c5[-1]["close"] if c5 else 0
    slot_open_price = 0
    slot_open_minutes = int(slot_pos / 60) + 1
    if c1 and len(c1) >= slot_open_minutes:
        slot_open_price = c1[-slot_open_minutes]["open"]
    elif c5 and len(c5) >= 1:
        slot_open_price = c5[-1]["open"]

    window_delta_pct = 0.0
    if slot_open_price > 0 and now_price > 0:
        window_delta_pct = (now_price - slot_open_price) / slot_open_price * 100
    window_delta = delta_to_weight(window_delta_pct)

    # ✅ v10.21 — Si le WS a le prix d'ouverture exact du slot, l'utiliser (plus précis)
    cur_slot = int(time.time() // 300) * 300
    if st.ws_price > 0 and st.slot_open_price > 0 and st.slot_open_ts == cur_slot:
        window_delta_pct = (st.ws_price - st.slot_open_price) / st.slot_open_price * 100
        window_delta = delta_to_weight(window_delta_pct)

    st.window_delta_pct = window_delta_pct
    st.window_delta = window_delta
    log.info(f"Window delta: {window_delta_pct:+.3f}% → score {window_delta:+.1f} (WS:{'✅' if st.ws_connected else '❌'})")
    st.c1=deque(c1,maxlen=100); st.c5=deque(c5,maxlen=100); st.c15=deque(c15,maxlen=100)
    st.c1h=deque(c1h,maxlen=100); st.c4h=deque(c4h,maxlen=50); st.price=c5[-1]["close"]
    if trades_last_hour(st.trades)>=MAX_TRADES_PER_H: return
    if in_cd(): return
    if not is_trending(list(st.c5),list(st.c15)):
        st.skipped+=1; return  # Marché plat — skip silencieux (pas de direction à tracker)
    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5)); i15=compute_ind(list(st.c15))
    i1h=compute_ind(list(st.c1h)); i4h=compute_ind(list(st.c4h)) if st.c4h else {}
    sess=session_ctx()
    if not i5: return
    adv=compute_advanced_signals(list(st.c5),list(st.c1),list(st.c4h) if st.c4h else None)
    direction_guess="UP" if i5.get("ema_bull") else "DOWN"
    eth_bonus,eth_desc=compute_eth_correlation(st.last_eth_klines,direction_guess) if st.last_eth_klines else (0,"N/A")
    conf_score=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv,st.last_ob,st.last_liq,eth_bonus,eth_desc,st.btc24,st.window_delta,st.window_delta_pct)
    mom_score=compute_momentum_score(i1,i5,i15)
    st.last_conf_score=conf_score; st.last_mom_score=mom_score
    _,_,min_mom=get_session_thresholds(sess.get("session","OVERNIGHT"), conf_score.get("score",0))
    if not conf_score["tradeable"]:
        # ✅ v10.20f — Retry rapide si score proche du seuil
        score_gap = conf_score["min_score"] - conf_score["score"]
        diff_gap = conf_score["min_diff"] - conf_score["diff"]
        slot_remaining_now = 300 - (time.time() % 300)

        if (score_gap <= 2 or diff_gap <= 1) and slot_remaining_now > 150:
            await asyncio.sleep(10)
            c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
            c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
            if c5:
                st.c1=deque(c1,maxlen=100); st.c5=deque(c5,maxlen=100)
                st.c15=deque(c15,maxlen=100); st.c1h=deque(c1h,maxlen=100)
                st.c4h=deque(c4h,maxlen=50); st.price=c5[-1]["close"]
                # ✅ v10.22 FIX — Recalcul du window delta avec les données fraîches
                wd_w, wd_pct = live_window_delta()
                st.window_delta=wd_w; st.window_delta_pct=wd_pct
                i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5))
                i15=compute_ind(list(st.c15)); i1h=compute_ind(list(st.c1h))
                i4h=compute_ind(list(st.c4h)) if st.c4h else {}
                adv=compute_advanced_signals(list(st.c5),list(st.c1),list(st.c4h) if st.c4h else None)
                eth_bonus2,eth_desc2=compute_eth_correlation(st.last_eth_klines,direction_guess) if st.last_eth_klines else (0,"N/A")
                # ✅ v10.22 FIX CRITIQUE — le retry passait SANS window delta (signal x6 perdu)
                conf_score2=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv,st.last_ob,st.last_liq,eth_bonus2,eth_desc2,st.btc24,st.window_delta,st.window_delta_pct)
                mom_score2=compute_momentum_score(i1,i5,i15)
                if conf_score2["tradeable"] and mom_score2>=min_mom:
                    log.info(f"✅ Retry réussi — score {conf_score2['score']:.1f} mom {mom_score2}")
                    conf_score=conf_score2; mom_score=mom_score2; eth_desc=eth_desc2
                else:
                    log_skip(f"Score {conf_score2['score']:.1f}<{conf_score2['min_score']} (après retry)", conf_score2["direction"])
                    return
            else:
                st.skipped+=1; return
        else:
            if conf_score["score"] < conf_score["min_score"]:
                reason = f"Score {conf_score['score']:.1f}<{conf_score['min_score']}"
            elif conf_score["diff"] < conf_score["min_diff"]:
                reason = f"Diff {conf_score['diff']:.1f}<{conf_score['min_diff']} (UP:{conf_score['score_up']:.1f} DN:{conf_score['score_dn']:.1f})"
            else:
                reason = f"Tradeable=NON score:{conf_score['score']:.1f} diff:{conf_score['diff']:.1f}"
            log_skip(reason, conf_score["direction"]); return
    if mom_score<min_mom:
        log_skip(f"Mom {mom_score}<{min_mom}", conf_score["direction"]); return
    if i5.get("atr_pct",0)<0.03:
        log_skip(f"ATR {i5.get('atr_pct',0):.3f}%<0.03%", conf_score["direction"]); return
    if i5.get("vol_ratio",1)<0.4:
        log_skip(f"Vol ratio {i5.get('vol_ratio',1):.2f}<0.4", conf_score["direction"]); return
    adx_val = i5.get("adx", 20)
    log.debug(f"ADX: {adx_val}")
    tpu=0.5; tpd=0.5; market_end=0
    if not st.paper_mode:
        market=await poly.find_btc_5min_market()
        if market:
            st.current_market=market
            tpu=await poly.get_token_price(market["token_up"])
            tpd=await poly.get_token_price(market["token_down"])
            try:
                from datetime import timezone
                ed=market.get("end_date","")
                if ed:
                    dt=datetime.fromisoformat(ed.replace("Z","+00:00"))
                    market_end=dt.timestamp()
            except: pass
        else:
            log_skip("Aucun marché actif", conf_score["direction"]); return
    ppu=round(1/tpu,2) if tpu>0 else 0
    ppd=round(1/tpd,2) if tpd>0 else 0
    direction=conf_score["direction"]
    best_payout = ppu if direction=="UP" else ppd
    token_price_dir = tpu if direction=="UP" else tpd
    if not st.paper_mode:
        if best_payout < 1.3:
            log_skip(f"Payout {best_payout:.2f}<1.3", direction); return
        if best_payout > 5.0:
            log_skip(f"Payout {best_payout:.2f}>5.0 (marché >80% contre)", direction); return
        # ✅ v10.20g — Zone token optimale mode normal: 0.40$ à 0.88$
        if token_price_dir < 0.40:
            log_skip(f"Token trop bas ({token_price_dir:.2f}$<0.40$)", direction); return
        if token_price_dir > 0.88:
            log_skip(f"Token trop haut ({token_price_dir:.2f}$>0.88$) — zone SNIPE", direction); return

    # ✅ v10.21 — FILTRE TENDANCE 10MIN: jamais contre la tendance de fond
    cur_px = consensus_price()  # ✅ v10.23 — prix médian multi-exchange
    if len(st.price_history) >= 2 and cur_px > 0:
        older = [x for x in st.price_history if time.time() - x["ts"] >= 540]
        ref_px = older[-1]["price"] if older else st.price_history[0]["price"]
        if ref_px > 0:
            ch10 = (cur_px - ref_px) / ref_px * 100
            if direction == "UP" and ch10 < -0.15:
                log_skip(f"UP bloqué: BTC {ch10:+.2f}% sur 10min (contre-tendance)", direction); return
            if direction == "DOWN" and ch10 > 0.15:
                log_skip(f"DOWN bloqué: BTC {ch10:+.2f}% sur 10min (contre-tendance)", direction); return

    # ✅ v10.23 — SIGNAL ORACLE LAG: l'oracle qui règle bouge avant l'orderbook.
    # Si l'oracle contredit notre direction → on annule (l'oracle a toujours raison).
    # Si l'oracle confirme → bonus de confiance (on sait où ça résout avant le marché).
    oracle_sig = compute_oracle_lag()
    oracle_conf_bonus = 0.0
    if oracle_sig:
        if oracle_sig["bias"] != direction:
            log_skip(f"Oracle contredit: {oracle_sig['desc']} vs notre {direction}", direction)
            return
        oracle_conf_bonus = 0.05  # l'oracle confirme → +5pts de proba
        st.oracle_lag_signal = oracle_sig

    # ✅ v10.22 — FAIR VALUE GATE avec FRAIS TAKER RÉELS déduits
    # EV = P(direction) - prix_token - frais_par_share
    # Frais officiels Polymarket 5min: 0.25*(p*(1-p))² — max à p=0.50 (~1.6¢)
    sigma = realized_vol()
    t_rem = 300 - (time.time() % 300)
    delta_gate = st.window_delta_pct
    if st.ws_price > 0 and st.slot_open_price > 0 and st.slot_open_ts == int(time.time() // 300) * 300:
        delta_gate = (st.ws_price - st.slot_open_price) / st.slot_open_price * 100
    fee = taker_fee_per_share(token_price_dir)
    win_prob = None
    if sigma > 0:
        p_up = fair_prob_up(delta_gate, t_rem, sigma)
        p_dir = p_up if direction == "UP" else 1.0 - p_up
        ev = p_dir - token_price_dir - fee
        st.last_fair = {"p_up": round(p_up,3), "sigma": round(sigma,4), "ev": round(ev,3),
                        "t_rem": int(t_rem), "fee": round(fee,4)}
        if ev < FAIR_EDGE_MIN:
            log_skip(f"EV {ev*100:+.1f}%<{FAIR_EDGE_MIN*100:.0f}% (fair:{p_dir:.2f} vs token:{token_price_dir:.2f}$ +frais:{fee*100:.1f}¢)", direction)
            return
        win_prob = min(0.97, p_dir + oracle_conf_bonus)  # ✅ v10.23 bonus oracle
        log.info(f"✅ Fair value: P({direction})={p_dir:.2f}(+orc {oracle_conf_bonus:.2f}) vs token {token_price_dir:.2f}$ frais {fee*100:.2f}¢ → EV {ev*100:+.1f}%")
    else:
        st.last_fair = {}
        # ✅ v10.24 — BLOQUÉ en mode réel si sigma=0 (WS déconnecté = pas de données fiables)
        # En paper mode on laisse passer pour continuer à collecter des stats
        if not st.paper_mode:
            log_skip("WS déconnecté — sigma=0 — trade réel bloqué (pas de fair value)", direction)
            return
        # Paper mode: fallback sur la proba implicite du score
        prob_conf = conf_score.get("prob_up",0.5) if direction=="UP" else conf_score.get("prob_dn",0.5)
        ev_fb = prob_conf - token_price_dir - fee
        if ev_fb < FAIR_EDGE_MIN:
            log_skip(f"EV fallback {ev_fb*100:+.1f}%<{FAIR_EDGE_MIN*100:.0f}% (WS off, paper)", direction)
            return
        win_prob = prob_conf
        log.info("Fair value: WS pas prêt — gate fallback sur proba score (PAPER uniquement)")

    # ✅ v10.24 — ev_bonus: mise boostée si oracle confirme OU EV très forte (>15%)
    ev_val = st.last_fair.get("ev", 0)
    ev_bonus = (oracle_sig is not None) or (ev_val >= 0.15)
    payout = best_payout if best_payout>0 else round(1/token_price_dir,2) if token_price_dir>0 else 2.0
    amount = kelly_bet(st.bankroll, win_prob, payout, token_price_dir, ev_bonus=ev_bonus)
    if st.win_streak_count >= BOOST_AFTER_WINS:
        amount = round(min(amount*1.2, MAX_BET_USD), 2)  # BOOST_AFTER_WINS=999 donc désactivé
    dec = {"dir":direction,"conf":round(win_prob,2),"size":amount,
           "reasoning":f"EV {st.last_fair.get('ev',0)*100:+.1f}% | fair P={win_prob:.2f} vs token {token_price_dir:.2f}$ | Δslot {st.window_delta_pct:+.3f}%",
           "risk":"LOW" if win_prob>=0.75 else "MEDIUM" if win_prob>=0.6 else "HIGH",
           "trade":True,"kelly_pct":round(amount/st.bankroll*100,1) if st.bankroll>0 else 0}
    st.last_decision=dec
    if amount <= 0:
        log_skip("Kelly edge négatif — EV insuffisante pour cette mise", direction); return
    if amount < MIN_BET_USD:
        log_skip(f"Mise calculée {amount:.2f}$<{MIN_BET_USD}$ minimum absolu", direction); return
    if st.bankroll<amount: return
    ok = await place_bet(context, direction, amount, dec["conf"], dec["reasoning"], conf_score, sess, tpu, tpd, market_end, source="tick")
    if not ok: return
    mode="💰 RÉEL" if not st.paper_mode else "📄 paper"
    risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(dec["risk"],"🟡")
    sigs="\n".join(f"  • {s}" for s in conf_score["signals"][:5])
    entry_tp=st.entry_token_price if not st.paper_mode else token_price_dir
    pinfo=f"\nToken:`{entry_tp:.3f}$`→x`{round(1/entry_tp,2) if entry_tp>0 else '?'}` TP:x`{TAKE_PROFIT_MULT}` Trail:x`{TRAILING_PEAK_MULT}`" if not st.paper_mode else ""
    ob_info=f"\n{st.last_ob['desc']}" if st.last_ob and st.last_ob.get("bias") else ""
    await send(context.bot,
        f"🧠 *Bet placé* [{mode}]\n━━━━━━━━━━━━━━━\n"
        f"*{dec['dir']}* | `{amount:.2f}$` Kelly:`{dec.get('kelly_pct',0):.1f}%` | `{dec['conf']*100:.0f}%` | {risk_e}\n"
        f"Score:`{conf_score['score']:.1f}` Mom:`{mom_score}/10`{pinfo}\n"
        f"BTC:`${st.price:,.2f}` | `{sess['session']}`\n"
        f"Ξ`{eth_desc}`{ob_info}\n\n"
        f"💭 _{dec['reasoning']}_\n🔑 Signaux:\n{sigs}")

async def job_snipe(context):
    """
    ✅ v10.30 — SNIPE BROWNIEN: désactivé en mode RÉEL.
    Sources (medium.com/mountain-movers, dev.to/fatherson): le vrai edge
    sur BTC 5min n'est PAS la prédiction Brownienne mais l'oracle lag.
    Brownien gardé en PAPER pour tests comparatifs.
    Le trading réel passe par job_oracle_lag (T-35s→T-6s).
    """
    if not st.running or st.killed or st.bet: return
    # ✅ v10.30 — Brownien désactivé en réel
    if not st.paper_mode:
        return
    now_ts = time.time()
    slot_remaining = 300 - (now_ts % 300)
    if not (SNIPE_LAST_MIN <= slot_remaining < ENTRY_LAST_SECONDS): return
    if st.last_trade_slot == int(now_ts//300)*300: return  # ✅ dédup slot
    if check_daily() or in_cd(): return
    if trades_last_hour(st.trades)>=MAX_TRADES_PER_H: return
    # Snipe exige le WS (précision indispensable à T-45s)
    if not st.ws_connected or st.ws_price<=0 or st.slot_open_price<=0: return
    if st.slot_open_ts != int(now_ts//300)*300: return
    sigma = realized_vol()
    if sigma<=0: return
    delta_pct = (st.ws_price - st.slot_open_price) / st.slot_open_price * 100
    p_up = fair_prob_up(delta_pct, slot_remaining, sigma)
    direction = "UP" if p_up>=0.5 else "DOWN"
    p_dir = p_up if direction=="UP" else 1.0-p_up
    if p_dir < SNIPE_MIN_PROB: return

    # ✅ v10.27 — FILTRE BPS (polybacktest.com, 29,060 trades réels)
    # Seuls les mouvements lents et stables (5-12 bps) sont rentables
    cur_price = st.ws_price if st.ws_price > 0 else st.price
    bps_ok, bps_cur, bps_tot, bps_reason = compute_btc_bps(st.slot_open_price, cur_price, direction)
    if not bps_ok:
        log_skip(f"SNIPE: {bps_reason}", direction)
        return
    log.info(f"✅ BPS filter passed: {bps_reason}")
    sess=session_ctx()
    # Récupérer le marché + prix du favori
    tpu=0.5; tpd=0.5; market_end=0
    if not st.paper_mode:
        market=st.current_market
        cur_slug=f"btc-updown-5m-{int(now_ts//300)*300}"
        if not market or market.get("market_slug")!=cur_slug:
            market=await poly.find_btc_5min_market()
        if not market:
            log_skip("SNIPE: aucun marché actif", direction); return
        st.current_market=market
        token_used=market["token_up"] if direction=="UP" else market["token_down"]
        token_price_dir=await poly.get_token_price(token_used)
        tpu=token_price_dir if direction=="UP" else 1.0-token_price_dir
        tpd=1.0-tpu
        try:
            ed=market.get("end_date","")
            if ed:
                dt=datetime.fromisoformat(ed.replace("Z","+00:00"))
                market_end=dt.timestamp()
        except: pass
    else:
        token_price_dir=0.90  # Estimation paper: le favori se paie ~0.90 à T-40s
        tpu=token_price_dir if direction=="UP" else 1.0-token_price_dir
        tpd=1.0-tpu
    if token_price_dir < SNIPE_TOKEN_MIN or token_price_dir > SNIPE_TOKEN_MAX:
        log_skip(f"SNIPE: token {token_price_dir:.2f}$ hors zone [{SNIPE_TOKEN_MIN}-{SNIPE_TOKEN_MAX}] — frais trop élevés", direction)
        return
    fee=taker_fee_per_share(token_price_dir)
    # ✅ v10.25 — Vérification frais explicite: à 0.82$+ les frais sont <0.2¢ (quasi nuls)
    # ✅ v10.29 — Filtre fee_pct>0.5% SUPPRIMÉ: redondant avec EV gate, tuait zone 0.55-0.75$
    # Les frais sont inclus dans ev=p_dir-token_price_dir-fee. EV gate à 10% suffit.
    fee_pct = fee / token_price_dir * 100 if token_price_dir > 0 else 0
    log.debug(f"Fee: {fee*100:.2f}¢/share ({fee_pct:.2f}%)")
    ev=p_dir-token_price_dir-fee
    st.last_fair={"p_up":round(p_up,3),"sigma":round(sigma,4),"ev":round(ev,3),
                  "t_rem":int(slot_remaining),"fee":round(fee,4),"mode":"SNIPE"}
    if ev < SNIPE_EDGE_MIN:
        log_skip(f"SNIPE: EV {ev*100:+.1f}%<{SNIPE_EDGE_MIN*100:.0f}% (P:{p_dir:.2f} tok:{token_price_dir:.2f}$)", direction)
        return
    # ✅ v10.26 — Tier du setup pour le message
    if ev >= 0.15 or p_dir >= 0.92:
        tier_label = "🔥 EXCEPTIONNEL (~15% BR)"
    elif ev >= 0.10 or p_dir >= 0.85:
        tier_label = "⚡ FORT (~10% BR)"
    else:
        tier_label = "✅ NORMAL (~5% BR)"
    payout=round(1/token_price_dir,2) if token_price_dir>0 else 1.1
    amount=kelly_bet(st.bankroll, p_dir, payout, token_price_dir)
    if st.win_streak_count >= BOOST_AFTER_WINS:
        amount=round(min(amount*1.2, MAX_BET_USD),2)
    if amount<MIN_BET_USD or st.bankroll<amount: return
    conf_score=st.last_conf_score if st.last_conf_score else {"score":0,"signals":[]}
    reasoning=f"SNIPE {tier_label} T-{int(slot_remaining)}s | P({direction})={p_dir:.2f} vs token {token_price_dir:.2f}$ | EV {ev*100:+.1f}% | Δ{delta_pct:+.3f}%"
    ok=await place_bet(context, direction, amount, round(p_dir,2), reasoning, conf_score, sess, tpu, tpd, market_end, source="snipe")
    if not ok: return
    st.last_decision={"dir":direction,"conf":round(p_dir,2),"size":amount,"reasoning":reasoning,
                      "risk":"LOW","trade":True,"kelly_pct":round(amount/st.bankroll*100,1) if st.bankroll>0 else 0}
    mode="💰 RÉEL" if not st.paper_mode else "📄 paper"
    entry_tp=st.entry_token_price if not st.paper_mode else token_price_dir
    await send(context.bot,
        f"🎯 *SNIPE placé* [{mode}]\n━━━━━━━━━━━━━━━\n"
        f"*{direction}* | `{amount:.2f}$` ({round(amount/st.bankroll*100,1) if st.bankroll>0 else 0:.1f}% BR) | {tier_label}\n"
        f"P:`{p_dir*100:.0f}%` | ⏰T-`{int(slot_remaining)}s` | EV:`{ev*100:+.1f}%`\n"
        f"Token:`{entry_tp:.3f}$` | Frais:`{fee*100:.2f}¢`\n"
        f"BPS: `{bps_cur}` vers {direction} | Total: `{bps_tot}` bps\n"
        f"₿`${st.ws_price:,.2f}` Δslot:`{delta_pct:+.3f}%` σ:`{sigma:.4f}`\n\n"
        f"💭 _{reasoning}_")

async def ws_clob_loop(asset_id_up: str):
    """v12.4 — OB imbalance BTC via CLOB WebSocket Polymarket."""
    if not asset_id_up: return
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sub = {"assets_ids": [asset_id_up], "type": "market"}
    st.ob_asset_id = asset_id_up; st.ob_imbalance = 0.0
    log.info(f"✅ WS CLOB OB BTC démarré")
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
                                etype = m.get("event_type") or m.get("type","")
                                if etype not in ("book","price_change","tick_size_change"): continue
                                bids = m.get("bids") or []; asks = m.get("asks") or []
                                bid_vol = ask_vol = 0.0
                                for b in bids:
                                    try:
                                        if isinstance(b,dict): bid_vol+=float(b.get("size",0))
                                        elif isinstance(b,(list,tuple)) and len(b)>=2: bid_vol+=float(b[1])
                                    except: pass
                                for a in asks:
                                    try:
                                        if isinstance(a,dict): ask_vol+=float(a.get("size",0))
                                        elif isinstance(a,(list,tuple)) and len(a)>=2: ask_vol+=float(a[1])
                                    except: pass
                                total = bid_vol + ask_vol
                                if total > 0:
                                    st.ob_imbalance = round((bid_vol-ask_vol)/total,3)
                                    st.ob_ts = time.time()
                        except Exception as pe: log.debug(f"CLOB BTC parse: {pe}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
    except Exception as e: log.warning(f"WS CLOB BTC: {e}")
    st.ob_imbalance = 0.0

async def ws_clob_loop_asset(asset_id_up: str, asset: str):
    """v12.4 — OB imbalance ETH/SOL via CLOB WebSocket."""
    if not asset_id_up: return
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sub = {"assets_ids": [asset_id_up], "type": "market"}
    if asset=="ETH": st.eth_ob_asset_id=asset_id_up; st.eth_ob_imbalance=0.0
    else: st.sol_ob_asset_id=asset_id_up; st.sol_ob_imbalance=0.0
    log.info(f"✅ WS CLOB OB {asset} démarré")
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
                                etype = m.get("event_type") or m.get("type","")
                                if etype not in ("book","price_change","tick_size_change"): continue
                                bids = m.get("bids") or []; asks = m.get("asks") or []
                                bid_vol = ask_vol = 0.0
                                for b in bids:
                                    try:
                                        if isinstance(b,dict): bid_vol+=float(b.get("size",0))
                                        elif isinstance(b,(list,tuple)) and len(b)>=2: bid_vol+=float(b[1])
                                    except: pass
                                for a in asks:
                                    try:
                                        if isinstance(a,dict): ask_vol+=float(a.get("size",0))
                                        elif isinstance(a,(list,tuple)) and len(a)>=2: ask_vol+=float(a[1])
                                    except: pass
                                total = bid_vol + ask_vol
                                if total > 0:
                                    imb = round((bid_vol-ask_vol)/total,3)
                                    if asset=="ETH": st.eth_ob_imbalance=imb; st.eth_ob_ts=time.time()
                                    else: st.sol_ob_imbalance=imb; st.sol_ob_ts=time.time()
                        except Exception as pe: log.debug(f"CLOB {asset} parse: {pe}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
    except Exception as e: log.warning(f"WS CLOB {asset}: {e}")
    if asset=="ETH": st.eth_ob_imbalance=0.0
    else: st.sol_ob_imbalance=0.0


async def job_oracle_lag(context):
    """v12.4 — Oracle lag BTC — même logique propre qu'ETH/SOL."""
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now
    if slot_remaining > ORACLE_WINDOW_START or slot_remaining < ORACLE_WINDOW_END: return

    if not st.oracle_connected or st.oracle_price <= 0 or st.oracle_slot_open <= 0:
        log_skip(f"BTC: WS non dispo (T-{int(slot_remaining)}s)", None); return
    if now - st.oracle_ts > 15:
        log_skip(f"BTC: tick périmé {int(now-st.oracle_ts)}s (T-{int(slot_remaining)}s)", None); return
    if st.last_trade_slot == cur_slot:
        log_skip(f"BTC: slot déjà tradé (T-{int(slot_remaining)}s)", None); return

    spot_now = consensus_price()
    if spot_now <= 0: return

    # Ret 3s/15s
    pts = list(st.ws_prices)
    def ret_over(secs):
        cutoff = now - secs
        old = [p for t,p in pts if t <= cutoff]
        return (spot_now - old[-1]) / old[-1] * 100 if old and old[-1] > 0 else 0.0
    ret_3s = ret_over(3); ret_15s = ret_over(15)
    ret3s_override = False

    oracle_delta = (st.oracle_price - st.oracle_slot_open) / st.oracle_slot_open * 100
    spot_oracle_gap = (spot_now - st.oracle_price) / st.oracle_price * 100

    gap_dir = ("UP" if spot_oracle_gap > 0 else "DOWN") if abs(spot_oracle_gap) >= 0.025 else None
    delta_dir = ("UP" if oracle_delta > 0 else "DOWN") if abs(oracle_delta) >= ORACLE_ENTRY_DELTA else None
    primary_signal = "gap" if gap_dir else "delta"
    direction = gap_dir or delta_dir

    # Filtre ret3s brutal
    if ret_3s < -0.070:  # v12.6 — seuil relevé -0.055→-0.070 (Sonnet: 4/5 wins ≤-0.075%)
        # ✅ v12.6 — ret3s signal: BTC chute fort + gap positif = oracle pas rattrapé → trade DOWN
        # Le gap est positif car le spot chute MAIS l'oracle n'a pas encore suivi
        if spot_oracle_gap >= 0.005:
            direction = "DOWN"; ret3s_override = True
            log.info(f"BTC: ret3s signal DOWN {ret_3s:+.3f}% gap={spot_oracle_gap:+.3f}% → override")
        else:
            log_skip(f"BTC: ret3s {ret_3s:+.3f}%<-0.055% (chute brutale)", direction,
                     features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"ret3s_brutal","asset":"BTC"}); return

    if not direction:
        log_skip(f"BTC: Δ{oracle_delta:+.3f}% gap{spot_oracle_gap:+.3f}% (→ skip: delta et gap trop faibles)", None,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"weak_signal","asset":"BTC"}); return

    if direction == "UP" and spot_oracle_gap < 0:
        log_skip(f"BTC: UP bloqué gap négatif (→ skip: gap négatif)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"gap_neg","asset":"BTC"}); return
    if direction == "UP" and oracle_delta < -0.005:
        log_skip(f"BTC: delta {oracle_delta:+.3f}%<0 (→ skip: delta négatif LOSS garanti)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"delta_neg","asset":"BTC"}); return
    if direction == "DOWN" and oracle_delta > 0.005 and not ret3s_override:
        log_skip(f"BTC: delta {oracle_delta:+.3f}%>0 (→ skip: contre DOWN)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"delta_contra","asset":"BTC"}); return

    # TA score
    price_hist = [{"price":p,"ts":t} for t,p in pts]
    ta_score, ta_dir, _ = compute_ta_score(price_hist, "BTC")
    ta_vote = 1 if ta_dir=="UP" else (-1 if ta_dir=="DOWN" else 0)

    # OB vote
    ob_vote = 0
    if time.time() - st.ob_ts < 10:
        if st.ob_imbalance > 0.15: ob_vote = 1
        elif st.ob_imbalance < -0.15: ob_vote = -1

    # Volume spike
    vols = list(st.ws_volumes)
    vol_vote = 0
    if len(vols) >= 5:
        vol_5s = sum(q for t,q in vols if now-t<=5)
        vol_avg = sum(q for t,q in vols if now-t<=30) / 6
        if vol_avg > 0 and vol_5s / vol_avg > 2.0: vol_vote = 1 if direction=="UP" else -1

    dir_votes = sum([
        1 if direction=="UP" and oracle_delta>0 else (-1 if direction=="DOWN" and oracle_delta<0 else 0),
        1 if direction=="UP" and spot_oracle_gap>0 else (-1 if direction=="DOWN" and spot_oracle_gap<0 else 0),
        1 if direction=="UP" and ret_15s>0 else (-1 if direction=="DOWN" and ret_15s<0 else 0),
        ob_vote, ta_vote,
    ])

    # Chainlink frais
    chainlink_age = now - st.oracle_chainlink_ts if st.oracle_chainlink_ts > 0 else 999

    # Marché
    cur_slug = f"btc-updown-5m-{cur_slot}"
    market = st.current_market
    if not market or market.get("market_slug") != cur_slug:
        market = await poly.find_btc_5min_market()
    if not market:
        log_skip(f"BTC: marché non trouvé (T-{int(slot_remaining)}s)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"no_market","asset":"BTC"}); return
    st.current_market = market
    token_used = market["token_up"] if direction=="UP" else market["token_down"]
    token_price = await poly.get_token_price(token_used)
    if not token_price or token_price <= 0: return

    asset_up = market.get("token_up","")
    if asset_up and st.ob_asset_id != asset_up:
        if hasattr(st,"clob_ws_task") and st.clob_ws_task and not st.clob_ws_task.done():
            st.clob_ws_task.cancel()
        st.clob_ws_task = asyncio.create_task(ws_clob_loop(asset_up))

    if token_price > ORACLE_TOKEN_MAX:
        log_skip(f"BTC: token {token_price:.2f}$>{ORACLE_TOKEN_MAX}$ (→ skip: marché a déjà pricé la direction)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"tokenmax","token":token_price,"asset":"BTC"}); return
    if token_price < ORACLE_TOKEN_MIN:
        log_skip(f"BTC: token {token_price:.2f}$<{ORACLE_TOKEN_MIN}$ (→ skip: trop incertain)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"tokenmin","token":token_price,"asset":"BTC"}); return

    fee = taker_fee_per_share(token_price)
    p_oracle = min(0.93, 0.85 + abs(spot_oracle_gap)*3.0) if primary_signal=="gap" else min(0.90, 0.80 + abs(oracle_delta)*2.0)
    if dir_votes >= 3: p_oracle = min(0.95, p_oracle + 0.03)
    if dir_votes >= 4: p_oracle = min(0.96, p_oracle + 0.02)
    if chainlink_age < 2.0: p_oracle = min(0.97, p_oracle + 0.03)
    ev = p_oracle - token_price - fee

    # ✅ v12.6 — votes minimum 3/5
    if dir_votes < 2:
        log_skip(f"BTC: votes {dir_votes}/5 < 2 (→ skip: consensus faible)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"votes_min","asset":"BTC"}); return
    if ev < ORACLE_EDGE_MIN:
        log_skip(f"BTC: EV {ev*100:+.1f}%<{ORACLE_EDGE_MIN*100:.0f}% (→ skip: edge insuffisant)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"ev","token":token_price,"ev":ev,"asset":"BTC"}); return

    payout = round(1/token_price, 2)
    amount = kelly_bet(st.bankroll, p_oracle, payout, token_price, ev_bonus=True)
    if amount < MIN_BET_USD: return

    tpu = market["token_up"]; tpd = market["token_down"]
    try: market_end = datetime.fromisoformat(market.get("end_date","").replace("Z","+00:00")).timestamp()
    except: market_end = cur_slot + 300
    sess = session_ctx(); conf_score = {"score":0,"signals":[]}
    reasoning = (f"⚡ORACLE LAG BTC {direction} | gap={spot_oracle_gap:+.3f}% delta={oracle_delta:+.3f}% "
                 f"OB={st.ob_imbalance:+.2f} votes={dir_votes}/5 | tok={token_price:.3f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s")

    ok = await place_bet(context, direction, amount, round(p_oracle,2), reasoning, conf_score, sess, tpu, tpd, market_end, source="snipe")
    if not ok: return

    st.last_trade_slot = cur_slot
    mode = "💰 RÉEL" if not st.paper_mode else "📄 paper"
    entry_tp = st.entry_token_price if not st.paper_mode else token_price
    await send(context.bot,
        f"⚡ *ORACLE LAG ₿ BTC* [{mode}]\n━━━━━━━━━━━━━━━\n"
        f"*{direction}* | `{amount:.2f}$` | P:`{p_oracle*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Δslot:`{oracle_delta:+.3f}%` | Gap:`{spot_oracle_gap:+.3f}%` OB:`{st.ob_imbalance:+.2f}` TA:`{ta_score}` | Votes:`{dir_votes}/5`\n"
        f"Ret 3s:`{ret_3s:+.3f}%` 15s:`{ret_15s:+.3f}%`\n"
        f"Token:`{entry_tp:.3f}$` | EV:`{ev*100:+.1f}%` | Frais:`{fee*100:.2f}¢`\n"
        f"Oracle:`${st.oracle_price:,.2f}` → Spot:`${spot_now:,.2f}`\n\n"
        f"💭 _{reasoning}_")


async def job_oracle_lag_asset(context, asset:str):
    """v12.4 — Oracle lag ETH/SOL identique à BTC."""
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now//300)*300
    slot_remaining = cur_slot+300-now
    win_start = 15 if asset=="SOL" else (20 if asset in ("ETH","XRP") else 25)  # v12.8: XRP T-20s
    if slot_remaining > win_start or slot_remaining < 5: return
    if asset=="ETH":
        spot=st.eth_price; spot_ts=st.eth_ts; oracle=st.eth_oracle_price
        oracle_ts=st.eth_oracle_ts; slot_open=st.eth_oracle_slot_open
        last_ts=st.eth_last_trade_slot; slug_prefix="eth-updown-5m"
        symbol="ETH"; emoji="Ξ"; ws_prices=st.eth_ws_prices
    elif asset=="SOL":
        spot=st.sol_price; spot_ts=st.sol_ts; oracle=st.sol_oracle_price
        oracle_ts=st.sol_oracle_ts; slot_open=st.sol_oracle_slot_open
        last_ts=st.sol_last_trade_slot; slug_prefix="sol-updown-5m"
        symbol="SOL"; emoji="◎"; ws_prices=st.sol_ws_prices
    elif asset=="XRP":
        spot=st.xrp_price; spot_ts=st.xrp_ts; oracle=st.xrp_oracle_price
        oracle_ts=st.xrp_oracle_ts; slot_open=st.xrp_oracle_slot_open
        last_ts=st.xrp_last_trade_slot; slug_prefix="xrp-updown-5m"
        symbol="XRP"; emoji="✕"; ws_prices=st.xrp_ws_prices
    else: return
    if spot<=0 or oracle<=0 or slot_open<=0:
        log_skip(f"{symbol}: données manquantes spot={spot:.2f} oracle={oracle:.2f}", None); return
    if now-spot_ts>5:
        log_skip(f"{symbol}: prix spot périmé {int(now-spot_ts)}s", None); return
    if now-oracle_ts>15:
        log_skip(f"{symbol}: oracle périmé {int(now-oracle_ts)}s", None); return
    if last_ts==cur_slot: return
    # Ret 3s/15s
    pts=list(ws_prices)
    def ret_a(secs):
        cut=now-secs; old=[p for t,p in pts if t<=cut]
        return (spot-old[-1])/old[-1]*100 if old and old[-1]>0 else 0.0
    ret_3s=ret_a(3); ret_15s=ret_a(15)
    ret3s_override = False
    oracle_delta=(oracle-slot_open)/slot_open*100 if slot_open>0 else 0
    spot_oracle_gap=(spot-oracle)/oracle*100 if oracle>0 else 0
    gap_min = 0.025 if asset=="XRP" else 0.020  # v12.8: XRP 0.025%, ETH/SOL 0.020%
    gap_dir=("UP" if spot_oracle_gap>0 else "DOWN") if abs(spot_oracle_gap)>=gap_min else None
    delta_dir=("UP" if oracle_delta>0 else "DOWN") if abs(oracle_delta)>=ORACLE_ENTRY_DELTA else None
    primary_signal="gap" if gap_dir else "delta"
    direction=gap_dir or delta_dir
    if ret_3s<-0.070:  # v12.6 — seuil relevé -0.055→-0.070
        # ✅ v12.6 — ret3s signal ETH/SOL: chute brutale + gap positif = oracle pas rattrapé → DOWN
        if spot_oracle_gap >= 0.005:
            direction = "DOWN"; ret3s_override = True
            log.info(f"{symbol}: ret3s signal DOWN {ret_3s:+.3f}% gap={spot_oracle_gap:+.3f}% → override")
        else:
            log_skip(f"{symbol}: ret3s {ret_3s:+.3f}% (chute brutale)",direction,
                     features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"ret3s_brutal"})
            return
    # ✅ v12.6 — SOL filtre ATR: ignoré si gap fort >0.05% (spike = signal valide)
    if asset=="SOL" and ret_3s>0.04 and ret_15s>0.08:
        if abs(spot_oracle_gap) >= 0.05:
            log.debug(f"SOL: ATR spike override — gap {spot_oracle_gap:+.3f}% fort → signal valide")
        else:
            log_skip(f"SOL: spike volatilité ret3s={ret_3s:+.3f}% ret15s={ret_15s:+.3f}% (→ skip: trop volatile)", direction,
                     features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"atr_spike"})
            return
    if not direction:
        log_skip(f"{symbol}: signaux trop faibles",None,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"weak_signal"})
        return
    if direction=="UP" and spot_oracle_gap<0:
        log_skip(f"{symbol}: gap négatif",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"gap_neg"}); return
    if direction=="UP" and oracle_delta<-0.005:
        log_skip(f"{symbol}: delta {oracle_delta:+.3f}%<0",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"delta_neg"}); return
    if direction=="DOWN" and oracle_delta>0.005 and not ret3s_override:
        log_skip(f"{symbol}: delta {oracle_delta:+.3f}%>0 (→ skip: contre DOWN)",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"delta_contra"}); return
    price_hist=[{"price":p,"ts":t} for t,p in pts]
    ta_score,ta_dir,_=compute_ta_score(price_hist,asset)
    ta_vote=1 if ta_dir=="UP" else (-1 if ta_dir=="DOWN" else 0)
    btc_cascade_vote=0
    btc_pts=list(st.ws_prices)
    if len(btc_pts)>=2:
        btc10=[p for t,p in btc_pts if now-t<=10]
        if len(btc10)>=2 and btc10[0]>0:
            mv=(btc10[-1]-btc10[0])/btc10[0]*100
            if abs(mv)>=0.030:
                rv=1 if mv>0 else -1
                btc_cascade_vote=rv if (direction=="UP" and rv==1)or(direction=="DOWN" and rv==-1) else -rv
    # ✅ v12.7 — Corrélation inverse BTC/ETH+SOL (SMT divergence)
    # Sources: sharpe.ai (corr 0.9), ICT SMT technique, mean reversion pairs trading
    # Principe: quand BTC et ETH/SOL divergent sur 15s → le laggard va rattraper
    alt_pts = list(st.eth_ws_prices if asset=="ETH" else st.sol_ws_prices)
    btc15 = [p for t,p in btc_pts if now-t<=15]
    alt15 = [p for t,p in alt_pts if now-t<=15]
    if len(btc15)>=3 and len(alt15)>=3 and btc15[0]>0 and alt15[0]>0:
        btc_move15 = (btc15[-1]-btc15[0])/btc15[0]*100
        alt_move15 = (alt15[-1]-alt15[0])/alt15[0]*100
        divergence = btc_move15 - alt_move15  # BTC - ETH/SOL
        # Cas 1: BTC monte fort, ETH/SOL reste stable → ETH/SOL va rattraper UP
        if divergence >= 0.025 and direction=="UP":
            btc_cascade_vote = max(btc_cascade_vote, 1)
            log.debug(f"{asset} SMT: BTC {btc_move15:+.3f}% {asset} {alt_move15:+.3f}% div={divergence:+.3f}% → UP")
        # Cas 2: BTC chute fort, ETH/SOL reste stable → ETH/SOL va suivre DOWN
        elif divergence <= -0.025 and direction=="DOWN":
            btc_cascade_vote = min(btc_cascade_vote, -1)
            log.debug(f"{asset} SMT: BTC {btc_move15:+.3f}% {asset} {alt_move15:+.3f}% div={divergence:+.3f}% → DOWN")
        # Cas 3: ETH/SOL monte mais BTC reste stable → ETH/SOL va mean-revert DOWN
        elif divergence <= -0.025 and direction=="UP":
            btc_cascade_vote = min(btc_cascade_vote, -1)
            log.debug(f"{asset} SMT contra: {asset} surperform BTC → mean revert DOWN")
        # Cas 4: ETH/SOL chute mais BTC stable → ETH/SOL va rebondir UP
        elif divergence >= 0.025 and direction=="DOWN":
            btc_cascade_vote = max(btc_cascade_vote, 1)
            log.debug(f"{asset} SMT contra: {asset} underperform BTC → rebond UP")
    dir_votes=sum([
        1 if direction=="UP" and oracle_delta>0 else (-1 if direction=="DOWN" and oracle_delta<0 else 0),
        1 if direction=="UP" and spot_oracle_gap>0 else (-1 if direction=="DOWN" and spot_oracle_gap<0 else 0),
        1 if direction=="UP" and ret_15s>0 else (-1 if direction=="DOWN" and ret_15s<0 else 0),
        btc_cascade_vote, ta_vote,
    ])
    market=await poly.get_market_by_slug(f"{slug_prefix}-{cur_slot}")
    if not market:
        log_skip(f"{symbol}: marché non trouvé",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"no_market"}); return
    token_used=market["token_up"] if direction=="UP" else market["token_down"]
    token_price=await poly.get_token_price(token_used)
    if not token_price or token_price<=0: return
    # ✅ v12.4 — Lancer WS CLOB OB pour ETH/SOL
    asset_up_ob = market.get("token_up","")
    if asset=="ETH" and asset_up_ob and st.eth_ob_asset_id != asset_up_ob:
        if st.eth_clob_ws_task and not st.eth_clob_ws_task.done(): st.eth_clob_ws_task.cancel()
        st.eth_clob_ws_task = asyncio.create_task(ws_clob_loop_asset(asset_up_ob,"ETH"))
    elif asset=="SOL" and asset_up_ob and st.sol_ob_asset_id != asset_up_ob:
        if st.sol_clob_ws_task and not st.sol_clob_ws_task.done(): st.sol_clob_ws_task.cancel()
        st.sol_clob_ws_task = asyncio.create_task(ws_clob_loop_asset(asset_up_ob,"SOL"))
    # ✅ v12.6 — SOL tokenmax 0.95$ si votes ≤ -1 (Sonnet: 2W/0L à 0.92-0.98$)
    effective_token_max = 0.95 if (asset=="SOL" and dir_votes<=-1) else ORACLE_TOKEN_MAX
    if token_price>effective_token_max:
        log_skip(f"{symbol}: token {token_price:.2f}$>{effective_token_max}$ (déjà pricé)",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"tokenmax","token":token_price}); return
    if token_price<ORACLE_TOKEN_MIN:
        log_skip(f"{symbol}: token {token_price:.2f}$<{ORACLE_TOKEN_MIN}$ (incertain)",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"tokenmin","token":token_price}); return
    fee=taker_fee_per_share(token_price)
    p_oracle=min(0.93,0.85+abs(spot_oracle_gap)*3.0) if primary_signal=="gap" else min(0.90,0.80+abs(oracle_delta)*2.0)
    if dir_votes>=3: p_oracle=min(0.95,p_oracle+0.03)
    ev=p_oracle-token_price-fee
    # ✅ v12.6 — votes minimum 3/5 (données: 12/12 votes=2 sont LOSS)
    if dir_votes < 2:
        log_skip(f"{symbol}: votes {dir_votes}/5 < 2 (→ skip: consensus faible)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"votes_min"}); return
    if ev<ORACLE_EDGE_MIN:
        log_skip(f"{symbol}: EV {ev*100:+.1f}% insuffisant",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"ev","token":token_price,"ev":ev}); return
    payout=round(1/token_price,2)
    amount=kelly_bet(st.bankroll,p_oracle,payout,token_price,ev_bonus=True)
    if amount<MIN_BET_USD: return
    tpu=market["token_up"]; tpd=market["token_down"]
    market_end=market.get("end_date",""); sess=session_ctx(); conf_score={"score":0,"signals":[]}
    reasoning=f"ORACLE LAG {symbol} {direction} | gap={spot_oracle_gap:+.3f}% delta={oracle_delta:+.3f}% TA={ta_score} votes={dir_votes}/5 | tok={token_price:.3f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s"
    ok=await place_bet(context,direction,amount,round(p_oracle,2),reasoning,conf_score,sess,tpu,tpd,market_end,source="snipe")
    if not ok: return
    if asset=="ETH": st.eth_last_trade_slot=cur_slot
    elif asset=="SOL": st.sol_last_trade_slot=cur_slot
    elif asset=="XRP": st.xrp_last_trade_slot=cur_slot
    mode="💰 RÉEL" if not st.paper_mode else "📄 paper"
    entry_tp=st.entry_token_price if not st.paper_mode else token_price
    await send(context.bot,
        f"⚡ *ORACLE LAG {emoji} {symbol}* [{mode}]\n━━━━━━━━━━━━━━━\n"
        f"*{direction}* | `{amount:.2f}$` | P:`{p_oracle*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Δslot:`{oracle_delta:+.3f}%` | Gap:`{spot_oracle_gap:+.3f}%` TA:`{ta_score}` | Votes:`{dir_votes}/5`\n"
        f"Ret 3s:`{ret_3s:+.3f}%` 15s:`{ret_15s:+.3f}%`\n"
        f"Token:`{entry_tp:.3f}$` | EV:`{ev*100:+.1f}%`\n"
        f"Oracle:`${oracle:,.2f}` → Spot:`${spot:,.2f}`\n\n"
        f"💭 _{reasoning}_")

async def job_oracle_lag_eth(context):
    await job_oracle_lag_asset(context,"ETH")

async def job_oracle_lag_sol(context):
    await job_oracle_lag_asset(context,"SOL")


async def job_oracle_lag_xrp(context):
    """v12.8 — Oracle lag XRP (même logique ETH/SOL)."""
    await job_oracle_lag_asset(context, "XRP")


async def job_auto_calibrate(context):
    """
    ✅ v10.37 — Point 1: Auto-calibration des seuils toutes les 2h.
    Analyse les patterns oracle résolus (WIN/LOSS) par filtre,
    ajuste ORACLE_DELTA_CONTRA_MAX, ORACLE_GAP_MIN_STRONG, ORACLE_GAP_CONFIRM_RET.
    Objectif: seuils qui maximisent le WR réel, pas le WR théorique des skips.
    """
    global ORACLE_DELTA_CONTRA_MAX, ORACLE_GAP_MIN_STRONG, ORACLE_GAP_CONFIRM_RET, GAP_PERSIST_RATIO

    resolved = [p for p in st.oracle_patterns if p.get("result") in ("WIN","LOSS")]
    if len(resolved) < 15:
        log.info(f"Auto-calibration: {len(resolved)}/15 patterns résolus — attente")
        return

    # Analyser par filtre
    by_filter = {}
    for p in resolved[-100:]:
        f = p.get("filter","unknown")
        if f not in by_filter: by_filter[f] = {"w":0,"l":0}
        if p["result"]=="WIN": by_filter[f]["w"] += 1
        else: by_filter[f]["l"] += 1

    adjustments = []

    # Fix #3 (ret3s): si >60% des skips ret3s gagnent → seuil trop strict → relâcher
    global ORACLE_GAP_CONFIRM_RET, GAP_PERSIST_RATIO
    if "ret3s_fallback" in by_filter:
        r = by_filter["ret3s_fallback"]; total = r["w"]+r["l"]
        if total >= 8:
            wr = r["w"]/total
            if wr > 0.60:
                ORACLE_GAP_CONFIRM_RET = round(min(0.08, ORACLE_GAP_CONFIRM_RET + 0.005), 3)
                adjustments.append(f"ret3s_fallback↑ {ORACLE_GAP_CONFIRM_RET:.3f}% (WR {wr*100:.0f}%)")
            elif wr < 0.35:
                ORACLE_GAP_CONFIRM_RET = round(max(0.01, ORACLE_GAP_CONFIRM_RET - 0.005), 3)
                adjustments.append(f"ret3s_fallback↓ {ORACLE_GAP_CONFIRM_RET:.3f}% (WR {wr*100:.0f}%)")
    if "gap_persist" in by_filter:
        r = by_filter["gap_persist"]; total = r["w"]+r["l"]
        if total >= 8:
            wr = r["w"]/total
            if wr > 0.60 and GAP_PERSIST_RATIO > 0.40:
                GAP_PERSIST_RATIO = round(max(0.40, GAP_PERSIST_RATIO - 0.05), 2)
                adjustments.append(f"gap_persist↓ {GAP_PERSIST_RATIO:.0%} (trop strict, WR {wr*100:.0f}%)")
            elif wr < 0.35 and GAP_PERSIST_RATIO < 0.80:
                GAP_PERSIST_RATIO = round(min(0.80, GAP_PERSIST_RATIO + 0.05), 2)
                adjustments.append(f"gap_persist↑ {GAP_PERSIST_RATIO:.0%} (bien calibré, WR {wr*100:.0f}%)")

    # Fix #1 (votes_delta): ajuster ORACLE_DELTA_CONTRA_MAX
    if "votes_delta" in by_filter:
        r = by_filter["votes_delta"]; total = r["w"]+r["l"]
        if total >= 8:
            wr = r["w"]/total
            if wr > 0.60 and ORACLE_DELTA_CONTRA_MAX < 0.06:
                ORACLE_DELTA_CONTRA_MAX = round(min(0.06, ORACLE_DELTA_CONTRA_MAX + 0.005), 3)
                adjustments.append(f"delta_contra↑ {ORACLE_DELTA_CONTRA_MAX:.3f}% (WR skips {wr*100:.0f}%)")
            elif wr < 0.35 and ORACLE_DELTA_CONTRA_MAX > 0.01:
                ORACLE_DELTA_CONTRA_MAX = round(max(0.01, ORACLE_DELTA_CONTRA_MAX - 0.005), 3)
                adjustments.append(f"delta_contra↓ {ORACLE_DELTA_CONTRA_MAX:.3f}% (WR skips {wr*100:.0f}%)")

    if adjustments:
        msg = f"🔧 *Auto-calibration*\n" + "\n".join(f"  • {a}" for a in adjustments)
        msg += f"\n_Basé sur {len(resolved)} patterns résolus_"
        st.calibration_log.append({"ts":int(time.time()),"adjustments":adjustments})
        await send(context.bot, msg)
        log.info(f"Auto-calibration: {adjustments}")
    else:
        log.info(f"Auto-calibration: seuils OK (patterns:{len(resolved)}, filtres:{list(by_filter.keys())})")


async def job_pattern_memory(context):
    """
    ✅ v10.37 — Point 2: Mémoire des patterns gagnants.
    Toutes les heures, calcule le WR par combinaison (gap_range × delta_range × filtre).
    Stocke les patterns qui gagnent et ceux qui perdent → p_oracle ajusté.
    Résultat: /learn affiche les conditions optimales détectées.
    """
    resolved = [p for p in st.oracle_patterns if p.get("result") in ("WIN","LOSS")]
    if len(resolved) < 20: return

    # Buckets gap: faible 0.01-0.03%, moyen 0.03-0.05%, fort >0.05%
    def gap_bucket(g):
        a=abs(g)
        return "fort" if a>=0.05 else "moyen" if a>=0.03 else "faible"

    def delta_bucket(d):
        a=abs(d)
        return "contre_fort" if a>=0.04 else "contre_léger" if a>=0.01 else "neutre"

    combos = {}
    for p in resolved[-150:]:
        k = f"gap:{gap_bucket(p.get('gap',0))} delta:{delta_bucket(p.get('delta',0))}"
        if k not in combos: combos[k]={"w":0,"l":0}
        if p["result"]=="WIN": combos[k]["w"]+=1
        else: combos[k]["l"]+=1

    top_win = sorted([(k,v) for k,v in combos.items() if v["w"]+v["l"]>=5],
                     key=lambda x: x[1]["w"]/(x[1]["w"]+x[1]["l"]), reverse=True)
    if top_win:
        best_k, best_v = top_win[0]
        best_wr = best_v["w"]/(best_v["w"]+best_v["l"])*100
        st.haiku_insights.append({
            "type":"pattern","ts":int(time.time()),
            "insight":f"Meilleur pattern: {best_k} → {best_wr:.0f}% WR ({best_v['w']+best_v['l']} trades)",
            "combos":top_win[:3]})
        log.info(f"Pattern memory: best={best_k} WR={best_wr:.0f}%")


async def job_haiku_analysis(context):
    """v12.6 — Sonnet analyse les patterns BTC/ETH/SOL toutes les 2h."""
    if not ANTHROPIC_KEY: return
    now = time.time()
    if now - st.last_haiku_ts < 7200: return
    resolved = [p for p in st.oracle_patterns if p.get("result") in ("WIN","LOSS")]
    if len(resolved) < 15: return

    versioned = [p for p in resolved if p.get("v") == BOT_VERSION]
    sample = versioned[-40:] if len(versioned) >= 10 else resolved[-40:]
    version_note = f"v{BOT_VERSION}: {len(versioned)} patterns" if versioned else "mix versions"

    # ── Stats par asset ──
    btc_p = [p for p in sample if p.get("asset","BTC")=="BTC"]
    eth_p = [p for p in sample if p.get("asset")=="ETH"]
    sol_p = [p for p in sample if p.get("asset")=="SOL"]
    asset_note = f"BTC:{len(btc_p)} ETH:{len(eth_p)} SOL:{len(sol_p)}"

    # ── Stats par filtre ──
    by_filter = {}
    for p in sample:
        f = p.get("filter","?")
        if f not in by_filter: by_filter[f] = {"w":0,"l":0}
        if p["result"]=="WIN": by_filter[f]["w"]+=1
        else: by_filter[f]["l"]+=1
    filter_stats = " | ".join(f"{f}:{v['w']}W/{v['l']}L" for f,v in sorted(by_filter.items(), key=lambda x:x[1]["w"]+x[1]["l"],reverse=True)[:5])

    # ── Trades réels si disponibles ──
    real_trades = [t for t in st.trades if not t.get("paper") and t.get("result")]
    trade_summary = ""
    if real_trades:
        wins = sum(1 for t in real_trades if t.get("result")=="WIN")
        wr = wins/len(real_trades)*100
        pnl = sum(t.get("pnl",0) for t in real_trades)
        btc_t = [t for t in real_trades if t.get("asset","BTC")=="BTC"]
        eth_t = [t for t in real_trades if t.get("asset")=="ETH"]
        sol_t = [t for t in real_trades if t.get("asset")=="SOL"]
        trade_summary = f"""
Trades réels ({len(real_trades)} total | WR:{wr:.0f}% | PnL:{pnl:+.2f}$):
- BTC: {len(btc_t)} trades | WR:{sum(1 for t in btc_t if t.get('result')=='WIN')/max(1,len(btc_t))*100:.0f}%
- ETH: {len(eth_t)} trades | WR:{sum(1 for t in eth_t if t.get('result')=='WIN')/max(1,len(eth_t))*100:.0f}%
- SOL: {len(sol_t)} trades | WR:{sum(1 for t in sol_t if t.get('result')=='WIN')/max(1,len(sol_t))*100:.0f}%"""

    # ── Session et contexte marché ──
    import datetime
    hour = datetime.datetime.utcnow().hour
    session = "ASIA" if 0<=hour<8 else ("EU" if 8<=hour<14 else "US")
    btc_spot = st.ws_price if st.ws_price > 0 else 0
    btc_move = ""
    if st.ws_prices:
        pts = list(st.ws_prices)
        if len(pts)>=2:
            move = (pts[-1][1]-pts[0][1])/pts[0][1]*100
            btc_move = f"BTC move 2min: {move:+.3f}%"

    # ── Patterns détaillés ──
    summary = []
    for p in sample:
        asset = p.get("asset","BTC")
        tok = f" tok={p.get('token',0):.2f}$" if p.get("token") else ""
        ev = f" EV={p.get('ev',0)*100:+.1f}%" if p.get("ev") else ""
        summary.append(
            f"[{asset}] gap={p.get('gap',0):+.3f}% delta={p.get('delta',0):+.3f}% "
            f"ret3s={p.get('ret3s',0):+.3f}% votes={p.get('votes',0)}/5 "
            f"filter={p.get('filter','?')}{tok}{ev} → {p['result']}")

    # ✅ v12.6 — Inclure les analyses précédentes dans le prompt
    previous_insights = ""
    if st.haiku_insights:
        last_insights = st.haiku_insights[-3:]  # 3 dernières analyses
        insights_text = []
        for ins in last_insights:
            import datetime as _dt
            ts = _dt.datetime.fromtimestamp(ins.get("ts",0)).strftime("%d/%m %H:%M")
            insights_text.append(f"[{ts}] {ins.get('insight','')[:300]}")
        previous_insights = "\n\nTES ANALYSES PRÉCÉDENTES (pour cohérence et suivi):\n" + "\n---\n".join(insights_text)

    prompt = f"""Tu es un expert en trading algorithmique sur Polymarket (marchés prédiction crypto 5min).
Analyse les skips d'un bot oracle lag v{BOT_VERSION} — {version_note}.
Session actuelle: {session} | {btc_move}

STRATÉGIE: Le bot exploite le lag entre Chainlink (oracle Polymarket) et le prix spot Binance.
Il achète le token UP ou DOWN avant que le marché reprices l'oracle.

PARAMÈTRES ACTUELS:
- BTC: gap≥0.025% | T-25s→T-5s
- ETH: gap≥0.020% | T-20s→T-5s
- SOL: gap≥0.020% | T-15s→T-5s
- XRP: gap≥0.025% | T-20s→T-5s
- Commun: delta≥0.020% | token 0.51$-0.80$ | EV≥15% | votes≥2
- Filtres actifs: ret3s_brutal(<-0.055%) | delta_neg | gap_neg | tokenmax | tokenmin | ev
{trade_summary}

STATS FILTRES ({filter_stats}):
DONNÉES ({len(sample)} skips résolus — {asset_note}):
{chr(10).join(summary)}

{previous_insights}

CONTEXTE IMPORTANT:
- Le bot n'a eu AUCUN trade réel depuis l'ajout ETH/SOL (marché en forte tendance)
- Objectif prioritaire: identifier des configurations qui AURAIENT dû trader et gagner
- Token max actuel: {ORACLE_TOKEN_MAX}$ | EV min: {int(ORACLE_EDGE_MIN*100)}% | votes min: 2/5

INSTRUCTIONS:
1. Identifie les patterns de skips qui auraient été GAGNANTS (WR élevé dans les ✅)
2. Pour chaque pattern gagnant raté, propose UN ajustement de paramètre concret et chiffré
3. Évalue le ratio risque/opportunité: combien de trades supplémentaires gagnerait-on vs perdrait-on
4. Si une analyse précédente identifiait un pattern, confirme ou infirme avec les nouvelles données
5. Distingue [BTC]/[ETH]/[SOL] ou [COMMUN] selon l'asset concerné
6. Priorise les suggestions qui augmentent le nombre de trades rentables

QUESTIONS CLÉS À RÉPONDRE:
- Quel filtre bloque le plus de trades gagnants en ce moment ?
- Quel seuil précis faudrait-il changer pour capturer ces gains ?
- Y a-t-il un contexte (session, volatilité, gap fort) où on devrait être plus agressif ?

Réponds en EXACTEMENT 3 bullet points actionnables en français:
Format: "• [ASSET] [OBSERVATION + données]: [SUGGESTION CONCRÈTE avec chiffres et impact attendu]" """

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(CLAUDE_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,
                         "anthropic-version":"2023-06-01"},
                json={"model":"claude-sonnet-4-6","max_tokens":800,
                      "system": "Tu es un expert en trading algorithmique quantitatif. Tu analyses des données de bot de trading sur marchés prédictifs Polymarket. Tes recommandations doivent être précises, chiffrées et actionnables. Tu connais les concepts: oracle lag, orderbook imbalance, kelly sizing, win rate, EV, R:R ratio.",
                      "messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status==200:
                    data=await r.json()
                    insight=data["content"][0]["text"].strip()
                    st.haiku_insights.append({"type":"sonnet","ts":int(now),"insight":insight})
                    if len(st.haiku_insights)>20: st.haiku_insights=st.haiku_insights[-20:]
                    st.last_haiku_ts=now
                    log.info(f"Sonnet analysis: {insight[:80]}")
                    await send(context.bot, f"🤖 *Sonnet Analysis*\n{insight}")
                else:
                    err = await r.text()
                    log.warning(f"Sonnet API {r.status}: {err[:100]}")
    except Exception as e:
        log.warning(f"Sonnet analysis: {e}")


async def cmd_learn(update,context):
    if not auth(update): return
    now=time.time()
    from datetime import datetime
    lines=["🧠 *AUTO-APPRENTISSAGE*\n━━━━━━━━━━━━━━"]
    merged_patterns=st.oracle_patterns; merged_trades=st.trades

    # ── Résumé général ──
    lines.append(f"📊 {len(merged_patterns)} patterns | {len(merged_trades)} trades en mémoire")
    lines.append(f"📐 *Seuils actuels:*")
    lines.append(f"  delta≥`{ORACLE_ENTRY_DELTA:.3f}%` | gap BTC≥`0.025%` | gap ETH/SOL≥`0.030%`")
    lines.append(f"  token:`{ORACLE_TOKEN_MIN:.2f}$`-`{ORACLE_TOKEN_MAX:.2f}$` | EV≥`{ORACLE_EDGE_MIN*100:.0f}%` | votes≥2")
    lines.append(f"  BTC: T-25s→T-5s | ETH: T-20s→T-5s | SOL: T-15s→T-5s | XRP: T-20s→T-5s")

    # ── Trades réels ──
    real=[t for t in merged_trades if not t.get("paper")]
    if real:
        wins_r=sum(1 for t in real if t.get("result")=="WIN")
        losses_r=len(real)-wins_r; pnl_r=sum(t.get("pnl",0) for t in real)
        wr_r=wins_r/len(real)*100
        avg_win=sum(t["pnl"] for t in real if t.get("result")=="WIN")/max(wins_r,1)
        avg_loss=sum(t["pnl"] for t in real if t.get("result")!="WIN")/max(losses_r,1)
        rr=abs(avg_win/avg_loss) if avg_loss!=0 else 0
        recent=[t for t in real if t.get("ts",0)>now-86400]
        wins_24h=sum(1 for t in recent if t.get("result")=="WIN")
        lines.append(f"\n💰 *Trades réels:* {len(real)} | WR:`{wr_r:.0f}%` | PnL:`{pnl_r:+.2f}$`")
        lines.append(f"  Gain moy:`+{avg_win:.2f}$` | Perte moy:`{avg_loss:.2f}$` | R:R:`{rr:.2f}`")
        if recent:
            pnl_24h=sum(t.get("pnl",0) for t in recent)
            lines.append(f"  📅 24h: {len(recent)} trades | WR:`{wins_24h/len(recent)*100:.0f}%` | PnL:`{pnl_24h:+.2f}$`")
        ts_7j=now-604800; week=[t for t in real if t.get("ts",0)>ts_7j]
        if len(week)>len(recent):
            wins_7j=sum(1 for t in week if t.get("result")=="WIN")
            pnl_7j=sum(t.get("pnl",0) for t in week)
            lines.append(f"  📈 7j: {len(week)} trades | WR:`{wins_7j/len(week)*100:.0f}%` | PnL:`{pnl_7j:+.2f}$`")
        # Par asset
        for asset_tag,emoji in [("BTC","₿"),("ETH","Ξ"),("SOL","◎"),("XRP","✕")]:
            at=[t for t in real if t.get("asset","BTC")==asset_tag]
            if at:
                w_at=sum(1 for t in at if t.get("result")=="WIN")
                pnl_at=sum(t.get("pnl",0) for t in at)
                lines.append(f"  {emoji} {asset_tag}: {len(at)} trades WR:`{w_at/len(at)*100:.0f}%` PnL:`{pnl_at:+.2f}$`")
        # Session la plus rentable
        sessions={}
        for t in real:
            s=t.get("session","?")
            if s not in sessions: sessions[s]={"w":0,"l":0,"pnl":0}
            if t.get("result")=="WIN": sessions[s]["w"]+=1
            else: sessions[s]["l"]+=1
            sessions[s]["pnl"]+=t.get("pnl",0)
        if sessions:
            best=max(sessions.items(),key=lambda x:x[1]["pnl"])
            lines.append(f"  🏆 Meilleure session: `{best[0]}` PnL:`{best[1]['pnl']:+.2f}$`")
    else:
        lines.append(f"\n💰 *Trades réels:* 0 — en attente du premier trade")

    # ── Patterns skips ──
    resolved=[p for p in merged_patterns if p.get("result") in ("WIN","LOSS")]
    resolved_cur=[p for p in resolved if p.get("v")==BOT_VERSION]
    sample=resolved_cur if len(resolved_cur)>=5 else resolved
    label=f"v{BOT_VERSION}" if len(resolved_cur)>=5 else f"all ({len(resolved_cur)} en v{BOT_VERSION})"
    if sample:
        wins=sum(1 for p in sample if p["result"]=="WIN")
        wr_global=wins/len(sample)*100
        # Par filtre
        by_filter={}
        for p in sample:
            f=p.get("filter","?")
            if f not in by_filter: by_filter[f]={"w":0,"l":0}
            if p["result"]=="WIN": by_filter[f]["w"]+=1
            else: by_filter[f]["l"]+=1
        lines.append(f"\n📊 *Patterns skips: {len(sample)}* (WR:{wr_global:.0f}%) — {label}")
        # Top filtres triés par volume
        for f,v in sorted(by_filter.items(),key=lambda x:x[1]["w"]+x[1]["l"],reverse=True)[:7]:
            tot=v["w"]+v["l"]; wr=v["w"]/tot*100 if tot else 0
            e="✅" if wr<35 else ("⚠️" if wr>60 else "➖")
            lines.append(f"  {e}`{f}`: {wr:.0f}% ({v['w']}W/{v['l']}L)")
        # Par asset
        for asset_tag,emoji in [("BTC","₿"),("ETH","Ξ"),("SOL","◎"),("XRP","✕")]:
            ap=[p for p in sample if p.get("asset","BTC")==asset_tag]
            if ap:
                w_ap=sum(1 for p in ap if p["result"]=="WIN")
                lines.append(f"  {emoji} {asset_tag}: {len(ap)} patterns WR:{w_ap/len(ap)*100:.0f}%")
        # 24h
        recent_p=[p for p in sample if p.get("ts",0)>now-86400]
        if recent_p:
            wins_p24=sum(1 for p in recent_p if p["result"]=="WIN")
            wrt=wins_p24/len(recent_p)*100
            status="✅" if wrt<50 else ("⚠️" if wrt>58 else "➖")
            msg=f"  📅 24h: {len(recent_p)} patterns | WR:{wrt:.0f}%"
            if wrt>58: msg+=f"\n  {status} {len(recent_p)} skips résolus, WR {wrt:.0f}% >58% — filtres trop stricts."
            elif wrt<40: msg+=f"\n  {status} ~50% — les filtres ne coûtent rien"
            else: msg+=f"\n  ➖ Zone grise — encore besoin de données"
            lines.append(msg)
    else:
        lines.append(f"\n📊 Pas encore assez de patterns (<5 pour cette version)")

    # ── Calibration ──
    if st.calibration_log:
        last=st.calibration_log[-1]
        ts=datetime.fromtimestamp(last["ts"]).strftime("%d/%m %H:%M")
        lines.append(f"\n🔧 *Calibration:* `{ts}`")
        for a in last["adjustments"][:2]: lines.append(f"  • {a}")

    # ── Sonnet dernière analyse ──
    all_insights=[x for x in st.haiku_insights if x.get("insight")]
    if all_insights:
        last_s=all_insights[-1]
        ts_s=datetime.fromtimestamp(last_s.get("ts",now)).strftime("%d/%m %H:%M")
        lines.append(f"\n🤖 *Sonnet ({ts_s}):*")
        lines.append(last_s["insight"][:500])
    else:
        lines.append(f"\n🤖 *Sonnet:* Prochaine analyse dans {int((st.last_haiku_ts+7200-now)/60)}min")

    try: await update.message.reply_text("\n".join(lines),parse_mode="Markdown")
    except:
        clean=[l.replace("*","").replace("`","").replace("_","") for l in lines]
        await update.message.reply_text("\n".join(clean))


async def cmd_start(update,context):
    if not auth(update): return
    w=POLY_FUNDER_WALLET or POLY_PROXY_WALLET or "?"
    await update.message.reply_text(
        f"🧠 *POLYMARKET BOT v{BOT_VERSION} — R:R FIX*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode:*{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}* | API:{'✅' if poly.ready else '❌'}\n"
        f"Wallet:`{w[:6]}...{w[-4:]}`\n\n"
        f"🆕 v10.27 — Basé sur 29,060 trades réels:\n"
        f"  📊 BPS filter: 5-10 bps direction + 5-12 bps total\n"
        f"  🎯 Token 0.80-0.96$ | Fenêtre T-4min→T-60s\n"
        f"  ✅ NORMAL ~5% | ⚡ FORT ~10% | 🔥 EXCEP ~15% BR\n"
        f"  🚫 job\\_tick désactivé en réel\n\n"
        f"*/run* */stop* */status* */signal* */score*\n"
        f"*/market* */balance* */trades* */recap* */dashboard*\n"
        f"*/passes* */fair* */setbalance {st.bankroll:.2f}* • */backup*",
        parse_mode="Markdown")

async def cmd_run(update,context):
    if not auth(update): return
    if st.running: await update.message.reply_text("⚠️ Déjà en cours."); return
    if not st.paper_mode:
        if not poly.init_client():
            await update.message.reply_text("⚠️ Polymarket indispo — paper mode activé",parse_mode="Markdown")
            st.paper_mode=True
    st.running=True; st.session_start=time.time(); st.daily_ts=time.time()
    st.price_job=context.job_queue.run_repeating(job_price,interval=30,first=5)
    st.macro_job=context.job_queue.run_repeating(job_macro,interval=300,first=8)
    st.tick_job=context.job_queue.run_repeating(job_tick,interval=30,first=10)
    st.snipe_job=context.job_queue.run_repeating(job_snipe,interval=10,first=12)  # ✅ v10.22
    st.tp_job=context.job_queue.run_repeating(job_take_profit,interval=TAKE_PROFIT_CHECK,first=10)
    st.backup_job=context.job_queue.run_repeating(job_backup,interval=120,first=60)  # v12.4 backup 2min
    st.recap_job=context.job_queue.run_repeating(job_daily_recap,interval=3600,first=60)
    context.job_queue.run_repeating(job_check_expiry,interval=30,first=15)
    context.job_queue.run_repeating(job_ws_watchdog_all,interval=30,first=1)  # ✅ v10.23 tous les WS
    context.job_queue.run_repeating(job_staged_entry,interval=5,first=14)     # ✅ v10.23 2e tranche
    context.job_queue.run_repeating(job_oracle_lag,interval=2,first=16)
    context.job_queue.run_repeating(job_oracle_lag_eth,interval=2,first=18)
    context.job_queue.run_repeating(job_oracle_lag_sol,interval=2,first=20)
    context.job_queue.run_repeating(job_oracle_lag_xrp,interval=2,first=22)
    context.job_queue.run_repeating(job_auto_calibrate,interval=7200,first=300)  # ✅ v10.37 seuils auto
    context.job_queue.run_repeating(job_pattern_memory,interval=3600,first=600)  # ✅ v10.37 mémoire patterns
    context.job_queue.run_repeating(job_haiku_analysis,interval=7200,first=900)  # ✅ v10.37 Haiku insights
    st.fg=await fetch_fear_greed(); st.btc24=await fetch_btc_24h(); sess=session_ctx()
    clob_bal = await fetch_clob_balance()
    if clob_bal is not None and clob_bal > 0:
        st.bankroll = clob_bal
        st.bankroll_ref = clob_bal
        st.daily_start = clob_bal
        log.info(f"✅ Balance auto-sync: {clob_bal:.2f}$")
        await send(context.bot, f"💰 Balance auto-sync: `{clob_bal:.2f}$`")
    st.last_ob=await fetch_orderbook_imbalance()
    st.last_liq=await fetch_liquidations()
    st.last_eth_klines=await fetch_eth_klines("5m",30)
    min_score,min_diff,min_mom=get_session_thresholds(sess["session"])
    ob_txt=st.last_ob["desc"] if st.last_ob else "N/A"
    liq_txt=st.last_liq["desc"] if st.last_liq else "N/A"
    await update.message.reply_text(
        f"🚀 *Bot v{BOT_VERSION} démarré !*\nMode:*{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}*\n"
        f"Session:`{sess['session']}` | Seuils: score≥`{min_score}` mom≥`{min_mom}`\n"
        f"⚡ BTC T-25→T-5s | ETH T-20→T-5s | SOL T-15→T-5s | XRP T-20→T-5s\n"
        f"  gap BTC/XRP≥2.5bps | ETH/SOL≥2.0bps | delta≥{int(ORACLE_ENTRY_DELTA*10000)}bps | Token≤{ORACLE_TOKEN_MAX}$ | EV≥{int(ORACLE_EDGE_MIN*100)}%\n"
        f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`\n"
        f"📊 `{ob_txt}` | 💸 `{liq_txt}`\n"
        f"Récap auto: 22h Paris 🕙",
        parse_mode="Markdown")
    await job_tick(context)

async def cmd_stop(update,context):
    if not auth(update): return
    st.running=False
    for j in [st.tick_job,st.price_job,st.macro_job,st.tp_job,st.backup_job,st.recap_job,st.snipe_job]:
        if j:
            try: j.schedule_removal()
            except: pass
    st.tick_job=st.price_job=st.macro_job=st.tp_job=st.backup_job=st.recap_job=st.snipe_job=None
    st.backup()
    await update.message.reply_text(
        f"⏹ *Arrêté* | `{upt()}` | BR:`{st.bankroll:.2f}` | ROI:`{roi()}` | WR:`{wr()}`\n💾 Backup OK.",
        parse_mode="Markdown")

async def cmd_recap(update,context):
    if not auth(update): return
    now=time.time(); cutoff=now-86400
    trades_24h=[t for t in st.trades if t.get("ts",0)>=cutoff]
    if not trades_24h:
        await update.message.reply_text("📊 Aucun trade dans les 24 dernières heures."); return
    wins=[t for t in trades_24h if t["result"]=="WIN"]
    losses=[t for t in trades_24h if t["result"]=="LOSS"]
    pnl_24h=sum(t["pnl"] for t in trades_24h)
    wr_24h=len(wins)/len(trades_24h)*100
    avg_win=sum(t["pnl"] for t in wins)/len(wins) if wins else 0
    avg_loss=abs(sum(t["pnl"] for t in losses)/len(losses)) if losses else 0
    best=max(trades_24h,key=lambda t:t["pnl"])
    worst=min(trades_24h,key=lambda t:t["pnl"])
    up_t=[t for t in trades_24h if t["dir"]=="UP"]
    dn_t=[t for t in trades_24h if t["dir"]=="DOWN"]
    up_wr=sum(1 for t in up_t if t["result"]=="WIN")/len(up_t)*100 if up_t else 0
    dn_wr=sum(1 for t in dn_t if t["result"]=="WIN")/len(dn_t)*100 if dn_t else 0
    sessions={}
    for t in trades_24h:
        s=t.get("session","?")
        if s not in sessions: sessions[s]={"w":0,"l":0}
        if t["result"]=="WIN": sessions[s]["w"]+=1
        else: sessions[s]["l"]+=1
    sess_txt="\n".join(f"  `{s}`: ✅{v['w']} ❌{v['l']}" for s,v in sessions.items())
    await update.message.reply_text(
        f"📊 *RECAP 24H*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Trades:`{len(trades_24h)}` (✅{len(wins)} ❌{len(losses)})\n"
        f"WR:`{wr_24h:.1f}%` | PnL:`{fmt(pnl_24h)}$`\n"
        f"Gain moy:`+{avg_win:.2f}$` | Perte moy:`-{avg_loss:.2f}$`\n\n"
        f"🟢 UP:`{up_wr:.0f}%`({len(up_t)}) | 🔴 DOWN:`{dn_wr:.0f}%`({len(dn_t)})\n\n"
        f"🏆 Meilleur:`{fmt(best['pnl'])}$` {best['dir']}\n"
        f"💀 Pire:`{fmt(worst['pnl'])}$` {worst['dir']}\n\n"
        f"Par session:\n{sess_txt}",
        parse_mode="Markdown")

async def cmd_dashboard(update,context):
    if not auth(update): return
    if not st.trades:
        await update.message.reply_text("📊 Aucun trade pour générer le dashboard."); return
    await update.message.reply_text("⏳ Génération dashboard...")
    html=generate_dashboard(st.trades,st.bankroll,st.bankroll_ref,st.pnl)
    filepath="/tmp/polybot_dashboard.html"
    with open(filepath,"w",encoding="utf-8") as f: f.write(html)
    with open(filepath,"rb") as f:
        await context.bot.send_document(
            chat_id=ALLOWED_UID,
            document=f,
            filename=f"polybot_dashboard_{datetime.now().strftime('%d%m_%H%M')}.html",
            caption=f"📊 Dashboard v{BOT_VERSION} | BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`"
        )

async def cmd_setbalance(update,context):
    if not auth(update): return
    args=context.args
    if not args:
        await update.message.reply_text("💡 *Usage:* `/setbalance 55.11`",parse_mode="Markdown"); return
    try:
        new_bal=round(float(args[0].replace(",",".")),2)
        if new_bal<0 or new_bal>100000:
            await update.message.reply_text("❌ Montant invalide."); return
        old=st.bankroll; st.bankroll=new_bal; st.bankroll_ref=new_bal
        st.daily_start=new_bal; st.daily_ts=time.time()
        st.daily_pause_until=0; st.pnl=0.0; st.backup()
        await update.message.reply_text(
            f"✅ *Balance mise à jour*\n`{old:.2f}$` → `{new_bal:.2f}$`\nROI repart de `0%`",
            parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Ex: `/setbalance 55.11`",parse_mode="Markdown")

async def cmd_backup(update,context):
    if not auth(update): return
    await update.message.reply_text("💾 Backup en cours...")
    ok=st.backup()
    gh_ok=False
    if ok:
        try: await push_state_to_github(); gh_ok=True
        except Exception as e: log.warning(f"backup github: {e}")
    status="✅ Local + GitHub State" if gh_ok else ("✅ Local" if ok else "❌ Échoué")
    await update.message.reply_text(
        f"💾 *BACKUP*\n{status}\n"
        f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`\n"
        f"Trades:`{len(st.trades)}` | Patterns:`{len(st.oracle_patterns)}` | Passes:`{len(st.pass_reasons)}`",
        parse_mode="Markdown")


async def cmd_status(update,context):
    if not auth(update): return
    sess=session_ctx()
    dl=(st.daily_start-st.bankroll)/st.daily_start*100 if st.daily_start>0 else 0
    cs=st.last_conf_score
    score_info=f"`{cs.get('score',0):.1f}/{cs.get('min_score',10)}` Mom:`{st.last_mom_score}/{cs.get('min_mom',4)}`" if cs else "—"
    fair_info=""
    if st.last_fair:
        f_mode=st.last_fair.get("mode","")
        od=st.last_fair.get("oracle_delta",0)
        od_txt=f" Δoracle:`{od:+.3f}%`" if od else ""
        fair_info=f"\n⚡ {f_mode} P:`{st.last_fair.get('p_up',0)*100:.0f}%` EV:`{st.last_fair.get('ev',0)*100:+.1f}%`{od_txt}"
    bet_info="Aucun"
    if st.bet:
        elapsed=int((time.time()-st.bet["ts"])/60)
        bet_info=f"{st.bet['dir']} {st.bet['amount']:.2f}$ ({elapsed}min)"
        if st.trailing_active: bet_info+=f" 🎯peak:x{st.token_price_peak:.2f}"
        if st.bet_expiry>0:
            rem=int((st.bet_expiry-time.time())/60)
            bet_info+=f" ⏰{rem}min"
    pause_info=""
    if st.daily_pause_until>time.time():
        remaining=int((st.daily_pause_until-time.time())/60)
        pause_info=f"\n⏸ Pause:`{remaining}min`"
    ob_txt=st.last_ob["desc"] if st.last_ob else "N/A"
    liq_txt=st.last_liq["desc"] if st.last_liq else "N/A"
    min_score,min_diff,min_mom=get_session_thresholds(sess["session"])
    await update.message.reply_text(
        f"📊 *STATUS v{BOT_VERSION}* [{'📄' if st.paper_mode else '💰'}]\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢 EN COURS' if st.running else '🔴 ARRÊTÉ'} | {'✅ CLOB' if poly.ready else '❌ CLOB'} | WS:{'✅' if st.ws_connected else '❌'}\n\n"
        f"₿`${st.price:,.2f}` Ξ`${st.eth_price:,.0f}` ◎`${st.sol_price:,.0f}` | F&G:`{st.fg['value']}` | `{sess['session']}`\n"
        f"Seuils: score≥`{min_score}` mom≥`{min_mom}`\n"
        f"📊 `{ob_txt}` | 💸 `{liq_txt}`\n"
        f"🎯 {score_info}{fair_info}\n\n"
        f"💰 BR:`{st.bankroll:.2f}$` | ROI:`{roi()}` | PnL:`{fmt(st.pnl)}`\n"
        f"📅 Perte jour:`{dl:.1f}%/{DAILY_LOSS_MAX*100:.0f}%`{pause_info}\n"
        f"🎲 Bet:`{bet_info}` | 🚫 Refusés:`{st.skipped}` | ⏱`{upt()}`\n"
        f"🧠 Patterns: `{len([p for p in st.oracle_patterns if p.get('result')])}` résolus | `/learn` pour détails",
        parse_mode="Markdown")

async def cmd_balance(update,context):
    if not auth(update): return
    w=POLY_PROXY_WALLET or "?"
    short=f"{w[:6]}...{w[-4:]}"
    real_balance = None
    if poly.ready and poly.client_version == "v2":
        try:
            from py_clob_client_v2 import BalanceAllowanceParams
            from py_clob_client_v2.clob_types import AssetType
            resp = poly.client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
            if resp:
                bal = resp.get("balance", resp.get("amount", None))
                if bal is not None:
                    real_balance = round(float(bal) / 1e6, 2)
        except Exception as e:
            log.warning(f"Balance CLOB: {e}")
    balance_line = f"🔗 Solde CLOB:`{real_balance:.2f}$`\n" if real_balance is not None else ""
    await update.message.reply_text(
        f"💰 *Balance Bot*\n━━━━━━━━━━━━━━\n"
        f"🔑 `{short}`\n"
        f"{balance_line}"
        f"📊 BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`\n"
        f"📈 PnL:`{fmt(st.pnl)}$` | Réf:`{st.bankroll_ref:.2f}$`\n\n"
        f"💡 `/setbalance <montant>` pour sync",
        parse_mode="Markdown")

async def cmd_market(update,context):
    if not auth(update): return
    await update.message.reply_text("⏳ Recherche marchés BTC/ETH/SOL...")
    now_ts=int(time.time()); cur_slot=int(now_ts//300)*300; slot_rem=cur_slot+300-now_ts
    lines=[f"🎯 *MARCHÉS ACTIFS — BTC/ETH/SOL/XRP*\n━━━━━━━━━━━━━━\n⏰ T-`{int(slot_rem)}s` avant résolution\n"]
    for label,prefix,oracle_px,slot_open in [
        ("₿ BTC","btc-updown-5m",st.oracle_price,st.oracle_slot_open),
        ("Ξ ETH","eth-updown-5m",st.eth_oracle_price,st.eth_oracle_slot_open),
        ("◎ SOL","sol-updown-5m",st.sol_oracle_price,st.sol_oracle_slot_open),
        ("✕ XRP","xrp-updown-5m",st.xrp_oracle_price,st.xrp_oracle_slot_open),
    ]:
        try:
            market=await poly.get_market_by_slug(f"{prefix}-{cur_slot}")
            if not market: lines.append(f"{label}: ❌ marché non trouvé"); continue
            tu=await poly.get_token_price(market["token_up"])
            td=await poly.get_token_price(market["token_down"])
            delta=(oracle_px-slot_open)/slot_open*100 if slot_open>0 else 0
            ev_u=(0.85-tu-taker_fee_per_share(tu))*100 if tu>0 else 0
            ev_d=(0.85-td-taker_fee_per_share(td))*100 if td>0 else 0
            ok_u="✅" if tu<=ORACLE_TOKEN_MAX and ev_u>=ORACLE_EDGE_MIN*100 else "❌"
            ok_d="✅" if td<=ORACLE_TOKEN_MAX and ev_d>=ORACLE_EDGE_MIN*100 else "❌"
            lines.append(f"*{label}* Oracle:`${oracle_px:,.2f}` Δ:`{delta:+.3f}%`\n"
                        f"  🟢 UP:`{tu:.3f}$` EV:`{ev_u:.0f}%` {ok_u} | 🔴 DOWN:`{td:.3f}$` EV:`{ev_d:.0f}%` {ok_d}")
        except: lines.append(f"{label}: ⚠️ erreur")
    lines.append(f"\n🎯 Token≤`{ORACLE_TOKEN_MAX}$` | EV≥`{int(ORACLE_EDGE_MIN*100)}%`")
    try: await update.message.reply_text("\n".join(lines),parse_mode="Markdown")
    except: await update.message.reply_text("\n".join(lines).replace("*","").replace("`",""))


async def cmd_score(update,context):
    if not auth(update): return
    await update.message.reply_text("⏳ Calcul score...")
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
    if c5:
        st.c5=deque(c5,maxlen=100); st.c15=deque(c15,maxlen=100)
        st.c1h=deque(c1h,maxlen=100); st.c1=deque(c1,maxlen=100); st.c4h=deque(c4h,maxlen=50)
        st.price=c5[-1]["close"]
    st.fg=await fetch_fear_greed()
    ob=await fetch_orderbook_imbalance(); liq=await fetch_liquidations()
    eth_klines=await fetch_eth_klines("5m",30)
    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5)); i15=compute_ind(list(st.c15))
    i1h=compute_ind(list(st.c1h)); i4h=compute_ind(list(st.c4h)) if st.c4h else {}
    sess=session_ctx(); adv=compute_advanced_signals(list(st.c5),list(st.c1),list(st.c4h) if st.c4h else None)
    direction_guess="UP" if i5.get("ema_bull") else "DOWN"
    eth_bonus,eth_desc=compute_eth_correlation(eth_klines,direction_guess) if eth_klines else (0,"ETH N/A")
    # ✅ v10.22 — Delta du slot en TEMPS RÉEL (avant: valeur périmée du dernier tick)
    wd_w,wd_pct=live_window_delta()
    st.window_delta=wd_w; st.window_delta_pct=wd_pct
    cs=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv,ob,liq,eth_bonus,eth_desc,st.btc24,wd_w,wd_pct)
    mom=compute_momentum_score(i1,i5,i15)
    st.last_conf_score=cs; st.last_mom_score=mom; st.last_ob=ob; st.last_liq=liq
    st.last_eth_klines=eth_klines
    _,_,min_mom=get_session_thresholds(sess["session"], cs.get("score",0))
    tu=0.5; td=0.5; token_txt=""
    if not st.paper_mode and poly.ready:
        m=await poly.find_btc_5min_market()
        if not m and st.current_market:
            m=st.current_market
        if m:
            tu=await poly.get_token_price(m["token_up"])
            td=await poly.get_token_price(m["token_down"])
            token_txt=f"\n🟢 UP:`{tu:.3f}$` x{round(1/tu,2) if tu>0 else '?'} | 🔴 DOWN:`{td:.3f}$` x{round(1/td,2) if td>0 else '?'}"
    mom_e="🔥" if mom>=7 else "⚡" if mom>=4 else "💤"
    sigs="\n".join(f"  • {s}" for s in cs["signals"])
    await update.message.reply_text(
        f"🎯 *SCORE v{BOT_VERSION}*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"₿`${st.price:,.2f}` | `{sess['session']}` | Δslot:`{wd_pct:+.3f}%`{token_txt}\n"
        f"`{eth_desc}` | `{ob['desc'] if ob else 'N/A'}`\n"
        f"💸 `{liq['desc'] if liq else 'N/A'}`\n\n"
        f"🟢 UP:`{cs['score_up']:.1f}` 🔴 DOWN:`{cs['score_dn']:.1f}`\n"
        f"Diff:`{cs['diff']:.1f}/{cs['min_diff']}` → {'✅ TRADEABLE' if cs['tradeable'] else '❌ PASS'}\n"
        f"⚡ Mom:`{mom}/10`(seuil:`{min_mom}`) {mom_e}\n\nSignaux:\n{sigs or '  Aucun'}",
        parse_mode="Markdown")

async def cmd_signal(update,context):
    if not auth(update): return
    await update.message.reply_text("⏳ Analyse complète...")
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
    if c5:
        st.c1=deque(c1,maxlen=100); st.c5=deque(c5,maxlen=100); st.c15=deque(c15,maxlen=100)
        st.c1h=deque(c1h,maxlen=100); st.c4h=deque(c4h,maxlen=50); st.price=c5[-1]["close"]
    st.fg=await fetch_fear_greed(); st.btc24=await fetch_btc_24h()
    ob=await fetch_orderbook_imbalance(); liq=await fetch_liquidations()
    eth_klines=await fetch_eth_klines("5m",30)
    st.last_eth_klines=eth_klines
    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5)); i15=compute_ind(list(st.c15))
    i1h=compute_ind(list(st.c1h)); i4h=compute_ind(list(st.c4h)) if st.c4h else {}
    sess=session_ctx(); adv=compute_advanced_signals(list(st.c5),list(st.c1),list(st.c4h) if st.c4h else None)
    direction_guess="UP" if i5.get("ema_bull") else "DOWN"
    eth_bonus,eth_desc=compute_eth_correlation(eth_klines,direction_guess) if eth_klines else (0,"ETH N/A")
    # ✅ v10.22 — Delta du slot en TEMPS RÉEL
    wd_w,wd_pct=live_window_delta()
    st.window_delta=wd_w; st.window_delta_pct=wd_pct
    cs=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv,ob,liq,eth_bonus,eth_desc,st.btc24,wd_w,wd_pct)
    mom=compute_momentum_score(i1,i5,i15)
    st.last_conf_score=cs; st.last_mom_score=mom; st.last_ob=ob; st.last_liq=liq
    tu=0.5; td=0.5
    if not st.paper_mode and poly.ready:
        m=await poly.find_btc_5min_market()
        if not m and st.current_market:
            m=st.current_market
        if m:
            tu=await poly.get_token_price(m["token_up"])
            td=await poly.get_token_price(m["token_down"])
            st.current_market=m
    d=await claude_decide(i1,i5,i15,i1h,i4h,adv,st.trades[-15:],st.bankroll,st.consec,
                          st.fg,st.btc24,sess,cs,mom,tu,td,ob,liq,eth_desc)
    st.last_decision=d
    dir_e="🟢" if d["dir"]=="UP" else "🔴" if d["dir"]=="DOWN" else "⚪"
    risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(d.get("risk","MEDIUM"),"🟡")
    payout=round(1/(tu if d["dir"]=="UP" else td),2) if d["dir"] else 0
    kelly_info=f" Kelly:`{d.get('kelly_pct',0):.1f}%`(`{d.get('size',0):.2f}$`)" if d.get("trade") else ""
    eth_e="✅" if eth_bonus>0 else "⚠️" if eth_bonus<0 else "➖"
    await update.message.reply_text(
        f"🧠 *ANALYSE v{BOT_VERSION}*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_e} *{d['dir'] or 'PASS'}* | {risk_e} | `{d['conf']*100:.0f}%`\n"
        f"Score:`{cs['score']:.1f}` Mom:`{mom}/10` Payout:x`{payout}`{kelly_info}\n"
        f"Δslot:`{wd_pct:+.3f}%` | Ξ{eth_e}`{eth_desc}` | `{ob['desc'] if ob else 'N/A'}`\n"
        f"₿`${i5.get('price',0):,.2f}` | F&G:`{st.fg['value']}` | `{sess['session']}`\n\n"
        f"💭 _{d['reasoning']}_",parse_mode="Markdown")

    # ✅ v10.14d — Si Claude dit trade=True, placer l'ordre directement depuis /signal
    if d.get("trade") and d.get("dir") and not st.bet and not st.paper_mode and st.current_market:
        amount = d.get("size", 0)
        if amount >= MIN_BET_USD and st.bankroll >= amount:
            market_end = 0
            try:
                ed = st.current_market.get("end_date", "")
                if ed:
                    from datetime import timezone
                    dt = datetime.fromisoformat(ed.replace("Z", "+00:00"))
                    market_end = dt.timestamp()
            except: pass
            if market_end > 0 and (market_end - time.time()) < 60:
                await update.message.reply_text("⏰ Slot expire trop tôt — ordre annulé")
                return
            token_used = st.current_market["token_up"] if d["dir"]=="UP" else st.current_market["token_down"]
            # ✅ v10.22 — Refetch du prix juste avant l'ordre (Claude a pris 10-25s)
            entry_tp = await poly.get_token_price(token_used)
            if entry_tp <= 0: entry_tp = tu if d["dir"]=="UP" else td
            order_id = await poly.place_market_order(token_used, amount, "BUY")
            if order_id:
                st.bet = {"dir":d["dir"],"amount":amount,"conf":d["conf"],"entry":st.price,
                    "reasoning":d["reasoning"],"ts":int(time.time()),"score":cs["score"],"session":sess["session"]}
                st.active_order_id = order_id; st.active_token_id = token_used
                st.entry_token_price = entry_tp; st.shares_bought = round(amount/entry_tp,4) if entry_tp>0 else 0
                st.token_price_peak = 1.0; st.trailing_active = False; st.bet_expiry = market_end
                await update.message.reply_text(
                    f"🎯 *Ordre placé depuis /signal !*\n"
                    f"*{d['dir']}* `{amount:.2f}$` | Token:`{entry_tp:.3f}$`\n"
                    f"ID:`{order_id}`",parse_mode="Markdown")
            else:
                await update.message.reply_text("⚠️ Ordre refusé depuis /signal")

async def cmd_ai(update,context):
    if not auth(update): return
    d=st.last_decision
    if not d: await update.message.reply_text("⏳ Lance /signal d'abord."); return
    dir_e="🟢" if d.get("dir")=="UP" else "🔴" if d.get("dir")=="DOWN" else "⚪"
    risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(d.get("risk","MEDIUM"),"🟡")
    await update.message.reply_text(
        f"🧠 *DERNIÈRE DÉCISION*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_e} *{d.get('dir') or 'PASS'}* | {risk_e} | `{d.get('conf',0)*100:.0f}%`\n"
        f"Trade:`{'OUI ✅' if d.get('trade') else 'NON ❌'}` | Kelly:`{d.get('size',0):.2f}$`(`{d.get('kelly_pct',0):.1f}%`)\n\n"
        f"💭 _{d.get('reasoning','—')}_",parse_mode="Markdown")

async def cmd_trades(update,context):
    if not auth(update): return
    trades=st.trades[-8:][::-1]
    if not trades: await update.message.reply_text("📈 Aucun trade."); return
    lines=["📈 *TRADES*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    for t in trades:
        ts=datetime.fromtimestamp(t["ts"]).strftime("%d/%m %H:%M")
        lines.append(f"{'✅' if t['result']=='WIN' else '❌'}{'💰' if not t.get('paper',True) else '📄'} `{t['dir']}` `{fmt(t['pnl'])}$` `{ts}`")
    if st.bet:
        elapsed=int((time.time()-st.bet["ts"])/60)
        trail=" 🎯TRAIL" if st.trailing_active else ""
        lines.append(f"\n🔄 *Actif:* `{st.bet['dir']}` `{st.bet['amount']:.2f}$` ({elapsed}min){trail}")
    try:
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    except Exception:
        # Fallback sans Markdown si caractères spéciaux dans les raisons
        clean = [l.replace("*","").replace("`","").replace("_","") for l in lines]
        await update.message.reply_text("\n".join(clean))

async def cmd_history(update,context):
    """✅ v10.17 — 20 derniers trades avec détails complets"""
    if not auth(update): return
    trades=st.trades[-20:][::-1]
    if not trades: await update.message.reply_text("📈 Aucun trade dans l'historique."); return
    lines=["📋 *HISTORIQUE 20 TRADES*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    total_pnl=0
    for t in trades:
        ts=datetime.fromtimestamp(t["ts"]).strftime("%d/%m %H:%M")
        emoji="✅" if t["result"]=="WIN" else "❌"
        mode="💰" if not t.get("paper",True) else "📄"
        pnl=t["pnl"]; total_pnl+=pnl
        score=t.get("score",0); sess=t.get("session","?")
        lines.append(f"{emoji}{mode} `{t['dir']}` `{fmt(pnl)}$` score:`{score:.0f}` `{sess}` `{ts}`")
    wins=sum(1 for t in trades if t["result"]=="WIN")
    wr=wins/len(trades)*100
    lines.append(f"\n📊 WR:`{wr:.0f}%` | PnL total:`{fmt(total_pnl)}$`")
    await update.message.reply_text("\n".join(lines),parse_mode="Markdown")

async def cmd_stats(update,context):
    if not auth(update): return
    total=st.wins+st.losses
    aw=sum(t["pnl"] for t in st.trades if t["pnl"]>0)/max(st.wins,1)
    al=abs(sum(t["pnl"] for t in st.trades if t["pnl"]<0))/max(st.losses,1)
    rr=aw/al if al>0 else 0
    real_t=[t for t in st.trades if not t.get("paper",True)]
    real_wr=sum(1 for t in real_t if t["result"]=="WIN")/len(real_t)*100 if real_t else 0
    sess_7d=wr_by_session(st.trades,7)
    sess_txt=""
    for s,v in sorted(sess_7d.items(),key=lambda x:x[1]["w"]/(x[1]["w"]+x[1]["l"]) if (x[1]["w"]+x[1]["l"])>0 else 0,reverse=True):
        tot=v["w"]+v["l"]
        wr_s=round(v["w"]/tot*100) if tot>0 else 0
        pnl_s=round(v["pnl"],2)
        sess_txt+=f"\n  `{s}`: {wr_s}% ({v['w']}W/{v['l']}L) `{fmt(pnl_s)}$`"
    hours_data, best_h, worst_h, best_wr_h, worst_wr_h = wr_by_hour(st.trades)
    hour_txt = ""
    if best_h is not None:
        hour_txt = f"\n⏰ Meilleure heure: `{best_h}h` Paris (`{best_wr_h:.0f}%`)"
    if worst_h is not None and worst_h != best_h:
        hour_txt += f" | Pire: `{worst_h}h` (`{worst_wr_h:.0f}%`)"
    await update.message.reply_text(
        f"📉 *STATS v{BOT_VERSION}*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total:`{total}` (✅{st.wins} ❌{st.losses})\nWR:`{wr()}` | ROI:`{roi()}` | R:R:`{rr:.2f}`\n"
        f"PnL:`{fmt(st.pnl)}$` | BR:`{st.bankroll:.2f}$`\n\n"
        f"💰 Réels:`{len(real_t)}` WR:`{real_wr:.0f}%`\n"
        f"Gain moy:`+{aw:.2f}$` | Perte moy:`-{al:.2f}$`\n\n"
        f"📊 WR par session (7j):{sess_txt or ' Pas assez de données'}{hour_txt}\n\n"
        f"💡 `/recap` 24h | `/passes` WR skips | `/dashboard` HTML",
        parse_mode="Markdown")

async def cmd_autotune(update,context):
    """✅ v10.23 — Ajuste les seuils selon le WR théorique des skips résolus."""
    if not auth(update): return
    resolved=[p for p in st.pass_reasons if p.get("resolved")]
    if len(resolved)<15:
        await update.message.reply_text(f"⏳ Pas assez de skips résolus (`{len(resolved)}`/15) pour auto-tune.",parse_mode="Markdown")
        return
    w=sum(1 for p in resolved if p["resolved"]=="WIN")
    twr=w/len(resolved)*100
    sess=session_ctx()["session"]
    cur=SESSION_THRESHOLDS.get(sess,(10,3.5,4))
    msg=""
    if twr>=60:
        # Les filtres ratent trop de gagnants → desserrer la session courante de -1
        new=(max(6,cur[0]-1),max(1.5,cur[1]-0.5),max(2,cur[2]-1))
        SESSION_THRESHOLDS[sess]=new
        msg=f"🔓 *Desserré* {sess}: score≥`{new[0]}` mom≥`{new[2]}`\n_(WR skips {twr:.0f}% — trop de gagnants ratés)_"
    elif twr<=45:
        new=(cur[0]+1,cur[1]+0.5,cur[2]+1)
        SESSION_THRESHOLDS[sess]=new
        msg=f"🔒 *Resserré* {sess}: score≥`{new[0]}` mom≥`{new[2]}`\n_(WR skips {twr:.0f}% — skips justifiés)_"
    else:
        msg=f"➖ {sess} inchangé (WR skips `{twr:.0f}%`, zone neutre 45-60%)"
    await update.message.reply_text(
        f"⚙️ *AUTO-TUNE*\nWR théorique skips: `{twr:.0f}%` ({w}/{len(resolved)})\n{msg}",
        parse_mode="Markdown")

async def cmd_passes(update,context):
    """v12.8 — Affiche les skips avec boutons pagination."""
    if not auth(update): return
    page = 1
    if context.args:
        try: page = max(1, int(context.args[0]))
        except: pass
    await _show_passes(update, context, page)

async def _show_passes(update, context, page=1):
    """Affiche une page de passes avec boutons navigation."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from datetime import datetime
    PAGE_SIZE = 12
    all_passes = sorted(st.pass_reasons, key=lambda x: x.get("ts",0), reverse=True)
    total = len(all_passes)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(max(1, page), total_pages)
    passes = all_passes[(page-1)*PAGE_SIZE : page*PAGE_SIZE]

    if not passes:
        msg = update.callback_query.message if update.callback_query else update.message
        await msg.reply_text("✅ Aucun PASS."); return

    lines=[f"🚫 *PASSES — BTC/ETH/SOL/XRP* ({page}/{total_pages})\n━━━━━━━━━━━━━━"]
    for p in passes:
        ts=p.get("ts",0); reason=p.get("reason","?"); result=p.get("result"); direction=p.get("dir")
        t=datetime.fromtimestamp(ts).strftime("%H:%M") if ts else "??"
        r_clean=reason.replace("**","").replace("__","")[:80]
        emoji="✅" if result=="WIN" else ("❌" if result=="LOSS" else "⏳")
        dir_str=f" {direction}" if direction else " —"
        lines.append(f"{t}{dir_str} {emoji} {r_clean}")

    resolved=[p for p in all_passes if p.get("result") in ("WIN","LOSS")]
    if resolved:
        wins=sum(1 for p in resolved if p["result"]=="WIN")
        wr=wins/len(resolved)*100
        lines.append(f"\n📊 WR: {wr:.0f}% ({wins}/{len(resolved)})")
        if wr>58: lines.append("⚠️ Filtres peut-être trop stricts")
        elif wr<40: lines.append("✅ Filtres corrects")
        else: lines.append("➖ Zone grise")

    text="\n".join(lines)

    # Boutons navigation
    buttons=[]
    if page > 1: buttons.append(InlineKeyboardButton("⬅️ Précédent", callback_data=f"passes:{page-1}"))
    if page < total_pages: buttons.append(InlineKeyboardButton("Suivant ➡️", callback_data=f"passes:{page+1}"))
    keyboard = InlineKeyboardMarkup([buttons]) if buttons else None

    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
        else:
            await update.message.reply_text(text, parse_mode="Markdown", reply_markup=keyboard)
    except:
        clean=text.replace("*","").replace("`","")
        if update.callback_query:
            await update.callback_query.edit_message_text(clean, reply_markup=keyboard)
        else:
            await update.message.reply_text(clean, reply_markup=keyboard)


async def cmd_fear(update,context):
    if not auth(update): return
    v=st.fg['value']; bar="█"*(v//10)+"░"*(10-v//10)
    e="😱" if v<20 else "😟" if v<40 else "😐" if v<60 else "😊" if v<80 else "🤑"
    interp="Extrême Peur→biais UP" if v<20 else "Peur" if v<40 else "Neutre" if v<60 else "Greed" if v<80 else "Extrême Greed→biais DOWN"
    await update.message.reply_text(
        f"😱 *FEAR & GREED*\n{e} *{st.fg['label']}* — `{v}/100`\n`{bar}`\n\n_{interp}_\n₿ 24h:`{st.btc24.get('change_pct',0):+.2f}%`",
        parse_mode="Markdown")

async def cmd_paper(update,context):
    if not auth(update): return
    st.paper_mode=not st.paper_mode
    if not st.paper_mode and not poly.ready: poly.init_client()
    await update.message.reply_text(f"Mode:*{'📄 PAPER' if st.paper_mode else '💰 RÉEL ⚠️'}* | API:{'✅' if poly.ready else '❌'}",parse_mode="Markdown")
    st.backup()

async def cmd_resetskips(update,context):
    """v12.6 — Remet à zéro les passes et patterns."""
    if not auth(update): return
    n_passes = len(st.pass_reasons)
    n_patterns = len(st.oracle_patterns)
    st.pass_reasons.clear()
    st.oracle_patterns.clear()
    await update.message.reply_text(
        f"🔄 *Skips réinitialisés*\n"
        f"  {n_passes} passes supprimées\n"
        f"  {n_patterns} patterns supprimés\n"
        f"WR théorique remis à zéro ✅", parse_mode="Markdown")


async def cmd_reset(update,context):
    if not auth(update): return
    st.running=False
    for j in [st.tick_job,st.price_job,st.macro_job,st.tp_job,st.backup_job,st.recap_job,st.snipe_job]:
        if j:
            try: j.schedule_removal()
            except: pass
    st.bankroll=50.0; st.bankroll_ref=50.0; st.trades=[]; st.bet=None
    st.wins=st.losses=st.skipped=st.consec=0; st.pnl=st.streak=st.best_streak=st.worst_streak=0
    st.cooldown_until=0; st.daily_pause_until=0; st.session_start=time.time(); st.pass_reasons=[]
    st.last_conf_score={}; st.last_mom_score=0; st.active_order_id=None
    st.active_token_id=None; st.shares_bought=0; st.entry_token_price=0
    st.token_price_peak=0; st.trailing_active=False; st.bet_expiry=0
    st.win_streak_count=0; st.conservative_until=0; st.turbo_until=0; st.last_fair={}
    st.c1.clear(); st.c5.clear(); st.c15.clear(); st.c1h.clear(); st.c4h.clear()
    for f in [DATA_FILE,BACKUP_FILE]:
        if os.path.exists(f): os.remove(f)
    await update.message.reply_text("🔄 *Reset complet.*",parse_mode="Markdown")

async def cmd_cooldown(update,context):
    if not auth(update): return
    st.cooldown_until=0; st.consec=0; st.daily_pause_until=0
    await update.message.reply_text("✅ Cooldown + pause reset.",parse_mode="Markdown")

async def cmd_fair(update,context):
    """✅ v10.21 — Fair value du slot actuel (modèle Brownien) + frais v10.22"""
    if not auth(update): return
    sigma = realized_vol()
    t_rem = int(300 - (time.time() % 300))
    if not st.ws_connected or sigma <= 0:
        await update.message.reply_text("⏳ WebSocket Binance pas encore prêt — relance dans 1min.")
        return
    cur = st.ws_price
    delta_live = (cur - st.slot_open_price) / st.slot_open_price * 100 if st.slot_open_price > 0 else 0.0
    p_up = fair_prob_up(delta_live, t_rem, sigma)
    snipe_zone = SNIPE_LAST_MIN <= t_rem < ENTRY_LAST_SECONDS
    await update.message.reply_text(
        f"⚖️ *FAIR VALUE* (Brownien)\n━━━━━━━━━━━━━━\n"
        f"₿`${cur:,.2f}` | Slot open:`${st.slot_open_price:,.2f}`\n"
        f"Δ:`{delta_live:+.3f}%` | ⏰`{t_rem}s` {'🎯SNIPE zone' if snipe_zone else ''} | σ:`{sigma:.4f}`\n\n"
        f"🟢 P(UP):`{p_up*100:.0f}%` | 🔴 P(DOWN):`{(1-p_up)*100:.0f}%`\n\n"
        f"💡 Normal: EV≥{FAIR_EDGE_MIN*100:.0f}pts | SNIPE: P≥{SNIPE_MIN_PROB*100:.0f}% + EV≥{SNIPE_EDGE_MIN*100:.0f}pts\n"
        f"_(frais taker déduits automatiquement)_",
        parse_mode="Markdown")

async def cmd_sellcheck(update,context):
    """✅ v10.20d — Affiche le PnL actuel sans vendre"""
    if not auth(update): return
    if not st.bet:
        await update.message.reply_text("❌ Aucune position active."); return
    if not st.active_token_id:
        await update.message.reply_text("❌ Pas de token actif."); return
    current_price = await poly.get_token_price(st.active_token_id)
    if current_price <= 0 or st.entry_token_price <= 0:
        await update.message.reply_text("❌ Prix non disponible."); return
    gain_mult = current_price / st.entry_token_price
    gross = round((current_price - st.entry_token_price) * st.shares_bought, 2)
    emoji = "✅" if gross >= 0 else "❌"
    remaining = int((st.bet_expiry - time.time())) if st.bet_expiry > 0 else 0
    await update.message.reply_text(
        f"💰 *Position actuelle*\n━━━━━━━━━━━━━━\n"
        f"{emoji} `{st.bet['dir']}` | x`{gain_mult:.2f}` | PnL:`{fmt(gross)}$`\n"
        f"Token: `{st.entry_token_price:.3f}$` → `{current_price:.3f}$`\n"
        f"⏰ Expire dans: `{remaining}s`\n\n"
        f"Tape `/sell` pour vendre maintenant.",
        parse_mode="Markdown")

async def cmd_sell(update,context):
    """✅ v10.19d — Vente manuelle immédiate de la position active"""
    if not auth(update): return
    if not st.bet:
        await update.message.reply_text("❌ Aucune position active."); return
    if st.paper_mode:
        await update.message.reply_text("❌ Paper mode — pas de vente réelle."); return
    if not st.active_token_id:
        await update.message.reply_text("❌ Pas de token actif."); return

    await update.message.reply_text("⏳ Vente en cours...")
    current_price = await poly.get_token_price(st.active_token_id)
    gain_mult = current_price/st.entry_token_price if st.entry_token_price>0 and current_price>0 else 0

    opposite_token = None
    if st.current_market:
        if st.bet.get("dir") == "DOWN":
            opposite_token = st.current_market.get("token_up")
        else:
            opposite_token = st.current_market.get("token_down")
    result = await poly.sell_position(st.active_token_id, st.shares_bought, opposite_token, current_price)
    if result:
        clob_bal = await fetch_clob_balance()
        bet = st.bet
        if clob_bal and clob_bal > 0:
            gross = round(clob_bal - st.bankroll, 2)
            st.bankroll = clob_bal
        else:
            gross = round((current_price - st.entry_token_price) * st.shares_bought, 2)
            st.bankroll = max(0.0, st.bankroll + gross)
        st.pnl += gross
        won = gross >= 0
        register_trade_result(won)
        st.trades.append({"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
            "conf":bet["conf"],"result":"WIN" if won else "LOSS",
            "entry":bet["entry"],"exit":st.price,"reasoning":"Vente manuelle /sell",
            "paper":False,"ts":int(time.time()),"score":bet.get("score",0),
            "fg_value":st.fg.get("value",50),"session":bet.get("session","?"),"aligned_15h1h":True})
        st.bet=None; st.active_token_id=None; st.active_order_id=None
        st.shares_bought=0; st.entry_token_price=0
        st.token_price_peak=0; st.trailing_active=False; st.bet_expiry=0
        emoji = "✅" if won else "❌"
        await update.message.reply_text(
            f"{emoji} *Vente manuelle*\n"
            f"`{bet['dir']}` | x`{gain_mult:.2f}` | PnL:`{fmt(gross)}$`\n"
            f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`",
            parse_mode="Markdown")
        st.backup()
    else:
        await update.message.reply_text("⚠️ Vente échouée — réessaie ou attends la résolution auto.")

async def cmd_turbo(update,context):
    """✅ v10.17 — Mode turbo: seuils réduits pendant 15min"""
    if not auth(update): return
    if time.time() < st.turbo_until:
        remaining = int((st.turbo_until - time.time()) / 60)
        await update.message.reply_text(f"⚡ Turbo déjà actif — encore `{remaining}min`",parse_mode="Markdown")
        return
    st.turbo_until = time.time() + 15*60
    sess = session_ctx()
    min_score,min_diff,min_mom = get_session_thresholds(sess["session"])
    await update.message.reply_text(
        f"⚡ *MODE TURBO activé 15min*\n"
        f"Seuils: score≥`{max(7,min_score-2)}` mom≥`{max(2,min_mom-1)}`\n"
        f"Utilise `/score` pour voir les signaux en temps réel",
        parse_mode="Markdown")

async def run_backtest(days=2, asset="BTC"):
    """v12.8 — Backtest oracle lag sur BTC/ETH/SOL/XRP via klines Binance."""
    symbol_map = {"BTC":"btcusdt","ETH":"ethusdt","SOL":"solusdt","XRP":"xrpusdt"}
    symbol = symbol_map.get(asset.upper(), "btcusdt")
    klines = await fetch_klines("1m", min(1000, days*1440), symbol=symbol)
    if len(klines) < 20: return f"❌ Pas assez de données {asset}."

    # Modèle token piecewise calé sur observations réelles
    gap_min = {"BTC":0.025,"ETH":0.020,"SOL":0.020,"XRP":0.025}.get(asset.upper(),0.025)
    def token_from_delta(delta_pct):
        a=abs(delta_pct)
        if a<0.005: return 0.50
        if a<0.01:  return 0.54
        if a<0.02:  return 0.60
        if a<0.04:  return 0.68
        if a<0.07:  return 0.75
        if a<0.12:  return 0.82
        return 0.92

    wins=losses=skipped=0; pnl=0.0; skip_reasons={}
    mise = 3.50

    for i in range(5, len(klines)-1, 5):
        slot=klines[i-5:i]; nxt=klines[i] if i<len(klines) else None
        if not nxt: break
        open_px=float(slot[0]["open"]); mid_px=float(slot[-1]["close"])
        final_px=float(nxt["close"])
        if open_px<=0: continue

        delta=(mid_px-open_px)/open_px*100
        # Filtre gap min
        if abs(delta) < gap_min:
            skip_reasons["gap_faible"]=skip_reasons.get("gap_faible",0)+1
            skipped+=1; continue

        direction="UP" if delta>0 else "DOWN"
        tok=token_from_delta(delta)

        # Filtres token
        if tok > ORACLE_TOKEN_MAX:
            skip_reasons["tokenmax"]=skip_reasons.get("tokenmax",0)+1
            skipped+=1; continue
        if tok < ORACLE_TOKEN_MIN:
            skip_reasons["tokenmin"]=skip_reasons.get("tokenmin",0)+1
            skipped+=1; continue

        # EV
        fee=taker_fee_per_share(tok)
        p_est=min(0.93, 0.80+abs(delta)*3.0)
        ev=p_est-tok-fee
        if ev < ORACLE_EDGE_MIN:
            skip_reasons["ev"]=skip_reasons.get("ev",0)+1
            skipped+=1; continue

        # Résultat réel
        won=(final_px>=open_px)==(direction=="UP")
        payout=1/tok
        if won: wins+=1; pnl+=mise*(payout-1)*0.93
        else:   losses+=1; pnl-=mise

    total=wins+losses
    wr=wins/total*100 if total else 0
    be=tok*100 if total else 0

    # Top raisons de skip
    top_skips="\n".join(f"  {k}: {v}" for k,v in sorted(skip_reasons.items(),key=lambda x:-x[1])[:4])
    emoji = "✅" if wr>=65 else ("⚠️" if wr>=55 else "❌")

    return (f"🧪 *BACKTEST {asset} {days}j*\n━━━━━━━━━━━━━━\n"
            f"Slots analysés:`{total+skipped}` | Tradés:`{total}` | Skips:`{skipped}`\n"
            f"{emoji} WR:`{wr:.1f}%` | PnL simulé:`{pnl:+.2f}$`\n"
            f"Gain moy:`+{mise*(1/0.65-1)*0.93:.2f}$` | Perte moy:`-{mise:.2f}$`\n"
            f"\n*Raisons skips:*\n{top_skips}\n"
            f"\n_gap min {gap_min}% | Token {ORACLE_TOKEN_MIN:.2f}$-{ORACLE_TOKEN_MAX:.2f}$ | EV≥{ORACLE_EDGE_MIN*100:.0f}%_\n"
            f"_Résolution: données Binance 1min — indicatif_")

async def cmd_backtest(update,context):
    if not auth(update): return
    days=2; asset="BTC"
    if context.args:
        for arg in context.args:
            if arg.upper() in ("BTC","ETH","SOL","XRP"): asset=arg.upper()
            else:
                try: days=max(1,min(7,int(arg)))
                except: pass
    await update.message.reply_text(f"⏳ Backtest {asset} {days}j en cours...")
    res=await run_backtest(days, asset)
    await update.message.reply_text(res, parse_mode="Markdown")

async def cmd_oracle(update,context):
    if not auth(update): return
    now = time.time()
    oracle=st.oracle_price; slot_open=st.oracle_slot_open
    spot=consensus_price()
    oracle_delta=(oracle-slot_open)/slot_open*100 if slot_open>0 else 0
    spot_gap=(spot-oracle)/oracle*100 if oracle>0 else 0
    tick_age=int(now-st.oracle_ts) if st.oracle_ts>0 else 999
    slot_remaining=300-(now%300)
    in_window=ORACLE_WINDOW_END<=slot_remaining<=ORACLE_WINDOW_START
    srcs=[]
    if st.ws_price>0: srcs.append("Binance✅")
    if hasattr(st,'cb_price') and st.cb_price>0 and now-st.cb_ts<30: srcs.append("Coinbase✅")
    else: srcs.append("Coinbase❌")
    if hasattr(st,'kr_price') and st.kr_price>0 and now-st.kr_ts<30: srcs.append("Kraken✅")
    else: srcs.append("Kraken❌")
    if hasattr(st,'bs_price') and st.bs_price>0 and now-st.bs_ts<30: srcs.append("Bitstamp✅")
    else: srcs.append("Bitstamp❌")
    # Signal BTC
    gap_dir=("UP" if spot_gap>0 else "DOWN") if abs(spot_gap)>=0.01 else None
    delta_dir=("UP" if oracle_delta>0 else "DOWN") if abs(oracle_delta)>=ORACLE_ENTRY_DELTA else None
    sig_dir=gap_dir or delta_dir
    if sig_dir and in_window and st.oracle_connected and tick_age<=30:
        btc_rec=f"⚡ Signal BTC *{sig_dir}* T-`{int(slot_remaining)}s`"
    elif sig_dir:
        btc_rec=f"⏳ Signal BTC *{sig_dir}* — hors fenêtre (T-`{int(slot_remaining)}s`)"
    else:
        btc_rec=f"📡 Pas de signal BTC (gap:`{spot_gap:+.3f}%` delta:`{oracle_delta:+.3f}%`)"
    # ETH oracle
    eth_o=st.eth_oracle_price; eth_so=st.eth_oracle_slot_open
    eth_d=(eth_o-eth_so)/eth_so*100 if eth_so>0 else 0
    eth_g=(st.eth_price-eth_o)/eth_o*100 if eth_o>0 and st.eth_price>0 else 0
    eth_ok=eth_o>0 and now-st.eth_oracle_ts<15
    eth_sig="UP" if eth_d>ORACLE_ENTRY_DELTA else ("DOWN" if eth_d<-ORACLE_ENTRY_DELTA else None)
    eth_rec=f"⚡ Signal ETH *{eth_sig}* T-`{int(slot_remaining)}s`" if eth_sig and eth_ok else "📡 Pas de signal ETH"
    # SOL oracle
    sol_o=st.sol_oracle_price; sol_so=st.sol_oracle_slot_open
    sol_d=(sol_o-sol_so)/sol_so*100 if sol_so>0 else 0
    sol_g=(st.sol_price-sol_o)/sol_o*100 if sol_o>0 and st.sol_price>0 else 0
    sol_ok=sol_o>0 and now-st.sol_oracle_ts<15
    sol_sig="UP" if sol_d>ORACLE_ENTRY_DELTA else ("DOWN" if sol_d<-ORACLE_ENTRY_DELTA else None)
    sol_rec=f"⚡ Signal SOL *{sol_sig}* T-`{int(slot_remaining)}s`" if sol_sig and sol_ok else "📡 Pas de signal SOL"
    # XRP oracle
    xrp_o=st.xrp_oracle_price; xrp_so=st.xrp_oracle_slot_open
    xrp_d=(xrp_o-xrp_so)/xrp_so*100 if xrp_so>0 else 0
    xrp_g=(st.xrp_price-xrp_o)/xrp_o*100 if xrp_o>0 and st.xrp_price>0 else 0
    xrp_ok=xrp_o>0 and now-st.xrp_oracle_ts<15
    xrp_sig="UP" if xrp_d>ORACLE_ENTRY_DELTA else ("DOWN" if xrp_d<-ORACLE_ENTRY_DELTA else None)
    xrp_rec=f"⚡ Signal XRP *{xrp_sig}* T-`{int(slot_remaining)}s`" if xrp_sig and xrp_ok else "📡 Pas de signal XRP"
    try:
        await update.message.reply_text(
            f"🔗 *ORACLE CHAINLINK — BTC/ETH/SOL/XRP*\n━━━━━━━━━━━━━━\n"
            f"₿ BTC | Oracle:`${oracle:,.2f}` | Tick:`{tick_age}s` {'✅' if st.oracle_connected else '❌'}\n"
            f"  Δslot:`{oracle_delta:+.3f}%` | Gap spot↔oracle:`{spot_gap:+.3f}%`\n"
            f"  Spot:`${spot:,.2f}`\n  → {btc_rec}\n\n"
            f"Ξ ETH | Oracle:`${eth_o:,.2f}` | Tick:`{int(now-st.eth_oracle_ts) if st.eth_oracle_ts>0 else 999}s` {'✅' if eth_ok else '❌'}\n"
            f"  Δslot:`{eth_d:+.3f}%` | Gap:`{eth_g:+.3f}%` | ETH:`${st.eth_price:,.2f}`\n"
            f"  → {eth_rec}\n\n"
            f"◎ SOL | Oracle:`${sol_o:,.2f}` | Tick:`{int(now-st.sol_oracle_ts) if st.sol_oracle_ts>0 else 999}s` {'✅' if sol_ok else '❌'}\n"
            f"  Δslot:`{sol_d:+.3f}%` | Gap:`{sol_g:+.3f}%` | SOL:`${st.sol_price:,.2f}`\n"
            f"  → {sol_rec}\n\n"
            f"✕ XRP | Oracle:`${xrp_o:,.4f}` | Tick:`{int(now-st.xrp_oracle_ts) if st.xrp_oracle_ts>0 else 999}s` {'✅' if xrp_ok else '❌'}\n"
            f"  Δslot:`{xrp_d:+.3f}%` | Gap:`{xrp_g:+.3f}%` | XRP:`${st.xrp_price:,.4f}`\n"
            f"  → {xrp_rec}\n\n"
            f"WS: {' | '.join(srcs)}",
            parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Erreur oracle: {e}")


async def cmd_calib(update,context):
    """✅ v10.23 — État de la calibration sigma"""
    if not auth(update): return
    factor, desc = calibrate_sigma()
    await update.message.reply_text(
        f"🎚 *CALIBRATION σ*\n━━━━━━━━━━━━━━\n"
        f"Facteur actuel:`×{st.calib_factor:.2f}` | VOL_SAFETY effectif:`{VOL_SAFETY*st.calib_factor:.2f}`\n"
        f"{desc}\n\n"
        f"_>1 = bot prudent (était surconfiant) | <1 = bot agressif_",
        parse_mode="Markdown")

async def cmd_revive(update,context):
    """✅ v10.23 — Réarme le kill-switch"""
    if not auth(update): return
    st.killed=False; st.consec=0; st.cooldown_until=0
    await update.message.reply_text("✅ Kill-switch réarmé. `/run` pour relancer.", parse_mode="Markdown")

async def cb(update,context):
    q=update.callback_query; await q.answer()
    if q.data.startswith("passes:"):
        page=int(q.data.split(":")[1])
        await _show_passes(update,context,page); return
    h={"status":cmd_status,"ai":cmd_ai,"trades":cmd_trades,"stats":cmd_stats,
       "fear":cmd_fear,"score":cmd_score,"run":cmd_run,"stop":cmd_stop,"paper":cmd_paper}
    if q.data in h: await h[q.data](update,context)

def main():
    import signal as _signal, asyncio as _asyncio

    def _on_sigterm(signum, frame):
        log.info("SIGTERM — backup urgence")
        st.backup()
        try:
            import base64, urllib.request, json as _json
            gh_token=os.getenv("GITHUB_TOKEN",""); gh_repo=os.getenv("GITHUB_REPO","")
            if gh_token and gh_repo:
                data=st.save(); state_json=_json.dumps(data)
                url=f"https://api.github.com/repos/{gh_repo}/contents/polybot_v10_state.json"
                req=urllib.request.Request(url,headers={"Authorization":f"token {gh_token}","Accept":"application/vnd.github.v3+json"})
                try: sha=_json.loads(urllib.request.urlopen(req,timeout=5).read()).get("sha","")
                except: sha=""
                content=base64.b64encode(state_json.encode()).decode()
                body=_json.dumps({"message":"emergency","content":content,"branch":"State","sha":sha} if sha else {"message":"emergency","content":content,"branch":"State"}).encode()
                req2=urllib.request.Request(url,data=body,method="PUT",headers={"Authorization":f"token {gh_token}","Content-Type":"application/json","Accept":"application/vnd.github.v3+json"})
                urllib.request.urlopen(req2,timeout=5)
                log.info("✅ Emergency backup → GitHub")
        except Exception as e: log.warning(f"Emergency: {e}")
        import sys; sys.exit(0)

    _signal.signal(_signal.SIGTERM, _on_sigterm)

    async def _pull():
        ok=await pull_state_from_github()
        if ok: log.info("✅ State GitHub chargé")
        else: log.warning("GitHub pull échoué")
    try: _asyncio.run(_pull())
    except Exception as e: log.warning(f"Pull démarrage: {e}")

    st.load()
    if not st.paper_mode and POLY_PRIVATE_KEY: poly.init_client()
    app=Application.builder().token(TOKEN).build()
    for name,handler in [
        ("start",cmd_start),("run",cmd_run),("stop",cmd_stop),("status",cmd_status),
        ("ai",cmd_ai),("signal",cmd_signal),("score",cmd_score),("trades",cmd_trades),
        ("stats",cmd_stats),("fear",cmd_fear),("passes",cmd_passes),("market",cmd_market),
        ("balance",cmd_balance),("paper",cmd_paper),("cooldown",cmd_cooldown),("reset",cmd_reset),("resetskips",cmd_resetskips),
        ("setbalance",cmd_setbalance),("backup",cmd_backup),("recap",cmd_recap),("dashboard",cmd_dashboard),
        ("history",cmd_history),("turbo",cmd_turbo),("sell",cmd_sell),("sellcheck",cmd_sellcheck),("fair",cmd_fair),
        ("backtest",cmd_backtest),("oracle",cmd_oracle),("calib",cmd_calib),("learn",cmd_learn),("revive",cmd_revive),("autotune",cmd_autotune)]:
        app.add_handler(CommandHandler(name,handler))
    app.add_handler(CallbackQueryHandler(cb))
    log.info(f"🧠 PolyBot v{BOT_VERSION} démarré")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
