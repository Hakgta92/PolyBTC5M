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

BOT_VERSION = "12.9"

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


MIN_BET_USD     = 1.0   # ✅ v12.9 (18/06) — abaissé 2$→1$ pour suivre le 4% du BR au plus près. 1$ = garde-fou minimal au-dessus du seuil d'ordre Polymarket
FAIR_EDGE_MIN   = 0.08
MAX_BET_USD     = 8.0   # ✅ v10.26 — Max 8$ (setup exceptionnel sur BR 35$ = ~23%)
MAX_BET_PCT     = 0.15  # ✅ v10.26 — Max Kelly 15% sur setup exceptionnel
KELLY_FRACTION  = 0.25

# ✅ v10.27 — Paramètres validés sur 29,060 trades réels (polybacktest.com)
ENTRY_LAST_SECONDS = 60   # Entrée jusqu'à T-60s (polybacktest: pas trop tard)
SNIPE_MIN_PROB     = 0.72 # ✅ v10.28 — abaissé (compensé par EV gate plus strict)
SNIPE_EDGE_MIN     = 0.10 # ✅ v10.28/29 — EV net après vrais frais ≥10% (ex: token 0.65$ → p_dir≥0.77)
SNIPE_TOKEN_MIN    = 0.10 # ✅ (23/06) demande user: synchronisé avec ORACLE_TOKEN_MIN (0.41→0.10$) — sinon
                            # cette vérif de sécurité dans place_bet aurait annulé les trades 0.10$-0.36$
SNIPE_TOKEN_MAX    = 0.70 # ✅ (21/06) aligné sur la fenêtre oracle 0.41→0.70$

# ✅ v10.24 — Stop loss réintroduit
STOP_LOSS_MULT     = 0.01   # v12.4 désactivé  # Vendre si token tombe sous 45% du prix d'entrée (perte >55%)

# ═══════════ v10.23 — NOUVELLES CONSTANTES ═══════════
# Oracle lag (le meilleur edge: l'oracle bouge en <1s, l'orderbook met ~55s)
ORACLE_LAG_MIN_PCT  = 0.03   # Divergence oracle vs orderbook mini pour signaler un lag exploitable
ORACLE_FRESH_S      = 3.0    # Tick Chainlink considéré frais si <3s
# Entrée étagée
STAGED_ENTRY        = False  # ❌ DÉSACTIVÉ (demande user 20/06): 1 seul bet/tranche par slot/crypto (pas de 2e tranche)
STAGED_FRACTIONS    = [0.6, 0.4]   # 60% à la 1re entrée, 40% à la 2e si signal tient
# Maker order (presque gratuit: tout est limite sur Polymarket de toute façon)
USE_MAKER_ORDERS    = True   # Ordre limite maker = zéro frais + rebate 25%
MAKER_UNDERCUT      = 0.02   # ✅ v10.25 — 2¢ sous le prix (meilleure chance d'être maker)
# ✅ #1 — Exécution fill-aware: on confirme le fill RÉEL au lieu de supposer l'ordre rempli
FILL_WAIT_S         = 3.0    # grâce laissée au maker GTC pour être rempli avant annulation
FILL_TAKER_WAIT_S   = 1.5    # délai de vérif du fill après bascule taker (croise le spread)
MAKER_RETRY_WINDOW_S = 1.0   # ✅ (22/06) demande user: réessaie le maker ~1s seulement, puis bascule taker si pas rempli
# Calibration sigma (auto-correction de VOL_SAFETY après N trades)
CALIB_MIN_TRADES    = 30     # Trades mini avant d'auto-calibrer
# Kill-switch drawdown
KILL_SWITCH_LOSSES  = 5      # Pertes consécutives → arrêt total (au-delà du cooldown)

# ✅ v10.30 — ORACLE LAG STRATEGY (source: medium.com/mountain-movers, dev.to/fatherson)
# Edge documenté: l'oracle Chainlink (qui RÈGLE le marché) bouge en <1s
# L'orderbook Polymarket met 30-55s à suivre → fenêtre d'arb
# Strategy: si oracle a bougé X% depuis slot open ET token gagnant encore pas cher → BUY
ORACLE_ENTRY_DELTA  = 0.02  # v12.4  # ✅ v10.31 — baissé 0.05→0.03% (-0.049% bloqué mais ✅ dans passes)
ORACLE_TOKEN_MAX    = 0.70  # ✅ (21/06) demande user: token 0.41→0.70$ sur TOUTES les cryptos
ORACLE_TOKEN_MIN    = 0.10  # ✅ (23/06) demande user: min abaissé 0.41$→0.10$ (toutes cryptos)
ORACLE_EDGE_MIN     = 0.15  # v12.4  # EV minimum — 15% (momentum/meanrev/confluence sur tous assets)
# ✅ v12.9 (18/06) — EV oracle lag ETH/SOL/XRP abaissé 15%→10% (demande user). ⚠️ RISQUE DOCUMENTÉ:
# les ev-skips ETH/SOL historiques sont 0W/7L (que des pertes dans cette zone). XRP non mesuré.
# Surveillance OBLIGATOIRE: si les 1ers trades ETH/SOL/XRP à EV 10-15% perdent, remonter à 15%.
ORACLE_EDGE_MIN_ALT = 0.05  # ✅ (22/06) demande user: EV mini oracle lag abaissé à 5%
LIQ_PRESSURE_WINDOW_S = 30      # ✅ (22/06) demande user: fenêtre de mesure pression liquidations (s)
LIQ_PRESSURE_MIN_USD  = 50000   # ✅ (22/06) seuil minimum (USD net) pour considérer le signal liquidation fiable
# ✅ (23/06) demande user: GARDE DE RETOURNEMENT — vend la position en cours si le marché part clairement
# contre nous, au lieu d'attendre la résolution. Exige une CONFIRMATION (pas un seul tick bruité, même
# logique que le lissage OB) sauf en cas de cascade de liquidation forte (réaction immédiate justifiée).
REVERSAL_GUARD_ENABLED   = True
REVERSAL_DELTA_PCT       = 0.05   # mouvement oracle contre nous (%, depuis l'open du slot) pour être "candidat"
REVERSAL_CONFIRM_READS   = 3      # nb de lectures consécutives d'accord nécessaires (~check_interval×3)
REVERSAL_CHECK_INTERVAL_S = 2.0   # fréquence de vérification du garde
REVERSAL_LIQ_OVERRIDE_USD = 100000  # cascade de liquidation contre nous ≥ ce seuil → sortie immédiate, sans attendre la confirmation
REVERSAL_MIN_REMAINING_S = 4       # sous ce temps restant, plus le temps d'agir → on n'essaie même pas
RESOLUTION_FIX_TS = 1782195288  # ✅ (22/06) timestamp du fix du bug de résolution (faux LOSS sur de vrais WIN,
                                 # voir _resolve_expired_bet). Tout trade AVANT ça a pu être mal résolu — /edge
                                 # les exclut du calcul pour ne pas fausser l'edge réel avec de vieilles données corrompues.
# ✅ v12.9 (18/06) — STRATÉGIE OB SIGNAL (demande user, basée sur slot recorder: OB acheteur→73% UP n=237,
# OB vendeur→88% DOWN n=156, sur marché neutre). Trade dans le sens du carnet quand l'imbalance est nette.
# ⚠️ NON VALIDÉ en exécution réelle (le 73% est mesuré à la résolution, possible look-ahead). Mise mini, surveillance.
OB_SIGNAL_ENABLED   = True
OB_SIGNAL_THRESHOLD = 0.12   # ✅ v12.9 (18/06) — abaissé 0.15→0.12 (demande user) pour plus de trades. Le filtre EV (3%) bloque naturellement les tokens trop chers, donc la qualité reste protégée
OB_SIGNAL_TOKEN_MIN = 0.40   # éviter les tokens déjà trop pricés ou trop incertains
OB_SIGNAL_TOKEN_MAX = 0.75
OB_SIGNAL_WIN_START = 150     # ✅ v12.9 (19/06) — élargi T-90→T-150 (demande user) pour capter plus de signaux OB. Fin reste T-30s
OB_SIGNAL_WIN_END   = 30
OB_SIGNAL_EV_MIN     = 0.03  # EV min dédié OB (bas car signal mesuré à 73% = edge réel; un seuil élevé bloquerait tout trade)
# ✅ (21/06) demande user — Stratégie ob_oracle_disagree (RÉEL, BTC uniquement): trade quand le carnet
# (OB) et l'oracle DIVERGENT; on suit le carnet (acheteurs→UP, vendeurs→DOWN). Token 0.41-0.75$, mise 2% BR.
OB_DISAGREE_ENABLED   = True
OB_DISAGREE_THRESHOLD = 0.15   # |OB imbalance| minimum pour agir
OB_DISAGREE_TOKEN_MIN = 0.41
OB_DISAGREE_TOKEN_MAX = 0.75
OB_DISAGREE_PCT       = 0.02   # 2% du bankroll
# ✅ v12.9 (18/06) — EV minimum SPÉCIFIQUE BTC oracle lag abaissé à 8% (Sonnet: ev-skips BTC 8W/2L=80%,
# l'EV semblait sous-estimé par le token élevé au dénominateur). UNIQUEMENT BTC oracle lag — ETH/SOL/XRP
# restent à 15% car leurs ev-skips sont 0W/7L (baisser = acheter des pertes). Surveillance rapprochée:
# si les 1ers trades BTC à EV 8-15% perdent, remonter à 15%.
ORACLE_EDGE_MIN_BTC = 0.05  # ✅ (22/06) demande user: EV mini oracle lag abaissé à 5%

# ✅ (21/06) demande user — FIABILISATION oracle_lag (#1→#5):
# #1 Marge de sécurité dépendante du temps: la résolution = oracle_close vs oracle_open, donc le delta
#    oracle doit dépasser une fraction du mouvement résiduel attendu (σ_oracle·√t_restant) sinon il peut
#    encore s'inverser avant la clôture. Strict tôt dans le slot, permissif près de T-30s.
ORACLE_SAFETY_K       = 0.12   # delta requis ≥ K × (σ·√t_restant). 0=désactivé, ↑=plus strict. (21/06: 0.45→0.12, sur-bloquait)
ORACLE_VOL_LOOKBACK   = 60     # fenêtre (s) d'estimation de la volatilité réalisée de l'oracle/spot
ORACLE_SAFETY_HORIZON = 60     # plafond (s) du temps restant projeté dans √t (évite de surpunir les entrées tôt dans le slot)
# #3 Persistance du gap (anti-spike): le spot doit être resté du bon côté de l'oracle, pas un pic isolé.
GAP_PERSIST_LOOKBACK  = 5      # fenêtre (s) de vérification de persistance du gap
GAP_PERSIST_MIN_FRAC  = 0.70   # fraction min de ticks du bon côté de l'oracle
# #4 Intégrité de la source de résolution (Chainlink): le bot lit EXACTEMENT le feed qui résout
#    (topic crypto_prices_chainlink). On exige sa fraîcheur + un open de slot capturé près de la frontière.
CHAINLINK_MAX_AGE     = 25     # âge max (s) du dernier tick Chainlink — au-delà, delta non fiable → skip
ORACLE_OPEN_LAG_MAX   = 12     # retard max (s) de capture de l'open du slot; au-delà ET delta marginal → skip
ORACLE_OPEN_LAG_DELTA = 0.020  # delta (%) sous lequel un open capturé tard rend le sens non fiable
# #5 Filtre spread du carnet Polymarket: un book large érode l'entrée réelle (frais inclus) → -EV caché.
ORACLE_MAX_SPREAD     = 0.05   # spread max (¢ en fraction, 0.05=5¢) toléré sur le token tradé
# #2 Calibration empirique de p_oracle: win-rate réel par bucket (asset/signal/|delta|/votes), mélangé
#    à la formule a priori via shrinkage bayésien (K pseudo-observations). Auto-corrige l'EV au fil des trades.
ORACLE_CALIB_PRIOR_K  = 25     # poids de l'a priori (formule). ↑=fait moins confiance à l'empirique tôt
ORACLE_CALIB_MIN_N    = 8      # nb min d'observations dans le bucket avant d'ajuster p_oracle

# ✅ v12.9 — 4ème stratégie CONFLUENCE (/conf): combine oracle (biais) × régime/setup (mean-rev ou momentum) × bruit
# Formule multiplicative: TDS = oracle_score × setup_score × (1-noise_penalty). Seuils de départ raisonnés, À CALIBRER.
TDS_GAP_MIN          = 0.025  # seuil minimum gap oracle pour avoir un biais (cohérent avec gap_min existant)
TDS_GAP_STRONG       = 0.060  # gap au-delà duquel le biais oracle est "fort" (score oracle=1.0)
TDS_OVEREXT_STRONG   = 0.15   # overext Bollinger pour un setup mean-rev "fort" (score setup=1.0)
TDS_RET60S_STRONG    = 0.60   # ret60s pour un setup momentum "fort" (score setup=1.0)
TDS_MIN_SCORE        = 0.35   # TDS minimum pour trader (produit de 3 facteurs <1 → seuil plus bas qu'un score additif)
TDS_ADAPT_MIN_SAMPLE = 20     # nb trades minimum par branche avant ajustement adaptatif (anti-overfitting, vs 5 proposé)
TDS_TOKEN_MIN        = 0.52
TDS_TOKEN_MAX        = 0.72

# ✅ v12.9 — SHADOW DOWN (mode log-only, demande user 18/06): mesure si les DOWN qu'on rate
# en marché baissier (gap+ / delta- persistant, SANS chute brutale ret3s) auraient gagné.
# AUCUN trade réel — juste un log_skip taggé shadow_down, résolu par le système existant.
# Hypothèse à valider AVANT toute implémentation réelle: ces DOWN sont-ils un edge ou un piège?
SHADOW_DOWN_ENABLED      = True   # passer à False pour désactiver le shadow logging
SHADOW_DOWN_GAP_MIN      = 0.005  # gap positif minimum (spot encore au-dessus oracle figé)
SHADOW_DOWN_DELTA_MIN    = 0.010  # |delta négatif| minimum (oracle descend de façon nette)
ORACLE_WINDOW_START = 45    # ✅ (22/06) demande user: fenêtre T-45s→T-5s (fin abaissée de T-10s)
ORACLE_WINDOW_END   = 5     # ✅ (22/06) demande user: fenêtre T-45s→T-5s (fin abaissée de T-10s)
# ✅ v10.36 — Filtres WR validés par étude live (medium.com/@gwrx2005, mars 2026)
# Source: filtre 10min → -93% pertes, seuils relevés → -73% fréquence = bien meilleur WR
ORACLE_DELTA_CONTRA_MAX = 0.03  # Si votes=1/3, delta contre doit être < 0.03% sinon skip
ORACLE_GAP_MIN_STRONG   = 0.05  # Gap "fort" = au-delà de ce seuil, même votes=1/3 accepté
ORACLE_TREND_10MIN      = 0.08  # Filtre tendance 10min: si BTC contre-tendance de 0.08%, skip
ORACLE_GAP_CONFIRM_RET  = 0.03  # v11.1 fallback (quand historique gap insuffisant)
GAP_PERSIST_RATIO      = 0.60   # ✅ v11.1 — 60% des points doivent être du même côté


TAKE_PROFIT_MULT    = 2.0
TRAILING_PEAK_MULT  = 99.0  # v12.4 désactivé
TRAILING_STOP_MULT  = 1.3
TAKE_PROFIT_CHECK   = 15   # ✅ v10.22 — 15s (avant: 30s, trop lent sur du 5min)
POLY_FEE            = 0.02 # Legacy: estimation flat pour le paper mode uniquement
MAX_CONSEC_LOSS     = 2
COOLDOWN_MIN        = 0      # v12.4
MAX_TRADES_PER_H    = 3    # ✅ v10.26 — Max 3/heure (supprimé la limite 1, garde-fou à 3)
CONSERVATIVE_AFTER_LOSSES = 3  # ✅ (22/06) demande user: mode conservateur après 3 pertes consécutives (était 2)
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

CLAUDE_API    = "https://api.anthropic.com/v1/messages"
FEAR_GREED_API= "https://api.alternative.me/fng/?limit=1"
DATA_FILE     = "polybot_v10_state.json"
BACKUP_FILE   = "polybot_v10_backup.json"

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO,
    handlers=[logging.FileHandler("polybot_v10.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)

# ✅ /lasterrors (demande user 20/06) — buffer mémoire des WARNING/ERROR, interrogeable depuis Telegram
# sans avoir besoin des logs Railway. Capture automatiquement tout log.warning/log.error du bot.
from collections import deque as _deque
_RECENT_ERRORS = _deque(maxlen=50)
class _MemErrorHandler(logging.Handler):
    def emit(self, record):
        try:
            if record.levelno >= logging.WARNING:
                msg = record.getMessage()
                # ✅ Sans ça, les exceptions non gérées par PTB ("No error handlers are
                # registered, logging exception.") n'affichaient que ce message générique
                # dans /lasterrors — le vrai type/cause de l'exception (dans la traceback
                # attachée au record) était silencieusement perdu.
                if record.exc_info:
                    import traceback as _tb
                    tb_lines = _tb.format_exception(*record.exc_info)
                    msg = msg + " | " + "".join(tb_lines[-2:]).strip().replace("\n", " ")
                _RECENT_ERRORS.append((time.time(), record.levelname, msg))
        except Exception:
            pass
_mem_err_handler = _MemErrorHandler(level=logging.WARNING)
logging.getLogger().addHandler(_mem_err_handler)

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

    # ✅ v10.26 — 3 tiers selon EV réelle. Fractions = multiples de KELLY_FRACTION (config),
    # caps = parts de MAX_BET_PCT (config) → ces 2 réglages pilotent désormais réellement le sizing.
    ev_real = win_prob - token_price  # EV approximative
    if ev_real >= 0.15 or win_prob >= 0.92:
        # TIER 3 — EXCEPTIONNEL
        fraction = KELLY_FRACTION * 2.2
        tier_pct = MAX_BET_PCT          # cap plein (~15% BR par défaut)
        tier_name = "EXCEPTIONNEL"
    elif ev_real >= 0.10 or win_prob >= 0.85:
        # TIER 2 — FORT
        fraction = KELLY_FRACTION * 1.6
        tier_pct = MAX_BET_PCT * (2/3)  # ~10% BR
        tier_name = "FORT"
    else:
        # TIER 1 — NORMAL
        fraction = KELLY_FRACTION
        tier_pct = MAX_BET_PCT * (1/3)  # ~5% BR
        tier_name = "NORMAL"

    raw_bet = bankroll * min(kp * fraction * liquidity_factor, tier_pct)
    # ✅ #7 — Plancher PROPORTIONNEL à l'edge (avant: 4% BR fixe quel que soit l'edge → trades
    # marginaux sur-dimensionnés). De 1% BR (edge ~nul) à 4% BR (edge fort, EV≥15%).
    edge_ratio = min(1.0, max(0.0, ev_real / 0.15))
    floor_pct = 0.01 + 0.03 * edge_ratio
    dynamic_min = max(MIN_BET_USD, round(bankroll * floor_pct, 2))
    # ✅ MAX_BET_USD est un PLAFOND ABSOLU — avant, dynamic_min (% du bankroll, non plafonné)
    # passait par-dessus via max(), donc un bankroll qui grossit (paper mode) faisait grimper
    # les mises à l'infini malgré le cap. min() final = plafond strict quel que soit dynamic_min.
    result = round(min(MAX_BET_USD, max(dynamic_min, raw_bet)), 2)
    log.debug(f"Kelly tier={tier_name} EV={ev_real:.2f} P={win_prob:.2f} floor={floor_pct*100:.1f}% → {result:.2f}$")
    return result

def kelly_bet_secondary(bankroll, win_prob, payout_mult, confidence=1.0):
    """
    ✅ v12.9 — Kelly DÉDIÉ momentum + mean-reversion + confluence (multi-asset), séparé de kelly_bet() partagée
    (kelly_bet a un plancher dynamique ~4% BR minimum, incompatible avec un cap 1-3%).
    Fraction conservatrice (0.25x Kelly), cap strict entre 1% et 3% du bankroll.
    Stratégies secondaires (pas l'oracle lag) → sizing volontairement plus prudent.
    ✅ v12.9 — paramètre `confidence` (défaut 1.0 = comportement identique, AUCUN changement pour
    momentum/meanrev qui ne le passent pas). Permet un sizing dynamique pour la confluence:
    confidence>1.0 augmente la mise (toujours capée 1-3%), <1.0 la réduit.
    """
    if win_prob <= 0 or payout_mult <= 1:
        return 0.0
    b = payout_mult - 1
    q = 1 - win_prob
    kp = (win_prob * b - q) / b
    if kp <= 0:
        return 0.0  # Edge négatif → ne pas trader
    pct = min(max(kp * 0.25, 0.01), 0.03)  # cap strict 1%-3% BR (base)
    pct = min(max(pct * confidence, 0.01), 0.03)  # ajustement confidence, cap 1-3% toujours respecté
    # ✅ MAX_BET_USD = plafond absolu en $ — le cap 1-3% seul ne suffit pas si le bankroll
    # grossit beaucoup (paper mode), il fait grimper la mise en $ sans limite.
    result = round(min(MAX_BET_USD, bankroll * pct), 2)
    log.debug(f"Kelly secondary: kp={kp:.3f} pct={pct*100:.1f}% conf={confidence:.2f} → {result:.2f}$")
    return result

def kelly_bet_oracle(bankroll, win_prob, payout_mult, token_price=0.5, votes=0):
    """
    ✅ Kelly DÉDIÉ oracle_lag (demande user 21/06, toutes cryptos BTC/ETH/SOL/XRP) — séparé de
    kelly_bet() (tiers 5/10/15% BR, partagée avec job_tick/ob_signal). Cible volontairement plus
    étroite et plus prudente: 3%-4% du bankroll, le curseur dans cette plage étant piloté par la
    force du setup (EV réelle + nombre de votes pour la direction), pas par des tiers larges.
    """
    if win_prob <= 0 or payout_mult <= 1:
        return 0.0
    b = payout_mult - 1
    q = 1 - win_prob
    kp = (win_prob * b - q) / b
    if kp <= 0:
        return 0.0  # Edge négatif → ne pas trader

    liquidity_factor = 1.0
    if token_price < 0.15 or token_price > 0.92:
        liquidity_factor = 0.8

    ev_real = win_prob - token_price
    edge_ratio = min(1.0, max(0.0, ev_real / 0.15))
    vote_ratio = min(1.0, max(0.0, votes / 6.0))  # dir_votes compte 6 signaux (cf. job_oracle_lag*)
    strength = max(edge_ratio, vote_ratio)
    pct = 0.02 + 0.02 * strength  # ✅ (21/06) demande user: adaptatif 2% (faible) → 4% (edge fort/votes max)
    raw_bet = bankroll * pct * liquidity_factor
    result = round(min(MAX_BET_USD, max(MIN_BET_USD, raw_bet)), 2)
    log.debug(f"Kelly oracle: kp={kp:.3f} pct={pct*100:.1f}% strength={strength:.2f} → {result:.2f}$")
    return result

# ═══════════ ✅ (21/06) FIABILISATION oracle_lag — helpers #1/#2/#3 ═══════════
def oracle_vol_pct(pts, now, lookback=ORACLE_VOL_LOOKBACK):
    """#1 — Volatilité réalisée du prix (spot, proxy de l'oracle qui le suit): écart-type des rendements
    1s, exprimé en %/√s. Renvoie None si données insuffisantes (l'appelant ne bloque alors PAS).
    ✅ (21/06) ÉCHANTILLONNAGE À 1s: avant on prenait CHAQUE tick aggTrade (plusieurs/seconde) et on
    divisait par √dt → le bruit de microstructure sous-seconde gonflait σ massivement (×5-10), donc le
    mouvement projeté écrasait tout petit delta oracle → "marge insuffisante" partout. On agrège
    désormais 1 prix par seconde (dernier de chaque seconde) avant de calculer les rendements 1s."""
    rows = [(t, p) for t, p in pts if now - t <= lookback and p > 0]
    if len(rows) < 8:
        return None
    # 1 point par seconde entière (le dernier prix vu dans cette seconde)
    per_sec = {}
    for t, p in rows:
        per_sec[int(t)] = p
    secs = sorted(per_sec)
    if len(secs) < 6:
        return None
    rets = [(per_sec[secs[i]] - per_sec[secs[i-1]]) / per_sec[secs[i-1]]
            for i in range(1, len(secs)) if per_sec[secs[i-1]] > 0]
    if len(rets) < 5:
        return None
    m = sum(rets) / len(rets)
    var = sum((x - m) ** 2 for x in rets) / len(rets)
    return math.sqrt(var) * 100.0  # %/√s

def oracle_safety_ok(pts, oracle_delta_pct, remaining, now):
    """#1 — Marge de sécurité dépendante du temps restant. La résolution = oracle_close vs oracle_open;
    pour que le pari tienne, le delta doit dépasser une fraction du mouvement résiduel attendu
    (σ·√t_restant). Renvoie (ok, expected_move_pct). ok=True si la vol n'est pas estimable.
    ✅ (21/06) horizon projeté plafonné à ORACLE_SAFETY_HORIZON pour ne pas surpunir les entrées tôt."""
    if ORACLE_SAFETY_K <= 0:
        return True, 0.0
    sig = oracle_vol_pct(pts, now)
    if sig is None:
        return True, 0.0
    horizon = min(max(1.0, remaining), ORACLE_SAFETY_HORIZON)
    expected = sig * math.sqrt(horizon)
    return (abs(oracle_delta_pct) >= ORACLE_SAFETY_K * expected), expected

def gap_persistent(pts, oracle_price, gap_dir, now, lookback=GAP_PERSIST_LOOKBACK):
    """#3 — Anti-spike: le spot doit être resté du bon côté de l'oracle sur `lookback`s (≥ fraction min),
    pas un pic isolé d'1 tick qui mean-revert et referme le gap dans le mauvais sens. True si données
    insuffisantes (ne bloque pas)."""
    if not gap_dir or oracle_price <= 0:
        return True
    rows = [p for t, p in pts if now - t <= lookback and p > 0]
    if len(rows) < 4:
        return True
    good = sum(1 for p in rows if (p > oracle_price if gap_dir == "UP" else p < oracle_price))
    return good / len(rows) >= GAP_PERSIST_MIN_FRAC

def oracle_bucket(asset, signal, delta_pct, votes):
    """#2 — Clé de bucket pour la calibration empirique de p_oracle (granularité volontairement grossière
    pour accumuler assez d'observations): asset / type de signal / magnitude delta / tranche de votes."""
    ad = abs(delta_pct)
    dmag = "d0" if ad < 0.01 else ("d1" if ad < 0.03 else ("d2" if ad < 0.06 else "d3"))
    vb = "v2" if votes < 3 else ("v3" if votes < 4 else "v4")
    return f"{asset}|{signal}|{dmag}|{vb}"

def oracle_calibrated_p(prior_p, bucket):
    """#2 — Mélange l'a priori (formule) au win-rate empirique du bucket via shrinkage bayésien:
    p = (prior·K + wins) / (K + n). Tant que n < ORACLE_CALIB_MIN_N on garde l'a priori tel quel."""
    rec = st.oracle_calib.get(bucket)
    if not rec:
        return prior_p
    wins, n = rec[0], rec[1]
    if n < ORACLE_CALIB_MIN_N:
        return prior_p
    blended = (prior_p * ORACLE_CALIB_PRIOR_K + wins) / (ORACLE_CALIB_PRIOR_K + n)
    return max(0.05, min(0.97, blended))

def oracle_calib_update(bucket, won):
    """#2 — Met à jour le compteur empirique [wins, total] du bucket à la résolution d'un trade oracle."""
    if not bucket:
        return
    rec = st.oracle_calib.get(bucket, [0, 0])
    rec[0] += 1 if won else 0
    rec[1] += 1
    st.oracle_calib[bucket] = rec

def ob_persistent_signal(asset, micro_sig, ofi):
    """✅ (22/06) demande user: LISSAGE + PERSISTANCE du signal OB (microprice/OFI) du carnet Polymarket.
    Une lecture instantanée unique est du bruit exploitable (spoofing/iceberg) — on exige que le signal
    garde le MÊME sens sur plusieurs lectures récentes (~5s, min 3 échantillons, ≥70% d'accord) avant
    de le considérer fiable. Retourne (micro_lissé, ofi_lissé, persistant:bool)."""
    now = time.time()
    hist = st.ob_hist.setdefault(asset, deque(maxlen=8))
    hist.append((now, micro_sig, ofi))
    recent = [h for h in hist if now - h[0] <= 5.0]
    if len(recent) < 3:
        return micro_sig, ofi, False
    micro_vals = [h[1] for h in recent]; ofi_vals = [h[2] for h in recent]
    micro_smooth = sum(micro_vals)/len(micro_vals)
    ofi_smooth = sum(ofi_vals)/len(ofi_vals)
    micro_agree = sum(1 for v in micro_vals if (v>0)==(micro_smooth>0)) / len(micro_vals)
    ofi_agree = sum(1 for v in ofi_vals if (v>0)==(ofi_smooth>0)) / len(ofi_vals)
    persistent = micro_agree >= 0.7 and ofi_agree >= 0.7
    return round(micro_smooth,4), round(ofi_smooth,2), persistent

def compute_vol_vote(volumes, direction, now):
    """✅ Vote volume (job_oracle_lag, toutes cryptos) — confirme la direction si spike de volume
    récent (qty des trades aggTrade Binance, cf. ws_*_loop). Avant ce fix les deques *_ws_volumes
    n'étaient jamais alimentées → ce vote retournait toujours 0 (code mort)."""
    vols = list(volumes)
    if len(vols) < 5:
        return 0
    vol_5s = sum(q for t, q in vols if now - t <= 5)
    vol_avg = sum(q for t, q in vols if now - t <= 30) / 6
    if vol_avg > 0 and vol_5s / vol_avg > 2.0:
        return 1 if direction == "UP" else -1
    return 0

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
        self.auth_failed=False  # ✅ (21/06) True si create_or_derive_api_key échoue (ex: 400 "Could not create api key")

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
            self.ready = True; self.auth_failed = False
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
            self.ready=True; self.auth_failed = False
            self.client_version = "v1"
            log.info("✅ Polymarket CLOB V1 initialisé"); return True
        except Exception as e:
            # ✅ (21/06) auth CLOB échouée (ex: 400 "Could not create api key" — souvent quand 2 instances
            # tournent et se volent la clé API). Flag lu par place_bet pour alerter sur Telegram.
            self.auth_failed = True
            log.error(f"Polymarket init: {e}"); return False

    async def get_market_by_slug(self, slug:str):
        """v12.9 — CORRIGÉ (bug majeur trouvé 17/06): l'ancien code utilisait /events?slug=X et
        /markets?slug=X (listes paginées avec filtrage CÔTÉ CLIENT) — ces endpoints ne garantissent
        pas un filtre exact côté serveur, donc BTC (gros volume) apparaissait souvent dans la liste
        retournée par défaut, alors qu'ETH/SOL/XRP (volume plus faible) en étaient souvent absents
        → "marché non trouvé" récurrent, confirmé par /passes montrant ce skip pour SOL/XRP en boucle.
        Fix: utilise les vrais endpoints DIRECTS documentés (docs.polymarket.com/api-reference/
        markets/get-market-by-slug et .../events/get-event-by-slug) — slug dans l'URL, match exact
        garanti côté serveur, peu importe le volume de l'asset."""
        headers={"User-Agent":"Mozilla/5.0","Accept":"application/json",
                 "Referer":"https://polymarket.com/","Origin":"https://polymarket.com"}
        for endpoint in [f"/events/slug/{slug}", f"/markets/slug/{slug}"]:
            try:
                async with aiohttp.ClientSession(headers=headers) as s:
                    async with s.get(f"{POLY_GAMMA}{endpoint}",
                                     timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status==200:
                            data=await r.json()
                            if not isinstance(data,dict): continue
                            # /events/slug/{slug} → objet Event avec "markets":[...]
                            # /markets/slug/{slug} → objet Market direct (pas de liste à filtrer)
                            markets_list = data.get("markets")
                            candidates = markets_list if markets_list else [data]
                            for m in candidates:
                                ids=m.get("clobTokenIds","[]")
                                if isinstance(ids,str):
                                    try: ids=json.loads(ids)
                                    except: ids=[]
                                if len(ids)>=2:
                                    return {"token_up":ids[0],"token_down":ids[1],
                                        "question":data.get("title",m.get("question",slug)),
                                        "condition_id":m.get("conditionId",""),
                                        "end_date":m.get("endDate",""),"market_slug":slug}
            except Exception as e: log.warning(f"get_market_by_slug {slug}: {e}")
        return None

    async def find_btc_5min_market(self):
        """v12.9 — CORRIGÉ (même bug que get_market_by_slug, fix 17/06): utilise maintenant les
        endpoints DIRECTS /events/slug/{slug} et /markets/slug/{slug} au lieu de listes paginées
        filtrées côté client. Garde le retry sur 3 timestamps (actuel, +300, -300) pour absorber
        un éventuel décalage d'horloge."""
        now=int(time.time()); current_ts=(now//300)*300
        headers={"User-Agent":"Mozilla/5.0","Accept":"application/json",
                 "Referer":"https://polymarket.com/","Origin":"https://polymarket.com"}
        for ts in [current_ts,current_ts+300,current_ts-300]:
            slug=f"btc-updown-5m-{ts}"
            for endpoint in [f"/events/slug/{slug}", f"/markets/slug/{slug}"]:
                try:
                    async with aiohttp.ClientSession(headers=headers) as s:
                        async with s.get(f"{POLY_GAMMA}{endpoint}",
                                         timeout=aiohttp.ClientTimeout(total=10)) as r:
                            if r.status==200:
                                data=await r.json()
                                if not isinstance(data,dict): continue
                                markets_list = data.get("markets")
                                candidates = markets_list if markets_list else [data]
                                for m in candidates:
                                    ids=m.get("clobTokenIds","[]")
                                    if isinstance(ids,str):
                                        try: ids=json.loads(ids)
                                        except: ids=[]
                                    if len(ids)>=2:
                                        return {"token_up":ids[0],"token_down":ids[1],
                                            "question":data.get("title",m.get("question",slug)),
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

    async def get_recent_trades(self, token_id, limit=20):
        """✅ v12.9 — Order flow: derniers trades réels sur le marché Polymarket (pas le spot Binance).
        Retourne une liste de dicts {price, size, side, ts}. Permet de voir si du smart money entre
        juste avant la résolution. Lecture seule, best-effort (retourne [] si indisponible)."""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{POLY_HOST}/trades", params={"market": token_id, "limit": limit},
                                 timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status==200:
                        data = await r.json()
                        trades = data if isinstance(data, list) else data.get("history", data.get("trades", []))
                        out=[]
                        for t in trades[:limit]:
                            if not isinstance(t, dict): continue
                            out.append({"price": float(t.get("price",0) or 0),
                                        "size": float(t.get("size",0) or 0),
                                        "side": t.get("side","") or t.get("taker_side",""),
                                        "ts": t.get("timestamp", t.get("match_time",0))})
                        return out
        except Exception as e:
            log.debug(f"get_recent_trades: {e}")
        return []

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
                # Maker: undercut léger (BUY → un peu plus bas; on reste sous l'ask)
                maker_price=round(max(0.01,min(0.99, ref_price - MAKER_UNDERCUT)),2)
                # ✅ (21/06) demande user: minimum 1 PART (pas 5 — l'achat manuel d'1 part marche).
                # Entier de parts (× prix 2 déc = montant maker 2 déc → respecte "maker amount max 2 decimals").
                size_val=float(max(1, round(amount_float/maker_price)))
                # ✅ (anti-doublon 20/06) UNIQUEMENT le maker GTC ici. Le repli taker est géré
                # EXCLUSIVEMENT par place_bet (avec vérif de fill via le solde). Avant, ce loop
                # plaçait aussi un FAK taker en interne quand le GTC ne renvoyait pas un succès
                # "propre" alors qu'il pouvait être live → 2 ordres/fills sur le même slot, en plus
                # du taker de place_bet.
                try:
                    resp=self.client.create_and_post_order(
                        order_args=OrderArgs(token_id=token_id, price=maker_price, side=side_v2, size=size_val),
                        options=PartialCreateOrderOptions(tick_size="0.01"),
                        order_type=OrderType.GTC)
                    log.info(f"place_order GTC @{maker_price}: {resp}")
                    if resp and (resp.get("success") or resp.get("orderID")):
                        return resp.get("orderID", resp.get("id","unknown"))
                except Exception as e:
                    log.warning(f"place_order GTC: {e}")
            except Exception as e:
                log.error(f"place_order v2: {e}")
            return None
        # ✅ (22/06) demande user: taker restauré en fallback (cas client v1, ne devrait normalement
        # pas arriver puisque v2 s'init en premier, mais si ça arrive on ne veut pas rester sans rien).
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

                # ✅ (21/06) min 1 part (cf. place_order) + entier (maker amount 2 déc).
                size_val = float(max(1, round(amount_float / price_val)))

                log.info(f"V2 order: token={token_id[:10]} price={price_val} size={size_val}")

                # ✅ (21/06) FAK UNIQUEMENT (avant: boucle FAK→GTC). Un FAK partiellement rempli pouvait
                # renvoyer success=False → on plaçait alors un GTC PLEINE TAILLE qui restait sur le book et
                # se remplissait plus tard = DOUBLE achat (cas vu: 7 parts + 5 parts ETH). Un seul ordre FAK:
                # il prend ce qui est dispo immédiatement et annule le reste, jamais d'ordre résiduel.
                try:
                    resp = self.client.create_and_post_order(
                        order_args=OrderArgs(token_id=token_id, price=price_val, side=side_v2, size=size_val),
                        options=PartialCreateOrderOptions(tick_size="0.01"),
                        order_type=OrderType.FAK,
                    )
                    log.info(f"V2 FAK réponse: {resp}")
                    if resp and resp.get("success"):
                        oid = resp.get("orderID", resp.get("id", "unknown"))
                        log.info(f"✅ Ordre V2 FAK placé: {oid}")
                        return oid
                    log.warning(f"V2 FAK refusé: {resp}")
                except Exception as e:
                    log.warning(f"V2 FAK erreur: {e}")
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
            price_val = round(min(0.99, max(0.01, price)), 2)
            # ✅ size = shares, pas $ — conversion budget$ / prix (cf. place_order/place_market_order)
            size_val = round(max(5.0, float(amount_usdc)) / price_val, 2)
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

    async def get_position_size(self, token_id):
        """✅ #1 — Solde réel du token conditionnel (shares détenues). Renvoie None si non
        vérifiable (client v1 ou erreur) → permet de confirmer un fill réel au lieu de
        SUPPOSER qu'un ordre maker posé est rempli. Comparé à une baseline avant l'ordre."""
        if not self.ready or getattr(self, "client_version", "v1") != "v2": return None
        try:
            from py_clob_client_v2 import BalanceAllowanceParams
            from py_clob_client_v2.clob_types import AssetType
            resp = self.client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id))
            if resp:
                bal = resp.get("balance", resp.get("amount", 0))
                # ✅ (21/06) /1e6: les tokens conditionnels Polymarket ont 6 décimales (ERC1155) → le solde
                # brut = parts × 1e6. Sans cette division, real_shares était gonflé ×1e6 → est_gross =
                # shares-cost explosait (BR à 2.4M$) et le prix d'entrée tombait à ~0 ("Token:0.000$").
                return float(bal) / 1e6
        except Exception as e:
            log.warning(f"get_position_size: {e}")
        return None

    async def get_position_size_polled(self, token_id, baseline, tries=3, delay=0.6):
        """✅ (anti-doublon 20/06) — Lit le solde du token avec RETRY pour défaire le LAG du solde
        CLOB (un fill peut n'apparaître dans le solde qu'après 1-2s). Renvoie le solde dès qu'il
        dépasse baseline (fill confirmé, sortie immédiate), sinon le dernier solde lu après `tries`.
        Évite les faux "no-fill" qui déclenchaient un 2e ordre taker en double sur le même slot."""
        last = None
        for i in range(max(1, tries)):
            b = await self.get_position_size(token_id)
            if b is not None:
                last = b
                if b > (baseline or 0) + 1e-9:
                    return b
            if i < tries - 1:
                await asyncio.sleep(delay)
        return last

    async def cancel_order(self, order_id):
        """✅ #1 — Annule un ordre (le reliquat non rempli d'un GTC maker). Best-effort.
        Renvoie un dict {ok, already_filled, resp}: `already_filled`/`ok=False` indiquent que le
        maker n'a PAS pu être annulé (probablement déjà matché) → place_bet évite alors le taker
        pour ne pas doubler la position sur le même slot."""
        if not self.ready or not self.client or not order_id:
            return {"ok": False, "already_filled": False, "resp": None}
        for meth, arg in (("cancel", order_id), ("cancel_order", order_id), ("cancel_orders", [order_id])):
            try:
                fn = getattr(self.client, meth, None)
                if fn:
                    resp = fn(arg)
                    already = False
                    try:
                        if isinstance(resp, dict):
                            nc = resp.get("not_canceled") or resp.get("notCanceled")
                            if nc and (str(order_id) in (nc if isinstance(nc,(dict,list,set)) else [nc])):
                                already = True
                    except Exception:
                        pass
                    return {"ok": True, "already_filled": already, "resp": resp}
            except Exception as e:
                log.warning(f"cancel_order ({meth}): {e}")
        return {"ok": False, "already_filled": False, "resp": None}

    async def get_order_matched(self, order_id):
        """✅ (21/06) Best-effort: nombre de PARTS déjà matchées d'un ordre, lu DIRECTEMENT via l'API
        (autoritatif, contrairement au solde qui lag de 1-3s). Sert à bloquer le taker quand le maker
        a (partiellement) matché → anti double-fill (cas vu: 9 parts maker + 1 part taker sur ETH).
        Renvoie un float ≥0, ou None si la lib/endpoint ne le supporte pas (→ repli sur le solde)."""
        if not self.ready or not self.client or not order_id:
            return None
        for meth in ("get_order", "get_order_by_id", "get_order_status"):
            try:
                fn = getattr(self.client, meth, None)
                if not fn: continue
                resp = fn(order_id)
                if isinstance(resp, dict):
                    for k in ("size_matched", "sizeMatched", "matched_size", "filled_size", "size_filled"):
                        if resp.get(k) is not None:
                            return float(resp.get(k) or 0)
            except Exception as e:
                log.debug(f"get_order_matched ({meth}): {e}")
        return None

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
                val = round(float(bal) / 1e6, 2)
                # ✅ (21/06) garde-fou: >1M$ = lecture corrompue (allowance/glitch API), pas un vrai solde
                # d'un bot 5min. On renvoie None plutôt que de propager une valeur aberrante (cf. BR à 6.3M$).
                if val > 1_000_000 or val < 0:
                    log.warning(f"fetch_clob_balance valeur aberrante ignorée: {val}$ (raw={bal})")
                    return None
                return val
    except Exception as e:
        log.warning(f"fetch_clob_balance: {e}")
    return None

async def fetch_onchain_positions():
    """✅ #8 — Lit les positions RÉELLES détenues on-chain via la data-api Polymarket.
    Retourne la liste des positions ouvertes (size>0) ou None si indisponible."""
    wallet = POLY_PROXY_WALLET or POLY_FUNDER_WALLET
    if not wallet: return None
    try:
        url = f"https://data-api.polymarket.com/positions?user={wallet}&sizeThreshold=0.5"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return None
                data = await r.json()
                if not isinstance(data, list): return None
                return [p for p in data if float(p.get("size", 0) or 0) > 0]
    except Exception as e:
        log.warning(f"fetch_onchain_positions: {e}")
        return None

async def job_reconcile(context):
    """✅ #8 — Réconciliation au démarrage: détecte une position réelle non suivie par le bot
    (crash/redéploiement = state perdu) ou un st.bet fantôme, et ALERTE l'opérateur sur Telegram.
    Lecture seule: ne reconstruit ni ne trade automatiquement (dir/marché incertains)."""
    if st.paper_mode or not poly.ready: return
    positions = await fetch_onchain_positions()
    if positions is None: return  # API indispo → on ne conclut rien
    real_open = len(positions) > 0
    tracked = sum(1 for a in ASSETS if getattr(st, f"bet{_possfx(a)}"))  # ✅ (21/06) par crypto
    if real_open and len(positions) > tracked:
        lines = "\n".join(f"• `{p.get('asset','?')[:10]}…` {float(p.get('size',0)):.1f} sh @`{float(p.get('avgPrice',0)):.3f}$`"
                          for p in positions[:5])
        await send(context.bot,
            f"⚠️ *RÉCONCILIATION* — position(s) réelle(s) NON suivie(s):\n{lines}\n\n"
            f"_Le bot suit {tracked} position(s), {len(positions)} détectée(s) on-chain. "
            f"Vérifie et solde manuellement si besoin._")
        log.warning(f"Réconciliation: {len(positions)} position(s) réelle(s), {tracked} suivie(s)")
    elif tracked and not real_open:
        await send(context.bot,
            "⚠️ *RÉCONCILIATION* — position(s) locale(s) présente(s) mais AUCUNE position réelle on-chain "
            "(déjà résolue/vendue). Nettoyage de l'état local.")
        log.warning("Réconciliation: position(s) fantôme(s) nettoyée(s) (pas de position on-chain)")
        for a in ASSETS:
            sfx = _possfx(a)
            setattr(st, f"bet{sfx}", None); setattr(st, f"active_token_id{sfx}", None); setattr(st, f"active_order_id{sfx}", None)
            setattr(st, f"shares_bought{sfx}", 0); setattr(st, f"entry_token_price{sfx}", 0); setattr(st, f"bet_expiry{sfx}", 0)
        st.token_price_peak=0; st.trailing_active=False

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
    ✅ v10.22 — Claude n'est PLUS appelé dans le chemin chaud (job_tick).
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
        self.ob_hist={}  # ✅ (22/06) historique récent micro_sig/ofi par asset, pour lissage+persistance
        self.reversal_hist={}  # ✅ (23/06) {asset: deque[bool]} lectures récentes "retournement contre nous"
        # ✅ (22/06) demande user: liquidations + funding rate Binance Futures (nouveau outil oracle lag)
        self.liq_hist={}      # {asset: deque[(ts, side, notional_usd)]} — flux !forceOrder@arr
        self.liq_ws_task=None
        self.funding_rate={}  # {asset: dernier funding rate connu (ex: 0.0001 = 0.01%/8h)}
        self.funding_ts={}    # {asset: ts de la dernière lecture}
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
        # ✅ (ts, qty) trades aggTrade Binance — alimenté par ws_binance_loop, sert au vol_vote
        # de job_oracle_lag (était déclaré mais jamais écrit avant ce fix → vol_vote toujours 0).
        self.ws_volumes=deque(maxlen=300)
        self.ws_price=0.0
        self.gap_history=deque(maxlen=60)  # ✅ v11.1 — (ts, gap%) historique du gap spot↔oracle
        # ✅ (21/06) FIABILISATION oracle_lag: calibration empirique p_oracle (#2) + retard de capture
        # de l'open de slot par crypto (#4, intégrité de la source de résolution Chainlink).
        self.oracle_calib={}      # bucket → [wins, total]
        self.oracle_open_lag={}   # asset → secondes entre la frontière du slot et la capture de l'open
        self.ws_connected=False
        self.ws_task=None
        self.slot_open_price=0.0
        self.slot_open_ts=0
        self.last_fair={}
        self.last_decision={}; self.last_conf_score={}; self.last_mom_score=0
        self.fg={"value":50,"label":"Neutral"}; self.btc24={}
        self.tick_job=self.price_job=self.macro_job=self.tp_job=self.backup_job=self.recap_job=None
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
        self.eth_ws_prices=deque(); self.eth_ws_volumes=deque(maxlen=300)
        self.eth_oracle_price=0.0; self.eth_oracle_ts=0.0
        self.eth_oracle_slot_open=0.0; self.eth_oracle_slot_ts=0
        self.eth_last_trade_slot=0
        # ✅ ob_ts (BTC) manquait ici — n'était posé que par ws_clob_loop() au 1er message WS, donc
        # st.ob_ts crashait (AttributeError) si lu avant ça (ex: job_oracle_lag juste après démarrage).
        self.ob_imbalance=0.0; self.ob_ts=0.0; self.ob_asset_id=""
        self.eth_ob_imbalance=0.0; self.eth_ob_ts=0.0; self.eth_ob_asset_id=""; self.eth_clob_ws_task=None
        # SOL
        self.sol_price=0.0; self.sol_ts=0; self.sol_ws_task=None
        self.sol_ws_prices=deque(); self.sol_ws_volumes=deque(maxlen=300)
        self.sol_oracle_price=0.0; self.sol_oracle_ts=0.0
        self.sol_oracle_slot_open=0.0; self.sol_oracle_slot_ts=0
        self.sol_last_trade_slot=0
        self.sol_ob_imbalance=0.0; self.sol_ob_ts=0.0; self.sol_ob_asset_id=""; self.sol_clob_ws_task=None
        # ✅ v12.8 — XRP
        self.xrp_price=0.0; self.xrp_ts=0; self.xrp_ws_task=None
        self.xrp_ws_prices=deque(); self.xrp_ws_volumes=deque(maxlen=300)
        self.xrp_oracle_price=0.0; self.xrp_oracle_ts=0.0
        self.xrp_oracle_slot_open=0.0; self.xrp_oracle_slot_ts=0
        self.xrp_last_trade_slot=0
        self.momentum_last_slot=0  # v12.9 — 2ème fenêtre momentum BTC
        self.meanrev_last_slot=0  # v12.9 — 3ème fenêtre mean-reversion BTC (coordonne avec momentum_last_slot)
        # v12.9 — Extension multi-asset momentum/meanrev (ETH/SOL/XRP), sizing dédié 1-3%
        self.momentum_last_slot_eth=0; self.momentum_last_slot_sol=0; self.momentum_last_slot_xrp=0
        self.meanrev_last_slot_eth=0; self.meanrev_last_slot_sol=0; self.meanrev_last_slot_xrp=0
        self.meanrev_regime_squeeze_count=0; self.meanrev_regime_expansion_count=0  # v12.9 — résumé agrégé pour /learn
        # v12.9 — 4ème stratégie CONFLUENCE (/conf): oracle bias × régime/setup × bruit
        self.tds_last_slot=0; self.tds_last_slot_eth=0; self.tds_last_slot_sol=0; self.tds_last_slot_xrp=0
        # ✅ v12.9 — verrou slot stratégie OB signal (par asset)
        self.ob_last_slot={}  # {asset: cur_slot dernier trade OB}
        # ✅ verrou slot PAR CRYPTO partagé par TOUTES les stratégies (1 seul trade/slot/crypto,
        # peu importe la stratégie). Posé dans place_bet AVANT l'achat (race-safe), libéré si l'achat échoue.
        self.asset_trade_slot={}  # {asset: cur_slot dernier trade toutes stratégies confondues}
        self.bet_in_flight=False  # ✅ True pendant l'exécution de place_bet (anti-race single-position inter-asset)
        # ✅ Slot RÉSERVÉ pour BTC oracle lag (demande user 20/06): BTC oracle pouvait ne jamais trader
        # car bloqué par le verrou global st.bet dès qu'une AUTRE strat/asset avait une position ouverte.
        # bet2/* est une 2e position parallèle dédiée exclusivement à job_oracle_lag (BTC) — max 2 positions
        # simultanées au total (1 normale + 1 réservée BTC oracle). Même verrou asset_trade_slot["BTC"]
        # partagé donc toujours 1 seul trade BTC par slot, mais BTC oracle n'attend plus son tour.
        self.bet2=None; self.active_order_id2=None; self.active_token_id2=None
        self.entry_token_price2=0.0; self.shares_bought2=0.0; self.bet_expiry2=0
        # ✅ (21/06) demande user: SLOT RÉSERVÉ supprimé. Chaque crypto a désormais son PROPRE slot de
        # position → BTC (st.bet, sfx="") + ETH/SOL/XRP (suffixes dédiés) peuvent tous trader 1×/slot
        # en parallèle. (1 bet/crypto/slot garanti par asset_trade_slot.)
        for _a in ("eth","sol","xrp"):
            setattr(self, f"bet_{_a}", None); setattr(self, f"active_token_id_{_a}", None)
            setattr(self, f"active_order_id_{_a}", None); setattr(self, f"shares_bought_{_a}", 0.0)
            setattr(self, f"entry_token_price_{_a}", 0.0); setattr(self, f"bet_expiry_{_a}", 0)
            setattr(self, f"expiry_alerted_{_a}", False)
        self.exec_stats={"maker":0,"taker":0,"nofill":0}  # ✅ qualité d'exécution (compteurs cumulés)
        # ✅ v12.9 — SLOT RECORDER (/slots): journal de TOUS les slots résolus avec conditions + résultat réel UP/DOWN.
        # Indépendant du trading. Résolution = oracle Chainlink (close vs open), règle officielle Polymarket vérifiée.
        self.slot_records=[]    # dicts: {asset,slot,open,close,result,gap,delta,rsi,macd,dual,regime,session,ts}
        self.slot_rec_last={}   # {asset: dernier slot_start enregistré} anti-doublon
        self.slot_rec_close={}  # {(asset,slot_start): close oracle exact capturé à la bascule}
        # ✅ v12.9 — TRACKER TIMING DE PRICING: à quel T-Xs le token dépasse 0.95$? (mesure si on entre trop tard)
        self.price_timing=[]       # dicts: {asset, slot, t_remaining_at_095, token_max, ts}
        self.price_timing_seen={}  # {(asset,slot): t_remaining où token a d'abord dépassé 0.95$} pour capturer le 1er franchissement
        self.price_timing_max={}   # {(asset,slot): token_max observé sur le slot}
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
            "running":self.running,  # ✅ (21/06) persisté pour auto-reprise après redeploy
            "version":BOT_VERSION,"saved_at":int(time.time()),
            "oracle_patterns":self.oracle_patterns[-200:],
            "oracle_calib":self.oracle_calib,  # ✅ (21/06) calibration empirique p_oracle (#2)
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
                    self.oracle_calib=d.get("oracle_calib",{})  # ✅ (21/06) calibration empirique (#2)
                    self.calibration_log=d.get("calibration_log",[])
                    self.haiku_insights=d.get("haiku_insights",[])
                    # ✅ Restaurer les seuils auto-calibrés
                    global FILTER_RET3S, FILTER_DELTA_CONTRA, FILTER_GAP_STRONG
                    FILTER_RET3S=d.get("filter_ret3s", FILTER_RET3S)
                    FILTER_DELTA_CONTRA=d.get("filter_delta_contra", FILTER_DELTA_CONTRA)
                    FILTER_GAP_STRONG=d.get("filter_gap_strong", FILTER_GAP_STRONG)
                    self.calib_factor=d.get("calib_factor",1.0); self.killed=d.get("killed",False)
                    self.running=d.get("running",False)  # ✅ (21/06) auto-reprise: restauré pour relancer au démarrage si actif avant
                    age=int((time.time()-d.get("saved_at",0))/60)
                    log.info(f"✅ State {filepath} ({age}min) BR:{self.bankroll:.2f}"); return
            except Exception as e: log.error(f"Load {filepath}: {e}")

st=State()

# ─── HELPERS v10.22 ────────────────────────────────────────────────────────
def log_skip(reason, direction=None, features=None):
    """✅ v10.37 — Log skip + features oracle pour auto-calibration.
    v12.9 — Ajout tag session pour stats segmentées (Asia/EU/US)."""
    st.skipped += 1
    now = int(time.time())
    sess = session_ctx()
    entry = {"ts": now, "reason": reason, "dir": direction, "session": sess.get("session","?"),
             "slot_end": (now // 300) * 300 + 300,
             # v12.8 — snapshot prix ACTUELS + oracle au moment du log
             "snap": {
                 "BTC": (st.oracle_slot_open, st.oracle_price, st.ws_price),
                 "ETH": (st.eth_oracle_slot_open, st.eth_oracle_price, st.eth_price),
                 "SOL": (st.sol_oracle_slot_open, st.sol_oracle_price, st.sol_price),
                 "XRP": (st.xrp_oracle_slot_open, st.xrp_oracle_price, st.xrp_price),
             },
             "open_px": st.oracle_slot_open if st.oracle_slot_open>0 else st.oracle_price,
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

def log_shadow_down(asset, gap, delta, ret3s):
    """✅ v12.9 — SHADOW DOWN (log-only). Enregistre un signal DOWN 'fantôme' dans le cas
    gap+ / delta- persistant (marché baissier où l'oracle figé est encore au-dessus du spot tombant),
    SANS chute brutale (sinon c'est déjà couvert par ret3s_override qui trade DOWN réellement).
    Ne place AUCUN trade. Taggé filter='shadow_down' → isolé dans /passes, /learn, Sonnet.
    Le système de résolution existant (_resolve_pending_passes) calculera WIN/LOSS automatiquement,
    ce qui répondra à la question: ces DOWN ratés sont-ils un edge réel ou un piège (mean-reversion)?"""
    if not SHADOW_DOWN_ENABLED: return
    log_skip(f"{asset}: [SHADOW] DOWN fantôme gap{gap:+.3f}%/delta{delta:+.3f}% (log-only, pas de trade)", "DOWN",
             features={"gap":gap,"delta":delta,"ret3s":ret3s,"votes":0,"filter":"shadow_down","asset":asset})

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
    # ✅ Robuste: gère le rate-limit Telegram (RetryAfter) + repli texte brut si le Markdown casse
    # + retry sur TOUTE exception (pas seulement RetryAfter — un simple blip réseau/timeout ne
    # doit jamais faire disparaître silencieusement une notif de trade RÉEL déjà exécuté).
    plain = text.replace("*","").replace("`","").replace("_","")
    for attempt in range(3):
        try:
            await bot.send_message(chat_id=ALLOWED_UID,text=text,parse_mode=parse_mode); return True
        except Exception as e:
            ra = getattr(e, "retry_after", None)
            log.error(f"Send (markdown, essai {attempt+1}/3): {e}")
            try:
                await bot.send_message(chat_id=ALLOWED_UID,text=plain); return True
            except Exception as e2:
                log.error(f"Send (texte brut, essai {attempt+1}/3): {e2}")
                ra = ra or getattr(e2, "retry_after", None)
            if attempt < 2:
                try: await asyncio.sleep(float(ra)+0.5 if ra else 1.5*(attempt+1))
                except: pass
    log.error("Send: notif perdue après 3 tentatives (markdown + texte brut)")
    return False

async def reply_md(update, text):
    """Réponse Markdown avec repli auto en texte brut si le parsing échoue → évite les commandes
    'muettes' (ex: /calib) quand un caractère casse le Markdown Telegram."""
    try:
        await update.message.reply_text(text, parse_mode="Markdown"); return
    except Exception as e:
        log.error(f"reply_md: {e}")
        try: await update.message.reply_text(text.replace("*","").replace("`","").replace("_",""))
        except Exception as e2: log.error(f"reply_md plain: {e2}")

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

async def _resolve_expired_bet(context, asset="BTC"):
    """✅ Clôture (alerte T-30s + résolution auto) d'une position expirée, PAR CRYPTO.
    ✅ (21/06) slot réservé supprimé: chaque crypto a son propre slot (_possfx)."""
    sfx = _possfx(asset)
    bet = getattr(st, f"bet{sfx}")
    if not bet: return
    now = time.time()
    bet_expiry = getattr(st, f"bet_expiry{sfx}")
    if bet_expiry <= 0: return
    remaining = bet_expiry - now
    active_token_id = getattr(st, f"active_token_id{sfx}")
    # ✅ (21/06) entrée robuste: si le prix d'entrée mesuré est 0 (fill non vu), on retombe sur
    # bet["entry_token"] (prix pré-ordre stocké) — sinon le multiplicateur affichait "x0.00" à tort.
    entry_token_price = getattr(st, f"entry_token_price{sfx}") or bet.get("entry_token", 0)
    tag = ""

    # ✅ (21/06) alerte ~30s avant expiration (était ~1min), fenêtre large (≥ l'intervalle 30s du job
    # pour ne jamais la rater) + flag anti-doublon (était envoyée 2× si le job échantillonnait 2× la fenêtre).
    if 20 <= remaining <= 50:
        if not getattr(st, f"expiry_alerted{sfx}", False):
            setattr(st, f"expiry_alerted{sfx}", True)
            current_price = await poly.get_token_price(active_token_id) if active_token_id else 0
            gain_mult = current_price/entry_token_price if entry_token_price>0 and current_price>0 else 0
            mult_txt = f"x`{gain_mult:.2f}`" if gain_mult>0 else "x`?` _(prix d'entrée inconnu)_"
            await send(context.bot,
                f"⏰ *Position expire dans ~{int(remaining)}s*{tag}\n"
                f"`{bet['dir']}` | Token:`{current_price:.3f}$` | {mult_txt}\n"
                f"BTC:`${st.price:,.2f}`")
        return
    # ✅ Clôture automatique 60s après expiration.
    # Résultat = VRAIE résolution (slot recorder: close vs open oracle = règle Polymarket),
    # PAS le signe du solde (qui lag à cause du settlement → faux WIN/LOSS + faux BR).
    if remaining >= -60: return

    bet_asset = bet.get("asset","BTC")
    bet_slot = (int(bet.get("ts", now))//300)*300
    # 1) Outcome RÉEL via le slot recorder
    rec = next((r for r in reversed(st.slot_records)
                if r.get("asset")==bet_asset and r.get("slot")==bet_slot
                and r.get("result") in ("UP","DOWN")), None)
    won = (rec["result"] == bet["dir"]) if rec else None
    # ✅ (22/06 fix) GRÂCE déplacée ICI, avant TOUTE heuristique de fallback (prix résolu ET reconstruction
    # oracle) — les deux peuvent donner une lecture confiante mais FAUSSE basée sur un prix transitoire/
    # périmé avant que le carnet ne reflète le vrai règlement (vu: BTC et XRP marqués LOSS alors que
    # Polymarket avait déjà redeem les shares gagnantes — résolu en ~95s, donc AVANT l'ancienne grâce de
    # 3min qui ne protégeait que l'étape 3 reconstruction, pas l'étape 2 prix token). Le slot recorder
    # (étape 1, seule source qui suit EXACTEMENT la règle Polymarket) a maintenant la priorité absolue
    # pendant 3min avant qu'on tente n'importe quelle heuristique de fallback.
    if won is None and remaining > -180:
        return  # réessaie au prochain tick — laisse le slot recorder arriver
    # 2) Fallback: prix du token résolu (gagnant→~1$, perdant→~0$)
    # ✅ (22/06 fix) DOUBLE VÉRIFICATION: une lecture unique peut être périmée/transitoire (carnet pas
    # encore stabilisé juste après résolution) → on exige 2 lectures espacées de 2s qui s'accordent
    # avant de trancher, sinon on reste sur won=None (laisse le slot recorder ou le prochain tick trancher).
    if won is None and active_token_id:
        res_price = await poly.get_token_price(active_token_id)
        verdict1 = True if res_price>=0.6 else (False if 0<res_price<=0.4 else None)
        if verdict1 is not None:
            await asyncio.sleep(2.0)
            res_price2 = await poly.get_token_price(active_token_id)
            verdict2 = True if res_price2>=0.6 else (False if 0<res_price2<=0.4 else None)
            if verdict1 == verdict2:
                won = verdict1
            else:
                log.info(f"{bet_asset}: prix token incohérent entre 2 lectures ({res_price:.2f}→{res_price2:.2f}) — pas de verdict, on réessaie")
    # 3) Fallback FIABLE: mouvement de l'oracle sur le slot (open mémorisé à l'entrée vs prix
    # oracle actuel ≈ close). Corrige les FAUX LOSS quand le recorder n'a pas encore enregistré le slot
    # ET que le prix token résolu renvoie 0 (marché clos) — cas vu: BTC/SOL gagnants marqués LOSS.
    if won is None:
        slot_open_px = bet.get("slot_open_px", 0)
        _close_map = {"BTC":st.oracle_price or st.ws_price,"ETH":st.eth_oracle_price or st.eth_price,
                      "SOL":st.sol_oracle_price or st.sol_price,"XRP":st.xrp_oracle_price or st.xrp_price}
        close_px = _close_map.get(bet_asset, 0)
        if slot_open_px > 0 and close_px > 0:
            won = (("UP" if close_px >= slot_open_px else "DOWN") == bet["dir"])
            log.info(f"{bet_asset}: résultat reconstruit oracle open={slot_open_px:.2f} close={close_px:.2f} → {'WIN' if won else 'LOSS'}")
    # 4) Toujours ambigu → on réessaie (le recorder finit par enregistrer); LOSS en TOUT DERNIER recours
    # après ~10min (avant: 3min → trop tôt, défaut LOSS sur des gagnants).
    if won is None:
        if remaining > -600: return
        won = False
    log.info(f"Slot résolu {bet_asset} {bet['dir']} → {'WIN' if won else 'LOSS'} (recorder={'oui' if rec else 'non'})")
    oracle_calib_update(bet.get("calib_bucket"), won)  # ✅ (21/06) #2 — alimente la calibration empirique p_oracle
    # Montant déterministe depuis les shares (position pleine, plus de vente anticipée)
    shares = getattr(st, f"shares_bought{sfx}") or 0; entry = entry_token_price or 0
    cost = round(shares*entry, 2) if entry>0 else bet.get("amount",0)
    est_gross = round((shares - cost) if won else -cost, 2)
    # BR: solde réel si le payout a été crédité ET cohérent — UNIQUEMENT si AUCUNE autre position
    # n'est ouverte en parallèle (sinon le solde reflète plusieurs cryptos et on mal-attribuerait le
    # gain). Avec jusqu'à 4 positions simultanées (1/crypto), on retombe sur l'estimation par shares.
    # ✅ (21/06) demande user: à la résolution, BR = SOLDE POLYMARKET RÉEL récupéré MAINTENANT (pas une
    # estimation locale qui dérivait — vu: message BR 36.14$ alors que le solde réel était 47.66$).
    # gross = PnL déterministe de CE trade (parts × résultat), pour le PnL cumulé/stats uniquement.
    gross = est_gross
    st.pnl += gross
    clob_bal = await fetch_clob_balance()  # valeurs aberrantes (>1M$/<0) déjà filtrées → None
    if clob_bal is not None and clob_bal > 0:
        st.bankroll = clob_bal  # ← BR réel
    else:
        st.bankroll = max(0.0, round(st.bankroll + est_gross, 2))  # repli si lecture indispo
    register_trade_result(won)  # ✅ streaks + conservateur aussi en réel
    result_txt = "WIN" if won else "LOSS"
    if not won and st.consec >= CONSERVATIVE_AFTER_LOSSES:
        await send(context.bot, f"⚠️ *Mode conservateur activé 2h* — {st.consec} pertes consécutives")
    st.trades.append({"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
        "conf":bet["conf"],"result":result_txt,"entry":bet["entry"],"exit":st.price,
        "reasoning":"Résolution auto slot expiré","paper":False,"ts":int(now),
        "score":bet.get("score",0),"fg_value":st.fg.get("value",50),
        "session":bet.get("session","?"),"aligned_15h1h":True,"source":bet.get("source","?"),
        "asset":bet_asset,"entry_token":bet.get("entry_token",0),"t_remaining":bet.get("t_remaining",0),
        "fill_type":bet.get("fill_type","?"),"fee_est":bet.get("fee_est",0),
        "cost":cost,"shares":shares})  # ✅ (22/06) coût/shares RÉELS pour le calcul d'edge réaliste (/edge)
    setattr(st, f"bet{sfx}", None); setattr(st, f"active_token_id{sfx}", None); setattr(st, f"active_order_id{sfx}", None)
    setattr(st, f"shares_bought{sfx}", 0); setattr(st, f"entry_token_price{sfx}", 0); setattr(st, f"bet_expiry{sfx}", 0)
    setattr(st, f"expiry_alerted{sfx}", False)  # ✅ (21/06) reset flag alerte T-30s à la clôture
    if asset=="BTC": st.token_price_peak=0; st.trailing_active=False
    emoji="✅" if won else "❌"
    # ✅ demande user 21/06: mise réelle (cost = shares réelles × prix d'entrée réel) + gain réel
    # (gross, calculé depuis ces mêmes shares/prix réels, ou depuis le solde CLOB quand fiable —
    # jamais le montant Kelly demandé).
    await send(context.bot,
        f"{emoji} *Trade résolu {bet_asset}*{tag} (slot)\n"
        f"`{bet['dir']}` | Mise réelle:`{fmt(cost)}$` | Gain réel:`{fmt(gross)}$`\n"
        f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`")
    st.backup()

async def job_check_expiry(context):
    """✅ v10.18b — Alerte + clôture automatique quand slot expiré, pour CHAQUE crypto (slot réservé supprimé)."""
    if st.paper_mode: return
    for a in ASSETS:
        await _resolve_expired_bet(context, asset=a)

async def job_sync_balance(context):
    """✅ (21/06) demande user: BR TOUJOURS synchronisé avec le solde réel Polymarket (CLOB USDC).
    Lit le solde toutes les 60s → st.bankroll = solde réel. Auto-répare toute dérive/corruption
    (ex: BR aberrant à 2.4M$ → revient au vrai solde). Les valeurs aberrantes (>1M$ / <0) sont déjà
    filtrées par fetch_clob_balance (→ None, ignorées). Ignoré en paper mode.
    NB: le solde reflète le cash disponible (hors parts en cours) → c'est exactement le solde affiché
    sur Polymarket; il remonte automatiquement quand les positions se résolvent."""
    if st.paper_mode or not poly.ready: return
    clob_bal = await fetch_clob_balance()
    if clob_bal is not None and clob_bal > 0:
        if abs(clob_bal - st.bankroll) >= 0.01:
            log.info(f"BR sync: {st.bankroll:.2f}$ → {clob_bal:.2f}$ (solde Polymarket réel)")
        st.bankroll = clob_bal
        if st.bankroll_ref <= 0: st.bankroll_ref = clob_bal

async def job_take_profit(context):
    """❌ DÉSACTIVÉ (demande user 20/06): plus AUCUNE vente anticipée (ni TP x2/x3/x4, ni stop, ni
    trailing). On laisse TOUJOURS la position aller jusqu'à la résolution du slot (job_check_expiry).
    No-op conservé pour ne pas toucher au scheduler."""
    return

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
                                # ✅ qty du trade aggTrade Binance — alimente vol_vote (job_oracle_lag),
                                # jamais peuplé avant ce fix (deque déclarée mais jamais écrite).
                                st.ws_volumes.append((now, float(d.get("q", 0))))
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
                                st.eth_ws_volumes.append((now, float(d.get("q", 0))))
                                _resolve_pending_passes()
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
                                st.sol_ws_volumes.append((now, float(d.get("q", 0))))
                                _resolve_pending_passes()
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
                                st.xrp_ws_volumes.append((now, float(d.get("q", 0))))
                                _resolve_pending_passes()
                                while st.xrp_ws_prices and now-st.xrp_ws_prices[0][0]>120:
                                    st.xrp_ws_prices.popleft()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
        except Exception as e: log.warning(f"WS XRP: {e}")
        await asyncio.sleep(5)


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

def compute_macd(prices, fast=12, slow=26, signal=9):
    """✅ v12.9 — MACD (indicateur top-cité par les papiers ML pour la direction crypto, avec le RSI).
    Retourne (macd_line, signal_line, histogram). histogram>0 = momentum haussier, <0 = baissier.
    Le croisement macd/signal (histogram change de signe) est le signal directionnel classique."""
    if len(prices) < slow + signal:
        return 0.0, 0.0, 0.0
    ema_fast = compute_ema(prices, fast)
    ema_slow = compute_ema(prices, slow)
    macd_line = ema_fast - ema_slow
    # signal line = EMA du MACD: on approxime sur la série des MACD récents
    macd_series = []
    for i in range(slow, len(prices)+1):
        sub = prices[:i]
        macd_series.append(compute_ema(sub, fast) - compute_ema(sub, slow))
    signal_line = compute_ema(macd_series, signal) if len(macd_series) >= signal else macd_line
    return macd_line, signal_line, macd_line - signal_line

def compute_ta_score(price_history, asset="BTC"):
    if len(price_history)<8: return 0,None,{}
    prices=[x["price"] for x in sorted(price_history,key=lambda x:x["ts"])]
    now=price_history[-1]["ts"] if price_history else 0
    score=0; details={}
    # ✅ v12.9 — Dual score asymétrique (mode mesure): up_score et down_score séparés.
    # Idée du modèle dual (papier CNN-LSTM): prédire UP et DOWN ne sont PAS symétriques.
    # On accumule séparément les arguments haussiers et baissiers, sans forcer down=-up.
    up_score=0.0; down_score=0.0
    rsi=compute_rsi(prices,7); details["rsi"]=round(rsi,1)
    if rsi<35: score+=2; up_score+=2.0
    elif rsi<45: score+=1; up_score+=1.0
    elif rsi>65: score-=2; down_score+=2.0
    elif rsi>55: score-=1; down_score+=1.0
    if len(prices)>=21:
        ema9=compute_ema(prices[-21:],9); ema21=compute_ema(prices[-21:],21)
        gap_pct=(ema9-ema21)/ema21*100 if ema21 else 0
        details["ema_gap"]=round(gap_pct,4)
        if gap_pct>0.02: score+=1; up_score+=1.0
        elif gap_pct<-0.02: score-=1; down_score+=1.0
    # ❌ (22/06) RETIRÉ du score sur demande user — confirmé ANTI-SIGNAL par /edge (48% précision,
    # pire que pile/face, n=646 sur SLOT EDGE TOUS). On garde le calcul pour affichage/mesure
    # (/oracle, /score) mais il ne contribue plus au score qui alimente le vote oracle_lag.
    macd_line, macd_signal, macd_hist = compute_macd(prices)
    details["macd_hist"]=round(macd_hist,4)
    pts_3m=[x for x in price_history if now-x["ts"]<=180]
    if len(pts_3m)>=2:
        roc=(pts_3m[-1]["price"]-pts_3m[0]["price"])/pts_3m[0]["price"]*100 if pts_3m[0]["price"] else 0
        details["mom3m"]=round(roc,4)
        if roc>0.03: score+=1; up_score+=1.0
        elif roc<-0.03: score-=1; down_score+=1.0
    if len(prices)>=10:
        recent=prices[-10:]; avg=sum(recent)/len(recent)
        std=(sum((p-avg)**2 for p in recent)/len(recent))**0.5
        if avg>0 and std/avg*100>0.1:
            score=int(score*0.7); up_score*=0.7; down_score*=0.7  # chop → réduire conviction
    details["up_score"]=round(up_score,2); details["down_score"]=round(down_score,2)
    # Direction dual (mesure): qui domine nettement? (marge ≥1.0 pour éviter le bruit)
    if up_score - down_score >= 1.0: details["dual_dir"]="UP"
    elif down_score - up_score >= 1.0: details["dual_dir"]="DOWN"
    else: details["dual_dir"]=None
    direction="UP" if score>0 else ("DOWN" if score<0 else None)
    return score,direction,details


def _resolve_pending_passes():
    """v12.9 — Résolution passes: utilise oracle_delta actuel comme proxy."""
    try:
        now = time.time()
        for pr in st.pass_reasons:
            if pr.get("resolved") is not None: continue
            if float(pr.get("slot_end", 0)) > now: continue
            direction = pr.get("dir")
            if direction not in ("UP", "DOWN"):
                pr["resolved"] = "❓"; continue
            # Détecter l'asset
            reason = str(pr.get("reason", ""))
            asset = "BTC"
            for a in ("ETH", "SOL", "XRP"):
                if reason.startswith(f"{a}:"): asset = a; break
            # Utiliser snap si disponible, sinon oracle actuel
            snap = pr.get("snap", {}).get(asset, (0, 0, 0))
            ref = next((float(v) for v in snap if v and float(v) > 0.001), 0)
            cur_map = {
                "BTC": st.ws_price or st.oracle_price,
                "ETH": st.eth_price or st.eth_oracle_price,
                "SOL": st.sol_price or st.sol_oracle_price,
                "XRP": st.xrp_price or st.xrp_oracle_price,
            }
            cur = cur_map.get(asset, 0)
            if ref > 0.001 and cur > 0.001:
                won = (cur > ref) == (direction == "UP")
                pr["resolved"] = "WIN" if won else "LOSS"
            else:
                # Fallback: utiliser le filtre lui-même comme résultat
                # deltaneg → LOSS garanti, tokenmax → résultat selon marché
                if "delta" in reason and "<0" in reason: pr["resolved"] = "LOSS"
                elif "token" in reason and ">0.8" in reason: pr["resolved"] = "LOSS"
                else: pr["resolved"] = "❓"
    except Exception as e:
        log.debug(f"resolve_passes: {e}")


def compute_brier_score(trades):
    """✅ v12.9 — Brier score: mesure la CALIBRATION de nos probabilités estimées vs résultats réels.
    BS = moyenne de (p_estimée - outcome)², où outcome = 1 si WIN, 0 si LOSS.
    Standard utilisé par Metaculus/Good Judgment. Interprétation:
      < 0.20 = bien calibré (edge réel, pas chance)  | ~0.25 = aléatoire (proba = du vent)  | > 0.25 = pire que le hasard
    Un prédicteur qui dit toujours 50% obtient exactement 0.25. Donc battre 0.25 = avoir une vraie info.
    Retourne (brier, n, avg_conf, realized_wr) ou None si pas assez de données."""
    resolved = [t for t in trades if t.get("result") in ("WIN","LOSS") and isinstance(t.get("conf"), (int,float))]
    if len(resolved) < 5:
        return None
    total = 0.0; conf_sum = 0.0; wins = 0
    for t in resolved:
        p = max(0.0, min(1.0, float(t["conf"])))
        outcome = 1.0 if t["result"] == "WIN" else 0.0
        total += (p - outcome) ** 2
        conf_sum += p
        wins += int(t["result"] == "WIN")
    n = len(resolved)
    return {"brier": round(total/n, 4), "n": n,
            "avg_conf": round(conf_sum/n, 3), "realized_wr": round(wins/n, 3)}


def _record_slot(asset, slot_start, open_px, close_px, prices_deque):
    """✅ v12.9 — Enregistre UN slot résolu avec ses conditions + résultat réel.
    Résultat selon la règle officielle Polymarket: UP si close ≥ open (source Chainlink), sinon DOWN.
    Capture les features au moment de l'enregistrement (proxy de fin de slot) pour analyse a posteriori."""
    try:
        result = "UP" if close_px >= open_px else "DOWN"
        delta_pct = (close_px - open_px) / open_px * 100 if open_px > 0 else 0.0
        # Features TA sur la fenêtre de prix disponible
        rsi = macd_hist = 0.0; dual = None; regime = "?"
        pts = list(prices_deque) if prices_deque else []
        if len(pts) >= 35:
            ph = [{"price": p, "ts": t} for t, p in pts]
            _s, _d, det = compute_ta_score(ph, asset)
            rsi = det.get("rsi", 0); macd_hist = det.get("macd_hist", 0); dual = det.get("dual_dir")
            # régime via bandwidth Bollinger sur 60s
            now = time.time()
            wp = [p for t, p in pts if now - t <= 60]
            if len(wp) >= 10:
                sma = sum(wp) / len(wp)
                if sma > 0:
                    std = (sum((p - sma) ** 2 for p in wp) / len(wp)) ** 0.5
                    bw = (4 * std) / sma * 100
                    regime = "squeeze" if bw <= 0.12 else "expansion"
        sess = session_ctx().get("session", "?")
        # ✅ v12.9 — Order Book Imbalance (piste prédiction légitime: déséquilibre achat/vente Polymarket)
        ob_map = {"BTC": getattr(st,"ob_imbalance",0), "ETH": getattr(st,"eth_ob_imbalance",0),
                  "SOL": getattr(st,"sol_ob_imbalance",0), "XRP": getattr(st,"xrp_ob_imbalance",0)}
        ob_imb = ob_map.get(asset, 0)
        # ✅ v12.9 — spread + profondeur $ (nouveaux outils marché)
        spr_map = {"BTC": getattr(st,"ob_spread",0), "ETH": getattr(st,"eth_ob_spread",0),
                   "SOL": getattr(st,"sol_ob_spread",0), "XRP": getattr(st,"xrp_ob_spread",0)}
        dep_map = {"BTC": getattr(st,"ob_depth",0), "ETH": getattr(st,"eth_ob_depth",0),
                   "SOL": getattr(st,"sol_ob_depth",0), "XRP": getattr(st,"xrp_ob_depth",0)}
        # ✅ v12.9 — microprice signal + OFI (mode mesure)
        micro_map = {"BTC": getattr(st,"ob_micro_signal",0), "ETH": getattr(st,"eth_ob_micro_signal",0),
                     "SOL": getattr(st,"sol_ob_micro_signal",0), "XRP": getattr(st,"xrp_ob_micro_signal",0)}
        ofi_map = {"BTC": getattr(st,"ob_ofi",0), "ETH": getattr(st,"eth_ob_ofi",0),
                   "SOL": getattr(st,"sol_ob_ofi",0), "XRP": getattr(st,"xrp_ob_ofi",0)}
        st.slot_records.append({
            "asset": asset, "slot": slot_start, "open": open_px, "close": close_px,
            "result": result, "delta": round(delta_pct, 4), "rsi": round(rsi, 1),
            "macd": round(macd_hist, 5), "dual": dual, "regime": regime, "ob": round(ob_imb, 3),
            "spread": round(spr_map.get(asset,0),4), "depth": round(dep_map.get(asset,0),2),
            "micro": round(micro_map.get(asset,0),4), "ofi": round(ofi_map.get(asset,0),2),
            "session": sess, "ts": int(time.time())})
        if len(st.slot_records) > 5000:
            st.slot_records = st.slot_records[-5000:]
    except Exception as e:
        log.debug(f"record_slot {asset}: {e}")


async def job_slot_recorder(context):
    """✅ v12.9 (fix2 18/06) — FILET DE SÉCURITÉ du slot recorder, indépendant de la bascule oracle WS.
    L'enregistrement principal se fait à la bascule dans ws_oracle_loop. Ce job est un backup:
    il capture l'open de chaque nouveau slot (prix oracle au 1er passage du slot) et enregistre
    le slot précédent s'il n'a pas déjà été enregistré par le mécanisme principal.
    Garantit qu'on ne perd aucun slot même si un tick oracle est raté."""
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    assets = [
        ("BTC", "oracle_price", st.ws_prices),
        ("ETH", "eth_oracle_price", st.eth_ws_prices),
        ("SOL", "sol_oracle_price", st.sol_ws_prices),
        ("XRP", "xrp_oracle_price", st.xrp_ws_prices),
    ]
    # st.slot_rec_open: {asset: (slot_start, open_price)} — capturé par ce job
    if not hasattr(st, "slot_rec_open"): st.slot_rec_open = {}
    for asset, price_attr, pdq in assets:
        oracle_px = getattr(st, price_attr, 0)
        if oracle_px <= 0: continue
        prev = st.slot_rec_open.get(asset)
        if prev is None:
            # Premier passage: mémoriser l'open du slot courant
            st.slot_rec_open[asset] = (cur_slot, oracle_px)
        elif prev[0] < cur_slot:
            # Le slot précédent (prev[0]) est terminé. L'enregistrer si pas déjà fait par le mécanisme principal.
            if st.slot_rec_last.get(asset) != prev[0]:
                _record_slot(asset, prev[0], prev[1], oracle_px, pdq)
                st.slot_rec_last[asset] = prev[0]
                log.info(f"📝 SLOT REC (backup) {asset}: total={len(st.slot_records)}")
            # Démarrer le suivi du slot courant
            st.slot_rec_open[asset] = (cur_slot, oracle_px)


async def job_price_timing(context):
    """✅ v12.9 — TRACKER TIMING DE PRICING: mesure à quel moment (T-Xs) le token de chaque crypto
    dépasse 0.95$, et le token max atteint. Répond à la question 'est-ce qu'on entre trop tard?'.
    Tourne toutes les 10s. Lecture seule (best-effort). Enregistre le 1er franchissement de 0.95$ par slot."""
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    t_remaining = cur_slot + 300 - now
    for asset, e in [("BTC","₿"),("ETH","Ξ"),("SOL","◎"),("XRP","✕")]:
        try:
            cfg = _asset_state_attrs(asset)
            market = await poly.get_market_by_slug(f"{cfg['slug']}-{cur_slot}")
            if not market: continue
            token = await poly.get_token_price(market["token_up"])
            if token <= 0: continue
            key = (asset, cur_slot)
            # token max du slot
            prev_max = st.price_timing_max.get(key, 0)
            if token > prev_max: st.price_timing_max[key] = token
            # 1er franchissement de 0.95$ (token "déjà pricé")
            if token >= 0.95 and key not in st.price_timing_seen:
                st.price_timing_seen[key] = t_remaining
                st.price_timing.append({"asset": asset, "slot": cur_slot,
                    "t_remaining_at_095": round(t_remaining,1),
                    "token_at_cross": round(token,3), "ts": int(now)})
                if len(st.price_timing) > 2000: st.price_timing = st.price_timing[-2000:]
            # Nettoyage des dicts (garder ~1h)
            if len(st.price_timing_seen) > 400:
                cutoff = cur_slot - 3600
                st.price_timing_seen = {k:v for k,v in st.price_timing_seen.items() if k[1] > cutoff}
                st.price_timing_max = {k:v for k,v in st.price_timing_max.items() if k[1] > cutoff}
        except Exception as ex:
            log.debug(f"price_timing {asset}: {ex}")



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
                            # ✅ (23/06) FIX: attribue le tick à son slot via le timestamp CHAINLINK (event time),
                            # pas via `now` (processing time). Avant: un tick représentant la VRAIE clôture du
                            # slot N pouvait arriver juste après que l'horloge du bot ait basculé sur le slot N+1
                            # (délai réseau/traitement) → mal classé comme "open" du nouveau slot, tandis qu'un
                            # tick plus ancien/périmé servait de "close" pour le slot N → résultat parfois inversé
                            # dans les cas serrés (vu: BTC marqué WIN par le bot alors que Polymarket dit LOSS).
                            slot_start = int(cl_ts//300)*300
                            st.oracle_chainlink_ts = cl_ts
                            if symbol == "btc/usd":
                                if st.oracle_slot_ts!=slot_start:
                                    # ✅ v12.9 SLOT RECORDER (fix2 18/06): close = ANCIEN prix oracle (avant écrasement par le nouveau)
                                    prev_close = st.oracle_price  # prix du slot qui se termine
                                    if st.oracle_slot_ts>0 and st.oracle_slot_open>0 and prev_close>0:
                                        _record_slot("BTC", st.oracle_slot_ts, st.oracle_slot_open, prev_close, st.ws_prices)
                                        log.info(f"📝 SLOT REC BTC: open={st.oracle_slot_open:.2f} close={prev_close:.2f} → total={len(st.slot_records)}")
                                    st.oracle_slot_ts=slot_start; st.oracle_slot_open=p
                                    st.oracle_open_lag["BTC"]=round(now-slot_start,1)  # #4 retard de capture de l'open
                                    log.info(f"📌 BTC slot open: ${p:,.2f}")
                                st.oracle_price=p; st.oracle_ts=now
                            elif symbol == "eth/usd" and p>100:
                                if st.eth_oracle_slot_ts!=slot_start:
                                    prev_close = st.eth_oracle_price
                                    if st.eth_oracle_slot_ts>0 and st.eth_oracle_slot_open>0 and prev_close>0:
                                        _record_slot("ETH", st.eth_oracle_slot_ts, st.eth_oracle_slot_open, prev_close, st.eth_ws_prices)
                                    st.eth_oracle_slot_ts=slot_start; st.eth_oracle_slot_open=p
                                    st.oracle_open_lag["ETH"]=round(now-slot_start,1)  # #4
                                st.eth_oracle_price=p; st.eth_oracle_ts=now
                            elif symbol == "sol/usd" and p>1:
                                if st.sol_oracle_slot_ts!=slot_start:
                                    prev_close = st.sol_oracle_price
                                    if st.sol_oracle_slot_ts>0 and st.sol_oracle_slot_open>0 and prev_close>0:
                                        _record_slot("SOL", st.sol_oracle_slot_ts, st.sol_oracle_slot_open, prev_close, st.sol_ws_prices)
                                    st.sol_oracle_slot_ts=slot_start; st.sol_oracle_slot_open=p
                                    st.oracle_open_lag["SOL"]=round(now-slot_start,1)  # #4
                                st.sol_oracle_price=p; st.sol_oracle_ts=now
                            elif symbol == "xrp/usd" and p>0.01:
                                if st.xrp_oracle_slot_ts!=slot_start:
                                    prev_close = st.xrp_oracle_price
                                    if st.xrp_oracle_slot_ts>0 and st.xrp_oracle_slot_open>0 and prev_close>0:
                                        _record_slot("XRP", st.xrp_oracle_slot_ts, st.xrp_oracle_slot_open, prev_close, st.xrp_ws_prices)
                                    st.xrp_oracle_slot_ts=slot_start; st.xrp_oracle_slot_open=p
                                    st.oracle_open_lag["XRP"]=round(now-slot_start,1)  # #4
                                st.xrp_oracle_price=p; st.xrp_oracle_ts=now
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except Exception as e:
            log.warning(f"WS Oracle: {e}")
        st.oracle_connected=False
        await asyncio.sleep(5)

async def ws_binance_liq_loop():
    """✅ (22/06) demande user: flux liquidations Binance Futures (!forceOrder@arr, public, sans clé).
    Mécanique (pas spéculatif): une liquidation SELL = position LONG liquidée → vente forcée → pression
    baissière immédiate. Une liquidation BUY = position SHORT liquidée → achat forcé → pression haussière.
    On agrège le notionnel net (USD) par asset sur une fenêtre glissante via liq_pressure()."""
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    symbol_map = {"BTCUSDT":"BTC","ETHUSDT":"ETH","SOLUSDT":"SOL","XRPUSDT":"XRP"}
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=20) as ws:
                    log.info("✅ WS Binance liquidations connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                d = json.loads(msg.data)
                                o = d.get("o", {}) if isinstance(d, dict) else {}
                                asset = symbol_map.get(o.get("s",""))
                                if not asset: continue
                                side = o.get("S","")
                                qty = float(o.get("q",0) or 0); price = float(o.get("ap", o.get("p",0)) or 0)
                                notional = qty * price
                                if notional <= 0: continue
                                st.liq_hist.setdefault(asset, deque(maxlen=200)).append((time.time(), side, notional))
                            except Exception as pe:
                                log.debug(f"liq parse: {pe}")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except Exception as e:
            log.warning(f"WS Binance liquidations déconnecté: {e}")
        await asyncio.sleep(5)

def liq_pressure(asset, window_s=LIQ_PRESSURE_WINDOW_S):
    """✅ (22/06) Pression NETTE de liquidations récentes (USD) sur `window_s`.
    >0 = shorts liquidés net (achat forcé, pression haussière) | <0 = longs liquidés net (pression baissière)."""
    hist = st.liq_hist.get(asset)
    if not hist: return 0.0
    now = time.time()
    net = 0.0
    for ts, side, notional in hist:
        if now - ts > window_s: continue
        net += notional if side == "BUY" else -notional
    return net

async def job_funding_rate(context):
    """✅ (22/06) demande user: funding rate Binance Futures par asset (poll ~60s, API publique).
    NOTE: affiché/loggé pour l'instant — PAS encore branché dans p_oracle. Le sens prédictif d'un
    funding extrême (continuation vs squeeze/retour à la moyenne) doit être validé empiriquement
    (cf. règle anti-biais n≥30) avant d'en faire une règle de trading, plutôt que deviner un sens."""
    symbol_map = {"BTC":"BTCUSDT","ETH":"ETHUSDT","SOL":"SOLUSDT","XRP":"XRPUSDT"}
    for asset, sym in symbol_map.items():
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": sym},
                                  timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        data = await r.json()
                        st.funding_rate[asset] = float(data.get("lastFundingRate", 0) or 0)
                        st.funding_ts[asset] = time.time()
        except Exception as e:
            log.debug(f"funding {asset}: {e}")

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
    # ✅ (22/06) demande user: flux liquidations Binance Futures
    if not hasattr(st,"liq_ws_task") or not st.liq_ws_task or st.liq_ws_task.done():
        st.liq_ws_task = asyncio.create_task(ws_binance_liq_loop())

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

def oracle_direction(asset):
    """✅ Direction 'oracle lag' d'un asset (gap spot↔oracle + delta oracle↔open du slot), ou None
    si neutre. Sert à exiger que l'OB signal soit DANS LE SENS de l'oracle (confirmation croisée).
    Même logique que job_oracle_lag mais lecture seule — ne déclenche aucun trade oracle."""
    a = (asset or "BTC").upper()
    if a == "BTC":
        spot = consensus_price() or st.ws_price; oracle = st.oracle_price; slot_open = st.oracle_slot_open
    else:
        pfx = a.lower()
        spot = getattr(st, f"{pfx}_price", 0); oracle = getattr(st, f"{pfx}_oracle_price", 0)
        slot_open = getattr(st, f"{pfx}_oracle_slot_open", 0)
    if spot <= 0 or oracle <= 0 or slot_open <= 0:
        return None
    gap = (spot - oracle) / oracle * 100
    delta = (oracle - slot_open) / slot_open * 100
    gap_dir = "UP" if gap >= 0.025 else ("DOWN" if gap <= -0.025 else None)
    delta_dir = "UP" if delta >= ORACLE_ENTRY_DELTA else ("DOWN" if delta <= -ORACLE_ENTRY_DELTA else None)
    return gap_dir or delta_dir

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

def calibration_report(min_per_bucket=5):
    """✅ #4 — Calibration proba prédite vs WR réalisé, par bucket de confiance ET par stratégie.
    Permet de voir où le modèle est sur/sous-confiant et propose un facteur correctif simple
    (WR_réel / proba_prédite, type Platt) applicable par bucket. N'altère AUCUNE décision —
    c'est un rapport de mesure (à utiliser pour recalibrer les heuristiques p_oracle/p_mom/...)."""
    resolved = [t for t in st.trades
                if t.get("conf",0) > 0 and t.get("result") in ("WIN","LOSS") and not t.get("paper")]
    if not resolved:
        return None, "Aucun trade réel résolu."
    buckets = [(0.50,0.60),(0.60,0.70),(0.70,0.80),(0.80,0.90),(0.90,1.01)]
    lines = []
    for lo, hi in buckets:
        grp = [t for t in resolved if lo <= t["conf"] < hi]
        if not grp: continue
        n = len(grp)
        pred = sum(t["conf"] for t in grp) / n
        wr = sum(1 for t in grp if t["result"]=="WIN") / n
        flag = "✅" if abs(pred-wr) <= 0.05 else ("🔴 surconfiant" if pred > wr else "🟢 sous-conf")
        corr = f" | ×{wr/pred:.2f}" if pred > 0 and n >= min_per_bucket else ""
        lines.append(f"`{lo:.2f}-{hi:.2f}` n={n} pred=`{pred*100:.0f}%` réel=`{wr*100:.0f}%` {flag}{corr}")
    # Breakdown par stratégie (source)
    by_src = {}
    for t in resolved:
        s = t.get("source","?")
        by_src.setdefault(s, []).append(t)
    src_lines = []
    for s, grp in sorted(by_src.items(), key=lambda kv: -len(kv[1])):
        n = len(grp)
        pred = sum(t["conf"] for t in grp) / n
        wr = sum(1 for t in grp if t["result"]=="WIN") / n
        src_lines.append(f"`{s}` n={n} pred=`{pred*100:.0f}%` réel=`{wr*100:.0f}%`")
    txt = "*Par bucket de proba:*\n" + ("\n".join(lines) or "_(pas assez de données)_")
    txt += "\n\n*Par stratégie:*\n" + ("\n".join(src_lines) or "_(n/a)_")
    return resolved, txt

def _wilson_lower(wins, n, z=1.96):
    """Borne basse de l'IC 95% (Wilson) sur un taux de réussite — robuste sur petits échantillons."""
    if n == 0: return 0.0
    p = wins / n
    denom = 1 + z*z/n
    centre = p + z*z/(2*n)
    margin = z * ((p*(1-p) + z*z/(4*n)) / n) ** 0.5
    return max(0.0, (centre - margin) / denom)

def edge_scorecard(include_paper_if_few=True):
    """✅ Scorecard d'edge: pour CHAQUE stratégie (source) et l'ensemble, mesure la rentabilité
    RÉELLE depuis le journal de trades — pas les heuristiques, les résultats.
    - PnL total + PnL moyen/trade
    - t-stat sur le PnL/trade (mean / (std/√n)) → significativité statistique
    - WR + borne basse Wilson 95%
    Verdict: ✅ rentable significatif | 🟡 positif non significatif | 🔴 perdant | ⚠️ n insuffisant.
    C'est l'outil de décision: ne garder QUE les stratégies prouvées +EV."""
    real = [t for t in st.trades if t.get("result") in ("WIN","LOSS") and not t.get("paper")]
    # ✅ (22/06) demande user: exclut les trades d'AVANT le fix du bug de résolution (faux LOSS sur de
    # vrais WIN) — ces anciens trades ont pu être mal résolus, les inclure fausserait l'edge réel.
    excluded_old = [t for t in real if t.get("ts",0) < RESOLUTION_FIX_TS]
    real = [t for t in real if t.get("ts",0) >= RESOLUTION_FIX_TS]
    mode = "RÉEL"
    if len(real) < 10 and include_paper_if_few:
        real = [t for t in st.trades if t.get("result") in ("WIN","LOSS") and t.get("ts",0) >= RESOLUTION_FIX_TS]
        mode = "RÉEL+PAPER (peu de réel)"
    if not real:
        n_old = len(excluded_old)
        return (f"Aucun trade résolu APRÈS le fix de résolution (22/06) à analyser encore.\n"
                f"({n_old} trades plus anciens exclus — potentiellement mal résolus par le bug corrigé, "
                f"voir /edge à nouveau après quelques trades.)" if n_old else "Aucun trade résolu à analyser.")

    def stats(grp):
        n = len(grp)
        wins = sum(1 for t in grp if t["result"]=="WIN")
        pnls = [float(t.get("pnl",0) or 0) for t in grp]
        staked = sum(float(t.get("amount",0) or 0) for t in grp) or 1e-9
        total = sum(pnls)
        mean = total / n
        var = sum((x-mean)**2 for x in pnls) / (n-1) if n > 1 else 0.0
        std = var ** 0.5
        tstat = (mean / (std / (n**0.5))) if std > 0 else 0.0
        wr = wins / n
        wlo = _wilson_lower(wins, n)
        roi = total / staked * 100
        # ✅ (22/06) demande user: edge RÉALISTE — EV théorique affiché à l'entrée (conf-entry_token-fee)
        # vs ROI réellement réalisé. L'écart mesure le coût caché (slippage, calibration, exécution).
        ev_theos = [float(t.get("conf",0) or 0) - float(t.get("entry_token",0) or 0) - float(t.get("fee_est",0) or 0)
                    for t in grp if t.get("entry_token")]
        ev_theo_avg = (sum(ev_theos)/len(ev_theos)*100) if ev_theos else 0.0
        if n < 20:           verdict = "⚠️ n<20"
        elif total <= 0:     verdict = "🔴 perdant"
        elif tstat >= 2.0:   verdict = "✅ rentable (signif.)"
        else:                verdict = "🟡 positif (non signif.)"
        return n, wr, wlo, total, mean, roi, tstat, verdict, ev_theo_avg

    by_src = {}
    for t in real:
        by_src.setdefault(t.get("source","?"), []).append(t)

    lines = [f"📊 *EDGE SCORECARD* ({mode})", "━━━━━━━━━━━━━━"]
    if excluded_old:
        lines.append(f"_ℹ️ {len(excluded_old)} trade(s) d'avant le fix résolution (22/06) exclus — potentiellement mal résolus_")
    n,wr,wlo,total,mean,roi,tstat,verdict,ev_theo = stats(real)
    lines.append(f"*GLOBAL* n={n} | PnL `{total:+.2f}$` | ROI `{roi:+.1f}%`")
    lines.append(f"  WR `{wr*100:.0f}%` (min `{wlo*100:.0f}%`) | t=`{tstat:.1f}` | {verdict}")
    lines.append(f"  EV théorique entrée: `{ev_theo:+.1f}%` vs ROI réalisé: `{roi:+.1f}%` → écart `{roi-ev_theo:+.1f}pt` _(slippage/calib)_")
    lines.append("")
    for src, grp in sorted(by_src.items(), key=lambda kv: -sum(float(t.get('pnl',0) or 0) for t in kv[1])):
        n,wr,wlo,total,mean,roi,tstat,verdict,ev_theo = stats(grp)
        lines.append(f"*{src}* n={n} | PnL `{total:+.2f}$` `{mean:+.2f}/t` | ROI `{roi:+.1f}%`")
        lines.append(f"  WR `{wr*100:.0f}%` (min `{wlo*100:.0f}%`) | t=`{tstat:.1f}` | {verdict}")
        lines.append(f"  EV théorique: `{ev_theo:+.1f}%` vs réalisé: `{roi:+.1f}%` → écart `{roi-ev_theo:+.1f}pt`")
    lines.append("")
    lines.append("_t≥2 = edge réel (95%). Coupe les 🔴. Scale les ✅. Attends n≥20-30 pour les ⚠️._")
    return "\n".join(lines)

def slot_edge_analysis(asset=None, min_n=30):
    """✅ #2 — Mine le journal slot_records (features → résultat UP/DOWN RÉEL, jusqu'à 5000 slots).
    Pour chaque signal, mesure sa PRÉCISION DIRECTIONNELLE: parmi les slots où le signal pointe
    UP ou DOWN, combien de fois le résultat réel a suivi. Compare à 50% (pile/face) avec la borne
    basse Wilson 95%. Backtest gratuit, SANS risquer de capital → dit quels signaux gardent un edge.
    ✅ = edge prouvé (Wilson>52%), 🔴 = anti-signal (acc<48%, exploitable inversé), ⚪ = bruit."""
    recs = [r for r in st.slot_records if r.get("result") in ("UP","DOWN")]
    if asset: recs = [r for r in recs if r.get("asset") == asset]
    if not recs:
        return "Aucun slot résolu enregistré (reviens dans 10-15 min)."
    n_all = len(recs)
    up_rate = sum(1 for r in recs if r["result"] == "UP") / n_all

    def sig_ob(r):    v=r.get("ob",0);    return "UP" if v>0.15 else ("DOWN" if v<-0.15 else None)
    def sig_ofi(r):   v=r.get("ofi",0);   return "UP" if v>0 else ("DOWN" if v<0 else None)
    def sig_micro(r): v=r.get("micro",0); return "UP" if v>0.002 else ("DOWN" if v<-0.002 else None)
    def sig_delta(r): v=r.get("delta",0); return "UP" if v>0 else ("DOWN" if v<0 else None)
    def sig_macd(r):  v=r.get("macd",0);  return "UP" if v>0 else ("DOWN" if v<0 else None)
    def sig_rsi(r):   v=r.get("rsi",50);  return "UP" if v>55 else ("DOWN" if v<45 else None)
    def sig_dual(r):  d=r.get("dual");    return d if d in ("UP","DOWN") else None
    def sig_ob_ofi(r):
        a,b = sig_ob(r), sig_ofi(r)
        return a if (a and a==b) else None  # OB et OFI d'accord uniquement
    signals = [("OB |>0.15|",sig_ob),("OFI signe",sig_ofi),("Microprice",sig_micro),
               ("Δ oracle",sig_delta),("MACD signe",sig_macd),("RSI 55/45",sig_rsi),
               ("Dual model",sig_dual),("OB+OFI accord",sig_ob_ofi)]

    lines = [f"🔬 *SLOT EDGE* {asset or 'TOUS'} — n={n_all}, base UP `{up_rate*100:.0f}%`",
             "━━━━━━━━━━━━━━", "_précision directionnelle vs 50% (pile/face)_"]
    scored = []
    for name, fn in signals:
        fired = [(fn(r), r["result"]) for r in recs]
        fired = [(p, res) for p, res in fired if p is not None]
        nn = len(fired)
        if nn < min_n:
            scored.append((0.5, f"⚪ {name}: n={nn} (insuffisant)")); continue
        hits = sum(1 for p, res in fired if p == res)
        acc = hits / nn
        wlo = _wilson_lower(hits, nn)
        flag = "✅" if wlo > 0.52 else ("🔴" if acc < 0.48 else "⚪")
        scored.append((acc, f"{flag} {name}: `{acc*100:.0f}%` (min `{wlo*100:.0f}%`) n={nn} `{(acc-0.5)*100:+.0f}pt`"))
    for _, txt in sorted(scored, key=lambda x: -x[0]):
        lines.append(txt)
    lines.append("\n_✅ edge prouvé · 🔴 anti-signal (inverse-le) · ⚪ bruit. Construis tes filtres sur les ✅._")
    return "\n".join(lines)

def exec_report():
    """✅ #1 — Qualité d'exécution: répartition maker/taker/non-rempli (compteurs cumulés) +
    fuite de frais (frais estimés payés vs PnL brut). Dit si le fill-aware capte le rebate maker
    ou paie le taker, et quelle part de l'edge part en frais."""
    es = getattr(st, "exec_stats", {}) or {}
    mk, tk, nf = es.get("maker",0), es.get("taker",0), es.get("nofill",0)
    tot_orders = mk + tk + nf
    real = [t for t in st.trades if t.get("result") in ("WIN","LOSS") and not t.get("paper")
            and t.get("ts",0) >= RESOLUTION_FIX_TS]  # ✅ (22/06) exclut les trades pré-fix résolution
    for t in real:
        by_fill.setdefault(t.get("fill_type","?"), []).append(t)
    lines = ["⚙️ *EXÉCUTION & FRAIS*", "━━━━━━━━━━━━━━"]
    if tot_orders:
        lines.append(f"Ordres: `{tot_orders}` | maker `{mk}` ({mk/tot_orders*100:.0f}%) · "
                     f"taker `{tk}` ({tk/tot_orders*100:.0f}%) · non-rempli `{nf}` ({nf/tot_orders*100:.0f}%)")
        lines.append(f"_maker = gratuit+rebate · taker = frais pleins · non-rempli = fantôme évité_")
    else:
        lines.append("_Aucune exécution réelle vérifiée encore (mode réel + client v2 requis)._")
    lines.append("")
    if real:
        net = gross - fees
        leak = (fees / abs(gross) * 100) if gross else 0
        lines.append(f"PnL brut `{gross:+.2f}$` − frais est. `{fees:.2f}$` = net `{net:+.2f}$`")
        lines.append(f"Fuite de frais: `{leak:.0f}%` du PnL brut")
        lines.append("\n*WR par type de fill:*")
        for ft, grp in sorted(by_fill.items(), key=lambda kv:-len(kv[1])):
            n=len(grp); wr=sum(1 for t in grp if t["result"]=="WIN")/n*100
            pnl=sum(float(t.get('pnl',0) or 0) for t in grp)
            lines.append(f"  `{ft}` n={n} WR `{wr:.0f}%` PnL `{pnl:+.2f}$`")
    else:
        lines.append("_Pas de trade réel résolu pour mesurer la fuite de frais._")
    return "\n".join(lines)

def _bucket_stats(trades, key, edges, fmt_lbl):
    """Helper: WR/PnL/EV par bucket d'une valeur numérique du trade."""
    out=[]
    for lo, hi in edges:
        grp=[t for t in trades if lo <= float(t.get(key,0) or 0) < hi]
        if not grp: continue
        n=len(grp); wr=sum(1 for t in grp if t["result"]=="WIN")/n*100
        pnl=sum(float(t.get('pnl',0) or 0) for t in grp)
        staked=sum(float(t.get('amount',0) or 0) for t in grp) or 1e-9
        roi=pnl/staked*100
        flag="✅" if pnl>0 and n>=10 else ("🔴" if pnl<0 and n>=10 else "⚪")
        out.append(f"{flag} `{fmt_lbl(lo,hi)}` n={n} WR `{wr:.0f}%` PnL `{pnl:+.2f}$` ROI `{roi:+.0f}%`")
    return out

def zones_report():
    """✅ #3 — Zones rentables: WR/PnL/ROI par PRIX D'ENTRÉE du token et par TIMING d'entrée
    (T-Xs restant). Révèle où tu gagnes vraiment → resserre la fenêtre/le prix sur les zones +EV."""
    real = [t for t in st.trades if t.get("result") in ("WIN","LOSS") and not t.get("paper")
            and t.get("ts",0) >= RESOLUTION_FIX_TS]  # ✅ (22/06) exclut les trades pré-fix résolution
    if len(real) < 5:
        real = [t for t in st.trades if t.get("result") in ("WIN","LOSS") and t.get("ts",0) >= RESOLUTION_FIX_TS]
        tag = " (réel+paper)"
    else:
        tag = ""
    if not real:
        return "Aucun trade résolu à analyser."
    price_edges=[(0.40,0.50),(0.50,0.55),(0.55,0.60),(0.60,0.65),(0.65,0.70),(0.70,0.80),(0.80,0.96)]
    time_edges=[(0,15),(15,30),(30,45),(45,60),(60,90),(90,150),(150,300)]
    lines=[f"🎯 *ZONES RENTABLES*{tag}", "━━━━━━━━━━━━━━", "*Par prix d'entrée du token:*"]
    pl=_bucket_stats(real,"entry_token",price_edges,lambda lo,hi:f"{lo:.2f}-{hi:.2f}$")
    lines += pl or ["_pas de données prix_"]
    lines.append("\n*Par timing d'entrée (T-Xs restant):*")
    tl=_bucket_stats(real,"t_remaining",time_edges,lambda lo,hi:f"T-{int(lo)}→{int(hi)}s")
    lines += tl or ["_pas de données timing_"]
    lines.append("\n_✅ zone gagnante · 🔴 zone perdante (à éviter). Concentre-toi sur les ✅._")
    return "\n".join(lines)

def risk_report():
    """✅ #4 — Risque: courbe d'équité, max drawdown, drawdown actuel, plus longue série de
    pertes, profit factor, espérance/trade. Pour ne pas se faire effacer sur une mauvaise série."""
    real = [t for t in st.trades if t.get("result") in ("WIN","LOSS") and not t.get("paper")
            and t.get("ts",0) >= RESOLUTION_FIX_TS]  # ✅ (22/06) exclut les trades pré-fix résolution
    if not real:
        return "Aucun trade réel résolu APRÈS le fix de résolution (22/06) pour les métriques de risque."
    pnls=[float(t.get("pnl",0) or 0) for t in real]
    n=len(pnls); wins=[p for p in pnls if p>0]; losses=[p for p in pnls if p<0]
    gross_win=sum(wins); gross_loss=abs(sum(losses))
    pf = gross_win/gross_loss if gross_loss>0 else float('inf')
    expectancy = sum(pnls)/n
    # equity curve + max drawdown
    eq=0.0; peak=0.0; max_dd=0.0; cur_dd=0.0
    for p in pnls:
        eq+=p; peak=max(peak,eq); dd=peak-eq; max_dd=max(max_dd,dd)
    cur_dd = peak-eq
    # plus longue série de pertes
    streak=0; max_streak=0
    for p in pnls:
        if p<0: streak+=1; max_streak=max(max_streak,streak)
        else: streak=0
    pf_txt = "∞" if pf==float('inf') else f"{pf:.2f}"
    verdict = "✅ sain" if pf>1.3 and expectancy>0 else ("🟡 fragile" if expectancy>0 else "🔴 perdant")
    return ("🛡 *RISQUE & DRAWDOWN* (réel)\n━━━━━━━━━━━━━━\n"
            f"Trades `{n}` | Équité cumulée `{eq:+.2f}$`\n"
            f"Profit factor `{pf_txt}` | Espérance `{expectancy:+.2f}$/t` | {verdict}\n"
            f"Max drawdown `{max_dd:.2f}$` | DD actuel `{cur_dd:.2f}$`\n"
            f"Plus longue série de pertes `{max_streak}`\n"
            f"Gains bruts `{gross_win:.2f}$` | Pertes brutes `{gross_loss:.2f}$`\n\n"
            "_PF>1.3 + espérance>0 = durable. DD actuel élevé = prudence._")

def strategy_matrix():
    """✅ #5 — Matrice ASSET × STRATÉGIE: PnL et WR par croisement → repère les cases qui
    gagnent (ex: oracle_lag sur BTC) et celles qui perdent (à désactiver)."""
    # ✅ (22/06) demande user: même filtre que /edge — exclut les trades d'avant le fix du bug de
    # résolution (faux LOSS sur de vrais WIN), sinon cette matrice serait aussi contaminée.
    real = [t for t in st.trades if t.get("result") in ("WIN","LOSS") and not t.get("paper")
            and t.get("ts",0) >= RESOLUTION_FIX_TS]
    if not real:
        return "Aucun trade réel résolu APRÈS le fix de résolution (22/06) pour la matrice. Attends quelques trades neufs."
    assets=["BTC","ETH","SOL","XRP"]
    srcs=sorted({t.get("source","?") for t in real})
    lines=["🧮 *MATRICE ASSET × STRATÉGIE* (PnL réel)","━━━━━━━━━━━━━━","_case = PnL$ (n)_"]
    header="`strat\\ast` " + " ".join(f"`{a[:3]}`" for a in assets)
    lines.append(header)
    for s in srcs:
        cells=[]
        for a in assets:
            grp=[t for t in real if t.get("source")==s and t.get("asset")==a]
            if grp:
                pnl=sum(float(t.get('pnl',0) or 0) for t in grp)
                mark="🟢" if pnl>0 else ("🔴" if pnl<0 else "⚪")
                cells.append(f"{mark}{pnl:+.0f}({len(grp)})")
            else:
                cells.append("·")
        lines.append(f"`{s[:9]:<9}` " + " ".join(cells))
    lines.append("\n_🟢 garde/scale · 🔴 désactive cette case. (n petit = attends plus de data)_")
    return "\n".join(lines)

def slot_combo_analysis(min_n=25):
    """✅ #6 — Mineur de COMBOS de signaux sur slot_records: teste les PAIRES de signaux
    qui s'accordent (ex: OB+OFI, Δoracle+micro) pour trouver les interactions à fort edge —
    souvent meilleures que chaque signal seul. Backtest gratuit, sans risque."""
    recs=[r for r in st.slot_records if r.get("result") in ("UP","DOWN")]
    if not recs:
        return "Aucun slot enregistré (reviens dans 10-15 min)."
    def d_ob(r):    v=r.get("ob",0);    return "UP" if v>0.15 else ("DOWN" if v<-0.15 else None)
    def d_ofi(r):   v=r.get("ofi",0);   return "UP" if v>0 else ("DOWN" if v<0 else None)
    def d_micro(r): v=r.get("micro",0); return "UP" if v>0.002 else ("DOWN" if v<-0.002 else None)
    def d_delta(r): v=r.get("delta",0); return "UP" if v>0 else ("DOWN" if v<0 else None)
    base={"OB":d_ob,"OFI":d_ofi,"micro":d_micro,"Δoracle":d_delta}
    names=list(base.keys())
    scored=[]
    for i in range(len(names)):
        for j in range(i+1,len(names)):
            na,nb=names[i],names[j]; fa,fb=base[na],base[nb]
            fired=[]
            for r in recs:
                a,b=fa(r),fb(r)
                if a and a==b: fired.append((a,r["result"]))  # les 2 signaux d'accord
            nn=len(fired)
            if nn<min_n: continue
            hits=sum(1 for p,res in fired if p==res)
            acc=hits/nn; wlo=_wilson_lower(hits,nn)
            scored.append((acc, f"{'✅' if wlo>0.54 else ('🔴' if acc<0.46 else '⚪')} {na}+{nb}: `{acc*100:.0f}%` (min `{wlo*100:.0f}%`) n={nn}"))
    lines=["🧬 *COMBOS DE SIGNAUX* (slot_records)","━━━━━━━━━━━━━━","_les 2 signaux d'accord → précision directionnelle_"]
    if scored:
        for _,txt in sorted(scored,key=lambda x:-x[0]): lines.append(txt)
    else:
        lines.append("_Pas assez de slots où des paires s'accordent (attends plus de data)._")
    lines.append("\n_✅ combo à fort edge → filtre prioritaire. 🔴 = à fuir/inverser._")
    return "\n".join(lines)

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
                and now>pr["slot_end"]+10 and pr.get("dir") in ("UP","DOWN")):
                # Utiliser le bon prix selon l'asset
                asset_tag="BTC"
                reason=pr.get("reason","")
                if reason.startswith("ETH:"): asset_tag="ETH"
                elif reason.startswith("SOL:"): asset_tag="SOL"
                elif reason.startswith("XRP:"): asset_tag="XRP"
                asset_opens=pr.get("asset_open",{})
                ref_px=asset_opens.get(asset_tag,0) or pr.get("open_px",0)
                cur_px={"BTC":p,"ETH":st.eth_price,"SOL":st.sol_price,"XRP":st.xrp_price}.get(asset_tag,p)
                if ref_px<=0 or cur_px<=0: continue
                won=(cur_px>ref_px)==(pr["dir"]=="UP")
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
        "session":bet.get("session","?"),"source":bet.get("source","?"),
        "asset":bet.get("asset","?"),"entry_token":bet.get("entry_token",0),"t_remaining":bet.get("t_remaining",0),
        "fill_type":bet.get("fill_type","?"),"fee_est":bet.get("fee_est",0),
        "aligned_15h1h":i15_n.get("ema_bull")==i1h_n.get("ema_bull") if i15_n and i1h_n else True})
    st.bet=None; st.token_price_peak=0; st.trailing_active=False; st.bet_expiry=0
    if not won and st.consec>=CONSERVATIVE_AFTER_LOSSES:
        await send(context.bot, f"⚠️ *Mode conservateur activé 2h* — {st.consec} pertes consécutives")
    elif won and st.win_streak_count>=BOOST_AFTER_WINS:
        await send(context.bot, f"🔥 *{st.win_streak_count} wins consécutifs* — Kelly +20%")
    cd_msg=f"\n⏸ Cooldown {COOLDOWN_MIN}min" if in_cd() else ""
    await send(context.bot,f"{'✅' if won else '❌'} *Trade clôturé* [📄]\n`{bet['dir']}` `${bet['entry']:,.0f}`→`${st.price:,.0f}`\nPnL:`{'+' if gross>=0 else ''}{gross:.2f}$` BR:`{st.bankroll:.2f}` ROI:`{roi()}`{cd_msg}")
    st.backup()

ASSETS = ("BTC", "ETH", "SOL", "XRP")
def _possfx(asset):
    """✅ (21/06) Suffixe d'attribut de position PAR CRYPTO (slot réservé supprimé). BTC garde st.bet
    (suffixe ""), ETH/SOL/XRP ont leur propre slot (st.bet_eth/_sol/_xrp) → les 4 cryptos peuvent
    tenir une position en parallèle, 1 par slot et par crypto (verrou asset_trade_slot)."""
    return "" if asset == "BTC" else f"_{asset.lower()}"

_last_clob_alert = [0.0]  # ✅ (21/06) dédoublonnage alerte "CLOB non authentifié" (1×/10min)
async def place_bet(context, direction, amount, conf, reasoning, conf_score, sess, tpu, tpd, market_end, source="tick", asset="BTC", reserved=False, market=None):
    """
    ✅ v10.23 — Placement centralisé: REFETCH prix + MAKER order (undercut) +
    ENTRÉE ÉTAGÉE (la 2e tranche est gérée dans st.bet["staged_remaining"]).
    Rappel source: sur Polymarket tout est un ordre LIMITE de toute façon.

    ✅ (21/06) demande user: slot RÉSERVÉ supprimé. Chaque crypto a son propre slot (_possfx) →
    BTC/ETH/SOL/XRP peuvent trader en parallèle, 1 par slot et par crypto. Le paramètre `reserved`
    est conservé pour compat de signature mais ignoré.
    """
    sfx = _possfx(asset)
    cur_slot = int(time.time()//300)*300
    # Normalise market_end en timestamp numérique: plusieurs stratégies passent une string ISO
    # (market.get("end_date")) → sinon `market_end > 0` crashe en mode réel (TypeError str/int).
    if isinstance(market_end, str):
        try: market_end = datetime.fromisoformat(market_end.replace("Z","+00:00")).timestamp()
        except Exception: market_end = 0.0
    elif not isinstance(market_end, (int, float)):
        market_end = 0.0
    if getattr(st, f"bet{sfx}") is not None:
        return False
    if getattr(st, "bet_in_flight", False):  # ✅ achat déjà en cours — sérialise les deux slots (pas de vraie concurrence d'ordres)
        return False
    if st.asset_trade_slot.get(asset) == cur_slot:  # ✅ verrou PAR CRYPTO (toutes stratégies + les 2 slots confondus)
        return False
    if not isinstance(conf_score, dict):
        conf_score = {"score":0,"signals":[]}
    # Verrous posés AVANT tout await (race-safe): in-flight global + slot par crypto
    st.bet_in_flight = True
    st.asset_trade_slot[asset] = cur_slot
    try:
        order_id=None; token_used=None; entry_tp=0.5
        # ✅ v10.23 — Entrée étagée: on place d'abord STAGED_FRACTIONS[0] du montant
        staged_remaining = 0.0
        first_amount = amount
        if STAGED_ENTRY and amount >= MIN_BET_USD*2 and source in ("tick","snipe"):
            first_amount = round(max(MIN_BET_USD, amount*STAGED_FRACTIONS[0]),2)
            staged_remaining = round(amount-first_amount,2)
            if staged_remaining < MIN_BET_USD:  # le reste serait sous le minimum → on met tout d'un coup
                first_amount = amount; staged_remaining = 0.0

        # ✅ (21/06) FIX ROUTAGE D'ACTIF: on utilise le marché passé EXPLICITEMENT (capturé par le job
        # appelant) et PAS le global st.current_market — partagé et écrasé par les jobs oracle concurrents
        # (BTC/ETH/SOL/XRP toutes les 2s) entre les await → un ordre "BTC" partait sur le marché XRP.
        mkt = market if market is not None else st.current_market
        if not st.paper_mode and mkt:
            token_used=mkt["token_up"] if direction=="UP" else mkt["token_down"]
            # ✅ (21/06) CLOB pas authentifié (clé API non créée) → impossible de poster un ordre réel.
            # Alerte Telegram dédoublonnée (1×/10min) au lieu d'un "ordre refusé" cryptique en boucle.
            # Cause fréquente: 2 instances du bot tournent et se volent la clé API (voir erreur Conflict).
            if not poly.ready or poly.auth_failed:
                if time.time() - _last_clob_alert[0] > 600:
                    _last_clob_alert[0] = time.time()
                    await send(context.bot, "🔴 *CLOB non authentifié* — clé API Polymarket non créée (erreur 400 \"Could not create api key\").\nAucun trade réel possible. Vérifie qu'**une seule** instance du bot tourne (l'erreur `Conflict` indique 2 instances qui se volent la clé), puis redéploie proprement.")
                log_skip("CLOB non authentifié (clé API non créée) — ordre réel impossible", direction)
                st.asset_trade_slot[asset] = 0
                return False
            if market_end > 0 and (market_end - time.time()) < 15:
                log_skip(f"Slot expire dans {market_end-time.time():.0f}s — ordre annulé", direction)
                st.asset_trade_slot[asset] = 0
                return False
            # ✅ REFETCH prix juste avant l'ordre
            fresh_tp = await poly.get_token_price(token_used)
            entry_tp = fresh_tp if fresh_tp > 0 else (tpu if direction=="UP" else tpd)
            if source=="tick" and (entry_tp < 0.35 or entry_tp > 0.92):
                log_skip(f"Prix token bougé avant ordre ({entry_tp:.2f}$)", direction); st.asset_trade_slot[asset] = 0; return False
            if source=="snipe" and (entry_tp < SNIPE_TOKEN_MIN-0.05 or entry_tp > SNIPE_TOKEN_MAX+0.03):
                log_skip(f"SNIPE: prix token bougé ({entry_tp:.2f}$)", direction); st.asset_trade_slot[asset] = 0; return False
            # ✅ #1 — Baseline du solde AVANT l'ordre, pour confirmer un fill réel (vs supposer rempli)
            bal0 = await poly.get_position_size(token_used)
            # ✅ (21/06) baseline CASH (USDC) AVANT l'ordre → permet de mesurer le COÛT RÉEL exécuté
            # (parts × prix de fill + FRAIS taker), donc d'afficher exactement la mise/prix Polymarket
            # au lieu d'une estimation (prix marché × parts) qui ignore le spread maker et les frais.
            usdc0 = await fetch_clob_balance()
            fill_type = "assumed"  # v1/non vérifiable: on suppose rempli (ancien comportement)
            real_shares = None     # shares RÉELLEMENT reçues (mesurées via le solde), pas supposées
            real_cost = None       # coût RÉEL en $ (débit cash) FRAIS INCLUS, mesuré via usdc0-usdc1
            filled = False
            order_id = None
            taker_blocked = False  # True si annulation maker incertaine → on s'interdit le taker (anti-doublon)

            if bal0 is None:
                # ✅ (22/06) demande user: taker restauré en fallback. Solde non vérifiable (client v1)
                # → 1 maker supposé rempli, sinon taker direct.
                order_id = await poly.place_order(token_used, first_amount, entry_tp, "BUY")
                if order_id:
                    filled = True; fill_type = "assumed"
                else:
                    log.info(f"{asset}: maker rejeté, taker direct (solde non vérifiable)")
                    order_id = await poly.place_market_order(token_used, first_amount, "BUY")
                    if not order_id:
                        await send(context.bot,"⚠️ *Ordre Polymarket refusé — réessai prochain slot*"); st.asset_trade_slot[asset] = 0; return False
                    filled = True; fill_type = "taker"
            else:
                # ✅ (22/06) demande user: délai maker ADAPTATIF au temps réel restant avant la fin du
                # slot (market_end), au lieu d'une constante fixe. Avec la fenêtre élargie jusqu'à T-5s,
                # un signal tardif peut n'avoir que ~5s de marge — attendre 3s pile pour 1 seul check de
                # fill prendrait 60% de ce qui reste. On répartit le temps dispo: 40% du temps restant
                # pour l'attente de fill (plafonné à FILL_WAIT_S, plancher 0.5s), 50% pour la fenêtre de
                # retry maker globale (plafonné à MAKER_RETRY_WINDOW_S). Loin de la fin (signal précoce
                # T-45s par ex), ça revient simplement aux constantes normales (plein temps disponible).
                time_left = max(0.0, market_end - time.time())
                fill_wait = max(0.5, min(FILL_WAIT_S, time_left * 0.4))
                retry_window = max(0.5, min(MAKER_RETRY_WINDOW_S, time_left * 0.5))
                # ✅ (21/06) demande user: on RÉESSAIE le maker (re-prix frais à chaque tentative) pendant
                # ~retry_window secondes avant de basculer en taker. Chaque tentative: pose GTC,
                # attend fill_wait, vérifie le fill; si non rempli ET annulation propre → reboucle.
                maker_deadline = time.time() + retry_window
                attempt = 0
                while time.time() < maker_deadline and not filled:
                    attempt += 1
                    fresh = await poly.get_token_price(token_used)
                    ref_px = fresh if fresh > 0 else entry_tp
                    mid = await poly.place_order(token_used, first_amount, ref_px, "BUY")  # maker GTC
                    if not mid:
                        log.info(f"{asset}: maker rejeté (essai {attempt}) — bascule taker")
                        break  # maker pas sur le book → taker safe plus bas
                    order_id = mid
                    await asyncio.sleep(fill_wait)
                    # Rempli pendant l'attente ? (solde CLOB en retard → poll avec retry)
                    bal1 = await poly.get_position_size_polled(token_used, bal0)
                    if bal1 is not None and bal1 > bal0:
                        filled = True; fill_type = "maker"; real_shares = round(bal1 - bal0, 4); break
                    # Pas (encore) vu rempli → on annule pour réessayer un meilleur prix
                    cancel_info = await poly.cancel_order(order_id)
                    cancel_uncertain = (not cancel_info.get("ok")) or cancel_info.get("already_filled")
                    # ✅ ANTI-DOUBLE-FILL autoritatif: demande à l'API combien de parts le maker a déjà matché.
                    matched = await poly.get_order_matched(order_id)
                    if matched and matched > 0:
                        filled = True; fill_type = "maker"
                        bconf = await poly.get_position_size_polled(token_used, bal0, tries=4, delay=0.8)
                        real_shares = round(bconf - bal0, 4) if (bconf is not None and bconf > bal0) else round(matched, 4)
                        log.info(f"{asset}: maker matché {matched} parts (API) — taker bloqué (anti-doublon)")
                        break
                    if cancel_uncertain:
                        # Annulation incertaine → maker peut-être rempli (lag). On NE reposte PAS et on
                        # s'interdit le taker. Vérif solde élargie.
                        bconf = await poly.get_position_size_polled(token_used, bal0, tries=4, delay=0.8)
                        if bconf is not None and bconf > bal0:
                            filled = True; fill_type = "maker"; real_shares = round(bconf - bal0, 4)
                        else:
                            taker_blocked = True
                            log.warning(f"{asset}: maker non annulable, fill non confirmé — pas de taker (anti-doublon)")
                        break
                    # Annulation propre + non rempli → on reboucle (re-post maker à prix frais) si fenêtre restante
                    log.info(f"{asset}: maker non rempli (essai {attempt}), reprise…")
                # ✅ Repli TAKER: maker épuisé sans fill ET annulation toujours propre (pas de risque doublon)
                if not filled and not taker_blocked:
                    # Dernière vérif solde avant de croiser (faux no-fill possible: fill maker tardif)
                    bconf = await poly.get_position_size_polled(token_used, bal0, tries=4, delay=0.8)
                    if bconf is not None and bconf > bal0:
                        filled = True; fill_type = "maker"; real_shares = round(bconf - bal0, 4)
                    else:
                        log.info(f"{asset}: maker non rempli après ~{retry_window:.1f}s (adaptatif) → taker")
                        order_id = await poly.place_market_order(token_used, first_amount, "BUY")
                        if order_id:
                            bal2 = await poly.get_position_size_polled(token_used, bal0, tries=4, delay=0.8)
                            filled = (bal2 is not None and bal2 > bal0)
                            fill_type = "taker" if filled else "none"
                            if filled: real_shares = round(bal2 - bal0, 4)
                        else:
                            filled = False
            # ✅ (21/06) COÛT RÉEL frais inclus = débit cash USDC pendant l'ordre (usdc0 - usdc1).
            if filled and real_shares and real_shares > 0:
                usdc1 = await fetch_clob_balance()
                if usdc0 is not None and usdc1 is not None and usdc0 > usdc1:
                    rc = round(usdc0 - usdc1, 2)
                    per = rc / real_shares
                    if 0 < per <= 1.05:  # prix/part plausible (≤1$ + petite marge frais) → fiable
                        real_cost = rc
            # ✅ Gestion UNIQUE du no-fill pour TOUS les chemins réels (maker rejeté, maker GTC, etc.)
            if not filled:
                st.exec_stats["nofill"] = st.exec_stats.get("nofill",0) + 1
                # On GARDE le verrou asset_trade_slot (déjà posé) pour tout le slot dès qu'un ordre a
                # atteint l'exchange — le solde peut être en retard (faux no-fill) et une autre stratégie
                # du même crypto re-rentrerait sinon. job_reconcile alerte si une position réelle subsiste.
                log.warning(f"{asset}: no-fill rapporté — verrou slot CONSERVÉ (anti-doublon, fill possible non vu)")
                log_skip(f"{asset}: ordre non rempli rapporté (verrou slot gardé anti-doublon)", direction)
                return False
            st.exec_stats[fill_type] = st.exec_stats.get(fill_type,0) + 1
            setattr(st, f"active_order_id{sfx}", order_id); setattr(st, f"active_token_id{sfx}", token_used)
            # ✅ (21/06) prix d'entrée = prix marché réel (entry_tp). Le budget Kelly ($) peut différer
            # du coût réel à cause du minimum Polymarket de 5 PARTS → coût réel = parts × prix (et non le
            # budget Kelly). La mise affichée/enregistrée = parts × prix.
            cost_measured = False
            if real_shares and real_shares > 0:
                shares_bought_final = real_shares
                if real_cost is not None:
                    # ✅ (21/06) mise + prix RÉELS mesurés via le débit cash USDC = exactement ce que
                    # Polymarket a prélevé (fill + frais inclus) → l'affichage Telegram colle au réel.
                    entry_token_price_final = round(real_cost / real_shares, 4)
                    first_amount = real_cost
                    cost_measured = True
                else:
                    entry_token_price_final = entry_tp if entry_tp>0 else round(first_amount/real_shares, 4)
                    first_amount = round(shares_bought_final * entry_token_price_final, 2)
            else:
                entry_token_price_final = entry_tp
                shares_bought_final = float(max(1, round(first_amount/entry_tp))) if entry_tp>0 else 0
                first_amount = round(shares_bought_final * entry_token_price_final, 2)
            setattr(st, f"entry_token_price{sfx}", entry_token_price_final)
            setattr(st, f"shares_bought{sfx}", shares_bought_final)
            # frais: si coût mesuré → déjà inclus dans first_amount (part frais ≈ coût - parts×prix_marché,
            # informatif). Sinon estimation: ~0 en maker (rebate), taker_fee_per_share sinon.
            if cost_measured:
                fee_est = round(max(0.0, first_amount - shares_bought_final * entry_tp), 3) if entry_tp>0 else 0.0
            else:
                fee_est = 0.0 if fill_type=="maker" else round(taker_fee_per_share(entry_token_price_final) * shares_bought_final, 3)
            if asset=="BTC": st.token_price_peak=1.0; st.trailing_active=False
            setattr(st, f"bet_expiry{sfx}", market_end if market_end>0 else (int(time.time()//300)*300+300))
        else:
            entry_tp = tpu if direction=="UP" else tpd
            setattr(st, f"entry_token_price{sfx}", entry_tp)
            setattr(st, f"shares_bought{sfx}", round(first_amount/entry_tp,4) if entry_tp>0 else 0)
            setattr(st, f"bet_expiry{sfx}", int(time.time()//300)*300+300)
            fill_type = "paper"; fee_est = 0.0
        # t restant dans le slot au moment de l'entrée (pour l'analyse de timing /zones)
        t_remaining = round(max(0.0, (market_end - time.time()) if market_end and market_end>0 else (cur_slot+300 - time.time())), 1)
        # ✅ (21/06) prix d'OUVERTURE du slot (oracle) mémorisé → permet de reconstruire le résultat
        # UP/DOWN à la résolution si le slot recorder n'a pas (encore) enregistré (corrige les faux LOSS).
        _slot_open_map = {"BTC":st.oracle_slot_open,"ETH":st.eth_oracle_slot_open,
                          "SOL":st.sol_oracle_slot_open,"XRP":st.xrp_oracle_slot_open}
        slot_open_px = _slot_open_map.get(asset, 0) or 0
        setattr(st, f"bet{sfx}", {"dir":direction,"amount":first_amount,"conf":conf,"entry":consensus_price() if consensus_price()>0 else st.price,
                "reasoning":reasoning,"ts":int(time.time()),"score":conf_score.get("score",0),"session":sess["session"],
                "staged_remaining":staged_remaining,"staged_done":staged_remaining<=0,"source":source,
                "asset":asset,"entry_token":round(entry_tp,4),"t_remaining":t_remaining,"slot_open_px":slot_open_px,
                "fill_type":fill_type,"fee_est":fee_est,"reserved":reserved})
        setattr(st, f"expiry_alerted{sfx}", False)  # ✅ (21/06) reset flag alerte T-30s pour la nouvelle position
        if asset == "BTC":
            st.last_trade_slot = cur_slot  # ✅ dédup BTC (job_tick/momentum/meanrev/oracle BTC s'y réfèrent)
        return True
    finally:
        st.bet_in_flight = False  # ✅ libère TOUJOURS le verrou in-flight (succès, échec ou exception)

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

    # ✅ v10.25 — job_tick désactivé en mode réel (paper/stats uniquement)
    # job_tick (entrée T-60s à T-50s, token 0.50-0.75$) = zone taker fees max = non rentable
    # En mode réel: on laisse tourner uniquement pour la résolution paper et les stats
    # Le trading réel passe par job_oracle_lag + job_momentum_* + job_mean_reversion_*
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
    ok = await place_bet(context, direction, amount, dec["conf"], dec["reasoning"], conf_score, sess, tpu, tpd, market_end, source="tick", asset="BTC", market=market)
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
                                # ✅ v12.9 — capture spread + profondeur $ + microprice + OFI
                                best_bid = 0.0; best_ask = 0.0; depth_usd = 0.0
                                best_bid_vol = 0.0; best_ask_vol = 0.0
                                for b in bids:
                                    try:
                                        if isinstance(b,dict): bp=float(b.get("price",0)); bs=float(b.get("size",0))
                                        elif isinstance(b,(list,tuple)) and len(b)>=2: bp=float(b[0]); bs=float(b[1])
                                        else: continue
                                        bid_vol+=bs; depth_usd+=bp*bs
                                        if bp>best_bid: best_bid=bp; best_bid_vol=bs
                                    except: pass
                                for a in asks:
                                    try:
                                        if isinstance(a,dict): ap=float(a.get("price",0)); asz=float(a.get("size",0))
                                        elif isinstance(a,(list,tuple)) and len(a)>=2: ap=float(a[0]); asz=float(a[1])
                                        else: continue
                                        ask_vol+=asz; depth_usd+=ap*asz
                                        if best_ask==0.0 or ap<best_ask: best_ask=ap; best_ask_vol=asz
                                    except: pass
                                total = bid_vol + ask_vol
                                if total > 0:
                                    st.ob_imbalance = round((bid_vol-ask_vol)/total,3)
                                    st.ob_ts = time.time()
                                    st.ob_spread = round(best_ask-best_bid,4) if (best_bid>0 and best_ask>0) else 0.0
                                    st.ob_depth = round(depth_usd,2)
                                    # ✅ v12.9 — MICROPRICE (Stoikov, mode mesure): weighted mid pondéré par l'imbalance top-of-book.
                                    # microprice = I×Pa + (1-I)×Pb, où I = Qb/(Qb+Qa). Penche vers le côté lourd du carnet.
                                    tb = best_bid_vol + best_ask_vol
                                    if best_bid>0 and best_ask>0 and tb>0:
                                        I = best_bid_vol / tb
                                        st.ob_microprice = round(I*best_ask + (1-I)*best_bid, 4)
                                        mid = (best_bid+best_ask)/2
                                        # signal microprice: >0 penche UP (microprice au-dessus du mid), <0 penche DOWN
                                        st.ob_micro_signal = round(st.ob_microprice - mid, 4)
                                    # ✅ v12.9 — OFI (Order Flow Imbalance, mode mesure): variation NETTE du top-of-book vs tick précédent.
                                    prev = getattr(st, "ob_prev_bbo", None)
                                    if prev and best_bid>0 and best_ask>0:
                                        pbb, pbbv, pba, pbav = prev
                                        # OFI standard: +ΔQb si bid monte/grossit, -ΔQa si ask monte/grossit
                                        ofi = 0.0
                                        if best_bid > pbb: ofi += best_bid_vol
                                        elif best_bid == pbb: ofi += (best_bid_vol - pbbv)
                                        else: ofi -= pbbv
                                        if best_ask < pba: ofi -= best_ask_vol
                                        elif best_ask == pba: ofi -= (best_ask_vol - pbav)
                                        else: ofi += pbav
                                        st.ob_ofi = round(ofi, 2)
                                    st.ob_prev_bbo = (best_bid, best_bid_vol, best_ask, best_ask_vol)
                                    # ✅ (22/06) demande user: version lissée + persistante (exige cohérence sur ~5s)
                                    st.ob_micro_smooth, st.ob_ofi_smooth, st.ob_persistent = ob_persistent_signal(
                                        "BTC", getattr(st,"ob_micro_signal",0.0), getattr(st,"ob_ofi",0.0))
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
                                # ✅ v12.9 — spread + profondeur $ + microprice + OFI (ETH/SOL)
                                best_bid = 0.0; best_ask = 0.0; depth_usd = 0.0
                                best_bid_vol = 0.0; best_ask_vol = 0.0
                                for b in bids:
                                    try:
                                        if isinstance(b,dict): bp=float(b.get("price",0)); bs=float(b.get("size",0))
                                        elif isinstance(b,(list,tuple)) and len(b)>=2: bp=float(b[0]); bs=float(b[1])
                                        else: continue
                                        bid_vol+=bs; depth_usd+=bp*bs
                                        if bp>best_bid: best_bid=bp; best_bid_vol=bs
                                    except: pass
                                for a in asks:
                                    try:
                                        if isinstance(a,dict): ap=float(a.get("price",0)); asz=float(a.get("size",0))
                                        elif isinstance(a,(list,tuple)) and len(a)>=2: ap=float(a[0]); asz=float(a[1])
                                        else: continue
                                        ask_vol+=asz; depth_usd+=ap*asz
                                        if best_ask==0.0 or ap<best_ask: best_ask=ap; best_ask_vol=asz
                                    except: pass
                                total = bid_vol + ask_vol
                                if total > 0:
                                    imb = round((bid_vol-ask_vol)/total,3)
                                    spr = round(best_ask-best_bid,4) if (best_bid>0 and best_ask>0) else 0.0
                                    dep = round(depth_usd,2)
                                    # microprice + signal
                                    micro_sig = 0.0
                                    tb = best_bid_vol + best_ask_vol
                                    if best_bid>0 and best_ask>0 and tb>0:
                                        I = best_bid_vol / tb
                                        micro = I*best_ask + (1-I)*best_bid
                                        micro_sig = round(micro - (best_bid+best_ask)/2, 4)
                                    # OFI vs tick précédent (stocké par asset)
                                    ofi = 0.0
                                    prev_attr = "eth_ob_prev_bbo" if asset=="ETH" else "sol_ob_prev_bbo"
                                    prev = getattr(st, prev_attr, None)
                                    if prev and best_bid>0 and best_ask>0:
                                        pbb, pbbv, pba, pbav = prev
                                        if best_bid > pbb: ofi += best_bid_vol
                                        elif best_bid == pbb: ofi += (best_bid_vol - pbbv)
                                        else: ofi -= pbbv
                                        if best_ask < pba: ofi -= best_ask_vol
                                        elif best_ask == pba: ofi -= (best_ask_vol - pbav)
                                        else: ofi += pbav
                                        ofi = round(ofi, 2)
                                    if best_bid>0 and best_ask>0:
                                        setattr(st, prev_attr, (best_bid, best_bid_vol, best_ask, best_ask_vol))
                                    if asset=="ETH":
                                        st.eth_ob_imbalance=imb; st.eth_ob_ts=time.time(); st.eth_ob_spread=spr; st.eth_ob_depth=dep
                                        st.eth_ob_micro_signal=micro_sig; st.eth_ob_ofi=ofi
                                        st.eth_ob_micro_smooth, st.eth_ob_ofi_smooth, st.eth_ob_persistent = ob_persistent_signal("ETH", micro_sig, ofi)
                                    else:
                                        st.sol_ob_imbalance=imb; st.sol_ob_ts=time.time(); st.sol_ob_spread=spr; st.sol_ob_depth=dep
                                        st.sol_ob_micro_signal=micro_sig; st.sol_ob_ofi=ofi
                                        st.sol_ob_micro_smooth, st.sol_ob_ofi_smooth, st.sol_ob_persistent = ob_persistent_signal("SOL", micro_sig, ofi)
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
    # ✅ v12.9 — Régime trendsession: >60% deltaneg → resserrer le DÉBUT (entrer plus tard, plus près de la décision)
    # ✅ (22/06) FIX permanent: resserrement = milieu de la fenêtre configurée (proportionnel), au lieu
    # d'un floor codé en dur qui redevenait un no-op (ou pire, un élargissement) chaque fois que la
    # fenêtre ORACLE_WINDOW_START/END change. Reste toujours strictement plus tardif que le défaut.
    recent_30=[p for p in st.pass_reasons if now-p.get("ts",0)<=1800]
    dn_ratio=sum(1 for p in recent_30 if "delta" in p.get("reason","").lower() and "<0" in p.get("reason",""))/max(len(recent_30),1)
    btc_win_start=(ORACLE_WINDOW_START+ORACLE_WINDOW_END)/2 if dn_ratio>0.60 else ORACLE_WINDOW_START
    if slot_remaining > btc_win_start or slot_remaining < ORACLE_WINDOW_END: return

    _resolve_pending_passes()  # ✅ v12.9 — Résolution immédiate

    if not st.oracle_connected or st.oracle_price <= 0 or st.oracle_slot_open <= 0:
        log_skip(f"BTC: WS non dispo (T-{int(slot_remaining)}s)", None); return
    if now - st.oracle_ts > 15:
        log_skip(f"BTC: tick périmé {int(now-st.oracle_ts)}s (T-{int(slot_remaining)}s)", None); return
    # ✅ (21/06) #4 — Intégrité source de résolution: le marché résout sur le PRIX CHAINLINK (open vs close).
    # Si le dernier tick Chainlink est trop vieux, st.oracle_price ne reflète plus le feed de résolution
    # → le delta est non fiable. On s'abstient plutôt que de parier sur une donnée périmée.
    cl_age = now - st.oracle_chainlink_ts if st.oracle_chainlink_ts > 0 else 999
    if cl_age > CHAINLINK_MAX_AGE:
        log_skip(f"BTC: Chainlink périmé {int(cl_age)}s (>{CHAINLINK_MAX_AGE}s) — source de résolution non fiable", None,
                 features={"filter":"chainlink_stale","asset":"BTC"}); return
    # ✅ v12.9 — verrou GLOBAL anti sur-exposition: 1 seul trade par slot toutes stratégies confondues
    # (nécessaire car l'oracle lag T-150→T-30 chevauche désormais momentum/meanrev/confluence T-150→T-60)
    if cur_slot in (st.last_trade_slot, getattr(st,"momentum_last_slot",0), getattr(st,"meanrev_last_slot",0), getattr(st,"tds_last_slot",0)):
        log_skip(f"BTC: slot déjà tradé par une stratégie (T-{int(slot_remaining)}s)", None); return

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
        elif direction == "DOWN":
            # ✅ v12.9 — FIX asymétrie: chute brutale CONFIRME un pari DOWN déjà établi (gap/delta)
            # → ne pas bloquer. Les autres filtres (deltaneg/tokenmax/EV) s'appliquent normalement ensuite.
            log.debug(f"BTC: ret3s {ret_3s:+.3f}% confirme DOWN déjà établi (gap={spot_oracle_gap:+.3f}%) → continuer")
        else:
            log_skip(f"BTC: ret3s {ret_3s:+.3f}%<-0.055% (chute brutale)", direction,
                     features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"ret3s_brutal","asset":"BTC"}); return

    if not direction:
        log_skip(f"BTC: Δ{oracle_delta:+.3f}% gap{spot_oracle_gap:+.3f}% (→ skip: delta et gap trop faibles)", None,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"weak_signal","asset":"BTC"}); return

    if direction == "UP" and spot_oracle_gap < 0:
        log_skip(f"BTC: UP bloqué gap négatif (→ skip: gap négatif)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"gap_neg","asset":"BTC"}); return
    # ✅ v12.9 Sonnet P1: BTC deltaneg exception si gap≥+0.040% ET ret3s>-0.050% (9W/3L=75%)
    if direction == "UP" and oracle_delta < -0.005:
        if (spot_oracle_gap >= 0.040 and abs(oracle_delta) >= 0.010 and ret_3s > -0.050):
            log.debug(f"BTC deltaneg override: gap={spot_oracle_gap:+.3f}% delta={oracle_delta:+.3f}% ret3s={ret_3s:+.3f}% → autoriser (9W/3L pattern)")
        else:
            # ✅ v12.9 SHADOW DOWN: avant de skip, logger un DOWN fantôme (log-only) si gap+/delta- persistant
            # sans chute brutale (ret3s pas en dessous du seuil override) → mesurer si DOWN aurait gagné
            if (spot_oracle_gap >= SHADOW_DOWN_GAP_MIN and abs(oracle_delta) >= SHADOW_DOWN_DELTA_MIN and ret_3s >= -0.070):
                log_shadow_down("BTC", spot_oracle_gap, oracle_delta, ret_3s)
            log_skip(f"BTC: delta {oracle_delta:+.3f}%<0 (→ skip: delta négatif LOSS garanti)", direction,
                     features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"delta_neg","asset":"BTC"}); return
    if direction == "DOWN" and oracle_delta > 0.005 and not ret3s_override:
        log_skip(f"BTC: delta {oracle_delta:+.3f}%>0 (→ skip: contre DOWN)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"delta_contra","asset":"BTC"}); return

    # ✅ (21/06) #4 — Open de slot capturé en retard (Chainlink n'a pas tické pile à la frontière) → l'open
    # de référence peut différer de celui de Polymarket: sur un delta marginal, le SENS est non fiable.
    open_lag = st.oracle_open_lag.get("BTC", 0)
    if open_lag > ORACLE_OPEN_LAG_MAX and abs(oracle_delta) < ORACLE_OPEN_LAG_DELTA:
        log_skip(f"BTC: open capturé tard ({open_lag:.0f}s) + delta marginal {oracle_delta:+.3f}% → sens non fiable", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"open_lag","asset":"BTC"}); return
    # ✅ (21/06) #1 — Marge de sécurité dépendante du temps: le delta oracle doit dominer le mouvement
    # résiduel attendu (σ·√t_restant), sinon l'oracle peut s'inverser avant la clôture. Strict tôt, souple tard.
    safe_ok, exp_move = oracle_safety_ok(pts, oracle_delta, slot_remaining, now)
    if not safe_ok:
        log_skip(f"BTC: marge insuffisante delta {oracle_delta:+.3f}% < {ORACLE_SAFETY_K:.2f}×{exp_move:.3f}% attendu (T-{int(slot_remaining)}s)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"safety_margin","exp_move":round(exp_move,4),"asset":"BTC"}); return
    # ✅ (21/06) #3 — Persistance du gap (signal gap uniquement): un pic isolé qui mean-revert referme le
    # gap dans le mauvais sens. On exige que le spot soit resté du bon côté de l'oracle sur ~5s.
    if primary_signal == "gap" and not gap_persistent(pts, st.oracle_price, gap_dir, now):
        log_skip(f"BTC: gap non persistant (spike) {spot_oracle_gap:+.3f}% — anti mean-revert", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"gap_spike","asset":"BTC"}); return

    # TA score
    price_hist = [{"price":p,"ts":t} for t,p in pts]
    ta_score, ta_dir, ta_details = compute_ta_score(price_hist, "BTC")
    ta_vote = 1 if ta_dir=="UP" else (-1 if ta_dir=="DOWN" else 0)
    dual_dir = ta_details.get("dual_dir")  # ✅ v12.9 dual model (mesure)

    # OB vote
    ob_vote = 0
    if time.time() - getattr(st, "ob_ts", 0) < 10:
        if st.ob_imbalance > 0.15: ob_vote = 1
        elif st.ob_imbalance < -0.15: ob_vote = -1

    # ✅ Volume spike — st.ws_volumes désormais alimenté par ws_binance_loop (qty aggTrade), et le vote
    # est replié dans dir_votes (avant: calculé puis jamais utilisé → code mort).
    vol_vote = compute_vol_vote(st.ws_volumes, direction, now)

    dir_votes = sum([
        1 if direction=="UP" and oracle_delta>0 else (-1 if direction=="DOWN" and oracle_delta<0 else 0),
        1 if direction=="UP" and spot_oracle_gap>0 else (-1 if direction=="DOWN" and spot_oracle_gap<0 else 0),
        1 if direction=="UP" and ret_15s>0 else (-1 if direction=="DOWN" and ret_15s<0 else 0),
        ob_vote, ta_vote, vol_vote,
    ])
    # ✅ v12.9 FIX BUG MAJEUR: dir_votes négatif quand DOWN confirmé (convention "bullishness").
    # ⚠️ dir_votes lui-même INCHANGÉ (exception SOL tokenmax dir_votes<=-1 ailleurs en dépend).
    votes_for_direction = dir_votes if direction == "UP" else -dir_votes

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
    if not token_price or token_price <= 0:
        # ✅ (21/06) avant: return SILENCIEUX → aucune passe loggée si le prix CLOB est indispo,
        # d'où "le bot ne fait plus rien" sans trace. Maintenant on loggue une passe visible.
        log_skip(f"BTC: prix token indispo (CLOB/price down) (T-{int(slot_remaining)}s)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"price_unavail","asset":"BTC"}); return

    asset_up = market.get("token_up","")
    if asset_up and st.ob_asset_id != asset_up:
        if hasattr(st,"clob_ws_task") and st.clob_ws_task and not st.clob_ws_task.done():
            st.clob_ws_task.cancel()
        st.clob_ws_task = asyncio.create_task(ws_clob_loop(asset_up))

    # ✅ (21/06) demande user: cap DUR 0.70$ (overrides retirés) — token 0.41→0.70$
    if token_price > ORACLE_TOKEN_MAX:
        log_skip(f"BTC: token {token_price:.2f}$>{ORACLE_TOKEN_MAX}$ (→ skip: marché a déjà pricé la direction)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"dual":dual_dir,"filter":"tokenmax","token":token_price,"asset":"BTC"}); return
    if token_price < ORACLE_TOKEN_MIN:
        log_skip(f"BTC: token {token_price:.2f}$<{ORACLE_TOKEN_MIN}$ (→ skip: trop incertain)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"dual":dual_dir,"filter":"tokenmin","token":token_price,"asset":"BTC"}); return
    # ✅ (21/06) #5 — Filtre spread du carnet: un book large → l'entrée réelle (frais inclus) érode l'EV
    # affichée. On skip si le spread frais dépasse le seuil (carnet périmé >10s → filtre ignoré, pas de blocage).
    if time.time() - getattr(st, "ob_ts", 0) < 10:
        cur_spread = getattr(st, "ob_spread", 0) or 0
        if cur_spread > ORACLE_MAX_SPREAD:
            log_skip(f"BTC: spread carnet {cur_spread*100:.1f}¢>{ORACLE_MAX_SPREAD*100:.0f}¢ (→ skip: entrée érode l'EV)", direction,
                     features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"dual":dual_dir,"filter":"wide_spread","spread":cur_spread,"asset":"BTC"}); return

    fee = taker_fee_per_share(token_price)
    p_oracle = min(0.93, 0.85 + abs(spot_oracle_gap)*3.0) if primary_signal=="gap" else min(0.90, 0.80 + abs(oracle_delta)*2.0)
    if votes_for_direction >= 3: p_oracle = min(0.95, p_oracle + 0.03)
    if votes_for_direction >= 4: p_oracle = min(0.96, p_oracle + 0.02)
    if chainlink_age < 2.0: p_oracle = min(0.97, p_oracle + 0.03)
    # ✅ #5 — Microprice + OFI (order flow temps réel) en CONFIRMATION: petit bonus proba si les deux
    # penchent dans le sens du trade et que le carnet est frais (<10s).
    # ✅ (22/06) demande user: utilise la version LISSÉE+PERSISTANTE (cohérente sur ~5s) au lieu de la
    # lecture instantanée — une lecture unique est du bruit exploitable (spoofing/iceberg).
    micro_sig = getattr(st, "ob_micro_smooth", 0.0); ofi = getattr(st, "ob_ofi_smooth", 0.0)
    persistent = getattr(st, "ob_persistent", False)
    if persistent and time.time() - getattr(st, "ob_ts", 0) < 10:
        micro_ok = (direction=="UP" and micro_sig > 0) or (direction=="DOWN" and micro_sig < 0)
        ofi_ok   = (direction=="UP" and ofi > 0) or (direction=="DOWN" and ofi < 0)
        # ✅ (22/06) demande user: bonus doublé (0.02→0.04) — "OB+OFI accord" confirmé à 92% de précision
        # directionnelle (n=145) par /edge, l'un des signaux les plus forts mesurés. Cette condition
        # (micro_ok ET ofi_ok ensemble) correspond exactement à cet "accord".
        if micro_ok and ofi_ok: p_oracle = min(0.97, p_oracle + 0.04)
    # ✅ (22/06) demande user: confirmation par pression de liquidation Binance Futures. Mécanique
    # directe (pas spéculatif): cascade de shorts liquidés = achat forcé = pression haussière immédiate
    # (et inversement pour les longs) — contrairement au funding rate, le SENS ici n'est pas ambigu.
    liq = liq_pressure("BTC")
    if abs(liq) >= LIQ_PRESSURE_MIN_USD:
        liq_ok = (direction=="UP" and liq > 0) or (direction=="DOWN" and liq < 0)
        if liq_ok: p_oracle = min(0.97, p_oracle + 0.02)
    # ✅ (21/06) #2 — Calibration empirique: remplace/ajuste p_oracle par le win-rate RÉEL observé pour ce
    # bucket (asset/signal/|delta|/votes), mélangé à la formule via shrinkage. Corrige les EV mal estimées.
    calib_bucket = oracle_bucket("BTC", primary_signal, oracle_delta, votes_for_direction)
    p_oracle = oracle_calibrated_p(p_oracle, calib_bucket)
    ev = p_oracle - token_price - fee

    # ✅ v12.9 FIX: vérifie le consensus POUR la direction parié, pas le score brut haussier
    if votes_for_direction < 2:
        log_skip(f"BTC: votes {votes_for_direction}/6 < 2 (→ skip: consensus faible)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":votes_for_direction,"dual":dual_dir,"filter":"votes_min","asset":"BTC"}); return
    if ev < ORACLE_EDGE_MIN_BTC:
        log_skip(f"BTC: EV {ev*100:+.1f}%<{ORACLE_EDGE_MIN_BTC*100:.0f}% (→ skip: edge insuffisant)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"dual":dual_dir,"filter":"ev","token":token_price,"ev":ev,"asset":"BTC"}); return

    payout = round(1/token_price, 2)
    # ✅ demande user 21/06: Kelly DÉDIÉ oracle_lag ciblant 3-4% du BR (au lieu des tiers 5/10/15%
    # de kelly_bet(), partagés avec job_tick/ob_signal) — remplace toutes les cryptos.
    amount = kelly_bet_oracle(st.bankroll, p_oracle, payout, token_price, votes=votes_for_direction)
    if amount < MIN_BET_USD: return

    # ✅ (22/06) FIX race condition: verrou posé ICI (avant le await place_bet, qui peut prendre
    # plusieurs secondes via les retries maker/taker) au lieu d'après. Avant: si job_oracle_lag était
    # re-déclenché pendant qu'un place_bet précédent était encore en cours (await), son propre check
    # externe était périmé et une 2e/3e tentative pouvait passer → plusieurs achats réels sur le même
    # slot BTC (vu: 3 achats UP à tailles différentes en ~1min). Le verrou interne asset_trade_slot
    # protège la plupart du temps mais certains chemins de retry le relâchent avant qu'un ordre touche
    # l'exchange — cette fenêtre est désormais fermée côté oracle_lag aussi.
    st.last_trade_slot = cur_slot

    # ✅ tpu/tpd doivent être des PRIX (float), pas les token_id (string) — sinon TypeError
    # str/int dès que place_bet compare entry_tp>0 (mode paper ou fallback prix).
    tpu = token_price if direction=="UP" else round(max(0.01,1-token_price),4)
    tpd = token_price if direction=="DOWN" else round(max(0.01,1-token_price),4)
    try: market_end = datetime.fromisoformat(market.get("end_date","").replace("Z","+00:00")).timestamp()
    except: market_end = cur_slot + 300
    sess = session_ctx(); conf_score = {"score":0,"signals":[]}
    reasoning = (f"⚡ORACLE LAG BTC {direction} | gap={spot_oracle_gap:+.3f}% delta={oracle_delta:+.3f}% "
                 f"OB={st.ob_imbalance:+.2f} votes={dir_votes}/6 | tok={token_price:.3f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s")

    # ✅ (21/06) slot réservé supprimé — BTC oracle utilise le slot BTC normal (st.bet). 1 bet BTC/slot
    # via asset_trade_slot["BTC"]; ETH/SOL/XRP ont leurs propres slots → les 4 cryptos en parallèle.
    ok = await place_bet(context, direction, amount, round(p_oracle,2), reasoning, conf_score, sess, tpu, tpd, market_end, source="snipe", asset="BTC", market=market)
    if not ok: return
    if st.bet: st.bet["calib_bucket"] = calib_bucket  # ✅ #2 — mémorise le bucket pour MAJ à la résolution

    mode = "💰 RÉEL" if not st.paper_mode else "📄 paper"
    # ✅ (21/06) fallback prix: si l'entrée réelle mesurée est 0 (fill non vu), afficher le prix token pré-ordre.
    entry_tp = (st.entry_token_price or token_price) if not st.paper_mode else token_price
    real_amount = (st.bet or {}).get("amount", amount)  # montant réellement placé
    await send(context.bot,
        f"⚡ *ORACLE LAG ₿ BTC* [{mode}]\n━━━━━━━━━━━━━━━\n"
        f"*{direction}* | Mise réelle:`{real_amount:.2f}$` | P:`{p_oracle*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Δslot:`{oracle_delta:+.3f}%` | Gap:`{spot_oracle_gap:+.3f}%` OB:`{st.ob_imbalance:+.2f}` TA:`{ta_score}` | Votes:`{dir_votes}/6`\n"
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
    win_start = ORACLE_WINDOW_START  # ✅ (21/06) T-90s uniforme BTC/ETH/SOL/XRP
    if slot_remaining > win_start or slot_remaining < ORACLE_WINDOW_END: return  # ✅ (21/06) END T-20s
    if asset=="ETH":
        spot=st.eth_price; spot_ts=st.eth_ts; oracle=st.eth_oracle_price
        oracle_ts=st.eth_oracle_ts; slot_open=st.eth_oracle_slot_open
        last_ts=st.eth_last_trade_slot; slug_prefix="eth-updown-5m"
        symbol="ETH"; emoji="Ξ"; ws_prices=st.eth_ws_prices; ws_volumes=st.eth_ws_volumes
    elif asset=="SOL":
        spot=st.sol_price; spot_ts=st.sol_ts; oracle=st.sol_oracle_price
        oracle_ts=st.sol_oracle_ts; slot_open=st.sol_oracle_slot_open
        last_ts=st.sol_last_trade_slot; slug_prefix="sol-updown-5m"
        symbol="SOL"; emoji="◎"; ws_prices=st.sol_ws_prices; ws_volumes=st.sol_ws_volumes
    elif asset=="XRP":
        spot=st.xrp_price; spot_ts=st.xrp_ts; oracle=st.xrp_oracle_price
        oracle_ts=st.xrp_oracle_ts; slot_open=st.xrp_oracle_slot_open
        last_ts=st.xrp_last_trade_slot; slug_prefix="xrp-updown-5m"
        symbol="XRP"; emoji="✕"; ws_prices=st.xrp_ws_prices; ws_volumes=st.xrp_ws_volumes
    else: return
    if spot<=0 or oracle<=0 or slot_open<=0:
        log_skip(f"{symbol}: données manquantes spot={spot:.2f} oracle={oracle:.2f}", None); return
    if now-spot_ts>5:
        log_skip(f"{symbol}: prix spot périmé {int(now-spot_ts)}s", None); return
    if now-oracle_ts>15:
        log_skip(f"{symbol}: oracle périmé {int(now-oracle_ts)}s", None); return
    # ✅ (21/06) #4 — fraîcheur Chainlink (source de résolution open vs close). cl_ts partagé (tous symboles).
    cl_age = now - st.oracle_chainlink_ts if st.oracle_chainlink_ts > 0 else 999
    if cl_age > CHAINLINK_MAX_AGE:
        log_skip(f"{symbol}: Chainlink périmé {int(cl_age)}s (>{CHAINLINK_MAX_AGE}s) — résolution non fiable", None,
                 features={"filter":"chainlink_stale"}); return
    if last_ts==cur_slot: return
    # ✅ demande user 21/06: même verrou anti sur-exposition multi-stratégies que BTC (avant: oracle_lag
    # ETH/SOL/XRP ne vérifiait que son propre dernier slot, pas momentum/meanrev/confluence du même actif).
    # Le verrou final reste st.asset_trade_slot[asset] dans place_bet — ceci évite juste du travail inutile.
    _pfx0 = asset.lower()
    if cur_slot in (getattr(st,f"momentum_last_slot_{_pfx0}",0), getattr(st,f"meanrev_last_slot_{_pfx0}",0), getattr(st,f"tds_last_slot_{_pfx0}",0)):
        log_skip(f"{symbol}: slot déjà tradé par une stratégie (T-{int(slot_remaining)}s)", None); return
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
        elif direction == "DOWN":
            # ✅ v12.9 — FIX asymétrie: chute brutale CONFIRME un pari DOWN déjà établi (gap/delta)
            log.debug(f"{symbol}: ret3s {ret_3s:+.3f}% confirme DOWN déjà établi (gap={spot_oracle_gap:+.3f}%) → continuer")
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
    # ✅ v12.9 Sonnet P2: ETH/SOL seuil deltaneg abaissé à -0.010% (0W/8L ETH, 0W/3L SOL)
    if direction=="UP" and oracle_delta<-0.010:
        # ✅ v12.9 SHADOW DOWN: logger un DOWN fantôme (log-only) si gap+/delta- persistant sans chute brutale
        if (spot_oracle_gap >= SHADOW_DOWN_GAP_MIN and abs(oracle_delta) >= SHADOW_DOWN_DELTA_MIN and ret_3s >= -0.070):
            log_shadow_down(symbol, spot_oracle_gap, oracle_delta, ret_3s)
        log_skip(f"{symbol}: delta {oracle_delta:+.3f}%<-0.010% (delta négatif)",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"delta_neg"}); return
    if direction=="DOWN" and oracle_delta>0.005 and not ret3s_override:
        log_skip(f"{symbol}: delta {oracle_delta:+.3f}%>0 (→ skip: contre DOWN)",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"delta_contra"}); return
    # ✅ (21/06) #4 — open de slot capturé tard + delta marginal → sens non fiable
    open_lag = st.oracle_open_lag.get(asset, 0)
    if open_lag > ORACLE_OPEN_LAG_MAX and abs(oracle_delta) < ORACLE_OPEN_LAG_DELTA:
        log_skip(f"{symbol}: open capturé tard ({open_lag:.0f}s) + delta marginal {oracle_delta:+.3f}% → sens non fiable",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"open_lag"}); return
    # ✅ (21/06) #1 — marge de sécurité dépendante du temps restant (delta vs σ·√t_restant)
    safe_ok, exp_move = oracle_safety_ok(pts, oracle_delta, slot_remaining, now)
    if not safe_ok:
        log_skip(f"{symbol}: marge insuffisante delta {oracle_delta:+.3f}% < {ORACLE_SAFETY_K:.2f}×{exp_move:.3f}% attendu (T-{int(slot_remaining)}s)",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"safety_margin","exp_move":round(exp_move,4)}); return
    # ✅ (21/06) #3 — persistance du gap (anti-spike), signal gap uniquement
    if primary_signal=="gap" and not gap_persistent(pts, oracle, gap_dir, now):
        log_skip(f"{symbol}: gap non persistant (spike) {spot_oracle_gap:+.3f}% — anti mean-revert",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":0,"filter":"gap_spike"}); return
    price_hist=[{"price":p,"ts":t} for t,p in pts]
    ta_score,ta_dir,ta_details=compute_ta_score(price_hist,asset)
    ta_vote=1 if ta_dir=="UP" else (-1 if ta_dir=="DOWN" else 0)
    dual_dir = ta_details.get("dual_dir")  # ✅ v12.9 dual model (mesure)
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
    # ✅ v12.9 Point6: défaut sûr pour divergence (sinon NameError si pas assez de points)
    divergence = 0.0
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
    # ✅ vol_vote (qty aggTrade Binance par asset) replié dans dir_votes — même fix que BTC.
    vol_vote = compute_vol_vote(ws_volumes, direction, now)
    dir_votes=sum([
        1 if direction=="UP" and oracle_delta>0 else (-1 if direction=="DOWN" and oracle_delta<0 else 0),
        1 if direction=="UP" and spot_oracle_gap>0 else (-1 if direction=="DOWN" and spot_oracle_gap<0 else 0),
        1 if direction=="UP" and ret_15s>0 else (-1 if direction=="DOWN" and ret_15s<0 else 0),
        btc_cascade_vote, ta_vote, vol_vote,
    ])
    # ✅ v12.9 FIX BUG MAJEUR: dir_votes négatif quand DOWN confirmé (convention "bullishness").
    # ⚠️ dir_votes lui-même INCHANGÉ (exception SOL tokenmax dir_votes<=-1 plus bas en dépend).
    votes_for_direction = dir_votes if direction == "UP" else -dir_votes
    market=await poly.get_market_by_slug(f"{slug_prefix}-{cur_slot}")
    if not market:
        log_skip(f"{symbol}: marché non trouvé",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"no_market"}); return
    token_used=market["token_up"] if direction=="UP" else market["token_down"]
    token_price=await poly.get_token_price(token_used)
    if not token_price or token_price<=0:
        # ✅ (21/06) avant: return SILENCIEUX → aucune passe loggée si le prix CLOB est indispo.
        log_skip(f"{symbol}: prix token indispo (CLOB/price down) (T-{int(slot_remaining)}s)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"filter":"price_unavail"}); return
    # ✅ v12.4 — Lancer WS CLOB OB pour ETH/SOL
    asset_up_ob = market.get("token_up","")
    if asset=="ETH" and asset_up_ob and st.eth_ob_asset_id != asset_up_ob:
        if st.eth_clob_ws_task and not st.eth_clob_ws_task.done(): st.eth_clob_ws_task.cancel()
        st.eth_clob_ws_task = asyncio.create_task(ws_clob_loop_asset(asset_up_ob,"ETH"))
    elif asset=="SOL" and asset_up_ob and st.sol_ob_asset_id != asset_up_ob:
        if st.sol_clob_ws_task and not st.sol_clob_ws_task.done(): st.sol_clob_ws_task.cancel()
        st.sol_clob_ws_task = asyncio.create_task(ws_clob_loop_asset(asset_up_ob,"SOL"))
    # ✅ (21/06) demande user: cap DUR 0.70$ pour TOUTES les cryptos (token 0.41→0.70$)
    effective_token_max = ORACLE_TOKEN_MAX
    if token_price>effective_token_max:
        log_skip(f"{symbol}: token {token_price:.2f}$>{effective_token_max}$ (déjà pricé)",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"dual":dual_dir,"filter":"tokenmax","token":token_price}); return
    if token_price<ORACLE_TOKEN_MIN:
        log_skip(f"{symbol}: token {token_price:.2f}$<{ORACLE_TOKEN_MIN}$ (incertain)",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"dual":dual_dir,"filter":"tokenmin","token":token_price}); return
    _pfx = asset.lower()
    # ✅ (21/06) #5 — filtre spread du carnet (érosion EV à l'entrée). XRP n'a pas de WS carnet → spread=0
    # (filtre naturellement ignoré). Carnet périmé >10s → ignoré aussi (pas de blocage).
    ob_ts_asset = getattr(st, f"{_pfx}_ob_ts", 0)
    if time.time() - ob_ts_asset < 10:
        cur_spread = getattr(st, f"{_pfx}_ob_spread", 0) or 0
        if cur_spread > ORACLE_MAX_SPREAD:
            log_skip(f"{symbol}: spread carnet {cur_spread*100:.1f}¢>{ORACLE_MAX_SPREAD*100:.0f}¢ (→ skip: entrée érode l'EV)",direction,
                     features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"dual":dual_dir,"filter":"wide_spread","spread":cur_spread}); return
    fee=taker_fee_per_share(token_price)
    p_oracle=min(0.93,0.85+abs(spot_oracle_gap)*3.0) if primary_signal=="gap" else min(0.90,0.80+abs(oracle_delta)*2.0)
    if votes_for_direction>=3: p_oracle=min(0.95,p_oracle+0.03)
    # ✅ #5 — Microprice + OFI par asset en confirmation (ETH/SOL calculés par ws_clob_loop_asset; XRP=0)
    # ✅ (22/06) demande user: version LISSÉE+PERSISTANTE au lieu de la lecture instantanée bruitée.
    micro_sig = getattr(st, f"{_pfx}_ob_micro_smooth", 0.0); ofi = getattr(st, f"{_pfx}_ob_ofi_smooth", 0.0)
    persistent = getattr(st, f"{_pfx}_ob_persistent", False)
    if persistent and time.time() - ob_ts_asset < 10:
        micro_ok = (direction=="UP" and micro_sig > 0) or (direction=="DOWN" and micro_sig < 0)
        ofi_ok   = (direction=="UP" and ofi > 0) or (direction=="DOWN" and ofi < 0)
        # ✅ (22/06) demande user: bonus doublé (0.02→0.04), même justification que BTC (92% n=145)
        if micro_ok and ofi_ok: p_oracle = min(0.96, p_oracle + 0.04)
    # ✅ (22/06) demande user: confirmation par pression de liquidation Binance Futures (mécanique directe)
    liq = liq_pressure(asset)
    if abs(liq) >= LIQ_PRESSURE_MIN_USD:
        liq_ok = (direction=="UP" and liq > 0) or (direction=="DOWN" and liq < 0)
        if liq_ok: p_oracle = min(0.96, p_oracle + 0.02)
    # ✅ (21/06) #2 — calibration empirique p_oracle (win-rate réel du bucket, shrinkage vers la formule)
    calib_bucket = oracle_bucket(asset, primary_signal, oracle_delta, votes_for_direction)
    p_oracle = oracle_calibrated_p(p_oracle, calib_bucket)
    ev=p_oracle-token_price-fee
    # ✅ v12.9 FIX: consensus POUR la direction parié (était dir_votes brut, cassé pour DOWN)
    if votes_for_direction < 2:
        log_skip(f"{symbol}: votes {votes_for_direction}/6 < 2 (→ skip: consensus faible)", direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":votes_for_direction,"dual":dual_dir,"filter":"votes_min"}); return
    if ev<ORACLE_EDGE_MIN_ALT:
        log_skip(f"{symbol}: EV {ev*100:+.1f}%<{ORACLE_EDGE_MIN_ALT*100:.0f}% insuffisant",direction,
                 features={"gap":spot_oracle_gap,"delta":oracle_delta,"ret3s":ret_3s,"votes":dir_votes,"dual":dual_dir,"filter":"ev","token":token_price,"ev":ev,"smt_div":round(divergence,3)}); return
    payout=round(1/token_price,2)
    # ✅ demande user 21/06: Kelly DÉDIÉ oracle_lag ciblant 3-4% du BR, toutes cryptos (cf. BTC).
    amount=kelly_bet_oracle(st.bankroll,p_oracle,payout,token_price,votes=votes_for_direction)
    if amount<MIN_BET_USD: return
    # ✅ (22/06) même fix que BTC: verrou posé ICI (avant le await place_bet) au lieu d'après, pour
    # fermer la fenêtre de race condition (job_oracle_lag_asset re-déclenché pendant un place_bet
    # encore en cours via ses retries maker/taker → doublons réels sur le même slot/asset).
    if asset=="ETH": st.eth_last_trade_slot=cur_slot
    elif asset=="SOL": st.sol_last_trade_slot=cur_slot
    elif asset=="XRP": st.xrp_last_trade_slot=cur_slot
    # ✅ tpu/tpd = PRIX (float), pas token_id (string) — cf. fix job_oracle_lag BTC
    tpu = token_price if direction=="UP" else round(max(0.01,1-token_price),4)
    tpd = token_price if direction=="DOWN" else round(max(0.01,1-token_price),4)
    market_end=market.get("end_date",""); sess=session_ctx(); conf_score={"score":0,"signals":[]}
    reasoning=f"ORACLE LAG {symbol} {direction} | gap={spot_oracle_gap:+.3f}% delta={oracle_delta:+.3f}% TA={ta_score} votes={dir_votes}/6 | tok={token_price:.3f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s"
    st.current_market=market  # ✅ place_bet route l'ordre réel via st.current_market — doit pointer le marché de l'asset
    ok=await place_bet(context,direction,amount,round(p_oracle,2),reasoning,conf_score,sess,tpu,tpd,market_end,source="snipe",asset=asset,market=market)
    if not ok: return
    _b = getattr(st, f"bet{_possfx(asset)}")
    if _b: _b["calib_bucket"] = calib_bucket  # ✅ #2 — mémorise le bucket pour MAJ à la résolution
    mode="💰 RÉEL" if not st.paper_mode else "📄 paper"
    # ✅ FIX: utilisait st.entry_token_price / st.bet (variables BTC, sans suffixe) au lieu des
    # versions PAR ASSET → affichait un prix/montant faux (souvent figé sur la dernière valeur BTC)
    # pour ETH/SOL/XRP. Le suffixe est déjà calculé juste au-dessus via _b.
    entry_tp=(getattr(st, f"entry_token_price{_possfx(asset)}") or token_price) if not st.paper_mode else token_price  # ✅ (21/06) fallback prix si entrée réelle=0
    # ✅ demande user 21/06: montant RÉELLEMENT placé (1ère tranche si entrée étagée), pas le montant
    # Kelly demandé — st.bet["amount"] est posé par place_bet() avec first_amount (réel envoyé à l'exchange).
    real_amount = (_b or {}).get("amount", amount)
    await send(context.bot,
        f"⚡ *ORACLE LAG {emoji} {symbol}* [{mode}]\n━━━━━━━━━━━━━━━\n"
        f"*{direction}* | Mise réelle:`{real_amount:.2f}$` | P:`{p_oracle*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Δslot:`{oracle_delta:+.3f}%` | Gap:`{spot_oracle_gap:+.3f}%` TA:`{ta_score}` | Votes:`{dir_votes}/6`\n"
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


async def job_resolve_passes(context):
    """v12.8 — Résout les passes théoriques pour BTC/ETH/SOL/XRP."""
    now = time.time()
    cur_prices = {
        "BTC": consensus_price() if consensus_price() > 0 else st.ws_price,
        "ETH": st.eth_price if st.eth_price > 0 else 0,
        "SOL": st.sol_price if st.sol_price > 0 else 0,
        "XRP": st.xrp_price if st.xrp_price > 0 else 0,
    }
    # Prix de référence des slots actuels (fallback si open_px manquant)
    slot_opens = {
        "BTC": st.oracle_slot_open if st.oracle_slot_open > 0 else st.oracle_price,
        "ETH": st.eth_oracle_slot_open if st.eth_oracle_slot_open > 0 else st.eth_oracle_price,
        "SOL": st.sol_oracle_slot_open if st.sol_oracle_slot_open > 0 else st.sol_oracle_price,
        "XRP": st.xrp_oracle_slot_open if st.xrp_oracle_slot_open > 0 else st.xrp_oracle_price,
    }

    for pr in st.pass_reasons:
        if pr.get("resolved") is not None: continue
        slot_end = pr.get("slot_end", 0)
        if slot_end <= 0 or now < slot_end + 5: continue
        direction = pr.get("dir")
        if direction not in ("UP","DOWN"): continue

        # Détecter l'asset depuis la raison
        reason = pr.get("reason","")
        asset = "BTC"
        for a in ("ETH","SOL","XRP"):
            if reason.startswith(f"{a}:") or f"[{a}]" in reason[:6]:
                asset = a; break

        cur_px = cur_prices.get(asset, 0)
        if cur_px <= 0: continue

        # Utiliser snap pour ref_px (slot open au moment du log)
        snap = pr.get("snap", {}).get(asset, (0, 0, 0))
        ref_px = snap[0] if snap[0]>0 else (snap[1] if snap[1]>0 else pr.get("open_px",0))
        if ref_px<=0: ref_px=snap[2] if len(snap)>2 and snap[2]>0 else 0  # fallback spot

        if ref_px<=0 or cur_px<=0:
            # Dernier fallback: oracle actuel (approximatif mais mieux que ⏳)
            ref_px={"BTC":st.oracle_price,"ETH":st.eth_oracle_price,"SOL":st.sol_oracle_price,"XRP":st.xrp_oracle_price}.get(asset,0)
        if ref_px<=0 or cur_px<=0 or abs(cur_px-ref_px)/max(ref_px,0.001)>0.10:
            pr["resolved"]="❓"; continue  # trop incertain
        won=(cur_px>ref_px)==(direction=="UP")
        pr["resolved"]="WIN" if won else "LOSS"

    # Résoudre oracle_patterns
    for pat in st.oracle_patterns:
        if pat.get("result") is not None: continue
        if pat.get("slot_end",0) <= 0 or now < pat["slot_end"] + 5: continue
        if pat.get("direction") not in ("UP","DOWN"): continue
        asset = pat.get("asset","BTC")
        cur_px = cur_prices.get(asset, 0)
        ref_px = pat.get("open_px", 0)
        if ref_px <= 0 or cur_px <= 0: continue
        won = (cur_px > ref_px) == (pat["direction"] == "UP")
        pat["result"] = "WIN" if won else "LOSS"


async def job_momentum_btc(context):
    """v12.9 — 2ème fenêtre BTC: momentum T-150s→T-60s.
    Source: 69.6% WR live (23 trades), wallet $42K profit (24W/5L)
    Signal: BTC move ≥0.30% en 60s + token 0.55-0.65$ + anti-reversal ret3s
    """
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now

    # Fenêtre T-150s→T-60s uniquement
    if not (60 <= slot_remaining <= 150): return
    if st.momentum_last_slot == cur_slot: return
    if cur_slot in (st.last_trade_slot, getattr(st,"meanrev_last_slot",0), getattr(st,"tds_last_slot",0)): return  # ✅ v12.9 verrou global
    if st.oracle_price <= 0 or st.ws_price <= 0: return

    # ── Calcul momentum 60s et 30s ──
    pts = list(st.ws_prices)
    if len(pts) < 5: return

    def ret_over(secs):
        cutoff = now - secs
        old = [p for t,p in pts if t <= cutoff]
        return (st.ws_price - old[-1]) / old[-1] * 100 if old and old[-1]>0 else 0.0

    ret_60s = ret_over(60)
    ret_30s = ret_over(30)
    ret_3s  = ret_over(3)

    # Signal: move ≥ 0.30% en 60s
    if abs(ret_60s) < 0.30: return

    direction = "UP" if ret_60s > 0 else "DOWN"

    # Filtre 1: ret30s dans même direction (momentum continu)
    if direction == "UP" and ret_30s < 0.05: return
    if direction == "DOWN" and ret_30s > -0.05: return

    # Filtre 2: anti-reversal ret3s dans même direction
    if direction == "UP" and ret_3s < -0.050: return
    if direction == "DOWN" and ret_3s > 0.050: return

    # ✅ v12.9 — Filtre tendance macro 10min (source: étude live Jung-Hua Liu mars 2026:
    # sans ce filtre, session réelle = -49.5% ROI avec 80% des trades UP pendant tendance DOWN;
    # avec filtre 10min ajouté = pertes réduites de 93%→13%, biais directionnel éliminé)
    # Appel API placé ICI (pas avant) pour ne pas spammer Binance à chaque tick de 2s —
    # seulement quand un signal momentum candidat est déjà détecté.
    try:
        klines_10m = await fetch_klines("1m", limit=10, symbol="btcusdt")
        if klines_10m and len(klines_10m) >= 5:
            trend_10m = (klines_10m[-1]["close"] - klines_10m[0]["open"]) / klines_10m[0]["open"] * 100
            if direction == "UP" and trend_10m <= -0.10:
                log_skip(f"BTC [MOM]: trend10m {trend_10m:+.3f}% contre UP (tendance macro contraire)", direction,
                         features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_trend_contra","asset":"BTC","source":"momentum"}); return
            if direction == "DOWN" and trend_10m >= 0.10:
                log_skip(f"BTC [MOM]: trend10m {trend_10m:+.3f}% contre DOWN (tendance macro contraire)", direction,
                         features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_trend_contra","asset":"BTC","source":"momentum"}); return
        else:
            trend_10m = 0.0  # fallback si klines indisponibles — ne bloque pas le trade
    except Exception:
        trend_10m = 0.0  # sécurité: si l'appel échoue, ne pas bloquer le momentum sur une panne réseau

    # ── Récupérer marché + token ──
    market = await poly.get_market_by_slug(f"btc-updown-5m-{cur_slot}")
    if not market: return
    token_used = market["token_up"] if direction=="UP" else market["token_down"]
    token_price = await poly.get_token_price(token_used)
    if not token_price or token_price <= 0: return

    # Token entre 0.55$ et 0.65$ max — momentum window spécifique
    MOMENTUM_TOKEN_MIN = 0.55
    MOMENTUM_TOKEN_MAX = 0.65
    if token_price > MOMENTUM_TOKEN_MAX:
        log_skip(f"BTC [MOM]: token {token_price:.2f}$>{MOMENTUM_TOKEN_MAX}$ (momentum déjà pricé)", direction,
                 features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_tokenmax","asset":"BTC","source":"momentum"}); return
    if token_price < MOMENTUM_TOKEN_MIN:
        log_skip(f"BTC [MOM]: token {token_price:.2f}$<{MOMENTUM_TOKEN_MIN}$ (signal trop faible)", direction,
                 features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_tokenmin","asset":"BTC","source":"momentum"}); return

    # ── EV ──
    fee = taker_fee_per_share(token_price)
    p_mom = min(0.90, 0.65 + abs(ret_60s) * 0.5)  # prob estimée
    ev = p_mom - token_price - fee
    if ev < ORACLE_EDGE_MIN:
        log_skip(f"BTC [MOM]: EV {ev*100:+.1f}%<{ORACLE_EDGE_MIN*100:.0f}%", direction,
                 features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_ev","asset":"BTC","source":"momentum"}); return

    # ── Kelly + bet ──
    payout = round(1/token_price, 2)
    amount = kelly_bet_secondary(st.bankroll, p_mom, payout)  # v12.9 — unifié 1-3% (était kelly_bet partagée 5-15%)
    if amount < MIN_BET_USD: return

    log.info(f"⚡ MOMENTUM BTC {direction} | ret60s={ret_60s:+.3f}% ret30s={ret_30s:+.3f}% tok={token_price:.2f}$ EV={ev*100:.1f}%")

    # ✅ tpu/tpd = PRIX (float), pas token_id (string) — cf. fix job_oracle_lag BTC
    tpu = token_price if direction=="UP" else round(max(0.01,1-token_price),4)
    tpd = token_price if direction=="DOWN" else round(max(0.01,1-token_price),4)
    market_end = market.get("end_date","")
    sess = session_ctx()
    conf_score = {"score":0,"signals":[]}
    reasoning = f"⚡MOMENTUM BTC {direction} | ret60s={ret_60s:+.3f}% ret30s={ret_30s:+.3f}% ret3s={ret_3s:+.3f}% trend10m={trend_10m:+.3f}% | tok={token_price:.2f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s"

    st.current_market = market  # ✅ place_bet route l'ordre réel via st.current_market
    ok = await place_bet(context, direction, amount, round(p_mom,2), reasoning, conf_score, sess, tpu, tpd, market_end, source="momentum", asset="BTC", market=market)
    if not ok: return

    st.momentum_last_slot = cur_slot
    mode = "💰 RÉEL" if not st.paper_mode else "📄 paper"
    await send(context.bot,
        f"🚀 *MOMENTUM BTC* [{mode}]\n━━━━━━━━━━━━━━\n"
        f"*{direction}* | `{amount:.2f}$` | P:`{p_mom*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Ret 60s:`{ret_60s:+.3f}%` | 30s:`{ret_30s:+.3f}%` | 3s:`{ret_3s:+.3f}%`\n"
        f"Trend 10m:`{trend_10m:+.3f}%` (filtre macro)\n"
        f"Token:`{token_price:.2f}$` | EV:`{ev*100:+.1f}%`\n\n"
        f"📝 _2ème fenêtre momentum — entrée tôt sur fort move_")


def _asset_state_attrs(asset):
    """v12.9 — Mappe un asset vers ses noms d'attributs st.* (momentum/meanrev/confluence multi-asset).
    BTC garde ses attributs historiques sans préfixe; ETH/SOL/XRP utilisent le préfixe existant."""
    a = asset.upper()
    if a == "BTC":
        return dict(price="ws_price", prices="ws_prices", oracle="oracle_price",
                     slug="btc-updown-5m", mom_slot="momentum_last_slot", mr_slot="meanrev_last_slot",
                     tds_slot="tds_last_slot", oracle_slot="last_trade_slot")
    pfx = a.lower()
    return dict(price=f"{pfx}_price", prices=f"{pfx}_ws_prices", oracle=f"{pfx}_oracle_price",
                 slug=f"{pfx}-updown-5m", mom_slot=f"momentum_last_slot_{pfx}", mr_slot=f"meanrev_last_slot_{pfx}",
                 tds_slot=f"tds_last_slot_{pfx}", oracle_slot=f"{pfx}_last_trade_slot")


async def job_momentum_asset(context, asset):
    """v12.9 — Momentum généralisé ETH/SOL/XRP (même logique que job_momentum_btc).
    ⚠️ AJOUT PUR — job_momentum_btc reste la fonction dédiée BTC, totalement inchangée à part le sizing.
    Sizing Kelly dédié 1-3% BR (kelly_bet_secondary), demande user 17/06.
    """
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now
    if not (60 <= slot_remaining <= 150): return

    cfg = _asset_state_attrs(asset)
    # ✅ (22/06) fix: momentum ne vérifiait que lui-même + meanrev, jamais oracle_lag ni confluence →
    # doublons possibles si une autre stratégie avait déjà tradé cet asset/slot (vu: 2 achats XRP même
    # slot à 57¢/58.7¢). Vérifie maintenant les 4 stratégies.
    if (getattr(st, cfg["mom_slot"]) == cur_slot or getattr(st, cfg["mr_slot"]) == cur_slot
            or getattr(st, cfg["tds_slot"]) == cur_slot or getattr(st, cfg["oracle_slot"]) == cur_slot):
        return

    cur_price = getattr(st, cfg["price"])
    oracle_price = getattr(st, cfg["oracle"])
    if oracle_price <= 0 or cur_price <= 0: return

    pts = list(getattr(st, cfg["prices"]))
    if len(pts) < 5: return

    def ret_over(secs):
        cutoff = now - secs
        old = [p for t,p in pts if t <= cutoff]
        return (cur_price - old[-1]) / old[-1] * 100 if old and old[-1]>0 else 0.0

    ret_60s = ret_over(60); ret_30s = ret_over(30); ret_3s = ret_over(3)
    if abs(ret_60s) < 0.30: return
    direction = "UP" if ret_60s > 0 else "DOWN"
    if direction == "UP" and ret_30s < 0.05: return
    if direction == "DOWN" and ret_30s > -0.05: return
    if direction == "UP" and ret_3s < -0.050: return
    if direction == "DOWN" and ret_3s > 0.050: return

    # Filtre tendance macro 10min (même logique que BTC, symbole Binance adapté à l'asset)
    try:
        klines_10m = await fetch_klines("1m", limit=10, symbol=f"{asset.lower()}usdt")
        if klines_10m and len(klines_10m) >= 5:
            trend_10m = (klines_10m[-1]["close"] - klines_10m[0]["open"]) / klines_10m[0]["open"] * 100
            if direction == "UP" and trend_10m <= -0.10:
                log_skip(f"{asset} [MOM]: trend10m {trend_10m:+.3f}% contre UP", direction,
                         features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_trend_contra","asset":asset,"source":"momentum"}); return
            if direction == "DOWN" and trend_10m >= 0.10:
                log_skip(f"{asset} [MOM]: trend10m {trend_10m:+.3f}% contre DOWN", direction,
                         features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_trend_contra","asset":asset,"source":"momentum"}); return
        else:
            trend_10m = 0.0
    except Exception:
        trend_10m = 0.0

    market = await poly.get_market_by_slug(f"{cfg['slug']}-{cur_slot}")
    if not market: return
    token_used = market["token_up"] if direction=="UP" else market["token_down"]
    token_price = await poly.get_token_price(token_used)
    if not token_price or token_price <= 0: return

    MOMENTUM_TOKEN_MIN = 0.55
    MOMENTUM_TOKEN_MAX = 0.65
    if token_price > MOMENTUM_TOKEN_MAX:
        log_skip(f"{asset} [MOM]: token {token_price:.2f}$>{MOMENTUM_TOKEN_MAX}$ (momentum déjà pricé)", direction,
                 features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_tokenmax","asset":asset,"source":"momentum"}); return
    if token_price < MOMENTUM_TOKEN_MIN:
        log_skip(f"{asset} [MOM]: token {token_price:.2f}$<{MOMENTUM_TOKEN_MIN}$ (signal trop faible)", direction,
                 features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_tokenmin","asset":asset,"source":"momentum"}); return

    fee = taker_fee_per_share(token_price)
    p_mom = min(0.90, 0.65 + abs(ret_60s) * 0.5)
    ev = p_mom - token_price - fee
    if ev < ORACLE_EDGE_MIN:
        log_skip(f"{asset} [MOM]: EV {ev*100:+.1f}%<{ORACLE_EDGE_MIN*100:.0f}%", direction,
                 features={"gap":0,"delta":ret_60s,"ret3s":ret_3s,"votes":0,"filter":"mom_ev","asset":asset,"source":"momentum"}); return

    payout = round(1/token_price, 2)
    amount = kelly_bet_secondary(st.bankroll, p_mom, payout)
    if amount < MIN_BET_USD: return

    log.info(f"⚡ MOMENTUM {asset} {direction} | ret60s={ret_60s:+.3f}% ret30s={ret_30s:+.3f}% tok={token_price:.2f}$ EV={ev*100:.1f}%")

    # ✅ tpu/tpd = PRIX (float), pas token_id (string) — cf. fix job_oracle_lag BTC
    tpu = token_price if direction=="UP" else round(max(0.01,1-token_price),4)
    tpd = token_price if direction=="DOWN" else round(max(0.01,1-token_price),4)
    market_end = market.get("end_date","")
    sess = session_ctx()
    conf_score = {"score":0,"signals":[]}
    reasoning = (f"⚡MOMENTUM {asset} {direction} | ret60s={ret_60s:+.3f}% ret30s={ret_30s:+.3f}% ret3s={ret_3s:+.3f}% "
                 f"trend10m={trend_10m:+.3f}% | tok={token_price:.2f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s")

    st.current_market = market  # ✅ place_bet route l'ordre réel via st.current_market — doit pointer le marché de l'asset
    ok = await place_bet(context, direction, amount, round(p_mom,2), reasoning, conf_score, sess, tpu, tpd, market_end, source="momentum", asset=asset, market=market)
    if not ok: return

    setattr(st, cfg["mom_slot"], cur_slot)
    mode = "💰 RÉEL" if not st.paper_mode else "📄 paper"
    await send(context.bot,
        f"🚀 *MOMENTUM {asset}* [{mode}]\n━━━━━━━━━━━━━━\n"
        f"*{direction}* | `{amount:.2f}$` | P:`{p_mom*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Ret 60s:`{ret_60s:+.3f}%` | 30s:`{ret_30s:+.3f}%` | 3s:`{ret_3s:+.3f}%`\n"
        f"Trend 10m:`{trend_10m:+.3f}%` (filtre macro)\n"
        f"Token:`{token_price:.2f}$` | EV:`{ev*100:+.1f}%`\n\n"
        f"📝 _Momentum {asset} — entrée tôt sur fort move_")


async def job_momentum_eth(context):
    await job_momentum_asset(context, "ETH")

async def job_momentum_sol(context):
    await job_momentum_asset(context, "SOL")

async def job_momentum_xrp(context):
    await job_momentum_asset(context, "XRP")


async def job_mean_reversion_btc(context):
    """v12.9 — BTC Mean-Reversion: parie CONTRE les spikes en régime squeeze (faible volatilité).
    Source: PolyPredictor (Bollinger Bandwidth squeeze/expansion régime-adaptatif),
    QuantPedia (alpha mean-reversion confirmé avec exécution limit/maker — cohérent avec notre
    place_order qui tente déjà un ordre maker en premier), architecture validée par bot live
    profitable séparant régimes "continuation" et "exhaustion+dislocation" (dev.to/fatherson).
    ⚠️ AJOUT PUR — ne touche ni à l'oracle lag, ni au momentum existant.
    Coordination anti-double-trade: partage st.momentum_last_slot avec job_momentum_btc
    (les 2 stratégies occupent la même fenêtre T-150s→T-60s, régimes complémentaires).
    Sizing Kelly dédié 1-3% BR (kelly_bet_secondary) — volontairement prudent, stratégie non
    encore validée en réel.
    """
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now

    # Même fenêtre que momentum (régimes complémentaires: squeeze ici, expansion pour momentum)
    if not (60 <= slot_remaining <= 150): return
    # Anti-double-trade: vérifie les 4 guards (momentum/meanrev/oracle_lag/confluence ont pu déjà trader)
    # ✅ (22/06) fix: manquait last_trade_slot (oracle_lag) et tds_last_slot (confluence) — même trou
    # que sur ETH/SOL/XRP, corrigé en même temps.
    if st.momentum_last_slot == cur_slot or st.meanrev_last_slot == cur_slot \
            or st.last_trade_slot == cur_slot or getattr(st,"tds_last_slot",0) == cur_slot: return
    if cur_slot in (st.last_trade_slot, getattr(st,"tds_last_slot",0)): return  # ✅ v12.9 verrou global
    if st.oracle_price <= 0 or st.ws_price <= 0: return

    pts = list(st.ws_prices)
    if len(pts) < 20: return  # pas assez de points pour un calcul Bollinger fiable

    # ── Bollinger Bandwidth sur les derniers 60s (détection régime squeeze/expansion) ──
    window_pts = [p for t,p in pts if now-t <= 60]
    if len(window_pts) < 10: return
    sma = sum(window_pts) / len(window_pts)
    if sma <= 0: return
    variance = sum((p-sma)**2 for p in window_pts) / len(window_pts)
    std = variance ** 0.5
    upper = sma + 2*std
    lower = sma - 2*std
    bandwidth = (upper - lower) / sma * 100

    # ✅ Seuil squeeze — point de départ raisonné, À CALIBRER avec données réelles (comme tous nos autres seuils)
    SQUEEZE_MAX_BANDWIDTH = 0.12
    if bandwidth > SQUEEZE_MAX_BANDWIDTH:
        st.meanrev_regime_expansion_count += 1  # v12.9 — résumé agrégé /learn (pas de log individuel, évite spam)
        return  # régime expansion/tendance → laisser momentum gérer ce cas, pas de mean-reversion ici
    st.meanrev_regime_squeeze_count += 1

    # ── Détection du spike (prix actuel hors bandes de Bollinger) ──
    cur_price = st.ws_price
    if cur_price >= upper:
        direction = "DOWN"  # surextension haussière → parier sur le retour à la moyenne
        overext = (cur_price - upper) / sma * 100
    elif cur_price <= lower:
        direction = "UP"  # surextension baissière → parier sur le retour à la moyenne
        overext = (lower - cur_price) / sma * 100
    else:
        return  # pas de spike actuellement, rien à faire

    # ── Anti-fakeout: si le mouvement accélère ENCORE dans le sens du spike, trop tôt pour la reversion ──
    def ret_over(secs):
        cutoff = now - secs
        old = [p for t,p in pts if t <= cutoff]
        return (cur_price - old[-1]) / old[-1] * 100 if old and old[-1]>0 else 0.0
    ret_10s = ret_over(10)
    ret_3s = ret_over(3)
    if direction == "DOWN" and ret_3s > 0 and abs(ret_3s) > abs(ret_10s)*0.5:
        log_skip(f"BTC [MEANREV]: spike haussier encore en accélération (ret3s={ret_3s:+.3f}%) — trop tôt pour reversion", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_fakeout","asset":"BTC","source":"meanrev"})
        return
    if direction == "UP" and ret_3s < 0 and abs(ret_3s) > abs(ret_10s)*0.5:
        log_skip(f"BTC [MEANREV]: spike baissier encore en accélération (ret3s={ret_3s:+.3f}%) — trop tôt pour reversion", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_fakeout","asset":"BTC","source":"meanrev"})
        return

    # ── Marché + token ──
    market = await poly.get_market_by_slug(f"btc-updown-5m-{cur_slot}")
    if not market: return
    token_used = market["token_up"] if direction=="UP" else market["token_down"]
    token_price = await poly.get_token_price(token_used)
    if not token_price or token_price <= 0: return

    MEANREV_TOKEN_MIN = 0.51
    MEANREV_TOKEN_MAX = 0.70
    if token_price > MEANREV_TOKEN_MAX:
        log_skip(f"BTC [MEANREV]: token {token_price:.2f}$>{MEANREV_TOKEN_MAX}$ (spike déjà pricé)", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_tokenmax","asset":"BTC","source":"meanrev"}); return
    if token_price < MEANREV_TOKEN_MIN:
        log_skip(f"BTC [MEANREV]: token {token_price:.2f}$<{MEANREV_TOKEN_MIN}$ (signal trop faible)", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_tokenmin","asset":"BTC","source":"meanrev"}); return

    # ── EV ──
    fee = taker_fee_per_share(token_price)
    p_rev = min(0.85, 0.55 + overext * 5)  # plus la surextension est grande, plus la proba de retour est haute (heuristique de départ)
    ev = p_rev - token_price - fee
    if ev < ORACLE_EDGE_MIN:
        log_skip(f"BTC [MEANREV]: EV {ev*100:+.1f}%<{ORACLE_EDGE_MIN*100:.0f}%", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_ev","asset":"BTC","source":"meanrev"}); return

    # ── Kelly dédié 1-4% BR (PAS kelly_bet partagée) ──
    payout = round(1/token_price, 2)
    amount = kelly_bet_secondary(st.bankroll, p_rev, payout)
    if amount < MIN_BET_USD: return

    log.info(f"🔄 MEAN-REV BTC {direction} | bandwidth={bandwidth:.3f}% overext={overext:.3f}% tok={token_price:.2f}$ EV={ev*100:.1f}%")

    # ✅ tpu/tpd = PRIX (float), pas token_id (string) — cf. fix job_oracle_lag BTC
    tpu = token_price if direction=="UP" else round(max(0.01,1-token_price),4)
    tpd = token_price if direction=="DOWN" else round(max(0.01,1-token_price),4)
    market_end = market.get("end_date","")
    sess = session_ctx()
    conf_score = {"score":0,"signals":[]}
    reasoning = (f"🔄MEAN-REV BTC {direction} | bandwidth={bandwidth:+.3f}% overext={overext:+.3f}% "
                 f"ret3s={ret_3s:+.3f}% | tok={token_price:.2f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s")

    st.current_market = market  # ✅ place_bet route l'ordre réel via st.current_market
    ok = await place_bet(context, direction, amount, round(p_rev,2), reasoning, conf_score, sess, tpu, tpd, market_end, source="meanrev", asset="BTC", market=market)
    if not ok: return

    st.meanrev_last_slot = cur_slot
    st.momentum_last_slot = cur_slot  # coordination anti-double-trade avec job_momentum_btc
    mode = "💰 RÉEL" if not st.paper_mode else "📄 paper"
    await send(context.bot,
        f"🔄 *MEAN-REVERSION BTC* [{mode}]\n━━━━━━━━━━━━━━\n"
        f"*{direction}* | `{amount:.2f}$` | P:`{p_rev*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Bollinger BW:`{bandwidth:.3f}%` (squeeze) | Overext:`{overext:+.3f}%`\n"
        f"Ret 10s:`{ret_10s:+.3f}%` | 3s:`{ret_3s:+.3f}%`\n"
        f"Token:`{token_price:.2f}$` | EV:`{ev*100:+.1f}%`\n\n"
        f"📝 _3ème fenêtre — parie contre un spike en régime calme_")


async def job_mean_reversion_asset(context, asset):
    """v12.9 — Mean-reversion généralisé ETH/SOL/XRP (même logique que job_mean_reversion_btc).
    ⚠️ AJOUT PUR — job_mean_reversion_btc reste la fonction dédiée BTC, totalement inchangée à part le sizing.
    Sizing Kelly dédié 1-3% BR (kelly_bet_secondary), demande user 17/06.
    """
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now
    if not (60 <= slot_remaining <= 150): return

    cfg = _asset_state_attrs(asset)
    # ✅ (22/06) fix: même trou que momentum — ne vérifiait que lui-même + momentum, jamais oracle_lag
    # ni confluence. Vérifie maintenant les 4 stratégies.
    if (getattr(st, cfg["mom_slot"]) == cur_slot or getattr(st, cfg["mr_slot"]) == cur_slot
            or getattr(st, cfg["tds_slot"]) == cur_slot or getattr(st, cfg["oracle_slot"]) == cur_slot):
        return

    cur_price = getattr(st, cfg["price"])
    oracle_price = getattr(st, cfg["oracle"])
    if oracle_price <= 0 or cur_price <= 0: return

    pts = list(getattr(st, cfg["prices"]))
    if len(pts) < 20: return

    window_pts = [p for t,p in pts if now-t <= 60]
    if len(window_pts) < 10: return
    sma = sum(window_pts) / len(window_pts)
    if sma <= 0: return
    variance = sum((p-sma)**2 for p in window_pts) / len(window_pts)
    std = variance ** 0.5
    upper = sma + 2*std
    lower = sma - 2*std
    bandwidth = (upper - lower) / sma * 100

    SQUEEZE_MAX_BANDWIDTH = 0.12
    if bandwidth > SQUEEZE_MAX_BANDWIDTH:
        st.meanrev_regime_expansion_count += 1  # v12.9 — résumé agrégé /learn
        return
    st.meanrev_regime_squeeze_count += 1

    if cur_price >= upper:
        direction = "DOWN"; overext = (cur_price - upper) / sma * 100
    elif cur_price <= lower:
        direction = "UP"; overext = (lower - cur_price) / sma * 100
    else:
        return

    def ret_over(secs):
        cutoff = now - secs
        old = [p for t,p in pts if t <= cutoff]
        return (cur_price - old[-1]) / old[-1] * 100 if old and old[-1]>0 else 0.0
    ret_10s = ret_over(10); ret_3s = ret_over(3)
    if direction == "DOWN" and ret_3s > 0 and abs(ret_3s) > abs(ret_10s)*0.5:
        log_skip(f"{asset} [MEANREV]: spike haussier encore en accélération (ret3s={ret_3s:+.3f}%) — trop tôt pour reversion", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_fakeout","asset":asset,"source":"meanrev"})
        return
    if direction == "UP" and ret_3s < 0 and abs(ret_3s) > abs(ret_10s)*0.5:
        log_skip(f"{asset} [MEANREV]: spike baissier encore en accélération (ret3s={ret_3s:+.3f}%) — trop tôt pour reversion", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_fakeout","asset":asset,"source":"meanrev"})
        return

    market = await poly.get_market_by_slug(f"{cfg['slug']}-{cur_slot}")
    if not market: return
    token_used = market["token_up"] if direction=="UP" else market["token_down"]
    token_price = await poly.get_token_price(token_used)
    if not token_price or token_price <= 0: return

    MEANREV_TOKEN_MIN = 0.51
    MEANREV_TOKEN_MAX = 0.70
    if token_price > MEANREV_TOKEN_MAX:
        log_skip(f"{asset} [MEANREV]: token {token_price:.2f}$>{MEANREV_TOKEN_MAX}$ (spike déjà pricé)", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_tokenmax","asset":asset,"source":"meanrev"}); return
    if token_price < MEANREV_TOKEN_MIN:
        log_skip(f"{asset} [MEANREV]: token {token_price:.2f}$<{MEANREV_TOKEN_MIN}$ (signal trop faible)", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_tokenmin","asset":asset,"source":"meanrev"}); return

    fee = taker_fee_per_share(token_price)
    p_rev = min(0.85, 0.55 + overext * 5)
    ev = p_rev - token_price - fee
    if ev < ORACLE_EDGE_MIN:
        log_skip(f"{asset} [MEANREV]: EV {ev*100:+.1f}%<{ORACLE_EDGE_MIN*100:.0f}%", direction,
                 features={"gap":0,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"mr_ev","asset":asset,"source":"meanrev"}); return

    payout = round(1/token_price, 2)
    amount = kelly_bet_secondary(st.bankroll, p_rev, payout)
    if amount < MIN_BET_USD: return

    log.info(f"🔄 MEAN-REV {asset} {direction} | bandwidth={bandwidth:.3f}% overext={overext:.3f}% tok={token_price:.2f}$ EV={ev*100:.1f}%")

    # ✅ tpu/tpd = PRIX (float), pas token_id (string) — cf. fix job_oracle_lag BTC
    tpu = token_price if direction=="UP" else round(max(0.01,1-token_price),4)
    tpd = token_price if direction=="DOWN" else round(max(0.01,1-token_price),4)
    market_end = market.get("end_date","")
    sess = session_ctx()
    conf_score = {"score":0,"signals":[]}
    reasoning = (f"🔄MEAN-REV {asset} {direction} | bandwidth={bandwidth:+.3f}% overext={overext:+.3f}% "
                 f"ret3s={ret_3s:+.3f}% | tok={token_price:.2f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s")

    st.current_market = market  # ✅ place_bet route l'ordre réel via st.current_market — doit pointer le marché de l'asset
    ok = await place_bet(context, direction, amount, round(p_rev,2), reasoning, conf_score, sess, tpu, tpd, market_end, source="meanrev", asset=asset, market=market)
    if not ok: return

    setattr(st, cfg["mr_slot"], cur_slot)
    setattr(st, cfg["mom_slot"], cur_slot)  # coordination anti-double-trade
    mode = "💰 RÉEL" if not st.paper_mode else "📄 paper"
    await send(context.bot,
        f"🔄 *MEAN-REVERSION {asset}* [{mode}]\n━━━━━━━━━━━━━━\n"
        f"*{direction}* | `{amount:.2f}$` | P:`{p_rev*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Bollinger BW:`{bandwidth:.3f}%` (squeeze) | Overext:`{overext:+.3f}%`\n"
        f"Ret 10s:`{ret_10s:+.3f}%` | 3s:`{ret_3s:+.3f}%`\n"
        f"Token:`{token_price:.2f}$` | EV:`{ev*100:+.1f}%`\n\n"
        f"📝 _Mean-reversion {asset} — parie contre un spike en régime calme_")


async def job_mean_reversion_eth(context):
    await job_mean_reversion_asset(context, "ETH")

async def job_mean_reversion_sol(context):
    await job_mean_reversion_asset(context, "SOL")

async def job_mean_reversion_xrp(context):
    await job_mean_reversion_asset(context, "XRP")


def _tds_adaptive_weight(setup_type):
    """v12.9 — Poids adaptatif MR/momentum pour la confluence, basé sur l'historique RÉEL des trades confluence.
    Reste neutre (1.0) tant qu'il n'y a pas ≥TDS_ADAPT_MIN_SAMPLE trades pour cette branche —
    évite l'ajustement sur un échantillon trop petit (risque réel signalé: 0 trade réel après 5 jours)."""
    tag = f"confluence-{setup_type}"
    relevant = [t for t in st.trades if t.get("source")=="confluence" and tag in t.get("reasoning","")]
    if len(relevant) < TDS_ADAPT_MIN_SAMPLE:
        return 1.0
    wins = sum(1 for t in relevant if t.get("result")=="WIN")
    wr = wins / len(relevant)
    return min(1.5, max(0.5, wr / 0.5))


async def job_ob_signal_asset(context, asset):
    """✅ v12.9 (18/06) — STRATÉGIE OB SIGNAL: trade dans le sens du carnet quand l'imbalance est nette.
    Basée sur les données du slot recorder (OB acheteur→73% UP, OB vendeur→88% DOWN sur marché neutre, n>150).
    Fenêtre T-150s→T-30s. Mise minimale. Respecte le verrou slot (1 trade/slot/asset toutes stratégies confondues).
    ⚠️ NON validé en exécution réelle — le 73% est mesuré à la résolution (look-ahead possible). Surveillance étroite."""
    if not OB_SIGNAL_ENABLED or not st.running or st.killed: return
    # ✅ (21/06) demande user: OB signal DÉSACTIVÉ en réel (perf non validée en exécution réelle).
    # Reste actif en paper pour continuer à mesurer la stratégie sans risquer de capital.
    if not st.paper_mode: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now
    if not (OB_SIGNAL_WIN_END <= slot_remaining <= OB_SIGNAL_WIN_START): return

    # ✅ v12.9 (19/06) — Verrou simplifié: SEUL ob_last_slot reste (anti-doublon: pas 2 trades OB sur le même slot).
    # Les verrous mom/mr/tds/oracle ont été RETIRÉS car ils étaient marqués "en coordination" SANS trade réel
    # (ex: job_mean_reversion_btc fait st.momentum_last_slot=cur_slot dès le régime squeeze, à T-150s, avant
    # même que l'OB n'entre dans sa fenêtre T-90s → BTC OB sortait ici en silence sans jamais trader).
    # Le multi-stratégie sur un même slot est accepté (option A user). ob_last_slot garde le contrôle anti-doublon.
    cfg = _asset_state_attrs(asset)
    if st.ob_last_slot.get(asset) == cur_slot: return

    # ✅ v12.9 — OB sur BTC/SOL/ETH: récupérer le marché et s'assurer que le WS carnet de l'asset tourne
    # (sinon l'imbalance reste périmé/à 0). XRP exclu (pas de WS carnet supporté).
    if asset == "XRP": return
    try:
        market = await poly.get_market_by_slug(f"{cfg['slug']}-{cur_slot}")
        if not market: return
        asset_up_ob = market.get("token_up","")
    except Exception as ex:
        log.debug(f"OB signal {asset} market: {ex}"); return

    # Lancer/rafraîchir le WS carnet pour l'asset si pas actif sur ce token OU si le carnet est périmé
    if asset == "BTC":
        stale = (now - getattr(st,"ob_ts",0)) > 20
        if asset_up_ob and (st.ob_asset_id != asset_up_ob or stale):
            if hasattr(st,"clob_ws_task") and st.clob_ws_task and not st.clob_ws_task.done(): st.clob_ws_task.cancel()
            st.clob_ws_task = asyncio.create_task(ws_clob_loop(asset_up_ob))
    elif asset == "ETH":
        stale = (now - getattr(st,"eth_ob_ts",0)) > 20
        if asset_up_ob and (st.eth_ob_asset_id != asset_up_ob or stale):
            if st.eth_clob_ws_task and not st.eth_clob_ws_task.done(): st.eth_clob_ws_task.cancel()
            st.eth_clob_ws_task = asyncio.create_task(ws_clob_loop_asset(asset_up_ob,"ETH"))
    elif asset == "SOL":
        stale = (now - getattr(st,"sol_ob_ts",0)) > 20
        if asset_up_ob and (st.sol_ob_asset_id != asset_up_ob or stale):
            if st.sol_clob_ws_task and not st.sol_clob_ws_task.done(): st.sol_clob_ws_task.cancel()
            st.sol_clob_ws_task = asyncio.create_task(ws_clob_loop_asset(asset_up_ob,"SOL"))

    # Lire l'OB imbalance + vérifier sa fraîcheur (< 30s)
    ob_data = {"BTC": (getattr(st,"ob_imbalance",0), getattr(st,"ob_ts",0)),
               "ETH": (getattr(st,"eth_ob_imbalance",0), getattr(st,"eth_ob_ts",0)),
               "SOL": (getattr(st,"sol_ob_imbalance",0), getattr(st,"sol_ob_ts",0))}
    ob, ob_ts = ob_data.get(asset, (0,0))
    if now - ob_ts > 30: return  # carnet périmé, on attend des données fraîches
    if abs(ob) < OB_SIGNAL_THRESHOLD: return  # imbalance pas assez nette

    direction = "UP" if ob > 0 else "DOWN"
    # ✅ Confirmation croisée: l'OB ne trade QUE si l'oracle lag pointe le MÊME sens (OB UP ⇒ oracle UP).
    odir = oracle_direction(asset)
    if odir != direction:
        log_skip(f"{asset} [OB]: OB={direction} mais oracle={odir or 'neutre'} — pas d'accord, skip", direction,
                 features={"ob":ob,"filter":"ob_oracle_disagree","asset":asset,"oracle_dir":odir or "none","source":"ob_signal"}); return
    try:
        token_id = market["token_up"] if direction=="UP" else market["token_down"]
        token_price = await poly.get_token_price(token_id)
    except Exception as ex:
        log.debug(f"OB signal {asset} token: {ex}"); return

    if token_price < OB_SIGNAL_TOKEN_MIN or token_price > OB_SIGNAL_TOKEN_MAX:
        log_skip(f"{asset} [OB]: token {token_price:.2f}$ hors plage {OB_SIGNAL_TOKEN_MIN}-{OB_SIGNAL_TOKEN_MAX}$", direction,
                 features={"ob":ob,"filter":"ob_token","asset":asset,"token":token_price,"source":"ob_signal"}); return

    # Proba estimée: basée sur la force de l'imbalance (calibré sur les 73%/88% observés, capé prudemment)
    p_conf = min(0.72, 0.55 + abs(ob) * 0.30)
    payout = round(1/token_price, 2) if token_price > 0 else 2.0
    fee = taker_fee_per_share(token_price)
    # ✅ #6 — EV par $ staké: 1$ achète 1/token_price shares, donc le frais par-share doit être
    # ramené par /token_price (sinon les frais étaient sous-comptés sur cette stratégie uniquement).
    fee_per_dollar = fee / token_price if token_price > 0 else fee
    ev = p_conf * (payout - 1) - (1 - p_conf) - fee_per_dollar
    if ev < OB_SIGNAL_EV_MIN:
        log_skip(f"{asset} [OB]: EV {ev*100:+.1f}%<{OB_SIGNAL_EV_MIN*100:.0f}% (OB={ob:+.2f})", direction,
                 features={"ob":ob,"filter":"ob_ev","asset":asset,"token":token_price,"ev":ev,"source":"ob_signal"}); return

    amount = kelly_bet(st.bankroll, p_conf, payout, token_price)
    if amount < MIN_BET_USD: return

    sess = session_ctx()  # ✅ place_bet attend le dict complet (fait sess["session"]) — pas la string
    reasoning = f"📖 OB SIGNAL {asset} {direction} | imbalance={ob:+.2f} tok={token_price:.2f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s"
    st.current_market = market
    ok = await place_bet(context, direction, amount, p_conf, reasoning, {"score":0,"signals":[]}, sess,
                         token_price if direction=="UP" else 1-token_price,
                         token_price if direction=="DOWN" else 1-token_price,
                         cur_slot+300, source="ob_signal", asset=asset, market=market)
    if ok:
        st.ob_last_slot[asset] = cur_slot
        log.info(f"📖 OB SIGNAL TRADE {asset} {direction} {amount:.2f}$ (OB={ob:+.2f})")
        await send(context.bot, f"📖 *OB SIGNAL* {asset} `{direction}` `{amount:.2f}$` @`{token_price:.2f}$` | imbalance=`{ob:+.2f}` EV=`{ev*100:+.1f}%`")


async def job_ob_signal_btc(context):  await job_ob_signal_asset(context, "BTC")
async def job_ob_signal_eth(context):  await job_ob_signal_asset(context, "ETH")
async def job_ob_signal_sol(context):  await job_ob_signal_asset(context, "SOL")
async def job_ob_signal_xrp(context):  await job_ob_signal_asset(context, "XRP")


async def job_ob_oracle_disagree(context):
    """✅ (21/06) demande user — Stratégie RÉELLE BTC uniquement: ob_oracle_disagree.
    Trade quand le CARNET (OB imbalance) et l'ORACLE divergent → on suit le carnet.
    OB acheteurs (imbalance>0) → UP, OB vendeurs (<0) → DOWN. Token 0.41-0.75$, mise 2% BR.
    1 bet BTC/slot (partagé avec les autres stratégies BTC via asset_trade_slot)."""
    if not OB_DISAGREE_ENABLED or not st.running or st.killed: return
    if st.paper_mode: return  # réel uniquement (demande user)
    now = time.time()
    cur_slot = int(now//300)*300
    slot_remaining = cur_slot+300-now
    if slot_remaining > 120 or slot_remaining < 30: return  # ✅ (21/06) demande user: fenêtre T-120→T-30s
    if st.bet is not None or st.asset_trade_slot.get("BTC") == cur_slot: return  # slot BTC déjà pris
    # Carnet frais ?
    if time.time() - getattr(st, "ob_ts", 0) > 10:
        log_skip("OBdis BTC: carnet périmé", None); return
    ob = st.ob_imbalance
    if abs(ob) < OB_DISAGREE_THRESHOLD: return  # carnet pas assez déséquilibré
    # Direction oracle
    if st.oracle_slot_open <= 0 or st.oracle_price <= 0: return
    oracle_delta = (st.oracle_price - st.oracle_slot_open)/st.oracle_slot_open*100
    ob_dir = "UP" if ob > 0 else "DOWN"
    oracle_dir = "UP" if oracle_delta > 0 else ("DOWN" if oracle_delta < 0 else None)
    # SIGNAL: il faut un DÉSACCORD carnet ↔ oracle
    if oracle_dir is None or ob_dir == oracle_dir:
        log_skip(f"OBdis BTC: pas de désaccord (OB {ob_dir} / oracle {oracle_dir})", ob_dir,
                 features={"ob":ob,"delta":oracle_delta,"filter":"no_disagree","asset":"BTC"}); return
    direction = ob_dir  # on suit le carnet
    market = await poly.find_btc_5min_market()
    if not market: return
    token_used = market["token_up"] if direction=="UP" else market["token_down"]
    token_price = await poly.get_token_price(token_used)
    if not token_price or token_price <= 0: return
    if token_price < OB_DISAGREE_TOKEN_MIN or token_price > OB_DISAGREE_TOKEN_MAX:
        log_skip(f"OBdis BTC: token {token_price:.2f}$ hors 0.41-0.75$", direction,
                 features={"ob":ob,"delta":oracle_delta,"token":token_price,"filter":"token_range","asset":"BTC"}); return
    amount = max(MIN_BET_USD, round(st.bankroll * OB_DISAGREE_PCT, 2))  # 2% BR
    tpu = token_price if direction=="UP" else round(max(0.01,1-token_price),4)
    tpd = token_price if direction=="DOWN" else round(max(0.01,1-token_price),4)
    market_end = market.get("end_date",""); sess = session_ctx()
    reasoning = (f"🔀 OB-ORACLE DISAGREE BTC {direction} | OB={ob:+.2f} ({'acheteurs' if ob>0 else 'vendeurs'}) "
                 f"≠ oracleΔ={oracle_delta:+.3f}% | tok={token_price:.3f}$ T-{int(slot_remaining)}s")
    st.current_market = market
    ok = await place_bet(context, direction, amount, 0.55, reasoning, {"score":0,"signals":[]}, sess,
                         tpu, tpd, market_end, source="ob_disagree", asset="BTC", market=market)
    if not ok: return
    real_amount = (st.bet or {}).get("amount", amount)
    await send(context.bot,
        f"🔀 *OB-ORACLE DISAGREE ₿ BTC* [💰 RÉEL]\n━━━━━━━━━━━━━━━\n"
        f"*{direction}* | Mise réelle:`{real_amount:.2f}$` | ⏰T-`{int(slot_remaining)}s`\n"
        f"Carnet:`{ob:+.2f}` ({'acheteurs→UP' if ob>0 else 'vendeurs→DOWN'}) ≠ OracleΔ:`{oracle_delta:+.3f}%`\n"
        f"Token:`{token_price:.3f}$`\n\n💭 _{reasoning}_")


async def job_confluence_asset(context, asset):
    """v12.9 — 4ème stratégie CONFLUENCE (/conf). Combine:
    A) Biais oracle (gap spot vs oracle, direction + magnitude)
    B) Régime + qualité setup (squeeze→mean-rev OU expansion→momentum, dans le sens de l'oracle uniquement)
    C) Pénalité bruit (chop détecté si ret10s/ret3s ont des signes opposés)
    Formule multiplicative TDS = oracle_score × setup_score × (1-noise) — vraie confluence, un facteur nul = pas de trade.
    Poids adaptatifs MR/momentum (_tds_adaptive_weight) restent neutres tant que <20 trades/branche.
    ⚠️ AJOUT PUR — ne modifie ni l'oracle lag, ni le momentum, ni le mean-reversion existants, les recombine seulement.
    Sizing Kelly dédié 1-3% BR (kelly_bet_secondary).
    """
    if not st.running or st.killed: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now
    if not (60 <= slot_remaining <= 150): return

    cfg = _asset_state_attrs(asset)
    # ✅ (22/06) fix: confluence vérifiait mom/mr/tds mais pas oracle_lag — ajouté pour cohérence totale.
    if (getattr(st, cfg["mom_slot"]) == cur_slot or getattr(st, cfg["mr_slot"]) == cur_slot
            or getattr(st, cfg["tds_slot"]) == cur_slot or getattr(st, cfg["oracle_slot"]) == cur_slot):
        return

    spot = getattr(st, cfg["price"])
    oracle = getattr(st, cfg["oracle"])
    pts = list(getattr(st, cfg["prices"]))
    if spot <= 0 or oracle <= 0 or len(pts) < 20: return

    # ── A. Biais oracle (léger, direction + magnitude — pas les filtres complets de job_oracle_lag) ──
    gap_pct = (spot - oracle) / oracle * 100
    if abs(gap_pct) < TDS_GAP_MIN: return
    oracle_dir = "UP" if gap_pct > 0 else "DOWN"
    oracle_score = min(1.0, abs(gap_pct) / TDS_GAP_STRONG)

    # ── B. Régime + setup (même calcul Bollinger que mean-reversion) ──
    window_pts = [p for t,p in pts if now-t <= 60]
    if len(window_pts) < 10: return
    sma = sum(window_pts) / len(window_pts)
    if sma <= 0: return
    variance = sum((p-sma)**2 for p in window_pts) / len(window_pts)
    std = variance ** 0.5
    upper = sma + 2*std; lower = sma - 2*std
    bandwidth = (upper - lower) / sma * 100
    is_squeeze = bandwidth <= 0.12

    def ret_over(secs):
        cutoff = now - secs
        old = [p for t,p in pts if t <= cutoff]
        return (spot - old[-1]) / old[-1] * 100 if old and old[-1] > 0 else 0.0
    ret_60s = ret_over(60); ret_30s = ret_over(30); ret_10s = ret_over(10); ret_3s = ret_over(3)

    setup_score = 0.0; setup_dir = None; setup_type = None; overext = 0.0
    if is_squeeze:
        if spot >= upper:
            cand_dir = "DOWN"; overext = (spot-upper)/sma*100
        elif spot <= lower:
            cand_dir = "UP"; overext = (lower-spot)/sma*100
        else:
            cand_dir = None
        if cand_dir is not None and cand_dir == oracle_dir:
            base = min(1.0, overext / TDS_OVEREXT_STRONG)
            setup_score = min(1.0, base * _tds_adaptive_weight("meanrev"))
            setup_dir = cand_dir; setup_type = "meanrev"
    else:
        if abs(ret_60s) >= 0.30:
            cand_dir = "UP" if ret_60s > 0 else "DOWN"
            if cand_dir == oracle_dir:
                confirm = 1.0 if (cand_dir=="UP" and ret_30s>=0.05) or (cand_dir=="DOWN" and ret_30s<=-0.05) else 0.6
                base = min(1.0, abs(ret_60s) / TDS_RET60S_STRONG) * confirm
                setup_score = min(1.0, base * _tds_adaptive_weight("momentum"))
                setup_dir = cand_dir; setup_type = "momentum"

    if setup_dir is None:
        log_skip(f"{asset} [CONF]: oracle {oracle_dir} (gap{gap_pct:+.3f}%) mais pas de setup régime aligné (BW={bandwidth:.3f}%)", oracle_dir,
                 features={"gap":gap_pct,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"conf_no_setup","asset":asset,"source":"confluence"})
        return

    # ── C. Pénalité bruit (chop/whipsaw: signe récent contraire au mouvement 10s) ──
    noise_penalty = 0.0
    if (ret_10s > 0 and ret_3s < -0.030) or (ret_10s < 0 and ret_3s > 0.030):
        noise_penalty = 0.5

    tds = oracle_score * setup_score * (1 - noise_penalty)
    if tds < TDS_MIN_SCORE:
        log_skip(f"{asset} [CONF]: TDS {tds:.2f}<{TDS_MIN_SCORE} (oracle={oracle_score:.2f} setup={setup_score:.2f} noise={noise_penalty:.1f})", setup_dir,
                 features={"gap":gap_pct,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"conf_tds_low","asset":asset,"source":"confluence"})
        return

    direction = setup_dir  # == oracle_dir (déjà vérifié aligné ci-dessus)

    market = await poly.get_market_by_slug(f"{cfg['slug']}-{cur_slot}")
    if not market: return
    token_used = market["token_up"] if direction=="UP" else market["token_down"]
    token_price = await poly.get_token_price(token_used)
    if not token_price or token_price <= 0: return

    if token_price > TDS_TOKEN_MAX or token_price < TDS_TOKEN_MIN:
        log_skip(f"{asset} [CONF]: token {token_price:.2f}$ hors range [{TDS_TOKEN_MIN}-{TDS_TOKEN_MAX}]", direction,
                 features={"gap":gap_pct,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"conf_token","asset":asset,"source":"confluence"})
        return

    fee = taker_fee_per_share(token_price)
    if setup_type == "meanrev":
        p_conf = min(0.85, 0.55 + overext * 5)
    else:
        p_conf = min(0.90, 0.65 + abs(ret_60s) * 0.5)
    p_conf = min(0.92, p_conf + 0.03)  # bonus confluence (heuristique — confirmation oracle+setup), À CALIBRER
    ev = p_conf - token_price - fee
    if ev < ORACLE_EDGE_MIN:
        log_skip(f"{asset} [CONF]: EV {ev*100:+.1f}%<{ORACLE_EDGE_MIN*100:.0f}%", direction,
                 features={"gap":gap_pct,"delta":bandwidth,"ret3s":ret_3s,"votes":0,"filter":"conf_ev","asset":asset,"source":"confluence"})
        return

    payout = round(1/token_price, 2)
    # ✅ v12.9 — Sizing dynamique: confidence dérivée du TDS lui-même (demande user 17/06).
    # TDS≈seuil(0.35) → confidence=0.7x (mise plus petite, confluence à peine validée)
    # TDS≈1.0 (confluence quasi-parfaite) → confidence=1.3x (mise plus grosse, dans le cap 1-3% BR)
    confidence = 0.7 + (tds - TDS_MIN_SCORE) / (1.0 - TDS_MIN_SCORE) * 0.6
    confidence = min(1.3, max(0.7, confidence))
    amount = kelly_bet_secondary(st.bankroll, p_conf, payout, confidence=confidence)
    if amount < MIN_BET_USD: return

    log.info(f"🎯 CONFLUENCE {asset} {direction} | TDS={tds:.2f} conf={confidence:.2f} type={setup_type} oracle={oracle_score:.2f} setup={setup_score:.2f} tok={token_price:.2f}$ EV={ev*100:.1f}%")

    # ✅ tpu/tpd = PRIX (float), pas token_id (string) — cf. fix job_oracle_lag BTC
    tpu = token_price if direction=="UP" else round(max(0.01,1-token_price),4)
    tpd = token_price if direction=="DOWN" else round(max(0.01,1-token_price),4)
    market_end = market.get("end_date","")
    sess = session_ctx()
    conf_score = {"score":0,"signals":[]}
    reasoning = (f"🎯CONFLUENCE confluence-{setup_type} {asset} {direction} | TDS={tds:.2f} conf={confidence:.2f} "
                 f"(oracle={oracle_score:.2f} setup={setup_score:.2f} noise={noise_penalty:.1f}) | "
                 f"gap={gap_pct:+.3f}% BW={bandwidth:.3f}% | tok={token_price:.2f}$ EV={ev*100:+.1f}% T-{int(slot_remaining)}s")

    st.current_market = market  # ✅ place_bet route l'ordre réel via st.current_market — doit pointer le marché de l'asset
    ok = await place_bet(context, direction, amount, round(p_conf,2), reasoning, conf_score, sess, tpu, tpd, market_end, source="confluence", asset=asset, market=market)
    if not ok: return

    setattr(st, cfg["tds_slot"], cur_slot)
    mode = "💰 RÉEL" if not st.paper_mode else "📄 paper"
    await send(context.bot,
        f"🎯 *CONFLUENCE {asset}* [{mode}]\n━━━━━━━━━━━━━━\n"
        f"*{direction}* | `{amount:.2f}$` | P:`{p_conf*100:.0f}%` | ⏰T-`{int(slot_remaining)}s`\n"
        f"TDS:`{tds:.2f}` (seuil {TDS_MIN_SCORE}) | Sizing conf:`{confidence:.2f}x` | Type:`{setup_type}`\n"
        f"Oracle:`{oracle_score:.2f}` (gap {gap_pct:+.3f}%) | Setup:`{setup_score:.2f}` | Noise:`{noise_penalty:.1f}`\n"
        f"Token:`{token_price:.2f}$` | EV:`{ev*100:+.1f}%`\n\n"
        f"📝 _4ème stratégie — confluence oracle+régime, sizing dynamique 1-3% BR (×{confidence:.2f} selon TDS)_")


async def job_confluence_btc(context):
    await job_confluence_asset(context, "BTC")

async def job_confluence_eth(context):
    await job_confluence_asset(context, "ETH")

async def job_confluence_sol(context):
    await job_confluence_asset(context, "SOL")

async def job_confluence_xrp(context):
    await job_confluence_asset(context, "XRP")


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
    xrp_p = [p for p in sample if p.get("asset")=="XRP"]
    mom_p = [p for p in sample if p.get("source")=="momentum"]
    meanrev_p = [p for p in sample if p.get("source")=="meanrev"]
    confluence_p = [p for p in sample if p.get("source")=="confluence"]
    ob_p = [p for p in sample if p.get("source")=="ob_signal" or p.get("filter","").startswith("ob_")]
    oracle_p = [p for p in sample if p.get("source") not in ("momentum","meanrev","confluence","ob_signal") and not p.get("filter","").startswith("ob_")]
    asset_note = f"BTC:{len(btc_p)} ETH:{len(eth_p)} SOL:{len(sol_p)} XRP:{len(xrp_p)} | OracleLag:{len(oracle_p)} Momentum:{len(mom_p)} MeanRev:{len(meanrev_p)} Confluence:{len(confluence_p)} OB:{len(ob_p)}"

    # v12.9 — Répartition par session pour détecter un biais de tendance dominante
    from collections import Counter as _Counter
    session_counts = _Counter(p.get("session","?") for p in sample)
    dominant_session, dominant_n = session_counts.most_common(1)[0] if session_counts else ("?",0)
    dominant_pct = (dominant_n / max(len(sample),1)) * 100
    session_note = f"Session dominante: {dominant_session} ({dominant_pct:.0f}% des données)" if dominant_pct >= 60 else "Données réparties sur plusieurs sessions"

    # ── Stats par filtre ──
    by_filter = {}
    for p in sample:
        f = p.get("filter","?")
        if f not in by_filter: by_filter[f] = {"w":0,"l":0}
        if p["result"]=="WIN": by_filter[f]["w"]+=1
        else: by_filter[f]["l"]+=1
    filter_stats = " | ".join(f"{f}:{v['w']}W/{v['l']}L" for f,v in sorted(by_filter.items(), key=lambda x:x[1]["w"]+x[1]["l"],reverse=True)[:5])

    # ✅ v12.9 — Résumé SLOT RECORDER pour Sonnet (stats UP/DOWN réelles par condition, indépendant du trading)
    slot_rec_note = ""
    try:
        recs = list(st.slot_records)
        if len(recs) >= 20:
            def _upr(s):
                return (sum(1 for r in s if r["result"]=="UP")/len(s)*100, len(s)) if s else (0,0)
            g_pct,g_n = _upr(recs)
            parts = [f"GLOBAL UP {g_pct:.0f}% (n={g_n})"]
            for reg in ("squeeze","expansion"):
                rp=[r for r in recs if r.get("regime")==reg]
                if len(rp)>=10: p,nn=_upr(rp); parts.append(f"{reg} UP {p:.0f}% (n={nn})")
            du=[r for r in recs if r.get("dual")=="UP"]; dd=[r for r in recs if r.get("dual")=="DOWN"]
            if len(du)>=10: p,nn=_upr(du); parts.append(f"dual=UP→UP réel {p:.0f}% (n={nn})")
            if len(dd)>=10: p,nn=_upr(dd); parts.append(f"dual=DOWN→UP réel {p:.0f}% (n={nn})")
            obb=[r for r in recs if r.get("ob",0)>0.15]; obs=[r for r in recs if r.get("ob",0)<-0.15]
            if len(obb)>=10: p,nn=_upr(obb); parts.append(f"OB-acheteurs→UP {p:.0f}% (n={nn})")
            if len(obs)>=10: p,nn=_upr(obs); parts.append(f"OB-vendeurs→UP {p:.0f}% (n={nn})")
            spr_v=[r.get("spread",0) for r in recs if r.get("spread",0)>0]
            dep_v=[r.get("depth",0) for r in recs if r.get("depth",0)>0]
            if spr_v: parts.append(f"spread moyen {sum(spr_v)/len(spr_v)*100:.1f}¢ (large=EV réel pire que calculé)")
            if dep_v: parts.append(f"profondeur moyenne {sum(dep_v)/len(dep_v):.0f}$ (faible=exécution difficile)")
            mu=[r for r in recs if r.get("micro",0)>0.002]; mdn=[r for r in recs if r.get("micro",0)<-0.002]
            if len(mu)>=10: p,nn=_upr(mu); parts.append(f"microprice↑→UP {p:.0f}% (n={nn})")
            if len(mdn)>=10: p,nn=_upr(mdn); parts.append(f"microprice↓→UP {p:.0f}% (n={nn})")
            ofp=[r for r in recs if r.get("ofi",0)>0]; ofn=[r for r in recs if r.get("ofi",0)<0]
            if len(ofp)>=10: p,nn=_upr(ofp); parts.append(f"OFI+→UP {p:.0f}% (n={nn})")
            if len(ofn)>=10: p,nn=_upr(ofn); parts.append(f"OFI-→UP {p:.0f}% (n={nn})")
            slot_rec_note = ("\n📊 SLOT RECORDER (tous slots résolus, oracle Chainlink, INDÉPENDANT du trading — "
                             "vérité terrain pour la valeur prédictive): " + " | ".join(parts) +
                             ". Un indicateur n'a de valeur prédictive que s'il s'écarte nettement de 50% sur n≥100. "
                             "Si dual=UP donne réellement >55% UP et dual=DOWN donne >55% DOWN, le dual model a une vraie valeur → recommander activation.")
    except Exception: pass

    # ✅ v12.9 Point1: snapshot filtres pour comparaison avec l'analyse suivante (boucle de feedback)
    filter_snapshot = {f: {"w":v["w"], "l":v["l"]} for f,v in by_filter.items()}
    evolution_note = ""
    if st.haiku_insights:
        prev_snap = st.haiku_insights[-1].get("filter_snapshot")
        if prev_snap:
            evo_lines = []
            for f, cur in filter_snapshot.items():
                if f in prev_snap:
                    old = prev_snap[f]
                    old_n, cur_n = old["w"]+old["l"], cur["w"]+cur["l"]
                    if old_n > 0 and cur_n > 0:
                        old_wr = old["w"]/old_n*100
                        cur_wr = cur["w"]/cur_n*100
                        evo_lines.append(f"{f}: WR {old_wr:.0f}%→{cur_wr:.0f}% (n={old_n}→{cur_n})")
            if evo_lines:
                evolution_note = "\n\nÉVOLUTION DEPUIS TA DERNIÈRE ANALYSE:\n" + " | ".join(evo_lines)

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
        # ✅ v12.9 — WR par stratégie (dont OB) pour que Sonnet compare le WR OB réel au 73% théorique
        ob_t = [t for t in real_trades if t.get("source")=="ob_signal"]
        lag_t = [t for t in real_trades if t.get("source") not in ("momentum","meanrev","confluence","ob_signal")]
        strat_line = ""
        if ob_t:
            wr_ob_real = sum(1 for t in ob_t if t.get('result')=='WIN')/len(ob_t)*100
            strat_line += f"\n- 📖 OB Signal: {len(ob_t)} trades | WR:{wr_ob_real:.0f}% (à comparer au 73% théorique du slot recorder — si nettement <, look-ahead)"
        if lag_t:
            strat_line += f"\n- Oracle lag: {len(lag_t)} trades | WR:{sum(1 for t in lag_t if t.get('result')=='WIN')/len(lag_t)*100:.0f}%"
        trade_summary = f"""
Trades réels ({len(real_trades)} total | WR:{wr:.0f}% | PnL:{pnl:+.2f}$):
- BTC: {len(btc_t)} trades | WR:{sum(1 for t in btc_t if t.get('result')=='WIN')/max(1,len(btc_t))*100:.0f}%
- ETH: {len(eth_t)} trades | WR:{sum(1 for t in eth_t if t.get('result')=='WIN')/max(1,len(eth_t))*100:.0f}%
- SOL: {len(sol_t)} trades | WR:{sum(1 for t in sol_t if t.get('result')=='WIN')/max(1,len(sol_t))*100:.0f}%{strat_line}"""

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

    # ✅ v12.9 Point3: contexte de régime marché 24h (réutilise fetch_klines existant, sûr si échec)
    regime_note = ""
    try:
        klines_24h = await fetch_klines("1h", limit=24, symbol="btcusdt")
        if klines_24h and len(klines_24h) >= 2:
            chg_24h = (klines_24h[-1]["close"] - klines_24h[0]["open"]) / klines_24h[0]["open"] * 100
            regime = "tendance forte" if abs(chg_24h) >= 2.0 else ("tendance modérée" if abs(chg_24h) >= 0.8 else "marché calme/range")
            regime_note = f"BTC 24h: {chg_24h:+.2f}% ({regime})"
    except Exception:
        pass

    # ── Patterns détaillés ──
    summary = []
    for p in sample:
        asset = p.get("asset","BTC")
        tok = f" tok={p.get('token',0):.2f}$" if p.get("token") else ""
        ev = f" EV={p.get('ev',0)*100:+.1f}%" if p.get("ev") else ""
        smt = f" smt={p.get('smt_div',0):+.3f}%" if p.get("smt_div") else ""
        summary.append(
            f"[{asset}] gap={p.get('gap',0):+.3f}% delta={p.get('delta',0):+.3f}% "
            f"ret3s={p.get('ret3s',0):+.3f}% votes={p.get('votes',0)}/5 "
            f"filter={p.get('filter','?')}{tok}{ev}{smt} → {p['result']}")

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

    # Calcul mise/gain/perte pour le prompt
    bankroll = st.bankroll
    avg_bet = round(bankroll * 0.04, 2)  # Kelly ~4% BR
    real_trades = [t for t in st.trades if not t.get("paper") and t.get("result")]
    wins_real = [t for t in real_trades if t.get("result")=="WIN"]
    losses_real = [t for t in real_trades if t.get("result")=="LOSS"]
    avg_win = round(sum(t.get("pnl",0) for t in wins_real)/max(len(wins_real),1), 2)
    avg_loss = round(abs(sum(t.get("pnl",0) for t in losses_real)/max(len(losses_real),1)), 2)
    if avg_win == 0: avg_win = round(avg_bet * 0.45, 2)  # estimation si pas de trades
    if avg_loss == 0: avg_loss = round(avg_bet * 0.85, 2)

    # ✅ v12.9 — Note Brier pour Sonnet (calibration de nos probabilités)
    brier_note = ""
    _bs = compute_brier_score(st.trades)
    if _bs:
        cal = "bien calibré" if _bs["brier"]<0.20 else ("limite/aléatoire" if _bs["brier"]<=0.25 else "MAL calibré")
        brier_note = (f"\n- 🎯 BRIER SCORE: {_bs['brier']} ({cal}). Confiance moyenne annoncée {_bs['avg_conf']*100:.0f}% "
                      f"vs WR réel {_bs['realized_wr']*100:.0f}% sur n={_bs['n']}. "
                      f"Si Brier>0.25 ou si conf>>WR réel, nos probabilités sont surestimées → le Kelly sur-mise et l'EV est faussé. "
                      f"Dans ce cas, recommande de RÉDUIRE les probabilités estimées (p_conf) plutôt que d'assouplir des filtres.")

    prompt = f"""Tu es un expert en trading algorithmique sur Polymarket (marchés prédiction crypto 5min).
Analyse les skips d'un bot oracle lag v{BOT_VERSION} — {version_note}.
Session actuelle: {session} | {btc_move} | {regime_note}

STRATÉGIE: Le bot exploite le lag entre Chainlink (oracle Polymarket) et le prix spot Binance.
Il achète le token UP ou DOWN avant que le marché reprices l'oracle.

PARAMÈTRES ACTUELS:
- BTC: gap≥0.025% | T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s
- ETH: gap≥0.020% | T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s
- SOL: gap≥0.020% | T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s
- XRP: gap≥0.025% | T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s
- ❌ (22/06) MOMENTUM, MEAN-REVERSION et CONFLUENCE DÉSACTIVÉES sur demande user — /edge a montré 0 trade
  réel sur 44 au total pour ces 3 (seules sources réelles: snipe=oracle_lag n=43, ob_disagree n=1).
  Code conservé (non supprimé) mais plus schedulé — ne plus analyser leurs skips, ils n'ont plus cours.
  Descriptions ci-dessous gardées pour mémoire seulement:
- MOMENTUM (BTC/ETH/SOL/XRP) [INACTIF]: ret60s≥0.30% | T-150s→T-60s | tok 0.55$-0.65$ | filtre trend macro 10min (bloque si tendance 10min contraire ≥0.10%, source: étude live ayant réduit pertes -93%→-13% avec ce filtre) | Kelly dédié 1-3% BR (2ème fenêtre indépendante). Extension ETH/SOL/XRP NOUVELLE (17/06) — surveiller si ces assets, documentés plus bruités à court terme, performent moins bien que BTC.
- MEAN-REVERSION (BTC/ETH/SOL/XRP) [INACTIF]: Bollinger Bandwidth≤0.12% (régime squeeze) | parie contre un spike (prix hors bandes 2σ) | tok 0.51$-0.70$ | Kelly dédié 1-3% BR (3ème fenêtre, même T-150s→T-60s, régime complémentaire au momentum — squeeze vs expansion). Stratégie NOUVELLE, seuils à calibrer avec données réelles. Extension ETH/SOL/XRP NOUVELLE (17/06).
- CONFLUENCE (BTC/ETH/SOL/XRP, 4ème stratégie /conf) [INACTIF]: TDS = oracle_score(gap≥0.025%, fort≥0.060%) × setup_score(mean-rev ou momentum, UNIQUEMENT si aligné avec le biais oracle) × (1-noise_penalty si chop détecté) | seuil TDS≥0.35 | tok 0.52$-0.72$ | Kelly dédié 1-3% BR avec SIZING DYNAMIQUE (confidence 0.7x à TDS=seuil → 1.3x à TDS=1.0, toujours capé 1-3% BR) | même fenêtre T-150s→T-60s. Poids adaptatifs MR/momentum ajustés UNIQUEMENT après ≥20 trades par branche (neutres sinon — anti-overfitting). Stratégie TRÈS NOUVELLE (17/06), tous les seuils sont des points de départ raisonnés à calibrer en priorité avec les premières données réelles.
- Commun: delta≥0.020% | token BTC {ORACLE_TOKEN_MIN:.2f}$-{ORACLE_TOKEN_MAX:.2f}$ (abaissé 0.41$→0.10$ le 23/06 sur demande user — ⚠️ RISQUE: zone <0.41$ jamais validée en réel, surveiller via /edge) (exceptions: ret3s≤+0.010% OU delta≥0.114%+gap≥0.060%) | ETH/XRP/SOL(votes≤-1) token max 0.95$ | EV≥{int(ORACLE_EDGE_MIN_BTC*100)}% pour BTC oracle lag, EV≥{int(ORACLE_EDGE_MIN_ALT*100)}% pour ETH/SOL/XRP oracle lag (abaissé à 5% le 22/06 sur demande user — ⚠️ RISQUE: seuil bas jamais validé en réel, surveiller via /edge et remonter si l'écart EV théorique/réalisé est négatif) | votes≥2 (consensus pour la direction parié, pas score brut)
- ✅ (22/06) Maker en priorité (zéro frais + rebate), TAKER en fallback si pas rempli. Délai d'attente
  maker ADAPTATIF (pas une constante fixe): proportionnel au temps réel restant avant la fin du slot —
  signal précoce (proche T-45s) = plein temps normal (~3s/tentative, jusqu'à 1s de fenêtre retry),
  signal tardif (proche T-5s) = bascule taker quasi immédiate pour ne pas rater le slot.
- ✅ (22/06, bonus doublé 23/06) Confirmations additionnelles oracle lag (bonus +0.04 à p_oracle, jamais un gate dur — doublé de 0.02 le 23/06 suite preuve /edge "OB+OFI accord" 92% précision n=145): (1) OB microprice+OFI LISSÉS sur ~5s avec exigence de persistance ≥70% (remplace l'ancienne lecture instantanée bruitée) — BTC/ETH/SOL seulement, XRP=0 (pas de flux OB XRP); (2) pression de liquidation Binance Futures nette ≥{LIQ_PRESSURE_MIN_USD}$ sur {LIQ_PRESSURE_WINDOW_S}s (mécanique directe: short liquidé→achat forcé→haussier, et inversement) — 4 cryptos. Funding rate Binance collecté et affiché via /oracle mais PAS ENCORE branché dans p_oracle (sens prédictif continuation vs retour à la moyenne pas encore validé empiriquement — ne pas en inventer un sans données).
- ✅ (23/06) demande user: GARDE DE RETOURNEMENT — surveille les positions réelles ouvertes toutes les
  {REVERSAL_CHECK_INTERVAL_S}s, vend en taker immédiatement (priorité vitesse) si le marché part
  clairement contre la direction parié de ≥{REVERSAL_DELTA_PCT}% (confirmé sur {REVERSAL_CONFIRM_READS}
  lectures consécutives, sauf cascade de liquidation ≥{REVERSAL_LIQ_OVERRIDE_USD}$ contre nous → sortie
  immédiate sans attendre confirmation). N'agit pas sous {REVERSAL_MIN_REMAINING_S}s restant (plus le
  temps). Ces sorties anticipées apparaissent dans /trades avec reasoning="🚨 Sortie auto..." — NE PAS
  les confondre avec des résolutions normales de fin de slot dans l'analyse.
- BTC deltaneg: bloqué sauf si gap≥0.040% ET ret3s>-0.050% (exception validée 9W/3L)
- ETH/SOL/XRP deltaneg: seuil strict -0.010% (0% WR historique si assoupli)
- Filtres actifs: ret3s_brutal(<-0.070%, ne bloque plus DOWN déjà confirmé) | delta_neg | gap_neg | tokenmax | tokenmin | ev
{trade_summary}

STATS FILTRES ({filter_stats}):
DONNÉES ({len(sample)} skips résolus — {asset_note}):
{chr(10).join(summary)}

{previous_insights}
{evolution_note}
{slot_rec_note}

CONTEXTE IMPORTANT:
- Bankroll: {bankroll:.2f}$ | Mise Kelly estimée: ~{avg_bet:.2f}$ par trade
- Gain moyen estimé par WIN: ~{avg_win:.2f}$ | Perte moyenne par LOSS: ~{avg_loss:.2f}$
- Le bot a eu très peu/pas de trades réels récemment. CAUSE IMPORTANTE: un bug get_market_by_slug (endpoints /events?slug au lieu de /events/slug/) empêchait ETH/SOL/XRP (et parfois BTC) de trouver leur marché → trades tués en silence, corrigé le 18/06. Donc une partie du "0 trade" était TECHNIQUE, pas un excès de filtrage. Ne conclus PAS hâtivement que les filtres sont trop stricts: vérifie d'abord via le SLOT RECORDER si les conditions avaient une vraie valeur prédictive.
- Objectif prioritaire: identifier des configurations qui AURAIENT dû trader et gagner
- Token max actuel: {ORACLE_TOKEN_MAX}$ | EV min: {int(ORACLE_EDGE_MIN_BTC*100)}% (BTC oracle lag) / {int(ORACLE_EDGE_MIN_ALT*100)}% (ETH/SOL/XRP oracle lag) | momentum/meanrev/confluence [INACTIF depuis 22/06, 0 trade réel/44] | votes min: 2/5{brier_note}
- {session_note}
- Mécanisme SMT (ETH/SOL uniquement): quand BTC et ETH/SOL divergent de ≥0.025% sur 15s, le laggard tend à rattraper (corrélation ~0.9). Si tu vois "smt=" dans les données, c'est ce signal de divergence cross-asset — facteur supplémentaire à considérer, pas encore pleinement exploité historiquement (collecte en cours).
- 🌑 SHADOW DOWN (filter=shadow_down): signaux DOWN "fantômes" en mode LOG-ONLY (aucun trade réel). Ils capturent le cas gap+/delta- persistant (marché baissier, oracle figé au-dessus du spot tombant) SANS chute brutale — un cas que les 4 stratégies ne tradent jamais actuellement. Question clé à trancher: ces DOWN auraient-ils GAGNÉ? Si shadow_down montre un WR≥58% sur n≥30 hors d'une seule session, c'est un EDGE réel à activer. Si WR≤48%, c'est un piège (mean-reversion: le spot rebondit au lieu de continuer à tomber) → garder désactivé. ATTENTION: ne te laisse pas piéger par un WR élevé issu d'une seule session 100% baissière (cf. règle anti-biais ci-dessous).
- 🔀 DUAL MODEL (champ "dual" dans les features = UP/DOWN/None): inspiré des papiers CNN-LSTM qui entraînent des modèles UP et DOWN séparés. On calcule up_score et down_score indépendamment (RSI, EMA9/21, MACD, momentum 3min) au lieu d'un score symétrique. dual = la direction qui domine (marge ≥1.0). MODE MESURE uniquement: ne change AUCUNE décision pour l'instant. Si tu vois que "dual" prédit la direction gagnante nettement mieux que les votes actuels (≥58% sur n≥30), signale-le comme piste d'activation. MACD vient d'être ajouté aux votes TA (top-feature ML avec RSI) — son impact se mesure dans ta_vote.
- 🎯 MICROPRICE (champ "micro") & 🌊 OFI (champ "ofi"): microstructure du carnet Polymarket, MODE MESURE. Le microprice (Stoikov) est le mid pondéré par l'imbalance top-of-book — la littérature (arxiv 2026) le donne meilleur prédicteur que l'imbalance brute, SURTOUT sur gros ticks comme Polymarket. micro>0 penche UP, <0 penche DOWN. L'OFI (Order Flow Imbalance) mesure la variation NETTE du top-of-book entre deux ticks (flux dynamique, pas photo statique). Question: micro et OFI prédisent-ils mieux que l'OB imbalance simple (déjà à ~62% UP côté acheteur)? Si micro↑→UP ou OFI+→UP dépassent nettement 55% sur n≥100 ET sur plusieurs sessions, ce sont des candidats d'activation. ATTENTION au biais directionnel de session (un signal qui "marche" en marché baissier peut être un artefact — cf. dual=DOWN qui s'est effondré de 68% à 50% en passant baissier→haussier).
- 📖 STRATÉGIE OB SIGNAL (source="ob_signal", trades RÉELS depuis 18/06): trade dans le sens du carnet quand |imbalance|≥0.12, fenêtre T-150→T-30s, token 0.40-0.75$, mise minimale. Basée sur le slot recorder (OB acheteur→73% UP, vendeur→88% DOWN, n>150 en marché neutre — mais mesuré à |OB|>0.15, donc à 0.12 le signal est un peu plus faible). ⚠️ NON validée en exécution réelle: le 73% est mesuré à la RÉSOLUTION, pas à l'entrée (risque de look-ahead). Surveille ces trades de près: si leur WR réel est nettement < au 73% mesuré, c'est que le signal à l'entrée est plus faible qu'à la résolution (look-ahead confirmé) → recommande de désactiver ou resserrer le seuil. Compare le WR réel ob_signal au 73% théorique.

⚠️ RÈGLE ANTI-BIAIS OBLIGATOIRE:
Si une session représente ≥60% des données (ex: nuit calme ASIA_EARLY ou forte tendance directionnelle),
tu DOIS le signaler explicitement et baisser ta confiance dans la suggestion.
Un WR de 90%+ sur une seule session/tendance ne généralise PAS aux autres conditions de marché.
Ne propose un changement de paramètre que si le pattern semble structurel (pas juste "le marché montait").

INSTRUCTIONS:
1. Identifie les patterns de skips qui auraient été GAGNANTS (WR élevé dans les ✅)
2. Pour chaque pattern gagnant raté, propose UN ajustement de paramètre concret et chiffré
3. Évalue le ratio risque/opportunité: combien de trades supplémentaires gagnerait-on vs perdrait-on
4. Si une analyse précédente identifiait un pattern OU si la section ÉVOLUTION montre un changement de WR sur un filtre, tu DOIS explicitement confirmer, infirmer, ou expliquer la contradiction — ne jamais ignorer silencieusement un résultat qui contredit ta dernière analyse
5. Distingue [BTC]/[ETH]/[SOL] ou [COMMUN] selon l'asset concerné
6. Priorise les suggestions qui augmentent le nombre de trades rentables

QUESTIONS CLÉS À RÉPONDRE:
- Quel filtre bloque le plus de trades gagnants en ce moment ?
- La 2ème fenêtre MOMENTUM BTC (T-150s→T-60s) performe-t-elle ? Faut-il ajuster ret60s seuil ou token range ?
- Quel seuil précis faudrait-il changer pour capturer ces gains ?
- Y a-t-il un contexte (session, volatilité, gap fort) où on devrait être plus agressif ?

CALCUL DE PROFIT OBLIGATOIRE pour chaque suggestion:
- Bankroll actuelle: {bankroll:.2f}$
- Mise moyenne par trade (Kelly ~3-5% BR): ~{avg_bet:.2f}$
- Pour chaque suggestion: calcule (W supplémentaires × gain moyen) - (L supplémentaires × perte moyenne)
- Gain moyen sur un trade: mise × (1/token - 1) | Perte moyenne: -mise
- Si tu proposes un changement → chiffre l'impact net en dollars ET en % de bankroll
- ⚠️ Si l'échantillon du pattern cité est <20 trades: donne une FOURCHETTE (ex: "+1$ à +4$") au lieu d'un chiffre exact — un chiffre précis sur petit échantillon est une fausse précision
- Indique ton niveau de CONFIANCE (0-100%) pour chaque suggestion, basé sur: taille d'échantillon, biais de session, cohérence avec analyses précédentes

Réponds en EXACTEMENT 3 bullet points actionnables en français:
Format: "• [ASSET] [OBSERVATION + données]: [SUGGESTION CONCRÈTE] → Impact: +Xw/-Yl = +Y.YY$ net (+Z% BR) | Confiance: NN%" """

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
                    st.haiku_insights.append({"type":"sonnet","ts":int(now),"insight":insight,"filter_snapshot":filter_snapshot})
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
    lines.append(f"  delta≥`{ORACLE_ENTRY_DELTA:.3f}%` | gap BTC≥`0.025%` | gap ETH/SOL≥`0.020%` | gap XRP≥`0.025%`")
    lines.append(f"  token:`{ORACLE_TOKEN_MIN:.2f}$`-`{ORACLE_TOKEN_MAX:.2f}$`(BTC) `0.95$`(ETH/XRP/SOL) | EV≥`{ORACLE_EDGE_MIN_BTC*100:.0f}%`(BTC)/`{ORACLE_EDGE_MIN_ALT*100:.0f}%`(ETH/SOL/XRP) | votes≥2(dir)")
    lines.append(f"  BTC: T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s | ETH: T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s | SOL: T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s | XRP: T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s")

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
        # ✅ v12.9 — Brier score: nos probabilités sont-elles calibrées?
        bs = compute_brier_score(real)
        if bs:
            if bs["brier"] < 0.20: verdict="🟢 calibré (edge réel)"
            elif bs["brier"] <= 0.25: verdict="🟡 limite (~aléatoire)"
            else: verdict="🔴 mal calibré (proba peu fiable)"
            lines.append(f"  🎯 Brier:`{bs['brier']}` {verdict}")
            lines.append(f"     conf moy:`{bs['avg_conf']*100:.0f}%` vs WR réel:`{bs['realized_wr']*100:.0f}%` (n={bs['n']})")
            gap_cal = bs['avg_conf'] - bs['realized_wr']
            if abs(gap_cal) > 0.10:
                lines.append(f"     ⚠️ surestimation `{gap_cal*100:+.0f}pts` — Kelly sur-mise, prudence")
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
        # WR par stratégie
        mom_trades = [t for t in real if t.get("source")=="momentum"]
        meanrev_trades = [t for t in real if t.get("source")=="meanrev"]
        confluence_trades = [t for t in real if t.get("source")=="confluence"]
        ob_trades = [t for t in real if t.get("source")=="ob_signal"]
        lag_trades = [t for t in real if t.get("source") not in ("momentum","meanrev","confluence","ob_signal")]
        if mom_trades:
            w_m=sum(1 for t in mom_trades if t.get("result")=="WIN")
            pnl_m=sum(t.get("pnl",0) for t in mom_trades)
            lines.append(f"  🚀 Momentum: {len(mom_trades)} trades WR:`{w_m/len(mom_trades)*100:.0f}%` PnL:`{pnl_m:+.2f}$`")
        if meanrev_trades:
            w_mr=sum(1 for t in meanrev_trades if t.get("result")=="WIN")
            pnl_mr=sum(t.get("pnl",0) for t in meanrev_trades)
            lines.append(f"  🔄 Mean-Rev: {len(meanrev_trades)} trades WR:`{w_mr/len(meanrev_trades)*100:.0f}%` PnL:`{pnl_mr:+.2f}$`")
        if confluence_trades:
            w_c=sum(1 for t in confluence_trades if t.get("result")=="WIN")
            pnl_c=sum(t.get("pnl",0) for t in confluence_trades)
            lines.append(f"  🎯 Confluence: {len(confluence_trades)} trades WR:`{w_c/len(confluence_trades)*100:.0f}%` PnL:`{pnl_c:+.2f}$`")
            c_mr=[t for t in confluence_trades if "confluence-meanrev" in t.get("reasoning","")]
            c_mom=[t for t in confluence_trades if "confluence-momentum" in t.get("reasoning","")]
            if c_mr or c_mom:
                lines.append(f"     └ MR:{len(c_mr)} (poids {_tds_adaptive_weight('meanrev'):.2f}) | MOM:{len(c_mom)} (poids {_tds_adaptive_weight('momentum'):.2f})")
        # ✅ v12.9 — trades réels stratégie OB SIGNAL (comparer le WR au 73% théorique pour détecter le look-ahead)
        if ob_trades:
            w_ob=sum(1 for t in ob_trades if t.get("result")=="WIN")
            pnl_ob=sum(t.get("pnl",0) for t in ob_trades)
            wr_ob=w_ob/len(ob_trades)*100
            verdict_ob=""
            if len(ob_trades)>=10:
                if wr_ob>=65: verdict_ob=" 🟢 tient le 73%"
                elif wr_ob>=55: verdict_ob=" 🟡 sous le 73% (signal + faible à l'entrée)"
                else: verdict_ob=" 🔴 look-ahead probable, resserrer/désactiver"
            lines.append(f"  📖 OB Signal: {len(ob_trades)} trades WR:`{wr_ob:.0f}%` PnL:`{pnl_ob:+.2f}$`{verdict_ob}")
        # ✅ v12.9 — Résumé agrégé régime squeeze/expansion (BTC+ETH+SOL+XRP cumulés, pas de spam /passes)
        total_regime = st.meanrev_regime_squeeze_count + st.meanrev_regime_expansion_count
        if total_regime > 0:
            pct_squeeze = st.meanrev_regime_squeeze_count / total_regime * 100
            lines.append(f"  📐 Régime (cumulé 4 assets): Squeeze `{pct_squeeze:.0f}%` ({st.meanrev_regime_squeeze_count}) | Expansion `{100-pct_squeeze:.0f}%` ({st.meanrev_regime_expansion_count})")
        if lag_trades:
            w_l=sum(1 for t in lag_trades if t.get("result")=="WIN")
            pnl_l=sum(t.get("pnl",0) for t in lag_trades)
            lines.append(f"  ⚡ Oracle lag: {len(lag_trades)} trades WR:`{w_l/len(lag_trades)*100:.0f}%` PnL:`{pnl_l:+.2f}$`")
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
        # ✅ v12.9 — SHADOW DOWN: bloc dédié avec interprétation INVERSÉE (WR élevé = DOWN aurait gagné = EDGE réel)
        if "shadow_down" in by_filter:
            sv=by_filter["shadow_down"]; stot=sv["w"]+sv["l"]
            if stot>0:
                swr=sv["w"]/stot*100
                if stot<30:
                    verdict=f"⏳ échantillon insuffisant (n={stot}, besoin ≥30)"
                elif swr>=58:
                    verdict=f"🟢 EDGE POTENTIEL — DOWN aurait gagné {swr:.0f}% (envisager activation réelle)"
                elif swr<=48:
                    verdict=f"🔴 PIÈGE confirmé — DOWN perd ({swr:.0f}%), garder en log-only/désactiver"
                else:
                    verdict=f"➖ zone grise ({swr:.0f}%) — proche coinflip, pas d'edge net"
                lines.append(f"\n  🌑 *SHADOW DOWN* (log-only): {sv['w']}W/{sv['l']}L\n     {verdict}")
        # ✅ v12.9 — DUAL MODEL (mode mesure): le dual_dir (up_score vs down_score) prédit-il mieux?
        # Un pattern a direction (signal oracle) + result (WIN si cette direction gagne) + dual (UP/DOWN/None).
        # On reconstruit la direction réellement gagnante, puis on mesure si dual l'aurait devinée.
        dual_pats=[p for p in sample if p.get("dual") in ("UP","DOWN")]
        if dual_pats:
            dual_correct=0
            for p in dual_pats:
                sig=p.get("direction"); res=p.get("result"); dd=p.get("dual")
                if sig not in ("UP","DOWN") or res not in ("WIN","LOSS"): continue
                winning_dir = sig if res=="WIN" else ("DOWN" if sig=="UP" else "UP")
                if dd==winning_dir: dual_correct+=1
            dtot=len([p for p in dual_pats if p.get("direction") in ("UP","DOWN") and p.get("result") in ("WIN","LOSS")])
            if dtot>0:
                dacc=dual_correct/dtot*100
                if dtot<30: dverdict=f"⏳ n={dtot} insuffisant (besoin ≥30)"
                elif dacc>=58: dverdict=f"🟢 dual prédit {dacc:.0f}% — signal utile (envisager vote dual)"
                elif dacc<=45: dverdict=f"🔴 dual à {dacc:.0f}% — pire que hasard, ne pas activer"
                else: dverdict=f"➖ dual {dacc:.0f}% — proche coinflip"
                lines.append(f"\n  🔀 *DUAL MODEL* (mesure): {dual_correct}/{dtot} corrects\n     {dverdict}")
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

def _schedule_all_jobs(jq):
    """✅ (21/06) Planification centralisée de TOUS les jobs de trading. Appelée par /run ET par
    l'auto-reprise au démarrage (main()), pour que les jobs ne dépendent plus d'un /run manuel
    après chaque redeploy (cause du "bot tourne mais aucune passe/trade")."""
    # ✅ (21/06) IDEMPOTENT: retire d'abord tous les jobs déjà planifiés. Sinon un 2e appel (ex: /stop
    # puis /run — /stop ne retirait que 6 jobs sur ~36) DOUBLAIT chaque stratégie → 2× trades + 2× alertes.
    for _j in list(jq.jobs()):
        try: _j.schedule_removal()
        except Exception: pass
    if not st.paper_mode:
        jq.run_once(job_reconcile, when=8)  # ✅ #8 — réconcilie l'état avec les positions réelles au démarrage
    st.price_job=jq.run_repeating(job_price,interval=30,first=5)
    st.macro_job=jq.run_repeating(job_macro,interval=300,first=8)
    # ❌ (22/06) DÉSACTIVÉ sur demande user — source="tick" jamais vue dans /edge (0 trade réel sur 44).
    # Code conservé, st.tick_job reste None (géré proprement par le cleanup de /stop).
    # st.tick_job=jq.run_repeating(job_tick,interval=30,first=10)
    st.tp_job=jq.run_repeating(job_take_profit,interval=TAKE_PROFIT_CHECK,first=10)
    jq.run_repeating(job_reversal_guard,interval=REVERSAL_CHECK_INTERVAL_S,first=6)  # ✅ (23/06) demande user
    st.backup_job=jq.run_repeating(job_backup,interval=120,first=60)  # v12.4 backup 2min
    st.recap_job=jq.run_repeating(job_daily_recap,interval=3600,first=60)
    jq.run_repeating(job_check_expiry,interval=30,first=15)
    jq.run_repeating(job_sync_balance,interval=60,first=12)  # ✅ (21/06) BR toujours = solde Polymarket réel
    jq.run_repeating(job_funding_rate,interval=60,first=15)  # ✅ (22/06) demande user: funding rate Binance Futures
    jq.run_repeating(job_ws_watchdog_all,interval=30,first=1)  # ✅ v10.23 tous les WS
    jq.run_repeating(job_staged_entry,interval=5,first=14)     # ✅ v10.23 2e tranche
    jq.run_repeating(job_oracle_lag,interval=2,first=16)
    jq.run_repeating(job_oracle_lag_eth,interval=2,first=18)
    jq.run_repeating(job_oracle_lag_sol,interval=2,first=20)
    jq.run_repeating(job_oracle_lag_xrp,interval=2,first=22)
    # ❌ (22/06) DÉSACTIVÉ sur demande user — /edge a montré 0 trade réel sur 44 au total pour momentum/
    # meanrev/confluence (seules sources présentes: snipe=oracle_lag n=43, ob_disagree n=1). Ces 3
    # stratégies ne contribuaient déjà à RIEN en pratique. Code conservé, juste plus schedulé.
    # jq.run_repeating(job_momentum_btc,interval=2,first=24)  # ✅ v12.9 — 2ème fenêtre momentum
    # jq.run_repeating(job_mean_reversion_btc,interval=2,first=26)  # ✅ v12.9 — 3ème fenêtre mean-reversion (ajout pur)
    # ✅ v12.9 — Extension multi-asset momentum+meanrev (ETH/SOL/XRP), sizing 1-3% BR dédié (demande user 17/06)
    # jq.run_repeating(job_momentum_eth,interval=2,first=28)
    # jq.run_repeating(job_momentum_sol,interval=2,first=30)
    # jq.run_repeating(job_momentum_xrp,interval=2,first=32)
    # jq.run_repeating(job_mean_reversion_eth,interval=2,first=34)
    # jq.run_repeating(job_mean_reversion_sol,interval=2,first=36)
    # jq.run_repeating(job_mean_reversion_xrp,interval=2,first=38)
    # ✅ v12.9 — 4ème stratégie CONFLUENCE (/conf), demande user 17/06
    # jq.run_repeating(job_confluence_btc,interval=2,first=40)
    # jq.run_repeating(job_confluence_eth,interval=2,first=42)
    # jq.run_repeating(job_confluence_sol,interval=2,first=44)
    # jq.run_repeating(job_confluence_xrp,interval=2,first=46)
    # ✅ v12.9 — STRATÉGIE OB SIGNAL (trade dans le sens du carnet, fenêtre T-150→T-30)
    jq.run_repeating(job_ob_signal_btc,interval=3,first=48)
    jq.run_repeating(job_ob_signal_eth,interval=3,first=49)
    jq.run_repeating(job_ob_signal_sol,interval=3,first=50)
    jq.run_repeating(job_ob_signal_xrp,interval=3,first=51)
    jq.run_repeating(job_ob_oracle_disagree,interval=3,first=52)  # ✅ (21/06) ob_oracle_disagree (BTC réel)
    jq.run_repeating(job_resolve_passes,interval=30,first=35)
    # ✅ v12.9 — SLOT RECORDER: enregistrement principal à la bascule (ws_oracle_loop) + ce job en filet de sécurité
    jq.run_repeating(job_slot_recorder,interval=30,first=50)
    # ✅ v12.9 — TRACKER TIMING DE PRICING: mesure à quel T-Xs le token dépasse 0.95$ (10s)
    jq.run_repeating(job_price_timing,interval=10,first=20)
    jq.run_repeating(job_auto_calibrate,interval=7200,first=300)  # ✅ v10.37 seuils auto
    jq.run_repeating(job_pattern_memory,interval=3600,first=600)  # ✅ v10.37 mémoire patterns
    jq.run_repeating(job_haiku_analysis,interval=7200,first=900)  # ✅ v10.37 Haiku insights

async def _job_autoresume_notify(context):
    """✅ (21/06) Notifie l'auto-reprise du trading après un redémarrage/redeploy."""
    await send(context.bot,
        f"♻️ *Auto-reprise* — le bot était actif avant le redémarrage, trading relancé automatiquement.\n"
        f"Mode:*{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}* | BR:`{st.bankroll:.2f}$`")

async def cmd_run(update,context):
    if not auth(update): return
    if st.running: await update.message.reply_text("⚠️ Déjà en cours."); return
    if st.killed:
        # ✅ (21/06) avant: /run planifiait les jobs MAIS ils sortaient tous sur `if st.killed: return`
        # → bot "démarré" mais 100% inerte sans explication. Maintenant on prévient.
        await update.message.reply_text("⛔ Kill-switch actif (pertes consécutives). Fais `/revive` d'abord, puis `/run`.", parse_mode="Markdown"); return
    if not st.paper_mode:
        if not poly.init_client():
            await update.message.reply_text("⚠️ Polymarket indispo — paper mode activé",parse_mode="Markdown")
            st.paper_mode=True
    st.running=True; st.session_start=time.time(); st.daily_ts=time.time()
    _schedule_all_jobs(context.job_queue)
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
        f"/oracle BTC T-{ORACLE_WINDOW_START}→T-{ORACLE_WINDOW_END}s | ETH T-{ORACLE_WINDOW_START}→T-{ORACLE_WINDOW_END}s | SOL T-{ORACLE_WINDOW_START}→T-{ORACLE_WINDOW_END}s | XRP T-{ORACLE_WINDOW_START}→T-{ORACLE_WINDOW_END}s\n"
        f"/momentum BTC/ETH/SOL/XRP T-150s→T-60s | move≥0.30%/60s | tok 0.55$-0.65$ | filtre trend10m | Kelly 1-3%\n"
        f"/meanrev BTC/ETH/SOL/XRP T-150s→T-60s | squeeze BW≤0.12% | tok 0.51$-0.70$ | Kelly 1-3%\n"
        f"/conf BTC/ETH/SOL/XRP T-150s→T-60s | TDS=oracle×setup×(1-bruit)≥0.35 | tok 0.52$-0.72$ | Kelly 1-3% dynamique\n"
        f"🌑 SHADOW DOWN (log-only): mesure les DOWN ratés en marché baissier (gap+/delta- persistant). 0 trade réel — voir /passes et /learn\n"
        f"📊 /slots: journal de TOUS les slots résolus (UP/DOWN réel + conditions) — indépendant du trading, pour analyse prédictive\n"
        f"🌊 /flow: order flow temps réel (derniers trades Polymarket des 4 cryptos, détecte le smart money)\n"
        f"📖 OB SIGNAL (NOUVEAU, réel): trade dans le sens du carnet si |imbalance|≥{OB_SIGNAL_THRESHOLD} | BTC/ETH/SOL (pas XRP) | T-150→T-30s | tok {OB_SIGNAL_TOKEN_MIN}-{OB_SIGNAL_TOKEN_MAX}$ | basé sur slot recorder (OB acheteur 73% UP). ⚠️ non validé, mise mini\n"
        f"  gap BTC/XRP≥2.5bps | ETH/SOL≥2.0bps | delta≥{int(ORACLE_ENTRY_DELTA*10000)}bps | Token≤{ORACLE_TOKEN_MAX}$(BTC)/0.95$(ETH/XRP/SOL) | EV≥{int(ORACLE_EDGE_MIN_BTC*100)}%(BTC)/{int(ORACLE_EDGE_MIN_ALT*100)}%(ETH/SOL/XRP)\n"
        f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`\n"
        f"📊 `{ob_txt}` | 💸 `{liq_txt}`\n"
        f"Récap auto: 22h Paris 🕙",
        parse_mode="Markdown")
    await job_tick(context)

async def cmd_stop(update,context):
    if not auth(update): return
    st.running=False
    # ✅ (21/06) retire TOUS les jobs planifiés (avant: seulement 6 sur ~36 → les stratégies oracle/
    # momentum/etc. continuaient à tourner après /stop, et /run les redoublait).
    for j in list(context.job_queue.jobs()):
        try: j.schedule_removal()
        except Exception: pass
    st.tick_job=st.price_job=st.macro_job=st.tp_job=st.backup_job=st.recap_job=None
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
    # ✅ (21/06) positions PAR CRYPTO (slot réservé supprimé) — affiche toutes les positions ouvertes
    _lines=[]
    for a in ASSETS:
        b=getattr(st, f"bet{_possfx(a)}")
        if not b: continue
        elapsed=int((time.time()-b["ts"])/60)
        exp=getattr(st, f"bet_expiry{_possfx(a)}", 0)
        rem=f" ⏰{int((exp-time.time())/60)}min" if exp>0 else ""
        _lines.append(f"{a}:{b['dir']} {b['amount']:.2f}$ ({elapsed}min){rem}")
    bet_info="\n".join(_lines) if _lines else "Aucun"
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

    # ✅ v10.14d — Si Claude dit trade=True, placer l'ordre depuis /signal
    # ✅ (anti-doublon 20/06): passe par place_bet (au lieu d'un place_market_order direct) pour
    # respecter le verrou asset_trade_slot (1 bet/slot/crypto) + la vérif de fill. Sinon un /signal
    # manuel pouvait doubler une position BTC déjà ouverte par une stratégie auto dans le même slot.
    if d.get("trade") and d.get("dir") and not st.bet and not st.paper_mode and st.current_market:
        amount = d.get("size", 0)
        if amount >= MIN_BET_USD and st.bankroll >= amount:
            market_end = st.current_market.get("end_date", "")
            ok = await place_bet(context, d["dir"], amount, d["conf"], d["reasoning"], cs, sess,
                                 tu, td, market_end, source="signal", asset="BTC", market=st.current_market)
            if ok:
                await update.message.reply_text(
                    f"🎯 *Ordre placé depuis /signal !*\n"
                    f"*{d['dir']}* `{amount:.2f}$` | Token:`{st.entry_token_price:.3f}$`",parse_mode="Markdown")
            else:
                await update.message.reply_text("⚠️ Ordre non placé (slot BTC déjà tradé, non rempli, ou refusé)")

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
    for a in ASSETS:  # ✅ (21/06) positions actives par crypto
        b=getattr(st, f"bet{_possfx(a)}")
        if not b: continue
        elapsed=int((time.time()-b["ts"])/60)
        lines.append(f"\n🔄 *Actif {a}:* `{b['dir']}` `{b['amount']:.2f}$` ({elapsed}min)")
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

async def cmd_fear(update,context):
    if not auth(update): return
    v=st.fg.get("value",50); bar="█"*(v//10)+"░"*(10-v//10)
    e="😱" if v<20 else "😟" if v<40 else "😐" if v<60 else "😊" if v<80 else "🤑"
    interp="Extrême Peur→biais UP" if v<20 else "Peur" if v<40 else "Neutre" if v<60 else "Greed" if v<80 else "Extrême Greed→biais DOWN"
    await update.message.reply_text(
        f"😱 *FEAR & GREED*\n{e} *{st.fg.get('label','N/A')}* — `{v}/100`\n`{bar}`\n\n_{interp}_",
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
    """v12.9 — Passes avec pagination boutons."""
    if not auth(update): return
    page = 1
    if context.args:
        try: page = max(1, int(context.args[0]))
        except: pass
    await _show_passes_page(update, context, page)

async def _show_passes_page(update, context, page=1):
    """Affiche une page de passes — logique originale + pagination."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from datetime import datetime
    _resolve_pending_passes()

    PAGE = 12
    # ✅ v12.9 (18/06) — masquer les skips confluence dans /passes (trop nombreux, peu informatifs). La stratégie tourne toujours.
    all_passes = list(reversed([p for p in st.pass_reasons if p.get("source") != "confluence" and not p.get("filter","").startswith("conf_")]))
    total = len(all_passes)
    total_pages = max(1, (total + PAGE - 1) // PAGE)
    page = min(max(1, page), total_pages)
    passes = all_passes[(page-1)*PAGE : page*PAGE]

    if not passes:
        msg = update.callback_query.message if update.callback_query else update.message
        await msg.reply_text("✅ Aucun PASS."); return

    lines=[f"🚫 *PASSES — BTC/ETH/SOL/XRP* ({page}/{total_pages})\n━━━━━━━━━━━━━━"]
    for p in passes:
        res = p.get("resolved")
        emoji = "✅" if res=="WIN" else "❌" if res=="LOSS" else "❓" if res=="❓" else ("—" if not p.get("dir") else "⏳")
        d = f"{p.get('dir')} " if p.get("dir") else "— "
        reason = p.get("reason","?")
        t = datetime.fromtimestamp(p.get("ts",0)).strftime("%H:%M") if p.get("ts") else "??"
        lines.append(f"{t} {d}{emoji} {reason[:80]}")

    # Stats WR
    resolved = [p for p in st.pass_reasons if p.get("resolved") in ("WIN","LOSS")]
    if resolved:
        w = sum(1 for p in resolved if p.get("resolved")=="WIN")
        twr = w/len(resolved)*100
        lines.append(f"\n📊 WR théorique des skips: {twr:.0f}% ({w}/{len(resolved)})")
        if twr >= 58: lines.append("⚠️ Filtres peut-être trop stricts")
        elif twr <= 50: lines.append("✅ ~50% — les filtres ne coûtent rien, le marché était plat")
        else: lines.append("➖ Zone grise — encore besoin de données")
    # ✅ v12.9 — Compteur SHADOW DOWN dédié (log-only, WR élevé = DOWN aurait gagné = edge)
    shadow = [p for p in st.pass_reasons if "[SHADOW]" in str(p.get("reason","")) and p.get("resolved") in ("WIN","LOSS")]
    if shadow:
        sw = sum(1 for p in shadow if p.get("resolved")=="WIN")
        swr = sw/len(shadow)*100
        verdict = "🟢 edge?" if (swr>=58 and len(shadow)>=30) else ("🔴 piège" if swr<=48 and len(shadow)>=30 else "⏳ +data")
        lines.append(f"🌑 SHADOW DOWN: {swr:.0f}% ({sw}/{len(shadow)}) {verdict}")
    # ✅ v12.9 — Compteur DUAL MODEL (mesure): précision du dual_dir vs direction gagnante réelle
    dual_res = [p for p in st.oracle_patterns if p.get("dual") in ("UP","DOWN")
                and p.get("direction") in ("UP","DOWN") and p.get("result") in ("WIN","LOSS")]
    if dual_res:
        dc = 0
        for p in dual_res:
            sig=p["direction"]; win_dir = sig if p["result"]=="WIN" else ("DOWN" if sig=="UP" else "UP")
            if p["dual"]==win_dir: dc+=1
        dacc = dc/len(dual_res)*100
        dv = "🟢 utile" if (dacc>=58 and len(dual_res)>=30) else ("🔴 faible" if dacc<=45 and len(dual_res)>=30 else "⏳ +data")
        lines.append(f"🔀 DUAL: {dacc:.0f}% ({dc}/{len(dual_res)}) {dv}")

    text = "\n".join(lines)

    # Boutons navigation
    btns = []
    if page > 1: btns.append(InlineKeyboardButton("⬅️", callback_data=f"passes:{page-1}"))
    if page < total_pages: btns.append(InlineKeyboardButton("➡️", callback_data=f"passes:{page+1}"))
    kbd = InlineKeyboardMarkup([btns]) if btns else None

    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kbd)
        else:
            await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kbd)
    except:
        clean = text.replace("*","").replace("`","").replace("_","")
        if update.callback_query:
            await update.callback_query.edit_message_text(clean, reply_markup=kbd)
        else:
            await update.message.reply_text(clean, reply_markup=kbd)


async def cmd_paper(update,context):
    if not auth(update): return
    st.paper_mode=not st.paper_mode
    if not st.paper_mode and not poly.ready: poly.init_client()
    await update.message.reply_text(f"Mode:*{'📄 PAPER' if st.paper_mode else '💰 RÉEL ⚠️'}* | API:{'✅' if poly.ready else '❌'}",parse_mode="Markdown")
    st.backup()


async def cmd_lasterrors(update,context):
    """✅ (demande user 20/06) — Affiche les derniers WARNING/ERROR du bot (buffer mémoire),
    pour diagnostiquer sans avoir besoin des logs Railway. Usage: /lasterrors [N] (défaut 15)."""
    if not auth(update): return
    n = 15
    if context.args:
        try: n = max(1, min(50, int(context.args[0])))
        except: pass
    if not _RECENT_ERRORS:
        await update.message.reply_text("✅ Aucun warning/erreur enregistré depuis le démarrage."); return
    items = list(_RECENT_ERRORS)[-n:][::-1]
    lines = [f"⚠️ *{len(items)} DERNIÈRES ERREURS/WARNINGS*\n━━━━━━━━━━━━━━"]
    for ts, lvl, msg in items:
        t = datetime.fromtimestamp(ts).strftime("%d/%m %H:%M:%S")
        e = "🔴" if lvl == "ERROR" or lvl == "CRITICAL" else "🟡"
        lines.append(f"{e} `{t}` {msg[:350]}")
    text = "\n".join(lines)
    try:
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception:
        await update.message.reply_text(text.replace("*","").replace("`",""))


async def cmd_cooldown(update,context):
    if not auth(update): return
    st.cooldown_until=0; st.consec=0; st.daily_pause_until=0
    await update.message.reply_text("✅ Cooldown + pause reset.",parse_mode="Markdown")


async def cmd_reset(update,context):
    if not auth(update): return
    st.running=False
    for j in [st.tick_job,st.price_job,st.macro_job,st.tp_job,st.backup_job,st.recap_job]:
        if j:
            try: j.schedule_removal()
            except: pass
    st.bankroll=50.0; st.bankroll_ref=50.0; st.trades=[]; st.bet=None; st.bet2=None
    for _a in ("eth","sol","xrp"): setattr(st, f"bet_{_a}", None)  # ✅ (21/06) clear slots par crypto
    st.wins=st.losses=st.skipped=st.consec=0; st.pnl=st.streak=st.best_streak=st.worst_streak=0
    st.cooldown_until=0; st.daily_pause_until=0; st.session_start=time.time(); st.pass_reasons=[]
    st.last_conf_score={}; st.last_mom_score=0; st.active_order_id=None; st.active_order_id2=None
    st.active_token_id=None; st.shares_bought=0; st.entry_token_price=0
    st.active_token_id2=None; st.shares_bought2=0; st.entry_token_price2=0
    st.token_price_peak=0; st.trailing_active=False; st.bet_expiry=0; st.bet_expiry2=0
    st.win_streak_count=0; st.conservative_until=0; st.turbo_until=0; st.last_fair={}
    st.c1.clear(); st.c5.clear(); st.c15.clear(); st.c1h.clear(); st.c4h.clear()
    for f in [DATA_FILE,BACKUP_FILE]:
        if os.path.exists(f): os.remove(f)
    await update.message.reply_text("🔄 *Reset complet.*",parse_mode="Markdown")


def _pick_bet_sfx(context):
    """✅ (21/06) Choisit quelle position cibler pour /sell et /sellcheck, PAR CRYPTO.
    Arg explicite 'btc'/'eth'/'sol'/'xrp' → ce slot. Sinon: la 1ère position ouverte."""
    arg = (context.args[0].upper() if context.args else "")
    if arg in ASSETS: return _possfx(arg)
    for a in ASSETS:
        if getattr(st, f"bet{_possfx(a)}") is not None: return _possfx(a)
    return ""

async def _execute_position_exit(context, sfx, reason_label="Sortie"):
    """✅ (23/06) demande user: extrait de cmd_sell pour être réutilisable par le garde de retournement
    automatique (qui n'a pas d'objet `update`). Vend la position active immédiatement (taker — la vitesse
    prime sur le rebate maker dans ce cas), met à jour bankroll/trades/state, envoie l'alerte Telegram.
    Retourne (success: bool, gross_pnl: float|None)."""
    bet = getattr(st, f"bet{sfx}")
    if not bet or st.paper_mode:
        return False, None
    active_token_id = getattr(st, f"active_token_id{sfx}")
    entry_token_price = getattr(st, f"entry_token_price{sfx}")
    shares_bought = getattr(st, f"shares_bought{sfx}")
    if not active_token_id:
        return False, None
    asset = bet.get("asset","?")
    current_price = await poly.get_token_price(active_token_id)
    gain_mult = current_price/entry_token_price if entry_token_price>0 and current_price>0 else 0
    opposite_token = None
    if st.current_market:
        opposite_token = st.current_market.get("token_down") if bet.get("dir")=="UP" else st.current_market.get("token_up")
    result = await poly.sell_position(active_token_id, shares_bought, opposite_token, current_price)
    if not result:
        return False, None
    others_open = any(getattr(st, f"bet{_possfx(a)}") for a in ASSETS if _possfx(a) != sfx)
    clob_bal = None if others_open else await fetch_clob_balance()
    if clob_bal and clob_bal > 0:
        gross = round(clob_bal - st.bankroll, 2)
        st.bankroll = clob_bal
    else:
        gross = round((current_price - entry_token_price) * shares_bought, 2)
        st.bankroll = max(0.0, st.bankroll + gross)
    st.pnl += gross
    won = gross >= 0
    register_trade_result(won)
    st.trades.append({"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
        "conf":bet["conf"],"result":"WIN" if won else "LOSS",
        "entry":bet["entry"],"exit":st.price,"reasoning":reason_label,
        "paper":False,"ts":int(time.time()),"score":bet.get("score",0),
        "fg_value":st.fg.get("value",50),"session":bet.get("session","?"),
        "source":bet.get("source","?"),"aligned_15h1h":True,
        "asset":asset,"entry_token":bet.get("entry_token",0),"t_remaining":bet.get("t_remaining",0),
        "fill_type":bet.get("fill_type","?"),"fee_est":bet.get("fee_est",0)})
    setattr(st, f"bet{sfx}", None); setattr(st, f"active_token_id{sfx}", None); setattr(st, f"active_order_id{sfx}", None)
    setattr(st, f"shares_bought{sfx}", 0); setattr(st, f"entry_token_price{sfx}", 0); setattr(st, f"bet_expiry{sfx}", 0)
    if sfx=="": st.token_price_peak=0; st.trailing_active=False
    emoji = "✅" if won else "❌"
    await send(context.bot,
        f"{emoji} *{reason_label} {asset}*\n"
        f"`{bet['dir']}` | x`{gain_mult:.2f}` | PnL:`{fmt(gross)}$`\n"
        f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`")
    st.backup()
    return True, gross

async def cmd_sell(update,context):
    """✅ v10.19d — Vente manuelle immédiate de la position active (+ slot réservé BTC oracle via /sell reserved)"""
    if not auth(update): return
    sfx = _pick_bet_sfx(context)
    bet = getattr(st, f"bet{sfx}")
    if not bet:
        await update.message.reply_text("❌ Aucune position active."); return
    if st.paper_mode:
        await update.message.reply_text("❌ Paper mode — pas de vente réelle."); return
    if not getattr(st, f"active_token_id{sfx}"):
        await update.message.reply_text("❌ Pas de token actif."); return
    _sell_asset = bet.get("asset","?")
    await update.message.reply_text(f"⏳ Vente {_sell_asset} en cours...")
    ok, gross = await _execute_position_exit(context, sfx, reason_label="Vente manuelle /sell")
    if not ok:
        await update.message.reply_text("⚠️ Vente échouée — réessaie ou attends la résolution auto.")


async def job_reversal_guard(context):
    """✅ (23/06) demande user: GARDE DE RETOURNEMENT — surveille les positions RÉELLES ouvertes en
    continu (toutes les ~2s) et vend immédiatement (taker — la vitesse prime sur le rebate maker ici)
    si le marché part clairement contre notre direction, au lieu d'attendre la résolution et perdre 100%.
    Exige une CONFIRMATION sur plusieurs lectures consécutives (même logique que le lissage OB: un seul
    tick bruité ne suffit pas) — SAUF cascade de liquidation forte contre nous, qui justifie une sortie
    immédiate sans attendre la confirmation (signal rapide et mécaniquement fiable)."""
    if not REVERSAL_GUARD_ENABLED or not st.running or st.killed or st.paper_mode: return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now
    for asset in ASSETS:
        sfx = _possfx(asset)
        bet = getattr(st, f"bet{sfx}", None)
        if not bet: continue
        cfg = _asset_state_attrs(asset)
        oracle_now = getattr(st, cfg["oracle"], 0)
        slot_open = bet.get("slot_open_px", 0)
        if oracle_now <= 0 or slot_open <= 0 or slot_remaining < REVERSAL_MIN_REMAINING_S:
            continue
        delta_pct = (oracle_now - slot_open) / slot_open * 100
        direction = bet.get("dir")
        against_us = (direction=="UP" and delta_pct < -REVERSAL_DELTA_PCT) or (direction=="DOWN" and delta_pct > REVERSAL_DELTA_PCT)
        hist = st.reversal_hist.setdefault(asset, deque(maxlen=REVERSAL_CONFIRM_READS+2))
        hist.append(against_us)
        recent = list(hist)[-REVERSAL_CONFIRM_READS:]
        confirmed = len(recent) >= REVERSAL_CONFIRM_READS and all(recent)
        liq = liq_pressure(asset)
        liq_override = (direction=="UP" and liq < -REVERSAL_LIQ_OVERRIDE_USD) or (direction=="DOWN" and liq > REVERSAL_LIQ_OVERRIDE_USD)
        if confirmed or liq_override:
            reason = "🚨 Sortie auto (cascade liquidation)" if liq_override and not confirmed else "🚨 Sortie auto (retournement confirmé)"
            log.info(f"{asset}: GARDE RETOURNEMENT déclenché — delta={delta_pct:+.3f}% dir={direction} liq={liq:+.0f}$ ({reason})")
            ok, gross = await _execute_position_exit(context, sfx, reason_label=reason)
            if ok:
                st.reversal_hist[asset] = deque(maxlen=REVERSAL_CONFIRM_READS+2)


    """✅ (21/06) Affiche le PnL actuel sans vendre, par crypto (/sellcheck [btc|eth|sol|xrp])."""
    if not auth(update): return
    open_assets = [a for a in ASSETS if getattr(st, f"bet{_possfx(a)}")]
    if not open_assets:
        await update.message.reply_text("❌ Aucune position active."); return
    sfx = _pick_bet_sfx(context)
    bet = getattr(st, f"bet{sfx}")
    if not bet:
        await update.message.reply_text("❌ Aucune position active."); return
    sc_asset = bet.get("asset","?")
    active_token_id = getattr(st, f"active_token_id{sfx}")
    entry_token_price = getattr(st, f"entry_token_price{sfx}")
    shares_bought = getattr(st, f"shares_bought{sfx}")
    bet_expiry = getattr(st, f"bet_expiry{sfx}")
    if not active_token_id:
        await update.message.reply_text("❌ Pas de token actif."); return
    current_price = await poly.get_token_price(active_token_id)
    if current_price <= 0 or entry_token_price <= 0:
        await update.message.reply_text("❌ Prix non disponible."); return
    gain_mult = current_price / entry_token_price
    gross = round((current_price - entry_token_price) * shares_bought, 2)
    emoji = "✅" if gross >= 0 else "❌"
    remaining = int((bet_expiry - time.time())) if bet_expiry > 0 else 0
    others = [a for a in open_assets if a != sc_asset]
    other_hint = f"\n💡 Autres positions ouvertes: {', '.join(others)} — `/sellcheck <crypto>`" if others else ""
    await update.message.reply_text(
        f"💰 *Position actuelle {sc_asset}*\n━━━━━━━━━━━━━━\n"
        f"{emoji} `{bet['dir']}` | x`{gain_mult:.2f}` | PnL:`{fmt(gross)}$`\n"
        f"Token: `{entry_token_price:.3f}$` → `{current_price:.3f}$`\n"
        f"⏰ Expire dans: `{remaining}s`\n\n"
        f"Tape `/sell {sc_asset.lower()}` pour vendre maintenant.{other_hint}",
        parse_mode="Markdown")


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
    await update.message.reply_text(
        f"⚖️ *FAIR VALUE* (Brownien)\n━━━━━━━━━━━━━━\n"
        f"₿`${cur:,.2f}` | Slot open:`${st.slot_open_price:,.2f}`\n"
        f"Δ:`{delta_live:+.3f}%` | ⏰`{t_rem}s` | σ:`{sigma:.4f}`\n\n"
        f"🟢 P(UP):`{p_up*100:.0f}%` | 🔴 P(DOWN):`{(1-p_up)*100:.0f}%`\n\n"
        f"💡 EV≥{FAIR_EDGE_MIN*100:.0f}pts (job_tick, paper/stats uniquement — pas de trading réel)\n"
        f"_(frais taker déduits automatiquement)_",
        parse_mode="Markdown")


async def cmd_backtest(update,context):
    if not auth(update): return
    days=2
    if context.args:
        try: days=max(1,min(7,int(context.args[0])))
        except: pass
    await update.message.reply_text(f"⏳ Backtest {days}j en cours...")
    res=await run_backtest(days)
    await update.message.reply_text(res, parse_mode="Markdown")


async def cmd_resetskips(update,context):
    """v12.9 — Remet à zéro les passes et patterns."""
    if not auth(update): return
    n_passes=len(st.pass_reasons); n_patterns=len(st.oracle_patterns)
    st.pass_reasons.clear(); st.oracle_patterns.clear()
    await update.message.reply_text(
        f"🔄 *Skips réinitialisés*\n  {n_passes} passes supprimées\n  {n_patterns} patterns supprimés\nWR théorique remis à zéro ✅",
        parse_mode="Markdown")


async def cmd_momentum(update,context):
    """v12.9 — Signal momentum BTC T-150s→T-60s en temps réel."""
    if not auth(update): return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now

    pts = list(st.ws_prices)
    if len(pts) < 5:
        await update.message.reply_text("❌ Pas assez de données WS BTC."); return

    def ret_over(secs):
        cutoff = now - secs
        old = [p for t,p in pts if t <= cutoff]
        return (st.ws_price - old[-1]) / old[-1] * 100 if old and old[-1]>0 else 0.0

    ret_60s = ret_over(60)
    ret_30s = ret_over(30)
    ret_10s = ret_over(10)
    ret_3s  = ret_over(3)

    # Status signal
    in_window = 60 <= slot_remaining <= 150
    signal = abs(ret_60s) >= 0.30
    direction = "UP 📈" if ret_60s > 0 else "DOWN 📉"
    mom_ok = (ret_60s > 0 and ret_30s > 0.05) or (ret_60s < 0 and ret_30s < -0.05)
    anti_rev = (ret_60s > 0 and ret_3s > -0.050) or (ret_60s < 0 and ret_3s < 0.050)

    if signal and in_window and mom_ok and anti_rev:
        status = "🚀 *SIGNAL ACTIF* — Momentum trade en cours!"
    elif signal and not in_window:
        status = f"⏳ Signal fort mais hors fenêtre (T-{int(slot_remaining)}s, fenêtre T-150s→T-60s)"
    elif not signal:
        status = f"📡 Pas de signal (ret60s={ret_60s:+.3f}% < ±0.30%)"
    else:
        status = f"⚠️ Signal faible ou momentum contra"

    last_mom = "jamais" if st.momentum_last_slot == 0 else f"slot {st.momentum_last_slot}"

    # ✅ v12.9 — Affichage du filtre trend macro 10min (même logique que job_momentum_btc)
    trend_10m = None
    trend_ok = True
    try:
        klines_10m = await fetch_klines("1m", limit=10, symbol="btcusdt")
        if klines_10m and len(klines_10m) >= 5:
            trend_10m = (klines_10m[-1]["close"] - klines_10m[0]["open"]) / klines_10m[0]["open"] * 100
            if ret_60s > 0 and trend_10m <= -0.10: trend_ok = False
            if ret_60s < 0 and trend_10m >= 0.10: trend_ok = False
    except Exception:
        pass
    trend_txt = f"`{trend_10m:+.3f}%` {'✅' if trend_ok else '❌ contraire'}" if trend_10m is not None else "`indisponible`"

    await update.message.reply_text(
        f"🚀 *MOMENTUM BTC — T-150s→T-60s* _(vue détaillée BTC — trading actif aussi sur ETH/SOL/XRP)_\n━━━━━━━━━━━━━━\n"
        f"Fenêtre: `T-{int(slot_remaining)}s` {'✅ ACTIVE' if in_window else '❌ hors fenêtre'}\n\n"
        f"₿ BTC:`${st.ws_price:,.2f}`\n"
        f"  Ret 60s:`{ret_60s:+.3f}%` {'✅' if abs(ret_60s)>=0.30 else '❌'} (seuil ±0.30%)\n"
        f"  Ret 30s:`{ret_30s:+.3f}%` {'✅' if mom_ok else '❌'} (momentum continu)\n"
        f"  Ret 10s:`{ret_10s:+.3f}%`\n"
        f"  Ret 3s:`{ret_3s:+.3f}%` {'✅' if anti_rev else '❌'} (anti-reversal)\n"
        f"  Trend 10m: {trend_txt} (filtre macro)\n\n"
        f"Direction: {direction if signal else '— neutre'}\n"
        f"Token cible: 0.55$→0.65$ | EV min: {ORACLE_EDGE_MIN*100:.0f}%\n\n"
        f"{status}\n"
        f"Dernier trade momentum: `{last_mom}`",
        parse_mode="Markdown")


async def cmd_mean_reversion(update,context):
    """v12.9 — Signal mean-reversion BTC en temps réel (Bollinger squeeze + spike fade)."""
    if not auth(update): return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now

    pts = list(st.ws_prices)
    if len(pts) < 20:
        await update.message.reply_text("❌ Pas assez de données WS BTC."); return

    window_pts = [p for t,p in pts if now-t <= 60]
    if len(window_pts) < 10:
        await update.message.reply_text("❌ Pas assez de points sur 60s."); return

    sma = sum(window_pts) / len(window_pts)
    variance = sum((p-sma)**2 for p in window_pts) / len(window_pts)
    std = variance ** 0.5
    upper = sma + 2*std
    lower = sma - 2*std
    bandwidth = (upper - lower) / sma * 100 if sma > 0 else 0

    SQUEEZE_MAX_BANDWIDTH = 0.12
    in_window = 60 <= slot_remaining <= 150
    is_squeeze = bandwidth <= SQUEEZE_MAX_BANDWIDTH
    cur_price = st.ws_price

    if cur_price >= upper:
        spike_dir = "DOWN 📉 (surextension haussière)"
        overext = (cur_price - upper) / sma * 100 if sma>0 else 0
    elif cur_price <= lower:
        spike_dir = "UP 📈 (surextension baissière)"
        overext = (lower - cur_price) / sma * 100 if sma>0 else 0
    else:
        spike_dir = "— aucun spike"
        overext = 0.0

    if not is_squeeze:
        status = f"📊 Régime EXPANSION (bandwidth {bandwidth:.3f}%>0.12%) — pas de mean-reversion, laisse momentum gérer"
    elif spike_dir == "— aucun spike":
        status = "😴 Squeeze actif mais pas de spike actuellement"
    elif not in_window:
        status = f"⏳ Spike détecté mais hors fenêtre (T-{int(slot_remaining)}s, fenêtre T-150s→T-60s)"
    else:
        status = "🔄 *SIGNAL ACTIF* — Mean-reversion en cours d'évaluation!"

    last_mr = "jamais" if st.meanrev_last_slot == 0 else f"slot {st.meanrev_last_slot}"

    await update.message.reply_text(
        f"🔄 *MEAN-REVERSION BTC — T-150s→T-60s* _(vue détaillée BTC — trading actif aussi sur ETH/SOL/XRP)_\n━━━━━━━━━━━━━━\n"
        f"Fenêtre: `T-{int(slot_remaining)}s` {'✅ ACTIVE' if in_window else '❌ hors fenêtre'}\n\n"
        f"₿ BTC:`${st.ws_price:,.2f}`\n"
        f"  Bollinger Bandwidth:`{bandwidth:.3f}%` {'✅ squeeze' if is_squeeze else '❌ expansion'} (seuil ≤0.12%)\n"
        f"  Bandes: `{lower:,.2f}` → `{upper:,.2f}`\n"
        f"  Spike: {spike_dir} (overext `{overext:+.3f}%`)\n\n"
        f"Token cible: 0.51$→0.70$ | EV min: {ORACLE_EDGE_MIN*100:.0f}% | Kelly: 1-3% BR\n\n"
        f"{status}\n"
        f"Dernier trade mean-rev: `{last_mr}`",
        parse_mode="Markdown")


async def cmd_regime(update,context):
    """v12.9 — Diagnostic instantané RANGE (squeeze) vs TREND (expansion) + biais oracle sur BTC/ETH/SOL/XRP.
    Réutilise le même calcul Bollinger Bandwidth que job_mean_reversion_*/job_confluence_*, lecture seule, instantané."""
    if not auth(update): return
    now = time.time()
    lines_out = ["📐 *RÉGIME MARCHÉ — instantané*\n━━━━━━━━━━━━━━"]

    for asset in ("BTC","ETH","SOL","XRP"):
        cfg = _asset_state_attrs(asset)
        cur_price = getattr(st, cfg["price"])
        oracle_price = getattr(st, cfg["oracle"])
        pts = list(getattr(st, cfg["prices"]))
        window_pts = [p for t,p in pts if now-t <= 60]

        if cur_price <= 0 or len(window_pts) < 10:
            lines_out.append(f"\n{'₿' if asset=='BTC' else asset}: `données insuffisantes`")
            continue

        sma = sum(window_pts) / len(window_pts)
        variance = sum((p-sma)**2 for p in window_pts) / len(window_pts)
        std = variance ** 0.5
        bandwidth = ((sma+2*std) - (sma-2*std)) / sma * 100 if sma > 0 else 0
        is_squeeze = bandwidth <= 0.12

        ret_60s = ((cur_price - window_pts[0]) / window_pts[0] * 100) if window_pts[0] > 0 else 0.0

        gap_txt = "`indisponible`"
        if oracle_price > 0:
            gap_pct = (cur_price - oracle_price) / oracle_price * 100
            oracle_dir = "UP 🟢" if gap_pct > 0 else "DOWN 🔴" if gap_pct < 0 else "neutre"
            strength = "fort" if abs(gap_pct) >= TDS_GAP_STRONG else ("faible" if abs(gap_pct) < TDS_GAP_MIN else "modéré")
            gap_txt = f"`{gap_pct:+.3f}%` {oracle_dir} ({strength})"

        tag = "🟦 RANGE (squeeze)" if is_squeeze else "🟥 TREND (expansion)"
        reco = "→ stratégie active: mean-reversion" if is_squeeze else "→ stratégie active: momentum"
        emoji = "₿" if asset=="BTC" else asset
        lines_out.append(f"\n{emoji} `${cur_price:,.4f}` | BW:`{bandwidth:.3f}%` | ret60s:`{ret_60s:+.3f}%`\n{tag}\nBiais oracle: {gap_txt}\n_{reco}_")

    lines_out.append(f"\n\n_Seuil squeeze: BW≤0.12% (à calibrer avec données réelles)_")
    await update.message.reply_text("\n".join(lines_out), parse_mode="Markdown")


async def cmd_confluence(update,context):
    """v12.9 — Diagnostic instantané CONFLUENCE (4ème stratégie /conf) sur BTC/ETH/SOL/XRP.
    Montre le score TDS en temps réel = oracle_score × setup_score × (1-noise), même calcul que job_confluence_*."""
    if not auth(update): return
    now = time.time()
    cur_slot = int(now // 300) * 300
    slot_remaining = cur_slot + 300 - now
    in_window = 60 <= slot_remaining <= 150
    lines_out = [f"🎯 *CONFLUENCE — TDS instantané*\n━━━━━━━━━━━━━━\n"
                 f"Fenêtre: `T-{int(slot_remaining)}s` {'✅ ACTIVE' if in_window else '❌ hors fenêtre (T-150s→T-60s)'}"]

    for asset in ("BTC","ETH","SOL","XRP"):
        cfg = _asset_state_attrs(asset)
        spot = getattr(st, cfg["price"])
        oracle = getattr(st, cfg["oracle"])
        pts = list(getattr(st, cfg["prices"]))
        emoji = "₿" if asset=="BTC" else asset

        if spot <= 0 or oracle <= 0 or len(pts) < 20:
            lines_out.append(f"\n{emoji}: `données insuffisantes`")
            continue

        gap_pct = (spot - oracle) / oracle * 100
        if abs(gap_pct) < TDS_GAP_MIN:
            lines_out.append(f"\n{emoji}: gap `{gap_pct:+.3f}%` trop faible → pas de biais oracle")
            continue
        oracle_dir = "UP" if gap_pct > 0 else "DOWN"
        oracle_score = min(1.0, abs(gap_pct) / TDS_GAP_STRONG)

        window_pts = [p for t,p in pts if now-t <= 60]
        if len(window_pts) < 10:
            lines_out.append(f"\n{emoji}: `pas assez de points sur 60s`")
            continue
        sma = sum(window_pts) / len(window_pts)
        variance = sum((p-sma)**2 for p in window_pts) / len(window_pts)
        std = variance ** 0.5
        upper = sma + 2*std; lower = sma - 2*std
        bandwidth = (upper-lower)/sma*100 if sma>0 else 0
        is_squeeze = bandwidth <= 0.12

        def ret_over(secs, _pts=pts, _now=now, _spot=spot):
            cutoff = _now - secs
            old = [p for t,p in _pts if t <= cutoff]
            return (_spot - old[-1]) / old[-1] * 100 if old and old[-1] > 0 else 0.0
        ret_60s = ret_over(60); ret_30s = ret_over(30); ret_10s = ret_over(10); ret_3s = ret_over(3)

        setup_score = 0.0; setup_dir = None; setup_type = None
        if is_squeeze:
            if spot >= upper: cand_dir="DOWN"; overext=(spot-upper)/sma*100
            elif spot <= lower: cand_dir="UP"; overext=(lower-spot)/sma*100
            else: cand_dir=None; overext=0.0
            if cand_dir is not None and cand_dir == oracle_dir:
                setup_score = min(1.0, min(1.0, overext/TDS_OVEREXT_STRONG) * _tds_adaptive_weight("meanrev"))
                setup_dir = cand_dir; setup_type = "meanrev"
        else:
            if abs(ret_60s) >= 0.30:
                cand_dir = "UP" if ret_60s>0 else "DOWN"
                if cand_dir == oracle_dir:
                    confirm = 1.0 if (cand_dir=="UP" and ret_30s>=0.05) or (cand_dir=="DOWN" and ret_30s<=-0.05) else 0.6
                    setup_score = min(1.0, min(1.0, abs(ret_60s)/TDS_RET60S_STRONG)*confirm * _tds_adaptive_weight("momentum"))
                    setup_dir = cand_dir; setup_type = "momentum"

        noise_penalty = 0.5 if ((ret_10s>0 and ret_3s<-0.030) or (ret_10s<0 and ret_3s>0.030)) else 0.0
        tds = oracle_score * setup_score * (1-noise_penalty)
        if setup_dir is not None and tds >= TDS_MIN_SCORE:
            status = "🟢 ACTIF" if in_window else "⏳ setup valide mais hors fenêtre"
        else:
            status = "⚪ pas de setup"
        conf_preview = ""
        if setup_dir is not None and tds >= TDS_MIN_SCORE:
            confidence = min(1.3, max(0.7, 0.7 + (tds - TDS_MIN_SCORE) / (1.0 - TDS_MIN_SCORE) * 0.6))
            conf_preview = f" | Sizing:`{confidence:.2f}x`"

        lines_out.append(
            f"\n{emoji} oracle:`{oracle_dir}` score:`{oracle_score:.2f}` | régime:`{'squeeze' if is_squeeze else 'expansion'}`\n"
            f"Setup:`{setup_type or '—'}` score:`{setup_score:.2f}` | Noise:`{noise_penalty:.1f}`\n"
            f"TDS:`{tds:.2f}` (seuil {TDS_MIN_SCORE}) {status}{conf_preview}")

    lines_out.append(f"\n\n_Poids adaptatifs neutres tant que <{TDS_ADAPT_MIN_SAMPLE} trades/branche_")
    await update.message.reply_text("\n".join(lines_out), parse_mode="Markdown")


async def cmd_slots(update,context):
    """✅ v12.9 — SLOT RECORDER (/slots): statistiques de TOUS les slots 5min résolus, indépendamment du trading.
    Répond à 'quelles conditions donnent UP vs DOWN?'. Source: oracle Chainlink (règle officielle Polymarket).
    En tête: PRÉDICTION du slot EN COURS sur les 4 cryptos (agrégation des signaux disponibles)."""
    if not auth(update): return

    # ── PRÉDICTION SLOT EN COURS (temps réel) ──
    now=time.time(); slot_rem=300-(now%300)
    pred_lines=["🔮 *PRÉDICTION SLOT EN COURS* (T-`%ds`)" % int(slot_rem)]
    for a,e,pdq_attr,o_attr,so_attr,px_attr in [
        ("BTC","₿","ws_prices","oracle_price","oracle_slot_open","ws_price"),
        ("ETH","Ξ","eth_ws_prices","eth_oracle_price","eth_oracle_slot_open","eth_price"),
        ("SOL","◎","sol_ws_prices","sol_oracle_price","sol_oracle_slot_open","sol_price"),
        ("XRP","✕","xrp_ws_prices","xrp_oracle_price","xrp_oracle_slot_open","xrp_price")]:
        oracle=getattr(st,o_attr,0); slot_open=getattr(st,so_attr,0); spot=getattr(st,px_attr,0)
        pdq=list(getattr(st,pdq_attr,[]))
        if oracle<=0 or slot_open<=0:
            pred_lines.append(f"{e} {a}: `données indispo`"); continue
        # Signaux: delta oracle (sens du slot jusqu'ici), gap spot/oracle, dual TA
        delta=(oracle-slot_open)/slot_open*100
        gap=(spot-oracle)/oracle*100 if spot>0 else 0
        votes_up=0; votes_dn=0; sig_txt=[]
        # 1) delta du slot (où en est le prix vs ouverture)
        if delta>0.005: votes_up+=1; sig_txt.append(f"Δ+{delta:.3f}%")
        elif delta<-0.005: votes_dn+=1; sig_txt.append(f"Δ{delta:.3f}%")
        # 2) dual model TA
        dd=None
        if len(pdq)>=35:
            _s,_d,det=compute_ta_score([{"price":p,"ts":t} for t,p in pdq],a)
            dd=det.get("dual_dir"); mh=det.get("macd_hist",0)
            if dd=="UP": votes_up+=1; sig_txt.append("dual↑")
            elif dd=="DOWN": votes_dn+=1; sig_txt.append("dual↓")
            if mh>0: votes_up+=1; sig_txt.append("MACD+")
            elif mh<0: votes_dn+=1; sig_txt.append("MACD-")
        # 3) order book imbalance (déséquilibre acheteurs/vendeurs Polymarket)
        ob_map={"BTC":getattr(st,"ob_imbalance",0),"ETH":getattr(st,"eth_ob_imbalance",0),
                "SOL":getattr(st,"sol_ob_imbalance",0),"XRP":getattr(st,"xrp_ob_imbalance",0)}
        obv=ob_map.get(a,0)
        if obv>0.15: votes_up+=1; sig_txt.append(f"OB↑{obv:.2f}")
        elif obv<-0.15: votes_dn+=1; sig_txt.append(f"OB↓{obv:.2f}")
        # 4) microprice signal (penche vers le côté lourd du carnet, pondéré spread)
        micro_map={"BTC":getattr(st,"ob_micro_signal",0),"ETH":getattr(st,"eth_ob_micro_signal",0),
                   "SOL":getattr(st,"sol_ob_micro_signal",0),"XRP":getattr(st,"xrp_ob_micro_signal",0)}
        msig=micro_map.get(a,0)
        if msig>0.002: votes_up+=1; sig_txt.append("micro↑")
        elif msig<-0.002: votes_dn+=1; sig_txt.append("micro↓")
        # 5) OFI (flux dynamique)
        ofi_map={"BTC":getattr(st,"ob_ofi",0),"ETH":getattr(st,"eth_ob_ofi",0),
                 "SOL":getattr(st,"sol_ob_ofi",0),"XRP":getattr(st,"xrp_ob_ofi",0)}
        ofiv=ofi_map.get(a,0)
        if ofiv>0: votes_up+=1; sig_txt.append("OFI+")
        elif ofiv<0: votes_dn+=1; sig_txt.append("OFI-")
        # Verdict
        if votes_up>votes_dn: verdict=f"🟢 UP ({votes_up}/{votes_up+votes_dn})"
        elif votes_dn>votes_up: verdict=f"🔴 DOWN ({votes_dn}/{votes_up+votes_dn})"
        else: verdict="⚪ indécis"
        pred_lines.append(f"{e} {a}: {verdict} | _{', '.join(sig_txt) or 'aucun signal'}_")
    pred_lines.append("_⚠️ Indication seulement — ce n'est PAS une garantie, le 5min reste très bruité._")

    recs = list(st.slot_records)
    if not recs:
        msg_empty = ("\n".join(pred_lines) +
            "\n\n📊 *SLOT RECORDER*\n━━━━━━━━━━━━━━\n"
            "Aucun slot résolu enregistré pour l'instant.\n"
            "_Le journal s'enregistre à chaque bascule de slot (~toutes les 5min par asset). Reviens dans 10-15 min._")
        try:
            await update.message.reply_text(msg_empty, parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(msg_empty.replace("*","").replace("`","").replace("_",""))
        return

    def wr_up(sample):
        n=len(sample)
        if n==0: return 0,0
        ups=sum(1 for r in sample if r["result"]=="UP")
        return ups/n*100, n

    lines=pred_lines+["\n📊 *SLOT RECORDER — tous slots résolus*\n━━━━━━━━━━━━━━"]
    pct,n = wr_up(recs)
    lines.append(f"Total: `{n}` slots | UP `{pct:.0f}%` / DOWN `{100-pct:.0f}%`")
    if abs(pct-50) < 3:
        lines.append("_→ ~équilibré: pas de biais directionnel structurel (normal)_")
    else:
        lines.append(f"_→ biais {'haussier' if pct>50 else 'baissier'} sur la période échantillonnée_")

    # Par asset
    lines.append("\n*Par asset:*")
    for a,e in [("BTC","₿"),("ETH","Ξ"),("SOL","◎"),("XRP","✕")]:
        ap=[r for r in recs if r["asset"]==a]
        if ap:
            p,nn=wr_up(ap); lines.append(f"{e} {a}: `{nn}` slots | UP `{p:.0f}%`")

    # Par régime — la vraie question: est-ce qu'un régime prédit la direction?
    lines.append("\n*Par régime:*")
    for reg in ("squeeze","expansion"):
        rp=[r for r in recs if r.get("regime")==reg]
        if rp:
            p,nn=wr_up(rp); lines.append(f"  {reg}: `{nn}` | UP `{p:.0f}%`")

    # Valeur prédictive: quand RSI<35 (survendu) → plus de UP? quand MACD>0 → plus de UP?
    lines.append("\n*Valeur prédictive (UP%):*")
    rsi_lo=[r for r in recs if r.get("rsi",50)<35]
    rsi_hi=[r for r in recs if r.get("rsi",50)>65]
    if rsi_lo: p,nn=wr_up(rsi_lo); lines.append(f"  RSI<35 (survendu): `{nn}` | UP `{p:.0f}%`")
    if rsi_hi: p,nn=wr_up(rsi_hi); lines.append(f"  RSI>65 (suracheté): `{nn}` | UP `{p:.0f}%`")
    macd_pos=[r for r in recs if r.get("macd",0)>0]
    macd_neg=[r for r in recs if r.get("macd",0)<0]
    if macd_pos: p,nn=wr_up(macd_pos); lines.append(f"  MACD>0: `{nn}` | UP `{p:.0f}%`")
    if macd_neg: p,nn=wr_up(macd_neg); lines.append(f"  MACD<0: `{nn}` | UP `{p:.0f}%`")
    # ✅ v12.9 — Order book imbalance: déséquilibre acheteurs/vendeurs prédit-il la direction?
    ob_buy=[r for r in recs if r.get("ob",0)>0.15]
    ob_sell=[r for r in recs if r.get("ob",0)<-0.15]
    if ob_buy: p,nn=wr_up(ob_buy); lines.append(f"  📖 OB acheteurs (>0.15): `{nn}` | UP `{p:.0f}%`")
    if ob_sell: p,nn=wr_up(ob_sell); lines.append(f"  📖 OB vendeurs (<-0.15): `{nn}` | UP `{p:.0f}%`")
    # ✅ v12.9 — Microprice signal: penche-t-il vers la bonne direction? (meilleur que l'imbalance brute selon la littérature)
    micro_up=[r for r in recs if r.get("micro",0)>0.002]
    micro_dn=[r for r in recs if r.get("micro",0)<-0.002]
    if micro_up: p,nn=wr_up(micro_up); lines.append(f"  🎯 microprice↑ (>0.002): `{nn}` | UP `{p:.0f}%`")
    if micro_dn: p,nn=wr_up(micro_dn); lines.append(f"  🎯 microprice↓ (<-0.002): `{nn}` | UP `{p:.0f}%`")
    # ✅ v12.9 — OFI (flux dynamique du carnet): >0 pression acheteuse, <0 pression vendeuse
    ofi_pos=[r for r in recs if r.get("ofi",0)>0]
    ofi_neg=[r for r in recs if r.get("ofi",0)<0]
    if ofi_pos: p,nn=wr_up(ofi_pos); lines.append(f"  🌊 OFI>0 (flux acheteur): `{nn}` | UP `{p:.0f}%`")
    if ofi_neg: p,nn=wr_up(ofi_neg); lines.append(f"  🌊 OFI<0 (flux vendeur): `{nn}` | UP `{p:.0f}%`")
    # ✅ v12.9 — Spread & profondeur: contexte de liquidité (pas prédictif de direction, mais d'exécution)
    spr_vals=[r.get("spread",0) for r in recs if r.get("spread",0)>0]
    dep_vals=[r.get("depth",0) for r in recs if r.get("depth",0)>0]
    if spr_vals or dep_vals:
        lines.append("\n*Liquidité (exécution):*")
        if spr_vals:
            avg_spr=sum(spr_vals)/len(spr_vals)
            lines.append(f"  Spread moyen: `{avg_spr*100:.1f}¢` (n={len(spr_vals)}) — large=EV réel pire")
        if dep_vals:
            avg_dep=sum(dep_vals)/len(dep_vals)
            lines.append(f"  Profondeur moyenne: `{avg_dep:.0f}$` (n={len(dep_vals)}) — faible=ordre dur à remplir")
    # Dual model: quand dual=UP, le slot finit-il vraiment UP?
    dual_up=[r for r in recs if r.get("dual")=="UP"]
    dual_dn=[r for r in recs if r.get("dual")=="DOWN"]
    if dual_up: p,nn=wr_up(dual_up); lines.append(f"  🔀 dual=UP: `{nn}` | UP réel `{p:.0f}%`")
    if dual_dn: p,nn=wr_up(dual_dn); lines.append(f"  🔀 dual=DOWN: `{nn}` | UP réel `{p:.0f}%` (donc DOWN `{100-p:.0f}%`)")

    # Avertissement échantillon/biais
    sessions = {}
    for r in recs:
        s=r.get("session","?"); sessions[s]=sessions.get(s,0)+1
    if sessions:
        dom = max(sessions.items(), key=lambda x:x[1])
        if dom[1]/len(recs) >= 0.6:
            sess_safe = dom[0].replace("_"," ")  # éviter que ASIA_LATE casse l'italique Markdown
            lines.append(f"\n⚠️ _{dom[1]/len(recs)*100:.0f}% des slots en session {sess_safe} — biais possible, à confirmer sur d'autres sessions_")
    # ✅ v12.9 — Brier score sur les trades réels (calibration de nos probabilités)
    bs_slots = compute_brier_score(st.trades)
    if bs_slots:
        v = "🟢 calibré" if bs_slots["brier"]<0.20 else ("🟡 limite" if bs_slots["brier"]<=0.25 else "🔴 mal calibré")
        lines.append(f"\n🎯 *Brier score:* `{bs_slots['brier']}` {v} (conf `{bs_slots['avg_conf']*100:.0f}%` vs WR `{bs_slots['realized_wr']*100:.0f}%`, n={bs_slots['n']})")
    # ✅ v12.9 — TIMING DE PRICING: à quel T-Xs le token dépasse 0.95$? (réponds à 'entre-t-on trop tard?')
    pt = list(st.price_timing)
    if pt:
        lines.append("\n⏱️ *Timing de pricing (token→0.95$):*")
        for a,e in [("BTC","₿"),("ETH","Ξ"),("SOL","◎"),("XRP","✕")]:
            ap=[r["t_remaining_at_095"] for r in pt if r["asset"]==a]
            if ap:
                avg_t=sum(ap)/len(ap)
                # token max moyen pour cet asset
                maxes=[v for (k_a,k_s),v in st.price_timing_max.items() if k_a==a]
                avg_max=sum(maxes)/len(maxes) if maxes else 0
                warn=" ⚠️ avant ta fenêtre!" if avg_t>ORACLE_WINDOW_START else ""
                lines.append(f"  {e} {a}: T-`{avg_t:.0f}s` en moy (n={len(ap)}) | tok max moy `{avg_max:.2f}$`{warn}")
        lines.append(f"  _Ta fenêtre oracle: T-{ORACLE_WINDOW_START}s→T-{ORACLE_WINDOW_END}s. Si le token atteint 0.95$ AVANT T-{ORACLE_WINDOW_START}s, tu entres trop tard._")
    lines.append(f"\n_Un indicateur n'a de valeur que s'il s'écarte nettement de 50% sur un gros échantillon (n≥100)._")

    text = "\n".join(lines)
    try:
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception:
        # Fallback: si le Markdown casse (caractère non apparié) ou message trop long, envoyer en clair
        clean = text.replace("*","").replace("`","").replace("_","")
        if len(clean) > 4000: clean = clean[:4000] + "\n…(tronqué)"
        await update.message.reply_text(clean)


async def cmd_flow(update,context):
    """✅ v12.9 — ORDER FLOW (/flow): derniers trades réels sur le marché Polymarket des 4 cryptos.
    Montre si du smart money entre (gros trades) et de quel côté, juste avant la résolution.
    Lecture seule, best-effort. Complète OB imbalance (statique) avec le flux (dynamique)."""
    if not auth(update): return
    now=time.time(); cur_slot=int(now//300)*300; slot_rem=300-(now%300)
    lines=[f"🌊 *ORDER FLOW — marché Polymarket* (T-`{int(slot_rem)}s`)\n━━━━━━━━━━━━━━"]
    for asset,e in [("BTC","₿"),("ETH","Ξ"),("SOL","◎"),("XRP","✕")]:
        cfg=_asset_state_attrs(asset)
        try:
            market=await poly.get_market_by_slug(f"{cfg['slug']}-{cur_slot}")
            if not market:
                lines.append(f"\n{e} {asset}: `marché non trouvé`"); continue
            trades=await poly.get_recent_trades(market["token_up"], limit=15)
            if not trades:
                lines.append(f"\n{e} {asset}: `pas de trades récents`"); continue
            buy_vol=sum(t["size"] for t in trades if "buy" in str(t["side"]).lower())
            sell_vol=sum(t["size"] for t in trades if "sell" in str(t["side"]).lower())
            tot=buy_vol+sell_vol
            big=max(trades, key=lambda t:t["size"]) if trades else None
            flow_dir="🟢 acheteur" if buy_vol>sell_vol*1.3 else ("🔴 vendeur" if sell_vol>buy_vol*1.3 else "⚪ équilibré")
            line=f"\n{e} {asset}: {flow_dir} | {len(trades)} trades"
            if tot>0: line+=f" | achat `{buy_vol/tot*100:.0f}%`"
            if big and big["size"]>0: line+=f"\n   gros: `{big['size']:.0f}` @ `{big['price']:.2f}$`"
            lines.append(line)
        except Exception as ex:
            lines.append(f"\n{e} {asset}: `erreur lecture`"); log.debug(f"flow {asset}: {ex}")
    lines.append("\n_Order flow = trades réels Polymarket (≠ prix spot Binance). Gros trade d'un côté = smart money possible._")
    text="\n".join(lines)
    try:
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception:
        await update.message.reply_text(text.replace("*","").replace("`","").replace("_",""))


async def cmd_sessionstats(update,context):
    """v12.9 — WR théorique des skips segmenté par session (Asia/EU/US).
    Évite les conclusions biaisées par une seule session (ex: nuit calme)."""
    if not auth(update): return
    _resolve_pending_passes()

    resolved = [p for p in st.pass_reasons if p.get("resolved") in ("WIN","LOSS")]
    if not resolved:
        await update.message.reply_text("❌ Aucune passe résolue encore."); return

    by_session = {}
    for p in resolved:
        s = p.get("session", "?")
        by_session.setdefault(s, {"W":0,"L":0})
        if p.get("resolved") == "WIN": by_session[s]["W"] += 1
        else: by_session[s]["L"] += 1

    order = ["US_OPEN","US_AFTERNOON","EU_OPEN","US_CLOSE","ASIA_LATE","ASIA_EARLY","OVERNIGHT","?"]
    label = {"US_OPEN":"🇺🇸 US Open (14-17h)","US_AFTERNOON":"🇺🇸 US PM (17-20h)",
              "EU_OPEN":"🇪🇺 EU Open (9-14h)","US_CLOSE":"🌆 US Close (20-22h)",
              "ASIA_LATE":"🌏 Asia Late (7-9h)","ASIA_EARLY":"🌏 Asia Early (1-7h)",
              "OVERNIGHT":"🌙 Overnight (22-1h)","?":"❓ Inconnu"}

    lines = ["📊 *WR théorique par SESSION*\n━━━━━━━━━━━━━━"]
    total_w, total_l = 0, 0
    for s in order:
        if s not in by_session: continue
        d = by_session[s]
        n = d["W"] + d["L"]
        if n == 0: continue
        wr = d["W"]/n*100
        total_w += d["W"]; total_l += d["L"]
        bar = "█"*int(wr//10) + "░"*(10-int(wr//10))
        lines.append(f"{label.get(s,s)}\n  `{bar}` {wr:.0f}% ({d['W']}W/{d['L']}L, n={n})")

    total = total_w + total_l
    lines.append(f"\n📈 *Global*: {total_w/max(total,1)*100:.0f}% ({total_w}W/{total_l}L, n={total})")
    lines.append("\n⚠️ _Une session avec n<30 n'est pas statistiquement fiable._")
    lines.append("_Une session biaisée (forte tendance) peut fausser le WR théorique pour TOUTES les sessions confondues._")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


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
    if eth_sig and eth_ok and in_window:
        eth_rec=f"⚡ Signal ETH *{eth_sig}* T-`{int(slot_remaining)}s`"
    elif eth_sig and eth_ok:
        eth_rec=f"⏳ Signal ETH *{eth_sig}* — hors fenêtre (T-`{int(slot_remaining)}s`)"
    else:
        eth_rec="📡 Pas de signal ETH"
    # SOL oracle
    sol_o=st.sol_oracle_price; sol_so=st.sol_oracle_slot_open
    sol_d=(sol_o-sol_so)/sol_so*100 if sol_so>0 else 0
    sol_g=(st.sol_price-sol_o)/sol_o*100 if sol_o>0 and st.sol_price>0 else 0
    sol_ok=sol_o>0 and now-st.sol_oracle_ts<15
    sol_sig="UP" if sol_d>ORACLE_ENTRY_DELTA else ("DOWN" if sol_d<-ORACLE_ENTRY_DELTA else None)
    if sol_sig and sol_ok and in_window:
        sol_rec=f"⚡ Signal SOL *{sol_sig}* T-`{int(slot_remaining)}s`"
    elif sol_sig and sol_ok:
        sol_rec=f"⏳ Signal SOL *{sol_sig}* — hors fenêtre (T-`{int(slot_remaining)}s`)"
    else:
        sol_rec="📡 Pas de signal SOL"
    # XRP oracle
    xrp_o=st.xrp_oracle_price; xrp_so=st.xrp_oracle_slot_open
    xrp_d=(xrp_o-xrp_so)/xrp_so*100 if xrp_so>0 else 0
    xrp_g=(st.xrp_price-xrp_o)/xrp_o*100 if xrp_o>0 and st.xrp_price>0 else 0
    xrp_ok=xrp_o>0 and now-st.xrp_oracle_ts<15
    xrp_sig="UP" if xrp_d>ORACLE_ENTRY_DELTA else ("DOWN" if xrp_d<-ORACLE_ENTRY_DELTA else None)
    if xrp_sig and xrp_ok and in_window:
        xrp_rec=f"⚡ Signal XRP *{xrp_sig}* T-`{int(slot_remaining)}s`"
    elif xrp_sig and xrp_ok:
        xrp_rec=f"⏳ Signal XRP *{xrp_sig}* — hors fenêtre (T-`{int(slot_remaining)}s`)"
    else:
        xrp_rec="📡 Pas de signal XRP"
    # ✅ v12.9 — MACD + dual model BTC temps réel
    ta_line = ""
    try:
        bpts = list(st.ws_prices)
        if len(bpts) >= 35:
            ph = [{"price":p,"ts":t} for t,p in bpts]
            _ts, _td, _tdet = compute_ta_score(ph, "BTC")
            mh = _tdet.get("macd_hist",0); rsi_v = _tdet.get("rsi",50)
            us = _tdet.get("up_score",0); ds = _tdet.get("down_score",0); dd = _tdet.get("dual_dir")
            macd_emoji = "🟢" if mh>0 else ("🔴" if mh<0 else "⚪")
            dual_txt = f"`{dd}`" if dd else "`neutre`"
            ta_line = (f"\n📊 TA BTC | RSI:`{rsi_v:.0f}` | MACD:{macd_emoji}`{mh:+.4f}`\n"
                       f"  🔀 Dual: UP`{us:.1f}` vs DOWN`{ds:.1f}` → {dual_txt}\n")
            # ✅ v12.9 — spread + profondeur BTC temps réel
            _spr=getattr(st,"ob_spread",0); _dep=getattr(st,"ob_depth",0)
            if _spr>0 or _dep>0:
                ta_line += f"  📖 Spread:`{_spr*100:.1f}¢` | Profondeur:`{_dep:.0f}$`\n"
            _msig=getattr(st,"ob_micro_signal",0); _ofi=getattr(st,"ob_ofi",0)
            if _msig!=0 or _ofi!=0:
                md="↑" if _msig>0 else ("↓" if _msig<0 else "—")
                od="+" if _ofi>0 else ("-" if _ofi<0 else "0")
                ta_line += f"  🎯 Microprice:`{md}` ({_msig:+.4f}) | 🌊 OFI:`{od}` ({_ofi:+.1f})\n"
    except Exception: pass
    # ✅ (22/06) demande user: funding rate + pression liquidations (nouveaux outils oracle lag)
    liq_line = ""
    try:
        rows = []
        for a in ("BTC","ETH","SOL","XRP"):
            fr = st.funding_rate.get(a); fts = st.funding_ts.get(a, 0)
            liq = liq_pressure(a)
            fr_txt = f"{fr*100:+.3f}%" if fr is not None and now-fts<180 else "n/a"
            liq_txt = f"{liq:+,.0f}$" if liq else "0$"
            liq_emoji = "🟢" if liq>0 else ("🔴" if liq<0 else "⚪")
            rows.append(f"  {a}: funding `{fr_txt}` | liq {liq_emoji}`{liq_txt}`")
        liq_line = "💸 *Funding + Liquidations* (30s)\n" + "\n".join(rows) + "\n\n"
    except Exception: pass
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
            f"{ta_line}"
            f"{liq_line}"
            f"WS: {' | '.join(srcs)}",
            parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Erreur oracle: {e}")


async def cmd_calib(update,context):
    """✅ v10.23 — État de la calibration sigma"""
    if not auth(update): return
    try:
        factor, desc = calibrate_sigma()
        _, report = calibration_report()
        msg = (f"🎚 *CALIBRATION σ*\n━━━━━━━━━━━━━━\n"
               f"Facteur actuel:`×{st.calib_factor:.2f}` | VOL_SAFETY effectif:`{VOL_SAFETY*st.calib_factor:.2f}`\n"
               f"{desc}\n\n"
               f"📊 *Proba prédite vs WR réel* (trades réels)\n{report}\n\n"
               f"_×N = facteur correctif suggéré (WR réel / proba prédite)_")
    except Exception as e:
        log.error(f"cmd_calib: {e}")
        msg = f"⚠️ /calib erreur: {e}"
    await reply_md(update, msg)

async def cmd_edge(update,context):
    """✅ Scorecard d'edge par stratégie (rentabilité réelle + significativité)."""
    if not auth(update): return
    await reply_md(update, edge_scorecard())

async def cmd_slotedge(update,context):
    """✅ #2 — Pouvoir prédictif réel des signaux, miné depuis slot_records.
    Usage: /slotedge [BTC|ETH|SOL|XRP]"""
    if not auth(update): return
    arg = (context.args[0].upper() if getattr(context,"args",None) else None)
    asset = arg if arg in ("BTC","ETH","SOL","XRP") else None
    await reply_md(update, slot_edge_analysis(asset))

async def cmd_exec(update,context):
    """✅ #1 — Qualité d'exécution (maker/taker/non-rempli) + fuite de frais."""
    if not auth(update): return
    await reply_md(update, exec_report())

async def cmd_zones(update,context):
    """✅ #3 — Zones rentables: WR/PnL par prix d'entrée et par timing."""
    if not auth(update): return
    await reply_md(update, zones_report())

async def cmd_risk(update,context):
    """✅ #4 — Risque: drawdown, profit factor, séries de pertes, espérance."""
    if not auth(update): return
    await reply_md(update, risk_report())

async def cmd_matrix(update,context):
    """✅ #5 — Matrice asset × stratégie (PnL réel par croisement)."""
    if not auth(update): return
    await reply_md(update, strategy_matrix())

async def cmd_slotcombo(update,context):
    """✅ #6 — Combos de signaux (paires) minés depuis slot_records."""
    if not auth(update): return
    await reply_md(update, slot_combo_analysis())

async def cmd_revive(update,context):
    """✅ v10.23 — Réarme le kill-switch"""
    if not auth(update): return
    st.killed=False; st.consec=0; st.cooldown_until=0
    await update.message.reply_text("✅ Kill-switch réarmé. `/run` pour relancer.", parse_mode="Markdown")

async def cb(update,context):
    q=update.callback_query; await q.answer()
    if q.data.startswith("passes:"):
        page=int(q.data.split(":")[1])
        await _show_passes_page(update,context,page); return
    h={"status":cmd_status,"ai":cmd_ai,"trades":cmd_trades,"stats":cmd_stats,
       "fear":cmd_fear,"score":cmd_score,"run":cmd_run,"stop":cmd_stop,"paper":cmd_paper}
    if q.data in h: await h[q.data](update,context)

_last_conflict_alert = [0.0]
async def error_handler(update, context):
    """✅ Sans handler PTB explicite, les exceptions des handlers/jobs étaient juste logguées
    par PTB lui-même ("No error handlers are registered, logging exception.") sans traceback
    exploitable dans /lasterrors, et sans aucune alerte Telegram. On logue ici avec la
    vraie traceback (capturée par _MemErrorHandler via exc_info) et on notifie l'admin."""
    log.error(f"Exception non gérée: {context.error}", exc_info=context.error)
    try:
        from telegram.error import Conflict as _TgConflict
        if isinstance(context.error, _TgConflict):
            # ✅ Conflict = 2 instances pollent getUpdates en même temps (recouvrement pendant un
            # redeploy le plus souvent) — PTB retente seul automatiquement. Pas une vraie panne
            # applicative: on évite de spammer une alerte à chaque cycle de poll.
            if time.time() - _last_conflict_alert[0] > 600:
                _last_conflict_alert[0] = time.time()
                await send(context.bot, "🟡 *Conflict Telegram* — une autre instance du bot est en train de poller (probable redeploy en cours). PTB retente seul; vérifie qu'il ne reste qu'1 instance active si ça persiste >2min.")
            return
        import traceback as _tb
        tail = "".join(_tb.format_exception(type(context.error), context.error, context.error.__traceback__))[-500:]
        await send(context.bot, f"🔴 *Erreur interne*\n`{type(context.error).__name__}: {context.error}`\n```{tail}```")
    except Exception:
        pass

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
        ("backtest",cmd_backtest),("oracle",cmd_oracle),("momentum",cmd_momentum),("meanrev",cmd_mean_reversion),("regime",cmd_regime),("conf",cmd_confluence),("slots",cmd_slots),("flow",cmd_flow),("sessionstats",cmd_sessionstats),("calib",cmd_calib),("edge",cmd_edge),("slotedge",cmd_slotedge),
        ("exec",cmd_exec),("zones",cmd_zones),("risk",cmd_risk),("matrix",cmd_matrix),("slotcombo",cmd_slotcombo),
        ("learn",cmd_learn),("revive",cmd_revive),("autotune",cmd_autotune),("lasterrors",cmd_lasterrors)]:
        app.add_handler(CommandHandler(name,handler))
    app.add_handler(CallbackQueryHandler(cb))
    app.add_error_handler(error_handler)
    # ✅ (21/06) AUTO-REPRISE: si le bot était actif avant le redémarrage (`running` désormais persisté)
    # et pas killed, on replanifie TOUS les jobs au démarrage. Plus besoin de refaire /run après chaque
    # redeploy — c'était la cause du "bot tourne mais aucune passe/trade" (jobs jamais planifiés).
    if st.running and not st.killed:
        st.session_start=time.time(); st.daily_ts=time.time()
        _schedule_all_jobs(app.job_queue)
        app.job_queue.run_once(_job_autoresume_notify, when=5)
        log.info("♻️ Auto-reprise: trading relancé automatiquement (running restauré, killed=False)")
    elif st.running and st.killed:
        st.running=False  # incohérent (kill-switch prime) → on ne reprend pas
    log.info(f"🧠 PolyBot v{BOT_VERSION} démarré")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
