"""
╔══════════════════════════════════════════════════════════════╗
║     POLYMARKET BTC UP/DOWN BOT — OPTIMIZED EDITION          ║
║     Multi-timeframe | 8 indicateurs | Paper trading         ║
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
TOKEN          = os.getenv("TELEGRAM_TOKEN", "VOTRE_TOKEN_ICI")
ALLOWED_UID    = int(os.getenv("ALLOWED_USER_ID", "0"))
PAPER_MODE     = os.getenv("PAPER_MODE", "true").lower() == "true"
BANKROLL_START = float(os.getenv("BANKROLL", "50.0"))

# Seuils optimisés — ne trade QUE les setups très solides
MIN_SCORE      = 6      # score minimum sur 12 pour entrer
MIN_CONF       = 0.65   # 65% confiance minimum
MAX_BET_PCT    = 0.05   # max 5% bankroll par trade
POLY_FEE       = 0.02   # 2% fee Polymarket
DAILY_LOSS_MAX = 0.10   # stop si -10% dans la journée

# Binance
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
BINANCE_PRICE  = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"


# Polymarket CLOB (lecture seule pour l'instant)
POLY_MARKETS   = "https://clob.polymarket.com/markets"
POLY_BOOK      = "https://clob.polymarket.com/book"

DATA_FILE = "polybot_v2_state.json"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("polybot_v2.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─── INDICATEURS ───────────────────────────────────────────────────────────
def ema(values, period):
    if len(values) < period:
        return values[-1] if values else 0
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(len(closes) - period, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0: gains += d
        else: losses -= d
    if losses == 0: return 100.0
    rs = gains / losses
    return round(100 - 100 / (1 + rs), 2)

def macd(closes):
    if len(closes) < 26:
        return 0, 0, 0
    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    macd_line = ema12 - ema26
    # Signal line = EMA9 of MACD (approximation)
    signal = macd_line * 0.9
    hist = macd_line - signal
    return round(macd_line, 4), round(signal, 4), round(hist, 4)

def bollinger(closes, period=20):
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    mid = sum(window) / period
    std = math.sqrt(sum((x - mid)**2 for x in window) / period)
    return round(mid - 2*std, 2), round(mid, 2), round(mid + 2*std, 2)

def atr(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    return round(sum(trs[-period:]) / min(len(trs), period), 2)

def stochastic(closes, highs, lows, period=14):
    if len(closes) < period:
        return 50.0, 50.0
    lowest  = min(lows[-period:])
    highest = max(highs[-period:])
    if highest == lowest:
        return 50.0, 50.0
    k = (closes[-1] - lowest) / (highest - lowest) * 100
    d = (closes[-2] - lowest) / (highest - lowest) * 100 if len(closes) >= period+1 else k
    return round(k, 1), round(d, 1)

def vwap(candles):
    if not candles:
        return 0
    total_vol = sum(c["vol"] for c in candles)
    if total_vol == 0:
        return candles[-1]["close"]
    return round(sum(((c["high"]+c["low"]+c["close"])/3) * c["vol"] for c in candles) / total_vol, 2)

def williams_r(closes, highs, lows, period=14):
    if len(closes) < period:
        return -50.0
    highest = max(highs[-period:])
    lowest  = min(lows[-period:])
    if highest == lowest:
        return -50.0
    return round(-100 * (highest - closes[-1]) / (highest - lowest), 1)

# ─── SIGNAL ENGINE (Multi-Timeframe) ────────────────────────────────────────
def compute_signal(candles_1m, candles_5m, candles_15m):
    """
    Analyse 3 timeframes simultanément.
    Score max = 12. Trade seulement si >= MIN_SCORE.
    """
    result = {
        "dir": None, "score": 0, "conf": 0.0,
        "reasons": [], "indicators": {}, "tf_bias": {}
    }

    if len(candles_5m) < 30:
        result["reasons"].append("⏳ Pas assez de données (besoin 30 bougies 5m)")
        return result

    def analyze_tf(candles, label):
        if len(candles) < 20:
            return 0, []
        closes = [c["close"] for c in candles]
        highs  = [c["high"]  for c in candles]
        lows   = [c["low"]   for c in candles]
        vols   = [c["vol"]   for c in candles]
        score  = 0
        reasons = []

        # 1. RSI
        r = rsi(closes)
        if r < 28:   score += 2; reasons.append(f"[{label}] 🟢 RSI {r} très survendu")
        elif r < 38: score += 1; reasons.append(f"[{label}] 🟡 RSI {r} survendu")
        elif r > 72: score -= 2; reasons.append(f"[{label}] 🔴 RSI {r} très suracheté")
        elif r > 62: score -= 1; reasons.append(f"[{label}] 🟡 RSI {r} suracheté")

        # 2. EMA cross 9/21
        e9  = ema(closes, 9)
        e21 = ema(closes, 21)
        if e9 > e21 * 1.0005:   score += 1; reasons.append(f"[{label}] 🟢 EMA9>EMA21")
        elif e9 < e21 * 0.9995: score -= 1; reasons.append(f"[{label}] 🔴 EMA9<EMA21")

        # 3. MACD
        ml, sl, hist = macd(closes)
        if hist > 0 and ml > 0:   score += 1; reasons.append(f"[{label}] 🟢 MACD haussier")
        elif hist < 0 and ml < 0: score -= 1; reasons.append(f"[{label}] 🔴 MACD baissier")

        # 4. Stochastique
        stk, std_ = stochastic(closes, highs, lows)
        if stk < 20 and stk > std_:   score += 1; reasons.append(f"[{label}] 🟢 Stoch {stk:.0f} oversold")
        elif stk > 80 and stk < std_: score -= 1; reasons.append(f"[{label}] 🔴 Stoch {stk:.0f} overbought")

        # 5. Bollinger
        bb_low, bb_mid, bb_high = bollinger(closes)
        last = closes[-1]
        if bb_low and last < bb_low:    score += 1; reasons.append(f"[{label}] 🟢 Sous BB basse")
        elif bb_high and last > bb_high: score -= 1; reasons.append(f"[{label}] 🔴 Au-dessus BB haute")

        # 6. Volume confirmation
        avg_vol = sum(vols[-10:]) / 10
        if vols[-1] > avg_vol * 1.5:
            score = score + 1 if score > 0 else score - 1
            reasons.append(f"[{label}] ⚡ Volume spike x{vols[-1]/avg_vol:.1f}")

        # 7. Williams %R
        wr = williams_r(closes, highs, lows)
        if wr < -80:   score += 1; reasons.append(f"[{label}] 🟢 Williams %R {wr} oversold")
        elif wr > -20: score -= 1; reasons.append(f"[{label}] 🔴 Williams %R {wr} overbought")

        return score, reasons

    # Analyse chaque timeframe
    score_1m,  reasons_1m  = analyze_tf(candles_1m,  "1m")
    score_5m,  reasons_5m  = analyze_tf(candles_5m,  "5m")
    score_15m, reasons_15m = analyze_tf(candles_15m, "15m")

    # Pondération : 15m > 5m > 1m
    total_score = score_1m * 1 + score_5m * 2 + score_15m * 3
    max_score = 12

    # Consensus timeframes
    tf_bias = {
        "1m":  "UP" if score_1m > 0  else "DOWN" if score_1m < 0  else "NEUTRE",
        "5m":  "UP" if score_5m > 0  else "DOWN" if score_5m < 0  else "NEUTRE",
        "15m": "UP" if score_15m > 0 else "DOWN" if score_15m < 0 else "NEUTRE",
    }

    # Filtre confluence : tous les TF doivent être alignés
    biases = list(tf_bias.values())
    all_up   = all(b == "UP"   for b in biases)
    all_down = all(b == "DOWN" for b in biases)

    # Filtre ATR — éviter les marchés trop calmes (range)
    atr_5m = atr(candles_5m)
    last_price = candles_5m[-1]["close"]
    atr_pct = (atr_5m / last_price * 100) if last_price > 0 else 0

    # Filtre VWAP
    vwap_val = vwap(candles_5m[-20:])
    above_vwap = last_price > vwap_val

    # Indicateurs 5m pour affichage
    closes_5m = [c["close"] for c in candles_5m]
    highs_5m  = [c["high"]  for c in candles_5m]
    lows_5m   = [c["low"]   for c in candles_5m]
    rsi_5m    = rsi(closes_5m)
    e9_5m     = ema(closes_5m, 9)
    e21_5m    = ema(closes_5m, 21)
    bb_l, bb_m, bb_h = bollinger(closes_5m)
    ml, sl, hist = macd(closes_5m)
    stk, std_ = stochastic(closes_5m, highs_5m, lows_5m)
    wr_val    = williams_r(closes_5m, highs_5m, lows_5m)

    result["indicators"] = {
        "price": last_price,
        "rsi_5m": rsi_5m,
        "ema9": round(e9_5m, 2),
        "ema21": round(e21_5m, 2),
        "macd": hist,
        "stoch_k": stk,
        "stoch_d": std_,
        "williams_r": wr_val,
        "atr": atr_5m,
        "atr_pct": round(atr_pct, 3),
        "vwap": vwap_val,
        "above_vwap": above_vwap,
        "bb_low": bb_l,
        "bb_mid": bb_m,
        "bb_high": bb_h,
        "vol_ratio": round(candles_5m[-1]["vol"] / (sum(c["vol"] for c in candles_5m[-10:]) / 10), 2) if len(candles_5m) >= 10 else 1.0,
    }
    result["tf_bias"] = tf_bias
    result["score"]   = total_score

    # Filtre volatilité : pas de trade si marché trop calme
    if atr_pct < 0.05:
        result["reasons"].append("⚪ ATR trop faible — marché en range, pas de trade")
        return result

    # Filtre confluence timeframes
    if not all_up and not all_down:
        result["reasons"].append(f"⚪ Timeframes non alignés: 1m={tf_bias['1m']} 5m={tf_bias['5m']} 15m={tf_bias['15m']}")
        result["reasons"] += reasons_5m
        return result

    conf = min(abs(total_score) / max_score, 1.0)
    result["conf"] = round(conf, 3)

    direction = None
    if total_score >= MIN_SCORE and all_up:
        direction = "UP"
    elif total_score <= -MIN_SCORE and all_down:
        direction = "DOWN"

    result["dir"]     = direction
    result["reasons"] = (reasons_15m + reasons_5m + reasons_1m)[:10]
    return result

# ─── STATE ─────────────────────────────────────────────────────────────────
class State:
    def __init__(self):
        self.running         = False
        self.paper_mode      = PAPER_MODE
        self.bankroll        = BANKROLL_START
        self.candles_1m      = deque(maxlen=100)
        self.candles_5m      = deque(maxlen=100)
        self.candles_15m     = deque(maxlen=100)
        self.current_price   = 0.0
        self.trades          = []
        self.active_bet      = None
        self.wins            = 0
        self.losses          = 0
        self.total_pnl       = 0.0
        self.bet_pct         = 0.03
        self.alerts          = True
        self.daily_start_br  = BANKROLL_START
        self.daily_reset_ts  = time.time()
        self.streak          = 0
        self.best_streak     = 0
        self.worst_streak    = 0
        self.session_start   = time.time()
        self.last_signal     = {}
        self.skipped_trades  = 0  # trades évités par filtres
        self.poly_market_id  = None
        self.poly_markets    = []
        self.tick_job        = None
        self.price_job       = None

    def save(self):
        try:
            d = {
                "bankroll": self.bankroll,
                "trades": self.trades[-200:],
                "wins": self.wins, "losses": self.losses,
                "total_pnl": self.total_pnl,
                "bet_pct": self.bet_pct,
                "best_streak": self.best_streak,
                "worst_streak": self.worst_streak,
                "skipped_trades": self.skipped_trades,
                "daily_start_br": self.daily_start_br,
                "daily_reset_ts": self.daily_reset_ts,
                "paper_mode": self.paper_mode,
            }
            with open(DATA_FILE, "w") as f:
                json.dump(d, f, indent=2)
        except Exception as e:
            log.error(f"Save: {e}")

    def load(self):
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE) as f:
                    d = json.load(f)
                self.bankroll       = d.get("bankroll", BANKROLL_START)
                self.trades         = d.get("trades", [])
                self.wins           = d.get("wins", 0)
                self.losses         = d.get("losses", 0)
                self.total_pnl      = d.get("total_pnl", 0.0)
                self.bet_pct        = d.get("bet_pct", 0.03)
                self.best_streak    = d.get("best_streak", 0)
                self.worst_streak   = d.get("worst_streak", 0)
                self.skipped_trades = d.get("skipped_trades", 0)
                self.daily_start_br = d.get("daily_start_br", self.bankroll)
                self.daily_reset_ts = d.get("daily_reset_ts", time.time())
                self.paper_mode     = d.get("paper_mode", PAPER_MODE)
                log.info("State chargé")
        except Exception as e:
            log.error(f"Load: {e}")

st = State()

# ─── BINANCE DATA ──────────────────────────────────────────────────────────
async def fetch_klines(interval, limit=50):
    url = f"{BINANCE_KLINES}?symbol=BTCUSDT&interval={interval}&limit={limit}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                candles = []
                for k in data:
                    candles.append({
                        "open":  float(k[1]),
                        "high":  float(k[2]),
                        "low":   float(k[3]),
                        "close": float(k[4]),
                        "vol":   float(k[5]),
                        "ts":    int(k[0]) // 1000,
                    })
                return candles
    except Exception as e:
        log.error(f"Binance klines {interval}: {e}")
        return []

async def fetch_price():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE_PRICE, timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                return float(data["price"])
    except Exception as e:
        log.error(f"Binance price: {e}")
        return st.current_price

async def fetch_poly_markets():
    """Récupère les marchés BTC UP/DOWN actifs sur Polymarket"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                POLY_MARKETS,
                params={"active": "true", "closed": "false"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    return []
                data = await r.json()
                markets = data.get("data", []) if isinstance(data, dict) else data
                btc_markets = [
                    m for m in markets
                    if isinstance(m, dict) and
                    "bitcoin" in m.get("question", "").lower() or
                    "btc" in m.get("question", "").lower()
                ]
                return btc_markets[:10]
    except Exception as e:
        log.error(f"Poly markets: {e}")
        return []

# ─── RISK MANAGEMENT ───────────────────────────────────────────────────────
def kelly_bet(conf, bankroll):
    edge = conf - 0.50
    if edge <= 0:
        return 0.0
    half_kelly = edge * 2 * 0.5
    size = min(st.bet_pct, half_kelly)
    amount = size * bankroll
    # Limites absolues
    amount = max(1.0, min(amount, 5.0))   # entre 1 et 5 USDC
    amount = min(amount, bankroll * MAX_BET_PCT)
    return round(amount, 2)

def check_daily_limit():
    now = time.time()
    if now - st.daily_reset_ts > 86400:
        st.daily_start_br = st.bankroll
        st.daily_reset_ts = now
    if st.daily_start_br == 0:
        return False
    loss_pct = (st.daily_start_br - st.bankroll) / st.daily_start_br
    return loss_pct >= DAILY_LOSS_MAX

# ─── TICK ──────────────────────────────────────────────────────────────────
async def price_update(context: ContextTypes.DEFAULT_TYPE):
    """Mise à jour prix toutes les 30 secondes"""
    price = await fetch_price()
    if price > 0:
        st.current_price = price

async def tick(context: ContextTypes.DEFAULT_TYPE):
    """Tick principal toutes les 5 minutes"""
    if not st.running:
        return

    # Check daily limit
    if check_daily_limit():
        st.running = False
        await context.bot.send_message(
            chat_id=ALLOWED_UID,
            text="🛑 *Limite journalière atteinte* — Bot arrêté.\nPerte max configurée atteinte.",
            parse_mode="Markdown"
        )
        return

    # Fetch données réelles Binance
    candles_1m  = await fetch_klines("1m",  60)
    candles_5m  = await fetch_klines("5m",  50)
    candles_15m = await fetch_klines("15m", 40)

    if not candles_5m:
        log.warning("Pas de données Binance")
        return

    # Update state
    st.candles_1m  = deque(candles_1m,  maxlen=100)
    st.candles_5m  = deque(candles_5m,  maxlen=100)
    st.candles_15m = deque(candles_15m, maxlen=100)
    st.current_price = candles_5m[-1]["close"]

    # Résoudre bet actif (après 5 min)
    if st.active_bet:
        bet = st.active_bet
        entry = bet["entry_price"]
        current = st.current_price
        actual_dir = "UP" if current > entry else "DOWN"
        won = bet["dir"] == actual_dir

        gross = bet["amount"] * (1 - POLY_FEE) if won else -bet["amount"]
        st.bankroll   = max(0.0, st.bankroll + gross)
        st.total_pnl += gross

        if won:
            st.wins += 1
            st.streak = st.streak + 1 if st.streak >= 0 else 1
            st.best_streak = max(st.best_streak, st.streak)
        else:
            st.losses += 1
            st.streak = st.streak - 1 if st.streak <= 0 else -1
            st.worst_streak = min(st.worst_streak, st.streak)

        record = {
            "dir": bet["dir"], "amount": bet["amount"],
            "pnl": round(gross, 4), "conf": bet["conf"],
            "result": "WIN" if won else "LOSS",
            "entry": entry, "exit": current,
            "score": bet.get("score", 0),
            "paper": st.paper_mode,
            "ts": int(time.time()),
        }
        st.trades.append(record)
        st.active_bet = None

        if st.alerts:
            emoji = "✅" if won else "❌"
            mode_tag = "📄 PAPER" if st.paper_mode else "💰 RÉEL"
            await context.bot.send_message(
                chat_id=ALLOWED_UID,
                text=(
                    f"{emoji} *Trade clôturé* [{mode_tag}]\n"
                    f"Direction: `{bet['dir']}`\n"
                    f"Entrée: `${entry:,.2f}` → Sortie: `${current:,.2f}`\n"
                    f"PnL: `{'+' if gross >= 0 else ''}{gross:.2f} USDC`\n"
                    f"Bankroll: `{st.bankroll:.2f} USDC`\n"
                    f"Streak: `{st.streak:+d}` | Score signal: `{bet.get('score',0):+d}`"
                ),
                parse_mode="Markdown"
            )
        st.save()

    # Compute signal
    sig = compute_signal(
        list(st.candles_1m),
        list(st.candles_5m),
        list(st.candles_15m)
    )
    st.last_signal = sig

    # Décision trade
    if sig["dir"] and sig["conf"] >= MIN_CONF and not st.active_bet:
        amount = kelly_bet(sig["conf"], st.bankroll)
        if amount >= 1.0:
            st.active_bet = {
                "dir": sig["dir"],
                "amount": amount,
                "conf": sig["conf"],
                "score": sig["score"],
                "entry_price": st.current_price,
                "ts": int(time.time()),
            }

            mode_tag = "📄 PAPER" if st.paper_mode else "💰 RÉEL"
            reasons_text = "\n".join(f"  {r}" for r in sig["reasons"][:5])
            tf = sig.get("tf_bias", {})

            if st.alerts:
                await context.bot.send_message(
                    chat_id=ALLOWED_UID,
                    text=(
                        f"📥 *Bet placé* [{mode_tag}]\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"Direction: *{sig['dir']}*\n"
                        f"Mise: `{amount:.2f} USDC`\n"
                        f"Confiance: `{sig['conf']*100:.0f}%`\n"
                        f"Score: `{sig['score']:+d}/12`\n"
                        f"Prix BTC: `${st.current_price:,.2f}`\n\n"
                        f"📊 TF Bias: `1m={tf.get('1m','?')}` `5m={tf.get('5m','?')}` `15m={tf.get('15m','?')}`\n\n"
                        f"🔍 Raisons:\n{reasons_text}"
                    ),
                    parse_mode="Markdown"
                )
    elif sig["dir"] and sig["conf"] < MIN_CONF:
        st.skipped_trades += 1
        log.info(f"Signal ignoré — confiance trop faible: {sig['conf']*100:.0f}%")

# ─── HELPERS ───────────────────────────────────────────────────────────────
def auth(update): return ALLOWED_UID == 0 or update.effective_user.id == ALLOWED_UID
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
        [InlineKeyboardButton("📊 Status",     callback_data="status"),
         InlineKeyboardButton("📡 Signal",     callback_data="signal")],
        [InlineKeyboardButton("📈 Trades",     callback_data="trades"),
         InlineKeyboardButton("📉 Stats",      callback_data="stats")],
        [InlineKeyboardButton("🔬 Indicateurs",callback_data="indicators"),
         InlineKeyboardButton("🌐 Marchés",    callback_data="markets")],
        [InlineKeyboardButton("▶️ Start",      callback_data="run"),
         InlineKeyboardButton("⏹ Stop",       callback_data="stop")],
        [InlineKeyboardButton("📄 Paper ON" if not st.paper_mode else "💰 Passer Réel",
                               callback_data="toggle_paper")],
    ])

# ─── COMMANDES ─────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    mode = "📄 PAPER TRADING" if st.paper_mode else "💰 TRADING RÉEL"
    await update.message.reply_text(
        f"🤖 *POLYMARKET BTC BOT v2*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode: *{mode}*\n\n"
        f"Prix BTC réels via Binance\n"
        f"Analyse 3 timeframes (1m/5m/15m)\n"
        f"8 indicateurs en confluence\n"
        f"Score min: {MIN_SCORE}/12 | Conf min: {MIN_CONF*100:.0f}%\n\n"
        f"*/run* — Démarrer\n"
        f"*/stop* — Arrêter\n"
        f"*/status* — État & bankroll\n"
        f"*/signal* — Signal actuel\n"
        f"*/trades* — Historique\n"
        f"*/stats* — Statistiques\n"
        f"*/indicators* — Tous les indicateurs\n"
        f"*/markets* — Marchés Poly actifs\n"
        f"*/paper* — Toggle paper/réel\n"
        f"*/set bet 3* — Mise 3%\n"
        f"*/set conf 65* — Conf min 65%\n"
        f"*/reset* — Reset complet",
        parse_mode="Markdown",
        reply_markup=main_kb()
    )

async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    if st.running:
        await update.message.reply_text("⚠️ Déjà en cours.")
        return

    st.running = True
    st.session_start = time.time()
    st.daily_start_br = st.bankroll
    st.daily_reset_ts = time.time()

    # Jobs
    st.price_job = context.job_queue.run_repeating(price_update, interval=30, first=5)
    st.tick_job  = context.job_queue.run_repeating(tick, interval=300, first=10)

    mode = "📄 PAPER" if st.paper_mode else "💰 RÉEL ⚠️"
    await update.message.reply_text(
        f"▶️ *Bot démarré — {mode}*\n"
        f"Tick: toutes les 5 minutes\n"
        f"Prix BTC: `${st.current_price:,.2f}`\n"
        f"Bankroll: `{st.bankroll:.2f} USDC`\n"
        f"Score min requis: `{MIN_SCORE}/12`",
        parse_mode="Markdown"
    )
    # Premier fetch immédiat
    await tick(context)

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    st.running = False
    for job in [st.tick_job, st.price_job]:
        if job: job.schedule_removal()
    st.tick_job = st.price_job = None
    st.save()
    await update.message.reply_text(
        f"⏹ *Bot arrêté*\n"
        f"Uptime: `{uptime()}`\n"
        f"Bankroll: `{st.bankroll:.2f} USDC`\n"
        f"PnL: `{fmt(st.total_pnl)} USDC`\n"
        f"Trades évités (filtres): `{st.skipped_trades}`",
        parse_mode="Markdown"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    sig = st.last_signal
    daily_loss = (st.daily_start_br - st.bankroll) / st.daily_start_br * 100 if st.daily_start_br > 0 else 0
    mode = "📄 PAPER" if st.paper_mode else "💰 RÉEL"

    bet_info = "NON"
    if st.active_bet:
        b = st.active_bet
        unrealized = (st.current_price - b["entry_price"]) / b["entry_price"] * 100
        bet_info = f"{b['dir']} {b['amount']:.2f}$ | {unrealized:+.2f}% depuis entrée"

    await update.message.reply_text(
        f"📊 *STATUS* [{mode}]\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢 EN COURS' if st.running else '🔴 ARRÊTÉ'}\n\n"
        f"₿ BTC: `${st.current_price:,.2f}`\n"
        f"💰 Bankroll: `{st.bankroll:.2f} USDC`\n"
        f"📈 ROI: `{roi()}`\n"
        f"💹 PnL: `{fmt(st.total_pnl)} USDC`\n"
        f"📅 Perte jour: `{daily_loss:.1f}%` / `{DAILY_LOSS_MAX*100:.0f}%`\n\n"
        f"📡 Signal: `{sig.get('dir') or 'WAIT'}` score `{sig.get('score',0):+d}/12`\n"
        f"🎯 Bet actif: `{bet_info}`\n"
        f"🚫 Trades filtrés: `{st.skipped_trades}`\n\n"
        f"⏱ Uptime: `{uptime()}`",
        parse_mode="Markdown",
        reply_markup=main_kb()
    )

async def cmd_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return

    # Fetch frais données si bot arrêté
    if not st.running or len(st.candles_5m) < 5:
        await update.message.reply_text("⏳ Fetching données Binance...")
        c1  = await fetch_klines("1m", 60)
        c5  = await fetch_klines("5m", 50)
        c15 = await fetch_klines("15m", 40)
        if c5:
            st.candles_1m  = deque(c1,  maxlen=100)
            st.candles_5m  = deque(c5,  maxlen=100)
            st.candles_15m = deque(c15, maxlen=100)
            st.current_price = c5[-1]["close"]

    sig = compute_signal(list(st.candles_1m), list(st.candles_5m), list(st.candles_15m))
    st.last_signal = sig

    ind = sig.get("indicators", {})
    tf  = sig.get("tf_bias", {})
    dir_emoji = "🟢" if sig["dir"] == "UP" else "🔴" if sig["dir"] == "DOWN" else "⚪"
    conf_bar = "█" * int(sig["conf"] * 10) + "░" * (10 - int(sig["conf"] * 10))
    reasons_text = "\n".join(f"  {r}" for r in sig["reasons"][:8]) or "  Aucune confluence"

    await update.message.reply_text(
        f"📡 *SIGNAL ACTUEL*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_emoji} Direction: *{sig['dir'] or 'WAIT'}*\n"
        f"🎯 Score: `{sig['score']:+d}/12` (min requis: {MIN_SCORE})\n"
        f"💪 Conf: `{sig['conf']*100:.0f}%` (min: {MIN_CONF*100:.0f}%)\n"
        f"`{conf_bar}`\n\n"
        f"₿ BTC: `${ind.get('price', 0):,.2f}`\n\n"
        f"📊 *Timeframes:*\n"
        f"  1m: `{tf.get('1m','—')}` | 5m: `{tf.get('5m','—')}` | 15m: `{tf.get('15m','—')}`\n\n"
        f"🔍 *Raisons:*\n{reasons_text}",
        parse_mode="Markdown"
    )

async def cmd_indicators(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    ind = st.last_signal.get("indicators", {})
    if not ind:
        await update.message.reply_text("⏳ Lance /signal d'abord.")
        return

    def trend_arrow(val, up_thresh, down_thresh):
        if val > up_thresh: return "🟢"
        if val < down_thresh: return "🔴"
        return "⚪"

    await update.message.reply_text(
        f"🔬 *INDICATEURS DÉTAILLÉS (5m)*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"₿ Prix: `${ind.get('price',0):,.2f}`\n"
        f"📊 VWAP: `${ind.get('vwap',0):,.2f}` {'🟢 AU-DESSUS' if ind.get('above_vwap') else '🔴 EN-DESSOUS'}\n\n"
        f"*Oscillateurs:*\n"
        f"  RSI-14: `{ind.get('rsi_5m','—')}` {trend_arrow(ind.get('rsi_5m',50), 60, 40)}\n"
        f"  Stoch K: `{ind.get('stoch_k','—')}` D: `{ind.get('stoch_d','—')}` {trend_arrow(ind.get('stoch_k',50), 60, 40)}\n"
        f"  Williams %R: `{ind.get('williams_r','—')}` {trend_arrow(ind.get('williams_r',-50), -20, -80)}\n\n"
        f"*Tendance:*\n"
        f"  EMA9: `${ind.get('ema9',0):,.2f}`\n"
        f"  EMA21: `${ind.get('ema21',0):,.2f}` {'🟢' if ind.get('ema9',0) > ind.get('ema21',1) else '🔴'}\n"
        f"  MACD hist: `{ind.get('macd',0):.4f}` {'🟢' if ind.get('macd',0) > 0 else '🔴'}\n\n"
        f"*Volatilité:*\n"
        f"  ATR: `${ind.get('atr',0):.2f}` (`{ind.get('atr_pct',0):.3f}%`)\n"
        f"  BB Haut: `${ind.get('bb_high',0):,.2f}`\n"
        f"  BB Bas: `${ind.get('bb_low',0):,.2f}`\n\n"
        f"*Volume:*\n"
        f"  Ratio: `x{ind.get('vol_ratio',1):.2f}` {trend_arrow(ind.get('vol_ratio',1), 1.5, 0.7)}",
        parse_mode="Markdown"
    )

async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    trades = st.trades[-10:][::-1]
    if not trades:
        await update.message.reply_text("📈 Aucun trade.")
        return

    lines = ["📈 *DERNIERS TRADES*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    for t in trades:
        emoji = "✅" if t["result"] == "WIN" else "❌"
        mode  = "📄" if t.get("paper") else "💰"
        ts    = datetime.fromtimestamp(t["ts"]).strftime("%d/%m %H:%M")
        lines.append(
            f"{emoji}{mode} `{t['dir']}` | `{fmt(t['pnl'])}$` | "
            f"score `{t.get('score',0):+d}` | `{ts}`"
        )

    if st.active_bet:
        b = st.active_bet
        lines.append(f"\n🔄 *Actif:* `{b['dir']}` `{b['amount']:.2f}$` entrée `${b['entry_price']:,.0f}`")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    total = st.wins + st.losses
    avg_win  = sum(t["pnl"] for t in st.trades if t["pnl"] > 0) / max(st.wins, 1)
    avg_loss = abs(sum(t["pnl"] for t in st.trades if t["pnl"] < 0)) / max(st.losses, 1)
    rr = avg_win / avg_loss if avg_loss > 0 else 0

    peak = BANKROLL_START
    max_dd = 0.0
    running_br = BANKROLL_START
    for t in st.trades:
        running_br += t["pnl"]
        if running_br > peak: peak = running_br
        dd = (peak - running_br) / peak * 100 if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    paper_trades = sum(1 for t in st.trades if t.get("paper"))
    real_trades  = total - paper_trades

    await update.message.reply_text(
        f"📉 *STATISTIQUES COMPLÈTES*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Total: `{total}` (✅{st.wins} ❌{st.losses})\n"
        f"   Paper: `{paper_trades}` | Réel: `{real_trades}`\n"
        f"🎯 Win Rate: `{win_rate()}`\n"
        f"💰 PnL: `{fmt(st.total_pnl)} USDC`\n"
        f"📈 ROI: `{roi()}`\n\n"
        f"⚖️ R:R moyen: `{rr:.2f}`\n"
        f"💚 Gain moyen: `+{avg_win:.2f}$`\n"
        f"🔴 Perte moyenne: `-{avg_loss:.2f}$`\n\n"
        f"🔥 Meilleure série: `+{st.best_streak}`\n"
        f"💀 Pire série: `{st.worst_streak}`\n"
        f"📍 Série actuelle: `{st.streak:+d}`\n\n"
        f"📉 Max Drawdown: `{max_dd:.1f}%`\n"
        f"🚫 Trades filtrés: `{st.skipped_trades}`\n"
        f"💼 Bankroll: `{st.bankroll:.2f} USDC`",
        parse_mode="Markdown"
    )

async def cmd_markets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    await update.message.reply_text("🌐 Fetching marchés Polymarket...")
    markets = await fetch_poly_markets()
    if not markets:
        await update.message.reply_text(
            "⚠️ Aucun marché BTC trouvé ou API indisponible.\n"
            "Vérifie sur polymarket.com → Finance → BTC"
        )
        return
    lines = ["🌐 *MARCHÉS BTC POLYMARKET*\n━━━━━━━━━━━━━━━━━━━━━━━━"]
    for m in markets[:5]:
        q = m.get("question", "?")[:50]
        lines.append(f"• `{q}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_paper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    st.paper_mode = not st.paper_mode
    mode = "📄 PAPER TRADING" if st.paper_mode else "💰 TRADING RÉEL ⚠️"
    warning = "\n\n⚠️ *Attention: ordres réels non implémentés dans cette version.*\nLe bot simulera les trades avec les vrais prix." if not st.paper_mode else ""
    await update.message.reply_text(
        f"Mode switché: *{mode}*{warning}",
        parse_mode="Markdown"
    )
    st.save()

async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "⚙️ `/set bet 3` — Mise 3%\n"
            "⚙️ `/set conf 65` — Confiance min 65%\n"
            "⚙️ `/set score 6` — Score min 6/12",
            parse_mode="Markdown"
        )
        return
    global MIN_SCORE, MIN_CONF
    k, v = args[0].lower(), args[1]
    if k == "bet":
        pct = float(v)/100
        if 0.01 <= pct <= 0.10:
            st.bet_pct = pct
            await update.message.reply_text(f"✅ Mise: `{pct*100:.0f}%`", parse_mode="Markdown")
    elif k == "conf":
        c = float(v)/100
        if 0.40 <= c <= 0.95:
            MIN_CONF = c
            await update.message.reply_text(f"✅ Conf min: `{c*100:.0f}%`", parse_mode="Markdown")
    elif k == "score":
        s = int(v)
        if 3 <= s <= 12:
            MIN_SCORE = s
            await update.message.reply_text(f"✅ Score min: `{s}/12`", parse_mode="Markdown")
    st.save()

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    st.running = False
    for job in [st.tick_job, st.price_job]:
        if job: job.schedule_removal()
    st.bankroll = BANKROLL_START
    st.candles_1m.clear(); st.candles_5m.clear(); st.candles_15m.clear()
    st.trades = []; st.active_bet = None
    st.wins = st.losses = st.skipped_trades = 0
    st.total_pnl = st.streak = st.best_streak = st.worst_streak = 0
    st.session_start = time.time()
    if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    await update.message.reply_text("🔄 *Reset complet.*", parse_mode="Markdown")

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    handlers = {
        "status": cmd_status, "signal": cmd_signal,
        "trades": cmd_trades,  "stats":  cmd_stats,
        "indicators": cmd_indicators, "markets": cmd_markets,
        "run": cmd_run, "stop": cmd_stop,
        "toggle_paper": cmd_paper,
    }
    if q.data in handlers:
        await handlers[q.data](update, context)

# ─── MAIN ──────────────────────────────────────────────────────────────────
def main():
    st.load()
    app = Application.builder().token(TOKEN).build()
    cmds = [
        ("start", cmd_start), ("run", cmd_run), ("stop", cmd_stop),
        ("status", cmd_status), ("signal", cmd_signal), ("trades", cmd_trades),
        ("stats", cmd_stats), ("indicators", cmd_indicators),
        ("markets", cmd_markets), ("paper", cmd_paper),
        ("set", cmd_set), ("reset", cmd_reset),
    ]
    for name, handler in cmds:
        app.add_handler(CommandHandler(name, handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    log.info(f"🤖 PolyBot v2 démarré — Mode: {'PAPER' if st.paper_mode else 'RÉEL'}")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
