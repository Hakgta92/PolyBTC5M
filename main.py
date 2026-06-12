"""
POLYMARKET BTC BOT v10.12
NOUVEAUTÉS:
  • Fix OB: Binance spot /depth (passe sur Railway)
  • Fix Liq: funding rate + open interest Binance futures
  • Résumé auto 22h Paris
  • WR par session sur 7 jours (/stats)
  • /dashboard HTML PnL + WR
  • Alerte expiration position réelle
"""

import asyncio, logging, os, json, time, math, aiohttp
from datetime import datetime, timedelta
from collections import deque
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

BOT_VERSION = "10.20j"

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

MIN_BET_USD     = 5.0  # ✅ v10.19g — Min 5$
MAX_BET_USD     = 10.0
MAX_BET_PCT     = 0.08
KELLY_FRACTION  = 0.25

TAKE_PROFIT_MULT    = 2.0
TRAILING_PEAK_MULT  = 1.5
TRAILING_STOP_MULT  = 1.3
TAKE_PROFIT_CHECK   = 30
POLY_FEE            = 0.02
MAX_CONSEC_LOSS     = 2
COOLDOWN_MIN        = 25
MAX_TRADES_PER_H    = 3
CONSERVATIVE_AFTER_LOSSES = 3   # ✅ v10.20 — Mode conservateur après N pertes
BOOST_AFTER_WINS = 5            # ✅ v10.20 — Augmenter mises après N wins
DAILY_LOSS_MAX      = 0.15
DAILY_PAUSE_H       = 2

SESSION_THRESHOLDS = {
    "US_OPEN":      (8,  2.5, 3),
    "US_AFTERNOON": (8,  2.5, 3),
    "EU_OPEN":      (9,  3.0, 4),
    "US_CLOSE":     (9,  3.0, 4),
    "ASIA_LATE":    (10, 3.5, 4),
    "ASIA_EARLY":   (11, 4.0, 5),
    "OVERNIGHT":    (12, 4.5, 6),
}

# ✅ v10.12f — Seuil momentum réduit si score très élevé
# Score ≥ 13 → momentum minimum réduit de 1 point (tous les signaux très alignés)
# Score ≥ 15 → momentum minimum réduit de 2 points (confluence exceptionnelle)
SCORE_MOMENTUM_BONUS = {13: 1, 15: 2}

CLAUDE_API    = "https://api.anthropic.com/v1/messages"
FEAR_GREED_API= "https://api.alternative.me/fng/?limit=1"
DATA_FILE     = "polybot_v10_state.json"
BACKUP_FILE   = "polybot_v10_backup.json"
DASHBOARD_FILE= "/tmp/polybot_dashboard.html"

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO,
    handlers=[logging.FileHandler("polybot_v10.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)

def kelly_bet(bankroll, win_prob, payout_mult, token_price=0.5):
    """
    ✅ v10.16 — Kelly adaptatif au vrai prix du token.
    token_price: prix actuel du token (0.01-0.99)
    payout_mult: 1/token_price
    """
    if win_prob<=0 or payout_mult<=1: return MIN_BET_USD
    b=payout_mult-1; q=1-win_prob
    kp=(win_prob*b-q)/b
    if kp<=0: return 0.0
    # Ajustement selon liquidité du marché (token proche de 0 ou 1 = moins liquide)
    liquidity_factor = 1.0
    if token_price < 0.15 or token_price > 0.85:
        liquidity_factor = 0.7  # Réduire la mise sur marchés très déséquilibrés
    raw_bet = bankroll * min(kp * KELLY_FRACTION * liquidity_factor, MAX_BET_PCT)
    return round(max(MIN_BET_USD, min(raw_bet, MAX_BET_USD)), 2)

# ─── DONNÉES AVANCÉES ──────────────────────────────────────────────────────
async def fetch_orderbook_imbalance():
    """
    ✅ v10.12c — Kraken spread + ticker comme proxy OB.
    Kraken status=200 confirmé sur Railway US West.
    bid/ask ratio et momentum prix = biais directionnel.
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
                        bid = float(t["b"][0])   # meilleur bid
                        ask = float(t["a"][0])   # meilleur ask
                        bid_vol = float(t["b"][2])  # volume bid
                        ask_vol = float(t["a"][2])  # volume ask
                        spread_pct = (ask - bid) / bid * 100
                        # Volume 24h buy vs sell (trades)
                        vol_24h = float(t["v"][1])
                        vwap_24h = float(t["p"][1])
                        price = float(t["c"][0])
                        # Si prix > VWAP 24h → pression achat
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
    ✅ v10.12c — Kraken futures OI + momentum comme proxy liquidations.
    Binance/Bybit bloqués (451/403) sur Railway US West.
    On utilise Kraken 24h stats pour détecter excès directionnel.
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
                        # Position dans le range 24h
                        if (high_24h - low_24h) > 0:
                            pos_in_range = (price - low_24h) / (high_24h - low_24h)
                        else:
                            pos_in_range = 0.5
                        # Détection excès: si prix très haut du range ET forte hausse = risque retournement
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
                    # Kraken peut retourner XETHUSD ou ETHUSD selon la version
                    ohlc = None
                    for key in ["XETHUSD", "ETHUSD", "ETHUSDT"]:
                        if key in result:
                            ohlc = result[key]
                            break
                    if not ohlc:
                        # Prendre le premier résultat non-last (Kraken inclut "last" dans result)
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

    # Données PnL cumulé
    cumul = 0; pnl_points = []
    for t in sorted(trades, key=lambda x: x.get("ts", 0)):
        cumul += t["pnl"]
        ts = datetime.fromtimestamp(t.get("ts", 0)).strftime("%d/%m %H:%M")
        pnl_points.append({"x": ts, "y": round(cumul, 2)})

    # WR par session
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

    # Derniers trades
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
            # Selon support Polymarket: funder doit être le deposit wallet (magic.link proxy)
            deposit_wallet = POLY_PROXY_WALLET  # 0xa565... wallet qui détient les fonds pUSD
            self.client = ClobClientV2(
                host=POLY_HOST,
                key=POLY_PRIVATE_KEY,
                chain_id=POLY_CHAIN_ID,
                signature_type=3,  # POLY_1271 = deposit wallet flow
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

    async def place_market_order(self,token_id,amount_usdc,side="BUY"):
        if not self.ready or not self.client: return None

        amount_float = float(amount_usdc)
        client_version = getattr(self, "client_version", "v1")

        # ✅ v10.14 — CLOB V2 API
        if client_version == "v2":
            try:
                from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
                # ✅ v10.15 — Paramètres confirmés fonctionnels (testé 2026-06-05)
                # size = montant USDC à dépenser (min 1$)
                # price = 0.50 pour ordre market au milieu
                # OrderType.FOK = Fill or Kill
                side_v2 = Side.BUY if side == "BUY" else Side.SELL
                size_val = round(max(5.0, amount_float), 2)  # min 5$

                # ✅ v10.19 — Prix dynamique avec slippage adaptatif
                try:
                    token_price_resp = await self.get_token_price(token_id)
                    if token_price_resp > 0 and token_price_resp < 1.0:
                        # Slippage adaptatif selon la liquidité
                        if token_price_resp < 0.2 or token_price_resp > 0.8:
                            slippage = 0.05  # Plus de slippage sur marchés déséquilibrés
                        else:
                            slippage = 0.02  # Slippage standard
                        price_val = round(min(0.99, token_price_resp * (1 + slippage)), 2)
                    else:
                        price_val = 0.50
                except:
                    price_val = 0.50

                log.info(f"V2 order: token={token_id[:10]} price={price_val} size={size_val}")

                for order_type_v2 in [OrderType.FAK, OrderType.GTC]:  # FAK = Fill and Kill (partiel OK)
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

    async def sell_position(self,token_id,shares):
        if not self.ready or not self.client: return None
        try:
            from py_clob_client_v2 import OrderArgs, OrderType, Side, PartialCreateOrderOptions
            # Vendre = BUY du côté opposé ou SELL du token
            resp = self.client.create_and_post_order(
                order_args=OrderArgs(token_id=token_id, price=0.99, side=Side.SELL, size=round(float(shares),2)),
                options=PartialCreateOrderOptions(tick_size="0.01"),
                order_type=OrderType.FOK,
            )
            log.info(f"sell_position V2: {resp}")
            return resp if resp and resp.get("success") else None
        except Exception as e:
            err = str(e)
            if "No orderbook" in err or "404" in err:
                # ✅ Slot expiré = résolution automatique par Polymarket
                log.info("sell_position: slot expiré, résolution auto Polymarket")
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
    """
    ✅ v10.20 — ADX (Average Directional Index)
    ADX > 25 = tendance forte → trade
    ADX < 20 = range/consolidation → éviter
    """
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
    # Range serré sur 6 bougies = consolidation
    if range_pct < 0.15: return True
    # Vérifier aussi sur 12 bougies pour détecter ranges plus larges
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
    # Divergence haussière: prix fait lower low mais RSI fait higher low
    if closes[-1] < closes[-4] and rsis[-1] > rsis[-4] and rsis[-1] < 40:
        return "BULLISH"
    # Divergence baissière: prix fait higher high mais RSI fait lower high
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
    # Divergence 4h = signal très fort (+3)
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
    Score ≥ 13 → min_mom -1 (signaux très alignés)
    Score ≥ 15 → min_mom -2 (confluence exceptionnelle)
    ✅ v10.17 — Mode turbo: seuils réduits si actif
    """
    min_score, min_diff, min_mom = SESSION_THRESHOLDS.get(session_name, (10, 3.5, 4))
    # Mode conservateur après pertes
    if hasattr(st, 'conservative_until') and time.time() < st.conservative_until:
        min_score = min_score + 2
        min_mom = min_mom + 1
        min_diff = min_diff + 0.5
    # Mode turbo
    elif hasattr(st, 'turbo_until') and time.time() < st.turbo_until:
        min_score = max(7, min_score - 2)
        min_mom = max(2, min_mom - 1)
        min_diff = max(1.5, min_diff - 0.5)
    # Réduit le seuil momentum si score très élevé
    elif score >= 15:
        min_mom = max(2, min_mom - 2)
    elif score >= 13:
        min_mom = max(2, min_mom - 1)
    return min_score, min_diff, min_mom

def compute_confluence_score(i1,i5,i15,i1h,i4h,fg,sess,adv,ob=None,liq=None,eth_bonus=0,eth_desc="",btc24=None,window_delta=0.0,window_delta_pct=0.0):
    up=0.0; dn=0.0; signals=[]

    # ✅ v10.20g — WINDOW DELTA: signal dominant (poids x6)
    # C'est le seul signal qui répond directement à la question du marché
    if window_delta > 0:
        up += abs(window_delta)
        signals.append(f"📈 Window delta +{window_delta_pct:+.3f}% (score +{abs(window_delta):.0f})")
    elif window_delta < 0:
        dn += abs(window_delta)
        signals.append(f"📉 Window delta {window_delta_pct:+.3f}% (score +{abs(window_delta):.0f})")
    else:
        signals.append(f"↔️ Window delta ~0% (indécis)")

    # EMA 5m et 1m (utiles sur court terme, gardés à poids réduit)
    if i5.get("ema_bull"): up+=1.0; signals.append("5m EMA ↑")
    else: dn+=1.0; signals.append("5m EMA ↓")
    if i1.get("ema_bull"): up+=0.5
    else: dn+=0.5

    # EMA 15m (gardé à poids réduit — moins de bruit que 1h/4h)
    if i15.get("ema_bull"): up+=1.0; signals.append("15m EMA ↑")
    else: dn+=1.0; signals.append("15m EMA ↓")

    # EMA 1h et 4h — poids réduit car trop lents pour 5min
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
    # ✅ Score provisoire pour adapter le seuil momentum
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
    # Pattern par session (30 derniers trades)
    recent=trades[-30:]
    sessions={}
    for t in recent:
        s=t.get("session","?")
        if s not in sessions: sessions[s]={"w":0,"l":0}
        if t["result"]=="WIN": sessions[s]["w"]+=1
        else: sessions[s]["l"]+=1
    # Trouver meilleure et pire session
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
    h=(datetime.utcnow().hour+2)%24; thr=0.10 if (22<=h or h<7) else 0.05
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
        h=(datetime.fromtimestamp(t["ts"]).hour+2)%24  # Heure Paris
        if h not in hours: hours[h]={"w":0,"l":0}
        if t["result"]=="WIN": hours[h]["w"]+=1
        else: hours[h]["l"]+=1
    # Trouver meilleure et pire heure
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
    """
    ✅ v10.18 — News BTC en temps réel via CryptoPanic (gratuit, pas de clé)
    Détecte les news importantes qui peuvent causer un spike de volatilité
    """
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
                    # Analyser le sentiment des dernières news
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
    # ✅ v10.18 — News et patterns dans le prompt
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
        self.turbo_until=0  # ✅ v10.17 Mode turbo timestamp
        self.conservative_until=0  # ✅ v10.20 Mode conservateur après pertes
        self.win_streak_count=0  # ✅ v10.20 Compteur wins consécutifs
        self.window_delta_pct=0.0  # ✅ v10.20g Window delta BTC depuis début slot
        self.window_delta=0.0
        self.last_decision={}; self.last_conf_score={}; self.last_mom_score=0
        self.fg={"value":50,"label":"Neutral"}; self.btc24={}
        self.tick_job=self.price_job=self.macro_job=self.tp_job=self.backup_job=self.recap_job=None
        self.current_market=None; self.active_order_id=None; self.active_token_id=None
        self.entry_token_price=0.0; self.shares_bought=0.0
        self.token_price_peak=0.0; self.trailing_active=False
        self.bet_expiry=0  # timestamp expiration du bet réel
        self.last_ob=None; self.last_liq=None; self.last_eth_klines=[]
        self.last_news={"sentiment":"neutral","score":0,"news":[]}  # ✅ v10.18
        self.price_history=[]  # ✅ v10.20b historique prix pour détecter moves rapides

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
                    for t in self.trades[-5:]:  # Seulement les 5 derniers pour éviter les doublons
                        writer.writerow({k: t.get(k,"") for k in fieldnames})
        except Exception as e:
            log.warning(f"CSV export: {e}")
        data={"bankroll":self.bankroll,"bankroll_ref":self.bankroll_ref,
            "trades":self.trades[-200:],"wins":self.wins,"losses":self.losses,"pnl":self.pnl,
            "best_streak":self.best_streak,"worst_streak":self.worst_streak,"consec":self.consec,
            "daily_start":self.daily_start,"daily_ts":self.daily_ts,
            "daily_pause_until":self.daily_pause_until,"paper_mode":self.paper_mode,
            "skipped":self.skipped,"pass_reasons":self.pass_reasons[-50:],
            "version":BOT_VERSION,"saved_at":int(time.time())}
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
                    age=int((time.time()-d.get("saved_at",0))/60)
                    log.info(f"✅ State {filepath} ({age}min) BR:{self.bankroll:.2f}"); return
            except Exception as e: log.error(f"Load {filepath}: {e}")

st=State()

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

async def send(bot,text,parse_mode="Markdown"):
    try: await bot.send_message(chat_id=ALLOWED_UID,text=text,parse_mode=parse_mode); return True
    except Exception as e:
        log.error(f"Send: {e}")
        try: await bot.send_message(chat_id=ALLOWED_UID,text=text.replace("*","").replace("`","").replace("_","")); return True
        except: return False

# ─── JOBS ──────────────────────────────────────────────────────────────────
async def job_backup(context):
    st.backup()

async def job_daily_recap(context):
    """✅ v10.16 — Résumé 22h + rapport hebdo dimanche + alerte bot arrêté"""
    h_paris=(datetime.utcnow().hour+2)%24
    # Alerte si le tick n'a pas tourné depuis >10min (bot potentiellement bloqué)
    if _last_tick_ts > 0 and (time.time() - _last_tick_ts) > 600:
        await send(context.bot, f"⚠️ *Alerte* — Dernier tick il y a `{int((time.time()-_last_tick_ts)/60)}min`. Bot potentiellement bloqué!")
    if h_paris!=22: return  # Tourne toutes les heures, envoie seulement à 22h
    now=time.time(); cutoff=now-86400
    trades_24h=[t for t in st.trades if t.get("ts",0)>=cutoff]
    if not trades_24h:
        # Quand même envoyer récap hebdo le dimanche
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

    # Alerte 1min avant expiration
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
            if won:
                st.wins += 1; st.consec = 0
                st.streak = st.streak+1 if st.streak>=0 else 1
                st.best_streak = max(st.best_streak, st.streak)
                result_txt = "WIN"
            else:
                st.losses += 1; st.consec += 1
                st.streak = st.streak-1 if st.streak<=0 else -1
                st.worst_streak = min(st.worst_streak, st.streak)
                result_txt = "LOSS"
                if st.consec >= MAX_CONSEC_LOSS:
                    st.cooldown_until = now + COOLDOWN_MIN*60
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

        # Mise à jour du peak
        if gain_mult > st.token_price_peak:
            st.token_price_peak = gain_mult
            if gain_mult >= TRAILING_PEAK_MULT and not st.trailing_active:
                st.trailing_active = True
                await send(context.bot,
                    f"🎯 *Trailing stop activé* x`{gain_mult:.2f}`\n"
                    f"Vente auto si retombe sous x`{TRAILING_STOP_MULT:.1f}`")

        sell_reason = None
        sell_pct = 100  # % des shares à vendre

        # ✅ v10.19 — Stop loss si token tombe à x0.5 (mais pas trop tôt)
        time_in_trade = time.time() - st.bet.get("ts", time.time())
        if gain_mult <= 0.5 and time_in_trade > 120:  # Seulement après 2min dans le trade
            # Vérifier si le slot expire bientôt — si oui attendre la résolution auto
            if st.bet_expiry > 0 and (st.bet_expiry - time.time()) < 90:
                pass  # Laisse résoudre automatiquement
            else:
                sell_reason = f"🛑 Stop loss x{gain_mult:.2f} (protection -50%)"
                sell_pct = 100

        # ✅ Paliers de vente anticipée basés sur le multiplicateur TOKEN
        # Token à 0.20$ → x5 potentiel. Si le prix monte à 0.60$ → x3 → on vend
        if current_price >= 0.95:
            # Token quasi résolu à notre faveur → vendre tout immédiatement
            sell_reason = f"✅ Résolution imminente (token={current_price:.2f}$)"
            sell_pct = 100
        elif gain_mult >= 4.0:
            sell_reason = f"🚀 x{gain_mult:.1f} — Take profit x4"
            sell_pct = 100
        elif gain_mult >= 3.0 and st.token_price_peak >= 3.0:
            sell_reason = f"💰 x{gain_mult:.1f} — Take profit x3"
            sell_pct = 80  # Vendre 80%, garder 20% pour plus
        elif gain_mult >= 2.0:
            sell_reason = f"💰 x{gain_mult:.1f} — Take profit x2"
            sell_pct = 60  # Vendre 60%, garder 40%
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
            result = await poly.sell_position(st.active_token_id, shares_to_sell)
            if result:
                gross = round((current_price - st.entry_token_price) * shares_to_sell, 2)
                # Sync balance CLOB
                clob_bal = await fetch_clob_balance()
                if clob_bal and clob_bal > 0:
                    st.bankroll = clob_bal
                else:
                    st.bankroll = max(0.0, st.bankroll + gross)
                st.pnl += gross
                bet = st.bet

                if sell_pct == 100:
                    # Trade complet
                    st.wins += 1; st.consec = 0
                    st.streak = st.streak + 1 if st.streak >= 0 else 1
                    st.best_streak = max(st.best_streak, st.streak)
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
                    # Vente partielle — mettre à jour les shares restantes
                    st.shares_bought = round(st.shares_bought - shares_to_sell, 4)
                    # Activer trailing sur le reste
                    st.trailing_active = True

                await send(context.bot,
                    f"🎯 *VENTE {sell_pct}%* — {sell_reason}\n"
                    f"`{bet['dir']}` | `+{gross:.2f} USDC`\n"
                    f"BR:`{st.bankroll:.2f}` | ROI:`{roi()}`")
                st.backup()
    except Exception as e: log.error(f"job_take_profit: {e}")

async def job_price(context):
    p=await fetch_price()
    if p>0:
        # ✅ v10.20b — Historique prix + détection moves rapides
        now=time.time()
        st.price_history.append({"price":p,"ts":now})
        # Garder 10min d'historique
        st.price_history=[x for x in st.price_history if now-x["ts"]<600]

        # Move brutal en 30s
        if st.price>0 and not st.bet:
            move_pct = (p - st.price) / st.price * 100
            if abs(move_pct) >= 1.0:
                direction = "📈 UP" if move_pct > 0 else "📉 DOWN"
                await send(context.bot,
                    f"⚡ *Move BTC détecté*\n"
                    f"{direction} `{move_pct:+.2f}%` en ~30s\n"
                    f"₿`${p:,.2f}` | Lance `/signal` pour analyser")

        # Move de 0.5% en 2min (120s)
        prices_2min=[x for x in st.price_history if now-x["ts"]<=120]
        if len(prices_2min)>=2 and not st.bet:
            p_old=prices_2min[0]["price"]
            move_2min=(p-p_old)/p_old*100 if p_old>0 else 0
            if abs(move_2min)>=0.5 and abs(move_2min)<1.0:  # Entre 0.5% et 1% (le 1%+ est déjà couvert)
                log.info(f"Move 2min: {move_2min:+.2f}%")
                # Pas de notification pour éviter le spam, juste log
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

async def job_tick(context):
    if not st.running: return

    # ✅ v10.13 — Synchronisation sur les slots de marché BTC 5min
    # Les slots expirent à :00, :05, :10... (multiples de 5min)
    # On trade seulement dans les 3 premières minutes du slot
    now_ts = time.time()
    slot_pos = now_ts % 300  # Position dans le slot actuel (0-299 secondes)
    slot_remaining = 300 - slot_pos  # Temps restant dans ce slot

    # ✅ v10.20g — Entrer à T-30s à T-60s avant la fin du slot (sweet spot optimal)
    # Trop tôt (T-150s+) = direction pas encore claire, token pas cher mais trop incertain
    # T-30s à T-60s = direction BTC quasi lockée, bon ratio précision/payout
    # T-10s = trop tard pour placer l'ordre blockchain (latence 2-5s)
    # ✅ v10.20j — Retour au timing original mais fenêtre large
    # Le window delta gère la qualité du signal, pas le timing
    if slot_remaining < 90:  # Moins de 90s = trop tard pour placer
        return
    # Skip si tout début du slot
    if slot_pos < 15:
        return

    global _last_tick_ts
    _last_tick_ts = time.time()
    paused=check_daily()
    if paused:
        remaining=int((st.daily_pause_until-time.time())/60)
        if remaining%30==0 and remaining>0:
            await send(context.bot,f"⏸ *Pause journalière* — reprise dans `{remaining}min`")
        return
    if in_cd(): return
    c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
    c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
    if not c5: return

    # ✅ v10.20g — WINDOW DELTA: signal dominant
    # Prix BTC au début du slot actuel (il y a slot_pos secondes)
    now_price = c5[-1]["close"] if c5 else 0
    # Le slot a commencé il y a slot_pos secondes
    # On cherche le prix au début du slot dans les klines 1min
    slot_open_price = 0
    slot_open_minutes = int(slot_pos / 60) + 1  # Combien de bougies 1min depuis le début du slot
    if c1 and len(c1) >= slot_open_minutes:
        slot_open_price = c1[-slot_open_minutes]["open"]
    elif c5 and len(c5) >= 1:
        slot_open_price = c5[-1]["open"]  # Fallback: ouverture de la bougie 5min actuelle
    
    window_delta = 0.0
    window_delta_pct = 0.0
    if slot_open_price > 0 and now_price > 0:
        window_delta_pct = (now_price - slot_open_price) / slot_open_price * 100
        # Poids x6 sur le window delta selon les meilleures pratiques
        if window_delta_pct > 0.15: window_delta = 6.0    # Direction très claire UP
        elif window_delta_pct > 0.05: window_delta = 4.0  # Direction claire UP
        elif window_delta_pct > 0.01: window_delta = 2.0  # Légère tendance UP
        elif window_delta_pct < -0.15: window_delta = -6.0  # Direction très claire DOWN
        elif window_delta_pct < -0.05: window_delta = -4.0  # Direction claire DOWN
        elif window_delta_pct < -0.01: window_delta = -2.0  # Légère tendance DOWN
    
    st.window_delta_pct = window_delta_pct
    st.window_delta = window_delta
    log.info(f"Window delta: {window_delta_pct:+.3f}% → score {window_delta:+.1f}")
    st.c1=deque(c1,maxlen=100); st.c5=deque(c5,maxlen=100); st.c15=deque(c15,maxlen=100)
    st.c1h=deque(c1h,maxlen=100); st.c4h=deque(c4h,maxlen=50); st.price=c5[-1]["close"]
    if trades_last_hour(st.trades)>=MAX_TRADES_PER_H: return
    if st.bet:
        bet=st.bet; won=bet["dir"]==("UP" if st.price>bet["entry"] else "DOWN")
        gross=bet["amount"]*(1-POLY_FEE) if won else -bet["amount"]
        if st.paper_mode:
            st.bankroll=max(0.0,st.bankroll+gross); st.pnl+=gross
            if won:
                st.wins+=1; st.consec=0; st.streak=st.streak+1 if st.streak>=0 else 1
                st.best_streak=max(st.best_streak,st.streak)
            else:
                st.losses+=1; st.consec+=1; st.streak=st.streak-1 if st.streak<=0 else -1
                st.worst_streak=min(st.worst_streak,st.streak)
                if st.consec>=MAX_CONSEC_LOSS: st.cooldown_until=time.time()+COOLDOWN_MIN*60
            i15_n=compute_ind(list(st.c15)); i1h_n=compute_ind(list(st.c1h))
            st.trades.append({"dir":bet["dir"],"amount":bet["amount"],"pnl":round(gross,4),
                "conf":bet["conf"],"result":"WIN" if won else "LOSS","entry":bet["entry"],"exit":st.price,
                "reasoning":bet.get("reasoning",""),"paper":True,"ts":int(time.time()),
                "score":bet.get("score",0),"fg_value":st.fg.get("value",50),
                "session":bet.get("session","?"),
                "aligned_15h1h":i15_n.get("ema_bull")==i1h_n.get("ema_bull")})
            st.bet=None; st.token_price_peak=0; st.trailing_active=False; st.bet_expiry=0
            # Auto-sync balance après trade
            clob_bal = await fetch_clob_balance()
            if clob_bal is not None and clob_bal > 0:
                st.bankroll = clob_bal
                log.info(f"Balance sync après trade: {clob_bal:.2f}$")
            # ✅ v10.20 — Mode conservateur après 3 pertes
            if not won:
                st.win_streak_count = 0
                if st.consec >= CONSERVATIVE_AFTER_LOSSES:
                    st.conservative_until = time.time() + 2*3600
                    await send(context.bot, f"⚠️ *Mode conservateur activé 2h* — {st.consec} pertes consécutives")
            else:
                st.win_streak_count += 1
                if st.win_streak_count >= BOOST_AFTER_WINS:
                    await send(context.bot, f"🔥 *{st.win_streak_count} wins consécutifs* — Kelly légèrement augmenté")
            cd_msg=f"\n⏸ Cooldown {COOLDOWN_MIN}min" if in_cd() else ""
            await send(context.bot,f"{'✅' if won else '❌'} *Trade clôturé* [📄]\n`{bet['dir']}` `${bet['entry']:,.0f}`→`${st.price:,.0f}`\nPnL:`{'+' if gross>=0 else ''}{gross:.2f}$` BR:`{st.bankroll:.2f}` ROI:`{roi()}`{cd_msg}")
            st.backup()
    if in_cd(): return
    if not is_trending(list(st.c5),list(st.c15)): st.skipped+=1; return
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
        # ✅ v10.20f — Retry rapide si score proche du seuil (dans les 2 points)
        score_gap = conf_score["min_score"] - conf_score["score"]
        diff_gap = conf_score["min_diff"] - conf_score["diff"]
        slot_remaining_now = 300 - (time.time() % 300)
        
        if (score_gap <= 2 or diff_gap <= 1) and slot_remaining_now > 150:
            # Score presque bon — refetcher les klines 10s plus tard
            await asyncio.sleep(10)
            c1=await fetch_klines("1m",60); c5=await fetch_klines("5m",50)
            c15=await fetch_klines("15m",40); c1h=await fetch_klines("1h",30); c4h=await fetch_klines("4h",20)
            if c5:
                st.c1=deque(c1,maxlen=100); st.c5=deque(c5,maxlen=100)
                st.c15=deque(c15,maxlen=100); st.c1h=deque(c1h,maxlen=100)
                st.c4h=deque(c4h,maxlen=50); st.price=c5[-1]["close"]
                i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5))
                i15=compute_ind(list(st.c15)); i1h=compute_ind(list(st.c1h))
                i4h=compute_ind(list(st.c4h)) if st.c4h else {}
                adv=compute_advanced_signals(list(st.c5),list(st.c1),list(st.c4h) if st.c4h else None)
                eth_bonus2,eth_desc2=compute_eth_correlation(st.last_eth_klines,direction_guess) if st.last_eth_klines else (0,"N/A")
                conf_score2=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv,st.last_ob,st.last_liq,eth_bonus2,eth_desc2,st.btc24)
                mom_score2=compute_momentum_score(i1,i5,i15)
                if conf_score2["tradeable"] and mom_score2>=min_mom:
                    log.info(f"✅ Retry réussi — score {conf_score2['score']:.1f} mom {mom_score2}")
                    conf_score=conf_score2; mom_score=mom_score2; eth_desc=eth_desc2
                    # Continuer avec les nouvelles données
                else:
                    st.skipped+=1
                    reason = f"Score {conf_score2['score']:.1f}<{conf_score2['min_score']} (après retry)"
                    st.pass_reasons.append({"ts":int(time.time()),"reason":reason}); return
            else:
                st.skipped+=1; return
        else:
            st.skipped+=1
            # Message précis selon la vraie raison
            if conf_score["score"] < conf_score["min_score"]:
                reason = f"Score {conf_score['score']:.1f}<{conf_score['min_score']}"
            elif conf_score["diff"] < conf_score["min_diff"]:
                reason = f"Diff {conf_score['diff']:.1f}<{conf_score['min_diff']} (UP:{conf_score['score_up']:.1f} DN:{conf_score['score_dn']:.1f})"
            else:
                reason = f"Tradeable=NON score:{conf_score['score']:.1f} diff:{conf_score['diff']:.1f}"
            st.pass_reasons.append({"ts":int(time.time()),"reason":reason}); return
    if mom_score<min_mom:
        st.skipped+=1; st.pass_reasons.append({"ts":int(time.time()),"reason":f"Mom {mom_score}<{min_mom}"}); return
    if i5.get("atr_pct",0)<0.03: st.skipped+=1; return
    if i5.get("vol_ratio",1)<0.4: st.skipped+=1; return
    # ✅ v10.20 — Filtre ADX: éviter les marchés sans tendance
    # ADX gardé comme info dans le score mais pas comme filtre bloquant
    adx_val = i5.get("adx", 20)
    log.debug(f"ADX: {adx_val}")
    tpu=0.5; tpd=0.5; market_end=0
    if not st.paper_mode:
        market=await poly.find_btc_5min_market()
        if market:
            st.current_market=market
            tpu=await poly.get_token_price(market["token_up"])
            tpd=await poly.get_token_price(market["token_down"])
            # Parse end_date pour alerte expiration
            try:
                from datetime import timezone
                ed=market.get("end_date","")
                if ed:
                    dt=datetime.fromisoformat(ed.replace("Z","+00:00"))
                    market_end=dt.timestamp()
            except: pass
        else:
            st.skipped+=1; st.pass_reasons.append({"ts":int(time.time()),"reason":"Aucun marché actif"}); return
    # ✅ v10.20d — Vérifier payout AVANT d'appeler Claude (économie API)
    ppu=round(1/tpu,2) if tpu>0 else 0
    ppd=round(1/tpd,2) if tpd>0 else 0
    best_payout = ppu if conf_score["direction"]=="UP" else ppd
    if best_payout < 1.3 and not st.paper_mode:
        st.skipped+=1
        st.pass_reasons.append({"ts":int(time.time()),"reason":f"Payout {best_payout:.2f}<1.3 (skip Claude)"})
        return
    # ✅ v10.20g — Zone token optimale: 0.40$ à 0.88$
    # < 0.40$ = marché trop déséquilibré (>60% contre toi)
    # > 0.88$ = quasi résolu, plus de marge de profit
    token_price_dir = tpu if conf_score["direction"]=="UP" else tpd
    if token_price_dir < 0.40 and not st.paper_mode:
        st.skipped+=1
        st.pass_reasons.append({"ts":int(time.time()),"reason":f"Token trop bas ({token_price_dir:.2f}$<0.40$) = risque trop élevé"})
        return
    if token_price_dir > 0.88 and not st.paper_mode:
        st.skipped+=1
        st.pass_reasons.append({"ts":int(time.time()),"reason":f"Token trop haut ({token_price_dir:.2f}$>0.88$) = plus de marge profit"})
        return
    dec=await claude_decide(i1,i5,i15,i1h,i4h,adv,st.trades[-15:],st.bankroll,st.consec,
                            st.fg,st.btc24,sess,conf_score,mom_score,tpu,tpd,st.last_ob,st.last_liq,eth_desc)
    st.last_decision=dec
    if dec["trade"] and dec["dir"] and not st.bet:
        amount=dec["size"]
        if amount<=0 or amount<MIN_BET_USD:
            st.skipped+=1; st.pass_reasons.append({"ts":int(time.time()),"reason":"Kelly edge négatif"}); return
        if st.bankroll<amount: return
        order_id=None; token_used=None; entry_tp=0.5
        if not st.paper_mode and st.current_market:
            token_used=st.current_market["token_up"] if dec["dir"]=="UP" else st.current_market["token_down"]
            entry_tp=tpu if dec["dir"]=="UP" else tpd
            # ✅ v10.13c — Vérification finale expiration avant d'envoyer l'ordre
            if market_end > 0 and (market_end - time.time()) < 60:
                log.warning(f"Ordre annulé — slot expire dans {market_end-time.time():.0f}s")
                st.skipped+=1
                st.pass_reasons.append({"ts":int(time.time()),"reason":"Slot expiré avant ordre"})
                return
            order_id=await poly.place_market_order(token_used,amount,"BUY")
            if not order_id:
                await send(context.bot,"⚠️ *Ordre Polymarket refusé — réessai prochain slot*"); return
            st.active_order_id=order_id; st.active_token_id=token_used
            st.entry_token_price=entry_tp; st.shares_bought=round(amount/entry_tp,4) if entry_tp>0 else 0
            st.token_price_peak=1.0; st.trailing_active=False
            st.bet_expiry=market_end  # ✅ Timestamp expiration pour alerte
        st.bet={"dir":dec["dir"],"amount":amount,"conf":dec["conf"],"entry":st.price,
                "reasoning":dec["reasoning"],"ts":int(time.time()),"score":conf_score["score"],"session":sess["session"]}
        mode="💰 RÉEL" if not st.paper_mode else "📄 paper"
        risk_e={"LOW":"🟢","MEDIUM":"🟡","HIGH":"🔴"}.get(dec["risk"],"🟡")
        sigs="\n".join(f"  • {s}" for s in conf_score["signals"][:5])
        pinfo=f"\nToken:`{entry_tp:.3f}$`→x`{round(1/entry_tp,2) if entry_tp>0 else '?'}` TP:x`{TAKE_PROFIT_MULT}` Trail:x`{TRAILING_PEAK_MULT}`" if not st.paper_mode else ""
        ob_info=f"\n{st.last_ob['desc']}" if st.last_ob and st.last_ob.get("bias") else ""
        await send(context.bot,
            f"🧠 *Bet placé* [{mode}]\n━━━━━━━━━━━━━━━\n"
            f"*{dec['dir']}* | `{amount:.2f}$` Kelly:`{dec.get('kelly_pct',0):.1f}%` | `{dec['conf']*100:.0f}%` | {risk_e}\n"
            f"Score:`{conf_score['score']:.1f}` Mom:`{mom_score}/10`{pinfo}\n"
            f"BTC:`${st.price:,.2f}` | `{sess['session']}`\n"
            f"Ξ`{eth_desc}`{ob_info}\n\n"
            f"💭 _{dec['reasoning']}_\n🔑 Signaux:\n{sigs}")
    else:
        st.skipped+=1; st.pass_reasons.append({"ts":int(time.time()),"reason":f"Claude PASS:{dec['reasoning'][:50]}"})

def auth(u): return ALLOWED_UID==0 or u.effective_user.id==ALLOWED_UID

def kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Status",callback_data="status"),InlineKeyboardButton("🧠 AI Last",callback_data="ai")],
        [InlineKeyboardButton("📈 Trades",callback_data="trades"),InlineKeyboardButton("📉 Stats",callback_data="stats")],
        [InlineKeyboardButton("😱 F&G",callback_data="fear"),InlineKeyboardButton("🎯 Score",callback_data="score")],
        [InlineKeyboardButton("▶️ Start",callback_data="run"),InlineKeyboardButton("⏹ Stop",callback_data="stop")],
        [InlineKeyboardButton("🟢 Actif" if st.running else "🔴 Arrêté",callback_data="status"),
         InlineKeyboardButton("💰 Réel" if not st.paper_mode else "📄 Paper",callback_data="paper")]])

async def cmd_start(update,context):
    if not auth(update): return
    w=POLY_FUNDER_WALLET or POLY_PROXY_WALLET or "?"
    await update.message.reply_text(
        f"🧠 *POLYMARKET BOT v{BOT_VERSION}*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode:*{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}* | API:{'✅' if poly.ready else '❌'}\n"
        f"Wallet:`{w[:6]}...{w[-4:]}`\n\n"
        f"🆕 v10.12:\n"
        f"  ✅ OB via Binance spot (Railway OK)\n"
        f"  ✅ Liq via Binance futures funding\n"
        f"  ✅ Résumé auto 22h Paris\n"
        f"  ✅ Alerte expiration position\n"
        f"  ✅ WR par session 7j\n"
        f"  ✅ /dashboard HTML\n\n"
        f"*/run* */stop* */status* */signal* */score*\n"
        f"*/market* */balance* */trades* */recap* */dashboard*\n"
        f"*/setbalance 55.11* • */backup*",
        parse_mode="Markdown")

async def cmd_run(update,context):
    if not auth(update): return
    if st.running: await update.message.reply_text("⚠️ Déjà en cours."); return
    if not ANTHROPIC_KEY: await update.message.reply_text("❌ ANTHROPIC_API_KEY manquante."); return
    if not st.paper_mode:
        if not poly.init_client():
            await update.message.reply_text("⚠️ Polymarket indispo — paper mode activé",parse_mode="Markdown")
            st.paper_mode=True
    st.running=True; st.session_start=time.time(); st.daily_ts=time.time()
    st.price_job=context.job_queue.run_repeating(job_price,interval=30,first=5)
    st.macro_job=context.job_queue.run_repeating(job_macro,interval=300,first=8)
    st.tick_job=context.job_queue.run_repeating(job_tick,interval=30,first=10)   # ✅ v10.19e: 30s
    st.tp_job=context.job_queue.run_repeating(job_take_profit,interval=TAKE_PROFIT_CHECK,first=10)
    st.backup_job=context.job_queue.run_repeating(job_backup,interval=600,first=60)
    st.recap_job=context.job_queue.run_repeating(job_daily_recap,interval=3600,first=60)
    context.job_queue.run_repeating(job_check_expiry,interval=30,first=15)
    st.fg=await fetch_fear_greed(); st.btc24=await fetch_btc_24h(); sess=session_ctx()
    # ✅ v10.15c — Auto-sync balance depuis CLOB V2
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
        f"▶️ *Bot v{BOT_VERSION} démarré !*\nMode:*{'📄 PAPER' if st.paper_mode else '💰 RÉEL'}*\n"
        f"Session:`{sess['session']}` | Seuils: score≥`{min_score}` mom≥`{min_mom}`\n"
        f"BR:`{st.bankroll:.2f}$` | ROI:`{roi()}`\n"
        f"📊 `{ob_txt}` | 💸 `{liq_txt}`\n"
        f"Récap auto: 22h Paris 🕙",
        parse_mode="Markdown")
    await job_tick(context)

async def cmd_stop(update,context):
    if not auth(update): return
    st.running=False
    for j in [st.tick_job,st.price_job,st.macro_job,st.tp_job,st.backup_job,st.recap_job]:
        if j:
            try: j.schedule_removal()
            except: pass
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
    """✅ v10.12 — Génère un dashboard HTML"""
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
    ok=st.backup()
    if ok:
        await update.message.reply_text(f"💾 *Backup*\nBR:`{st.bankroll:.2f}$` | ROI:`{roi()}` | Trades:`{len(st.trades)}`",parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Backup échoué.")

async def cmd_status(update,context):
    if not auth(update): return
    sess=session_ctx()
    dl=(st.daily_start-st.bankroll)/st.daily_start*100 if st.daily_start>0 else 0
    # ✅ v10.19 — Solde CLOB temps réel dans status (async)
    cs=st.last_conf_score
    score_info=f"`{cs.get('score',0):.1f}/{cs.get('min_score',10)}` Mom:`{st.last_mom_score}/{cs.get('min_mom',4)}`" if cs else "—"
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
        f"{'🟢 EN COURS' if st.running else '🔴 ARRÊTÉ'} | {'✅ CLOB' if poly.ready else '❌ CLOB'}\n\n"
        f"₿`${st.price:,.2f}` | F&G:`{st.fg['value']}` | `{sess['session']}`\n"
        f"Seuils: score≥`{min_score}` mom≥`{min_mom}`\n"
        f"📊 `{ob_txt}` | 💸 `{liq_txt}`\n"
        f"🎯 {score_info}\n\n"
        f"💰 BR:`{st.bankroll:.2f}$` | ROI:`{roi()}` | PnL:`{fmt(st.pnl)}`\n"
        f"📅 Perte jour:`{dl:.1f}%/{DAILY_LOSS_MAX*100:.0f}%`{pause_info}\n"
        f"🎲 Bet:`{bet_info}` | 🚫 Refusés:`{st.skipped}` | ⏱`{upt()}`",
        parse_mode="Markdown")

async def cmd_balance(update,context):
    if not auth(update): return
    w=POLY_PROXY_WALLET or "?"
    short=f"{w[:6]}...{w[-4:]}"
    # ✅ v10.15 — Tente de lire le solde réel via CLOB V2
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
    await update.message.reply_text("⏳ Recherche marché...")
    market=await poly.find_btc_5min_market()
    if not market: await update.message.reply_text("❌ Aucun marché BTC 5min trouvé."); return
    tu=await poly.get_token_price(market["token_up"]); td=await poly.get_token_price(market["token_down"])
    pu=round(1/tu,2) if tu>0 else 0; pd=round(1/td,2) if td>0 else 0
    ku=kelly_bet(st.bankroll,0.6,pu); kd=kelly_bet(st.bankroll,0.6,pd)
    liq=st.last_liq; ob=st.last_ob
    await update.message.reply_text(
        f"🎯 *MARCHÉ ACTIF*\n━━━━━━━━━━━━━━━━━━━━━━━━\n_{market['question']}_\n\n"
        f"🟢 UP:`{tu:.3f}$`→x`{pu}` Kelly≈`{ku:.2f}$`\n"
        f"🔴 DOWN:`{td:.3f}$`→x`{pd}` Kelly≈`{kd:.2f}$`\n"
        f"Fin:`{market.get('end_date','?')}`\n\n"
        f"📊 `{ob['desc'] if ob else 'N/A'}` | 💸 `{liq['desc'] if liq else 'N/A'}`",
        parse_mode="Markdown")

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
    cs=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv,ob,liq,eth_bonus,eth_desc,st.btc24,st.window_delta,st.window_delta_pct)
    mom=compute_momentum_score(i1,i5,i15)
    st.last_conf_score=cs; st.last_mom_score=mom; st.last_ob=ob; st.last_liq=liq
    st.last_eth_klines=eth_klines  # ✅ Cache
    _,_,min_mom=get_session_thresholds(sess["session"], cs.get("score",0) if "cs" in dir() else 0)
    # ✅ v10.13f — Cherche le marché, fallback sur cache
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
        f"₿`${st.price:,.2f}` | `{sess['session']}`{token_txt}\n"
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
    st.last_eth_klines=eth_klines  # ✅ Met à jour le cache
    i1=compute_ind(list(st.c1)); i5=compute_ind(list(st.c5)); i15=compute_ind(list(st.c15))
    i1h=compute_ind(list(st.c1h)); i4h=compute_ind(list(st.c4h)) if st.c4h else {}
    sess=session_ctx(); adv=compute_advanced_signals(list(st.c5),list(st.c1),list(st.c4h) if st.c4h else None)
    direction_guess="UP" if i5.get("ema_bull") else "DOWN"
    eth_bonus,eth_desc=compute_eth_correlation(eth_klines,direction_guess) if eth_klines else (0,"ETH N/A")
    cs=compute_confluence_score(i1,i5,i15,i1h,i4h,st.fg,sess,adv,ob,liq,eth_bonus,eth_desc,st.btc24,st.window_delta,st.window_delta_pct)
    mom=compute_momentum_score(i1,i5,i15)
    st.last_conf_score=cs; st.last_mom_score=mom; st.last_ob=ob; st.last_liq=liq
    # ✅ v10.13f — Cherche le marché, fallback sur le cache du dernier tick
    tu=0.5; td=0.5
    if not st.paper_mode and poly.ready:
        m=await poly.find_btc_5min_market()
        if not m and st.current_market:
            m=st.current_market  # Utilise le dernier marché connu
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
        f"Ξ{eth_e}`{eth_desc}` | `{ob['desc'] if ob else 'N/A'}`\n"
        f"₿`${i5.get('price',0):,.2f}` | F&G:`{st.fg['value']}` | `{sess['session']}`\n\n"
        f"💭 _{d['reasoning']}_",parse_mode="Markdown")

    # ✅ v10.14d — Si Claude dit trade=True, placer l'ordre directement depuis /signal
    if d.get("trade") and d.get("dir") and not st.bet and not st.paper_mode and st.current_market:
        amount = d.get("size", 0)
        if amount >= MIN_BET_USD and st.bankroll >= amount:
            # Vérifier expiration
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
            entry_tp = tu if d["dir"]=="UP" else td
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
    await update.message.reply_text("\n".join(lines),parse_mode="Markdown")

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
    # WR par session 7 jours
    sess_7d=wr_by_session(st.trades,7)
    sess_txt=""
    for s,v in sorted(sess_7d.items(),key=lambda x:x[1]["w"]/(x[1]["w"]+x[1]["l"]) if (x[1]["w"]+x[1]["l"])>0 else 0,reverse=True):
        tot=v["w"]+v["l"]
        wr_s=round(v["w"]/tot*100) if tot>0 else 0
        pnl_s=round(v["pnl"],2)
        sess_txt+=f"\n  `{s}`: {wr_s}% ({v['w']}W/{v['l']}L) `{fmt(pnl_s)}$`"
    # Meilleure/pire heure
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
        f"💡 `/recap` 24h | `/dashboard` HTML",
        parse_mode="Markdown")

async def cmd_passes(update,context):
    if not auth(update): return
    passes=st.pass_reasons[-10:][::-1]
    if not passes: await update.message.reply_text("✅ Aucun PASS."); return
    lines=["🚫 *DERNIERS PASS*"]
    for p in passes:
        lines.append(f"`{datetime.fromtimestamp(p['ts']).strftime('%H:%M')}` {p['reason']}")
    await update.message.reply_text("\n".join(lines),parse_mode="Markdown")

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

async def cmd_reset(update,context):
    if not auth(update): return
    st.running=False
    for j in [st.tick_job,st.price_job,st.macro_job,st.tp_job,st.backup_job,st.recap_job]:
        if j:
            try: j.schedule_removal()
            except: pass
    st.bankroll=50.0; st.bankroll_ref=50.0; st.trades=[]; st.bet=None
    st.wins=st.losses=st.skipped=st.consec=0; st.pnl=st.streak=st.best_streak=st.worst_streak=0
    st.cooldown_until=0; st.daily_pause_until=0; st.session_start=time.time(); st.pass_reasons=[]
    st.last_conf_score={}; st.last_mom_score=0; st.active_order_id=None
    st.active_token_id=None; st.shares_bought=0; st.entry_token_price=0
    st.token_price_peak=0; st.trailing_active=False; st.bet_expiry=0
    st.c1.clear(); st.c5.clear(); st.c15.clear(); st.c1h.clear(); st.c4h.clear()
    for f in [DATA_FILE,BACKUP_FILE]:
        if os.path.exists(f): os.remove(f)
    await update.message.reply_text("🔄 *Reset complet.*",parse_mode="Markdown")

async def cmd_cooldown(update,context):
    if not auth(update): return
    st.cooldown_until=0; st.consec=0; st.daily_pause_until=0
    await update.message.reply_text("✅ Cooldown + pause reset.",parse_mode="Markdown")

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

    result = await poly.sell_position(st.active_token_id, st.shares_bought)
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
        if won:
            st.wins += 1; st.consec = 0
            st.streak = st.streak+1 if st.streak>=0 else 1
            st.best_streak = max(st.best_streak, st.streak)
        else:
            st.losses += 1; st.consec += 1
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
    st.turbo_until = time.time() + 15*60  # 15 minutes
    sess = session_ctx()
    min_score,min_diff,min_mom = get_session_thresholds(sess["session"])
    await update.message.reply_text(
        f"⚡ *MODE TURBO activé 15min*\n"
        f"Seuils: score≥`{max(7,min_score-2)}` mom≥`{max(2,min_mom-1)}`\n"
        f"Utilise `/score` pour voir les signaux en temps réel",
        parse_mode="Markdown")

async def cb(update,context):
    q=update.callback_query; await q.answer()
    h={"status":cmd_status,"ai":cmd_ai,"trades":cmd_trades,"stats":cmd_stats,
       "fear":cmd_fear,"score":cmd_score,"run":cmd_run,"stop":cmd_stop,"paper":cmd_paper}
    if q.data in h: await h[q.data](update,context)

def main():
    st.load()
    if not st.paper_mode and POLY_PRIVATE_KEY: poly.init_client()
    app=Application.builder().token(TOKEN).build()
    for name,handler in [
        ("start",cmd_start),("run",cmd_run),("stop",cmd_stop),("status",cmd_status),
        ("ai",cmd_ai),("signal",cmd_signal),("score",cmd_score),("trades",cmd_trades),
        ("stats",cmd_stats),("fear",cmd_fear),("passes",cmd_passes),("market",cmd_market),
        ("balance",cmd_balance),("paper",cmd_paper),("cooldown",cmd_cooldown),("reset",cmd_reset),
        ("setbalance",cmd_setbalance),("backup",cmd_backup),("recap",cmd_recap),("dashboard",cmd_dashboard),
        ("history",cmd_history),("turbo",cmd_turbo),("sell",cmd_sell),("sellcheck",cmd_sellcheck)]:
        app.add_handler(CommandHandler(name,handler))
    app.add_handler(CallbackQueryHandler(cb))
    log.info(f"🧠 PolyBot v{BOT_VERSION} démarré")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
