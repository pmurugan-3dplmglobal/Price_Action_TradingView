import os
import json
import logging
import csv
import time
import threading
from datetime import datetime as dt, timedelta, time as datetime_time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import requests
import yfinance as yf

# ──────────────────────────────────────────────
#  CONSTANTS & REGISTRIES
# ──────────────────────────────────────────────

TOKEN_FILE = "input/kite_access_token.txt"
JOURNAL_FILE = "output/monitor/trade_journal.csv"

LOOKBACK_LIMITS = {
    "minute": 60,
    "3minute": 100,
    "5minute": 100,
    "10minute": 100,
    "15minute": 200,
    "15min": 200,
    "30minute": 200,
    "30min": 200,
    "60minute": 400,
    "1hr": 400,
    "1h": 400,
    "75min": 400,
    "75mins": 400,
    "75minute": 400,
    "3hr": 400,
    "3h": 400,
    "180min": 400,
    "4hr": 400,
    "4h": 400,
    "240min": 400,
    "day": 2000,
    "d": 2000,
    "1d": 2000,
    "week": 2000,
    "w": 2000,
    "1w": 2000
}

def get_next_candle_start_time(candle_date, timeframe_str):
    try:
        dt_val = pd.to_datetime(candle_date)
        tf_s = str(timeframe_str).lower()
        if "week" in tf_s or tf_s in ["w", "1w"]: tf_minutes = 10080
        elif "4h" in tf_s or "240min" in tf_s: tf_minutes = 240
        elif "3h" in tf_s or "180min" in tf_s: tf_minutes = 180
        elif "75min" in tf_s or tf_s == "75minute" or "75m" in tf_s: tf_minutes = 75
        elif "60min" in tf_s or tf_s == "60minute" or "1hour" in tf_s or "1hr" in tf_s or "1h" in tf_s: tf_minutes = 60
        elif "30min" in tf_s or tf_s == "30minute": tf_minutes = 30
        elif "15min" in tf_s or tf_s == "15minute": tf_minutes = 15
        elif "10min" in tf_s or tf_s == "10minute": tf_minutes = 10
        elif "5min" in tf_s or tf_s == "5minute": tf_minutes = 5
        elif "3min" in tf_s or tf_s == "3minute": tf_minutes = 3
        elif "day" in tf_s or tf_s in ["d", "1d"]: tf_minutes = 1440
        else: tf_minutes = 1440
        next_dt = dt_val + pd.Timedelta(minutes=tf_minutes)
        return str(next_dt)
    except Exception:
        return str(candle_date)

INDEX_REGISTRY = {
    "NIFTY": {"token": 256265, "lot_size": 65, "strike_step": 50, "tradingsymbol": "NIFTY 50", "exchange": "NFO"},
    "BANKNIFTY": {"token": 260105, "lot_size": 30, "strike_step": 100, "tradingsymbol": "NIFTY BANK", "exchange": "NFO"},
    "SENSEX": {"token": 265, "lot_size": 10, "strike_step": 100, "tradingsymbol": "BSE SENSEX", "exchange": "BFO"}
}

def get_adaptive_lookback(timeframe_str, asset_class="STOCK_SPOT", user_lookback=None):
    """
    Priority Hierarchy:
      1. User-configured lookback_days (if > 0)
      2. Adaptive lookup based on Timeframe & Asset Class
      Note: Intraday timeframes (1hr, 75min, 30m, 15m) are strictly capped at 180 days
            to comply with Zerodha API max limit of 200 days per query.
    """
    tf_s = str(timeframe_str).lower()
    is_daily_or_weekly = "week" in tf_s or tf_s in ["w", "1w"] or "day" in tf_s or tf_s in ["d", "1d"]

    if user_lookback is not None and isinstance(user_lookback, (int, float)) and user_lookback > 0:
        if not is_daily_or_weekly:
            return min(int(user_lookback), 180)
        return int(user_lookback)

    if is_daily_or_weekly:
        return 2000
    elif "4h" in tf_s or "3h" in tf_s or "180min" in tf_s or "240min" in tf_s or "75min" in tf_s or "75m" in tf_s or "60min" in tf_s or "1hour" in tf_s or "1hr" in tf_s or "1h" in tf_s:
        return 180
    else:
        return 60


def resample_timeframe(df, timeframe_str):
    """
    Resample dataframe candles for custom non-native timeframes (e.g. 75min, 3h, 4h, week).
    Native Kite TFs (3m, 5m, 10m, 15m, 30m, 60m, day) are returned as is.
    """
    if df is None or df.empty:
        return df

    tf_s = str(timeframe_str).lower()
    origin = None
    if tf_s in ["75min", "75mins", "75m", "75minute"]:
        rule = '75min'
        origin = 'start'
    elif tf_s in ["3hr", "3h", "180min", "180minute"]:
        rule = '180min'
    elif tf_s in ["4hr", "4h", "4hour", "240min", "240minute"]:
        rule = '240min'
    elif tf_s in ["week", "weekly", "w", "1w"]:
        rule = 'W-FRI'
    else:
        return df

    try:
        hist = df.copy()
        time_col = None
        for col in ['date', 'datetime', 'timestamp', 'time']:
            if col in hist.columns:
                time_col = col
                break
        if not time_col:
            return df

        hist[time_col] = pd.to_datetime(hist[time_col])
        hist = hist.set_index(time_col)
        resample_kwargs = {'rule': rule}
        if origin:
            resample_kwargs['origin'] = origin
        resampled = hist.resample(**resample_kwargs).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna().reset_index()
        return resampled
    except Exception as e:
        logging.warning(f"Resampling failed for {timeframe_str}: {e}")
        return df

SUPER_STOCKS = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY",
    "ITC", "SBIN", "BHARTIARTL", "LT", "WIPRO"
]

STOCK_REGISTRY = {
    "ADANIENT": {"token": 112129, "lot_size": 250, "strike_step": 50},
    "ADANIPORTS": {"token": 3861249, "lot_size": 400, "strike_step": 20},
    "APOLLOHOSP": {"token": 415745, "lot_size": 125, "strike_step": 100},
    "ASIANPAINT": {"token": 60417, "lot_size": 200, "strike_step": 20},
    "AXISBANK": {"token": 1510401, "lot_size": 625, "strike_step": 10},
    "BAJAJ-AUTO": {"token": 4267777, "lot_size": 125, "strike_step": 100},
    "BAJAJFINSV": {"token": 4268545, "lot_size": 500, "strike_step": 20},
    "BAJFINANCE": {"token": 81153, "lot_size": 125, "strike_step": 100},
    "BEL": {"token": 54017, "lot_size": 1000, "strike_step": 5},
    "BHARTIARTL": {"token": 2714625, "lot_size": 950, "strike_step": 20},
    "CIPLA": {"token": 177665, "lot_size": 650, "strike_step": 20},
    "COALINDIA": {"token": 5215745, "lot_size": 1250, "strike_step": 10},
    "DRREDDY": {"token": 225537, "lot_size": 125, "strike_step": 100},
    "EICHERMOT": {"token": 232961, "lot_size": 175, "strike_step": 50},
    "ETERNAL": {"token": 1304833, "lot_size": 2425, "strike_step": 5},
    "GRASIM": {"token": 315393, "lot_size": 400, "strike_step": 20},
    "HCLTECH": {"token": 1837313, "lot_size": 700, "strike_step": 20},
    "HDFCBANK": {"token": 341249, "lot_size": 550, "strike_step": 10},
    "HDFCLIFE": {"token": 119553, "lot_size": 1100, "strike_step": 10},
    "HINDALCO": {"token": 348417, "lot_size": 1400, "strike_step": 10},
    "HINDUNILVR": {"token": 3404801, "lot_size": 300, "strike_step": 20},
    "ICICIBANK": {"token": 1270529, "lot_size": 700, "strike_step": 10},
    "INDIGO": {"token": 2865921, "lot_size": 300, "strike_step": 50},
    "INFY": {"token": 408065, "lot_size": 400, "strike_step": 20},
    "ITC": {"token": 424961, "lot_size": 1600, "strike_step": 5},
    "JIOFIN": {"token": 21806081, "lot_size": 2000, "strike_step": 5},
    "JSWSTEEL": {"token": 3001857, "lot_size": 675, "strike_step": 10},
    "KOTAKBANK": {"token": 492033, "lot_size": 400, "strike_step": 20},
    "LT": {"token": 2939649, "lot_size": 300, "strike_step": 50},
    "LTIM": {"token": 4574465, "lot_size": 150, "strike_step": 50},
    "M&M": {"token": 519937, "lot_size": 350, "strike_step": 20},
    "MARUTI": {"token": 2800641, "lot_size": 50, "strike_step": 100},
    "MAXHEALTH": {"token": 5728513, "lot_size": 525, "strike_step": 10},
    "NESTLEIND": {"token": 4543233, "lot_size": 500, "strike_step": 20},
    "NTPC": {"token": 2977281, "lot_size": 3000, "strike_step": 5},
    "ONGC": {"token": 633601, "lot_size": 3850, "strike_step": 5},
    "POWERGRID": {"token": 3834113, "lot_size": 3600, "strike_step": 5},
    "RELIANCE": {"token": 738561, "lot_size": 250, "strike_step": 20},
    "SBILIFE": {"token": 5633, "lot_size": 750, "strike_step": 20},
    "SBIN": {"token": 7795201, "lot_size": 1500, "strike_step": 10},
    "SHRIRAMFIN": {"token": 3184129, "lot_size": 300, "strike_step": 20},
    "SUNPHARMA": {"token": 857857, "lot_size": 700, "strike_step": 20},
    "TATACONSUM": {"token": 3465729, "lot_size": 550, "strike_step": 20},
    "TATAMOTORS": {"token": 884737, "lot_size": 1600, "strike_step": 10},
    "TATASTEEL": {"token": 897537, "lot_size": 5500, "strike_step": 2},
    "TMPV": {"token": 884737, "lot_size": 1600, "strike_step": 10},

    "TCS": {"token": 2953217, "lot_size": 175, "strike_step": 50},
    "TECHM": {"token": 3418369, "lot_size": 600, "strike_step": 20},
    "TITAN": {"token": 895745, "lot_size": 375, "strike_step": 50},
    "TRENT": {"token": 5064961, "lot_size": 150, "strike_step": 100},
    "ULTRACEMCO": {"token": 2952193, "lot_size": 100, "strike_step": 100},
    "VEDL": {"token": 0, "lot_size": 1000, "strike_step": 5},
    "WIPRO": {"token": 969473, "lot_size": 1500, "strike_step": 5}
}

def sync_stock_tokens(kite):
    try:
        instruments = kite.instruments("NSE")
        df = pd.DataFrame(instruments)
        if not df.empty:
            df['tradingsymbol'] = df['tradingsymbol'].str.strip()
            df['segment'] = df['segment'].str.strip()
            synced = 0
            for sym in STOCK_REGISTRY:
                m = df[(df['tradingsymbol'] == sym) & (df['segment'] == 'NSE')]
                if not m.empty:
                    STOCK_REGISTRY[sym]["token"] = int(m.iloc[0]['instrument_token'])
                    synced += 1
            logging.info(f"Synced tokens for {synced} stocks")
    except Exception as e:
        logging.error(f"Stock token sync failed: {e}")

# ──────────────────────────────────────────────
#  SESSION & UTILITIES
# ──────────────────────────────────────────────

def get_best_token_file(default_path=TOKEN_FILE):
    cwd = os.getcwd()
    base = os.path.dirname(os.path.dirname(__file__))
    candidates = [
        default_path,
        os.path.join(cwd, "input", "kite_access_token.txt"),
        os.path.join(cwd, "Trade_Option", "input", "kite_access_token.txt"),
        os.path.join(cwd, "Trade_Stock", "input", "kite_access_token.txt"),
        os.path.join(base, "input", "kite_access_token.txt"),
        os.path.join(base, "Trade_Option", "input", "kite_access_token.txt"),
        os.path.join(base, "Trade_Stock", "input", "kite_access_token.txt")
    ]
    best_file = None
    best_mtime = 0
    for c in candidates:
        if os.path.exists(c):
            try:
                mtime = os.path.getmtime(c)
                if mtime > best_mtime:
                    best_mtime = mtime
                    best_file = c
            except Exception:
                pass
    return best_file or default_path

def load_kite_session(token_file=TOKEN_FILE):
    target_file = get_best_token_file(token_file)
    if os.path.exists(target_file):
        try:
            with open(target_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("api_key") and data.get("access_token"):
                return data["api_key"], data["access_token"]
        except Exception:
            pass
    logging.info("[OPEN_SOURCE] TradingView Open-Source Edition active. Free Yahoo Finance data feed enabled.")
    return "open_source_key", "open_source_token"

def ensure_kite_session(kite, token_file=TOKEN_FILE):
    """Ensure the KiteConnect object in memory has the latest access token from disk if it changed."""
    try:
        target_file = get_best_token_file(token_file)
        if not kite or not os.path.exists(target_file):
            return
        with open(target_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        at = data.get("access_token")
        if at and getattr(kite, "access_token", None) != at:
            kite.set_access_token(at)
            logging.info(f"[KITE_SESSION] Updated in-memory KiteConnect access_token from {target_file}")
    except Exception:
        pass


def log_to_journal(symbol, pattern, timeframe, action, status, details="", pnl_pct=0.0, entry="", sl="", target="", rr="", journal_file=JOURNAL_FILE, lock=None, event_time=None):
    file_exists = os.path.exists(journal_file)
    headers = ["Timestamp", "Symbol", "Pattern", "Timeframe", "Action", "Status", "Entry", "SL", "Target", "RR", "Details", "P&L %"]
    if event_time is not None:
        raw = str(event_time).replace('T', ' ')
        if '+' in raw:
            raw = raw.split('+')[0]
        ts_str = raw
    else:
        ts_str = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [
        ts_str,
        symbol, pattern, timeframe, action, status,
        f"{entry:.2f}" if isinstance(entry, (int, float)) and entry else str(entry) if entry else "",
        f"{sl:.2f}" if isinstance(sl, (int, float)) and sl else str(sl) if sl else "",
        f"{target:.2f}" if isinstance(target, (int, float)) and target else str(target) if target else "",
        f"{rr:.2f}" if isinstance(rr, (int, float)) and rr else str(rr) if rr else "",
        details,
        f"{pnl_pct:.2f}%" if pnl_pct != 0.0 else "-"
    ]
    def _write():
        try:
            p_dir = os.path.dirname(os.path.abspath(journal_file))
            if p_dir:
                os.makedirs(p_dir, exist_ok=True)
            file_exists = os.path.exists(journal_file)
            with open(journal_file, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")
                if not file_exists:
                    writer.writerow(headers)
                writer.writerow(row)
        except Exception as e:
            logging.error(f"Journal write failed: {e}")

    if lock:
        with lock:
            _write()
    else:
        _write()

def is_market_hours():
    now = dt.now()
    if now.weekday() in [5, 6]:
        return False
    t = now.time()
    return datetime_time(9, 15) <= t <= datetime_time(15, 30)

def get_weekly_expiry(target_weekday=1):
    now = dt.now()
    days_ahead = (target_weekday - now.weekday()) % 7
    if days_ahead == 0 and now.hour >= 15:
        days_ahead = 7
    return (now + timedelta(days=days_ahead)).date()

def cap_lookback_days(timeframe, requested_days):
    limit = LOOKBACK_LIMITS.get(timeframe, 200)
    return min(requested_days, limit)

def check_left_side_rule(df, anchor_low, setup_count=0, skip_adjacent=0, lookback_candles=100):
    """Verify no candle in the preceding lookback_candles has a CLOSE below anchor's low (tails/wicks permitted)."""
    if df is None or df.empty:
        return True
    end_idx = len(df) - (setup_count + skip_adjacent) if (setup_count + skip_adjacent) > 0 else len(df)
    start_idx = max(0, end_idx - lookback_candles)
    left = df.iloc[start_idx:end_idx] if end_idx > start_idx else pd.DataFrame()
    if not left.empty and anchor_low > float(left['close'].min()):
        return False
    return True

# Alias for backward compatibility
check_left_side = check_left_side_rule

def find_profit_targets(df_hist, entry_close, stop_loss=None):
    """
    Timeframe & Asset class adaptive profit target finder.
    Handles Intraday Options (1m, 3m, 5m, 15m, 30m, 60m) AND Daily/Weekly/Monthly Stock & Index charts.
    - Daily/Weekly/Monthly TFs: Scans up to 730 days (2 years) to extract major 52-week & multi-month swing highs.
    - Intraday Option TFs: Scans active 30 days window to ignore ancient decaying option contract highs.
    """
    if df_hist is None or len(df_hist) < 3:
        return None, None, None

    hist = df_hist.copy()

    # 1. Identify datetime column
    time_col = None
    for col in ['datetime', 'date', 'timestamp', 'time', 'date_time']:
        if col in hist.columns:
            time_col = col
            break

    is_higher_tf = False
    if time_col is not None:
        try:
            hist[time_col] = pd.to_datetime(hist[time_col])
            hist = hist.sort_values(time_col).reset_index(drop=True)
            time_diffs = hist[time_col].diff().dropna()
            if not time_diffs.empty:
                median_diff = time_diffs.median()
                # If candle spacing is >= 20 hours, it is a Daily, Weekly, or Monthly chart
                if median_diff >= pd.Timedelta(hours=20):
                    is_higher_tf = True
        except Exception:
            pass

    # 2. Adaptive lookback window based on Timeframe & Asset Type
    if time_col is not None:
        try:
            max_dt = hist[time_col].max()
            if is_higher_tf:
                # Daily / Weekly / Monthly charts: Look back up to 730 days (2 years) to capture major 52-week swing highs
                min_dt = max_dt - pd.Timedelta(days=730)
            else:
                # Intraday (1m, 3m, 5m, 15m, 60m): Look back 30 calendar days (~20 trading sessions)
                min_dt = max_dt - pd.Timedelta(days=30)
            
            sub_hist = hist[hist[time_col] >= min_dt]
            if len(sub_hist) >= 5:
                hist = sub_hist
        except Exception:
            pass

    # 3. Find lowest low in active dataset
    ll_idx = hist['low'].idxmin()

    # 4. Calculate ATR for dynamic capping & fallback target spacing
    high_low_diff = (hist['high'] - hist['low']).abs()
    atr = float(high_low_diff.tail(20).mean()) if len(hist) >= 5 else (entry_close * 0.02)
    if pd.isna(atr) or atr <= 0:
        atr = entry_close * 0.02

    # 5. Dynamic target cap & minimum start relative to entry price & asset type
    risk = (entry_close - stop_loss) if (stop_loss and stop_loss < entry_close) else max(atr * 1.5, entry_close * 0.03)

    if entry_close < 300:  # Option contract premium
        max_target_cap = max(entry_close * 3.5, entry_close + 15 * atr)
        min_target_start = max(entry_close * 1.20, entry_close + 1.5 * risk)
        step_tol = 0.04
    elif is_higher_tf:     # Daily/Weekly/Monthly Stock or Index (allows major 52-week peaks)
        max_target_cap = max(entry_close * 2.0, entry_close + 20 * atr)
        min_target_start = max(entry_close * 1.03, entry_close + 1.5 * risk)
        step_tol = 0.03
    else:                  # Intraday Spot Stock / Index
        max_target_cap = max(entry_close * 1.25, entry_close + 10 * atr)
        min_target_start = max(entry_close * 1.02, entry_close + 1.5 * risk)
        step_tol = 0.02

    # 6. Extract Non-Negated 5-bar structural swing high resistance pivots above min_target_start
    non_negated_targets = []
    n = len(hist)
    for i in range(n - 2, 1, -1):
        w = hist.iloc[max(0, i-2):min(n, i+3)]
        if len(w) >= 3 and hist.iloc[i]['high'] == w['high'].max():
            h_val = float(hist.iloc[i]['high'])
            if min_target_start <= h_val <= max_target_cap:
                # NEGATION THEORY RULE: Discard if price action after bar i closed above h_val prior to entry (Breached / Negated)
                subsequent_bars = hist.iloc[i+1:]
                if not subsequent_bars.empty:
                    max_subsequent_close = float(subsequent_bars['close'].max())
                    if max_subsequent_close > h_val * 1.005:
                        continue  # Negated target level -> Discarded
                non_negated_targets.append(h_val)

    if not non_negated_targets:
        for i in range(n - 1, 0, -1):
            h_val = float(hist.iloc[i]['high'])
            if min_target_start <= h_val <= max_target_cap:
                subsequent_bars = hist.iloc[i+1:]
                if not subsequent_bars.empty:
                    if float(subsequent_bars['close'].max()) > h_val * 1.005:
                        continue
                non_negated_targets.append(h_val)

    # Sort non-negated target levels ascending by price
    sorted_levels = sorted(list(set(non_negated_targets)))

    # Cluster non-negated levels within step_tol distance
    clustered = []
    for p in sorted_levels:
        if not clustered or (p - clustered[-1]) / clustered[-1] > step_tol:
            clustered.append(round(p, 2))

    t1 = clustered[0] if len(clustered) >= 1 else None
    t2 = clustered[1] if len(clustered) >= 2 else None
    t3 = clustered[2] if len(clustered) >= 3 else None

    # Strict Negation Theory Rule: T1, T2, T3 are strictly based on non-negated chart swing pivots.
    # If a 2nd or 3rd non-negated swing level does not exist on the chart, keep T2/T3 as None (N/A).
    if t1 is None:
        t1 = round(entry_close + max(1.5 * risk, entry_close * 0.20), 2)

    if t2 is not None and t2 <= t1 * (1 + step_tol):
        t2 = round(t1 * (1 + step_tol * 2), 2)
    if t3 is not None and t2 is not None and t3 <= t2 * (1 + step_tol):
        t3 = round(t2 * (1 + step_tol * 2), 2)

    return t1, t2, t3

def calculate_position_size(spot_price, stop_loss, capital=100000.0, risk_percent=1.0):
    risk_per_unit = abs(spot_price - stop_loss)
    if risk_per_unit <= 0:
        return 0
    max_risk_amount = capital * (risk_percent / 100.0)
    units = int(max_risk_amount / risk_per_unit)
    return max(units, 1)

def calculate_sl_buffer(price_level, side="BULL"):
    """
    Asset-adaptive & price-tiered Stop Loss buffer:
    - For Cheap Options (price < 50): max(0.15, price * 0.02)  (e.g., 0.15 - 0.20 pt buffer for ~7.50 options)
    - For Mid Options (50 <= price < 200): max(0.50, price * 0.015)
    - For High Options / Stock Spot (200 <= price < 500): max(1.00, price * 0.01)
    - For Index Spot (price >= 500): max(2.00, price * 0.005)
    """
    price = float(price_level)
    if price < 50:
        buffer = max(0.15, price * 0.02)
    elif price < 200:
        buffer = max(0.50, price * 0.015)
    elif price < 500:
        buffer = max(1.00, price * 0.01)
    else:
        buffer = max(2.00, price * 0.005)

    if str(side).upper() == "BEAR":
        return round(price + buffer, 2)
    else:
        return round(price - buffer, 2)

def check_circuit_and_spread_shield(kite, symbol, exchange="NSE", side="BUY"):
    """
    Circuit Band & Liquidity Safety Shield:
    Checks if stock is locked at Upper or Lower Circuit before triggering order placement.
    Returns True if order is safe to execute, False if locked in circuit.
    """
    if kite is None or not symbol:
        return True
    try:
        q_key = f"{exchange}:{symbol}"
        q = kite.quote([q_key])
        q_data = q.get(q_key, {})
        if not q_data:
            return True
        
        ltp = float(q_data.get("last_price", 0))
        lower_circuit = float(q_data.get("lower_circuit_limit", 0))
        upper_circuit = float(q_data.get("upper_circuit_limit", 0))
        
        if ltp > 0:
            if str(side).upper() == "BUY" and upper_circuit > 0 and ltp >= upper_circuit:
                logging.warning(f"[CIRCUIT SHIELD] Buy blocked for {symbol}: Locked at Upper Circuit ({upper_circuit})")
                return False
            if str(side).upper() in ["SELL", "SHORT", "EXIT"] and lower_circuit > 0 and ltp <= lower_circuit:
                logging.warning(f"[CIRCUIT SHIELD] Sell blocked for {symbol}: Locked at Lower Circuit ({lower_circuit})")
                return False
        return True
    except Exception as e:
        logging.warning(f"Circuit check exception for {symbol}: {e}")
        return True

def clean_timestamp(ts):
    """Clean ISO timestamp string by stripping timezone offsets (+05:30), seconds, and T separator."""
    if not ts or ts == '-':
        return ""
    s = str(ts).split('+')[0].split('.')[0].replace('T', ' ').strip()
    p = s.split(' ')
    if len(p) == 2:
        date_part, time_part = p[0], p[1]
        t_parts = time_part.split(':')
        if len(t_parts) >= 2:
            return f"{date_part} {t_parts[0]}:{t_parts[1]}"
    return s

# ──────────────────────────────────────────────
#  ANCHOR (A-FORMATION) DETECTION — 5 PATTERNS
# ──────────────────────────────────────────────

def find_anchor_bullish_engulfing(df):
    """A = bullish engulfing candle. Bearish candle-1, then bullish candle that wraps its body+wick."""
    if len(df) < 5:
        return None
    bearish_candle, bull_anchor = df.iloc[-4], df.iloc[-3]
    if not (float(bearish_candle['close']) < float(bearish_candle['open'])):
        return None
    if not (float(bull_anchor['close']) > float(bull_anchor['open'])):
        return None
    if not (float(bull_anchor['open']) <= float(bearish_candle['close']) and float(bull_anchor['close']) > float(bearish_candle['high'])):
        return None
    a_low = float(bull_anchor['low'])
    anchor_close = float(bull_anchor['close'])
    sl_val = calculate_sl_buffer(a_low, side="BULL")
    return {"Pattern": "BULL_A_ABCD_Engulf", "Close": anchor_close, "SL": sl_val, "Signal": "A_Formation", "CandleATime": str(bull_anchor.get('date', ''))}

def find_anchor_ll_sweep(df):
    """
    A = Low 2 (second lower low).
    Rules:
      1. Need > 2 candles (at least 3 candles gap) between Low 1 and Low 2.
      2. In-between candles must NOT close below Low 1 (wicks allowed).
      3. Low 2 sweeps below Low 1.
    """
    if len(df) < 30:
        return None

    search_range = df.iloc[-29:-7]
    if search_range.empty:
        return None

    low_1_idx = search_range['low'].idxmin()
    low_1 = float(df.loc[low_1_idx, 'low'])

    sweep_idx = df.index[-4]

    pos_low_1 = df.index.get_loc(low_1_idx)
    pos_sweep = df.index.get_loc(sweep_idx)
    if (pos_sweep - pos_low_1 - 1) < 3:
        return None

    inbetween_df = df.iloc[pos_low_1 + 1 : pos_sweep]
    if not inbetween_df.empty:
        if (inbetween_df['close'] < low_1).any():
            return None

    sweep_candle, bounce_candle, confirm_candle_1, confirm_candle_2 = df.iloc[-4], df.iloc[-3], df.iloc[-2], df.iloc[-1]
    sweep_low = float(sweep_candle['low'])
    is_red = float(sweep_candle['close']) < float(sweep_candle['open'])
    is_green = float(sweep_candle['close']) >= float(sweep_candle['open'])

    # Var 1: Red sweep candle (dips/closes below Low 1, recovered by bounce candle)
    v1 = is_red and (sweep_low < low_1) and (float(sweep_candle['close']) > low_1)
    v2 = is_red and (float(sweep_candle['close']) < low_1) and (float(bounce_candle['close']) > low_1)
    
    # Var 2 (Page 10): Green/Neutral wick sweep candle (wick pierces Low 1, body closes green above Low 1)
    v3 = is_green and (sweep_low < low_1) and (float(sweep_candle['close']) > low_1)

    pattern_name = "BULL_A_LL_Sweep_Var1" if (v1 or v2) else "BULL_A_LL_Sweep_Var2"

    anchor_close = float(bounce_candle['close'])
    sl_val = calculate_sl_buffer(sweep_low, side="BULL")
    return {"Pattern": pattern_name, "Close": anchor_close, "SL": sl_val, "Signal": "Low2_Formation", "CandleATime": str(sweep_candle.get('date', ''))}

def find_anchor_hammer_baby(df):
    """A = baby/hammer candle completely inside bearish mother's body, with long lower wick."""
    if len(df) < 5:
        return None
    mother_candle, baby_candle, post_baby_1, post_baby_2, post_baby_3 = df.iloc[-5], df.iloc[-4], df.iloc[-3], df.iloc[-2], df.iloc[-1]
    if not (float(mother_candle['close']) < float(mother_candle['open'])):
        return None
    is_green = float(baby_candle['close']) >= float(baby_candle['open'])
    body = abs(float(baby_candle['close']) - float(baby_candle['open']))
    lower_wick = float(min(float(baby_candle['open']), float(baby_candle['close']))) - float(baby_candle['low'])
    upper_wick = float(baby_candle['high']) - float(max(float(baby_candle['open']), float(baby_candle['close'])))

    # Lower wick must be dominant (at least 1.5x body for green, 2.0x body for red)
    min_wick_ratio = 1.2 if is_green else 1.8
    if lower_wick < (body * min_wick_ratio):
        return None
    if lower_wick <= upper_wick:
        return None

    if float(post_baby_2['close']) < float(baby_candle['low']) or float(post_baby_3['close']) < float(baby_candle['low']):
        return None
    anchor_close = float(baby_candle['close'])
    b_low = float(baby_candle['low'])
    sl_val = calculate_sl_buffer(b_low, side="BULL")
    return {"Pattern": "BULL_A_Baby_Candle", "Close": anchor_close, "SL": sl_val, "Signal": "Baby_Formation", "CandleATime": str(baby_candle.get('date', ''))}

def find_anchor_bullish_harami(df):
    """A = bullish inside bar (cin) fully inside bearish mother body."""
    if len(df) < 5:
        return None
    bearish_mother, bullish_inside, post_harami_1, post_harami_2, post_harami_3 = df.iloc[-5], df.iloc[-4], df.iloc[-3], df.iloc[-2], df.iloc[-1]
    if not (float(bearish_mother['close']) < float(bearish_mother['open']) and float(bullish_inside['close']) > float(bullish_inside['open'])):
        return None
    if not (float(bullish_inside['high']) <= float(bearish_mother['open']) and float(bullish_inside['low']) >= float(bearish_mother['close'])):
        return None
    inside_low = float(bullish_inside['low'])
    if float(post_harami_2['close']) < inside_low or float(post_harami_3['close']) < inside_low:
        return None
    anchor_close = float(bullish_inside['close'])
    sl_val = calculate_sl_buffer(inside_low, side="BULL")
    return {"Pattern": "BULL_A_Harami", "Close": anchor_close, "SL": sl_val, "Signal": "Harami_Formation", "CandleATime": str(bullish_inside.get('date', ''))}

def find_anchor_two_higher_highs(df):
    """Setup 3: A1 & A2 are two successive higher high candles with bullish engulfing structure."""
    if len(df) < 5:
        return None
    a1, a2 = df.iloc[-4], df.iloc[-3]
    if not (float(a1['close']) > float(a1['open']) and float(a2['close']) > float(a2['open'])):
        return None
    if not (float(a2['high']) > float(a1['high']) and float(a2['low']) > float(a1['low'])):
        return None
    a_low = min(float(a1['low']), float(a2['low']))
    anchor_close = float(a2['close'])
    sl_val = calculate_sl_buffer(a_low, side="BULL")
    return {"Pattern": "BULL_A_Two_Higher_Highs", "Close": anchor_close, "SL": sl_val, "Signal": "HigherHigh_Engulf", "CandleATime": str(a2.get('date', ''))}

# ──────────────────────────────────────────────
#  ANCHOR BCD BREAKOUT SCANNER (A -> B -> C -> D)
# ──────────────────────────────────────────────

def scan_anchor_bcd_breakout(df_entry, df_anchor):
    """
    Two-phase A-first scanner:
      Phase 1: Find anchor candle A (using 5 anchor detectors + base fallback).
      Phase 2: From A, scan forward sequentially: B (breakout > A.high) ->
               C (red retest) -> D (confirmation close > A.high).
      Returns first complete A -> B -> C -> D pattern, or None.
    """
    anchor_funcs = [
        find_anchor_bullish_engulfing,
        find_anchor_ll_sweep,
        find_anchor_hammer_baby,
        find_anchor_bullish_harami,
        find_anchor_two_higher_highs
    ]

    # ── Phase 1: Find anchor A candles ──
    anchors = []
    for a_idx in range(4, len(df_entry) - 3):
        a = df_entry.iloc[a_idx]
        sub_df = df_entry.iloc[: min(len(df_entry), a_idx + 3)]
        sub_df_direct = df_entry.iloc[: a_idx + 1]

        anchor_match = None
        for fn in anchor_funcs:
            res = fn(sub_df) or fn(sub_df_direct)
            if res:
                anchor_match = res
                break

        benchmark = float(a['high'])
        invalidation = anchor_match["SL"] if anchor_match else calculate_sl_buffer(a['low'], side="BULL")
        anchor_name = anchor_match["Pattern"] if anchor_match else "BULL_A_Base"

        # Left-Side Rule: no close below A.low in preceding 100 candles
        a_low = float(a['low'])
        left_df = df_entry.iloc[max(0, a_idx - 100) : a_idx]
        if not left_df.empty and float(left_df['close'].min()) < a_low:
            continue

        # Pre-compute targets for NoPA filter
        t1, t2, t3 = find_profit_targets(df_anchor, benchmark, stop_loss=invalidation)

        # NoPA: discard if SL/T1/T2 already closed past post-A (closing basis)
        if t1 is not None:
            after_a = df_entry.iloc[a_idx + 1 :]
            if not after_a.empty:
                if float(after_a['close'].min()) <= invalidation:
                    continue
                if float(after_a['close'].max()) >= t1:
                    continue
                if t2 is not None and float(after_a['close'].max()) >= t2:
                    continue

        a_time_val = anchor_match.get("CandleATime") if anchor_match and anchor_match.get("CandleATime") else str(a.get('date', ''))
        anchors.append({
            "idx": a_idx, "a": a, "benchmark": benchmark,
            "invalidation": invalidation, "anchor_name": anchor_name, "a_low": a_low,
            "t1": t1, "t2": t2, "t3": t3, "a_time": a_time_val
        })

    valid_matches = []
    # ── Phase 2: For each anchor, scan forward B -> C -> D ──
    for cand in reversed(anchors):
        a_idx = cand["idx"]
        a = cand["a"]
        benchmark = cand["benchmark"]
        invalidation = cand["invalidation"]
        anchor_name = cand["anchor_name"]
        a_low = cand["a_low"]

        remaining = df_entry.iloc[a_idx + 1:]
        if len(remaining) < 3:
            continue

        # Point B: FIRST candle after A closing above benchmark
        b_idx = None
        for j in range(len(remaining)):
            if float(remaining.iloc[j]['close']) > benchmark:
                b_idx = a_idx + 1 + j
                break
        if b_idx is None:
            continue

        # Point C: FIRST candle AFTER B with red retest (dips to/close to benchmark, stays above SL)
        c_slice = df_entry.iloc[b_idx + 1:]
        c_idx = None
        for j in range(len(c_slice)):
            c_row = c_slice.iloc[j]
            c_low = float(c_row['low'])
            c_close = float(c_row['close'])
            c_open = float(c_row['open'])
            is_red = c_close < c_open
            if (c_low <= benchmark and c_close > invalidation and is_red) or \
               (c_low <= invalidation and c_close > invalidation and c_close < float(a['open']) and is_red):
                c_idx = b_idx + 1 + j
                break
        if c_idx is None:
            continue

        # Point D: FIRST GREEN candle AFTER C closing above benchmark
        d_slice = df_entry.iloc[c_idx + 1:]
        d_idx = None
        for j in range(len(d_slice)):
            d_row = d_slice.iloc[j]
            if float(d_row['close']) > benchmark and float(d_row['close']) > float(d_row['open']):
                d_idx = c_idx + 1 + j
                break
        if d_idx is None:
            continue

        d = df_entry.iloc[d_idx]

        # Invalidation between A and D: no candle closes below SL (A.low - buffer)
        between = df_entry.iloc[a_idx + 1 : d_idx]
        if not between.empty and float(between['close'].min()) < invalidation:
            continue

        close_price = float(d['close'])
        sl_val = invalidation
        t1, t2, t3 = find_profit_targets(df_anchor, close_price, stop_loss=sl_val)
        if t1 is None or close_price >= t1:
            continue

        stage_status = "FRESH_ENTRY"
        priority_level = "HIGH_PRIORITY"

        # Post-D 3-Tier Classification & Setup Freshness Filter
        after_d = df_entry.iloc[d_idx + 1 :]
        candles_since_d = len(df_entry) - 1 - d_idx
        latest_close = float(df_entry.iloc[-1]['close'])

        # Rule 1: Discard stale setups older than 60 candles to wait for new setup in next cycle
        if candles_since_d > 60:
            continue

        # Rule 2: Discard if current price closed below SL floor line
        if latest_close <= invalidation:
            continue

        if not after_d.empty:
            # 3. Discard if SL hit in any candle after D (A.low - buffer)
            if float(after_d['close'].min()) <= invalidation:
                continue
            # 4. Check if T1 has been reached after D
            if float(after_d['close'].max()) >= t1:
                # If T3 reached or T2 reached or no T2/T3 available -> All targets completed -> Discard
                if (t3 is not None and float(after_d['close'].max()) >= t3) or t2 is None or float(after_d['close'].max()) >= t2:
                    continue
                # T1 was hit, but T2/T3 is still pending -> Qualifies as LOW PRIORITY T2 Continuation if intact
                stage_status = "T2_CONTINUATION"
                priority_level = "LOW_PRIORITY"
                sl_val = t1  # Trailed SL to T1 level to protect banked gains

        risk = close_price - sl_val
        if risk <= 0 or risk < close_price * 0.002 or ((t1 - close_price) / risk) < 1.88:
            continue

        rr = (t1 - close_price) / risk if risk > 0 else 0
        short_names = {
            "BULL_A_ABCD_Engulf": "BE_ABCD",
            "BULL_A_LL_Sweep": "LL_ABCD",
            "BULL_A_LL_Sweep_Var1": "LL_ABCD",
            "BULL_A_LL_Sweep_Var2": "LL_ABCD",
            "BULL_A_Baby_Candle": "HAMMER_ABCD",
            "BULL_A_Harami": "HARAMI_ABCD",
            "BULL_A_Two_Higher_Highs": "HH_ABCD",
            "BULL_A_Base": "BASE_ABCD"
        }
        pattern_label = short_names.get(anchor_name, "BASE_ABCD")
        d_time_str = str(d.get("date", ""))
        a_time_str = str(cand.get("a_time") or a.get("date", ""))

        valid_matches.append({
            "Pattern": pattern_label,
            "SL": sl_val,
            "T1": t1,
            "T2": t2,
            "T3": t3,
            "Close": close_price,
            "RR": round(rr, 2),
            "CandleTime": d_time_str,
            "CandleATime": a_time_str,
            "Stage_Status": stage_status,
            "Priority": priority_level,
            "d_idx": d_idx
        })

    if not valid_matches:
        return None

    PATTERN_PRIORITY_MAP = {
        "Engulfing": 5,
        "LL_Sweep": 5,
        "Baby_Candle": 4,
        "Harami": 4,
        "Two_Higher_Highs": 3,
        "Base": 1  # Trend continuation / re-entry base has lowest priority
    }

    def _pattern_rank(match_obj):
        p_name = match_obj.get("Pattern", "")
        for k, rank in PATTERN_PRIORITY_MAP.items():
            if k in p_name:
                return rank
        return 2

    # Prefer LATEST formed pattern (d_idx), then Primary Reversal over Continuation Base, then HIGH_PRIORITY, then R:R
    valid_matches.sort(key=lambda x: (x["d_idx"], _pattern_rank(x), x["Priority"] == "HIGH_PRIORITY", x["RR"]), reverse=True)
    best_latest = valid_matches[0]
    best_latest.pop("d_idx", None)
    return best_latest

    return None

# Alias for backward compatibility across engines and scripts
def _record_completed_scan_trade(contract_name, pattern_label, entry, sl, target, rr, action_str, status_str, pnl_pct, entry_time, exit_time):
    try:
        import trade_db, csv
        journal_db_path = os.path.join("output", "monitor", "journal_trades_db.json")
        csv_path = os.path.join("output", "monitor", "trade_journal.csv")
        
        entry_row = {
            "Timestamp": exit_time or entry_time,
            "Symbol": contract_name,
            "Pattern": pattern_label,
            "Action": action_str,
            "Status": status_str,
            "Entry": round(entry, 2),
            "SL": round(sl, 2),
            "Target": round(target, 2),
            "RR": round(rr, 2),
            "P&L %": f"{pnl_pct:+.2f}%",
            "entry_time": entry_time,
            "exit_time": exit_time
        }
        
        existing = trade_db._read_json(journal_db_path, {"journal_entries": []})
        entries = existing.get("journal_entries", [])
        if any(e.get("Symbol") == contract_name and e.get("Timestamp") == entry_row["Timestamp"] for e in entries):
            return
            
        entries.append(entry_row)
        trade_db._write_json(journal_db_path, {"updated_at": time.strftime("%Y-%m-%d %H:%M:%S"), "journal_entries": entries})
        
        write_header = not os.path.exists(csv_path)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Timestamp", "Symbol", "Pattern", "Action", "Status", "Entry", "SL", "Target", "RR", "P&L %"])
            if write_header:
                writer.writeheader()
            writer.writerow({
                "Timestamp": entry_row["Timestamp"],
                "Symbol": entry_row["Symbol"],
                "Pattern": entry_row["Pattern"],
                "Action": entry_row["Action"],
                "Status": entry_row["Status"],
                "Entry": entry_row["Entry"],
                "SL": entry_row["SL"],
                "Target": entry_row["Target"],
                "RR": entry_row["RR"],
                "P&L %": entry_row["P&L %"]
            })
    except Exception as e:
        logging.warning(f"Journal record error: {e}")


# ──────────────────────────────────────────────
#  SHARED ENGINE UTILITIES (identical between engines)
# ──────────────────────────────────────────────

def get_fetch_timeframe(timeframe_str):
    """
    Translates any timeframe string (native or custom resampled like 75min, 4hr, 3hr, week)
    into a valid native Zerodha Kite interval string.
    Native Kite intervals: ["minute", "3minute", "5minute", "10minute", "15minute", "30minute", "60minute", "day"]
    """
    tf_clean = str(timeframe_str).lower()
    if tf_clean in ["week", "weekly", "w", "1w", "day", "d", "1d"]:
        return "day"
    elif tf_clean in ["3hr", "3hrs", "3h", "180min", "180minute", "4hr", "4hrs", "4h", "4hour", "240min", "240minute", "1hr", "1hrs", "1h", "60min", "60minute"]:
        return "60minute"
    elif tf_clean in ["75min", "75mins", "75m", "75minute"]:
        return "15minute"
    elif tf_clean in ["30min", "30minute"]:
        return "30minute"
    elif tf_clean in ["15min", "15minute"]:
        return "15minute"
    elif tf_clean in ["10min", "10minute"]:
        return "10minute"
    elif tf_clean in ["5min", "5minute"]:
        return "5minute"
    elif tf_clean in ["3min", "3minute"]:
        return "3minute"
    elif tf_clean in ["minute", "1min"]:
        return "minute"
    else:
        return "day"

REVERSE_TOKEN_MAP = {v["token"]: k for k, v in STOCK_REGISTRY.items() if isinstance(v, dict) and v.get("token")}
REVERSE_TOKEN_MAP.update({v["token"]: k for k, v in INDEX_REGISTRY.items() if isinstance(v, dict) and v.get("token")})

def resolve_token_or_symbol_to_name(token_or_sym):
    if isinstance(token_or_sym, int) or (isinstance(token_or_sym, str) and token_or_sym.isdigit()):
        num = int(token_or_sym)
        return REVERSE_TOKEN_MAP.get(num, str(token_or_sym))
    return str(token_or_sym)

def fetch_open_source_candles(token_or_sym, timeframe_str, from_date=None, to_date=None):
    sym = resolve_token_or_symbol_to_name(token_or_sym)
    clean_sym = sym.strip().upper()

    if clean_sym.isdigit():
        return pd.DataFrame()

    symbol_map = {
        "NIFTY": "^NSEI",
        "NIFTY 50": "^NSEI",
        "NIFTY50": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
        "NIFTY BANK": "^NSEBANK",
        "SENSEX": "^BSESN",
        "BSE SENSEX": "^BSESN"
    }

    if clean_sym in symbol_map:
        ticker = symbol_map[clean_sym]
    elif "NIFTY" in clean_sym and not clean_sym.startswith('^'):
        ticker = "^NSEBANK" if "BANK" in clean_sym else "^NSEI"
    elif "SENSEX" in clean_sym and not clean_sym.startswith('^'):
        ticker = "^BSESN"
    elif clean_sym.startswith('^') or '.NS' in clean_sym or '.BO' in clean_sym:
        ticker = clean_sym
    else:
        import re
        base = re.sub(r'\d+.*', '', clean_sym)
        base = base if base else clean_sym
        ticker = f"{base}.NS"
    
    tf_clean = str(timeframe_str).lower()
    if tf_clean in ['day', '1d', 'd', 'week', '1w', 'w']:
        interval, period = '1d', '2y'
    elif tf_clean in ['60min', '60minute', '1hr', '1h', '75min', '75mins', '4hr', '4h', '3hr', '3h']:
        interval, period = '60m', '2mo'
    elif tf_clean in ['30min', '30minute']:
        interval, period = '30m', '1mo'
    elif tf_clean in ['15min', '15minute']:
        interval, period = '15m', '1mo'
    elif tf_clean in ['10min', '10minute', '5min', '5minute', '3min', '3minute', 'minute', '1min']:
        interval, period = '5m', '1mo'
    else:
        interval, period = '1d', '2y'
        
    df = pd.DataFrame()
    try:
        t = yf.Ticker(ticker)
        df_raw = t.history(period=period, interval=interval)
        if not df_raw.empty:
            df = df_raw.reset_index()
            col_map = {'Datetime': 'date', 'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}
            df = df.rename(columns=col_map)
            df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
            df['date'] = df['date'].astype(str)
    except Exception as e:
        logging.warning(f"yfinance fetch failed for {ticker} ({timeframe_str}): {e}")

    if df.empty:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={period}&interval={interval}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                res = data['chart']['result'][0]
                timestamps = res['timestamp']
                quote = res['indicators']['quote'][0]
                df = pd.DataFrame({
                    'date': [str(pd.to_datetime(ts, unit='s').tz_localize('UTC').tz_convert('Asia/Kolkata')) for ts in timestamps],
                    'open': quote['open'],
                    'high': quote['high'],
                    'low': quote['low'],
                    'close': quote['close'],
                    'volume': quote['volume']
                }).dropna().reset_index(drop=True)
        except Exception as e2:
            logging.error(f"Yahoo HTTP fallback fetch failed for {ticker}: {e2}")

    return resample_timeframe(df, timeframe_str)

def fetch_and_resample_candles(kite, token, from_date, to_date, timeframe_str):
    acc_token = getattr(kite, "access_token", "") if kite else ""
    if kite is not None and hasattr(kite, "historical_data") and acc_token and acc_token != "open_source_token":
        fetch_tf = get_fetch_timeframe(timeframe_str)
        if hasattr(kite, "timeout") and not kite.timeout:
            kite.timeout = 10
        try:
            raw = kite.historical_data(token, from_date, to_date, fetch_tf)
            if raw:
                df = pd.DataFrame(raw)
                return resample_timeframe(df, timeframe_str)
        except Exception:
            pass

    return fetch_open_source_candles(token, timeframe_str, from_date, to_date)

def fetch_option_data(kite, token, from_date, to_date, primary_tf, fallback_tf, min_candles=5):
    df = fetch_and_resample_candles(kite, token, from_date, to_date, primary_tf)
    if len(df) >= min_candles:
        return df
    df = fetch_and_resample_candles(kite, token, from_date, to_date, fallback_tf)
    if len(df) >= min_candles:
        logging.info(f"Fallback to {fallback_tf} for token {token} (only {len(df)} candles on {primary_tf})")
    return df

def trading_days_between(start, end):
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days

def calc_rr(entry, sl, t1, t2):
    if entry is None or sl is None or t1 is None:
        return 0
    risk = entry - sl
    if risk <= 0:
        return 0
    targets = [t1]
    if t2 is not None:
        targets.append(t2)
    return sum((t - entry) / risk for t in targets) / len(targets)

def live_execution_enabled(flag_path):
    return os.path.exists(flag_path)

# ──────────────────────────────────────────────
#  SHARED POSITION MANAGEMENT
# ──────────────────────────────────────────────

NFO_CACHE_FILE = os.path.join("output", "monitor", "nfo_instruments_cache.csv")

def get_option_lot_size(contract):
    """Look up actual lot size from NFO instruments cache, not from registry."""
    try:
        if not os.path.exists(NFO_CACHE_FILE):
            return None
        df = pd.read_csv(NFO_CACHE_FILE)
        row = df[df['tradingsymbol'] == contract]
        if not row.empty:
            return int(row.iloc[0]['lot_size'])
    except Exception as e:
        logging.warning(f"Lot size lookup failed for {contract}: {e}")
    return None

def close_stock_position(kite, pos, live_market=True, product=None):
    if not kite:
        logging.info(f"[BACKTEST EXIT] Closed stock {pos.get('contract','')}")
        return
    contract = pos.get("contract") or pos.get("symbol")
    if not contract:
        logging.error("close_stock_position failed: missing contract/symbol name")
        return
    if is_contract_exit_executed(contract):
        prev = EXECUTED_EXITS.get(contract, {})
        logging.info(f"[EXIT GUARD BLOCK] {contract} stock exit order already submitted (Order ID: {prev.get('order_id')}). Skipping duplicate exit call.")
        return
    target_product = product
    try:
        if kite:
            net_positions = kite.positions().get("net", [])
            for p in net_positions:
                if p.get("tradingsymbol") == contract and abs(int(p.get("quantity", 0))) > 0:
                    prod = p.get("product")
                    if prod:
                        target_product = prod
                        break
    except Exception as e:
        logging.warning(f"Could not fetch Kite stock position product for {contract}: {e}")
    if not target_product:
        target_product = pos.get("product") or kite.PRODUCT_CNC
    try:
        q = kite.quote(f"{kite.EXCHANGE_NSE}:{contract}")
        ltp = q[f"{kite.EXCHANGE_NSE}:{contract}"]["last_price"]
        bid = q[f"{kite.EXCHANGE_NSE}:{contract}"]["depth"]["buy"][0]["price"]
        price = round((bid if bid > 0 else ltp) * 0.995, 1)
        qty = pos.get("position_size", pos.get("quantity", 1))
        try:
            oid = kite.place_order(
                variety=kite.VARIETY_REGULAR, tradingsymbol=contract,
                exchange=kite.EXCHANGE_NSE, transaction_type=kite.TRANSACTION_TYPE_SELL,
                quantity=qty, order_type=kite.ORDER_TYPE_LIMIT,
                price=price, product=target_product
            )
            save_executed_exit(contract, oid, {"type": "LIMIT", "price": price, "qty": qty})
            logging.info(f"Closed stock {contract} with product {target_product} (Order ID: {oid})")
        except Exception as primary_err:
            logging.warning(f"Primary stock exit with {target_product} failed for {contract}: {primary_err}. Retrying with fallback...")
            alt_product = kite.PRODUCT_MIS if target_product == kite.PRODUCT_CNC else kite.PRODUCT_CNC
            try:
                oid = kite.place_order(
                    variety=kite.VARIETY_REGULAR, tradingsymbol=contract,
                    exchange=kite.EXCHANGE_NSE, transaction_type=kite.TRANSACTION_TYPE_SELL,
                    quantity=qty, order_type=kite.ORDER_TYPE_LIMIT,
                    price=price, product=alt_product
                )
                save_executed_exit(contract, oid, {"type": "LIMIT_ALT", "price": price, "qty": qty})
                logging.info(f"Fallback stock exit SUCCESS for {contract} with product {alt_product} (Order ID: {oid})")
            except Exception as alt_err:
                logging.error(f"Fallback stock exit failed for {contract}: {alt_err}")
    except Exception as e:
        logging.error(f"Stock exit failed for {contract}: {e}")

EXECUTED_EXITS_FILE = os.path.join(os.getcwd(), "output", "monitor", "executed_exit_orders.json")
EXECUTED_EXITS = {}

def load_executed_exits():
    global EXECUTED_EXITS
    if os.path.exists(EXECUTED_EXITS_FILE):
        try:
            with open(EXECUTED_EXITS_FILE, "r", encoding="utf-8") as f:
                EXECUTED_EXITS = json.load(f)
        except Exception:
            EXECUTED_EXITS = {}

def save_executed_exit(contract, order_id, details=None):
    global EXECUTED_EXITS
    load_executed_exits()
    EXECUTED_EXITS[contract] = {
        "order_id": str(order_id),
        "timestamp": dt.now().isoformat(),
        "details": details or {}
    }
    try:
        os.makedirs(os.path.dirname(EXECUTED_EXITS_FILE), exist_ok=True)
        with open(EXECUTED_EXITS_FILE, "w", encoding="utf-8") as f:
            json.dump(EXECUTED_EXITS, f, indent=4)
    except Exception as e:
        logging.error(f"Failed to save executed exit order file: {e}")

def is_contract_exit_executed(contract):
    load_executed_exits()
    return contract in EXECUTED_EXITS

def clear_executed_exit(contract):
    global EXECUTED_EXITS
    load_executed_exits()
    if contract in EXECUTED_EXITS:
        del EXECUTED_EXITS[contract]
        try:
            os.makedirs(os.path.dirname(EXECUTED_EXITS_FILE), exist_ok=True)
            with open(EXECUTED_EXITS_FILE, "w", encoding="utf-8") as f:
                json.dump(EXECUTED_EXITS, f, indent=4)
            logging.info(f"[EXIT GUARD RESET] Reset exit guard for {contract} due to new trade re-entry.")
        except Exception as e:
            logging.error(f"Failed to clear executed exit for {contract}: {e}")

def close_position(kite, pos, live_market=True, product=None):
    contract = pos.get("contract") or pos.get("tradingsymbol")
    if not contract:
        return
    
    target_product = product
    try:
        if not target_product:
            kp = kite.positions()
            for p in (kp.get("day", []) + kp.get("net", [])):
                if p.get("tradingsymbol") == contract:
                    target_product = p.get("product")
                    break
    except Exception as e:
        logging.warning(f"Could not fetch Kite position product for {contract}: {e}")
    if not target_product:
        target_product = pos.get("product") or kite.PRODUCT_NRML

    c_str = str(contract).upper()
    if "SENSEX" in c_str or "BSE" in c_str:
        target_exch = "BFO"
    elif "CE" in c_str or "PE" in c_str or "NIFTY" in c_str or "BANK" in c_str:
        target_exch = "NFO"
    else:
        target_exch = "NSE"

    qty = pos.get("quantity") or (get_option_lot_size(contract) or pos.get("lot_size", 1)) * pos.get("position_size", 1)

    if is_contract_exit_executed(contract):
        prev = EXECUTED_EXITS.get(contract, {})
        oid = prev.get("order_id")
        if oid and kite and live_market:
            o_status = None
            try:
                orders = kite.orders()
                for o in orders:
                    if str(o.get("order_id")) == str(oid):
                        o_status = o.get("status")
                        break
                if o_status in ["OPEN", "TRIGGER PENDING"]:
                    logging.warning(f"[PENDING LIMIT EXIT DETECTED] Order {oid} for {contract} is OPEN/UNFILLED. Cancelling limit order and executing MARKET exit fallback...")
                    try:
                        kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=oid)
                    except Exception as c_err:
                        logging.warning(f"Could not cancel pending order {oid}: {c_err}")
                    
                    m_oid = kite.place_order(
                        variety=kite.VARIETY_REGULAR, tradingsymbol=contract,
                        exchange=target_exch, transaction_type=kite.TRANSACTION_TYPE_SELL,
                        quantity=qty, order_type=kite.ORDER_TYPE_MARKET,
                        product=target_product
                    )
                    save_executed_exit(contract, m_oid, {"type": "MARKET_FALLBACK", "qty": qty})
                    logging.info(f"Fallback MARKET exit SUCCESS for {contract} on exchange {target_exch} (Order ID: {m_oid})")
                    return
                elif o_status in ["CANCELLED", "REJECTED", "EXPIRED", "CANCELLED ALL"]:
                    logging.warning(f"[EXIT GUARD RESET] Order {oid} for {contract} is {o_status}. Clearing exit guard and retrying exit.")
                    clear_executed_exit(contract)
                else:
                    logging.info(f"[EXIT GUARD BLOCK] {contract} exit order {oid} is {o_status or 'UNKNOWN'}. Skipping duplicate exit call.")
                    return
            except Exception as check_err:
                logging.debug(f"Could not verify exit order status for {contract}: {check_err}")
                logging.info(f"[EXIT GUARD BLOCK] {contract} exit order {oid} status could not be verified. Skipping duplicate exit call.")
                return
        else:
            logging.info(f"[EXIT GUARD BLOCK] {contract} exit order already submitted (Order ID: {prev.get('order_id')}). Skipping duplicate exit call.")
            return

    if not live_market:
        logging.info(f"[BACKTEST EXIT] {contract}")
        return

    try:
        q_key = f"{target_exch}:{contract}"
        q = kite.quote([q_key])
        ltp = q.get(q_key, {}).get("last_price", 0)
        bid = 0
        depth = q.get(q_key, {}).get("depth", {}).get("buy", [])
        if depth and len(depth) > 0:
            bid = float(depth[0].get("price", 0))
        raw_price = (bid if bid > 0 else ltp) * 0.995
        price = round(round(raw_price / 0.05) * 0.05, 2)
        try:
            oid = kite.place_order(
                variety=kite.VARIETY_REGULAR, tradingsymbol=contract,
                exchange=target_exch, transaction_type=kite.TRANSACTION_TYPE_SELL,
                quantity=qty, order_type=kite.ORDER_TYPE_LIMIT,
                price=price, product=target_product
            )
            save_executed_exit(contract, oid, {"type": "LIMIT", "price": price, "qty": qty})
            logging.info(f"Closed {contract} with LIMIT order price {price} on exchange {target_exch} (Order ID: {oid})")
        except Exception as primary_err:
            logging.warning(f"Primary LIMIT exit with {target_product} on {target_exch} failed for {contract}: {primary_err}. Retrying with MARKET order...")
            try:
                oid = kite.place_order(
                    variety=kite.VARIETY_REGULAR, tradingsymbol=contract,
                    exchange=target_exch, transaction_type=kite.TRANSACTION_TYPE_SELL,
                    quantity=qty, order_type=kite.ORDER_TYPE_MARKET,
                    product=target_product
                )
                save_executed_exit(contract, oid, {"type": "MARKET", "price": ltp, "qty": qty})
                logging.info(f"Fallback MARKET exit SUCCESS for {contract} on exchange {target_exch} with product {target_product}")
            except Exception as m_err:
                logging.error(f"Fallback MARKET exit failed for {contract}: {m_err}")
    except Exception as e:
        logging.error(f"Exit failed for {contract}: {e}")
        logging.error(f"Exit failed for {contract}: {e}")

def load_program_config_for_engine(cfg_section, extra_fields=None):
    """Load engine config from program_config.json. Returns dict of applied overrides."""
    applied = {}
    try:
        possible_paths = [
            os.path.join(os.getcwd(), "input", "program_config.json"),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "input", "program_config.json"),
            os.path.join(os.path.dirname(__file__), "input", "program_config.json")
        ]
        cfg_path = next((p for p in possible_paths if os.path.exists(p)), None)
        if cfg_path:
            with open(cfg_path, encoding="utf-8") as f:
                full = json.load(f)
            if "_backtest" in full:
                applied["LIVE_MARKET_DEPLOYMENT"] = not bool(full["_backtest"])
            else:
                applied["LIVE_MARKET_DEPLOYMENT"] = True
            cfg = full.get(cfg_section, {})
            for src_key, dst_key in [
                ("timeframe_entry", "TIMEFRAME_ENTRY"),
                ("timeframe_anchor", "TIMEFRAME_ANCHOR"),
                ("lookback_days", "LOOKBACK_DAYS"),
                ("scan_interval", "SCAN_INTERVAL_SECONDS"),
                ("risk_percent", "MAX_RISK_PERCENT"),
                ("capital", "INITIAL_CAPITAL"),
            ]:
                if src_key in cfg:
                    applied[dst_key] = cfg[src_key]
            if extra_fields:
                for src_key, dst_key in extra_fields:
                    if src_key in cfg:
                        applied[dst_key] = cfg[src_key]
                    elif src_key in full:
                        applied[dst_key] = full[src_key]
        else:
            applied["LIVE_MARKET_DEPLOYMENT"] = True
    except Exception as e:
        logging.warning(f"Config load ({cfg_section}): {e}")
        applied["LIVE_MARKET_DEPLOYMENT"] = True
    return applied

def sync_kite_positions(kite, registry, positions_dict, lock, engine, timeframe_entry, timeframe_anchor):
    try:
        kite_pos = kite.positions()
        for p in kite_pos.get("net", []):
            sym = next((s for s in registry if s in p.get("tradingsymbol", "")), None)
            if not sym:
                continue
            nq = int(p.get("quantity", 0))
            if nq <= 0:
                with lock:
                    if sym in positions_dict:
                        del positions_dict[sym]
                continue
            contract = p["tradingsymbol"]
            entry = float(p.get("net_price") or p.get("buy_price") or p.get("average_price") or 0)
            lot_size = registry.get(sym, {}).get("lot_size", 1)
            is_stock = p.get("exchange", "") == "NSE"
            with lock:
                if sym in positions_dict:
                    if not positions_dict[sym].get("option_token"):
                        positions_dict[sym]["option_token"] = int(p.get("instrument_token", 0))
                    if not positions_dict[sym].get("user_edited"):
                        scan_sl = lookup_scan_sl_target(contract, sym, engine, kite, entry, timeframe_entry, timeframe_anchor)
                        if scan_sl:
                            for k, v in scan_sl.items():
                                positions_dict[sym][k] = v
                            tid = positions_dict[sym].get("trade_id")
                            if tid:
                                import trade_db
                                trade_db.update_trade(tid, scan_sl)
                    continue
                positions_dict[sym] = {
                    "contract": contract, "option_token": int(p.get("instrument_token", 0)),
                    "entry_spot": entry,
                    "current_sl": 0, "t1": 0, "t2": 0, "t3": 0,
                    "trailing_stage": 0, "lot_size": lot_size if not is_stock else 1,
                    "position_size": nq // lot_size if not is_stock else nq,
                    "pattern": "MANUAL_ENTRY",
                    "timeframe": timeframe_entry, "side": "CE",
                    "entry_time": dt.now().isoformat(),
                    "position_type": "stock" if is_stock else "option"
                }
            import trade_db
            tid = trade_db.create_trade(engine, sym, {"contract": contract, "entry_spot": entry, "current_sl": 0, "t1": 0, "t2": 0, "t3": 0, "lot_size": lot_size, "pattern": "MANUAL_ENTRY", "entry_time": dt.now().isoformat()})
            with lock:
                positions_dict[sym]["trade_id"] = tid
            logging.info(f"[KITE_SYNC] New manual position: {contract} entry={entry}")
            scan_sl = lookup_scan_sl_target(contract, sym, engine, kite, entry, timeframe_entry, timeframe_anchor)
            if scan_sl:
                with lock:
                    for k, v in scan_sl.items():
                        positions_dict[sym][k] = v
                trade_db.update_trade(tid, scan_sl)
                logging.info(f"[KITE_SYNC] Applied scan SL/Target for {contract}: SL={scan_sl.get('current_sl')} T1={scan_sl.get('t1')} T2={scan_sl.get('t2')} T3={scan_sl.get('t3')}")
    except Exception as e:
        logging.warning(f"Kite position sync failed: {e}")

def derive_sl_targets_for_contract(kite, contract, entry_price, timeframe_entry="30minute", timeframe_anchor="30minute"):
    """
    Derive SL and Targets for a specific contract.
    - Targets are derived strictly from Negation Theory (find_profit_targets non-negated swing high levels).
    - SL Exception for Manual Entries: If pattern SL is looser than 10% or missing, SL is set to Entry_Price * 0.90 (10% max loss).
    """
    try:
        ref_now = dt.now()
        from_d = (ref_now - timedelta(days=30)).strftime("%Y-%m-%d")
        to_d = ref_now.strftime("%Y-%m-%d")
        
        contract_str = str(contract).upper()
        if "SENSEX" in contract_str or "BSE" in contract_str:
            exch = "BFO"
        elif "CE" in contract_str or "PE" in contract_str or "NIFTY" in contract_str or "BANK" in contract_str:
            exch = "NFO"
        else:
            exch = "NSE"
            
        quote_key = f"{exch}:{contract}"
        ep = float(entry_price) if (entry_price and float(entry_price) > 0) else 0.0
        max_loss_sl = round(ep * 0.90, 2) if ep > 0 else 0.0

        token = None
        if kite:
            try:
                q = kite.quote([quote_key])
                token = q.get(quote_key, {}).get("instrument_token")
                if not ep:
                    ep = float(q.get(quote_key, {}).get("last_price", 0))
                    max_loss_sl = round(ep * 0.90, 2) if ep > 0 else 0.0
            except Exception as q_err:
                logging.warning(f"Kite quote error for {quote_key}: {q_err}")

        df_e, df_a = None, None
        if kite and token:
            try:
                df_e = fetch_and_resample_candles(kite, token, from_d, to_d, timeframe_entry)
                df_a = fetch_and_resample_candles(kite, token, from_d, to_d, timeframe_anchor)
            except Exception as fetch_err:
                logging.warning(f"Candle fetch error for {contract}: {fetch_err}")

        sl_val = None
        t1, t2, t3 = None, None, None
        pattern_name = "NEGATION_DERIVED_MANUAL"

        if df_a is not None and len(df_a) >= 5:
            res = scan_anchor_bcd_breakout(df_e if df_e is not None else df_a, df_a)
            if res:
                pattern_name = res.get("Pattern", "ABC_BREAKOUT")
                pattern_sl = float(res["SL"])
                if ep > 0:
                    sl_val = max(pattern_sl, max_loss_sl) if pattern_sl < ep else max_loss_sl
                else:
                    sl_val = pattern_sl
            else:
                anchor_low = float(df_a.iloc[-10:]['low'].min())
                swing_sl = round(anchor_low - max(0.50, anchor_low * 0.02), 2)
                if ep > 0:
                    sl_val = max(swing_sl, max_loss_sl) if (swing_sl > 0 and swing_sl < ep) else max_loss_sl
                else:
                    sl_val = swing_sl
                pattern_name = "TIMEFRAME_SWING_MANUAL"

            # Always derive Targets via Negation Theory on df_a!
            t1, t2, t3 = find_profit_targets(df_a, ep if ep > 0 else float(df_a.iloc[-1]['close']), stop_loss=sl_val)

        # Fallback for SL if missing
        if sl_val is None or sl_val <= 0 or (ep > 0 and sl_val >= ep):
            sl_val = max_loss_sl if max_loss_sl > 0 else (round(ep * 0.90, 2) if ep > 0 else 0.0)

        # Fallback for T1 only if Negation Theory target is missing or below entry
        if ep > 0 and sl_val > 0 and sl_val < ep:
            risk = round(ep - sl_val, 2)
            if t1 is None or t1 <= ep:
                t1 = round(ep + (1.88 * risk), 2)

        return {
            "entry_price": round(ep, 2) if ep else 0.0,
            "current_sl": sl_val,
            "t1": t1,
            "t2": t2,
            "t3": t3,
            "pattern": pattern_name
        }
    except Exception as e:
        logging.warning(f"Derive contract SL/Target failed for {contract}: {e}")
        if entry_price and float(entry_price) > 0:
            ep = float(entry_price)
            sl_val = round(ep * 0.90, 2)
            risk = round(ep - sl_val, 2)
            return {
                "entry_price": round(ep, 2),
                "current_sl": sl_val,
                "t1": round(ep + 1.88 * risk, 2),
                "t2": round(ep + 2.50 * risk, 2),
                "t3": round(ep + 3.50 * risk, 2),
                "pattern": "FALLBACK_10PCT_MANUAL"
            }
        return None

def lookup_scan_sl_target(contract, symbol, engine, kite=None, entry_price=0, timeframe_entry="15minute", timeframe_anchor="15minute", entry_date=None, is_stock=False):
    """
    Search trade_db and scan display files using:
    - Options: contract name as primary key (e.g. NIFTY26JUL23850CE)
    - Stocks: symbol + entry_date as primary key (e.g. RELIANCE_2026-07-23)
    If not found, fall back to Negation Theory derivation on contract/stock historical candles.
    """
    if not entry_date:
        entry_date = dt.now().strftime("%Y-%m-%d")
        
    stock_key = f"{symbol}_{entry_date}".replace(" ", "").upper()
    clean_c = stock_key if is_stock else str(contract or symbol).replace(" ", "").upper()

    # 0. Check sl_target_overrides.json first for any user overrides
    try:
        ov_paths = [
            os.path.join(os.getcwd(), "output", "monitor", "sl_target_overrides.json"),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "output", "monitor", "sl_target_overrides.json"),
            os.path.join(os.path.dirname(__file__), "output", "monitor", "sl_target_overrides.json")
        ]
        ov_path = next((p for p in ov_paths if os.path.exists(p)), None)
        if ov_path:
            with open(ov_path, encoding="utf-8") as f:
                overrides = json.load(f)
            best_match = None
            best_len = -1
            for eng_k in (engine, "nifty50", "index"):
                eng_ov = overrides.get(eng_k, {})
                for sym_k, vals in eng_ov.items():
                    clean_k = str(sym_k).replace(" ", "").upper()
                    if not clean_k:
                        continue
                    if clean_k == clean_c:
                        best_match = (vals, len(clean_k))
                        break
                    if clean_c in clean_k and len(clean_k) > best_len:
                        best_match = (vals, len(clean_k))
                        best_len = len(clean_k)
                if best_match:
                    break
            if best_match:
                vals = best_match[0]
                return {
                    "current_sl": vals.get("current_sl"),
                    "t1": vals.get("t1"),
                    "t2": vals.get("t2"),
                    "t3": vals.get("t3"),
                    "pattern": "USER_OVERRIDE",
                    "user_edited": True
                }
    except Exception:
        pass
    
    try:
        import trade_db
        all_trades = trade_db.get_all_trades(engine)
        best_db = None
        best_len = -1
        for t in all_trades:
            t_is_stock = t.get("position_type") == "stock"
            t_date = (t.get("created_at") or t.get("entry_time") or "")[:10]
            tc = f"{t.get('symbol')}_{t_date}".replace(" ", "").upper() if t_is_stock else str(t.get("contract") or t.get("symbol") or "").replace(" ", "").upper()
            if not tc:
                continue
            if tc == clean_c:
                best_db = t
                break
            if clean_c in tc and len(tc) > best_len:
                best_db = t
                best_len = len(tc)
        if best_db:
            sl = best_db.get("current_sl")
            t1 = best_db.get("t1")
            if sl and t1:
                return {"current_sl": sl, "t1": t1, "t2": best_db.get("t2"), "t3": best_db.get("t3"), "pattern": best_db.get("pattern", "DB_SYNC")}
    except Exception:
        pass

    paths = {"index": "output/monitor/scan_display_index.json", "nifty50": "output/monitor/scan_display_data.json"}
    path = paths.get(engine)
    if path and os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
            for section in ("staged_trades", "carry_forward", "active_live"):
                for trade in data.get(section, []):
                    t_date = (trade.get("entry_time") or "")[:10]
                    tc = f"{trade.get('symbol')}_{t_date}".replace(" ", "").upper() if is_stock else str(trade.get("contract") or trade.get("symbol") or "").replace(" ", "").upper()
                    if not tc:
                        continue
                    if tc == clean_c:
                        sl = trade.get("current_sl")
                        t1 = trade.get("t1")
                        if sl and t1:
                            return {"current_sl": sl, "t1": t1, "t2": trade.get("t2"), "t3": trade.get("t3"), "pattern": trade.get("pattern", "SCAN_SYNC")}
                if clean_c:
                    best_t = None
                    best_len = -1
                    for trade in data.get(section, []):
                        t_date = (trade.get("entry_time") or "")[:10]
                        tc = f"{trade.get('symbol')}_{t_date}".replace(" ", "").upper() if is_stock else str(trade.get("contract") or trade.get("symbol") or "").replace(" ", "").upper()
                        if tc and clean_c in tc and len(tc) > best_len:
                            best_t = trade
                            best_len = len(tc)
                    if best_t:
                        sl = best_t.get("current_sl")
                        t1 = best_t.get("t1")
                        if sl and t1:
                            return {"current_sl": sl, "t1": t1, "t2": best_t.get("t2"), "t3": best_t.get("t3"), "pattern": best_t.get("pattern", "SCAN_SYNC")}
        except Exception:
            pass

    if kite and (contract or symbol) and entry_price > 0:
        return derive_sl_targets_for_contract(kite, contract or symbol, entry_price, timeframe_entry, timeframe_anchor)

    return None

def write_scan_display_data(staged, active, display_file, engine_name=None):
    try:
        now_str = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        today = dt.now().strftime("%Y-%m-%d")
        import trade_db
        db_trades = trade_db.get_all_trades(engine_name) if engine_name else []
        db_map = {}
        for dbt in db_trades:
            c = str(dbt.get("contract") or dbt.get("symbol") or "").replace(" ", "").upper()
            if c: db_map[c] = dbt

        def build_trade(t, result, entry_time, exit_time):
            contract = t.get("contract") or t.get("symbol") or ""
            c_clean = str(contract).replace(" ", "").upper()
            db_record = db_map.get(c_clean)
            entry = db_record.get("entry_spot") if (db_record and db_record.get("entry_spot") is not None) else t.get("entry_spot")
            sl = db_record.get("current_sl") if (db_record and db_record.get("current_sl")) else t.get("current_sl")
            t1 = db_record.get("t1") if (db_record and db_record.get("t1")) else t.get("t1")
            t2 = db_record.get("t2") if (db_record and db_record.get("t2")) else t.get("t2")
            t3 = db_record.get("t3") if (db_record and db_record.get("t3")) else t.get("t3")
            pattern = db_record.get("pattern") if (db_record and db_record.get("pattern")) else t.get("pattern", "")
            rr = calc_rr(entry, sl, t1, t2) if entry is not None else 0
            return {
                "symbol": t.get("symbol", ""),
                "contract": contract,
                "side": t.get("side", ""),
                "entry_spot": entry,
                "current_sl": sl,
                "t1": t1,
                "t2": t2,
                "t3": t3,
                "pattern": pattern,
                "entry_time": clean_timestamp(entry_time),
                "exit_time": clean_timestamp(exit_time),
                "result": result,
                "carry_forward": False,
                "rr": round(rr, 2),
                "candle_a_time": clean_timestamp(t.get("candle_a_time") or t.get("CandleATime") or t.get("entry_time", "")),
                "timeframe": t.get("timeframe", ""),
                "candle_tf_time": t.get("candle_tf_time", "")
            }
        new_staged = [build_trade(t, t.get("pattern", "BE_ABCD"), t.get("entry_time", now_str), None) for t in (staged or [])]
        carry_fwd = []
        active_live = []
        active_keys = set()
        for s, p in active.items():
            t = p.copy()
            t["symbol"] = s
            et = p.get("entry_time", now_str)
            entry_date = et[:10] if isinstance(et, str) else today
            cf = entry_date < today
            entry_time_display = et if isinstance(et, str) else now_str
            trade = build_trade(t, "ACTIVE", entry_time_display, None)
            trade["carry_forward"] = cf
            if cf:
                carry_fwd.append(trade)
            else:
                active_live.append(trade)
            c = str(p.get("contract") or "").replace(" ", "").upper()
            if c: active_keys.add(c)

        def _trade_key(t):
            return str(t.get("contract") or t.get("symbol") or "").replace(" ", "").upper()

        cleared_at = None
        preserved = []
        staged_list = new_staged if new_staged else []

        # Deduplicate staged trades by unique contract key: keep freshest entry_time & highest RR
        contract_map = {}
        for t in staged_list:
            key = _trade_key(t)
            if not key or key in active_keys:
                continue
            if key not in contract_map:
                contract_map[key] = t
            else:
                prev = contract_map[key]
                prev_time = str(prev.get("entry_time") or "")
                curr_time = str(t.get("entry_time") or "")
                if curr_time > prev_time or (curr_time == prev_time and float(t.get("rr", 0)) > float(prev.get("rr", 0))):
                    contract_map[key] = t

        deduped_staged = list(contract_map.values())
        deduped_staged.sort(key=lambda x: float(x.get("rr", 0)), reverse=True)

        data = {
            "date": today,
            "timestamp": now_str,
            "staged_trades": deduped_staged,
            "carry_forward": carry_fwd,
            "active_live": active_live
        }
        if engine_name:
            data["engine"] = engine_name
        os.makedirs(os.path.dirname(display_file), exist_ok=True)
        with open(display_file, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.error(f"Display data write failed: {e}")

def derive_sl_targets_for_symbol(kite, symbol, entry_price, registry, timeframe_entry, timeframe_anchor, lookback_days, resolve_fn):
    """Run ABC reversal + anchor scanners on a single symbol to derive SL/T1/T2/T3."""
    try:
        config = registry.get(symbol)
        if not config:
            return None
        ref_now = dt.now()
        limits = {"minute": 60, "3minute": 100, "5minute": 100, "10minute": 100, "15minute": 200, "30minute": 200, "60minute": 400, "75minute": 400, "75min": 400, "day": 2000}
        max_days = limits.get(timeframe_entry, 200)
        from_d = (ref_now - timedelta(days=min(lookback_days, max_days))).strftime("%Y-%m-%d")
        to_d = ref_now.strftime("%Y-%m-%d")
        spot_quote = kite.ltp([config["token"]])
        current_spot = float(list(spot_quote.values())[0]["last_price"])
        step = config["strike_step"]
        ce_opts = resolve_fn(symbol, current_spot, step, "CE", 0)
        pe_opts = resolve_fn(symbol, current_spot, step, "PE", 0)
        ce_map = {c["strike"]: c for c in ce_opts}
        pe_map = {p["strike"]: p for p in pe_opts}
        for strike in sorted(set(ce_map) & set(pe_map)):
            ce, pe = ce_map[strike], pe_map[strike]
            for side, opt in [("CE", ce), ("PE", pe)]:
                df_e = fetch_and_resample_candles(kite, opt["token"], from_d, to_d, timeframe_entry)
                df_a = fetch_and_resample_candles(kite, opt["token"], from_d, to_d, timeframe_anchor)
                if len(df_e) < 5 or len(df_a) < 5:
                    continue
                result = scan_anchor_bcd_breakout(df_e, df_a)
                if result:
                    return {"SL": result["SL"], "T1": result["T1"], "T2": result["T2"], "T3": result["T3"], "pattern": result["Pattern"], "side": side, "strike": strike}
                anchor_scanners = [find_anchor_bullish_engulfing, find_anchor_ll_sweep, find_anchor_hammer_baby, find_anchor_bullish_harami, find_anchor_two_higher_highs]
                for scanner in anchor_scanners:
                    res = scanner(df_a)
                    if res:
                        t1, t2, t3 = find_profit_targets(df_a, entry_price, stop_loss=res.get("SL"))
                        if t1:
                            return {"SL": res["SL"], "T1": t1, "T2": t2, "T3": t3, "pattern": res["Pattern"], "side": side, "strike": strike}
        return None
    except Exception as e:
        logging.warning(f"SL/Target derivation failed for {symbol}: {e}")
        return None

def reconcile_positions(kite, registry, positions_dict, lock, engine, timeframe_entry, timeframe_anchor, lookback_days, resolve_fn, save_state_fn=None):
    """Cross-reference ACTIVE_POSITIONS against Kite open positions and DB."""
    today = dt.now().strftime("%Y-%m-%d")
    kite_symbols = set()
    try:
        kite_pos = kite.positions()
        for plist in [kite_pos.get("day", []), kite_pos.get("net", [])]:
            for p in plist:
                sym = next((s for s in registry if s in p.get("tradingsymbol", "")), None)
                if sym and abs(int(p.get("quantity", 0))) > 0:
                    kite_symbols.add(sym)
    except Exception as e:
        logging.warning(f"Kite position fetch for reconciliation failed: {e}")
    import trade_db
    db_active = {t["symbol"] for t in trade_db.get_active_trades(engine) if t.get("symbol") in registry}
    with lock:
        stale_zero = [s for s, p in list(positions_dict.items())
                      if p.get("pattern") == "KITE_RECOVERED"
                      and p.get("position_type") != "stock"
                      and (p.get("entry_spot") or 0) == 0
                      and (p.get("current_sl") or 0) == 0]
        for s in stale_zero:
            logging.info(f"[RECONCILE] Removing stale KITE_RECOVERED ghost: {s}")
            tid = positions_dict[s].get("trade_id")
            if tid:
                try: trade_db.remove_trades([tid])
                except Exception: pass
            positions_dict.pop(s, None)
        if stale_zero:
            logging.info(f"[RECONCILE] Purged {len(stale_zero)} ghost positions")
        stale = [s for s in positions_dict if s not in registry] + \
                [s for s in positions_dict if s in registry and s not in kite_symbols and s not in db_active]
        for s in stale:
            pos = positions_dict[s]
            tid = pos.get("trade_id")
            logging.info(f"[RECONCILE] Removing stale position: {s}")
            if tid:
                trade_db.remove_trades([tid])
            positions_dict.pop(s, None)
        for s, pos in list(positions_dict.items()):
            now_str = dt.now().isoformat()
            if "entry_time" not in pos:
                pos["entry_time"] = now_str
            entry_date = pos["entry_time"][:10] if isinstance(pos["entry_time"], str) else today
            pos["carry_forward"] = entry_date < today
            if (pos.get("current_sl") or 0) == 0 or (pos.get("t1") or 0) == 0:
                db_found = False
                contract = pos.get("contract", "")
                if contract:
                    all_trades = trade_db.get_all_trades(engine)
                    for t in all_trades:
                        if t.get("contract") == contract and t.get("current_sl") and t.get("t1"):
                            pos["current_sl"] = t["current_sl"]
                            pos["t1"] = t["t1"]
                            pos["t2"] = t.get("t2")
                            pos["t3"] = t.get("t3")
                            pos["pattern"] = t.get("pattern", pos.get("pattern", "DB_RECOVERED"))
                            db_found = True
                            logging.info(f"[RECONCILE] Restored SL/Targets for {s} from DB: SL={pos['current_sl']} T1={pos['t1']}")
                            tid = pos.get("trade_id")
                            if tid:
                                trade_db.update_trade(tid, {"current_sl": pos["current_sl"], "t1": pos["t1"], "t2": pos["t2"], "t3": pos["t3"]})
                            break
                if not db_found:
                    config = registry.get(s)
                    if config:
                        result = derive_sl_targets_for_symbol(kite, s, pos.get("entry_spot", 0), registry, timeframe_entry, timeframe_anchor, lookback_days, resolve_fn)
                        if result:
                            pos["current_sl"] = result["SL"]
                            pos["t1"] = result["T1"]
                            pos["t2"] = result["T2"]
                            pos["t3"] = result["T3"]
                            pos["pattern"] = result.get("pattern", pos.get("pattern", "DERIVED"))
                            pos["side"] = result.get("side", pos.get("side", "CE"))
                            pos["strike"] = result.get("strike", pos.get("strike", 0))
                            tid = pos.get("trade_id")
                            if tid:
                                trade_db.update_trade(tid, {"current_sl": result["SL"], "t1": result["T1"], "t2": result["T2"], "t3": result["T3"], "pattern": pos["pattern"]})
                            logging.info(f"[RECONCILE] Derived SL/Targets for {s}: SL={result['SL']} T1={result['T1']} T2={result['T2']} T3={result['T3']}")
                        else:
                            logging.info(f"[RECONCILE] No pattern match for {s}, leaving as passive tracking")
    SL_TARGET_OVERRIDES_FILE = os.path.join("output", "monitor", "sl_target_overrides.json")
    if os.path.exists(SL_TARGET_OVERRIDES_FILE):
        try:
            with open(SL_TARGET_OVERRIDES_FILE) as f:
                eng_overrides = json.load(f).get(engine, {})
            for sym, vals in eng_overrides.items():
                if sym in positions_dict:
                    for k in ("current_sl", "t1", "t2", "t3"):
                        if k in vals:
                            positions_dict[sym][k] = vals[k]
                    tid = positions_dict[sym].get("trade_id")
                    if tid:
                        trade_db.update_trade(tid, {k: positions_dict[sym][k] for k in ("current_sl", "t1", "t2", "t3") if k in positions_dict[sym]})
                    logging.info(f"[RECONCILE] Re-applied override for {sym}: SL={positions_dict[sym].get('current_sl')} T1={positions_dict[sym].get('t1')}")
        except Exception as e:
            logging.warning(f"Override re-apply failed: {e}")
    if save_state_fn:
        save_state_fn()

def is_anchor_valid_and_active(df_anchor, candle_a_time, sl_target, t1_target):
    """
    Generic Universal Rule (Anchor TF Specific):
    For a given Anchor TF dataframe (`df_anchor`), verify that:
    1. Anchor is newest/valid.
    2. No subsequent candle on this Anchor TF closed below SL (closing basis for SL).
    3. No subsequent candle on this Anchor TF touched T1 (high price >= T1).
    Returns True if Anchor is valid and active; False if invalidated or already completed.
    """
    if df_anchor is None or df_anchor.empty or not candle_a_time:
        return True
    try:
        c_time_str = str(candle_a_time)
        if 'date' not in df_anchor.columns:
            return True
        subseq = df_anchor[df_anchor['date'].astype(str) > c_time_str]
        if subseq.empty:
            return True
        
        sl_val = float(sl_target) if sl_target else 0.0
        t1_val = float(t1_target) if t1_target else 0.0
        
        # Rule 1: Discard if any subsequent Anchor TF candle closed below SL
        if sl_val > 0 and (subseq['close'].astype(float) <= sl_val).any():
            return False
            
        # Rule 2: Discard if any subsequent Anchor TF candle touched T1 (high >= T1)
        if t1_val > 0 and (subseq['high'].astype(float) >= t1_val).any():
            return False
            
        return True
    except Exception as e:
        logging.warning(f"Error checking anchor validity: {e}")
        return True

def is_setup_already_completed(df_candles, candle_time, t1_target, sl_target):
    """Return True if setup was completed or invalidated."""
    return not is_anchor_valid_and_active(df_candles, candle_time, sl_target, t1_target)

def find_newest_valid_anchor(df):
    """
    Scans `df` (Anchor TF candles) starting from the NEWEST candle going backwards.
    Finds the first (newest) valid Anchor pattern where:
    1. Pattern detector matches an Anchor (LL Sweep, Engulfing, Baby Candle, Harami, Two HH).
    2. No subsequent candle in `df` closed below SL (closing basis).
    3. No subsequent candle in `df` touched T1 (high >= T1).
    Returns the newest valid anchor dict (with T1, T2, T3, RR), or None.
    """
    if df is None or len(df) < 5:
        return None

    scanners = [
        find_anchor_ll_sweep,
        find_anchor_bullish_engulfing,
        find_anchor_hammer_baby,
        find_anchor_bullish_harami,
        find_anchor_two_higher_highs,
    ]

    for end_idx in range(len(df), 4, -1):
        sub_df = df.iloc[:end_idx]
        for scanner_func in scanners:
            result = scanner_func(sub_df)
            if result:
                candle_a_time = str(result.get("CandleATime") or "")
                sl_val = result["SL"]
                t1, t2, t3 = find_profit_targets(df, result["Close"], stop_loss=sl_val)
                
                # Check validity on all subsequent candles in the full dataframe
                if is_anchor_valid_and_active(df, candle_a_time, sl_val, t1):
                    risk = round(result["Close"] - sl_val, 2) if (result["Close"] > sl_val) else 0.0
                    rr = round((t1 - result["Close"]) / risk, 2) if (t1 and risk > 0) else 0.0
                    return {
                        "Pattern": result["Pattern"],
                        "Close": result["Close"],
                        "SL": sl_val,
                        "T1": t1, "T2": t2, "T3": t3,
                        "RR": rr,
                        "CandleATime": candle_a_time
                    }
    return None

def safe_kite_call(func, *args, retries=3, delay=0.8, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            err_str = str(err).lower()
            if "too many" in err_str or "requests" in err_str or "access_token" in err_str or "api_key" in err_str or "429" in err_str:
                time.sleep(delay * (attempt + 1))
            else:
                raise err
    return func(*args, **kwargs)

def scan_symbol(kite, symbol, config, from_entry, to_entry, from_anchor, to_anchor,
                entry_scanners, anchor_scanners, resolve_fn, engine_name,
                timeframe_entry, timeframe_anchor, timeframe_fallback,
                active_positions, position_lock, trade_db, strike_range,
                log_fn):
    trades = []
    try:
        spot_quote = safe_kite_call(kite.ltp, [config["token"]])
        current_spot = float(list(spot_quote.values())[0]["last_price"])
    except Exception:
        try:
            df_spot = safe_kite_call(fetch_and_resample_candles, kite, config["token"], from_entry, to_entry, timeframe_entry)
            if df_spot.empty:
                return []
            current_spot = float(df_spot.iloc[-1]['close'])
        except Exception as e:
            logging.warning(f"Spot data failed for {symbol}: {e}")
            return []
    ce_list = resolve_fn(symbol, current_spot, config['strike_step'], "CE", strike_range)
    pe_list = resolve_fn(symbol, current_spot, config['strike_step'], "PE", strike_range)
    ce_map = {c["strike"]: c for c in ce_list}
    pe_map = {p["strike"]: p for p in pe_list}
    for strike in sorted(set(ce_map) & set(pe_map)):
        ce = ce_map[strike]
        pe = pe_map[strike]
        same_tf = timeframe_entry == timeframe_anchor and from_entry == from_anchor and to_entry == to_anchor
        dfs = {}
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                tasks = {
                    pool.submit(fetch_and_resample_candles, kite, ce["token"], from_entry, to_entry, timeframe_entry): ("ce", "entry"),
                    pool.submit(fetch_and_resample_candles, kite, pe["token"], from_entry, to_entry, timeframe_entry): ("pe", "entry"),
                }
                if not same_tf:
                    tasks[pool.submit(fetch_and_resample_candles, kite, ce["token"], from_anchor, to_anchor, timeframe_anchor)] = ("ce", "anchor")
                    tasks[pool.submit(fetch_and_resample_candles, kite, pe["token"], from_anchor, to_anchor, timeframe_anchor)] = ("pe", "anchor")
                for f in as_completed(tasks):
                    tag, kind = tasks[f]
                    try:
                        dfs[(tag, kind)] = pd.DataFrame(f.result())
                    except Exception as e:
                        logging.warning(f"{tag} {kind} failed for {symbol} {strike}: {e}")
                        dfs[(tag, kind)] = pd.DataFrame()
        except Exception as e:
            logging.warning(f"Contract data failed for {symbol} {strike}: {e}")
            continue
        if same_tf:
            dfs[("ce", "anchor")] = dfs.get(("ce", "entry"), pd.DataFrame())
            dfs[("pe", "anchor")] = dfs.get(("pe", "entry"), pd.DataFrame())
        for tag_key, kind_key, from_d, to_d in [
            ("ce", "entry", from_entry, to_entry),
            ("pe", "entry", from_entry, to_entry),
            ("ce", "anchor", from_anchor, to_anchor),
            ("pe", "anchor", from_anchor, to_anchor),
        ]:
            if same_tf and kind_key == "anchor":
                continue
            df = dfs.get((tag_key, kind_key), pd.DataFrame())
            if len(df) < 5:
                tok = ce["token"] if tag_key == "ce" else pe["token"]
                tf = timeframe_entry if kind_key == "entry" else timeframe_anchor
                dfs[(tag_key, kind_key)] = fetch_option_data(kite, tok, from_d, to_d, tf, timeframe_fallback)
        if same_tf:
            dfs[("ce", "anchor")] = dfs.get(("ce", "entry"), pd.DataFrame())
            dfs[("pe", "anchor")] = dfs.get(("pe", "entry"), pd.DataFrame())
        df_ce_e = dfs.get(("ce", "entry"), pd.DataFrame())
        df_pe_e = dfs.get(("pe", "entry"), pd.DataFrame())
        df_ce_a = dfs.get(("ce", "anchor"), pd.DataFrame())
        df_pe_a = dfs.get(("pe", "anchor"), pd.DataFrame())
        if df_ce_e.empty or df_pe_e.empty:
            continue
        matched = False
        for name, scanner in entry_scanners:
            if matched:
                break
            result_ce = scanner(df_ce_e, df_ce_a)
            if result_ce:
                candle_time = str(result_ce.get("CandleTime") or df_ce_e.iloc[-1]['date'])
                if result_ce["Close"] < 300 and result_ce["T1"] > result_ce["Close"] * 5:
                    log_fn(ce['tradingsymbol'], result_ce["Pattern"], timeframe_entry,
                           "SCAN_MATCH", "NO_TARGETS", "Stale ITM regime targets",
                           entry=result_ce["Close"], sl=result_ce["SL"],
                           target=0, rr=0, event_time=candle_time)
                    matched = True
                    break
                key = f"{symbol}|{result_ce['Pattern']}|CE|{strike}"
                candle_a_time = str(result_ce.get("CandleATime", ""))
                if not is_anchor_valid_and_active(df_ce_a, candle_a_time or candle_time, result_ce.get("SL"), result_ce.get("T1")):
                    logging.info(f"CE MATCH already completed T1/SL (skip): {ce['tradingsymbol']} | {result_ce['Pattern']}")
                    matched = True
                    break
                pos_size = calculate_position_size(current_spot, result_ce["SL"])
                rr_str = f"RR: {result_ce['RR']}" if result_ce.get('RR') else ""
                candle_a_time = str(result_ce.get("CandleATime", ""))
                logging.info(f"CYCLE MATCH staged: {ce['tradingsymbol']} | {result_ce['Pattern']} | CE | Strike {strike} | Size: {pos_size} | Entry: {result_ce['Close']:.2f} | SL: {result_ce['SL']:.2f} | T1: {result_ce['T1']} | T2: {result_ce['T2']} | T3: {result_ce['T3']} | RR: {result_ce.get('RR', '')} | A:{candle_a_time} D:{candle_time}")
                trade_data = {
                    "symbol": symbol, "contract": ce['tradingsymbol'], "option_token": ce['token'],
                    "index_token": config["token"], "strike": strike, "entry_spot": result_ce["Close"],
                    "current_sl": result_ce["SL"], "t1": result_ce["T1"], "t2": result_ce["T2"],
                    "t3": result_ce["T3"], "rr": result_ce.get("RR"), "trailing_stage": 0,
                    "lot_size": config["lot_size"], "position_size": pos_size,
                    "pattern": result_ce["Pattern"], "timeframe": timeframe_entry, "side": "CE",
                    "strike_step": config["strike_step"], "entry_time": candle_time,
                    "candle_a_time": candle_a_time
                }
                trade_db.stage_cycle_trade(engine_name, trade_data)
                trades.append(trade_data)
                log_fn(ce['tradingsymbol'], result_ce['Pattern'], timeframe_entry,
                       "SCAN_MATCH", "STAGED", f"Side=CE Strike={strike} RR={result_ce.get('RR','')}",
                       entry=result_ce['Close'], sl=result_ce['SL'],
                       target=result_ce.get('T3',''), rr=result_ce.get('RR',''),
                       event_time=candle_time)
                matched = True
                break
            result_pe = scanner(df_pe_e, df_pe_a)
            if result_pe:
                candle_time = str(result_pe.get("CandleTime") or df_pe_e.iloc[-1]['date'])
                if result_pe["Close"] < 300 and result_pe["T1"] > result_pe["Close"] * 5:
                    log_fn(pe['tradingsymbol'], result_pe["Pattern"], timeframe_entry,
                           "SCAN_MATCH", "NO_TARGETS", "Stale ITM regime targets",
                           entry=result_pe["Close"], sl=result_pe["SL"],
                           target=0, rr=0, event_time=candle_time)
                    matched = True
                    break
                key = f"{symbol}|{result_pe['Pattern']}|PE|{strike}"
                candle_a_time = str(result_pe.get("CandleATime", ""))
                if not is_anchor_valid_and_active(df_pe_a, candle_a_time or candle_time, result_pe.get("SL"), result_pe.get("T1")):
                    logging.info(f"PE MATCH already completed T1/SL (skip): {pe['tradingsymbol']} | {result_pe['Pattern']}")
                    matched = True
                    break
                pos_size = calculate_position_size(current_spot, result_pe["SL"])
                candle_a_time = str(result_pe.get("CandleATime", ""))
                logging.info(f"CYCLE MATCH staged: {pe['tradingsymbol']} | {result_pe['Pattern']} | PE | Strike {strike} | Size: {pos_size} | Entry: {result_pe['Close']:.2f} | SL: {result_pe['SL']:.2f} | T1: {result_pe['T1']} | T2: {result_pe['T2']} | T3: {result_pe['T3']} | RR: {result_pe.get('RR', '')} | A:{candle_a_time} D:{candle_time}")
                trade_data = {
                    "symbol": symbol, "contract": pe['tradingsymbol'], "option_token": pe['token'],
                    "index_token": config["token"], "strike": strike, "entry_spot": result_pe["Close"],
                    "current_sl": result_pe["SL"], "t1": result_pe["T1"], "t2": result_pe["T2"],
                    "t3": result_pe["T3"], "rr": result_pe.get("RR"), "trailing_stage": 0,
                    "lot_size": config["lot_size"], "position_size": pos_size,
                    "pattern": result_pe["Pattern"], "timeframe": timeframe_entry, "side": "PE",
                    "strike_step": config["strike_step"], "entry_time": candle_time,
                    "candle_a_time": candle_a_time
                }
                trade_db.stage_cycle_trade(engine_name, trade_data)
                trades.append(trade_data)
                log_fn(pe['tradingsymbol'], result_pe['Pattern'], timeframe_entry,
                       "SCAN_MATCH", "STAGED", f"Side=PE Strike={strike} RR={result_pe.get('RR','')}",
                       entry=result_pe['Close'], sl=result_pe['SL'],
                       target=result_pe.get('T3',''), rr=result_pe.get('RR',''),
                       event_time=candle_time)
                matched = True
                break
        for name, scanner in anchor_scanners:
            res_ce = scanner(df_ce_a) if not df_ce_a.empty else None
            if res_ce:
                logging.info(f"ANCHOR FORMED: {ce['tradingsymbol']} | {res_ce['Pattern']} | Close: {res_ce['Close']:.2f} | SL: {res_ce['SL']:.2f}")
                continue
            res_pe = scanner(df_pe_a) if not df_pe_a.empty else None
            if res_pe:
                logging.info(f"ANCHOR FORMED: {pe['tradingsymbol']} | {res_pe['Pattern']} | Close: {res_pe['Close']:.2f} | SL: {res_pe['SL']:.2f}")
    return trades


def _load_program_config_file():
    possible_paths = [
        os.path.join(os.getcwd(), "input", "program_config.json"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "input", "program_config.json"),
        os.path.join(os.path.dirname(__file__), "input", "program_config.json")
    ]
    cfg_path = next((p for p in possible_paths if os.path.exists(p)), None)
    if cfg_path:
        try:
            with open(cfg_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def monitor_active_positions(kite, registry, positions_dict, lock, product_type, engine_name,
                              timeframe_entry, trade_db, log_fn, save_state_fn=None,
                              live=True):
    from_date = (dt.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    to_date = dt.now().strftime("%Y-%m-%d")
    to_clear = []

    # Load sl_mode from program config if available ("hybrid", "candle_close", or "tick_ltp")
    cfg = _load_program_config_file()
    sl_mode = cfg.get("sl_mode", "hybrid")
    emergency_buffer_pct = float(cfg.get("emergency_buffer_pct", 0.15))
    failsafe_start_str = cfg.get("failsafe_start_time", "09:45")
    try:
        f_h, f_m = map(int, failsafe_start_str.split(":"))
        fs_start_t = datetime_time(f_h, f_m)
    except Exception:
        fs_start_t = datetime_time(9, 45)

    if dt.now().time() < fs_start_t:
        logging.debug(f"[FAILSAFE PAUSED BEFORE {failsafe_start_str} AM] Automated active position exit checks paused until {failsafe_start_str} AM.")
        return

    with lock:
        items = list(positions_dict.items())

    for sym, pos in items:
        try:
            token = pos.get("option_token") or registry.get(sym, {}).get("token")
            if not token:
                continue

            pos_tf = pos.get("timeframe") or timeframe_entry
            df = fetch_and_resample_candles(kite, token, from_date, to_date, pos_tf)
            if df.empty:
                continue

            last = df.iloc[-1]
            cp = float(last['close'])
            tid = pos.get("trade_id")
            is_stock = pos.get("position_type") == "stock"
            current_sl = float(pos.get("current_sl", 0))

            # Fetch live quote for LTP
            live_ltp = 0.0
            try:
                contract_name = pos.get("contract") or pos.get("symbol") or sym
                exch = "NSE" if is_stock else "NFO"
                q_key = f"{exch}:{contract_name}"
                q_res = kite.quote([q_key])
                if q_key in q_res:
                    q_info = q_res[q_key]
                    live_ltp = float(q_info.get("last_price", 0))
            except Exception as q_err:
                logging.debug(f"Live quote fetch error for {sym}: {q_err}")

            # Compute High (hp) strictly for candles AFTER trade entry_time + live_ltp
            entry_time_str = str(pos.get("entry_time", ""))
            hp = live_ltp if live_ltp > 0 else cp
            for idx in range(len(df)):
                c_row = df.iloc[idx]
                c_date = str(c_row.get('date', ''))
                if entry_time_str and c_date < entry_time_str[:16]:
                    continue
                hp = max(hp, float(c_row['high']))

            sl_hit = False
            sl_reason = ""
            event_time = last.get('date')

            # Track current TF candle timestamp on position for UI/monitoring
            with lock:
                if sym in positions_dict:
                    positions_dict[sym]["candle_tf_time"] = str(event_time) if event_time else ""
                    positions_dict[sym]["timeframe"] = pos_tf

            # 1) SL Evaluation (Separated SL Monitor: Skipped 09:15-09:45 AM, Active at 09:45 AM+)
            now_time_str = dt.now().strftime("%H:%M")
            is_before_0945 = now_time_str < "09:45"
            is_start_0945 = "09:45" <= now_time_str <= "09:47"

            if current_sl > 0:
                if is_before_0945:
                    # Target monitoring runs from 09:15 AM, but SL is skipped until 09:45 AM
                    pass
                elif is_start_0945:
                    # 09:45 AM Failsafe Check: If trading below SL & prev candle closed below SL & current candle trading below SL
                    prev_closed_below = (len(df) >= 2 and float(df.iloc[-2]['close']) <= current_sl)
                    curr_below = (live_ltp > 0 and live_ltp <= current_sl) or (float(df.iloc[-1]['close']) <= current_sl)
                    if curr_below and prev_closed_below:
                        sl_hit = True
                        sl_reason = f"SL_FAILSAFE_0945_TRIGGER (LTP {live_ltp:.2f} <= {current_sl:.2f} & Prev Bar Closed Below)"
                        cp = live_ltp if live_ltp > 0 else float(df.iloc[-1]['close'])
                        event_time = last.get('date')
                else:
                    # Normal Active SL Monitoring after 09:45 AM
                    entry_time_str = str(pos.get("entry_time", ""))
                    for idx in range(len(df)):
                        c_row = df.iloc[idx]
                        c_date = str(c_row.get('date', ''))
                        if entry_time_str and c_date < entry_time_str[:16]:
                            continue
                        if float(c_row['close']) <= current_sl:
                            sl_hit = True
                            sl_reason = f"CANDLE_CLOSE_SL ({pos_tf} Bar @ {c_date})"
                            cp = float(c_row['close'])
                            event_time = c_row.get('date')
                            break

            # 2) Emergency Hard Stop / Direct LTP evaluation (Active after 09:45 AM)
            if not sl_hit and current_sl > 0 and live_ltp > 0 and not is_before_0945:
                if sl_mode == "tick_ltp" and live_ltp <= current_sl:
                    sl_hit = True
                    sl_reason = f"TICK_LTP_SL ({live_ltp})"
                    cp = live_ltp
                elif sl_mode == "hybrid":
                    emergency_threshold = current_sl * (1.0 - emergency_buffer_pct)
                    if live_ltp <= emergency_threshold:
                        sl_hit = True
                        sl_reason = f"EMERGENCY_HARD_SL (LTP {live_ltp:.2f} <= {emergency_threshold:.2f})"
                        cp = live_ltp

            if sl_hit:
                logging.warning(f"SL [{sl_reason}]: {sym} at {cp} (TF: {pos_tf})")
                if is_stock:
                    close_stock_position(kite, pos, live, product_type)
                else:
                    close_position(kite, pos, live, product_type)
                entry_s = pos.get("entry_spot", 0)
                pnl = ((cp - entry_s) / entry_s * 100) if entry_s else 0
                log_fn(sym, pos.get("pattern", ""), pos_tf, "EXIT_SL", "CLOSED",
                       f"SL hit [{sl_reason}]: {cp}", pnl,
                       entry=entry_s, sl=current_sl, target=pos.get("t1", ""),
                       event_time=event_time)
                if tid:
                    trade_db.update_trade(tid, {
                        "status": "SL_HIT",
                        "exit_time": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "pnl_percent": round(pnl, 2),
                        "details": f"SL hit [{sl_reason}] | TF: {pos_tf}"
                    })
                to_clear.append(sym)
                continue
            t1_val = float(pos.get("t1")) if pos.get("t1") is not None and pos.get("t1") != "N/A" else None
            t2_val = float(pos.get("t2")) if pos.get("t2") is not None and pos.get("t2") != "N/A" else None
            t3_val = float(pos.get("t3")) if pos.get("t3") is not None and pos.get("t3") != "N/A" else None

            has_higher_targets = (t2_val is not None and t2_val > 0) or (t3_val is not None and t3_val > 0)

            # Early exit target buffers (1 to 2 points earlier to prevent missing out on wicks)
            def _get_target_buffer(t_val):
                if not t_val or t_val <= 0: return 0.0
                if t_val <= 50: return max(0.50, round(t_val * 0.015, 2))
                elif t_val <= 200: return max(1.00, round(t_val * 0.015, 2))
                else: return max(2.00, round(t_val * 0.010, 2))

            buf_t1 = _get_target_buffer(t1_val)
            buf_t2 = _get_target_buffer(t2_val)
            buf_t3 = _get_target_buffer(t3_val)

            # 3) Target Exits & Trailing Evaluation
            if t1_val and hp >= (t1_val - buf_t1):
                # RULE: If T2 or T3 is NOT available, exit 100% at T1 (early exit threshold)!
                if not has_higher_targets:
                    logging.info(f"T1 FULL EXIT (No T2/T3): {sym} reached {hp:.2f} (Target: {t1_val:.2f}, Buffer: {buf_t1:.2f})")
                    if is_stock:
                        close_stock_position(kite, pos, live, product_type)
                    else:
                        close_position(kite, pos, live, product_type)
                    entry_s = pos.get("entry_spot", 0)
                    pnl = ((t1_val - entry_s) / entry_s * 100) if entry_s else 0
                    log_fn(sym, pos.get("pattern", ""), pos_tf, "EXIT_T1", "CLOSED",
                           f"T1={t1_val:.2f} (Early Exit @ {hp:.2f})", pnl,
                           entry=entry_s, sl=pos.get("current_sl", ""), target=t1_val,
                           event_time=last.get('date'))
                    if tid:
                        trade_db.update_trade(tid, {
                            "status": "TARGET_HIT",
                            "exit_time": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "pnl_percent": round(pnl, 2),
                            "details": f"T1 exit ({hp:.2f} >= {t1_val - buf_t1:.2f})"
                        })
                    to_clear.append(sym)
                    continue
                elif pos.get("trailing_stage", 0) == 0:
                    new_sl = pos.get("entry_spot", 0)
                    with lock:
                        if sym in positions_dict:
                            positions_dict[sym]["current_sl"] = new_sl
                            positions_dict[sym]["trailing_stage"] = 1
                    logging.info(f"TRAIL-1 {sym}: SL=BE ({new_sl:.2f})")
                    log_fn(sym, pos.get("pattern", ""), timeframe_entry, "TRAIL_BE", "MUTATED",
                           f"SL={new_sl:.2f}",
                           entry=pos.get("entry_spot", 0), sl=new_sl, target=t1_val,
                           event_time=last.get('date'))
                    if tid:
                        trade_db.update_trade(tid, {"trailing_stage": 1, "current_sl": new_sl})

            if pos.get("trailing_stage", 0) == 1 and t2_val and hp >= (t2_val - buf_t2):
                new_sl = t1_val or pos.get("entry_spot", 0)
                with lock:
                    if sym in positions_dict:
                        positions_dict[sym]["current_sl"] = new_sl
                        positions_dict[sym]["trailing_stage"] = 2
                logging.info(f"TRAIL-2 {sym}: SL=T1 ({new_sl:.2f})")
                log_fn(sym, pos.get("pattern", ""), timeframe_entry, "TRAIL_T1", "MUTATED",
                       f"SL={new_sl:.2f}",
                       entry=pos.get("entry_spot", 0), sl=new_sl, target=t2_val,
                       event_time=last.get('date'))
                if tid:
                    trade_db.update_trade(tid, {"trailing_stage": 2, "current_sl": new_sl})

            if t3_val and hp >= (t3_val - buf_t3):
                logging.info(f"T3 EXIT: {sym} reached {hp:.2f} (Target: {t3_val:.2f})")
                if pos.get("position_type") == "stock":
                    close_stock_position(kite, pos, live, product_type)
                else:
                    close_position(kite, pos, live, product_type)
                entry_s = pos.get("entry_spot", 0)
                pnl = ((t3_val - entry_s) / entry_s * 100) if entry_s else 0
                log_fn(sym, pos.get("pattern", ""), timeframe_entry, "EXIT_T3", "CLOSED",
                       f"T3={t3_val:.2f}", pnl,
                       entry=entry_s, sl=pos.get("current_sl", ""), target=t3_val,
                       event_time=last.get('date'))
                if tid:
                    trade_db.update_trade(tid, {"status": "TARGET_HIT", "exit_time": dt.now().strftime("%Y-%m-%d %H:%M:%S"), "pnl_percent": round(pnl, 2)})
                to_clear.append(sym)
        except Exception as e:
            logging.error(f"Risk error {sym}: {e}")

    if to_clear:
        with lock:
            for s in to_clear:
                positions_dict.pop(s, None)

    if to_clear and save_state_fn:
        save_state_fn()
    if to_clear and save_state_fn:
        save_state_fn()


def simulate_trade_outcome(kite, trade, target_date, resolve_token_fn=None):
    try:
        sym = trade["symbol"]
        cp = trade["entry_spot"]
        side = trade.get("side", "CE")
        strike = trade.get("strike")
        strike_step = trade.get("strike_step", 50)
        token = trade.get("option_token")
        if not token and resolve_token_fn:
            target_strike = strike or int(round(cp / strike_step) * strike_step)
            opt_type = "CE" if side == "CE" else "PE"
            contract = resolve_token_fn(sym, cp, strike_step, opt_type, target_strike)
            if not contract:
                return {"result": None, "detail": "option_resolve_failed", "entry_time": None, "exit_time": None, "pnl_pct": None}
            token = contract
        if not token:
            return {"result": None, "detail": "no_token", "entry_time": None, "exit_time": None, "pnl_pct": None}
        entry = cp
        sl_val = trade["current_sl"]
        t1 = trade.get("t1")
        t2 = trade.get("t2")
        t3 = trade.get("t3")
        expiry_limit = target_date + timedelta(days=14)
        tf = trade.get("timeframe", "15minute") or "15minute"
        from_str = target_date.strftime("%Y-%m-%d")
        to_str = expiry_limit.strftime("%Y-%m-%d")
        for attempt in range(3):
            try:
                df = fetch_and_resample_candles(kite, token, from_str, to_str, tf)
                break
            except Exception as e:
                if "Too many requests" in str(e) and attempt < 2:
                    time.sleep(5)
                    continue
                raise
        if df.empty:
            return {"result": None, "detail": "no_data", "entry_time": None, "exit_time": None, "pnl_pct": None}
        entry_idx = None
        best_diff = float('inf')
        for i in range(len(df)):
            cclose = float(df.iloc[i]['close'])
            diff = abs(cclose - entry)
            if diff < best_diff:
                best_diff = diff
                entry_idx = i
        if entry_idx is None:
            return {"result": None, "detail": "entry_candle_not_found", "entry_time": None, "exit_time": None, "pnl_pct": None}
        if entry_idx >= len(df) - 1:
            return {"result": None, "detail": "no_subsequent_candles", "entry_time": None, "exit_time": None, "pnl_pct": None}
        entry_time = df.iloc[entry_idx]['date']
        for i in range(entry_idx + 1, len(df)):
            candle = df.iloc[i]
            low = float(candle['low'])
            high = float(candle['high'])
            if low <= sl_val:
                exit_time = candle['date']
                pnl = (sl_val - entry) / entry * 100
                return {"result": "SL_HIT", "detail": f"SL_HIT at {exit_time}", "entry_time": str(entry_time), "exit_time": str(exit_time), "pnl_pct": round(pnl, 2)}
            if t1 and high >= t1:
                exit_t = candle['date']
                if t3 and high >= t3:
                    pnl = (t3 - entry) / entry * 100
                    return {"result": "T3_HIT", "detail": f"T3_HIT at {exit_t}", "entry_time": str(entry_time), "exit_time": str(exit_t), "pnl_pct": round(pnl, 2)}
                if t2 and high >= t2:
                    pnl = (t2 - entry) / entry * 100
                    return {"result": "T2_HIT", "detail": f"T2_HIT at {exit_t}", "entry_time": str(entry_time), "exit_time": str(exit_t), "pnl_pct": round(pnl, 2)}
                pnl = (t1 - entry) / entry * 100
                return {"result": "T1_HIT", "detail": f"T1_HIT at {exit_t}", "entry_time": str(entry_time), "exit_time": str(exit_t), "pnl_pct": round(pnl, 2)}
        return {"result": "NO_EXIT", "detail": "No SL or target hit before expiry", "entry_time": str(entry_time), "exit_time": None, "pnl_pct": None}
    except Exception as e:
        logging.error(f"[SIM] Exception: {e}")
        return {"result": None, "detail": str(e), "entry_time": None, "exit_time": None, "pnl_pct": None}


def resolve_option_strikes(nfo_instruments, base_symbol, spot_price, step_size, option_type, n_range=0):
    """Return ATM strike plus n_range strikes ITM/OTM. nfo_instruments can be None for derived calls."""
    if nfo_instruments is None:
        return []
    if nfo_instruments is None or nfo_instruments.empty or 'name' not in nfo_instruments.columns:
        return []
    atm = int(round(spot_price / step_size) * step_size)
    out = []
    seen = set()
    for offset in range(-n_range, n_range + 1):
        strike = atm + offset * step_size
        if strike in seen:
            continue
        seen.add(strike)
        try:
            df = nfo_instruments[
                (nfo_instruments['name'] == base_symbol.strip().upper()) &
                (nfo_instruments['instrument_type'] == option_type.upper()) &
                (nfo_instruments['strike'] == float(strike))
            ].copy()
            if df.empty:
                continue
            df['expiry_dt'] = pd.to_datetime(df['expiry']).dt.date
            today = dt.now().date()
            future = df[df['expiry_dt'] >= today].sort_values(by='expiry_dt')
            if not future.empty:
                expiries = future['expiry_dt'].unique()
                curr_exp = expiries[0]
                days_rem = (curr_exp - today).days
                is_stock_contract = base_symbol.strip().upper() not in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]
                if is_stock_contract and days_rem <= 4 and len(expiries) > 1:
                    target_exp = expiries[1]
                    sub = future[future['expiry_dt'] == target_exp]
                    c = sub.iloc[0] if not sub.empty else future.iloc[0]
                else:
                    c = future.iloc[0]
            elif not df.empty:
                c = df.iloc[0]
            else:
                continue
            out.append({"strike": strike, "token": int(c['instrument_token']), "tradingsymbol": c['tradingsymbol']})
        except Exception as e:
            logging.error(f"Strike resolution error for {base_symbol} {option_type} @ {strike}: {e}")
            continue
    return out


# ──────────────────────────────────────────────
#  BEARISH REVERSAL PATTERNS & NEGATION TARGETS
# ──────────────────────────────────────────────

def check_left_side_rule_bearish(df, anchor_high, setup_count=0, skip_adjacent=0, lookback_candles=100):
    """Verify no candle in preceding lookback_candles has a CLOSE above anchor's high (tails/wicks permitted)."""
    if df is None or df.empty:
        return True
    end_idx = len(df) - (setup_count + skip_adjacent) if (setup_count + skip_adjacent) > 0 else len(df)
    start_idx = max(0, end_idx - lookback_candles)
    left = df.iloc[start_idx:end_idx] if end_idx > start_idx else pd.DataFrame()
    if not left.empty and anchor_high < float(left['close'].max()):
        return False
    return True

check_left_side_bearish = check_left_side_rule_bearish

def find_profit_targets_bearish(df_hist, entry_close, stop_loss=None):
    """
    Timeframe & Asset class adaptive profit target finder for BEARISH / Short setups.
    Scans historical 5-bar swing low support pivots below entry_close.
    Negation Theory Rule for Support Levels:
    A swing low support S is NEGATED if subsequent price closed below S prior to entry.
    Extracts non-negated support levels and sorts them descending (T1 is nearest support below entry).
    """
    if df_hist is None or len(df_hist) < 3:
        return None, None, None

    hist = df_hist.copy()

    time_col = None
    for col in ['datetime', 'date', 'timestamp', 'time', 'date_time']:
        if col in hist.columns:
            time_col = col
            break

    is_higher_tf = False
    if time_col is not None:
        try:
            hist[time_col] = pd.to_datetime(hist[time_col])
            hist = hist.sort_values(time_col).reset_index(drop=True)
            time_diffs = hist[time_col].diff().dropna()
            if not time_diffs.empty:
                median_diff = time_diffs.median()
                if median_diff >= pd.Timedelta(hours=20):
                    is_higher_tf = True
        except Exception:
            pass

    if time_col is not None:
        try:
            max_dt = hist[time_col].max()
            if is_higher_tf:
                min_dt = max_dt - pd.Timedelta(days=730)
            else:
                min_dt = max_dt - pd.Timedelta(days=30)
            sub_hist = hist[hist[time_col] >= min_dt]
            if len(sub_hist) >= 5:
                hist = sub_hist
        except Exception:
            pass

    high_low_diff = (hist['high'] - hist['low']).abs()
    atr = float(high_low_diff.tail(20).mean()) if len(hist) >= 5 else (entry_close * 0.02)
    if pd.isna(atr) or atr <= 0:
        atr = entry_close * 0.02

    risk = (stop_loss - entry_close) if (stop_loss and stop_loss > entry_close) else max(atr * 1.5, entry_close * 0.03)

    if entry_close < 300:
        max_target_cap = max(entry_close * 0.3, entry_close - 15 * atr)
        min_target_start = min(entry_close * 0.80, entry_close - 1.5 * risk)
        step_tol = 0.04
    elif is_higher_tf:
        max_target_cap = max(entry_close * 0.5, entry_close - 20 * atr)
        min_target_start = min(entry_close * 0.97, entry_close - 1.5 * risk)
        step_tol = 0.03
    else:
        max_target_cap = max(entry_close * 0.75, entry_close - 10 * atr)
        min_target_start = min(entry_close * 0.98, entry_close - 1.5 * risk)
        step_tol = 0.02

    non_negated_targets = []
    n = len(hist)
    for i in range(n - 2, 1, -1):
        w = hist.iloc[max(0, i-2):min(n, i+3)]
        if len(w) >= 3 and hist.iloc[i]['low'] == w['low'].min():
            l_val = float(hist.iloc[i]['low'])
            if max_target_cap <= l_val <= min_target_start:
                subsequent_bars = hist.iloc[i+1:]
                if not subsequent_bars.empty:
                    min_subsequent_close = float(subsequent_bars['close'].min())
                    if min_subsequent_close < l_val * 0.995:
                        continue  # Negated support target level -> Discarded
                non_negated_targets.append(l_val)

    if not non_negated_targets:
        for i in range(n - 1, 0, -1):
            l_val = float(hist.iloc[i]['low'])
            if max_target_cap <= l_val <= min_target_start:
                subsequent_bars = hist.iloc[i+1:]
                if not subsequent_bars.empty:
                    if float(subsequent_bars['close'].min()) < l_val * 0.995:
                        continue
                non_negated_targets.append(l_val)

    # Sort non-negated target levels descending by price for short trades (T1 > T2 > T3)
    sorted_levels = sorted(list(set(non_negated_targets)), reverse=True)

    clustered = []
    for p in sorted_levels:
        if not clustered or (clustered[-1] - p) / clustered[-1] > step_tol:
            clustered.append(round(p, 2))

    t1 = clustered[0] if len(clustered) >= 1 else None
    t2 = clustered[1] if len(clustered) >= 2 else None
    t3 = clustered[2] if len(clustered) >= 3 else None

    # Strict Negation Theory Rule: T1, T2, T3 are strictly based on non-negated chart swing pivots.
    # If a 2nd or 3rd non-negated swing level does not exist on the chart, keep T2/T3 as None (N/A).
    if t1 is None:
        t1 = round(entry_close - max(1.5 * risk, entry_close * 0.05), 2)

    if t2 is not None and t2 >= t1 * (1 - step_tol):
        t2 = round(t1 * (1 - step_tol * 2), 2)
    if t3 is not None and t2 is not None and t3 >= t2 * (1 - step_tol):
        t3 = round(t2 * (1 - step_tol * 2), 2)

    return t1, t2, t3


# ──────────────────────────────────────────────
#  BEARISH ANCHOR (A-FORMATION) DETECTORS — 5 PATTERNS
# ──────────────────────────────────────────────

def find_anchor_bearish_engulfing(df):
    """Setup 1 (Bearish): A = bearish engulfing candle. Bullish candle-1, then bearish candle wrapping body+wick."""
    if len(df) < 5:
        return None
    bullish_candle, bear_anchor = df.iloc[-4], df.iloc[-3]
    if not (float(bullish_candle['close']) > float(bullish_candle['open'])):
        return None
    if not (float(bear_anchor['close']) < float(bear_anchor['open'])):
        return None
    if not (float(bear_anchor['open']) >= float(bullish_candle['close']) and float(bear_anchor['close']) < float(bullish_candle['low'])):
        return None
    a_high = float(bear_anchor['high'])
    anchor_close = float(bear_anchor['close'])
    sl_val = calculate_sl_buffer(a_high, side="BEAR")
    return {"Pattern": "BEAR_A_ABCD_Engulf", "Close": anchor_close, "SL": sl_val, "Signal": "Bear_A_Formation", "CandleATime": str(bear_anchor.get('date', ''))}

def find_anchor_hh_sweep(df):
    """
    Setup 2 (Bearish): A = High 2 (sweep above prior swing high High 1).
    Rules:
      1. Need > 2 candles (at least 3 candles gap) between High 1 and High 2.
      2. In-between candles must NOT close above High 1 (wicks allowed).
      3. High 2 sweeps above High 1.
    """
    if len(df) < 30:
        return None

    search_range = df.iloc[-29:-7]
    if search_range.empty:
        return None

    high_1_idx = search_range['high'].idxmax()
    high_1 = float(df.loc[high_1_idx, 'high'])

    sweep_idx = df.index[-4]

    pos_high_1 = df.index.get_loc(high_1_idx)
    pos_sweep = df.index.get_loc(sweep_idx)
    if (pos_sweep - pos_high_1 - 1) < 3:
        return None

    inbetween_df = df.iloc[pos_high_1 + 1 : pos_sweep]
    if not inbetween_df.empty:
        if (inbetween_df['close'] > high_1).any():
            return None

    sweep_candle, rejection_candle, confirm_candle_1, confirm_candle_2 = df.iloc[-4], df.iloc[-3], df.iloc[-2], df.iloc[-1]
    sweep_high = float(sweep_candle['high'])
    is_green = float(sweep_candle['close']) > float(sweep_candle['open'])
    is_red = float(sweep_candle['close']) <= float(sweep_candle['open'])

    # Var 1: Green sweep candle (sweeps/closes above High 1, rejected back down)
    v1 = is_green and (sweep_high > high_1) and (float(sweep_candle['close']) < high_1)
    v2 = is_green and (float(sweep_candle['close']) > high_1) and (float(rejection_candle['close']) < high_1)

    # Var 2: Red/Neutral wick sweep candle (upper wick pierces High 1, body closes red below High 1)
    v3 = is_red and (sweep_high > high_1) and (float(sweep_candle['close']) < high_1)

    if not (v1 or v2 or v3):
        return None
    if not (float(rejection_candle['close']) < float(sweep_candle['low'])):
        return None
    if float(confirm_candle_1['close']) > sweep_high or float(confirm_candle_2['close']) > sweep_high:
        return None

    pattern_name = "BEAR_A_HH_Sweep_Var1" if (v1 or v2) else "BEAR_A_HH_Sweep_Var2"

    anchor_close = float(rejection_candle['close'])
    sl_val = calculate_sl_buffer(sweep_high, side="BEAR")
    return {"Pattern": pattern_name, "Close": anchor_close, "SL": sl_val, "Signal": "High2_Formation", "CandleATime": str(sweep_candle.get('date', ''))}

def find_anchor_two_lower_lows(df):
    """Setup 3 (Bearish): A1 & A2 are two successive lower low bearish candles."""
    if len(df) < 5:
        return None
    a1, a2 = df.iloc[-4], df.iloc[-3]
    if not (float(a1['close']) < float(a1['open']) and float(a2['close']) < float(a2['open'])):
        return None
    if not (float(a2['low']) < float(a1['low']) and float(a2['high']) < float(a1['high'])):
        return None
    a_high = max(float(a1['high']), float(a2['high']))
    anchor_close = float(a2['close'])
    sl_val = calculate_sl_buffer(a_high, side="BEAR")
    return {"Pattern": "BEAR_A_Two_Lower_Lows", "Close": anchor_close, "SL": sl_val, "Signal": "LowerLow_Engulf", "CandleATime": str(a2.get('date', ''))}

def find_anchor_shooting_star_baby(df):
    """Setup 4 (Bearish): A = shooting star / baby candle inside bullish mother body, with long upper wick."""
    if len(df) < 5:
        return None
    mother_candle, baby_candle, post_baby_1, post_baby_2, post_baby_3 = df.iloc[-5], df.iloc[-4], df.iloc[-3], df.iloc[-2], df.iloc[-1]
    is_red = float(baby_candle['close']) <= float(baby_candle['open'])
    body = abs(float(baby_candle['close']) - float(baby_candle['open']))
    upper_wick = float(baby_candle['high']) - float(max(float(baby_candle['open']), float(baby_candle['close'])))
    lower_wick = float(min(float(baby_candle['open']), float(baby_candle['close']))) - float(baby_candle['low'])

    min_wick_ratio = 1.2 if is_red else 1.8
    if upper_wick < (body * min_wick_ratio):
        return None
    if upper_wick <= lower_wick:
        return None

    if float(post_baby_2['close']) > float(baby_candle['high']) or float(post_baby_3['close']) > float(baby_candle['high']):
        return None
    anchor_close = float(baby_candle['close'])
    b_high = float(baby_candle['high'])
    sl_val = calculate_sl_buffer(b_high, side="BEAR")
    return {"Pattern": "BEAR_A_ShootingStar_Baby", "Close": anchor_close, "SL": sl_val, "Signal": "ShootingStar_Formation", "CandleATime": str(baby_candle.get('date', ''))}

def find_anchor_bearish_harami(df):
    """Setup 5 (Bearish): A = bearish inside bar fully inside bullish mother body."""
    if len(df) < 5:
        return None
    bullish_mother, bearish_inside, post_harami_1, post_harami_2, post_harami_3 = df.iloc[-5], df.iloc[-4], df.iloc[-3], df.iloc[-2], df.iloc[-1]
    if not (float(bullish_mother['close']) > float(bullish_mother['open']) and float(bearish_inside['close']) < float(bearish_inside['open'])):
        return None
    if not (float(bearish_inside['high']) <= float(bullish_mother['close']) and float(bearish_inside['low']) >= float(bullish_mother['open'])):
        return None
    inside_high = float(bearish_inside['high'])
    if float(post_harami_2['close']) > inside_high or float(post_harami_3['close']) > inside_high:
        return None
    anchor_close = float(bearish_inside['close'])
    sl_val = calculate_sl_buffer(inside_high, side="BEAR")
    return {"Pattern": "BEAR_A_Harami", "Close": anchor_close, "SL": sl_val, "Signal": "Bear_Harami_Formation", "CandleATime": str(bearish_inside.get('date', ''))}


# ──────────────────────────────────────────────
#  BEARISH BREAKOUT SCANNER (A -> B -> C -> D)
# ──────────────────────────────────────────────

def scan_anchor_bcd_breakout_bearish(df_entry, df_anchor):
    """
    Two-phase A-first Bearish scanner:
      Phase 1: Find anchor candle A (using 5 bearish detectors + base fallback).
      Phase 2: From A, scan forward sequentially: B (breakout < A.low) ->
               C (green retest) -> D (confirmation close < A.low).
    """
    if df_entry is None or df_entry.empty or df_anchor is None or df_anchor.empty:
        return None

    if len(df_anchor) < 10 or len(df_entry) < 10:
        return None

    detectors = [
        find_anchor_bearish_engulfing,
        find_anchor_hh_sweep,
        find_anchor_two_lower_lows,
        find_anchor_shooting_star_baby,
        find_anchor_bearish_harami
    ]

    best_match = None
    min_anchor_search_len = min(60, len(df_anchor))

    for anchor_idx in range(len(df_anchor) - 3, len(df_anchor) - min_anchor_search_len, -1):
        sub_anchor_df = df_anchor.iloc[:anchor_idx + 1]
        anchor_candle = sub_anchor_df.iloc[-1]
        
        det_result = None
        for det in detectors:
            res = det(sub_anchor_df)
            if res:
                det_result = res
                break
        
        is_bear_candle = float(anchor_candle['close']) < float(anchor_candle['open'])
        if not det_result and not is_bear_candle:
            continue

        a_high = float(anchor_candle['high'])
        a_low = float(anchor_candle['low'])
        a_close = float(anchor_candle['close'])
        a_date = det_result.get("CandleATime") if det_result and det_result.get("CandleATime") else anchor_candle.get('date', '')

        anchor_entry_matches = df_entry[df_entry['date'] == a_date] if 'date' in df_entry.columns else pd.DataFrame()
        if anchor_entry_matches.empty:
            continue

        e_anchor_idx = anchor_entry_matches.index[0]

        if not check_left_side_rule_bearish(df_entry, a_high, setup_count=0, skip_adjacent=(len(df_entry) - 1 - e_anchor_idx)):
            continue

        b_idx = None
        for i in range(e_anchor_idx + 1, min(e_anchor_idx + 30, len(df_entry))):
            candle = df_entry.iloc[i]
            if float(candle['close']) > a_high:
                break
            if float(candle['close']) < a_low:
                b_idx = i
                break

        if b_idx is None:
            continue

        c_idx = None
        for i in range(b_idx + 1, min(b_idx + 30, len(df_entry))):
            candle = df_entry.iloc[i]
            if float(candle['close']) > a_high:
                break
            if float(candle['high']) >= a_low and float(candle['close']) < a_high:
                c_idx = i
                break

        if c_idx is None:
            continue

        d_idx = None
        for i in range(c_idx + 1, min(c_idx + 30, len(df_entry))):
            candle = df_entry.iloc[i]
            if float(candle['close']) > a_high:
                break
            if float(candle['close']) < a_low and float(candle['close']) < float(candle['open']):
                d_idx = i
                break

        if d_idx is None:
            continue

        sl_val = det_result.get("SL") if (det_result and det_result.get("SL")) else round(a_high + max(0.50, a_high * 0.02), 2)

        intermediate_bars = df_entry.iloc[e_anchor_idx:d_idx + 1]
        if float(intermediate_bars['close'].max()) > sl_val:
            continue

        pattern_type = det_result["Pattern"] if det_result else "BEAR_A_BCD_Breakout"
        entry_candle = df_entry.iloc[d_idx]
        entry_close = float(entry_candle['close'])


        t1, t2, t3 = find_profit_targets_bearish(df_anchor, entry_close, stop_loss=sl_val)
        if not t1 or t1 >= entry_close:
            t1 = round(entry_close - max(1.5 * abs(sl_val - entry_close), entry_close * 0.05), 2)

        stage_status = "FRESH_ENTRY"
        priority_level = "HIGH_PRIORITY"

        # Post-D 3-Tier Classification Filter
        after_d = df_entry.iloc[d_idx + 1 :]
        if not after_d.empty:
            # 1. Discard if SL hit after D (A.high + buffer)
            if float(after_d['close'].max()) >= sl_val:
                continue
            # 2. Check if T1 has been reached after D
            if t1 is not None and float(after_d['close'].min()) <= t1:
                # If T3 reached or T2 reached or no T2/T3 available -> All targets completed
                if (t3 is not None and float(after_d['close'].min()) <= t3) or t2 is None or float(after_d['close'].min()) <= t2:
                    continue
                # T1 was hit, but T2/T3 is still pending -> Qualifies as LOW PRIORITY T2 Continuation
                stage_status = "T2_CONTINUATION"
                priority_level = "LOW_PRIORITY"
                sl_val = t1  # Trailed SL to T1 level to protect banked gains

        risk = sl_val - entry_close
        if risk <= 0:
            continue

        rr = (entry_close - t1) / risk
        if rr < 1.88:
            continue

        setup_data = {
            "Pattern": pattern_type,
            "Close": entry_close,
            "SL": sl_val,
            "T1": t1,
            "T2": t2,
            "T3": t3,
            "RR": round(rr, 2),
            "A_Date": str(a_date),
            "D_Date": str(entry_candle.get('date', '')),
            "Stage_Status": stage_status,
            "Priority": priority_level,
            "d_idx": d_idx
        }

        if best_match is None or setup_data["d_idx"] > best_match.get("d_idx", -1) or \
           (setup_data["d_idx"] == best_match.get("d_idx", -1) and setup_data["Priority"] == "HIGH_PRIORITY" and setup_data["RR"] > best_match.get("RR", 0)):
            best_match = setup_data

    if best_match:
        best_match.pop("d_idx", None)
    return best_match


def scan_trend_continuation_reentry(df_entry, df_anchor):
    """
    Setup Page 16 (Bullish Trend Continuation + Re-Entry):
    1. Context: Established Uptrend (Higher Highs & Higher Lows in preceding window).
    2. Retest: Price pulls back to prior swing support level.
    3. Trigger: Bullish Engulfing or Reclaim candle forms at support.
    4. Execution: Immediate Re-entry on the next candle close (No BCD delay).
    """
    if len(df_entry) < 20:
        return None

    lookback = df_entry.iloc[-25:-2]
    if lookback.empty or len(lookback) < 10:
        return None

    mid_point = len(lookback) // 2
    part1 = lookback.iloc[:mid_point]
    part2 = lookback.iloc[mid_point:]

    if not (part2['high'].max() > part1['high'].max() and part2['low'].min() > part1['low'].min()):
        return None

    trigger_candle = df_entry.iloc[-2]
    current_candle = df_entry.iloc[-1]

    is_green_trigger = float(trigger_candle['close']) > float(trigger_candle['open'])
    if not is_green_trigger:
        return None

    support_level = float(part2['low'].min())
    trigger_low = float(trigger_candle['low'])
    trigger_close = float(trigger_candle['close'])

    if not (trigger_low <= (support_level * 1.015) and trigger_close >= support_level):
        return None

    entry_price = float(current_candle['close'])
    sl_val = round(trigger_low - max(0.50, trigger_low * 0.02), 2)

    if entry_price <= sl_val:
        return None

    t1, t2, t3 = find_profit_targets(df_anchor, entry_price, stop_loss=sl_val)
    if t1 is None or t1 <= entry_price:
        return None

    risk = entry_price - sl_val
    if risk <= 0 or risk < entry_price * 0.002 or ((t1 - entry_price) / risk) < 1.88:
        return None

    rr = (t1 - entry_price) / risk
    return {
        "Pattern": "TREND_CONT_BULL",
        "SL": sl_val,
        "T1": t1,
        "T2": t2,
        "T3": t3,
        "Entry": entry_price,
        "RR": round(rr, 2),
        "Signal": "Immediate_ReEntry",
        "D_time": str(current_candle.get("date", "")),
        "A_time": str(trigger_candle.get("date", ""))
    }

def scan_trend_continuation_reentry_bearish(df_entry, df_anchor):
    """
    Setup Page 17 (Bearish Trend Continuation + Re-Entry):
    1. Context: Established Downtrend (Lower Highs & Lower Lows in preceding window).
    2. Retest: Price pulls back to prior swing resistance level.
    3. Trigger: Bearish Engulfing or Rejection candle forms at resistance.
    4. Execution: Immediate Re-entry / Short on the next candle close (No BCD delay).
    """
    if len(df_entry) < 20:
        return None

    lookback = df_entry.iloc[-25:-2]
    if lookback.empty or len(lookback) < 10:
        return None

    mid_point = len(lookback) // 2
    part1 = lookback.iloc[:mid_point]
    part2 = lookback.iloc[mid_point:]

    if not (part2['high'].max() < part1['high'].max() and part2['low'].min() < part1['low'].min()):
        return None

    trigger_candle = df_entry.iloc[-2]
    current_candle = df_entry.iloc[-1]

    is_red_trigger = float(trigger_candle['close']) < float(trigger_candle['open'])
    if not is_red_trigger:
        return None

    resistance_level = float(part2['high'].max())
    trigger_high = float(trigger_candle['high'])
    trigger_close = float(trigger_candle['close'])

    if not (trigger_high >= (resistance_level * 0.985) and trigger_close <= resistance_level):
        return None

    entry_price = float(current_candle['close'])
    sl_val = round(trigger_high + max(0.50, trigger_high * 0.02), 2)

    if entry_price >= sl_val:
        return None

    t1, t2, t3 = find_profit_targets_bearish(df_anchor, entry_price, stop_loss=sl_val)
    if t1 is None or t1 >= entry_price:
        return None

    risk = sl_val - entry_price
    if risk <= 0 or risk < entry_price * 0.002 or ((entry_price - t1) / risk) < 1.88:
        return None

    rr = (entry_price - t1) / risk
    return {
        "Pattern": "TREND_CONT_BEAR",
        "SL": sl_val,
        "T1": t1,
        "T2": t2,
        "T3": t3,
        "Entry": entry_price,
        "RR": round(rr, 2),
        "Signal": "Immediate_ReEntry_Bear",
        "D_time": str(current_candle.get("date", "")),
        "A_time": str(trigger_candle.get("date", ""))
    }

def scan_anchor_bcd_breakout_generic(df_entry, df_anchor, side="BULL"):
    """
    Unified A-first breakout scanner supporting both BULLISH and BEARISH reversals,
    plus fast Trend Continuation Re-entries (Pages 16 & 17).
    """
    if str(side).upper() == "BEAR":
        res = scan_anchor_bcd_breakout_bearish(df_entry, df_anchor)
        if not res:
            res = scan_trend_continuation_reentry_bearish(df_entry, df_anchor)
        return res
    else:
        res = scan_anchor_bcd_breakout(df_entry, df_anchor)
        if not res:
            res = scan_trend_continuation_reentry(df_entry, df_anchor)
        return res



