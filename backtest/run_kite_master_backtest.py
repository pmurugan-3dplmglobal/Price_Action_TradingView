import os
import sys
import json
import time
import pandas as pd
from datetime import datetime as dt, timedelta

# Add common directory to sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
COMMON_DIR = os.path.join(PROJECT_ROOT, "common")
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

from kiteconnect import KiteConnect
from trading_core import (
    load_kite_session,
    find_anchor_bullish_engulfing,
    find_anchor_ll_sweep,
    find_anchor_hammer_baby,
    find_anchor_bullish_harami,
    find_anchor_two_higher_highs,
    find_profit_targets,
    find_profit_targets_bearish,
    INDEX_REGISTRY,
    STOCK_REGISTRY,
    sync_stock_tokens
)

def fetch_kite_candles(kite, instrument_token, interval, from_date, to_date):
    try:
        records = kite.historical_data(instrument_token, from_date, to_date, interval)
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        return pd.DataFrame()

def scan_all_spot_patterns(df_spot, side="BULL"):
    if df_spot.empty or len(df_spot) < 15:
        return []

    found_setups = []
    seen_d_indices = set()

    for a_idx in range(4, len(df_spot) - 3):
        a = df_spot.iloc[a_idx]
        benchmark = float(a['high']) if side == "BULL" else float(a['low'])
        invalidation = round(float(a['low']) - max(0.50, float(a['low']) * 0.002), 2) if side == "BULL" else round(float(a['high']) + max(0.50, float(a['high']) * 0.002), 2)

        remaining = df_spot.iloc[a_idx + 1:]
        if len(remaining) < 3:
            continue

        b_idx = None
        for j in range(len(remaining)):
            c_val = float(remaining.iloc[j]['close'])
            if (side == "BULL" and c_val > benchmark) or (side == "BEAR" and c_val < benchmark):
                b_idx = a_idx + 1 + j
                break
        if b_idx is None:
            continue

        c_slice = df_spot.iloc[b_idx + 1:]
        c_idx = None
        for j in range(len(c_slice)):
            c_row = c_slice.iloc[j]
            c_low = float(c_row['low'])
            c_high = float(c_row['high'])
            c_close = float(c_row['close'])
            c_open = float(c_row['open'])
            
            if side == "BULL":
                is_red = c_close < c_open
                if (c_low <= benchmark and c_close > invalidation and is_red) or \
                   (c_low <= invalidation and c_close > invalidation and c_close < float(a['open']) and is_red):
                    c_idx = b_idx + 1 + j
                    break
            else:
                is_green = c_close > c_open
                if (c_high >= benchmark and c_close < invalidation and is_green) or \
                   (c_high >= invalidation and c_close < invalidation and c_close > float(a['open']) and is_green):
                    c_idx = b_idx + 1 + j
                    break
        if c_idx is None:
            continue

        d_slice = df_spot.iloc[c_idx + 1:]
        d_idx = None
        for j in range(len(d_slice)):
            d_row = d_slice.iloc[j]
            d_close = float(d_row['close'])
            d_open = float(d_row['open'])
            if side == "BULL" and d_close > benchmark and d_close > d_open:
                d_idx = c_idx + 1 + j
                break
            elif side == "BEAR" and d_close < benchmark and d_close < d_open:
                d_idx = c_idx + 1 + j
                break
        if d_idx is None:
            continue

        if d_idx in seen_d_indices:
            continue

        d = df_spot.iloc[d_idx]
        d_dt = d['date']
        a_dt = a['date']

        between = df_spot.iloc[a_idx + 1 : d_idx]
        if side == "BULL" and not between.empty and float(between['close'].min()) < invalidation:
            continue
        if side == "BEAR" and not between.empty and float(between['close'].max()) > invalidation:
            continue

        close_price = float(d['close'])
        sl_val = invalidation

        if side == "BULL":
            t1, t2, t3 = find_profit_targets(df_spot, close_price, stop_loss=sl_val)
            if t1 is None or close_price >= t1: continue
            risk = close_price - sl_val
            if risk <= 0 or ((t1 - close_price) / risk) < 1.88: continue
            rr = (t1 - close_price) / risk
        else:
            t1, t2, t3 = find_profit_targets_bearish(df_spot, close_price, stop_loss=sl_val)
            if t1 is None or close_price <= t1: continue
            risk = sl_val - close_price
            if risk <= 0 or ((close_price - t1) / risk) < 1.88: continue
            rr = (close_price - t1) / risk

        seen_d_indices.add(d_idx)

        # Simulate outcome
        subs = df_spot.iloc[d_idx + 1:]
        outcome = "ACTIVE / OPEN"
        pnl_pct = round((float(df_spot.iloc[-1]['close']) - close_price) / close_price * 100, 2) if side == "BULL" else round((close_price - float(df_spot.iloc[-1]['close'])) / close_price * 100, 2)

        for _, row in subs.iterrows():
            l = float(row['low'])
            h = float(row['high'])
            c = float(row['close'])
            if side == "BULL":
                if c <= sl_val or l <= sl_val:
                    outcome = "SL HIT (LOSS)"
                    pnl_pct = round((sl_val - close_price) / close_price * 100, 2)
                    break
                if t3 and h >= t3:
                    outcome = "T3 HIT (WIN)"
                    pnl_pct = round((t3 - close_price) / close_price * 100, 2)
                    break
                if t2 and h >= t2:
                    outcome = "T2 HIT (WIN)"
                    pnl_pct = round((t2 - close_price) / close_price * 100, 2)
                    break
                if h >= t1:
                    outcome = "T1 HIT (WIN)"
                    pnl_pct = round((t1 - close_price) / close_price * 100, 2)
                    break
            else:
                if c >= sl_val or h >= sl_val:
                    outcome = "SL HIT (LOSS)"
                    pnl_pct = round((close_price - sl_val) / close_price * 100, 2)
                    break
                if t3 and l <= t3:
                    outcome = "T3 HIT (WIN)"
                    pnl_pct = round((close_price - t3) / close_price * 100, 2)
                    break
                if t2 and l <= t2:
                    outcome = "T2 HIT (WIN)"
                    pnl_pct = round((close_price - t2) / close_price * 100, 2)
                    break
                if l <= t1:
                    outcome = "T1 HIT (WIN)"
                    pnl_pct = round((close_price - t1) / close_price * 100, 2)
                    break

        found_setups.append({
            "Pattern": "BE_ABCD" if side == "BULL" else "BEAR_ABCD",
            "BM_Time": a_dt.strftime('%Y-%m-%d %H:%M:%S'),
            "D_Time": d_dt.strftime('%Y-%m-%d %H:%M:%S'),
            "Entry": close_price,
            "SL": sl_val,
            "T1": t1,
            "T2": t2 or 0.0,
            "T3": t3 or 0.0,
            "RR": round(rr, 2),
            "Outcome": outcome,
            "P&L %": pnl_pct
        })

    return found_setups

def run_kite_master_backtest(from_date="2026-07-20", to_date="2026-07-25"):
    t0 = time.time()
    
    # 1. Initialize Kite Session
    token_file = os.path.join(PROJECT_ROOT, "Trade_Option", "input", "kite_access_token.txt")
    if not os.path.exists(token_file):
        token_file = os.path.join(PROJECT_ROOT, "Trade_Stock", "input", "kite_access_token.txt")
    api_key, access_token = load_kite_session(token_file=token_file)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    user_name = kite.profile()['user_shortname']
    print(f"--> Authenticated Zerodha Kite Session: {user_name}", flush=True)

    try:
        sync_stock_tokens(kite)
    except Exception:
        pass

    # 2. INDEX OPTIONS ENGINE (15-MIN TF)
    print("--> Scanning Index Options Engine (15m TF)...", flush=True)
    index_results = []
    for sym, meta in INDEX_REGISTRY.items():
        token = meta["token"]
        df_index = fetch_kite_candles(kite, token, "15minute", from_date, to_date)
        if not df_index.empty:
            for side, opt_side in [("BULL", "CE"), ("BEAR", "PE")]:
                setups = scan_all_spot_patterns(df_index, side=side)
                for s in setups:
                    s["Category"] = "INDEX_OPTION"
                    s["Asset"] = f"{sym} {opt_side}"
                    s["TF"] = "15m"
                    s["Side"] = opt_side
                    index_results.append(s)

    index_results.sort(key=lambda x: str(x.get("D_Time", "")))

    # 3. NIFTY 50 STOCK OPTIONS ENGINE (30-MIN TF)
    print("--> Scanning Nifty 50 Stock Options Engine (30m TF)...", flush=True)
    stock_results = []
    stock_count = 0
    for sym, meta in STOCK_REGISTRY.items():
        token = meta.get("token")
        if not token:
            continue
        stock_count += 1
        df_stock = fetch_kite_candles(kite, token, "30minute", from_date, to_date)
        if not df_stock.empty:
            for side, opt_side in [("BULL", "CE"), ("BEAR", "PE")]:
                setups = scan_all_spot_patterns(df_stock, side=side)
                for s in setups:
                    s["Category"] = "STOCK_OPTION"
                    s["Asset"] = f"{sym} {opt_side}"
                    s["TF"] = "30m"
                    s["Side"] = opt_side
                    stock_results.append(s)

    stock_results.sort(key=lambda x: str(x.get("D_Time", "")))

    # SAVE RESULTS TO JSON
    idx_wins = sum(1 for r in index_results if "WIN" in r["Outcome"])
    idx_losses = sum(1 for r in index_results if "LOSS" in r["Outcome"] or "SL HIT" in r["Outcome"])
    idx_total = len(index_results)
    idx_win_rate = (idx_wins / (idx_wins + idx_losses) * 100) if (idx_wins + idx_losses) > 0 else 0.0

    stk_wins = sum(1 for r in stock_results if "WIN" in r["Outcome"])
    stk_losses = sum(1 for r in stock_results if "LOSS" in r["Outcome"] or "SL HIT" in r["Outcome"])
    stk_total = len(stock_results)
    stk_win_rate = (stk_wins / (stk_wins + stk_losses) * 100) if (stk_wins + stk_losses) > 0 else 0.0

    summary_data = {
        "from_date": from_date,
        "to_date": to_date,
        "user": user_name,
        "execution_time_seconds": round(time.time() - t0, 2),
        "index_options_15m": {
            "total_trades": idx_total,
            "wins": idx_wins,
            "losses": idx_losses,
            "active": idx_total - (idx_wins + idx_losses),
            "win_rate": round(idx_win_rate, 2),
            "trades": index_results
        },
        "stock_options_30m": {
            "stocks_scanned": stock_count,
            "total_trades": stk_total,
            "wins": stk_wins,
            "losses": stk_losses,
            "active": stk_total - (stk_wins + stk_losses),
            "win_rate": round(stk_win_rate, 2),
            "trades": stock_results
        }
    }

    export_file = os.path.join(BASE_DIR, "master_backtest_results.json")
    with open(export_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)

    print(f"\nKITE_BACKTEST_FINISHED_SUCCESSFULLY in {time.time() - t0:.2f}s!", flush=True)

if __name__ == "__main__":
    run_kite_master_backtest(from_date="2026-07-20", to_date="2026-07-25")
