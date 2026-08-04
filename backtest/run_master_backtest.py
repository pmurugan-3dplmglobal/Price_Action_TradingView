import os
import sys
import json
import time
import requests
import pandas as pd

# Add common directory to sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
COMMON_DIR = os.path.join(PROJECT_ROOT, "common")
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

from trading_core import (
    find_anchor_bullish_engulfing,
    find_anchor_ll_sweep,
    find_anchor_hammer_baby,
    find_anchor_bullish_harami,
    find_anchor_two_higher_highs,
    find_profit_targets,
    STOCK_REGISTRY
)

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
session = requests.Session()
session.headers.update(HTTP_HEADERS)

def fetch_yahoo_candles(symbol, interval="30m", range_str="1mo"):
    symbol_map = {
        "NIFTY": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
        "SENSEX": "^BSESN"
    }
    ticker_symbol = symbol_map.get(symbol, symbol)
    if not ticker_symbol.startswith("^") and not ticker_symbol.endswith(".NS"):
        ticker_symbol = symbol + ".NS"

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_symbol}?range={range_str}&interval={interval}"
    try:
        r = session.get(url, timeout=3)
        if r.status_code != 200:
            return symbol, pd.DataFrame()
        data = r.json()
        result = data['chart']['result'][0]
        timestamps = result['timestamp']
        quote = result['indicators']['quote'][0]
        
        df = pd.DataFrame({
            'date': [pd.to_datetime(ts, unit='s').tz_localize('UTC').tz_convert('Asia/Kolkata') for ts in timestamps],
            'open': quote['open'],
            'high': quote['high'],
            'low': quote['low'],
            'close': quote['close'],
            'volume': quote['volume']
        }).dropna()
        return symbol, df
    except Exception:
        return symbol, pd.DataFrame()

def simulate_option_premium_series(df_spot, option_type="CE"):
    if df_spot.empty:
        return pd.DataFrame()

    df_opt = df_spot.copy()
    first_close = df_spot.iloc[0]['close']
    base_premium = round(max(5.0, first_close * 0.015), 2)
    delta = 0.50

    closes, highs, lows = [], [], []
    curr_premium = base_premium

    for _, row in df_spot.iterrows():
        s_open = row['open']
        s_high = row['high']
        s_low = row['low']
        s_close = row['close']

        if option_type == "CE":
            change_close = (s_close - s_open) * delta
            change_high = (s_high - s_open) * delta
            change_low = (s_low - s_open) * delta
        else: # PE
            change_close = (s_open - s_close) * delta
            change_high = (s_open - s_low) * delta
            change_low = (s_open - s_high) * delta

        opt_open = max(2.0, curr_premium)
        opt_close = max(2.0, opt_open + change_close)
        opt_high = max(opt_open, opt_close, opt_open + change_high)
        opt_low = max(1.0, min(opt_open, opt_close, opt_open + change_low))

        closes.append(round(opt_close, 2))
        highs.append(round(opt_high, 2))
        lows.append(round(opt_low, 2))
        curr_premium = opt_close

    df_opt['open'] = [round(c, 2) for c in df_spot['open']]
    df_opt['high'] = highs
    df_opt['low'] = lows
    df_opt['close'] = closes
    return df_opt

def scan_all_patterns_in_dataset(df_opt, start_dt, end_dt):
    if df_opt.empty or len(df_opt) < 15:
        return []

    anchor_funcs = [
        find_anchor_bullish_engulfing,
        find_anchor_ll_sweep,
        find_anchor_hammer_baby,
        find_anchor_bullish_harami,
        find_anchor_two_higher_highs
    ]

    short_names = {
        "BULL_A_ABCD_Engulf": "BE_ABCD",
        "BULL_A_LL_Sweep": "LL_ABCD",
        "BULL_A_Baby_Candle": "HAMMER_ABCD",
        "BULL_A_Harami": "HARAMI_ABCD",
        "BULL_A_Two_Higher_Highs": "HH_ABCD",
        "BULL_A_Base": "BASE_ABCD"
    }

    found_setups = []
    seen_d_indices = set()

    # Filter candidate A range to relevant window
    window_df = df_opt[(df_opt['date'] >= start_dt - pd.Timedelta(days=5)) & (df_opt['date'] <= end_dt)]
    if window_df.empty:
        return []

    start_idx = max(4, window_df.index[0])
    end_idx = min(len(df_opt) - 3, window_df.index[-1])

    for a_idx in range(start_idx, end_idx + 1):
        a = df_opt.iloc[a_idx]
        sub_df = df_opt.iloc[: min(len(df_opt), a_idx + 3)]
        sub_df_direct = df_opt.iloc[: a_idx + 1]

        anchor_match = None
        for fn in anchor_funcs:
            res = fn(sub_df) or fn(sub_df_direct)
            if res:
                anchor_match = res
                break

        benchmark = float(a['high'])
        invalidation = anchor_match["SL"] if anchor_match else round(float(a['low']) - max(0.50, float(a['low']) * 0.02), 2)
        anchor_name = anchor_match["Pattern"] if anchor_match else "BULL_A_Base"

        remaining = df_opt.iloc[a_idx + 1:]
        if len(remaining) < 3:
            continue

        b_idx = None
        for j in range(len(remaining)):
            if float(remaining.iloc[j]['close']) > benchmark:
                b_idx = a_idx + 1 + j
                break
        if b_idx is None:
            continue

        c_slice = df_opt.iloc[b_idx + 1:]
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

        d_slice = df_opt.iloc[c_idx + 1:]
        d_idx = None
        for j in range(len(d_slice)):
            d_row = d_slice.iloc[j]
            if float(d_row['close']) > benchmark and float(d_row['close']) > float(d_row['open']):
                d_idx = c_idx + 1 + j
                break
        if d_idx is None:
            continue

        if d_idx in seen_d_indices:
            continue

        d = df_opt.iloc[d_idx]
        d_dt = d['date']
        a_dt = a['date']

        if d_dt < start_dt or d_dt > end_dt:
            continue

        between = df_opt.iloc[a_idx + 1 : d_idx]
        if not between.empty and float(between['close'].min()) < invalidation:
            continue

        close_price = float(d['close'])
        sl_val = invalidation
        t1, t2, t3 = find_profit_targets(df_opt, close_price, stop_loss=sl_val)
        if t1 is None or close_price >= t1:
            continue

        risk = close_price - sl_val
        if risk <= 0 or risk < close_price * 0.002 or ((t1 - close_price) / risk) < 1.88:
            continue

        rr = (t1 - close_price) / risk if risk > 0 else 0
        seen_d_indices.add(d_idx)

        # Simulate outcome
        subs = df_opt.iloc[d_idx + 1:]
        outcome = "ACTIVE / OPEN"
        pnl_pct = round((float(df_opt.iloc[-1]['close']) - close_price) / close_price * 100, 2) if not subs.empty else 0.0

        for _, row in subs.iterrows():
            l = float(row['low'])
            h = float(row['high'])
            c = float(row['close'])
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

        found_setups.append({
            "Pattern": short_names.get(anchor_name, "BE_ABCD"),
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

def run_master_backtest(start_date="2026-07-20", end_date="2026-07-25"):
    t0 = time.time()
    start_dt = pd.to_datetime(start_date).tz_localize('Asia/Kolkata')
    end_dt = pd.to_datetime(end_date + " 23:59:59").tz_localize('Asia/Kolkata')
    
    # 1. INDEX OPTIONS ENGINE (15-MIN TF)
    index_symbols = ["NIFTY", "BANKNIFTY", "SENSEX"]
    index_results = []
    for sym in index_symbols:
        _, df = fetch_yahoo_candles(sym, "15m", "1mo")
        if not df.empty:
            for opt_type in ["CE", "PE"]:
                df_opt = simulate_option_premium_series(df, option_type=opt_type)
                setups = scan_all_patterns_in_dataset(df_opt, start_dt, end_dt)
                for s in setups:
                    s["Category"] = "INDEX_OPTION"
                    s["Asset"] = f"{sym} {opt_type}"
                    s["TF"] = "15m"
                    s["Side"] = opt_type
                    index_results.append(s)

    index_results.sort(key=lambda x: str(x.get("D_Time", "")))

    # 2. NIFTY 50 STOCK OPTIONS ENGINE (30-MIN TF)
    stock_symbols = sorted(STOCK_REGISTRY.keys())
    stock_results = []
    for sym in stock_symbols:
        _, df = fetch_yahoo_candles(sym, "30m", "1mo")
        if not df.empty:
            for opt_type in ["CE", "PE"]:
                df_opt = simulate_option_premium_series(df, option_type=opt_type)
                setups = scan_all_patterns_in_dataset(df_opt, start_dt, end_dt)
                for s in setups:
                    s["Category"] = "STOCK_OPTION"
                    s["Asset"] = f"{sym} {opt_type}"
                    s["TF"] = "30m"
                    s["Side"] = opt_type
                    stock_results.append(s)

    stock_results.sort(key=lambda x: str(x.get("D_Time", "")))

    idx_wins = sum(1 for r in index_results if "WIN" in r["Outcome"])
    idx_losses = sum(1 for r in index_results if "LOSS" in r["Outcome"] or "SL HIT" in r["Outcome"])
    idx_total = len(index_results)
    idx_win_rate = (idx_wins / (idx_wins + idx_losses) * 100) if (idx_wins + idx_losses) > 0 else 0.0

    stk_wins = sum(1 for r in stock_results if "WIN" in r["Outcome"])
    stk_losses = sum(1 for r in stock_results if "LOSS" in r["Outcome"] or "SL HIT" in r["Outcome"])
    stk_total = len(stock_results)
    stk_win_rate = (stk_wins / (stk_wins + stk_losses) * 100) if (stk_wins + stk_losses) > 0 else 0.0

    summary_data = {
        "start_date": start_date,
        "end_date": end_date,
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

    print(f"BACKTEST_FINISHED_SUCCESSFULLY in {time.time() - t0:.2f}s", flush=True)

if __name__ == "__main__":
    run_master_backtest(start_date="2026-07-20", end_date="2026-07-25")
