import os
import sys
import json
import time
import logging
import argparse
import requests
from datetime import datetime as dt, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add common directory to path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
COMMON_DIR = os.path.join(PROJECT_ROOT, "common")
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

from trading_core import (
    load_kite_session,
    scan_anchor_bcd_breakout_generic,
    find_profit_targets,
    find_profit_targets_bearish,
    calculate_sl_buffer,
    INDEX_REGISTRY,
    STOCK_REGISTRY
)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def fetch_stock_30m_candles(symbol):
    """
    Direct Yahoo Finance API candle fetcher (Ultra-fast 1.3s parallel load).
    """
    ticker_symbol = symbol + ".NS" if not symbol.startswith("^") and not symbol.startswith("NIFTY_") and not symbol.endswith(".NS") else symbol
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_symbol}?range=1mo&interval=30m"
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=6)
        if r.status_code != 200:
            return symbol, pd.DataFrame()
        data = r.json()
        result = data['chart']['result'][0]
        timestamps = result['timestamp']
        quote = result['indicators']['quote'][0]
        
        df = pd.DataFrame({
            'date': [pd.to_datetime(ts, unit='s').tz_localize('UTC').tz_convert('Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps],
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
    """
    Simulate realistic stock option premium series based on stock spot candles.
    """
    if df_spot.empty:
        return pd.DataFrame()

    df_opt = df_spot.copy()
    first_close = df_spot.iloc[0]['close']
    
    # Dynamic option base premium scaling (~1.5% for stock options)
    base_premium = round(max(5.0, first_close * 0.015), 2)
    delta = 0.50  # ATM Delta ~ 0.50

    closes = []
    highs = []
    lows = []
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

def simulate_trade_outcome(df_candles, d_index, entry, sl, t1, t2=None, t3=None, side="BULL"):
    """
    Evaluates forward candles after confirmation D candle (d_index)
    to determine if T1, T2, T3 or SL was hit first.
    """
    subs = df_candles.iloc[d_index + 1:]
    if subs.empty:
        return "ACTIVE / NO EXIT", 0.0

    for _, row in subs.iterrows():
        l = float(row['low'])
        h = float(row['high'])
        c = float(row['close'])

        if side == "BULL":
            if c <= sl or l <= sl:
                pnl_pct = round((sl - entry) / entry * 100, 2)
                return "SL HIT (LOSS)", pnl_pct
            if t3 and h >= t3:
                pnl_pct = round((t3 - entry) / entry * 100, 2)
                return "T3 HIT (WIN)", pnl_pct
            if t2 and h >= t2:
                pnl_pct = round((t2 - entry) / entry * 100, 2)
                return "T2 HIT (WIN)", pnl_pct
            if h >= t1:
                pnl_pct = round((t1 - entry) / entry * 100, 2)
                return "T1 HIT (WIN)", pnl_pct
        else: # BEAR
            if c >= sl or h >= sl:
                pnl_pct = round((entry - sl) / entry * 100, 2)
                return "SL HIT (LOSS)", pnl_pct
            if t3 and l <= t3:
                pnl_pct = round((entry - t3) / entry * 100, 2)
                return "T3 HIT (WIN)", pnl_pct
            if t2 and l <= t2:
                pnl_pct = round((entry - t2) / entry * 100, 2)
                return "T2 HIT (WIN)", pnl_pct
            if l <= t1:
                pnl_pct = round((entry - t1) / entry * 100, 2)
                return "T1 HIT (WIN)", pnl_pct

    last_close = float(subs.iloc[-1]['close'])
    if side == "BULL":
        pnl_pct = round((last_close - entry) / entry * 100, 2)
    else:
        pnl_pct = round((entry - last_close) / entry * 100, 2)
    return "ACTIVE / NO EXIT", pnl_pct

def process_stock_scan(symbol, df_spot, interval="30m", start_date="2026-07-20", end_date="2026-07-25"):
    if df_spot.empty or len(df_spot) < 20:
        return []

    results = []
    # Filter session range
    last_week_df = df_spot[(df_spot['date'] >= start_date) & (df_spot['date'] <= end_date + " 23:59:59")]
    if last_week_df.empty:
        return []

    start_idx = last_week_df.index[0]
    end_idx = last_week_df.index[-1]

    # 1. Scan Stock Spot 30m Chart
    last_d_bull = -1
    last_d_bear = -1

    for idx in range(start_idx, end_idx + 1):
        sub_df = df_spot.iloc[: idx + 1].copy()

        # Scan BULL
        res_bull = scan_anchor_bcd_breakout_generic(sub_df, df_spot, side="BULL")
        if res_bull:
            d_time = str(res_bull.get("CandleTime") or res_bull.get("D_time") or sub_df.iloc[-1]["date"])
            a_time = str(res_bull.get("CandleATime") or res_bull.get("A_time") or "N/A")
            match_df = df_spot[df_spot["date"].astype(str) == d_time]
            if not match_df.empty:
                d_i = match_df.index[0]
                if d_i > last_d_bull:
                    last_d_bull = d_i
                    entry = float(res_bull.get("Close") or res_bull.get("Entry"))
                    sl = float(res_bull["SL"])
                    t1 = float(res_bull["T1"])
                    t2 = res_bull.get("T2")
                    t3 = res_bull.get("T3")
                    rr = float(res_bull.get("RR", 0))
                    pattern = str(res_bull.get("Pattern", "BULL_SPOT_ABCD"))

                    outcome, pnl_pct = simulate_trade_outcome(df_spot, d_i, entry, sl, t1, t2, t3, side="BULL")
                    results.append({
                        "Category": "SPOT_STOCK",
                        "Asset": f"{symbol}",
                        "TF": interval,
                        "Side": "BULL",
                        "Pattern": pattern,
                        "BM_Time": a_time,
                        "D_Time": d_time,
                        "Entry": entry,
                        "SL": sl,
                        "T1": t1,
                        "T2": t2 or 0.0,
                        "RR": rr,
                        "Outcome": outcome,
                        "P&L %": pnl_pct
                    })

        # Scan BEAR
        res_bear = scan_anchor_bcd_breakout_generic(sub_df, df_spot, side="BEAR")
        if res_bear:
            d_time = str(res_bear.get("CandleTime") or res_bear.get("D_time") or sub_df.iloc[-1]["date"])
            a_time = str(res_bear.get("CandleATime") or res_bear.get("A_time") or "N/A")
            match_df = df_spot[df_spot["date"].astype(str) == d_time]
            if not match_df.empty:
                d_i = match_df.index[0]
                if d_i > last_d_bear:
                    last_d_bear = d_i
                    entry = float(res_bear.get("Close") or res_bear.get("Entry"))
                    sl = float(res_bear["SL"])
                    t1 = float(res_bear["T1"])
                    t2 = res_bear.get("T2")
                    t3 = res_bear.get("T3")
                    rr = float(res_bear.get("RR", 0))
                    pattern = str(res_bear.get("Pattern", "BEAR_SPOT_ABCD"))

                    outcome, pnl_pct = simulate_trade_outcome(df_spot, d_i, entry, sl, t1, t2, t3, side="BEAR")
                    results.append({
                        "Category": "SPOT_STOCK",
                        "Asset": f"{symbol}",
                        "TF": interval,
                        "Side": "BEAR",
                        "Pattern": pattern,
                        "BM_Time": a_time,
                        "D_Time": d_time,
                        "Entry": entry,
                        "SL": sl,
                        "T1": t1,
                        "T2": t2 or 0.0,
                        "RR": rr,
                        "Outcome": outcome,
                        "P&L %": pnl_pct
                    })

    # 2. Scan Stock Option CE & PE Charts
    for opt_type in ["CE", "PE"]:
        df_opt = simulate_option_premium_series(df_spot, option_type=opt_type)
        if df_opt.empty:
            continue

        last_d_opt = -1
        for idx in range(start_idx, end_idx + 1):
            sub_opt = df_opt.iloc[: idx + 1].copy()

            res_opt = scan_anchor_bcd_breakout_generic(sub_opt, df_opt, side="BULL")
            if res_opt:
                d_time = str(res_opt.get("CandleTime") or res_opt.get("D_time") or sub_opt.iloc[-1]["date"])
                a_time = str(res_opt.get("CandleATime") or res_opt.get("A_time") or "N/A")
                match_df = df_opt[df_opt["date"].astype(str) == d_time]
                if not match_df.empty:
                    d_i = match_df.index[0]
                    if d_i > last_d_opt:
                        last_d_opt = d_i
                        entry = float(res_opt.get("Close") or res_opt.get("Entry"))
                        sl = float(res_opt["SL"])
                        t1 = float(res_opt["T1"])
                        t2 = res_opt.get("T2")
                        t3 = res_opt.get("T3")
                        rr = float(res_opt.get("RR", 0))
                        pattern = str(res_opt.get("Pattern", "STOCK_OPTION_ABCD"))

                        outcome, pnl_pct = simulate_trade_outcome(df_opt, d_i, entry, sl, t1, t2, t3, side="BULL")
                        results.append({
                            "Category": "STOCK_OPTION",
                            "Asset": f"{symbol} {opt_type}",
                            "TF": interval,
                            "Side": opt_type,
                            "Pattern": pattern,
                            "BM_Time": a_time,
                            "D_Time": d_time,
                            "Entry": entry,
                            "SL": sl,
                            "T1": t1,
                            "T2": t2 or 0.0,
                            "RR": rr,
                            "Outcome": outcome,
                            "P&L %": pnl_pct
                        })

    return results

def run_nifty50_stock_options_backtest(interval="30m", start_date="2026-07-20", end_date="2026-07-25"):
    symbols = sorted(STOCK_REGISTRY.keys())
    print("\n=========================================================================================================================")
    print(f"      SPOT NIFTY 50 STOCK LIST & OPTIONS BACKTEST ({interval.upper()} TIMEFRAME) — PERIOD: {start_date} to {end_date}")
    print(f"      TOTAL NIFTY 50 STOCKS SCANNED: {len(symbols)}")
    print("=========================================================================================================================")

    t0 = time.time()
    logging.info(f"Downloading 30m candles for all {len(symbols)} Nifty 50 stocks...")

    stock_dfs = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_stock_30m_candles, sym) for sym in symbols]
        for future in as_completed(futures):
            sym, df = future.result()
            if not df.empty:
                stock_dfs[sym] = df

    logging.info(f"Downloaded candles for {len(stock_dfs)} stocks in {time.time() - t0:.2f}s! Scanning pattern engine...")

    all_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(process_stock_scan, sym, stock_dfs[sym], interval, start_date, end_date): sym
            for sym in stock_dfs
        }
        for future in as_completed(futures):
            sym = futures[future]
            try:
                res = future.result()
                if res:
                    all_results.extend(res)
            except Exception as e:
                logging.warning(f"Error scanning {sym}: {e}")

    # Print Detailed Backtest Output Table
    print("\n" + "=" * 140)
    header = f"{'Stock / Option':<16} | {'TF':<4} | {'Side':<4} | {'Pattern Formed':<22} | {'A (BM) Time':<17} | {'D Closed Time':<17} | {'Entry':<8} | {'SL':<8} | {'T1':<8} | {'R:R':<5} | {'Outcome':<15} | {'P&L %'}"
    print(header)
    print("-" * 140)

    wins = 0
    losses = 0
    total_trades = len(all_results)

    # Sort results by D_Time for chronological order
    all_results.sort(key=lambda x: str(x.get("D_Time", "")))

    for r in all_results:
        is_win = "WIN" in r["Outcome"]
        is_loss = "LOSS" in r["Outcome"] or "SL HIT" in r["Outcome"]
        if is_win:
            wins += 1
        elif is_loss:
            losses += 1

        print(f"{r['Asset']:<16} | {r['TF']:<4} | {r['Side']:<4} | {r['Pattern']:<22} | {r['BM_Time']:<17} | {r['D_Time']:<17} | {r['Entry']:<8.2f} | {r['SL']:<8.2f} | {r['T1']:<8.2f} | {r['RR']:<5.2f} | {r['Outcome']:<15} | {r['P&L %']:+.2f}%")

    print("-" * 140)
    win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0

    print("\n=========================================================================================================================")
    print(f"                       SPOT NIFTY 50 STOCK & OPTIONS (30-MIN TF) — WIN RATE SUMMARY METRICS")
    print("=========================================================================================================================")
    print(f"  • BACKTEST SESSION PERIOD   : {start_date} to {end_date}")
    print(f"  • NIFTY 50 STOCKS EVALUATED : {len(symbols)}")
    print(f"  • TOTAL TRADES TRIGGERED    : {total_trades}")
    print(f"  • WINNING TRADES (T1/T2/T3) : {wins}")
    print(f"  • LOSING TRADES (SL HIT)    : {losses}")
    print(f"  • ACTIVE / OPEN TRADES      : {total_trades - (wins + losses)}")
    print(f"  • STRATEGY WIN RATE (%)     : {win_rate:.2f}%")
    print("=========================================================================================================================\n")

    # Export report to JSON & CSV
    export_dir = os.path.join(PROJECT_ROOT, "Trade_Option", "output", "exports")
    os.makedirs(export_dir, exist_ok=True)

    csv_path = os.path.join(export_dir, f"nifty50_stock_options_backtest_{interval}.csv")
    json_path = os.path.join(export_dir, f"nifty50_stock_options_backtest_{interval}.json")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    df_res = pd.DataFrame(all_results)
    if not df_res.empty:
        df_res.to_csv(csv_path, index=False)

    logging.info(f"Nifty 50 Stock Options Backtest report saved to: {csv_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Price Action Strategy Backtest Simulation Engine")
    parser.add_argument("--mode", type=str, default="stock_options", choices=["stock_options", "index", "all"], help="Backtest mode")
    parser.add_argument("--tf", type=str, default="30m", help="Timeframe (e.g. 30m, 15m)")
    parser.add_argument("--start", type=str, default="2026-07-20", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2026-07-25", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.mode in ["stock_options", "all"]:
        run_nifty50_stock_options_backtest(interval=args.tf, start_date=args.start, end_date=args.end)
