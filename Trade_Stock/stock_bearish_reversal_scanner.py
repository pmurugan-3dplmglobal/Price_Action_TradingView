import os
import json
import logging
import time
import sys
import threading
import csv
COMMON_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "common"))
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)
from datetime import datetime as dt, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np

from kiteconnect import KiteConnect

LOOKBACK_DAYS = 120
TOKEN_FILE = "input/kite_access_token.txt"
TIMEFRAME_ENTRY = "day"
TIMEFRAME_ANCHOR = "day"

OUTPUT_FILE = f"output/exports/Nifty50_Daily_Scan_BEAR_{dt.now().strftime('%Y%m%d_%H%M')}.csv"

ACTIVE_POSITIONS = {}
position_lock = threading.Lock()
ANCHOR_SCAN_REQUEST_FILE = os.path.join("output", "monitor", "anchor_scan_request.txt")
ANCHOR_SCAN_STOP_FILE = os.path.join("output", "monitor", "anchor_scan_stop.txt")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCAN_DISPLAY_FILE = os.path.join(BASE_DIR, "output", "monitor", "scan_display_data.json")
JOURNAL_FILE = os.path.join(BASE_DIR, "output", "monitor", "trade_journal.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("output/logs/bull_bear_daily_scanner.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

from trading_core import (
    load_kite_session,
    log_to_journal,
    scan_anchor_bcd_breakout_bearish,
    scan_anchor_bcd_breakout_generic,
    find_anchor_bearish_engulfing,
    find_anchor_hh_sweep,
    find_anchor_two_lower_lows,
    find_anchor_shooting_star_baby,
    find_anchor_bearish_harami,
    get_adaptive_lookback,
    resample_timeframe,
    sync_stock_tokens,
    fetch_and_resample_candles,
    write_scan_display_data as shared_write_display,
    clean_timestamp,
    STOCK_REGISTRY,
    SUPER_STOCKS
)
from equity_universe import get_universe_symbols_and_tokens, is_liquid_cash_stock

TARGET_INDEX = "NIFTY50"

def run_scan(kite):
    effective_lookback = get_adaptive_lookback(TIMEFRAME_ENTRY, "STOCK_SPOT", LOOKBACK_DAYS)
    from_date = (dt.now() - timedelta(days=min(effective_lookback, 2000))).strftime("%Y-%m-%d")
    to_date = dt.now().strftime("%Y-%m-%d")
    scanners = [
        ("S1_Bear_Anchor_BCD", lambda df_e, df_a: scan_anchor_bcd_breakout_generic(df_e, df_a, side="BEAR")),
    ]
    results = []
    results_lock = threading.Lock()
    symbols_list, token_map = get_universe_symbols_and_tokens(kite, TARGET_INDEX)
    scan_order = sorted(symbols_list)
    logging.info(f"Executing Bearish Scan for Universe '{TARGET_INDEX}' ({len(scan_order)} symbols) on timeframe '{TIMEFRAME_ENTRY}'...")
    tf_clean = str(TIMEFRAME_ENTRY).lower()
    if tf_clean in ["week", "weekly", "w", "1w", "day", "d", "1d"]:
        fetch_tf = "day"
    elif tf_clean in ["3hr", "3h", "180min", "180minute", "4h", "4hour", "240min", "240minute", "1hr", "1h", "60min", "60minute"]:
        fetch_tf = "60minute"
    elif tf_clean in ["75min", "75mins", "75m", "75minute"]:
        fetch_tf = "15minute"
    elif tf_clean in ["30min", "30minute"]:
        fetch_tf = "30minute"
    elif tf_clean in ["15min", "15minute"]:
        fetch_tf = "15minute"
    elif tf_clean in ["10min", "10minute"]:
        fetch_tf = "10minute"
    elif tf_clean in ["5min", "5minute"]:
        fetch_tf = "5minute"
    elif tf_clean in ["3min", "3minute"]:
        fetch_tf = "3minute"
    else:
        fetch_tf = "day"

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {}
        for symbol in scan_order:
            tok = token_map.get(symbol, 0)
            if not tok:
                logging.warning(f"Skipping {symbol}: Instrument token missing")
                with results_lock:
                    results.append({"Symbol": symbol, "Pattern": "NO_TOKEN"})
                continue
            futures[pool.submit(
                lambda t=tok: pd.DataFrame(kite.historical_data(t, from_date, to_date, fetch_tf))
            )] = symbol
            time.sleep(0.2)
        for f in as_completed(futures):
            symbol = futures[f]
            try:
                df_raw = f.result()
                df_e = resample_timeframe(df_raw, TIMEFRAME_ENTRY)
            except Exception as e:
                logging.warning(f"Data error for {symbol}: {e}")
                with results_lock:
                    results.append({"Symbol": symbol, "Pattern": "ERROR", "Error": str(e)})
                continue
            if df_e.empty:
                with results_lock:
                    results.append({"Symbol": symbol, "Pattern": "NO_DATA"})
                continue
            if TARGET_INDEX != "NIFTY50" and not is_liquid_cash_stock(df_e):
                logging.info(f"Skipping {symbol} - failed cash liquidity shield (low volume/turnover)")
                with results_lock:
                    results.append({"Symbol": symbol, "Pattern": "ILLIQUID_SKIPPED"})
                continue
            df_a = df_e.copy()
            latest = df_e.iloc[-1]
            matched = False
            for name, scanner_func in scanners:
                result = scanner_func(df_e, df_a)
                if result:
                    result["Symbol"] = symbol
                    entry_val = float(result.get("Close") or result.get("Entry") or 0.0)
                    result["Close"] = entry_val
                    result["Scan_Date"] = dt.now().strftime("%Y-%m-%d")
                    result["Latest_Close"] = round(float(latest['close']), 2)
                    result["Latest_High"] = round(float(latest['high']), 2)
                    result["Latest_Low"] = round(float(latest['low']), 2)
                    result["Latest_Open"] = round(float(latest['open']), 2)
                    result["Volume"] = int(latest.get('volume', 0))
                    result["Pattern_Name"] = name
                    with results_lock:
                        results.append(result)
                    logging.info(f"  -> BEAR MATCH: {symbol} | {result['Pattern']} | Entry: {entry_val:.2f} | SL: {result['SL']:.2f} | T1: {result['T1']:.2f} | RR: {result['RR']:.2f}")
                    log_to_journal(symbol, result["Pattern"], TIMEFRAME_ENTRY,
                                   "SCAN_MATCH_BEAR", "MATCHED",
                                   f"Entry={entry_val:.2f} SL={result['SL']:.2f} RR={result['RR']:.2f}",
                                   entry=entry_val, sl=result['SL'],
                                   target=result.get('T3',''), rr=result['RR'])

                    with position_lock:
                        all_disp = [r for r in results if r.get("Pattern") and r.get("Pattern") not in ["NO_MATCH", "ERROR", "NO_DATA"]]
                        formatted_all = [{
                            "symbol": r.get("Symbol") or r.get("symbol", ""),
                            "contract": r.get("Symbol") or r.get("symbol", ""),
                            "entry_spot": r.get("Close"),
                            "current_sl": r.get("SL"),
                            "t1": r.get("T1"),
                            "t2": r.get("T2"),
                            "t3": r.get("T3"),
                            "rr": r.get("RR", 0.0),
                            "pattern": r.get("Pattern"),
                            "timeframe": TIMEFRAME_ENTRY,
                            "side": "SELL",
                            "entry_time": clean_timestamp(r.get("CandleATime") or r.get("CandleTime")),
                            "candle_a_time": clean_timestamp(r.get("CandleATime") or r.get("CandleTime"))
                        } for r in all_disp if r.get("Symbol") or r.get("symbol")]
                        shared_write_display(formatted_all, dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "nifty50")
                    matched = True
                    break
            if not matched:
                with results_lock:
                    results.append({"Symbol": symbol, "Pattern": "NO_MATCH"})
    formed_display = []
    for r in results:
        if r.get("Pattern") and r.get("Pattern") not in ["NO_MATCH", "ERROR", "NO_DATA"]:
            c_time = clean_timestamp(r.get("CandleATime") or r.get("CandleTime") or r.get("Scan_Date"))
            formed_display.append({
                "symbol": r.get("Symbol"),
                "contract": r.get("Symbol"),
                "entry_spot": r.get("Close"),
                "current_sl": r.get("SL"),
                "t1": r.get("T1"),
                "t2": r.get("T2"),
                "t3": r.get("T3"),
                "rr": r.get("RR", 0.0),
                "pattern": r.get("Pattern"),
                "timeframe": TIMEFRAME_ENTRY,
                "side": "SELL",
                "entry_time": c_time,
                "candle_a_time": c_time
            })
    if formed_display:
        with position_lock:
            shared_write_display(formed_display, dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "nifty50")
    return results

def export_results(results):
    rows = []
    for r in results:
        rows.append({
            "Symbol": r.get("Symbol", ""),
            "Pattern": r.get("Pattern", ""),
            "Entry": r.get("Close", ""),
            "Stop_Loss": r.get("SL", ""),
            "T1": r.get("T1", ""),
            "T2": r.get("T2", ""),
            "T3": r.get("T3", ""),
            "R_R_Ratio": round(r.get("RR", 0), 2) if r.get("RR") else "",
            "Latest_Close": r.get("Latest_Close", ""),
            "Latest_High": r.get("Latest_High", ""),
            "Latest_Low": r.get("Latest_Low", ""),
            "Latest_Open": r.get("Latest_Open", ""),
            "Volume": r.get("Volume", ""),
            "Error": r.get("Error", ""),
            "Scan_Date": r.get("Scan_Date", "")
        })
    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    return OUTPUT_FILE

def run_anchor_scan(kite):
    logging.info("On-demand scan requested: executing full A-B-C-D bearish breakout scan across Nifty 50 stocks...")
    load_program_config()
    results = run_scan(kite)
    logging.info(f"On-demand scan complete: found {len([r for r in (results or []) if r.get('T1')])} full A-B-C-D bearish setup(s)")

def print_summary(results):
    matches = [r for r in results if r.get("T1")]
    no_match = [r for r in results if r.get("Pattern") == "NO_MATCH"]
    errors = [r for r in results if r.get("Pattern") == "ERROR"]
    print("\n" + "=" * 80)
    print(f"  NIFTY 50 BEARISH DAILY SCAN SUMMARY")
    print(f"  Scan Time: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print(f"  Total Stocks Scanned: {len(results)}")
    print(f"  Pattern Matches:     {len(matches)}")
    print(f"  No Match:            {len(no_match)}")
    print(f"  Errors:              {len(errors)}")
    print("-" * 80)
    if matches:
        print(f"\n  {'Symbol':<12} {'Pattern':<25} {'Entry':<10} {'SL':<10} {'T1':<10} {'T2':<10} {'RR':<8}")
        print(f"  {'-'*12} {'-'*25} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8}")
        for m in sorted(matches, key=lambda x: x.get("RR", 0), reverse=True):
            rr = round(m["RR"], 2) if m.get("RR") else 0
            c_s = f"{m['Close']:<10.2f}" if isinstance(m.get('Close'), (int, float)) else f"{'N/A':<10}"
            sl_s = f"{m['SL']:<10.2f}" if isinstance(m.get('SL'), (int, float)) else f"{'N/A':<10}"
            t1_s = f"{m['T1']:<10.2f}" if isinstance(m.get('T1'), (int, float)) else f"{'N/A':<10}"
            t2_s = f"{m['T2']:<10.2f}" if isinstance(m.get('T2'), (int, float)) else f"{'N/A':<10}"
            print(f"  {m['Symbol']:<12} {m['Pattern']:<25} {c_s} {sl_s} {t1_s} {t2_s} {rr:<8.2f}")
    print("=" * 80)


def load_program_config():
    try:
        cfg_path = os.path.join(os.path.dirname(__file__), "input", "program_config.json")
        if os.path.exists(cfg_path):
            with open(cfg_path) as f:
                cfg = json.load(f).get("bear_trade", {})
            if "timeframe" in cfg:
                globals().update({"TIMEFRAME_ENTRY": cfg["timeframe"], "TIMEFRAME_ANCHOR": cfg["timeframe"]})
            if "lookback_days" in cfg: globals().update({"LOOKBACK_DAYS": int(cfg["lookback_days"])})
            if "target_index" in cfg: globals().update({"TARGET_INDEX": str(cfg["target_index"])})
    except Exception as e:
        logging.warning(f"Config load: {e}")

def main():
    load_program_config()
    anchor_only = "--anchor-only" in sys.argv
    logging.info("=" * 60)
    logging.info("  NIFTY 50 BEARISH DAILY REVERSAL SCANNER")
    logging.info("=" * 60)
    try:
        ak, at = load_kite_session()
        kite = KiteConnect(api_key=ak)
        kite.set_access_token(at)
        sync_stock_tokens(kite)
        if anchor_only:
            logging.info("Running anchor-only scan (daily bear)...")
            run_anchor_scan(kite)
            return
        logging.info(f"Scanning {len(STOCK_REGISTRY)} stocks for Bearish setups...")
        logging.info(f"Lookback: {LOOKBACK_DAYS} days")
        
        results = run_scan(kite)
        print_summary(results)
        out = export_results(results)
        logging.info(f"Bearish Results exported to: {os.path.abspath(out)}")
        print(f"\n  Report saved: {os.path.abspath(out)}")
        print()
    except Exception as e:
        logging.error(f"Bearish Scanner failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
