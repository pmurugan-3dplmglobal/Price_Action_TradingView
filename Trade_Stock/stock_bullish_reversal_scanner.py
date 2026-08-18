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

LOOKBACK_DAYS = 120
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TIMEFRAME_ENTRY = "day"
TIMEFRAME_ANCHOR = "day"

OUTPUT_FILE = os.path.join(BASE_DIR, "output", "exports", f"Nifty50_Daily_Scan_{dt.now().strftime('%Y%m%d_%H%M')}.csv")

ACTIVE_POSITIONS = {}
position_lock = threading.Lock()
ANCHOR_SCAN_REQUEST_FILE = os.path.join(BASE_DIR, "output", "monitor", "anchor_scan_request.txt")
ANCHOR_SCAN_STOP_FILE = os.path.join(BASE_DIR, "output", "monitor", "anchor_scan_stop.txt")

SCAN_DISPLAY_FILE = os.path.join(BASE_DIR, "output", "monitor", "scan_display_data.json")
JOURNAL_FILE = os.path.join(BASE_DIR, "output", "monitor", "trade_journal.csv")

LOG_FILE_PATH = os.path.join(BASE_DIR, "output", "logs", "bull_daily_scanner.log")
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

from trading_core import (
    load_kite_session,
    log_to_journal,
    scan_anchor_bcd_breakout,
    scan_anchor_bcd_breakout_generic,
    find_anchor_bullish_engulfing,
    find_anchor_ll_sweep,
    find_anchor_hammer_baby,
    find_anchor_bullish_harami,
    get_adaptive_lookback,
    resample_timeframe,
    sync_stock_tokens,
    fetch_and_resample_candles,
    write_scan_display_data as shared_write_display,
    clean_timestamp,
    detect_parabolic_multi_swings,
    is_anchor_after_terminal_base,
    STOCK_REGISTRY,
    SUPER_STOCKS
)
from equity_universe import get_universe_symbols_and_tokens, is_liquid_cash_stock

LOOKBACK_DAYS = 120
TOKEN_FILE = os.path.join(BASE_DIR, "input", "kite_access_token.txt")
TIMEFRAME_ENTRY = "day"
TIMEFRAME_ANCHOR = "day"
TARGET_INDEX = "NIFTY50"
ENABLE_SWINGFILTER = True
MIN_CASCADING_WAVES = 3
MIN_R2 = 0.55

journal_lock = threading.Lock()

def run_scan(kite):
    effective_lookback = get_adaptive_lookback(TIMEFRAME_ENTRY, "STOCK_SPOT", LOOKBACK_DAYS)
    from_date = (dt.now() - timedelta(days=min(effective_lookback, 2000))).strftime("%Y-%m-%d")
    to_date = dt.now().strftime("%Y-%m-%d")
    scanners = [
        ("S1_Anchor_BCD", lambda df_e, df_a: scan_anchor_bcd_breakout_generic(df_e, df_a, side="BULL")),
    ]
    results = []
    results_lock = threading.Lock()
    symbols_list, token_map = get_universe_symbols_and_tokens(kite, TARGET_INDEX)
    scan_order = sorted(symbols_list)
    logging.info(f"Executing Bullish Scan for Universe '{TARGET_INDEX}' ({len(scan_order)} symbols) on timeframe '{TIMEFRAME_ENTRY}'...")
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
            futures[pool.submit(
                lambda s=symbol: fetch_and_resample_candles(kite, s, from_date, to_date, TIMEFRAME_ENTRY)
            )] = symbol
            time.sleep(0.05)
        for f in as_completed(futures):
            symbol = futures[f]
            try:
                df_e = f.result()
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

            # Phase 0: Parabolic Multi-Swing Exhaustion Filter
            para_struct = {}
            if ENABLE_SWINGFILTER:
                para_struct = detect_parabolic_multi_swings(
                    df_e, side="BULL", min_swings=MIN_CASCADING_WAVES, min_r2=MIN_R2
                )
                if not para_struct.get("matched", False):
                    logging.debug(f"Skipping {symbol} - failed Parabolic Curve filter ({para_struct.get('valid_arch_count', 0)}/{MIN_CASCADING_WAVES} waves)")
                    with results_lock:
                        results.append({"Symbol": symbol, "Pattern": "NO_MATCH", "Reason": "Failed Parabolic Exhaustion Filter"})
                    continue

            df_a = df_e.copy()
            latest = df_e.iloc[-1]
            matched = False
            for name, scanner_func in scanners:
                result = scanner_func(df_e, df_a)
                if result:
                    # Enforce that Anchor Candle A formed at or after the 4th swing (terminal base)
                    if ENABLE_SWINGFILTER and not is_anchor_after_terminal_base(
                        df_a, result.get("CandleATime", result.get("CandleTime")), para_struct
                    ):
                        logging.debug(f"{symbol} skipped - Anchor formed before 4th swing terminal base")
                        continue

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
                    result["Parabolic_Matched"] = para_struct.get("matched", False) if ENABLE_SWINGFILTER else True
                    result["Parabolic_Waves"] = para_struct.get("valid_arch_count", 0) if ENABLE_SWINGFILTER else 0
                    result["Terminal_Base"] = para_struct.get("has_terminal_base", False) if ENABLE_SWINGFILTER else False
                    result["Parabolic_Score"] = f"{result['Parabolic_Waves']}W{'+Abs' if result['Terminal_Base'] else ''}" if ENABLE_SWINGFILTER else "N/A"
                    with results_lock:
                        results.append(result)
                    logging.info(f"  -> MATCH: {symbol} | {result['Pattern']} | Entry: {entry_val:.2f} | SL: {result['SL']:.2f} | T1: {result['T1']:.2f} | RR: {result['RR']:.2f} | Parabolic: {result['Parabolic_Score']}")
                    log_to_journal(symbol, result["Pattern"], TIMEFRAME_ENTRY,
                                   "SCAN_MATCH", "MATCHED",
                                   f"Entry={entry_val:.2f} SL={result['SL']:.2f} RR={result['RR']:.2f} Parabolic={result['Parabolic_Score']}",
                                   entry=entry_val, sl=result['SL'],
                                   target=result.get('T3',''), rr=result['RR'])

                    c_time = clean_timestamp(result.get("CandleATime") or result.get("CandleTime") or dt.now().strftime("%Y-%m-%d %H:%M"))
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
                            "parabolic_score": r.get("Parabolic_Score", "N/A"),
                            "parabolic_waves": r.get("Parabolic_Waves", 0),
                            "terminal_base": r.get("Terminal_Base", False),
                            "timeframe": TIMEFRAME_ENTRY,
                            "side": "BUY",
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
                "parabolic_score": r.get("Parabolic_Score", "N/A"),
                "parabolic_waves": r.get("Parabolic_Waves", 0),
                "terminal_base": r.get("Terminal_Base", False),
                "timeframe": TIMEFRAME_ENTRY,
                "side": "BUY",
                "entry_time": c_time,
                "candle_a_time": c_time
            })
    with position_lock:
        shared_write_display(formed_display, dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "nifty50")
    return results

def export_results(results):
    rows = []
    for r in results:
        rows.append({
            "Symbol": r.get("Symbol", ""),
            "Pattern": r.get("Pattern", ""),
            "Parabolic_Score": r.get("Parabolic_Score", ""),
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

# ──────────────────────────────────────────────
#  ANCHOR (A-FORMATION) DETECTION — 4 PATTERNS
# ──────────────────────────────────────────────

def run_anchor_scan(kite):
    logging.info("On-demand scan requested: executing full A-B-C-D bullish breakout scan across Nifty 50 stocks...")
    load_program_config()
    results = run_scan(kite)
    logging.info(f"On-demand scan complete: found {len([r for r in (results or []) if r.get('T1')])} full A-B-C-D bullish setup(s)")

def print_summary(results):
    matches = [r for r in results if r.get("T1")]
    no_match = [r for r in results if r.get("Pattern") == "NO_MATCH"]
    errors = [r for r in results if r.get("Pattern") == "ERROR"]
    print("\n" + "=" * 80)
    print(f"  NIFTY 50 DAILY SCAN SUMMARY")
    print(f"  Scan Time: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print(f"  Total Stocks Scanned: {len(results)}")
    print(f"  Pattern Matches:     {len(matches)}")
    print(f"  No Match:            {len(no_match)}")
    print(f"  Errors:              {len(errors)}")
    print("-" * 80)
    if matches:
        print(f"\n  {'Symbol':<12} {'Pattern':<20} {'Parabolic':<10} {'Entry':<10} {'SL':<10} {'T1':<10} {'T2':<10} {'RR':<8}")
        print(f"  {'-'*12} {'-'*20} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8}")
        for m in sorted(matches, key=lambda x: x.get("RR", 0), reverse=True):
            rr = round(m["RR"], 2) if m.get("RR") else 0
            c_s = f"{m['Close']:<10.2f}" if isinstance(m.get('Close'), (int, float)) else f"{'N/A':<10}"
            sl_s = f"{m['SL']:<10.2f}" if isinstance(m.get('SL'), (int, float)) else f"{'N/A':<10}"
            t1_s = f"{m['T1']:<10.2f}" if isinstance(m.get('T1'), (int, float)) else f"{'N/A':<10}"
            t2_s = f"{m['T2']:<10.2f}" if isinstance(m.get('T2'), (int, float)) else f"{'N/A':<10}"
            para_s = f"{str(m.get('Parabolic_Score', 'N/A')):<10}"
            print(f"  {m['Symbol']:<12} {m['Pattern']:<20} {para_s} {c_s} {sl_s} {t1_s} {t2_s} {rr:<8.2f}")
    print("=" * 80)


def load_program_config():
    try:
        cfg_path = os.path.join(os.path.dirname(__file__), "input", "program_config.json")
        if os.path.exists(cfg_path):
            with open(cfg_path) as f:
                cfg = json.load(f).get("daily", {})
            if "timeframe" in cfg:
                globals().update({"TIMEFRAME_ENTRY": cfg["timeframe"], "TIMEFRAME_ANCHOR": cfg["timeframe"]})
            if "lookback_days" in cfg: globals().update({"LOOKBACK_DAYS": int(cfg["lookback_days"])})
            if "target_index" in cfg: globals().update({"TARGET_INDEX": str(cfg["target_index"])})
            if "enable_swingfilter" in cfg or "enable_parabolic_filter" in cfg:
                val = cfg.get("enable_swingfilter", cfg.get("enable_parabolic_filter"))
                b_val = str(val).lower() in ["true", "1", "yes"] if isinstance(val, (str, bool, int)) else True
                globals().update({"ENABLE_SWINGFILTER": b_val})
            if "min_cascading_waves" in cfg:
                globals().update({"MIN_CASCADING_WAVES": int(cfg["min_cascading_waves"])})
            if "min_r2" in cfg:
                globals().update({"MIN_R2": float(cfg["min_r2"])})
    except Exception as e:
        logging.warning(f"Config load: {e}")

def main():
    load_program_config()
    anchor_only = "--anchor-only" in sys.argv
    logging.info("=" * 60)
    logging.info("  NIFTY 50 DAILY TIMEFRAME SCANNER")
    logging.info("=" * 60)
    try:
        kite = None
        logging.info("[OPEN_SOURCE] Open-source Yahoo Finance data feed active.")
        if anchor_only:
            logging.info("Running anchor-only scan (daily)...")
            run_anchor_scan(kite)
            return
        logging.info(f"Scanning {len(STOCK_REGISTRY)} stocks on daily timeframe...")
        logging.info(f"Lookback: {LOOKBACK_DAYS} days")
        
        if os.path.exists(ANCHOR_SCAN_REQUEST_FILE):
            try:
                with open(ANCHOR_SCAN_REQUEST_FILE) as f:
                    engine = f.read().strip()
                os.remove(ANCHOR_SCAN_REQUEST_FILE)
                if engine != "daily":
                    logging.info(f"Anchor scan flag not for daily, skipping (got {engine})")
                else:
                    logging.info(f"Anchor scan requested via flag file (engine: {engine})")
                    run_anchor_scan(kite)
            except Exception:
                pass
        
        results = run_scan(kite)
        print_summary(results)
        out = export_results(results)
        logging.info(f"Results exported to: {os.path.abspath(out)}")
        print(f"\n  Report saved: {os.path.abspath(out)}")
        print()
    except Exception as e:
        logging.error(f"Scanner failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
