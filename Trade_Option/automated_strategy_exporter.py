import os
import sys
import json
import logging
import csv
import time
import argparse
from datetime import datetime as dt, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

COMMON_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "common"))
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

import trade_db

from trading_core import (
    load_kite_session,
    fetch_and_resample_candles,
    scan_anchor_bcd_breakout,
    find_anchor_bullish_engulfing,
    find_anchor_ll_sweep,
    find_anchor_hammer_baby,
    find_anchor_bullish_harami,
    find_anchor_two_higher_highs,
    resolve_option_strikes as shared_resolve_strikes,
    scan_symbol,
    INDEX_REGISTRY,
    STOCK_REGISTRY
)

BASE_EXPORT_DIR = r"G:\Poovendan\AI\Trading\Share\Export_output\Automated"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "output", "logs", "automated_strategy_exporter.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

NFO_INSTRUMENTS = pd.DataFrame()

def sync_instruments(kite):
    global NFO_INSTRUMENTS
    try:
        logging.info("Syncing Kite instrument master (NSE, NFO, BFO)...")
        nse = kite.instruments("NSE")
        df_nse = pd.DataFrame(nse)
        if not df_nse.empty:
            df_nse['tradingsymbol'] = df_nse['tradingsymbol'].str.strip()
            df_nse['segment'] = df_nse['segment'].str.strip()
            synced = 0
            for sym in STOCK_REGISTRY:
                m = df_nse[(df_nse['tradingsymbol'] == sym) & (df_nse['segment'] == 'NSE')]
                if not m.empty:
                    STOCK_REGISTRY[sym]["token"] = int(m.iloc[0]['instrument_token'])
                    synced += 1
            logging.info(f"Synced tokens for {synced} stocks.")

        nfo = kite.instruments("NFO")
        try:
            bfo = kite.instruments("BFO")
        except Exception:
            bfo = []
        combined = (nfo if nfo else []) + (bfo if bfo else [])
        NFO_INSTRUMENTS = pd.DataFrame(combined)
        logging.info(f"Synced {len(NFO_INSTRUMENTS)} option contracts.")
    except Exception as e:
        logging.error(f"Instrument sync error: {e}")

def resolve_option_strikes(symbol, spot_price, step_size, option_type, n_range):
    return shared_resolve_strikes(NFO_INSTRUMENTS, symbol, spot_price, step_size, option_type, n_range)

def dummy_log_fn(*args, **kwargs):
    pass

def run_scan_for_registry(kite, registry, engine_name, timeframe, strike_range=0, max_workers=2):
    ref_now = dt.now()
    limits = {
        "minute": 60, "3minute": 100, "5minute": 100, "10minute": 100,
        "15minute": 200, "30minute": 200, "60minute": 400, "75minute": 400, "day": 2000
    }
    lookback = limits.get(timeframe, 180)
    from_date = (ref_now - timedelta(days=lookback)).strftime("%Y-%m-%d")
    to_date = ref_now.strftime("%Y-%m-%d")

    entry_scanners = [("Setup_1_Anchor_BCD", scan_anchor_bcd_breakout)]
    anchor_scanners = [
        ("A1", find_anchor_bullish_engulfing),
        ("A2", find_anchor_ll_sweep),
        ("A3", find_anchor_hammer_baby),
        ("A4", find_anchor_bullish_harami),
        ("A5", find_anchor_two_higher_highs),
    ]

    all_trades = []
    dummy_positions = {}

    class SimpleLock:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass

    lock_obj = SimpleLock()

    def _scan_one(symbol, config):
        try:
            time.sleep(0.15)  # Throttling to keep API request rate safely within Zerodha limit (max 3 req/sec)
            return scan_symbol(
                kite, symbol, config,
                from_date, to_date, from_date, to_date,
                entry_scanners, anchor_scanners,
                resolve_option_strikes, engine_name,
                timeframe, timeframe, timeframe,
                dummy_positions, lock_obj, trade_db,
                strike_range, dummy_log_fn
            )
        except Exception as err:
            logging.error(f"Error scanning {symbol} ({engine_name} {timeframe}): {err}")
            return []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_scan_one, sym, cfg): sym for sym, cfg in registry.items()}
        for f in as_completed(futures):
            res = f.result()
            if res:
                all_trades.extend(res)

    return all_trades

def export_trades_to_csv(trades, csv_path, scan_slot):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    scan_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    headers = [
        "Scan_Time", "Scan_Slot", "Symbol", "Contract", "Pattern", "Pattern_Code",
        "Side", "Timeframe", "Entry_Spot", "SL", "T1", "T2", "T3",
        "RR", "Candle_Time", "Priority", "Stage_Status"
    ]

    rows = []
    for t in trades:
        rows.append({
            "Scan_Time": scan_time,
            "Scan_Slot": scan_slot,
            "Symbol": t.get("symbol", ""),
            "Contract": t.get("contract", ""),
            "Pattern": t.get("pattern", ""),
            "Pattern_Code": t.get("pattern_code", ""),
            "Side": t.get("side", ""),
            "Timeframe": t.get("timeframe", ""),
            "Entry_Spot": t.get("entry_spot", ""),
            "SL": t.get("current_sl", t.get("sl", "")),
            "T1": t.get("t1", ""),
            "T2": t.get("t2", ""),
            "T3": t.get("t3", ""),
            "RR": t.get("rr", ""),
            "Candle_Time": t.get("entry_time", t.get("CandleTime", "")),
            "Priority": t.get("priority", "HIGH_PRIORITY"),
            "Stage_Status": t.get("stage_status", "FRESH_ENTRY")
        })

    df = pd.DataFrame(rows, columns=headers)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logging.info(f"Exported {len(df)} rows to: {csv_path}")

def execute_scheduled_export(slot_name=None):
    if not slot_name:
        now_time = dt.now().time()
        if now_time < dt.strptime("11:30", "%H:%M").time():
            slot_name = "10_30_AM"
        elif now_time < dt.strptime("14:00", "%H:%M").time():
            slot_name = "01_00_PM"
        else:
            slot_name = "03_15_PM"

    today_folder_name = dt.now().strftime("%d_%b_%y")  # e.g., 30_Jul_26
    slot_dir = os.path.join(BASE_EXPORT_DIR, today_folder_name, slot_name)
    os.makedirs(slot_dir, exist_ok=True)

    logging.info(f"Starting Automated Strategy Export for Slot [{slot_name}] -> Directory: {slot_dir}")

    kite = None
    logging.info("[OPEN_SOURCE] Open-source Yahoo Finance data feed active.")
    sync_instruments(kite)

    index_jobs = [
        {"tag": "3min",  "tf": "3minute"},
        {"tag": "5min",  "tf": "5minute"},
        {"tag": "15min", "tf": "15minute"},
    ]

    stock_jobs = [
        {"tag": "15min", "tf": "15minute"},
        {"tag": "60min", "tf": "60minute"},
    ]

    # 1. Process Index Option Scans (3 files)
    for job in index_jobs:
        tf = job["tf"]
        tag = job["tag"]
        csv_filename = f"Index_Option_Scan_{tag}_{slot_name}.csv"
        csv_path = os.path.join(slot_dir, csv_filename)
        logging.info(f"Scanning Index Options for timeframe [{tf}]...")
        trades = run_scan_for_registry(kite, INDEX_REGISTRY, "index", tf, strike_range=3, max_workers=3)
        export_trades_to_csv(trades, csv_path, slot_name)

    # 2. Process Stock Option Scans (2 files)
    for job in stock_jobs:
        tf = job["tf"]
        tag = job["tag"]
        csv_filename = f"Stock_Option_Scan_{tag}_{slot_name}.csv"
        csv_path = os.path.join(slot_dir, csv_filename)
        logging.info(f"Scanning Stock Options for timeframe [{tf}]...")
        trades = run_scan_for_registry(kite, STOCK_REGISTRY, "nifty50", tf, strike_range=0, max_workers=5)
        export_trades_to_csv(trades, csv_path, slot_name)

    logging.info(f"SUCCESS: Automated export completed for slot [{slot_name}]. Output: {slot_dir}")
    print(f"\n[SUCCESS] Export complete for slot [{slot_name}]! Files saved at:\n{slot_dir}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated Strategy Multi-Timeframe Daily Exporter")
    parser.add_argument("--slot", choices=["10_30_AM", "01_00_PM", "03_15_PM"], help="Specific execution slot name")
    args = parser.parse_args()

    execute_scheduled_export(slot_name=args.slot)
