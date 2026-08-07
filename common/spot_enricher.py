# Common Spot Trend & Target Annotator Utility
# Enriches Scan Tab CSV exports with underlying Spot Trend and Spot T1 Target without modifying existing trading engines.

import os
import logging
import pandas as pd
from datetime import datetime as dt, timedelta

from trading_core import (
    load_kite_session,
    fetch_and_resample_candles,
    scan_anchor_bcd_breakout_generic,
    find_profit_targets,
    find_profit_targets_bearish,
    STOCK_REGISTRY,
    INDEX_REGISTRY
)

def extract_underlying_symbol(contract_or_symbol):
    """
    Extracts the underlying stock/index symbol from a contract or symbol string.
    Example: BAJAJFINSV26AUG1920PE -> BAJAJFINSV, NIFTY26AUG24500CE -> NIFTY
    """
    if not contract_or_symbol:
        return ""
    
    clean = str(contract_or_symbol).strip().upper()
    
    # Check known Index Registry
    for idx_name in INDEX_REGISTRY:
        if clean.startswith(idx_name):
            return idx_name
            
    # Check known Stock Registry
    for stock_name in sorted(STOCK_REGISTRY.keys(), key=len, reverse=True):
        if clean.startswith(stock_name):
            return stock_name
            
    # Fallback strip digits/CE/PE suffixes
    base = clean.split("26")[0].split("25")[0].replace("CE", "").replace("PE", "").strip()
    return base or clean

def evaluate_spot_trend_and_t1(kite, underlying_symbol, timeframe="day"):
    """
    Fetches underlying spot candles and calculates Spot_Trend (BULL/BEAR) and Spot_T1_Target.
    """
    try:
        # Determine token for underlying spot
        token = None
        if underlying_symbol in INDEX_REGISTRY:
            token = INDEX_REGISTRY[underlying_symbol]["token"]
        elif underlying_symbol in STOCK_REGISTRY:
            token = STOCK_REGISTRY[underlying_symbol]["token"]
            
        if not token and kite:
            # Fallback: search NSE master if token missing
            try:
                insts = kite.instruments("NSE")
                for item in insts:
                    if item.get("tradingsymbol") == underlying_symbol and item.get("segment") == "NSE":
                        token = int(item["instrument_token"])
                        break
            except Exception:
                pass
                
        if not token:
            return "UNKNOWN", "N/A"

        from_date = (dt.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        to_date = dt.now().strftime("%Y-%m-%d")
        
        df_raw = fetch_and_resample_candles(kite, token, from_date, to_date, timeframe)
        if df_raw is None or df_raw.empty:
            return "NO_DATA", "N/A"
            
        latest_close = float(df_raw.iloc[-1]['close'])
        
        # Check Bullish Spot Setup
        bull_res = scan_anchor_bcd_breakout_generic(df_raw, df_raw, side="BULL")
        if bull_res and bull_res.get("T1"):
            return "BULL", round(float(bull_res["T1"]), 2)
            
        # Check Bearish Spot Setup
        bear_res = scan_anchor_bcd_breakout_generic(df_raw, df_raw, side="BEAR")
        if bear_res and bear_res.get("T1"):
            return "BEAR", round(float(bear_res["T1"]), 2)
            
        # Fallback trend evaluation via 20-bar SMA comparison & Negation target
        sma20 = float(df_raw['close'].tail(20).mean())
        if latest_close >= sma20:
            trend = "BULL"
            fallback_sl = float(df_raw['low'].tail(20).min())
            t1, _, _ = find_profit_targets(df_raw, latest_close, stop_loss=fallback_sl)
            t1_val = round(float(t1), 2) if t1 else "N/A"
        else:
            trend = "BEAR"
            fallback_sl = float(df_raw['high'].tail(20).max())
            t1, _, _ = find_profit_targets_bearish(df_raw, latest_close, stop_loss=fallback_sl)
            t1_val = round(float(t1), 2) if t1 else "N/A"
            
        return trend, t1_val

    except Exception as e:
        logging.warning(f"Error evaluating spot trend for {underlying_symbol}: {e}")
        return "ERROR", "N/A"

def enrich_scan_export_csv(input_csv_path, kite=None, output_csv_path=None):
    """
    Reads an exported CSV file, evaluates underlying spot trend & T1 target for each row,
    and saves an enriched CSV file with Spot_Trend and Spot_T1_Target columns appended on the right.
    """
    if not os.path.exists(input_csv_path):
        logging.error(f"Input CSV not found: {input_csv_path}")
        return None
        
    try:
        if not kite:
            ak, at = load_kite_session()
            from kiteconnect import KiteConnect
            kite = KiteConnect(api_key=ak)
            kite.set_access_token(at)

        df = pd.read_csv(input_csv_path)
        if df.empty:
            return input_csv_path

        spot_trends = []
        spot_t1s = []

        symbol_col = "Symbol" if "Symbol" in df.columns else ("symbol" if "symbol" in df.columns else ("contract" if "contract" in df.columns else None))
        
        for idx, row in df.iterrows():
            raw_sym = str(row.get(symbol_col, "")) if symbol_col else ""
            underlying = extract_underlying_symbol(raw_sym)
            
            if underlying:
                trend, t1_val = evaluate_spot_trend_and_t1(kite, underlying)
                spot_trends.append(trend)
                spot_t1s.append(t1_val)
            else:
                spot_trends.append("N/A")
                spot_t1s.append("N/A")

        df["Spot_Trend"] = spot_trends
        df["Spot_T1_Target"] = spot_t1s

        if not output_csv_path:
            base, ext = os.path.splitext(input_csv_path)
            output_csv_path = f"{base}_enriched{ext}"

        os.makedirs(os.path.dirname(os.path.abspath(output_csv_path)), exist_ok=True)
        df.to_csv(output_csv_path, index=False)
        logging.info(f"Enriched CSV successfully saved to: {output_csv_path}")
        return output_csv_path

    except Exception as e:
        logging.error(f"Failed to enrich CSV {input_csv_path}: {e}")
        return input_csv_path

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
        out = enrich_scan_export_csv(target_file)
        print(f"Enriched file saved at: {out}")
