import os
import json
import logging
import time
import threading
import sys
import csv

COMMON_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "common"))
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

from datetime import datetime as dt, timedelta, time as datetime_time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np

import trade_db
from fyers_session import is_fyers_authenticated, get_fyers_session
from fyers_data import fetch_fyers_candles, fetch_fyers_option_chain

from trading_core import (
    load_kite_session,
    ensure_kite_session,
    log_to_journal,
    is_market_hours,
    get_weekly_expiry,
    cap_lookback_days,
    check_left_side_rule,
    find_profit_targets,
    find_profit_targets_bearish,
    calculate_position_size,
    scan_anchor_bcd_breakout,
    scan_anchor_bcd_breakout_bearish,
    find_anchor_bullish_engulfing,
    find_anchor_ll_sweep,
    find_anchor_hammer_baby,
    find_anchor_bullish_harami,
    find_anchor_two_higher_highs,
    find_anchor_bearish_engulfing,
    find_anchor_hh_sweep,
    find_anchor_shooting_star_baby,
    find_anchor_bearish_harami,
    find_anchor_two_lower_lows,
    fetch_option_data,
    fetch_and_resample_candles,
    trading_days_between,
    calc_rr,
    live_execution_enabled,
    close_position as shared_close_position,
    load_program_config_for_engine,
    sync_kite_positions as shared_sync_kite,
    write_scan_display_data as shared_write_display,
    derive_sl_targets_for_symbol,
    lookup_scan_sl_target,
    reconcile_positions as shared_reconcile,
    resolve_option_strikes as shared_resolve_strikes,
    scan_symbol,
    monitor_active_positions as shared_monitor_positions,
    simulate_trade_outcome as shared_simulate,
    is_anchor_valid_and_active,
    find_newest_valid_anchor,
    detect_parabolic_multi_swings,
    is_anchor_after_terminal_base,
    clean_liquid_candles,
    INDEX_REGISTRY
)

LIVE_MARKET_DEPLOYMENT = True
ENABLE_SWINGFILTER = True
LOOKBACK_DAYS = 30
INITIAL_CAPITAL = 100000.0
MAX_RISK_PERCENT = 2.0
TOKEN_FILE = "input/kite_access_token.txt"
SCAN_INTERVAL_SECONDS = 15

TIMEFRAME_ENTRY = "15minute"
TIMEFRAME_ANCHOR = "15minute"
TIMEFRAME_FALLBACK = "15minute"
STRIKE_RANGE = 0
BACKTEST_DATE = None

ACTIVE_POSITIONS = {}
position_lock = threading.Lock()
instrument_dump = None
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANCHOR_SCAN_REQUEST_FILE = os.path.join(BASE_DIR, "output", "monitor", "anchor_scan_request.txt")
ANCHOR_SCAN_STOP_FILE = os.path.join(BASE_DIR, "output", "monitor", "anchor_scan_stop.txt")
LIVE_EXECUTION_FLAG = os.path.join(BASE_DIR, "input", "index_spot_live.flag")
SCAN_DISPLAY_FILE = os.path.join(BASE_DIR, "output", "monitor", "scan_display_index_spot.json")
SL_TARGET_OVERRIDES_FILE = os.path.join(BASE_DIR, "output", "monitor", "sl_target_overrides.json")

journal_lock = threading.Lock()
JOURNAL_FILE = os.path.join(BASE_DIR, "output", "monitor", "trade_journal.csv")

class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        if record.levelno >= logging.WARNING or "MATCH" in record.getMessage() or "ANCHOR" in record.getMessage():
            self.flush()

LOG_FILE_PATH = os.path.join(BASE_DIR, "output", "logs", "bull_index_spot_engine.log")
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        FlushFileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info("="*60)
logging.info("  Index Spot Directional Trade Engine Initializing (v2.0)")
logging.info(f"  Underlying Spot Universe: {list(INDEX_REGISTRY.keys())}")
logging.info("="*60)


def sync_kite_positions(kite):
    shared_sync_kite(kite, INDEX_REGISTRY, ACTIVE_POSITIONS, position_lock, "index_spot", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR)


def reconcile_positions(kite):
    shared_reconcile(kite, INDEX_REGISTRY, ACTIVE_POSITIONS, position_lock, "index_spot", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR, LOOKBACK_DAYS, shared_resolve_strikes)


def monitor_active_positions(kite):
    shared_monitor_positions(kite, ACTIVE_POSITIONS, position_lock, "index_spot",
                             TIMEFRAME_ENTRY, trade_db, log_to_journal,
                             save_state_fn=lambda pos: shared_write_display([], dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "index_spot"),
                             is_stock=False)


def simulate_trade_outcome(kite, symbol, pattern, entry_price, sl_price, target_price,
                           timeframe, engine_type="index_spot", lookback_candles=100, is_stock=False):
    shared_simulate(kite, symbol, pattern, entry_price, sl_price, target_price, timeframe, engine_type, lookback_candles, is_stock)


def _process_spot_symbol(sym, cfg):
    """
    Evaluates Index Spot price action:
    1. Fetches historical candles on Index Spot (NIFTY, BANKNIFTY, NIFTYIT, SENSEX, FINNIFTY, MIDCPNIFTY).
    2. Runs Stage 0 Parabolic Multi-Swing Exhaustion Filter.
    3. Runs Stage 1 & 2 for both BULL (Call Option Trigger) and BEAR (Put Option Trigger).
    4. Resolves ATM Option Contract (CE or PE) and calculates Option Premium Entry, SL, and Target.
    """
    try:
        fyers_sym = cfg.get("fyers_symbol") or cfg.get("tradingsymbol") or sym
        df_entry = fetch_fyers_candles(fyers_sym, TIMEFRAME_ENTRY, lookback_days=min(LOOKBACK_DAYS, 10))
        df_anchor = fetch_fyers_candles(fyers_sym, TIMEFRAME_ANCHOR, lookback_days=min(LOOKBACK_DAYS, 10))
        if df_entry is None or df_anchor is None or len(df_entry) < 20:
            return None

        # Clean zero-volume bars
        df_entry = clean_liquid_candles(df_entry)
        df_anchor = clean_liquid_candles(df_anchor)
        if len(df_entry) < 15 or len(df_anchor) < 15:
            return None

        current_spot = float(df_entry["close"].iloc[-1])

        # Evaluate BULL Setup on Spot (triggers CE Option)
        bull_match = None
        bull_swing = {}
        if ENABLE_SWINGFILTER:
            bull_swing = detect_parabolic_multi_swings(df_anchor.tail(80), side="BULL", min_swings=3, max_bars_since_base=18)
            if bull_swing.get("matched", False):
                m = scan_anchor_bcd_breakout(df_entry.tail(150), df_anchor.tail(80))
                if m and is_anchor_after_terminal_base(df_anchor, m.get("CandleATime", m.get("CandleTime")), bull_swing):
                    bull_match = m

        # Evaluate BEAR Setup on Spot (triggers PE Option)
        bear_match = None
        bear_swing = {}
        if ENABLE_SWINGFILTER:
            bear_swing = detect_parabolic_multi_swings(df_anchor.tail(80), side="BEAR", min_swings=3, max_bars_since_base=18)
            if bear_swing.get("matched", False):
                m = scan_anchor_bcd_breakout_bearish(df_entry.tail(150), df_anchor.tail(80))
                if m and is_anchor_after_terminal_base(df_anchor, m.get("CandleATime", m.get("CandleTime")), bear_swing):
                    bear_match = m

        matched_side = None
        m = None
        swing_struct = {}
        if bull_match and not bear_match:
            matched_side = "CE"
            m = bull_match
            swing_struct = bull_swing
        elif bear_match and not bull_match:
            matched_side = "PE"
            m = bear_match
            swing_struct = bear_swing
        elif bull_match and bear_match:
            # Pick highest RR
            if float(bull_match.get("RR", 0)) >= float(bear_match.get("RR", 0)):
                matched_side = "CE"
                m = bull_match
                swing_struct = bull_swing
            else:
                matched_side = "PE"
                m = bear_match
                swing_struct = bear_swing

        if not m or not matched_side:
            return None

        # Resolve ATM Option Contract for the matched side
        chain = fetch_fyers_option_chain(sym, strikecount=max(STRIKE_RANGE * 2 + 1, 3))
        target_opts = [o for o in chain if o.get("option_type") == matched_side]
        if not target_opts:
            return None

        # Pick ATM strike closest to spot
        best_opt = min(target_opts, key=lambda o: abs(o.get("strike", 0) - current_spot))
        opt_sym = best_opt.get("symbol")
        contract_name = opt_sym.replace("NSE:", "").replace("BSE:", "")
        opt_ltp = float(best_opt.get("ltp", 0))

        # Spot levels
        spot_cl = float(m.get("Close", current_spot))
        spot_sl = float(m.get("SL", current_spot * 0.99))
        spot_t1 = float(m.get("T1", current_spot * 1.01))
        spot_t2 = float(m.get("T2", 0)) if m.get("T2") else None
        spot_t3 = float(m.get("T3", 0)) if m.get("T3") else None
        spot_diff = abs(spot_cl - spot_sl)

        # Delta estimate: ATM ~ 0.50 delta
        delta = float(best_opt.get("delta", 0.50)) or 0.50
        if delta < 0.2:
            delta = 0.50

        # Projected Option Entry, SL, Targets
        opt_entry = round(opt_ltp if opt_ltp > 0 else (spot_diff * delta), 2)
        opt_sl_diff = round(spot_diff * delta, 2)
        opt_sl = round(max(opt_entry - opt_sl_diff, 0.05), 2)
        opt_t1 = round(opt_entry + abs(spot_t1 - spot_cl) * delta, 2)
        opt_t2 = round(opt_entry + abs(spot_t2 - spot_cl) * delta, 2) if spot_t2 else None
        opt_t3 = round(opt_entry + abs(spot_t3 - spot_cl) * delta, 2) if spot_t3 else None

        rr = round(calc_rr(opt_entry, opt_sl, opt_t1, opt_t2, opt_t3), 2)

        tier = swing_struct.get("tier", 2) if ENABLE_SWINGFILTER else 2
        tier_label = swing_struct.get("tier_label", "TIER_2_CORE") if ENABLE_SWINGFILTER else "N/A"
        tier_badge = swing_struct.get("tier_badge", "🥈 T2") if ENABLE_SWINGFILTER else ""
        risk_scale = swing_struct.get("risk_scale", 1.0) if ENABLE_SWINGFILTER else 1.0

        pos_size = 1
        try:
            risk_amount = (INITIAL_CAPITAL * (MAX_RISK_PERCENT / 100.0)) * risk_scale
            lot_sz = cfg.get("lot_size", 1)
            sl_diff_val = abs(opt_entry - opt_sl)
            if sl_diff_val > 0 and lot_sz > 0:
                pos_size = max(1, int(risk_amount / (sl_diff_val * lot_sz)))
        except Exception:
            pos_size = 1

        waves = swing_struct.get("valid_arch_count", 0) if ENABLE_SWINGFILTER else 0
        has_abs = swing_struct.get("has_terminal_base", False) if ENABLE_SWINGFILTER else False
        para_badge = f"{tier_badge} {waves}S{'+Abs' if has_abs else ''}".strip() if ENABLE_SWINGFILTER else "N/A"

        trade = {
            "symbol": sym,
            "contract": contract_name,
            "fyers_symbol": opt_sym,
            "pattern": m.get("Pattern"),
            "parabolic_score": para_badge,
            "parabolic_waves": waves,
            "terminal_base": has_abs,
            "tier": tier,
            "tier_label": tier_label,
            "tier_badge": tier_badge,
            "risk_scale": risk_scale,
            "side": matched_side,
            "timeframe": TIMEFRAME_ENTRY,
            "strike": best_opt.get("strike"),
            "entry_spot": opt_entry,
            "current_sl": opt_sl,
            "t1": opt_t1,
            "t2": opt_t2,
            "t3": opt_t3,
            "spot_entry": round(spot_cl, 2),
            "spot_sl": round(spot_sl, 2),
            "spot_t1": round(spot_t1, 2),
            "rr": rr,
            "entry_time": m.get("CandleTime"),
            "anchor_time": m.get("CandleATime"),
            "lot_size": cfg.get("lot_size", 1),
            "position_size": pos_size,
            "status": "STAGED",
            "feed_source": "FYERS_INDEX_SPOT"
        }
        logging.info(f"SPOT DIRECTIONAL MATCH: {sym} ({matched_side}) -> {contract_name} | {m.get('Pattern')} | {tier_badge} | Spot: {spot_cl:.2f} | Opt Entry: {opt_entry:.2f} | SL: {opt_sl:.2f} | T1: {opt_t1} | RR: {rr}")
        return trade
    except Exception as e:
        logging.error(f"Error scanning Index Spot symbol {sym}: {e}")
        return None


def run_cycle(kite=None, log_scan_ready=True):
    """Main scanning cycle across all Index Spot charts."""
    global TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR, LOOKBACK_DAYS, MAX_RISK_PERCENT, INITIAL_CAPITAL, ENABLE_SWINGFILTER

    cfg = load_program_config_for_engine("index_spot")
    if cfg:
        TIMEFRAME_ENTRY = cfg.get("timeframe_entry", TIMEFRAME_ENTRY)
        TIMEFRAME_ANCHOR = cfg.get("timeframe_anchor", TIMEFRAME_ANCHOR)
        LOOKBACK_DAYS = cfg.get("lookback_days", LOOKBACK_DAYS)
        MAX_RISK_PERCENT = cfg.get("risk_percent", MAX_RISK_PERCENT)
        INITIAL_CAPITAL = cfg.get("capital", INITIAL_CAPITAL)
        ENABLE_SWINGFILTER = cfg.get("enable_swingfilter", ENABLE_SWINGFILTER)

    temp_stored_trades = []
    logging.info(f"[INDEX_SPOT] Scanning Index Spot Charts ({list(INDEX_REGISTRY.keys())}) on {TIMEFRAME_ENTRY}...")

    tasks = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for sym, cfg in INDEX_REGISTRY.items():
            with position_lock:
                if sym in ACTIVE_POSITIONS:
                    continue
            tasks.append(pool.submit(_process_spot_symbol, sym, cfg))
            time.sleep(0.08)

        for fut in as_completed(tasks):
            res = fut.result()
            if res:
                temp_stored_trades.append(res)

    logging.info(f"[INDEX_SPOT] Staging {len(temp_stored_trades)} directional index option trades.")
    shared_write_display(temp_stored_trades, dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "index_spot")

    staged = temp_stored_trades
    try:
        if hasattr(trade_db, "store_cycle_trades"):
            trade_db.store_cycle_trades("index_spot", staged or [])
        elif hasattr(trade_db, "stage_cycle_trade"):
            trade_db.clear_cycle_trades("index_spot")
            for t in (staged or []):
                trade_db.stage_cycle_trade("index_spot", t)
    except Exception as e:
        logging.warning(f"Failed to record cycle trades in DB: {e}")

    if not staged:
        return

    # Select best risk-reward candidate
    best = max(staged, key=lambda t: (t.get("t3") or t.get("t1") or 0) - t.get("entry_spot", 0))
    key = f"{best['symbol']}_{best.get('side', 'CE')}_{best.get('entry_time', '')}"

    if not trade_db.is_pattern_executed("index_spot", key):
        if not live_execution_enabled(LIVE_EXECUTION_FLAG):
            trade_db.record_executed_pattern("index_spot", key, {"contract": best["contract"], "entry": best["entry_spot"]})
            profit = round((best.get("t3") or best.get("t1") or 0) - best["entry_spot"], 2)
            rr_best = best.get("rr", 0.0)
            log_to_journal(best["contract"], best["pattern"], TIMEFRAME_ENTRY,
                           "SIMULATE_EXIT", "PROFIT" if profit > 0 else "LOSS",
                           f"Exit at {best.get('t1')} ({profit:+.2f} Rs, RR={rr_best})",
                           entry=best["entry_spot"], sl=best["current_sl"], target=best.get("t1", ""), rr=rr_best,
                           pnl=profit)
            time.sleep(0.5)
            log_to_journal(best["contract"], best["pattern"], TIMEFRAME_ENTRY,
                           "SCAN_MATCH", "MATCHED",
                           f"Stage 2 BCD Breakout confirmed on Spot -> {best['contract']}",
                           entry=best["entry_spot"], sl=best["current_sl"], target=best.get("t1", ""), rr=rr_best,
                           pnl=0.0)
            logging.info(f"Simulated execution recorded for {best['contract']} (RR={rr_best})")
        else:
            logging.info(f"LIVE EXECUTION TRIGGERED: {best['contract']} | Entry: {best['entry_spot']} | SL: {best['current_sl']}")
            with position_lock:
                ACTIVE_POSITIONS[best["symbol"]] = {
                    "symbol": best["symbol"],
                    "contract": best["contract"],
                    "entry_spot": best["entry_spot"],
                    "current_sl": best["current_sl"],
                    "t1": best.get("t1"),
                    "t2": best.get("t2"),
                    "t3": best.get("t3"),
                    "quantity": best.get("position_size", 1) * best.get("lot_size", 1),
                    "status": "ACTIVE",
                    "side": best.get("side", "CE"),
                    "timeframe": TIMEFRAME_ENTRY
                }
            shared_write_display(staged or [], dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "index_spot")


def main():
    logging.info("Starting Index Spot Directional Trade Engine daemon...")
    kite = None
    try:
        kite = load_kite_session(TOKEN_FILE)
    except Exception:
        pass

    while True:
        try:
            run_cycle(kite)
            time.sleep(SCAN_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logging.info("Index Spot Engine stopped by user.")
            break
        except Exception as e:
            logging.error(f"Error in Index Spot Engine main loop: {e}", exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    main()
