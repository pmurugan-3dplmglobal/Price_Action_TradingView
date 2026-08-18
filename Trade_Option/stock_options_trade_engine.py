import os
import json
import logging
import time
import threading
import sys
COMMON_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "common"))
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime as dt, timedelta, time as datetime_time
import pandas as pd
import numpy as np

import trade_db

from trading_core import (
    load_kite_session,
    ensure_kite_session,
    log_to_journal,
    is_market_hours,
    cap_lookback_days,
    check_left_side,
    find_profit_targets,
    calculate_position_size,
    scan_anchor_bcd_breakout,
    find_anchor_bullish_engulfing,
    find_anchor_ll_sweep,
    find_anchor_hammer_baby,
    find_anchor_bullish_harami,
    find_anchor_two_higher_highs,
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
    derive_sl_targets_for_contract,
    lookup_scan_sl_target,
    reconcile_positions as shared_reconcile,
    resolve_option_strikes as shared_resolve_strikes,
    scan_symbol,
    monitor_active_positions as shared_monitor_positions,
    simulate_trade_outcome as shared_simulate,
    is_anchor_valid_and_active,
    find_newest_valid_anchor,
    STOCK_REGISTRY,
    SUPER_STOCKS
)

LIVE_MARKET_DEPLOYMENT = True
LOOKBACK_DAYS = 30
INITIAL_CAPITAL = 100000.0
MAX_RISK_PERCENT = 1.0
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(BASE_DIR, "input", "kite_access_token.txt")
STATE_FILE = os.path.join(BASE_DIR, "output", "monitor", "stock_positions_state.json")
SCAN_INTERVAL_SECONDS = 300
STRIKE_RANGE = 0

TIMEFRAME_ENTRY = "15minute"
TIMEFRAME_ANCHOR = "30minute"
BACKTEST_DATE = None

ACTIVE_POSITIONS = {}
position_lock = threading.Lock()
NFO_INSTRUMENTS = pd.DataFrame()
instruments_lock = threading.Lock()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANCHOR_SCAN_REQUEST_FILE = os.path.join(BASE_DIR, "output", "monitor", "anchor_scan_request.txt")
ANCHOR_SCAN_STOP_FILE = os.path.join(BASE_DIR, "output", "monitor", "anchor_scan_stop.txt")
LIVE_EXECUTION_FLAG = os.path.join(BASE_DIR, "input", "nifty50_live.flag")
SCAN_DISPLAY_FILE = os.path.join(BASE_DIR, "output", "monitor", "scan_display_data.json")
SL_TARGET_OVERRIDES_FILE = os.path.join(BASE_DIR, "output", "monitor", "sl_target_overrides.json")

journal_lock = threading.Lock()
JOURNAL_FILE = os.path.join(BASE_DIR, "output", "monitor", "trade_journal.csv")

class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        if record.levelno >= logging.WARNING or "MATCH" in record.getMessage() or "ANCHOR" in record.getMessage():
            self.flush()

LOG_FILE_PATH = os.path.join(BASE_DIR, "output", "logs", "bull_nifty50_scanner.log")
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        FlushFileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def save_state():
    with position_lock:
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(ACTIVE_POSITIONS, f, indent=4)
        except Exception as e:
            logging.error(f"State save failed: {e}")

def load_state():
    global ACTIVE_POSITIONS
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                ACTIVE_POSITIONS = json.load(f)
            logging.info(f"Loaded {len(ACTIVE_POSITIONS)} stored active positions from local database")
        except Exception:
            ACTIVE_POSITIONS = {}

NFO_CACHE_FILE = os.path.join(os.path.dirname(BASE_DIR), "output", "monitor", "nfo_instruments_cache.csv")

def sync_instruments(kite):
    if not kite or not hasattr(kite, "instruments"):
        _load_cached_nfo()
        return
    def _do_sync():
        global NFO_INSTRUMENTS
        instr = kite.instruments("NSE")
        df = pd.DataFrame(instr)
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
        nfo = kite.instruments("NFO")
        try:
            bfo = kite.instruments("BFO")
        except Exception:
            bfo = []
        combined = (nfo if nfo else []) + (bfo if bfo else [])
        with instruments_lock:
            NFO_INSTRUMENTS = pd.DataFrame(combined)
            if not NFO_INSTRUMENTS.empty:
                NFO_INSTRUMENTS['name'] = NFO_INSTRUMENTS['name'].str.strip().str.upper()
                NFO_INSTRUMENTS['instrument_type'] = NFO_INSTRUMENTS['instrument_type'].str.strip().str.upper()
                logging.info(f"Synced {len(NFO_INSTRUMENTS)} NFO/BFO contracts")
                os.makedirs(os.path.dirname(NFO_CACHE_FILE), exist_ok=True)
                NFO_INSTRUMENTS.to_csv(NFO_CACHE_FILE, index=False)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(_do_sync)
        future.result(timeout=90)
    except TimeoutError:
        logging.warning("Instrument sync timed out after 90s, trying cached NFO data")
        _load_cached_nfo()
    except Exception as e:
        logging.error(f"Instrument sync failed: {e}")
        _load_cached_nfo()
    finally:
        pool.shutdown(wait=False)

def _load_cached_nfo():
    global NFO_INSTRUMENTS
    candidates = [
        NFO_CACHE_FILE,
        os.path.join(BASE_DIR, "output", "monitor", "nfo_instruments_cache.csv"),
        os.path.join(os.path.dirname(BASE_DIR), "Trade_Option", "output", "monitor", "nfo_instruments_cache.csv"),
        os.path.join("output", "monitor", "nfo_instruments_cache.csv")
    ]
    for c in candidates:
        if os.path.exists(c):
            try:
                df = pd.read_csv(c)
                if not df.empty:
                    with instruments_lock:
                        NFO_INSTRUMENTS = df
                    logging.info(f"Loaded {len(NFO_INSTRUMENTS)} NFO contracts from cache ({c})")
                    return
            except Exception as e:
                logging.warning(f"Failed to load cached NFO from {c}: {e}")

# ──────────────────────────────────────────────
#  OPTION CONTRACT RESOLUTION
# ──────────────────────────────────────────────

def resolve_option_contract(symbol, spot, step, opt_type, target_strike=None):
    with instruments_lock:
        if NFO_INSTRUMENTS.empty:
            s = target_strike or int(round(spot / step) * step)
            return f"{symbol}{dt.now().strftime('%y%b').upper()}{s}{opt_type}"
        try:
            m = NFO_INSTRUMENTS[
                (NFO_INSTRUMENTS['name'] == symbol.strip().upper()) &
                (NFO_INSTRUMENTS['instrument_type'] == opt_type.upper())
            ].copy()
            if m.empty:
                return None
            m['strike'] = m['strike'].astype(float)
            target = target_strike or round(spot / step) * step
            sub = m[m['strike'] == float(target)].copy()
            if sub.empty:
                idx = (m['strike'] - spot).abs().idxmin()
                sel = m.loc[idx]
            else:
                sub['expiry_dt'] = pd.to_datetime(sub['expiry']).dt.date
                today = dt.now().date()
                future = sub[sub['expiry_dt'] >= today].sort_values(by='expiry_dt')
                if not future.empty:
                    expiries = future['expiry_dt'].unique()
                    curr_exp = expiries[0]
                    days_rem = (curr_exp - today).days
                    # 85% Threshold Rule: If <= 4 days remaining to monthly expiry, select NEXT MONTH
                    if days_rem <= 4 and len(expiries) > 1:
                        target_exp = expiries[1]
                        logging.info(f"[STOCK EXPIRY ROLLOVER 85%] {symbol}: {days_rem}d to expiry ({curr_exp}) -> Selected NEXT MONTH ({target_exp})")
                        sel = future[future['expiry_dt'] == target_exp].iloc[0]
                    else:
                        sel = future.iloc[0]
                else:
                    sel = sub.iloc[0] if not sub.empty else m.iloc[0]
            return str(sel['tradingsymbol'])
        except Exception as e:
            logging.error(f"Option resolve error for {symbol}: {e}")
            s = target_strike or int(round(spot / step) * step)
            return f"{symbol}{dt.now().strftime('%y%b').upper()}{s}{opt_type}"

def resolve_option_strikes(symbol, spot_price, step_size, option_type, n_range):
    with instruments_lock:
        return shared_resolve_strikes(NFO_INSTRUMENTS, symbol, spot_price, step_size, option_type, n_range)


# ──────────────────────────────────────────────
#  EXECUTION FUNCTIONS
# ──────────────────────────────────────────────

def close_position(kite, pos):
    return shared_close_position(kite, pos, LIVE_MARKET_DEPLOYMENT, getattr(kite, "PRODUCT_NRML", "NRML"))

def _derive_sl_targets_for_symbol(kite, symbol, entry_price):
    return derive_sl_targets_for_symbol(kite, symbol, entry_price, STOCK_REGISTRY, TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR, LOOKBACK_DAYS, lambda sym, sp, step, opt, r: resolve_option_strikes(sym, sp, step, opt, r))

def reconcile_positions(kite):
    shared_reconcile(kite, STOCK_REGISTRY, ACTIVE_POSITIONS, position_lock, "nifty50", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR, LOOKBACK_DAYS, lambda sym, sp, step, opt, r: resolve_option_strikes(sym, sp, step, opt, r), save_state)

# ──────────────────────────────────────────────
#  SCAN CYCLE — RUNS EVERY N SECONDS
# ──────────────────────────────────────────────

def _process_stock(kite, symbol, config, from_entry, to_entry, from_anchor, to_anchor, entry_scanners, anchor_scanners):
    return scan_symbol(kite, symbol, config, from_entry, to_entry, from_anchor, to_anchor,
                       entry_scanners, anchor_scanners,
                       lambda sym, sp, step, opt, r: shared_resolve_strikes(NFO_INSTRUMENTS, sym, sp, step, opt, r),
                       "nifty50", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR, TIMEFRAME_ENTRY,
                       ACTIVE_POSITIONS, position_lock, trade_db, STRIKE_RANGE,
                       log_to_journal)


def run_scan_cycle(kite):
    if NFO_INSTRUMENTS.empty:
        sync_instruments(kite)
    cfg_applied = load_program_config_for_engine("nifty50", [("strike_range", "STRIKE_RANGE")])
    for k, v in cfg_applied.items():
        if k == "STRIKE_RANGE": globals()["STRIKE_RANGE"] = int(v) if isinstance(v, (int, float)) else v
        elif k in ("TIMEFRAME_ENTRY", "TIMEFRAME_ANCHOR"): globals()[k] = v
        elif k == "LIVE_MARKET_DEPLOYMENT": globals()["LIVE_MARKET_DEPLOYMENT"] = v
        elif k == "LOOKBACK_DAYS": globals()["LOOKBACK_DAYS"] = int(v)
        elif k == "SCAN_INTERVAL_SECONDS": globals()["SCAN_INTERVAL_SECONDS"] = int(v)
        elif k == "MAX_RISK_PERCENT": globals()["MAX_RISK_PERCENT"] = float(v)
        elif k == "INITIAL_CAPITAL": globals()["INITIAL_CAPITAL"] = float(v)

    target_date = BACKTEST_DATE
    if target_date is None:
        ref_now = dt.now()
    else:
        ref_now = target_date
    limits = {"minute": 60, "3minute": 100, "5minute": 100, "10minute": 100, "15minute": 200, "30minute": 200, "60minute": 400, "75minute": 400, "75min": 400, "day": 2000}
    max_days_entry = limits.get(TIMEFRAME_ENTRY, 180)
    max_days_anchor = limits.get(TIMEFRAME_ANCHOR, 180)
    from_entry = (ref_now - timedelta(days=min(LOOKBACK_DAYS, max_days_entry))).strftime("%Y-%m-%d")
    to_entry = ref_now.strftime("%Y-%m-%d")
    from_anchor = (ref_now - timedelta(days=min(LOOKBACK_DAYS, max_days_anchor))).strftime("%Y-%m-%d")
    to_anchor = ref_now.strftime("%Y-%m-%d")

    entry_scanners = [
        ("Setup_1_Anchor_BCD", scan_anchor_bcd_breakout),
    ]
    anchor_scanners = [
        ("A1", find_anchor_bullish_engulfing),
        ("A2", find_anchor_ll_sweep),
        ("A3", find_anchor_hammer_baby),
        ("A4", find_anchor_bullish_harami),
        ("A5", find_anchor_two_higher_highs),
    ]

    scan_order = sorted(STOCK_REGISTRY.keys())
    temp_stored_trades = []

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {}
        for symbol in scan_order:
            config = STOCK_REGISTRY[symbol]
            with position_lock:
                if symbol in ACTIVE_POSITIONS:
                    continue
            futures[pool.submit(_process_stock, kite, symbol, config,
                from_entry, to_entry, from_anchor, to_anchor,
                entry_scanners, anchor_scanners)] = symbol

        for f in as_completed(futures):
            symbol = futures[f]
            try:
                result = f.result()
                if result:
                    temp_stored_trades.extend(result)
            except Exception as e:
                logging.error(f"Error processing {symbol}: {e}")

    with position_lock:
        shared_write_display(temp_stored_trades, dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "nifty50")

    if not temp_stored_trades:
        logging.info("No new trades meet criteria this cycle.")
    return temp_stored_trades

def _avg_target_rank(trade):
    targets = [t for t in [trade.get("t1"), trade.get("t2"), trade.get("t3")] if t]
    if not targets:
        return 0
    avg_target = sum(targets) / len(targets)
    risk = trade.get("entry_spot", 0) - trade.get("current_sl", 0)
    if risk <= 0:
        return 0
    return (avg_target - trade["entry_spot"]) / risk

def execute_highest_rr_trade(kite, staged):
    """After a scan cycle, pick best by avg RR and execute (if live)."""
    if not staged:
        return
    best = max(staged, key=_avg_target_rank)
    sym = best["symbol"]
    side = best.get("side", "CE")
    strike = best.get("strike", "")
    key = f"{sym}|{best['pattern']}|{side}|{strike}"
    if trade_db.is_pattern_executed("nifty50", key):
        logging.info(f"Best cycle trade {key} already executed; skipping")
        return
    cp = best["entry_spot"]
    strike_step = best.get("strike_step", 50)
    pos_size = calculate_position_size(cp, best["current_sl"])
    target_strike = strike if strike else int(round(cp / strike_step) * strike_step)
    opt_type = "CE" if side == "CE" else "PE"
    contract = resolve_option_contract(sym, cp, strike_step, opt_type, target_strike)
    if not contract:
        logging.error(f"Could not resolve option for {sym}")
        return
    option_token = _resolve_option_token(contract)
    live_ok = LIVE_MARKET_DEPLOYMENT and live_execution_enabled(LIVE_EXECUTION_FLAG) and is_market_hours()
    if live_ok:
        with position_lock:
            if sym in ACTIVE_POSITIONS:
                logging.info(f"TF Entry: {TIMEFRAME_ENTRY} | Anchor: {TIMEFRAME_ANCHOR} | Interval: {SCAN_INTERVAL_SECONDS}s | Risk: {MAX_RISK_PERCENT}%")
                logging.info(f"{sym} already active; skipping new trade")
                return
            pos = {
                "contract": contract, "option_token": option_token,
                "entry_spot": cp, "current_sl": best["current_sl"],
                "t1": best["t1"], "t2": best["t2"], "t3": best["t3"],
                "trailing_stage": 0, "lot_size": best["lot_size"], "position_size": pos_size,
                "pattern": best["pattern"], "timeframe": TIMEFRAME_ENTRY,
                "side": opt_type, "strike": target_strike,
                "entry_time": dt.now().isoformat(),
                "position_type": "option"
            }
            pos["trade_id"] = trade_db.create_trade("nifty50", sym, {k: v for k, v in pos.items() if k != "trade_id"})
            ACTIVE_POSITIONS[sym] = pos
        save_state()
    avg_rr = round(_avg_target_rank(best), 2)
    acc_token = getattr(kite, "access_token", "") if kite else ""
    if live_ok:
        if acc_token and acc_token != "open_source_token":
            try:
                q = kite.quote(f"{kite.EXCHANGE_NFO}:{contract}")
                ltp = q[f"{kite.EXCHANGE_NFO}:{contract}"]["last_price"]
                ask = q[f"{kite.EXCHANGE_NFO}:{contract}"]["depth"]["sell"][0]["price"]
                price = round((ask if ask > 0 else ltp) * 1.005, 1)
                qty = best["lot_size"] * pos_size
                oid = kite.place_order(
                    variety=getattr(kite, "VARIETY_REGULAR", "regular"), tradingsymbol=contract,
                    exchange=getattr(kite, "EXCHANGE_NFO", "NFO"), transaction_type=getattr(kite, "TRANSACTION_TYPE_BUY", "BUY"),
                    quantity=qty, order_type=getattr(kite, "ORDER_TYPE_LIMIT", "LIMIT"), price=price,
                    product=getattr(kite, "PRODUCT_NRML", "NRML")
                )
                log_to_journal(sym, best["pattern"], TIMEFRAME_ENTRY, "BUY", "SUCCESS",
                               f"Order: {oid}, Qty: {qty}, {opt_type}@{target_strike}", entry=cp, sl=best["current_sl"], target=best["t1"], rr=avg_rr,
                               event_time=best.get("entry_time"))
            except Exception as e:
                log_to_journal(sym, best["pattern"], TIMEFRAME_ENTRY, "BUY", "FAILED", str(e),
                               entry=cp, sl=best["current_sl"], target=best["t1"],
                               event_time=best.get("entry_time"))
                with position_lock:
                    ACTIVE_POSITIONS.pop(sym, None)
                save_state()
                return
        else:
            qty = best["lot_size"] * pos_size
            log_to_journal(sym, best["pattern"], TIMEFRAME_ENTRY, "BUY", "SUCCESS",
                           f"Paper Order: SIM_{dt.now().strftime('%H%M%S')}, Qty: {qty}, {opt_type}@{target_strike}", entry=cp, sl=best["current_sl"], target=best["t1"], rr=avg_rr,
                           event_time=best.get("entry_time"))
    elif BACKTEST_DATE is not None:
        log_to_journal(sym, best["pattern"], TIMEFRAME_ENTRY, "BACKTEST_BEST", "SUCCESS",
                       f"Contract: {contract}, Size: {pos_size}, {opt_type}@{target_strike}", entry=cp, sl=best["current_sl"], target=best["t1"],
                       event_time=best.get("entry_time"))
        sim = simulate_trade_outcome(kite, best, BACKTEST_DATE)
        if sim["result"]:
            log_to_journal(sym, best["pattern"], TIMEFRAME_ENTRY,
                           sim["result"], "COMPLETED", sim["detail"],
                           entry=cp, sl=best["current_sl"], target=best.get("t1",""), rr=avg_rr,
                           event_time=sim.get("exit_time") or sim.get("entry_time"))
            logging.info(f"[BACKTEST] Trade outcome: {sim['result']} | {sim['detail']} | P&L: {sim['pnl_pct']}%")
    else:
        log_to_journal(sym, best["pattern"], TIMEFRAME_ENTRY, "SCAN_READY", "SUCCESS",
                       f"Contract: {contract}, Size: {pos_size}, {opt_type}@{target_strike} | Manual entry pending", entry=cp, sl=best["current_sl"], target=best["t1"],
                       event_time=best.get("entry_time"))
        logging.info(f"SCAN_READY best trade: {sym} {contract} | Entry: {cp} | SL: {best['current_sl']} | T1: {best.get('t1','')}")
        targets = [t for t in [best.get("t1"), best.get("t2"), best.get("t3")] if t]
        avg_target = sum(targets) / len(targets) if targets else 0
        logging.info(f"SCAN_READY best cycle trade: {sym} | {best['pattern']} | avg-target={avg_target:.2f} | avg-RR={avg_rr}")
        return
    trade_db.record_executed_pattern("nifty50", key, {"contract": contract, "entry": cp})
    targets = [t for t in [best.get("t1"), best.get("t2"), best.get("t3")] if t]
    avg_target = sum(targets) / len(targets) if targets else 0
    logging.info(f"EXECUTED best cycle trade: {sym} | {best['pattern']} | avg-target={avg_target:.2f} | avg-RR={avg_rr}")

# ──────────────────────────────────────────────
#  ANCHOR SCAN — RUNS ON DEMAND VIA DASHBOARD
# ──────────────────────────────────────────────

def run_anchor_scan(kite):
    logging.info("On-demand scan requested: executing full A-B-C-D breakout scan across Nifty 50 option contracts...")
    staged = run_scan_cycle(kite)
    with position_lock:
        shared_write_display(staged or [], dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "nifty50")
    logging.info(f"On-demand scan complete: found {len(staged or [])} full A-B-C-D breakout setup(s)")

# ──────────────────────────────────────────────
#  POSITION MONITORING — SL, TRAILING, TARGETS
# ──────────────────────────────────────────────

def monitor_active_positions(kite):
    return shared_monitor_positions(kite, STOCK_REGISTRY, ACTIVE_POSITIONS, position_lock,
                                     getattr(kite, "PRODUCT_NRML", "NRML"), "nifty50", TIMEFRAME_ENTRY,
                                     trade_db, log_to_journal, save_state,
                                     live=LIVE_MARKET_DEPLOYMENT)

def position_monitor_loop(kite):
    """Background thread that checks stop-loss, trailing, and targets every 60s."""
    while True:
        try:
            ensure_kite_session(kite)
            monitor_active_positions(kite)
        except Exception as e:
            logging.error(f"Position monitor error: {e}")
        time.sleep(60)

# ──────────────────────────────────────────────
#  DISPLAY DATA WRITER + KITE SYNC
# ──────────────────────────────────────────────



def write_scan_display_data(staged, active, display_file=SCAN_DISPLAY_FILE, engine_name="nifty50"):
    return shared_write_display(staged, active, display_file, engine_name)

def _sync_kite_positions(kite):
    return shared_sync_kite(kite, STOCK_REGISTRY, ACTIVE_POSITIONS, position_lock, "nifty50", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR)

# ──────────────────────────────────────────────
#  MAIN LOOP — SCAN CYCLE + ANCHOR POLL
# ──────────────────────────────────────────────

def main_scan_loop(kite):
    _sync_counter = 0
    _cycle_count = 0
    while True:
        try:
            ensure_kite_session(kite)
            _sync_counter += 1
            if _sync_counter % 5 == 0 and not BACKTEST_DATE:
                shared_sync_kite(kite, STOCK_REGISTRY, ACTIVE_POSITIONS, position_lock, "nifty50", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR)
            if os.path.exists(SL_TARGET_OVERRIDES_FILE):
                try:
                    with open(SL_TARGET_OVERRIDES_FILE) as f:
                        overrides = json.load(f)
                    eng_overrides = overrides.get("nifty50", {})
                    if eng_overrides:
                        with position_lock:
                            for sym, vals in eng_overrides.items():
                                target_pos = None
                                if sym in ACTIVE_POSITIONS:
                                    target_pos = ACTIVE_POSITIONS[sym]
                                else:
                                    best_pos = None
                                    best_len = -1
                                    for k, p in ACTIVE_POSITIONS.items():
                                        p_contract = str(p.get("contract") or "").replace(" ", "").upper()
                                        p_symbol = str(p.get("symbol") or "").replace(" ", "").upper()
                                        k_clean = str(k).replace(" ", "").upper()
                                        if p_contract == sym or p_symbol == sym or k_clean == sym:
                                            best_pos = p
                                            break
                                        if sym in p_contract and len(p_contract) > best_len:
                                            best_pos = p
                                            best_len = len(p_contract)
                                        elif sym in p_symbol and len(p_symbol) > best_len:
                                            best_pos = p
                                            best_len = len(p_symbol)
                                    target_pos = best_pos
                                if target_pos:
                                    changed = False
                                    for key in ("current_sl", "t1", "t2", "t3"):
                                        if key in vals:
                                            target_pos[key] = vals[key]
                                            changed = True
                                    if changed:
                                        tid = target_pos.get("trade_id")
                                        if tid:
                                            trade_db.update_trade(tid, {k: target_pos[k] for k in ("current_sl", "t1", "t2", "t3") if k in target_pos})
                                        logging.info(f"[OVERRIDE] Applied SL/T for {target_pos.get('contract', sym)}: SL={target_pos.get('current_sl')} T1={target_pos.get('t1')} T2={target_pos.get('t2')} T3={target_pos.get('t3')}")
                        save_state()
                except Exception as e:
                    logging.warning(f"Override apply failed: {e}")
            logging.info("[BEAT] Starting Nifty 50 scan cycle...")
            if os.path.exists(ANCHOR_SCAN_REQUEST_FILE):
                try:
                    with open(ANCHOR_SCAN_REQUEST_FILE) as f:
                        engine = f.read().strip()
                    os.remove(ANCHOR_SCAN_REQUEST_FILE)
                    if engine != "nifty50":
                        logging.info(f"Anchor scan flag not for nifty50, skipping (got {engine})")
                    else:
                        logging.info(f"Anchor scan requested via flag file (engine: {engine})")
                        run_anchor_scan(kite)
                except Exception:
                    pass
            start = time.time()
            staged = run_scan_cycle(kite)
            if staged:
                execute_highest_rr_trade(kite, staged)
            else:
                logging.info("[CYCLE] No trades staged this cycle.")
            trade_db.clear_cycle_trades("nifty50")
            with position_lock:
                shared_write_display(staged or [], dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "nifty50")
            _cycle_count += 1
            elapsed = time.time() - start
            sleep = max(0, SCAN_INTERVAL_SECONDS - elapsed)
            logging.info(f"[CYCLE COMPLETE] {_cycle_count} cycle complete in {elapsed:.2f}s | Found {len(staged or [])} setup(s)")
            time.sleep(sleep)
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(10)

def load_program_config():
    cfg_applied = load_program_config_for_engine("nifty50", [("strike_range", "STRIKE_RANGE")])
    for k, v in cfg_applied.items():
        if k == "STRIKE_RANGE": globals()["STRIKE_RANGE"] = int(v) if isinstance(v, (int, float)) else v
        elif k in ("TIMEFRAME_ENTRY", "TIMEFRAME_ANCHOR"): globals()[k] = v
        elif k == "LIVE_MARKET_DEPLOYMENT": globals()["LIVE_MARKET_DEPLOYMENT"] = v
        elif k == "LOOKBACK_DAYS": globals()["LOOKBACK_DAYS"] = int(v)
        elif k == "SCAN_INTERVAL_SECONDS": globals()["SCAN_INTERVAL_SECONDS"] = int(v)
        elif k == "MAX_RISK_PERCENT": globals()["MAX_RISK_PERCENT"] = float(v)
        elif k == "INITIAL_CAPITAL": globals()["INITIAL_CAPITAL"] = float(v)


def _resolve_option_token(contract_symbol):
    with instruments_lock:
        if NFO_INSTRUMENTS.empty:
            return None
        m = NFO_INSTRUMENTS[NFO_INSTRUMENTS['tradingsymbol'] == contract_symbol]
        if m.empty:
            return None
        return int(m.iloc[0]['instrument_token'])

def simulate_trade_outcome(kite, trade, target_date):
    return shared_simulate(kite, trade, target_date)

def run_multi_day_backtest(kite, start_date, end_date):
    global BACKTEST_DATE, LIVE_MARKET_DEPLOYMENT
    LIVE_MARKET_DEPLOYMENT = False
    days = trading_days_between(start_date, end_date)
    logging.info(f"Multi-day backtest: {len(days)} trading days from {start_date} to {end_date}")
    results = {"total_days": len(days), "days_with_trades": 0, "total_trades": 0, "wins": 0, "losses": 0, "no_exits": 0, "by_symbol": {}}
    for idx, day in enumerate(days):
        BACKTEST_DATE = day
        logging.info(f"[{idx+1}/{len(days)}] Backtesting {day}...")
        try:
            staged = run_scan_cycle(kite)
            if staged:
                results["days_with_trades"] += 1
                results["total_trades"] += 1
                best = max(staged, key=_avg_target_rank)
                sym = best["symbol"]
                if sym not in results["by_symbol"]:
                    results["by_symbol"][sym] = {"trades": 0, "wins": 0, "losses": 0, "no_exits": 0}
                results["by_symbol"][sym]["trades"] += 1
                key = f"{sym}|{best['pattern']}|{best.get('side', 'CE')}|{best.get('strike', '')}"
                if not trade_db.is_pattern_executed("nifty50", key):
                    trade_db.record_executed_pattern("nifty50", key, {"entry": best["entry_spot"]})
                strike_step = best.get("strike_step", 50)
                contract_display = resolve_option_contract(sym, best["entry_spot"], strike_step, best.get("side", "CE"), best.get("strike"))
                if not contract_display:
                    contract_display = sym
                log_to_journal(contract_display, best['pattern'], TIMEFRAME_ENTRY,
                               "BACKTEST_ENTRY", "ENTRY",
                               details=f"Symbol={sym} Strike={best.get('strike','')}",
                               entry=best['entry_spot'], sl=best['current_sl'],
                               target=best.get('t3') or best.get('t1') or "",
                               rr=best.get('rr'),
                               event_time=best.get("entry_time"))
                sim = simulate_trade_outcome(kite, best, day)
                sim_result = sim["result"]
                exit_action = ""
                pnl = 0.0
                if sim_result == "SL_HIT":
                    exit_action = "EXIT_SL"
                    pnl = sim["pnl_pct"] or 0.0
                    results["losses"] += 1
                    results["by_symbol"][sym]["losses"] += 1
                elif sim_result in ("T1_HIT", "T2_HIT", "T3_HIT"):
                    exit_action = sim_result.replace("_HIT", "")
                    pnl = sim["pnl_pct"] or 0.0
                    results["wins"] += 1
                    results["by_symbol"][sym]["wins"] += 1
                else:
                    exit_action = "EXIT_UNKNOWN"
                    results["no_exits"] += 1
                    results["by_symbol"][sym]["no_exits"] += 1
                if exit_action:
                    log_to_journal(contract_display, best['pattern'], TIMEFRAME_ENTRY,
                                   exit_action, sim_result or "NO_EXIT",
                                   details=f"Symbol={sym} Strike={best.get('strike','')}",
                                   entry=best['entry_spot'], sl=best['current_sl'],
                                   target=best.get('t3') or best.get('t1') or "",
                                   rr=best.get('rr'), pnl_pct=pnl,
                                   event_time=sim.get("exit_time") or sim.get("entry_time"))
                logging.info(f"  Trade: {contract_display} | {best['pattern']} | outcome={sim_result or 'unknown'} | P&L={pnl:.2f}%")
            trade_db.clear_cycle_trades("nifty50")
            time.sleep(3)
        except Exception as e:
            logging.error(f"  Error on {day}: {e}")
            time.sleep(3)
    wr = results["wins"] / (results["wins"] + results["losses"]) * 100 if (results["wins"] + results["losses"]) > 0 else 0
    logging.info(f"\n{'='*60}")
    logging.info(f"BACKTEST RESULTS: {start_date} to {end_date}")
    logging.info(f"{'='*60}")
    logging.info(f"Trading days scanned: {results['total_days']}")
    logging.info(f"Days with trades:     {results['days_with_trades']}")
    logging.info(f"Total trades found:   {results['total_trades']}")
    logging.info(f"Wins:                 {results['wins']}")
    logging.info(f"Losses:               {results['losses']}")
    logging.info(f"No exit:              {results['no_exits']}")
    logging.info(f"Win rate:             {wr:.1f}%")
    for sym, s in sorted(results["by_symbol"].items()):
        swr = s["wins"] / (s["wins"] + s["losses"]) * 100 if (s["wins"] + s["losses"]) > 0 else 0
        logging.info(f"  {sym}: {s['trades']} trades, {s['wins']}W/{s['losses']}L, {swr:.1f}% WR")
    logging.info(f"{'='*60}")
    return results

def main():
    global BACKTEST_DATE, LIVE_MARKET_DEPLOYMENT
    load_program_config()
    anchor_only = "--anchor-only" in sys.argv
    date_arg = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--date=")), None)
    range_arg = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--backtest-range=")), None)
    if date_arg:
        try:
            BACKTEST_DATE = dt.strptime(date_arg, "%Y-%m-%d").date()
        except Exception:
            BACKTEST_DATE = None
            logging.warning(f"Invalid --date value: {date_arg}")
    try:
        kite = None
        logging.info("[OPEN_SOURCE] Open-source Yahoo Finance data feed active.")
        sync_instruments(kite)
        if BACKTEST_DATE is None:
            load_state()
            active = trade_db.get_active_trades("nifty50")
            for t in active:
                if t["symbol"] not in STOCK_REGISTRY: continue
                pos = {k: v for k, v in t.items() if k not in ("id", "engine", "symbol", "status", "created_at", "updated_at")}
                pos.setdefault("pattern", "DB_RECOVERED")
                pos.setdefault("lot_size", 1)
                pos.setdefault("entry_spot", 0)
                pos.setdefault("current_sl", 0)
                pos.setdefault("t1", 0)
                pos.setdefault("t2", 0)
                pos.setdefault("t3", 0)
                pos.setdefault("trailing_stage", 0)
                pos.setdefault("position_type", "option")
                pos["trade_id"] = t["id"]
                if "entry_time" not in pos:
                    pos["entry_time"] = t.get("created_at") or dt.now().isoformat()
                with position_lock:
                    ACTIVE_POSITIONS[t["symbol"]] = pos
                logging.info(f"Loaded active position from database: {t['symbol']}")
            acc_token = getattr(kite, "access_token", "") if kite else ""
            if kite and acc_token and acc_token != "open_source_token":
                try:
                    kite_positions = kite.positions()
                    for p in kite_positions.get("day", []) + kite_positions.get("net", []):
                        if p["exchange"] not in ("NFO", "NSE") or int(p.get("quantity", 0)) == 0:
                            continue
                        symbol = next((s for s in STOCK_REGISTRY if s in p["tradingsymbol"]), None)
                        if not symbol or symbol in ACTIVE_POSITIONS:
                            continue
                        nq = abs(int(p.get("quantity", 0)))
                        if nq == 0: continue
                        if p["exchange"] == "NFO":
                            lots = nq // STOCK_REGISTRY[symbol]["lot_size"]
                            if lots == 0: continue
                            pos = {
                                "contract": p["tradingsymbol"], "option_token": int(p.get("instrument_token", 0)),
                                "entry_spot": float(p.get("net_price") or p.get("buy_price") or p.get("average_price") or 0),
                                "current_sl": 0, "t1": 0, "t2": 0, "t3": 0,
                                "trailing_stage": 0, "lot_size": STOCK_REGISTRY[symbol]["lot_size"],
                                "position_size": lots, "pattern": "KITE_RECOVERED",
                                "timeframe": TIMEFRAME_ENTRY,
                                "entry_time": dt.now().isoformat(),
                                "position_type": "option"
                            }
                        else:
                            pos = {
                                "contract": p["tradingsymbol"], "option_token": int(p.get("instrument_token", 0)),
                                "entry_spot": float(p.get("net_price") or p.get("buy_price") or p.get("average_price") or 0),
                                "current_sl": 0, "t1": 0, "t2": 0, "t3": 0,
                                "trailing_stage": 0, "lot_size": 1,
                                "position_size": nq, "pattern": "KITE_RECOVERED",
                                "timeframe": TIMEFRAME_ENTRY,
                                "entry_time": dt.now().isoformat(),
                                "position_type": "stock"
                            }
                        pos["trade_id"] = trade_db.create_trade("nifty50", symbol, {k: v for k, v in pos.items() if k != "trade_id"})
                        scan_sl = lookup_scan_sl_target(p["tradingsymbol"], symbol, "nifty50", kite, pos["entry_spot"], TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR)
                        if scan_sl:
                            pos.update(scan_sl)
                            trade_db.update_trade(pos["trade_id"], scan_sl)
                            logging.info(f"[KITE_RECOVER] Applied scan SL/Target for {symbol}: SL={scan_sl['current_sl']} T1={scan_sl['t1']} T2={scan_sl['t2']} T3={scan_sl['t3']}")
                        ACTIVE_POSITIONS[symbol] = pos
                        logging.info(f"Recovered from Kite: {symbol} {p['tradingsymbol']} qty={nq}")
                except Exception as e:
                    logging.warning(f"Kite position recovery failed: {e}")
            reconcile_positions(kite)
        if anchor_only:
            run_anchor_scan(kite)
            return
    except Exception as e:
        logging.error(f"Init: {e}")
        return
    if range_arg:
        LIVE_MARKET_DEPLOYMENT = False
        parts = range_arg.split(",")
        start = dt.strptime(parts[0].strip(), "%Y-%m-%d").date()
        end = dt.strptime(parts[1].strip(), "%Y-%m-%d").date()
        run_multi_day_backtest(kite, start, end)
        return
    if BACKTEST_DATE is not None:
        LIVE_MARKET_DEPLOYMENT = False
        logging.info(f"Backtest run for date {BACKTEST_DATE} (dry, no real orders)...")
        staged = run_scan_cycle(kite)
        if staged:
            best = max(staged, key=_avg_target_rank)
            execute_highest_rr_trade(kite, staged)
            with position_lock:
                ACTIVE_POSITIONS.clear()
                write_scan_display_data(staged, dict(ACTIVE_POSITIONS))
            logging.info(f"\n{'='*100}")
            logging.info(f"{'TRADE LOG':^100}")
            logging.info(f"{'='*100}")
            hdr = f"{'#':<4} {'Symbol':<14} {'Contract':<24} {'Side':<4} {'Entry':>8} {'SL':>8} {'T1':>8} {'T2':>8} {'T3':>8} {'EntryTime':<24} {'ExitTime':<24} {'Result':<12} {'P&L%':>8}"
            logging.info(hdr)
            logging.info(f"{'-'*100}")
            for idx, t in enumerate(staged, 1):
                sim = simulate_trade_outcome(kite, t, BACKTEST_DATE)
                et = str(sim["entry_time"]) if sim["entry_time"] is not None else "-"
                ext = str(sim["exit_time"]) if sim["exit_time"] is not None else "-"
                r = sim["result"] or "FAIL"
                pnl = sim["pnl_pct"]
                pnl_s = f"{pnl:+.2f}%" if pnl is not None else "-"
                t1v = t.get("t1", "-")
                t2v = t.get("t2", "-")
                t3v = t.get("t3", "-")
                logging.info(f"{idx:<4} {t['symbol']:<14} {t.get('contract',''):<24} {t.get('side',''):<4} {t['entry_spot']:>8.2f} {t['current_sl']:>8.2f} {str(t1v):>8} {str(t2v):>8} {str(t3v):>8} {et:<24} {ext:<24} {r:<12} {pnl_s:>8}")
            logging.info(f"{'='*100}")
            logging.info(f"BEST TRADE: {best['symbol']} {best.get('contract','')} | avg-target RR={_avg_target_rank(best):.2f}")
        else:
            with position_lock:
                ACTIVE_POSITIONS.clear()
                write_scan_display_data([], dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "nifty50")
            logging.info("[BACKTEST] No trades staged for this date.")
        trade_db.clear_cycle_trades("nifty50")
        return
    if not LIVE_MARKET_DEPLOYMENT:
        logging.error("Config has _backtest=true but no --date= or --backtest-range= flag. "
                      "Use --date=YYYY-MM-DD or --backtest-range=START,END to run backtest. Exiting.")
        return
    logging.info(f"TF: {TIMEFRAME_ENTRY} | Interval: {SCAN_INTERVAL_SECONDS}s | Risk: {MAX_RISK_PERCENT}%")
    t1 = threading.Thread(target=position_monitor_loop, args=(kite,), daemon=True)
    t1.start()
    t2 = threading.Thread(target=main_scan_loop, args=(kite,), daemon=True)
    t2.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Engine stopped.")

if __name__ == "__main__":
    main()
