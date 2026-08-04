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

from kiteconnect import KiteConnect
import trade_db

from trading_core import (
    load_kite_session,
    ensure_kite_session,
    log_to_journal,
    is_market_hours,
    get_weekly_expiry,
    cap_lookback_days,
    check_left_side_rule,
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
    lookup_scan_sl_target,
    reconcile_positions as shared_reconcile,
    resolve_option_strikes as shared_resolve_strikes,
    scan_symbol,
    monitor_active_positions as shared_monitor_positions,
    simulate_trade_outcome as shared_simulate,
    is_anchor_valid_and_active,
    find_newest_valid_anchor,
    INDEX_REGISTRY
)

LIVE_MARKET_DEPLOYMENT = True
LOOKBACK_DAYS = 30
INITIAL_CAPITAL = 100000.0
MAX_RISK_PERCENT = 1.0
TOKEN_FILE = "input/kite_access_token.txt"
SCAN_INTERVAL_SECONDS = 15

TIMEFRAME_ENTRY = "3minute"
TIMEFRAME_ANCHOR = "15minute"
TIMEFRAME_FALLBACK = "3minute"
STRIKE_RANGE = 3
BACKTEST_DATE = None

ACTIVE_POSITIONS = {}
position_lock = threading.Lock()
instrument_dump = None
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANCHOR_SCAN_REQUEST_FILE = os.path.join(BASE_DIR, "output", "monitor", "anchor_scan_request.txt")
ANCHOR_SCAN_STOP_FILE = os.path.join(BASE_DIR, "output", "monitor", "anchor_scan_stop.txt")
LIVE_EXECUTION_FLAG = os.path.join(BASE_DIR, "input", "index_live.flag")
SCAN_DISPLAY_FILE = os.path.join(BASE_DIR, "output", "monitor", "scan_display_index.json")
SL_TARGET_OVERRIDES_FILE = os.path.join(BASE_DIR, "output", "monitor", "sl_target_overrides.json")

journal_lock = threading.Lock()
JOURNAL_FILE = os.path.join(BASE_DIR, "output", "monitor", "trade_journal.csv")

class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE_PATH = os.path.join(BASE_DIR, "output", "logs", "bull_index_trade_engine.log")
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        FlushFileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


def fetch_instruments(kite):
    global instrument_dump
    try:
        logging.info("Syncing NFO and BFO instruments...")
        nfo = kite.instruments("NFO")
        try:
            bfo = kite.instruments("BFO")
        except Exception as b_err:
            logging.warning(f"BFO sync warning: {b_err}")
            bfo = []
        combined = (nfo if nfo else []) + (bfo if bfo else [])
        instrument_dump = pd.DataFrame(combined)
        logging.info(f"Synced {len(instrument_dump)} NFO/BFO contracts.")
    except Exception as e:
        logging.error(f"Instrument sync failed: {e}")
        raise

def resolve_option_contract(base_symbol, spot_price, step_size, option_type, expiry_offset=0):
    global instrument_dump
    if instrument_dump is None or instrument_dump.empty:
        return None
    strike = int(round(spot_price / step_size) * step_size)
    try:
        df = instrument_dump[
            (instrument_dump['name'] == base_symbol) &
            (instrument_dump['instrument_type'] == option_type) &
            (instrument_dump['strike'] == strike)
        ].copy()
        if df.empty:
            return None
        df['expiry'] = pd.to_datetime(df['expiry']).dt.date
        df = df[df['expiry'] >= dt.now().date()].sort_values(by='expiry')
        if df.empty:
            return None
        expiries = df['expiry'].unique()
        selected_idx = min(expiry_offset, len(expiries) - 1)
        target_expiry = expiries[selected_idx]
        sub = df[df['expiry'] == target_expiry]
        if not sub.empty:
            c = sub.iloc[0]
            return {"token": int(c['instrument_token']), "tradingsymbol": c['tradingsymbol'], "expiry": str(target_expiry)}
        c = df.iloc[0]
        return {"token": int(c['instrument_token']), "tradingsymbol": c['tradingsymbol'], "expiry": str(c['expiry'])}
    except Exception as e:
        logging.error(f"Contract resolution error: {e}")
        return None

# ──────────────────────────────────────────────
#  SCAN CYCLE — RUNS EVERY N SECONDS
# ──────────────────────────────────────────────

def run_scan_cycle(kite):
    cfg_applied = load_program_config_for_engine("index", [("strike_range", "STRIKE_RANGE")])
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
    temp_stored_trades = []
    for symbol, config in INDEX_REGISTRY.items():
        with position_lock:
            if symbol in ACTIVE_POSITIONS:
                continue
        trades = scan_symbol(kite, symbol, config, from_entry, to_entry, from_anchor, to_anchor,
                             entry_scanners, anchor_scanners,
                             lambda sym, sp, step, opt, r: shared_resolve_strikes(instrument_dump, sym, sp, step, opt, r),
                             "index", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR, TIMEFRAME_FALLBACK,
                             ACTIVE_POSITIONS, position_lock, trade_db, STRIKE_RANGE,
                             log_to_journal)
        temp_stored_trades.extend(trades)
    with position_lock:
        shared_write_display(temp_stored_trades, dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "index")
    return temp_stored_trades

# ──────────────────────────────────────────────
#  ANCHOR SCAN — RUNS ON DEMAND VIA DASHBOARD
# ──────────────────────────────────────────────

def run_anchor_scan(kite):
    logging.info("On-demand scan requested: executing full A-B-C-D breakout scan across index option contracts...")
    staged = run_scan_cycle(kite)
    with position_lock:
        shared_write_display(staged or [], dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "index")
    logging.info(f"On-demand scan complete: found {len(staged or [])} full A-B-C-D breakout setup(s)")


def execute_index_entry(kite, pos):
    if not LIVE_MARKET_DEPLOYMENT:
        logging.info(f"[BACKTEST ENTRY] {pos['contract']} ({pos['side']})")
        return True
    try:
        c_str = str(pos['contract']).upper()
        target_exch = "BFO" if ("SENSEX" in c_str or "BSE" in c_str) else "NFO"
        q_key = f"{target_exch}:{pos['contract']}"
        q = kite.quote([q_key])
        ltp = float(q.get(q_key, {}).get("last_price", 0))
        ask = 0
        depth = q.get(q_key, {}).get("depth", {}).get("sell", [])
        if depth and len(depth) > 0:
            ask = float(depth[0].get("price", 0))
        price = round((ask if ask > 0 else ltp) * 1.005, 1)
        kite.place_order(
            variety=kite.VARIETY_REGULAR, tradingsymbol=pos["contract"],
            exchange=target_exch, transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=pos["lot_size"] * pos["position_size"], order_type=kite.ORDER_TYPE_LIMIT,
            price=price, product=kite.PRODUCT_NRML
        )
        return True
    except Exception as e:
        logging.error(f"Entry failed for {pos['contract']}: {e}")
        return False

def simulate_trade_outcome(kite, trade, target_date):
    return shared_simulate(kite, trade, target_date)

# ──────────────────────────────────────────────
#  EXECUTION FUNCTIONS
# ──────────────────────────────────────────────

def execute_highest_rr_trade(kite, staged):
    """After a scan cycle, pick best by profit and execute (if live)."""
    if not staged:
        return
    best = max(staged, key=lambda t: (t.get("t3") or t.get("t1") or 0) - t.get("entry_spot", 0))
    key = f"{best['symbol']}|{best['pattern']}|{best['side']}|{best.get('strike', '')}"
    if trade_db.is_pattern_executed("index", key):
        logging.info(f"Best cycle trade {key} already executed; skipping")
        return
    live_ok = LIVE_MARKET_DEPLOYMENT and live_execution_enabled(LIVE_EXECUTION_FLAG) and is_market_hours()
    if live_ok or BACKTEST_DATE is not None:
        pos = best.copy()
        pos["entry_time"] = dt.now().isoformat()
        pos.setdefault("position_type", "option")
        if live_ok:
            with position_lock:
                if best["symbol"] in ACTIVE_POSITIONS:
                    logging.info(f"{best['symbol']} already active; skipping new trade")
                    return
                pos["trade_id"] = trade_db.create_trade("index", best["symbol"], {k: v for k, v in pos.items() if k != "trade_id"})
                ACTIVE_POSITIONS[best["symbol"]] = pos
        ok = execute_index_entry(kite, pos)
        if ok:
            trade_db.record_executed_pattern("index", key, {"contract": best["contract"], "entry": best["entry_spot"]})
            profit = round((best.get("t3") or best.get("t1") or 0) - best["entry_spot"], 2)
            rr_best = best.get("rr", "")
            if live_ok:
                log_to_journal(best["symbol"], best["pattern"], best["timeframe"],
                               "BUY_" + best["side"], "SUCCESS", f"Contract: {best['contract']}, Qty: {best['position_size']}",
                               entry=best["entry_spot"], sl=best["current_sl"], target=best.get("t1", ""), rr=rr_best,
                               event_time=best.get("entry_time"))
            else:
                log_to_journal(best["symbol"], best["pattern"], best["timeframe"],
                               "DRY_" + best["side"], "SUCCESS", f"Contract: {best['contract']}, Size: {best['position_size']}",
                               entry=best["entry_spot"], sl=best["current_sl"], target=best.get("t1", ""), rr=rr_best,
                               event_time=best.get("entry_time"))
                sim = simulate_trade_outcome(kite, best, BACKTEST_DATE)
                if sim["result"]:
                    log_to_journal(best["symbol"], best["pattern"], best["timeframe"],
                                   sim["result"], "COMPLETED", sim["detail"],
                                   entry=best["entry_spot"], sl=best["current_sl"], target=best.get("t1", ""), rr=rr_best,
                                   event_time=sim.get("exit_time") or sim.get("entry_time"))
                    logging.info(f"[BACKTEST] Trade outcome: {sim['result']} | {sim['detail']}")
            logging.info(f"EXECUTED best cycle trade: {best['symbol']} {best['side']} | {best['pattern']} | max-profit={profit}")
        else:
            ACTIVE_POSITIONS.pop(best["symbol"], None)
            if pos.get("trade_id"):
                trade_db.update_trade(pos["trade_id"], {"status": "FAILED", "updated_at": dt.now().strftime("%Y-%m-%d %H:%M:%S")})
    else:
        cp = best["entry_spot"]
        contract = best.get("contract", "")
        pos_size = best.get("position_size", 0)
        log_to_journal(best["symbol"], best["pattern"], best["timeframe"],
                       "SCAN_READY", "SUCCESS",
                       f"Contract: {contract}, Size: {pos_size} | Manual entry pending",
                       entry=cp, sl=best["current_sl"], target=best.get("t1", ""),
                       event_time=best.get("entry_time"))
        logging.info(f"SCAN_READY best trade: {best['symbol']} {contract} | Entry: {cp} | SL: {best['current_sl']}")
        return

def monitor_active_positions(kite):
    return shared_monitor_positions(kite, INDEX_REGISTRY, ACTIVE_POSITIONS, position_lock,
                                     kite.PRODUCT_MIS, "index", TIMEFRAME_ENTRY,
                                     trade_db, log_to_journal,
                                     live=LIVE_MARKET_DEPLOYMENT)

# ──────────────────────────────────────────────
#  DISPLAY DATA WRITER + KITE SYNC
# ──────────────────────────────────────────────



# ──────────────────────────────────────────────
#  MAIN LOOP — SCAN CYCLE + RISK MONITOR
# ──────────────────────────────────────────────

def main_scan_loop(kite):
    active = trade_db.get_active_trades("index")
    for t in active:
        if t["symbol"] in INDEX_REGISTRY:
            with position_lock:
                pos = {k: v for k, v in t.items() if k not in ("id", "engine", "symbol", "status", "created_at", "updated_at")}
                pos["trade_id"] = t["id"]
                pos["entry_spot"] = pos.get("entry_spot") or t.get("entry_spot")
                if "entry_time" not in pos:
                    pos["entry_time"] = t.get("created_at") or dt.now().isoformat()
                ACTIVE_POSITIONS[t["symbol"]] = pos
            logging.info(f"Recovered position: {t['symbol']} | {t.get('contract','')}")
    try:
        kite_positions = kite.positions()
        for p in kite_positions.get("day", []) + kite_positions.get("net", []):
            if p["exchange"] != "NFO" or int(p["quantity"]) == 0:
                continue
            symbol = next((s for s in INDEX_REGISTRY if s in p["tradingsymbol"]), None)
            if not symbol or symbol in ACTIVE_POSITIONS:
                continue
            nq = abs(int(p["quantity"]))
            lots = nq // INDEX_REGISTRY[symbol]["lot_size"]
            if lots == 0:
                continue
            side = "CE" if "CE" in p["tradingsymbol"] else "PE"
            pos = {
                "contract": p["tradingsymbol"], "option_token": int(p["instrument_token"]),
                "entry_spot": float(p.get("net_price") or p.get("buy_price") or p.get("average_price") or 0), "current_sl": 0,
                "t1": 0, "t2": 0, "t3": 0, "trailing_stage": 0,
                "lot_size": INDEX_REGISTRY[symbol]["lot_size"], "position_size": lots,
                "pattern": "KITE_RECOVERED", "side": side,
                "timeframe": TIMEFRAME_ENTRY,
                "entry_time": dt.now().isoformat(),
                "position_type": "option"
            }
            pos["trade_id"] = trade_db.create_trade("index", symbol, {k: v for k, v in pos.items() if k != "trade_id"})
            scan_sl = lookup_scan_sl_target(p["tradingsymbol"], symbol, "index", kite, pos["entry_spot"], TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR)
            if scan_sl:
                pos.update(scan_sl)
                trade_db.update_trade(pos["trade_id"], scan_sl)
                logging.info(f"[KITE_RECOVER] Applied scan SL/Target for {symbol}: SL={scan_sl['current_sl']} T1={scan_sl['t1']} T2={scan_sl['t2']} T3={scan_sl['t3']}")
            ACTIVE_POSITIONS[symbol] = pos
            logging.info(f"Recovered from Kite: {symbol} {p['tradingsymbol']} qty={nq}")
    except Exception as e:
        logging.warning(f"Kite position recovery failed: {e}")
    shared_reconcile(kite, INDEX_REGISTRY, ACTIVE_POSITIONS, position_lock, "index", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR, LOOKBACK_DAYS, lambda sym, sp, step, opt, r: shared_resolve_strikes(instrument_dump, sym, sp, step, opt, r))
    cycle = 0
    while True:
        try:
            ensure_kite_session(kite)
            cycle += 1
            if cycle == 1 or cycle % 4 == 1:
                with position_lock:
                    active = len(ACTIVE_POSITIONS)
                    symbols = list(ACTIVE_POSITIONS.keys())
                logging.info(f"[BEAT] Cycle {cycle} | Active: {active} {symbols if active else ''}")
            if cycle % 10 == 0:
                shared_sync_kite(kite, INDEX_REGISTRY, ACTIVE_POSITIONS, position_lock, "index", TIMEFRAME_ENTRY, TIMEFRAME_ANCHOR)
            if os.path.exists(SL_TARGET_OVERRIDES_FILE):
                try:
                    with open(SL_TARGET_OVERRIDES_FILE) as f:
                        overrides = json.load(f)
                    eng_overrides = overrides.get("index", {})
                    if eng_overrides:
                        with position_lock:
                            for sym, vals in eng_overrides.items():
                                target_pos = None
                                if sym in ACTIVE_POSITIONS:
                                    target_pos = ACTIVE_POSITIONS[sym]
                                else:
                                    for k, p in ACTIVE_POSITIONS.items():
                                        if p.get("contract") == sym or p.get("symbol") == sym or sym in k:
                                            target_pos = p
                                            break
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
                except Exception as e:
                    logging.warning(f"Override apply failed: {e}")
            temp_stored_trades = run_scan_cycle(kite)

            if temp_stored_trades:
                execute_highest_rr_trade(kite, temp_stored_trades)
            else:
                logging.info("[CYCLE] No trades staged this cycle.")

            trade_db.clear_cycle_trades("index")
            with position_lock:
                shared_write_display(temp_stored_trades or [], dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "index")
            logging.info(f"[CYCLE COMPLETE] {cycle} cycle complete | Found {len(temp_stored_trades or [])} setup(s)")
            monitor_active_positions(kite)
            time.sleep(max(0, SCAN_INTERVAL_SECONDS))
        except Exception as e:
            logging.error(f"Background error: {e}")
            time.sleep(5)



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
            if staged and len(staged) >= 1:
                results["days_with_trades"] += 1
                results["total_trades"] += 1
                best = max(staged, key=lambda t: (t.get("t3") or t.get("t1") or 0) - t.get("entry_spot", 0))
                sym = best["symbol"]
                if sym not in results["by_symbol"]:
                    results["by_symbol"][sym] = {"trades": 0, "wins": 0, "losses": 0, "no_exits": 0}
                results["by_symbol"][sym]["trades"] += 1
                key = f"{best['symbol']}|{best['pattern']}|{best['side']}|{best.get('strike', '')}"
                if not trade_db.is_pattern_executed("index", key):
                    trade_db.record_executed_pattern("index", key, {"contract": best["contract"], "entry": best["entry_spot"]})
                contract_display = best.get('contract', sym)
                log_to_journal(contract_display, best['pattern'], best.get('timeframe', TIMEFRAME_ENTRY),
                               "BACKTEST_ENTRY", "ENTRY",
                               details=f"Symbol={sym} Strike={best.get('strike','')}",
                               entry=best['entry_spot'], sl=best['current_sl'],
                               target=best.get('t3') or best.get('t1') or "",
                               rr=best.get('rr'),
                               event_time=best.get("entry_time"))
                sim = simulate_trade_outcome(kite, best, day)
                sim_result = sim["result"]
                exit_action = ""
                pnl = sim.get("pnl_pct") or 0.0
                if sim_result == "SL_HIT":
                    exit_action = "EXIT_SL"
                    results["losses"] += 1
                    results["by_symbol"][sym]["losses"] += 1
                elif sim_result in ("T1_HIT", "T2_HIT", "T3_HIT"):
                    exit_action = sim_result.replace("_HIT", "")
                    results["wins"] += 1
                    results["by_symbol"][sym]["wins"] += 1
                else:
                    exit_action = "EXIT_UNKNOWN"
                    results["no_exits"] += 1
                    results["by_symbol"][sym]["no_exits"] += 1
                if exit_action:
                    log_to_journal(contract_display, best['pattern'], best.get('timeframe', TIMEFRAME_ENTRY),
                                   exit_action, sim_result or "NO_EXIT",
                                   details=f"Symbol={sym} Strike={best.get('strike','')}",
                                   entry=best['entry_spot'], sl=best['current_sl'],
                                   target=best.get('t3') or best.get('t1') or "",
                                   rr=best.get('rr'), pnl_pct=pnl,
                                   event_time=sim.get("exit_time") or sim.get("entry_time"))
                logging.info(f"  Trade: {best['contract']} | {best['pattern']} | outcome={sim_result or 'unknown'} | P&L={pnl:.2f}%")
            trade_db.clear_cycle_trades("index")
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
    cfg_applied = load_program_config_for_engine("index", [("strike_range", "STRIKE_RANGE")])
    for k, v in cfg_applied.items():
        if k == "STRIKE_RANGE": globals()["STRIKE_RANGE"] = int(v) if isinstance(v, (int, float)) else v
        elif k in ("TIMEFRAME_ENTRY", "TIMEFRAME_ANCHOR"): globals()[k] = v
        elif k == "LIVE_MARKET_DEPLOYMENT": globals()["LIVE_MARKET_DEPLOYMENT"] = v
        elif k == "LOOKBACK_DAYS": globals()["LOOKBACK_DAYS"] = int(v)
        elif k == "SCAN_INTERVAL_SECONDS": globals()["SCAN_INTERVAL_SECONDS"] = int(v)
        elif k == "MAX_RISK_PERCENT": globals()["MAX_RISK_PERCENT"] = float(v)
        elif k == "INITIAL_CAPITAL": globals()["INITIAL_CAPITAL"] = float(v)
    anchor_only = "--anchor-only" in sys.argv
    date_arg = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--date=")), None)
    range_arg = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--backtest-range=")), None)
    if date_arg:
        try:
            BACKTEST_DATE = dt.strptime(date_arg, "%Y-%m-%d").date()
        except Exception:
            BACKTEST_DATE = None
            logging.warning(f"Invalid --date value: {date_arg}")
    if not anchor_only and BACKTEST_DATE is None and range_arg is None:
        logging.info("Starting Index Trade Engine...")
    try:
        api_key, access_token = load_kite_session()
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        fetch_instruments(kite)
    except Exception as e:
        logging.error(f"Init failed: {e}")
        return
    if anchor_only:
        run_anchor_scan(kite)
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
            execute_highest_rr_trade(kite, staged)
        else:
            logging.info("[BACKTEST] No trades staged for this date.")
        with position_lock:
            ACTIVE_POSITIONS.clear()
            shared_write_display(staged or [], dict(ACTIVE_POSITIONS), SCAN_DISPLAY_FILE, "index")
        trade_db.clear_cycle_trades("index")
        return
    if not LIVE_MARKET_DEPLOYMENT:
        logging.error("Config has _backtest=true but no --date= or --backtest-range= flag. "
                      "Use --date=YYYY-MM-DD or --backtest-range=START,END to run backtest. Exiting.")
        return
    logging.info(f"Scanner: {TIMEFRAME_ENTRY} | Anchor: {TIMEFRAME_ANCHOR} | Capital: {INITIAL_CAPITAL} | Risk: {MAX_RISK_PERCENT}%")
    worker = threading.Thread(target=main_scan_loop, args=(kite,), daemon=True)
    worker.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Engine stopped by user.")

if __name__ == "__main__":
    main()
