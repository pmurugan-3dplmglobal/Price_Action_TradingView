import os
import sys
import json
import time
import pandas as pd
from datetime import datetime as dt, timedelta

# Add paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
COMMON_DIR = os.path.join(PROJECT_ROOT, "common")
OPTION_DIR = os.path.join(PROJECT_ROOT, "Trade_Option")
if COMMON_DIR not in sys.path: sys.path.insert(0, COMMON_DIR)
if OPTION_DIR not in sys.path: sys.path.insert(0, OPTION_DIR)

from kiteconnect import KiteConnect
from trading_core import (
    load_kite_session,
    INDEX_REGISTRY,
    STOCK_REGISTRY,
    sync_stock_tokens
)

import index_options_trade_engine as index_engine
import stock_options_trade_engine as stock_engine

def run_fast_multi_day_backtest(start_date="2026-07-20", end_date="2026-07-25"):
    t0 = time.time()
    
    # 1. Initialize Kite Session
    token_file = os.path.join(PROJECT_ROOT, "Trade_Option", "input", "kite_access_token.txt")
    if not os.path.exists(token_file):
        token_file = os.path.join(PROJECT_ROOT, "Trade_Stock", "input", "kite_access_token.txt")
    api_key, access_token = load_kite_session(token_file=token_file)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    user_name = kite.profile()['user_shortname']
    print(f"========================================================================================================================")
    print(f"      PRICE ACTION STRATEGY — OFFICIAL OPTION STRIKE BACKTEST REPORT (LAST WEEK: {start_date} to {end_date})")
    print(f"      Authenticated Zerodha Kite Session: {user_name}")
    print(f"========================================================================================================================\n")

    # Sync tokens
    sync_stock_tokens(kite)

    # 2. RUN INDEX OPTIONS BACKTEST (15m TF)
    print("--> [1/2] RUNNING INDEX OPTIONS ENGINE BACKTEST (15M TF on NIFTY, BANKNIFTY, SENSEX Strike Contracts)...", flush=True)
    index_engine.BACKTEST_DATE = None
    index_engine.LIVE_MARKET_DEPLOYMENT = False
    
    days = index_engine.trading_days_between(start_date, end_date)
    idx_trades = []

    for day in days:
        index_engine.BACKTEST_DATE = day
        staged = index_engine.run_scan_cycle(kite)
        if staged:
            for t in staged:
                sim = index_engine.simulate_trade_outcome(kite, t, day)
                idx_trades.append({
                    "Category": "INDEX_OPTION",
                    "Asset": t.get("contract") or f"{t['symbol']} {t.get('strike','')}{t['side']}",
                    "Symbol": t["symbol"],
                    "TF": "15m",
                    "Side": t["side"],
                    "Pattern": t["pattern"],
                    "BM_Time": str(t.get("entry_time") or day),
                    "D_Time": str(t.get("d_time") or day),
                    "Entry": float(t.get("entry_spot") or 0.0),
                    "SL": float(t.get("current_sl") or 0.0),
                    "T1": float(t.get("t1") or 0.0),
                    "T2": float(t.get("t2") or 0.0),
                    "T3": float(t.get("t3") or 0.0),
                    "RR": float(t.get("rr") or 0.0),
                    "Outcome": sim.get("result") or "ACTIVE",
                    "P&L %": float(sim.get("pnl_pct") or 0.0)
                })

    # 3. RUN STOCK OPTIONS BACKTEST (30m TF)
    print("--> [2/2] RUNNING NIFTY 50 STOCK OPTIONS ENGINE BACKTEST (30M TF across Nifty 50 Strike Contracts)...", flush=True)
    stock_engine.BACKTEST_DATE = None
    stock_engine.LIVE_MARKET_DEPLOYMENT = False
    
    stk_trades = []
    for day in days:
        stock_engine.BACKTEST_DATE = day
        staged = stock_engine.run_scan_cycle(kite)
        if staged:
            for t in staged:
                sim = stock_engine.simulate_trade_outcome(kite, t, day)
                stk_trades.append({
                    "Category": "STOCK_OPTION",
                    "Asset": t.get("contract") or f"{t['symbol']} {t.get('strike','')}{t['side']}",
                    "Symbol": t["symbol"],
                    "TF": "30m",
                    "Side": t["side"],
                    "Pattern": t["pattern"],
                    "BM_Time": str(t.get("entry_time") or day),
                    "D_Time": str(t.get("d_time") or day),
                    "Entry": float(t.get("entry_spot") or 0.0),
                    "SL": float(t.get("current_sl") or 0.0),
                    "T1": float(t.get("t1") or 0.0),
                    "T2": float(t.get("t2") or 0.0),
                    "T3": float(t.get("t3") or 0.0),
                    "RR": float(t.get("rr") or 0.0),
                    "Outcome": sim.get("result") or "ACTIVE",
                    "P&L %": float(sim.get("pnl_pct") or 0.0)
                })

    # PRINT SUMMARY METRICS
    idx_wins = sum(1 for r in idx_trades if "T1" in r["Outcome"] or "T2" in r["Outcome"] or "T3" in r["Outcome"] or "WIN" in r["Outcome"])
    idx_losses = sum(1 for r in idx_trades if "SL" in r["Outcome"] or "LOSS" in r["Outcome"])
    idx_total = len(idx_trades)
    idx_win_rate = (idx_wins / (idx_wins + idx_losses) * 100) if (idx_wins + idx_losses) > 0 else 0.0

    stk_wins = sum(1 for r in stk_trades if "T1" in r["Outcome"] or "T2" in r["Outcome"] or "T3" in r["Outcome"] or "WIN" in r["Outcome"])
    stk_losses = sum(1 for r in stk_trades if "SL" in r["Outcome"] or "LOSS" in r["Outcome"])
    stk_total = len(stk_trades)
    stk_win_rate = (stk_wins / (stk_wins + stk_losses) * 100) if (stk_wins + stk_losses) > 0 else 0.0

    print("\n" + "=" * 140)
    print("                                1. INDEX OPTIONS ENGINE (15-MIN TF) — OPTION STRIKE BACKTEST TRADES")
    print("=" * 140)
    header = f"{'Option Contract':<22} | {'TF':<4} | {'Side':<4} | {'Pattern Formed':<16} | {'A Formed Time (BM)':<20} | {'D Formed Time':<20} | {'Entry':<8} | {'SL':<8} | {'T1':<8} | {'R:R':<5} | {'Outcome':<15} | {'P&L %'}"
    print(header)
    print("-" * 140)

    for r in idx_trades:
        print(f"{r['Asset']:<22} | {r['TF']:<4} | {r['Side']:<4} | {r['Pattern']:<16} | {r['BM_Time']:<20} | {r['D_Time']:<20} | {r['Entry']:<8.2f} | {r['SL']:<8.2f} | {r['T1']:<8.2f} | {r['RR']:<5.2f} | {r['Outcome']:<15} | {r['P&L %']:+.2f}%")

    print("-" * 140)
    print(f"  • INDEX OPTIONS ENGINE METRICS: Total Trades: {idx_total} | Wins: {idx_wins} | Losses: {idx_losses} | Active: {idx_total - (idx_wins + idx_losses)} | WIN RATE: {idx_win_rate:.2f}%")

    print("\n" + "=" * 140)
    print("                            2. NIFTY 50 STOCK OPTIONS ENGINE (30-MIN TF) — OPTION STRIKE BACKTEST TRADES")
    print("=" * 140)
    print(header)
    print("-" * 140)

    for r in stk_trades:
        print(f"{r['Asset']:<22} | {r['TF']:<4} | {r['Side']:<4} | {r['Pattern']:<16} | {r['BM_Time']:<20} | {r['D_Time']:<20} | {r['Entry']:<8.2f} | {r['SL']:<8.2f} | {r['T1']:<8.2f} | {r['RR']:<5.2f} | {r['Outcome']:<15} | {r['P&L %']:+.2f}%")

    print("-" * 140)
    print(f"  • STOCK OPTIONS ENGINE METRICS: Total Trades: {stk_total} | Wins: {stk_wins} | Losses: {stk_losses} | Active: {stk_total - (stk_wins + stk_losses)} | WIN RATE: {stk_win_rate:.2f}%")
    print("=" * 140 + "\n")

    summary_data = {
        "start_date": start_date,
        "end_date": end_date,
        "user": user_name,
        "execution_time_seconds": round(time.time() - t0, 2),
        "index_options_15m": {
            "total_trades": idx_total,
            "wins": idx_wins,
            "losses": idx_losses,
            "active": idx_total - (idx_wins + idx_losses),
            "win_rate": round(idx_win_rate, 2),
            "trades": idx_trades
        },
        "stock_options_30m": {
            "total_trades": stk_total,
            "wins": stk_wins,
            "losses": stk_losses,
            "active": stk_total - (stk_wins + stk_losses),
            "win_rate": round(stk_win_rate, 2),
            "trades": stk_trades
        }
    }

    export_file = os.path.join(BASE_DIR, "master_backtest_results.json")
    with open(export_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)

    print(f"OFFICIAL_OPTION_STRIKE_BACKTEST_FINISHED in {time.time() - t0:.2f}s")

if __name__ == "__main__":
    run_fast_multi_day_backtest(start_date="2026-07-20", end_date="2026-07-25")
