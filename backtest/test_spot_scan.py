import os
import sys
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
COMMON_DIR = os.path.join(PROJECT_ROOT, "common")
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

from kiteconnect import KiteConnect
from trading_core import (
    load_kite_session,
    scan_anchor_bcd_breakout_generic,
    find_profit_targets,
    find_anchor_bullish_engulfing,
    find_anchor_ll_sweep,
    find_anchor_hammer_baby,
    find_anchor_bullish_harami,
    find_anchor_two_higher_highs,
    INDEX_REGISTRY,
    STOCK_REGISTRY,
    sync_stock_tokens
)

token_file = os.path.join(PROJECT_ROOT, "Trade_Option", "input", "kite_access_token.txt")
if not os.path.exists(token_file):
    token_file = os.path.join(PROJECT_ROOT, "Trade_Stock", "input", "kite_access_token.txt")

api_key, access_token = load_kite_session(token_file=token_file)
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

sync_stock_tokens(kite)

# Test NIFTY (256265) 15m
data_nifty = kite.historical_data(256265, "2026-07-20", "2026-07-25", "15minute")
df_nifty = pd.DataFrame(data_nifty)
df_nifty['date'] = pd.to_datetime(df_nifty['date'])

print("NIFTY Candles count:", len(df_nifty))

# Scan Bullish
res_bull = scan_anchor_bcd_breakout_generic(df_nifty, df_nifty, side="BULL")
print("NIFTY BULL:", res_bull)

# Scan Bearish
res_bear = scan_anchor_bcd_breakout_generic(df_nifty, df_nifty, side="BEAR")
print("NIFTY BEAR:", res_bear)

# Test Stock (e.g. RELIANCE / INFYS)
rel_token = STOCK_REGISTRY.get("RELIANCE", {}).get("token")
if rel_token:
    df_rel = pd.DataFrame(kite.historical_data(rel_token, "2026-07-20", "2026-07-25", "30minute"))
    df_rel['date'] = pd.to_datetime(df_rel['date'])
    print("RELIANCE Candles count:", len(df_rel))
    print("RELIANCE BULL:", scan_anchor_bcd_breakout_generic(df_rel, df_rel, side="BULL"))
    print("RELIANCE BEAR:", scan_anchor_bcd_breakout_generic(df_rel, df_rel, side="BEAR"))
