import os
import sys
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
COMMON_DIR = os.path.join(PROJECT_ROOT, "common")
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

from kiteconnect import KiteConnect
from trading_core import load_kite_session, INDEX_REGISTRY

token_file = os.path.join(PROJECT_ROOT, "Trade_Option", "input", "kite_access_token.txt")
if not os.path.exists(token_file):
    token_file = os.path.join(PROJECT_ROOT, "Trade_Stock", "input", "kite_access_token.txt")

api_key, access_token = load_kite_session(token_file=token_file)
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Get NFO instrument dump
instruments = kite.instruments("NFO")
df_nfo = pd.DataFrame(instruments)

print("Total NFO instruments:", len(df_nfo))

# Filter NIFTY 26JUL 24000 CE & PE tokens
nifty_ce = df_nfo[(df_nfo['name'] == 'NIFTY') & (df_nfo['instrument_type'] == 'CE') & (df_nfo['strike'] == 24000)]
if not nifty_ce.empty:
    tok = int(nifty_ce.iloc[0]['instrument_token'])
    symbol = nifty_ce.iloc[0]['tradingsymbol']
    print(f"Testing Option Strike: {symbol} (Token: {tok})")
    candles = kite.historical_data(tok, "2026-07-20", "2026-07-25", "15minute")
    df_opt = pd.DataFrame(candles)
    print(df_opt.head())
    print("Option Candle Count:", len(df_opt))
