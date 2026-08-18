import os
import time
import logging
import threading
import pandas as pd
from datetime import datetime as dt, timedelta

try:
    from common.fyers_session import get_fyers_session
except ImportError:
    from fyers_session import get_fyers_session

INDEX_SYMBOL_MAP = {
    "NIFTY": "NSE:NIFTY50-INDEX",
    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
    "SENSEX": "BSE:SENSEX-INDEX",
    "FINNIFTY": "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
}

TIMEFRAME_MAP = {
    "1minute": "1",
    "1m": "1",
    "3minute": "3",
    "3m": "3",
    "5minute": "5",
    "5m": "5",
    "10minute": "10",
    "10m": "10",
    "15minute": "15",
    "15m": "15",
    "30minute": "30",
    "30m": "30",
    "60minute": "60",
    "60m": "60",
    "75min": "75",
    "75minute": "75",
    "day": "D",
    "1d": "D",
    "daily": "D",
}

_fyers_lock = threading.Lock()
_last_call_time = 0.0
_candle_cache = {}

def format_fyers_symbol(raw_symbol):
    """Normalize raw symbol to Fyers exchange:symbol format."""
    s = str(raw_symbol).strip().upper()
    if ":" in s:
        return s
    if s in INDEX_SYMBOL_MAP:
        return INDEX_SYMBOL_MAP[s]
    if "SENSEX" in s or "BSE" in s:
        return f"BSE:{s}"
    return f"NSE:{s}"

def _rate_limited_fyers_call(call_func, *args, **kwargs):
    """Ensures calls to Fyers history/optionchain obey the 10 req/s rate limit."""
    global _last_call_time
    with _fyers_lock:
        elapsed = time.time() - _last_call_time
        if elapsed < 0.12:
            time.sleep(0.12 - elapsed)
        _last_call_time = time.time()
        return call_func(*args, **kwargs)

def fetch_fyers_candles(symbol, timeframe="15minute", lookback_days=30, retries=3):
    """
    Fetches historical OHLCV candles from Fyers API with rate-limit protection,
    in-memory short cache, and returns standard DataFrame.
    """
    fyers = get_fyers_session()
    if not fyers:
        logging.warning(f"Fyers session not active when fetching candles for {symbol}")
        return None

    fyers_symbol = format_fyers_symbol(symbol)
    resolution = TIMEFRAME_MAP.get(str(timeframe).lower(), "15")

    cache_key = f"{fyers_symbol}:{resolution}:{lookback_days}"
    cached = _candle_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < 10:
        return cached["df"].copy()

    now = dt.now()
    range_to = now.strftime("%Y-%m-%d")
    range_from = (now - timedelta(days=max(lookback_days, 5))).strftime("%Y-%m-%d")

    data = {
        "symbol": fyers_symbol,
        "resolution": resolution,
        "date_format": "1",
        "range_from": range_from,
        "range_to": range_to,
        "cont_flag": "1"
    }

    for attempt in range(retries):
        try:
            res = _rate_limited_fyers_call(fyers.history, data=data)
            if isinstance(res, dict) and res.get("s") == "ok" and res.get("candles"):
                raw_candles = res["candles"]
                records = []
                for c in raw_candles:
                    ts = dt.fromtimestamp(c[0]).strftime("%Y-%m-%d %H:%M:%S+05:30")
                    records.append({
                        "date": ts,
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "volume": int(c[5])
                    })

                df = pd.DataFrame(records)
                df["date"] = pd.to_datetime(df["date"])
                df.sort_values("date", inplace=True)
                df.reset_index(drop=True, inplace=True)
                _candle_cache[cache_key] = {"ts": time.time(), "df": df}
                return df
            elif isinstance(res, dict) and "limit" in str(res.get("message", "")).lower():
                time.sleep(0.3 * (attempt + 1))
                continue
            else:
                return None
        except Exception as e:
            logging.error(f"Fyers candle fetch attempt {attempt+1} failed for {fyers_symbol}: {e}")
            time.sleep(0.2)
    return None

def fetch_fyers_option_chain(underlying_symbol, strikecount=3):
    """
    Fetches real-time option chain for an index or equity from Fyers.
    Returns list of option contract dicts.
    """
    fyers = get_fyers_session()
    if not fyers:
        return []

    fyers_symbol = format_fyers_symbol(underlying_symbol)
    data = {
        "symbol": fyers_symbol,
        "strikecount": strikecount
    }

    try:
        res = _rate_limited_fyers_call(fyers.optionchain, data=data)
        if isinstance(res, dict) and res.get("s") == "ok":
            chain = res.get("data", {}).get("optionsChain", [])
            valid_options = []
            for item in chain:
                opt_type = item.get("option_type", "").upper()
                if opt_type in ("CE", "PE"):
                    valid_options.append({
                        "symbol": item.get("symbol"),
                        "strike": float(item.get("strike_price", 0)),
                        "option_type": opt_type,
                        "ltp": float(item.get("ltp", 0)),
                        "volume": item.get("volume", 0),
                        "oi": item.get("oi", 0),
                        "delta": item.get("delta", 0.5)
                    })
            return valid_options
        else:
            logging.warning(f"Fyers option chain failed for {fyers_symbol}: {res}")
            return []
    except Exception as e:
        logging.error(f"Fyers option chain error for {fyers_symbol}: {e}")
        return []
