import os
import time
import logging
import threading
import pandas as pd
from datetime import datetime as dt, timedelta, timezone

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
    "4hr": "240",
    "4hour": "240",
    "4h": "240",
    "240min": "240",
    "240minute": "240",
    "day": "D",
    "1d": "D",
    "daily": "D",
}

_fyers_lock = threading.Lock()
_last_call_time = 0.0
_rate_limit_cooldown_until = 0.0
_candle_cache = {}
_option_chain_cache = {}

IST_TZ = timezone(timedelta(hours=5, minutes=30))

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

def format_fyers_equity_symbol(raw_symbol):
    """Normalize a plain equity name to Fyers option-chain format (e.g. NSE:SBIN-EQ).
    Index names (NIFTY, BANKNIFTY, SENSEX etc.) are routed to their INDEX format.
    Option contract symbols (containing digits like NIFTY2681824200CE) pass through as-is.
    Handles special equity names like BAJAJ-AUTO and M&M correctly."""
    s = str(raw_symbol).strip().upper()
    if ":" in s:
        # Already fully-qualified
        parts = s.split(":", 1)
        name = parts[1]
        # Already has suffix (-EQ, -INDEX, -BE, etc.) → pass through
        if name.endswith("-EQ") or name.endswith("-INDEX") or name.endswith("-BE"):
            return s
        # Index symbol values (NIFTY50-INDEX etc.) in INDEX_SYMBOL_MAP values
        if s in INDEX_SYMBOL_MAP.values():
            return s
        # Contains digits → option contract (e.g. NSE:NIFTY2681824200CE) → pass through
        if any(ch.isdigit() for ch in name):
            return s
        # Plain equity name without -EQ (e.g. NSE:SBIN, NSE:BAJAJ-AUTO) → add -EQ
        return f"{parts[0]}:{name}-EQ"
    if s in INDEX_SYMBOL_MAP:
        return INDEX_SYMBOL_MAP[s]
    # No colon: check if it has digits → option contract symbol
    if any(ch.isdigit() for ch in s):
        return f"NSE:{s}"
    # No digits → equity name (SBIN, BAJAJ-AUTO, M&M, POWERGRID) → add -EQ
    return f"NSE:{s}-EQ"

def _rate_limited_fyers_call(call_func, *args, **kwargs):
    """Ensures calls to Fyers history/optionchain obey safe 5 req/s rate limits with dynamic 429 cooldown."""
    global _last_call_time, _rate_limit_cooldown_until
    with _fyers_lock:
        now = time.time()
        # If currently in a rate limit cooldown, wait it out
        if now < _rate_limit_cooldown_until:
            wait_time = _rate_limit_cooldown_until - now
            time.sleep(wait_time)

        now = time.time()
        elapsed = now - _last_call_time
        if elapsed < 0.20:
            time.sleep(0.20 - elapsed)
        _last_call_time = time.time()
        
        try:
            res = call_func(*args, **kwargs)
            # If rate limit 429 response received, trigger global cooldown
            if isinstance(res, dict):
                code = res.get("code")
                msg = str(res.get("message", "")).lower()
                if code == 429 or "limit" in msg or "too many" in msg:
                    _rate_limit_cooldown_until = time.time() + 1.2
            return res
        except Exception as e:
            err_str = str(e).lower()
            if "429" in err_str or "limit" in err_str:
                _rate_limit_cooldown_until = time.time() + 1.2
            raise

def fetch_fyers_candles(symbol, timeframe="15minute", lookback_days=30, retries=4):
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
    if cached and (time.time() - cached["ts"]) < 15:
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
                    ts = dt.fromtimestamp(c[0], tz=IST_TZ).strftime("%Y-%m-%d %H:%M:%S+05:30")
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
            elif isinstance(res, dict) and (res.get("code") == 429 or "limit" in str(res.get("message", "")).lower()):
                time.sleep(0.4 * (attempt + 1))
                continue
            else:
                return None
        except Exception as e:
            logging.debug(f"Fyers candle fetch attempt {attempt+1} failed for {fyers_symbol}: {e}")
            time.sleep(0.3 * (attempt + 1))
    return None

def fetch_fyers_option_chain(underlying_symbol, strikecount=3, retries=4):
    """
    Fetches real-time option chain for an index or equity from Fyers.
    Features 30-second TTL in-memory caching and automatic 429 backoff retry loop.
    Returns list of option contract dicts.
    """
    fyers = get_fyers_session()
    if not fyers:
        return []

    fyers_symbol = format_fyers_equity_symbol(underlying_symbol)
    cache_key = f"{fyers_symbol}:{strikecount}"
    cached = _option_chain_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < 30:
        return cached["data"]

    data = {
        "symbol": fyers_symbol,
        "strikecount": strikecount
    }

    for attempt in range(retries):
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
                _option_chain_cache[cache_key] = {"ts": time.time(), "data": valid_options}
                return valid_options
            elif isinstance(res, dict) and (res.get("code") == 429 or "limit" in str(res.get("message", "")).lower() or "too many" in str(res.get("message", "")).lower()):
                time.sleep(0.5 * (attempt + 1))
                continue
            else:
                logging.warning(f"Fyers option chain failed for {fyers_symbol}: {res}")
                return []
        except Exception as e:
            logging.debug(f"Fyers option chain attempt {attempt+1} error for {fyers_symbol}: {e}")
            time.sleep(0.4 * (attempt + 1))

    return []
