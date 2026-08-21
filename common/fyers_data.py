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
    "NIFTY50": "NSE:NIFTY50-INDEX",
    "NSE:NIFTY": "NSE:NIFTY50-INDEX",
    "NSE:NIFTY-INDEX": "NSE:NIFTY50-INDEX",
    "NSE:NIFTY50": "NSE:NIFTY50-INDEX",
    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
    "NIFTYBANK": "NSE:NIFTYBANK-INDEX",
    "NSE:BANKNIFTY": "NSE:NIFTYBANK-INDEX",
    "NSE:BANKNIFTY-INDEX": "NSE:NIFTYBANK-INDEX",
    "NSE:NIFTYBANK": "NSE:NIFTYBANK-INDEX",
    "SENSEX": "BSE:SENSEX-INDEX",
    "BSE:SENSEX": "BSE:SENSEX-INDEX",
    "FINNIFTY": "NSE:FINNIFTY-INDEX",
    "NSE:FINNIFTY": "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
    "NSE:MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
    "NIFTYIT": "NSE:NIFTYIT-INDEX",
    "NSE:NIFTYIT": "NSE:NIFTYIT-INDEX",
    "IT": "NSE:NIFTYIT-INDEX",
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
_cache_lock = threading.Lock()
_last_call_time = 0.0
_rate_limit_cooldown_until = 0.0
_candle_cache = {}
_option_chain_cache = {}
_last_prune_time = 0.0

def _prune_caches():
    global _last_prune_time
    now = time.time()
    if now - _last_prune_time < 60 and len(_candle_cache) < 300 and len(_option_chain_cache) < 300:
        return
    with _cache_lock:
        _last_prune_time = now
        # Prune candle cache older than 60s
        expired_candles = [k for k, v in list(_candle_cache.items()) if (now - v.get("ts", 0)) > 60]
        for k in expired_candles:
            _candle_cache.pop(k, None)
        # Prune option chain cache older than 120s
        expired_chains = [k for k, v in list(_option_chain_cache.items()) if (now - v.get("ts", 0)) > 120]
        for k in expired_chains:
            _option_chain_cache.pop(k, None)

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

EQUITY_ALIAS_MAP = {
    "TATAMOTORS": "TMPV",
    "NSE:TATAMOTORS": "NSE:TMPV",
    "NSE:TATAMOTORS-EQ": "NSE:TMPV-EQ",
    "TATAMOTOR": "TMPV",
    "M_M": "M&M",
    "BAJAJ_AUTO": "BAJAJ-AUTO",
    "MCDOWELL_N": "UNITDSPR",
}

def format_fyers_equity_symbol(raw_symbol):
    """Normalize a plain equity name to Fyers option-chain format (e.g. NSE:SBIN-EQ).
    Index names (NIFTY, BANKNIFTY, SENSEX etc.) are routed to their INDEX format.
    Option contract symbols (containing digits like NIFTY2681824200CE) pass through as-is.
    Handles special equity names like BAJAJ-AUTO, M&M, and TATAMOTORS -> TMPV correctly."""
    s = str(raw_symbol).strip().upper()
    if s in INDEX_SYMBOL_MAP:
        return INDEX_SYMBOL_MAP[s]
    if ":" in s:
        parts = s.split(":", 1)
        if parts[1] in INDEX_SYMBOL_MAP:
            return INDEX_SYMBOL_MAP[parts[1]]
    if s in EQUITY_ALIAS_MAP:
        s = EQUITY_ALIAS_MAP[s]
    if ":" in s:
        # Already fully-qualified
        parts = s.split(":", 1)
        name = parts[1]
        if name in EQUITY_ALIAS_MAP:
            name = EQUITY_ALIAS_MAP[name]
        # Already has suffix (-EQ, -INDEX, -BE, etc.) → pass through
        if name.endswith("-EQ") or name.endswith("-INDEX") or name.endswith("-BE"):
            base = name.rsplit("-", 1)[0]
            if base in EQUITY_ALIAS_MAP:
                return f"{parts[0]}:{EQUITY_ALIAS_MAP[base]}-EQ"
            return f"{parts[0]}:{name}"
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

    _prune_caches()
    cache_key = f"{fyers_symbol}:{resolution}:{lookback_days}"
    with _cache_lock:
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
                with _cache_lock:
                    _candle_cache[cache_key] = {"ts": time.time(), "df": df.copy()}
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
    _prune_caches()
    cache_key = f"{fyers_symbol}:{strikecount}"
    with _cache_lock:
        cached = _option_chain_cache.get(cache_key)
        if cached and (time.time() - cached["ts"]) < 30:
            return list(cached["data"])

    data = {
        "symbol": fyers_symbol,
        "strikecount": strikecount
    }

    for attempt in range(retries):
        try:
            res = _rate_limited_fyers_call(fyers.optionchain, data=data)
            if isinstance(res, dict) and res.get("s") == "ok":
                chain = res.get("data", {}).get("optionsChain", [])
                
                # Check for near-expiry rollover (<= 6 days to monthly expiry)
                expiry_data = res.get("data", {}).get("expiryData", [])
                if expiry_data and len(expiry_data) > 1:
                    try:
                        exp_str = expiry_data[0].get("date", "")
                        if exp_str:
                            exp_d = dt.strptime(exp_str, "%d-%b-%Y").date()
                            days_rem = (exp_d - dt.now().date()).days
                            if days_rem <= 6:
                                next_ts = expiry_data[1].get("expiry")
                                if next_ts:
                                    data_next = {"symbol": fyers_symbol, "strikecount": strikecount, "timestamp": next_ts}
                                    res_next = _rate_limited_fyers_call(fyers.optionchain, data=data_next)
                                    if isinstance(res_next, dict) and res_next.get("s") == "ok":
                                        next_chain = res_next.get("data", {}).get("optionsChain", [])
                                        if next_chain:
                                            # Include both current and next month contracts
                                            chain = list(chain) + list(next_chain)
                                            logging.debug(f"[FYERS EXPIRY ROLLOVER] {fyers_symbol}: {days_rem}d to {exp_str} -> Appended next month ({expiry_data[1].get('date')})")
                    except Exception as exp_err:
                        logging.debug(f"Fyers expiry parse note for {fyers_symbol}: {exp_err}")

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
                with _cache_lock:
                    _option_chain_cache[cache_key] = {"ts": time.time(), "data": list(valid_options)}
                return valid_options
            elif isinstance(res, dict) and (res.get("code") == 429 or "limit" in str(res.get("message", "")).lower() or "too many" in str(res.get("message", "")).lower()):
                time.sleep(0.5 * (attempt + 1))
                continue
            else:
                code = res.get("code") if isinstance(res, dict) else None
                if code in (-300, -470):
                    logging.debug(f"Fyers option chain not available for {fyers_symbol}: {res.get('message', '')}")
                else:
                    logging.warning(f"Fyers option chain failed for {fyers_symbol}: {res}")
                return []
        except Exception as e:
            logging.debug(f"Fyers option chain attempt {attempt+1} error for {fyers_symbol}: {e}")
            time.sleep(0.4 * (attempt + 1))

    return []
