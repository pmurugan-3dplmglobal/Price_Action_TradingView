# Common Equity Universe & Liquidity Shield Manager
import logging
import pandas as pd

# ──────────────────────────────────────────────
#  PREDEFINED NSE INDICES CONSTITUENTS
# ──────────────────────────────────────────────

NIFTY50_SYMBOLS = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
    "BPCL", "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY",
    "EICHERMOT", "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK",
    "INDUSINDBK", "INFY", "ITC", "JSWSTEEL", "KOTAKBANK",
    "LT", "LTIM", "M&M", "MARUTI", "NESTLEIND",
    "NTPC", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE",
    "SBIN", "SHRIRAMFIN", "SUNPHARMA", "TATACONSUM", "TATAMOTORS",
    "TATASTEEL", "TCS", "TECHM", "TITAN", "ULTRACEMCO", "WIPRO"
]

NIFTY_NEXT100_SYMBOLS = [
    "ABB", "ACC", "ADANIENSOL", "ADANIGREEN", "ADANIPOWER",
    "ATGL", "AMBUJACEM", "BANKBARODA", "BERGEPAINT", "BOSCHLTD",
    "CANBK", "CHOLAFIN", "COLPAL", "DLF", "DMART",
    "GAIL", "GODREJCP", "HAL", "HAVELLES", "ICICIGI",
    "ICICIPRULI", "IOC", "IRCTC", "IRFC", "JINDALSTEL",
    "JIOFIN", "LODHA", "MAXHEALTH", "NAUKRI", "NHPC",
    "NMDC", "OBEROIRLTY", "OIL", "PAYTM", "PFC",
    "PIDILITIND", "PNB", "RECLTD", "RVNL", "SIEMENS",
    "SRF", "TATAELXSI", "TATAPOWER", "TORNTPOWER", "TRENT",
    "TVSMOTOR", "UNITDSPR", "VBL", "VEDL", "ZYDUSLIFE"
]

NIFTY_MIDCAP100_SYMBOLS = [
    "AARTIIND", "ABCAPITAL", "ABFRL", "ALKEM", "APLAPOLLO",
    "ASTRAL", "AUROPHARMA", "BALKRISIND", "BANDHANBNK", "BHARATFORG",
    "BSOFT", "CGPOWER", "COFORGE", "CONCOR", "CUMMINSIND",
    "DALBHARAT", "DIXON", "ESCORTS", "FEDERALBNK", "FORTIS",
    "GLENMARK", "GMRINFRA", "GODREJPROP", "GUJGASLTD", "IDFCFIRSTB",
    "INDIANB", "INDHOTEL", "INDUSTOWER", "IPCALAB", "JISLJALEQS",
    "JUBLFOOD", "KEI", "KPITTECH", "LICHSGFIN", "LUPIN",
    "M&MFIN", "MFSL", "MPHASIS", "MRF", "MUTHOOTFIN",
    "NATIONALUM", "NAVINFLUOR", "OBEROIRLTY", "OFSS", "PAGEIND",
    "PERSISTENT", "PETRONET", "POLYCAB", "POONAWALLA", "PRESTIGE",
    "SAIL", "SCHAEFFLER", "SOLARINDS", "SONACOMS", "SUNDARMFIN",
    "SUPREMEIND", "SYNGENE", "TATACHEM", "TATACOMM", "TIINDIA",
    "TORNTPHARM", "VOLTAS", "WHIRLPOOL", "YESBANK", "ZEEL"
]

NIFTY_SMALLCAP250_SYMBOLS = [
    "ANGELONE", "APARINDS", "BECTORFOOD", "BATAINDIA", "BLUESTARCO",
    "CAMPUS", "CDSL", "CEATLTD", "CENTURYPLY", "CERA",
    "CESC", "CHAMBLFERT", "CIEINDIA", "CROMPTON", "CYIENT",
    "DATAPATTNS", "DEEPAKNTR", "DELHIVERY", "DEVYANI", "ECLERX",
    "EQUITASBNK", "EXIDEIND", "FINPIPE", "FIRSTSOURCE", "FIVESTAR",
    "GLS", "GNFC", "GODFRYPHLP", "GRANULES", "HAPPSTMNDS",
    "HFCL", "HOMEFIRST", "HONASA", "IDBI", "IIFL",
    "IEX", "INDRAMEDCO", "INTELLECT", "JBCHEPHARM", "JKCEMENT",
    "KAYNES", "KEC", "KALYANKJIL", "KARURVYSYA", "LALPATHLAB",
    "LATENTVIEW", "LEMONTREE", "LXCHEM", "MAPMYINDIA", "MASTEK",
    "METROPOLIS", "MINDACORP", "MSUMI", "NATCOPHARM", "NIPPONLIFE",
    "PRAJIND", "RADICO", "RAIRAIL", "ROUTE", "RITES",
    "SAPPDIR", "SONATSOFTW", "SUVENPHAR", "TANLA", "TATAINVEST",
    "UCOBANK", "UTIAMC", "VIPIND", "ZENSARTECH"
]

INDICES_REGISTRY_MAP = {
    "NIFTY50": NIFTY50_SYMBOLS,
    "NIFTY_NEXT_100": NIFTY_NEXT100_SYMBOLS,
    "NIFTY_MIDCAP_100": NIFTY_MIDCAP100_SYMBOLS,
    "NIFTY_SMALLCAP_250": NIFTY_SMALLCAP250_SYMBOLS
}

# ──────────────────────────────────────────────
#  TOKEN RESOLUTION & UNIVERSE LOOKUP
# ──────────────────────────────────────────────

_NSE_TOKEN_CACHE = {}

def get_universe_symbols_and_tokens(kite=None, target_index_name="NIFTY50"):
    """
    Returns (symbols_list, token_map) for requested index universe.
    Integrates with Kite Connect API to resolve instrument tokens dynamically.
    """
    global _NSE_TOKEN_CACHE
    from trading_core import STOCK_REGISTRY

    symbols = INDICES_REGISTRY_MAP.get(target_index_name)
    if not symbols:
        symbols = sorted(list(STOCK_REGISTRY.keys()))

    token_map = {}
    for sym in symbols:
        if sym in STOCK_REGISTRY and STOCK_REGISTRY[sym].get("token"):
            token_map[sym] = STOCK_REGISTRY[sym]["token"]

    missing_symbols = [s for s in symbols if s not in token_map or token_map[s] == 0]
    acc_t = getattr(kite, "access_token", "") if kite else ""
    if missing_symbols and kite and acc_t and acc_t != "open_source_token":
        if not _NSE_TOKEN_CACHE:
            try:
                logging.info(f"Fetching NSE instrument master for universe '{target_index_name}' ({len(missing_symbols)} missing tokens)...")
                insts = kite.instruments("NSE")
                for item in insts:
                    if item.get("segment") == "NSE":
                        _NSE_TOKEN_CACHE[item["tradingsymbol"].strip()] = int(item["instrument_token"])
                logging.info(f"Cached {len(_NSE_TOKEN_CACHE)} NSE instrument tokens")
            except Exception as e:
                logging.error(f"Failed to fetch NSE instruments for token map: {e}")

        for sym in missing_symbols:
            if sym in _NSE_TOKEN_CACHE:
                token_map[sym] = _NSE_TOKEN_CACHE[sym]

    return symbols, token_map

# ──────────────────────────────────────────────
#  LIQUIDITY & TURNOVER SHIELD
# ──────────────────────────────────────────────

def is_liquid_cash_stock(df, min_volume=500000, min_turnover_cr=10.0):
    """
    Evaluates whether a cash stock meets minimum daily volume and turnover shields
    to prevent illiquid slippage or low-cap trap setups.
    """
    if df is None or df.empty or len(df) < 5:
        return False
    try:
        avg_vol = float(df['volume'].tail(20).mean()) if 'volume' in df.columns else 0
        avg_close = float(df['close'].tail(20).mean()) if 'close' in df.columns else 0
        turnover_cr = (avg_vol * avg_close) / 10_000_000.0
        
        if avg_vol < min_volume and turnover_cr < min_turnover_cr:
            return False
        return True
    except Exception as e:
        logging.warning(f"Liquidity check exception: {e}")
        return True
