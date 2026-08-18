import os
import json
import logging
from fyers_apiv3 import fyersModel

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(BASE_DIR, "input", "fyers_config.json")
TOKEN_FILE = os.path.join(BASE_DIR, "input", "fyers_access_token.txt")
LOG_DIR = os.path.join(BASE_DIR, "output", "logs")

_fyers_instance = None

def load_fyers_config():
    if not os.path.exists(CONFIG_FILE):
        return None
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Failed to load Fyers config: {e}")
        return None

def load_fyers_token():
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            token = f.read().strip()
            return token if token else None
    except Exception as e:
        logging.warning(f"Failed to read Fyers token file: {e}")
        return None

def get_fyers_session(force_refresh=False):
    """Returns an authenticated FyersModel client instance if valid token exists."""
    global _fyers_instance
    if _fyers_instance is not None and not force_refresh:
        return _fyers_instance

    cfg = load_fyers_config()
    token = load_fyers_token()
    if not cfg or not token:
        return None

    client_id = cfg.get("client_id")
    if not client_id:
        return None

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path_with_sep = os.path.abspath(LOG_DIR) + os.sep

    try:
        fyers = fyersModel.FyersModel(
            client_id=client_id,
            token=token,
            is_async=False,
            log_path=log_path_with_sep
        )
        profile = fyers.get_profile()
        if isinstance(profile, dict) and profile.get("s") == "ok":
            _fyers_instance = fyers
            return _fyers_instance
        else:
            logging.warning(f"Fyers profile check failed: {profile}")
            return None
    except Exception as e:
        logging.error(f"Fyers session initialization failed: {e}")
        return None

def is_fyers_authenticated():
    session = get_fyers_session()
    return session is not None
