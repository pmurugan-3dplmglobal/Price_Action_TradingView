import os
import sys
import json
import hashlib
import requests
import urllib.parse
from fyers_apiv3 import fyersModel

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "input", "fyers_config.json")
TOKEN_FILE = os.path.join(BASE_DIR, "input", "fyers_access_token.txt")

def load_fyers_config():
    if not os.path.exists(CONFIG_FILE):
        print(f"[ERROR] Config file not found: {CONFIG_FILE}")
        return None
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load {CONFIG_FILE}: {e}")
        return None

def exchange_auth_code(auth_input):
    cfg = load_fyers_config()
    if not cfg:
        return None

    client_id = cfg.get("client_id", "GWDYN0AZW1-200").strip()
    secret_key = cfg.get("secret_key", "").strip()

    auth_code = auth_input.strip()
    if "auth_code=" in auth_code:
        parts = auth_code.split("auth_code=")
        if len(parts) > 1:
            auth_code = parts[1].split("&")[0].split("]")[0].strip()

    candidates = [
        client_id,                         # GWDYN0AZW1-200
        client_id.replace("-200", "-100"), # GWDYN0AZW1-100
        client_id.split("-")[0],           # GWDYN0AZW1
    ]
    seen = set()
    unique_candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    url = "https://api-t1.fyers.in/api/v3/validate-authcode"
    for cid in unique_candidates:
        app_id_hash = hashlib.sha256(f"{cid}:{secret_key}".encode()).hexdigest()
        data = {
            "grant_type": "authorization_code",
            "appIdHash": app_id_hash,
            "code": auth_code,
        }
        try:
            res = requests.post(url, json=data, timeout=10)
            resp = res.json()
            if isinstance(resp, dict) and resp.get("s") == "ok":
                access_token = resp.get("access_token")
                os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
                with open(TOKEN_FILE, "w", encoding="utf-8") as f:
                    f.write(access_token)
                print(f"\n[SUCCESS] Access token generated & saved using appId '{cid}'!")
                print(f"Token: {access_token[:20]}...")
                return access_token
            else:
                print(f"[DEBUG] Attempt with '{cid}' -> {resp}")
        except Exception as e:
            print(f"[ERROR] Request failed: {e}")

    return None

if __name__ == "__main__":
    if len(sys.argv) > 1:
        exchange_auth_code(sys.argv[1])
    else:
        auth_input = input("Paste auth_code or Redirect URL: ").strip()
        exchange_auth_code(auth_input)
