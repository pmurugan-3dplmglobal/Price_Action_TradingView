import os
import json
import traceback
import logging
from datetime import datetime
from kiteconnect import KiteConnect

# ==============================================================================
# LOGGING SETUP (Saves to token_generation.log)
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("output/logs/token_generation.log", mode="w", encoding="utf-8"),
        logging.StreamHandler() # Keeps printing to screen if window stays open
    ]
)

# ==============================================================================
# CREDENTIALS CONFIGURATION
# ==============================================================================
CONFIG_FILE = "input/program_config.json"
TOKEN_FILE = "input/kite_access_token.txt"

def get_kite_credentials():
    """Read Kite API key/secret from program_config.json (moved out of source)."""
    api_key, api_secret = "", ""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                cfg = json.load(f)
            api_key = cfg.get("api_key", "")
            api_secret = cfg.get("api_secret", "")
        except Exception:
            pass
    return api_key, api_secret

def main():
    current_directory = os.getcwd()
    absolute_token_path = os.path.abspath(TOKEN_FILE)
    
    logging.info("================================================================================")
    logging.info("🔍 DIAGNOSTIC SYSTEM INITIALIZATION")
    logging.info(f"   • Current Script Directory: {current_directory}")
    logging.info(f"   • Target File Storage Path: {absolute_token_path}")
    logging.info("================================================================================")

    api_key, api_secret = get_kite_credentials()
    if not api_key or not api_secret:
        logging.error("api_key/api_secret missing in program_config.json")
        return

    try:
        kite = KiteConnect(api_key=api_key)
    except Exception as e:
        logging.error(f"Failed to initialize KiteConnect library wrapper: {e}")
        return

    # 1. Generate and display the authentication URL
    login_url = f"https://kite.zerodha.com/connect/login?api_key={api_key}"
    logging.info("\n🌐 STEP 1: Copy and paste this URL into your browser:")
    logging.info("-" * 80)
    logging.info(login_url)
    logging.info("-" * 80)
    
    # 2. Capture the redirect URI parameters from the browser address bar
    logging.info("\n🔐 STEP 2: Log in. After the page redirects, copy the FULL URL from your address bar.")
    try:
        redirect_response = input("Paste the entire redirect URL here: ").strip()
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user input block close request.")
        return
    
    try:
        logging.info("⚙️ Processing string parameters from redirection data...")
        # Extract the request_token from the pasted URL string
        if "request_token=" in redirect_response:
            request_token = redirect_response.split("request_token=")[1].split("&")[0]
            logging.info("   [LOG] URL splitting parsed token successfully.")
        else:
            request_token = redirect_response  
            logging.info("   [LOG] Raw token input fallthrough detected.")
            
        logging.info(f"📡 Request Token Extracted: '{request_token}'")
        logging.info("Executing handshake session creation with Zerodha servers...")
        
     
 		# 3. Exchange request token for a permanent daily access token
        session = kite.generate_session(request_token, api_secret=api_secret)
        access_token = session["access_token"]
        logging.info("   [LOG] Handshake secure validation achieved from server API endpoints.")

        # 4. Save the dictionary locally to file
        token_data = {
            "api_key": api_key,
            "access_token": access_token,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        logging.info(f"💾 Attempting filesystem block authorization lock to: {TOKEN_FILE}...")
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f, indent=4)
            
        logging.info("\n================================================================================")
        logging.info("🎉 SUCCESS! Access Token successfully written to target destination.")
        logging.info(f"   • File verification location: {os.path.abspath(TOKEN_FILE)}")
        logging.info(f"   • File Size On Disk: {os.path.getsize(TOKEN_FILE)} bytes")
        logging.info("================================================================================")
        logging.info("You can now close this script and execute your trading files safely.")
        
    except Exception as e:
        logging.error("\n❌ HANDSHAKE CRITICAL EXCEPTION ENCOUNTERED:")
        logging.error("-" * 80)
        # Formats the trace crash logs so they parse neatly into the text file log
        error_trace = traceback.format_exc()
        logging.error(error_trace)
        logging.error("-" * 80)

if __name__ == "__main__":
    main()