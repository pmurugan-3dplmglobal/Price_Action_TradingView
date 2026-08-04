import os
import sys
import time
import logging
from datetime import datetime as dt

COMMON_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "common"))
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

from automated_strategy_exporter import execute_scheduled_export

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("output/logs/export_scheduler_daemon.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Scheduled slots and target times
TARGET_SLOTS = [
    {"slot": "10_30_AM", "hour": 10, "minute": 30},
    {"slot": "01_00_PM", "hour": 13, "minute": 0},
    {"slot": "03_15_PM", "hour": 15, "minute": 15},
]

executed_today = set()

def main():
    logging.info("Starting Automated Strategy Export Daemon...")
    print("=========================================================")
    print(" Automated Strategy Export Daemon Active")
    print(" Monitoring clock for slots: 10:30 AM, 1:00 PM, 3:15 PM")
    print("=========================================================")

    current_day = dt.now().strftime("%Y-%m-%d")

    while True:
        try:
            now = dt.now()
            today_str = now.strftime("%Y-%m-%d")

            # Reset executed set at midnight
            if today_str != current_day:
                current_day = today_str
                executed_today.clear()
                logging.info(f"New day detected: {current_day}. Resetting schedule state.")

            for slot_info in TARGET_SLOTS:
                slot_key = f"{today_str}_{slot_info['slot']}"
                if slot_key in executed_today:
                    continue

                target_time = now.replace(hour=slot_info['hour'], minute=slot_info['minute'], second=0, microsecond=0)
                # If current time is past or within target window (up to 15 mins after target time)
                if now >= target_time and (now - target_time).total_seconds() < 900:
                    logging.info(f"[DAEMON TRIGGER] Triggering export for slot [{slot_info['slot']}] at {now.strftime('%H:%M:%S')}")
                    execute_scheduled_export(slot_name=slot_info['slot'])
                    executed_today.add(slot_key)

            time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Export daemon stopped by user.")
            print("\nExport daemon stopped.")
            break
        except Exception as e:
            logging.error(f"Daemon loop error: {e}")
            time.sleep(15)

if __name__ == "__main__":
    main()
