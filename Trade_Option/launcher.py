import sys
import webbrowser
import subprocess
import os

DASHBOARD_URL = "http://localhost:6060"

def main():
    print("=" * 60)
    print("  TRADING SYSTEM LAUNCHER")
    print("  Multi-Pattern Strategy Suite")
    print("=" * 60)
    print()
    print("  The Trading Control Center web dashboard")
    print("  is the new primary interface.")
    print()
    print(f"  Opening {DASHBOARD_URL} in your browser...")
    print()
    print("  From the dashboard you can:")
    print("    - Start/Stop programs in parallel")
    print("    - View live reports per program")
    print("    - Monitor positions, journal, logs")
    print()
    print("  Starting web server...")
    print()

    webbrowser.open(DASHBOARD_URL)
    subprocess.run([sys.executable, "app.py"])

if __name__ == "__main__":
    main()
