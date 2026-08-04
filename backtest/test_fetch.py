import requests
import pandas as pd

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
url = "https://query1.finance.yahoo.com/v8/finance/chart/^NSEI?range=1mo&interval=15m"
try:
    r = requests.get(url, headers=headers, timeout=5)
    print("STATUS:", r.status_code)
    if r.status_code == 200:
        data = r.json()
        result = data['chart']['result'][0]
        timestamps = result['timestamp']
        quote = result['indicators']['quote'][0]
        df = pd.DataFrame({
            'date': [pd.to_datetime(ts, unit='s').tz_localize('UTC').tz_convert('Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps],
            'close': quote['close']
        })
        print(df.head())
        print("TOTAL CANDLES:", len(df))
except Exception as e:
    print("ERROR:", e)
