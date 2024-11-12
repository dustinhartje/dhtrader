from datetime import datetime as dt

TIMEFRAMES = ['1m', '5m', '15m', '1h', '1d', '1w', '1mo']
TRADING_HOURS = ['regular', 'extended']

def valid_timeframe(t):
    if t in TIMEFRAMES:
        return True
    else:
        return False

def dt_as_dt(d):
    if isinstance(d, dt):
        return d
    else:
        return dt.strptime(d, "%Y-%m-%d %H:%M:%S")


def dt_as_str(d):
    if isinstance(d, str):
        return d
    else:
        return d.strftime("%Y-%m-%d %H:%M:%S")
