from datetime import datetime as dt

TIMEFRAMES = ['1m', '5m', '15m', '1h', '1d', '1w', '1mo']
TRADING_HOURS = ['rth', 'eth']

def valid_timeframe(t, exit=True):
    if t in TIMEFRAMES:
        return True
    else:
        if exit:
            raise ValueError(f"{t} is not a valid timeframe in {TIMEFRAMES}")
        return False


def dt_as_dt(d):
    """return a datetime object regardless of datetime or string input"""
    if isinstance(d, dt):
        return d
    else:
        return dt.strptime(d, "%Y-%m-%d %H:%M:%S")


def dt_as_str(d):
    """return a string object regardless of datetime or string input"""
    if isinstance(d, str):
        return d
    else:
        return d.strftime("%Y-%m-%d %H:%M:%S")


def dt_to_epoch(d):
    """return an epoch integer from a datetime or string"""
    return int(dt_as_dt(d).strftime('%s'))


def dt_from_epoch(d):
    """return a datetime object from an epoch integer"""
    return dt.fromtimestamp(d)


def test_basics():
    ts = "2024-01-01 12:30:00"
    print(f"Starting with string: {ts} {type(ts)}")
    td = dt_as_dt(ts)
    print(f"Converted to datetime: {td} {type(td)}")
    tds = dt_as_str(td)
    print(f"Converted back to string: {tds} {type(tds)}")
    tse = dt_to_epoch(ts)
    print(f"String converted to epoch: {tse} {type(tse)}")
    tde = dt_to_epoch(td)
    print(f"Datetime converted to epoch: {tde} {type(tde)}")
    t=dt_from_epoch(tde)
    print(f"Epoch converted back to datetime: {t} {type(t)}")
    print("Testing valid timeframe")
    print(valid_timeframe('1m'))
    print("Testing an invalid timeframe, should raise an exception and exit")
    print(valid_timeframe('20m'))

if __name__ == '__main__':
    test_basics()

