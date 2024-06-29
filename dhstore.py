# Wrapper for current (dhmongo.py) data storage I'm using to allow
# migration to a different storage solution in the future without
# massive overhaul of backtest, chart, and trader code bases.

from . import dhmongo as dhm


def store_trades(trade_name: str,
                 trade_version: str,
                 trades: list,
                 collection: str = "trades"):
    """Store one or more trades in central storage"""
    # make a working copy
    working_trades = []
    for t in trades:
        t.trade_version = trade_version
        t.trade_name = trade_name
        working_trades.append(t.to_clean_dict())
    result = dhm.store_trades(trades=working_trades,
                              collection=collection)
    return result
