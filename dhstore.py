# Wrapper for current (dhmongo.py) data storage I'm using to allow
# migration to a different storage solution in the future without
# massive overhaul of backtest, chart, and trader code bases.

# TODO rework this file to return non-pymongo objects in it's results
#      it's purpose is to abstract the mongo specifics away which it's not
#      currently doing, and instead act as an shim to translate the mongo
#      specifics into generic types that could still be used by anything
#      accessing the data even if I swapped out the storage layer entirely

import dhmongo as dhm

COLL_TRADES = "trades"
COLL_IND_META = "indicators_meta"
COLL_IND = "indicators"

def store_trades(trade_name: str,
                 trade_version: str,
                 trades: list,
                 collection: str = COLL_TRADES):
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

def list_indicators(meta_collection: str = COLL_IND_META):
    result = dhm.list_indicators(meta_collection=meta_collection)

    return result

def get_indicator_datapoints(indicator_id: str,
                             dp_collection: str = COLL_IND,
                             earliest_dt: str = "",
                             latest_dt: str = "",
                             ):
    result = dhm.get_indicator_datapoints(indicator_id=indicator_id,
                                          dp_collection=dp_collection,
                                          earliest_dt=earliest_dt,
                                          latest_dt=latest_dt,
                                          )

    return result

def store_indicator(indicator_id:str,
                    short_name: str,
                    long_name: str,
                    description: str,
                    timeframe: str,
                    trading_hours: str,
                    symbol: str,
                    calc_version: str,
                    calc_details: str,
                    datapoints: list,
                    meta_collection: str = COLL_IND_META,
                    dp_collection: str = COLL_IND):
    """Store indicator meta and datapoints in central storage"""
    # make a working copy
    working_dp = []
    for d in datapoints:
        working_dp.append(d.to_clean_dict())
    result = dhm.store_indicators(indicator_id=indicator_id,
                                  short_name=short_name,
                                  long_name=long_name,
                                  description=description,
                                  timeframe=timeframe,
                                  trading_hours=trading_hours,
                                  symbol=symbol,
                                  calc_version=calc_version,
                                  calc_details=calc_details,
                                  datapoints=working_dp,
                                  meta_collection=meta_collection,
                                  dp_collection=dp_collection,
                                  )

    return result

