# Wrapper for current (dhmongo.py) data storage I'm using to allow
# migration to a different storage solution in the future without
# massive overhaul of backtest, chart, and trader code bases.

# TODO rework this file to return non-pymongo objects in it's results
#      it's purpose is to abstract the mongo specifics away which it's not
#      currently doing, and instead act as an shim to translate the mongo
#      specifics into generic types that could still be used by anything
#      accessing the data even if I swapped out the storage layer entirely

import dhcharts as dhc
import dhutil as dhu
import dhmongo as dhm

# TODO these should be moved to dhmongo.py as they are mongo specific
#      this will require a bit of refactoring within functions below

COLL_TRADES = "trades"
COLL_IND_META = "indicators_meta"
COLL_IND = "indicators"

def store_trades(trade_name: str,
                 trade_version: str,
                 trades: list,
                 collection: str = COLL_TRADES):
    """Store one or more trades in central storage"""
    # TODO convert this to receive a Trade() object, possibly a list of them?

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
    # TODO update this to return an IndicatorDatapoint() object
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
    # TODO I really should set this up to receive an Indicator object
    #      and break it down rather than indivdiual params.  That was the
    #      whole point of having two files...

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

def store_candle(candle):
    """Write a single candle to central storage"""
    if not isinstance(candle, dhc.Candle):
        raise TypeError(f"candle {type(candle)} must be a "
                         "<class dhc.Candle> object")
    dhu.valid_timeframe(candle.c_timeframe)
    dhm.store_candle(c_datetime=candle.c_datetime,
                     c_timeframe=candle.c_timeframe,
                     c_open=candle.c_open,
                     c_high=candle.c_high,
                     c_low=candle.c_low,
                     c_close=candle.c_close,
                     c_volume=candle.c_volume,
                     c_symbol=candle.c_symbol,
                     c_epoch=candle.c_epoch,
                    )


def get_candles(start_epoch: int,
                end_epoch: int,
                timeframe: str,
                symbol: str,
                ):
    """Returns a list of candle docs within the start and end epochs given
    inclusive of both epochs"""
    result = dhm.get_candles(start_epoch=start_epoch,
                             end_epoch=end_epoch,
                             timeframe=timeframe,
                             symbol=symbol,
                            )

    candles = []
    for r in result:
        candles.append(dhc.Candle(c_datetime=r["c_datetime"],
                                  c_timeframe=r["c_timeframe"],
                                  c_open=r["c_open"],
                                  c_high=r["c_high"],
                                  c_low=r["c_low"],
                                  c_close=r["c_close"],
                                  c_volume=r["c_volume"],
                                  c_symbol=r["c_symbol"],
                                  c_epoch=r["c_epoch"],
                      ))

    return candles

def test_basics():
    print("Storing 2 test candles")
    tc1 = dhc.Candle(c_datetime="2024-02-10 09:20:00",
                     c_timeframe= "1m",
                     c_open= 5501.5,
                     c_high= 5510,
                     c_low= 5500.5,
                     c_close= 5510,
                     c_volume= 400,
                     c_symbol= "DELETEME",
                    )
    tc1.store()
    tc2 = dhc.Candle(c_datetime="2024-02-10 09:21:00",
                     c_timeframe= "1m",
                     c_open= 5503.5,
                     c_high= 5512,
                     c_low= 5500.5,
                     c_close= 5500,
                     c_volume= 600,
                     c_symbol= "DELETEME",
                    )
    tc2.store()
    print("Now let's retrieve them")
    result = get_candles(start_epoch=1704130201,
                         end_epoch=17044834300,
                         timeframe="1m",
                         symbol="DELETEME",
                         )
    for r in result:
        print(r.__dict__)
    print("And drop the test collection to clean up")
    dhm.drop_collection("candles_DELETEME_1m")

if __name__ == '__main__':
    test_basics()
