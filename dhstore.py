# Wrapper for current (dhmongo.py) data storage I'm using to allow
# migration to a different storage solution in the future without
# massive overhaul of backtest, chart, and trader code bases.

# TODO rework this file to return non-pymongo objects in it's results
#      it's purpose is to abstract the mongo specifics away which it's not
#      currently doing, and instead act as an shim to translate the mongo
#      specifics into generic types that could still be used by anything
#      accessing the data even if I swapped out the storage layer entirely
# TODO also review each function, they should accept dhcharts objects for
#      the most part rather than detailed arguments to reduce duplication
#      and be more shim-ish in nature

import csv
from collections import Counter
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


def store_indicator(indicator_id: str,
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


def load_candles_from_csv(filepath: str,
                          start_dt,
                          end_dt,
                          ):
    """Loads 1m candles from a CSV file into central storage"""


def store_candle(candle):
    """Write a single dhcharts.Candle() to central storage"""
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
                     c_date=candle.c_date,
                     c_time=candle.c_time,
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


def review_candles(timeframe: str,
                   symbol: str,
                   check_integrity: bool = False,
                   ):
    """Provides aggregate summary data about candles in central storage
    with options to further check completeness (integrity) of candle data"""
    overview = dhm.review_candles(timeframe=timeframe,
                                  symbol=symbol,
                                  )
    start_epoch = dhu.dt_to_epoch(overview["earliest_dt"])
    end_epoch = dhu.dt_to_epoch(overview["latest_dt"])
    if check_integrity:
        candles = get_candles(timeframe=timeframe,
                              symbol=symbol,
                              start_epoch=start_epoch,
                              end_epoch=end_epoch,
                              )

        # Perform a basic check on the times list vs expected for the timeframe
        breakdown = dhu.summarize_candles(timeframe=timeframe,
                                          symbol=symbol,
                                          candles=candles,
                                          )
        status = "OK"
        err_msg = ""
        summary_data = breakdown["summary_data"]
        summary_expected = breakdown["summary_expected"]
        if summary_expected is not None:
            for k, v in summary_expected.items():
                if summary_data[k] != v:
                    status = "ERROR"
                    if err_msg != "":
                        err_msg += "\n"
                    err_msg += f"{k} summary data does not match expected"
        else:
            status = "UNKNOWN"
            err_msg = f"Expected data not defined for timeframe: {timeframe}"

        # Perform a detailed analysis of actual vs expected timestamps
        dt_actual = []
        for c in candles:
            dt_actual.append(dhu.dt_as_str(c.c_datetime))
        start_dt = dhu.dt_from_epoch(start_epoch)
        end_dt = dhu.dt_from_epoch(end_epoch)
        dt_expected = dhu.expected_candle_datetimes(start_dt=start_dt,
                                                    end_dt=end_dt,
                                                    symbol=symbol,
                                                    timeframe=timeframe,
                                                    )
        # Remove any candles falling inside of non-standard market closures
        all_events = get_events(start_epoch=start_epoch,
                                end_epoch=end_epoch,
                                symbol=symbol,
                                )
        closure_events = []
        dt_expected_open = []
        # Only evaluate market Closed events
        for e in all_events:
            if e.category == "Closed":
                closure_events.append(e)
        # Check each candle against closures to build a new expected list
        for c in dt_expected:
            include = True
            for e in closure_events:
                if e.start_epoch < dhu.dt_to_epoch(c) < e.end_epoch:
                    include = False
            if include:
                dt_expected_open.append(c)
        # TODO above logic to exclude closure events seems to be working
        #      but I still have a few hourly candles failing integrity
        #      in both e1h and r1h timeframes.  dig deeper into these
        #      by running the copy of check_candle_integrity_all_timeframes.py
        #      in dhtrader repo to get the list and checking them one by
        #      one.  Either I have a minor issue with logic here or, I think
        #      more likely, I need to make some adjustments to the timestamps
        #      I have in the stored events.  Look at actual 1m candles avail
        #      in storage and/or in firstrate data download CSVs as source
        #      of truth to figure out what hours the market was ACTUALLY
        #      recoding candles around the times the hourly candles are
        #      failing integrity because the way the CME site displays
        #      it is quite unintuitive

        # Convert expected to strings for comparison and review
        dt_expected_str = []
        for d in dt_expected_open:
            dt_expected_str.append(dhu.dt_as_str(d))
        # Ensure we don't have any timestamp duplications
        set_actual = set(dt_actual)
        set_expected = set(dt_expected_str)
        if len(dt_actual) != len(set_actual):
            counters = Counter(dt_actual)
            dupes = []
            for k, v in counters.items():
                if v > 1:
                    dupes.append({k: v})
            raise Exception(f"len(dt_actual) {len(dt_actual)} != "
                            f"len(set(actual) {len(set_actual)}.  Likely "
                            "there are duplicates in stored candle data "
                            "which will corrupt analysis results.  Duplicates "
                            f"found in dt_actual:\n\n{dupes}"
                            )
        if len(dt_expected_str) != len(set_expected):
            counters = Counter(dt_expected_str)
            dupes = []
            for k, v in counters.items():
                if v > 1:
                    dupes.append({k: v})
            raise Exception(f"len(dt_expected) {len(dt_expected)} != "
                            f"len(set(expected) {len(set_expected)}.  Likely "
                            "there is a problem in expected candle calcs "
                            "which will corrupt analysis results.  Duplicates "
                            f"found in dt_expected:\n\n{dupes}"
                            )

        # Check for differences between actual and expected candle sets
        missing_from_actual = sorted(set_expected - set_actual)
        missing_candles_count = len(missing_from_actual)
        unexpected_in_actual = set_actual - set_expected
        unexpected_candles_count = len(unexpected_in_actual)
        gap_analysis = {"missing_candles_count": missing_candles_count,
                        "unexpected_candles_count": unexpected_candles_count,
                        "missing_candles": missing_from_actual,
                        "unexpected_candles": unexpected_in_actual,
                        }
        if missing_candles_count > 0:
            status = "ERROR"
            if err_msg != "":
                err_msg += "\n"
            err_msg += f"{missing_candles_count} expected candles missing"
        if unexpected_candles_count > 0:
            status = "ERROR"
            if err_msg != "":
                err_msg += "\n"
            err_msg += f"{unexpected_candles_count} unexpected candles found"

        # TODO Need some kind of running event update process, maybe part of
        #       refreshdata.py to add new events; todoist daily or weekly
        #       to check upcoming days/weeks and get them in ahead of events?
        #      ** I think this is best done via a .json or .yaml file that
        #         contains my list of events and a script
        #         (or part of refresh data)
        #         that just runs through and upserts them so I can just add
        #         more to the json.  Alternatively I could have a script that
        #         adds additional events adhoc and then exports the full list
        #         so it's available as a backup to be reinserted if needed.
        #         I would probably want to commit this list so it's backed
        #         up in git for future restoration.
        #      *Where should the master list live in the code so I can rebuild
        #       them in storage if it's lost/changed?  dhutil?  dhcharts?  a
        #       new script entirely?  it should be in dhtrader I think just
        #       need to noodle more on which script gets it
        #      *What happens if I missed one, what would need to be wiped or
        #       recalced?  should there be a method or function to accomplish
        #       this somehow?
        #
        #      --what happens on daylight savings time changes?
        #      --market holidays when closed
        #      --FOMC meetings (soft) - possibly one event for the full day
        #                               and separate for announcment/presser
        #      --OPEX (soft)
        #      --holidays when open with low volume (soft) - check annual
        #        holiday calendars for ideas around this
        #      --contract rollover periods (the whole week?)
        integrity_data = {"status": status, "err_msg": err_msg}
    else:
        integrity_data = None
        breakdown = None
        summary_data = None
        summary_expected = None
        gap_analysis = None

    return {"overview": overview,
            "integrity_data": integrity_data,
            "summary_data": summary_data,
            "summary_expected": summary_expected,
            "gap_analysis": gap_analysis,
            }


def drop_candles(timeframe: str,
                 symbol: str,
                 earliest_dt=None,
                 latest_dt=None,
                 ):
    """Deletes candles from central storage"""
    if earliest_dt is None and latest_dt is None:
        return dhm.drop_collection(f"candles_{symbol}_{timeframe}")
    else:
        return "Sorry, Dusty hasn't written code for select timeframes yet"


def store_event(event):
    """Write a single dhcharts.Event() to central storage"""
    if not isinstance(event, dhc.Event):
        raise TypeError(f"event {type(event)} must be a "
                        "<class dhcharts.Event> object")
    result = dhm.store_event(start_dt=event.start_dt,
                             end_dt=event.end_dt,
                             symbol=event.symbol,
                             category=event.category,
                             tags=event.tags,
                             notes=event.notes,
                             start_epoch=event.start_epoch,
                             end_epoch=event.end_epoch,
                             )

    return result


def get_events(start_epoch: int,
               end_epoch: int,
               symbol: str,
               categories: list = None,
               tags: list = None,
               ):
    """Returns a list of events starting within the start and end epochs given
    inclusive of both epochs.  Note this will return events that end after
    end_epoch so long as they start before or on it."""
    # TODO add ability to further filter by categories and tags if passed
    result = dhm.get_events(start_epoch=start_epoch,
                            end_epoch=end_epoch,
                            symbol=symbol,
                            categories=categories,
                            tags=tags,
                            )

    events = []
    for r in result:
        events.append(dhc.Event(start_dt=r["start_dt"],
                                end_dt=r["end_dt"],
                                symbol=symbol,
                                category=r["category"],
                                tags=r["tags"],
                                notes=r["notes"],
                                ))

    return events


def test_basics():
    """runs a few basics tests, mostly used during initial development
       to confirm functionality as desired"""
    # TODO consider converting these into unit tests some day

    # Test basic candle storing functionality
    print("\nStoring 2 test candles")
    tc1 = dhc.Candle(c_datetime="2024-02-10 09:20:00",
                     c_timeframe="1m",
                     c_open=5501.5,
                     c_high=5510,
                     c_low=5500.5,
                     c_close=5510,
                     c_volume=400,
                     c_symbol="DELETEME",
                     )
    tc1.store()
    tc2 = dhc.Candle(c_datetime="2024-02-10 09:21:00",
                     c_timeframe="1m",
                     c_open=5503.5,
                     c_high=5512,
                     c_low=5500.5,
                     c_close=5500,
                     c_volume=600,
                     c_symbol="DELETEME",
                     )
    tc2.store()
    print("\nNow let's retrieve them")
    result = get_candles(start_epoch=1704130201,
                         end_epoch=17044834300,
                         timeframe="1m",
                         symbol="DELETEME",
                         )
    for r in result:
        print(r.__dict__)
    print("\nAnd drop the test collection to clean up")
    drop_candles(timeframe="1m", symbol="DELETEME")

    print("\nLets check the collections list to confirm it no longer exists")
    collections = dhm.list_collections()
    print(collections)
    if "candles_DELETEME_1m" in collections:
        raise Exception("Oops, why is 'candles_DELETEME_1m' still there?!")

    # Test storing raw candles read from a csv i.e. daily updates
    print("\nStoring 5/10 candles from testcandles.csv with date filtering")
    candles = dhu.read_candles_from_csv(start_dt='2024-01-01 00:00:00',
                                        end_dt='2024-01-02 00:00:00',
                                        filepath='testcandles.csv',
                                        symbol='DELETEME',
                                        )
    for c in candles:
        store_candle(c)
    print("\nCheck a summary of them")
    print(review_candles(timeframe='1m', symbol="DELETEME"))
    print("\nNow let's retrieve them")
    result = get_candles(start_epoch=1704130201,
                         end_epoch=17044834300,
                         timeframe="1m",
                         symbol="DELETEME",
                         )
    for r in result:
        print(r.__dict__)
    print("\nAnd drop the test collection to clean up")
    drop_candles(timeframe="1m", symbol="DELETEME")

    # Test candle integrity check process
    print("\nChecking integrity of stored r1h candles")
    integrity = review_candles(timeframe='r1h',
                               symbol='ES',
                               check_integrity=True,
                               )
    print(integrity)
    print(f"\n\nIntegrity result: {integrity['integrity_data']}")

    # Test event storage and retrieval
    print("\n----------------------------------------------------------------")
    print("\nTesting event retrieval, assumes some ES events in storage")
    result = get_events(start_epoch=1704085200,
                        end_epoch=1735707599,
                        symbol="ES",
                        )
    print(f"Found {len(result)} events in 2024.  Showing the first 5:")
    first_five = result[:5]
    for r in first_five:
        print(r.__dict__)


if __name__ == '__main__':
    test_basics()
