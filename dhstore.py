# Wrapper for current (dhmongo.py) data storage I'm using to allow
# migration to a different storage solution in the future without
# massive overhaul of backtest, chart, and trader code bases.

import csv
import json
import sys
import progressbar
from collections import Counter, defaultdict
from datetime import datetime as dt
from datetime import timedelta
from copy import deepcopy
import logging
import dhcharts as dhc
import dhtrades as dht
import dhutil as dhu
import dhmongo as dhm

COLL_TRADES = "trades"
COLL_TRADESERIES = "tradeseries"
COLL_BACKTESTS = "backtests"
COLL_IND_META = "indicators_meta"
COLL_IND_DPS = "indicators_datapoints"

log = logging.getLogger("dhstore")
log.addHandler(logging.NullHandler())


##############################################################################
# Non-class specific functions
def list_mongo_collections():
    return dhm.list_collections()


def drop_mongo_collection(collection: str):
    """Used for brute force cleanup of storage, it will wipe all data from the
    named collection in mongo.  WIELD THIS POWER CAREFULLY!!!"""
    return dhm.drop_collection(collection=collection)


def get_all_records_by_collection(collection: str,
                                  limit=0):
    """Return <limit> (default 0 == all) records from a given collection
    without attempting to reconstruct them into dhtrader classes."""
    return dhm.get_all_records_by_collection(collection=collection,
                                             limit=limit)


##############################################################################
# Trades
def reconstruct_trade(t):
    """Takes a dictionary and builds a Trade() object from it.  Primarily used
    by other functions to convert results retrieved from storage."""
    return dht.Trade(open_dt=t["open_dt"],
                     direction=t["direction"],
                     timeframe=t["timeframe"],
                     trading_hours=t["trading_hours"],
                     entry_price=t["entry_price"],
                     close_dt=t["close_dt"],
                     created_dt=t["created_dt"],
                     open_epoch=t["open_epoch"],
                     high_price=t["high_price"],
                     low_price=t["low_price"],
                     exit_price=t["exit_price"],
                     stop_target=t["stop_target"],
                     prof_target=t["prof_target"],
                     stop_ticks=t["stop_ticks"],
                     prof_ticks=t["prof_ticks"],
                     offset_ticks=t["offset_ticks"],
                     symbol=t["symbol"],
                     is_open=t["is_open"],
                     profitable=t["profitable"],
                     name=t["name"],
                     version=t["version"],
                     ts_id=t["ts_id"],
                     bt_id=t["bt_id"],
                     tags=t["tags"],
                     )


def get_all_trades(collection: str = COLL_TRADES,
                   limit=0):
    "Get <limit> (default 0 == all) stored trades and return as a list."""
    result = []
    r = dhm.get_all_records_by_collection(collection=collection,
                                          limit=limit)
    for t in r:
        result.append(reconstruct_trade(t))

    return result


def get_trades_by_field(field: str,
                        value,
                        collection: str = COLL_TRADES,
                        limit=0,
                        ):
    """Returns Trade() objects matching the field=value provided.
    Default limit=0 returns all objects, or set to return only top X. """
    result = []
    r = dhm.get_trades_by_field(field=field,
                                value=value,
                                collection=collection,
                                limit=limit,
                                )
    for t in r:
        result.append(reconstruct_trade(t))

    return result


def store_trades(trades: list,
                 collection: str = COLL_TRADES,
                 ):
    """Store one or more dhtrades.Trade() objects in central storage"""

    # make a working copy to pass dicts
    working_trades = []
    for t in trades:
        working_trades.append(t.to_clean_dict())
    result = dhm.store_trades(trades=working_trades,
                              collection=collection)

    return result


def review_trades(symbol: str = "ES",
                  collection: str = COLL_TRADES,
                  bt_id: str = None,
                  ts_id: str = None,
                  include_epochs: bool = False,
                  check_integrity: bool = False,
                  multi_ok: list = None,
                  list_issues: bool = False,
                  out_path: str = None,
                  out_file: str = "trade_integrity_results.json",
                  pretty: bool = False,
                  ):
    """Provides aggregate summary data about trades in central storage,
    optionally filtering by bt_id and/or ts_id.  Earliest and latest dates
    are returned as strings and include_epochs can optionally also provide them
    as epochs.

    multi_ok should be a list of strings that can appear in a Trade's ts_id,
    typically all or part of a Backtest's bt_id.  A trade that matches any
    string in this list will not be flagged for spanning multiple days.

    pretty=True returns in a print friendly, multiline, indended
    string format."""
    if multi_ok is None:
        multi_ok = []
    review = dhm.review_trades(symbol=symbol,
                               collection=collection,
                               bt_id=bt_id,
                               ts_id=ts_id,
                               )
    for t in review:
        t["earliest"] = dhu.dt_as_str(dhu.dt_from_epoch(t["earliest_epoch"]))
        t["latest"] = dhu.dt_as_str(dhu.dt_from_epoch(t["latest_epoch"]))
        if not include_epochs:
            t.pop("earliest_epoch")
            t.pop("latest_epoch")

    if check_integrity:
        dhu.log_say("Checking integrity of all Trades in storage")
        time_full = dhu.OperationTimer(
                name="Trade integrity check full run timer")

        # Get all TradeSeries in storage to loop through
        if bt_id is None and ts_id is None:
            all_ts = get_all_tradeseries()
        elif ts_id is None:
            all_ts = get_tradeseries_by_field(field="bt_id",
                                              value=bt_id,
                                              include_trades=False)
        elif bt_id is None:
            all_ts = get_tradeseries_by_field(field="ts_id",
                                              value=ts_id,
                                              include_trades=False)
        else:
            raise ValueError("Integrity check supports bt_id or ts_id, not "
                             f"both!  We got ts_id={ts_id} bt_id={bt_id}")
        duplicates = []
        multidays = []
        total_trades = 0
        unique_trades = 0

        # Start a progress bar
        bar_total = len(all_ts)
        bar_eta = progressbar.ETA(format_not_started='--:--:--',
                                  format_finished='Time: %(elapsed)8s',
                                  format='Remaining: %(eta)8s',
                                  format_zero='Remaining: 00:00:00',
                                  format_na='Remaining: N/A',
                                  )
        bar_label = (f"%(value)d of {bar_total} TradeSeries checked in "
                     "%(elapsed)s ")
        widgets = [progressbar.Percentage(),
                   progressbar.Bar(),
                   progressbar.FormatLabel(bar_label),
                   bar_eta,
                   ]
        bar = progressbar.ProgressBar(
                widgets=widgets,
                max_value=bar_total).start()

        # Loop through all TradeSeries, checking for duplicates and multidays
        for i, x in enumerate(all_ts):
            log.info(f"Checking stored Trade integrity for ts_id={x.ts_id}")
            bar.update(i)
            unique = set()
            ts = deepcopy(x)
            ts.load_trades()
            # Determine if this ts_id allows multiday trades
            check_multi = True
            for x in multi_ok:
                if x in ts.ts_id:
                    check_multi = False
                    break
            # Check for duplicate Trades w/matching ts_id and open_dt values
            for t in ts.trades:
                total_trades += 1
                this = (t.ts_id, t.open_dt)
                if this in unique:
                    duplicates.append(this)
                    log.warning(f"Duplicate Trades found in storage: {this}")
                else:
                    unique.add(this)
                    unique_trades += 1
                if check_multi:
                    if not t.closed_intraday():
                        this = {"ts_id": t.ts_id,
                                "open_dt": t.open_dt,
                                "close_dt": t.close_dt}
                        log.warning("Unapproved multiday trade found in "
                                    f"storage: {this}")
                        multidays.append(this)
        bar.finish()

        # Output findings
        status = "OK"
        issues = []
        if len(duplicates) > 0:
            status = "ERRORS"
            issues.append(f"{len(duplicates)} duplicate trades found")
        if len(multidays) > 0:
            status = "ERRORS"
            issues.append(f"{len(multidays)} invalid multiday trades found")
        if len(issues) == 0:
            issues = None
        integrity = {"status": status,
                     "issues": issues,
                     "total_trades": total_trades,
                     "unique_trades": unique_trades,
                     "duplicate_trades": len(duplicates),
                     "invalid_multiday_trades": len(multidays)
                     }
        if list_issues:
            integrity["duplicates"] = duplicates
            integrity["multidays"] = multidays
        time_full.stop()
        print(time_full.summary())
    else:
        integrity = {"status": "integrity checks were not run"}

    result = {"integrity": integrity, "review": review}
    if out_path is not None:
        # Write result and any issues found to disk
        filename = "/".join([out_path, out_file])
        blob = deepcopy(result)
        # Add issue details for disk output if not already included
        if not list_issues:
            blob["duplicates"] = duplicates
            blob["multidays"] = multidays
        with open(filename, "w") as f:
            f.write(json.dumps(blob))
        dhu.log_say(f"Wrote integrity results and issues to {filename}")

    if pretty:
        return json.dumps(result,
                          indent=4,
                          )
    else:
        return result


def delete_one_trade(symbol: str,
                     open_dt: str,
                     ts_id: str,
                     collection: str = COLL_TRADES,
                     ):
    """Delete a single trade from storage, identifying by symbol, open_dt, and
    ts_id"""
    query = {"symbol": symbol,
             "open_dt": open_dt,
             "ts_id": ts_id,
             }

    return dhm.delete_one_document(query=query, collection=collection)


def delete_trades(symbol: str,
                  field: str,
                  value,
                  collection: str = COLL_TRADES,
                  ):
    """Delete all trade records with 'field' matching 'value'.  Typically
    used to delete by name, ts_id, or bt_id fields.

    Example to delete all trade records with name=="DELETEME":
        delete_trades(symbol="ES", field="name", value="DELETEME")
    """
    result = dhm.delete_trades(symbol=symbol,
                               collection=collection,
                               field=field,
                               value=value,
                               )

    return result


##############################################################################
# TradeSeries
def reconstruct_tradeseries(ts):
    """Takes a dictionary and builds a Trade() object from it.  Primarily used
    by other functions to convert results retrieved from storage."""
    return dht.TradeSeries(start_dt=ts["start_dt"],
                           end_dt=ts["end_dt"],
                           timeframe=ts["timeframe"],
                           trading_hours=ts["trading_hours"],
                           symbol=ts["symbol"],
                           name=ts["name"],
                           params_str=ts["params_str"],
                           ts_id=ts["ts_id"],
                           bt_id=ts["bt_id"],
                           trades=[],
                           tags=ts["tags"],
                           )


def get_all_tradeseries(collection: str = COLL_TRADESERIES,
                        limit=0):
    "Get <limit> (default 0 == all) stored tradeseries returned as a list."""
    result = []
    r = dhm.get_all_records_by_collection(collection=collection, limit=limit)
    for t in r:
        result.append(reconstruct_tradeseries(t))

    return result


def get_tradeseries_by_field(field: str,
                             value,
                             collection: str = COLL_TRADESERIES,
                             collection_trades: str = COLL_TRADES,
                             limit=0,
                             include_trades: bool = True,
                             ):
    """Returns a list of all TradeSeries matching the bt_id provided."""
    result = []
    r = dhm.get_tradeseries_by_field(field=field,
                                     value=value,
                                     collection=collection,
                                     limit=limit,
                                     )
    for t in r:
        ts = reconstruct_tradeseries(t)
        if include_trades:
            ts.trades = get_trades_by_field(field="ts_id",
                                            value=ts.ts_id,
                                            collection=collection_trades,
                                            )
            ts.sort_trades()
        result.append(ts)

    return result


def store_tradeseries(series: list,
                      collection: str = COLL_TRADESERIES,
                      ):
    """Store a list of TradeSeries() objects in central storage"""
    result = []
    for ts in series:
        result.append(dhm.store_tradeseries(ts.to_clean_dict(),
                                            collection=collection,
                                            ))

    return result


def review_tradeseries(symbol: str = "ES",
                       collection: str = COLL_TRADESERIES,
                       bt_id: str = None,
                       include_trades: bool = False,
                       pretty: bool = False,
                       check_integrity: bool = False,
                       ):
    """Provides aggregate summary data about tradeseries in central storage,
    optionally filtering by bt_id.  Earliest start_dt and latest end_dt are
    returned as strings.  pretty=True returns in a print friendly, multiline,
    indented string format."""
    if check_integrity:
        if bt_id is None:
            print("Fetching all TradeSeries from storage")
            all_ts = get_all_tradeseries()
        else:
            print(f"Fetching all TradeSeries for {bt_id} from storage")
            all_ts = get_tradeseries_by_field(field="bt_id",
                                              value=bt_id,
                                              include_trades=False)
        print("Loading all Trades into TradeSeries and checking for issues")
        bar_total = len(all_ts)
        bar_eta = progressbar.ETA(format_not_started='--:--:--',
                                  format_finished='Time: %(elapsed)8s',
                                  format='Remaining: %(eta)8s',
                                  format_zero='Remaining: 00:00:00',
                                  format_na='Remaining: N/A',
                                  )
        bar_label = (f"%(value)d of {bar_total} checked in "
                     "%(elapsed)s ")
        widgets = [progressbar.Percentage(),
                   progressbar.Bar(),
                   progressbar.FormatLabel(bar_label),
                   bar_eta,
                   ]
        bar = progressbar.ProgressBar(
                widgets=widgets,
                max_value=bar_total).start()
        # Loop through all TradeSeries/Trades, noting any issues found
        trade_overlaps = []
        for i, ts in enumerate(all_ts):
            ts.load_trades()
            last_trade = None
            last_close_tf = None
            for t in ts.trades:
                # Capture the parent timeframe bars that we open and close in
                open_tf = dhu.this_candle_start(dt=t.open_dt,
                                                timeframe=ts.timeframe)
                close_tf = dhu.this_candle_start(dt=t.close_dt,
                                                 timeframe=ts.timeframe)
                if last_trade is not None:
                    # Check if we opened in the same timeframe bar as the
                    # prior Trade closed in (not expected)
                    if open_tf == last_close_tf:
                        issue = {"issue_type": "Trade timeframe bar overlap",
                                 "ts_id": ts.ts_id,
                                 "timeframe": ts.timeframe,
                                 "trade_open": str(t.open_dt),
                                 "trade_open_tf": str(open_tf),
                                 "prev_trade_open": str(last_trade.open_dt),
                                 "prev_trade_close": str(last_trade.close_dt),
                                 "prev_trade_close_tf": str(last_close_tf),
                                 }
                        trade_overlaps.append(issue)
                # Config last vars with current values for next Trade in loop
                last_trade = t
                last_close_tf = close_tf
            bar.update(i)
            # Reclaim memory
            ts.trades.clear()
        bar.finish()
        # Finalize integrity status
        if len(trade_overlaps) > 0:
            status = "ERRORS"
            issues = {"trade_overlaps": trade_overlaps}
        else:
            status = "OK"
            issues = None
        integrity = {"status": status, "issues": issues}
    else:
        integrity = {"status": None, "issues": None}
    # Standard review shows key details of TradeSeries and optionally Trades
    review = dhm.review_tradeseries(symbol=symbol,
                                    collection=collection,
                                    bt_id=bt_id,
                                    )
    if include_trades:
        for ts in review:
            ts["trades"] = review_trades(symbol=symbol,
                                         bt_id=ts["_id"]["bt_id"],
                                         )
    result = {"integrity": integrity, "review": review}
    if pretty:
        result = json.dumps(result,
                            indent=4,
                            )
    return result


def delete_tradeseries(symbol: str,
                       field: str,
                       value,
                       collection: str = COLL_TRADESERIES,
                       coll_trades=COLL_TRADES,
                       include_trades: bool = False,
                       ):
    """Delete all tradeseries records in central storage with 'field' matching
    'value'.  Typically used to delete by ts_id, or bt_id fields.
    """
    result = {}
    result["tradeseries"] = dhm.delete_tradeseries(
            symbol=symbol,
            collection=collection,
            field=field,
            value=value,
            )
    if include_trades:
        result["trades"] = delete_trades(symbol=symbol,
                                         field=field,
                                         value=value,
                                         collection=coll_trades,
                                         )

    return result


##############################################################################
# Backtests
def get_all_backtests(collection: str = COLL_BACKTESTS,
                      limit=0):
    """Get <limit> (default 0 == all) stored backtests returned as a list of
    dicts.  Because dhtrader.Backtest() is meant to be subclassed we don't
    return Backtest() objects here.  Subclass implementations can warp this
    function to convert dicts into their subclass object types as needed."""
    return dhm.get_all_records_by_collection(collection=collection,
                                             limit=limit)


def get_backtests_by_field(field: str,
                           value,
                           collection: str = COLL_BACKTESTS,
                           limit=0,
                           ):
    """Returns a list of Backtest() objects (as dictionaries), matching the
    field=value provided.  Because dhtrader.Backtest() is meant to be
    subclassed we don't return Backtest() objects here.  Subclass
    implementations can warp this function to convert dicts into their
    subclass specific object types as needed."""
    result = dhm.get_backtests_by_field(field=field,
                                        value=value,
                                        collection=collection,
                                        limit=limit,
                                        )

    return result


def store_backtests(backtests: list,
                    collection: str = COLL_BACKTESTS,
                    ):
    """Store one or more Backtest() objects in central storage"""
    result = []
    for bt in backtests:
        result.append(dhm.store_backtest(bt.to_clean_dict(),
                                         collection=collection,
                                         ))

    return result


def review_backtests(symbol: str = "ES",
                     collection: str = COLL_BACKTESTS,
                     include_tradeseries: bool = False,
                     include_trades: bool = False,
                     pretty: bool = False,
                     ):
    """Provides aggregate summary data about backtests in central storage"""
    review = dhm.review_backtests(symbol=symbol,
                                  collection=collection,
                                  )
    if include_tradeseries:
        for bt in review:
            bt["tradeseries"] = review_tradeseries(
                    symbol=symbol,
                    bt_id=bt["_id"]["bt_id"],
                    include_trades=include_trades,
                    )
    if pretty:
        return json.dumps(review,
                          indent=4,
                          )
    else:
        return review


def delete_backtests(symbol: str,
                     field: str,
                     value,
                     collection: str = COLL_BACKTESTS,
                     coll_tradeseries: str = COLL_TRADESERIES,
                     coll_trades: str = COLL_TRADES,
                     include_tradeseries: bool = False,
                     include_trades: bool = False,
                     ):
    """Delete all backtests records in central storage with 'field' matching
    'value'.  Typically used to delete by bt_id field.
    """
    result = {}
    result["backtests"] = dhm.delete_backtests(symbol=symbol,
                                               collection=collection,
                                               field=field,
                                               value=value,
                                               )
    if include_tradeseries:
        result["tradeseries"] = delete_tradeseries(
                symbol=symbol,
                field=field,
                value=value,
                collection=coll_tradeseries,
                coll_trades=coll_trades,
                include_trades=include_trades,
                )

    return result


##############################################################################
# Indicators
def list_indicators(meta_collection: str = COLL_IND_META):
    """Return a simple list of indicators in storage"""
    result = dhm.list_indicators(meta_collection=meta_collection)

    return result


def list_indicators_names(meta_collection: str = COLL_IND_META):
    """Return a simple list of indicators (ind_id only) in storage"""
    indicators = dhm.list_indicators(meta_collection=meta_collection)
    result = []
    for i in indicators:
        result.append(i['ind_id'])

    return result


def review_indicators(meta_collection: str = COLL_IND_META,
                      dp_collection: str = COLL_IND_DPS):
    """Return a more detailed overview of indicators in storage"""
    result = dhm.review_indicators(meta_collection=meta_collection,
                                   dp_collection=dp_collection,
                                   )

    return result


def get_indicator(ind_id: str,
                  meta_collection: str = COLL_IND_META,
                  autoload_chart: bool = True,
                  autoload_datapoints: bool = True,
                  ):
    """Returns an indicator based on ind_id (which should be unique) and
    optionally (default=True) autoloads it's datapoints for the given range
    of earliest_dt to latest_dt
    """
    try:
        i = dhm.get_indicator(ind_id=ind_id,
                              meta_collection=meta_collection,
                              autoload_datapoints=autoload_datapoints,
                              )[0]
    except IndexError:
        return None

    # The stored class_name attribute tells us which object class to return
    if i["class_name"] == "IndicatorSMA":
        result = dhc.IndicatorSMA(description=i["description"],
                                  timeframe=i["timeframe"],
                                  trading_hours=i["trading_hours"],
                                  symbol=i["symbol"],
                                  calc_version=i["calc_version"],
                                  calc_details=i["calc_details"],
                                  ind_id=i["ind_id"],
                                  autoload_chart=autoload_chart,
                                  name=i["name"],
                                  parameters=i["parameters"],
                                  )
    elif i["class_name"] == "IndicatorEMA":
        result = dhc.IndicatorEMA(description=i["description"],
                                  timeframe=i["timeframe"],
                                  trading_hours=i["trading_hours"],
                                  symbol=i["symbol"],
                                  calc_version=i["calc_version"],
                                  calc_details=i["calc_details"],
                                  ind_id=i["ind_id"],
                                  autoload_chart=autoload_chart,
                                  name=i["name"],
                                  parameters=i["parameters"],
                                  )
    else:
        raise ValueError(f"Unable to match class_name of {i['class_name']} "
                         "with a known Indicator() subclass."
                         )
    if autoload_datapoints:
        result.load_datapoints()

    return result


def get_indicator_datapoints(ind_id: str,
                             dp_collection: str = COLL_IND_DPS,
                             earliest_dt: str = None,
                             latest_dt: str = None,
                             ):
    """Returns a list of IndicatorDatapoint() objects for the given timeframe
    and ind_id"""
    working = dhm.get_indicator_datapoints(ind_id=ind_id,
                                           dp_collection=dp_collection,
                                           earliest_dt=earliest_dt,
                                           latest_dt=latest_dt,
                                           )
    result = []
    for d in working:
        result.append(dhc.IndicatorDataPoint(dt=d["dt"],
                                             value=d["value"],
                                             ind_id=d["ind_id"],
                                             epoch=d["epoch"]
                                             ))

    return result


def store_indicator_datapoints(datapoints: list,
                               collection: str = COLL_IND_DPS,
                               skip_dupes: bool = True,
                               ):
    """Store one or more IndicatorDatapoint() objects in central storage"""
    store_dps = []
    r_skipped = []
    for d in datapoints:
        if skip_dupes:
            stored = get_indicator_datapoints(ind_id=d.ind_id,
                                              earliest_dt=d.dt,
                                              latest_dt=d.dt,
                                              )
            # If all is well we should get 0 or 1 result in the list
            if len(stored) == 0:
                checker = stored
            elif len(stored) == 1:
                checker = stored[0]
            else:
                msg = ("Possible duplicate found, needs a human to review."
                       "There should be 0 or 1 result per dt or these can't "
                       f"be trusted!  Looking for {d} and found {stored}"
                       )
                raise Exception(msg)
            # If the stored datapoint is the same we can skip it, else store it
            if d == checker:
                r_skipped.append(d.to_clean_dict())
            else:
                store_dps.append(d.to_clean_dict())
        else:
            store_dps.append(d.to_clean_dict())

    op_timer = dhu.OperationTimer(name="Indicator Datapoints Storage Job")
    r_stored = dhm.store_indicator_datapoints(datapoints=store_dps,
                                              collection=collection,
                                              )
    op_timer.stop()
    result = {"skipped": r_skipped,
              "stored": r_stored,
              "elapsed": op_timer,
              }

    return result


def store_indicator(indicator,
                    meta_collection: str = COLL_IND_META,
                    dp_collection: str = COLL_IND_DPS,
                    store_datapoints: bool = True,
                    fast_dps_check: bool = False,
                    show_progress: bool = False,
                    ):
    """Store indicator meta and datapoints in central storage.  Does not
    overwrite existing datapoints unless overwrite_dp is True"""
    op_timer = dhu.OperationTimer(name="Indicator Storage Job")
    # First store/replace the indicator meta doc itself
    i = indicator.to_clean_dict()
    i["datapoints"] = len(i["datapoints"])
    result_ind = dhm.store_indicator(i,
                                     meta_collection=meta_collection,
                                     )
    # Then work on datapoints if asked
    if store_datapoints:
        result_dps = []
        dps_skipped = 0
        dps_stored = 0
        if show_progress:
            progress = 0
            bar_started = False
            bar_total = len(indicator.datapoints)
            bar_eta = progressbar.ETA(format_not_started='--:--:--',
                                      format_finished='Time: %(elapsed)8s',
                                      format='Remaining: %(eta)8s',
                                      format_zero='Remaining: 00:00:00',
                                      format_na='Remaining: N/A',
                                      )

        # To prevent each datapoint from running it's own query, we'll
        # retrieve all potentially relevant stored datapoints to compare
        # in a single query first and then provide the by epoch.  In theory
        # this should be faster than many queries during the storage loop.
        # Update - it really isn't significantly faster.  Sad face.

        # Determine earliest and latest datapoints in indicator
        earliest = dhu.dt_to_epoch(dt.now())
        latest = 0
        for d in indicator.datapoints:
            earliest = min(d.epoch, earliest)
            latest = max(d.epoch, latest)
        earliest_to_store = dhu.dt_from_epoch(earliest)
        latest_to_store = dhu.dt_from_epoch(latest)

        # Retrieve all stored datapoints for this timeframe
        dps_in_storage = get_indicator_datapoints(
                ind_id=indicator.ind_id,
                earliest_dt=earliest_to_store,
                latest_dt=latest_to_store,
                )

        # Put them in a dict for easier comparison to each datapoint
        checkers = {}
        earliest = dhu.dt_to_epoch(dt.now())
        latest = 0
        for d in dps_in_storage:
            checkers[d.epoch] = d
            latest = max(d.epoch, latest)
        latest_stored = latest

        # Loop through all datapoints, providing the stored version if avail
        # to compare.  This avoids spend time storing duplicates.
        for d in indicator.datapoints:
            if fast_dps_check:
                # Only attempt to store if newer than the latest found
                # This is the high speed but less robust mode, useful for
                # daily updates but may leave issues or gaps
                if d.epoch > latest_stored:
                    if show_progress and not bar_started:
                        # Once we find something new enough to store, start the
                        # progress bar up
                        bar_label = (f"%(value)d of {bar_total} stored in "
                                     "%(elapsed)s ")
                        widgets = [progressbar.Percentage(),
                                   progressbar.Bar(),
                                   progressbar.FormatLabel(bar_label),
                                   bar_eta,
                                   ]
                        bar = progressbar.ProgressBar(
                                widgets=widgets,
                                max_value=bar_total).start()
                        bar_started = True
                    s = d.store()
                else:
                    s = {"skipped": [d], "stored": [], "elapsed": None}
                    bar_total -= 1

            else:
                # Slower mode that verifies each datapoint vs those stored
                # and only writes if it's missing or different
                # If we found a stored datapoint with the same epoch, pass it
                # to it's .store() method to compare and store on diffs
                if show_progress and not bar_started:
                    bar_label = (f"%(value)d of {bar_total} stored in "
                                 "%(elapsed)s ")
                    widgets = [progressbar.Percentage(),
                               progressbar.Bar(),
                               progressbar.FormatLabel(bar_label),
                               bar_eta,
                               ]
                    bar = progressbar.ProgressBar(
                            widgets=widgets,
                            max_value=bar_total).start()
                    bar_started = True
                if d.epoch in checkers.keys():
                    s = d.store(checker=d.epoch)
                # Otherwise just store it, overwriting any existing
                else:
                    s = d.store()
            result_dps.append(s)
            dps_skipped += len(s["skipped"])
            dps_stored += len(s["stored"])
            if show_progress and bar_started:
                progress += 1
                bar.update(progress)
    op_timer.stop()
    if show_progress and bar_started:
        bar.finish()

    result = {"indicator": result_ind,
              "datapoints_stored": dps_stored,
              "datapoints_skipped": dps_skipped,
              "elapsed": op_timer,
              "datapoints": result_dps,
              }

    return result


def delete_indicator(ind_id: str,
                     meta_collection: str = COLL_IND_META,
                     dp_collection: str = COLL_IND_DPS,
                     ):
    """Remove a single indicator and all of it's datapoints from central
    storage based on it's ind_id attribute"""
    return dhm.delete_indicator(ind_id=ind_id,
                                meta_collection=meta_collection,
                                dp_collection=dp_collection,
                                )


##############################################################################
# Symbols

def get_symbol_by_ticker(ticker: str):
    """Temp function to help other objects get symbols by name.  This should
    be replaced by proper storage and retrieval functions eventually but since
    I don't forsee working with any symbol other than ES for the forseeable
    future I'm deprioritizing that work."""
    if ticker in ["ES", "DELETEME"]:
        return dhc.Symbol(ticker=ticker,
                          name=ticker,
                          leverage_ratio=50,
                          tick_size=0.25,
                          )
    else:
        raise ValueError("Only ['ES', 'DELETEME'] is currently supported as "
                         f"Symbol ticker, we got {ticker}")

##############################################################################
# Candles


def store_candle(candle):
    """Write a single dhcharts.Candle() to central storage"""
    dhu.valid_timeframe(candle.c_timeframe)
    dhm.store_candle(c_datetime=candle.c_datetime,
                     c_timeframe=candle.c_timeframe,
                     c_open=candle.c_open,
                     c_high=candle.c_high,
                     c_low=candle.c_low,
                     c_close=candle.c_close,
                     c_volume=candle.c_volume,
                     c_symbol=candle.c_symbol.ticker,
                     c_epoch=candle.c_epoch,
                     c_date=candle.c_date,
                     c_time=candle.c_time,
                     )


def get_candles(start_epoch: int,
                end_epoch: int,
                timeframe: str,
                symbol: str = "ES",
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
                   symbol="ES",
                   check_integrity: bool = False,
                   return_detail: bool = False,
                   ):
    """Provides aggregate summary data about candles in central storage
    with options to further check completeness/integrity of candle data and
    provide remediation"""
    if isinstance(symbol, str):
        symbol = get_symbol_by_ticker(ticker=symbol)
    print("Retrieving candles from storage, this may take a few minutes...")
    overview = dhm.review_candles(timeframe=timeframe,
                                  symbol=symbol.ticker,
                                  )
    if overview is None:
        print(f"No candles found for the specified timeframe {timeframe}")
        return None
    start_epoch = dhu.dt_to_epoch(overview["earliest_dt"])
    end_epoch = dhu.dt_to_epoch(overview["latest_dt"])
    if check_integrity:
        candles = get_candles(timeframe=timeframe,
                              symbol=symbol.ticker,
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
                        err_msg += " || "
                    err_msg += f"{k} summary data does not match expected: "
                    err_msg += f"ACTUAL {summary_data[k]} != EXPECTED {v}"
        else:
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

        # Convert expected to strings for comparison and review
        dt_expected_str = []
        for d in dt_expected:
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
        # and parse results into a few helpful views
        missing_from_actual = sorted(set_expected - set_actual)
        missing_candles_count = len(missing_from_actual)
        missing_candles_by_date = defaultdict(list)
        for c in missing_from_actual:
            k = c.split(' ')[0]
            v = c.split(' ')[1]
            missing_candles_by_date[k].append(v)
        missing_candles_by_date = dhu.sort_dict(dict(missing_candles_by_date))
        missing_count_by_date = {}
        for k, v in missing_candles_by_date.items():
            missing_count_by_date[k] = len(v)
        missing_candles_by_hour = defaultdict(list)
        for c in missing_from_actual:
            k = c.split(' ')[1].split(':')[0]
            missing_candles_by_hour[k].append(c)
        missing_candles_by_hour = dhu.sort_dict(dict(missing_candles_by_hour))
        missing_count_by_hour = {}
        for k, v in missing_candles_by_hour.items():
            missing_count_by_hour[k] = len(v)
        unexpected_in_actual = sorted(set_actual - set_expected)
        unexpected_candles_count = len(unexpected_in_actual)
        # Create human digestible ranges
        missing_ranges = dhu.rangify_candle_times(times=missing_from_actual,
                                                  timeframe=timeframe,
                                                  )
        unexpected_ranges = dhu.rangify_candle_times(
                times=unexpected_in_actual,
                timeframe=timeframe,
                )
        gap_analysis = {"missing_candles_count": missing_candles_count,
                        "unexpected_candles_count": unexpected_candles_count,
                        "missing_candles": missing_from_actual,
                        "unexpected_candles": unexpected_in_actual,
                        "missing_candles_ranges": missing_ranges,
                        "unexpected_candles_ranges": unexpected_ranges,
                        }
        if missing_candles_count > 0:
            status = "ERROR"
            if err_msg != "":
                err_msg += " || "
            err_msg += f"{missing_candles_count} expected candles missing"
        if unexpected_candles_count > 0:
            status = "ERROR"
            if err_msg != "":
                err_msg += "\n"
            err_msg += f"{unexpected_candles_count} unexpected candles found"
        integrity_data = {"status": status, "err_msg": err_msg}
    else:
        integrity_data = None
        breakdown = None
        summary_data = None
        summary_expected = None
        gap_analysis = None

    if return_detail:
        result = {"overview": overview,
                  "integrity_data": integrity_data,
                  "summary_data": summary_data,
                  "summary_expected": summary_expected,
                  "gap_analysis": gap_analysis,
                  "missing_count_by_date": missing_count_by_date,
                  "missing_count_by_hour": missing_count_by_hour,
                  "missing_candles_by_date": missing_candles_by_date,
                  "missing_candles_by_hour": missing_candles_by_hour,
                  }
    else:
        result = {"overview": overview,
                  "integrity_data": integrity_data,
                  "gap_analysis": gap_analysis,
                  }

    return result


def drop_candles(timeframe: str,
                 symbol: str,
                 earliest_dt=None,
                 latest_dt=None,
                 ):
    """Deletes candles from central storage either en masse or for a
    specific datetime range."""
    if earliest_dt is None and latest_dt is None:
        return dhm.drop_collection(f"candles_{symbol}_{timeframe}")
    else:
        return dhm.drop_candles(timeframe=timeframe,
                                symbol=symbol,
                                earliest_dt=earliest_dt,
                                latest_dt=latest_dt,
                                )


##############################################################################
# Events
def store_event(event):
    """Write a single dhcharts.Event() to central storage"""
    if not isinstance(event, dhc.Event):
        raise TypeError(f"event {type(event)} must be a "
                        "<class dhcharts.Event> object")
    result = dhm.store_event(start_dt=event.start_dt,
                             end_dt=event.end_dt,
                             symbol=event.symbol.ticker,
                             category=event.category,
                             tags=event.tags,
                             notes=event.notes,
                             start_epoch=event.start_epoch,
                             end_epoch=event.end_epoch,
                             )

    return result


def get_events(symbol="ES",
               start_epoch: int = None,
               end_epoch: int = None,
               categories: list = None,
               tags: list = None,
               ):
    """Returns a list of events starting within the start and end epochs given
    inclusive of both epochs.  Note this will return events that end after
    end_epoch so long as they start before or on it."""
    if isinstance(symbol, str):
        symbol = get_symbol_by_ticker(ticker=symbol)
    if start_epoch is None:
        start_epoch = dhu.dt_to_epoch("1900-01-01 00:00:00")
    if end_epoch is None:
        end_epoch = dhu.dt_to_epoch(dt.now())
    result = dhm.get_events(start_epoch=start_epoch,
                            end_epoch=end_epoch,
                            symbol=symbol.ticker,
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


def drop_events(symbol: str,
                earliest_dt=None,
                latest_dt=None,
                ):
    """Deletes events from central storage"""
    if earliest_dt is None and latest_dt is None:
        return dhm.drop_collection(f"events_{symbol}")
    else:
        return "Sorry, Dusty hasn't written code for select timeframes yet"


##############################################################################
# Tests
def test_basics():
    """runs a few basics tests, mostly used during initial development
       to confirm functionality as desired"""

    print("=========================== CANDLES ==============================")
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

    # Test candle integrity check process - detailed
    # runs slowly, only uncomment as needed
    print("\nChecking for missing e1h candles")
    integrity = review_candles(timeframe='e1h',
                               symbol='ES',
                               check_integrity=True,
                               return_detail=True,
                               )
    print(integrity)
    print("\nCount of missing candles by date")
    for k, v in integrity["missing_count_by_date"].items():
        print(k, v)
    print("\nCount of missing candles by hour")
    for k, v in integrity["missing_count_by_hour"].items():
        print(k, v)

    print("============================ EVENTS ==============================")
    # Test event storage and retrieval
    print("\n----------------------------------------------------------------")
    print("\nTesting event retrieval, assumes some ES events in storage")
    result = get_events(start_epoch=1704085200,
                        end_epoch=1735707599,
                        symbol="ES",
                        )
    print(f"Found {len(result)} events since 2024.  Showing the first 5:")
    first_five = result[:5]
    for r in first_five:
        print(r.pretty())

    print("Testing complete.")
    print("\n----------------------------------------------------------------")

    # Prompt to run full reviews of stored items
    prompt = dhu.prompt_yn("List all stored ES events?")
    if prompt:
        events = get_events(start_epoch=dhu.dt_to_epoch("2000-01-01 00:00:00"),
                            end_epoch=dhu.dt_to_epoch(dt.now()),
                            symbol="ES",
                            )
        for e in events:
            print(e)
    print("\n----------------------------------------------------------------")
    prompt = dhu.prompt_yn("Run full integrity check of stored candles")
    if prompt:
        for t in ['1m', '5m', '15m', 'r1h', 'e1h']:
            integrity = review_candles(timeframe=t,
                                       symbol='ES',
                                       check_integrity=True,
                                       return_detail=False,
                                       )
            print(integrity)

    print("========================== INDICATORS ============================")
    # Review indicators in storage
    # NOTE - There are a number of comprehensive Indicators tests in dhcharts,
    #        no need to duplicate them here
    print("\n----------------------------------------------------------------")
    print("Reviewing stored indicators:\n\n")
    indicators = review_indicators()
    print("==== Meta Docs ====")
    for m in indicators["meta_docs"]:
        print(m['ind_id'])
        print(f"{m}\n")
    print("==== Datapoints ====")
    for d in indicators["datapoints"]:
        print(f"{d['ind_id']} contains {d['count']} datapoints from "
              f"{d['earliest_dt']} to {d['latest_dt']}"
              )


if __name__ == '__main__':
    test_basics()
