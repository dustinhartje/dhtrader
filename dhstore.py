"""Storage abstraction layer wrapping MongoDB persistence (dhmongo).

This module provides high-level storage functions for candles, trades,
backtests, indicators, and events. It abstracts the underlying MongoDB
implementation (dhmongo.py) to allow future migration to different storage
solutions without changing higher-level code.

Core datetime-calculation utilities (expected_candle_datetimes,
next_candle_start) are imported from dhcommon, which provides the base
utility layer with no storage dependencies.
"""

import json
import progressbar
from collections import Counter, defaultdict
from datetime import datetime as dt
from datetime import timedelta
from copy import deepcopy
import logging
from pathlib import Path
from .dhtypes import (
    Candle, Event, IndicatorDataPoint, Symbol, IndicatorSMA, IndicatorEMA,
    Trade, TradeSeries)
from .dhcommon import (
    dt_as_str, dt_as_dt, dt_from_epoch, dt_to_epoch, valid_timeframe,
    this_candle_start, summarize_candles, log_say, sort_dict,
    rangify_candle_times, expected_candle_datetimes,
    ProgBar, OperationTimer)
from . import dhmongo as dhm

COLL_TRADES = "trades"
COLL_TRADESERIES = "tradeseries"
COLL_BACKTESTS = "backtests"
COLL_IND_META = "indicators_meta"
COLL_IND_DPS = "indicators_datapoints"

# Cache for Symbol instances to avoid repeated creation
SYMBOL_CACHE = {}

log = logging.getLogger("dhstore")
log.addHandler(logging.NullHandler())


##############################################################################
# Progress bar helper functions
def start_progbar(show_progress: bool, total: int,
                  desc: str) -> ProgBar:
    """Start a progress bar if show_progress is True and total > 0.

    Returns ProgBar object or None.
    """
    if show_progress and total > 0:
        return ProgBar(total=total, desc=desc)
    return None


def update_progbar(pbar: ProgBar, index: int, total: int,
                   update_every: int = 500):
    """Update progress bar at intervals and at final item."""
    if pbar is not None and (index % update_every == 0 or
                             index == total):
        pbar.update(index)


def finish_progbar(pbar: ProgBar):
    """Finish and cleanup progress bar if it exists."""
    if pbar is not None:
        pbar.finish()


##############################################################################
# Non-class specific functions
def list_mongo_collections():
    """Return a list of all collection names in the MongoDB database."""
    return dhm.list_collections()


def drop_mongo_collection(collection: str):
    """Wipe all data from the named mongo collection (brute force cleanup).

    WIELD THIS POWER CAREFULLY!!!
    """
    return dhm.drop_collection(collection=collection)


def get_all_records_by_collection(collection: str,
                                  limit=0,
                                  show_progress: bool = False,
                                  ):
    """Return raw records from a collection without rebuilding dhtrader types.

    limit defaults to 0 which returns all records.
    """
    return dhm.get_all_records_by_collection(collection=collection,
                                             limit=limit,
                                             show_progress=show_progress)


##############################################################################
# Trades
def reconstruct_trade(t):
    """Takes a dictionary and builds a Trade() object from it.

    Primarily used by other functions to convert results retrieved from
    storage.
    """
    return Trade(open_dt=t["open_dt"],
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
                   limit=0,
                   show_progress: bool = False,
                   ):
    """Get <limit> (default 0 == all) stored trades and return as a list."""
    result = []
    r = dhm.get_all_records_by_collection(collection=collection,
                                          limit=limit,
                                          show_progress=show_progress)
    total = len(r)
    pbar = start_progbar(show_progress, total,
                         "Trade objects built")
    for i, t in enumerate(r, start=1):
        result.append(reconstruct_trade(t))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def get_trades_by_field(field: str,
                        value,
                        collection: str = COLL_TRADES,
                        limit=0,
                        show_progress: bool = False,
                        ):
    """Returns Trade() objects matching the field=value provided.

    Default limit=0 returns all objects, or set to return only top X.
    """
    # Retrieve trades from storage
    log.info(f"Retrieving trades by {field}={value}, "
             f"limit={limit}")
    result = []
    r = dhm.get_trades_by_field(field=field,
                                value=value,
                                collection=collection,
                                limit=limit,
                                show_progress=show_progress,
                                )
    log.info(f"Retrieved {len(r)} trade records")

    # Reconstruct Trade objects
    log.info("Building Trade objects")
    total = len(r)
    pbar = start_progbar(show_progress, total,
                         "Trade objects built")
    for i, t in enumerate(r, start=1):
        result.append(reconstruct_trade(t))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)
    log.info(f"Returning {len(result)} Trade objects")

    return result


def store_trades(trades: list,
                 collection: str = COLL_TRADES,
                 ):
    """Store one or more Trade() objects in central storage."""
    # Convert Trade objects to dictionaries for storage
    log.info(f"Preparing {len(trades)} trades to store by converting to dicts")
    working_trades = []
    for t in trades:
        working_trades.append(t.to_clean_dict())

    # Store in database
    log.info(f"Writing {len(working_trades)} trades to "
             f"collection={collection}")
    result = dhm.store_trades(trades=working_trades,
                              collection=collection)
    log.info("Storage complete, returning result")

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
    """Provide aggregate summary data about trades in central storage.

    Optionally filters by bt_id and/or ts_id.  Earliest and latest dates
    are returned as strings; include_epochs can also provide them as epochs.

    multi_ok should be a list of strings that can appear in a Trade's
    ts_id, typically all or part of a Backtest's bt_id.  A trade that
    matches any string in this list will not be flagged for spanning
    multiple days.

    pretty=True returns in a print friendly, multiline, indended string
    format.
    """
    if multi_ok is None:
        multi_ok = []
    review = dhm.review_trades(symbol=symbol,
                               collection=collection,
                               bt_id=bt_id,
                               ts_id=ts_id,
                               )
    for t in review:
        t["earliest"] = dt_as_str(dt_from_epoch(t["earliest_epoch"]))
        t["latest"] = dt_as_str(dt_from_epoch(t["latest_epoch"]))
        if not include_epochs:
            t.pop("earliest_epoch")
            t.pop("latest_epoch")

    if check_integrity:
        log_say("Checking integrity of all Trades in storage")
        time_full = OperationTimer(
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
        autoclosed_issues = {}
        total_trades = 0
        unique_trades = 0

        # Cache all events for autoclosed trade integrity checking
        log_say("Caching all events for autoclosed integrity checks")
        all_events = get_events(symbol="ES")
        # Create a set of event start_dt strings for fast lookup
        event_start_times = {event.start_dt for event in all_events}
        log_say(f"Cached {len(event_start_times)} unique event "
                f"start times")

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
                # Check autoclosed trades for integrity
                if "autoclosed" in t.tags and t.close_time != "15:55:00":
                    # Calculate expected event start time (5 min after close)
                    close_dt_obj = dt_as_dt(t.close_dt)
                    expected_event_start = close_dt_obj + timedelta(
                        minutes=5)
                    expected_start_str = dt_as_str(
                        expected_event_start)
                    # Check if expected event start time exists in cached
                    # events
                    if expected_start_str not in event_start_times:
                        # No matching event found, record integrity issue
                        if t.close_dt not in autoclosed_issues:
                            autoclosed_issues[t.close_dt] = 0
                        autoclosed_issues[t.close_dt] += 1
                        log.warning(
                            f"Autoclosed trade integrity issue: "
                            f"close_dt={t.close_dt}, expected event at "
                            f"{expected_start_str} not found")
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
        if len(autoclosed_issues) > 0:
            status = "ERRORS"
            total_autoclosed_bad = sum(autoclosed_issues.values())
            issues.append(
                f"{total_autoclosed_bad} autoclosed trade integrity "
                f"issues found across {len(autoclosed_issues)} unique "
                f"close_dt values")
        if len(issues) == 0:
            issues = None
        integrity = {"status": status,
                     "issues": issues,
                     "total_trades": total_trades,
                     "unique_trades": unique_trades,
                     "duplicate_trades": len(duplicates),
                     "invalid_multiday_trades": len(multidays),
                     "autoclosed_integrity_issues": len(autoclosed_issues),
                     "total_autoclosed_bad_trades": sum(
                         autoclosed_issues.values()) if autoclosed_issues
                     else 0
                     }
        if list_issues:
            integrity["duplicates"] = duplicates
            integrity["multidays"] = multidays
            integrity["autoclosed_issues"] = autoclosed_issues
        time_full.stop()
        print(time_full.summary())
    else:
        integrity = {"status": "integrity checks were not run"}

    result = {"integrity": integrity, "review": review}
    if out_path is not None:
        # Write result and any issues found to disk
        filename = Path(out_path) / out_file
        blob = deepcopy(result)
        # Add issue details for disk output if not already included
        if not list_issues:
            blob["integrity"]["duplicates"] = duplicates
            blob["integrity"]["multidays"] = multidays
            blob["integrity"]["autoclosed_issues"] = autoclosed_issues
        with open(filename, "w") as f:
            f.write(json.dumps(blob))
        log_say(f"Wrote integrity results and issues to {filename}")

    if pretty:
        return json.dumps(result,
                          indent=4,
                          )
    else:
        return result


def delete_trades_by_field(symbol: str,
                           field: str,
                           value,
                           collection: str = COLL_TRADES,
                           ):
    """Delete all trade records with 'field' matching 'value'.

    Typically used to delete by name, ts_id, or bt_id fields.

    Example to delete all trade records with name=="DELETEME":
    delete_trades_by_field(symbol="ES", field="name",
    value="DELETEME")
    """
    result = dhm.delete_trades_by_field(symbol=symbol,
                                        collection=collection,
                                        field=field,
                                        value=value,
                                        )

    return result


def delete_trades(trades: list,
                  collection: str = COLL_TRADES,
                  ):
    """Delete Trade objects from central storage by open_dt, ts_id, and symbol.
    """
    # Extract identifying fields from Trade objects
    query_dicts = []
    for t in trades:
        query_dicts.append({
            "open_dt": t.open_dt,
            "ts_id": t.ts_id,
            "symbol": t.symbol.ticker,
        })

    result = dhm.delete_trades(trades=query_dicts,
                               collection=collection)

    return result


##############################################################################
# TradeSeries
def reconstruct_tradeseries(ts):
    """Takes a dictionary and builds a Trade() object from it.

    Primarily used by other functions to convert results retrieved from
    storage.
    """
    return TradeSeries(start_dt=ts["start_dt"],
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
                        limit=0,
                        show_progress: bool = False,
                        ):
    """Get <limit> (default 0 == all) stored tradeseries returned as a list."""
    result = []
    r = dhm.get_all_records_by_collection(collection=collection,
                                          limit=limit,
                                          show_progress=show_progress)
    total = len(r)
    pbar = start_progbar(show_progress, total,
                         "TradeSeries objects built")
    for i, t in enumerate(r, start=1):
        result.append(reconstruct_tradeseries(t))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def get_tradeseries_by_field(field: str,
                             value,
                             collection: str = COLL_TRADESERIES,
                             collection_trades: str = COLL_TRADES,
                             limit=0,
                             include_trades: bool = True,
                             show_progress: bool = False,
                             ):
    """Returns a list of all TradeSeries matching the bt_id provided."""
    # Retrieve TradeSeries from storage
    log.info(f"Retrieving TradeSeries by {field}={value} with limit={limit} "
             f"and include_trades={include_trades}")
    result = []
    r = dhm.get_tradeseries_by_field(field=field,
                                     value=value,
                                     collection=collection,
                                     limit=limit,
                                     show_progress=show_progress,
                                     )
    log.info(f"Retrieved {len(r)} TradeSeries records")

    # Reconstruct TradeSeries objects and optionally load trades
    log.info("Building TradeSeries objects and optionally loading trades")
    total = len(r)
    pbar = start_progbar(show_progress, total,
                         "TradeSeries objects built")
    for i, t in enumerate(r, start=1):
        ts = reconstruct_tradeseries(t)
        if include_trades:
            log.info(f"Loading trades for ts_id={ts.ts_id}")
            ts.trades = get_trades_by_field(field="ts_id",
                                            value=ts.ts_id,
                                            collection=collection_trades,
                                            )
            ts.sort_trades()
        result.append(ts)
        update_progbar(pbar, i, total)
    finish_progbar(pbar)
    log.info(f"Finished reconstruction, returning {len(result)} "
             f"TradeSeries")

    return result


def store_tradeseries(series: list,
                      collection: str = COLL_TRADESERIES,
                      include_trades: bool = False,
                      ):
    """Store a list of TradeSeries() objects in central storage.

    Args:
        series: List of TradeSeries objects to store
        collection: MongoDB collection name for tradeseries
        include_trades: If True, also stores all trades from each tradeseries

    Returns:
        List of storage results, with trades_result if include_trades=True
    """
    # Store TradeSeries in database
    log.info(f"Storing {len(series)} TradeSeries in collection={collection} "
             f"include_trades={include_trades}")
    result = []
    for ts in series:
        ts_result = dhm.store_tradeseries(ts.to_clean_dict(),
                                          collection=collection,
                                          )
        ts_result["ts_id"] = ts.ts_id

        # Optionally store trades from this tradeseries
        if include_trades and len(ts.trades) > 0:
            log.info(f"include_trades=True, storing {len(ts.trades)} trades "
                     f"from ts_id={ts.ts_id}")
            trades_result = store_trades(trades=ts.trades)
            ts_result["trades_result"] = trades_result
        elif include_trades:
            log.info(f"include_trades=True but ts_id={ts.ts_id} has no "
                     "trades to store")
            ts_result["trades_result"] = []

        result.append(ts_result)

    log.info(f"Storage complete, {len(result)} records written")

    return result


def review_tradeseries(symbol: str = "ES",
                       collection: str = COLL_TRADESERIES,
                       bt_id: str = None,
                       include_trades: bool = False,
                       pretty: bool = False,
                       check_integrity: bool = False,
                       ):
    """Provide aggregate summary data about tradeseries in central storage.

    Optionally filters by bt_id.  Earliest start_dt and latest end_dt
    are returned as strings.  pretty=True returns a formatted string.
    """
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
                open_tf = this_candle_start(dt=t.open_dt,
                                            timeframe=ts.timeframe)
                close_tf = this_candle_start(dt=t.close_dt,
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


def delete_tradeseries_by_field(symbol: str,
                                field: str,
                                value,
                                collection: str = COLL_TRADESERIES,
                                coll_trades=COLL_TRADES,
                                include_trades: bool = False,
                                ):
    """Delete all tradeseries records with 'field' matching 'value'.

    Typically used to delete by ts_id or bt_id fields.
    """
    result = {}
    result["tradeseries"] = dhm.delete_tradeseries_by_field(
            symbol=symbol,
            collection=collection,
            field=field,
            value=value,
            )
    if include_trades:
        result["trades"] = delete_trades_by_field(symbol=symbol,
                                                  field=field,
                                                  value=value,
                                                  collection=coll_trades,
                                                  )

    return result


def delete_tradeseries(tradeseries: list,
                       collection: str = COLL_TRADESERIES,
                       ):
    """Delete TradeSeries objects from central storage using ts_id.
    """
    # Extract ts_id from TradeSeries objects
    ts_ids = []
    for ts in tradeseries:
        ts_ids.append(ts.ts_id)

    result = dhm.delete_tradeseries(ts_ids=ts_ids,
                                    collection=collection)

    return result


##############################################################################
# Backtests
def get_all_backtests(collection: str = COLL_BACKTESTS,
                      limit=0,
                      show_progress: bool = False,
                      ):
    """Return stored backtests as a list of dicts.

    limit defaults to 0 which returns all records.  Because Backtest() is
    meant to be subclassed, dicts are returned so subclass implementations
    can convert them into their specific object types as needed.
    """
    return dhm.get_all_records_by_collection(collection=collection,
                                             limit=limit,
                                             show_progress=show_progress)


def get_backtests_by_field(field: str,
                           value,
                           collection: str = COLL_BACKTESTS,
                           limit=0,
                           show_progress: bool = False,
                           ):
    """Return a list of Backtest dicts matching the given field=value.

    Because Backtest() is meant to be subclassed, dicts are returned so
    subclass implementations can convert them into their specific object
    types as needed.
    """
    result = dhm.get_backtests_by_field(field=field,
                                        value=value,
                                        collection=collection,
                                        limit=limit,
                                        show_progress=show_progress,
                                        )

    return result


def store_backtests(backtests: list,
                    collection: str = COLL_BACKTESTS,
                    include_tradeseries: bool = False,
                    include_trades: bool = False,
                    ):
    """Store one or more Backtest() objects in central storage.

    Args:
        backtests: List of Backtest objects to store
        collection: MongoDB collection name for backtests
        include_tradeseries: If True, also stores all tradeseries from each
                            backtest
        include_trades: If True, also stores all trades (requires
                       include_tradeseries=True)

    Returns:
        List of storage results, with tradeseries_result included if
        include_tradeseries=True

    Raises:
        ValueError: If include_trades=True but include_tradeseries=False
    """
    # Validate arguments
    if include_trades and not include_tradeseries:
        raise ValueError("include_trades=True requires "
                         "include_tradeseries=True")

    log.info(f"Storing {len(backtests)} Backtests in "
             f"collection={collection} include_tradeseries="
             f"{include_tradeseries} include_trades={include_trades}")

    result = []
    for bt in backtests:
        bt_result = dhm.store_backtest(bt.to_clean_dict(),
                                       collection=collection,
                                       )
        bt_result["bt_id"] = bt.bt_id

        # Optionally store tradeseries from this backtest
        if include_tradeseries and len(bt.tradeseries) > 0:
            log.info(f"include_tradeseries=True, storing "
                     f"{len(bt.tradeseries)} tradeseries from "
                     f"bt_id={bt.bt_id}")
            ts_result = store_tradeseries(series=bt.tradeseries,
                                          include_trades=include_trades,
                                          )
            bt_result["tradeseries_result"] = ts_result
        elif include_tradeseries:
            log.info(f"include_tradeseries=True but bt_id={bt.bt_id} has no "
                     "tradeseries to store")
            bt_result["tradeseries_result"] = []

        result.append(bt_result)

    log.info(f"Storage complete, {len(result)} backtests written")

    return result


def review_backtests(symbol: str = "ES",
                     collection: str = COLL_BACKTESTS,
                     include_tradeseries: bool = False,
                     include_trades: bool = False,
                     pretty: bool = False,
                     ):
    """Provides aggregate summary data about backtests in central storage."""
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


def delete_backtests_by_field(symbol: str,
                              field: str,
                              value,
                              collection: str = COLL_BACKTESTS,
                              coll_tradeseries: str = COLL_TRADESERIES,
                              coll_trades: str = COLL_TRADES,
                              include_tradeseries: bool = False,
                              include_trades: bool = False,
                              ):
    """Delete all backtests records with 'field' matching 'value'.

    Typically used to delete by bt_id field.
    """
    result = {}
    result["backtests"] = dhm.delete_backtests_by_field(
            symbol=symbol,
            collection=collection,
            field=field,
            value=value,
            )
    if include_tradeseries:
        result["tradeseries"] = delete_tradeseries_by_field(
                symbol=symbol,
                field=field,
                value=value,
                collection=coll_tradeseries,
                coll_trades=coll_trades,
                include_trades=include_trades,
                )

    return result


def delete_backtests(backtests: list,
                     collection: str = COLL_BACKTESTS,
                     ):
    """Delete Backtest objects from central storage using bt_id.
    """
    # Extract bt_id from Backtest objects
    bt_ids = []
    for bt in backtests:
        bt_ids.append(bt.bt_id)

    result = dhm.delete_backtests(bt_ids=bt_ids,
                                  collection=collection)

    return result


##############################################################################
# Indicators
def list_indicators(meta_collection: str = COLL_IND_META):
    """Return a simple list of indicators in storage."""
    result = dhm.list_indicators(meta_collection=meta_collection)

    return result


def list_indicators_names(meta_collection: str = COLL_IND_META):
    """Return a simple list of indicators (ind_id only) in storage."""
    indicators = dhm.list_indicators(meta_collection=meta_collection)
    result = []
    for i in indicators:
        result.append(i['ind_id'])

    return result


def review_indicators(meta_collection: str = COLL_IND_META,
                      dp_collection: str = COLL_IND_DPS):
    """Return a more detailed overview of indicators in storage."""
    result = dhm.review_indicators(meta_collection=meta_collection,
                                   dp_collection=dp_collection,
                                   )

    return result


def get_indicator(ind_id: str,
                  meta_collection: str = COLL_IND_META,
                  autoload_chart: bool = False,
                  autoload_datapoints: bool = False,
                  ):
    """Return an indicator by ind_id, optionally loading chart and datapoints.

    autoload_chart and autoload_datapoints both default to False for
    performance.  When enabled they load for the earliest_dt to latest_dt
    range.
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
        result = IndicatorSMA(description=i["description"],
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
        result = IndicatorEMA(description=i["description"],
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
                             show_progress: bool = False,
                             ):
    """Return IndicatorDatapoint objects for the given timeframe and ind_id.
    """
    # Retrieve datapoints from storage
    msg = f"Retrieving datapoints for ind_id={ind_id}"
    if earliest_dt or latest_dt:
        msg = " ".join([msg, f"with date range: {earliest_dt} to "
                        f"{latest_dt}"])
    else:
        msg = " ".join([msg, "including all dates available"])
    log.info(msg)
    working = dhm.get_indicator_datapoints(ind_id=ind_id,
                                           dp_collection=dp_collection,
                                           earliest_dt=earliest_dt,
                                           latest_dt=latest_dt,
                                           )
    log.info(f"Retrieved {len(working)} raw datapoint records")

    # Build IndicatorDataPoint objects
    log.info("Building IndicatorDataPoint objects")
    result = []
    total = len(working)
    pbar = start_progbar(show_progress, total,
                         "IndicatorDataPoint objects built")
    for i, d in enumerate(working, start=1):
        result.append(IndicatorDataPoint(dt=d["dt"],
                                         value=d["value"],
                                         ind_id=d["ind_id"],
                                         epoch=d["epoch"]
                                         ))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)
    log.info(f"Returning {len(result)} datapoints")

    return result


def store_indicator_datapoints(datapoints: list,
                               collection: str = COLL_IND_DPS,
                               skip_dupes: bool = True,
                               ):
    """Store one or more IndicatorDatapoint() objects in central storage."""
    log.info(f"Processing {len(datapoints)} datapoints with "
             f"skip_dupes={skip_dupes}")
    store_dps = []
    r_skipped = []
    for d in datapoints:
        if skip_dupes:
            log.info("Checking for duplicates already in storage")
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

    op_timer = OperationTimer(name="Indicator Datapoints Storage Job")
    log.info(f"Storing {len(store_dps)} datapoints to collection={collection}")
    r_stored = dhm.store_indicator_datapoints(datapoints=store_dps,
                                              collection=collection,
                                              )
    op_timer.stop()
    log.info(f"Complete: {len(store_dps)} stored, {len(r_skipped)} skipped")
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
    """Store indicator meta and datapoints in central storage.

    Does not overwrite existing datapoints unless overwrite_dp is True
    """
    op_timer = OperationTimer(name="Indicator Storage Job")
    # First store/replace the indicator meta doc itself
    i = indicator.to_clean_dict()
    i["datapoints"] = len(indicator.datapoints)
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
        earliest = dt_to_epoch(dt.now())
        latest = 0
        for d in indicator.datapoints:
            earliest = min(d.epoch, earliest)
            latest = max(d.epoch, latest)
        earliest_to_store = dt_from_epoch(earliest)
        latest_to_store = dt_from_epoch(latest)

        # Retrieve all stored datapoints for this timeframe
        dps_in_storage = get_indicator_datapoints(
                ind_id=indicator.ind_id,
                earliest_dt=earliest_to_store,
                latest_dt=latest_to_store,
                )

        # Put them in a dict for easier comparison to each datapoint
        checkers = {}
        earliest = dt_to_epoch(dt.now())
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
                    s = store_indicator_datapoints([d],
                                                   collection=dp_collection,
                                                   skip_dupes=False)
                else:
                    s = {"skipped": [d.to_clean_dict()],
                         "stored": [],
                         "elapsed": None}
                    bar_total -= 1

            else:
                # Slower mode that verifies each datapoint vs those stored
                # and only writes if it's missing or different
                # If we found a stored datapoint with the same epoch, pass it
                # to the storage function to compare and store on diffs
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
                    # Compare with stored datapoint
                    if d == checkers[d.epoch]:
                        s = {"skipped": [d.to_clean_dict()],
                             "stored": [],
                             "elapsed": None}
                    else:
                        s = store_indicator_datapoints(
                            [d],
                            collection=dp_collection,
                            skip_dupes=False)
                # Otherwise just store it, overwriting any existing
                else:
                    s = store_indicator_datapoints([d],
                                                   collection=dp_collection,
                                                   skip_dupes=False)
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
    """Remove an indicator and all its datapoints from storage by ind_id.
    """
    return dhm.delete_indicator(ind_id=ind_id,
                                meta_collection=meta_collection,
                                dp_collection=dp_collection,
                                )


##############################################################################
# Symbols

def get_symbol_by_ticker(ticker: str):
    """Return a Symbol by ticker name, caching instances for performance.

    This is a temporary function that should eventually be replaced by
    proper storage and retrieval functions.  Caching reuses Symbol instances
    so their internal caches (e.g., closed_hours_cache) are shared across
    all Charts and Indicators.
    """
    if ticker in ["ES", "DELETEME"]:
        if ticker not in SYMBOL_CACHE:
            SYMBOL_CACHE[ticker] = Symbol(ticker=ticker,
                                          name=ticker,
                                          leverage_ratio=50,
                                          tick_size=0.25,
                                          )
        return SYMBOL_CACHE[ticker]
    else:
        raise ValueError("Only ['ES', 'DELETEME'] is currently supported as "
                         f"Symbol ticker, we got {ticker}")

##############################################################################
# Candles


def store_candle(candle):
    """Write a single Candle() to central storage."""
    log.debug(f"Storing {candle.c_symbol.ticker} "
              f"{candle.c_timeframe} candle at {candle.c_datetime}")
    valid_timeframe(candle.c_timeframe)
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


def store_candles(candles):
    """Write multiple dhtypes.Candle() objects to central storage."""
    for candle in candles:
        store_candle(candle)


def get_candles(start_epoch: int,
                end_epoch: int,
                timeframe: str,
                symbol: str = "ES",
                show_progress: bool = False,
                ):
    """Return candle docs within the given start and end epochs, inclusive.
    """
    # Retrieve candle dictionaries from storage
    log.info(f"Retrieving candles from storage for {symbol} "
             f"{timeframe} between "
             f"{dt_as_str(dt_from_epoch(start_epoch))} and "
             f"{dt_as_str(dt_from_epoch(end_epoch))}")
    result = dhm.get_candles(start_epoch=start_epoch,
                             end_epoch=end_epoch,
                             timeframe=timeframe,
                             symbol=symbol,
                             show_progress=show_progress,
                             )
    log.info("Finished retrieval from storage")

    # Build Candle() objects from retrieved dictionaries
    log.info("Building Candle objects from retrieved data")
    candles = []
    total = len(result)
    pbar = start_progbar(show_progress, total,
                         f"{symbol} {timeframe} Candle objects built")
    for i, r in enumerate(result, start=1):
        candles.append(Candle(c_datetime=r["c_datetime"],
                              c_timeframe=r["c_timeframe"],
                              c_open=r["c_open"],
                              c_high=r["c_high"],
                              c_low=r["c_low"],
                              c_close=r["c_close"],
                              c_volume=r["c_volume"],
                              c_symbol=r["c_symbol"],
                              c_epoch=r["c_epoch"],
                              ))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)
    log.info("Finished building Candle objects, returning "
             f"{len(candles)} candles")

    return candles


def review_candles(timeframe: str,
                   symbol="ES",
                   check_integrity: bool = False,
                   show_progress: bool = False,
                   return_detail: bool = False,
                   ):
    """Provide aggregate summary data about candles in central storage.

    Options to further check completeness/integrity of candle data and
    provide remediation.
    """
    if isinstance(symbol, str):
        symbol = get_symbol_by_ticker(ticker=symbol)
    log_say("Retrieving candles overview from storage")
    log.info(f"Retrieving candles from storage for {symbol} {timeframe}")
    overview = dhm.review_candles(timeframe=timeframe,
                                  symbol=symbol.ticker,
                                  )
    log_say(f"Finished retrieval from storage for {symbol} {timeframe}")
    if overview is None:
        log_say(f"No candles found for the specified timeframe {timeframe}")
        return None
    start_epoch = dt_to_epoch(overview["earliest_dt"])
    end_epoch = dt_to_epoch(overview["latest_dt"])
    if check_integrity:
        log_say("Starting integrity checks and gap analysis because "
                "check_integrity=True")
        log_say("Retrieving candles from storage for "
                f"{symbol} {timeframe} between "
                f"{dt_as_str(dt_from_epoch(start_epoch))} and "
                f"{dt_as_str(dt_from_epoch(end_epoch))}")
        candles = get_candles(timeframe=timeframe,
                              symbol=symbol.ticker,
                              start_epoch=start_epoch,
                              end_epoch=end_epoch,
                              )

        # Perform a basic check on the times list vs expected for the timeframe
        log_say("Summarizing retrieved candles")
        breakdown = summarize_candles(timeframe=timeframe,
                                      symbol=symbol,
                                      candles=candles,)
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
        log_say("Performing detailed analysis of actual vs expected "
                "candle datetimes")
        dt_actual = []
        for c in candles:
            dt_actual.append(dt_as_str(c.c_datetime))
        start_dt = dt_from_epoch(start_epoch)
        end_dt = dt_from_epoch(end_epoch)
        log_say(f"Fetching all events from storage for {symbol} in the "
                "target datetime range")
        all_events = get_events(start_epoch=start_epoch,
                                end_epoch=end_epoch,
                                symbol=symbol,
                                )
        log_say("Calculating expected candle datetimes for "
                f"{symbol.ticker} {timeframe} between "
                f"{dt_as_str(start_dt)} and {dt_as_str(end_dt)}")
        dt_expected = expected_candle_datetimes(start_dt=start_dt,
                                                end_dt=end_dt,
                                                symbol=symbol,
                                                timeframe=timeframe,
                                                events=all_events,
                                                show_progress=show_progress,
                                                )
        log_say("Finished calculating expected datetimes, starting "
                "comparison of actual (stored) vs expected candles")

        # Convert expected to strings for comparison and review
        dt_expected_str = []
        for d in dt_expected:
            dt_expected_str.append(dt_as_str(d))
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
        missing_candles_by_date = sort_dict(dict(missing_candles_by_date))
        missing_count_by_date = {}
        for k, v in missing_candles_by_date.items():
            missing_count_by_date[k] = len(v)
        missing_candles_by_hour = defaultdict(list)
        for c in missing_from_actual:
            k = c.split(' ')[1].split(':')[0]
            missing_candles_by_hour[k].append(c)
        missing_candles_by_hour = sort_dict(dict(missing_candles_by_hour))
        missing_count_by_hour = {}
        for k, v in missing_candles_by_hour.items():
            missing_count_by_hour[k] = len(v)
        unexpected_in_actual = sorted(set_actual - set_expected)
        unexpected_candles_count = len(unexpected_in_actual)
        # Log individual candle issues at DEBUG level
        for c in missing_from_actual:
            log.debug(f"{symbol} {timeframe} MISSING: {c}")
        for c in unexpected_in_actual:
            log.debug(f"{symbol} {timeframe} UNEXPECTED: {c}")
        # Create human digestible ranges
        missing_ranges = rangify_candle_times(times=missing_from_actual,
                                              timeframe=timeframe)
        unexpected_ranges = rangify_candle_times(times=unexpected_in_actual,
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
        log_say("Skipping integrity checks and gap analysis because "
                "check_integrity=False")
        integrity_data = None
        breakdown = None
        summary_data = None
        summary_expected = None
        gap_analysis = None

    if return_detail:
        log_say("Returning detailed review")
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
        log_say("Returning summary review")
        result = {"overview": overview,
                  "integrity_data": integrity_data,
                  "gap_analysis": gap_analysis,
                  }
        if result["gap_analysis"] is not None:
            result["gap_analysis"].pop("missing_candles")
            result["gap_analysis"].pop("unexpected_candles")

    return result


def delete_candles(timeframe: str,
                   symbol: str,
                   earliest_dt=None,
                   latest_dt=None,
                   ):
    """Delete candles from central storage en masse or for a datetime range.
    """
    if earliest_dt is None and latest_dt is None:
        return dhm.clear_collection(f"candles_{symbol}_{timeframe}")
    else:
        return dhm.delete_candles(timeframe=timeframe,
                                  symbol=symbol,
                                  earliest_dt=earliest_dt,
                                  latest_dt=latest_dt,
                                  )


##############################################################################
# Events
def store_event(event):
    """Write a single Event() to central storage."""
    if not isinstance(event, Event):
        raise TypeError(f"event {type(event)} must be a "
                        "<class Event> object")
    log.debug(f"Storing event: {str(event)}")
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
               show_progress: bool = False,
               ):
    """Return events starting within the given start and end epochs, inclusive.

    Note: events that end after end_epoch are included so long as they
    start before or on it.
    """
    if isinstance(symbol, str):
        symbol = get_symbol_by_ticker(ticker=symbol)
    if start_epoch is None:
        start_epoch = 0
    if end_epoch is None:
        end_epoch = dt_to_epoch(dt.now())

    # Retrieve events from storage
    msg = (f"Retrieving events for {symbol.ticker} between "
           f"{dt_as_str(dt_from_epoch(start_epoch))} and "
           f"{dt_as_str(dt_from_epoch(end_epoch))}")
    if categories:
        " ".join([msg, f"Filtering by categories: {categories}"])
    if tags:
        " ".join([msg, f"Filtering by tags: {tags}"])
    log.info(msg)
    result = dhm.get_events(start_epoch=start_epoch,
                            end_epoch=end_epoch,
                            symbol=symbol.ticker,
                            categories=categories,
                            tags=tags,
                            )
    log.info(f"Retrieved {len(result)} event records from storage")

    # Build Event objects
    log.info("Building Event objects")
    events = []
    total = len(result)
    pbar = start_progbar(show_progress, total,
                         "Event objects built")
    for i, r in enumerate(result, start=1):
        events.append(Event(start_dt=r["start_dt"],
                            end_dt=r["end_dt"],
                            symbol=symbol,
                            category=r["category"],
                            tags=r["tags"],
                            notes=r["notes"],
                            ))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)
    log.info(f"Returning {len(events)} events")

    return events


def clear_events(symbol: str,
                 earliest_dt=None,
                 latest_dt=None,
                 ):
    """Deletes events from central storage."""
    if earliest_dt is None and latest_dt is None:
        return dhm.clear_collection(f"events_{symbol}")
    else:
        return "Sorry, Dusty hasn't written code for select timeframes yet"
