"""Low-level MongoDB operations for dhtrader data storage and retrieval.

This module provides MongoDB operations for candles, trades, backtests,
indicators, and events. These functions are wrapped by corresponding
functions in dhstore.py, which provides a storage abstraction layer
to allow migration to a different database without requiring changes
to higher-level code.

Running this script ad hoc will perform a basic connect, write, read test.

REF: https://github.com/mongodb-university/atlas_starter_python/blob
     /master/atlas-starter.py
"""

import os
import sys
import pymongo
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError
import logging
from dotenv import load_dotenv, find_dotenv
from datetime import datetime as dt
from .dhcommon import (
    ProgBar, prompt_yn, valid_timeframe, dt_as_str, dt_from_epoch,
    dt_to_epoch)

log = logging.getLogger("dhmongo")
log.addHandler(logging.NullHandler())


# Establish mongo connection parameters and client
MONGO_ENV_FILE = 'mongo.env'
load_dotenv(find_dotenv(MONGO_ENV_FILE))

MONGO_CONN = os.getenv("MONGO_CONN")
MONGO_DB = os.getenv("MONGO_DB")
if MONGO_CONN is None:
    raise Exception(f"Unable to retrieve MONGO_CONN from {MONGO_ENV_FILE}")
if MONGO_DB is None:
    raise Exception(f"Unable to retrieve MONGO_DB from {MONGO_ENV_FILE}")

try:
    mc = pymongo.MongoClient(MONGO_CONN)
    db = mc[MONGO_DB]
except Exception:
    print("\n\nWell that failed.  So sad!")
    print("\nThere's a good chance it's because my current IP address is not "
          "allowed on the Atlas side.")
    print("Log into the Atlas web interface > Clusters > Cluster0 > Connect "
          "button.  It should show a dialogue that allows me to add it.")
    print("If this keeps happening maybe add a subnet range too?")
    sys.exit()


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
def review_database():
    """Quick function to gather useful information about the state of mongo."""
    raw = db.command("dbstats")
    overview = {"collection": raw["collections"],
                "objects": raw["objects"],
                "data_GB": round(raw["dataSize"]/1024/1024/1024, 2),
                "storage_GB": round(raw["storageSize"]/1024/1024/1024, 2),
                }
    result = {"raw": raw,
              "overview": overview,
              }

    return result


def list_collections():
    """List all current collections in mongo."""
    return db.list_collection_names()


def clear_collection(collection: str):
    """Delete all records from a collection, preserving indexes and settings.
    """
    c = db[collection]
    if not prompt_yn(f"Clear all records from collection "
                     f"'{collection}'? This cannot be undone.  You should "
                     "probably do a backup first..."):
        return None

    return c.delete_many({})


def drop_collection(collection: str):
    """Irretrievable drops a collection from the store.

    Use carefully!
    """
    c = db[collection]
    if not prompt_yn(f"Drop collection '{collection}'? This cannot be"
                     "undone and will delete all data, indexes, and other "
                     "collection settings. You probably actually want to "
                     "use clear_collection() instead, right?  Are you "
                     "SURE you want to drop this?"):
        return None

    return c.drop()


def get_all_records_by_collection(collection: str,
                                  limit=0,
                                  show_progress: bool = False,
                                  ):
    """Return records from a collection, typically via dhstore wrappers.

    limit defaults to 0 which returns all records.
    """
    c = db[collection]
    total = c.count_documents({})
    if limit > 0:
        total = min(total, limit)
    pbar = start_progbar(show_progress, total,
                         f"records fetched from {collection}")
    cursor = c.find().limit(limit)
    result = []
    for i, doc in enumerate(cursor, start=1):
        result.append(doc)
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def run_query(query, collection: str, show_progress: bool = False):
    """Run a standard mongo query and return the result."""
    c = db[collection]
    total = c.count_documents(query)
    pbar = start_progbar(show_progress, total,
                         f"records fetched from {collection}")
    cursor = c.find(query)
    result = []
    for i, doc in enumerate(cursor, start=1):
        result.append(doc)
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def count_records_by_field(collection: str,
                           field: str,
                           ):
    """Return aggregation with count of records split by a single field."""
    c = db[collection]
    result = list(c.aggregate([{"$group": {"_id": f"${field}",
                                           "count": {"$sum": 1},
                                           }}]))
    for r in result:
        r[field] = r.pop("_id")

    return result


def list_values_by_regex_match(values_field: str,
                               regex_field: str,
                               regex: str,
                               collection: str,
                               ):
    """Return unique values in values_field split by regex match/nomatch.

    Example: review tags based on whether a Trade closed in the autoclose
    timeframe, i.e. split by those closing at 15:55:00 vs. those that do
    not.
    """
    c = db[collection]
    pipeline = [
        {
            # Add a field to categorize by match vs no match
            "$addFields": {
                "matches_regex": {
                    "$cond": {
                        "if": {"$regexMatch":
                               {"input": f"${regex_field}",
                                "regex": regex}
                               },
                        "then": True,
                        "else": False
                    }
                }
            }
        },
        {
            # Group and collect unique values
            "$group": {
                "_id": "$matches_regex",
                "unique_values": {"$addToSet": f"${values_field}"}
            }
        }
    ]
    return list(c.aggregate(pipeline))


def update_records_value(search: dict,
                         update_field: str,
                         update_value: str,
                         collection: str,
                         ):
    """Update all records matching a mongo filter with a new field value.

    'search' should be a dict representing the mongo filter.

    Example replacing bt_id 'foo' with 'bar' on all matching backtests::

        update_record_value(
            search={"bt_id": "foo"},
            update_field="bt_id",
            update_value="bar",
            collection="backtests"
        )
    """
    c = db[collection]
    result = c.update_many(search,
                           {"$set": {update_field: update_value}}
                           )

    return result


def delete_one_document(query: dict,
                        collection: str,
                        ):
    """Delete a single document from storage using the provided query.

    This is primarily called by more specific functions in dhstore.py for
    individual object types using relevant parameters to build the query
    there.
    """
    c = db[collection]
    result = c.delete_one(query)

    return result

##############################################################################
# Trades


def get_trades_by_field(field: str,
                        value,
                        collection: str,
                        limit=0,
                        show_progress: bool = False,
                        ):
    """Returns <limit> (0 = all) Trade documents matching (field == value)."""
    c = db[collection]
    total = c.count_documents({field: value})
    if limit > 0:
        total = min(total, limit)
    pbar = start_progbar(show_progress, total,
                         f"trade records fetched from {collection}")
    cursor = c.find({field: value}).limit(limit)
    result = []
    for i, doc in enumerate(cursor, start=1):
        result.append(doc)
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def store_trades(trades: list,
                 collection: str,
                 ):
    """Store one or more trades in mongo using bulk operations."""
    c = db[collection]

    if not trades:
        return []

    # Build bulk operations list
    operations = []
    for t in trades:
        filter_doc = {
            "open_dt": t["open_dt"],
            "direction": t["direction"],
            "name": t["name"],
            "version": t["version"],
            "symbol": t["symbol"],
            "ts_id": t["ts_id"],
            "bt_id": t["bt_id"],
        }
        operations.append(ReplaceOne(filter_doc, t, upsert=True))

    # Execute as single bulk operation and return only confirmed successes.
    successful_indexes = set(range(len(trades)))
    try:
        c.bulk_write(operations, ordered=False)
    except BulkWriteError as err:
        details = err.details or {}
        failed_indexes = {
            we.get("index")
            for we in details.get("writeErrors", [])
            if isinstance(we.get("index"), int)
        }
        successful_indexes -= failed_indexes

        # Write concern errors make confirmation ambiguous. Be conservative.
        if details.get("writeConcernErrors"):
            log.error("store_trades encountered writeConcernErrors; "
                      "cannot confirm per-trade success")
            return []

    return [dict(trades[i])
            for i in range(len(trades))
            if i in successful_indexes]


def review_trades(symbol: str,
                  collection: str,
                  bt_id: str = None,
                  ts_id: str = None,
                  ):
    """Provides aggregate summary data about trades in mongo."""
    c = db[collection]
    match = {"symbol": symbol}
    group = {"_id": {"name": "$name"},
             "earliest_epoch": {"$min": "$open_epoch"},
             "latest_epoch": {"$max": "$open_epoch"},
             }
    if bt_id is not None:
        match["bt_id"] = bt_id
        group["_id"]["bt_id"] = "$bt_id"
    if ts_id is not None:
        match["ts_id"] = ts_id
        group["_id"]["ts_id"] = "$ts_id"
    group["count"] = {"$sum": 1}
    try:
        trades = list(c.aggregate([{"$match": match},
                                   {"$group": group}]))

        return trades
    # IndexError is raised if collection does not exist yet
    except IndexError:
        return None


def delete_trades_by_field(symbol: str,
                           field: str,
                           value,
                           collection: str,
                           ):
    """Delete all trade records with 'field' matching 'value'.

    Typically used to delete by name, ts_id, or bt_id fields.

    Example to delete all trade records with name=="DELETEME":
    delete_trades_by_field(symbol="ES", field="name",
    value="DELETEME")
    """
    c = db[collection]
    result = c.delete_many({field: value})

    return result


def delete_trades(trades: list,
                  collection: str,
                  ):
    """Delete trades from mongo using open_dt, ts_id, and symbol as keys.
    """
    c = db[collection]
    result = []
    for t in trades:
        r = c.find_one_and_delete({"open_dt": t["open_dt"],
                                   "ts_id": t["ts_id"],
                                   "symbol": t["symbol"],
                                   })
        result.append(r)

    return result


##############################################################################
# TradeSeries

def get_tradeseries_by_field(field: str,
                             value,
                             collection: str,
                             limit=0,
                             show_progress: bool = False,
                             ):
    """Returns a list of TradeSeries() matching the field=value provided."""
    c = db[collection]
    total = c.count_documents({field: value})
    if limit > 0:
        total = min(total, limit)
    pbar = start_progbar(show_progress, total,
                         "tradeseries records fetched from "
                         f"{collection}")
    cursor = c.find({field: value}).limit(limit)
    result = []
    for i, doc in enumerate(cursor, start=1):
        result.append(doc)
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def store_tradeseries(series: dict,
                      collection: str,
                      ):
    """Store one TradeSeries object in mongo."""
    c = db[collection]
    result = c.find_one_and_replace({"ts_id": series["ts_id"]},
                                    series,
                                    new=True,
                                    upsert=True,
                                    )

    return result


def review_tradeseries(symbol: str,
                       collection: str,
                       bt_id: str,
                       ):
    """Provides aggregate summary data about tradeseries in mongo."""
    c = db[collection]
    match = {"symbol": symbol}
    group = {"_id": {"name": "$name", "bt_id": "$bt_id"},
             "earliest_start": {"$min": "$start_dt"},
             "latest_end": {"$max": "$end_dt"},
             }
    if bt_id is not None:
        match["bt_id"] = bt_id
    group["count"] = {"$sum": 1}
    try:
        tradeseries = list(c.aggregate([{"$match": match},
                                        {"$group": group}]))
    # IndexError is raised if collection does not exist yet
    except IndexError:
        return None
    else:
        return tradeseries


def delete_tradeseries_by_field(symbol: str,
                                field: str,
                                value,
                                collection: str,
                                ):
    """Delete all tradeseries records in mongo with 'field' matching 'value'.

    Typically used to delete by ts_id, or bt_id fields.
    """
    c = db[collection]
    result = c.delete_many({field: value})

    return result


def delete_tradeseries(ts_ids: list,
                       collection: str,
                       ):
    """Delete tradeseries from mongo using ts_id as the identifying field.
    """
    c = db[collection]
    result = []
    for ts_id in ts_ids:
        r = c.find_one_and_delete({"ts_id": ts_id})
        result.append(r)

    return result


##############################################################################
# Backtests


def get_backtests_by_field(field: str,
                           value,
                           collection: str,
                           limit=0,
                           show_progress: bool = False,
                           ):
    """Returns a list of Backtest() matching the field=value provided."""
    c = db[collection]
    total = c.count_documents({field: value})
    if limit > 0:
        total = min(total, limit)
    pbar = start_progbar(show_progress, total,
                         "backtest records fetched from "
                         f"{collection}")
    cursor = c.find({field: value}).limit(limit)
    result = []
    for i, doc in enumerate(cursor, start=1):
        result.append(doc)
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def store_backtest(backtest: dict,
                   collection: str,
                   ):
    """Store one Backtest object in mongo."""
    c = db[collection]
    result = c.find_one_and_replace({"bt_id": backtest["bt_id"]},
                                    backtest,
                                    new=True,
                                    upsert=True,
                                    )
    return result


def review_backtests(symbol: str,
                     collection: str,
                     ):
    """Provides aggregate summary data about backtests in mongo."""
    c = db[collection]
    match = {"symbol": symbol}
    group = {"_id": {"name": "$name", "bt_id": "$bt_id"},
             "earliest_start": {"$min": "$start_dt"},
             "latest_end": {"$max": "$end_dt"},
             }
    group["count"] = {"$sum": 1}
    try:
        backtests = list(c.aggregate([{"$match": match},
                                      {"$group": group}]))

        return backtests
    # IndexError is raised if collection does not exist yet
    except IndexError:
        return None


def delete_backtests_by_field(symbol: str,
                              field: str,
                              value,
                              collection: str,
                              ):
    """Delete all backtests records in mongo with 'field' matching 'value'.

    Typically used to delete by bt_id field.
    """
    c = db[collection]
    result = c.delete_many({field: value})

    return result


def delete_backtests(bt_ids: list,
                     collection: str,
                     ):
    """Delete backtests from mongo using bt_id as the identifying field.
    """
    c = db[collection]
    result = []
    for bt_id in bt_ids:
        r = c.find_one_and_delete({"bt_id": bt_id})
        result.append(r)

    return result


##############################################################################
# Candles

def store_candle(c_datetime,
                 c_timeframe: str,
                 c_open: float,
                 c_high: float,
                 c_low: float,
                 c_close: float,
                 c_volume: int,
                 c_symbol: str,
                 c_epoch: int,
                 c_date: str,
                 c_time: str,
                 name: str = "None",
                 ):
    """Stores a single candle object in mongo.

    Overwrites if it exists.
    """
    valid_timeframe(c_timeframe)
    collection = f"candles_{c_symbol}_{c_timeframe}"
    c_dt = dt_as_str(c_datetime)
    candle_doc = {"c_datetime": c_dt,
                  "c_timeframe": c_timeframe,
                  "c_open": c_open,
                  "c_high": c_high,
                  "c_low": c_low,
                  "c_close": c_close,
                  "c_volume": c_volume,
                  "c_symbol": c_symbol,
                  "c_epoch": c_epoch,
                  "c_date": c_date,
                  "c_time": c_time,
                  "name": name,
                  }
    c = db[collection]
    result = c.find_one_and_replace({"c_datetime": c_dt},
                                    candle_doc,
                                    new=True,
                                    upsert=True,
                                    )
    return result


def get_candles(start_epoch: int,
                end_epoch: int,
                timeframe: str,
                symbol: str,
                show_progress: bool = False,
                ):
    """Return candle docs within the given start and end epochs, inclusive.
    """
    c = db[f"candles_{symbol}_{timeframe}"]
    candle_filter = {
        "$and": [
            {"c_epoch": {"$gte": start_epoch}},
            {"c_epoch": {"$lte": end_epoch}},
        ]
    }
    total = c.count_documents(candle_filter)
    pbar = start_progbar(show_progress, total,
                         f"{symbol} {timeframe} candles fetched")
    cursor = c.find(candle_filter).sort("c_epoch", pymongo.ASCENDING)
    result = []
    for i, doc in enumerate(cursor, start=1):
        result.append(doc)
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def review_candles(timeframe: str,
                   symbol: str,
                   ):
    """Provides aggregate summary data about candles in central storage."""
    c = db[f"candles_{symbol}_{timeframe}"]
    try:
        epochs = list(c.aggregate([{"$group": {"_id": "null",
                      "earliest_epoch": {"$min": "$c_epoch"},
                      "latest_epoch": {"$max": "$c_epoch"}}}]))[0]
    # IndexError is raised if collection does not exist yet
    except IndexError:
        return None
    else:
        earliest_epoch = dt_from_epoch(epochs['earliest_epoch'])
        earliest_dt = dt_as_str(earliest_epoch)
        latest_epoch = dt_from_epoch(epochs['latest_epoch'])
        latest_dt = dt_as_str(latest_epoch)
        count = c.count_documents({})
        result = {"earliest_dt": earliest_dt, "latest_dt": latest_dt,
                  "latest_dt": latest_dt, "latest_dt": latest_dt,
                  "timeframe": timeframe, "count": count,
                  }

        return result


def delete_candles(timeframe: str,
                   symbol: str,
                   earliest_dt: str,
                   latest_dt: str,
                   ):
    """Delete candles from mongo for a specific datetime range."""
    c = db[f"candles_{symbol}_{timeframe}"]
    if earliest_dt is None:
        earliest_dt = "1970-01-01 00:00:00"
    if latest_dt is None:
        latest_dt = dt_as_str(dt.now())
    start_epoch = dt_to_epoch(earliest_dt)
    end_epoch = dt_to_epoch(latest_dt)
    result = c.delete_many({"$and": [{"c_epoch": {"$gte": start_epoch}},
                                     {"c_epoch": {"$lte": end_epoch}},
                                     ]})

    return result


def delete_candles_by_field(symbol: str,
                            timeframe: str,
                            field: str,
                            value,
                            ):
    """Delete all candles from mongo with 'field' matching 'value'.

    Typically used to delete by name or other identifying fields.

    Example to delete all candles with name=="DELETEME":
    delete_candles_by_field(symbol="ES", timeframe="1m",
                            field="name", value="DELETEME")
    """
    c = db[f"candles_{symbol}_{timeframe}"]
    result = c.delete_many({field: value})

    return result


##############################################################################
# Indicators
def list_indicators(meta_collection: str):
    """Lists all available indicators in mongo based on metadata."""
    c = db[meta_collection]
    result = c.find()

    return list(result)


def get_indicator_datapoints(ind_id: str,
                             dp_collection: str,
                             earliest_dt: str = None,
                             latest_dt: str = None,
                             ):
    """Return datapoints for ind_id within earliest_dt to latest_dt, sorted.

    Range is inclusive of both endpoints.
    """
    if earliest_dt is None:
        earliest_epoch = 0
    else:
        earliest_epoch = dt_to_epoch(earliest_dt)
    if latest_dt is None:
        latest_epoch = dt_to_epoch(dt.now())
    else:
        latest_epoch = dt_to_epoch(latest_dt)
    c = db[dp_collection]
    result = c.find({"$and": [{"ind_id": ind_id},
                              {"epoch": {"$gte": earliest_epoch}},
                              {"epoch": {"$lte": latest_epoch}},
                              ]})

    return list(result)


def store_indicator_datapoints(datapoints: list,
                               collection: str,
                               ):
    """Store one or more IndicatorDatapoint objects in mongo."""
    c = db[collection]
    result = []
    for d in datapoints:
        r = c.find_one_and_replace({"ind_id": d["ind_id"],
                                    "dt": d["dt"],
                                    "epoch": d["epoch"],
                                    },
                                   d,
                                   new=True,
                                   upsert=True,
                                   )
        result.append(r)

    return result


def review_indicators(meta_collection: str,
                      dp_collection: str,
                      ):
    """Return a more detailed overview of indicators in storage."""
    meta = list_indicators(meta_collection=meta_collection)
    result = {"meta_docs": meta, "datapoints": []}
    c = db[dp_collection]
    try:
        dps = c.aggregate([{"$group": {"_id": "$ind_id",
                                       "earliest_epoch": {"$min": "$epoch"},
                                       "latest_epoch": {"$max": "$epoch"},
                                       "count": {"$sum": 1},
                                       }}])
    # IndexError is raised if collection does not exist yet
    except IndexError:
        this = None
    else:
        for i in list(dps):
            ind_id = i['_id']
            earliest_epoch = dt_from_epoch(i['earliest_epoch'])
            earliest_dt = dt_as_str(earliest_epoch)
            latest_epoch = dt_from_epoch(i['latest_epoch'])
            latest_dt = dt_as_str(latest_epoch)
            count = i['count']
            this = {"ind_id": ind_id, "count": count,
                    "earliest_dt": earliest_dt, "latest_dt": latest_dt,
                    "latest_dt": latest_dt, "latest_dt": latest_dt,
                    }
            result["datapoints"].append(this)

    return result


def get_indicator(ind_id: str,
                  meta_collection: str,
                  autoload_datapoints: bool,
                  ):
    """Returns an indicator based on ind_id."""
    c = db[meta_collection]
    result = c.find({"ind_id": ind_id},
                    )

    return list(result)


def store_indicator(indicator: dict,
                    meta_collection: str,
                    ):
    """Store indicator meta in mongo."""
    c = db[meta_collection]
    # upsert=True updates existing or creates new if not found
    result = c.find_one_and_replace({"ind_id": indicator["ind_id"]},
                                    indicator,
                                    new=True,
                                    upsert=True,
                                    )

    return result


def delete_indicator(ind_id: str,
                     meta_collection: str,
                     dp_collection: str,
                     ):
    """Remove an indicator and all its datapoints from storage by ind_id.
    """
    c = db[meta_collection]
    result_meta = c.delete_many({"ind_id": ind_id})
    c = db[dp_collection]
    result_datapoints = c.delete_many({"ind_id": ind_id})

    return {"meta": result_meta,
            "datapoints": result_datapoints,
            }


##############################################################################
# Events
def store_event(start_dt,
                end_dt,
                symbol: str,
                category: str,
                tags: list,
                notes: str,
                start_epoch: int,
                end_epoch: int,
                name: str = "None",
                ):
    """Write a single Event() to mongo."""
    event_doc = {"start_dt": dt_as_str(start_dt),
                 "end_dt": dt_as_str(end_dt),
                 "category": category,
                 "tags": tags,
                 "notes": notes,
                 "start_epoch": start_epoch,
                 "end_epoch": end_epoch,
                 "name": name,
                 }
    collection = f"events_{symbol}"
    c = db[collection]
    result = c.find_one_and_replace({"start_dt": event_doc["start_dt"],
                                     "category": event_doc["category"]},
                                    event_doc,
                                    new=True,
                                    upsert=True,
                                    )

    return result


def delete_events_by_field(symbol: str,
                           field: str,
                           value,
                           ):
    """Delete all events from mongo for a symbol with 'field' matching 'value'.

    Typically used to delete by name or category fields.

    Example to delete all events with name=="DELETEME":
    delete_events_by_field(symbol="ES", field="name", value="DELETEME")
    """
    c = db[f"events_{symbol}"]
    result = c.delete_many({field: value})

    return result


def get_events(symbol: str,
               start_epoch: int,
               end_epoch: int,
               categories: list = None,
               tags: list = None,
               ):
    """Return events starting within the given start and end epochs, inclusive.

    Note: events that end after end_epoch are included so long as they
    start before or on it.
    """
    c = db[f"events_{symbol}"]
    events = list(c.find({"$and": [{"start_epoch": {"$gte": start_epoch}},
                         {"start_epoch": {"$lte": end_epoch}}]}))
    if categories is not None:
        filtered_by_cat = []
        for e in events:
            if e["category"] in categories:
                filtered_by_cat.append(e)
    else:
        filtered_by_cat = events

    if tags is not None:
        filtered_by_tag = []
        for e in filtered_by_cat:
            for t in tags:
                if t in e["tags"]:
                    filtered_by_tag.append(e)
                    break
    else:
        filtered_by_tag = filtered_by_cat

    result = filtered_by_tag

    return result


##############################################################################
# Backfill utilities
def deleteme_backfill_names_to_candles_and_events():
    """Backfill the 'name' field on candle and event records.

    Sets 'name' to the string "None" for every candle document that
    is missing the field, and 'name' to the string "None" for every
    event document missing the field.  This handles data that was stored
    before the name field was added to Candle and Event.

    After updating, a verification query confirms that every record in
    each affected collection now has the field set to a string value.
    Raises RuntimeError if verification fails for any collection.

    Returns a dict summarising the number of records updated per
    collection.
    """
    all_collections = list_collections()
    candle_collections = [
        c for c in all_collections if c.startswith("candles_")
    ]
    event_collections = [
        c for c in all_collections if c.startswith("events_")
    ]

    results = {"candles": {}, "events": {}}

    for coll in candle_collections:
        c = db[coll]
        update_result = c.update_many(
            {"name": {"$exists": False}},
            {"$set": {"name": "None"}},
        )
        results["candles"][coll] = {
            "updated": update_result.modified_count,
        }
        missing = c.count_documents(
            {"$or": [
                {"name": {"$exists": False}},
                {"name": {"$not": {"$type": "string"}}},
            ]},
        )
        if missing > 0:
            raise RuntimeError(
                f"Backfill verification failed: {missing} candle "
                f"record(s) in '{coll}' still lack a string name "
                "field after update."
            )
        results["candles"][coll]["verified"] = True

    for coll in event_collections:
        c = db[coll]
        update_result = c.update_many(
            {"name": {"$exists": False}},
            {"$set": {"name": "None"}},
        )
        results["events"][coll] = {
            "updated": update_result.modified_count,
        }
        missing = c.count_documents(
            {"$or": [
                {"name": {"$exists": False}},
                {"name": {"$not": {"$type": "string"}}},
            ]},
        )
        if missing > 0:
            raise RuntimeError(
                f"Backfill verification failed: {missing} event "
                f"record(s) in '{coll}' still lack a string name "
                "field after update."
            )
        results["events"][coll]["verified"] = True

    return results
