# Mongo specific functions for storing and retrieving data
#
# These functions will be wrapped by dhstore.py functions to allow for
# changing database layer in the future without major overhaul
#
# Running this script ad hoc will perform a basic connect, write, read test
#
# REF: https://github.com/mongodb-university/atlas_starter_python/blob
#      /master/atlas-starter.py

import os
import sys
import pymongo
import logging
from dotenv import load_dotenv, find_dotenv
from operator import itemgetter
from datetime import datetime as dt
from datetime import timedelta
import dhutil as dhu
import dhcharts as dhc

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
# Non-class specific functions
def review_database():
    """Quick function to gather useful information about the state of mongo"""
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
    """List all current collections in mongo"""
    return db.list_collection_names()


def clear_collection(collection: str):
    """Deletes all records from a collection but keeps the collection itself
    and any indexes/settings intact."""
    c = db[collection]
    if not dhu.prompt_yn(f"Clear all records from collection "
                         f"'{collection}'? This cannot be undone.  You should "
                         "probably do a backup first..."):
        return None

    return c.delete_many({})


def drop_collection(collection: str):
    """Irretrievable drops a collection from the store.  Use carefully!"""
    c = db[collection]
    if not dhu.prompt_yn(f"Drop collection '{collection}'? This cannot be"
                         "undone and will delete all data, indexes, and other "
                         "collection settings. You probably actually want to "
                         "use clear_collection() instead, right?  Are you "
                         "SURE you want to drop this?"):
        return None

    return c.drop()


def get_all_records_by_collection(collection: str,
                                  limit=0):
    """Return <limit> (default 0 == all) records from a collection, typically
    wrapped by more specific functions in dhstore such as get_all_trades()."""
    c = db[collection]
    result = c.find().limit(limit)

    return list(result)


def run_query(query, collection: str):
    """Run a standard mongo query and return the result"""
    c = db[collection]
    return list(c.find(query))


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
    """Return aggregation with list of unique values in values_field split
    by regex match or nomatch in regex_field.  An example usage: review tags
    list based on whether or not a Trade closed in the autoclose timeframe
    i.e. split by those that close at 15:55:00 for rth hours and those that
    do not."""
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
    """Using 'search', which should be a dict representing a mongo filter,
    update all matching records update_field with the new update_value

    Example:
    To replace bt_id 'foo' with 'bar' on all backtests currently matching 'foo'
    update_record_value(search={"bt_id": "foo"},
                        update_field="bt_id",
                        update_value="bar",
                        collection="backtests")
    """
    c = db[collection]
    result = c.update_many(search,
                           {"$set": {update_field: update_value}}
                           )

    return result


def delete_one_document(query: dict,
                        collection: str,
                        ):
    """Delete a single document from storage using the provided query.  This
    is primarily called by more specific functions in dhstore.py for individual
    object types using relevant parameters to build the query there."""
    c = db[collection]
    result = c.delete_one(query)

    return result

##############################################################################
# Trades


def get_trades_by_field(field: str,
                        value,
                        collection: str,
                        limit=0,
                        ):
    """Returns <limit> (0 = all) Trade documents matching (field == value)."""
    c = db[collection]
    result = c.find({field: value}).limit(limit)

    return list(result)


def store_trades(trades: list,
                 collection: str,
                 ):
    """Store one or more trades in mongo"""
    c = db[collection]
    result = []
    for t in trades:
        r = c.find_one_and_replace({"open_dt": t["open_dt"],
                                    "direction": t["direction"],
                                    "name": t["name"],
                                    "version": t["version"],
                                    "symbol": t["symbol"],
                                    "ts_id": t["ts_id"],
                                    "bt_id": t["bt_id"],
                                    },
                                   t,
                                   new=True,
                                   upsert=True,
                                   )
        result.append(r)

    return result


def review_trades(symbol: str,
                  collection: str,
                  bt_id: str = None,
                  ts_id: str = None,
                  ):
    """Provides aggregate summary data about trades in mongo"""
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


def delete_trades(symbol: str,
                  field: str,
                  value,
                  collection: str,
                  ):
    """Delete all trade records with 'field' matching 'value'.  Typically
    used to delete by name, ts_id, or bt_id fields.

    Example to delete all trade records with name=="DELETEME":
        delete_trades(symbol="ES", field="name", value="DELETEME")
    """
    c = db[collection]
    result = c.delete_many({field: value})

    return result


##############################################################################
# TradeSeries

def get_tradeseries_by_field(field: str,
                             value,
                             collection: str,
                             limit=0,
                             ):
    """Returns a list of TradeSeries() matching the field=value provided."""
    c = db[collection]
    result = c.find({field: value}).limit(limit)

    return list(result)


def store_tradeseries(series: dict,
                      collection: str,
                      ):
    """Store one TradeSeries object in mongo"""
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
    """Provides aggregate summary data about tradeseries in mongo"""
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


def delete_tradeseries(symbol: str,
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


##############################################################################
# Backtests


def get_backtests_by_field(field: str,
                           value,
                           collection: str,
                           limit=0,
                           ):
    """Returns a list of Backtest() matching the field=value provided."""
    c = db[collection]
    result = c.find({field: value}).limit(limit)

    return list(result)


def store_backtest(backtest: dict,
                   collection: str,
                   ):
    """Store one Backtest object in mongo"""
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
    """Provides aggregate summary data about backtests in mongo"""
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


def delete_backtests(symbol: str,
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
                 ):
    """Stores a single candle object in mongo.  Overwrites if it exists."""
    dhu.valid_timeframe(c_timeframe)
    collection = f"candles_{c_symbol}_{c_timeframe}"
    c_dt = dhu.dt_as_str(c_datetime)
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
                ):
    """Returns a list of candle docs within the start and end epochs given
    inclusive of both epochs"""
    c = db[f"candles_{symbol}_{timeframe}"]
    result = c.find({"$and": [{"c_epoch": {"$gte": start_epoch}},
                    {"c_epoch": {"$lte": end_epoch}}]})

    return sorted(list(result), key=itemgetter('c_epoch'))


def review_candles(timeframe: str,
                   symbol: str,
                   ):
    """Provides aggregate summary data about candles in central storage"""
    c = db[f"candles_{symbol}_{timeframe}"]
    try:
        epochs = list(c.aggregate([{"$group": {"_id": "null",
                      "earliest_epoch": {"$min": "$c_epoch"},
                      "latest_epoch": {"$max": "$c_epoch"}}}]))[0]
    # IndexError is raised if collection does not exist yet
    except IndexError:
        return None
    else:
        earliest_epoch = dhu.dt_from_epoch(epochs['earliest_epoch'])
        earliest_dt = dhu.dt_as_str(earliest_epoch)
        latest_epoch = dhu.dt_from_epoch(epochs['latest_epoch'])
        latest_dt = dhu.dt_as_str(latest_epoch)
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
    "Delete candles from mongo for a specific datetime range."""
    c = db[f"candles_{symbol}_{timeframe}"]
    if earliest_dt is None:
        earliest_dt = "1970-01-01 00:00:00"
    if latest_dt is None:
        latest_dt = dhu.dt_as_str(dt.now())
    start_epoch = dhu.dt_to_epoch(earliest_dt)
    end_epoch = dhu.dt_to_epoch(latest_dt)
    result = c.delete_many({"$and": [{"c_epoch": {"$gte": start_epoch}},
                                     {"c_epoch": {"$lte": end_epoch}},
                                     ]})

    return result


##############################################################################
# Indicators
def list_indicators(meta_collection: str):
    """Lists all available indicators in mongo based on metadata"""
    c = db[meta_collection]
    result = c.find()

    return list(result)


def get_indicator_datapoints(ind_id: str,
                             dp_collection: str,
                             earliest_dt: str = None,
                             latest_dt: str = None,
                             ):
    """Retrieves all datapoints for the given ind_id that fall within
    the range of earliest_dt and latest_dt (inclusive of both), returning
    them as a chronologially sorted list"""
    if earliest_dt is None:
        earliest_epoch = 0
    else:
        earliest_epoch = dhu.dt_to_epoch(earliest_dt)
    if latest_dt is None:
        latest_epoch = dhu.dt_to_epoch(dt.now())
    else:
        latest_epoch = dhu.dt_to_epoch(latest_dt)
    c = db[dp_collection]
    result = c.find({"$and": [{"ind_id": ind_id},
                              {"epoch": {"$gte": earliest_epoch}},
                              {"epoch": {"$lte": latest_epoch}},
                              ]})

    return list(result)


def store_indicator_datapoints(datapoints: list,
                               collection: str,
                               ):
    """Store one or more IndicatorDatapoint objects in mongo"""
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
    """Return a more detailed overview of indicators in storage"""
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
            earliest_epoch = dhu.dt_from_epoch(i['earliest_epoch'])
            earliest_dt = dhu.dt_as_str(earliest_epoch)
            latest_epoch = dhu.dt_from_epoch(i['latest_epoch'])
            latest_dt = dhu.dt_as_str(latest_epoch)
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
    """Returns an indicator based on ind_id"""
    c = db[meta_collection]
    result = c.find({"ind_id": ind_id},
                    )

    return list(result)


def store_indicator(indicator: dict,
                    meta_collection: str,
                    ):
    """Store indicator meta in mongo"""
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
    """Remove a single indicator and all of it's datapoints from central
    storage based on it's ind_id attribute"""
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
                ):
    """Write a single dhcharts.Event() to mongo"""
    event_doc = {"start_dt": dhu.dt_as_str(start_dt),
                 "end_dt": dhu.dt_as_str(end_dt),
                 "category": category,
                 "tags": tags,
                 "notes": notes,
                 "start_epoch": start_epoch,
                 "end_epoch": end_epoch,
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


def get_events(symbol: str,
               start_epoch: int,
               end_epoch: int,
               categories: list = None,
               tags: list = None,
               ):
    """Returns a list of events starting within the start and end epochs given
    inclusive of both epochs.  Note this will return events that end after
    end_epoch so long as they start before or on it."""
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
# Tests
def test_basics():
    """runs a few basics tests, mostly used during initial development
       to confirm functionality as desired.  Also dumps a basic summary
       of the database for quick checks at the end"""
    print("\nListing current collections before we make changes")
    print(list_collections())

    print("\nCreating a collection and inserting a test doc")
    c = db["DELETEME_TEST_STUFF"]
    result = c.insert_one({"name": "test doc", "usefulness": "Not at all"})
    print(result)

    print("\nListing collections again to confirm the new one")
    print(list_collections())

    print("\nListing docs in the collection")
    result = c.find()
    print(result)
    for doc in result:
        print(doc)

    print("\nDropping collection")
    result = c.drop()
    print(result)

    print("\nListing collections again")
    print(list_collections())

    print("\nStoring 2 candles")
    result = store_candle(c_datetime="2024-01-01 12:30:00",
                          c_timeframe="1m",
                          c_open=5500.25,
                          c_high=5505,
                          c_low=5497.5,
                          c_close=5501.5,
                          c_volume=500,
                          c_symbol="DELETEME",
                          c_epoch=dhu.dt_to_epoch("2024-01-01 12:30:00"),
                          c_date="2024-01-01",
                          c_time="12:30:00",
                          )
    print(f"Candle 1: {result}")
    result = store_candle(c_datetime="2024-01-05 14:35:00",
                          c_timeframe="1m",
                          c_open=5501.5,
                          c_high=5510,
                          c_low=5500.5,
                          c_close=5510,
                          c_volume=400,
                          c_symbol="DELETEME",
                          c_epoch=dhu.dt_to_epoch("2024-01-05 14:35:00"),
                          c_date="2024-01-05",
                          c_time="14:35:00"
                          )
    print(f"Candle 2: {result}")
    print("\nListing all candles stored in test collection")
    c = db["candles_DELETEME_1m"]
    result = c.find()
    for candle in result:
        print(candle)
    print("\nNow try to find only the first candle using epoch filters")
    result = get_candles(start_epoch=1704130200,
                         end_epoch=1704130201,
                         timeframe="1m",
                         symbol="DELETEME",
                         )
    print(result)
    print("\nand then find only the second candle using epoch filters")
    result = get_candles(start_epoch=1704130201,
                         end_epoch=1704483300,
                         timeframe="1m",
                         symbol="DELETEME",
                         )
    print(result)

    print("\nNow try to store a Candle using it's build in method")
    test_candle = dhc.Candle(c_datetime="2024-02-10 09:20:00",
                             c_timeframe="1m",
                             c_open=5501.5,
                             c_high=5510,
                             c_low=5500.5,
                             c_close=5510,
                             c_volume=400,
                             c_symbol="DELETEME",
                             )
    test_candle.store()
    print("\nsearch to see if it's there...")
    result = c.find()
    for r in result:
        print(r)
    result = get_candles(start_epoch=1704130201,
                         end_epoch=1704483300,
                         timeframe="1m",
                         symbol="DELETEME",
                         )

    # Test candle summary review functions
    print("\nListing collections")
    result = db.list_collection_names()

    print("\nNow lets show a summary of the stored candles")
    print(review_candles(timeframe='1m', symbol="DELETEME"))

    print("\nNow I'll just cleanup after myself in mongo...")
    c.drop()

    # Test event storage and retrieval
    c = db["events_DELETEME"]
    print("\nTesting event storage by creating 3 test events")
    result = store_event(start_dt="2024-11-01 13:00:00",
                         end_dt="2024-11-01 14:00:00",
                         symbol="DELETEME",
                         category="Closed",
                         tags=[],
                         notes="Not really though, lul!",
                         start_epoch=1730480400,
                         end_epoch=1730484000,
                         )
    print(result)
    result = store_event(start_dt="2024-11-05 16:00:00",
                         end_dt="2024-11-06 9:30:00",
                         symbol="DELETEME",
                         category="LowVolume",
                         tags=[],
                         notes="Nobody trades at night",
                         start_epoch=1730840400,
                         end_epoch=1730903400,
                         )
    print(result)
    result = store_event(start_dt="2024-11-20 14:00:00",
                         end_dt="2024-11-20 15:00:00",
                         symbol="DELETEME",
                         category="Data",
                         tags=['FOMC'],
                         notes="FOMC meeting volatility expected",
                         start_epoch=1732129200,
                         end_epoch=1732132800,
                         )
    print(result)

    print("\nListing docs in the events_DELETEME collection")
    result = c.find()
    print(result)
    for doc in result:
        print(doc)
    print("\nTesting get_events() on this collection, should get the 11/1 "
          "and 11/5 events but not 11/20")
    result = get_events(start_epoch=1730840400,
                        end_epoch=1732133000,
                        symbol="DELETEME",
                        )
    print(result)

    print("\nNow I'll just cleanup after myself in mongo...")
    c.drop()

    print("\nListing collections")
    result = db.list_collection_names()
    print(result)

    print("\n===============================================================")
    print("DATABASE AND COLLECTIONS SUMMARY DATA")
    print("\nLet's get some overall db stats")
    print(review_database()["overview"])

    print("\nListing collections")
    result = db.list_collection_names()
    print(result)

    print("\nChecking raw candles summaries.  Note this should return None "
          "if the collection is not yet populated")
    c = review_candles(timeframe='1m', symbol='ES')
    print("1m")
    print(c)
    c = review_candles(timeframe='5m', symbol='ES')
    print("5m")
    print(c)
    c = review_candles(timeframe='15m', symbol='ES')
    print("15m")
    print(c)
    c = review_candles(timeframe='r1h', symbol='ES')
    print("r1h")
    print(c)
    c = review_candles(timeframe='e1h', symbol='ES')
    print("e1h")
    print(c)
    c = review_candles(timeframe='1d', symbol='ES')
    print("1d")
    print(c)


if __name__ == '__main__':
    test_basics()
