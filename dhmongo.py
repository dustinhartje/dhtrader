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
from dotenv import load_dotenv, find_dotenv
from operator import itemgetter
import dhutil as dhu
import dhcharts as dhc

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
    db = mc.MONGO_DB
except Exception:
    print("\n\nWell that failed.  So sad!")
    print("\nThere's a good chance it's because my current IP address is not "
          "allowed on the Atlas side.")
    print("Log into the Atlas web interface > Clusters > Cluster0 > Connect "
          "button.  It should show a dialogue that allows me to add it.")
    print("If this keeps happening maybe add a subnet range too?")
    sys.exit()


def list_collections():
    """List all current collections in mongo"""
    return db.list_collection_names()


def drop_collection(collection: str):
    """Irretrievable drops a collection from the store.  Use carefully!"""
    c = db[collection]
    result = c.drop()

    return result


def store_trades(trades,
                 collection: str = "trades"):
    c = db[collection]
    result = c.insert_many(trades)

    return result


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


def drop_candles(timeframe: str,
                 symbol: str,
                 earliest_dt: str,
                 latest_dt: str,
                 ):
    "Delete candles from mongo for a specific datetime range."""
    c = db[f"candles_{symbol}_{timeframe}"]
    start_epoch = dhu.dt_to_epoch(earliest_dt)
    end_epoch = dhu.dt_to_epoch(latest_dt)
    result = c.delete_many({"$and": [{"c_epoch": {"$gte": start_epoch}},
                                     {"c_epoch": {"$lte": end_epoch}},
                                     ]})

    return result


def list_indicators(meta_collection: str):
    """Lists all available indicators in mongo based on metadata"""
    c = db[meta_collection]
    result = c.find()

    return list(result)


def get_indicator_datapoints(ind_id: str,
                             dp_collection: str,
                             earliest_dt: str = "",
                             latest_dt: str = "",
                             ):
    """retrieves all datapoints for the given ind_id that fall within
    the range of earliest_dt and latest_dt (inclusive of both), returning
    them as a chronologially sorted list"""
    # TODO LOWPRI actually use *_dt args when I find a need, seems likely
    c = db[dp_collection]
    result = c.find({"ind_id": ind_id})

    return list(result)


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


def store_indicator(ind_id: str,
                    name: str,
                    description: str,
                    timeframe: str,
                    trading_hours: str,
                    symbol: str,
                    calc_version: str,
                    calc_details: str,
                    parameters: dict,
                    datapoints: list,
                    meta_collection: str,
                    dp_collection: str,
                    ):
    # First update or create the meta record for this unique ind_id
    meta_doc = {"ind_id": ind_id,
                "name": name,
                "description": description,
                "timeframe": timeframe,
                "trading_hours": trading_hours,
                "symbol": symbol,
                "calc_version": calc_version,
                "calc_details": calc_details,
                "parameters": parameters,
                }
    c = db[meta_collection]
    # If a prior meta doc for this id exists, replace it entirely else add it
    # upsert=True inserts if not found
    result_meta = c.find_one_and_replace({"ind_id": ind_id},
                                         meta_doc,
                                         new=True,
                                         upsert=True)

    # Now insert the datapoints per last entry in
    # https://stackoverflow.com/questions/18371351/python-pymongo-
    # insert-and-update-documents
    c = db[dp_collection]
    result_datapoints = []
    for d in datapoints:
        # TODO review and test with find_one_and_replace, not sure why
        #      I did update_many here?  Is it possible to do update_many
        #      and still maintain upsert and list of unique fields to match
        #      when providing an iterable instead of looping through each?
        #      Low priority as it's working this way, but it's probably slow
        r = c.update_many({'ind_id': d['ind_id'],
                           'dt': d['dt'],
                           'epoch': d['epoch']},
                          {"$set": {"value": d['value']}},
                          upsert=True)
    result_datapoints.append(r)
    result = {"meta": result_meta, "datapoints": result_datapoints}

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
    c = db[f"events_{symbol}"]
    result = c.find({"$and": [{"start_epoch": {"$gte": start_epoch}},
                    {"start_epoch": {"$lte": end_epoch}}]})

    return list(result)


def test_basics():
    """runs a few basics tests, mostly used during initial development
       to confirm functionality as desired.  Also dumps a basic summary
       of the database for quick checks at the end"""
    # TODO consider converting these into unit tests some day
    # https://docs.python.org/3/library/unittest.html

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
    result = db.command("dbstats")
    print(result)
    data_size_GB = round(result["dataSize"]/1024/1024/1024, 2)
    storage_size_GB = round(result["storageSize"]/1024/1024/1024, 2)
    print(f"data_size_GB = {data_size_GB}")
    print(f"storage_size_GB = {storage_size_GB}")

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
