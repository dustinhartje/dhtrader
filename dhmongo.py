# Mongo specific functions for storing and retrieving data
#
# These functions will be wrapped by dhstore.py functions to allow for
# changing database layer in the future without major overhaul
#
# Running this script ad hoc will perform a basic connect, write, read test
#
# REF: https://github.com/mongodb-university/atlas_starter_python/blob
#      /master/atlas-starter.py

import os, sys
import pymongo
from dotenv import load_dotenv, find_dotenv

# Establish mongo connection parameters and client
MONGO_ENV_FILE = 'mongo.env'
load_dotenv(find_dotenv(MONGO_ENV_FILE))

MONGO_CONN = os.getenv("MONGO_CONN")
MONGO_DB = os.getenv("MONGO_DB")
if MONGO_CONN is None:
    raise Exception(f"Unable to retrieve MONGO_CONN from {MONGO_ENV_FILE}")
if MONGO_DB is None:
    raise Exception("Unable to retrieve MONGO_DB from {MONGO_ENV_FILE}")

try:
    mc = pymongo.MongoClient(MONGO_CONN)
    db = mc.MONGO_DB
except:
    print("\n\nWell that failed.  So sad!")
    print("\nThere's a good chance it's because my current IP address is not "
          "allowed on the Atlas side.")
    print("Log into the Atlas web interface > Clusters > Cluster0 > Connect "
          "button.  It should show a dialogue that allows me to add it.")
    print("If this keeps happening maybe add a subnet range too?")
    sys.exit()


def store_trades(trades,
                 collection: str = "trades"):
    c = db[collection]
    result = c.insert_many(trades)

    return result


def list_indicators(meta_collection: str):
    c = db[meta_collection]
    result = c.find()

    return result

def get_indicator_datapoints(indicator_id: str,
                             dp_collection: str,
                             earliest_dt: str = "",
                             latest_dt: str = "",
                             ):
    """retrieves all datapoints for the given indicator_id that fall within
    the range of earliest_dt and latest_dt (inclusive of both), returning
    them as a chronologially sorted list"""
    #TODO actually use dt params, for initial testing it's just grabbing all
    c = db[dp_collection]
    result = c.find({"indicator_id": indicator_id})

    return result
                             

def store_indicators(indicator_id: str,
                     short_name: str,
                     long_name: str,
                     description: str,
                     timeframe: str,
                     trading_hourse: str,
                     symbol: str,
                     calc_version: str,
                     calc_details: str,
                     datapoints: list,
                     meta_collection: str,
                     dp_collection: str,
                     ):
    # First update or create the meta record for this unique indicator_id
    meta_doc = {"indicator_id": indicator_id,
                "short_name": short_name,
                "long_name": long_name,
                "description": description,
                "timeframe": timeframe,
                "trading_hourse": trading_hours,
                "symbol": symbol,
                "calc_version": calc_version,
                "calc_details": calc_details,
               }
    c = db[meta_collection]
    # If a prior meta doc for this id exists, replace it entirely else add it
    # upsert=True inserts if not found
    result_meta = c.find_one_and_replace({"indicator_id": indicator_id},
                                         meta_doc,
                                         new=True,
                                         upsert=True)

    # Now insert the datapoints per last entry in
    # https://stackoverflow.com/questions/18371351/python-pymongo-
    # insert-and-update-documents
    c = db[dp_collection]
    result_dp = []
    for d in datapoints:
        r = c.update_many({'indicator_id': d['indicator_id'],
                           'dt': d['dt']},
                          {"$set": {"value": d['value']}},
                          upsert=True)
    result_dp.append(r)
    result = {"meta": result_meta, "datapoints": result_dp}

    return result


def test_basics():
    """Used when script is run adhoc to perform basic connect/r/w test"""

    print("Listing collections")
    result = db.list_collection_names()
    print(result)

    print("Creating a collection and inserting a test doc")
    c = db["test_stuff"]
    result = c.insert_one({"name": "test doc", "usefulness": "Not at all"})
    print(result)

    print("Listing collections")
    result = db.list_collection_names()
    print(result)

    print("Listing docs in the collection")
    result = c.find()
    print(result)
    for doc in result:
        print(doc)

    print("Dropping collection")
    result = c.drop()
    print(result)

    print("Listing collections")
    result = db.list_collection_names()
    print(result)

    print("...and we're done!")


if __name__ == '__main__':
    test_basics()
