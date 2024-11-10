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
