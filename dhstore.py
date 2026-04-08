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
import re
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime as dt
from datetime import date, timedelta
from copy import deepcopy
import logging
from pathlib import Path
from .dhtypes import (
    Candle, Event, IndicatorDataPoint, Symbol, IndicatorSMA, IndicatorEMA,
    Trade, TradeSeries, TradePlan)
from .dhcommon import (
    dt_as_str, dt_as_dt, dt_from_epoch, dt_to_epoch, valid_timeframe,
    this_candle_start, summarize_candles, log_say, sort_dict,
    rangify_candle_times, expected_candle_datetimes, MARKET_ERAS,
    ProgBar, OperationTimer, DEFAULT_OBJ_NAME)
from . import dhmongo as dhm

# All collection names owned by dhtrader classes.  Custom document
# functions refuse to operate on any collection listed here.
# Add new managed collections here when new classes are introduced.
COLLECTIONS: dict = {
    "trades": "trades",
    "tradeseries": "tradeseries",
    "backtests": "backtests",
    "tradeplans": "tradeplans",
    "ind_meta": "indicators_meta",
    "ind_dps": "indicators_datapoints",
    "images": "images",
}

# Regex patterns for dynamically named managed collections.
# Add new patterns here when new dynamic collections are introduced.
# gridfs pattern is scoped to the images bucket prefix so other
# collections with 'files' or 'chunks' in the name are not blocked.
COLL_PATTERNS: dict = {
    "candles": re.compile(r"candles_.+_.+"),
    "events": re.compile(r"events_.+"),
    "gridfs": re.compile(
        rf"^{re.escape(COLLECTIONS['images'])}\.("
        r"files|chunks)$"
    ),
}

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
# Custom document functions (non-class-specific collections)

def _guard_managed_collection(collection: str, func_name: str):
    """Raise ValueError if collection is managed by a dhtrader class.

    Guards against accidental writes or deletes to class-owned collections
    (COLLECTIONS dict) or dynamically named managed collections
    (COLL_PATTERNS dict).
    """
    if collection in COLLECTIONS.values():
        log.critical(
            f"{func_name}: collection {collection!r} is a managed "
            "collection and cannot be used with custom document functions"
        )
        raise ValueError(
            f"collection {collection!r} is managed by a dhtrader class "
            "and cannot be used with custom document functions"
        )
    for pattern_name, pattern in COLL_PATTERNS.items():
        if pattern.search(collection):
            log.critical(
                f"{func_name}: collection {collection!r} matches managed "
                f"pattern {pattern_name!r} and cannot be used with "
                "custom document functions"
            )
            raise ValueError(
                f"collection {collection!r} matches managed pattern "
                f"{pattern_name!r} and cannot be used with custom "
                "document functions"
            )


def store_custom_documents(collection: str,
                           documents: list,
                           ):
    """Store a list of plain dicts in a non-managed collection.

    Args:
        collection: Target collection name.  Must not be a
            managed (class-owned) collection.
        documents: Non-empty list of dicts to store.

    Raises:
        ValueError: If collection is managed, documents is
            empty, any element is not a dict, or any element
            cannot be serialized to JSON.

    Returns:
        list: Mongo result dicts, one per document stored.
    """
    log.info(
        f"store_custom_documents: collection={collection!r}, "
        f"count={len(documents)}"
    )
    # Guard against writes to managed (class-owned) collections.
    _guard_managed_collection(collection, "store_custom_documents")

    # Validate inputs before touching storage.
    if not documents:
        log.critical(
            "store_custom_documents: documents list is empty"
        )
        raise ValueError(
            "store_custom_documents: documents must be a non-empty list"
        )
    for i, doc in enumerate(documents):
        if not isinstance(doc, dict):
            # Attempt to coerce the object to a dict before failing.
            # Try vars() for objects with __dict__, then dict() for
            # mappings and iterables of (key, value) pairs.
            converted = None
            try:
                converted = dict(vars(doc))
            except TypeError:
                pass
            if converted is None:
                try:
                    converted = dict(doc)
                except (TypeError, ValueError):
                    pass
            if converted is None:
                log.critical(
                    f"store_custom_documents: element {i} cannot be "
                    f"converted to dict: {type(doc)}"
                )
                raise ValueError(
                    f"store_custom_documents: element {i} cannot be "
                    f"converted to dict, got {type(doc)}"
                )
            log.debug(
                f"store_custom_documents: element {i} coerced from "
                f"{type(doc)} to dict"
            )
            documents[i] = converted
            doc = documents[i]
        try:
            json.dumps(doc)
        except (TypeError, ValueError) as exc:
            log.critical(
                f"store_custom_documents: element {i} is not "
                f"JSON-serializable: {exc}"
            )
            raise ValueError(
                f"store_custom_documents: element {i} is not "
                f"JSON-serializable: {exc}"
            ) from exc
        # Require a non-blank 'name' field on every document so
        # doc_id can be prefixed meaningfully and records are
        # always identifiable by name in storage.
        name_val = doc.get("name")
        if not name_val or not str(name_val).strip():
            log.critical(
                f"store_custom_documents: element {i} has no "
                "valid 'name' field"
            )
            raise ValueError(
                f"store_custom_documents: element {i} must have "
                "a non-blank 'name' field"
            )

    # Assign a unique doc_id to each document that does not already have
    # one.  doc_id is generated here (not in dhmongo) so the caller can
    # reference it before storage completes.  The doc_id is prefixed
    # with the document's name attribute (if present) to aid debugging.
    for doc in documents:
        if not doc.get("doc_id"):
            name_prefix = str(doc["name"]).strip()
            while True:
                candidate = f"{name_prefix}_{str(uuid.uuid4())}"
                existing = dhm.get_custom_documents_by_field(
                    collection=collection,
                    field="doc_id",
                    value=candidate,
                    limit=1,
                )
                if not existing:
                    break
            doc["doc_id"] = candidate

    # Store each document via upsert and collect results.
    time_store = time.perf_counter()
    results = []
    for doc in documents:
        result = dhm.store_custom_document(
            document=doc,
            collection=collection,
        )
        results.append(result)
    log.debug(
        f"store_custom_documents: storage loop elapsed "
        f"{time.perf_counter() - time_store:.6f}s"
    )

    log.info(
        f"store_custom_documents: stored {len(results)} documents "
        f"to {collection!r}"
    )
    return results


def delete_custom_documents_by_field(collection: str,
                                     field: str,
                                     value,
                                     ):
    """Delete documents matching field==value from a non-managed collection.

    Args:
        collection: Target collection name.
        field: Document field to match on.
        value: Value to match.

    Raises:
        ValueError: If collection is managed.

    Returns:
        pymongo DeleteResult.
    """
    log.info(
        f"delete_custom_documents_by_field: collection={collection!r}, "
        f"field={field!r}, value={value!r}"
    )
    # Guard against deletes from managed (class-owned) collections.
    _guard_managed_collection(
        collection, "delete_custom_documents_by_field"
    )

    time_delete = time.perf_counter()
    result = dhm.delete_custom_documents_by_field(
        collection=collection,
        field=field,
        value=value,
    )
    log.debug(
        f"delete_custom_documents_by_field: delete elapsed "
        f"{time.perf_counter() - time_delete:.6f}s"
    )

    log.info(
        f"delete_custom_documents_by_field: deleted "
        f"{result.deleted_count} documents from {collection!r}"
    )
    return result


def list_custom_documents(collection: str,
                          field: str = None,
                          value=None,
                          limit: int = 0,
                          ):
    """Return a list of dicts from a non-managed collection.

    If field and value are provided, filters by that field.
    If neither is provided, returns all documents.  limit=0
    means no limit.

    Raises:
        ValueError: If collection is managed.

    Returns:
        list of dicts.
    """
    log.info(
        f"list_custom_documents: collection={collection!r}, "
        f"field={field!r}, value={value!r}, limit={limit}"
    )
    _guard_managed_collection(collection, "list_custom_documents")

    if field is not None:
        results = dhm.get_custom_documents_by_field(
            collection=collection,
            field=field,
            value=value,
            limit=limit,
        )
    else:
        results = dhm.get_all_custom_documents(
            collection=collection,
            limit=limit,
        )

    log.info(
        f"list_custom_documents: returning {len(results)} documents "
        f"from {collection!r}"
    )
    return results


def get_custom_documents_by_field(collection: str,
                                  field: str,
                                  value,
                                  limit: int = 0,
                                  ):
    """Return documents matching field==value from a non-managed collection.

    Args:
        collection: Source collection name.
        field: Document field to filter on.
        value: Value to match.
        limit: Maximum number of results (0 = no limit).

    Raises:
        ValueError: If collection is managed.

    Returns:
        list of dicts.
    """
    log.info(
        f"get_custom_documents_by_field: collection={collection!r}, "
        f"field={field!r}, value={value!r}, limit={limit}"
    )
    _guard_managed_collection(
        collection, "get_custom_documents_by_field"
    )
    results = dhm.get_custom_documents_by_field(
        collection=collection,
        field=field,
        value=value,
        limit=limit,
    )
    log.info(
        f"get_custom_documents_by_field: returning {len(results)} "
        f"documents from {collection!r}"
    )
    return results


def get_all_custom_documents(collection: str,
                             limit: int = 0,
                             ):
    """Return all documents from a non-managed collection.

    Args:
        collection: Source collection name.
        limit: Maximum number of results (0 = no limit).

    Raises:
        ValueError: If collection is managed.

    Returns:
        list of dicts.
    """
    log.info(
        f"get_all_custom_documents: collection={collection!r}, "
        f"limit={limit}"
    )
    _guard_managed_collection(collection, "get_all_custom_documents")
    results = dhm.get_all_custom_documents(
        collection=collection,
        limit=limit,
    )
    log.info(
        f"get_all_custom_documents: returning {len(results)} documents "
        f"from {collection!r}"
    )
    return results


def review_custom_documents(collection: str,
                            field: str = None,
                            value=None,
                            limit: int = 20,
                            ):
    """Print a human-readable summary of documents from a non-managed
    collection.  Intended for interactive/CLI use.

    Raises:
        ValueError: If collection is managed.
    """
    log.info(
        f"review_custom_documents: collection={collection!r}, "
        f"field={field!r}, value={value!r}, limit={limit}"
    )
    _guard_managed_collection(collection, "review_custom_documents")

    docs = list_custom_documents(
        collection=collection,
        field=field,
        value=value,
        limit=limit,
    )

    print(f"\n--- review_custom_documents: {collection!r} "
          f"({len(docs)} documents) ---")
    for doc in docs:
        # Strip the internal MongoDB _id from display output.
        display = {k: v for k, v in doc.items() if k != "_id"}
        print(json.dumps(display, indent=2, default=str))
    print("---\n")

    log.info(
        f"review_custom_documents: displayed {len(docs)} documents "
        f"from {collection!r}"
    )


##############################################################################
# Cross-collection integrity checks

# Collections to skip when checking for missing name attributes.
# Populate with collection names that legitimately have no name field.
MISSING_NAME_IGNORE_COLLECTIONS: list = []
FUTURE_DATETIMES_IGNORE_COLLECTIONS: list = ['events_ES']


def _is_datetime_string(value):
    """Return True when value can be parsed by dt_as_dt() and is not None."""
    try:
        return False if dt_as_dt(value) is None else True
    except Exception:
        return False


def _is_epoch_int(value):
    """Return True when value appears to be a Unix epoch second integer."""
    if not isinstance(value, int):
        return False
    # 1980-01-01 through 2200-01-01 captures realistic storage bounds.
    return 315532800 <= value <= 7258118400


def _detect_temporal_fields(collection: str,
                            sample_limit: int = 20):
    """Detect datetime-like string fields and epoch-like int fields.

    Samples a small number of records in the collection and inspects field
    values to identify candidates for temporal integrity checks.
    """
    samples = dhm.get_all_records_by_collection(collection=collection,
                                                limit=sample_limit)
    dt_string_fields = set()
    epoch_int_fields = set()
    # Infer field types from a small sample rather than hardcoding —
    # different collections may store dates in different fields.
    for doc in samples:
        for field, value in doc.items():
            if field == "_id":
                continue
            if _is_datetime_string(value):
                dt_string_fields.add(field)
            elif _is_epoch_int(value):
                epoch_int_fields.add(field)

    return {
        "dt_string_fields": sorted(dt_string_fields),
        "epoch_int_fields": sorted(epoch_int_fields),
        "samples_checked": len(samples),
    }


def check_integrity_future_datetimes():
    """Identify documents with datetime attributes set in the future.

    Dynamically samples each collection to detect fields containing
    datetime strings (formatted like dt_as_str()) and integer epoch
    seconds, then checks those fields for values greater than "now".
    Does not modify any data.

    Returns:
        dict: Results with keys status, total_issues,
        issues_by_collection, and detected_fields_by_collection.
    """
    now_epoch = dt_to_epoch(dt.now())
    now_str = dt_as_str(dt_from_epoch(now_epoch))
    issues_by_collection = {}
    detected_fields_by_collection = {}
    total_issues = 0

    for coll in list_mongo_collections():
        if coll in FUTURE_DATETIMES_IGNORE_COLLECTIONS:
            continue
        detected = _detect_temporal_fields(collection=coll)
        detected_fields_by_collection[coll] = detected
        found = []

        for epoch_field in detected["epoch_int_fields"]:
            docs = dhm.run_query(
                {epoch_field: {"$gt": now_epoch}},
                collection=coll,
            )
            for doc in docs:
                value = doc.get(epoch_field)
                if not _is_epoch_int(value):
                    continue
                found.append({
                    "collection": coll,
                    "field": epoch_field,
                    "value": value,
                    "name": doc.get("name"),
                    "_id": str(doc.get("_id")),
                })

        for field in detected["dt_string_fields"]:
            docs = dhm.run_query(
                {"$and": [
                    {field: {"$ne": None}},
                    {field: {"$gt": now_str}},
                ]},
                collection=coll,
            )
            for doc in docs:
                value = doc.get(field)
                if not _is_datetime_string(value):
                    continue
                found.append({
                    "collection": coll,
                    "field": field,
                    "value": value,
                    "name": doc.get("name"),
                    "_id": str(doc.get("_id")),
                })

        if found:
            issues_by_collection[coll] = found
            total_issues += len(found)

    status = "OK" if total_issues == 0 else "ERRORS"
    return {
        "status": status,
        "total_issues": total_issues,
        "issues_by_collection": issues_by_collection,
        "detected_fields_by_collection": detected_fields_by_collection,
    }


def check_integrity_no_test_orphans():
    """Identify documents with TEST or DELETEME in their name attribute.

    Dynamically checks all collections in MongoDB for documents where
    the name field contains 'TEST' or 'DELETEME'.  These strings
    indicate leftover unit test objects that must not persist in any
    non-test environment.  Does not modify any data.

    Returns:
        dict: Results with keys status, total_issues,
        and issues_by_collection.
    """
    issues_by_collection = {}
    total_issues = 0
    query = {"name": {"$regex": "TEST|DELETEME"}}

    for coll in list_mongo_collections():
        docs = dhm.run_query(query, collection=coll)
        if docs:
            found = [
                {
                    "collection": coll,
                    "name": doc.get("name"),
                    "_id": str(doc.get("_id")),
                }
                for doc in docs
            ]
            issues_by_collection[coll] = found
            total_issues += len(found)

    status = "OK" if total_issues == 0 else "ERRORS"
    return {
        "status": status,
        "total_issues": total_issues,
        "issues_by_collection": issues_by_collection,
    }


def check_integrity_no_nameless_objects(ignore=None):
    """Identify documents missing a non-null, non-empty name attribute.

    Dynamically checks all collections in MongoDB for documents where
    the name field is absent, null, or an empty string.  Collections
    in the ignore list are bypassed entirely.  Does not modify any data.

    Args:
        ignore: List of collection name strings to skip.
            Defaults to MISSING_NAME_IGNORE_COLLECTIONS.

    Returns:
        dict: Results with keys status, total_issues,
        issues_by_collection, sample_issues_by_collection,
        and skipped_collections.
    """
    if ignore is None:
        ignore = MISSING_NAME_IGNORE_COLLECTIONS
    issues_by_collection = {}
    sample_issues_by_collection = {}
    skipped_collections = []
    total_issues = 0
    query = {"$or": [{"name": None}, {"name": ""}]}

    for coll in list_mongo_collections():
        if coll in ignore:
            skipped_collections.append(coll)
            continue
        count = dhm.count_records(collection=coll, query=query)
        if count > 0:
            issues_by_collection[coll] = count
            docs = dhm.run_query(query=query,
                                 collection=coll,
                                 limit=10)
            sample_issues_by_collection[coll] = [
                {
                    "collection": coll,
                    "_id": str(doc.get("_id")),
                    "name": doc.get("name"),
                    "ts_id": doc.get("ts_id"),
                    "bt_id": doc.get("bt_id"),
                }
                for doc in docs
            ]
            total_issues += count

    status = "OK" if total_issues == 0 else "ERRORS"
    return {
        "status": status,
        "total_issues": total_issues,
        "issues_by_collection": issues_by_collection,
        "sample_issues_by_collection": sample_issues_by_collection,
        "skipped_collections": skipped_collections,
    }


def check_integrity_trade_ids(collection: str = COLLECTIONS["trades"]):
    """Check all stored trades have non-empty, unique trade_id values.

    Iterates every trade record in the collection once and checks two
    conditions:
    - trade_id is neither None nor blank (missing/backfill gap)
    - trade_id is unique across the collection (no duplicates)

    Does not modify any data.

    Returns:
        dict: Results with keys status, total_trades, missing_count,
        missing_samples, duplicate_count, and duplicate_samples.
    """
    all_docs = dhm.get_all_records_by_collection(collection=collection)
    total_trades = len(all_docs)
    missing = []
    trade_id_counts = Counter()

    for doc in all_docs:
        tid = doc.get("trade_id")
        if tid is None or (
            isinstance(tid, str) and tid.strip() == ""
        ):
            missing.append({
                "_id": str(doc.get("_id")),
                "ts_id": doc.get("ts_id"),
                "open_dt": doc.get("open_dt"),
                "trade_id": tid,
            })
        else:
            trade_id_counts[tid] += 1

    duplicates = {
        tid: count
        for tid, count in trade_id_counts.items()
        if count > 1
    }

    status = "OK" if not missing and not duplicates else "ERRORS"
    return {
        "status": status,
        "total_trades": total_trades,
        "missing_count": len(missing),
        "missing_samples": missing[:10],
        "duplicate_count": len(duplicates),
        "duplicate_samples": [
            {"trade_id": tid, "count": count}
            for tid, count in sorted(
                duplicates.items(),
                key=lambda x: x[1],
                reverse=True,
            )[:10]
        ],
    }


##############################################################################
# Trades
def reconstruct_trade(t):
    """Takes a dictionary and builds a Trade() object from it.

    Primarily used by other functions to convert results retrieved from
    storage.
    """
    close_dt = t["close_dt"]
    # Trade() expects a close_dt during initialization; rebuild unclosed
    # trades using open_dt first, then restore unclosed state.
    init_close_dt = close_dt if close_dt is not None else t["open_dt"]
    result = Trade(open_dt=t["open_dt"],
                   direction=t["direction"],
                   timeframe=t["timeframe"],
                   trading_hours=t["trading_hours"],
                   entry_price=t["entry_price"],
                   close_dt=init_close_dt,
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
                   trade_id=t["trade_id"],
                   tags=t["tags"],
                   )
    if close_dt is None:
        result.close_dt = None
        result.close_date = None
        result.close_time = None
        result.is_open = True

    return result


def get_all_trades(collection: str = COLLECTIONS["trades"],
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
                        collection: str = COLLECTIONS["trades"],
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


def get_trades_by_field_in(field: str,
                           values: list,
                           collection: str = COLLECTIONS["trades"],
                           limit=0,
                           show_progress: bool = False,
                           ):
    """Return Trade objects where field is in values."""
    result = []
    r = dhm.get_trades_by_field_in(field=field,
                                   values=values,
                                   collection=collection,
                                   limit=limit,
                                   show_progress=show_progress,
                                   )

    total = len(r)
    pbar = start_progbar(show_progress, total,
                         "Trade objects built")
    for i, t in enumerate(r, start=1):
        result.append(reconstruct_trade(t))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def store_trades(trades: list,
                 collection: str = COLLECTIONS["trades"],
                 ):
    """Store one or more Trade() objects in central storage."""
    # Convert Trade objects to dictionaries for storage
    log.info(f"Preparing {len(trades)} trades to store by converting to dicts")
    working_trades = []
    for t in trades:
        if t.trade_id is None or str(t.trade_id).strip() == "":
            raise ValueError(
                f"Cannot store Trade with empty trade_id {t.trade_id}. "
                "Bind ts_id first."
            )
        working_trades.append(t.to_clean_dict())

    # Store in database
    log.info(f"Writing {len(working_trades)} trades to "
             f"collection={collection}")
    result = dhm.store_trades(trades=working_trades,
                              collection=collection)
    log.info("Storage complete, returning result")

    return result


def review_trades(symbol: str = "ES",
                  collection: str = COLLECTIONS["trades"],
                  bt_id: str = None,
                  ts_id: str = None,
                  include_epochs: bool = False,
                  check_integrity: bool = False,
                  show_progress: bool = False,
                  multi_ok: list = None,
                  orphan_ok: list = None,
                  list_issues: bool = False,
                  out_path: str = None,
                  out_file: str = "backtests_integrity_results.json",
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
    if orphan_ok is None:
        orphan_ok = []
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
        log_say("Checking integrity for all Backtests in storage")
        time_full = OperationTimer(
            name="backtests_integrity check full run timer")

        # Get all TradeSeries in storage to check for orphaned test objects
        if bt_id is None and ts_id is None:
            all_ts = get_all_tradeseries()
        elif bt_id is not None and ts_id is None:
            all_ts = get_tradeseries_by_field(field="bt_id",
                                              value=bt_id,
                                              include_trades=False)
        elif bt_id is None and ts_id is not None:
            all_ts = get_tradeseries_by_field(field="ts_id",
                                              value=ts_id,
                                              include_trades=False)
        else:
            raise ValueError("Integrity check supports bt_id or ts_id, not "
                             f"both!  We got ts_id={ts_id} bt_id={bt_id}")

        # Get Trades directly from storage for integrity checks
        if bt_id is None and ts_id is None:
            all_trades = get_all_trades()
        elif bt_id is not None and ts_id is None:
            all_trades = get_trades_by_field(field="bt_id",
                                             value=bt_id)
        elif bt_id is None and ts_id is not None:
            all_trades = get_trades_by_field(field="ts_id",
                                             value=ts_id)
        else:
            raise ValueError("Integrity check supports bt_id or ts_id, not "
                             f"both!  We got ts_id={ts_id} bt_id={bt_id}")
        duplicates = []
        multidays = []
        unclosed_trades = []
        orphaned_test_objects = []
        autoclosed_issues = {}
        total_trades = 0
        unique_trades = 0

        def contains_test_marker(value):
            """Check for substrings only used in unit test objects."""
            if value is None:
                return False
            check = str(value)
            return "DELETEME" in check or "TEST" in check

        def append_orphaned(object_type, name, this_ts_id, this_bt_id, ignore):
            """Detect and flag any orphaned test objects."""
            # Toss any None fields, they just add complexity and won't match
            check_against = [
                s for s in [name, this_ts_id, this_bt_id] if s is not None
                ]
            # Do nothing if any fields match an ignore string
            for i in ignore:
                for s in check_against:
                    if i in s:
                        return False
            # Check remaining fields for test markers, flagging orphan if found
            for s in check_against:
                if contains_test_marker(s):
                    orphaned_test_objects.append({
                        "issue_type": "orphaned_test_objects",
                        "object_type": object_type,
                        "name": name,
                        "ts_id": this_ts_id,
                        "bt_id": this_bt_id,
                        })
                return True
            return False

        # Check backtests for orphaned test objects
        if bt_id is None:
            all_backtests = get_all_backtests()
        else:
            all_backtests = get_backtests_by_field(field="bt_id",
                                                   value=bt_id)
        for b in all_backtests:
            append_orphaned(object_type="backtest",
                            name=b.get("name"),
                            this_ts_id=None,
                            this_bt_id=b.get("bt_id"),
                            ignore=orphan_ok)

        # Cache all events for autoclosed trade integrity checking
        log_say("Caching all events for autoclosed integrity checks")
        all_events = get_events(symbol="ES")
        # Create a set of event start_dt strings for fast lookup
        event_start_times = {event.start_dt for event in all_events}
        log_say(f"Cached {len(event_start_times)} unique event "
                f"start times")

        # Start a progress bar for TradeSeries orphan checks
        bar_total = len(all_ts)
        pbar = start_progbar(show_progress, bar_total,
                             "TradeSeries checked")

        # Loop through all TradeSeries, checking orphaned test objects only
        for i, x in enumerate(all_ts):
            log.info("Checking stored TradeSeries integrity "
                     f"for ts_id={x.ts_id}")
            update_progbar(pbar, i, bar_total)
            append_orphaned(object_type="tradeseries",
                            name=x.name,
                            this_ts_id=x.ts_id,
                            this_bt_id=x.bt_id,
                            ignore=orphan_ok)

        finish_progbar(pbar)

        # Start a progress bar for Trade integrity checks
        bar_total = len(all_trades)
        pbar = start_progbar(show_progress, bar_total,
                             "Trades checked")

        # Loop through Trades for orphan, duplicate, multiday, and
        # autoclosed-event integrity checks
        unique = set()
        for i, t in enumerate(all_trades):
            update_progbar(pbar, i, bar_total)
            total_trades += 1
            append_orphaned(object_type="trade",
                            name=t.name,
                            this_ts_id=t.ts_id,
                            this_bt_id=t.bt_id,
                            ignore=orphan_ok)

            this = (t.ts_id, t.open_dt)
            if this in unique:
                duplicates.append(this)
                log.warning(f"Duplicate Trades found in storage: {this}")
            else:
                unique.add(this)
                unique_trades += 1

            # Determine if this ts_id allows multiday trades
            check_multi = True
            for allowed in multi_ok:
                if allowed in t.ts_id:
                    check_multi = False
                    break

            if check_multi:
                # First flag as unclosed if no close_dt set
                if t.close_dt is None:
                    this = {"issue_type": "unclosed_trade",
                            "ts_id": t.ts_id,
                            "open_dt": t.open_dt,
                            "close_dt": t.close_dt}
                    log.warning("Unclosed trade found in storage: "
                                f"{this}")
                    unclosed_trades.append(this)
                # Then check for multiday trades
                elif not t.closed_intraday():
                    this = {"ts_id": t.ts_id,
                            "open_dt": t.open_dt,
                            "close_dt": t.close_dt}
                    log.warning("Unapproved multiday trade found in "
                                f"storage: {this}")
                    multidays.append(this)

            # Check autoclosed trades for integrity
            if ("autoclosed" in t.tags
                    and t.close_dt is not None
                    and t.close_time != "15:55:00"):
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
        finish_progbar(pbar)

        # Output findings
        status = "OK"
        issues = []
        if len(duplicates) > 0:
            status = "ERRORS"
            issues.append(f"{len(duplicates)} duplicate trades found")
        if len(multidays) > 0:
            status = "ERRORS"
            issues.append(f"{len(multidays)} invalid multiday trades found")
        if len(unclosed_trades) > 0:
            status = "ERRORS"
            issues.append(f"{len(unclosed_trades)} unclosed_trade errors "
                          "found")
        if len(orphaned_test_objects) > 0:
            status = "ERRORS"
            issues.append(
                f"{len(orphaned_test_objects)} orphaned_test_objects "
                "errors found"
            )
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
                     "unclosed_trade_errors": len(unclosed_trades),
                     "orphaned_test_objects_errors": len(
                         orphaned_test_objects),
                     "autoclosed_integrity_issues": len(autoclosed_issues),
                     "total_autoclosed_bad_trades": sum(
                         autoclosed_issues.values()) if autoclosed_issues
                     else 0
                     }
        if list_issues:
            integrity["duplicates"] = duplicates
            integrity["multidays"] = multidays
            integrity["unclosed_trades"] = unclosed_trades
            integrity["orphaned_test_objects"] = orphaned_test_objects
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
            blob["integrity"]["unclosed_trades"] = unclosed_trades
            blob["integrity"]["orphaned_test_objects"] = (
                orphaned_test_objects)
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
                           collection: str = COLLECTIONS["trades"],
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
                  collection: str = COLLECTIONS["trades"],
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
    """Takes a dictionary and builds a TradeSeries() object from it.

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


def get_all_tradeseries(collection: str = COLLECTIONS["tradeseries"],
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
                             collection: str = COLLECTIONS["tradeseries"],
                             collection_trades: str = COLLECTIONS["trades"],
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
                      collection: str = COLLECTIONS["tradeseries"],
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
                       collection: str = COLLECTIONS["tradeseries"],
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
        bar = None
        if bar_total > 0:
            bar = ProgBar(total=bar_total,
                          desc="checked")
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
            if bar is not None:
                bar.update(i + 1)
            # Reclaim memory
            ts.trades.clear()
        if bar is not None:
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
                                collection: str = COLLECTIONS["tradeseries"],
                                coll_trades=COLLECTIONS["trades"],
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
                       collection: str = COLLECTIONS["tradeseries"],
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
def get_all_backtests(collection: str = COLLECTIONS["backtests"],
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
                           collection: str = COLLECTIONS["backtests"],
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
                    collection: str = COLLECTIONS["backtests"],
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
                     collection: str = COLLECTIONS["backtests"],
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
                              collection: str = COLLECTIONS["backtests"],
                              coll_tradeseries: str = (
                                  COLLECTIONS["tradeseries"]),
                              coll_trades: str = COLLECTIONS["trades"],
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
                     collection: str = COLLECTIONS["backtests"],
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
# TradePlans
_TRADEPLAN_CTOR_KEYS = frozenset({
    "contracts", "con_fee", "tp_id", "name", "id_slug", "tags",
    "cfg_label",
    "profit_perc", "start_dt", "end_dt", "drawdown_open",
    "drawdown_limit", "notes", "thresholds", "tradeseries",
    "how_gl_heatmap_viz", "weekly_price_overlay_visuals",
})


def reconstruct_tradeplan(tp,
                          collection_trades: str = COLLECTIONS["trades"]):
    """Take a dictionary and build a TradePlan object from it.

    Reconstructs using constructor fields first, then reattaches any
    remaining stored keys via setattr to preserve post-init state.
    """
    raw_ts = tp.get("tradeseries")
    ts = None
    if isinstance(raw_ts, dict):
        ts = reconstruct_tradeseries(raw_ts)

    trade_ids = tp.get("trade_ids", [])
    if trade_ids is None:
        trade_ids = []
    if not isinstance(trade_ids, list):
        raise TypeError("TradePlan trade_ids must be a list when provided")
    for trade_id in trade_ids:
        if not isinstance(trade_id, str) or not trade_id:
            raise TypeError(
                "TradePlan trade_ids entries must be non-empty strings"
            )

    if ts is not None:
        ts.trades = get_trades_by_field_in(
            field="trade_id",
            values=trade_ids,
            collection=collection_trades,
        )
        ts.sort_trades()

    obj = TradePlan(
        contracts=tp["contracts"],
        con_fee=tp.get("con_fee", 0.0),
        tp_id=tp.get("tp_id"),
        name=tp.get("name"),
        id_slug=tp.get("id_slug"),
        tags=tp.get("tags", []),
        cfg_label=tp.get("cfg_label"),
        profit_perc=tp.get("profit_perc", 100),
        start_dt=tp.get("start_dt"),
        end_dt=tp.get("end_dt"),
        drawdown_open=tp.get("drawdown_open"),
        drawdown_limit=tp.get("drawdown_limit"),
        notes=tp.get("notes", []),
        thresholds=tp.get("thresholds", {}),
        tradeseries=ts,
        how_gl_heatmap_viz=tp.get("how_gl_heatmap_viz"),
        weekly_price_overlay_visuals=tp.get(
            "weekly_price_overlay_visuals"
        ),
    )

    for k, v in tp.items():
        if k not in _TRADEPLAN_CTOR_KEYS and k not in {"_id", "trade_ids"}:
            setattr(obj, k, v)

    return obj


def get_all_tradeplans(collection: str = COLLECTIONS["tradeplans"],
                       collection_trades: str = COLLECTIONS["trades"],
                       limit=0,
                       as_dict: bool = False,
                       show_progress: bool = False,
                       ):
    """Get stored tradeplans as TradePlan objects or raw dicts."""
    r = dhm.get_all_records_by_collection(collection=collection,
                                          limit=limit,
                                          show_progress=show_progress)
    if as_dict:
        return r

    total = len(r)
    pbar = start_progbar(show_progress, total,
                         "TradePlan objects built")
    result = []
    for i, t in enumerate(r, start=1):
        result.append(reconstruct_tradeplan(
            t,
            collection_trades=collection_trades,
        ))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def get_tradeplans_by_field(field: str,
                            value,
                            collection: str = COLLECTIONS["tradeplans"],
                            collection_trades: str = COLLECTIONS["trades"],
                            as_dict: bool = False,
                            limit=0,
                            show_progress: bool = False,
                            ):
    """Return TradePlan objects or dicts matching given field=value."""
    r = dhm.get_tradeplans_by_field(field=field,
                                    value=value,
                                    collection=collection,
                                    limit=limit,
                                    show_progress=show_progress,
                                    )
    if as_dict:
        return r

    total = len(r)
    pbar = start_progbar(show_progress, total,
                         "TradePlan objects built")
    result = []
    for i, t in enumerate(r, start=1):
        result.append(reconstruct_tradeplan(
            t,
            collection_trades=collection_trades,
        ))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)

    return result


def store_tradeplans(tradeplans: list,
                     collection: str = COLLECTIONS["tradeplans"],
                     ):
    """Store a list of TradePlan objects in central storage."""
    for tp in tradeplans:
        if not tp.name:
            raise ValueError(
                "TradePlan.name must not be None or empty "
                f"string before storing: tp_id={tp.tp_id!r} name={tp.name!r}"
            )
        if not tp.id_slug:
            raise ValueError(
                "TradePlan.id_slug must not be None or empty "
                f"string before storing: tp_id={tp.tp_id!r} "
                f"id_slug={tp.id_slug!r}"
            )
        if not tp.cfg_label:
            raise ValueError(
                "TradePlan.cfg_label must not be None or empty "
                f"string before storing: tp_id={tp.tp_id!r} "
                f"cfg_label={tp.cfg_label!r}"
            )
    result = []
    for tp in tradeplans:
        tp_result = dhm.store_tradeplan(tp.to_clean_dict(),
                                        collection=collection,
                                        )
        tp_result["tp_id"] = tp.tp_id
        result.append(tp_result)

    return result


def delete_tradeplans_by_field(field: str,
                               value,
                               collection: str = COLLECTIONS["tradeplans"],
                               ):
    """Delete all tradeplan records with 'field' matching 'value'."""
    result = dhm.delete_tradeplans_by_field(field=field,
                                            value=value,
                                            collection=collection,
                                            )

    return result


def delete_tradeplans(tradeplans: list,
                      collection: str = COLLECTIONS["tradeplans"],
                      ):
    """Delete TradePlan objects from central storage using tp_id."""
    tp_ids = [tp.tp_id for tp in tradeplans]
    result = dhm.delete_tradeplans(tp_ids=tp_ids,
                                   collection=collection,
                                   )

    return result


##############################################################################
# Indicators
def list_indicators(meta_collection: str = COLLECTIONS["ind_meta"]):
    """Return a simple list of indicators in storage."""
    result = dhm.list_indicators(meta_collection=meta_collection)

    return result


def list_indicators_names(meta_collection: str = COLLECTIONS["ind_meta"]):
    """Return a simple list of indicators (ind_id only) in storage."""
    indicators = dhm.list_indicators(meta_collection=meta_collection)
    result = []
    for i in indicators:
        result.append(i['ind_id'])

    return result


def review_indicators(meta_collection: str = COLLECTIONS["ind_meta"],
                      dp_collection: str = COLLECTIONS["ind_dps"]):
    """Return a more detailed overview of indicators in storage."""
    result = dhm.review_indicators(meta_collection=meta_collection,
                                   dp_collection=dp_collection,
                                   )

    return result


def get_indicator(ind_id: str,
                  meta_collection: str = COLLECTIONS["ind_meta"],
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
                             dp_collection: str = COLLECTIONS["ind_dps"],
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
                                         epoch=d["epoch"],
                                         name=d["name"],
                                         ))
        update_progbar(pbar, i, total)
    finish_progbar(pbar)
    log.info(f"Returning {len(result)} datapoints")

    return result


def store_indicator_datapoints(datapoints: list,
                               collection: str = COLLECTIONS["ind_dps"],
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
                    meta_collection: str = COLLECTIONS["ind_meta"],
                    dp_collection: str = COLLECTIONS["ind_dps"],
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
            bar = None

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
                        bar = ProgBar(total=bar_total,
                                      desc="stored")
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
                    bar = ProgBar(total=bar_total,
                                  desc="stored")
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
                     meta_collection: str = COLLECTIONS["ind_meta"],
                     dp_collection: str = COLLECTIONS["ind_dps"],
                     ):
    """Remove an indicator and all its datapoints from storage by ind_id.
    """
    return dhm.delete_indicator(ind_id=ind_id,
                                meta_collection=meta_collection,
                                dp_collection=dp_collection,
                                )


def get_indicators_by_name(name: str,
                           meta_collection: str = COLLECTIONS["ind_meta"],
                           autoload_chart: bool = False,
                           autoload_datapoints: bool = False,
                           ):
    """Return a list of Indicator objects for all indicators with the name.

    autoload_chart and autoload_datapoints both default to False for
    performance.  When enabled they load for the earliest_dt to latest_dt
    range.
    """
    raw_docs = dhm.get_indicators_by_name(name=name,
                                          meta_collection=meta_collection,
                                          )
    result = []
    for doc in raw_docs:
        common = dict(description=doc["description"],
                      timeframe=doc["timeframe"],
                      trading_hours=doc["trading_hours"],
                      symbol=doc["symbol"],
                      calc_version=doc["calc_version"],
                      calc_details=doc["calc_details"],
                      ind_id=doc["ind_id"],
                      autoload_chart=autoload_chart,
                      name=doc["name"],
                      parameters=doc["parameters"],
                      )
        if doc["class_name"] == "IndicatorSMA":
            ind = IndicatorSMA(**common)
        elif doc["class_name"] == "IndicatorEMA":
            ind = IndicatorEMA(**common)
        else:
            raise ValueError(
                f"Unable to match class_name of {doc['class_name']} "
                "with a known Indicator() subclass."
            )
        if autoload_datapoints:
            ind.load_datapoints()
        result.append(ind)
    return result


def delete_indicators_by_name(name: str,
                              meta_collection: str = COLLECTIONS["ind_meta"],
                              dp_collection: str = COLLECTIONS["ind_dps"],
                              ):
    """Delete all indicators and their datapoints with the given name."""
    indicators = get_indicators_by_name(name=name,
                                        meta_collection=meta_collection)
    for ind in indicators:
        delete_indicator(ind_id=ind.ind_id,
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
                     name=candle.name,
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
    log_sym = symbol if isinstance(symbol, str) else symbol.ticker
    log.info(f"Retrieving candles from storage for {log_sym} "
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
                              name=r.get("name", DEFAULT_OBJ_NAME)
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
                   start_dt=None,
                   end_dt=None,
                   ):
    """Provide aggregate summary data about candles in central storage.

    Options to further check completeness/integrity of candle data and
    provide remediation.

    There is substantial overlap in the integrity checks performed by this
    function with dhutil.compare_candles_vs_csv() validations.  Both are worth
    running as this reviews from a different perspective and covers higher
    timeframes for which we may not have CSVs available to validate against.
    They are complementary despite some redundancy.
    """
    if isinstance(symbol, str):
        symbol = get_symbol_by_ticker(ticker=symbol)
    log_say("Retrieving candles overview from storage")
    log_sym = symbol if isinstance(symbol, str) else symbol.ticker
    log.info(f"Retrieving candles from storage for {log_sym} {timeframe}")
    overview = dhm.review_candles(timeframe=timeframe,
                                  symbol=symbol.ticker,
                                  )
    log_say(f"Finished retrieval from storage for {log_sym} {timeframe}")
    if overview is None:
        log_say(f"No candles found for the specified timeframe {timeframe}")
        return None
    if start_dt is not None:
        # Use passed start_dt only if it's newer than earliest in storage
        start_epoch = max(dt_to_epoch(start_dt),
                          dt_to_epoch(overview["earliest_dt"]))
    else:
        start_epoch = dt_to_epoch(overview["earliest_dt"])
    if end_dt is not None:
        # Use passed end_dt only if it's older than latest in storage
        end_epoch = min(dt_to_epoch(end_dt),
                        dt_to_epoch(overview["latest_dt"]))
    else:
        end_epoch = dt_to_epoch(overview["latest_dt"])
    if check_integrity:
        log_say("Starting integrity checks and gap analysis because "
                "check_integrity=True")
        log_say("Retrieving candles from storage for "
                f"{log_sym} {timeframe} between "
                f"{dt_as_str(dt_from_epoch(start_epoch))} and "
                f"{dt_as_str(dt_from_epoch(end_epoch))}")
        candles = get_candles(timeframe=timeframe,
                              symbol=symbol.ticker,
                              start_epoch=start_epoch,
                              end_epoch=end_epoch,
                              )

        # Build expected candle datetimes first so summarize_candles can
        # derive expected minutes/hours/times dynamically (era-aware)
        dt_stored = []
        for c in candles:
            dt_stored.append(dt_as_str(c.c_datetime))
        start_dt = dt_from_epoch(start_epoch)
        end_dt = dt_from_epoch(end_epoch)
        log_say(f"Fetching all events from storage for {symbol.ticker} in the "
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

        # Perform a per-era summary check on times vs expected
        log_say("Summarizing retrieved candles per market era")
        era_summaries = []
        status = "OK"
        err_msg = ""

        stored_start_date = start_dt.date()
        stored_end_date = end_dt.date()
        for i, era in enumerate(MARKET_ERAS):
            era_start = era["start_date"]
            if i + 1 < len(MARKET_ERAS):
                era_end = (MARKET_ERAS[i + 1]["start_date"]
                           - timedelta(days=1))
            else:
                era_end = stored_end_date
            slice_start = max(era_start, stored_start_date)
            slice_end = min(era_end, stored_end_date)
            if slice_start > slice_end:
                continue
            # Filter candles and expected datetimes to the current era
            era_candles = [
                c for c in candles
                if (slice_start
                    <= dt_as_dt(c.c_datetime).date()
                    <= slice_end)
            ]
            if not era_candles:
                continue
            era_expected_dts = [
                d for d in dt_expected
                if (slice_start
                    <= dt_as_dt(d).date()
                    <= slice_end)
            ]
            # Summarize the era's candles to get counts and lists
            era_breakdown = summarize_candles(
                timeframe=timeframe,
                symbol=symbol,
                candles=era_candles,
                expected_dts=era_expected_dts,
            )
            era_name = era["name"]
            era_sd = era_breakdown["summary_data"]
            era_se = era_breakdown["summary_expected"]
            era_errors = []
            if era_se is not None:
                # Compare stored vs expected for each key in summary_data
                for k, v in era_se.items():
                    if era_sd[k] != v:
                        stored_vals = era_sd[k]
                        expected_vals = v
                        # For list-type fields, compute set diffs and show
                        # both missing from STORED and unexpected in STORED
                        if (
                            isinstance(stored_vals, list)
                            and isinstance(expected_vals, list)
                        ):
                            missing_from_stored = sorted(
                                set(expected_vals) - set(stored_vals)
                            )
                            unexpected_in_stored = sorted(
                                set(stored_vals) - set(expected_vals)
                            )
                            era_errors.append(
                                f"{k}: missing from STORED="
                                f"{len(missing_from_stored)} "
                                f"{missing_from_stored}; "
                                f"unexpected in STORED="
                                f"{len(unexpected_in_stored)} "
                                f"{unexpected_in_stored}"
                            )
                        else:
                            # For non-list fields, show stored vs expected
                            era_errors.append(
                                f"{k}: STORED={stored_vals} "
                                f"EXPECTED={expected_vals}"
                            )
            else:
                # No expected data defined for this timeframe
                era_errors.append(
                    "Expected data not defined for "
                    f"timeframe: {timeframe}"
                )
            # Store era summary with summary data and any errors found
            era_summaries.append({
                "era_name": era_name,
                "summary_data": era_sd,
                "summary_expected": era_se,
                "errors": era_errors,
            })
            # Accumulate errors into the overall err_msg for reporting
            if era_errors:
                status = "ERROR"
                era_errs_str = " || ".join(era_errors)
                if err_msg:
                    err_msg += " || "
                err_msg += f"[{era_name}] {era_errs_str}"
        summary_data = era_summaries
        summary_expected = None

        # Perform a detailed analysis of stored vs expected timestamps
        log_say("Performing detailed analysis of stored vs expected "
                "candle datetimes")
        log_say("Comparing stored vs expected candles")

        # Convert expected to strings for comparison and review
        dt_expected_str = []
        for d in dt_expected:
            dt_expected_str.append(dt_as_str(d))
        # Ensure we don't have any timestamp duplications
        set_stored = set(dt_stored)
        set_expected = set(dt_expected_str)
        if len(dt_stored) != len(set_stored):
            counters = Counter(dt_stored)
            dupes = []
            for k, v in counters.items():
                if v > 1:
                    dupes.append({k: v})
            raise Exception(f"len(dt_stored) {len(dt_stored)} != "
                            f"len(set(stored) {len(set_stored)}.  Likely "
                            "there are duplicates in stored candle data "
                            "which will corrupt analysis results.  Duplicates "
                            f"found in dt_stored:\n\n{dupes}"
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

        # Check for differences between stored and expected candle sets
        # and parse results into a few helpful views
        missing_from_stored = sorted(set_expected - set_stored)
        missing_candles_count = len(missing_from_stored)
        missing_candles_by_date = defaultdict(list)
        for c in missing_from_stored:
            k = c.split(' ')[0]
            v = c.split(' ')[1]
            missing_candles_by_date[k].append(v)
        missing_candles_by_date = sort_dict(dict(missing_candles_by_date))
        missing_count_by_date = {}
        for k, v in missing_candles_by_date.items():
            missing_count_by_date[k] = len(v)
        missing_candles_by_hour = defaultdict(list)
        for c in missing_from_stored:
            k = c.split(' ')[1].split(':')[0]
            missing_candles_by_hour[k].append(c)
        missing_candles_by_hour = sort_dict(dict(missing_candles_by_hour))
        missing_count_by_hour = {}
        for k, v in missing_candles_by_hour.items():
            missing_count_by_hour[k] = len(v)
        unexpected_in_stored = sorted(set_stored - set_expected)
        unexpected_candles_count = len(unexpected_in_stored)
        # Log individual candle issues at DEBUG level
        for c in missing_from_stored:
            log.debug(f"{symbol} {timeframe} MISSING: {c}")
        for c in unexpected_in_stored:
            log.debug(f"{symbol} {timeframe} UNEXPECTED: {c}")
        # Create human digestible ranges
        missing_ranges = rangify_candle_times(times=missing_from_stored,
                                              timeframe=timeframe)
        unexpected_ranges = rangify_candle_times(times=unexpected_in_stored,
                                                 timeframe=timeframe,
                                                 )
        gap_analysis = {"missing_candles_count": missing_candles_count,
                        "unexpected_candles_count": unexpected_candles_count,
                        "missing_candles": missing_from_stored,
                        "unexpected_candles": unexpected_in_stored,
                        "missing_candles_ranges": missing_ranges,
                        "unexpected_candles_ranges": unexpected_ranges,
                        }
        if missing_candles_count > 0:
            status = "ERROR"
            if err_msg != "":
                err_msg += " || "
            err_msg += f"{missing_candles_count} missing from STORED "
        if unexpected_candles_count > 0:
            status = "ERROR"
            if err_msg != "":
                err_msg += " || "
            err_msg += (f"{unexpected_candles_count} unexpected candles "
                        "in STORED")
        integrity_data = {"status": status, "err_msg": err_msg}
    else:
        log_say("Skipping integrity checks and gap analysis because "
                "check_integrity=False")
        integrity_data = None
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


def delete_candles_by_field(symbol: str,
                            timeframe: str,
                            field: str,
                            value,
                            ):
    """Delete candles from central storage with 'field' matching 'value'.

    Typically used to delete by name or other identifying fields.

    Example to delete all candles with name=="DELETEME"::

        delete_candles_by_field(symbol="ES", timeframe="1m",
                                field="name", value="DELETEME")
    """
    return dhm.delete_candles_by_field(symbol=symbol,
                                       timeframe=timeframe,
                                       field=field,
                                       value=value,
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
                             name=event.name,
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
                            name=r["name"],
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


def delete_events_by_field(symbol: str,
                           field: str,
                           value,
                           ):
    """Delete events from central storage with 'field' matching 'value'.

    Typically used to delete by name or category fields.

    Example to delete all events with name=="DELETEME":
    delete_events_by_field(symbol="ES", field="name", value="DELETEME")
    """
    return dhm.delete_events_by_field(symbol=symbol,
                                      field=field,
                                      value=value,
                                      )
