#!/usr/bin/env python3
"""Backfill missing trade_id values on stored Trade documents.

Usage examples:
    python3 bin/backfill_trade_id.py --dry-run
    python3 bin/backfill_trade_id.py --apply

The script updates only records missing trade_id and reports:
- how many were newly assigned
- how many existing values already matched expected
- how many existing values differed and were not changed

Differing records are written to a JSON file for manual review.
"""

import argparse
import json
from pathlib import Path

from dhtrader import dhmongo as dhm


def _expected_trade_id(doc):
    """Build the expected trade_id from ts_id and open_epoch."""
    ts_id = doc.get("ts_id")
    open_epoch = doc.get("open_epoch")
    if not isinstance(ts_id, str) or not ts_id.strip():
        return None
    try:
        open_epoch = int(open_epoch)
    except (TypeError, ValueError):
        return None
    return f"{ts_id}_{open_epoch}"


def main():
    """Run the trade_id backfill in dry-run or apply mode."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--collection",
        default="trades",
        help="Mongo collection to scan (default: trades)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates (default is dry-run)",
    )
    parser.add_argument(
        "--diff-out",
        default="trade_id_backfill_differences.json",
        help="Path to write differing existing trade_id records",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable scan/apply progress bars",
    )
    args = parser.parse_args()

    collection = dhm.db[args.collection]
    show_progress = not args.no_progress
    total_docs = collection.count_documents({})
    scan_pbar = dhm.start_progbar(
        show_progress=show_progress,
        total=total_docs,
        desc=f"scan {args.collection} for trade_id backfill",
    )

    added = 0
    matching = 0
    differing = 0
    invalid_source = 0
    differing_records = []
    pending_updates = []

    for i, doc in enumerate(collection.find({}), start=1):
        expected = _expected_trade_id(doc)
        if expected is None:
            invalid_source += 1
            dhm.update_progbar(scan_pbar, i, total_docs)
            continue

        existing = doc.get("trade_id")
        if existing is None:
            added += 1
            pending_updates.append({"_id": doc["_id"],
                                    "trade_id": expected})
            dhm.update_progbar(scan_pbar, i, total_docs)
            continue

        if existing == expected:
            matching += 1
            dhm.update_progbar(scan_pbar, i, total_docs)
            continue

        differing += 1
        differing_records.append(
            {
                "_id": str(doc.get("_id")),
                "ts_id": doc.get("ts_id"),
                "open_epoch": doc.get("open_epoch"),
                "existing_trade_id": existing,
                "expected_trade_id": expected,
                "name": doc.get("name"),
                "bt_id": doc.get("bt_id"),
            }
        )
        dhm.update_progbar(scan_pbar, i, total_docs)

    dhm.finish_progbar(scan_pbar)

    if args.apply and pending_updates:
        updates_total = len(pending_updates)
        update_pbar = dhm.start_progbar(
            show_progress=show_progress,
            total=updates_total,
            desc=f"apply {updates_total} trade_id updates",
        )
        for i, update in enumerate(pending_updates, start=1):
            collection.update_one(
                {"_id": update["_id"]},
                {"$set": {"trade_id": update["trade_id"]}},
            )
            dhm.update_progbar(update_pbar, i, updates_total)
        dhm.finish_progbar(update_pbar)

    diff_path = Path(args.diff_out)
    with diff_path.open("w", encoding="utf-8") as f:
        json.dump(differing_records, f, indent=2)

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"Mode: {mode}")
    print(f"Collection: {args.collection}")
    print(f"Total records scanned: {total_docs}")
    print(f"Newly assigned trade_id: {added}")
    print(f"Existing trade_id already matching: {matching}")
    print(f"Existing trade_id differing (unchanged): {differing}")
    print(f"Invalid source records (ts_id/open_epoch unusable): "
          f"{invalid_source}")
    print(f"Differing records written to: {diff_path}")


if __name__ == "__main__":
    main()
