# Why DHTrader exists and where it's going

My personal classes representing charting and trading objects to be used in backtesting and analysis.  Objects in this repo should never include proprietary trade systems or ideas, they are just the framework.  Those items belong in private repos only.

This is not a detailed and robust backtesting/analysis platform nor is it intended to be used by others, it's mostly shared for the sake of having a public "code portfolio" and reference for snippets when discussing Python coding with others.  There is no feature roadmap, I just add things as I need them.

Two main reasons exist for building this.  Mostly it's a personal project to help me learn OOP and develop more robust coding skills beyond what my "Ops Guy" mostly-scripting professional career has provided opportunities for.  The reason to have this project outside of learning was to develop a framework to backtest and analyze trades that include a live drawdown factor in proprietary funded trading accounts ("prop firms") which I did not find when initially reviewing the more popular backtesting libraries available.  It also allows me to approach digging into ideas without the bias inherent towards particular techniques or ideas built into popular systems, in which anything that works is likely to get arbitraged out quickly.

See [docs index](/docs/index.html) for class details

## Core Domain Classes (`dhtypes.py`)

The framework's domain model is defined in `dhtypes.py`. These classes are
the core object types used throughout storage, backtesting, and analysis:

- `Symbol`: Market symbol metadata, trading schedules, and market-open
  checks.
- `Candle`: Single OHLCV bar with timeframe/trading-hours context.
- `Chart`: Candle collection and indicator-aware chart operations.
- `Event`: Market events such as closures/announcements used during
  analysis.
- `Day`: Day-level wrapper for candle/time-slice operations.
- `IndicatorDataPoint`: Single indicator output value at a timestamp.
- `Indicator`: Base indicator API for retrieval, calculation, and
  persistence.
- `IndicatorSMA`: Simple moving average indicator implementation.
- `IndicatorEMA`: Exponential moving average indicator implementation.
- `Trade`: Individual trade lifecycle, outcomes, and drawdown helpers.
- `TradeSeries`: Group of trades with aggregate and risk metrics.
- `Backtest`: Collection of TradeSeries with backtest-wide statistics.
- `TradePlan`: Strategy plan container combining configuration,
  selected TradeSeries, and plan-level analytics.

### TradePlan Details

`TradePlan` is the reusable base class for plan configuration and plan
analytics in this framework.

Implementation projects are expected to subclass `TradePlan` and add
additional methods as needed for strategy-specific analytic and refinement
workflows.

Note to self - docs are not autoupdating, run mkdocs.sh to update them.
Perhaps this can be a pre-commit hook?

## Storage Architecture (`dhstore.py`)

### Managed Collections

All managed collections are declared at the top of `dhstore.py` so the
guard logic in the custom document functions has a single authoritative
source of truth.

**`COLLECTIONS` dict** — fixed-name collections:

| Key | Collection name |
|---|---|
| `trades` | `trades` |
| `tradeseries` | `tradeseries` |
| `backtests` | `backtests` |
| `tradeplans` | `tradeplans` |
| `ind_meta` | `indicators_meta` |
| `ind_dps` | `indicators_datapoints` |
| `images` | `images` (GridFS bucket root) |

**`COLL_PATTERNS` dict** — dynamically named managed collections
matched by compiled regex:

| Key | Pattern | Matches |
|---|---|---|
| `candles` | `candles_.+_.+` | Any candles collection |
| `events` | `events_.+` | Any events collection |
| `gridfs` | `^images\.(files\|chunks)$` | GridFS bucket collections |

### Custom Document Functions

These functions in `dhstore.py` allow caller-defined collections (not
in `COLLECTIONS` or `COLL_PATTERNS`) to store arbitrary structured
documents.  All functions guard against accidental writes to managed
collections and raise `ValueError` if the target collection is managed.

- **`store_custom_documents(collection, documents)`** — upserts a list
  of documents, one per dict.  Each document must have a non-blank
  `name` field.  `doc_id` is auto-assigned as `"{name}_{new_uuid()}"` if
  not pre-set by the caller.  Non-dict objects are coerced via
  `vars()`/`dict()` before raising on failure.  Documents must be
  JSON-serializable.
- **`delete_custom_documents_by_field(collection, field, value)`** —
  deletes all custom documents where `field == value`.
- **`get_custom_documents_by_field(collection, field, value)`** —
  returns matching custom documents as a list of dicts.
- **`get_all_custom_documents(collection)`** — returns all documents
  in the collection as a list of dicts.

### StoredImage and GridFS

**`StoredImage`** (defined in `dhtypes.py`) is a metadata-only object
for images stored in MongoDB GridFS:

| Field | Description |
|---|---|
| `image_id` | Unique ID, auto-generated as `"{name}_{new_uuid()}"` |
| `image_id_short` | Last 8 hex chars of `image_id`; use in log messages |
| `name` | Required non-blank label; raises `ValueError` if blank or None |
| `created_epoch` | Integer epoch timestamp at creation; not used in `image_id` |

Binary data is not held in the object.  Use `load_data()` to retrieve
bytes from GridFS after construction.

GridFS storage functions in `dhstore.py`:

- **`store_images(images, bucket)`** — stores a list of
  `(StoredImage, bytes)` tuples to GridFS.
  `image_id` must already be set on each object before calling.
  Returns a list of `image_id` strings.
- **`store_image_from_path(path, name, ...)`** — convenience wrapper;
  reads binary from a file path, constructs a `StoredImage`, and
  stores it in one call.
- **`get_image_data(image_id, bucket)`** — returns raw bytes from
  GridFS for the given `image_id`.
- **`get_image_by_id(image_id, bucket)`** — returns an `ImageResponse`
  with `data`, `content_type`, `filename`, and `metadata` fields in a
  single round-trip.
- **`get_images_metadata_by_field(field, value, bucket)`** — returns
  matching GridFS metadata documents (no binary data).
- **`delete_images_by_field(field, value, bucket)`** — deletes all
  GridFS images where `metadata.<field> == value`.
- **`delete_images_by_image_id(image_ids, bucket)`** — deletes
  specific GridFS images by a list of `image_id` strings.

A unique index on `metadata.image_id` in `images.files` prevents
duplicate image entries.

### Integrity Checks

`dhstore.py` exposes integrity check functions used to audit stored
data quality:

- **`check_integrity_no_nameless_objects()`** — finds documents
  without a valid `name` field across all managed collections.
- **`check_integrity_unique_fields(collection, fields, query)`** —
  checks that the named fields in a collection have non-empty, unique
  values.  Returns a dict with `status`, `total_docs`, and a `fields`
  sub-dict keyed by field name; each field entry contains
  `missing_count`, `missing_samples`, `duplicate_count`, and
  `duplicate_samples`.  An optional `query` dict may be supplied to
  restrict which documents are checked.
- **`check_integrity_orphaned_images(reference_map)`** — finds GridFS
  images whose `image_id` is not referenced by any document in the
  caller-supplied map of `{collection: [field_paths]}`.  Uses
  `distinct()` per field path so indexes are leveraged rather than
  loading full documents.

### Checking Your MongoDB Setup

Use `verify_database_configuration()` to check that the MongoDB databases,
collections, and indexes your application needs already exist. A collection
holds documents; an index supports lookups and can enforce unique values.
This is a **read-only check**: it does not create storage, repair indexes, or
change stored data.

**This API is MongoDB-specific, not storage-system agnostic.** Although it is
available through the storage layer (`dhstore.py`), that layer currently
delegates the check to the MongoDB implementation (`dhmongo.py`). The JSON
format below describes MongoDB collections and indexes; it is not a portable
configuration format for other database systems.

#### 1. Select the connection before importing dhtrader

Configure `MONGO_CONN` with your MongoDB connection URI and `MONGO_DB` with your
application's default database name. Set these in the process environment or
your application's `mongo.env` file **before importing dhtrader**. Importing
the library establishes its Mongo client; editing the environment afterward
does not retarget that existing client.

The verification function uses this same connection. It checks database names
from the supplied configuration without changing the application's default
database.

#### 2. Define and prepare the storage your application needs

Your application owns its database names, index definitions, and setup process;
dhtrader does not supply a universal configuration file. Keep the expected
layout in a JSON file, ideally the same definition used by your application's
database-setup script. For example, save this as `mongo_configuration.json`:

```json
{
  "schema_version": 1,
  "databases": {
    "example": {
      "collections": {
        "records": {
          "indexes": [
            {"keys": [["owner", 1], ["sequence", -1]], "unique": true}
          ]
        }
      }
    }
  }
}
```

This example expects a database named `example`, a collection named `records`,
and a compound index on `owner` followed by `sequence`. Direction `1` means
ascending and `-1` means descending. With `unique: true`, each combination of
those two field values must be unique; either field may repeat on its own.

Saving this file does **not** create that database or index. Prepare them with
your application's setup script or MongoDB administration tools first.

#### 3. Run the check and inspect the result

```python
import json
from pathlib import Path

from dhtrader import verify_database_configuration

with Path("mongo_configuration.json").open() as source:
    configuration = json.load(source)

report = verify_database_configuration(configuration)
if report["ok"]:
    print("MongoDB setup matches the configuration.")
else:
    print("MongoDB setup needs attention:")
    for issue in report["issues"]:
        print(issue)
```

The example reads the JSON file from your current working directory. By default,
the check inspects every database listed in that file. To check only one,
use `verify_database_configuration(configuration, database="example")`.
That name must appear in the configuration.

The result contains `ok` (whether all requirements match) and `issues` (a list
of problems). Each issue identifies its `database`, `collection`, ordered index
`keys`, and `reason`. Collection or keys are `None` when not applicable.

| Reason | What to check |
|---|---|
| `missing_database` | The configured database is absent or not visible to this connection. |
| `missing_collection` | A required collection is absent from its database. |
| `missing_index` | No index has the required fields in the required order. |
| `index_options_mismatch` | An index has the right fields/order, but incompatible options such as uniqueness. |

An absent database or collection is reported once, rather than repeating each
missing child. Extra collections/indexes are allowed, and index names do not
need to match.

Your application decides whether to stop when `ok` is false. The library does
not automatically block other storage calls, repair problems, or retry work.
Run the check explicitly during startup or when needed; importing dhtrader
does not perform this verification. A successful check describes the setup at
that moment, not a guarantee against later administrative changes.

#### Configuration limits and access errors

- Schema version 1 supports ordinary ascending/descending indexes, with an
  explicit `unique` boolean and ordered `[field, direction]` pairs.
- Every configured database needs at least one collection. A collection's
  `indexes` array may be empty.
- Unknown fields, duplicate index-key definitions, and numeric-only field
  names are rejected. Numeric-only names could be reordered by JavaScript
  setup tools.
- Sparse, partial, TTL (automatic expiry), collation, hidden, and other special
  index options do not satisfy an ordinary configured index.
- Invalid configuration raises `ValueError` before any MongoDB requests.
  Connection and permission errors also raise exceptions; they are not
  converted into successful reports.
- The MongoDB account needs permission to enumerate the configured databases
  and collections and list their indexes. Restricted visibility can make an
  existing database appear missing; confirm access before assuming it needs
  to be created.

## For AI Agents

**Important:** AI agents working on this project MUST read
[AGENTS.md](AGENTS.md) before making any changes. This file contains
essential guidelines for:

- Python file editing requirements
- Pre-edit and post-edit verification checklists
- Line length and whitespace validation procedures
- Tools and resources for verification

See [AGENTS.md](AGENTS.md) for complete agent guidelines.

## Code Quality Standards and Automated Edits

All code changes - whether by developers or automated agents - must adhere to:

- **Maximum Line Length:** 79 characters in Python files (PEP 8)
- **No Trailing Whitespace:** Python files must be clean
- **Unix Line Endings:** LF only (not CRLF)

**For Complete Details:**
See [CODING_STANDARDS.md](CODING_STANDARDS.md) for canonical reference
including validation commands, tool configuration, and examples.

**Validation Tools:**

Use validation commands from
[CODING_STANDARDS.md](CODING_STANDARDS.md#validation-command-reference):

```bash
./validate-file-quality.sh <filename>  # Automated validation
flake8 .                                # Lint all files
```

See [CODING_STANDARDS.md](CODING_STANDARDS.md) for complete documentation.

## Setup

### Git Hooks
To ensure code quality standards are enforced on commits:
```bash
cd /path/to/dhtrader
./bin/create-hook-symlinks.sh
```

This creates symlinks in the git hooks directory pointing to the scripts
in `githooks/`. The pre-commit hook validates:
- Line length (79 chars max)
- No trailing whitespace
- Other code quality standards

### Virtual Environment

While not strictly necessary when used as a library, it is sometimes helpful to run adhoc scripts and python console commands from this repository.  These steps will build a compatible python virtual environment for such purposes.

**Usage**
If Initial Setup was already done, activate using:
`. env/bin/activate`
and deactivate using:
`deactivate`

**Initial Setup**
1. Create the virutal environment in the root of the local repo clone
`python3 -m venv env`

2. Activate the virtual environment
`. env/bin/activate`

3. Upgrade pip and install requirements.txt
```
python3 -m pip install --upgrade pip
pip install -r requirements.txt
```

4. Confirm no broken requirements
`pip check`
should return:
No broken requirements found.

# ES Futures Market Era Analysis

For historical backtesting accuracy, the `MARKET_ERAS` configuration, defined in `dhtypes.py`, specifies trading hours for different periods of ES futures history (2008-present). The configuration was derived from detailed analysis of 6.3M+ candle records and validated to 99.99% accuracy.

See [ES Market Era Analysis](docs/es_market_era_analysis.md) for:
- Complete trading hour specifications across 4 historical periods
- Methodology for identifying era boundaries from actual candle data
- Validation results confirming configuration accuracy
- Technical details on how trading hours evolved from 2008-2026

# Handling nested objects/subclasses storage and retrieval

To keep logic within each class's own methods where it belongs, I'm writing storage functions in dhstore.py and dhmongo.py so that they only store information pertient to the object in question itself and not any of it's nested objects.  Nested objects might include a Chart(), a list of Trades(), or a number of other items along these lines.  This was decided after several such functions were already created and may not be fully backported.  See Backtest and TradeSeries for examples of this implementation

Each class should then determine, within it's own methods exclusively, whether and how to go about looping through the nested objects it contains when performing storage and retrieval tasks and then call the related functions or methods on each of the nested objects where appropriate.  If there is a need for them to link to the parent they should each include an attribute with a unique id shared by the parent.

Exception for TradePlan: the TradePlan record may include lightweight
metadata about nested items plus `trade_ids`/`tradeseries_ids` references as
part of the TradePlan document itself for retrieval context.
It does not embed full nested object payloads there; full Trade and
TradeSeries objects remain stored/retrieved through their own collections.

Each class with nested objects should include a .to_clean_dict() method which will return a python dictionary of it's storable attributes while stripping any nested objects.  This way the storage function can receive the entire object and call back to it to get just the storable parts to pass to dhmongo as a json object while remaining compatible with future storage systems that may need something different.

# Events suggestions
The following events are what I find helpful to load into my central storage for use in backtesting analysis.  There is no hard fast rule here, it's up to the user to determine what events are relevant for their testing.  That being said, gap analsyis and similar functions will throw errors if market closures are not included.

* Holidays (Closed)
* FOMC Rate Announcements - mark the whole day or just relevant hours?
* OPEX - mark the whole day
* ES Contract rollover periods - TBD, which days to include?
* Big data drops with potential to move the market
  * CPI
  * PPI
  * NFP
  * What else?
* Periods of high volatility due to unexpected news

# Analyzing with trailing drawdown effects in prop firm accounts
The library makes some efforts to assist traders in factoring trailing drawdowns into their analysis process to simulate how real world trading would play out in a prop firm account.  This is primarily handled through on-demand methods on the Trade and TradeSeries object types so that it can be applied without recalculating the backtest.  For traders working in real cash accounts these methods can simply be ignored as they are not applied as defaults.

Trailing drawdowns are assumed to work like Apex Trader Funding style as that's what I'm using.  The details are listed out at https://support.apextraderfunding.com/hc/en-us/articles/4408610260507-How-Does-the-Trailing-Threshold-Work-Master-Course but I will attempt to summarize in my own words here:

* Each account has a maximum trailing threshold amount, often referred to as a "drawdown distance", which determines the account balance dollar amount at which your account will be liquidated and closed if you lose too much money trading.
* This drawdown distance is udpated live as trades are running, tracking the difference between the entry price and the current price until it is locked in by the trade being closed.  It can never exceed the maximum trailing threshold amount, nor can it go below zero as this will liquidate the account.
* When a profitable trade would take the drawdown distance above the maximum trailing threshold amount, the level at which you will be liquidated increases by the same amount as the "overreach" even if you do not close the trade until it pulls back.

Note that this library only factors in eval style drawdowns that never stop trailing, and it does not necessarily trigger liquidation out of the box, allowing the user to simply work with the outputs of the applicable methods to decide when and whether to consider it a failed Trade or TradeSeries in the context of their specific strategy ideas.
