# Code Standards and Enforcement

This document is the canonical prose reference for coding standards in this
repository.

## Scope Matrix

- **Line length (79 max):** Python files only (`*.py`)
- **Trailing whitespace (spaces or tabs at line end):** all code and config
    files
- **Markdown files (`*.md`):** trailing whitespace is allowed when needed for
    formatting/rendering
- **Blank lines:** must not contain whitespace characters
- **Line endings:** LF only for all text files

## Enforcement Tools

### 1. Flake8 (`.flake8`)

Primary Python linting and line-length enforcement.

```bash
flake8 .
```

Key enforced Python rules include line length and common whitespace issues.

### 2. EditorConfig (`.editorconfig`)

Editor-level consistency:

- UTF-8
- LF line endings
- final newline
- trailing whitespace trimming (markdown exempted)
- indentation conventions

### 3. File Validator (`validate-file-quality.sh`)

Per-file validator used for edited files:

- Python files: line length + trailing whitespace + line endings
- Markdown files: line endings only
- Other non-Python text files: trailing whitespace + line endings

```bash
./validate-file-quality.sh <file>
```

Optional auto-fix mode:

```bash
./validate-file-quality.sh --fix <file>
```

`--fix` removes trailing spaces/tabs and converts CRLF to LF before
re-validating.

### 4. Pre-commit Hook (`githooks/pre-commit`)

Runs repository checks before commit:

- `flake8 .` for Python linting

## Validation Command Reference

### Python line-length check (79 max)

```bash
awk 'length > 79 {print NR": "length" chars: "$0}' <file.py>
```

### Trailing whitespace check (spaces and tabs)

```bash
grep -nE '[[:blank:]]$' <file>
```

### LF line ending check

```bash
file <file> | grep -q "CRLF" && echo "CRLF found"
```

### Remove trailing spaces/tabs

```bash
sed -i 's/[[:blank:]]\+$//' <file>
```

## Making Changes

1. Keep Python lines at 79 chars or less.
2. Keep all edited code/config/docs files free of trailing spaces/tabs.
3. Keep LF line endings.
4. Run:
   - `flake8 .` when Python files are edited
   - `./validate-file-quality.sh <file>` for each edited text file

## Configuration Files Reference

- `.flake8`: Python linting and line-length policy
- `.editorconfig`: editor behavior for whitespace, endings, indentation
- `setup.cfg`: pytest and coverage settings
- `githooks/pre-commit`: commit-time quality checks
- `validate-file-quality.sh`: file-level validation/fix helper

## Logging and Comments

### Log levels

Use these levels consistently in all Python code:

- **`log.critical`** — immediately before every `raise`
  statement, whether the exception exits the script or
  propagates to a caller.  Include the function name and
  the value that triggered the rejection so the message
  is self-contained without needing a stack trace.  Always
  visible regardless of log level configuration.
  Example:
  ```python
  log.critical(
      f"retrieve_backtests: cache missing for {bt_id!r}"
  )
  raise ValueError(f"Missing trades cache for {bt_id}")
  ```

- **`log.error`** — non-raising error conditions: unexpected
  states that are logged as errors but allow the function
  to continue (e.g., a write concern warning that returns
  an empty result instead of raising).  Clearly visible in
  all standard log configurations.

- **`log.info` / `log_say`** — key milestones: function
  entry with primary arguments, completion with result
  count or ID, and non-error state changes.  `log_say()`
  writes to both console and log file; `log.info()` goes
  to log file only.  Use `log_say` for messages a human
  watching the console should see; use `log.info` for
  file-only audit trail entries.

- **`log.debug`** — high-volume or low-value detail reserved
  for troubleshooting: per-item loop messages, timing
  values (via `log_timer()`), and test-mode flag overrides.
  Must not appear during normal operation.

### Timing pattern

Capture a start time with `tpc()` immediately before a
block, then emit a `log_timer()` call at `log.debug` level
after:

```python
time_step = tpc()
# ... work ...
log_timer("label", tpc() - time_step,
          refresh_id=..., run_id=...)
```

### Public function entry and exit

Every public store, retrieve, delete, list, and review
function must log at entry with its key arguments and at
exit with the result count or ID:

```python
log.info(
    f"store_custom_documents: collection={collection!r}, "
    f"count={len(documents)}"
)
# ... work ...
log.info(
    f"store_custom_documents: stored {len(results)} docs "
    f"to {collection!r}"
)
```

### Inline comments

- Required before every non-obvious block.
- Must describe intent or reason, **not restate what the
  code does**.  "Filter to profitable series" is not
  useful; "Exclude series below profit threshold before
  merging to avoid diluting the merged result" is.
- Keep comments accurate — update or remove them when the
  code they describe changes.

### No logging in unit tests

## Class Serialization

### `to_clean_dict()` pattern

Use `dict(self.__dict__)` as the base rather than enumerating
every attribute by hand.  This keeps the method DRY — new
attributes added to `__init__` are automatically present.

```python
def to_clean_dict(self) -> dict:
    working = dict(self.__dict__)
    # Add per-field conversions here only when needed,
    # e.g. datetime → ISO string.
    return working
```

Do **not** return a hardcoded dict that lists every attribute.
If a new attribute needs custom serialization, add an inline
conversion immediately after `working = dict(self.__dict__)`.

### `from_dict()` pattern

Use `d["key"]` (direct subscript access) for every field — do **not**
use `.get()` with fallback defaults.  If a key is missing the dict,
Python raises `KeyError` immediately, which is the desired behavior:
objects must be updated when the schema changes; silent defaults hide
staleness.

```python
@classmethod
def from_dict(cls, d: dict) -> "MyClass":
    return cls(
        my_id=d["my_id"],
        name=d["name"],
        notes=d["notes"],
        # ...
    )
```

When a stored document is missing a key it means the schema has drifted
and the stored data must be migrated before it can be loaded.  Make the
error visible rather than masking it with defaults.

### Notes field

Classes that need a `notes` field should follow the
`TradePlan`/`AnalyzeBacktestResult` pattern:

- Declare `notes=None` in `__init__`
- Normalize via `self._normalize_str_list(notes, "notes")`
- Provide `_normalize_str_list(self, values, field_name)` as
  an instance method: `None → []`; iterates elements and coerces
  each to `str()`; raises `TypeError` if not iterable.

## Unique ID Fields

Use this standard for any data class that is **not guaranteed to have a
naturally unique combination of fields** (e.g. datetime + name) that
could serve as a composite key without risk of collision.  When two
instances of a class can be created with identical field values — or when
two instances created in the same second would appear identical — a
uuid4-based ID is required.  The goal is collision-free IDs that remain
short enough for readable log output.

### Generation pattern

Use `uuid.uuid4()` with hyphens stripped.  Always assign the bare uuid
to `self.uuid` first, then derive the class-specific id and short id
from it:

```python
import uuid

# Pure uuid (e.g. Trade.trade_id IS the uuid)
self.uuid = str(uuid.uuid4()).replace("-", "")
self.trade_id = self.uuid
self.trade_id_short = self.uuid[-8:]

# Prefix + uuid (e.g. StoredImage.image_id, TradePlan.tp_id suffix)
self.uuid = str(uuid.uuid4()).replace("-", "")
self.image_id = f"{self.name}_{self.uuid}"
self.image_id_short = self.uuid[-8:]
```

When loading from storage (the class-specific id is provided), derive
`self.uuid` from the stored id instead of generating a new one:

```python
# Trade: trade_id IS the uuid
self.uuid = provided_trade_id

# StoredImage: uuid is the 32-char hex suffix after the last '_'
parts = image_id.rsplit("_", 1)
self.uuid = parts[1] if len(parts) == 2 and len(parts[1]) == 32 else image_id[-32:]

# TradePlan: uuid is the last 32-char hex segment of tp_id
parts = tp_id.rsplit("_", 1)
self.uuid = parts[1]  # validated as all-hex before accepting
```

Rules:
- **Generate `self.uuid` first**, then derive all other id fields.
- **No hyphens** in the uuid portion — use `.replace("-", "")`.
- **Prefix** is optional and class-specific; use a meaningful string
  that identifies context (name, ts_id, etc.).
- **Never** use `int(datetime.now().timestamp())` (epoch seconds) as the
  sole uniqueness guarantee; two objects created in the same second
  collide.
- **Include `uuid` in `to_clean_dict()`** for classes with explicit
  serialization dicts.  Classes that use `deepcopy(self.__dict__)` for
  serialization include it automatically.

### Short-form attribute (`*_id_short`)

Every class that has a `*_id` field also exposes `*_id_short`.  The
short form **preserves all human-readable prefix segments** of the full
id, but replaces the 32-char uuid suffix with only its last 8 hex chars.
This keeps log messages identifiable while remaining concise.

```python
# Trade: trade_id IS the uuid (no prefix) — short form is just last 8
self.uuid = str(uuid.uuid4()).replace("-", "")
self.trade_id = self.uuid
self.trade_id_short = self.uuid[-8:]

# StoredImage: prefix is the name — preserved in full
self.uuid = str(uuid.uuid4()).replace("-", "")
self.image_id = f"{self.name}_{self.uuid}"
self.image_id_short = f"{self.name}_{self.uuid[-8:]}"

# TradePlan: prefix is ts_id + id_slug + cfg_label — preserved in full
prefix = f"{ts.ts_id}_{self.id_slug}_{self.cfg_label}"
self.tp_id = f"{prefix}_{self.uuid}"
self.tp_id_short = f"{prefix}_{self.uuid[-8:]}"
```

`*_id_short` is never simply `full_id[-8:]` unless the full id has no
prefix (i.e. it IS the uuid).

### Logging convention

**Use `*_id_short` in all log messages** (info, debug, critical, error).
Full IDs are 32+ characters and inflate log lines unhelpfully.  Full IDs
are stored in the object and in the database; the short form is for
human-readable audit trail entries:

```python
log.info(
    f"store_trades: stored trade_id_short={t.trade_id_short!r}"
)
```

### Serialization

Include both the full `*_id` and `*_id_short` in `to_clean_dict()`.
When reconstructing from a stored dict via `from_dict()`, load the
stored `*_id`; then recompute `*_id_short = stored_id[-8:]` rather than
loading it from the dict (it is always derivable).

### Timestamps alongside IDs

Every class with a `*_id` field must also carry:

- `created_epoch` — integer Unix timestamp (`int(datetime.now().timestamp())`)
- `created_dt` — ISO datetime string derived from `created_epoch`

If the class already has a `created_dt`, derive `created_epoch` from it
(`int(datetime.fromisoformat(created_dt).timestamp())`).  Store both in
`to_clean_dict()`.

### `time_tag()` and session identifiers

`time_tag(suffix)` returns a datetime string in the format
`YYYYMMDD-<epoch_seconds>-<suffix>`.  The `suffix` argument must be the
last 8 hex characters of the uuid field belonging to the object or
session the tag represents.  This ensures the suffix in the time_tag
matches `*_id_short` on the object that owns it.

**Top-level sessions** (no parent object): generate a uuid locally and
pass its last 8 chars:

```python
run_id = time_tag(str(uuid.uuid4()).replace("-", "")[-8:])
```

**Nested contexts** (e.g. a window analysis created inside a refresh
cycle): pass the parent object's `*_id[-8:]`:

```python
window_run_id = time_tag(refresh_id[-8:])  # links to parent cycle
```

`time_tag()` never generates its own uuid — the caller is always
responsible for providing the suffix.
