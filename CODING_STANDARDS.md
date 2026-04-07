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
