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
