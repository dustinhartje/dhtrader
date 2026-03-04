# AI Agent Implementation Plan: Circular Import Elimination

**Target**: `dhtrader` Python module
**Agent Environment**: GitHub Copilot workspace (cloud-based)
**Date**: March 3, 2026
**Strategy Source**: CIRCULAR_IMPORTS_STRATEGY.md v3.2

---

## OBJECTIVE

Eliminate circular imports in the `dhtrader` module by restructuring to a layered architecture:
- Create `dhtypes.py` with pure domain classes (no persistence)
- Eliminate `dhcharts.py` and `dhtrades.py` entirely
- Move all persistence to functional API in `dhstore.py`
- Maintain PEP 8 compliance (all imports at module top)
- Update all callers in `dhtrader` and `backtesting` repositories

---

## SCOPE

### Repositories
1. **Primary**: `dhtrader` (main module being refactored)
2. **Dependent**: `backtesting` (only consumer, must be migrated simultaneously)

### Files to Create
- `dhtypes.py` (NEW: ~3000 lines, all domain classes)

### Files to Eliminate
- `dhcharts.py` (DELETE: classes move to dhtypes.py)
- `dhtrades.py` (DELETE: classes move to dhtypes.py)

### Files to Refactor
- `dhstore.py` (UPDATE: change imports, add functional storage API)
- `dhutil.py` (UPDATE: change imports, receive utility functions)
- `__init__.py` (UPDATE: publish final API)
- All test files in `tests/` (UPDATE: change imports)
- All Python files in `backtesting` repo (UPDATE: change imports and call patterns)

### Files Unchanged
- `dhcommon.py` (pure utilities, no dependencies)
- `dhmongo.py` (may need type hint imports from dhtypes)

---

## CONSTRAINTS

### Critical Rules
1. **PEP 8 Compliance**: All imports at module top (no lazy imports in functions/methods)
2. **No Backward Compatibility**: Migrate all callers directly to final API
3. **No Circular Imports**: `dhtypes.py` imports only from stdlib + `dhcommon`
4. **Functional Storage API**: Replace `obj.store()` with `store_<type>(obj)`
5. **Two-Repo Coordination**: Both `dhtrader` and `backtesting` must be updated together

### Git Workflow Requirements
- Create feature branches in BOTH repositories (e.g., `refactor/eliminate-circular-imports`)
- Create pull requests in both repos but DO NOT MERGE until all work is complete
- Each commit in `dhtrader` requires a parallel synchronization commit in `backtesting`
- Commit messages should reference phase number and repository

### Virtual Environment Requirement
- Use the backtesting virtual environment located at `backtesting/env/`
- Activate this environment before running ANY commands in either repository
- If the environment cannot be directly utilized, create your own with identical configuration
- All Python execution must use this environment (specify full path or activate first)

### Unit Test Update Rules
- **Existing test files** (`tests/test_*.py` in both repos): Update imports and storage calls only
  - Do NOT modify test logic, assertions, or test structure
  - Do NOT add new tests to existing test files
  - These tests WILL FAIL during refactoring; document failures after all phases complete (see Phase 10)
- **Temporary test files**: May be created with any content needed for validation during refactoring
  - These can be deleted once validation completes
  - Use naming convention like `tests/test_temp_<feature>.py` for clarity

### Commit Synchronization Pattern
For each commit made in `dhtrader`:
1. Update all imports and function/method calls in `dhtrader` files being modified
2. Make commit in `dhtrader` with clear message: `refactor: <description> (phase N)`
3. Navigate to `backtesting` repository
4. Update all imports and function/method calls to `dhtrader` in `backtesting` files
5. Make parallel commit in `backtesting` with message: `refactor: update dhtrader imports (phase N sync)`
6. Return to `dhtrader` for next phase

### Python Standards
- Line length: 79 characters max
- Line endings: LF only (Unix)
- No trailing whitespace
- PEP 484 type hints throughout

---

## ARCHITECTURE_TRANSITION

### Current State (Circular)
```
dhcharts.py ←→ dhstore.py
    ↑              ↓
    └── dhtrades.py ──┘

dhcharts.py imports: get_symbol_by_ticker, get_candles, store_candle, get_events, store_event, get_indicator_datapoints, store_indicator, store_indicator_datapoints (from dhstore)
dhstore.py imports: Candle, Event, IndicatorDataPoint, Symbol, IndicatorSMA, IndicatorEMA (from dhcharts)
dhstore.py imports: Trade, TradeSeries (from dhtrades)
dhtrades.py imports: Candle, Chart (from dhcharts)
dhtrades.py imports: storage functions (from dhstore)
```

### Target State (Layered)
```
stdlib + dhcommon
    ↓
dhtypes.py (pure domain classes)
    ↓
dhstore.py (persistence functions)
    ↓
dhutil.py + tests/ (usage layer)
```

**Dependency Rules**:
- `dhtypes.py`: imports ONLY from `dhcommon` + stdlib
- `dhstore.py`: imports from `dhtypes` + `dhmongo` + `dhcommon`
- `dhutil.py`: imports from `dhtypes` + `dhstore` + `dhcommon`
- `tests/`: imports from `dhtypes` + `dhstore`

---

## CLASSES_INVENTORY

### From dhcharts.py → dhtypes.py
- `Candle` (candlestick data model)
- `Event` (market events)
- `Symbol` (trading symbols)
- `IndicatorDataPoint` (indicator data)
- `Indicator` (base class)
- `IndicatorSMA` (simple moving average)
- `IndicatorEMA` (exponential moving average)
- `Chart` (chart analysis)
- `Day` (daily analysis)

### From dhtrades.py → dhtypes.py
- `Trade` (individual trade)
- `TradeSeries` (series of trades)
- `Backtest` (backtest results)

### Non-Class Content to Relocate

**From dhcharts.py**:
- Constants: `CANDLE_TIMEFRAMES`, `BEGINNING_OF_TIME`, `MARKET_ERAS` → move to `dhcommon.py`
- Logger: `log = logging.getLogger("dhcharts")` → keep in destination module
- Function: `bot()` → move to `dhcommon.py` or inline/remove

**From dhtrades.py**:
- Logger: `log = logging.getLogger("dhtrades")` → keep in destination module
- No top-level functions or constants requiring relocation

---

## ENVIRONMENT_SETUP

**Before starting any implementation work, configure the environment using committed configuration files.**

**CRITICAL: Virtual Environment Activation**

The virtual environment is created at `backtesting/env/` and must remain ACTIVE for all commands in both repositories. When you activate the venv with `source /path/to/backtesting/env/bin/activate`, it modifies your shell's PATH and PYTHONPATH. This remains active when you `cd` to other directories (including `/path/to/dhtrader`), so you can navigate between repos while keeping the same venv active.

**Key Points**:
- Activation: `source /path/to/backtesting/env/bin/activate`
- Remains active across `cd` commands to `/path/to/dhtrader`
- Deactivation: `deactivate` (only when you want to exit)
- Verification: `which python3` always shows `/path/to/backtesting/env/bin/python3` while venv is active
- All Python execution in BOTH repos must use this activated venv

### 1. Python Version Requirement

**Required: Python 3.8.10 (exact version)**

Verify your Python version:
```bash
python3 --version  # Should output: Python 3.8.10

# If you have multiple Python versions installed, specify explicitly
python3.8 --version  # Should output: Python 3.8.10
```

**If Python 3.8.10 is not available**:
- Install or switch to Python 3.8.10 specifically
- Virtual environment will inherit this version
- Verify before creating venv: `python3 -c "import sys; assert sys.version_info == (3, 8, 10), f'Wrong version: {sys.version_info}'"`

### 2. Create Virtual Environment from backtesting/requirements.txt

The backtesting repository contains the canonical requirements for this project.

```bash
# Navigate to backtesting directory
cd /path/to/backtesting

# Create virtual environment
python3 -m venv env

# Activate the virtual environment
source env/bin/activate  # On macOS/Linux
# OR
env\Scripts\activate     # On Windows

# Verify activation (should show path to backtesting/env/bin/python)
which python3  # or 'where python' on Windows
python3 --version

# Upgrade pip to latest for better dependency resolution
python3 -m pip install --upgrade pip setuptools wheel

# Install all project dependencies from requirements.txt
pip install -r requirements.txt

# Verify critical packages installed
python3 -c "import pytest; import flake8; import pymongo; print('✓ Core packages installed')"
```

### 3. Configure Python Path for Both Repositories (Within Virtual Environment)

Both repositories must be accessible from the Python path. The project uses `.pth` files for this purpose.

```bash
# Ensure virtual environment is still activated
which python3  # Should show: .../backtesting/env/bin/python3

# Add dhtrader and backtesting to Python path via environment variable
# This makes imports work from either repository
export PYTHONPATH=/path/to/dhtrader:/path/to/backtesting:$PYTHONPATH

# Verify the export (optional)
echo "Current PYTHONPATH:" $PYTHONPATH

# Verify Python can find both modules
python3 <<'VERIFY'
import sys
print("sys.path includes:")
for p in sys.path:
    if 'dhtrader' in p or 'backtesting' in p:
        print(f"  {p}")
VERIFY

# Note: Add this export to ~/.bashrc or ~/.zshrc to persist across sessions:
# export PYTHONPATH=/path/to/dhtrader:/path/to/backtesting:$PYTHONPATH
```

### 4. Install and Configure Pre-Commit Hooks

Pre-commit hooks enforce code quality standards before commits. Configuration files are committed to each repository.

**dhtrader pre-commit hook** (`dhtrader/githooks/pre-commit`):
- Runs flake8 linting with extended-ignore F401,E402
- Excludes v1/* directory
- Enforces on every commit

**backtesting pre-commit hook** (`backtesting/githooks/pre-commit`):
- Runs flake8 linting with extended-ignore F401,E402
- Excludes: v1/*, *deleteme*, *DELETEME*, env/*, reports/*, logs/*, visuals/*, downloads/*, troubleshooting/*
- Enforces on every commit

**Installation**:
```bash
# Install pre-commit hook in dhtrader
cd /path/to/dhtrader
git config core.hooksPath githooks
chmod +x githooks/pre-commit
echo "✓ dhtrader pre-commit hook installed"

# Install pre-commit hook in backtesting
cd /path/to/backtesting
git config core.hooksPath githooks
chmod +x githooks/pre-commit
echo "✓ backtesting pre-commit hook installed"

# Verify hooks are installed (should show hook script names)
cd /path/to/dhtrader
ls -la .git/hooks/  # May show symlinks to githooks/

cd /path/to/backtesting
ls -la .git/hooks/  # May show symlinks to githooks/
```

### 5. Understand Code Quality Standards

Standards are defined across multiple configuration files:

**Flake8 Configuration** (`dhtrader/.flake8` and matched in backtesting/githooks/pre-commit):
- Maximum line length: **79 characters** (PEP 8)
- Extended ignores: F401 (unused imports), E402 (module imports not at top)
- Enforces: E501 (line too long), W291 (trailing whitespace), W293 (blank line whitespace), W391 (blank line at EOF)
- Reports statistics and shows source code for errors

**EditorConfig** (`dhtrader/.editorconfig`):
- Character encoding: UTF-8
- Line endings: LF (Unix) - **CRITICAL for commits**
- Trailing whitespace: Trimmed on all lines
- Python indent: 4 spaces, max line 79 characters
- Blank lines: Have trailing whitespace removed
- Final newline: Always inserted

**Validate Tool** (`dhtrader/validate-file-quality.sh`):
- Checks line length (79 char max)
- Checks trailing whitespace
- Checks line endings (LF required)
- Usage: `./validate-file-quality.sh <filename>`

Run validation on modified files:
```bash
cd /path/to/dhtrader
./validate-file-quality.sh dhtypes.py
./validate-file-quality.sh dhstore.py
./validate-file-quality.sh dhutil.py
./validate-file-quality.sh __init__.py
```

### 6. Configure Pytest

Pytest configuration is defined in repository config files:

**dhtrader configuration** (`dhtrader/setup.cfg` `[tool:pytest]` section):
- Test paths: `tests/`
- Markers:
  - `@pytest.mark.storage`: tests that interact with storage
  - `@pytest.mark.slow`: tests that run slowly
  - `@pytest.mark.historical`: tests using real market data from testdata folder

**backtesting configuration** (`backtesting/pytest.ini`):
- Test paths: `tests/`
- Same markers as dhtrader (for consistency)

**Running Tests**:
```bash
# Run all tests (fast + slow)
pytest tests/ -v

# Run only fast tests (skip @pytest.mark.slow)
pytest tests/ -v -m "not slow"

# Run only storage tests
pytest tests/ -v -m "storage"

# Run with test scripts available in repos
cd /path/to/dhtrader
./test.sh -f  # Fast tests only
./test.sh tests/test_dhtypes.py  # Specific file
./test.sh -h  # Help

cd /path/to/backtesting
./test.sh -f  # Fast tests only
```

### 7. Create Feature Branches in Both Repositories

```bash
# In dhtrader repo
cd /path/to/dhtrader
git checkout -b refactor/eliminate-circular-imports

# Verify branch created
git branch  # Should show: * refactor/eliminate-circular-imports

# In backtesting repo (sibling directory)
cd /path/to/backtesting
git checkout -b refactor/eliminate-circular-imports

# Verify branch created
git branch  # Should show: * refactor/eliminate-circular-imports
```

### 8. Verify Complete Environment Setup

Run comprehensive verification:

```bash
# Verify virtual environment is activated
which python3  # Should show path containing backtesting/env/bin/python3

python3 <<'EOF'
import sys
import pytest
import flake8
import pymongo
import pandas

print("Python version:", sys.version)
print("✓ Virtual environment: ACTIVE")
print("✓ pytest:", pytest.__version__)
print("✓ flake8:", flake8.__version__)
print("✓ pymongo:", pymongo.__version__)
print("✓ pandas:", pandas.__version__)
EOF

# Verify dhtrader modules are importable (using current state - before refactoring)
python3 <<'EOF'
from dhcharts import Candle, Chart
from dhtrades import Trade, TradeSeries
from dhstore import get_symbol_by_ticker
print('✓ dhtrader module imports work')
EOF

# Verify pre-commit hooks installed in both repos
cd /path/to/dhtrader
ls -la githooks/pre-commit && echo "✓ dhtrader hook: EXISTS"

cd /path/to/backtesting
ls -la githooks/pre-commit && echo "✓ backtesting hook: EXISTS"

# Verify git hook configuration  
git config core.hooksPath && echo "✓ Git hooks configured"

# Verify code quality tools work
cd /path/to/dhtrader
flake8 --version && echo "✓ flake8: READY"
./validate-file-quality.sh __init__.py && echo "✓ validate-file-quality.sh: READY"
```

**Expected Output**:
```
Python version: Python 3.8.10
✓ Virtual environment: ACTIVE
✓ pytest: 8.3.4
✓ flake8: 5.0.4
✓ pymongo: 4.7.3
✓ pandas: 2.0.3
✓ dhtrader imports work
✓ dhtrader hook: EXISTS
✓ backtesting hook: EXISTS
✓ Git hooks configured
✓ flake8: READY
✓ validate-file-quality.sh: READY
```

### 9. Environment Setup Complete

Once all verifications pass:
- ✓ Virtual environment is active and working
- ✓ All dependencies installed from backtesting/requirements.txt
- ✓ PYTHONPATH configured to include both dhtrader and backtesting
- ✓ Pre-commit hooks installed in both repositories
- ✓ Code quality tools configured and ready
- ✓ Pytest markers and test scripts accessible
- ✓ Feature branches created in both repos
- ✓ Ready to begin implementation phases

**ALL SUBSEQUENT COMMANDS IN THIS PLAN ASSUME:**
1. Virtual environment (`source /path/to/backtesting/env/bin/activate`) is ACTIVE
2. PYTHONPATH is set: `export PYTHONPATH=/path/to/dhtrader:/path/to/backtesting:$PYTHONPATH`
3. These setup steps have been completed successfully
4. Both repos are on the feature branch `refactor/eliminate-circular-imports`
5. Pre-commit hooks are installed and will run on all commits
6. When switching between repos with `cd /path/to/dhtrader` or `cd /path/to/backtesting`, the venv and PYTHONPATH remain active

---

## IMPLEMENTATION_PHASES

### PHASE 1a: Create dhtypes.py Foundation (dhtrader)

**Objective**: Extract all domain classes into new pure module

**Actions**:
1. Create empty file `dhtypes.py` in dhtrader root
2. Copy class definitions from `dhcharts.py`:
   - Classes: Candle, Event, Symbol, IndicatorDataPoint, Indicator, IndicatorSMA, IndicatorEMA, Chart, Day
   - Include: `__init__()`, analysis methods (e.g., `Candle.contains_price()`, `Symbol.market_is_open()`, `Chart.review_candles()`, `IndicatorSMA.calculate()`, `IndicatorEMA.calculate()`, `Trade.gain_loss()`)
   - Exclude: ALL `.store()` methods, ALL `.save()` methods
3. Copy class definitions from `dhtrades.py`:
   - Classes: Trade, TradeSeries, Backtest
   - Include: `__init__()`, analysis methods (e.g., `Trade.gain_loss()`)
   - Exclude: ALL persistence methods
4. Add imports at top of `dhtypes.py`:
   ```python
   from typing import List, Optional, Dict, Tuple, Any
   from datetime import datetime, timedelta
   from dhcommon import <import only required utilities>
   import logging
   ```
5. Remove all imports of `dhstore`, `dhcharts`, `dhtrades` from class code
6. Add module docstring explaining pure domain models
7. Add `__all__` export list with all class names

**Validation**:
```bash
# Test import without triggering storage layer
# Execute in dhtrader root directory
python3 -c "from dhtypes import Candle, Symbol, Trade, Chart, Backtest; print('✓ dhtypes imports successfully')"

# Verify no dhstore imports
grep -n "from dhstore import\|import dhstore" dhtypes.py
# Expected output: (empty - no matches)
```

**Commit**:
```bash
cd /path/to/dhtrader
git add dhtypes.py
git commit -m "refactor: create dhtypes module with pure domain classes (phase 1a)"
```

---

### PHASE 1b: Update backtesting Imports for dhtypes

**Objective**: Ensure backtesting can import from new dhtypes module

**Actions**:
1. In backtesting repository, find all files that import from `dhcharts` or `dhtrades`:
   ```bash
   grep -rIn "from dhtrader.dhcharts\|from dhtrader.dhtrades\|import dhcharts\|import dhtrades" . --include='*.py' --exclude-dir=env --exclude-dir=venv
   ```

2. For each file found, perform import updates BUT DO NOT UPDATE storage calls yet:
   ```python
   # OLD:
   # from dhtrader.dhcharts import Candle, Chart, Symbol
   # from dhtrader.dhtrades import Trade

   # NEW (Phase 1b - imports only):
   from dhtrader.dhtypes import Candle, Chart, Symbol, Trade
   ```

3. Test imports work:
   ```bash
   python3 -c "from dhtrader.dhtypes import Candle, Trade; print('✓ dhtypes imports from backtesting')"
   ```

**Commit**:
```bash
cd /path/to/backtesting
git add .
git commit -m "refactor: update imports to use dhtypes module (phase 1b sync)"
```

---

### PHASE 2a: Refactor dhstore.py (dhtrader)

**Objective**: Convert to functional API and eliminate circular imports

**Actions**:
1. Update imports at module top:
   ```python
   # REMOVE these imports:
   # from dhcharts import Candle, Event, IndicatorDataPoint, Symbol, IndicatorSMA, IndicatorEMA
   # from dhtrades import Trade, TradeSeries

   # ADD this import instead:
   from dhtypes import (
       Candle, Event, IndicatorDataPoint, Symbol, IndicatorSMA, IndicatorEMA,
       Trade, TradeSeries, Backtest, Chart
   )
   from typing import List, Optional, Dict
   import dhmongo
   from dhcommon import LOGGER
   ```

2. Create functional storage API for all persistence operations:
   ```python
   def store_candle(candle: Candle) -> bool:
       """Persist a Candle object to storage"""
       return dhmongo.insert_candle(candle)

   def store_candles(candles: List[Candle]) -> bool:
       """Persist multiple Candles to storage"""
       return dhmongo.insert_candles(candles)

   def store_trade(trade: Trade) -> Trade:
       """Persist a Trade object to storage"""
       result = dhmongo.insert_trade(trade.to_dict())
       return trade

   def store_backtests(backtests: List[Backtest]) -> bool:
       """Persist Backtest objects to storage"""
       return dhmongo.insert_backtests(backtests)

   def get_candles(symbol: Symbol, start_dt: datetime, end_dt: datetime, timeframe: str) -> List[Candle]:
       """Retrieve candles from storage"""
       raw_data = dhmongo.find_candles(symbol.ticker, start_dt, end_dt, timeframe)
       return [Candle(**row) for row in raw_data]

   # Continue for all storage operations...
   ```

3. Ensure all existing storage functions use `dhtypes` classes for type hints

**Validation**:
```bash
# Test dhstore imports
python3 -c "from dhstore import store_candle, store_trade, get_candles; print('✓ dhstore imports successfully')"

# Verify no circular imports
python3 -c "from dhtypes import Candle; from dhstore import store_candle; print('✓ No circular imports')"

# Check for old imports (should be empty)
grep -n "from dhcharts import\|from dhtrades import" dhstore.py
```

**Commit**:
```bash
cd /path/to/dhtrader
git add dhstore.py
git commit -m "refactor: convert dhstore to functional API using dhtypes (phase 2a)"
```

---

### PHASE 2b: Update backtesting Imports for dhstore Functions

**Objective**: Update backtesting storage calls to use new functional API

**Actions**:
1. In backtesting repository, search for all storage method calls:
   ```bash
   grep -rIn "\.store(" . --include='*.py' --exclude-dir=env --exclude-dir=venv
   ```

2. For each `obj.store()` or similar call, replace with function call:
   ```python
   # OLD:
   candle.store()
   trade.store()

   # NEW:
   from dhtrader.dhstore import store_candle, store_trade
   store_candle(candle)
   store_trade(trade)
   ```

3. Update all storage function imports to new signatures:
   ```python
   # OLD might have imported from dhstore directly
   from dhtrader.dhstore import get_candles  # signature unchanged

   # Verify the functions exist with expected signatures
   python3 -c "from dhtrader.dhstore import store_candle, store_trade, get_candles; print('✓ dhstore functions available')"
   ```

**Commit**:
```bash
cd /path/to/backtesting
git add .
git commit -m "refactor: update storage calls to use functional API (phase 2b sync)"
```

---

### PHASE 3a: Relocate Non-Class Content (dhtrader)

**Objective**: Move utility functions/constants before deleting source files

**Actions**:
1. Scan dhcharts.py for non-class content:
   ```bash
   grep -n "^def \|^[A-Z_]* = " dhcharts.py
   ```
   Expected findings:
   - `CANDLE_TIMEFRAMES` constant
   - `BEGINNING_OF_TIME` constant
   - `MARKET_ERAS` constant
   - `bot()` function
   - Logger setup

2. Relocate constants to `dhcommon.py`:
   - Move `CANDLE_TIMEFRAMES`, `BEGINNING_OF_TIME`, `MARKET_ERAS`
   - Update imports in all files that reference these

3. Relocate `bot()` function:
   - If still used: move to `dhcommon.py` or `dhutil.py`
   - If unused: remove

4. Scan dhtrades.py for non-class content:
   ```bash
   grep -n "^def \|^[A-Z_]* = " dhtrades.py
   ```
   Expected findings: Logger only (stays in dhtypes.py)

**Validation**:
```bash
# Verify constants accessible
python3 -c "from dhcommon import CANDLE_TIMEFRAMES, BEGINNING_OF_TIME, MARKET_ERAS; print('✓ Constants moved')"
```

**Commit**:
```bash
cd /path/to/dhtrader
git add dhcommon.py dhcharts.py dhtrades.py  # Update dhcommon, don't delete files yet
git commit -m "refactor: relocate constants and functions from dhcharts/dhtrades (phase 3a)"
```

---

### PHASE 3b: Verify backtesting Compatibility

**Objective**: Ensure backtesting still works with relocated constants

**Actions**:
1. If backtesting imports any constants from dhcharts or dhtrades, update to use dhcommon:
   ```bash
   grep -rIn "CANDLE_TIMEFRAMES\|BEGINNING_OF_TIME\|MARKET_ERAS\|from dhtrader.dhcharts import\|from dhtrader.dhtrades import" . --include='*.py' --exclude-dir=env
   ```

2. If constants are used, add imports:
   ```python
   from dhtrader.dhcommon import CANDLE_TIMEFRAMES, BEGINNING_OF_TIME, MARKET_ERAS
   ```

3. If no changes needed, skip this phase and note in commit that no changes were required.

**Commit** (if changes made):
```bash
cd /path/to/backtesting
git add .
git commit -m "refactor: update constant imports from dhcommon (phase 3b sync)"
# If no changes needed:
# git commit --allow-empty -m "refactor: no backtesting changes needed for constants (phase 3b)"
```

---

### PHASE 4a: Eliminate dhcharts.py and dhtrades.py (dhtrader)

**Objective**: Remove source files after all content relocated

**Actions**:
1. Verify no remaining content needed from these files
2. Delete files:
   ```bash
   # Execute in dhtrader root
   rm dhcharts.py dhtrades.py
   ```

**Validation**:
```bash
# Verify files deleted
test ! -f dhcharts.py && echo "✓ dhcharts.py deleted" || echo "✗ dhcharts.py still exists"
test ! -f dhtrades.py && echo "✓ dhtrades.py deleted" || echo "✗ dhtrades.py still exists"

# Test imports fail as expected
python3 -c "
try:
    import dhcharts
    print('✗ ERROR: dhcharts still importable')
except (ModuleNotFoundError, ImportError):
    print('✓ dhcharts eliminated')
"

python3 -c "
try:
    import dhtrades
    print('✗ ERROR: dhtrades still importable')
except (ModuleNotFoundError, ImportError):
    print('✓ dhtrades eliminated')
"

# Test new imports work
python3 -c "from dhtypes import Candle, Chart, Trade; from dhstore import store_candle; print('✓ New imports work')"
```

**Commit**:
```bash
cd /path/to/dhtrader
git rm dhcharts.py dhtrades.py  # Use git rm to track file deletion
git commit -m "refactor: eliminate dhcharts.py and dhtrades.py (phase 4a)"
```

---

### PHASE 4b: Verify backtesting File Deletions Acknowledged

**Objective**: Confirm backtesting imports have already been updated in earlier phases

**Actions**:
1. Verify no attempts to import from deleted files remain:
   ```bash
   grep -rIn "import dhcharts\|import dhtrades\|from dhcharts\|from dhtrades" . --include='*.py' --exclude-dir=env
   ```
   Expected: No results (or only in comments)

2. If legacy imports found, update them (should not happen if phases 1b/2b completed correctly)

**Commit** (if no changes needed):
```bash
cd /path/to/backtesting
git commit --allow-empty -m "refactor: dhcharts/dhtrades deletion acknowledged (phase 4b)"
```

---

### PHASE 5a: Update dhutil.py (dhtrader)

**Objective**: Update imports to use dhtypes

**Actions**:
1. Replace all imports:
   ```python
   # OLD:
   # from dhcharts import Candle, Symbol, Chart
   # from dhtrades import Trade, TradeSeries

   # NEW:
   from dhtypes import Candle, Symbol, Chart, Trade, TradeSeries
   from dhstore import store_candle, get_candles  # If needed
   from dhcommon import LOGGER
   ```

2. Update any storage calls from method-style to function-style:
   ```python
   # OLD: candle.store()
   # NEW: store_candle(candle)
   ```

**Validation**:
```bash
# Test dhutil imports successfully
python3 -c "import dhutil; print('✓ dhutil imports successfully')"

# Verify no old imports remain
grep -n "from dhcharts\|from dhtrades\|import dhcharts\|import dhtrades" dhutil.py
# Expected: (empty)
```

**Commit**:
```bash
cd /path/to/dhtrader
git add dhutil.py
git commit -m "refactor: update dhutil.py to use dhtypes and functional storage API (phase 5a)"
```

---

### PHASE 5b: Update backtesting Imports for dhutil (if applicable)

**Objective**: Update backtesting if it imports from dhutil

**Actions**:
1. Search for dhutil imports:
   ```bash
   grep -rIn "from dhtrader.dhutil\|import dhutil" . --include='*.py' --exclude-dir=env
   ```

2. If imports found, verify they still work with updated dhutil:
   ```bash
   python3 -c "from dhtrader import dhutil; print('✓ dhutil imports work')"
   ```

3. If storage calls in backtesting code using dhutil, update to functional style (should already be done)

**Commit** (if changes needed):
```bash
cd /path/to/backtesting
git add .
git commit -m "refactor: verify dhutil imports work with updated module (phase 5b)"
# Or if no changes:
git commit --allow-empty -m "refactor: no backtesting changes needed for dhutil (phase 5b)"
```

---

### PHASE 6a: Update Test Files (dhtrader)

**Objective**: Update test imports and call patterns in dhtrader tests

**Important**: Only update imports and storage calls in EXISTING test files (`tests/test_*.py`). May create temporary test files if needed for validation.

**Actions**:
1. Find all existing test files:
   ```bash
   find tests/ -name "test_*.py" -type f
   ```

2. For each existing test file, replace imports only:
   ```python
   # OLD:
   # from dhcharts import Candle, Chart, Symbol, Event, Indicator, IndicatorSMA, IndicatorEMA, Day
   # from dhtrades import Trade, TradeSeries, Backtest

   # NEW:
   from dhtypes import Candle, Chart, Symbol, Event, Indicator, IndicatorSMA, IndicatorEMA, Day, Trade, TradeSeries, Backtest
   from dhstore import store_candle, store_trade, store_backtests, get_candles, get_trades, get_backtests
   ```

3. Replace method-style storage calls with function calls (only in existing tests):
   ```python
   # OLD:
   # candle.store()
   # trade.store()

   # NEW:
   from dhstore import store_candle, store_trade
   store_candle(candle)
   store_trade(trade)
   ```

4. Search for `.store(` in all existing test files and convert
5. **DO NOT** modify test logic, assertions, or structure - only imports and function calls

**Temporary Test Files**:
- Can create `tests/test_temp_<feature>.py` for validation
- Use full content as needed to validate changes
- Delete after validation completes

**Validation**:
```bash
# Check for old imports (should be empty)
grep -rn "from dhcharts\|from dhtrades\|import dhcharts\|import dhtrades" tests/test_*.py

# Check for old method calls in existing tests
grep -rn "\.store()" tests/test_*.py
```

**Commit**:
```bash
cd /path/to/dhtrader
git add tests/
git commit -m "refactor: update test imports and storage calls to new API (phase 6a)"
```

---

### PHASE 6b: Update Test Files (backtesting)

**Objective**: Update test imports and call patterns in backtesting tests

**Important**: Same rules as Phase 6a - only update existing test files (`tests/test_*.py`). May create temporary test files.

**Actions**:
1. Find all existing test files:
   ```bash
   find tests/ -name "test_*.py" -type f
   ```

2. For each existing test file, replace imports:
   ```python
   # OLD:
   # from dhtrader.dhcharts import ...
   # from dhtrader.dhtrades import ...

   # NEW:
   from dhtrader.dhtypes import ...
   from dhtrader.dhstore import ...
   ```

3. Replace method-style storage calls:
   ```python
   # OLD: obj.store()
   # NEW:
   from dhtrader.dhstore import store_candle  # or appropriate function
   store_candle(obj)
   ```

4. **DO NOT** modify test logic, assertions, or structure

**Validation**:
```bash
# Check for old imports
grep -rn "from dhtrader.dhcharts\|from dhtrader.dhtrades\|import dhcharts\|import dhtrades" tests/test_*.py
```

**Commit**:
```bash
cd /path/to/backtesting
git add tests/
git commit -m "refactor: update test imports and storage calls to new API (phase 6b)"
```

---

### PHASE 7a: Create Public API (dhtrader)

**Objective**: Define clean final API in __init__.py

**Actions**:
1. Update `__init__.py` in dhtrader root:
   ```python
   """
   dhtrader: Trading analysis module

   Public API exports domain classes and storage functions.
   No backward compatibility layer - use direct imports.
   """

   from .dhtypes import (
       Candle,
       Event,
       Symbol,
       IndicatorDataPoint,
       Indicator,
       IndicatorSMA,
       IndicatorEMA,
       Chart,
       Day,
       Trade,
       TradeSeries,
       Backtest,
   )

   from .dhstore import (
       store_candle,
       store_candles,
       store_trade,
       store_backtests,
       store_event,
       get_candles,
       get_trades,
       get_backtests,
       get_events,
       get_symbol_by_ticker,
   )

   __all__ = [
       # Domain classes
       'Candle',
       'Event',
       'Symbol',
       'IndicatorDataPoint',
       'Indicator',
       'IndicatorSMA',
       'IndicatorEMA',
       'Chart',
       'Day',
       'Trade',
       'TradeSeries',
       'Backtest',
       # Storage functions
       'store_candle',
       'store_candles',
       'store_trade',
       'store_backtests',
       'store_event',
       'get_candles',
       'get_trades',
       'get_backtests',
       'get_events',
       'get_symbol_by_ticker',
   ]

   __version__ = '2.0.0'  # Major version bump for breaking changes
   ```

**Validation**:
```bash
# Test public API imports
python3 -c "from dhtrader import Candle, Chart, Trade, store_candle, get_candles; print('✓ Public API works')"

# Verify __all__ exports
python3 -c "import dhtrader; print(len(dhtrader.__all__), 'exports'); assert 'Candle' in dhtrader.__all__"
```

---

### PHASE 9: Comprehensive Testing

**Objective**: Validate complete refactoring success

**Actions**:
1. Run full test suite in dhtrader:
   ```bash
   # Execute in dhtrader root
   pytest tests/ -v
   # OR
   ./test.sh
   ```

2. Check for circular imports:
   ```bash
   python3 -c "
   import sys
   try:
       import dhtypes
       import dhstore
       import dhutil
       print('✓ No circular imports detected')
   except ImportError as e:
       print(f'✗ Import error: {e}')
       sys.exit(1)
   "
   ```

3. Verify PEP 8 compliance:
   ```bash
   # Compile check
   python3 -m py_compile dhtypes.py dhstore.py dhutil.py __init__.py

   # Run validation script if available
   # Adjust path based on environment
   ./validate-file-quality.sh dhtypes.py
   ./validate-file-quality.sh dhstore.py
   ./validate-file-quality.sh dhutil.py
   ```

4. Run backtesting tests if available:
   ```bash
   # Execute in backtesting root
   pytest tests/ -v
   # OR test CLI workflows
   ```

**Expected Result**:
- ✓ No circular import errors
- ✓ PEP 8 validation passes
- ⚠ Some unit tests WILL FAIL (expected - document in Phase 9)

**Commit**:
```bash
cd /path/to/dhtrader
git add .
git commit -m "refactor: comprehensive testing and validation (phase 8)"

cd /path/to/backtesting
git add .
git commit -m "refactor: comprehensive testing and validation (phase 8)"
```

---

### PHASE 9: Test Failure Documentation

**Objective**: Document failing unit tests and categorize by cause for user review

**Important**: Do NOT attempt to fix failing tests. Only document and categorize.

**Actions**:
1. Capture test failures from dhtrader:
   ```bash
   cd /path/to/dhtrader
   pytest tests/ -v 2>&1 | tee test_failures_dhtrader.txt
   ```

2. Parse failures and create summary document:
   ```bash
   cat > TEST_FAILURES_SUMMARY.md << 'EOF'
# Unit Test Failures Summary

## dhtrader Repository

### Failing Tests by Category

#### Import-Related Failures
(Failed imports or missing classes/functions - caused by module restructuring)

#### Storage API Changes
(Tests expecting method-style .store() but now using functional API)

#### API Signature Changes
(Function/method signatures changed or parameters differ)

#### Other / Unclear
(Failures not obviously related to refactoring)

### Test Count Summary
- Total tests run: [NUMBER]
- Failed: [NUMBER]
- Passed: [NUMBER]
- Refactoring-related: [SUBSET]

## backtesting Repository

### Failing Tests by Category
(Same structure as dhtrader)

### Recommendations
- Tests to investigate for pre-existing issues
- Tests that should be resolved after refactoring

EOF
   ```

3. Populate the summary with actual test results:
   - Parse pytest output
   - Group failures by cause
   - Note which failures are refactoring-related vs potentially pre-existing
   - Do NOT modify test code or attempt fixes

4. Capture backtesting test failures (if applicable):
   ```bash
   cd /path/to/backtesting
   pytest tests/ -v 2>&1 | tee test_failures_backtesting.txt
   ```

**Commit**:
```bash
cd /path/to/dhtrader
git add TEST_FAILURES_SUMMARY.md test_failures_dhtrader.txt
git commit -m "docs: document unit test failures for user review (phase 9)"

cd /path/to/backtesting
(if backtesting has test output)
git add test_failures_backtesting.txt
git commit -m "docs: document unit test failures for user review (phase 9)"
```

---

### PHASE 10: Final Branch and PR Status

**Objective**: Verify branches are ready for user review (do not merge)

**Actions**:
1. Verify all work is committed in both repos:
   ```bash
   cd /path/to/dhtrader
   git status  # Should show: On branch refactor/eliminate-circular-imports, nothing to commit

   cd /path/to/backtesting
   git status  # Should show: On branch refactor/eliminate-circular-imports, nothing to commit
   ```

2. Push branches to remote (if using GitHub/GitLab):
   ```bash
   cd /path/to/dhtrader
   git push origin refactor/eliminate-circular-imports

   cd /path/to/backtesting
   git push origin refactor/eliminate-circular-imports
   ```

3. Create Pull Requests (if using GitHub/GitLab):
   - Title: "Refactor: Eliminate circular imports via dhtypes/dhstore layering"
   - Description: Reference CIRCULAR_IMPORTS_STRATEGY.md and this implementation plan
   - Status: **DO NOT MERGE** - awaiting user review
   - Link backtesting PR to reference dhtrader PR

4. Document completion:
   ```bash
   cd /path/to/dhtrader
   cat > REFACTORING_COMPLETE.md << 'EOF'
# Circular Import Elimination - Refactoring Complete

## Completion Status
- ✓ Phase 1-9 all complete
- ✓ Feature branches created in dhtrader and backtesting repos
- ✓ All commits synchronized between repositories
- ✓ Parallel PRs created (see links below)
- ✓ Test failures documented in TEST_FAILURES_SUMMARY.md
- ✓ PEP 8 compliance verified

## Architecture Changes
- ✓ dhtypes.py created with pure domain classes
- ✓ dhcharts.py and dhtrades.py eliminated
- ✓ dhstore.py refactored to functional storage API
- ✓ All imports updated in dhtrader and backtesting repos
- ✓ Public API published in __init__.py

## Pull Requests
- dhtrader: [PR link or branch name]
- backtesting: [PR link or branch name]

## Awaiting User Review
- Test failures require categorization (see TEST_FAILURES_SUMMARY.md)
- PRs should NOT be merged until user approval
- Next steps: User review and approval before merge

## Branch Names
- dhtrader: refactor/eliminate-circular-imports
- backtesting: refactor/eliminate-circular-imports

EOF
   git add REFACTORING_COMPLETE.md
   git commit -m "docs: refactoring complete - awaiting user review (phase 10)"
   ```

**Summary**:
```
✓ All implementation phases (1-10) complete
✓ Feature branches: refactor/eliminate-circular-imports in both repos
✓ Commits: Synchronized between dhtrader and backtesting
✓ PRs: Created but NOT merged
✓ Status: Awaiting user review per criteria
```

---

## MIGRATION_MAPPINGS

### Import Mappings

| Old Import | New Import |
|------------|------------|
| `from dhcharts import Candle` | `from dhtypes import Candle` |
| `from dhcharts import Chart` | `from dhtypes import Chart` |
| `from dhcharts import Symbol` | `from dhtypes import Symbol` |
| `from dhcharts import Event` | `from dhtypes import Event` |
| `from dhcharts import Indicator, IndicatorSMA, IndicatorEMA` | `from dhtypes import Indicator, IndicatorSMA, IndicatorEMA` |
| `from dhcharts import Day` | `from dhtypes import Day` |
| `from dhtrades import Trade` | `from dhtypes import Trade` |
| `from dhtrades import TradeSeries` | `from dhtypes import TradeSeries` |
| `from dhtrades import Backtest` | `from dhtypes import Backtest` |
| `import dhcharts` | `import dhtypes` (then use `dhtypes.Candle`) |
| `import dhtrades` | `import dhtypes` (then use `dhtypes.Trade`) |

### Storage Call Mappings

| Old Pattern | New Pattern |
|-------------|-------------|
| `candle.store()` | `from dhstore import store_candle`<br>`store_candle(candle)` |
| `trade.store()` | `from dhstore import store_trade`<br>`store_trade(trade)` |
| `event.store()` | `from dhstore import store_event`<br>`store_event(event)` |
| `chart.add_candle(c)` followed by storage | `from dhstore import store_candle`<br>`store_candle(c)` |

### Analysis Method Mappings (No Change)

These methods stay on classes in dhtypes.py:
- `Symbol.market_is_open()` → remains unchanged
- `Chart.review_candles()` → remains unchanged
- `Chart.load_candles()` → may need refactor to accept candles as parameter
- `IndicatorSMA.calculate()` → remains unchanged
- `IndicatorEMA.calculate()` → remains unchanged
- `Trade.gain_loss()` → remains unchanged
- `Candle.contains_price()` → remains unchanged

---

## VALIDATION_PATTERNS

### Search Patterns for Legacy Code

```bash
# Find old imports (should return empty after migration)
grep -rIn "from dhcharts\|from dhtrades\|import dhcharts\|import dhtrades" . --include='*.py' --exclude-dir=env --exclude-dir=venv --exclude-dir=__pycache__

# Find method-style storage calls (should return empty after migration)
grep -rIn "\.store(" . --include='*.py' --exclude-dir=env --exclude-dir=venv --exclude-dir=__pycache__

# Verify dhtypes has no dhstore imports
grep -n "from dhstore\|import dhstore" dhtypes.py
# Expected output: (empty)

# Verify dhstore imports from dhtypes
grep -n "from dhtypes import" dhstore.py
# Expected output: (one or more matches)
```

### Import Test Commands

```bash
# Test each module imports independently
python3 -c "import dhtypes; print('✓ dhtypes')"
python3 -c "import dhstore; print('✓ dhstore')"
python3 -c "import dhutil; print('✓ dhutil')"

# Test combined imports
python3 -c "from dhtypes import Candle; from dhstore import store_candle; print('✓ Combined')"

# Test public API
python3 -c "from dhtrader import Candle, Trade, store_candle; print('✓ Public API')"

# Test old modules fail
python3 -c "try: import dhcharts; print('✗ FAIL'); except ImportError: print('✓ dhcharts eliminated')"
python3 -c "try: import dhtrades; print('✗ FAIL'); except ImportError: print('✓ dhtrades eliminated')"
```

---

## SUCCESS_CRITERIA

### Boolean Checkpoints

- [ ] `dhtypes.py` created with all domain classes
- [ ] `dhtypes.py` imports only from `dhcommon` and stdlib (no `dhstore`)
- [ ] `dhcharts.py` deleted
- [ ] `dhtrades.py` deleted
- [ ] `dhstore.py` imports from `dhtypes` (not `dhcharts` or `dhtrades`)
- [ ] All storage operations converted to functional API
- [ ] `dhutil.py` updated with new imports
- [ ] All test files updated with new imports
- [ ] All test files use functional storage API
- [ ] `backtesting` repo updated with new imports
- [ ] `backtesting` repo uses functional storage API
- [ ] `__init__.py` exports final public API
- [ ] dhtrader test suite passes (100%)
- [ ] backtesting test suite passes (if available)
- [ ] No circular import errors on module load
- [ ] PEP 8 validation passes on all modified files
- [ ] No legacy imports found: `grep -r "from dhcharts\|from dhtrades\|import dhcharts\|import dhtrades" . --include='*.py'` returns empty
- [ ] No method-style storage: `grep -r "\.store()" . --include='*.py'` returns empty (excluding comments/non-dhtrader code)

### Verification Commands

```bash
# Run these commands to verify success
# Execute in dhtrader repository root

# 1. No circular imports
python3 -c "import dhtypes, dhstore, dhutil; print('✓ No circular imports')"

# 2. Old modules eliminated
python3 -c "import sys; import dhcharts" 2>&1 | grep -q "ModuleNotFoundError" && echo "✓ dhcharts eliminated"
python3 -c "import sys; import dhtrades" 2>&1 | grep -q "ModuleNotFoundError" && echo "✓ dhtrades eliminated"

# 3. Test suite passes
pytest tests/ -v
# OR
./test.sh

# 4. No legacy imports in dhtrader
! grep -rq "from dhcharts\|from dhtrades\|import dhcharts\|import dhtrades" . --include='*.py' --exclude-dir=__pycache__ && echo "✓ No legacy imports in dhtrader"

# 5. No legacy imports in backtesting (adjust path as needed)
cd ../backtesting  # Or appropriate path
! grep -rq "from dhtrader.dhcharts\|from dhtrader.dhtrades\|import dhcharts\|import dhtrades" . --include='*.py' --exclude-dir=env && echo "✓ No legacy imports in backtesting"

# 6. PEP 8 validation
cd -  # Back to dhtrader
python3 -m py_compile dhtypes.py dhstore.py dhutil.py __init__.py && echo "✓ Syntax valid"
```

---

## TROUBLESHOOTING

### Common Issues

**Issue**: `ImportError: circular import`
**Cause**: dhtypes.py has import from dhstore
**Solution**: Remove all dhstore imports from dhtypes.py

**Issue**: `ModuleNotFoundError: No module named 'dhtypes'`
**Cause**: File not created or in wrong location
**Solution**: Verify dhtypes.py exists in dhtrader root directory

**Issue**: `AttributeError: 'Candle' object has no attribute 'store'`
**Cause**: Test/code still using method-style storage
**Solution**: Replace `obj.store()` with `from dhstore import store_<type>; store_<type>(obj)`

**Issue**: Tests fail after refactoring
**Cause**: Imports not updated in test files
**Solution**: Update all test imports to use `dhtypes` and `dhstore`

**Issue**: dhcharts or dhtrades still importable
**Cause**: Files not deleted or bytecode cache exists
**Solution**:
```bash
rm dhcharts.py dhtrades.py
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} +
```

---

## NOTES_FOR_AGENT

### Code Quality Standards (Enforced by Pre-Commit Hooks)

**Important**: Every commit will be checked by pre-commit hooks in both repositories.

**79-Character Line Limit (PEP 8)**:
- Maximum line length: 79 characters
- Applies to: All Python files (.py)
- Tool: flake8 with configuration in `.flake8`
- Validation: `./validate-file-quality.sh <filename>`
- When line is too long, use parentheses to break across multiple lines:
  ```python
  # WRONG (too long):
  some_function(argument1, argument2, argument3, argument4, argument5)

  # CORRECT (line length 79):
  some_function(
      argument1, argument2, argument3,
      argument4, argument5
  )
  ```

**No Trailing Whitespace**:
- All lines must NOT end with spaces or tabs
- Blank lines must be completely empty (zero characters)
- Check: `grep -n " $" filename  # Should return nothing`
- Tool: `.editorconfig` removes automatically on save if configured

**Unix Line Endings (LF Only)**:
- All files must use LF line endings, not CRLF
- Configure git: `git config core.safecrlf true`
- Check: `file filename  # Should say "LF line terminators" not "CRLF"`

**Pre-Commit Hook Execution**:
- Runs: `flake8 . --extend-ignore=F401,E402` (with repo-specific excludes)
- Location: `githooks/pre-commit` in each repo
- When: Automatically before each `git commit` (after environment setup step 4)
- If hook fails: commit is blocked; fix issues and try commit again
- Example:
  ```bash
  git commit -m "refactor: create dhtypes"
  # If hook fails:
  # 1. Fix flake8 errors shown
  # 2. Re-run: git commit -m "refactor: create dhtypes"
  ```

**PEP 8 Compliance Checklist**:
- [ ] No line exceeds 79 characters
- [ ] No trailing whitespace on any line
- [ ] All blank lines are completely empty
- [ ] All files end with exactly one newline
- [ ] Line endings are LF (Unix), not CRLF (Windows)
- [ ] All imports at module top (no lazy imports)
- [ ] Type hints present on functions/methods

**Run Validation Manually**:
```bash
cd /path/to/dhtrader

# Validate single file
./validate-file-quality.sh dhtypes.py

# Validate all Python files (one at a time or in loop)
for file in dhtypes.py dhstore.py dhutil.py; do
    ./validate-file-quality.sh "$file" || exit 1
done

# Run flake8 directly (full repo)
flake8 . --exclude=v1/*

# Run linter on modified files only
git diff --name-only | grep '\.py$' | while read f; do
    ./validate-file-quality.sh "$f" || exit 1
done
```

### Pytest Configuration and Testing

**Pytest is configured via**:
- `dhtrader/setup.cfg` (`[tool:pytest]` section)
- `backtesting/pytest.ini`

**Configuration Details**:
- Test paths: `tests/`
- Available markers:
  - `@pytest.mark.storage`: Tests that interact with storage/database
  - `@pytest.mark.slow`: Tests that are slow-running
  - `@pytest.mark.historical`: Tests using real market data from testdata folder

**Test Execution** (via test.sh scripts):
```bash
# All tests (fast + slow)
./test.sh

# Fast tests only (skip marked as slow)
./test.sh -f

# Specific test file
./test.sh tests/test_dhtypes.py

# Specific test function
./test.sh tests/test_dhtypes.py::test_candle_creation

# Specific marker
./test.sh -m "storage"
./test.sh -m "not slow"

# Help
./test.sh -h
```

**During Refactoring**:
- Many tests WILL FAIL (expected until all phases complete)
- Do NOT fix test failures until Phase 9 (documentation phase)
- Document failures, don't resolve them

### Path Adaptations
- All paths in this document assume execution context is the repository root
- Commands using `~/git/dhtrader` should be adapted to your workspace root
- Sibling repository `backtesting` path may vary (use `../backtesting` or environment variable)
- Virtual environment directories (env, venv, .venv) should be excluded from file searches

### Execution Context
- Prefer relative paths when working within a repository
- Use absolute paths when crossing repository boundaries
- Verify file existence before deletion: `test -f filename && rm filename`
- Use `--exclude-dir` flags to skip virtual environments and caches
- Always activate virtual environment before running Python: `source ../backtesting/env/bin/activate`

### Pre-Commit Hook Notes
- Hooks are installed during ENVIRONMENT_SETUP step 4
- Located in: `githooks/pre-commit` (symlinked to `.git/hooks/pre-commit`)
- Run automatically on `git commit` (after setup)
- To skip hook (NOT RECOMMENDED): `git commit --no-verify` (violates criteria)
- To run hook manually: `/path/to/githooks/pre-commit`

### Testing Strategy
- Run full test suite after major phases to catch regressions
- If test framework unknown, try: `pytest`, `python -m pytest`, `./test.sh`, `python -m unittest`
- Verify imports before running tests: `python3 -c "import <module>"`
- Expected failures during refactoring are documented in Phase 9
- Do NOT attempt to fix failing tests until all phases complete

### Edge Cases
- If `bot()` function is critical, keep in `dhutil.py` rather than `dhcommon.py`
- If additional utility modules exist, follow same import update pattern
- If new classes discovered during implementation, add to dhtypes.py
- Logger setup can remain in dhtypes.py with module-specific name
- If a file has very long lines that cannot be broken (e.g., URLs), discuss with user before committing

### Git Workflow
- Commit after each phase for rollback safety
- Use clear commit messages referencing phase number: `refactor: <description> (phase Na)`
- Feature branch: `refactor/eliminate-circular-imports` (created in ENVIRONMENT_SETUP)
- Do NOT merge feature branch until user approval (Phase 10)
- Tag when complete: `git tag v2.0.0-no-circular-imports`
- Pre-commit hooks run automatically; failures block commits

---

## REFERENCE_SNIPPETS

### dhtypes.py Template Header
```python
"""
dhtypes: Pure domain model classes for dhtrader

Contains all domain classes with no persistence logic.
All classes are pure data models or analysis logic only.

Architecture Rules:
- NO imports from dhstore (would create circular import)
- ONLY imports from dhcommon and Python stdlib
- NO .store() or .save() methods (use dhstore functions instead)
"""

from typing import List, Optional, Dict, Tuple, Any, Union
from datetime import datetime, timedelta
import logging
from dhcommon import <required_utilities>

__all__ = [
    'Candle',
    'Event',
    'Symbol',
    'IndicatorDataPoint',
    'Indicator',
    'IndicatorSMA',
    'IndicatorEMA',
    'Chart',
    'Day',
    'Trade',
    'TradeSeries',
    'Backtest',
]

log = logging.getLogger("dhtypes")
```

### dhstore.py Import Block Template
```python
"""
dhstore: Persistence layer for dhtrader domain models

All storage operations as explicit functions.
Imports domain classes from dhtypes (one-way dependency).
"""

from typing import List, Optional, Dict, Tuple, Any
from datetime import datetime
from dhtypes import (
    Candle,
    Event,
    Symbol,
    IndicatorDataPoint,
    IndicatorSMA,
    IndicatorEMA,
    Trade,
    TradeSeries,
    Backtest,
    Chart,
)
import dhmongo
from dhcommon import LOGGER, dt_to_epoch, dt_as_dt
```

### Functional Storage Pattern
```python
# Domain model usage (no persistence)
from dhtypes import Candle

candle = Candle(
    c_datetime=datetime.now(),
    c_open=100.0,
    c_high=105.0,
    c_low=99.0,
    c_close=104.0,
    c_volume=1000
)

# Analysis (stays on domain model)
body_size = candle.calculate_body_size()

# Persistence (explicit function call)
from dhstore import store_candle
result = store_candle(candle)
```

---

**END OF IMPLEMENTATION PLAN**
