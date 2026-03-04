# Circular Imports Elimination Strategy for dhtrader Module

**Author**: GitHub Copilot
**Date**: March 2, 2026
**Purpose**: Provide a comprehensive strategy to eliminate all circular imports in the `dhtrader` submodule while maintaining functionality and optimizing for modern Python architecture conventions.

---

## Executive Summary

The `dhtrader` module currently contains a **primary circular import dependency** between `dhcharts.py` and `dhstore.py`, along with **secondary coupling issues** involving `dhutil.py`, `dhtrades.py`, and `dhcommon.py`. This document outlines a **PEP 8-compliant systematic approach** to eliminate these circular dependencies while adhering to Python standards, modern packaging conventions, and architectural best practices.

**Current Status**: The module functions despite the circular imports due to late runtime binding, but this fragility creates maintenance challenges and limits IDE/type-checker support.

**Recommended Approach**: Restructure the module using a **layered architecture** with clear separation of concerns:
- Create a new `dhtypes` module containing **pure domain model classes** (no storage methods)
- **Eliminate `dhcharts.py` and `dhtrades.py` entirely** - all classes move to `dhtypes.py`
- Move any utility functions/variables from `dhcharts.py` and `dhtrades.py` to appropriate modules (`dhutil.py` or `dhstore.py`)
- Move all persistence operations to `dhstore.py` as **explicit functions**
- Keep all imports at the **top of files** (100% PEP 8 compliant)
- Result: Functional API using `store_candle(candle)` instead of `candle.store()`

This approach maintains clean architecture while preserving strict adherence to PEP 8 standards with all imports at module level.

---

## Table of Contents

1. [Circular Import Analysis](#1-circular-import-analysis)
2. [Modern Python Architecture Principles](#2-modern-python-architecture-principles)
3. [Proposed Solution: PEP 8 Compliant Layered Architecture](#3-proposed-solution-pep-8-compliant-layered-architecture)
4. [Implementation Roadmap](#4-implementation-roadmap)
   - [Phase 1: Foundation (Class Extraction)](#phase-1-foundation-class-extraction)
   - [Phase 2: Refactor Storage Layer](#phase-2-refactor-storage-layer)
   - [Phase 3: Update All Module Imports](#phase-3-update-all-module-imports)
   - [Phase 4: Create Public API](#phase-4-create-public-api)
   - [Phase 5: Comprehensive Testing](#phase-5-comprehensive-testing)
   - [Phase 6: Documentation](#phase-6-documentation)
   - [Phase 7: Git Commit & Review](#phase-7-git-commit--review)
5. [Handling Complex Refactoring Cases](#5-handling-complex-refactoring-cases)
6. [Benefits of the PEP 8 Strict Approach](#6-benefits-of-the-pep-8-strict-approach)
7. [Risk Assessment & Mitigation](#7-risk-assessment--mitigation)
8. [Testing Strategy for PEP 8 Strict Refactoring](#8-testing-strategy-for-pep-8-strict-refactoring)
9. [Migration Guide for Developers](#9-migration-guide-for-developers)
10. [Success Criteria](#10-success-criteria)
11. [Quick Reference: File-by-File Changes](#11-quick-reference-file-by-file-changes)
12. [Troubleshooting Common Issues](#12-troubleshooting-common-issues)
13. [References & Resources](#13-references--resources)
14. [Document History](#14-document-history)

---

## 1. Circular Import Analysis

### 1.1 Primary Circular Dependency: dhcharts ↔ dhstore

```
dhcharts.py imports from dhstore.py:
  ├─ get_symbol_by_ticker()          [Used in Line 701, 820, 1007]
  ├─ get_candles()                   [Used in Line 918]
  ├─ store_candle()                  [Used in Line 791 - Candle.store()]
  ├─ get_events()                    [Used in Line 471, 926]
  ├─ store_event()                   [Used in Line 1045 - Event.store()]
  ├─ get_indicator_datapoints()      [Imported but verify usage]
  ├─ store_indicator()               [Imported but verify usage]
  └─ store_indicator_datapoints()    [Imported but verify usage]

dhstore.py imports from dhcharts.py:
  ├─ Candle                          [Type definition for stored objects]
  ├─ Event                           [Type definition for stored objects]
  ├─ IndicatorDataPoint              [Type definition for stored objects]
  ├─ Symbol                          [Type definition for Symbol instances]
  ├─ IndicatorSMA                    [Type for indicator operations]
  └─ IndicatorEMA                    [Type for indicator operations]
```

**Why This is Problematic:**
- Creates runtime dependency order fragility (import order matters)
- Prevents clean type checking and IDE autocompletion on first pass
- Makes it impossible to import class definitions without triggering persistence layer initialization
- Violates the Dependency Inversion Principle (high-level domain models depend on low-level persistence details)

### 1.2 Secondary Dependencies

**dhutil.py imports from both dhcharts and dhstore**:
- Uses: `Candle` class (from dhcharts)
- Uses: `get_symbol_by_ticker()`, `get_candles()`, `get_events()`, `review_candles()` (from dhstore)
- **Status**: One-way dependence (no reverse import) - less critical but still couples utility to persistence

**dhtrades.py imports from dhcharts and dhstore**:
- Uses: `Candle`, `Chart` (from dhcharts)
- Uses: Database accessor/mutation functions (from dhstore)
- **Status**: One-way dependence - acceptable pattern but could be improved

**dhcommon.py**:
- **Status**: Pure utility module with NO internal imports ✓
- **Role**: Foundation module for all others

**Resolution Strategy**: 
- `dhcharts.py` and `dhtrades.py` will be **completely eliminated**
- All classes from these files move to the new `dhtypes.py` module
- Any utility functions/variables move to `dhutil.py`, `dhstore.py`, or `dhcommon.py` as appropriate

---

## 2. Modern Python Architecture Principles

### 2.1 Relevant Standards and Best Practices

**PEP 484 - Type Hints**:
- Enables better IDE support and static type checking
- Can use `TYPE_CHECKING` guard to import types only during type checking

**PEP 8 - Style Guide**:
- Clear module responsibilities
- Explicit is better than implicit
- Simple is better than complex

**PEP 3107 / PEP 484 - Annotations**:
- Forward references using string literals to avoid circular imports
- Protocol classes for structural subtyping

**Architectural Patterns**:
1. **Layered/Clean Architecture**: Separate domain, application, and infrastructure layers
2. **Dependency Inversion**: High-level modules should not depend on low-level modules
3. **Interface Segregation**: Depend on abstractions, not concrete implementations
4. **Don't Repeat Yourself**: Shared types in canonical location

### 2.2 Why Current Design Violates These Patterns

| Principle | Issue | Impact |
|-----------|-------|--------|
| **Layered Architecture** | Domain classes (Candle, Chart) directly import persistence functions | Can't use domain classes without loading database layer |
| **Dependency Inversion** | `dhcharts` (business logic) depends on `dhstore` (infrastructure) | Tightly coupled; hard to mock or replace storage |
| **Interface Segregation** | Importing 9+ functions at module level when only some used in each class | Over-specification; harder to reason about dependencies |
| **Testability** | Circular dependencies prevent clean mock injection | Must integrate with actual database during tests |

---

## 3. Proposed Solution: PEP 8 Compliant Layered Architecture

This strategy eliminates circular imports while maintaining **100% PEP 8 compliance** with all imports at the module level.

### 3.1 Architecture Overview

**Core Principle**: Separate domain model classes from persistence operations entirely

```
dhtypes.py (Pure data classes - NO imports from dhstore):
  ├─ Candle              [Data model only]
  ├─ Event               [Data model only]
  ├─ Symbol              [Data model only]
  ├─ IndicatorDataPoint  [Data model only]
  ├─ Indicator           [Base class + analysis logic]
  ├─ IndicatorSMA        [Indicator subclass]
  ├─ IndicatorEMA        [Indicator subclass]
  ├─ Chart               [Analysis logic only]
  ├─ Day                 [Day-level analysis logic]
  ├─ Trade               [Data model + analysis logic]
  ├─ TradeSeries         [Series logic + analysis]
  └─ Backtest            [Backtest logic + analysis]

dhstore.py (All persistence operations as functions):
  ├─ def store_candle(candle: dhtypes.Candle) -> bool
  ├─ def store_trade(trade: dhtypes.Trade) -> bool
  ├─ def store_backtest(backtest: dhtypes.Backtest) -> bool
  ├─ def store_event(event: dhtypes.Event) -> bool
  ├─ def get_symbol_by_ticker(ticker: str) -> dhtypes.Symbol
  ├─ def get_candles(...) -> List[dhtypes.Candle]
  ├─ def get_trades(...) -> List[dhtypes.Trade]
  ├─ def get_backtests(...) -> List[dhtypes.Backtest]
  └─ [All other storage/retrieval functions]

dhcommon.py   [Existing - Pure utilities, no changes needed]

dhutil.py     [UPDATED - receives any utility functions from dhcharts.py/dhtrades.py]

[dhcharts.py and dhtrades.py are ELIMINATED - all classes moved to dhtypes.py,
 any utility functions moved to dhutil.py or dhstore.py as appropriate]
```

### 3.2 Import Dependency Graph

```
Standard Library ─┐
                  ├─> dhcommon (utilities only)
Third-party libs ┘        ↑
                          │
                   dhtypes.py (pure domain models)
                          ↑
                          │
    (dhstore imports dhtypes)
    ├─ dhstore.py (persistence layer)
    ├─ dhmongo.py (actual DB implementation)
    └─ dhcommon
                          ↑
                          │
    (Usage modules import from dhtypes + dhstore)
    ├─ dhutil.py (can import dhtypes, dhstore)
    └─ test files
    
    [dhcharts.py and dhtrades.py eliminated - content moved to dhtypes/dhutil/dhstore]
```

**No circular imports possible** because:
- dhtypes.py imports ONLY from dhcommon and standard library
- dhstore.py imports from dhtypes but NOT vice versa
- dhcharts/dhtrades/dhutil import from dhtypes AND dhstore (one-way dependency)
- NO file imports from dhtypes after importing dhstore

### 3.3 Key Design Principles

#### Principle 1: Pure Domain Models in dhtypes

**dhtypes.py contains ONLY**:
- Class definitions with `__init__()` methods
- Data validation methods
- Analysis/transformation methods (e.g., `Chart.calculate_sma()`)
- Utility methods needed for domain logic
- **NO storage methods** (no `.store()`, `.save()`, `.to_db()`)
- **NO imports** from dhstore, dhcharts, or dhtrades
- **ONLY imports** from: dhcommon and Python standard library

**Example**:
```python
# dhtypes.py
class Candle:
    def __init__(self, c_datetime, c_open, c_high, c_low, c_close, c_volume):
        self.c_datetime = c_datetime
        self.c_open = float(c_open)
        self.c_high = float(c_high)
        # ... etc - ONLY initialization

    def calculate_body_size(self):
        """Analysis method - OK to include"""
        return abs(self.c_close - self.c_open)

    # NO storage methods like:
    # def store(self): ...  <- Should NOT be here

class Trade:
    def __init__(self, entry_dt, direction, entry_price, ...):
        self.entry_dt = entry_dt
        self.direction = direction
        # ... etc - ONLY initialization

    def calculate_profit(self, exit_price):
        """Analysis method - OK to include"""
        if self.direction == 'long':
            return (exit_price - self.entry_price) * self.contracts
        else:
            return (self.entry_price - exit_price) * self.contracts
```

#### Principle 2: All Persistence in dhstore as Functions

**dhstore.py contains**:
- Persistence functions accept domain objects as parameters
- Return domain objects or lists of domain objects
- **ALL imports at the top** (PEP 8 compliant)
- Type hints using dhtypes classes

**Example**:
```python
# dhstore.py - ALL imports at TOP (PEP 8 required location)
from dhtypes import Candle, Trade, Backtest, Event, Chart, Symbol, IndicatorDataPoint
import dhmongo as dhm
from dhcommon import dt_to_epoch, dt_as_dt, log_say
from typing import List, Optional

# Storage functions:
def store_candle(candle: Candle) -> bool:
    """Persist a Candle object to storage"""
    # Implementation using dhmongo
    return dhm.store_candle(candle)

def store_trade(trade: Trade) -> Trade:
    """Persist a Trade object to storage and return it"""
    # Implementation
    result = dhm.store_trade(trade.to_dict())
    return trade

def get_candles(start_epoch: int, end_epoch: int,
                timeframe: str, symbol: str) -> List[Candle]:
    """Retrieve candles from storage"""
    # Implementation returns Candle objects
    raw_data = dhm.get_candles(start_epoch, end_epoch)
    return [Candle(**row) for row in raw_data]

def get_symbol_by_ticker(ticker: str) -> Symbol:
    """Retrieve or create a Symbol"""
    # Implementation
    return Symbol(ticker)
```

#### Principle 3: Usage Pattern (Functional Style)

Instead of OOP `.store()` method calls:

```python
# BEFORE (with circular imports and/or lazy imports):
trade.store()  # Method call - ambiguous where storage happens
candle.store()

# AFTER (PEP 8 Strict, explicit, clear):
from dhtypes import Trade, Candle
from dhstore import store_trade, store_candle

trade = Trade(...)
store_trade(trade)  # Explicit function call - clear what's happening

candle = Candle(...)
store_candle(candle)  # Explicit function call - clear what's happening
```

**Benefits of this pattern**:
- ✓ Explicit is better than implicit (PEP 20 - Zen of Python)
- ✓ Clear separation: domain model vs persistence operation
- ✓ Easy to test: pass mock function as parameter if needed
- ✓ Works with multiple storage backends easily
- ✓ Scales well with functional programming patterns

### 3.4 Module Summary

| Module | Purpose | Imports From | Notes |
|--------|---------|---|---|
| `dhcommon.py` | Shared utilities & constants | stdlib only | No changes needed |
| `dhtypes.py` | Domain model classes | `dhcommon` + stdlib | NEW; all classes from dhcharts + dhtrades |
| `dhstore.py` | Persistence operations | `dhtypes`, `dhmongo`, `dhcommon` | REFACTORED; functions only |
| `dhutil.py` | Utility functions | `dhtypes`, `dhstore`, `dhcommon` | UPDATED; receives utility functions from dhcharts/dhtrades |
| `dhmongo.py` | Database implementation | (internal) | No changes needed |
| ~~`dhcharts.py`~~ | ~~Chart analysis~~ | N/A | **ELIMINATED** - classes moved to dhtypes |
| ~~`dhtrades.py`~~ | ~~Trade/backtest logic~~ | N/A | **ELIMINATED** - classes moved to dhtypes |

---

## 4. Implementation Roadmap

**Goal**: Implement the PEP 8 Strict strategy to eliminate all circular imports while maintaining full standards compliance.

---

### Phase 1: Foundation (Class Extraction)

**Objective**: Create the new `dhtypes.py` module with all domain classes, without any storage methods or circular dependencies.

1. **Create `/home/dusty/git/backtesting/dhtrader/dhtypes.py`**
   - Copy all class definitions from existing modules:
     - From `dhcharts.py`: Candle, Event, Symbol, IndicatorDataPoint, Indicator, IndicatorSMA, IndicatorEMA, Chart, Day
     - From `dhtrades.py`: Trade, TradeSeries, Backtest
   - **Important**: Keep only initialization and analysis methods:
     - ✓ `__init__()` methods
     - ✓ Utility/helper methods (e.g., `Symbol.is_market_open()`, `Candle.close_time()`)
     - ✓ Analysis methods (e.g., `Chart.add_indicator()`)
     - ✗ **Remove ALL storage methods** (`.store()`, `.store_to_db()`, etc.)
   - Ensure imports are only: `dhcommon` and Python standard library
   - Add comprehensive docstrings and type hints using PEP 484 style
   - Example structure:
     ```python
     # dhtrader/dhtypes.py
     from typing import List, Optional
     from datetime import datetime
     from dhcommon import CONSTANTS, ALL_SYMBOLS

     class Candle:
         """Pure data model for candlestick data"""
         def __init__(self, o: float, h: float, l: float, c: float, v: int):
             self.open = o
             self.high = h
             self.low = l
             self.close = c
             self.volume = v

         def is_bullish(self) -> bool:
             """Analysis method - stays on class"""
             return self.close >= self.open

         # ✗ NO: def store(self) -> bool: ...
         # ✗ NO: from dhstore import store_candle (would be circular)

     class Symbol:
         """Pure data model for trading symbols"""
         def __init__(self, ticker: str, market: str):
             self.ticker = ticker
             self.market = market
     ```

2. **Validation - Step 1**
   ```bash
   # Test basic import without triggering storage layer
   python3 -c "from dhtrader.dhtypes import Candle, Symbol, Trade; print('✓ dhtypes imports successfully')"
   ```

---

### Phase 2: Refactor Storage Layer

**Objective**: Remove circular imports by creating all storage functions in dhstore.py with explicit imports at module top.

3. **Update `/home/dusty/git/backtesting/dhtrader/dhstore.py`**
   - **Step A**: Add imports at the module top level (before any functions):
     ```python
     # dhtrader/dhstore.py - at TOP of file
     from typing import List, Optional, Dict, Tuple
     from dhtypes import Candle, Event, Symbol, Trade, TradeSeries, Backtest, Chart
     from dhmongo import (
         find_candles,
         insert_candle,
         find_symbol,
         # ... all other dhmongo functions
     )
     from dhcommon import LOGGER
     ```

   - **Step B**: Create wrapper functions for all storage operations:
     ```python
     def store_candle(candle: Candle) -> bool:
         """Persist a single candle to storage"""
         return dhmongo.insert_candle(candle)

     def store_candles(candles: List[Candle]) -> bool:
         """Persist multiple candles to storage"""
         return dhmongo.insert_candles(candles)

     def store_trade(trade: Trade) -> bool:
         """Persist a trade record to storage"""
         return dhmongo.insert_trade(trade)

     def get_candles(symbol: Symbol, start_dt: datetime, end_dt: datetime) -> List[Candle]:
         """Retrieve candles from storage"""
         raw_data = dhmongo.find_candles(symbol.ticker, start_dt, end_dt)
         return [Candle(*row) for row in raw_data]

     # ... continue for all storage operations
     ```

   - **Step C**: **CRITICAL** - Remove these imports if they exist:
     ```python
     # DELETE THESE LINES:
     from dhcharts import ...  # ✗ REMOVE - creates circular import
     from dhtrades import ...  # ✗ REMOVE - creates circular import
     ```

4. **Identify and relocate non-class content from dhcharts.py and dhtrades.py**
   
   Before deleting these files, identify any utility functions or variables that need to be preserved:
   
   - **Step A**: Scan dhcharts.py for non-class content:
     ```bash
     # Find functions that aren't part of classes
     grep -n "^def " /home/dusty/git/backtesting/dhtrader/dhcharts.py
     ```
     - If utility functions exist (e.g., helper functions for chart analysis), move them to:
       - `dhutil.py` if they're general utilities
       - `dhstore.py` if they're storage-related
   
   - **Step B**: Scan dhtrades.py for non-class content:
     ```bash
     # Find functions and module-level variables
     grep -n "^def \|^[A-Z_]* = " /home/dusty/git/backtesting/dhtrader/dhtrades.py
     ```
     - Move any utility functions to `dhutil.py`
     - Move any constants to `dhcommon.py` if they're shared
   
   - **Step C**: After relocating all non-class content:
     ```bash
     # DELETE the files entirely
     rm /home/dusty/git/backtesting/dhtrader/dhcharts.py
     rm /home/dusty/git/backtesting/dhtrader/dhtrades.py
     ```
   
   **Important**: All classes have already been moved to dhtypes.py in Phase 1, so these files should contain only utility functions/variables at this point.

5. **Validation - Step 2**
   ```bash
   # Test imports individually
   python3 -c "from dhtrader.dhtypes import Candle; print('✓ dhtypes imports')"
   python3 -c "from dhtrader.dhstore import store_candle; print('✓ dhstore imports')"
   
   # Verify dhcharts.py and dhtrades.py don't exist
   test ! -f /home/dusty/git/backtesting/dhtrader/dhcharts.py && echo "✓ dhcharts.py deleted"
   test ! -f /home/dusty/git/backtesting/dhtrader/dhtrades.py && echo "✓ dhtrades.py deleted"
   
   # Test combined imports work
   python3 -c "from dhtrader.dhtypes import Candle, Chart, Trade; from dhtrader.dhstore import get_candles, store_candle; print('✓ No circular imports!')"
   ```

---

### Phase 3: Update All Module Imports

**Objective**: Ensure all remaining modules properly import from dhtypes and dhstore, not the old circular sources.

6. **Update `/home/dusty/git/backtesting/dhtrader/dhutil.py`**
   - Change imports:
     ```python
     # OLD:
     from dhcharts import Candle, Symbol
     from dhtrades import Trade

     # NEW:
     from dhtypes import Candle, Symbol, Trade
     from dhstore import store_candle
     from dhcommon import LOGGER
     ```

7. **Update `/home/dusty/git/backtesting/dhtrader/dhmongo.py`**
   - Verify no problematic imports (should only use dhtypes for type hints)
   - Add any missing dhtypes imports if type hints reference moved classes

8. **Update all test files**
   - `tests/test_dhcharts.py` - **Consider renaming or eliminating**:
     - If testing chart-specific logic: rename to `tests/test_chart_analysis.py`
     - If only testing classes: merge into `tests/test_dhtypes.py`
     - Update all imports:
       ```python
       # OLD:
       from dhcharts import Candle, Chart

       # NEW:
       from dhtypes import Candle, Chart
       from dhstore import store_candle, get_candles
       ```
   - `tests/test_dhtrades.py` - **Consider renaming or eliminating**:
     - Similar approach: rename to `tests/test_trade_analysis.py` or merge into `tests/test_dhtypes.py`
   - `tests/test_dhstore.py`: Update to import from dhtypes
   - All other test files: Replace `from dhcharts import X` and `from dhtrades import Y` with appropriate dhtypes imports

9. **Validation - Step 3**
    ```bash
    # Test that all modules import without circular errors
    python3 -c "
    from dhtrader.dhtypes import Candle, Symbol, Trade, Chart, Backtest
    from dhtrader.dhstore import get_candles, store_candle
    from dhtrader.dhutil import SomeUtilFunction
    print('✓ All modules import successfully')
    "
    
    # Verify deleted files cannot be imported
    python3 -c "try:
        from dhtrader import dhcharts
        print('✗ ERROR: dhcharts.py still exists')
    except ModuleNotFoundError:
        print('✓ dhcharts.py successfully eliminated')"
    ```

---

### Phase 4: Create Public API

**Objective**: Maintain backward compatibility while establishing the new structure.

10. **Update `/home/dusty/git/backtesting/dhtrader/__init__.py`**
    - Define public API that re-exports key classes for backward compatibility:
      ```python
      # dhtrader/__init__.py
      # Import from internal modules
      from .dhtypes import (
          Candle, Event, Symbol, Indicator, IndicatorSMA, IndicatorEMA,
          Chart, Day, Trade, TradeSeries, Backtest
      )
      from .dhstore import (
          # Important: carefully expose only needed functions
          get_candles, store_candle, get_events, store_trade
      )
      # Note: dhcharts and dhtrades have been eliminated

      __all__ = [
          'Candle', 'Event', 'Symbol', 'Indicator', 'IndicatorSMA', 'IndicatorEMA',
          'Chart', 'Day', 'Trade', 'TradeSeries', 'Backtest',
          'get_candles', 'store_candle', 'get_events', 'store_trade',
      ]
      ```

    - This allows old code to continue working:
      ```python
      # Still works:
      from dhtrader import Candle, Chart, Backtest

      # Also works (more explicit):
      from dhtrader.dhtypes import Candle, Chart
      from dhtrader.dhstore import get_candles
      ```

12. **Validation - Step 4**
    ```bash
    # Test backward compatibility
    python3 -c "from dhtrader import Candle, Chart, Trade; print('✓ Backward compatible imports work')"
    ```

---

### Phase 5: Comprehensive Testing

**Objective**: Verify the refactored architecture works correctly.

13. **Run full test suite**
    ```bash
    cd /home/dusty/git/backtesting
    pytest dhtrader/tests/ -v
    ```
    - All existing tests should pass
    - No import-related failures

14. **Check for circular imports**
    ```bash
    python3 -c "
    import sys
    try:
        from dhtrader import dhcharts
        from dhtrader import dhstore
        from dhtrader import dhutil
        from dhtrader import dhtrades
        print('✓ No circular imports detected')
    except ImportError as e:
        print(f'✗ Import error: {e}')
        sys.exit(1)
    "
    ```

15. **Verify PEP 8 compliance**
    ```bash
    # Compile check
    python3 -m py_compile dhtrader/dhtypes.py dhtrader/dhstore.py dhtrader/dhcharts.py

    # Optional: Run linter if available
    # flake8 dhtrader/ --max-line-length=79
    ```

16. **Static type checking (optional but recommended)**
    ```bash
    # If using mypy
    # mypy dhtrader/ --ignore-missing-imports

    # Or pyright
    # pyright dhtrader/
    ```

---

### Phase 6: Documentation

**Objective**: Record the new architecture for future developers.

17. **Create `/home/dusty/git/backtesting/dhtrader/ARCHITECTURE_REFACTORED.md`**
    - Document the circular import problem and solution
    - Show the new module dependency graph
    - Provide import guidelines for new code
    - Include examples of proper usage

18. **Update existing documentation**
    - Update any existing ARCHITECTURE.md or README files
    - Reference the PEP 8 compliance improvements
    - Note the functional API changes (`.store()` becomes `store_candle()`)

---

### Phase 7: Git Commit & Review

19. **Commit with clear history**
    ```bash
    # Commit 1: Create dhtypes foundation
    git add dhtrader/dhtypes.py
    git commit -m "refactor: create dhtypes module with pure data classes from dhcharts and dhtrades"

    # Commit 2: Eliminate dhcharts.py and dhtrades.py
    git rm dhtrader/dhcharts.py dhtrader/dhtrades.py
    git add dhtrader/dhstore.py dhtrader/dhutil.py  # Updated with moved functions
    git commit -m "refactor: eliminate dhcharts and dhtrades, consolidate into dhtypes/dhstore/dhutil"

    # Commit 3: Update secondary modules
    git add dhtrader/dhutil.py dhtrader/dhmongo.py
    git commit -m "refactor: update imports to use dhtypes"

    # Commit 4: Test files
    git add dhtrader/tests/
    git commit -m "test: update test imports for new module structure"

    # Commit 5: Public API
    git add dhtrader/__init__.py
    git commit -m "refactor: update public API for backward compatibility"
    ```

20. **Create pull request for code review**
    - Reference the CIRCULAR_IMPORTS_STRATEGY.md document
    - Highlight that all tests pass
    - Note PEP 8 compliance improvements
    - Mention backward compatibility maintained

---

## 5. Handling Complex Refactoring Cases

### 5.1 Storage Function Patterns

Classes that were originally in dhcharts.py (like Chart) may have had methods that called storage functions like `get_events()`, `get_candles()`, etc. When moving these classes to dhtypes.py, these patterns should be refactored:

**Pattern 1: Dependency Injection** (Recommended)
```python
# OLD - dhcharts.py (circular):
class Chart:
    def __init__(self, symbol: Symbol):
        self.candles = get_candles(symbol)  # Imports from dhstore
        self.events = get_events(symbol)    # Creates circular import

# NEW - dhtypes.py + usage code:
class Chart:
    def __init__(self, symbol: Symbol, candles: List[Candle], events: List[Event]):
        # No imports needed; data passed in
        self.candles = candles
        self.events = events

# Usage code (caller responsibility):
from dhstore import get_candles, get_events
from dhtypes import Chart

candles = get_candles(symbol)
events = get_events(symbol)
chart = Chart(symbol, candles, events)
```

**Pattern 2: Lazy Data Loading** (If absolutely necessary)
```python
# If you must load data internally, do it explicitly:
from dhstore import get_candles

class Chart:
    def __init__(self, symbol: Symbol):
        self.symbol = symbol
        self._candles = None  # Lazy load

    @property
    def candles(self) -> List[Candle]:
        if self._candles is None:
            from dhstore import get_candles
            self._candles = get_candles(self.symbol)
        return self._candles
```
**Note**: This still causes runtime circular imports but defers them. Prefer Pattern 1.

### 5.2 Symbol.market_is_open() and Similar Methods

Methods that need to query storage (like checking if market is open) should be handled functionally:

```python
# OLD - circular:
class Symbol:
    def market_is_open(self) -> bool:
        from dhstore import get_events
        return len(get_events(self)) > 0

# NEW - functional:
# dhtypes.py - just pure class
class Symbol:
    @property
    def ticker(self) -> str:
        return self._ticker

# dhstore.py - functions handle storage logic
def is_market_open(symbol: Symbol) -> bool:
    """Check if market is open for a symbol"""
    events = get_events(symbol)
    return len(events) > 0

# Usage:
from dhstore import is_market_open
from dhtypes import Symbol

if is_market_open(symbol):
    # market is open
```

### 5.3 Backward Compatibility Strategy

**Trade-off**: The PEP 8 Strict approach changes the public API. Users familiar with `candle.store()` must now call `store_candle(candle)`.

**Mitigation** - Provide convenience wrapper in `dhtrader/__init__.py` or dedicated compat module:

```python
# dhtrader/dhcompat.py (optional backward-compat layer)
from dhtypes import Candle as _Candle
from dhstore import store_candle as _store_candle

class CandleCompat(_Candle):
    """Backward-compatible Candle with .store() method"""
    def store(self) -> bool:
        return _store_candle(self)

# Old code can optionally use:
from dhtrader.dhcompat import CandleCompat
candle = CandleCompat(...)
candle.store()  # Still works
```

**Recommendation**: Don't create compat wrappers. Instead, update code to functional style and document the change in ARCHITECTURE_REFACTORED.md.

---

## 6. Benefits of the PEP 8 Strict Approach

### 6.1 Architectural Benefits
✓ **Complete Separation of Concerns**: Domain models have zero knowledge of persistence
✓ **Dependency Inversion Principle**: Storage layer depends on domain layer, never reverse
✓ **Testability**: Test dhtypes.Candle in complete isolation without any mock setup
✓ **Maintainability**: Clear module responsibilities with no ambiguity
✓ **Extensibility**: Easily add new storage backends or data sources without touching domain classes

### 6.2 Standards Compliance
✓ **PEP 8 Strict**: All imports at module top, no lazy imports in method bodies
✓ **PEP 484**: Full type hints with proper forward references
✓ **PEP 20 (Zen of Python)**: "Explicit is better than implicit" - functions make intent clear
✓ **Industry Standard**: Matches patterns used in Django, Flask, SQLAlchemy, FastAPI

### 6.3 Engineering Benefits
✓ **IDE/Type Checker Support**: Full autocompletion and type validation from first import
✓ **Import Reliability**: No import order race conditions; imports always work
✓ **Debugging**: Clearer stack traces; storage function calls are explicit in backtrace
✓ **Performance**: Potentially faster imports due to reduced initialization chains
✓ **CI/CD Integration**: Import analysis tools can reliably scan module dependencies

### 6.4 Code Quality Improvements
✓ **Functional Programming**: Encourages thinking about data transformations
✓ **Immutability**: Domain objects can be more easily kept immutable
✓ **Reusability**: Storage functions callable from any context (CLI, web, tests)
✓ **Documentation**: Code structure documents the architecture (dependencies flow downward)

---

## 7. Risk Assessment & Mitigation

### 7.1 Risks Specific to PEP 8 Strict Approach

| Risk | Likelihood | Severity | Mitigation |
|------|------------|----------|-----------|
| **Breaking public API** | High | High | Comprehensive migration guide; backward-compat re-exports in __init__.py |
| **Method to function signature confusion** | Medium | Medium | Clear documentation of new patterns (store_candle(c) vs c.store()) |
| **Missed method → function migrations** | Medium | Medium | Grep for `.store(`, `.save(`, etc.; comprehensive test coverage |
| **Complex state transitions** | Low | High | Refactor complex cases incrementally; test each phase |
| **Test failures due to import order** | Low | Medium | Run full suite frequently; CI/CD will catch |

### 7.2 Mitigation Strategies

1. **Comprehensive Search Before Starting**
   ```bash
   # Find all storage-related methods that need refactoring
   grep -r "\.store(" dhtrader/ --include="*.py"
   grep -r "def store" dhtrader/ --include="*.py"
   grep -r "def save" dhtrader/ --include="*.py"
   ```

2. **Incremental Testing**
   - After Phase 1 (create dhtypes): Run tests
   - After Phase 2 (remove circular): Run tests
   - After Phase 3 (update imports): Run tests
   - After Phase 4 (API creation): Run tests

3. **Type Checking Throughout**
   ```bash
   # After each phase
   python3 -m py_compile dhtrader/*.py
   # Optional: mypy or pyright if available
   ```

4. **Documentation of Breaking Changes**
   - Maintain version notes
   - Provide before/after examples
   - Show migration path for dependent code

---

## 8. Testing Strategy for PEP 8 Strict Refactoring

### 8.1 Test Coverage Checklist

- [ ] **Import tests**: Verify no circular imports on module load
- [ ] **Unit tests**: All existing unit tests pass unchanged
- [ ] **Integration tests**: Cross-module functionality works (Chart + dhstore)
- [ ] **Type hint tests**: mypy/pyright validation passes
- [ ] **Backward compatibility tests**: Old import paths still work via __init__.py

### 8.2 New Test Patterns

```python
# tests/test_dhtypes.py - NEW: Test pure domain classes without storage
from dhtypes import Candle, Event, Symbol

def test_candle_is_bullish():
    candle = Candle(o=100, h=105, l=99, c=104, v=1000)
    assert candle.is_bullish() == True
    # No imports of dhstore; completely isolated

def test_symbol_creation():
    symbol = Symbol("ES", "futures")
    assert symbol.ticker == "ES"
    # Domain model tested independently

# tests/test_dhstore_functions.py - NEW: Test storage functions directly
from dhstore import store_candle, get_candles
from dhtypes import Candle

def test_store_candle():
    candle = Candle(...)
    result = store_candle(candle)
    assert result == True

# tests/test_integration.py - UPDATED: Integration tests as before
from dhtypes import Candle
from dhstore import store_candle, get_candles

def test_store_and_retrieve():
    candle = Candle(...)
    store_candle(candle)
    retrieved = get_candles(...)
    assert len(retrieved) > 0
```

### 8.3 Test Execution Commands

```bash
# Run all tests after each phase
pytest dhtrader/tests/ -v

# Run specific test module
pytest dhtrader/tests/test_dhtypes.py -v

# Run with import tracing to verify no circular imports
python3 -c "
import sys
sys.path.insert(0, '/home/dusty/git/backtesting')
import importlib
importlib.import_module('dhtrader.dhcharts')
importlib.import_module('dhtrader.dhstore')
print('✓ No circular imports')
"
```

---

## 9. Migration Guide for Developers

### Old Code (With Circular Imports)
```python
# dhtrader/some_module.py
from dhcharts import Candle  # Could fail if dhstore not yet imported
from dhstore import get_candles, store_candle

candles = get_candles(...)
```

### New Code (After Refactoring - PEP 8 Strict)
```python
# dhtrader/some_module.py
from dhtypes import Candle  # Always safe, no circular imports
from dhstore import get_candles, store_candle

candles = get_candles(...)  # Returns List[Candle]
```

### Import Guidelines for Future Development

**Rule 1**: Always import data types from `dhtypes`
```python
from dhtypes import Candle, Event, Symbol, Trade, Backtest, Chart
```

**Rule 2**: Import storage functions from `dhstore`
```python
from dhstore import get_candles, store_candle, get_events, store_trade
```

**Rule 3**: Import utility functions from `dhutil`
```python
from dhutil import some_utility_function  # If needed
```

**Rule 4**: Never import from dhcharts or dhtrades (these files are eliminated)
```python
# WRONG - these files no longer exist:
from dhcharts import Candle
from dhtrades import Trade

# CORRECT:
from dhtypes import Candle, Trade
```

#### Before/After Examples

**Pattern 1: Creating and storing objects**
```python
# BEFORE (potentially circular):
my_candle = Candle(o=100, h=105, l=99, c=104, v=1000)
my_candle.store()  # Method on class

# AFTER (PEP 8 functional):
from dhtypes import Candle
from dhstore import store_candle

my_candle = Candle(o=100, h=105, l=99, c=104, v=1000)
store_candle(my_candle)  # Function call
```

**Pattern 2: Retrieving data**
```python
# BEFORE (circular risk):
from dhcharts import Symbol
candles = symbol.get_candles()  # Method on class

# AFTER (PEP 8 functional):
from dhtypes import Symbol
from dhstore import get_candles

symbol = Symbol("ES", "futures")
candles = get_candles(symbol, start_dt, end_dt)  # Function call
```

**Pattern 3: Analysis workflow**
```python
# BEFORE (circular):
from dhcharts import Chart, Candle
chart = Chart()
chart.add_candle(c)

# AFTER (PEP 8 functional):
from dhtypes import Chart, Candle
from dhstore import get_candles

candles = get_candles(symbol, start_dt, end_dt)
chart = Chart(symbol, candles)  # Dependency injection
chart.add_indicator('SMA', 20)
```

---

## 10. Success Criteria

The elimination of circular imports will be considered successful when:

1. ✓ **No circular import errors on module load**
   ```bash
   python3 -c "from dhtrader import dhtypes, dhstore, dhutil"
   ```
   Should produce no errors or warnings

2. ✓ **All existing tests pass**
   ```bash
   pytest dhtrader/tests/ -v
   ```
   100% pass rate maintained (after updating test imports)

3. ✓ **Type checker validation passes**
   ```bash
   python3 -m py_compile dhtrader/*.py  # Syntax check
   # Optional: mypy/pyright for type checking
   ```

4. ✓ **Backward compatibility maintained**
   ```bash
   python3 -c "from dhtrader import Candle, Chart, Trade"
   ```
   Old import paths still work via __init__.py re-exports

5. ✓ **Clean module dependency graph**
   - dhtypes.py: Pure data classes (imports only: dhcommon, stdlib)
   - dhstore.py: Persistence layer (imports: dhtypes, dhmongo, dhcommon)
   - dhutil.py: Utility layer (imports: dhtypes, dhstore, dhcommon)
   - One-way dependency chain with no bi-directional imports
   - dhcharts.py and dhtrades.py eliminated (classes moved to dhtypes.py)

6. ✓ **IDE/Type checker support improved**
   - Autocompletion works on first import
   - No "unresolved import" warnings
   - Type hints resolve correctly without import errors

---

## 11. Quick Reference: File-by-File Changes

### dhtypes.py (NEW FILE)
- **Purpose**: Pure domain models
- **Contains**: All class definitions (Candle, Event, Symbol, Indicator*, Chart, Day, Trade, TradeSeries, Backtest)
- **Imports**: Only dhcommon and stdlib
- **Size**: ~3,000 lines (combined from dhcharts + dhtrades)
- **Key Point**: NO storage methods, NO persistence logic
- **Source**: Classes extracted from dhcharts.py and dhtrades.py (which are now deleted)

### dhstore.py (REFACTORED)
- **Purpose**: All persistence operations as functions
- **Key Changes**:
  - Add: `from dhtypes import Candle, Event, Symbol, Trade, ...`
  - Remove: `from dhcharts import ...`, `from dhtrades import ...`
  - Convert methods to functions: `store_candle()`, `store_trade()`, etc.
- **Imports**: dhtypes, dhmongo, dhcommon
- **Result**: No circular imports possible (dhtypes never imports dhstore)

### dhcharts.py (ELIMINATED)
- **Status**: **File deleted entirely**
- **Classes moved to**: dhtypes.py
- **Utility functions moved to**: dhutil.py or dhstore.py (as appropriate)
- **Reason**: Eliminates circular import source and simplifies architecture

### dhtrades.py (ELIMINATED)
- **Status**: **File deleted entirely**
- **Classes moved to**: dhtypes.py (Trade, TradeSeries, Backtest)
- **Utility functions moved to**: dhutil.py or dhstore.py (as appropriate)
- **Reason**: Consolidates all domain models in one location

### dhutil.py (UPDATED)
- **Purpose**: Utility functions
- **Key Changes**:
  - Add: `from dhtypes import Candle, Symbol, Trade, ...`
  - Remove: `from dhcharts import ...`, `from dhtrades import ...`
  - Receives: Any utility functions from dhcharts.py and dhtrades.py
- **Imports**: dhtypes, dhstore, dhcommon
  - Add: `from dhtypes import Candle, Symbol, Trade, ...`
  - Remove: `from dhcharts import ...`, `from dhtrades import ...`
- **Imports**: dhtypes, dhstore, dhcommon

### __init__.py (UPDATED)
- **Purpose**: Public API and backward compatibility
- **Key Changes**:
  - Export all key classes from dhtypes
  - Re-export storage functions from dhstore
  - Maintains backward-compatible import paths
- **Example**:
  ```python
  from .dhtypes import Candle, Event, Symbol, Trade, Backtest, Chart
  from .dhstore import store_candle, get_candles, store_trade

  __all__ = ['Candle', 'Event', 'Symbol', 'Trade', ...
  ```

---

## 12. Troubleshooting Common Issues

### Issue: "ImportError: circular import detected"
**Cause**: Missed a place where dhstore/dhcharts were imported in reverse
**Solution**:
```bash
# Find problematic imports
grep -r "from dhcharts import" dhtrader/dhstore.py
grep -r "from dhtrades import" dhtrader/dhstore.py

# Should return nothing - if found, remove those imports
```

### Issue: "ModuleNotFoundError: No module named 'dhtypes'"
**Cause**: File not created or wrong location
**Solution**:
```bash
# Verify dhtypes.py exists
ls -la /home/dusty/git/backtesting/dhtrader/dhtypes.py

# Verify it's importable
python3 -c "from dhtrader.dhtypes import Candle"
```

### Issue: Tests fail with attribute errors
**Cause**: Tests still using old import paths or expecting `.store()` methods
**Solution**: Update test imports and call `store_candle()` instead of `.store()`
```python
# OLD:
candle = Candle(...)
candle.store()

# NEW:
from dhstore import store_candle
candle = Candle(...)
store_candle(candle)
```

### Issue: Type checker shows "unresolved import"
**Cause**: Missing or incorrect type hints
**Solution**: Ensure dhtypes module has proper `__all__` exports and type annotations
```python
# In dhtypes.py
__all__ = ['Candle', 'Event', 'Symbol', ...]

class Candle:
    def __init__(self, o: float, h: float, ...):
        ...
```

### Issue: "ModuleNotFoundError: No module named 'dhcharts'" or "'dhtrades'"
**Cause**: Code still trying to import from eliminated files
**Solution**: Update all imports to use dhtypes
```bash
# Find all remaining references
grep -r "from dhcharts import" .
grep -r "from dhtrades import" .
grep -r "import dhcharts" .
grep -r "import dhtrades" .

# Update each file:
# OLD:
from dhcharts import Candle
from dhtrades import Trade

# NEW:
from dhtypes import Candle, Trade
```

---

## 13. References & Resources

### Python Enhancement Proposals (PEPs)
- **PEP 8**: Style Guide for Python Code (https://peps.python.org/pep-0008/)
- **PEP 484**: Type Hints (https://peps.python.org/pep-0484/)
- **PEP 20**: Zen of Python (https://peps.python.org/pep-0020/)

### Dependency Inversion Principle
- Robert C. Martin's SOLID principles
- Ensure high-level modules don't depend on low-level implementation details

### Import Best Practices
- Always import at module top (PEP 8 compliance)
- Use explicit imports (avoid wildcard `import *`)
- Arrange imports: standard library → third-party → local
- Keep imports simple and minimal

---

## 14. Document History

- **v1.0** - Initial comprehensive strategy with three approaches
- **v2.0** - Added PEP 8 Strict exploration
- **v3.0** - Consolidated to PEP 8 Strict only
- **v3.1** - Updated strategy to completely eliminate dhcharts.py and dhtrades.py (current)

