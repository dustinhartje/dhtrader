# Testdata Directory Documentation

## Table of Contents
- [Overview](#overview)
- [Test Datasets](#test-datasets)
  - [set1](#set1)
- [Test Data Creation and Usage](#test-data-creation-and-usage)
  - [Data Utilities Library (`testdata.py`)](#data-utilities-library-testdatapy)
  - [Data Extraction (`extract_testdata.py`)](#data-extraction-extract_testdatapy)
  - [Data Rebuild Reference (`build_testdata.py`)](#data-rebuild-reference-build_testdatapy)
  - [Set Data File Types](#set-data-file-types)
- [Expected Results Manual Validation](#expected-results-manual-validation)
- [AI Run Validation](#ai-run-validation)
  - [Overview](#overview-1)
  - [Calculation and Comparison Prompt](#calculation-and-comparison-prompt)
  - [AI Generated Scripts and Data Files](#ai-generated-scripts-and-data-files)

## Overview

The `testdata` directory contains test datasets, validation scripts, and analysis tools for the ES futures backtesting framework. The primary purpose is to provide a consistent, reproducible dataset for unit testing/validating backtest calculations and ensuring algorithm accuracy.

## Test Datasets
### set1
3 weeks of ES 1m and e1h candles, one long and one short Backtest with one TradeSeries each.  Includes candles and all potential trades for all hours that the market was open.  No deoverlapping or other processing has been done to this dataset.  Avoided holidays due to open Issue #118 with early market closures not closing Trades at end of day.
* start_dt: 2025-11-30 18:00:00
* end_dt:   2025-12-19 17:00:00
* bt_ids: "BacktestEMABounce-eth_e1h_9", "BacktestEMAReject-eth_e1h_9"
* ts_ids: "BacktestEMABounce-eth_e1h_9_s80-p160-o0", "BacktestEMAReject-eth_e1h_9_s80-p160-o40"

## Test Data Creation and Usage

### Data Utilities Library (`testdata.py`)

Provides `Extractor` and `Rebuilder` classes for managing test data lifecycle.

**Purpose**: Core utilities for extracting from storage and rebuilding from JSON

**Key Classes**:
- `Extractor` - Extract objects from dhstore and save as JSON/CSV
- `Rebuilder` - Rebuild framework objects from JSON files

### Data Extraction (`extract_testdata.py`)

Extracts real trading data from the MongoDB storage system and saves it as JSON files for testing purposes.

**Purpose**: Create reproducible test datasets from production data

**Key Functions**:
- `extract_testdata()` - Main extraction function that pulls candles, trades, and metadata
- Exports trade data including candles, trades, tradeseries, and backtests to disk in setN_*.json and setN_*.csv files.
- Creates metadata file with extraction parameters
- Asserts expected values during extraction to catch changes in source data.  Once defined and extracted, each set should remain static and does not need re-extraction unless it is accidentally altered in some way or one wishes to validate it has not changed in storage.

**Usage**: Run as a script to extract fresh test data from storage.

### Data Rebuild Reference (`build_testdata.py`)

Primarily used as reference for rebuilding setN data into dhtrader objects for testing purposes.

**Purpose**: Verify that test data can be accurately reconstructed into framework objects.  Provide code reference for use in building unit tests.

**Key Functions**:
- `rebuild_testdata()` - Rebuilds all test data from JSON into objects to validate accuracy
- Allows date filtering to rebuild partial datasets for specific test cases i.e. a short test with only a few hours vs several weeks of data.

**Usage**: Serves as a reference template for Rebuilder usage in unit tests. Can also verify test data integrity.

### Set Data File Types

**setN_metadata.json**
- Overall metadata for the test dataset, specifying details such as dates, backtests used, timeframes, trading_hours, etc

**setN_*_candles.json / .csv**
- Typically includes 1m and "timeframe" candles for the set

**setN_ind_dps_*ema.json / .csv**
- Indicator datapoints used to calculate the set

**setN_trades.json**
- All trades in the set including both directions stored as a list with one dictionary per TradeSeries

**setN_trades_long/short.json / .csv**
- All trades extracted for this set, split by direction

**setN_backtests.json / .csv**
- All Backtests included in the set

**setN_tradeseries.json / .csv**
- All TradeSeries included in the set

## Expected Results Manual Validation
A Google Sheets document was used to manually calculate expected results for set1 outside of my code with minimal review of code during buildout to ensure unbiased fresh thinking of the intended application of each value.  This was fine tuned until all results matched up, with some minor bug fixing in dhtrader code (see commits in Jan 2026) and updates to the spreadsheet until everything matched up to give strong conviction that the code is calculating accurately as intended.

The spreadsheet is not shared publically, this link is primarily for my personal reference in adding future testdata sets:
https://docs.google.com/spreadsheets/d/1MF1AVOJpeCcAsDO4-50UG-Yrq5o7WnoUuN6byuanPis/edit?pli=1&gid=0#gid=0

## AI Run Validation

### Overview
I used CoPilot in VSCode with the Claude Sonnet 4.5 model to calculate the expected Trades for set1 outside of my code, allowing it to write it's own scripts with the prompt below (which took several refinements to get all details correct).  It proceeded to caclculate all expected Trades based on *set1_1m_candles.csv*, *set1_e1h_candles.csv*, and *set1_ind_dps_e1h9ema.csv* data only and compared it's results to *set1_trades_long.csv*, and *set1_trades_short.csv*, validating that the expected results match what my system calculated and stored.

Importantly, these AI scripts do not come anywhere near to recreating the full functionality of dhtrader + backtesting.  They only validate this specific dataset from an outside perspective.  The prompt may be useful as a template for future validations and data troubleshooting.

### Calculation and Comparison Prompt
```
All data represents ES trading data.  set1_1m_candles.csv represents 1 minute price chart candles.  set1_e1h_candles.csv represents 1 hour price chart candles including regular and extended hours.  set1_ind_dps_e1h9ma.csv represents the exponential hourly moving average with a length of 9.

Using the following instructions, calculate all trades that would have occurred as described and write them to a CSV file called "ai_trades_long.csv".

profit_dollars = 40
stop_dollars = 20
offset_ticks = 0
direction = long

For each hour, there is an entry target that is equal to the prior hour's EMA value rounded up to the nearest 0.25 minus offset_ticks * 0.25 for long trades.  For short trades use the prior hour's EMA value rounded down to the nearest 0.25 plus offeset_ticks * 0.25.  Only if the price opened above the entry target in the first 1 minute candle of the hour for long trades, or below for short trades, then you may add trades during that hour, otherwise skip to the next hour.  For valid hours, identify the first time that the price went at least 0.25 below the entry target for long trades or above for short trades during that hour.  Note a trade opening at this time with an "entry_price" equal to the entry target and include the prior ema value in a column called "prev_e1h_9ema_value" and time in a column called "prev_e1h_9ema_time".  Note that the prior ema value may be more than 1 hour before the current hour if the market was closed during that time, but always prefer the most recent ema datapoint you can find before the current hour.  That trade will have a "profit_target" equal to the entry target plus profit_dollars for long trades or minutes profit_dollars for short trades.  That trade will have a "stop_target" equal to the entry target minus stop_dollars for long trades or plus stop_dollars for short trades.  Using the 1 minute candle data, record the highest ("high_price") and lowest ("low_price") seen until the price does one of these 3 things: hits the stop_target, reaches profit_target + 0.25 for long / profit_target - 0.25 for short, or the time reaches 15:55:00 after the trade was entered.  Note that trades entering on a given day after 15:55 should be allowed to run until 15:55 the next day if necessary.

If the price hit the stop_target first or hits both stop_target and profit_target in the same 1 minute candle, set exit_price equal to the stop target and also set the low_price (if long) or high_price (if short) equal to stop_target.  If it went through the profit_target first without touching the stop_target in the same 1 minute candle, set the exit_price equal to the profit_target and also set the high_price (if long) or low_price (if short) equal to profit_target.  If it reached 15:55:00 without reaching either stop_target or profit_target, set the exit price equal to the opening price of the 15:55:00 candle.  In all cases, set the exit_time equal to the c_datetime of the final 1 minute candle that triggered the exit.

Special situations may alter the logic above as follows:
1) If the profit_target is triggered in the opening 1 minute candle, do not consider it valid to trigger an exit unless the opening 1 minute candle closes at or beyond the profit_target.
2) if the stop_target and profit_target are both hit in the same 1 minute candle, record the high_price (if long) or low_price (if short) as equal to the profit_target.
3) when a trade exits at 15:55, do not use the 15:55 candle to update the high_price or low_price
4) no trade should ever be opened between 15:55 and 17:59

Then run a similar calculation for short trades with the following changes:
1) offset_ticks = 40
2) direction = short
3) write these results to "ai_trades_short.csv"

After creating the csvs and summarizing the calculation results, use compare_ai_vs_set1.py to compare both the long and the short trade sets separately and display a summary of results in the chat window.
```

### AI Generated Scripts and Data Files
### calculate_ai_trades.py

**Purpose**: Recreate Backtest trade calculations from scratch using only documented rules

**Key Features**:
- Created by CoPilot using Claude Sonnet 4.5 model via VSCode chat
- Implements both long (bounce) and short (rejection) strategies
- Handles all edge cases: entry candle special case, stop/profit collisions, autoclose logic
- Validates against production Backtest results in set1

**Output**: ai_trades_long.csv and ai_trades_short.csv

### compare_ai_vs_set1.py

**Purpose**: Compare AI-calculated long trades against set1 reference trades

**IMPORTANT**: These may not run directly after porting from their original repo and other modifications to the process.  Use with caution / update as needed

**Key Features**:
- Compares AI calculated and set1 trade sets, matching by entry time and reviewing all fields for differences
- Reports match statistics, field-level differences, and trades missing from either set

**ai_trades_long/short.csv**
- Long/Short trades calculated by the AI script (`calculate_ai_trades.py`)
- Should match set1_trades_long/short.csv exactly
