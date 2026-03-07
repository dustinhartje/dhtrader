"""Tests for dhutil candle utility functions."""
import os
from dhtrader import (
    Candle,
    dt_as_dt,
    read_candles_from_csv,
)


# Path to the test CSV file included in the repo
TESTCANDLES_CSV = os.path.join(
    os.path.dirname(__file__), '..', 'testcandles.csv'
)


# #############################################################################
# read_candles_from_csv
# #############################################################################

def test_read_candles_from_csv_returns_list():
    """Verify read_candles_from_csv returns a list."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert isinstance(result, list)


def test_read_candles_from_csv_returns_candle_objects():
    """Verify read_candles_from_csv returns Candle objects."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result) > 0
    for c in result:
        assert isinstance(c, Candle)


def test_read_candles_from_csv_correct_count():
    """Verify read_candles_from_csv returns all 10 candles in test file."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result) == 10


def test_read_candles_from_csv_start_filter():
    """Verify read_candles_from_csv respects start_dt filter."""
    result = read_candles_from_csv(
        start_dt="2024-02-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    # testcandles.csv has 5 candles starting 2024-02-01
    assert len(result) == 5
    for c in result:
        assert dt_as_dt(c.c_datetime) >= dt_as_dt("2024-02-01 00:00:00")


def test_read_candles_from_csv_end_filter():
    """Verify read_candles_from_csv respects end_dt filter."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-01-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    # testcandles.csv has 5 candles in January 2024
    assert len(result) == 5
    for c in result:
        assert dt_as_dt(c.c_datetime) <= dt_as_dt("2024-01-31 23:59:59")


def test_read_candles_from_csv_empty_result():
    """Verify read_candles_from_csv returns empty list for out-of-range dates.
    """
    result = read_candles_from_csv(
        start_dt="2025-01-01 00:00:00",
        end_dt="2025-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert result == []


def test_read_candles_from_csv_candle_attributes():
    """Verify read_candles_from_csv populates Candle attributes correctly."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 18:00:00",
        end_dt="2024-01-01 18:00:00",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result) == 1
    c = result[0]
    assert c.c_datetime == "2024-01-01 18:00:00"
    assert c.c_open == 4818.00
    assert c.c_high == 4819.50
    assert c.c_low == 4815.75
    assert c.c_close == 4818.75
    assert c.c_volume == 1483
    assert c.c_timeframe == "1m"


def test_read_candles_from_csv_default_timeframe():
    """Verify read_candles_from_csv uses 1m as the default timeframe."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    for c in result:
        assert c.c_timeframe == "1m"


def test_read_candles_from_csv_default_symbol():
    """Verify read_candles_from_csv uses ES as the default symbol."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    for c in result:
        assert c.c_symbol.ticker == "ES"


def test_read_candles_from_csv_custom_timeframe():
    """Verify read_candles_from_csv uses provided timeframe argument."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
        timeframe="5m",
    )
    for c in result:
        assert c.c_timeframe == "5m"


def test_read_candles_from_csv_ordering():
    """Verify read_candles_from_csv returns candles in CSV file order."""
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-01-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert result[0].c_datetime == "2024-01-01 18:00:00"
    assert result[-1].c_datetime == "2024-01-01 18:04:00"
