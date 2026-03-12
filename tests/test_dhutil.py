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


def test_read_candles_from_csv():
    """Verify read_candles_from_csv reads and filters CSV candles correctly."""
    # Returns a list of Candle objects when dates span all rows
    result = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert isinstance(result, list)
    assert len(result) > 0
    for c in result:
        assert isinstance(c, Candle)
    # All 10 candles in the test file are returned for a full-range query
    assert len(result) == 10
    # start_dt filter excludes earlier candles
    result_start = read_candles_from_csv(
        start_dt="2024-02-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result_start) == 5
    for c in result_start:
        assert dt_as_dt(c.c_datetime) >= dt_as_dt("2024-02-01 00:00:00")
    # end_dt filter excludes later candles
    result_end = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-01-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result_end) == 5
    for c in result_end:
        assert dt_as_dt(c.c_datetime) <= dt_as_dt("2024-01-31 23:59:59")
    # Out-of-range dates return an empty list
    result_empty = read_candles_from_csv(
        start_dt="2025-01-01 00:00:00",
        end_dt="2025-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert result_empty == []
    # Candle attributes match values from the CSV source file
    result_one = read_candles_from_csv(
        start_dt="2024-01-01 18:00:00",
        end_dt="2024-01-01 18:00:00",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result_one) == 1
    c = result_one[0]
    assert c.c_datetime == "2024-01-01 18:00:00"
    assert c.c_open == 4818.00
    assert c.c_high == 4819.50
    assert c.c_low == 4815.75
    assert c.c_close == 4818.75
    assert c.c_volume == 1483
    # Default timeframe is 1m — check against the full-range result
    for candle in result:
        assert candle.c_timeframe == "1m"
    # Default symbol is ES — check against the full-range result
    for candle in result:
        assert candle.c_symbol.ticker == "ES"
    # Custom timeframe is applied when provided
    result_tf = read_candles_from_csv(
        start_dt="2024-01-01 00:00:00",
        end_dt="2024-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
        timeframe="5m",
    )
    for candle in result_tf:
        assert candle.c_timeframe == "5m"
    # Candles are returned in CSV file order
    assert result_end[0].c_datetime == "2024-01-01 18:00:00"
    assert result_end[-1].c_datetime == "2024-01-01 18:04:00"
