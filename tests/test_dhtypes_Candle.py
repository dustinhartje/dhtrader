"""Tests for Candle creation, validation, and output methods."""
import json
import pytest
from dhtrader import (
    Candle)


@pytest.fixture
def candle():
    """Create and return a default ES 1m Candle fixture."""
    return Candle(c_datetime="2025-01-02 12:00:00",
                  c_timeframe="1m",
                  c_open=5000,
                  c_high=5007.75,
                  c_low=4995.5,
                  c_close=5002,
                  c_volume=1501,
                  c_symbol="ES",
                  )


def test_Candle_create_and_verify_common_methods():
    """Test Candle __init__ values, __eq__, __ne__, __str__, __repr__,
    to_clean_dict, to_json, pretty, and brief.
    """
    candle = Candle(c_datetime="2025-01-02 12:00:00",
                    c_timeframe="1m",
                    c_open=5000,
                    c_high=5007.75,
                    c_low=4995.5,
                    c_close=5002,
                    c_volume=1501,
                    c_symbol="ES",
                    )
    assert isinstance(candle, Candle)
    # __init__
    assert candle.c_datetime == "2025-01-02 12:00:00"
    assert candle.c_timeframe == "1m"
    assert candle.c_open == 5000.0
    assert candle.c_high == 5007.75
    assert candle.c_low == 4995.5
    assert candle.c_close == 5002.0
    assert candle.c_volume == 1501
    assert candle.c_symbol.ticker == "ES"
    assert candle.c_tags == []
    assert isinstance(candle.c_epoch, int)
    assert candle.c_date == "2025-01-02"
    assert candle.c_time == "12:00:00"
    assert candle.c_end_datetime == "2025-01-02 12:01:00"
    assert candle.c_size == 12.25
    assert candle.c_body_size == 2.0
    assert candle.c_upper_wick_size == 5.75
    assert candle.c_lower_wick_size == 4.5
    assert isinstance(candle.c_body_perc, float)
    assert isinstance(candle.c_upper_wick_perc, float)
    assert isinstance(candle.c_lower_wick_perc, float)
    assert candle.c_direction == "bullish"
    assert candle.name == "None"
    expected_attrs = {
        "c_body_perc", "c_body_size", "c_close", "c_date",
        "c_datetime", "c_direction", "c_end_datetime", "c_epoch",
        "c_high", "c_low", "c_lower_wick_perc", "c_lower_wick_size",
        "c_open", "c_size", "c_symbol", "c_tags", "c_time",
        "c_timeframe", "c_upper_wick_perc", "c_upper_wick_size",
        "c_volume", "name",
    }
    actual_attrs = set(vars(candle).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "Candle attributes changed. Update this test's "
        "__init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )
    other = Candle(c_datetime="2025-01-02 12:00:00",
                   c_timeframe="1m",
                   c_open=5000,
                   c_high=5007.75,
                   c_low=4995.5,
                   c_close=5002,
                   c_volume=1501,
                   c_symbol="ES",
                   )
    diff = Candle(c_datetime="2025-01-02 12:00:00",
                  c_timeframe="1m",
                  c_open=5001,
                  c_high=5007.75,
                  c_low=4995.5,
                  c_close=5002,
                  c_volume=1501,
                  c_symbol="ES",
                  )
    # __eq__
    assert candle == other
    assert not (candle == diff)
    # __ne__
    assert not (candle != other)
    assert candle != diff
    # __str__
    assert isinstance(str(candle), str)
    assert len(str(candle)) > 0
    # __repr__
    assert isinstance(repr(candle), str)
    assert len(repr(candle)) > 0
    assert str(candle) == repr(candle)
    # to_clean_dict
    d = candle.to_clean_dict()
    assert isinstance(d, dict)
    assert "c_datetime" in d
    assert "c_open" in d
    assert "c_high" in d
    assert "c_low" in d
    assert "c_close" in d
    assert "c_volume" in d
    assert d["c_datetime"] == "2025-01-02 12:00:00"
    assert d["c_open"] == 5000.0
    assert d["c_symbol"] == "ES"
    # to_json
    j = candle.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["c_datetime"] == "2025-01-02 12:00:00"
    assert parsed["c_open"] == 5000.0
    # pretty
    assert isinstance(candle.pretty(), str)
    assert len(candle.pretty().splitlines()) == 24
    # brief
    result = candle.brief()
    assert result == ("ES 1m 2025-01-02 12:00:00 | "
                      "O: 5000.0 H: 5007.75 L: 4995.5 C: 5002.0 V: 1501")


def test_Candle_calculated_attributes(candle):
    """Verify Candle computed attributes are correct on creation."""
    # Size, body, and wick calculations
    assert candle.c_size == abs(candle.c_high - candle.c_low)
    assert candle.c_body_size == abs(candle.c_open - candle.c_close)
    upper = candle.c_high - max(candle.c_open, candle.c_close)
    assert candle.c_upper_wick_size == upper
    lower = min(candle.c_open, candle.c_close) - candle.c_low
    assert candle.c_lower_wick_size == lower
    # Bullish when close > open
    assert candle.c_direction == "bullish"
    # Bearish when close < open
    bearish = Candle(c_datetime="2025-01-02 12:00:00",
                     c_timeframe="1m",
                     c_open=5005,
                     c_high=5007.75,
                     c_low=4995.5,
                     c_close=5002,
                     c_volume=1501,
                     c_symbol="ES",
                     )
    assert bearish.c_direction == "bearish"
    # Unchanged when close == open
    unchanged = Candle(c_datetime="2025-01-02 12:00:00",
                       c_timeframe="1m",
                       c_open=5002,
                       c_high=5007.75,
                       c_low=4995.5,
                       c_close=5002,
                       c_volume=1501,
                       c_symbol="ES",
                       )
    assert unchanged.c_direction == "unchanged"
    # Zero-size candle has None percentages
    zero = Candle(c_datetime="2025-01-02 12:00:00",
                  c_timeframe="1m",
                  c_open=5000,
                  c_high=5000,
                  c_low=5000,
                  c_close=5000,
                  c_volume=0,
                  c_symbol="ES",
                  )
    assert zero.c_body_perc is None
    assert zero.c_upper_wick_perc is None
    assert zero.c_lower_wick_perc is None
    # Date, time, epoch, and end_datetime are set correctly
    assert candle.c_date == "2025-01-02"
    assert candle.c_time == "12:00:00"
    assert isinstance(candle.c_epoch, int)
    assert candle.c_end_datetime == "2025-01-02 12:01:00"


def test_Candle_contains_price(candle):
    """Verify contains_price returns True within range, False outside."""
    # Prices within and at high/low boundaries are True
    assert candle.contains_price(5000)
    assert candle.contains_price(candle.c_low)
    assert candle.contains_price(candle.c_high)
    assert candle.contains_price(5003)
    # Prices outside high/low boundaries are False
    assert not candle.contains_price(candle.c_high + 0.01)
    assert not candle.contains_price(candle.c_low - 0.01)
    assert not candle.contains_price(0)
    assert not candle.contains_price(9999)


def test_Candle_contains_datetime(candle):
    """Verify contains_datetime uses exclusive start/end boundaries."""
    # A datetime strictly inside the candle window returns True
    # (c_datetime is 2025-01-02 12:00:00, window is open before end)
    assert candle.contains_datetime("2025-01-02 12:00:30")
    # At candle start is False (exclusive lower bound)
    assert not candle.contains_datetime("2025-01-02 12:00:00")
    # At candle end (12:01:00 for 1m) is False (exclusive upper bound)
    assert not candle.contains_datetime("2025-01-02 12:01:00")
    # Before candle start is False
    assert not candle.contains_datetime("2025-01-02 11:59:00")
