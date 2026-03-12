"""Tests for Candle creation, validation, and output methods."""
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


def test_Candle_create_and_verify_pretty():
    """Verify Candle.pretty() output line count."""
    out_candle = Candle(c_datetime="2025-01-02 12:00:00",
                        c_timeframe="1m",
                        c_open=5000,
                        c_high=5007.75,
                        c_low=4995.5,
                        c_close=5002,
                        c_volume=1501,
                        c_symbol="ES",
                        )
    assert isinstance(out_candle, Candle)
    assert len(out_candle.pretty().splitlines()) == 23


def test_Candle_calculated_attributes(candle):
    """Verify Candle computed attributes are correct on creation."""
    assert candle.c_size == abs(candle.c_high - candle.c_low)
    assert candle.c_body_size == abs(candle.c_open - candle.c_close)
    upper = candle.c_high - max(candle.c_open, candle.c_close)
    assert candle.c_upper_wick_size == upper
    lower = min(candle.c_open, candle.c_close) - candle.c_low
    assert candle.c_lower_wick_size == lower
    assert candle.c_direction == "bullish"


def test_Candle_direction_bearish():
    """Verify Candle.c_direction is bearish when close < open."""
    c = Candle(c_datetime="2025-01-02 12:00:00",
               c_timeframe="1m",
               c_open=5005,
               c_high=5007.75,
               c_low=4995.5,
               c_close=5002,
               c_volume=1501,
               c_symbol="ES",
               )
    assert c.c_direction == "bearish"


def test_Candle_direction_unchanged():
    """Verify Candle.c_direction is unchanged when open equals close."""
    c = Candle(c_datetime="2025-01-02 12:00:00",
               c_timeframe="1m",
               c_open=5002,
               c_high=5007.75,
               c_low=4995.5,
               c_close=5002,
               c_volume=1501,
               c_symbol="ES",
               )
    assert c.c_direction == "unchanged"


def test_Candle_zero_size_percs_are_none():
    """Verify Candle body/wick percentages are None when candle size is 0."""
    c = Candle(c_datetime="2025-01-02 12:00:00",
               c_timeframe="1m",
               c_open=5000,
               c_high=5000,
               c_low=5000,
               c_close=5000,
               c_volume=0,
               c_symbol="ES",
               )
    assert c.c_body_perc is None
    assert c.c_upper_wick_perc is None
    assert c.c_lower_wick_perc is None


def test_Candle_contains_price_true(candle):
    """Verify contains_price returns True for price within high/low range."""
    assert candle.contains_price(5000)
    assert candle.contains_price(candle.c_low)
    assert candle.contains_price(candle.c_high)
    assert candle.contains_price(5003)


def test_Candle_contains_price_false(candle):
    """Verify contains_price returns False for price outside high/low range."""
    assert not candle.contains_price(candle.c_high + 0.01)
    assert not candle.contains_price(candle.c_low - 0.01)
    assert not candle.contains_price(0)
    assert not candle.contains_price(9999)


def test_Candle_contains_datetime_true(candle):
    """Verify contains_datetime returns True for dt inside candle window."""
    # c_datetime is 2025-01-02 12:00:00, window is (open, end_datetime)
    assert candle.contains_datetime("2025-01-02 12:00:30")


def test_Candle_contains_datetime_false_at_start(candle):
    """Verify contains_datetime returns False at candle start time (exclusive).
    """
    assert not candle.contains_datetime("2025-01-02 12:00:00")


def test_Candle_contains_datetime_false_at_end(candle):
    """Verify contains_datetime returns False at candle end time (exclusive).
    """
    # For 1m candle starting at 12:00:00, end is 12:01:00
    assert not candle.contains_datetime("2025-01-02 12:01:00")


def test_Candle_contains_datetime_false_before(candle):
    """Verify contains_datetime returns False for dt before candle start."""
    assert not candle.contains_datetime("2025-01-02 11:59:00")


def test_Candle_equality_equal(candle):
    """Verify Candle __eq__ returns True for identical candles."""
    other = Candle(c_datetime="2025-01-02 12:00:00",
                   c_timeframe="1m",
                   c_open=5000,
                   c_high=5007.75,
                   c_low=4995.5,
                   c_close=5002,
                   c_volume=1501,
                   c_symbol="ES",
                   )
    assert candle == other


def test_Candle_equality_not_equal_price(candle):
    """Verify Candle __eq__ returns False when prices differ."""
    other = Candle(c_datetime="2025-01-02 12:00:00",
                   c_timeframe="1m",
                   c_open=5001,
                   c_high=5007.75,
                   c_low=4995.5,
                   c_close=5002,
                   c_volume=1501,
                   c_symbol="ES",
                   )
    assert candle != other


def test_Candle_equality_not_equal_datetime(candle):
    """Verify Candle __eq__ returns False when datetimes differ."""
    other = Candle(c_datetime="2025-01-02 12:01:00",
                   c_timeframe="1m",
                   c_open=5000,
                   c_high=5007.75,
                   c_low=4995.5,
                   c_close=5002,
                   c_volume=1501,
                   c_symbol="ES",
                   )
    assert candle != other


def test_Candle_to_clean_dict(candle):
    """Verify Candle.to_clean_dict returns a dict with expected keys."""
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


def test_Candle_brief(candle):
    """Verify Candle.brief returns a single-line summary string."""
    result = candle.brief()
    assert isinstance(result, str)
    assert "ES" in result
    assert "1m" in result
    assert "2025-01-02 12:00:00" in result
    assert "5000" in result


def test_Candle_str_repr(candle):
    """Verify Candle __str__ and __repr__ return non-empty strings."""
    assert isinstance(str(candle), str)
    assert len(str(candle)) > 0
    assert isinstance(repr(candle), str)
    assert len(repr(candle)) > 0


def test_Candle_c_date_and_c_time(candle):
    """Verify Candle.c_date and c_time are set correctly."""
    assert candle.c_date == "2025-01-02"
    assert candle.c_time == "12:00:00"


def test_Candle_c_epoch_is_int(candle):
    """Verify Candle.c_epoch is set as an integer."""
    assert isinstance(candle.c_epoch, int)


def test_Candle_c_end_datetime_1m(candle):
    """Verify Candle.c_end_datetime is 1 minute after start for 1m candle."""
    assert candle.c_end_datetime == "2025-01-02 12:01:00"
