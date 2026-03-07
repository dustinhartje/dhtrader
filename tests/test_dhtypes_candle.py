"""Tests for Candle creation, validation, and output methods."""
from dhtrader import (
    Candle)


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
