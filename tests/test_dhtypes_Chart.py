"""Tests for Chart creation, candle loading, and date restriction."""
import pytest
from dhtrader import (
    Candle, Chart)


def test_Chart_create_and_verify_pretty():
    """Verify Chart.pretty() output line count."""
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
    out_chart = Chart(c_timeframe="1m",
                      c_trading_hours="rth",
                      c_symbol="ES",
                      c_start="2025-01-02 12:00:00",
                      c_end="2025-01-02 12:10:00",
                      autoload=False,
                      )
    assert isinstance(out_chart, Chart)
    out_chart.add_candle(out_candle)
    assert len(out_chart.pretty().splitlines()) == 15
    assert len(out_chart.pretty(suppress_candles=False).splitlines()) == 37


@pytest.mark.storage
def test_Chart_restrict_dates():
    """Verify restrict_dates adjusts candle ranges.

    Storage Usage: Chart autoload=True loads candles.
    """
    # Create a multimonth chart and confirm initial dates and candle count
    ch = Chart(c_timeframe="15m",
               c_trading_hours="eth",
               c_symbol="ES",
               c_start="2024-09-15 00:00:00",
               c_end="2024-11-15 00:00:00",
               autoload=True)
    assert ch.c_start == "2024-09-15 00:00:00"
    assert ch.c_end == "2024-11-15 00:00:00"
    assert len(ch.c_candles) == 4073
    # Adjust the start date and confirm
    ch.restrict_dates(new_start_dt="2024-09-17 00:00:00",
                      new_end_dt="2024-11-15 00:00:00")
    assert ch.c_start == "2024-09-17 00:00:00"
    assert ch.c_candles[0].c_datetime == "2024-09-17 00:00:00"
    assert ch.c_end == "2024-11-15 00:00:00"
    assert ch.c_candles[-1].c_datetime == "2024-11-15 00:00:00"
    assert len(ch.c_candles) == 3957
    # Adjust the end date and confirm
    ch.restrict_dates(new_start_dt="2024-09-17 00:00:00",
                      new_end_dt="2024-09-25 00:00:00")
    assert ch.c_start == "2024-09-17 00:00:00"
    assert ch.c_candles[0].c_datetime == "2024-09-17 00:00:00"
    assert ch.c_end == "2024-09-25 00:00:00"
    assert ch.c_candles[-1].c_datetime == "2024-09-25 00:00:00"
    assert len(ch.c_candles) == 553
    # Adjust both dates and confirm
    ch.restrict_dates(new_start_dt="2024-09-18 04:27:00",
                      new_end_dt="2024-09-23 12:00:00")
    assert ch.c_start == "2024-09-18 04:27:00"
    assert ch.c_candles[0].c_datetime == "2024-09-18 04:30:00"
    assert ch.c_end == "2024-09-23 12:00:00"
    assert ch.c_candles[-1].c_datetime == "2024-09-23 12:00:00"
    assert len(ch.c_candles) == 307

    # Ensure that setting dates outside of the current bounds raises errors
    # First adjust only start
    with pytest.raises(ValueError):
        ch.restrict_dates(new_start_dt="2024-09-12 00:00:00",
                          new_end_dt="2024-09-23 12:00:00")
    # Then only end
    with pytest.raises(ValueError):
        ch.restrict_dates(new_start_dt="2024-09-18 04:27:00",
                          new_end_dt="2024-09-27 12:00:00")

    # And finally both
    with pytest.raises(ValueError):
        ch.restrict_dates(new_start_dt="2024-09-12 00:00:00",
                          new_end_dt="2024-09-27 12:00:00")
