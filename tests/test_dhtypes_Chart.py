import pytest
import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
from dhtypes import (
    Candle, Chart, Day, Event, Indicator, IndicatorDataPoint,
    IndicatorEMA, IndicatorSMA, Symbol)


def test_Chart_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
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
    assert len(out_chart.pretty().splitlines()) == 14
    assert len(out_chart.pretty(suppress_candles=False).splitlines()) == 36


@pytest.mark.storage
def test_Chart_restrict_dates():
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
