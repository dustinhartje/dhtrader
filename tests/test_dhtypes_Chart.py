"""Tests for Chart creation, candle loading, and date restriction."""
import json
import pytest
from dhtrader import (
    Candle, Chart)


def test_Chart_create_and_verify_common_methods():
    """Test Chart __init__ values, __eq__, __ne__, __str__, __repr__,
    to_clean_dict, to_json, and pretty.

    Chart does not define brief.
    """
    out_candle = Candle(c_datetime="2099-01-02 12:00:00",
                        c_timeframe="1m",
                        c_open=5000,
                        c_high=5007.75,
                        c_low=4995.5,
                        c_close=5002,
                        c_volume=1501,
                        c_symbol="ES",
                        )
    chart = Chart(c_timeframe="1m",
                  c_trading_hours="rth",
                  c_symbol="ES",
                  c_start="2099-01-02 12:00:00",
                  c_end="2099-01-02 12:10:00",
                  autoload=False,
                  )
    assert isinstance(chart, Chart)
    # __init__
    assert chart.c_timeframe == "1m"
    assert chart.c_trading_hours == "rth"
    assert chart.c_symbol.ticker == "ES"
    assert chart.c_start == "2099-01-02 12:00:00"
    assert chart.c_end == "2099-01-02 12:10:00"
    assert chart.c_candles == []
    assert chart.autoload is False
    assert chart.show_progress is False
    assert chart.candles_count == 0
    assert chart.earliest_candle is None
    assert chart.latest_candle is None
    expected_attrs = {
        "autoload", "c_candles", "c_end", "c_start", "c_symbol",
        "c_timeframe", "c_trading_hours", "candles_count",
        "earliest_candle", "latest_candle", "show_progress",
    }
    actual_attrs = set(vars(chart).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "Chart attributes changed. Update this test's "
        "__init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )
    chart.add_candle(out_candle)
    chart2 = Chart(c_timeframe="1m",
                   c_trading_hours="rth",
                   c_symbol="ES",
                   c_start="2099-01-02 12:00:00",
                   c_end="2099-01-02 12:10:00",
                   autoload=False,
                   )
    chart2.add_candle(out_candle)
    diff = Chart(c_timeframe="5m",
                 c_trading_hours="rth",
                 c_symbol="ES",
                 c_start="2099-01-02 12:00:00",
                 c_end="2099-01-02 12:10:00",
                 autoload=False,
                 )
    # __eq__
    assert chart == chart2
    assert not (chart == diff)
    # __ne__
    assert not (chart != chart2)
    assert chart != diff
    # __str__
    assert isinstance(str(chart), str)
    assert len(str(chart)) > 0
    # __repr__
    assert isinstance(repr(chart), str)
    assert str(chart) == repr(chart)
    # to_clean_dict
    d = chart.to_clean_dict()
    assert isinstance(d, dict)
    assert d["c_timeframe"] == "1m"
    assert d["c_symbol"] == "ES"
    assert d["c_start"] == "2099-01-02 12:00:00"
    # to_json
    j = chart.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["c_timeframe"] == "1m"
    assert parsed["c_symbol"] == "ES"
    # pretty
    assert len(chart.pretty().splitlines()) == 15
    assert len(chart.pretty(suppress_candles=False).splitlines()) == 38


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
