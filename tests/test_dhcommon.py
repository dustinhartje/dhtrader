"""Tests for dhcommon utility functions and constants."""
import json
import pytest
from datetime import datetime, timedelta, date
from dhtrader import (
    bot,
    check_tf_th_compatibility,
    diff_dicts,
    dow_name,
    dt_as_dt,
    dt_as_str,
    dt_as_time,
    dt_from_epoch,
    dt_to_epoch,
    log_say,
    OperationTimer,
    rangify_candle_times,
    sort_dict,
    this_candle_start,
    timeframe_delta,
    valid_timeframe,
    valid_trading_hours,
)
from dhtrader.dhcommon import (
    dict_of_weeks,
    start_of_week_date,
    valid_event_category,
)


def test_log_say(capsys):
    """Verify log_say prints message at each supported log level."""
    for level in ["info", "debug", "warn", "error", "critical"]:
        log_say(f"test {level} message", level=level)
        captured = capsys.readouterr()
        assert f"test {level} message" in captured.out


def test_OperationTimer_init():
    """Verify OperationTimer starts or does not start based on auto_start."""
    # Auto start (default True)
    t = OperationTimer(name="test_op")
    assert t.name == "test_op"
    assert t.start_dt is not None
    assert isinstance(t.start_dt, datetime)
    # auto_start=False should not set start_dt
    t2 = OperationTimer(name="test_op", auto_start=False)
    assert t2.start_dt is None


def test_OperationTimer_stop():
    """Verify OperationTimer.stop sets end_dt and calculates elapsed."""
    t = OperationTimer(name="test_stop")
    t.stop()
    assert t.end_dt is not None
    assert isinstance(t.end_dt, datetime)
    assert t.elapsed_dt is not None
    assert isinstance(t.elapsed_str, str)
    assert len(t.elapsed_str) > 0


def test_OperationTimer_update_elapsed():
    """Verify update_elapsed calculates elapsed from start to now."""
    t = OperationTimer(name="test_elapsed")
    t.update_elapsed()
    assert t.elapsed_dt is not None
    assert isinstance(t.elapsed_str, str)


def test_OperationTimer_create_and_verify_common_methods():
    """Test OperationTimer __str__, __repr__, to_clean_dict, to_json,
    and pretty.

    OperationTimer does not define __eq__, __ne__, or brief.
    """
    t = OperationTimer(name="test_common")
    t.stop()
    # __str__
    assert isinstance(str(t), str)
    assert len(str(t)) > 0
    # __repr__
    assert isinstance(repr(t), str)
    assert str(t) == repr(t)
    # to_clean_dict
    d = t.to_clean_dict()
    assert isinstance(d, dict)
    assert d["name"] == "test_common"
    assert "start_dt" in d
    assert "end_dt" in d
    assert "elapsed_dt" in d
    # to_json
    j = t.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["name"] == "test_common"
    assert "start_dt" in parsed
    assert "end_dt" in parsed
    assert "elapsed_dt" in parsed
    # pretty
    p = t.pretty()
    assert isinstance(p, str)
    assert "\n" in p
    assert "test_common" in p


def test_OperationTimer_summary():
    """Verify OperationTimer.summary returns a one-line string."""
    t = OperationTimer(name="test_summary")
    t.stop()
    s = t.summary()
    assert isinstance(s, str)
    assert "test_summary" in s


def test_sort_dict():
    """Verify sort_dict returns keys in ascending order."""
    # Basic sorting
    d = {"z": 3, "a": 1, "m": 2}
    result = sort_dict(d)
    assert list(result.keys()) == ["a", "m", "z"]
    # Preserves values
    d2 = {"z": 30, "a": 10, "m": 20}
    result2 = sort_dict(d2)
    assert result2["a"] == 10
    assert result2["z"] == 30
    # Empty dict
    assert sort_dict({}) == {}
    # Single entry
    assert sort_dict({"only": "value"}) == {"only": "value"}
    # Already sorted
    d3 = {"a": 1, "b": 2, "c": 3}
    assert list(sort_dict(d3).keys()) == ["a", "b", "c"]


def test_diff_dicts():
    """Verify diff_dicts returns correct difference tuples between dicts."""
    # No difference
    assert diff_dicts({"a": 1, "b": 2}, {"a": 1, "b": 2}) == {}
    # Value difference
    assert diff_dicts(
        {"a": 1, "b": 2}, {"a": 1, "b": 3}
    ) == {"b": (2, 3)}
    # Key missing from first dict
    assert diff_dicts(
        {"a": 1}, {"a": 1, "b": 2}
    ) == {"b": (None, 2)}
    # Key missing from second dict
    assert diff_dicts(
        {"a": 1, "b": 2}, {"a": 1}
    ) == {"b": (2, None)}
    # Both empty
    assert diff_dicts({}, {}) == {}
    # Multiple differences
    result = diff_dicts(
        {"a": 1, "b": 2, "c": 3}, {"a": 9, "b": 2, "c": 8}
    )
    assert result == {"a": (1, 9), "c": (3, 8)}


def test_valid_timeframe():
    """Verify valid_timeframe accepts valid and rejects invalid timeframes."""
    valid_tfs = [
        '1m', '5m', '15m', 'r1h', 'e1h',
        'r1d', 'e1d', 'r1w', 'e1w', 'r1mo', 'e1mo',
    ]
    for tf in valid_tfs:
        assert valid_timeframe(tf) is True
    # Invalid raises ValueError by default (exit=True)
    with pytest.raises(ValueError):
        valid_timeframe("invalid_tf")
    # Empty string raises ValueError
    with pytest.raises(ValueError):
        valid_timeframe("")
    # Returns False without raising when exit=False
    assert valid_timeframe("bad_tf", exit=False) is False


def test_valid_trading_hours():
    """Verify valid_trading_hours accepts rth/eth and rejects other values."""
    assert valid_trading_hours("rth") is True
    assert valid_trading_hours("eth") is True
    with pytest.raises(ValueError):
        valid_trading_hours("invalid")
    assert valid_trading_hours("bad", exit=False) is False


def test_check_tf_th_compatibility():
    """Verify check_tf_th_compatibility rejects incompatible tf/th combos."""
    # Compatible ETH timeframes
    for tf in ["1m", "5m", "15m", "e1h", "e1d"]:
        assert check_tf_th_compatibility(tf, "eth") is True
    # Compatible RTH timeframes
    for tf in ["1m", "5m", "15m", "r1h", "r1d"]:
        assert check_tf_th_compatibility(tf, "rth") is True
    # RTH-only timeframes with ETH raises ValueError
    for tf in ["r1h", "r1d", "r1w", "r1mo"]:
        with pytest.raises(ValueError):
            check_tf_th_compatibility(tf, "eth")
    # ETH-only timeframes with RTH raises ValueError
    for tf in ["e1h", "e1d", "e1w", "e1mo"]:
        with pytest.raises(ValueError):
            check_tf_th_compatibility(tf, "rth")
    # Returns False without raising when exit=False
    assert check_tf_th_compatibility("r1h", "eth", exit=False) is False


def test_valid_event_category():
    """Verify valid_event_category accepts valid and rejects invalid values."""
    valid_cats = ['Closed', 'Data', 'Unplanned', 'LowVolume', 'Rollover']
    for cat in valid_cats:
        assert valid_event_category(cat) is True
    with pytest.raises(ValueError):
        valid_event_category("BadCategory")
    assert valid_event_category("BadCategory", exit=False) is False


def test_dt_as_dt():
    """Verify dt_as_dt converts various inputs to datetime or None."""
    # None returns None
    assert dt_as_dt(None) is None
    # datetime returns same datetime
    d = datetime(2025, 1, 15, 10, 30, 0)
    assert dt_as_dt(d) == d
    assert isinstance(dt_as_dt(d), datetime)
    # Canonical string parses correctly
    assert dt_as_dt("2025-01-15 10:30:00") == datetime(2025, 1, 15, 10, 30)
    # Flex string with two-digit year is treated as 2000+year
    assert dt_as_dt("25-1-15 10:30:00") == datetime(2025, 1, 15, 10, 30)
    # Flex string without leading zeroes
    assert dt_as_dt("2025-1-5 9:5:0") == datetime(2025, 1, 5, 9, 5, 0)
    # Unsupported string raises ValueError
    with pytest.raises(ValueError):
        dt_as_dt("not-a-date")
    # Non-string, non-datetime raises TypeError
    with pytest.raises(TypeError):
        dt_as_dt(12345)


def test_dt_as_str():
    """Verify dt_as_str converts various inputs to canonical string or None."""
    # None returns None
    assert dt_as_str(None) is None
    # datetime returns canonical string
    assert dt_as_str(datetime(2025, 1, 15, 10, 30, 0)) == (
        "2025-01-15 10:30:00"
    )
    # Canonical string returns same string unchanged
    assert dt_as_str("2025-01-15 10:30:00") == "2025-01-15 10:30:00"
    # Flex string is normalized to canonical format
    assert dt_as_str("25-1-5 9:5:0") == "2025-01-05 09:05:00"
    # Non-string, non-datetime raises TypeError
    with pytest.raises(TypeError):
        dt_as_str(12345)


def test_dt_as_time():
    """Verify dt_as_time converts HH:MM:SS strings to time objects."""
    from datetime import time
    # None returns None
    assert dt_as_time(None) is None
    # Valid string returns time object with correct fields
    result = dt_as_time("10:30:00")
    assert isinstance(result, time)
    assert result.hour == 10
    assert result.minute == 30
    assert result.second == 0
    # Midnight parses correctly
    assert dt_as_time("00:00:00") == time(0, 0, 0)


def test_dow_name():
    """Verify dow_name returns correct weekday name for each index 0-6."""
    assert dow_name(0) == "Monday"
    assert dow_name(1) == "Tuesday"
    assert dow_name(2) == "Wednesday"
    assert dow_name(3) == "Thursday"
    assert dow_name(4) == "Friday"
    assert dow_name(5) == "Saturday"
    assert dow_name(6) == "Sunday"


def test_dt_to_epoch():
    """Verify dt_to_epoch converts datetime or string to epoch int or None."""
    assert dt_to_epoch(None) is None
    result = dt_to_epoch("2025-01-01 00:00:00")
    assert isinstance(result, int)


def test_dt_from_epoch():
    """Verify dt_from_epoch converts epoch to datetime, with roundtrip."""
    assert dt_from_epoch(None) is None
    epoch = dt_to_epoch("2025-01-15 10:30:00")
    result = dt_from_epoch(epoch)
    assert isinstance(result, datetime)
    # Roundtrip: epoch → datetime → string should equal original
    assert dt_as_str(result) == "2025-01-15 10:30:00"


def test_timeframe_delta():
    """Verify timeframe_delta returns correct timedelta for each timeframe."""
    assert timeframe_delta("1m") == timedelta(minutes=1)
    assert timeframe_delta("5m") == timedelta(minutes=5)
    assert timeframe_delta("15m") == timedelta(minutes=15)
    assert timeframe_delta("e1h") == timedelta(hours=1)
    assert timeframe_delta("r1h") == timedelta(hours=1)
    with pytest.raises(ValueError):
        timeframe_delta("invalid")


def test_start_of_week_date():
    """Verify start_of_week_date returns the Sunday starting the week."""
    # 2025-01-13 is Monday → prior Sunday 2025-01-12
    assert start_of_week_date("2025-01-13 10:00:00") == date(2025, 1, 12)
    # 2025-01-12 is Sunday → same day
    assert start_of_week_date("2025-01-12 10:00:00") == date(2025, 1, 12)
    # 2025-01-18 is Saturday → prior Sunday 2025-01-12
    assert start_of_week_date("2025-01-18 10:00:00") == date(2025, 1, 12)
    # 2025-01-17 is Friday → prior Sunday 2025-01-12
    assert start_of_week_date("2025-01-17 10:00:00") == date(2025, 1, 12)
    # Returns a date object
    assert isinstance(start_of_week_date("2025-01-15 10:00:00"), date)


def test_dict_of_weeks():
    """Verify dict_of_weeks returns correct week-keyed template dictionary."""
    template = {"count": 0}
    # Single week
    result = dict_of_weeks(
        "2025-01-13 00:00:00", "2025-01-17 00:00:00", template
    )
    assert isinstance(result, dict)
    assert len(result) == 1
    # Multiple weeks (Jan 12, Jan 19, Jan 26, Feb 2)
    result2 = dict_of_weeks(
        "2025-01-13 00:00:00", "2025-02-07 00:00:00", template
    )
    assert len(result2) == 4
    # Template is deep copied so each week has its own independent copy
    result3 = dict_of_weeks(
        "2025-01-13 00:00:00", "2025-01-26 00:00:00", template
    )
    weeks = list(result3.keys())
    result3[weeks[0]]["count"] = 99
    assert result3[weeks[1]]["count"] == 0
    # Keys are date strings starting on Sunday
    result4 = dict_of_weeks(
        "2025-01-13 00:00:00", "2025-01-19 00:00:00", {}
    )
    assert list(result4.keys())[0] == "2025-01-12"


def test_this_candle_start():
    """Verify this_candle_start returns correct parent candle start time."""
    # 1m: floors to the minute
    assert this_candle_start("2025-01-15 10:37:45", "1m") == (
        datetime(2025, 1, 15, 10, 37, 0)
    )
    # 5m: floors to 5m boundary
    assert this_candle_start("2025-01-15 10:37:45", "5m") == (
        datetime(2025, 1, 15, 10, 35, 0)
    )
    assert this_candle_start("2025-01-15 10:35:00", "5m") == (
        datetime(2025, 1, 15, 10, 35, 0)
    )
    # 15m: floors to 15m boundary
    assert this_candle_start("2025-01-15 10:37:45", "15m") == (
        datetime(2025, 1, 15, 10, 30, 0)
    )
    assert this_candle_start("2025-01-15 10:30:00", "15m") == (
        datetime(2025, 1, 15, 10, 30, 0)
    )
    # r1h: starts at :30
    assert this_candle_start("2025-01-15 10:37:45", "r1h") == (
        datetime(2025, 1, 15, 10, 30, 0)
    )
    assert this_candle_start("2025-01-15 10:30:00", "r1h") == (
        datetime(2025, 1, 15, 10, 30, 0)
    )
    # e1h: starts at :00
    assert this_candle_start("2025-01-15 10:37:45", "e1h") == (
        datetime(2025, 1, 15, 10, 0, 0)
    )
    assert this_candle_start("2025-01-15 10:00:00", "e1h") == (
        datetime(2025, 1, 15, 10, 0, 0)
    )
    # e1d: before 6pm returns yesterday 18:00
    assert this_candle_start("2025-01-15 10:00:00", "e1d") == (
        datetime(2025, 1, 14, 18, 0, 0)
    )
    # e1d: after 6pm returns today 18:00
    assert this_candle_start("2025-01-15 19:00:00", "e1d") == (
        datetime(2025, 1, 15, 18, 0, 0)
    )
    # e1d: at exactly 18:00 returns that time
    assert this_candle_start("2025-01-15 18:00:00", "e1d") == (
        datetime(2025, 1, 15, 18, 0, 0)
    )
    # e1w: Sunday at/after 6pm returns that Sunday 18:00
    # 2025-01-12 is a Sunday
    assert this_candle_start("2025-01-12 19:00:00", "e1w") == (
        datetime(2025, 1, 12, 18, 0, 0)
    )
    # e1w: mid-week returns prior Sunday 18:00
    # 2025-01-15 is Wednesday, prior Sunday is 2025-01-12
    assert this_candle_start("2025-01-15 10:00:00", "e1w") == (
        datetime(2025, 1, 12, 18, 0, 0)
    )
    # e1w: Sunday before 6pm returns previous Sunday 18:00
    # 2025-01-12 is Sunday, prior Sunday is 2025-01-05
    assert this_candle_start("2025-01-12 10:00:00", "e1w") == (
        datetime(2025, 1, 5, 18, 0, 0)
    )
    # Invalid timeframe raises ValueError
    with pytest.raises(ValueError):
        this_candle_start("2025-01-15 10:00:00", "invalid")


def test_rangify_candle_times():
    """Verify rangify_candle_times aggregates consecutive times into ranges."""
    # Single time creates one range
    times = [datetime(2025, 1, 15, 10, 0, 0)]
    result = rangify_candle_times(times, "1m")
    assert len(result) == 1
    assert result[0]["start_dt"] == "2025-01-15 10:00:00"
    assert result[0]["end_dt"] == "2025-01-15 10:00:00"
    # Consecutive times collapse into one range
    times2 = [
        datetime(2025, 1, 15, 10, 0, 0),
        datetime(2025, 1, 15, 10, 1, 0),
        datetime(2025, 1, 15, 10, 2, 0),
    ]
    result2 = rangify_candle_times(times2, "1m")
    assert len(result2) == 1
    assert result2[0]["start_dt"] == "2025-01-15 10:00:00"
    assert result2[0]["end_dt"] == "2025-01-15 10:02:00"
    # Gap creates two separate ranges
    times3 = [
        datetime(2025, 1, 15, 10, 0, 0),
        datetime(2025, 1, 15, 10, 1, 0),
        datetime(2025, 1, 15, 10, 5, 0),
        datetime(2025, 1, 15, 10, 6, 0),
    ]
    result3 = rangify_candle_times(times3, "1m")
    assert len(result3) == 2
    assert result3[0]["end_dt"] == "2025-01-15 10:01:00"
    assert result3[1]["start_dt"] == "2025-01-15 10:05:00"
    # Unsorted input is sorted before ranging
    times4 = [
        datetime(2025, 1, 15, 10, 2, 0),
        datetime(2025, 1, 15, 10, 0, 0),
        datetime(2025, 1, 15, 10, 1, 0),
    ]
    result4 = rangify_candle_times(times4, "1m")
    assert len(result4) == 1
    assert result4[0]["start_dt"] == "2025-01-15 10:00:00"
    # 5m timeframe consecutive times collapse into one range
    times5 = [
        datetime(2025, 1, 15, 10, 0, 0),
        datetime(2025, 1, 15, 10, 5, 0),
        datetime(2025, 1, 15, 10, 10, 0),
    ]
    result5 = rangify_candle_times(times5, "5m")
    assert len(result5) == 1


def test_bot():
    """Verify bot() returns the expected beginning-of-time string."""
    result = bot()
    assert isinstance(result, str)
    assert result == "2008-01-01 00:00:00"
