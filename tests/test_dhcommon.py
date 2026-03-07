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


# #############################################################################
# log_say
# #############################################################################

def test_log_say_info(capsys):
    """Verify log_say prints message at info level."""
    log_say("test info message")
    captured = capsys.readouterr()
    assert "test info message" in captured.out


def test_log_say_debug(capsys):
    """Verify log_say prints message at debug level."""
    log_say("test debug message", level="debug")
    captured = capsys.readouterr()
    assert "test debug message" in captured.out


def test_log_say_warn(capsys):
    """Verify log_say prints message at warn level."""
    log_say("test warn message", level="warn")
    captured = capsys.readouterr()
    assert "test warn message" in captured.out


def test_log_say_error(capsys):
    """Verify log_say prints message at error level."""
    log_say("test error message", level="error")
    captured = capsys.readouterr()
    assert "test error message" in captured.out


def test_log_say_critical(capsys):
    """Verify log_say prints message at critical level."""
    log_say("test critical message", level="critical")
    captured = capsys.readouterr()
    assert "test critical message" in captured.out


# #############################################################################
# OperationTimer
# #############################################################################

def test_OperationTimer_auto_start():
    """Verify OperationTimer starts automatically by default."""
    t = OperationTimer(name="test_op")
    assert t.name == "test_op"
    assert t.start_dt is not None
    assert isinstance(t.start_dt, datetime)


def test_OperationTimer_no_auto_start():
    """Verify OperationTimer does not start when auto_start=False."""
    t = OperationTimer(name="test_op", auto_start=False)
    assert t.start_dt is None


def test_OperationTimer_stop_and_elapsed():
    """Verify OperationTimer stop sets end_dt and elapsed_dt."""
    t = OperationTimer(name="test_op")
    t.stop()
    assert t.end_dt is not None
    assert isinstance(t.end_dt, datetime)
    assert t.elapsed_dt is not None
    assert isinstance(t.elapsed_str, str)
    assert len(t.elapsed_str) > 0


def test_OperationTimer_update_elapsed():
    """Verify update_elapsed calculates elapsed from start to now."""
    t = OperationTimer(name="test_op")
    t.update_elapsed()
    assert t.elapsed_dt is not None
    assert isinstance(t.elapsed_str, str)


def test_OperationTimer_to_json():
    """Verify OperationTimer.to_json returns valid JSON."""
    t = OperationTimer(name="test_to_json")
    t.stop()
    j = t.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert parsed["name"] == "test_to_json"
    assert "start_dt" in parsed
    assert "end_dt" in parsed
    assert "elapsed_dt" in parsed


def test_OperationTimer_to_clean_dict():
    """Verify OperationTimer.to_clean_dict returns a dict."""
    t = OperationTimer(name="test_clean")
    t.stop()
    d = t.to_clean_dict()
    assert isinstance(d, dict)
    assert d["name"] == "test_clean"


def test_OperationTimer_pretty():
    """Verify OperationTimer.pretty returns formatted JSON string."""
    t = OperationTimer(name="test_pretty")
    t.stop()
    p = t.pretty()
    assert isinstance(p, str)
    assert "\n" in p
    assert "test_pretty" in p


def test_OperationTimer_summary():
    """Verify OperationTimer.summary returns a one-line string."""
    t = OperationTimer(name="test_summary")
    t.stop()
    s = t.summary()
    assert isinstance(s, str)
    assert "test_summary" in s


def test_OperationTimer_str_repr():
    """Verify OperationTimer __str__ and __repr__ return strings."""
    t = OperationTimer(name="test_str")
    assert isinstance(str(t), str)
    assert isinstance(repr(t), str)


# #############################################################################
# sort_dict
# #############################################################################

def test_sort_dict_basic():
    """Verify sort_dict returns keys in ascending order."""
    d = {"z": 3, "a": 1, "m": 2}
    result = sort_dict(d)
    assert list(result.keys()) == ["a", "m", "z"]


def test_sort_dict_preserves_values():
    """Verify sort_dict preserves values when sorting by keys."""
    d = {"z": 30, "a": 10, "m": 20}
    result = sort_dict(d)
    assert result["a"] == 10
    assert result["m"] == 20
    assert result["z"] == 30


def test_sort_dict_empty():
    """Verify sort_dict handles an empty dictionary."""
    result = sort_dict({})
    assert result == {}


def test_sort_dict_single():
    """Verify sort_dict handles a single-entry dictionary."""
    result = sort_dict({"only": "value"})
    assert result == {"only": "value"}


def test_sort_dict_already_sorted():
    """Verify sort_dict handles an already-sorted dictionary."""
    d = {"a": 1, "b": 2, "c": 3}
    result = sort_dict(d)
    assert list(result.keys()) == ["a", "b", "c"]


# #############################################################################
# diff_dicts
# #############################################################################

def test_diff_dicts_no_difference():
    """Verify diff_dicts returns empty dict for identical dicts."""
    d1 = {"a": 1, "b": 2}
    d2 = {"a": 1, "b": 2}
    assert diff_dicts(d1, d2) == {}


def test_diff_dicts_value_difference():
    """Verify diff_dicts captures differing values as tuples."""
    d1 = {"a": 1, "b": 2}
    d2 = {"a": 1, "b": 3}
    result = diff_dicts(d1, d2)
    assert result == {"b": (2, 3)}


def test_diff_dicts_missing_key_in_first():
    """Verify diff_dicts handles key missing from first dict."""
    d1 = {"a": 1}
    d2 = {"a": 1, "b": 2}
    result = diff_dicts(d1, d2)
    assert result == {"b": (None, 2)}


def test_diff_dicts_missing_key_in_second():
    """Verify diff_dicts handles key missing from second dict."""
    d1 = {"a": 1, "b": 2}
    d2 = {"a": 1}
    result = diff_dicts(d1, d2)
    assert result == {"b": (2, None)}


def test_diff_dicts_empty_dicts():
    """Verify diff_dicts handles both dicts empty."""
    assert diff_dicts({}, {}) == {}


def test_diff_dicts_multiple_differences():
    """Verify diff_dicts captures multiple differing keys."""
    d1 = {"a": 1, "b": 2, "c": 3}
    d2 = {"a": 9, "b": 2, "c": 8}
    result = diff_dicts(d1, d2)
    assert result == {"a": (1, 9), "c": (3, 8)}


# #############################################################################
# valid_timeframe
# #############################################################################

def test_valid_timeframe_valid_values():
    """Verify valid_timeframe returns True for all valid timeframes."""
    valid_tfs = [
        '1m', '5m', '15m', 'r1h', 'e1h', 'r1d', 'e1d', 'r1w', 'e1w',
        'r1mo', 'e1mo'
    ]
    for tf in valid_tfs:
        assert valid_timeframe(tf) is True


def test_valid_timeframe_invalid_raises():
    """Verify valid_timeframe raises ValueError for invalid timeframe."""
    with pytest.raises(ValueError):
        valid_timeframe("invalid_tf")


def test_valid_timeframe_invalid_no_exit():
    """Verify valid_timeframe returns False and no raise when exit=False."""
    result = valid_timeframe("bad_tf", exit=False)
    assert result is False


def test_valid_timeframe_empty_string_raises():
    """Verify valid_timeframe raises ValueError for empty string."""
    with pytest.raises(ValueError):
        valid_timeframe("")


# #############################################################################
# valid_trading_hours
# #############################################################################

def test_valid_trading_hours_valid_values():
    """Verify valid_trading_hours returns True for rth and eth."""
    assert valid_trading_hours("rth") is True
    assert valid_trading_hours("eth") is True


def test_valid_trading_hours_invalid_raises():
    """Verify valid_trading_hours raises ValueError for invalid value."""
    with pytest.raises(ValueError):
        valid_trading_hours("invalid")


def test_valid_trading_hours_invalid_no_exit():
    """Verify valid_trading_hours returns False, no raise when exit=False."""
    result = valid_trading_hours("bad", exit=False)
    assert result is False


# #############################################################################
# check_tf_th_compatibility
# #############################################################################

def test_check_tf_th_compatibility_valid_eth():
    """Verify compatible ETH timeframes pass the compatibility check."""
    assert check_tf_th_compatibility("1m", "eth") is True
    assert check_tf_th_compatibility("5m", "eth") is True
    assert check_tf_th_compatibility("15m", "eth") is True
    assert check_tf_th_compatibility("e1h", "eth") is True
    assert check_tf_th_compatibility("e1d", "eth") is True


def test_check_tf_th_compatibility_valid_rth():
    """Verify compatible RTH timeframes pass the compatibility check."""
    assert check_tf_th_compatibility("1m", "rth") is True
    assert check_tf_th_compatibility("5m", "rth") is True
    assert check_tf_th_compatibility("15m", "rth") is True
    assert check_tf_th_compatibility("r1h", "rth") is True
    assert check_tf_th_compatibility("r1d", "rth") is True


def test_check_tf_th_compatibility_eth_rth_tf_raises():
    """Verify RTH-only timeframes with ETH raises ValueError."""
    for tf in ["r1h", "r1d", "r1w", "r1mo"]:
        with pytest.raises(ValueError):
            check_tf_th_compatibility(tf, "eth")


def test_check_tf_th_compatibility_rth_eth_tf_raises():
    """Verify ETH-only timeframes with RTH raises ValueError."""
    for tf in ["e1h", "e1d", "e1w", "e1mo"]:
        with pytest.raises(ValueError):
            check_tf_th_compatibility(tf, "rth")


def test_check_tf_th_compatibility_no_exit():
    """Verify incompatible combo returns False and no raise when exit=False."""
    result = check_tf_th_compatibility("r1h", "eth", exit=False)
    assert result is False


# #############################################################################
# valid_event_category
# #############################################################################

def test_valid_event_category_valid_values():
    """Verify valid_event_category returns True for all valid categories."""
    valid_cats = [
        'Closed', 'Data', 'Unplanned', 'LowVolume', 'Rollover'
    ]
    for cat in valid_cats:
        assert valid_event_category(cat) is True


def test_valid_event_category_invalid_raises():
    """Verify valid_event_category raises ValueError for invalid category."""
    with pytest.raises(ValueError):
        valid_event_category("BadCategory")


def test_valid_event_category_invalid_no_exit():
    """Verify valid_event_category returns False and no raise when exit=False.
    """
    result = valid_event_category("BadCategory", exit=False)
    assert result is False


# #############################################################################
# dt_as_dt
# #############################################################################

def test_dt_as_dt_from_none():
    """Verify dt_as_dt returns None for None input."""
    assert dt_as_dt(None) is None


def test_dt_as_dt_from_datetime():
    """Verify dt_as_dt returns same datetime when given a datetime."""
    d = datetime(2025, 1, 15, 10, 30, 0)
    result = dt_as_dt(d)
    assert result == d
    assert isinstance(result, datetime)


def test_dt_as_dt_from_canonical_string():
    """Verify dt_as_dt parses canonical YYYY-mm-dd HH:MM:SS string."""
    result = dt_as_dt("2025-01-15 10:30:00")
    assert result == datetime(2025, 1, 15, 10, 30, 0)


def test_dt_as_dt_from_flex_string_two_digit_year():
    """Verify dt_as_dt parses two-digit year as 2000+year."""
    result = dt_as_dt("25-1-15 10:30:00")
    assert result == datetime(2025, 1, 15, 10, 30, 0)


def test_dt_as_dt_from_flex_string_no_leading_zeroes():
    """Verify dt_as_dt parses dates and times without leading zeroes."""
    result = dt_as_dt("2025-1-5 9:5:0")
    assert result == datetime(2025, 1, 5, 9, 5, 0)


def test_dt_as_dt_invalid_string_raises():
    """Verify dt_as_dt raises ValueError for unsupported string format."""
    with pytest.raises(ValueError):
        dt_as_dt("not-a-date")


def test_dt_as_dt_invalid_type_raises():
    """Verify dt_as_dt raises TypeError for non-string, non-datetime input."""
    with pytest.raises(TypeError):
        dt_as_dt(12345)


# #############################################################################
# dt_as_str
# #############################################################################

def test_dt_as_str_from_none():
    """Verify dt_as_str returns None for None input."""
    assert dt_as_str(None) is None


def test_dt_as_str_from_datetime():
    """Verify dt_as_str returns canonical string from datetime."""
    d = datetime(2025, 1, 15, 10, 30, 0)
    assert dt_as_str(d) == "2025-01-15 10:30:00"


def test_dt_as_str_from_canonical_string():
    """Verify dt_as_str returns same canonical string unchanged."""
    s = "2025-01-15 10:30:00"
    assert dt_as_str(s) == s


def test_dt_as_str_from_flex_string():
    """Verify dt_as_str converts flex string to canonical format."""
    result = dt_as_str("25-1-5 9:5:0")
    assert result == "2025-01-05 09:05:00"


def test_dt_as_str_invalid_type_raises():
    """Verify dt_as_str raises TypeError for non-string, non-datetime input."""
    with pytest.raises(TypeError):
        dt_as_str(12345)


# #############################################################################
# dt_as_time
# #############################################################################

def test_dt_as_time_from_none():
    """Verify dt_as_time returns None for None input."""
    assert dt_as_time(None) is None


def test_dt_as_time_returns_time_object():
    """Verify dt_as_time returns a datetime.time object."""
    from datetime import time
    result = dt_as_time("10:30:00")
    assert result is not None
    assert isinstance(result, time)
    assert result.hour == 10
    assert result.minute == 30
    assert result.second == 0


def test_dt_as_time_midnight():
    """Verify dt_as_time correctly handles midnight (00:00:00)."""
    from datetime import time
    result = dt_as_time("00:00:00")
    assert result == time(0, 0, 0)


# #############################################################################
# dow_name
# #############################################################################

def test_dow_name_all_days():
    """Verify dow_name returns correct name for each weekday index."""
    assert dow_name(0) == "Monday"
    assert dow_name(1) == "Tuesday"
    assert dow_name(2) == "Wednesday"
    assert dow_name(3) == "Thursday"
    assert dow_name(4) == "Friday"
    assert dow_name(5) == "Saturday"
    assert dow_name(6) == "Sunday"


# #############################################################################
# dt_to_epoch / dt_from_epoch
# #############################################################################

def test_dt_to_epoch_from_none():
    """Verify dt_to_epoch returns None for None input."""
    assert dt_to_epoch(None) is None


def test_dt_to_epoch_returns_int():
    """Verify dt_to_epoch returns an integer epoch value."""
    result = dt_to_epoch("2025-01-01 00:00:00")
    assert isinstance(result, int)


def test_dt_from_epoch_from_none():
    """Verify dt_from_epoch returns None for None input."""
    assert dt_from_epoch(None) is None


def test_dt_from_epoch_returns_datetime():
    """Verify dt_from_epoch returns a datetime object."""
    epoch = dt_to_epoch("2025-01-15 10:30:00")
    result = dt_from_epoch(epoch)
    assert isinstance(result, datetime)


def test_dt_to_epoch_and_back_roundtrip():
    """Verify dt_to_epoch and dt_from_epoch are inverse operations."""
    original = "2025-01-15 10:30:00"
    epoch = dt_to_epoch(original)
    restored = dt_as_str(dt_from_epoch(epoch))
    assert restored == original


# #############################################################################
# timeframe_delta
# #############################################################################

def test_timeframe_delta_1m():
    """Verify timeframe_delta returns 1 minute for '1m'."""
    assert timeframe_delta("1m") == timedelta(minutes=1)


def test_timeframe_delta_5m():
    """Verify timeframe_delta returns 5 minutes for '5m'."""
    assert timeframe_delta("5m") == timedelta(minutes=5)


def test_timeframe_delta_15m():
    """Verify timeframe_delta returns 15 minutes for '15m'."""
    assert timeframe_delta("15m") == timedelta(minutes=15)


def test_timeframe_delta_e1h():
    """Verify timeframe_delta returns 1 hour for 'e1h'."""
    assert timeframe_delta("e1h") == timedelta(hours=1)


def test_timeframe_delta_r1h():
    """Verify timeframe_delta returns 1 hour for 'r1h'."""
    assert timeframe_delta("r1h") == timedelta(hours=1)


def test_timeframe_delta_invalid_raises():
    """Verify timeframe_delta raises ValueError for unsupported timeframe."""
    with pytest.raises(ValueError):
        timeframe_delta("invalid")


# #############################################################################
# start_of_week_date
# #############################################################################

def test_start_of_week_date_from_monday():
    """Verify start_of_week_date returns prior Sunday for Monday input."""
    # 2025-01-13 is a Monday
    result = start_of_week_date("2025-01-13 10:00:00")
    assert result == date(2025, 1, 12)


def test_start_of_week_date_from_sunday():
    """Verify start_of_week_date returns same day for Sunday input."""
    # 2025-01-12 is a Sunday
    result = start_of_week_date("2025-01-12 10:00:00")
    assert result == date(2025, 1, 12)


def test_start_of_week_date_from_saturday():
    """Verify start_of_week_date returns prior Sunday for Saturday input."""
    # 2025-01-18 is a Saturday
    result = start_of_week_date("2025-01-18 10:00:00")
    assert result == date(2025, 1, 12)


def test_start_of_week_date_from_friday():
    """Verify start_of_week_date returns prior Sunday for Friday input."""
    # 2025-01-17 is a Friday
    result = start_of_week_date("2025-01-17 10:00:00")
    assert result == date(2025, 1, 12)


def test_start_of_week_date_returns_date():
    """Verify start_of_week_date returns a date object."""
    result = start_of_week_date("2025-01-15 10:00:00")
    assert isinstance(result, date)


# #############################################################################
# dict_of_weeks
# #############################################################################

def test_dict_of_weeks_single_week():
    """Verify dict_of_weeks returns one entry for dates in the same week."""
    template = {"count": 0}
    result = dict_of_weeks("2025-01-13 00:00:00",
                           "2025-01-17 00:00:00",
                           template)
    assert isinstance(result, dict)
    assert len(result) == 1


def test_dict_of_weeks_multiple_weeks():
    """Verify dict_of_weeks returns correct number of week entries."""
    template = {"count": 0}
    # 4 weeks: Jan 12, Jan 19, Jan 26, Feb 2
    result = dict_of_weeks("2025-01-13 00:00:00",
                           "2025-02-07 00:00:00",
                           template)
    assert len(result) == 4


def test_dict_of_weeks_template_is_copied():
    """Verify dict_of_weeks gives each week its own copy of the template."""
    template = {"count": 0}
    result = dict_of_weeks("2025-01-13 00:00:00",
                           "2025-01-26 00:00:00",
                           template)
    weeks = list(result.keys())
    result[weeks[0]]["count"] = 99
    assert result[weeks[1]]["count"] == 0


def test_dict_of_weeks_keys_are_date_strings():
    """Verify dict_of_weeks keys are date strings starting with Sunday."""
    template = {}
    result = dict_of_weeks("2025-01-13 00:00:00",
                           "2025-01-19 00:00:00",
                           template)
    key = list(result.keys())[0]
    assert key == "2025-01-12"


# #############################################################################
# this_candle_start
# #############################################################################

def test_this_candle_start_1m():
    """Verify this_candle_start returns same minute for 1m timeframe."""
    result = this_candle_start("2025-01-15 10:37:45", "1m")
    assert result == datetime(2025, 1, 15, 10, 37, 0)


def test_this_candle_start_5m():
    """Verify this_candle_start returns correct 5m boundary."""
    result = this_candle_start("2025-01-15 10:37:45", "5m")
    assert result == datetime(2025, 1, 15, 10, 35, 0)


def test_this_candle_start_5m_on_boundary():
    """Verify this_candle_start returns same time when on 5m boundary."""
    result = this_candle_start("2025-01-15 10:35:00", "5m")
    assert result == datetime(2025, 1, 15, 10, 35, 0)


def test_this_candle_start_15m():
    """Verify this_candle_start returns correct 15m boundary."""
    result = this_candle_start("2025-01-15 10:37:45", "15m")
    assert result == datetime(2025, 1, 15, 10, 30, 0)


def test_this_candle_start_15m_on_boundary():
    """Verify this_candle_start returns same time when on 15m boundary."""
    result = this_candle_start("2025-01-15 10:30:00", "15m")
    assert result == datetime(2025, 1, 15, 10, 30, 0)


def test_this_candle_start_r1h():
    """Verify this_candle_start returns :30 start for r1h timeframe."""
    result = this_candle_start("2025-01-15 10:37:45", "r1h")
    assert result == datetime(2025, 1, 15, 10, 30, 0)


def test_this_candle_start_r1h_on_boundary():
    """Verify this_candle_start returns same time when on r1h boundary."""
    result = this_candle_start("2025-01-15 10:30:00", "r1h")
    assert result == datetime(2025, 1, 15, 10, 30, 0)


def test_this_candle_start_e1h():
    """Verify this_candle_start returns :00 start for e1h timeframe."""
    result = this_candle_start("2025-01-15 10:37:45", "e1h")
    assert result == datetime(2025, 1, 15, 10, 0, 0)


def test_this_candle_start_e1h_on_boundary():
    """Verify this_candle_start returns same time when on e1h boundary."""
    result = this_candle_start("2025-01-15 10:00:00", "e1h")
    assert result == datetime(2025, 1, 15, 10, 0, 0)


def test_this_candle_start_e1d_before_6pm():
    """Verify this_candle_start returns yesterday 18:00 before 6pm for e1d."""
    # Before 6pm - should return yesterday at 18:00
    result = this_candle_start("2025-01-15 10:00:00", "e1d")
    assert result == datetime(2025, 1, 14, 18, 0, 0)


def test_this_candle_start_e1d_after_6pm():
    """Verify this_candle_start returns today 18:00 after 6pm for e1d."""
    # After 6pm - should return today at 18:00
    result = this_candle_start("2025-01-15 19:00:00", "e1d")
    assert result == datetime(2025, 1, 15, 18, 0, 0)


def test_this_candle_start_e1d_on_boundary():
    """Verify this_candle_start returns same time when on e1d boundary."""
    result = this_candle_start("2025-01-15 18:00:00", "e1d")
    assert result == datetime(2025, 1, 15, 18, 0, 0)


def test_this_candle_start_e1w_sunday_after_6pm():
    """Verify this_candle_start returns Sun 18:00 when on Sunday after 6pm."""
    # 2025-01-12 is a Sunday
    result = this_candle_start("2025-01-12 19:00:00", "e1w")
    assert result == datetime(2025, 1, 12, 18, 0, 0)


def test_this_candle_start_e1w_mid_week():
    """Verify this_candle_start returns prior Sunday 18:00 for mid-week."""
    # 2025-01-15 is a Wednesday, prior Sunday is 2025-01-12
    result = this_candle_start("2025-01-15 10:00:00", "e1w")
    assert result == datetime(2025, 1, 12, 18, 0, 0)


def test_this_candle_start_e1w_sunday_before_6pm():
    """Verify this_candle_start returns prior Sunday 18:00 if Sun before 6pm.
    """
    # Sunday before 6pm - should return previous Sunday at 18:00
    # 2025-01-12 is Sunday, prior Sunday is 2025-01-05
    result = this_candle_start("2025-01-12 10:00:00", "e1w")
    assert result == datetime(2025, 1, 5, 18, 0, 0)


def test_this_candle_start_invalid_timeframe():
    """Verify this_candle_start raises ValueError for unsupported timeframe."""
    with pytest.raises(ValueError):
        this_candle_start("2025-01-15 10:00:00", "invalid")


# #############################################################################
# rangify_candle_times
# #############################################################################

def test_rangify_candle_times_single_time():
    """Verify rangify_candle_times handles a single time as one-entry list."""
    times = [datetime(2025, 1, 15, 10, 0, 0)]
    result = rangify_candle_times(times, "1m")
    assert len(result) == 1
    assert result[0]["start_dt"] == "2025-01-15 10:00:00"
    assert result[0]["end_dt"] == "2025-01-15 10:00:00"


def test_rangify_candle_times_consecutive():
    """Verify rangify_candle_times collapses consecutive times into one range.
    """
    times = [
        datetime(2025, 1, 15, 10, 0, 0),
        datetime(2025, 1, 15, 10, 1, 0),
        datetime(2025, 1, 15, 10, 2, 0),
    ]
    result = rangify_candle_times(times, "1m")
    assert len(result) == 1
    assert result[0]["start_dt"] == "2025-01-15 10:00:00"
    assert result[0]["end_dt"] == "2025-01-15 10:02:00"


def test_rangify_candle_times_with_gap():
    """Verify rangify_candle_times creates separate ranges when there is a gap.
    """
    times = [
        datetime(2025, 1, 15, 10, 0, 0),
        datetime(2025, 1, 15, 10, 1, 0),
        datetime(2025, 1, 15, 10, 5, 0),
        datetime(2025, 1, 15, 10, 6, 0),
    ]
    result = rangify_candle_times(times, "1m")
    assert len(result) == 2
    assert result[0]["start_dt"] == "2025-01-15 10:00:00"
    assert result[0]["end_dt"] == "2025-01-15 10:01:00"
    assert result[1]["start_dt"] == "2025-01-15 10:05:00"
    assert result[1]["end_dt"] == "2025-01-15 10:06:00"


def test_rangify_candle_times_unsorted_input():
    """Verify rangify_candle_times sorts unsorted input before ranging."""
    times = [
        datetime(2025, 1, 15, 10, 2, 0),
        datetime(2025, 1, 15, 10, 0, 0),
        datetime(2025, 1, 15, 10, 1, 0),
    ]
    result = rangify_candle_times(times, "1m")
    assert len(result) == 1
    assert result[0]["start_dt"] == "2025-01-15 10:00:00"
    assert result[0]["end_dt"] == "2025-01-15 10:02:00"


def test_rangify_candle_times_5m():
    """Verify rangify_candle_times works correctly for 5m timeframe."""
    times = [
        datetime(2025, 1, 15, 10, 0, 0),
        datetime(2025, 1, 15, 10, 5, 0),
        datetime(2025, 1, 15, 10, 10, 0),
    ]
    result = rangify_candle_times(times, "5m")
    assert len(result) == 1
    assert result[0]["start_dt"] == "2025-01-15 10:00:00"
    assert result[0]["end_dt"] == "2025-01-15 10:10:00"


# #############################################################################
# bot
# #############################################################################

def test_bot_returns_string():
    """Verify bot() returns a string."""
    result = bot()
    assert isinstance(result, str)


def test_bot_returns_beginning_of_time():
    """Verify bot() returns the expected beginning of time string."""
    result = bot()
    assert result == "2008-01-01 00:00:00"
