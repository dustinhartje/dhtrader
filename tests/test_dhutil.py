"""Tests for dhutil candle utility functions."""
import os
import pytest
from unittest.mock import patch
from dhtrader import (
    Candle,
    dt_as_dt,
    dt_to_epoch,
    read_candles_from_csv,
    generate_zero_volume_candle,
    compare_candles_vs_csv,
    store_candles_from_csv,
    delete_candles_by_field,
    get_candles,
    remediate_candle_gaps,
)


# Path to the test CSV files included in the repo
TESTS_DIR = os.path.dirname(__file__)
TESTCANDLES_CSV = os.path.join(
    TESTS_DIR, 'test_data_read_2099_candles.csv'
)
TEST_2099_GOOD_CSV = os.path.join(
    TESTS_DIR, 'test_data_2099_candles.csv'
)
TEST_2099_BAD_CSV = os.path.join(
    TESTS_DIR, 'test_data_2099_candles_bad.csv'
)
TEST_2099_GAPS_CSV = os.path.join(
    TESTS_DIR, 'test_data_2099_candles_gaps.csv'
)

# 2099-01-06 is a Tuesday.  In the current (last) market era ETH is
# open 18:00-16:59 daily with only a 17:00-17:59 daily close window.
# Tuesday 18:00-18:09 is therefore OPEN for ETH and NOT open for RTH,
# making it an ideal window for gap-fill tests.
TEST_2099_START = "2099-01-06 18:00:00"
TEST_2099_END = "2099-01-06 18:09:00"
TEST_2099_TIMEFRAME = "1m"
TEST_2099_SYMBOL = "ES"


_TEST_CANDLE_NAME = "DELETEME_DHUTIL_TESTS"


def _delete_2099_candles():
    """Delete only candles stored with the DELETEME_DHUTIL_TESTS name.

    Name-based deletion ensures only test candles are removed,
    regardless of datetime range.
    """
    delete_candles_by_field(
        symbol=TEST_2099_SYMBOL,
        timeframe=TEST_2099_TIMEFRAME,
        field="name",
        value=_TEST_CANDLE_NAME,
    )


@pytest.fixture
def cleanup_2099_candles():
    """Clean the 2099 test candles before and after the test.

    Runs a pre-test cleanup to clear any leftover state, yields
    control to the test, then always runs a post-test cleanup
    regardless of pass or fail.  Deletion is scoped to
    _TEST_CANDLE_NAME so it can never touch production candles.
    """
    _delete_2099_candles()
    yield
    _delete_2099_candles()


@pytest.mark.suppress_stdout
def test_read_candles_from_csv():
    """Verify read_candles_from_csv reads and filters CSV candles correctly."""
    # Returns a list of Candle objects when dates span all rows
    result = read_candles_from_csv(
        start_dt="2099-01-01 00:00:00",
        end_dt="2099-12-31 23:59:59",
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
        start_dt="2099-02-01 00:00:00",
        end_dt="2099-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result_start) == 5
    for c in result_start:
        assert dt_as_dt(c.c_datetime) >= dt_as_dt("2099-02-01 00:00:00")
    # end_dt filter excludes later candles
    result_end = read_candles_from_csv(
        start_dt="2099-01-01 00:00:00",
        end_dt="2099-01-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result_end) == 5
    for c in result_end:
        assert dt_as_dt(c.c_datetime) <= dt_as_dt("2099-01-31 23:59:59")
    # Out-of-range dates return an empty list
    result_empty = read_candles_from_csv(
        start_dt="2100-01-01 00:00:00",
        end_dt="2100-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
    )
    assert result_empty == []
    # Candle attributes match values from the CSV source file
    result_one = read_candles_from_csv(
        start_dt="2099-01-01 18:00:00",
        end_dt="2099-01-01 18:00:00",
        filepath=TESTCANDLES_CSV,
    )
    assert len(result_one) == 1
    c = result_one[0]
    assert c.c_datetime == "2099-01-01 18:00:00"
    assert c.c_open == 4818.00
    assert c.c_high == 4819.50
    assert c.c_low == 4815.75
    assert c.c_close == 4818.75
    assert c.c_volume == 1483
    # Default timeframe is 1m
    for candle in result:
        assert candle.c_timeframe == "1m"
    # Default symbol is ES
    for candle in result:
        assert candle.c_symbol.ticker == "ES"
    # Custom timeframe is applied when provided
    result_tf = read_candles_from_csv(
        start_dt="2099-01-01 00:00:00",
        end_dt="2099-12-31 23:59:59",
        filepath=TESTCANDLES_CSV,
        timeframe="5m",
    )
    for candle in result_tf:
        assert candle.c_timeframe == "5m"
    # Candles are returned in CSV file order
    assert result_end[0].c_datetime == "2099-01-01 18:00:00"
    assert result_end[-1].c_datetime == "2099-01-01 18:04:00"


@pytest.mark.suppress_stdout
def test_generate_zero_volume_candle():
    """Verify generate_zero_volume_candle output for common scenarios.

    Storage calls are mocked so this test does not write or read any
    real data.
    """
    prior_candle = Candle(
        c_datetime="2099-03-01 18:00:00",
        c_timeframe="1m",
        c_open=5000.00,
        c_high=5001.00,
        c_low=4999.00,
        c_close=5000.50,
        c_volume=100,
        c_symbol="ES",
    )
    target_dt = "2099-03-01 18:01:00"

    # Returns a Candle when exactly one prior candle is found
    with patch('dhtrader.dhutil.get_candles',
               return_value=[prior_candle]):
        result = generate_zero_volume_candle(
            c_datetime=target_dt,
            timeframe="1m",
            symbol="ES",
        )
    assert isinstance(result, Candle)
    assert result.c_datetime == target_dt
    assert result.c_volume == 0
    # All OHLC values are set to the prior candle's close
    assert result.c_open == prior_candle.c_close
    assert result.c_high == prior_candle.c_close
    assert result.c_low == prior_candle.c_close
    assert result.c_close == prior_candle.c_close
    assert result.c_timeframe == "1m"
    assert result.c_symbol.ticker == "ES"

    # Returns None when no prior candle exists in storage
    with patch('dhtrader.dhutil.get_candles', return_value=[]):
        result_none = generate_zero_volume_candle(
            c_datetime=target_dt,
            timeframe="1m",
            symbol="ES",
        )
    assert result_none is None

    # Returns None when storage returns multiple candles (ambiguous)
    prior_candle2 = Candle(
        c_datetime="2099-03-01 17:59:00",
        c_timeframe="1m",
        c_open=5001.00,
        c_high=5002.00,
        c_low=5000.00,
        c_close=5001.50,
        c_volume=50,
        c_symbol="ES",
    )
    with patch('dhtrader.dhutil.get_candles',
               return_value=[prior_candle, prior_candle2]):
        result_multi = generate_zero_volume_candle(
            c_datetime=target_dt,
            timeframe="1m",
            symbol="ES",
        )
    assert result_multi is None

    # Unsupported symbol raises ValueError
    with pytest.raises(ValueError,
                       match="Only symbol: 'ES' is currently supported"):
        generate_zero_volume_candle(
            c_datetime=target_dt,
            timeframe="1m",
            symbol="NQ",
        )

    # Unsupported timeframe raises ValueError
    with pytest.raises(ValueError,
                       match="timeframe: 5m is not currently supported"):
        generate_zero_volume_candle(
            c_datetime=target_dt,
            timeframe="5m",
            symbol="ES",
        )

    # Verify get_candles is called for the correct prior epoch
    # (one minute before the target datetime)
    with patch('dhtrader.dhutil.get_candles') as mock_get:
        mock_get.return_value = [prior_candle]
        generate_zero_volume_candle(
            c_datetime=target_dt,
            timeframe="1m",
            symbol="ES",
        )
    expected_epoch = dt_to_epoch(dt_as_dt("2099-03-01 18:00:00"))
    mock_get.assert_called_once_with(
        start_epoch=expected_epoch,
        end_epoch=expected_epoch,
        timeframe="1m",
        symbol="ES",
    )


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_store_candles_from_csv_and_compare(cleanup_2099_candles):
    """Store candles from a CSV then verify compare_candles_vs_csv results.

    Uses a 2099 time window to guarantee isolation from all real data.
    The fixture ensures candles are removed before and after the test.

    Storage Usage: store_candles_from_csv, get_candles,
    compare_candles_vs_csv.
    """
    # Store candles from the good CSV
    store_candles_from_csv(
        filepath=TEST_2099_GOOD_CSV,
        start_dt=TEST_2099_START,
        end_dt=TEST_2099_END,
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
        name=_TEST_CANDLE_NAME,
    )
    # Confirm all 10 candles were persisted
    stored = get_candles(
        start_epoch=dt_to_epoch(TEST_2099_START),
        end_epoch=dt_to_epoch(TEST_2099_END),
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
    )
    assert len(stored) == 10
    for c in stored:
        assert c.c_timeframe == TEST_2099_TIMEFRAME
        assert c.c_symbol.ticker == TEST_2099_SYMBOL

    # compare_candles_vs_csv against the good CSV should report all_equal
    result = compare_candles_vs_csv(
        filepath=TEST_2099_GOOD_CSV,
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
        start_dt=TEST_2099_START,
        end_dt=TEST_2099_END,
    )
    assert result is not None
    assert result["all_equal"] is True
    assert result["counts"]["stored_candles"] == 10
    assert result["counts"]["csv_candles"] == 10
    assert result["counts"]["missing_from_storage"] == 0
    assert result["counts"]["extras_in_storage"] == 0
    assert result["counts"]["diffs_from_csv"] == 0
    assert result["counts"]["minor_diffs_from_csv"] == 0
    assert result["missing_from_storage"] == {}
    assert result["extras_in_storage"] == {}
    assert result["differences"] == {}

    # compare_candles_vs_csv against the bad CSV should detect errors:
    #   candle 18:01 has a large close diff (5501.00 stored vs 5501.75 CSV)
    #   candle 18:02 has a large volume diff (600 stored vs 20 CSV)
    #   candle 18:03 is missing from the bad CSV (extra in storage)
    result_bad = compare_candles_vs_csv(
        filepath=TEST_2099_BAD_CSV,
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
        start_dt=TEST_2099_START,
        end_dt=TEST_2099_END,
    )
    assert result_bad is not None
    assert result_bad["all_equal"] is False
    # 18:03 is in storage but not in the bad CSV
    assert result_bad["counts"]["extras_in_storage"] == 1
    # close diff on 18:01, volume diff on 18:02 = 2 diff fields total
    assert result_bad["counts"]["diffs_from_csv"] == 2
    # No candles in the bad CSV should be missing from storage
    assert result_bad["counts"]["missing_from_storage"] == 0
    # Verify the specific datetimes and that each diff is classified as
    # a major (not minor) discrepancy
    bad_diffs = result_bad["differences"]
    assert "2099-01-06 18:01:00" in bad_diffs
    close_diff = bad_diffs["2099-01-06 18:01:00"]
    assert "c_close" in close_diff["diffs"]
    assert "c_close" not in close_diff["minor_diffs"]
    assert "2099-01-06 18:02:00" in bad_diffs
    vol_diff = bad_diffs["2099-01-06 18:02:00"]
    assert "c_volume" in vol_diff["diffs"]
    assert "c_volume" not in vol_diff["minor_diffs"]


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_remediate_candle_gaps(cleanup_2099_candles):
    """Test remediate_candle_gaps on a controlled 2099 candle set.

    Stores 7 candles with 3 deliberate gaps in a Tuesday 18:xx ETH
    window (never RTH).  Gap classification is driven by the average
    volume of adjacent candles:

      18:00 - UNCLEAR: no prior candle (first open minute); all 5
              post-neighbors vol=600 (avg=600 >= 500).  Returns error
              because no prior candle exists to anchor the zero-vol fill.
      18:06 - OBVIOUS: mixed neighbors avg ~486 < 500, ETH, has pre+post
      18:08 - OBVIOUS: mixed neighbors avg=440 < 500, ETH, has pre+post

    Runs with dry_run=True first to confirm detection without storing,
    then dry_run=False to confirm the zero-volume fix candles are stored.
    All auto-fix arguments are True and prompt=False to avoid any
    interactive input.

    Storage Usage: store_candles_from_csv, get_candles,
    remediate_candle_gaps.
    """
    # Store the 7 gap candles into the 2099 test window
    store_candles_from_csv(
        filepath=TEST_2099_GAPS_CSV,
        start_dt=TEST_2099_START,
        end_dt=TEST_2099_END,
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
        name=_TEST_CANDLE_NAME,
    )
    stored = get_candles(
        start_epoch=dt_to_epoch(TEST_2099_START),
        end_epoch=dt_to_epoch(TEST_2099_END),
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
    )
    assert len(stored) == 7

    # --- dry_run=True: verify gaps are found but nothing is stored ---
    result_dry = remediate_candle_gaps(
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
        prompt=False,
        fix_obvious=True,
        fix_unclear=True,
        dry_run=True,
        start_dt=TEST_2099_START,
        end_dt=TEST_2099_END,
        name=_TEST_CANDLE_NAME,
    )
    # 2 obvious gaps (18:06, 18:08), 0 unclear fixed (18:00 errors)
    assert len(result_dry["fixed_obvious"]) == 2
    assert len(result_dry["fixed_unclear"]) == 0
    assert len(result_dry["skipped"]) == 0
    assert len(result_dry["errored"]) == 1
    # Dry run must NOT write any candles to storage
    stored_after_dry = get_candles(
        start_epoch=dt_to_epoch(TEST_2099_START),
        end_epoch=dt_to_epoch(TEST_2099_END),
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
    )
    assert len(stored_after_dry) == 7

    # --- dry_run=False: verify gaps are actually filled in storage ---
    result_real = remediate_candle_gaps(
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
        prompt=False,
        fix_obvious=True,
        fix_unclear=True,
        dry_run=False,
        start_dt=TEST_2099_START,
        end_dt=TEST_2099_END,
        name=_TEST_CANDLE_NAME,
    )
    assert len(result_real["fixed_obvious"]) == 2
    assert len(result_real["fixed_unclear"]) == 0
    assert len(result_real["skipped"]) == 0
    assert len(result_real["errored"]) == 1

    # 9 candles should now be in storage (7 original + 2 obvious fixes;
    # 18:00 errored so it was not stored)
    stored_final = get_candles(
        start_epoch=dt_to_epoch(TEST_2099_START),
        end_epoch=dt_to_epoch(TEST_2099_END),
        timeframe=TEST_2099_TIMEFRAME,
        symbol=TEST_2099_SYMBOL,
    )
    assert len(stored_final) == 9

    # Verify each zero-volume candle has correct OHLC and vol=0
    stored_by_dt = {c.c_datetime: c for c in stored_final}

    # 18:00 gap errored: no prior candle, so it was not stored
    assert "2099-01-06 18:00:00" not in stored_by_dt

    # 18:06 obvious gap: prior candle is 18:05 (close=5502.50)
    assert "2099-01-06 18:06:00" in stored_by_dt
    c_06 = stored_by_dt["2099-01-06 18:06:00"]
    assert c_06.c_volume == 0
    assert c_06.c_open == 5502.50
    assert c_06.c_high == 5502.50
    assert c_06.c_low == 5502.50
    assert c_06.c_close == 5502.50

    # 18:08 obvious gap: prior candle is 18:07 (close=5503.25)
    assert "2099-01-06 18:08:00" in stored_by_dt
    c_08 = stored_by_dt["2099-01-06 18:08:00"]
    assert c_08.c_volume == 0
    assert c_08.c_open == 5503.25
    assert c_08.c_high == 5503.25
    assert c_08.c_low == 5503.25
    assert c_08.c_close == 5503.25
