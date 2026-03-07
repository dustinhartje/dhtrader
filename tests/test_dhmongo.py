"""Tests for dhmongo low-level MongoDB operation functions."""
import pytest
from dhtrader.dhmongo import (
    finish_progbar,
    list_collections,
    review_database,
    start_progbar,
    store_candle,
    get_candles,
    delete_candles,
    update_progbar,
)
from dhtrader.dhcommon import dt_to_epoch


# #############################################################################
# start_progbar / update_progbar / finish_progbar
# These helper functions do not require MongoDB connections.
# #############################################################################

def test_start_progbar_returns_none_when_not_shown():
    """Verify start_progbar returns None when show_progress is False."""
    result = start_progbar(show_progress=False, total=10, desc="test")
    assert result is None


def test_start_progbar_returns_none_when_total_zero():
    """Verify start_progbar returns None when total is 0."""
    result = start_progbar(show_progress=True, total=0, desc="test")
    assert result is None


def test_update_progbar_with_none_does_not_raise():
    """Verify update_progbar handles None pbar without raising."""
    update_progbar(pbar=None, index=1, total=10)


def test_finish_progbar_with_none_does_not_raise():
    """Verify finish_progbar handles None pbar without raising."""
    finish_progbar(pbar=None)


# #############################################################################
# MongoDB-dependent functions (require @pytest.mark.storage)
# #############################################################################

@pytest.mark.storage
def test_list_collections_returns_list():
    """Verify list_collections returns a list.

    Storage Usage: list_collections queries MongoDB.
    """
    result = list_collections()
    assert isinstance(result, list)


@pytest.mark.storage
def test_review_database_returns_dict():
    """Verify review_database returns dict with expected keys.

    Storage Usage: review_database queries MongoDB dbstats.
    """
    result = review_database()
    assert isinstance(result, dict)
    assert "raw" in result
    assert "overview" in result
    overview = result["overview"]
    assert "collection" in overview
    assert "objects" in overview
    assert "data_GB" in overview
    assert "storage_GB" in overview


@pytest.mark.storage
def test_store_and_get_candle_roundtrip():
    """Verify store_candle and get_candles work as a roundtrip.

    Storage Usage: store_candle writes, get_candles reads,
    delete_candles cleans up.
    """
    test_dt = "2099-12-31 23:00:00"
    test_epoch = dt_to_epoch(test_dt)
    test_symbol = "ES"
    test_tf = "1m"

    # Store a test candle
    store_candle(
        c_datetime=test_dt,
        c_timeframe=test_tf,
        c_open=9000.0,
        c_high=9010.0,
        c_low=8990.0,
        c_close=9005.0,
        c_volume=100,
        c_symbol=test_symbol,
        c_epoch=test_epoch,
        c_date=test_dt[:10],
        c_time=test_dt[11:19],
    )

    # Retrieve the candle
    results = get_candles(
        start_epoch=test_epoch,
        end_epoch=test_epoch,
        timeframe=test_tf,
        symbol=test_symbol,
    )
    assert len(results) == 1
    doc = results[0]
    assert doc["c_datetime"] == test_dt
    assert doc["c_open"] == 9000.0
    assert doc["c_high"] == 9010.0
    assert doc["c_low"] == 8990.0
    assert doc["c_close"] == 9005.0
    assert doc["c_volume"] == 100
    assert doc["c_symbol"] == test_symbol

    # Cleanup
    delete_candles(
        timeframe=test_tf,
        symbol=test_symbol,
        earliest_dt=test_dt,
        latest_dt=test_dt,
    )

    # Verify cleanup
    after = get_candles(
        start_epoch=test_epoch,
        end_epoch=test_epoch,
        timeframe=test_tf,
        symbol=test_symbol,
    )
    assert len(after) == 0


@pytest.mark.storage
def test_get_candles_empty_range():
    """Verify get_candles returns empty list for out-of-range epochs.

    Storage Usage: get_candles queries MongoDB.
    """
    # Use an epoch far in the future where no data should exist
    far_epoch = dt_to_epoch("2099-01-01 00:00:00")
    results = get_candles(
        start_epoch=far_epoch,
        end_epoch=far_epoch,
        timeframe="1m",
        symbol="ES",
    )
    assert isinstance(results, list)
    assert len(results) == 0
