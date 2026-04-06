"""Tests for dhmongo low-level MongoDB operation functions."""
import pytest
from dhtrader.dhmongo import (
    delete_backtests,
    delete_backtests_by_field,
    delete_candles,
    delete_candles_by_field,
    delete_events_by_field,
    delete_indicator,
    delete_trades_by_field,
    delete_tradeseries,
    finish_progbar,
    get_backtests_by_field,
    get_candles,
    get_events,
    get_indicator,
    get_indicator_datapoints,
    get_trades_by_field,
    get_tradeseries_by_field,
    list_collections,
    review_database,
    start_progbar,
    store_backtest,
    store_candle,
    store_event,
    store_indicator,
    store_indicator_datapoints,
    store_trades,
    store_tradeseries,
    update_progbar,
)
from dhtrader.dhcommon import dt_to_epoch

# ---------------------------------------------------------------------------
# Sentinel values used across all storage tests.
# ---------------------------------------------------------------------------
_TEST_DELETEME_NAME = "DELETEME_DHMONGO_TESTS"
_TEST_IND_ID = "DELETEME_DHMONGO_IND"
_TEST_EVENT_SYMBOL = "ES"
_TEST_DT = "2099-12-31 22:00:00"   # far-future sentinel used across tests
_TEST_EVENT_DT = _TEST_DT


@pytest.fixture
def cleanup_dhmongo_storage():
    """Clean up DELETEME test records before and after each storage test.

    Uses name-based deletion for all object types so that test records
    are unambiguously identified and cleaned from production collections.
    Runs both before and after the test so that stale state from a prior
    failed run cannot affect results.
    """

    def _cleanup():
        delete_trades_by_field(
            symbol="ES",
            field="name",
            value=_TEST_DELETEME_NAME,
            collection="trades",
        )
        delete_tradeseries(
            ts_ids=[_TEST_DELETEME_NAME],
            collection="tradeseries",
        )
        delete_backtests(
            bt_ids=[_TEST_DELETEME_NAME],
            collection="backtests",
        )
        delete_indicator(
            ind_id=_TEST_IND_ID,
            meta_collection="indicators_meta",
            dp_collection="indicators_datapoints",
        )
        delete_candles_by_field(
            symbol=_TEST_EVENT_SYMBOL,
            timeframe="1m",
            field="name",
            value=_TEST_DELETEME_NAME,
        )
        delete_events_by_field(
            symbol=_TEST_EVENT_SYMBOL,
            field="name",
            value=_TEST_DELETEME_NAME,
        )

    _cleanup()
    yield
    _cleanup()


@pytest.mark.suppress_stdout
def test_start_progbar():
    """Verify start_progbar returns None when progress display is disabled."""
    # Returns None when show_progress is False
    result = start_progbar(show_progress=False, total=10, desc="test")
    assert result is None
    # Returns None when total is 0
    result2 = start_progbar(show_progress=True, total=0, desc="test")
    assert result2 is None


@pytest.mark.suppress_stdout
def test_update_and_finish_progbar_with_none():
    """Verify update_progbar and finish_progbar handle None pbar gracefully."""
    update_progbar(pbar=None, index=1, total=10)
    finish_progbar(pbar=None)


# #############################################################################
# Storage-dependent tests
# #############################################################################

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_list_collections_returns_list():
    """Verify list_collections returns a list containing events_ES.

    Storage Usage: list_collections queries MongoDB.
    """
    result = list_collections()
    assert isinstance(result, list)
    assert "events_ES" in result


@pytest.mark.storage
@pytest.mark.suppress_stdout
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
@pytest.mark.suppress_stdout
def test_store_and_get_candle_roundtrip(cleanup_dhmongo_storage):
    """Verify store_candle and get_candles work as a roundtrip.

    Uses a far-future sentinel datetime and name="DELETEME_DHMONGO_TESTS"
    so the test record is unambiguously identifiable.  Cleanup is driven
    by the name field via delete_candles_by_field.

    Storage Usage: store_candle writes, get_candles reads,
    delete_candles_by_field cleans up.
    """
    test_dt = _TEST_DT
    test_epoch = dt_to_epoch(test_dt)
    test_symbol = "ES"
    test_tf = "1m"

    # Store a test candle with DELETEME name
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
        name=_TEST_DELETEME_NAME,
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
    assert doc["name"] == _TEST_DELETEME_NAME

    # Cleanup by name field
    delete_candles_by_field(
        symbol=test_symbol,
        timeframe=test_tf,
        field="name",
        value=_TEST_DELETEME_NAME,
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
@pytest.mark.suppress_stdout
def test_get_candles_empty_range():
    """Verify get_candles returns empty list for out-of-range epochs.

    Storage Usage: get_candles queries MongoDB.
    """
    # Use an epoch far in the future where no data should exist
    far_epoch = dt_to_epoch("2098-01-01 00:00:00")
    results = get_candles(
        start_epoch=far_epoch,
        end_epoch=far_epoch,
        timeframe="1m",
        symbol="ES",
    )
    assert isinstance(results, list)
    assert len(results) == 0


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_store_and_get_trade_roundtrip(cleanup_dhmongo_storage):
    """Verify store_trades and get_trades_by_field work as a roundtrip.

    Storage Usage: store_trades writes, get_trades_by_field reads,
    delete_trades_by_field cleans up.
    """
    test_dt = _TEST_DT
    test_open_epoch = dt_to_epoch(test_dt)
    trade_doc = {
        "open_dt": test_dt,
        "close_dt": "2099-12-31 23:00:00",
        "direction": "long",
        "name": _TEST_DELETEME_NAME,
        "version": "1.0.0",
        "symbol": "ES",
        "ts_id": _TEST_DELETEME_NAME,
        "bt_id": _TEST_DELETEME_NAME,
        "entry_price": 9000.0,
        "exit_price": 9010.0,
        "open_epoch": test_open_epoch,
        "trade_id": f"{_TEST_DELETEME_NAME}_{test_open_epoch}",
    }

    # Store the trade
    store_trades(trades=[trade_doc], collection="trades")

    # Retrieve by name
    results = get_trades_by_field(
        field="name",
        value=_TEST_DELETEME_NAME,
        collection="trades",
    )
    assert len(results) == 1
    doc = results[0]
    assert doc["open_dt"] == test_dt
    assert doc["direction"] == "long"
    assert doc["name"] == _TEST_DELETEME_NAME
    assert doc["entry_price"] == 9000.0
    assert doc["exit_price"] == 9010.0
    assert doc["trade_id"] == f"{_TEST_DELETEME_NAME}_{test_open_epoch}"

    # Delete by name field
    delete_trades_by_field(
        symbol="ES",
        field="name",
        value=_TEST_DELETEME_NAME,
        collection="trades",
    )

    # Verify deletion
    after = get_trades_by_field(
        field="name",
        value=_TEST_DELETEME_NAME,
        collection="trades",
    )
    assert len(after) == 0


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_store_and_get_tradeseries_roundtrip(cleanup_dhmongo_storage):
    """Verify store_tradeseries and get_tradeseries_by_field roundtrip.

    Storage Usage: store_tradeseries writes, get_tradeseries_by_field
    reads, delete_tradeseries cleans up.
    """
    ts_doc = {
        "ts_id": _TEST_DELETEME_NAME,
        "name": _TEST_DELETEME_NAME,
        "bt_id": _TEST_DELETEME_NAME,
        "symbol": "ES",
        "start_dt": "2099-12-31 18:00:00",
        "end_dt": "2099-12-31 23:00:00",
    }

    # Store the tradeseries
    store_tradeseries(series=ts_doc, collection="tradeseries")

    # Retrieve by ts_id
    results = get_tradeseries_by_field(
        field="ts_id",
        value=_TEST_DELETEME_NAME,
        collection="tradeseries",
    )
    assert len(results) == 1
    doc = results[0]
    assert doc["ts_id"] == _TEST_DELETEME_NAME
    assert doc["name"] == _TEST_DELETEME_NAME

    # Delete by ts_id
    delete_tradeseries(
        ts_ids=[_TEST_DELETEME_NAME],
        collection="tradeseries",
    )

    # Verify deletion
    after = get_tradeseries_by_field(
        field="ts_id",
        value=_TEST_DELETEME_NAME,
        collection="tradeseries",
    )
    assert len(after) == 0


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_store_and_get_backtest_roundtrip(cleanup_dhmongo_storage):
    """Verify store_backtest and get_backtests_by_field work as a roundtrip.

    Storage Usage: store_backtest writes, get_backtests_by_field reads,
    delete_backtests cleans up.
    """
    bt_doc = {
        "bt_id": _TEST_DELETEME_NAME,
        "name": _TEST_DELETEME_NAME,
        "symbol": "ES",
        "start_dt": "2099-12-31 00:00:00",
        "end_dt": "2099-12-31 23:59:59",
    }

    # Store the backtest
    store_backtest(backtest=bt_doc, collection="backtests")

    # Retrieve by bt_id
    results = get_backtests_by_field(
        field="bt_id",
        value=_TEST_DELETEME_NAME,
        collection="backtests",
    )
    assert len(results) == 1
    doc = results[0]
    assert doc["bt_id"] == _TEST_DELETEME_NAME
    assert doc["name"] == _TEST_DELETEME_NAME

    # Delete by bt_id
    delete_backtests(
        bt_ids=[_TEST_DELETEME_NAME],
        collection="backtests",
    )

    # Verify deletion
    after = get_backtests_by_field(
        field="bt_id",
        value=_TEST_DELETEME_NAME,
        collection="backtests",
    )
    assert len(after) == 0


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_store_and_get_indicator_roundtrip(cleanup_dhmongo_storage):
    """Verify indicator meta and datapoint store/get/delete roundtrip.

    Storage Usage: store_indicator and store_indicator_datapoints write,
    get_indicator and get_indicator_datapoints read, delete_indicator
    removes both meta and datapoints.
    """
    ind_doc = {
        "ind_id": _TEST_IND_ID,
        "name": _TEST_DELETEME_NAME,
        "symbol": "ES",
        "timeframe": "1m",
        "type": "EMA",
        "params": {"period": 9},
    }
    dp_dt = _TEST_DT
    dp_epoch = dt_to_epoch(dp_dt)
    dp_doc = {
        "ind_id": _TEST_IND_ID,
        "dt": dp_dt,
        "epoch": dp_epoch,
        "value": 9000.0,
    }

    # Store indicator meta and one datapoint
    store_indicator(indicator=ind_doc, meta_collection="indicators_meta")
    store_indicator_datapoints(
        datapoints=[dp_doc],
        collection="indicators_datapoints",
    )

    # Retrieve meta
    meta_results = get_indicator(
        ind_id=_TEST_IND_ID,
        meta_collection="indicators_meta",
        autoload_datapoints=False,
    )
    assert len(meta_results) == 1
    assert meta_results[0]["ind_id"] == _TEST_IND_ID
    assert meta_results[0]["name"] == _TEST_DELETEME_NAME

    # Retrieve datapoints
    dp_results = get_indicator_datapoints(
        ind_id=_TEST_IND_ID,
        dp_collection="indicators_datapoints",
        earliest_dt=dp_dt,
        latest_dt=dp_dt,
    )
    assert len(dp_results) == 1
    assert dp_results[0]["ind_id"] == _TEST_IND_ID
    assert dp_results[0]["value"] == 9000.0

    # Delete indicator meta and all its datapoints
    delete_indicator(
        ind_id=_TEST_IND_ID,
        meta_collection="indicators_meta",
        dp_collection="indicators_datapoints",
    )

    # Verify meta deleted
    after_meta = get_indicator(
        ind_id=_TEST_IND_ID,
        meta_collection="indicators_meta",
        autoload_datapoints=False,
    )
    assert len(after_meta) == 0

    # Verify datapoints deleted
    after_dps = get_indicator_datapoints(
        ind_id=_TEST_IND_ID,
        dp_collection="indicators_datapoints",
        earliest_dt=dp_dt,
        latest_dt=dp_dt,
    )
    assert len(after_dps) == 0


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_store_and_get_event_roundtrip(cleanup_dhmongo_storage):
    """Verify store_event and get_events work as a roundtrip.

    Uses name="DELETEME_DHMONGO_TESTS" for unambiguous identification
    and cleanup via delete_events_by_field.

    Storage Usage: store_event writes, get_events reads,
    delete_events_by_field cleans up.
    """
    start_epoch = dt_to_epoch(_TEST_EVENT_DT)
    end_epoch = dt_to_epoch("2099-12-31 23:00:00")

    # Store the event with DELETEME name
    store_event(
        start_dt=_TEST_EVENT_DT,
        end_dt="2099-12-31 23:00:00",
        symbol=_TEST_EVENT_SYMBOL,
        category="DELETEME_category",
        tags=["DELETEME"],
        notes="DELETEME test event for dhmongo roundtrip",
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        name=_TEST_DELETEME_NAME,
    )

    # Retrieve the event
    results = get_events(
        symbol=_TEST_EVENT_SYMBOL,
        start_epoch=start_epoch,
        end_epoch=start_epoch,
    )
    matching = [e for e in results if e["start_dt"] == _TEST_EVENT_DT]
    assert len(matching) == 1
    event = matching[0]
    assert event["name"] == _TEST_DELETEME_NAME
    assert "DELETEME" in event["tags"]

    # Cleanup by name field
    delete_events_by_field(
        symbol=_TEST_EVENT_SYMBOL,
        field="name",
        value=_TEST_DELETEME_NAME,
    )

    # Verify deletion
    after = get_events(
        symbol=_TEST_EVENT_SYMBOL,
        start_epoch=start_epoch,
        end_epoch=start_epoch,
    )
    matching_after = [
        e for e in after if e["start_dt"] == _TEST_EVENT_DT
    ]
    assert len(matching_after) == 0
