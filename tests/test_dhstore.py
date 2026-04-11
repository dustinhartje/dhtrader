"""Tests for dhstore storage functions.

Covers TradeSeries and backtests integrity storage checks, plus custom document
store/retrieve/delete/review functions (and their dhmongo backing functions)
because these have no class to build a dedicated test file for.
"""
import re
import pytest
from dhtrader import (
    Backtest,
    COLLECTIONS,
    COLL_PATTERNS,
    delete_backtests_by_field,
    delete_custom_documents_by_field,
    get_all_custom_documents,
    get_backtests_by_field,
    get_custom_documents_by_field,
    get_trades_by_field,
    get_tradeseries_by_field,
    list_custom_documents,
    review_custom_documents,
    review_trades,
    review_tradeseries,
    store_backtests,
    store_custom_documents,
    store_trades,
    store_tradeseries,
    Trade,
    TradeSeries,
)
import dhtrader.dhmongo as _dhm


def clear_storage_by_name(name: str):
    """Delete all Backtests, TradeSeries, and Trades with the given name."""
    delete_backtests_by_field(symbol="ES", field="name", value=name,
                              include_tradeseries=True, include_trades=True)
    r = get_backtests_by_field(field="name", value=name)
    assert len(r) == 0
    r = get_tradeseries_by_field(field="name", value=name)
    assert len(r) == 0
    r = get_trades_by_field(field="name", value=name)
    assert len(r) == 0


@pytest.fixture
def cleanup_dhstore_storage():
    """Register integrity-test names for pre- and post-test cleanup.

    The returned helper records each supplied name, immediately clears any
    matching Backtests, TradeSeries, and Trades before the test continues,
    then clears the full registered set again during fixture teardown.
    """
    names = set()

    def register(*new_names):
        for name in new_names:
            names.add(name)
        for name in sorted(names):
            clear_storage_by_name(name)

    yield register

    for name in sorted(names):
        clear_storage_by_name(name)


@pytest.mark.slow
@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_Backtest_TradeSeries_and_Trade_integrity_checks(
        cleanup_dhstore_storage):
    """Verify storage integrity checks fail for invalid Backtests, TradeSeries,
    and Trades.

    Storage Usage: store_tradeseries, store_backtests, store_trades,
    get_tradeseries_by_field, get_trades_by_field, review_tradeseries,
    review_trades.
    """
    bt = "DELETEME_DHSTORE_TESTS"

    # Store a control TradeSeries that has compliant Trades
    control_name = f"{bt}_control_pass"
    cleanup_dhstore_storage(control_name)
    ts_good = TradeSeries(start_dt="2099-01-05 00:00:00",
                          end_dt="2099-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=control_name,
                          params_str="",
                          ts_id=control_name,
                          bt_id=bt)
    ts_good.add_trade(Trade(
        open_dt="2099-01-05 10:03:24", direction="long",
        close_dt="2099-01-05 10:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=control_name,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    ts_good.add_trade(Trade(
        open_dt="2099-01-05 11:23:25", direction="long",
        close_dt="2099-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=control_name,
        ts_id=ts_good.ts_id, bt_id=ts_good.bt_id))
    store_tradeseries([ts_good], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2

    # Confirm storage integrity check against sample bt_id passes
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None

    # Confirm Trade storage integrity check against bt_id passes, ignoring
    # test bt for orphan checks
    r = review_trades(bt_id=bt, check_integrity=True, orphan_ok=[bt])
    print(r)
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None

    # Clean from storage and verify gone
    ts_good.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Store TradeSeries with multiple Trades opening in the same bar
    same_bar_name = f"{bt}_same_bar_fail"
    cleanup_dhstore_storage(same_bar_name)
    ts_fail = TradeSeries(start_dt="2099-01-05 00:00:00",
                          end_dt="2099-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=same_bar_name,
                          params_str="",
                          ts_id=same_bar_name,
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2099-01-05 10:03:24", direction="long",
        close_dt="2099-01-05 10:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=same_bar_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2099-01-05 10:23:25", direction="long",
        close_dt="2099-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=same_bar_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2

    # Confirm storage integrity check fails for sample bt_id
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == {"trade_overlaps": [
        {"issue_type": "Trade timeframe bar overlap",
         "ts_id": same_bar_name,
         "timeframe": "e1h",
         "trade_open": "2099-01-05 10:23:25",
         "trade_open_tf": "2099-01-05 10:00:00",
         "prev_trade_open": "2099-01-05 10:03:24",
         "prev_trade_close": "2099-01-05 10:45:32",
         "prev_trade_close_tf": "2099-01-05 10:00:00"}]}

    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Store TradeSeries with a Trade opening in the same bar that the prior
    # Trade closed in, after running past the original opening bar
    next_bar_name = f"{bt}_next_bar_fail"
    cleanup_dhstore_storage(next_bar_name)
    ts_fail = TradeSeries(start_dt="2099-01-05 00:00:00",
                          end_dt="2099-01-06 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=next_bar_name,
                          params_str="",
                          ts_id=next_bar_name,
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2099-01-05 10:03:24", direction="long",
        close_dt="2099-01-05 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=next_bar_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2099-01-05 11:53:18", direction="long",
        close_dt="2099-01-05 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=next_bar_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm storage integrity check against sample bt_id fails
    r = review_tradeseries(bt_id=bt, check_integrity=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == {'trade_overlaps': [
        {'issue_type': 'Trade timeframe bar overlap',
         'ts_id': next_bar_name,
         'timeframe': 'e1h',
         'trade_open': '2099-01-05 11:53:18',
         'trade_open_tf': '2099-01-05 11:00:00',
         'prev_trade_open': '2099-01-05 10:03:24',
         'prev_trade_close': '2099-01-05 11:45:32',
         'prev_trade_close_tf': '2099-01-05 11:00:00'}]}
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Add a multiday Trade and confirm Trades storage integrity check fails
    # Store TradeSeries with a Trade that spans multiple days
    multiday_name = f"{bt}_multiday_fail"
    cleanup_dhstore_storage(multiday_name)
    ts_fail = TradeSeries(start_dt="2099-01-06 00:00:00",
                          end_dt="2099-01-08 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=multiday_name,
                          params_str="",
                          ts_id=multiday_name,
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2099-01-06 10:03:24", direction="long",
        close_dt="2099-01-07 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=multiday_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2099-01-07 11:53:18", direction="long",
        close_dt="2099-01-07 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=multiday_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 2
    # Confirm Trade storage integrity check against sample bt_id fails
    r = review_trades(bt_id=bt, check_integrity=True, orphan_ok=[bt])
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == ["1 invalid multiday trades found"]
    # Confirm Trade multi_ok list passes otherwise invalid Trades
    r = review_trades(bt_id=bt,
                      check_integrity=True,
                      multi_ok=[bt],
                      orphan_ok=[bt])
    assert r["integrity"]["status"] == "OK"
    assert r["integrity"]["issues"] is None
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0

    # Add an unclosed Trade and confirm it is flagged as unclosed_trade
    unclosed_name = f"{bt}_unclosed_fail"
    cleanup_dhstore_storage(unclosed_name)
    ts_fail = TradeSeries(start_dt="2099-01-06 00:00:00",
                          end_dt="2099-01-08 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=unclosed_name,
                          params_str="",
                          ts_id=unclosed_name,
                          bt_id=bt)
    t_unclosed = Trade(
        open_dt="2099-01-06 10:03:24", direction="long",
        close_dt="2099-01-06 10:45:32", timeframe="e1h",
        trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=unclosed_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id)
    t_unclosed.close_dt = None
    t_unclosed.close_date = None
    t_unclosed.close_time = None
    t_unclosed.exit_price = None
    t_unclosed.profitable = None
    t_unclosed.is_open = True
    ts_fail.add_trade(t_unclosed)
    store_tradeseries([ts_fail], include_trades=True)
    r = review_trades(bt_id=bt,
                      check_integrity=True,
                      list_issues=True,
                      orphan_ok=[bt])
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["unclosed_trade_errors"] == 1
    assert r["integrity"]["invalid_multiday_trades"] == 0
    assert r["integrity"]["issues"] == [
        "1 unclosed_trade errors found"
    ]
    assert r["integrity"]["unclosed_trades"][0]["issue_type"] == (
        "unclosed_trade"
    )
    ts_fail.delete_from_storage(include_trades=True)

    # Confirm orphaned_test_objects catches all supported object types
    orphan_backtest_name = "DELETEME_orphan_backtest"
    orphan_ts_name = "DELETEME_orphan_tradeseries"
    orphan_trade_name = "DELETEME_orphan_trade"
    cleanup_dhstore_storage(orphan_backtest_name,
                            orphan_ts_name,
                            orphan_trade_name)
    bt_orphan = Backtest(start_dt="2099-01-01 00:00:00",
                         end_dt="2099-01-02 00:00:00",
                         timeframe="e1h",
                         trading_hours="eth",
                         symbol="ES",
                         name=orphan_backtest_name,
                         parameters={},
                         bt_id=bt,
                         prefer_stored=False,
                         autoload_charts=False)
    ts_orphan = TradeSeries(start_dt="2099-01-01 00:00:00",
                            end_dt="2099-01-01 23:59:59",
                            timeframe="e1h",
                            trading_hours="eth",
                            symbol="ES",
                            name=orphan_ts_name,
                            params_str="",
                            ts_id=f"{bt}_TEST_TS",
                            bt_id=bt)
    ts_orphan.add_trade(Trade(
        open_dt="2099-01-01 10:03:24", direction="long",
        close_dt="2099-01-01 10:45:32", timeframe="e1h",
        trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=orphan_trade_name,
        ts_id=ts_orphan.ts_id, bt_id=ts_orphan.bt_id))
    bt_orphan.tradeseries = [ts_orphan]
    store_backtests([bt_orphan],
                    include_tradeseries=True,
                    include_trades=True)
    r = review_trades(bt_id=bt,
                      check_integrity=True,
                      list_issues=True)
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["orphaned_test_objects_errors"] >= 3
    issue_types = {
        x["object_type"] for x in r["integrity"]["orphaned_test_objects"]
    }
    assert {"backtest", "tradeseries", "trade"}.issubset(issue_types)
    clear_storage_by_name(orphan_backtest_name)
    clear_storage_by_name(orphan_ts_name)
    clear_storage_by_name(orphan_trade_name)

    # Confirm duplicate trades fail during Trade backtests_integrity check
    duplicate_name = f"{bt}_duplicate_fail"
    duplicate_trade_name = f"{bt}_duplicate_trade"
    cleanup_dhstore_storage(duplicate_name, duplicate_trade_name)
    ts_fail = TradeSeries(start_dt="2099-01-06 00:00:00",
                          end_dt="2099-01-08 00:00:00",
                          timeframe="e1h",
                          trading_hours="eth",
                          symbol="ES",
                          name=duplicate_name,
                          params_str="",
                          ts_id=duplicate_name,
                          bt_id=bt)
    ts_fail.add_trade(Trade(
        open_dt="2099-01-06 10:03:24", direction="long",
        close_dt="2099-01-06 11:45:32", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=duplicate_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    ts_fail.add_trade(Trade(
        open_dt="2099-01-06 10:03:24", direction="long",
        close_dt="2099-01-06 12:32:15", timeframe="e1h", trading_hours="eth",
        entry_price=5000, exit_price=5001, high_price=5005, low_price=4995,
        prof_target=5001, stop_target=4000,
        name=duplicate_name,
        ts_id=ts_fail.ts_id, bt_id=ts_fail.bt_id))
    store_tradeseries([ts_fail], include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    # While we stored 2 trades, the second should have overwritten the first
    # by design.  Therefor we only see the second Trade in storage
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 1
    assert stored[0].open_dt == "2099-01-06 10:03:24"
    assert stored[0].close_dt == "2099-01-06 12:32:15"
    # Store the trades
    store_trades([ts_fail.trades[0]])
    stored = get_trades_by_field(field="bt_id", value=bt)
    # Now we should still have only 1 trade, but it's the first not the second
    # this time due to another overwrite
    assert len(stored) == 1
    assert stored[0].open_dt == "2099-01-06 10:03:24"
    assert stored[0].close_dt == "2099-01-06 11:45:32"
    # Change the name attribute to allow it to store itself without replacing
    ts_fail.trades[0].name = duplicate_trade_name
    store_trades([ts_fail.trades[0]])
    # Confirm integrity check now fails due to duplicate trade
    r = review_trades(bt_id=bt, check_integrity=True, orphan_ok=[bt])
    assert r["integrity"]["status"] == "ERRORS"
    assert r["integrity"]["issues"] == ["1 duplicate trades found"]
    # Clean from storage and verify gone
    ts_fail.delete_from_storage(include_trades=True)
    stored = get_tradeseries_by_field(field="bt_id", value=bt)
    assert len(stored) == 0
    stored = get_trades_by_field(field="bt_id", value=bt)
    assert len(stored) == 0


# ===========================================================================
# Custom document tests
# ===========================================================================

# Non-managed collection name used for all write tests.
_TEST_COLL = "custom_docs_DELETEME"

# Marker stored in every test document's "name" field so the
# cleanup fixture can remove them without scanning the whole collection.
_TEST_MARKER = "DELETEME_CUSTOM_DOC_TESTS"


@pytest.fixture
def cleanup_custom_docs():
    """Remove all documents from _TEST_COLL before and after each test.

    Drops the entire collection rather than filtering by marker so
    that nameless orphans left by older code versions are also removed.
    The collection is named with DELETEME so a full drop is safe.
    """
    def _clean():
        _dhm.db[_TEST_COLL].drop()

    _clean()
    yield
    _clean()


# ---------------------------------------------------------------------------
# Guard: managed COLLECTIONS values raise ValueError
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
@pytest.mark.parametrize("coll", list(COLLECTIONS.values()))
def test_custom_document_guard_rejects_managed_collections(coll):
    """store/delete/list/review/get raise ValueError for managed coll."""
    doc = {"name": "should_not_store", "x": 1}
    with pytest.raises(ValueError):
        store_custom_documents(coll, [doc])
    with pytest.raises(ValueError):
        delete_custom_documents_by_field(coll, "name", "x")
    with pytest.raises(ValueError):
        list_custom_documents(coll)
    with pytest.raises(ValueError):
        review_custom_documents(coll)
    with pytest.raises(ValueError):
        get_custom_documents_by_field(coll, "name", "x")
    with pytest.raises(ValueError):
        get_all_custom_documents(coll)


# ---------------------------------------------------------------------------
# Guard: pattern-matched managed collection names raise ValueError
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
@pytest.mark.parametrize("coll", [
    "candles_ES_1m",
    "candles_NQ_5m",
    "events_ES",
    "images.files",
    "images.chunks",
])
def test_custom_document_guard_rejects_pattern_matched_collections(coll):
    """Pattern-matched collections are rejected by the guard."""
    doc = {"name": "should_not_store", "x": 1}
    with pytest.raises(ValueError):
        store_custom_documents(coll, [doc])
    with pytest.raises(ValueError):
        delete_custom_documents_by_field(coll, "name", "x")
    with pytest.raises(ValueError):
        list_custom_documents(coll)
    with pytest.raises(ValueError):
        review_custom_documents(coll)
    with pytest.raises(ValueError):
        get_custom_documents_by_field(coll, "name", "x")
    with pytest.raises(ValueError):
        get_all_custom_documents(coll)


# ---------------------------------------------------------------------------
# gridfs pattern does not block unrelated collections with 'files'/'chunks'
# ---------------------------------------------------------------------------

@pytest.mark.suppress_stdout
@pytest.mark.parametrize("coll", [
    "my_files_collection",
    "chunks_data",
    "report_files",
])
def test_custom_document_guard_allows_non_images_files_chunks(coll):
    """collections with files/chunks not in images bucket are allowed."""
    gridfs_pattern = COLL_PATTERNS["gridfs"]
    assert not gridfs_pattern.search(coll), (
        f"gridfs pattern unexpectedly matched {coll!r}"
    )


# ---------------------------------------------------------------------------
# COLL_PATTERNS is exported correctly
# ---------------------------------------------------------------------------

def test_custom_document_coll_patterns_are_compiled_regexes():
    """COLL_PATTERNS dict is non-empty and values are compiled regexes."""
    assert isinstance(COLL_PATTERNS, dict)
    assert len(COLL_PATTERNS) > 0
    for key, pattern in COLL_PATTERNS.items():
        assert hasattr(pattern, "search"), (
            f"COLL_PATTERNS[{key!r}] is not a compiled regex"
        )
        assert isinstance(pattern, re.Pattern)


# ---------------------------------------------------------------------------
# Validation errors on bad inputs
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_empty_list_raises():
    """store_custom_documents raises ValueError on empty list."""
    with pytest.raises(ValueError, match="non-empty list"):
        store_custom_documents(_TEST_COLL, [])


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_unconvertible_element_raises():
    """store_custom_documents raises when element cannot be dict-coerced."""
    # A plain string cannot be converted to dict by vars() or dict().
    with pytest.raises(ValueError, match="cannot be converted to dict"):
        store_custom_documents(
            _TEST_COLL,
            [{"name": _TEST_MARKER, "ok": 1}, "not_a_dict"],
        )


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_non_json_serializable_raises():
    """store_custom_documents raises ValueError for non-serializable doc."""
    with pytest.raises(ValueError, match="JSON-serializable"):
        store_custom_documents(
            _TEST_COLL,
            [{"name": _TEST_MARKER, "bad": object()}],
        )


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_missing_name_raises():
    """store_custom_documents raises ValueError when name key is absent."""
    with pytest.raises(ValueError, match="non-blank 'name'"):
        store_custom_documents(_TEST_COLL, [{"value": 42}])


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_blank_name_raises():
    """store_custom_documents raises ValueError when name is blank."""
    with pytest.raises(ValueError, match="non-blank 'name'"):
        store_custom_documents(_TEST_COLL, [{"name": "  ", "v": 1}])


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_none_name_raises():
    """store_custom_documents raises ValueError when name is None."""
    with pytest.raises(ValueError, match="non-blank 'name'"):
        store_custom_documents(_TEST_COLL, [{"name": None, "v": 1}])


# ---------------------------------------------------------------------------
# dict coercion
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_object_with_dunder_dict_is_coerced(
        cleanup_custom_docs):
    """An object with __dict__ is coerced to dict and stored successfully.

    Verifies the stored document contains the original attribute values.
    """
    class _Doc:
        def __init__(self):
            self.name = _TEST_MARKER
            self.payload = "coerced_value"
            self.score = 77

    obj = _Doc()
    # Should not raise; the object is coerced via vars()
    store_custom_documents(_TEST_COLL, [obj])

    # Verify the document was actually stored with correct field values
    results = get_custom_documents_by_field(
        _TEST_COLL, "name", _TEST_MARKER
    )
    assert len(results) == 1
    stored = results[0]
    assert stored["name"] == _TEST_MARKER
    assert stored["payload"] == "coerced_value"
    assert stored["score"] == 77


# ---------------------------------------------------------------------------
# doc_id assignment
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_assigns_doc_id_prefixed_with_name(
        cleanup_custom_docs):
    """doc_id is auto-assigned when not supplied.

    Verifies format, prefix, and that retrieval by doc_id returns the
    full original document.
    """
    doc = {"name": _TEST_MARKER, "value": 42}
    assert "doc_id" not in doc
    store_custom_documents(_TEST_COLL, [doc])

    assert "doc_id" in doc
    assert isinstance(doc["doc_id"], str)
    assert doc["doc_id"].startswith(f"{_TEST_MARKER}_")
    suffix = doc["doc_id"][len(_TEST_MARKER) + 1:]
    # new_uuid() canonical form is 36 chars
    assert len(suffix) == 36

    # Retrieve by doc_id and verify full document content
    results = get_custom_documents_by_field(
        _TEST_COLL, "doc_id", doc["doc_id"]
    )
    assert len(results) == 1
    stored = results[0]
    assert stored["doc_id"] == doc["doc_id"]
    assert stored["name"] == _TEST_MARKER
    assert stored["value"] == 42


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_no_name_raises(cleanup_custom_docs):
    """store_custom_documents raises when the document has no name field."""
    with pytest.raises(ValueError, match="non-blank 'name'"):
        store_custom_documents(_TEST_COLL, [{"value": 1}])


# ---------------------------------------------------------------------------
# Store + retrieve roundtrip
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_and_get_by_field_roundtrip(
        cleanup_custom_docs):
    """store_custom_documents / get_custom_documents_by_field roundtrip.

    Verifies that every user-supplied field is returned correctly and
    the doc_id is preserved exactly.
    """
    preset_id = "roundtrip-doc-id-001"
    doc = {
        "name": _TEST_MARKER,
        "doc_id": preset_id,
        "color": "blue",
        "count": 5,
        "tags": ["a", "b"],
    }
    store_custom_documents(_TEST_COLL, [doc])

    results = get_custom_documents_by_field(
        _TEST_COLL, "doc_id", preset_id
    )
    assert len(results) == 1
    stored = results[0]
    assert stored["doc_id"] == preset_id
    assert stored["name"] == _TEST_MARKER
    assert stored["color"] == "blue"
    assert stored["count"] == 5
    assert stored["tags"] == ["a", "b"]


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_preserves_caller_doc_id(
        cleanup_custom_docs):
    """A caller-supplied doc_id is preserved and not overwritten.

    Verifies doc_id and all other fields survive the store unchanged.
    """
    preset_id = "preserve-doc-id-12345"
    doc = {
        "name": _TEST_MARKER,
        "doc_id": preset_id,
        "v": 1,
        "note": "original",
    }
    store_custom_documents(_TEST_COLL, [doc])

    assert doc["doc_id"] == preset_id
    results = list_custom_documents(_TEST_COLL, "doc_id", preset_id)
    assert len(results) == 1
    stored = results[0]
    assert stored["doc_id"] == preset_id
    assert stored["name"] == _TEST_MARKER
    assert stored["v"] == 1
    assert stored["note"] == "original"


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_upserts_on_same_doc_id(
        cleanup_custom_docs):
    """Storing the same doc_id twice upserts; only one record is kept.

    Verifies updated field value and new field are present, original
    doc_id and name are unchanged.
    """
    preset_id = "upsert-test-doc-id"
    doc = {
        "name": _TEST_MARKER,
        "doc_id": preset_id,
        "v": 1,
    }
    store_custom_documents(_TEST_COLL, [doc])

    doc["v"] = 2
    doc["extra"] = "added_on_upsert"
    store_custom_documents(_TEST_COLL, [doc])

    results = list_custom_documents(_TEST_COLL, "doc_id", preset_id)
    assert len(results) == 1
    stored = results[0]
    assert stored["v"] == 2
    assert stored["extra"] == "added_on_upsert"
    assert stored["doc_id"] == preset_id
    assert stored["name"] == _TEST_MARKER


# ---------------------------------------------------------------------------
# list_custom_documents
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_list_with_field_filter(cleanup_custom_docs):
    """list_custom_documents with field+value returns only matching docs.

    Verifies count, filter correctness, and that distinct seq values
    from the original documents are present in results.
    """
    docs = [
        {"name": _TEST_MARKER, "category": "alpha", "seq": 0},
        {"name": _TEST_MARKER, "category": "alpha", "seq": 1},
        {"name": _TEST_MARKER, "category": "beta", "seq": 2},
    ]
    store_custom_documents(_TEST_COLL, docs)

    alpha_docs = list_custom_documents(_TEST_COLL, "category", "alpha")
    assert len(alpha_docs) == 2
    seqs_found = set()
    for d in alpha_docs:
        assert d["category"] == "alpha"
        assert d["name"] == _TEST_MARKER
        seqs_found.add(d["seq"])
    assert seqs_found == {0, 1}


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_list_without_filter_returns_all(
        cleanup_custom_docs):
    """list_custom_documents with no filter returns all stored documents.

    Verifies that every originally stored seq value is present in results.
    """
    docs = [
        {"name": _TEST_MARKER, "seq": i} for i in range(3)
    ]
    store_custom_documents(_TEST_COLL, docs)

    all_docs = list_custom_documents(_TEST_COLL)
    test_docs = [
        d for d in all_docs if d.get("name") == _TEST_MARKER
    ]
    assert len(test_docs) >= 3
    seqs_found = {d["seq"] for d in test_docs}
    assert {0, 1, 2}.issubset(seqs_found)


# ---------------------------------------------------------------------------
# get_custom_documents_by_field and get_all_custom_documents
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_get_by_field_returns_matching_with_values(
        cleanup_custom_docs):
    """get_custom_documents_by_field returns only matching documents.

    Verifies count, filter correctness, and that distinct payload values
    from the original store call are present in results.
    """
    docs = [
        {"name": _TEST_MARKER, "tag": "find_me", "payload": "x1"},
        {"name": _TEST_MARKER, "tag": "find_me", "payload": "x2"},
        {"name": _TEST_MARKER, "tag": "skip_me", "payload": "x3"},
    ]
    store_custom_documents(_TEST_COLL, docs)

    results = get_custom_documents_by_field(
        _TEST_COLL, "tag", "find_me"
    )
    assert len(results) == 2
    payloads_found = set()
    for r in results:
        assert r["tag"] == "find_me"
        assert r["name"] == _TEST_MARKER
        payloads_found.add(r["payload"])
    assert payloads_found == {"x1", "x2"}


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_get_all_returns_all_with_values(
        cleanup_custom_docs):
    """get_all_custom_documents returns all documents with correct values.

    Verifies that every originally stored seq and label pair is present
    in the returned results.
    """
    docs = [
        {"name": _TEST_MARKER, "seq": i, "label": f"item_{i}"}
        for i in range(4)
    ]
    store_custom_documents(_TEST_COLL, docs)

    results = get_all_custom_documents(_TEST_COLL)
    test_results = [
        r for r in results if r.get("name") == _TEST_MARKER
    ]
    assert len(test_results) >= 4
    seqs_found = {r["seq"] for r in test_results}
    labels_found = {r["label"] for r in test_results}
    assert {0, 1, 2, 3}.issubset(seqs_found)
    assert {
        "item_0", "item_1", "item_2", "item_3"
    }.issubset(labels_found)


# ---------------------------------------------------------------------------
# delete_custom_documents_by_field
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_delete_removes_only_matching_records(
        cleanup_custom_docs):
    """delete_custom_documents_by_field removes only matching records.

    Verifies deleted_count, that targeted docs are gone, and that
    kept docs retain their original field values.
    """
    docs = [
        {
            "name": _TEST_MARKER,
            "tag": "to_delete",
            "payload": "gone_1",
        },
        {
            "name": _TEST_MARKER,
            "tag": "to_delete",
            "payload": "gone_2",
        },
        {
            "name": _TEST_MARKER,
            "tag": "to_keep",
            "payload": "kept_value",
        },
    ]
    store_custom_documents(_TEST_COLL, docs)

    result = delete_custom_documents_by_field(
        _TEST_COLL, "tag", "to_delete"
    )
    assert result.deleted_count == 2

    gone = list_custom_documents(_TEST_COLL, "tag", "to_delete")
    assert len(gone) == 0

    kept = list_custom_documents(_TEST_COLL, "tag", "to_keep")
    assert len(kept) == 1
    assert kept[0]["payload"] == "kept_value"
    assert kept[0]["name"] == _TEST_MARKER
    assert kept[0]["tag"] == "to_keep"


# ---------------------------------------------------------------------------
# review_custom_documents
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_review_empty_collection_does_not_raise(
        cleanup_custom_docs):
    """review_custom_documents on an empty collection does not raise."""
    delete_custom_documents_by_field(
        _TEST_COLL, "name", _TEST_MARKER
    )
    # Should print a summary header and no docs without raising.
    review_custom_documents(_TEST_COLL, "name", _TEST_MARKER)
