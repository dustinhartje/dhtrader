"""Tests for TradePlan creation, serialization, and storage."""
import json
import pytest
from dhtrader import (
    Trade, TradePlan, TradeSeries,
    dt_as_dt,
    dt_to_epoch,
    get_tradeplans_by_field,
    store_tradeplans,
    delete_tradeplans,
    delete_tradeplans_by_field,
    store_trades,
    delete_trades_by_field,
    get_trades_by_field,
    COLLECTIONS,
    check_integrity_unique_fields,
)
from dhtrader import dhstore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_trade(open_dt="2099-01-02 12:00:00",
                 ts_id="DELETEME_TS",
                 name="DELETEME_tp_trade",
                 ):
    """Create a minimal closed Trade for use in TradePlan tests."""
    return Trade(
        open_dt=open_dt,
        close_dt="2099-01-02 12:05:00",
        direction="long",
        timeframe="5m",
        trading_hours="rth",
        entry_price=5000,
        exit_price=5010,
        high_price=5010,
        low_price=4995,
        stop_ticks=20,
        prof_ticks=40,
        name=name,
        ts_id=ts_id,
    )


def create_tradeseries(name="DELETEME_tp_ts",
                       ts_id=None,
                       trades=None,
                       ):
    """Create a TradeSeries with optional trades attached."""
    ts = TradeSeries(
        start_dt="2099-01-01 00:00:00",
        end_dt="2099-02-01 00:00:00",
        timeframe="5m",
        trading_hours="rth",
        symbol="ES",
        name=name,
        params_str="s20-p40-o0",
        ts_id=ts_id,
    )
    for t in (trades or []):
        ts.add_trade(t)
    return ts


def create_tradeplan(name="DELETEME_tp_default",
                     id_slug="DELETEME_tp_default_tag",
                     cfg_label="DELETEME_tp_default_lbl",
                     tags=None,
                     notes=None,
                     with_trades=True,
                     ):
    """Create a TradePlan with an attached TradeSeries for tests."""
    ts = create_tradeseries(name=name)
    if with_trades:
        ts.add_trade(create_trade(
            open_dt="2099-01-05 12:00:00",
            ts_id=ts.ts_id,
            name=name,
        ))
        ts.add_trade(create_trade(
            open_dt="2099-01-06 09:35:00",
            ts_id=ts.ts_id,
            name=name,
        ))
    return TradePlan(
        contracts=2,
        con_fee=3.04,
        name=name,
        id_slug=id_slug,
        tags=tags,
        notes=notes,
        cfg_label=cfg_label,
        profit_perc=100,
        start_dt="2099-01-01 00:00:00",
        end_dt="2099-02-01 00:00:00",
        drawdown_open=6000,
        drawdown_limit=6500,
        thresholds={"label": "t1", "mrr": 0.5, "msp": 75},
        tradeseries=ts,
    )


def clear_tradeplan_storage_by_name(name: str):
    """Delete stored TradePlans and Trades matching the given name."""
    delete_tradeplans_by_field(field="name", value=name)
    stored = get_tradeplans_by_field(field="name", value=name)
    assert len(stored) == 0


@pytest.fixture
def cleanup_tradeplan_storage():
    """Register TradePlan names for pre- and post-test cleanup.

    The returned helper records each supplied name, immediately clears
    any matching TradePlans before the test begins, then clears all
    registered entries during fixture teardown.
    """
    names = set()

    def register(*new_names):
        for name in new_names:
            names.add(name)
        for name in sorted(names):
            clear_tradeplan_storage_by_name(name)
            delete_trades_by_field(symbol="ES", field="name",
                                   value=name)

    yield register

    for name in sorted(names):
        clear_tradeplan_storage_by_name(name)
        delete_trades_by_field(symbol="ES", field="name",
                               value=name)


def _make_trade(open_dt, ts_id):
    """Create a Trade with deterministic trade_id for tests."""
    return Trade(
        open_dt=open_dt,
        direction="long",
        timeframe="5m",
        trading_hours="rth",
        entry_price=5000,
        stop_ticks=10,
        prof_ticks=10,
        name="DELETEME_tp_recon_trade",
        ts_id=ts_id,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.suppress_stdout
def test_TradePlan_create_and_verify_common_methods():
    """Test TradePlan __init__, __str__, __repr__, to_clean_dict, to_json,
    and pretty for attribute correctness and serialization.
    """
    tp = create_tradeplan(name="DELETEME_tp_common",
                          id_slug="DELETEME_tp_common_tag",
                          cfg_label="DELETEME_tp_common_lbl")

    # __init__ attribute values
    assert tp.contracts == 2
    assert tp.con_fee == 3.04
    assert tp.name == "DELETEME_tp_common"
    assert tp.id_slug == "DELETEME_tp_common_tag"
    assert tp.cfg_label == "DELETEME_tp_common_lbl"
    assert tp.profit_perc == 100
    assert tp.start_dt == "2099-01-01 00:00:00"
    assert tp.end_dt == "2099-02-01 00:00:00"
    assert tp.drawdown_open == 6000
    assert tp.drawdown_limit == 6500
    assert isinstance(tp.tags, list)
    assert isinstance(tp.notes, list)
    assert isinstance(tp.thresholds, dict)
    assert isinstance(tp.tradeseries, TradeSeries)
    assert tp.how_gl_heatmap_viz is None
    assert isinstance(tp.weekly_price_overlay_visuals, list)
    assert isinstance(tp.tp_id, str)
    assert len(tp.tp_id) > 0
    assert tp.tp_id_short.endswith(tp.uniq_id[-8:])
    assert isinstance(tp.created_epoch, int)
    assert isinstance(tp.created_dt, str)
    assert dt_as_dt(tp.created_dt) is not None
    assert tp.created_epoch == dt_to_epoch(tp.created_dt)
    # Confirm no unexpected attributes were added or removed
    expected_attrs = {
        "contracts", "con_fee", "tp_id", "tp_id_short", "uniq_id",
        "override_tp_id", "name",
        "id_slug", "tags", "cfg_label", "profit_perc",
        "start_dt", "end_dt", "drawdown_open", "drawdown_limit",
        "notes", "thresholds", "tradeseries",
        "how_gl_heatmap_viz", "weekly_price_overlay_visuals",
        "created_dt", "created_epoch",
    }
    actual_attrs = set(vars(tp).keys())
    added = actual_attrs - expected_attrs
    removed = expected_attrs - actual_attrs
    assert actual_attrs == expected_attrs, (
        "TradePlan attributes changed. Update this test's "
        "__init__ section. "
        f"New attrs needing assertions: {sorted(added)}. "
        f"Removed attrs: {sorted(removed)}."
    )

    # __str__ and __repr__
    assert isinstance(str(tp), str)
    assert len(str(tp)) > 0
    assert isinstance(repr(tp), str)
    assert str(tp) == repr(tp)

    # to_clean_dict
    d = tp.to_clean_dict()
    assert isinstance(d, dict)
    assert d["name"] == "DELETEME_tp_common"
    assert d["id_slug"] == "DELETEME_tp_common_tag"
    assert d["contracts"] == 2
    assert "trade_ids" in d
    assert isinstance(d["trade_ids"], list)
    assert len(d["trade_ids"]) == 2
    assert "tradeseries" in d
    assert isinstance(d["tradeseries"], dict)
    # trade_ids must not appear as a runtime attribute
    assert not hasattr(tp, "trade_ids")

    # to_json
    j = tp.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert isinstance(parsed, dict)
    assert parsed["name"] == "DELETEME_tp_common"
    assert parsed["id_slug"] == "DELETEME_tp_common_tag"
    assert parsed["contracts"] == 2

    # pretty
    p = tp.pretty()
    assert isinstance(p, str)
    assert len(p.splitlines()) == 46
    reparsed = json.loads(p)
    assert reparsed["name"] == "DELETEME_tp_common"
    assert reparsed["id_slug"] == "DELETEME_tp_common_tag"


@pytest.mark.suppress_stdout
def test_TradePlan_tags_and_notes_normalization():
    """tags and notes should be normalized to lists of strings."""
    # Default: both are empty lists
    tp = create_tradeplan(tags=None, notes=None)
    assert tp.tags == []
    assert tp.notes == []

    # Provided as string lists: preserved as-is
    tp = create_tradeplan(tags=["alpha", "beta"], notes=["note1"])
    assert tp.tags == ["alpha", "beta"]
    assert tp.notes == ["note1"]

    # Non-string items should be coerced to str
    tp = create_tradeplan(tags=[1, 2], notes=[3])
    assert tp.tags == ["1", "2"]
    assert tp.notes == ["3"]

    # Non-iterable should raise
    with pytest.raises((TypeError, Exception)):
        TradePlan(
            contracts=1,
            name="DELETEME_tp_tags",
            id_slug="DELETEME_tp_tags_tag",
            cfg_label="DELETEME_tp_tags_lbl",
            tags=42,
        )


@pytest.mark.suppress_stdout
def test_TradePlan_tp_id_generation_with_uuid_suffix():
    """tp_id should include id_slug, cfg_label, and a uuid4 suffix.

    When replace_tradeseries is called, the existing uuid suffix should
    be preserved rather than regenerated.
    """
    tp = create_tradeplan(name="DELETEME_tp_id",
                          id_slug="DELETEME_tp_id_tag",
                          cfg_label="DELETEME_tp_id_lbl")
    assert "DELETEME_tp_id_tag" in tp.tp_id
    assert "DELETEME_tp_id_lbl" in tp.tp_id
    # uuid suffix format: ends with _<32 hex chars>
    parts = tp.tp_id.rsplit("_", 1)
    assert len(parts) == 2, f"Expected uuid suffix in tp_id: {tp.tp_id}"
    assert len(parts[1]) == 32
    assert "-" not in parts[1]
    # Verify tp_id_short preserves prefix and shortens uuid portion
    assert tp.tp_id_short.endswith(tp.uniq_id[-8:])
    assert "DELETEME_tp_id_tag" in tp.tp_id_short
    assert "DELETEME_tp_id_lbl" in tp.tp_id_short
    assert len(tp.tp_id_short) < len(tp.tp_id)

    original_tp_id = tp.tp_id
    original_uuid = parts[1]

    # Replacing tradeseries should preserve the uuid suffix
    new_ts = create_tradeseries(name="DELETEME_tp_id_ts2")
    tp.replace_tradeseries(new_ts)
    new_parts = tp.tp_id.rsplit("_", 1)
    assert len(new_parts) == 2
    assert new_parts[1] == original_uuid, (
        "UUID suffix changed after replace_tradeseries; "
        f"before={original_tp_id}, after={tp.tp_id}"
    )

    # Explicit tp_id overrides auto-generation
    tp_explicit = TradePlan(
        contracts=1,
        name="DELETEME_tp_id_explicit",
        id_slug="DELETEME_tp_id_ex_tag",
        cfg_label="DELETEME_tp_id_ex_lbl",
        tp_id="EXPLICIT_ID_abc123",
    )
    assert tp_explicit.tp_id == "EXPLICIT_ID_abc123"
    tp_explicit.replace_tradeseries(create_tradeseries())
    assert tp_explicit.tp_id == "EXPLICIT_ID_abc123"


@pytest.mark.suppress_stdout
def test_TradePlan_replace_tradeseries():
    """replace_tradeseries should swap the attached series and update tp_id
    while preserving the uuid suffix.
    """
    tp = create_tradeplan(name="DELETEME_tp_replace",
                          id_slug="DELETEME_tp_replace_tag",
                          cfg_label="DELETEME_tp_replace_lbl")
    original_uuid = tp.tp_id.rsplit("_", 1)[1]
    assert len(original_uuid) == 32

    ts2 = create_tradeseries(name="DELETEME_tp_replace_b")
    tp.replace_tradeseries(ts2)

    assert tp.tradeseries is not None
    assert tp.tradeseries.name == "DELETEME_tp_replace_b"
    new_uuid = tp.tp_id.rsplit("_", 1)[1]
    assert new_uuid == original_uuid

    # replace_tradeseries(None) should set tradeseries to None
    tp.replace_tradeseries(None)
    assert tp.tradeseries is None
    assert "NoTradeSeries" in tp.tp_id


@pytest.mark.suppress_stdout
def test_TradePlan_source_ts_ids():
    """source_ts_ids should return sorted unique ts_ids from trades."""
    # No tradeseries
    tp = TradePlan(contracts=1, name="DELETEME_tp_src",
                   id_slug="DELETEME_tp_src_tag",
                   cfg_label="DELETEME_tp_src_lbl")
    assert tp.source_ts_ids() == []

    # Tradeseries with no trades
    ts = create_tradeseries(name="DELETEME_tp_src_ts")
    tp = TradePlan(contracts=1, name="DELETEME_tp_src",
                   id_slug="DELETEME_tp_src_tag",
                   cfg_label="DELETEME_tp_src_lbl", tradeseries=ts)
    assert tp.source_ts_ids() == []

    # Trades all from the same ts_id
    ts = create_tradeseries(name="DELETEME_tp_src_ts")
    ts.add_trade(create_trade(
        open_dt="2099-01-05 12:00:00", ts_id=ts.ts_id))
    ts.add_trade(create_trade(
        open_dt="2099-01-06 09:00:00", ts_id=ts.ts_id))
    tp = TradePlan(contracts=1, name="DELETEME_tp_src",
                   id_slug="DELETEME_tp_src_tag",
                   cfg_label="DELETEME_tp_src_lbl", tradeseries=ts)
    assert tp.source_ts_ids() == [ts.ts_id]

    # Trades from two different ts_ids (merged scenario).
    # Directly assign trades with original ts_ids preserved rather than
    # using add_trade(), which would rebind ts_id to the merged series.
    t_a = create_trade(open_dt="2099-01-05 12:00:00", ts_id="ZZ_TS_A")
    t_b = create_trade(open_dt="2099-01-06 09:00:00", ts_id="AA_TS_B")
    ts_merged = create_tradeseries(name="DELETEME_tp_src_merged")
    ts_merged.trades = [t_a, t_b]
    tp = TradePlan(contracts=1, name="DELETEME_tp_src",
                   id_slug="DELETEME_tp_src_tag",
                   cfg_label="DELETEME_tp_src_lbl", tradeseries=ts_merged)
    assert tp.source_ts_ids() == ["AA_TS_B", "ZZ_TS_A"]


@pytest.mark.suppress_stdout
def test_TradePlan_list_trades():
    """list_trades should return one entry per trade in brief format."""
    tp = create_tradeplan(with_trades=True)
    results = tp.list_trades(one_line=True)
    assert isinstance(results, list)
    assert len(results) == 2
    for line in results:
        assert isinstance(line, str)
        assert len(line) > 0


@pytest.mark.suppress_stdout
def test_TradePlan_serialization_excludes_trade_ids_at_runtime():
    """trade_ids must appear in serialized output but not on the object."""
    tp = create_tradeplan(with_trades=True)

    # Runtime object must not have trade_ids
    assert not hasattr(tp, "trade_ids")

    # Serialized output must include trade_ids
    d = tp.to_clean_dict()
    assert "trade_ids" in d
    assert len(d["trade_ids"]) == 2

    # tradeseries in serialized output must suppress embedded trades —
    # suppress_trades=True produces a sentinel string list, not Trade dicts
    ts_dict = d["tradeseries"]
    assert isinstance(ts_dict, dict)
    trades_val = ts_dict.get("trades", [])
    assert not any(isinstance(t, dict) for t in trades_val)


@pytest.mark.suppress_stdout
def test_TradePlan_store_retrieve_delete(cleanup_tradeplan_storage):
    """Verify TradePlan round-trip: store, retrieve, delete."""
    name = "DELETEME_tp_roundtrip"
    cleanup_tradeplan_storage(name)

    tp = create_tradeplan(name=name,
                          id_slug="DELETEME_tp_roundtrip_tag",
                          cfg_label="DELETEME_tp_roundtrip_lbl",
                          with_trades=True)
    original_tp_id = tp.tp_id
    original_trade_count = len(tp.tradeseries.trades)

    # Store trades first so they are retrievable during TradePlan hydration
    store_trades(tp.tradeseries.trades)

    # Store the TradePlan
    results = store_tradeplans([tp])
    assert len(results) == 1
    assert results[0]["tp_id"] == original_tp_id

    # Retrieve by name and confirm reconstruction
    retrieved = get_tradeplans_by_field(field="name", value=name)
    assert len(retrieved) == 1
    r = retrieved[0]
    assert isinstance(r, TradePlan)
    assert r.tp_id == original_tp_id
    assert r.name == name
    assert r.id_slug == "DELETEME_tp_roundtrip_tag"
    assert r.cfg_label == "DELETEME_tp_roundtrip_lbl"
    assert r.contracts == 2
    assert r.drawdown_open == 6000
    assert r.drawdown_limit == 6500

    # Trades should be hydrated from trade_ids
    assert r.tradeseries is not None
    assert len(r.tradeseries.trades) == original_trade_count
    assert not hasattr(r, "trade_ids")

    # Trade IDs should survive the round-trip
    original_ids = sorted(
        t.trade_id for t in tp.tradeseries.trades
    )
    retrieved_ids = sorted(
        t.trade_id for t in r.tradeseries.trades
    )
    assert retrieved_ids == original_ids

    # Delete by tp_id and confirm removal
    delete_tradeplans([tp])
    after = get_tradeplans_by_field(field="name", value=name)
    assert len(after) == 0

    # Cleanup trades
    delete_trades_by_field(symbol="ES", field="name",
                           value=name)


@pytest.mark.suppress_stdout
def test_TradePlan_required_identifiers_for_storage():
    """name, id_slug, and cfg_label must be non-empty before storing.

    TradePlan accepts None for these fields at construction time —
    they have defined behavior in tp_id generation (embedded as the
    literal string "None"). Storage must be rejected to prevent
    orphaned records that cannot be meaningfully looked up or cleaned.
    """
    # None values are accepted at construction — tp_id embeds "None"
    tp = TradePlan(contracts=1, name="DELETEME_tp_req",
                   id_slug=None, cfg_label=None)
    assert "None" in tp.tp_id

    # store_tradeplans must raise when name is None
    tp_none_name = TradePlan(
        contracts=1,
        name=None,
        id_slug="DELETEME_tp_req_tag",
        cfg_label="DELETEME_tp_req_lbl")
    with pytest.raises(ValueError):
        store_tradeplans([tp_none_name])

    # store_tradeplans must raise when name is empty string
    tp_empty_name = TradePlan(
        contracts=1,
        name="",
        id_slug="DELETEME_tp_req_tag",
        cfg_label="DELETEME_tp_req_lbl")
    with pytest.raises(ValueError):
        store_tradeplans([tp_empty_name])

    # store_tradeplans must raise when id_slug is None
    tp_none_tag = TradePlan(
        contracts=1,
        name="DELETEME_tp_req",
        id_slug=None,
        cfg_label="DELETEME_tp_req_lbl")
    with pytest.raises(ValueError):
        store_tradeplans([tp_none_tag])

    # store_tradeplans must raise when cfg_label is None
    tp_none_label = TradePlan(
        contracts=1,
        name="DELETEME_tp_req",
        id_slug="DELETEME_tp_req_tag",
        cfg_label=None)
    with pytest.raises(ValueError):
        store_tradeplans([tp_none_label])

    # store_tradeplans must raise when id_slug is empty string
    tp_empty_tag = TradePlan(
        contracts=1,
        name="DELETEME_tp_req",
        id_slug="",
        cfg_label="DELETEME_tp_req_lbl")
    with pytest.raises(ValueError):
        store_tradeplans([tp_empty_tag])

    # store_tradeplans must raise when cfg_label is empty string
    tp_empty_label = TradePlan(
        contracts=1,
        name="DELETEME_tp_req",
        id_slug="DELETEME_tp_req_tag",
        cfg_label="")
    with pytest.raises(ValueError):
        store_tradeplans([tp_empty_label])


@pytest.mark.suppress_stdout
def test_reconstruct_tradeplan_always_hydrates_by_trade_id(monkeypatch):
    """TradePlan reconstruction should always hydrate trades by trade_id."""
    ts_id = "TS_PLAN"
    t_late = _make_trade("2099-01-02 12:10:00", ts_id)
    t_early = _make_trade("2099-01-02 12:00:00", ts_id)

    def fake_get_trades_by_field_in(field, values, collection, limit=0,
                                    show_progress=False):
        assert field == "trade_id"
        assert values == [t_late.trade_id, t_early.trade_id]
        return [t_late, t_early]

    monkeypatch.setattr(
        dhstore,
        "get_trades_by_field_in",
        fake_get_trades_by_field_in,
    )

    tp_doc = {
        "contracts": 1,
        "con_fee": 0.0,
        "tp_id": "TP-1",
        "name": "DELETEME_tp_recon",
        "id_slug": "DELETEME_tp_recon_tag",
        "tags": ["x"],
        "cfg_label": "DELETEME_tp_recon_lbl",
        "profit_perc": 100,
        "start_dt": "2099-01-01 00:00:00",
        "end_dt": "2099-01-02 00:00:00",
        "drawdown_open": 1000,
        "drawdown_limit": 500,
        "notes": ["note"],
        "thresholds": {},
        "tradeseries": {
            "start_dt": "2099-01-01 00:00:00",
            "end_dt": "2099-01-02 00:00:00",
            "timeframe": "5m",
            "trading_hours": "rth",
            "symbol": "ES",
            "name": "DELETEME_tp_recon_ts",
            "params_str": "",
            "ts_id": ts_id,
            "bt_id": "BT-1",
            "trades": ["suppressed"],
            "tags": [],
        },
        "trade_ids": [t_late.trade_id, t_early.trade_id],
    }

    tp = dhstore.reconstruct_tradeplan(tp_doc)

    assert not hasattr(tp, "trade_ids")
    assert tp.name == "DELETEME_tp_recon"
    assert len(tp.tradeseries.trades) == 2
    assert (tp.tradeseries.trades[0].open_epoch
            < tp.tradeseries.trades[1].open_epoch)
    assert tp.tradeseries.trades[0].trade_id == t_early.trade_id
    assert tp.tradeseries.trades[1].trade_id == t_late.trade_id


# ---------------------------------------------------------------------------
# Integrity: unique fields
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tradeplan_unique_fields_result():
    """Run tp_id/uniq_id uniqueness check once and cache result."""
    return check_integrity_unique_fields(
        collection=COLLECTIONS["tradeplans"],
        fields=["tp_id", "uniq_id"],
    )


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_TradePlan_missing_unique_fields(tradeplan_unique_fields_result):
    """Confirm all stored TradePlan docs have non-empty tp_id/uniq_id."""
    result = tradeplan_unique_fields_result
    assert result is not None, "check_integrity_unique_fields returned None"
    assert "status" in result
    assert "total_docs" in result
    assert "fields" in result
    for field_name in ["tp_id", "uniq_id"]:
        field = result["fields"].get(field_name, {})
        missing_count = field.get("missing_count", 0)
        missing_samples = field.get("missing_samples", [])
        if missing_count > 0:
            samples = "\n".join(
                f"  - _id={s['_id']}" for s in missing_samples
            )
            pytest.fail(
                f"TradePlan docs with missing {field_name}: "
                f"{missing_count} of {result['total_docs']}\n"
                f"{samples}"
            )


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_TradePlan_duplicate_unique_fields(tradeplan_unique_fields_result):
    """Confirm all stored TradePlan docs have unique tp_id/uniq_id."""
    result = tradeplan_unique_fields_result
    assert result is not None, "check_integrity_unique_fields returned None"
    assert "status" in result
    assert "total_docs" in result
    assert "fields" in result
    for field_name in ["tp_id", "uniq_id"]:
        field = result["fields"].get(field_name, {})
        duplicate_count = field.get("duplicate_count", 0)
        duplicate_samples = field.get("duplicate_samples", [])
        if duplicate_count > 0:
            samples = "\n".join(
                f"  - {field_name}={s['value']} count={s['count']}"
                for s in duplicate_samples
            )
            pytest.fail(
                f"Duplicate {field_name} in TradePlan: "
                f"{duplicate_count} duplicate(s) among "
                f"{result['total_docs']} doc(s)\n"
                f"{samples}"
            )
