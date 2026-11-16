"""Tests for the testdata package, Extractor, Rebuilder and build script."""
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import dhtrader.testdata as testdata_package
from dhtrader import Backtest, Candle, Trade, TradeSeries, dt_to_epoch
from dhtrader.testdata import testdata as testdata_module
from dhtrader.testdata.testdata import Extractor, Rebuilder, prepare_for_csv

TESTDATA_DIR = Path(__file__).resolve().parents[1] / "testdata"
SET1_DIR = TESTDATA_DIR / "set1"
SET1_BOUNCE_BT_ID = "BacktestEMABounce-eth_e1h_9"
SET1_BOUNCE_TS_ID = "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
SET1_REJECT_BT_ID = "BacktestEMAReject-eth_e1h_9"


class _StoredObjectDouble:
    """Expose to_clean_dict() like storage-returned dhtrader objects."""

    def __init__(self, **fields):
        self.__dict__.update(fields)

    def to_clean_dict(self):
        """Return a copy of the stored fields."""
        return dict(self.__dict__)


def _read_csv_rows(csv_path):
    """Return CSV header and rows as dicts."""
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames, list(reader)


def _read_json(json_path):
    """Return parsed JSON file contents."""
    with open(json_path) as f:
        return json.load(f)


def _trade_dict(open_dt, direction, ts_id):
    """Build a minimal extracted-trade dictionary."""
    return {"open_dt": open_dt,
            "open_epoch": dt_to_epoch(open_dt),
            "timeframe": "e1h",
            "direction": direction,
            "ts_id": ts_id,
            "bt_id": "DELETEME_bt",
            }


# Package exports ##########################################################


def test_testdata_package_lazily_exports_extractor_and_rebuilder():
    """Package attributes resolve to the testdata module classes."""
    assert testdata_package.Extractor is Extractor
    assert testdata_package.Rebuilder is Rebuilder


def test_testdata_package_rejects_unknown_attribute():
    """Unknown package attributes raise AttributeError."""
    with pytest.raises(AttributeError, match="DELETEME_missing"):
        getattr(testdata_package, "DELETEME_missing")


# Standalone build script ##################################################


@pytest.mark.historical
def test_build_testdata_script_runs_standalone():
    """build_testdata.py imports dhtrader via its own path setup."""
    # Drop PYTHONPATH so the script must rely on its standalone path setup
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    result = subprocess.run([sys.executable, "build_testdata.py"],
                            cwd=TESTDATA_DIR, env=env,
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr[-2000:]
    assert "All objects match expected counts and types." in result.stdout


# Rebuilder ################################################################


@pytest.mark.historical
@pytest.mark.suppress_stdout
def test_rebuild_candles_without_date_limits_returns_all_sorted():
    """All set1 e1h candles rebuild as sorted Candle objects."""
    cans = Rebuilder().rebuild_candles(
        in_file=SET1_DIR / "set1_e1h_candles.json")
    assert len(cans) == 345
    assert all(isinstance(c, Candle) for c in cans)
    assert cans[0].c_datetime == "2025-11-30 18:00:00"
    assert cans[-1].c_datetime == "2025-12-19 16:00:00"
    assert [c.c_epoch for c in cans] == sorted(c.c_epoch for c in cans)


@pytest.mark.historical
@pytest.mark.suppress_stdout
@pytest.mark.parametrize("start_dt,end_dt,expected_dts", [
    ("2025-11-30 19:00:00", "2025-11-30 21:00:00",
     ["2025-11-30 19:00:00", "2025-11-30 20:00:00", "2025-11-30 21:00:00"]),
    ("2025-12-19 15:00:00", None,
     ["2025-12-19 15:00:00", "2025-12-19 16:00:00"]),
    (None, "2025-11-30 19:00:00",
     ["2025-11-30 18:00:00", "2025-11-30 19:00:00"]),
])
def test_rebuild_candles_applies_inclusive_date_limits(
        start_dt, end_dt, expected_dts):
    """Date limits include boundary candles and default open ends."""
    cans = Rebuilder().rebuild_candles(
        in_file=SET1_DIR / "set1_e1h_candles.json",
        start_dt=start_dt, end_dt=end_dt)
    assert [c.c_datetime for c in cans] == expected_dts


@pytest.mark.suppress_stdout
def test_rebuild_candles_sorts_unordered_file_rows(tmp_path):
    """Candles are returned in epoch order regardless of file order."""
    rows = [{"c_datetime": dt, "c_timeframe": "1m", "c_open": 1.0,
             "c_high": 2.0, "c_low": 0.5, "c_close": 1.5, "c_volume": 10,
             "c_symbol": "ES"}
            for dt in ("2099-01-05 10:02:00", "2099-01-05 10:00:00",
                       "2099-01-05 10:01:00")]
    in_file = tmp_path / "DELETEME_candles.json"
    in_file.write_text(json.dumps(rows))
    cans = Rebuilder().rebuild_candles(in_file=in_file)
    assert [c.c_datetime for c in cans] == [
        "2099-01-05 10:00:00", "2099-01-05 10:01:00", "2099-01-05 10:02:00"]


@pytest.mark.suppress_stdout
@pytest.mark.parametrize("method", [
    "rebuild_candles", "rebuild_trades", "rebuild_tradeseries",
    "rebuild_backtests"])
def test_rebuild_rejects_non_list_json(tmp_path, method):
    """Every rebuild method requires a JSON list."""
    in_file = tmp_path / "DELETEME_not_a_list.json"
    in_file.write_text(json.dumps({"DELETEME": True}))
    with pytest.raises(ValueError, match="must be"):
        getattr(Rebuilder(), method)(in_file=in_file)


@pytest.mark.historical
@pytest.mark.suppress_stdout
def test_rebuild_backtests_without_children_uses_file_fields():
    """Backtests rebuild from file fields without TradeSeries."""
    raw_bts = _read_json(SET1_DIR / "set1_backtests.json")
    bts = Rebuilder().rebuild_backtests(
        in_file=SET1_DIR / "set1_backtests.json")
    assert [bt.bt_id for bt in bts] == [SET1_BOUNCE_BT_ID, SET1_REJECT_BT_ID]
    for bt, raw_bt in zip(bts, raw_bts):
        assert isinstance(bt, Backtest)
        assert bt.tradeseries == []
        assert bt.start_dt == "2025-11-30 18:00:00"
        assert bt.end_dt == "2025-12-19 17:00:00"
        assert bt.class_name == raw_bt["class_name"]
        assert bt.parameters == raw_bt["parameters"]


@pytest.mark.historical
@pytest.mark.suppress_stdout
def test_rebuild_backtests_filters_and_limits_nested_objects():
    """bt_ids and date limits apply to Backtest, TradeSeries and Trades."""
    start_dt = "2025-12-10 00:00:00"
    end_dt = "2025-12-20 23:59:59"
    bts = Rebuilder().rebuild_backtests(
        in_file=SET1_DIR / "set1_backtests.json",
        bt_ids=[SET1_BOUNCE_BT_ID],
        start_dt=start_dt,
        end_dt=end_dt,
        tradeseries_file=SET1_DIR / "set1_tradeseries.json",
        trades_file=SET1_DIR / "set1_trades.json")
    assert [bt.bt_id for bt in bts] == [SET1_BOUNCE_BT_ID]
    assert (bts[0].start_dt, bts[0].end_dt) == (start_dt, end_dt)
    assert [ts.ts_id for ts in bts[0].tradeseries] == [SET1_BOUNCE_TS_ID]
    ts = bts[0].tradeseries[0]
    assert isinstance(ts, TradeSeries)
    assert (ts.start_dt, ts.end_dt) == (start_dt, end_dt)
    assert len(ts.trades) == 28
    for t in ts.trades:
        assert isinstance(t, Trade)
        assert t.ts_id == SET1_BOUNCE_TS_ID
        assert t.bt_id == SET1_BOUNCE_BT_ID
        assert dt_to_epoch(start_dt) <= t.open_epoch <= dt_to_epoch(end_dt)


# Extractor ################################################################


def test_prepare_for_csv_serializes_nested_values_only():
    """Lists and dicts become JSON strings; scalars and inputs are kept."""
    rows = [{"name": "DELETEME", "count": 2, "tags": ["a"],
             "parameters": {"length": 9}, "missing": None}]
    csv_rows = prepare_for_csv(rows)
    assert csv_rows == [{"name": "DELETEME", "count": 2, "tags": '["a"]',
                         "parameters": '{"length": 9}', "missing": None}]
    assert rows[0]["tags"] == ["a"]


def test_extract_candles_queries_epochs_and_writes_files(
        monkeypatch, tmp_path):
    """Candles are queried by epoch and written as JSON plus ordered CSV."""
    calls = []
    cans = [Candle(c_datetime="2099-01-05 10:00:00", c_timeframe="1m",
                   c_open=1.0, c_high=2.0, c_low=0.5, c_close=1.5,
                   c_volume=10, c_symbol="ES")]

    def fake_get_candles(**kwargs):
        calls.append(kwargs)
        return cans

    monkeypatch.setattr(testdata_module, "get_candles", fake_get_candles)
    out_file = str(tmp_path / "DELETEME_candles.json")
    cans_json = Extractor().extract_candles(
        start_dt="2099-01-05 09:00:00", end_dt="2099-01-05 11:00:00",
        timeframe="1m", out_file=out_file)
    assert calls == [{"start_epoch": dt_to_epoch("2099-01-05 09:00:00"),
                      "end_epoch": dt_to_epoch("2099-01-05 11:00:00"),
                      "timeframe": "1m", "symbol": "ES"}]
    assert cans_json == [cans[0].to_clean_dict()]
    assert _read_json(out_file) == cans_json
    header, csv_rows = _read_csv_rows(out_file.replace(".json", ".csv"))
    assert header[0] == "c_datetime"
    assert "c_tags" not in header
    assert [r["c_datetime"] for r in csv_rows] == ["2099-01-05 10:00:00"]


def test_extract_trades_filters_annotates_splits_and_writes_files(
        monkeypatch, tmp_path):
    """Trades are date-limited, annotated with prior indicator values,
    split by direction and written to every output file."""
    long_ts_id = "DELETEME_ts_long"
    short_ts_id = "DELETEME_ts_short"
    stored_trades = {
        long_ts_id: [_trade_dict(dt, "long", long_ts_id)
                     for dt in ("2099-01-05 09:20:00", "2099-01-05 10:15:00",
                                "2099-01-05 12:10:00",
                                "2099-01-05 13:00:00")],
        short_ts_id: [_trade_dict(dt, "short", short_ts_id)
                      for dt in ("2099-01-05 08:59:00",
                                 "2099-01-05 11:20:00")],
    }
    calls = []

    def fake_get_trades_by_field(field, values):
        calls.append((field, values))
        return [_StoredObjectDouble(**t) for t in stored_trades[values]]

    monkeypatch.setattr(testdata_module, "get_trades_by_field",
                        fake_get_trades_by_field)
    ind_dps = [{"dt": "2099-01-05 09:00:00", "value": 1.0},
               {"dt": "2099-01-05 10:00:00", "value": 2.0},
               {"dt": "2099-01-05 11:00:00", "value": 3.0}]
    out_file = str(tmp_path / "DELETEME_trades.json")
    result = Extractor().extract_trades(
        ts_ids=[long_ts_id, short_ts_id],
        start_dt="2099-01-05 09:00:00",
        end_dt="2099-01-05 12:10:00",
        ind_dps=ind_dps,
        out_file=out_file)

    assert calls == [("ts_id", long_ts_id), ("ts_id", short_ts_id)]
    # First candle has no prior datapoint; 12:00 has no datapoint at all
    assert [(t["open_dt"], t["prev_ind_dp"])
            for t in result["long_trades"]] == [
        ("2099-01-05 09:20:00", None),
        ("2099-01-05 10:15:00", 1.0),
        ("2099-01-05 12:10:00", None)]
    assert [(t["open_dt"], t["prev_ind_dp"])
            for t in result["short_trades"]] == [
        ("2099-01-05 11:20:00", 2.0)]
    assert [ts["ts_id"] for ts in result["all_trades"]] == [
        long_ts_id, short_ts_id]
    assert result["all_trades"][0]["trades"] == result["long_trades"]
    assert result["all_trades"][1]["trades"] == result["short_trades"]

    base_file = out_file.replace(".json", "")
    assert _read_json(out_file) == result["all_trades"]
    assert _read_json(f"{base_file}_long.json") == result["long_trades"]
    assert _read_json(f"{base_file}_short.json") == result["short_trades"]
    _, long_rows = _read_csv_rows(f"{base_file}_long.csv")
    assert [(r["open_dt"], r["prev_ind_dp"]) for r in long_rows] == [
        ("2099-01-05 09:20:00", ""),
        ("2099-01-05 10:15:00", "1.0"),
        ("2099-01-05 12:10:00", "")]
    _, short_rows = _read_csv_rows(f"{base_file}_short.csv")
    assert [r["ts_id"] for r in short_rows] == [short_ts_id]


def test_extract_trades_without_options_returns_unmodified_trades(
        monkeypatch, tmp_path):
    """A single ts_id string is accepted and no files or annotations are
    produced when no dates, indicator datapoints or out_file are given."""
    ts_id = "DELETEME_ts"
    stored = [_trade_dict("2099-01-05 10:15:00", "long", ts_id),
              _trade_dict("2099-01-06 10:15:00", "short", ts_id)]
    monkeypatch.setattr(
        testdata_module, "get_trades_by_field",
        lambda field, values: [_StoredObjectDouble(**t) for t in stored])
    monkeypatch.chdir(tmp_path)
    result = Extractor().extract_trades(ts_ids=ts_id, start_dt=None,
                                        end_dt=None)
    assert result["all_trades"] == [{"ts_id": ts_id, "trades": stored}]
    assert result["long_trades"] == [stored[0]]
    assert result["short_trades"] == [stored[1]]
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("method,id_arg", [
    ("extract_trades", "ts_ids"),
    ("extract_tradeseries", "ts_ids"),
    ("extract_backtests", "bt_ids"),
])
def test_extract_rejects_non_string_non_list_ids(method, id_arg):
    """ID arguments must be a string or list of strings."""
    kwargs = {id_arg: ("DELETEME",)}
    if method == "extract_trades":
        kwargs.update(start_dt=None, end_dt=None)
    with pytest.raises(ValueError, match="string or list"):
        getattr(Extractor(), method)(**kwargs)


def test_extract_tradeseries_overrides_dates_and_writes_files(
        monkeypatch, tmp_path):
    """TradeSeries load without trades, take date overrides and write
    JSON-encoded nested CSV columns."""
    calls = []

    def fake_get_tradeseries_by_field(field, value, include_trades):
        calls.append((field, value, include_trades))
        return [_StoredObjectDouble(
            ts_id=value, bt_id="DELETEME_bt", name="DELETEME",
            params_str="s80-p160-o0", start_dt="2099-01-01 00:00:00",
            end_dt="2099-12-31 00:00:00", symbol="ES", timeframe="e1h",
            trading_hours="eth", trades=[], tags=["DELETEME"])]

    monkeypatch.setattr(testdata_module, "get_tradeseries_by_field",
                        fake_get_tradeseries_by_field)
    out_file = str(tmp_path / "DELETEME_tradeseries.json")
    tss_json = Extractor().extract_tradeseries(
        ts_ids="DELETEME_ts", start_dt="2099-01-05 00:00:00",
        end_dt="2099-01-06 00:00:00", out_file=out_file)
    assert calls == [("ts_id", "DELETEME_ts", False)]
    assert [(ts["start_dt"], ts["end_dt"]) for ts in tss_json] == [
        ("2099-01-05 00:00:00", "2099-01-06 00:00:00")]
    assert _read_json(out_file) == tss_json
    _, rows = _read_csv_rows(out_file.replace(".json", ".csv"))
    assert [(r["ts_id"], r["trades"], r["tags"]) for r in rows] == [
        ("DELETEME_ts", "[]", '["DELETEME"]')]


def test_extract_backtests_overrides_dates_and_writes_files(
        monkeypatch, tmp_path):
    """Backtest documents take date overrides and write JSON-encoded
    nested CSV columns."""
    calls = []

    def fake_get_backtests_by_field(field, value):
        calls.append((field, value))
        return [{"_id": "DELETEME_doc", "bt_id": value,
                 "start_dt": "2099-01-01 00:00:00",
                 "end_dt": "2099-12-31 00:00:00",
                 "parameters": {"length": 9}, "tradeseries": []}]

    monkeypatch.setattr(testdata_module, "get_backtests_by_field",
                        fake_get_backtests_by_field)
    out_file = str(tmp_path / "DELETEME_backtests.json")
    bts_json = Extractor().extract_backtests(
        bt_ids=["DELETEME_bt"], start_dt="2099-01-05 00:00:00",
        end_dt="2099-01-06 00:00:00", out_file=out_file)
    assert calls == [("bt_id", "DELETEME_bt")]
    assert [(bt["start_dt"], bt["end_dt"]) for bt in bts_json] == [
        ("2099-01-05 00:00:00", "2099-01-06 00:00:00")]
    assert _read_json(out_file) == bts_json
    _, rows = _read_csv_rows(out_file.replace(".json", ".csv"))
    assert [(r["bt_id"], r["parameters"]) for r in rows] == [
        ("DELETEME_bt", '{"length": 9}')]


def test_extract_indicator_datapoints_queries_range_and_writes_files(
        monkeypatch, tmp_path):
    """Indicator datapoints are queried by range and written with all
    datapoint fields as CSV columns."""
    calls = []
    dps = [_StoredObjectDouble(dt="2099-01-05 10:00:00", value=1.5,
                               ind_id="DELETEME_ind", epoch=1, name="EMA")]

    def fake_get_indicator_datapoints(ind_id, earliest_dt, latest_dt):
        calls.append((ind_id, earliest_dt, latest_dt))
        return dps

    monkeypatch.setattr(testdata_module, "get_indicator_datapoints",
                        fake_get_indicator_datapoints)
    out_file = str(tmp_path / "DELETEME_ind_dps.json")
    dps_json = Extractor().extract_indicator_datapoints(
        ind_id="DELETEME_ind", start_dt="2099-01-05 00:00:00",
        end_dt="2099-01-06 00:00:00", out_file=out_file)
    assert calls == [("DELETEME_ind", "2099-01-05 00:00:00",
                      "2099-01-06 00:00:00")]
    assert dps_json == [dps[0].to_clean_dict()]
    assert _read_json(out_file) == dps_json
    header, rows = _read_csv_rows(out_file.replace(".json", ".csv"))
    assert header == list(dps_json[0])
    assert rows[0]["value"] == "1.5"
