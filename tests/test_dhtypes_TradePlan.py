"""Tests for TradePlan reconstruction behavior in dhstore."""

from dhtrader import Trade
from dhtrader import dhstore


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
        name="DELETEME",
        ts_id=ts_id,
    )


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
        "nametag": "tag",
        "tags": ["x"],
        "label": "label",
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
            "name": "DELETEME",
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
    assert len(tp.tradeseries.trades) == 2
    assert (tp.tradeseries.trades[0].open_epoch
            < tp.tradeseries.trades[1].open_epoch)
    assert tp.tradeseries.trades[0].trade_id == t_early.trade_id
    assert tp.tradeseries.trades[1].trade_id == t_late.trade_id
