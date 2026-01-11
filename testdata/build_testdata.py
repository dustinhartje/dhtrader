from testdata import Rebuilder
from dhutil import dt_to_epoch
from dhcharts import Candle
from dhtrades import Trade, TradeSeries, Backtest

"""Rebuild test data from json files into objects for testing and development
purposes.  This script primarily serves as a reference template for Rebuilder
usage in unit tests.  It can also be used to verify test data is being rebuilt
accurately.  Run as a script to test ad-hoc or import and call
extract_testdata() from other test modules to extract data for testing
without writing files.

NOTE - due to the nesting structure I have not been able to determine how to
setup pathing to get all packages to import correctly when running from a unit
test file.  Maybe circle back to that later but it works fine run as a script
which is enough for now."""


def say(msg: str, console_output=True):
    if console_output:
        print(f"{msg}")

# This script primarily serves as a reference template for Rebuilder usage in
# unit tests.  It can also be used to verify test data is rebuilt accurately.


def rebuild_testdata(console_output=False):
    """Rebuild test data from json files into objects for validation testing"""
    r = Rebuilder()
    # Candles #############################################
    # Rebuild all 1m candles
    say("\n# Rebuliding all 1m Candles\n", console_output)
    cans_1m = r.rebuild_candles(in_file="set1/set1_1m_candles.json")
    assert len(cans_1m) == 20700
    say(f"Rebuilt {len(cans_1m)} 1m candles with no date restrictions.",
        console_output)
    for c in cans_1m:
        assert isinstance(c, Candle)
    # Rebuild 1m candles with restricted dates
    cans_1m_dt = r.rebuild_candles(in_file="set1/set1_1m_candles.json",
                                   start_dt="2025-12-03 09:30:00",
                                   end_dt="2025-12-24 16:00:00")
    say(f"Rebuilt {len(cans_1m_dt)} candles with date restrictions: "
        "2025-12-03 09:30:00 to 2025-12-24 16:00:00 (xmas eve early close "
        "at 13:15:00).", console_output)
    say(f"First candle datetime: {cans_1m_dt[0].c_datetime}", console_output)
    say(f"Last candle datetime: {cans_1m_dt[-1].c_datetime}", console_output)
    assert len(cans_1m_dt) == 17010
    assert cans_1m_dt[0].c_datetime == "2025-12-03 09:30:00"
    print(cans_1m_dt[-1].c_datetime)
    assert cans_1m_dt[-1].c_datetime == "2025-12-19 16:59:00"
    for c in cans_1m_dt:
        assert isinstance(c, Candle)
    say("\n# Rebuliding all tf Candles\n", console_output)
    # Rebuild all tf candles
    cans_tf = r.rebuild_candles(in_file="set1/set1_e1h_candles.json")
    print(len(cans_tf))
    assert len(cans_tf) == 345
    for c in cans_tf:
        assert isinstance(c, Candle)

    # Trades ##############################################
    # Rebuild all Trades
    say("\n# Rebuilding all Trades\n", console_output)
    trades = r.rebuild_trades(in_file="set1/set1_trades.json")
    say(f"{len(trades)}", console_output)
    assert len(trades) == 130
    for t in trades:
        assert isinstance(t, Trade)
    say(f"Rebuilt {len(trades)} trades with no date restrictions.",
        console_output)
    say(f"First trade open_dt: {trades[0].open_dt}", console_output)
    say(f"Last trade open_dt: {trades[-1].open_dt}", console_output)

    # Rebuild Trades for 1 ts_id
    say("\n# Rebuilding Trades for 1 ts_id\n", console_output)
    trades_dt = r.rebuild_trades(
        in_file="set1/set1_trades.json",
        ts_id="BacktestEMABounce-eth_e1h_9_s80-p160-o0")
    assert len(trades_dt) == 76
    for t in trades_dt:
        assert isinstance(t, Trade)
    say(f"Rebuilt {len(trades_dt)} trades with date restrictions: "
        "2025-12-10 00:00:00 to 2025-12-20 23:59:59.", console_output)

    # Rebuild Trades for 1 ts_id with date restrictions
    say("\n# Rebuilding Trades for 1 ts_id with date restrictions\n",
        console_output)
    trades_dt = r.rebuild_trades(
        in_file="set1/set1_trades.json",
        ts_id="BacktestEMABounce-eth_e1h_9_s80-p160-o0",
        start_dt="2025-12-10 00:00:00",
        end_dt="2025-12-20 23:59:59")

    assert len(trades_dt) == 28
    for t in trades_dt:
        assert isinstance(t, Trade)
    say(f"Rebuilt {len(trades_dt)} trades with date restrictions: "
        "2025-12-10 00:00:00 to 2025-12-20 23:59:59.", console_output)

    # TradeSeries #########################################

    # Rebuild all TradeSeries without date restrictions or Trades
    say("\n# Rebuilding all TradeSeries without date restrictions or "
        "Trades\n", console_output)
    tss = r.rebuild_tradeseries(in_file="set1/set1_tradeseries.json")
    assert len(tss) == 2
    for ts in tss:
        assert isinstance(ts, TradeSeries)
        assert len(ts.trades) == 0
        assert ts.start_dt == "2025-11-30 18:00:00"
        assert ts.end_dt == "2025-12-19 17:00:00"
    assert tss[0].ts_id == "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    assert tss[1].ts_id == "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    say(f"Rebuilt {len(tss)} TradeSeries with no date restrictions.",
        console_output)

    # Rebuild all TradeSeries without date restrictions but with Trades
    say("\n# Rebuilding all TradeSeries without date restrictions but with "
        "Trades\n", console_output)
    tss = r.rebuild_tradeseries(in_file="set1/set1_tradeseries.json",
                                trades_file="set1/set1_trades.json")
    assert len(tss) == 2
    for ts in tss:
        assert isinstance(ts, TradeSeries)
        assert ts.start_dt == "2025-11-30 18:00:00"
        assert ts.end_dt == "2025-12-19 17:00:00"
    assert len(tss[0].trades) == 76
    assert len(tss[1].trades) == 54
    assert tss[0].ts_id == "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    assert tss[1].ts_id == "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    for t in tss[0].trades:
        assert isinstance(t, Trade)
        assert t.ts_id == "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    for t in tss[1].trades:
        assert isinstance(t, Trade)
        assert t.ts_id == "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    say(f"Rebuilt {len(tss)} TradeSeries with no date restrictions.",
        console_output)

    # Rebuild one TradeSeries without Trades but with date restrictions
    say("\n# Rebuilding one TradeSeries with date restrictions but without "
        "Trades \n", console_output)
    tss = r.rebuild_tradeseries(
        in_file="set1/set1_tradeseries.json",
        ts_ids=["BacktestEMABounce-eth_e1h_9_s80-p160-o0"],
        start_dt="2025-12-10 00:00:00",
        end_dt="2025-12-20 23:59:59")
    assert len(tss) == 1
    assert isinstance(tss[0], TradeSeries)
    assert len(tss[0].trades) == 0
    assert tss[0].ts_id == "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    say(f"Rebuilt 1 TradeSeries {tss[0].ts_id} with no date restrictions or "
        "Trades.", console_output)

    # Rebuild the other TradeSeries with Trades and date restrictions
    say("\n# Rebuilding one TradeSeries with date restrictions and Trades\n",
        console_output)
    tss = r.rebuild_tradeseries(
        in_file="set1/set1_tradeseries.json",
        ts_ids=["BacktestEMAReject-eth_e1h_9_s80-p160-o40"],
        start_dt="2025-12-10 00:00:00",
        end_dt="2025-12-20 23:59:59",
        trades_file="set1/set1_trades.json")
    assert len(tss) == 1
    assert isinstance(tss[0], TradeSeries)
    assert tss[0].ts_id == "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    assert len(tss[0].trades) == 31
    for t in tss[0].trades:
        assert isinstance(t, Trade)
        assert t.ts_id == "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    say(f"Rebuilt 1 TradeSeries {tss[0].ts_id} with date restrictions and "
        f"{len(tss[0].trades)} Trades.", console_output)

    # Backtests ###########################################
    say("\n# Rebuilding all Backtests without date restrictions, "
        "TradeSeries, or Trades\n", console_output)
    bts = r.rebuild_backtests(in_file="set1/set1_backtests.json")
    assert len(bts) == 2
    for bt in bts:
        assert isinstance(bt, Backtest)
        assert len(bt.tradeseries) == 0
        assert bt.start_dt == "2025-11-30 18:00:00"
        assert bt.end_dt == "2025-12-19 17:00:00"
    assert bts[0].bt_id == "BacktestEMABounce-eth_e1h_9"
    assert bts[1].bt_id == "BacktestEMAReject-eth_e1h_9"
    say(f"Rebuilt {len(bts)} Backtests with no date restrictions, "
        "TradeSeries, or Trades.", console_output)

    # Rebuild all Backtests with TradeSeries and Trades but no date
    # restrictions
    say("\n# Rebuilding all Backtests with TradeSeries and Trades but no "
        "date restrictions\n", console_output)
    bts = r.rebuild_backtests(
        in_file="set1/set1_backtests.json",
        tradeseries_file="set1/set1_tradeseries.json",
        trades_file="set1/set1_trades.json")
    assert len(bts) == 2
    for bt in bts:
        assert isinstance(bt, Backtest)
        assert bt.start_dt == "2025-11-30 18:00:00"
        assert bt.end_dt == "2025-12-19 17:00:00"
    assert len(bts[0].tradeseries) == 1
    assert len(bts[1].tradeseries) == 1
    assert bts[0].bt_id == "BacktestEMABounce-eth_e1h_9"
    assert bts[1].bt_id == "BacktestEMAReject-eth_e1h_9"
    ts_id = "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    assert bts[0].tradeseries[0].ts_id == ts_id
    assert len(bts[0].tradeseries[0].trades) == 76
    for t in bts[0].tradeseries[0].trades:
        assert isinstance(t, Trade)
        assert t.ts_id == ts_id
    ts_id = "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    assert bts[1].tradeseries[0].ts_id == ts_id
    assert len(bts[1].tradeseries[0].trades) == 54
    for t in bts[1].tradeseries[0].trades:
        assert isinstance(t, Trade)
        assert t.ts_id == ts_id
    say(f"Rebuilt {len(bts)} Backtests with no date restrictions but with "
        "TradeSeries and Trades.", console_output)

    # Rebuild one Backtest with TradeSeries and Trades and date restrictions
    say("\n# Rebuilding one Backtest with TradeSeries and Trades and date "
        "restrictions\n", console_output)
    bts = r.rebuild_backtests(
        in_file="set1/set1_backtests.json",
        bt_ids=["BacktestEMABounce-eth_e1h_9"],
        start_dt="2025-12-10 00:00:00",
        end_dt="2025-12-20 23:59:59",
        tradeseries_file="set1/set1_tradeseries.json",
        trades_file="set1/set1_trades.json")
    assert len(bts) == 1
    assert isinstance(bts[0], Backtest)
    assert bts[0].bt_id == "BacktestEMABounce-eth_e1h_9"
    assert bts[0].start_dt == "2025-12-10 00:00:00"
    assert bts[0].end_dt == "2025-12-20 23:59:59"
    assert len(bts[0].tradeseries) == 1
    ts = bts[0].tradeseries[0]
    assert ts.ts_id == "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    assert ts.start_dt == "2025-12-10 00:00:00"
    assert ts.end_dt == "2025-12-20 23:59:59"
    assert len(ts.trades) == 28
    for t in ts.trades:
        assert isinstance(t, Trade)
        assert t.ts_id == "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
        assert t.bt_id == "BacktestEMABounce-eth_e1h_9"
    say(f"Rebuilt 1 Backtest {bts[0].bt_id} with date restrictions, "
        f"1 TradeSeries {ts.ts_id}, and {len(ts.trades)} Trades.",
        console_output)

    # All done! ###########################################
    say("\n############################################", console_output)
    say("All objects match expected counts and types.", console_output)


if __name__ == "__main__":
    rebuild_testdata(console_output=True)
