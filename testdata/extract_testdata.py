import os
import sys
from testdata import Extractor
from datetime import timedelta
import json
from dhutil import dt_as_dt, dt_as_str
"""Extract test data from storage for testing and development purposes.  Run
as a script to extract and write to json files.  Or import and call
extract_all() from other test modules to extract data for testing
without writing files.

NOTE - due to the nesting structure I have not been able to determine how to
setup pathing to get all packages to import correctly when running from a unit
test file.  Maybe circle back to that later but it works fine run as a script
which is enough for now."""


def say(msg: str, console_output=True):
    if console_output:
        print(f"{msg}")


def extract_testdata(blob: dict, write=False, console_output=False):
    e = Extractor()
    name = blob["name"]
    description = blob["description"]
    sd = blob["start_dt"]
    ed = blob["end_dt"]
    tf = blob["timeframe"]
    th = blob["trading_hours"]
    sy = blob["symbol"]
    bt_ids = blob["bt_ids"]
    ts_ids = blob["ts_ids"]
    ind_id = blob["ind_id"]
    tag = f"{name}:" if write else f"{name} (test):"
    if write:
        path = f"./{name}/"
        os.makedirs(path, exist_ok=True)

    # Candles #############################################
    # 1m candles
    say(f"{tag} Extracting 1m Candles...", console_output)
    out_file = None if not write else f"{path}{name}_1m_candles.json"
    cans_1m = e.extract_candles(start_dt=sd,
                                end_dt=ed,
                                timeframe="1m",
                                symbol=sy,
                                out_file=out_file)
    say(f"{tag} {len(cans_1m)} 1m Candles extracted.", console_output)
    # timeframe based candles
    if tf != "1m":
        say(f"{tag} Extracting {tf} Candles...", console_output)
        out_file = None if not write else f"{path}{name}_{tf}_candles.json"
        cans_tf = e.extract_candles(start_dt=sd,
                                    end_dt=ed,
                                    timeframe=tf,
                                    symbol=sy,
                                    out_file=out_file)
        print(f"{tag} {len(cans_tf)} {tf} Candles extracted.")

    # Indicator Datapoints ################################
    if ind_id is not None:
        say(f"{tag} Extracting Indicator Datapoints...",
            console_output)
        if "EMA" in ind_id:
            parts = ind_id.split('_')
            ind_name = f"{parts[2]}{parts[5][1:]}ema"
        else:
            raise ValueError("Unable to parse indicator name from ind_id "
                             f"{ind_id}")
        out_file = (None if not write else
                    f"{path}{name}_ind_dps_{ind_name}.json")
        # Extract extra indicator datapoints to ensure previous datapoint avail
        # for first candle even after weekends/holidays/long closures
        ind_sd = dt_as_str(dt_as_dt(sd) - timedelta(days=7))
        ind_dps = e.extract_indicator_datapoints(
            ind_id=ind_id,
            start_dt=ind_sd,
            end_dt=ed,
            out_file=out_file)
        say(f"{tag} {len(ind_dps)} Indicator Datapoints extracted.",
            console_output)
    else:
        ind_dps = None

    # Trades ##############################################
    say(f"{tag} Extracting Trades...", console_output)
    out_file = None if not write else f"{path}{name}_trades.json"
    trades = e.extract_trades(ts_ids=ts_ids,
                              start_dt=sd,
                              end_dt=ed,
                              symbol=sy,
                              out_file=out_file,
                              ind_dps=ind_dps)
    say(f"{tag} Trades extracted:", console_output)
    for ts in trades["all_trades"]:
        say(f"ts_id={ts['ts_id']}: {len(ts['trades'])}", console_output)

    # TradeSeries #########################################
    say(f"{tag} Extracting TradeSeries...", console_output)
    out_file = None if not write else f"{path}{name}_tradeseries.json"
    tradeseries = e.extract_tradeseries(ts_ids=ts_ids,
                                        start_dt=sd,
                                        end_dt=ed,
                                        symbol=sy,
                                        out_file=out_file)
    say(f"{tag} TradeSeries extracted:", console_output)
    for ts in tradeseries:
        say(f"ts_id={ts['ts_id']}", console_output)

    # Backtests ###########################################
    say(f"{tag} Extracting Backtests...", console_output)
    out_file = None if not write else f"{path}{name}_backtests.json"
    backtests = e.extract_backtests(bt_ids=bt_ids,
                                    start_dt=sd,
                                    end_dt=ed,
                                    out_file=out_file)
    say(f"{tag} Backtests extracted:", console_output)
    for bt in backtests:
        say(f"bt_id={bt['bt_id']}", console_output)

    # Metadata ############################################
    if write:
        metadata = {
            "description": description,
            "start_dt": sd,
            "end_dt": ed,
            "timeframe": tf,
            "trading_hours": th,
            "symbol": sy,
            "ind_id": ind_id,
            "bt_ids": bt_ids,
            "ts_ids": ts_ids,
        }
        metadata_file = f"{name}_metadata.json"
        say(f"{tag} Writing metadata to {metadata_file}...",
            console_output)
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=4, default=str)

    # Return all extracted objects ########################
    return {"candles_1m": cans_1m,
            "candles_tf": cans_tf if tf != "1m" else None,
            "ind_dps": ind_dps,
            "trades": trades,
            "tradeseries": tradeseries,
            "backtests": backtests,
            }


def extract_all(write=False, console_output=False):
    # Test extracting all objects from storage.  If anything fails, raise
    # an exception before writing any files.
    set1 = {"name": "set1",
            "description": (
                "3 weeks of ES 1m and e1h candles for the below "
                "BacktestIndTag and TradeSeries.  Includes all hours the "
                "market was open and all possible trades.  No deoverlapping "
                "or other processing has been done to this dataset.  "
                "Avoided holidays due to open Issue with early market "
                "closures not closing Trades at end of day."
            ),
            "start_dt": "2025-11-30 18:00:00",
            "end_dt": "2025-12-19 17:00:00",
            "timeframe": "e1h",
            "trading_hours": "eth",
            "symbol": "ES",
            "bt_ids": ['BacktestEMABounce-eth_e1h_9',
                       'BacktestEMAReject-eth_e1h_9'],
            "ts_ids": ['BacktestEMABounce-eth_e1h_9_s80-p160-o0',
                       'BacktestEMAReject-eth_e1h_9_s80-p160-o40'],
            "ind_id": "ES_eth_e1h_EMA_close_l9_s2",
            }
    say("\n######## Extracting Data For Set1 ########", console_output)
    ex_test = extract_testdata(blob=set1,
                               write=False,
                               console_output=console_output)
    # Confirm expected ids and counts
    say("\n######## Confirming Extracted Data ########", console_output)
    assert len(ex_test["candles_1m"]) == 20700
    assert len(ex_test["candles_tf"]) == 345
    t = ex_test["trades"]["all_trades"]
    assert t[0]["ts_id"] == "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    assert t[1]["ts_id"] == "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    assert len(t[0]["trades"]) == 76
    assert len(t[1]["trades"]) == 54
    # Confirm first trade as expected
    print(t[0]["trades"][0])
    assert t[0]["trades"][0] == {
        'open_dt': '2025-11-30 19:06:00', 'close_dt': '2025-11-30 19:48:00',
        'created_dt': '2025-12-06 13:20:29', 'direction': 'long',
        'timeframe': 'e1h', 'trading_hours': 'eth', 'entry_price': 6848.25,
        'stop_target': 6828.25, 'prof_target': 6888.25, 'high_price': 6848.25,
        'low_price': 6828.25, 'exit_price': 6828.25, 'stop_ticks': 80,
        'prof_ticks': 160, 'offset_ticks': 0, 'symbol': 'ES', 'is_open': False,
        'profitable': False, 'name': 'BacktestEMABounce-eth_e1h_9',
        'version': '1.0.0', 'ts_id': 'BacktestEMABounce-eth_e1h_9_s80-p160-o0',
        'bt_id': 'BacktestEMABounce-eth_e1h_9', 'tags': [], 'flipper': 1,
        'open_epoch': 1764547560, 'first_min_open': False,
        'prev_ind_dp': 6848.23}
    # Validate indicator datapoints in long and short trades
    t = ex_test["trades"]["long_trades"]
    print(t)
    ts = ex_test["tradeseries"]
    assert ts[0]["ts_id"] == "BacktestEMABounce-eth_e1h_9_s80-p160-o0"
    assert ts[1]["ts_id"] == "BacktestEMAReject-eth_e1h_9_s80-p160-o40"
    bt = ex_test["backtests"]
    assert bt[0]["bt_id"] == "BacktestEMABounce-eth_e1h_9"
    assert bt[1]["bt_id"] == "BacktestEMAReject-eth_e1h_9"
    dps = ex_test["ind_dps"]
    assert len(dps) == 442
    assert dps[0] == {'dt': '2025-11-23 18:00:00',
                      'value': 6626.4,
                      'ind_id': 'ES_eth_e1h_EMA_close_l9_s2',
                      'epoch': 1763938800}
    # If all extractions passed (no Exception and assertions good),
    # rerun extraction writing to json files this time
    say("All ids and object counts match expectations!", console_output)
    say("\n######## Writing Extracted Data To JSON/CSV Files ########",
        console_output)
    # Do not write files if running as a test.  If called from
    # __main__ tho we will write files on the 2nd pass.
    if write:
        extract_testdata(blob=set1, write=write, console_output=console_output)


if __name__ == "__main__":
    extract_all(write=True, console_output=True)
