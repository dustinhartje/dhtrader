import json
import csv
import site
from datetime import datetime
site.addsitedir('modulepaths')
from dhcharts import Candle
from dhtrades import Backtest, TradeSeries, Trade
from dhstore import get_backtests_by_field, get_trades_by_field, get_candles
from dhstore import get_tradeseries_by_field, get_indicator_datapoints
from dhutil import dt_to_epoch, this_candle_start


def prepare_for_csv(data_list):
    """Convert list of dicts to CSV-ready format by serializing
    lists/dicts to JSON strings
    """
    result = []
    for row in data_list:
        new_row = {}
        for key, value in row.items():
            if isinstance(value, (list, dict)):
                # Convert to JSON string for CSV compatibility
                new_row[key] = json.dumps(value)
            else:
                new_row[key] = value
        result.append(new_row)
    return result


class Extractor():
    """Extract various objects from dhstore to build test data files"""

    def extract_candles(self,
                        start_dt,
                        end_dt,
                        timeframe,
                        symbol="ES",
                        out_file=None,
                        ):
        """Extract candles from storage and optionally save as JSON
        and CSV files
        """
        cans_obj = get_candles(start_epoch=dt_to_epoch(start_dt),
                               end_epoch=dt_to_epoch(end_dt),
                               timeframe=timeframe, symbol=symbol)
        cans_json = [c.to_clean_dict() for c in cans_obj]
        if out_file is not None:
            # Write JSON file
            with (open(out_file, 'w')) as f:
                json.dump(cans_json, f, default=str)
            # Write CSV file with useful and ordered fields only
            csv_file = out_file.replace('.json', '.csv')
            if cans_json:
                fieldnames = [
                    'c_datetime', 'c_open', 'c_high', 'c_low',
                    'c_close', 'c_volume', 'c_symbol', 'c_timeframe',
                    'c_epoch'
                ]
                with open(csv_file, 'w', newline='') as f:
                    writer = csv.DictWriter(
                        f, fieldnames=fieldnames,
                        extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(cans_json)
        return cans_json

    def extract_trades(self,
                       ts_ids,  # single str or list of str
                       start_dt,
                       end_dt,
                       symbol="ES",
                       ind_dps=None,
                       out_file=None,
                       ):
        """Extract trades from storage and optionally save as JSON
        and CSV files
        """
        if isinstance(ts_ids, str):
            ts_ids = [ts_ids]
        if not isinstance(ts_ids, list):
            raise ValueError("ts_ids must be a string or list of strings")
        all_trades = []
        long_trades = []
        short_trades = []
        for ts_id in ts_ids:
            trades_obj = get_trades_by_field(field="ts_id",
                                             value=ts_id)
            all_trades.append({"ts_id": ts_id,
                               "trades": [t.to_clean_dict()
                                          for t in trades_obj]})
        if start_dt is not None or end_dt is not None:
            for ts in all_trades:
                filtered_trades = []
                for t in ts['trades']:
                    include_trade = True
                    if start_dt is not None:
                        if t['open_epoch'] < dt_to_epoch(start_dt):
                            include_trade = False
                    if end_dt is not None:
                        if t['open_epoch'] > dt_to_epoch(end_dt):
                            include_trade = False
                    if include_trade:
                        filtered_trades.append(t)
                ts['trades'] = filtered_trades

        # Build index of indicator datapoints by datetime
        # string if provided
        ind_dp_index = {}
        if ind_dps is not None:
            for i, dp in enumerate(ind_dps):
                # Index by datetime string with index position
                ind_dp_index[dp['dt']] = i

        # Add indicator datapoints and parse long vs short trades
        for ts in all_trades:
            for trade in ts['trades']:
                # Add indicator datapoint value if available
                if ind_dps is not None:
                    # Round trade time down to the hour
                    trade_parent_dt = this_candle_start(
                        dt=trade['open_dt'],
                        timeframe=trade['timeframe'])
                    # Convert to string to match index keys
                    trade_parent = str(trade_parent_dt)
                    # Find the current hour's indicator index
                    if trade_parent in ind_dp_index:
                        current_idx = ind_dp_index[trade_parent]
                        # Get the previous datapoint by index
                        if current_idx > 0:
                            prev_ind_dp = ind_dps[current_idx - 1]
                            trade['prev_ind_dp'] = prev_ind_dp['value']
                        else:
                            trade['prev_ind_dp'] = None
                    else:
                        trade['prev_ind_dp'] = None
                # Copy trades to long vs short lists
                direction = trade['direction']
                if direction == 'long':
                    long_trades.append(trade)
                elif direction == 'short':
                    short_trades.append(trade)

        # Write output files if specified
        if out_file is not None:
            # Write JSON file including all Trades
            with (open(out_file, 'w')) as f:
                json.dump(all_trades, f, default=str)
            base_file = out_file.replace('.json', '')

            # Write long trades
            long_json_file = f"{base_file}_long.json"
            with open(long_json_file, 'w') as f:
                json.dump(long_trades, f, default=str)
            long_csv_file = f"{base_file}_long.csv"
            fieldnames = [
                'open_dt', 'close_dt', 'entry_price',
                'exit_price', 'high_price', 'low_price',
                'profitable', 'first_min_open', 'bt_id',
                'ts_id', 'direction', 'flipper', 'is_open',
                'prof_target', 'prof_ticks', 'stop_target',
                'stop_ticks', 'offset_ticks', 'open_epoch',
                'symbol', 'timeframe', 'trading_hours',
                'prev_ind_dp'
            ]
            with open(long_csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(
                    f, fieldnames=fieldnames,
                    extrasaction='ignore')
                writer.writeheader()
                writer.writerows(long_trades)

            # Write short trades
            short_json_file = f"{base_file}_short.json"
            with open(short_json_file, 'w') as f:
                json.dump(short_trades, f, default=str)
            short_csv_file = f"{base_file}_short.csv"
            fieldnames = [
                'open_dt', 'close_dt', 'entry_price',
                'exit_price', 'high_price', 'low_price',
                'profitable', 'first_min_open', 'bt_id',
                'ts_id', 'direction', 'flipper', 'is_open',
                'prof_target', 'prof_ticks', 'stop_target',
                'stop_ticks', 'offset_ticks', 'open_epoch',
                'symbol', 'timeframe', 'trading_hours',
                'prev_ind_dp'
            ]
            with open(short_csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(
                    f, fieldnames=fieldnames,
                    extrasaction='ignore')
                writer.writeheader()
                writer.writerows(short_trades)

        return {"all_trades": all_trades,
                "long_trades": long_trades,
                "short_trades": short_trades,
                }

    def extract_tradeseries(self,
                            ts_ids,  # single str or list of str
                            start_dt=None,
                            end_dt=None,
                            symbol="ES",
                            out_file=None,
                            ):
        """Extract one or more tradeseries from storage by ts_id and
        optionally save as JSON and CSV files
        """
        if isinstance(ts_ids, str):
            ts_ids = [ts_ids]
        if not isinstance(ts_ids, list):
            raise ValueError("ts_ids must be a string or list of strings")
        tss = []
        for ts_id in ts_ids:
            tss.append(get_tradeseries_by_field(field="ts_id",
                                                value=ts_id,
                                                include_trades=False)[0])
        if start_dt is not None:
            for ts in tss:
                ts.start_dt = start_dt
        if end_dt is not None:
            for ts in tss:
                ts.end_dt = end_dt
        tss_json = [ts.to_clean_dict() for ts in tss]
        if out_file is not None:
            # Write JSON file
            with (open(out_file, 'w')) as f:
                json.dump(tss_json, f, default=str)
            csv_file = out_file.replace('.json', '.csv')
            # Write CSV file with useful and ordered fields only
            fieldnames = [
                'bt_id', 'end_dt', 'name', 'params_str',
                'start_dt', 'symbol', 'tags', 'timeframe',
                'trades', 'trading_hours', 'ts_id'
            ]
            csv_data = prepare_for_csv(tss_json)
            with open(csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(
                    f, fieldnames=fieldnames,
                    extrasaction='ignore')
                writer.writeheader()
                writer.writerows(csv_data)

        return tss_json

    def extract_backtests(self,
                          bt_ids,  # single str or list of str
                          start_dt=None,
                          end_dt=None,
                          out_file=None,
                          ):
        """Extract one or more BacktestIndTag objects from storage by
        bt_id and optionally save as JSON and CSV files
        """
        if isinstance(bt_ids, str):
            bt_ids = [bt_ids]
        if not isinstance(bt_ids, list):
            raise ValueError("bt_ids must be a string or list of strings")
        bts_json = []
        for bt_id in bt_ids:
            bt = get_backtests_by_field(field="bt_id",
                                        value=bt_id)[0]
            if start_dt is not None:
                bt["start_dt"] = start_dt
            if end_dt is not None:
                bt["end_dt"] = end_dt
            bts_json.append(bt)
        if out_file is not None:
            # Write JSON file
            with (open(out_file, 'w')) as f:
                json.dump(bts_json, f, default=str)
            # Write CSV file with useful and ordered fields only
            csv_file = out_file.replace('.json', '.csv')
            fieldnames = [
                '_id', 'autoclose', 'autoload_charts', 'bt_id',
                'chart_1m', 'chart_tf', 'class_name', 'contracts',
                'direction', 'end_dt', 'flipper', 'indicator',
                'length', 'name', 'offs', 'parameters',
                'prefer_stored', 'profs', 'progress_bar',
                'start_dt', 'stops', 'symbol', 'timeframe',
                'tradeseries', 'trading_hours'
            ]
            csv_data = prepare_for_csv(bts_json)
            with open(csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(
                    f, fieldnames=fieldnames,
                    extrasaction='ignore')
                writer.writeheader()
                writer.writerows(csv_data)
        return bts_json

    def extract_indicator_datapoints(self,
                                     ind_id,
                                     start_dt,
                                     end_dt,
                                     out_file=None,
                                     ):
        """Extract indicator datapoints from storage and optionally
        save as JSON and CSV files
        """
        ind_dps = get_indicator_datapoints(ind_id=ind_id,
                                           earliest_dt=start_dt,
                                           latest_dt=end_dt)
        ind_dps_json = [dp.to_clean_dict() for dp in ind_dps]
        fieldnames = ind_dps_json[0].keys()
        if out_file is not None:
            # Write JSON file
            with open(out_file, 'w') as f:
                json.dump(ind_dps_json, f, default=str)
            # Write CSV file
            csv_file = out_file.replace('.json', '.csv')
            with open(csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(ind_dps_json)
        return ind_dps_json


class Rebuilder():
    """Recreate DHTrader objects from test data files"""
    def rebuild_candles(self,
                        in_file,
                        start_dt=None,
                        end_dt=None,
                        ):
        """Rebuild candles from JSON test data file"""
        print(f"Reading candles from {in_file}")
        with (open(in_file, 'r')) as f:
            cans_json = json.load(f)
        if not isinstance(cans_json, list):
            raise ValueError("Input JSON must be a list of Candle dicts")
        cans = []
        for c in cans_json:
            cans.append(Candle(c_datetime=c['c_datetime'],
                               c_timeframe=c['c_timeframe'],
                               c_open=c['c_open'],
                               c_high=c['c_high'],
                               c_low=c['c_low'],
                               c_close=c['c_close'],
                               c_volume=c['c_volume'],
                               c_symbol=c['c_symbol'],
                               ))
        cans.sort(key=lambda c: c.c_epoch)
        if start_dt is not None or end_dt is not None:
            if start_dt is None:
                start_epoch = cans[0].c_epoch
            else:
                start_epoch = dt_to_epoch(start_dt)
            if end_dt is None:
                end_epoch = cans[-1].c_epoch
            else:
                end_epoch = dt_to_epoch(end_dt)
            cans = [c for c in cans
                    if c.c_epoch >= start_epoch and c.c_epoch <= end_epoch]
        return cans

    def rebuild_trades(self,
                       in_file,
                       ts_id=None,
                       start_dt=None,
                       end_dt=None,
                       ):
        """Rebuild trades from JSON test data file"""
        print(f"Reading trades from {in_file}")
        with (open(in_file, 'r')) as f:
            all_trades = json.load(f)
        if not isinstance(all_trades, list):
            raise ValueError("Input JSON must be a list of Trade dicts")
        trades = []
        # Append trades matching by ts_id if specified
        print(f"Rebuilding trades for ts_id={ts_id} (None uses all ts_ids)")
        for ts in all_trades:
            print(f"Processing ts_id: {ts['ts_id']} with {len(ts['trades'])} "
                  "trades")
            if ts_id is None or ts['ts_id'] == ts_id:
                for t in ts['trades']:
                    trades.append(Trade(open_dt=t['open_dt'],
                                        direction=t['direction'],
                                        timeframe=t['timeframe'],
                                        trading_hours=t['trading_hours'],
                                        entry_price=t['entry_price'],
                                        close_dt=t['close_dt'],
                                        created_dt=t['created_dt'],
                                        open_epoch=t['open_epoch'],
                                        high_price=t['high_price'],
                                        low_price=t['low_price'],
                                        exit_price=t['exit_price'],
                                        stop_target=t['stop_target'],
                                        prof_target=t['prof_target'],
                                        stop_ticks=t['stop_ticks'],
                                        prof_ticks=t['prof_ticks'],
                                        offset_ticks=t['offset_ticks'],
                                        symbol=t['symbol'],
                                        is_open=t['is_open'],
                                        profitable=t['profitable'],
                                        name=t['name'],
                                        version=t['version'],
                                        ts_id=t['ts_id'],
                                        bt_id=t['bt_id'],
                                        tags=t['tags'],
                                        ))

        trades.sort(key=lambda t: t.open_epoch)

        # Limit by dates if specified
        if start_dt is not None or end_dt is not None:
            if start_dt is None:
                start_epoch = trades[0].open_epoch
            else:
                start_epoch = dt_to_epoch(start_dt)
            if end_dt is None:
                end_epoch = trades[-1].open_epoch
            else:
                end_epoch = dt_to_epoch(end_dt)
            trades = [t for t in trades
                      if t.open_epoch >= start_epoch and
                      t.open_epoch <= end_epoch]

        return trades

    def rebuild_tradeseries(self,
                            in_file,
                            ts_ids=None,
                            bt_ids=None,
                            start_dt=None,
                            end_dt=None,
                            trades_file=None,
                            ):
        """Rebuild one or more tradeseries from JSON test data file.  Provide
        trades_file to also load Trades for each TradeSeries."""
        print(f"Reading TradeSeries from {in_file}")
        with (open(in_file, 'r')) as f:
            tss_json = json.load(f)
        if not isinstance(tss_json, list):
            raise ValueError("Input JSON must be a list of TradeSeries dicts")
        if trades_file is not None:
            print(f"Also loading Trades from {trades_file}")
            all_trades = self.rebuild_trades(in_file=trades_file,
                                             start_dt=start_dt,
                                             end_dt=end_dt)
        else:
            all_trades = None
        tss = []
        for ts in tss_json:
            # pick out this ts's trades if trades were loaded
            if ts_ids is not None:
                if ts['ts_id'] not in ts_ids:
                    continue
            if bt_ids is not None:
                if ts['bt_id'] not in bt_ids:
                    continue
            if all_trades is not None:
                trades = [t for t in all_trades if t.ts_id == ts['ts_id']]
            else:
                trades = []
            tss.append(TradeSeries(start_dt=ts['start_dt'],
                                   end_dt=ts['end_dt'],
                                   timeframe=ts['timeframe'],
                                   trading_hours=ts['trading_hours'],
                                   symbol=ts['symbol'],
                                   name=ts['name'],
                                   params_str=ts['params_str'],
                                   ts_id=ts['ts_id'],
                                   bt_id=ts['bt_id'],
                                   trades=trades,
                                   tags=None,
                                   ))
            if start_dt is not None:
                tss[-1].start_dt = start_dt
            if end_dt is not None:
                tss[-1].end_dt = end_dt
            if trades_file is not None:
                tss[-1].trades = self.rebuild_trades(in_file=trades_file,
                                                     ts_id=ts['ts_id'],
                                                     start_dt=start_dt,
                                                     end_dt=end_dt)

        return tss

    def rebuild_backtests(self,
                          in_file,
                          bt_ids=None,
                          start_dt=None,
                          end_dt=None,
                          tradeseries_file=None,
                          trades_file=None,
                          ):
        """Rebuild one or more Backtest objects from JSON test data
        file.  Provide tradeseries_file to also load TradeSeries for each
        backtest, and trades_file to load Trades for each TradeSeries."""
        print(f"Reading Backtest from {in_file}")
        with (open(in_file, 'r')) as f:
            bts_json = json.load(f)
        if not isinstance(bts_json, list):
            raise ValueError("Input JSON must be list of Backtest dicts")
        bts = []
        for bt in bts_json:
            if bt_ids is not None:
                if bt['bt_id'] not in bt_ids:
                    continue
            bts.append(Backtest(start_dt=bt['start_dt'],
                                end_dt=bt['end_dt'],
                                timeframe=bt['timeframe'],
                                trading_hours=bt['trading_hours'],
                                symbol=bt['symbol'],
                                name=bt['name'],
                                parameters=bt['parameters'],
                                bt_id=bt['bt_id'],
                                class_name=bt['class_name'],
                                autoload_charts=False,
                                prefer_stored=False,
                                tradeseries=None,
                                ))
            if start_dt is not None:
                bts[-1].start_dt = start_dt
            if end_dt is not None:
                bts[-1].end_dt = end_dt
            if tradeseries_file is not None:
                bts[-1].tradeseries = self.rebuild_tradeseries(
                    in_file=tradeseries_file,
                    bt_ids=[bt['bt_id']],
                    start_dt=start_dt,
                    end_dt=end_dt,
                    trades_file=trades_file)
        return bts
