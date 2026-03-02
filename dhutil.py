from datetime import timedelta, datetime as dt
import csv
import sys
from tabulate import tabulate
from dhcommon import (
    dt_as_dt, dt_as_str, dt_to_epoch, timeframe_delta,
    this_candle_start, valid_trading_hours, check_tf_th_compatibility)
from dhcharts import Candle
from dhstore import (
    get_symbol_by_ticker, get_candles, get_events, review_candles)


def next_candle_start(dt,
                      trading_hours: str,
                      symbol: str = "ES",
                      timeframe: str = "1m",
                      events: list = None,
                      ):
    """Returns the next datetime that represents a proper candle start
    after the given datetime (dt).  Will not return given datetime even if
    it starts a candle."""
    if isinstance(symbol, str):
        symbol = get_symbol_by_ticker(ticker=symbol)
    valid_trading_hours(trading_hours)
    check_tf_th_compatibility(tf=timeframe, th=trading_hours)
    # Start with a rounded minute, no seconds or ms supported
    next_dt = dt_as_dt(dt)
    next_dt = next_dt.replace(microsecond=0, second=0)
    min_delta = timedelta(minutes=1)

    done = False
    while not done:
        # All timeframes add at least 1 minute each loop
        next_dt = next_dt + min_delta
        # Then each timeframe other than 1m keeps adding minutes until it
        # reaches a minute representing it's appropriate candle start time.
        if timeframe == "5m":
            while next_dt.minute % 5 != 0:
                next_dt = next_dt + min_delta
        elif timeframe == "15m":
            while next_dt.minute % 15 != 0:
                next_dt = next_dt + min_delta
        elif timeframe == "r1h":
            while next_dt.minute != 30:
                next_dt = next_dt + min_delta
        elif timeframe == "e1h":
            while next_dt.minute != 0:
                next_dt = next_dt + min_delta
        else:
            raise ValueError(f"timeframe: {timeframe} not supported")
        # Ensure the market is open at the dt found, otherwise keep looping
        done = symbol.market_is_open(trading_hours=trading_hours,
                                     target_dt=next_dt,
                                     check_closed_events=True,
                                     events=events,
                                     )

    return next_dt


def generate_zero_volume_candle(c_datetime,
                                timeframe: str = "1m",
                                symbol: str = "ES",
                                ):
    """Returns a zero volume candle with OHLC values all set to the prior
    candle's closing value.  Primarily used to fill gaps in 1m candle storage
    where data providers sometimes omit candles with zero trading volume."""
    if symbol != "ES":
        raise ValueError("Only symbol: 'ES' is currently supported")
    if timeframe == "1m":
        delta = timeframe_delta(timeframe)
    else:
        raise ValueError(f"timeframe: {timeframe} is not currently supported")
    prior_epoch = dt_to_epoch(dt_as_dt(c_datetime) - delta)
    prior_candle = get_candles(start_epoch=prior_epoch,
                               end_epoch=prior_epoch,
                               timeframe=timeframe,
                               symbol=symbol)
    # Ensure we got back exactly one Candle and use it's closing value
    if len(prior_candle) == 1 and isinstance(prior_candle[0], Candle):
        v = prior_candle[0].c_close
        result = Candle(c_datetime=c_datetime,
                        c_timeframe=timeframe,
                        c_open=v,
                        c_high=v,
                        c_low=v,
                        c_close=v,
                        c_volume=0,
                        c_symbol=symbol,
                        )
    else:
        result = None

    return result


def expected_candle_datetimes(start_dt,
                              end_dt,
                              timeframe: str,
                              symbol="ES",
                              exclude_categories: list = None,
                              ):
    """Return a sorted list of datetimes within the provided start_dt and
    end_dt (inclusive of both) that should exist for the given symbol based on
    standard market hours, after removing any known Closed events.
    Optionally also exclude anything within known Event times matching
    exclude_categories."""
    if isinstance(symbol, str):
        symbol = get_symbol_by_ticker(ticker=symbol)
    if symbol.ticker == "ES":
        if timeframe == "r1h":
            trading_hours = "rth"
        else:
            trading_hours = "eth"
    else:
        raise ValueError("Only ES is currently supported as symbol for now")
    # Build a list of possible candles within standard market hours
    result_std = []
    adder = timeframe_delta(timeframe)
    # Start with the start_dt if it is a valid candle start
    this = this_candle_start(dt=start_dt,
                             timeframe=timeframe,
                             )
    # Otherwise start with the next valid candle
    if not this == dt_as_dt(start_dt):
        this = next_candle_start(dt=start_dt,
                                 timeframe=timeframe,
                                 trading_hours=trading_hours,
                                 )
    ender = dt_as_dt(end_dt)
    while this <= ender:
        if symbol.market_is_open(trading_hours=trading_hours,
                                 target_dt=this,
                                 check_closed_events=False,
                                 ):
            result_std.append(this)
        this = this + adder

    # Remove any candles falling inside of non-standard market closures
    start_epoch = dt_to_epoch(start_dt)
    end_epoch = dt_to_epoch(end_dt)
    all_events = get_events(start_epoch=start_epoch,
                            end_epoch=end_epoch,
                            symbol=symbol,
                            )
    closures = []
    result = []
    # Only evaluate market Closed events
    for e in all_events:
        if e.category == "Closed":
            closures.append(e)
    # Check each candle against closures to build a new expected list
    for c in result_std:
        include = True
        for e in closures:
            if e.start_epoch <= dt_to_epoch(c) <= e.end_epoch:
                include = False
        if include:
            result.append(c)

    return result


def remediate_candle_gaps(timeframe: str = "1m",
                          symbol="ES",
                          prompt: bool = True,
                          fix_obvious: bool = False,
                          fix_unclear: bool = False,
                          dry_run=False,
                          ):
    """Identifies gaps for review in stored candles and offers to store zero
    volume candles in their place.  Currently only supports 1m as the other
    timeframes are calced from these, but I wrote in timeframe to keep
    consistent argument flows and allow future expansion optionality.

    fix_obvious: Automatically fix any candles that are obviously gaps due to
    normal after hours zero volume periods

    prompt: Whether to prompt the user for confirmation before fixing candles
            that have not been fixed due to obvious criteria being met

    dry_run: Run all logic as if remediation is being performed, but only
    print candle storage actions without actually performing them."""
    if timeframe == "1m":
        delta = timedelta(minutes=1)
    else:
        raise ValueError("timeframe: {timeframe} is not currently supported")
    if isinstance(symbol, str):
        symbol = get_symbol_by_ticker(ticker=symbol)
    print(f"Remediating {timeframe} Candle Gaps for {symbol.ticker}")
    print(f"Auto fixing obvious zero volume gaps: {fix_obvious}")
    print(f"Prompting before fixing unclear candles: {prompt}")
    print(f"Dry Run Mode (only simulate changes): {dry_run}")
    print("Reviewing candles with integrity checks to identify issues...")
    review = review_candles(timeframe='1m',
                            symbol=symbol,
                            check_integrity=True,
                            return_detail=True,
                            )
    print("Review complete, beginning remediation operations")
    missing_candles = review["missing_candles_by_date"]
    count_date = review["missing_count_by_date"]
    dates = []
    fixed_obvious = []
    fixed_unclear = []
    skipped = []
    errored = []
    for k, v in count_date.items():
        dates.append(k)
    total_dates = len(dates)
    total_missing = str(review["gap_analysis"]["missing_candles_count"])
    print(f"\n{total_dates} dates found with {total_missing} missing candles.")
    print("\nWorking through them by date\n")
    for d in dates:
        obvious_fix = []
        unclear_fix = []
        print("\n============================================================")
        print(f"{d} - {len(missing_candles[d])} missing candles")
        for c in missing_candles[d]:
            c_dt = dt_as_dt(f"{d} {c}")
            pre_start = dt_to_epoch(c_dt - (delta * 5))
            pre_end = dt_to_epoch(c_dt - delta)
            post_start = dt_to_epoch(c_dt + delta)
            post_end = dt_to_epoch(c_dt + (delta * 5))
            pre_cans = get_candles(timeframe=timeframe,
                                   symbol=symbol.ticker,
                                   start_epoch=pre_start,
                                   end_epoch=pre_end,
                                   )
            post_cans = get_candles(timeframe=timeframe,
                                    symbol=symbol.ticker,
                                    start_epoch=post_start,
                                    end_epoch=post_end,
                                    )
            # Print an overview of each candle and surrounding candles
            # for human review before fixing
            this_time = str(c)
            this_vol = "0"
            pre_cans_times = []
            pre_cans_vols = []
            post_cans_times = []
            post_cans_vols = []
            for can in pre_cans:
                pre_cans_times.append(str(can.c_datetime).split(" ")[1][:5])
                pre_cans_vols.append(str(can.c_volume))
            for can in post_cans:
                post_cans_times.append(str(can.c_datetime).split(" ")[1][:5])
                post_cans_vols.append(str(can.c_volume))
            times = pre_cans_times.copy()
            adj_times = pre_cans_times.copy()
            times.extend([f"[{this_time}]"])
            times.extend(post_cans_times)
            adj_times.extend(post_cans_times)
            vols = pre_cans_vols.copy()
            vols.extend([f"[{this_vol}]"])
            vols.extend(post_cans_vols)
            adj_vols = []
            for v in pre_cans_vols:
                adj_vols.append(int(v))
            for v in post_cans_vols:
                adj_vols.append(int(v))
            # Mark obvious if candle falls outside of regular trading hours,
            # some adjacent candles exist on both sides (or is at a market
            # boundary), and the avg vol of adjacent candles is low
            if len(adj_vols) > 0:
                avg_vol = sum(adj_vols) / len(adj_vols)
            else:
                avg_vol = 0
            # Check if market is open, including after hours.
            is_open = symbol.market_is_open(trading_hours="eth",
                                            target_dt=c_dt,
                                            check_closed_events=True,
                                            )
            # Check if market is in regular trading hours, which should very
            # rarely have zero volume candles and thus would not be obvious.
            is_open_rth = symbol.market_is_open(trading_hours="rth",
                                                target_dt=c_dt,
                                                check_closed_events=True,
                                                )
            # Check if this candle is the first or last open minute before or
            # after a market closure, in which case we can skip neighbor checks
            is_first_open_min = (is_open and not symbol.market_is_open(
                trading_hours="eth",
                target_dt=c_dt - delta,
                check_closed_events=True,
                ))
            is_last_open_min = (is_open and not symbol.market_is_open(
                trading_hours="eth",
                target_dt=c_dt + delta,
                check_closed_events=True,
                ))
            # Consider the candle to have neighbors if found, or if the
            # previous/next candle would fall outside of market hours
            has_pre = len(pre_cans) > 0 or is_first_open_min
            has_post = len(post_cans) > 0 or is_last_open_min
            # Combine all of the logic to check if this is an "obvious" fix
            # candidate to replace with a zero volume candle.
            is_obvious = False
            if (not is_open_rth and has_pre and has_post
                    and 0 < avg_vol < 500):
                is_obvious = True
            # Also check for historically known exceptions
            # 2008-2012 had many 16:15 candles with zero volume for unclear
            # reasons despite having many that had trades, so treat as obvious.
            era = symbol.get_era(target_dt=c_dt)["name"]
            if (era == "2008_thru_2012"
                    and c_dt.hour == 16 and c_dt.minute == 15):
                is_obvious = True
            # 2013-2015 had similar anomalies with later closes at 17:15
            if (era == "2012holidays_thru_2015holidays"
                    and c_dt.hour == 17 and c_dt.minute == 15):
                is_obvious = True
            if is_obvious:
                print(f"OBVIOUS: {c_dt.hour} len(pre_cans)={len(pre_cans)} "
                      f"len(post_cans)={len(post_cans)} avg_vol={avg_vol}")
                obvious_fix.append({"c_dt": c_dt,
                                    "times": times,
                                    "vols": vols,
                                    })
            else:
                print(f"UNCLEAR: hr={c_dt.hour} len(pre_cans)={len(pre_cans)} "
                      f"len(post_cans)={len(post_cans)} avg_vol={avg_vol}")
                unclear_fix.append({"c_dt": c_dt,
                                    "times": times,
                                    "vols": vols,
                                    })

        # Work through obvious fixes if flagged, else move them to unclear
        if not fix_obvious:
            unclear_fix.extend(obvious_fix)
        else:
            if dry_run:
                msg = "DRY RUN: "
            else:
                msg = ""
            msg += f"Fixing {len(obvious_fix)} obvious candles"
            print(msg)
            for c in obvious_fix:
                print(tabulate([c["vols"]],
                               headers=c["times"],
                               stralign="center",
                               numalign="center",
                               tablefmt="plain",
                               )
                      )
                z = generate_zero_volume_candle(c_datetime=c["c_dt"],
                                                timeframe="1m",
                                                symbol="ES",
                                                )
                if z is not None:
                    fixed_obvious.append(c)
                    if dry_run:
                        print(f"DRY RUN: I would have fixed with: {z}")
                    else:
                        print(f"Storing zero volume fix candle: {z}")
                        z.store()
                else:
                    print(f"Error creating zero volume candle, skipped {c_dt}")
                    errored.append(c)
        # Now work through unclear candles that did not meed obvious criteria
        print("\n------------------------------------------------------------")
        print(f"{len(unclear_fix)} Unclear candles require human review for "
              f"{d}\n")
        for c in unclear_fix:
            print(tabulate([c["vols"]],
                           headers=c["times"],
                           stralign="center",
                           numalign="center",
                           tablefmt="plain",
                           )
                  )
        cont = ""
        if dry_run:
            cont_msg = "DRY RUN: "
        else:
            cont_msg = ""
        cont_msg += ("Fix by inserting zero volume candles for these [X=Exit] "
                     "(Y/N/X)?")
        while cont not in ["Y", "y", "N", "n", "X", "x"]:
            if fix_unclear and prompt and len(unclear_fix) > 0:
                cont = input(cont_msg)
            else:
                cont = "Y"
            if cont in ["Y", "y"]:
                for c in unclear_fix:
                    z = generate_zero_volume_candle(c_datetime=c["c_dt"],
                                                    timeframe="1m",
                                                    symbol="ES",
                                                    )
                    if z is not None:
                        if dry_run and fix_unclear:
                            print(f"DRY RUN: I would have fixed with: {z}")
                            fixed_unclear.append(c)
                        elif fix_unclear:
                            print(f"Storing zero volume fix candle: {z}")
                            z.store()
                            fixed_unclear.append(c)
                        else:
                            print("Skipping unclear candles for this run")
                            skipped.append(c)
                    else:
                        print("Error creating zero volume candle, skipping",
                              c["c_dt"])
                        errored.append(c)
            elif cont in ["X", "x"]:
                print("Exiting without processing any more days...")
                sys.exit()
            else:
                print("Skipping unclear candles for this day")
                for c in unclear_fix:
                    skipped.append(c)
    if dry_run:
        overview = "DRY_RUN Would have: "
    else:
        overview = ""
    overview += (f"Fixed {len(fixed_obvious)} obvious candles, "
                 f"Fixed {len(fixed_unclear)} unclear candles, "
                 f"Skipped {len(skipped)} candles, "
                 f"Errors encountered on {len(errored)} candles"
                 )

    return {"fixed_obvious": fixed_obvious,
            "fixed_unclear": fixed_unclear,
            "skipped": skipped,
            "errored": errored,
            "overview": overview,
            }


def read_candles_from_csv(start_dt,
                          end_dt,
                          filepath: str,
                          symbol: str = 'ES',
                          timeframe: str = '1m',
                          ):
    """Reads lines from a csv file and returns them as a list of
    dhcharts.Candle objects.  Assumes format matches FirstRate data
    standard of no header row the the following order of fields:
    datetime,open,high,low,close,volume
    This will fail badly if the format of the source file is incorrect!"""
    start_dt = dt_as_dt(start_dt)
    end_dt = dt_as_dt(end_dt)
    candles = []
    with open(filepath, newline='') as src_file:
        rows = csv.reader(src_file)
        for r in rows:
            this_dt = dt.strptime(r[0], '%Y-%m-%d %H:%M:%S')
            if start_dt <= this_dt <= end_dt:
                c = Candle(c_datetime=this_dt,
                           c_timeframe=timeframe,
                           c_symbol=symbol,
                           c_open=r[1],
                           c_high=r[2],
                           c_low=r[3],
                           c_close=r[4],
                           c_volume=r[5],
                           )
                candles.append(c)

    return candles


def store_candles_from_csv(filepath: str,
                           start_dt,
                           end_dt,
                           timeframe: str = "1m",
                           symbol: str = "ES",
                           ):
    """Loads 1m candles from a CSV file into central storage.  Mostly useful
    for quick manual gap fill operations via python console."""
    candles = read_candles_from_csv(start_dt=start_dt,
                                    end_dt=end_dt,
                                    filepath=filepath,
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    )
    print(f"{len(candles)} candles found, storing them")
    for c in candles:
        c.store()
    print("Done storing, attempting to retrieve them for validation.")
    new_candles = get_candles(start_epoch=dt_to_epoch(start_dt),
                              end_epoch=dt_to_epoch(end_dt),
                              timeframe=timeframe,
                              symbol=symbol,
                              )
    new_dts = []
    for c in new_candles:
        new_dts.append(dt_as_str(c.c_datetime))
    print(new_dts)
    print(f"{len(new_candles)} candles retrieved successfully")


def compare_candles_vs_csv(filepath,
                           timeframe: str = "1m",
                           symbol: str = "ES",
                           price_diff_threshold: float = 0.5,
                           vol_diff_threshold_perc: int = 10,
                           start_dt=None,
                           end_dt=None,
                           expect_missing_from_storage: list = None):
    """Check stored candles against a CSV source file, primarily used to
    confirm calculated higher timeframes against data provider equivalents
    where available to sanity check calculation process"""
    if expect_missing_from_storage is None:
        expect_missing_from_storage = []
    # Determine start/end datetimes from stored candles if not provided
    if start_dt is None or end_dt is None:
        review = review_candles(timeframe=timeframe,
                                symbol=symbol,
                                check_integrity=False,
                                return_detail=False,
                                )
        if review is None:
            print(f"No {timeframe} candles found in storage, skipping...")
            return None
        if start_dt is None:
            start_dt = review["overview"]["earliest_dt"]
        if end_dt is None:
            end_dt = review["overview"]["latest_dt"]
    print(f"Comparing {symbol} {timeframe} candles in {start_dt} to {end_dt}")
    # Get candles from storage
    start_epoch = dt_to_epoch(start_dt)
    end_epoch = dt_to_epoch(end_dt)
    stored_cans = get_candles(start_epoch=start_epoch,
                              end_epoch=end_epoch,
                              timeframe=timeframe,
                              symbol=symbol,
                              )
    # Get candles from CSV
    csv_cans = read_candles_from_csv(start_dt=start_dt,
                                     end_dt=end_dt,
                                     filepath=filepath,
                                     symbol=symbol,
                                     timeframe=timeframe,
                                     )
    # Compare sets to find any diffs
    all_equal = True
    stored = {}
    csved = {}
    missing = {}
    missing_expected = {}
    extras = {}
    zeros = {}
    diffs = {}
    for c in stored_cans:
        stored[c.c_epoch] = c
    for c in csv_cans:
        csved[c.c_epoch] = c
    # Note any candles in the csv that aren't found in storage
    for k, v in csved.items():
        if k not in stored.keys():
            if v.c_datetime in expect_missing_from_storage:
                missing_expected[k] = v
            else:
                missing[k] = v
    # Note any candles in storage that aren't found in the CSV
    for k, v in stored.items():
        if k not in csved.keys():
            if v.c_volume > 0:
                extras[k] = v
            else:
                zeros[k] = v
    # Now lets find the diffs for non-missing keys
    for k, v in stored.items():
        if k in csved.keys():
            if v != csved[k]:
                sc = v
                cc = csved[k]
                c_diffs = {}
                c_minor_diffs = {}
                if sc.c_datetime != cc.c_datetime:
                    c_diffs["c_datetime"] = {"stored": sc.c_datetime,
                                             "csv": cc.c_datetime}
                # For small OHLC differences we can usually disregard as
                # a minor calculation / order timing anomaly if the prices are
                # very close.  These are statistically trivial and likely
                # couldn't be resolved in any case.
                if sc.c_open != cc.c_open:
                    if abs(sc.c_open - cc.c_open) <= price_diff_threshold:
                        c_minor_diffs["c_open"] = {"stored": sc.c_open,
                                                   "csv": cc.c_open}
                    else:
                        c_diffs["c_open"] = {"stored": sc.c_open,
                                             "csv": cc.c_open}
                if sc.c_high != cc.c_high:
                    if abs(sc.c_high - cc.c_high) <= price_diff_threshold:
                        c_minor_diffs["c_high"] = {"stored": sc.c_high,
                                                   "csv": cc.c_high}
                    else:
                        c_diffs["c_high"] = {"stored": sc.c_high,
                                             "csv": cc.c_high}
                if sc.c_low != cc.c_low:
                    if abs(sc.c_low - cc.c_low) <= price_diff_threshold:
                        c_minor_diffs["c_low"] = {"stored": sc.c_low,
                                                  "csv": cc.c_low}
                    else:
                        c_diffs["c_low"] = {"stored": sc.c_low,
                                            "csv": cc.c_low}
                if sc.c_close != cc.c_close:
                    if abs(sc.c_close - cc.c_close) <= price_diff_threshold:
                        c_minor_diffs["c_close"] = {"stored": sc.c_close,
                                                    "csv": cc.c_close}
                    else:
                        c_diffs["c_close"] = {"stored": sc.c_close,
                                              "csv": cc.c_close}
                if sc.c_volume != cc.c_volume:
                    if sc.c_volume == 0 and cc.c_volume == 0:
                        vol_diff_perc = 0
                    else:
                        vol_diff_perc = (abs(sc.c_volume - cc.c_volume)
                                         / sum([sc.c_volume, cc.c_volume])
                                         * 100)
                    if vol_diff_perc <= vol_diff_threshold_perc:
                        c_minor_diffs["c_volume"] = {"stored": sc.c_volume,
                                                     "csv": cc.c_volume}
                    else:
                        c_diffs["c_volume"] = {"stored": sc.c_volume,
                                               "csv": cc.c_volume}
                # Store diffs by stringified datetime to simplify later review
                this_diff = {"stored_candle": stored[k],
                             "csv_candle": csved[k],
                             "diffs": c_diffs,
                             "minor_diffs": c_minor_diffs,
                             }
                diffs[dt_as_str(v.c_datetime)] = this_diff
    # And put it together into a returnable
    count_stored_dupes = len(stored_cans) - len(stored)
    count_csv_dupes = len(csv_cans) - len(csved)
    count_stored_candles = len(stored_cans)
    count_csv_candles = len(csv_cans)
    count_missing_storage = len(missing)
    count_missing_expected = len(missing_expected)
    count_extras_storage = len(extras)
    count_zerovol_storage = len(zeros)
    count_diffs = 0
    count_minor_diffs = 0
    for k, v in diffs.items():
        count_diffs += len(v["diffs"])
        count_minor_diffs += len(v["minor_diffs"])
    if (count_stored_dupes > 0 or count_csv_dupes > 0
            or count_missing_storage > 0 or count_extras_storage > 0
            or count_diffs > 0):
        all_equal = False
    count_csv_plus = (count_csv_candles + count_zerovol_storage
                      - count_missing_expected)
    if count_stored_candles != count_csv_plus:
        all_equal = False
    counts = {"stored_dupes": count_stored_dupes,
              "csv_dupes": count_csv_dupes,
              "stored_candles": count_stored_candles,
              "csv_candles": count_csv_candles,
              "missing_from_storage": count_missing_storage,
              "missing_from_storage_expected": count_missing_expected,
              "extras_in_storage": count_extras_storage,
              "zero_vol_extras_in_storage": count_zerovol_storage,
              "diffs_from_csv": count_diffs,
              "minor_diffs_from_csv": count_minor_diffs,
              }
    result = {"all_equal": all_equal,
              "counts": counts,
              "differences": diffs,
              "missing_from_storage_expected": missing_expected,
              "missing_from_storage": missing,
              "extras_in_storage": extras,
              "zero_volume_storage_only": zeros,
              }

    return result
