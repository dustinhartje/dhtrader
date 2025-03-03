from datetime import datetime as dt
from datetime import timedelta, date
from copy import deepcopy
import csv
import sys
import re
import logging
import json
from tabulate import tabulate
import dhcharts as dhc
import dhstore as dhs

TIMEFRAMES = ['1m', '5m', '15m', 'r1h', 'e1h', '1d', '1w', '1mo']
TRADING_HOURS = ['rth', 'eth']
EVENT_CATEGORIES = ['Closed', 'Data', 'Unplanned', 'LowVolume', 'Rollover']

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def logi(msg: str):
    log.info(msg)


def logw(msg: str):
    log.warning(msg)


def loge(msg: str):
    log.error(msg)


def logc(msg: str):
    log.critical(msg)


def logd(msg: str):
    log.debug(msg)


# TODO review all functions in this file.  Some may make sense to move to
#      other files (such as dhstore for storage validation stuff) or as
#      methods on dhcharts objects rather than standalone functions

class OperationTimer():
    def __init__(self,
                 name: str,
                 start_dt=None,
                 end_dt=None,
                 elapsed_dt=None,
                 elapsed_str="",
                 auto_start: bool = True,
                 ):
        self.name = name
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.elapsed_dt = elapsed_dt
        self.elapsed_str = elapsed_str
        self.auto_start = auto_start
        if self.auto_start:
            self.start()

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def to_json(self):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        self.update_elapsed()
        working = deepcopy(self.__dict__)
        if self.start_dt is not None:
            working["start_dt"] = dt_as_str(self.start_dt)
        if self.end_dt is not None:
            working["end_dt"] = dt_as_str(self.end_dt)
        if self.elapsed_dt is not None:
            working["elapsed_dt"] = str(self.elapsed_dt)

        return json.dumps(working)

    def to_clean_dict(self):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json())

    def pretty(self):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        Optionally suppress_datapoints to reduce output size when not needed.
        """
        return json.dumps(self.to_clean_dict(),
                          indent=4,
                          )

    def summary(self):
        """Provide a one line str summary of the timer's current status, useful
        for monotiring running timers or final review."""
        self.update_elapsed()
        return (f"OpTimer {self.name} | started {dt_as_str(self.start_dt)} | "
                f"elapsed {self.elapsed_str} | ended {dt_as_str(self.end_dt)}")

    def start(self):
        self.start_dt = dt.now()

    def update_elapsed(self):
        if self.end_dt is None:
            now = dt.now()
        else:
            now = self.end_dt
        self.elapsed_dt = now - self.start_dt
        self.elapsed_str = re.sub("\\..*", "", str(self.elapsed_dt))

    def stop(self):
        self.end_dt = dt.now()
        self.update_elapsed()


def sort_dict(d: dict):
    """Uses insertion ordering to sort a dictionary by keys"""
    keys = []
    sorted_dict = {}
    for k in d.keys():
        keys.append(k)
    keys = sorted(keys)
    for k in keys:
        sorted_dict[k] = d[k]

    return sorted_dict


def prompt_yn(msg):
    p = ""
    while p not in ["Y", "y", "N", "n"]:
        p = input(f"{msg} (Y/N)?:")
    if p in ["Y", "y"]:
        return True
    else:
        return False


def valid_timeframe(t, exit=True):
    if t in TIMEFRAMES:
        return True
    else:
        err_msg = f"{t} is not a valid timeframe in {TIMEFRAMES}"
        if exit:
            raise ValueError(err_msg)
        else:
            print(err_msg)
        return False


def valid_trading_hours(t, exit=True):
    if t in TRADING_HOURS:
        return True
    else:
        err_msg = f"{t} is not a valid specifier in {TRADING_HOURS}"
        if exit:
            raise ValueError(err_msg)
        else:
            print(err_msg)
        return False


def check_tf_th_compatibility(tf, th, exit=True):
    """Confirm that a given timeframe (tf) and trading hours (th) are
    compatible.  Usually we want to exit if this is not so as data cannot be
    trusted otherwise.
    """
    result = True
    if th == "eth":
        if tf in ["r1h", "r1d", "r1w"]:
            result = False
    if th == "rth":
        if tf in ["e1h", "e1d", "e1w"]:
            result = False
    if exit and not result:
        raise ValueError(f"timeframe {tf} and trading_hours {th} cannot "
                         "coexist, please change one of them.")

    return result


def valid_event_category(c, exit=True):
    if c in EVENT_CATEGORIES:
        return True
    else:
        err_msg = "{c} is not a valid event category in {EVENT_CATEGORIES}"
        if exit:
            raise ValueError(err_msg)
        else:
            print(err_msg)
        return False


def dt_as_dt(d):
    """return a datetime object regardless of datetime or string input"""
    if d is None:
        return None
    if isinstance(d, dt):
        return d
    else:
        return dt.strptime(d, "%Y-%m-%d %H:%M:%S")


def dt_as_str(d):
    """return a string object regardless of datetime or string input"""
    if d is None:
        return None
    if isinstance(d, str):
        return d
    else:
        return d.strftime("%Y-%m-%d %H:%M:%S")


def dt_as_time(time: str):
    """Return a datetime.time object for the given %H:%M:%S string"""
    if time is None:
        return None
    return dt_as_dt(f"2000-01-01 {time}").time()


def dow_name(dow: int):
    """Return the human name for a day of the week given it's index as
    represented in datetime.weekday()"""
    names = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
             4: "Friday", 5: "Saturday", 6: "Sunday"}
    return names[dow]


def dt_to_epoch(d):
    """return an epoch integer from a datetime or string"""
    if d is None:
        return None
    return int(dt_as_dt(d).strftime('%s'))


def dt_from_epoch(d):
    """return a datetime object from an epoch integer"""
    if d is None:
        return None
    return dt.fromtimestamp(d)


def timeframe_delta(timeframe: str):
    """return a timedelta object based on the candle timeframe given"""
    if timeframe == "1m":
        return timedelta(minutes=1)
    elif timeframe == "5m":
        return timedelta(minutes=5)
    elif timeframe == "15m":
        return timedelta(minutes=15)
    elif timeframe in ["r1h", "e1h"]:
        return timedelta(hours=1)
    elif timeframe in ["1d", "r1d", "e1d"]:
        return timedelta(days=1)
    else:
        raise ValueError(f"timeframe: {timeframe} not supported")


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
        symbol = dhs.get_symbol_by_ticker(ticker=symbol)
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


def this_candle_start(dt, timeframe: str = "1m"):
    """Returns the datetime that represents a proper candle start
    in which the given datetime would exit in this timeframe.  May return the
    same as input.  This does not confirm market open like next_candle_start()
    since it may not be able to provide an answer in some cases..
    """
    this_dt = dt_as_dt(dt)
    min_delta = timedelta(minutes=1)
    # Start by removing secs and microsecs to get to the whole minute
    this_dt = this_dt.replace(microsecond=0, second=0)
    # Now drop back 1 minute at a time to reach the timeframe correct start
    if timeframe == "1m":
        pass
    elif timeframe == "5m":
        while this_dt.minute % 5 != 0:
            this_dt = this_dt - min_delta
    elif timeframe == "15m":
        while this_dt.minute % 15 != 0:
            this_dt = this_dt - min_delta
    elif timeframe == "r1h":
        while this_dt.minute != 30:
            this_dt = this_dt - min_delta
    elif timeframe == "e1h":
        while this_dt.minute != 0:
            this_dt = this_dt - min_delta
    else:
        raise ValueError(f"timeframe: {timeframe} not supported")

    return this_dt


def rangify_candle_times(times: list,
                         timeframe: str,
                         ):
    """Takes a list of datetimes and returns a list of aggregated datetime
    ranges.  Primarily intended to make human review sane on large sets of
    gap and unexpected candles during integrity checks"""
    delta = timeframe_delta(timeframe)
    sorted_times = sorted(times)
    ranges = []
    this_range = None
    for t in sorted_times:
        # For the starting range just set both values to the current time
        if this_range is None:
            this_range = {"start_dt": dt_as_str(t), "end_dt": dt_as_str(t)}
        else:
            # If the time is one increment after the previously seen time
            # just update the current range
            if dt_as_dt(t) == dt_as_dt(this_range["end_dt"]) + delta:
                this_range["end_dt"] = dt_as_str(t)
            # Otherwise add the current range to the list and start a new one
            else:
                ranges.append(this_range)
                this_range = None
                this_range = {"start_dt": dt_as_str(t), "end_dt": dt_as_str(t)}
    # The last range won't get added in the loop so add it after
    ranges.append(this_range)

    return ranges


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
    prior_candle = dhs.get_candles(start_epoch=prior_epoch,
                                   end_epoch=prior_epoch,
                                   timeframe=timeframe,
                                   symbol=symbol,
                                   )
    # Ensure we got back exactly one Candle and use it's closing value
    if len(prior_candle) == 1 and isinstance(prior_candle[0], dhc.Candle):
        v = prior_candle[0].c_close
        result = dhc.Candle(c_datetime=c_datetime,
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
        symbol = dhs.get_symbol_by_ticker(ticker=symbol)
    if symbol.ticker == "ES":
        if timeframe == "r1h":
            weekday_open = {"hour": 9, "minute": 30, "second": 0}
            weekday_close = {"hour": 16, "minute": 14, "second": 0}
            # Market opens for the week Sunday evening
            week_open = {"day": 0, "hour": 9, "minute": 30, "second": 0}
            # Market closes for the week Friday evening
            week_close = {"day": 4, "hour": 16, "minute": 14, "second": 0}
            trading_hours = "rth"
        else:
            weekday_open = {"hour": 18, "minute": 0, "second": 0}
            weekday_close = {"hour": 16, "minute": 59, "second": 0}
            # Market opens for the week Sunday evening
            week_open = {"day": 6, "hour": 18, "minute": 0, "second": 0}
            # Market closes for the week Friday evening
            week_close = {"day": 4, "hour": 16, "minute": 59, "second": 0}
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
        include = True
        # TODO revisit this now that I've got Symbol.market_is_open() avail
        #      it may be possible to integrate that method to simplify?
        #      In fact, also integrating next_candle_start() might turn this
        #      into about a 3 line loop...?
        # Check if this candle falls in the weekday closure window
        this_weekday_close = this.replace(hour=weekday_close["hour"],
                                          minute=weekday_close["minute"],
                                          second=weekday_close["second"],
                                          )
        this_weekday_open = this.replace(hour=weekday_open["hour"],
                                         minute=weekday_open["minute"],
                                         second=weekday_open["second"],
                                         )
        # Same vs next day open dictates test logic
        # if opening time is later we reopen same day, exclude between
        if this_weekday_open > this_weekday_close:
            if (this > this_weekday_close) and (this < this_weekday_open):
                include = False
        # Otherwise we reopen next day, exclude outside
        else:
            if (this > this_weekday_close) or (this < this_weekday_open):
                include = False
        # Check if this candle falls in the weekend closure window
        this_week_close = this.replace(hour=week_close["hour"],
                                       minute=week_close["minute"],
                                       second=week_close["second"],
                                       )
        days_delta = week_close["day"] - this_week_close.weekday()
        this_week_close = this_week_close + timedelta(days=days_delta)
        this_week_open = this.replace(hour=week_open["hour"],
                                      minute=week_open["minute"],
                                      second=week_open["second"],
                                      )
        days_delta = week_open["day"] - this_week_open.weekday()
        this_week_open = this_week_open + timedelta(days=days_delta)
        if this_week_open < this_week_close:
            this_week_open = this_week_open + timedelta(days=7)
        # Range has been determined, test it
        if (this > this_week_close) and (this < this_week_open):
            include = False
        if include:
            result_std.append(this)
        this = this + adder

    # Remove any candles falling inside of non-standard market closures
    start_epoch = dt_to_epoch(start_dt)
    end_epoch = dt_to_epoch(end_dt)
    all_events = dhs.get_events(start_epoch=start_epoch,
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
        symbol = dhs.get_symbol_by_ticker(ticker=symbol)
    print("Remediating Candle Gaps")
    print(f"Auto fixing obvious zero volume gaps: {fix_obvious}")
    print(f"Prompting before fixing unclear candles: {prompt}")
    print(f"Dry Run Mode (only simulate changes): {dry_run}")
    review = dhs.review_candles(timeframe='1m',
                                symbol=symbol,
                                check_integrity=True,
                                return_detail=True,
                                )
    candles = review["missing_candles_by_date"]
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
        print(f"{d} - {len(candles[d])} missing candles")
        for c in candles[d]:
            c_dt = dt_as_dt(f"{d} {c}")
            pre_start = dt_to_epoch(c_dt - (delta * 5))
            pre_end = dt_to_epoch(c_dt - delta)
            post_start = dt_to_epoch(c_dt + delta)
            post_end = dt_to_epoch(c_dt + (delta * 5))
            pre_cans = dhs.get_candles(timeframe=timeframe,
                                       symbol=symbol,
                                       start_epoch=pre_start,
                                       end_epoch=pre_end,
                                       )
            post_cans = dhs.get_candles(timeframe=timeframe,
                                        symbol=symbol,
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
            # all 10 adjacent candles exist, and the avg vol of adjacent
            # candles is low
            if len(adj_vols) > 0:
                avg_vol = sum(adj_vols) / len(adj_vols)
            else:
                avg_vol = 0
            if ((c_dt.hour < 9 or c_dt.hour > 15) and len(pre_cans) == 5
                    and len(post_cans) == 5 and 0 < avg_vol < 100):
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
        print(f"{len(unclear_fix)} Unclear candles require human review\n")
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
                c = dhc.Candle(c_datetime=this_dt,
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
    new_candles = dhs.get_candles(start_epoch=dt_to_epoch(start_dt),
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
                           diff_threshold: float = 0.5,
                           start_dt=None,
                           end_dt=None,
                           ):
    """Check stored candles against a CSV source file, primarily used to
    confirm calculated higher timeframes against data provider equivalents
    where available to sanity check calculation process"""
    # Determine start/end datetimes from stored candles if not provided
    if start_dt is None or end_dt is None:
        review = dhs.review_candles(timeframe=timeframe,
                                    symbol=symbol,
                                    check_integrity=False,
                                    return_detail=False,
                                    )
        if start_dt is None:
            start_dt = review["overview"]["earliest_dt"]
        if end_dt is None:
            end_dt = review["overview"]["latest_dt"]
    print(f"Comparing {symbol} {timeframe} candles in {start_dt} to {end_dt}")
    # Get candles from storage
    start_epoch = dt_to_epoch(start_dt)
    end_epoch = dt_to_epoch(end_dt)
    stored_cans = dhs.get_candles(start_epoch=start_epoch,
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
                    if abs(sc.c_open - cc.c_open) <= diff_threshold:
                        c_minor_diffs["c_open"] = {"stored": sc.c_open,
                                                   "csv": cc.c_open}
                    else:
                        c_diffs["c_open"] = {"stored": sc.c_open,
                                             "csv": cc.c_open}
                if sc.c_high != cc.c_high:
                    if abs(sc.c_high - cc.c_high) <= diff_threshold:
                        c_minor_diffs["c_high"] = {"stored": sc.c_high,
                                                   "csv": cc.c_high}
                    else:
                        c_diffs["c_high"] = {"stored": sc.c_high,
                                             "csv": cc.c_high}
                if sc.c_low != cc.c_low:
                    if abs(sc.c_low - cc.c_low) <= diff_threshold:
                        c_minor_diffs["c_low"] = {"stored": sc.c_low,
                                                  "csv": cc.c_low}
                    else:
                        c_diffs["c_low"] = {"stored": sc.c_low,
                                            "csv": cc.c_low}
                if sc.c_close != cc.c_close:
                    if abs(sc.c_close - cc.c_close) <= diff_threshold:
                        c_minor_diffs["c_close"] = {"stored": sc.c_close,
                                                    "csv": cc.c_close}
                    else:
                        c_diffs["c_close"] = {"stored": sc.c_close,
                                              "csv": cc.c_close}
                if sc.c_volume != cc.c_volume:
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
    count_csv_plus_zeros = count_csv_candles + count_zerovol_storage
    if count_stored_candles != count_csv_plus_zeros:
        all_equal = False
    counts = {"stored_dupes": count_stored_dupes,
              "csv_dupes": count_csv_dupes,
              "stored_candles": count_stored_candles,
              "csv_candles": count_csv_candles,
              "missing_from_storage": count_missing_storage,
              "extras_in_storage": count_extras_storage,
              "zero_vol_extras_in_storage": count_zerovol_storage,
              "diffs_from_csv": count_diffs,
              "minor_diffs_from_csv": count_minor_diffs,
              }
    result = {"all_equal": all_equal,
              "counts": counts,
              "differences": diffs,
              "missing_from_storage": missing,
              "extras_in_storage": extras,
              "zero_volume_storage_only": zeros,
              }

    return result


def summarize_candles(timeframe: str,
                      symbol: str = "ES",
                      candles: list = None,
                      ):
    if not isinstance(candles, list):
        raise TypeError(f"candles must be a list, not {type(candles)}")
    times = set()
    mins = set()
    hours = set()
    dates = set()
    datetimes = set()
    dows = set()
    for c in candles:
        this_dt = dt_as_dt(c.c_datetime)
        times.add(str(this_dt.time()))
        minute = str(this_dt.minute)
        if len(minute) < 2:
            minute = "0" + minute
        mins.add(minute)
        hour = str(this_dt.hour)
        if len(hour) < 2:
            hour = "0" + hour
        hours.add(hour)
        dates.add(str(this_dt.date()))
        datetimes.add(str(this_dt))
        dows.add(f"{this_dt.weekday()}{this_dt.strftime('%A')}")
    mins_list = sorted(list(mins))
    hours_list = sorted(list(hours))
    times_list = sorted(list(times))
    dates_list = sorted(list(dates))
    dows_list_nums = sorted(list(dows))
    dows_list = []
    for d in dows_list_nums:
        dows_list.append(d[1:])
    summary_data = {"minutes": mins_list,
                    "hours": hours_list,
                    "times": times_list,
                    "dates": dates_list,
                    "Days of Week": dows_list,
                    }
    summary_expected = {}
    if timeframe == '5m':
        minutes = ['00', '05', '10', '15', '20', '25', '30', '35', '40', '45',
                   '50', '55']
        hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
                 '10', '11', '12', '13', '14', '15', '16', '18', '19', '20',
                 '21', '22', '23']
        times = ['00:00:00', '00:05:00', '00:10:00', '00:15:00', '00:20:00',
                 '00:25:00', '00:30:00', '00:35:00', '00:40:00', '00:45:00',
                 '00:50:00', '00:55:00', '01:00:00', '01:05:00', '01:10:00',
                 '01:15:00', '01:20:00', '01:25:00', '01:30:00', '01:35:00',
                 '01:40:00', '01:45:00', '01:50:00', '01:55:00', '02:00:00',
                 '02:05:00', '02:10:00', '02:15:00', '02:20:00', '02:25:00',
                 '02:30:00', '02:35:00', '02:40:00', '02:45:00', '02:50:00',
                 '02:55:00', '03:00:00', '03:05:00', '03:10:00', '03:15:00',
                 '03:20:00', '03:25:00', '03:30:00', '03:35:00', '03:40:00',
                 '03:45:00', '03:50:00', '03:55:00', '04:00:00', '04:05:00',
                 '04:10:00', '04:15:00', '04:20:00', '04:25:00', '04:30:00',
                 '04:35:00', '04:40:00', '04:45:00', '04:50:00', '04:55:00',
                 '05:00:00', '05:05:00', '05:10:00', '05:15:00', '05:20:00',
                 '05:25:00', '05:30:00', '05:35:00', '05:40:00', '05:45:00',
                 '05:50:00', '05:55:00', '06:00:00', '06:05:00', '06:10:00',
                 '06:15:00', '06:20:00', '06:25:00', '06:30:00', '06:35:00',
                 '06:40:00', '06:45:00', '06:50:00', '06:55:00', '07:00:00',
                 '07:05:00', '07:10:00', '07:15:00', '07:20:00', '07:25:00',
                 '07:30:00', '07:35:00', '07:40:00', '07:45:00', '07:50:00',
                 '07:55:00', '08:00:00', '08:05:00', '08:10:00', '08:15:00',
                 '08:20:00', '08:25:00', '08:30:00', '08:35:00', '08:40:00',
                 '08:45:00', '08:50:00', '08:55:00', '09:00:00', '09:05:00',
                 '09:10:00', '09:15:00', '09:20:00', '09:25:00', '09:30:00',
                 '09:35:00', '09:40:00', '09:45:00', '09:50:00', '09:55:00',
                 '10:00:00', '10:05:00', '10:10:00', '10:15:00', '10:20:00',
                 '10:25:00', '10:30:00', '10:35:00', '10:40:00', '10:45:00',
                 '10:50:00', '10:55:00', '11:00:00', '11:05:00', '11:10:00',
                 '11:15:00', '11:20:00', '11:25:00', '11:30:00', '11:35:00',
                 '11:40:00', '11:45:00', '11:50:00', '11:55:00', '12:00:00',
                 '12:05:00', '12:10:00', '12:15:00', '12:20:00', '12:25:00',
                 '12:30:00', '12:35:00', '12:40:00', '12:45:00', '12:50:00',
                 '12:55:00', '13:00:00', '13:05:00', '13:10:00', '13:15:00',
                 '13:20:00', '13:25:00', '13:30:00', '13:35:00', '13:40:00',
                 '13:45:00', '13:50:00', '13:55:00', '14:00:00', '14:05:00',
                 '14:10:00', '14:15:00', '14:20:00', '14:25:00', '14:30:00',
                 '14:35:00', '14:40:00', '14:45:00', '14:50:00', '14:55:00',
                 '15:00:00', '15:05:00', '15:10:00', '15:15:00', '15:20:00',
                 '15:25:00', '15:30:00', '15:35:00', '15:40:00', '15:45:00',
                 '15:50:00', '15:55:00', '16:00:00', '16:05:00', '16:10:00',
                 '16:15:00', '16:20:00', '16:25:00', '16:30:00', '16:35:00',
                 '16:40:00', '16:45:00', '16:50:00', '16:55:00', '18:00:00',
                 '18:05:00', '18:10:00', '18:15:00', '18:20:00', '18:25:00',
                 '18:30:00', '18:35:00', '18:40:00', '18:45:00', '18:50:00',
                 '18:55:00', '19:00:00', '19:05:00', '19:10:00', '19:15:00',
                 '19:20:00', '19:25:00', '19:30:00', '19:35:00', '19:40:00',
                 '19:45:00', '19:50:00', '19:55:00', '20:00:00', '20:05:00',
                 '20:10:00', '20:15:00', '20:20:00', '20:25:00', '20:30:00',
                 '20:35:00', '20:40:00', '20:45:00', '20:50:00', '20:55:00',
                 '21:00:00', '21:05:00', '21:10:00', '21:15:00', '21:20:00',
                 '21:25:00', '21:30:00', '21:35:00', '21:40:00', '21:45:00',
                 '21:50:00', '21:55:00', '22:00:00', '22:05:00', '22:10:00',
                 '22:15:00', '22:20:00', '22:25:00', '22:30:00', '22:35:00',
                 '22:40:00', '22:45:00', '22:50:00', '22:55:00', '23:00:00',
                 '23:05:00', '23:10:00', '23:15:00', '23:20:00', '23:25:00',
                 '23:30:00', '23:35:00', '23:40:00', '23:45:00', '23:50:00',
                 '23:55:00']
    elif timeframe == '15m':
        minutes = ['00', '15', '30', '45']
        hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
                 '10', '11', '12', '13', '14', '15', '16', '18', '19', '20',
                 '21', '22', '23']
        times = ['00:00:00', '00:15:00', '00:30:00', '00:45:00',
                 '01:00:00', '01:15:00', '01:30:00', '01:45:00',
                 '02:00:00', '02:15:00', '02:30:00', '02:45:00',
                 '03:00:00', '03:15:00', '03:30:00', '03:45:00',
                 '04:00:00', '04:15:00', '04:30:00', '04:45:00',
                 '05:00:00', '05:15:00', '05:30:00', '05:45:00',
                 '06:00:00', '06:15:00', '06:30:00', '06:45:00',
                 '07:00:00', '07:15:00', '07:30:00', '07:45:00',
                 '08:00:00', '08:15:00', '08:30:00', '08:45:00',
                 '09:00:00', '09:15:00', '09:30:00', '09:45:00',
                 '10:00:00', '10:15:00', '10:30:00', '10:45:00',
                 '11:00:00', '11:15:00', '11:30:00', '11:45:00',
                 '12:00:00', '12:15:00', '12:30:00', '12:45:00',
                 '13:00:00', '13:15:00', '13:30:00', '13:45:00',
                 '14:00:00', '14:15:00', '14:30:00', '14:45:00',
                 '15:00:00', '15:15:00', '15:30:00', '15:45:00',
                 '16:00:00', '16:15:00', '16:30:00', '16:45:00',
                 '18:00:00', '18:15:00', '18:30:00', '18:45:00',
                 '19:00:00', '19:15:00', '19:30:00', '19:45:00',
                 '20:00:00', '20:15:00', '20:30:00', '20:45:00',
                 '21:00:00', '21:15:00', '21:30:00', '21:45:00',
                 '22:00:00', '22:15:00', '22:30:00', '22:45:00',
                 '23:00:00', '23:15:00', '23:30:00', '23:45:00',
                 ]
    elif timeframe == 'r1h':
        minutes = ['30']
        hours = ['09', '10', '11', '12', '13', '14', '15']
        times = ['09:30:00', '10:30:00', '11:30:00', '12:30:00', '13:30:00',
                 '14:30:00', '15:30:00']
    elif timeframe == 'e1h':
        minutes = ['00']
        hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
                 '10', '11', '12', '13', '14', '15', '16', '18', '19', '20',
                 '21', '22', '23']
        times = ['00:00:00', '01:00:00', '02:00:00', '03:00:00', '04:00:00',
                 '05:00:00', '06:00:00', '07:00:00', '08:00:00', '09:00:00',
                 '10:00:00', '11:00:00', '12:00:00', '13:00:00', '14:00:00',
                 '15:00:00', '16:00:00', '18:00:00', '19:00:00', '20:00:00',
                 '21:00:00', '22:00:00', '23:00:00']
    else:
        summary_expected = None
    if summary_expected is not None:
        summary_expected = {'minutes': minutes,
                            'hours': hours,
                            'times': times,
                            }

    return {"summary_data": summary_data,
            "summary_expected": summary_expected,
            }


def test_basics():
    """runs a few basics tests, mostly used during initial development
       to confirm functionality as desired"""
    # TODO consider converting these into unit tests some day
    # https://docs.python.org/3/library/unittest.html

    # TODO in lieu of real unit tests, start a test_results empty list and
    #      record a quick oneliner for each easily confirmable test as it
    #      finishes, something like "OK - Trade() Storage and retrieval"
    #      then print them all at the end.  For non-easily-confirmed could
    #      add a note like "UNKNOWN - Visual confirm needed for Trade.pretty()
    # Test datatime functions
    ts = "2024-01-01 12:30:00"
    print(f"Starting with string: {ts} {type(ts)}")
    td = dt_as_dt(ts)
    print(f"Converted to datetime: {td} {type(td)}")
    tds = dt_as_str(td)
    print(f"Converted back to string: {tds} {type(tds)}")
    tse = dt_to_epoch(ts)
    print(f"String converted to epoch: {tse} {type(tse)}")
    tde = dt_to_epoch(td)
    print(f"Datetime converted to epoch: {tde} {type(tde)}")
    t = dt_from_epoch(tde)
    print(f"Epoch converted back to datetime: {t} {type(t)}")

    # Test expected candle range generation
    print("\nExpected candles spanning weekend closure, 1m")
    weekend_gap = expected_candle_datetimes(start_dt="2024-11-29 16:52:00",
                                            end_dt="2024-12-01 18:07:00",
                                            symbol="ES",
                                            timeframe="1m",
                                            )
    for c in weekend_gap:
        print(dt_as_str(c))
    print("\nExpected candles spanning weekday closure, 5m")
    weekday_gap = expected_candle_datetimes(start_dt="2024-12-02 16:30:00",
                                            end_dt="2024-12-02 18:40:00",
                                            symbol="ES",
                                            timeframe="5m",
                                            )
    for c in weekday_gap:
        print(dt_as_str(c))
    print("\nExpected candles spanning weekday closure, 15m")
    weekday_gap = expected_candle_datetimes(start_dt="2024-12-02 16:30:00",
                                            end_dt="2024-12-02 18:40:00",
                                            symbol="ES",
                                            timeframe="15m",
                                            )
    for c in weekday_gap:
        print(dt_as_str(c))
    print("\nExpected candles spanning weekday closure, e1h")
    weekday_gap = expected_candle_datetimes(start_dt="2024-12-02 12:05:00",
                                            end_dt="2024-12-03 4:05:00",
                                            symbol="ES",
                                            timeframe="e1h",
                                            )
    for c in weekday_gap:
        print(dt_as_str(c))
    print("\nExpected candles spanning weekday and weekend closures, r1h")
    weekday_gap = expected_candle_datetimes(start_dt="2024-11-15 11:05:00",
                                            end_dt="2024-11-18 12:05:00",
                                            symbol="ES",
                                            timeframe="r1h",
                                            )
    for c in weekday_gap:
        print(dt_as_str(c))
    print("\nExpected candles spanning 2 days with early holiday closes, r1h")
    print("This should show r1h candles from 9:30-13:00 only for both days")
    weekday_gap = expected_candle_datetimes(start_dt="2024-07-03 09:00:00",
                                            end_dt="2024-07-05 00:00:00",
                                            symbol="ES",
                                            timeframe="r1h",
                                            )
    for c in weekday_gap:
        print(dt_as_str(c))

    # Test timeframe validation functions
    print("Testing valid timeframe, should return True")
    print(valid_timeframe('1m'))
    print("Testing an invalid timeframe, should return an error messge")
    print(valid_timeframe('20m', exit=False))

    # Optionally run candle gap mitigation
    remediate = ""
    while remediate not in ["Y", "y", "N", "n"]:
        remediate = input("\nFinished testing.  Would you like to dry-run 1m "
                          "candle remediation to simulate filling candle gaps "
                          "(Y/N)?")
        if remediate in ["Y", "y"]:
            remed = remediate_candle_gaps(timeframe="1m",
                                          symbol="ES",
                                          prompt=False,
                                          fix_obvious=True,
                                          fix_unclear=True,
                                          dry_run=True,
                                          )
            print("==========================================================")
            print("==========================================================")
            print("==========================================================")
            print(remed["overview"])


if __name__ == '__main__':
    test_basics()
