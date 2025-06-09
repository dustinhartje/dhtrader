import json
from datetime import timedelta, datetime as dt
from copy import deepcopy
import logging
import dhutil as dhu
import dhstore as dhs
import dhcharts as dhc

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


class Trade():
    """Represents a single trade that could have been made.

    Attributes:
        open_dt (str): datetime trade was initiated
        open_epoch (int): epoch time of open_dt for sorting (autocalculated)
        direction (str): 'long' or 'short'
        timeframe (str): timeframe of the chart the Trade was created from
        trading_hours (str): regular trading hours (rth)
            or extended/globex (eth) of the underlying chart
        entry_price (float): price at which trade was initiated
        stop_target (float): price at which trade would auto exit at loss
        prof_target (float): price at which trade would auto exit at profit
        close_dt (str): datetime trade was closed
        created_dt (str): datetime this object was created
        high_price (float): highest price seen during trade
        low_price (float): lowest price seen during trade
        exit_price (float): price at which trade was closed
        stop_target (float): Target price at which trade should exit at a loss
        prof_target (float): Target price at which trade should exit at a gain
        stop_ticks (float): number of ticks to reach stop_target from entry,
            primarily used as trade rules identification during analysis
        prof_ticks (float): number of ticks to reach prof_target from entry,
            primarily used as trade rules identification during analysis
        offset_ticks (float): number of ticks away from target price that
            this trade made entry, primarily used as trade rules
            identification during analysis
        symbol (str): ticker being traded
        is_open (bool): True if trade has not yet been closed via .close()
        profitable (bool): True if trade made money
        name (str): Label for identifying groups of similar trades
            such as those run by the same backtester
        version (str): Version of trade (for future use)
        ts_id (str): unique id of associated TradeSeries this was created by
        bt_id (str): unique id of associated Backtest this was created by
    """
    def __init__(self,
                 open_dt: str,
                 direction: str,
                 timeframe: str,
                 trading_hours: str,
                 entry_price: float,
                 close_dt: str = None,
                 created_dt: str = None,
                 open_epoch: int = None,
                 high_price: float = None,
                 low_price: float = None,
                 exit_price: float = None,
                 stop_target: float = None,
                 prof_target: float = None,
                 stop_ticks: int = None,
                 prof_ticks: int = None,
                 offset_ticks: int = 0,
                 symbol="ES",
                 is_open: bool = True,
                 profitable: bool = None,
                 name: str = None,
                 version: str = "1.0.0",
                 ts_id: str = None,
                 bt_id: str = None,
                 ):

        # Passable attributes
        self.open_dt = open_dt
        self.close_dt = close_dt
        if created_dt is None:
            self.created_dt = dhu.dt_as_str(dt.now())
        else:
            self.created_dt = created_dt
        if direction in ['long', 'short']:
            self.direction = direction
        else:
            raise ValueError(f"invalid value for direction of {direction} "
                             "received, must be in ['long', 'short'] only."
                             )
        if dhu.valid_timeframe(timeframe):
            self.timeframe = timeframe
        if dhu.valid_trading_hours(trading_hours):
            self.trading_hours = trading_hours
        self.entry_price = entry_price
        self.stop_target = stop_target
        self.prof_target = prof_target
        if high_price is None:
            self.high_price = entry_price
        else:
            self.high_price = high_price
        if low_price is None:
            self.low_price = entry_price
        else:
            self.low_price = low_price
        self.exit_price = exit_price
        self.stop_ticks = stop_ticks
        self.prof_ticks = prof_ticks
        self.offset_ticks = offset_ticks
        if isinstance(symbol, str):
            self.symbol = dhs.get_symbol_by_ticker(ticker=symbol)
        else:
            self.symbol = symbol
        self.is_open = is_open
        self.profitable = profitable
        self.name = name
        self.version = version
        self.ts_id = ts_id
        self.bt_id = bt_id

        # Calculated attributes
        if self.direction == "long":
            self.flipper = 1
        elif self.direction == "short":
            self.flipper = -1
        else:
            self.flipper = 0  # If this happens there's a bug somewhere
        self.open_epoch = dhu.dt_to_epoch(self.open_dt)
        # Mark Trades that open in the first minute of their timeframe bar
        # These can be difficult to trade quickly against changing indicators
        if self.timeframe == "5m":
            start_mins = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        elif self.timeframe == "15m":
            start_mins = [0, 15, 30, 45]
        elif self.timeframe == "e1h":
            start_mins = [0]
        elif self.timeframe == "r1h":
            start_mins = [30]
        else:
            start_mins = None
            self.first_min_open = False
        if start_mins is not None:
            if dhu.dt_as_dt(self.open_dt).minute in start_mins:
                self.first_min_open = True
            else:
                self.first_min_open = False
        # Calc or confirm ticks and targets to prevent later innaccuracies
        if self.prof_ticks is None:
            if self.prof_target is None:
                # If neither provided we can't continue
                raise ValueError("Must provide either prof_ticks or "
                                 "prof_target (or both).  Neither was passed")
            else:
                # Need to calculate prof_ticks
                self.prof_ticks = ((self.prof_target - self.entry_price)
                                   * self.flipper
                                   / self.symbol.tick_size)
        else:
            if self.prof_target is None:
                # Need to calculate prof_target
                self.prof_target = self.entry_price + ((self.prof_ticks
                                                       * self.flipper)
                                                       * self.symbol.tick_size)
            else:
                # Both provided, make sure they math out correctly
                pt = self.entry_price + ((self.prof_ticks
                                         * self.flipper)
                                         * self.symbol.tick_size)
                if not pt == self.prof_target:
                    msg = (f"Provided prof_target does not match prof_ticks "
                           "calculation against entry_price.  These numbers "
                           "cannot be trusted for later calculations.  "
                           f"entry_price={self.entry_price} "
                           f"prof_ticks={self.prof_ticks} "
                           f"direction={self.direction} "
                           f"should get prof_target of {pt} but we got "
                           f"prof_target={self.prof_target} instead."
                           )
                    raise ValueError(msg)
        if self.stop_ticks is None:
            if self.stop_target is None:
                # If neither provided we can't continue
                raise ValueError("Must provide either stop_ticks or "
                                 "stop_target (or both).  Neither was passed")
            else:
                # Need to calculate stop_ticks
                self.stop_ticks = ((self.entry_price - self.stop_target)
                                   * self.flipper
                                   / self.symbol.tick_size)
        else:
            if self.stop_target is None:
                # Need to calculate stop_target
                self.stop_target = self.entry_price - ((self.stop_ticks
                                                       * self.flipper)
                                                       * self.symbol.tick_size)
            else:
                # Both provided, make sure they math out correctly
                st = self.entry_price - ((self.stop_ticks
                                         * self.flipper)
                                         * self.symbol.tick_size)
                if not st == self.stop_target:
                    msg = (f"Provided stop_target does not match stop_ticks "
                           "calculation against entry_price.  These numbers "
                           "cannot be trusted for later calculations.  "
                           f"entry_price={self.entry_price} "
                           f"stop_ticks={self.stop_ticks} "
                           f"direction={self.direction} "
                           f"should get stop_target of {st} but we got "
                           f"stop_target={self.stop_target} instead."
                           )
                    raise ValueError(msg)
        # Make sure ticks end up integers, no decimals allowed
        if self.prof_ticks == int(self.prof_ticks):
            self.prof_ticks = int(self.prof_ticks)
        else:
            raise ValueError("prof_ticks must be an integer but we got "
                             f"{self.prof_ticks}")
        if self.stop_ticks == int(self.stop_ticks):
            self.stop_ticks = int(self.stop_ticks)
        else:
            raise ValueError("stop_ticks must be an integer but we got "
                             f"{self.stop_ticks}")

        # If closing attributes were passed, run close() to ensure all
        # related attributes that may not have been passed in are finalized.
        if self.exit_price is not None:
            self.close(price=self.exit_price,
                       dt=self.close_dt,
                       )

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return (self.open_dt == other.open_dt
                and self.timeframe == other.timeframe
                and self.trading_hours == other.trading_hours
                and self.direction == other.direction
                and self.entry_price == other.entry_price
                and self.high_price == other.high_price
                and self.low_price == other.low_price
                and self.stop_target == other.stop_target
                and self.prof_target == other.prof_target
                and self.close_dt == other.close_dt
                and self.created_dt == other.created_dt
                and self.open_epoch == other.open_epoch
                and self.exit_price == other.exit_price
                and self.stop_ticks == other.stop_ticks
                and self.prof_ticks == other.prof_ticks
                and self.offset_ticks == other.offset_ticks
                and self.symbol == other.symbol
                and self.is_open == other.is_open
                and self.profitable == other.profitable
                and self.name == other.name
                and self.version == other.version
                and self.ts_id == other.ts_id
                and self.bt_id == other.bt_id
                )

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_json(self):
        """returns a json version of this Trade object while normalizing
        custom types for json compatibility (i.e. datetime to string)"""
        # Make sure dates are strings not datetimes
        working = deepcopy(self.__dict__)
        if self.open_dt is not None:
            working["open_dt"] = dhu.dt_as_str(self.open_dt)
        if self.close_dt is not None:
            working["close_dt"] = dhu.dt_as_str(self.close_dt)
        if self.created_dt is not None:
            working["created_dt"] = dhu.dt_as_str(self.created_dt)
        # Change symbol to string of ticker
        working["symbol"] = working["symbol"].ticker

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
        return json.dumps(self.to_clean_dict(), indent=4)

    def brief(self):
        """Return a single line summary string of this Trade's vitals"""
        return (f"{self.open_dt} - {self.close_dt} | "
                f"{dhu.dt_as_dt(self.open_dt).strftime('%A')} | "
                f"entry={self.entry_price} | exit={self.exit_price} | "
                f"profitable={self.profitable}")

    def store(self):
        """Store this Trade in central storage"""
        return dhs.store_trades(trades=[self])

    def delete_from_storage(self):
        """Delete this Trade from central storage if it exists"""
        return dhs.delete_one_trade(symbol=self.symbol.ticker,
                                    open_dt=self.open_dt,
                                    ts_id=self.ts_id,
                                    )

    def candle_update(self,
                      candle,
                      ):
        """Incorporates a new candle into an open Trade, updating low and
        high price attributes and checking for trade closing triggers."""
        if not self.is_open:
            raise Exception("Cannot run update() on a closed Trade, this "
                            "would break reality.")
        if not isinstance(candle, dhc.Candle):
            raise TypeError(f"candle {candle} must be a dhcharts.Candle obj, "
                            f"we got a {type(candle)} instead")
        # Set short var names for calcs in this method
        tick = self.symbol.tick_size
        close_price = None
        # Calculate updates based on direction
        if self.direction == "long":
            # Close on stop to be conservative even if profit_target hit
            if candle.c_low <= self.stop_target:
                close_price = self.stop_target
                self.low_price = min(self.low_price, self.stop_target)
            else:
                self.low_price = min(self.low_price, candle.c_low)
            # Now if we passed the profit target we can close at a gain
            # Exact tag doesn't guarantee fill so we must see a tick past
            if dhu.dt_as_dt(self.open_dt) == dhu.dt_as_dt(candle.c_datetime):
                # if we're still in the entry minute, we need to compare the
                # closing price to be sure it tagged profit after the entry
                if candle.c_close >= (self.prof_target + tick):
                    if close_price is None:
                        close_price = self.prof_target
                    self.high_price = max(self.high_price, self.prof_target)
                else:
                    self.high_price = max(self.high_price, candle.c_close)
            else:
                # Otherwise we compare against the candle high
                if candle.c_high >= (self.prof_target + tick):
                    if close_price is None:
                        close_price = self.prof_target
                    # Even if we closed on a stop, assume the worst case that
                    # we got to profit_target but did not get filled before
                    # falling to stop_target.  This has the most conservative
                    # /worst case effect on drawdown impacts.
                    self.high_price = max(self.high_price, self.prof_target)
                else:
                    self.high_price = max(self.high_price, candle.c_high)
        elif self.direction == "short":
            # Close on stop to be conservative even if profit_target hit
            if candle.c_high >= self.stop_target:
                close_price = self.stop_target
                self.high_price = max(self.high_price, self.stop_target)
            else:
                self.high_price = max(self.high_price, candle.c_high)
            # Now if we passed the profit target we can close at a gain
            # Exact tag doesn't guarantee fill so we must see a tick past
            if dhu.dt_as_dt(self.open_dt) == dhu.dt_as_dt(candle.c_datetime):
                # if we're still in the entry minute, we need to compare the
                # closing price to be sure it tagged profit after the entry
                if candle.c_close <= (self.prof_target - tick):
                    if close_price is None:
                        close_price = self.prof_target
                    self.low_price = min(self.low_price, self.prof_target)
                else:
                    self.low_price = min(self.low_price, candle.c_close)
            else:
                # Otherwise we compare against the candle low
                if candle.c_low <= (self.prof_target - tick):
                    if close_price is None:
                        close_price = self.prof_target
                    # Even if we closed on a stop_target, assume worst case
                    # that we got to profit_target but did not get filled
                    # before falling to stop_target.  This has the most
                    # conservative/worst case effect on drawdown impacts.
                    self.low_price = min(self.low_price, self.prof_target)
                else:
                    self.low_price = min(self.low_price, candle.c_low)

        # Close the trade if we hit a target
        if close_price is not None:
            self.close(close_price, candle.c_datetime)
            return {"closed": True}
        else:
            return {"closed": False}

    def drawdown_impact(self,
                        drawdown_open: float,
                        drawdown_limit: float,
                        contracts: int,
                        contract_value: float,
                        contract_fee: float
                        ):
        """Calculates and returns resulting values of drawdown range and ending
        values seen during the trade for given series specific inputs.
        Primarily used by TradeSeries to loop through all Trade
        objects checking for drawdown liquidations"""
        # Until I find a reasonable need, assume we don't care about drawdowns
        # until the trade is closed.  Returning results could cause mistakes.
        if self.is_open:
            return None
        fees = contracts * contract_fee
        # Determine max gain and loss prices seen during trade
        if self.direction == "long":
            max_gain = self.high_price - self.entry_price
            max_loss = self.entry_price - self.low_price
        elif self.direction == "short":
            max_gain = self.entry_price - self.low_price
            max_loss = self.high_price - self.entry_price
        # Use these to calculate highest and lowest drawdown distances seen
        cmult = contracts * contract_value
        drawdown_high = drawdown_open + (max_gain * cmult) - fees
        drawdown_low = drawdown_open - (max_loss * cmult) - fees
        if drawdown_high > drawdown_limit:
            drawdown_trail_increase = drawdown_high - drawdown_limit
        else:
            drawdown_trail_increase = 0
        # Calculate the closing drawdown level i.e. where it will be after
        # the trade is finished.
        drawdown_close = (((self.exit_price - self.entry_price)
                          * self.flipper
                          * cmult)
                          + drawdown_open
                          - (contracts * contract_fee))
        # Closing drawdown cannot exceed drawdown limit on account
        drawdown_close = min(drawdown_close, drawdown_limit)
        # Round results because float math anomalies create trailing decimals
        drawdown_open = round(drawdown_open, 2)
        drawdown_close = round(drawdown_close, 2)
        drawdown_high = round(drawdown_high, 2)
        drawdown_low = round(drawdown_low, 2)
        drawdown_trail_increase = round(drawdown_trail_increase, 2)

        return {"drawdown_open": drawdown_open,
                "drawdown_close": drawdown_close,
                "drawdown_trail_increase": drawdown_trail_increase,
                "drawdown_high": drawdown_high,
                "drawdown_low": drawdown_low,
                }

    def balance_impact(self,
                       balance_open: float,
                       contracts: int,
                       contract_value: float,
                       contract_fee: float,
                       ):
        """Calculates and returns ending balance and gain/loss values for the
        trade given series specific inputs.  Primarily used by TradeSeries to
        loop through all Trade objects to evaluate ending gains and balances.
        """
        # Cannot return balance impact until trade is closed
        if self.is_open:
            return None
        fees = contracts * contract_fee
        # Determine max gain and loss prices seen during trade
        if self.direction == "long":
            max_gain = self.high_price - self.entry_price
            max_loss = self.entry_price - self.low_price
        elif self.direction == "short":
            max_gain = self.entry_price - self.low_price
            max_loss = self.high_price - self.entry_price
        # Use these to calculate highest and lowest balances seen
        cmult = contracts * contract_value
        balance_high = balance_open + (max_gain * cmult) - fees
        balance_low = balance_open - (max_loss * cmult) - fees
        # Calculate gain/loss of the trade
        gain_loss = (((self.exit_price - self.entry_price)
                     * contracts * contract_value * self.flipper)
                     - fees)
        # Closing balance is just the difference from opening balance
        balance_close = round((balance_open + gain_loss), 2)
        # Round results because float math anomalies create trailing decimals
        balance_close = round(balance_close, 2)
        balance_high = round(balance_high, 2)
        balance_low = round(balance_low, 2)
        gain_loss = round(gain_loss, 2)

        return {"balance_open": balance_open,
                "balance_close": balance_close,
                "balance_high": balance_high,
                "balance_low": balance_low,
                "gain_loss": gain_loss,
                }

    def close(self,
              price: float,
              dt,
              ):
        """Closes the trade at the price given, finalizing related attributes.
        """
        self.is_open = False
        self.close_dt = dhu.dt_as_str(dt)
        self.exit_price = price
        if (self.exit_price - self.entry_price) * self.flipper > 0:
            self.profitable = True
        else:
            self.profitable = False


class TradeSeries():
    """Represents a series of trades presumably following the same rules

    Attributes:
        start_dt (str or datetime): Beginning of time period evaluated which
            may be earlier than the first trade datetime
        end_dt (str or datetime): End of time period evaluated
        timeframe (str): timeframe of underlying chart trades were evaluated
            from
        trading_hours (str): trading_hours of underlying chart trades were
            evaluated from
        symbol (str): The symbol or "ticker" being evaluated
        name (str): Human friendly label representing this object
        params_str (str): Represents Backtest or trade specific parameters
            to be passed in by creating objects and functions.  This is
            used to generate a unique ts_id in backtesting strategies so that
            multiple similar and related TradeSeries can be differentiated,
            stored, and retrieved effectively where needed even if created
            simultaneously with the same epoch value.
        ts_id (str): unique ID used for storage and analysis purposes,
            can be used to link related Trade() and Backtest() objects
        bt_id (str): bt_id of the Backtest() object that created this if any
        trades (list): list of trades in the series
    """
    def __init__(self,
                 start_dt,
                 end_dt,
                 timeframe: str,
                 trading_hours: str,
                 symbol="ES",
                 name: str = "",
                 params_str: str = "",
                 ts_id: str = None,
                 bt_id: str = None,
                 trades: list = None,
                 ):

        self.start_dt = dhu.dt_as_str(start_dt)
        self.end_dt = dhu.dt_as_str(end_dt)
        if dhu.valid_timeframe(timeframe):
            self.timeframe = timeframe
        if dhu.valid_trading_hours(trading_hours):
            self.trading_hours = trading_hours
        if isinstance(symbol, str):
            self.symbol = dhs.get_symbol_by_ticker(ticker=symbol)
        else:
            self.symbol = symbol
        self.name = name
        self.params_str = params_str
        if ts_id is None:
            self.ts_id = "_".join([self.name,
                                   self.params_str,
                                   ])
        else:
            self.ts_id = ts_id
        self.bt_id = bt_id
        if trades is None:
            self.trades = []
        else:
            self.trades = trades.copy()
            for t in self.trades:
                t.ts_id = self.ts_id
                t.bt_id = self.bt_id

    def __eq__(self, other):
        return (self.start_dt == other.start_dt
                and self.end_dt == other.end_dt
                and self.timeframe == other.timeframe
                and self.symbol == other.symbol
                and self.name == other.name
                and self.params_str == other.params_str
                and self.ts_id == other.ts_id
                and self.bt_id == other.bt_id
                and self.trades == other.trades
                )

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_json(self,
                suppress_trades: bool = True,
                ):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        working = deepcopy(self.__dict__)
        clean_trades = []
        if suppress_trades:
            num = len(self.trades)
            clean_trades = [f"{num} Trades suppressed for output sanity"]
        else:
            for t in self.trades:
                clean_trades.append(t.to_clean_dict())
        working["trades"] = clean_trades
        working["symbol"] = working["symbol"].ticker

        return json.dumps(working)

    def to_clean_dict(self,
                      suppress_trades: bool = True,
                      ):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json(suppress_trades=suppress_trades))

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def pretty(self,
               suppress_trades: bool = True,
               ):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        Optionally suppress_datapoints to reduce output size when not needed.
        """
        return json.dumps(self.to_clean_dict(suppress_trades=suppress_trades),
                          indent=4,
                          )

    def trades_brief(self):
        """Return a list of one line brief summaries of all Trades' vitals
        in this TradeSeries"""
        result = []
        for t in self.trades:
            result.append(t.brief())

        return result

    def update_bt_id(self, bt_id):
        """Update bt_id on this and any attached Trade objects.  Typicaly
        called by a Backtest when adding this object to it's list."""
        self.bt_id = bt_id
        for t in self.trades:
            t.bt_id = bt_id

    def store(self,
              store_trades: bool = False
              ):
        result = {"tradeseries": dhs.store_tradeseries(series=[self]),
                  "trades": [],
                  }
        if store_trades and self.trades is not None:
            for t in self.trades:
                result["trades"].append(t.store())

        return result

    def delete_from_storage(self,
                            include_trades: bool = True,
                            ):
        """Delete this object from central storage if it exists by ts_id.
        Optionally (default=True) also remove all Trade objects
        matching this object's ts_id."""
        result = {"tradeseries": None, "trades": []}

        result["tradeseries"] = dhs.delete_tradeseries(symbol=self.symbol,
                                                       field="ts_id",
                                                       value=self.ts_id,
                                                       )
        if include_trades:
            result["trades"] = dhs.delete_trades(symbol=self.symbol,
                                                 field="ts_id",
                                                 value=self.ts_id,
                                                 )

        return result

    def add_trade(self,
                  trade,
                  ):
        # Associate this Trade with this Backtest
        trade.ts_id = self.ts_id
        self.trades.append(trade)

    def sort_trades(self):
        self.trades.sort(key=lambda t: t.open_epoch)

    def get_trade_by_open_dt(self, dt):
        """Return the first trade found with open_dt matching the provided
        datetime, or None if nothing matches."""
        for t in self.trades:
            if dhu.dt_as_dt(t.open_dt) == dhu.dt_as_dt(dt):
                return t

        return None

    def count_trades(self):
        """Return the number of Trades currently attached"""
        if self.trades is None:
            return 0
        else:
            return len(self.trades)

    def restrict_dates(self,
                       new_start_dt: str,
                       new_end_dt: str,
                       update_storage: bool = False,
                       ):
        """Reduce the date range of the TradeSeries and remove any Trades that
        are no longer in bounds"""
        os = dhu.dt_as_dt(self.start_dt)
        oe = dhu.dt_as_dt(self.end_dt)
        ns = dhu.dt_as_dt(new_start_dt)
        ne = dhu.dt_as_dt(new_end_dt)
        ns_epoch = dhu.dt_to_epoch(new_start_dt)
        ne_epoch = dhu.dt_to_epoch(new_end_dt)
        # Ensure new dates don't expand the daterange, they should only reduce
        # or keep unchanged
        if ns < os:
            raise ValueError(f"new_start_dt {new_start_dt} cannot be earlier "
                             f"than the current self.start_dt {self.start_dt}")
        if ne > oe:
            raise ValueError(f"new_end_dt {new_end_dt} cannot be later "
                             f"than the current self.end_dt {self.end_dt}")
        # Update TradeSeries dates
        self.start_dt = new_start_dt
        self.end_dt = new_end_dt
        if update_storage:
            # Store (update) the TradeSeries
            self.store(store_trades=False)
            # Remove all trades from storage that are no longer in bounds
            remove_trades = [t for t in self.trades
                             if (t.open_epoch < ns_epoch
                                 or t.open_epoch > ne_epoch)
                             ]
            for t in remove_trades:
                t.delete_from_storage()
        # Remove trades from the current object's list as well by rebuilding
        # with only trades inside the new datetime range
        self.trades = [t for t in self.trades
                       if ns_epoch <= t.open_epoch <= ne_epoch
                       ]

    def balance_impact(self,
                       balance_open: float,
                       contracts: int,
                       contract_value: float,
                       contract_fee: float,
                       include_first_min: bool = True,
                       ):
        """Runs through current trades list, calculating changes to a running
        account balance starting with balance_open.  Returns high, low, and
        ending balance."""
        # Make sure trades are in order or results can't be trusted
        self.sort_trades()
        # All vars start at the opening balance provided
        balance_close = balance_open
        balance_high = balance_open
        balance_low = balance_open
        liquidated = False
        for t in self.trades:
            if not t.first_min_open or include_first_min:
                r = t.balance_impact(balance_open=balance_close,
                                     contracts=contracts,
                                     contract_value=contract_value,
                                     contract_fee=contract_fee,
                                     )
                balance_high = max(balance_high, r["balance_high"])
                balance_low = min(balance_low, r["balance_low"])
                balance_close = r["balance_close"]
        if balance_low <= 0:
            liquidated = True

        return {"balance_open": balance_open,
                "balance_close": balance_close,
                "balance_high": balance_high,
                "balance_low": balance_low,
                "liquidated": liquidated,
                }

    def drawdown_impact(self,
                        drawdown_open: float,
                        drawdown_limit: float,
                        contracts: int,
                        contract_value: float,
                        contract_fee: float,
                        include_first_min: bool = True,
                        ):
        """Runs through current trades list, calculating changes to a running
        account drawdown starting with drawdown_open.  Returns high, low, and
        ending drawdown."""
        # Make sure trades are in order or results can't be trusted
        self.sort_trades()
        drawdown_close = drawdown_open
        drawdown_high = drawdown_open
        drawdown_low = drawdown_open
        liquidated = False
        for t in self.trades:
            if not t.first_min_open or include_first_min:
                r = t.drawdown_impact(drawdown_open=drawdown_close,
                                      drawdown_limit=drawdown_limit,
                                      contracts=contracts,
                                      contract_value=contract_value,
                                      contract_fee=contract_fee,
                                      )
                drawdown_high = max(drawdown_high, r["drawdown_high"])
                drawdown_low = min(drawdown_low, r["drawdown_low"])
                drawdown_close = r["drawdown_close"]
        if drawdown_low <= 0:
            liquidated = True

        return {"drawdown_open": drawdown_open,
                "drawdown_close": drawdown_close,
                "drawdown_high": drawdown_high,
                "drawdown_low": drawdown_low,
                "liquidated": liquidated,
                }

    def stats(self, include_first_min=True):
        """Return useful statistics calculated from the attached Trades"""
        sequence = ""
        total_trades = 0
        profits = 0
        losses = 0
        days_traded = set()
        ticks = set()
        rr = {"max": None, "min": None, "total_risk": 0, "total_reward": 0}
        for t in self.trades:
            if not t.first_min_open or include_first_min:
                total_trades += 1
                # Risk reward calcs
                ticks.add((("stop", t.stop_ticks),
                          ("prof", t.prof_ticks),
                          ("offset", t.offset_ticks)))
                this_rr = round(t.stop_ticks/t.prof_ticks, 2)
                if rr["max"] is None:
                    rr["max"] = this_rr
                else:
                    rr["max"] = max(rr["max"], this_rr)
                if rr["min"] is None:
                    rr["min"] = this_rr
                else:
                    rr["min"] = min(rr["min"], this_rr)
                rr["total_risk"] += t.stop_ticks
                rr["total_reward"] += t.prof_ticks
                # Add date to days_traded set
                days_traded.add(dhu.dt_as_dt(t.open_dt).date())
                # Update profitability
                if t.profitable:
                    profits += 1
                    sequence = "".join([sequence, "g"])
                else:
                    losses += 1
                    sequence = "".join([sequence, "L"])
        success_percent = round(profits/total_trades, 4)*100
        risk_reward = round(rr["total_risk"] / rr["total_reward"], 2)
        min_risk_reward = rr["min"]
        max_risk_reward = rr["max"]
        trading_days = len(days_traded)
        total_days = (dhu.dt_as_dt(self.end_dt)
                      - dhu.dt_as_dt(self.start_dt)).days
        total_weeks = round(total_days/7, 2)
        trades_per_day = round(total_trades/total_days, 2)
        trades_per_trading_day = round(total_trades/trading_days, 2)
        trades_per_week = round(total_trades/total_weeks, 2)

        return {"gl_sequence": sequence,
                "profitable_trades": profits,
                "losing_trades": losses,
                "total_trades": total_trades,
                "success_percent": success_percent,
                "risk_reward": risk_reward,
                "min_risk_reward": min_risk_reward,
                "max_risk_reward": max_risk_reward,
                "trading_days": trading_days,
                "total_days": total_days,
                "total_weeks": total_weeks,
                "trades_per_week": trades_per_week,
                "trades_per_day": trades_per_day,
                "trades_per_trading_day": trades_per_trading_day,
                "trade_ticks": ticks,
                }

    def weekly_stats(self, include_first_min: bool = True):
        """Return useful statistics calculated from the attached Trades
        aggregated into weekly buckets using Monday as the start of the week
        and Monday's date as the name of each bucket."""
        # Build a dict of weeks with zeroes as default values to ensure we
        # represent non-traded weeks in the result rather than leave gaps
        template = {"total_trades": 0,
                    "profitable_trades": 0,
                    "losing_trades": 0,
                    "gl_in_ticks": 0,
                    "success_rate": "nil",
                    }
        result = dhu.dict_of_weeks(start_dt=self.start_dt,
                                   end_dt=self.end_dt,
                                   template=template)
        # Loop through trades to aggregate stats
        for t in self.trades:
            if not t.first_min_open or include_first_min:
                d = dhu.dt_as_dt(t.open_dt)
                w = str(d.date() - timedelta(days=d.weekday()))
                result[w]["total_trades"] += 1
                if t.profitable:
                    result[w]["profitable_trades"] += 1
                    result[w]["gl_in_ticks"] += t.prof_ticks
                else:
                    result[w]["losing_trades"] += 1
                    result[w]["gl_in_ticks"] -= t.stop_ticks
        # Calculate success rates
        for k in result.keys():
            if result[k]["total_trades"] > 0:
                srate = round(result[k]["profitable_trades"]
                              / result[k]["total_trades"]
                              * 100, 0)
                result[k]["success_rate"] = srate

        return result


class Backtest():
    """Represents a backtest that can be run with specific parameters.  This
    is a parent class containing core functionality and likely won't be used
    to run backtests directly.  Subclasses should be created with updated
    methods and parameters representing the specific rules of the backtests
    being performed.

    Attributes:
        start_dt (str or datetime): Beginning of time period evaluated which
            may be earlier than the first trade datetime
        end_dt (str or datetime): End of time period evaluated
        timeframe (str): timeframe of underlying chart trades were evaluated
            on
        trading_hours (str): whether to run trades during regular trading
            hours only ('rth') or include extended/globex hours ('eth')
        symbol (str or dhcharts.Symbol): The symbol or "ticker" being
            evaluated.  If passed as a str, the object will fetch the symbol
            with a matching 'name'.
        name (str): Human friendly label representing this object
        class_name (str): attrib to identify subclass, primarily used by
            storage functions to reassemble retrieved data into the correct
            object type
        parameters (dict): Backtest specific parameters needed to evaluate.
            These will vary and be handled by subclases typically.
        bt_id (str): unique ID used for storage and analysis purposes,
            can be used to link related TradeSeries() and Analyzer() objects
        chart_tf (dhcharts.Chart): Underlying chart used for evaluation at
            same tf as Backtest object.  This is typically used to evaluate
            timeframe specific candle patterns and attributes that may be
            needed by the specific backtest rules.
        chart_1m (dhcharts.Chart): underlying 1m chart is tyipcally used in
            combination with chart_tf to find specific entries and exits and
            build Trade() objects.
        autoload_charts (bool): Whether to automatically load chart_tf and
            chart_1m from central storage at creation
            at creation (default True)
        prefer_stored (bool): If a backtest with the same bt_id is in storage,
            configure this object with it's configuration rather than creating
            a new backtest (default True)
        tradeseries (list): List of TradeSeries() objects which will be
            created when the Backtest is run
    """
    def __init__(self,
                 start_dt,
                 end_dt,
                 timeframe: str,
                 trading_hours: str,
                 symbol,
                 name: str,
                 parameters: dict,
                 bt_id: str = None,
                 class_name: str = "Backtest",
                 chart_tf=None,
                 chart_1m=None,
                 autoload_charts: bool = False,
                 prefer_stored: bool = True,
                 tradeseries: list = None,
                 ):
        self.start_dt = dhu.dt_as_str(start_dt)
        self.end_dt = dhu.dt_as_str(end_dt)
        if dhu.valid_timeframe(timeframe):
            self.timeframe = timeframe
        if dhu.valid_trading_hours(trading_hours):
            self.trading_hours = trading_hours
        dhu.check_tf_th_compatibility(tf=timeframe, th=trading_hours)
        if isinstance(symbol, str):
            self.symbol = dhs.get_symbol_by_ticker(ticker=symbol)
        else:
            self.symbol = symbol
        self.name = name
        if bt_id is None:
            self.bt_id = name
        else:
            self.bt_id = bt_id
        self.class_name = class_name
        self.parameters = deepcopy(parameters)
        self.chart_tf = chart_tf
        self.chart_1m = chart_1m
        if tradeseries is None:
            self.tradeseries = []
        else:
            self.tradeseries = tradeseries.copy()
            for ts in self.tradeseries:
                ts.update_bt_id(self.bt_id)
        self.prefer_stored = prefer_stored
        if self.prefer_stored:
            from_store = self.config_from_storage()
        else:
            from_store = False
        self.autoload_charts = autoload_charts
        # Only load charts if this copy wasn't configured from storage
        # as config_from_storage will load charts via rerunning __init__
        if self.autoload_charts and not from_store:
            self.load_charts()

    def __eq__(self, other):
        return (self.start_dt == other.start_dt
                and self.end_dt == other.end_dt
                and self.timeframe == other.timeframe
                and self.trading_hours == other.trading_hours
                and self.symbol == other.symbol
                and self.name == other.name
                and self.bt_id == other.bt_id
                and self.class_name == other.class_name
                and self.chart_tf == other.chart_tf
                and self.chart_1m == other.chart_1m
                and self.tradeseries == other.tradeseries
                and self.sub_eq(other)
                )

    def __ne__(self, other):
        return not self.__eq__(other)

    def sub_eq(self, other):
        """Placeholder method for subclasses to add additional attributes
        or conditions required to evaluate __eq__ for this class.  Any
        comparison of parameters should be done here as they are subclass
        specific."""
        return self.parameters == other.parameters

    def sub_to_json(self, working):
        """Placeholder for subclasses to normalize any additional attributes
        they may have added to make them also JSON serializable."""
        return working

    def to_json(self,
                suppress_tradeseries: bool = True,
                suppress_trades: bool = True,
                suppress_charts: bool = True,
                suppress_chart_candles: bool = True,
                ):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        working = deepcopy(self.__dict__)
        working["symbol"] = working["symbol"].ticker
        if suppress_charts:
            working["chart_tf"] = "Chart suppressed for output sanity"
            working["chart_1m"] = "Chart suppressed for output sanity"
        else:
            if self.chart_tf is not None:
                working["chart_tf"] = self.chart_tf.to_clean_dict(
                        suppress_candles=suppress_chart_candles,
                        )
            if self.chart_1m is not None:
                working["chart_1m"] = self.chart_1m.to_clean_dict(
                        suppress_candles=suppress_chart_candles,
                        )
        if suppress_tradeseries:
            num = len(self.tradeseries)
            m = f"{num} Tradeseries suppressed for output sanity"
            clean_tradeseries = [m]
        else:
            clean_tradeseries = []
            for t in self.tradeseries:
                clean_tradeseries.append(t.to_clean_dict(
                    suppress_trades=suppress_trades,
                    ))
        working["tradeseries"] = clean_tradeseries

        return json.dumps(self.sub_to_json(working))

    def to_clean_dict(self,
                      suppress_tradeseries: bool = True,
                      suppress_trades: bool = True,
                      suppress_charts: bool = True,
                      suppress_chart_candles: bool = True,
                      ):
        """Converts to JSON string then back to a python dict.  This helps
        to normalize types (I'm looking at YOU datetime) while ensuring
        a portable python data structure"""
        return json.loads(self.to_json(
            suppress_tradeseries=suppress_tradeseries,
            suppress_trades=suppress_trades,
            suppress_charts=suppress_charts,
            suppress_chart_candles=suppress_chart_candles,
            ))

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def pretty(self,
               suppress_tradeseries: bool = True,
               suppress_trades: bool = True,
               suppress_charts: bool = True,
               suppress_chart_candles: bool = True,
               ):
        """Attempts to return an indented multiline version of this object,
        meant to provide an easy to read output for console or other purposes.
        Optionally suppress_datapoints to reduce output size when not needed.
        """
        return json.dumps(self.to_clean_dict(
            suppress_tradeseries=suppress_tradeseries,
            suppress_trades=suppress_trades,
            suppress_charts=suppress_charts,
            suppress_chart_candles=suppress_chart_candles,
            ),
            indent=4,
            )

    def load_charts(self):
        """Load a Chart based on this object's datetimes, symbol, and
        timeframe arguments.  This will be the base data for calculating
        trades.  This method also restricts the Backtest's timeframe (start_dt
        and end_dt attributes) to match the earliest and latest 1m candles
        available from storage to ensure future updates do not leave
        calculation gaps where valid candles were not yet available on earlier
        runs."""
        # Build candle charts, retrieving candles from storage
        self.chart_tf = dhc.Chart(c_timeframe=self.timeframe,
                                  c_trading_hours=self.trading_hours,
                                  c_symbol=self.symbol,
                                  c_start=self.start_dt,
                                  c_end=self.end_dt,
                                  autoload=True,
                                  )
        self.chart_1m = dhc.Chart(c_timeframe="1m",
                                  c_trading_hours=self.trading_hours,
                                  c_symbol=self.symbol,
                                  c_start=self.start_dt,
                                  c_end=self.end_dt,
                                  autoload=True,
                                  )
        # Limit the timeframe of the Backtest based on existing candles
        self.start_dt = self.chart_1m.c_candles[0].c_datetime
        self.end_dt = self.chart_1m.c_candles[-1].c_datetime
        # And adjust the chart timeframes to match the Backtest
        self.chart_tf.c_start = self.start_dt
        self.chart_tf.c_end = self.end_dt
        self.chart_1m.c_start = self.start_dt
        self.chart_1m.c_end = self.end_dt

    def store(self,
              store_tradeseries: bool = True,
              store_trades: bool = True,
              ):
        """Store the backtest and optionally any attached TradeSeries and their
        attached Trades.  This first deletes any of the same 3 object types
        from storage with this Backtest's bt_id to ensure no duplication
        occurs."""
        self.delete_from_storage(include_tradeseries=store_tradeseries,
                                 include_trades=store_trades,
                                 )
        result = {"backtest": dhs.store_backtests(backtests=[self]),
                  "tradeseries": [],
                  }
        st = store_trades
        if store_tradeseries and self.tradeseries is not None:
            for s in self.tradeseries:
                result["tradeseries"].append(s.store(store_trades=st))

        return result

    def delete_from_storage(self,
                            include_tradeseries: bool = True,
                            include_trades: bool = True,
                            ):
        """Delete this object from central storage if it exists by bt_id.
        Optionally (default=True) also remove all TradeSeries and Trade objects
        matching this object's bt_id."""
        result = {"backtest": None, "tradeseries": []}

        result["backtest"] = dhs.delete_backtests(symbol=self.symbol,
                                                  field="bt_id",
                                                  value=self.bt_id,
                                                  )
        if include_tradeseries:
            for ts in self.tradeseries:
                result["tradeseries"].append(ts.delete_from_storage(
                    include_trades=include_trades,
                    ))

        return result

    def count_tradeseries(self):
        """Return the number of TradeSeries currently attached"""
        if self.tradeseries is None:
            return 0
        else:
            return len(self.tradeseries)

    def count_trades(self):
        """Return the number of Trades currently attached"""
        count = 0
        for ts in self.tradeseries:
            count += ts.count_trades()
        return count

    def update_tradeseries(self,
                           ts,
                           clear_storage: bool = True):
        """Add an existing TradeSeries to this Backtest, typically used
        for pulling results in from previous runs to update with new data.
        If a TradeSeries with the same ts_id is already attached, this will
        replace it.  clear_storage is passed to remove_tradeseries to delete
        the replaced TradeSeries from storage when updating."""
        if self.tradeseries is None:
            self.tradeseries = []
        # Associate this TradeSeries with this Backtest
        ts.update_bt_id(self.bt_id)
        # Remove any existing TradeSeries with the same ts_id in case this
        # is an update rather than new addition
        self.remove_tradeseries(ts.ts_id, clear_storage=clear_storage)
        # Add the tradeseries to the Backtest
        self.tradeseries.append(ts)
        self.sort_tradeseries()

    def remove_tradeseries(self,
                           ts_id,
                           clear_storage: bool = True):
        """Removes any TradeSeries with the given ts_id from the Backtest.
        Optionally remove TradeSeries and it's Trades from storage as well
        (default = True)."""
        # Delete TradeSeries and associated Trades from storage
        if clear_storage:
            dhs.delete_tradeseries(symbol="ES",
                                   field="ts_id",
                                   value=ts_id,
                                   )
            dhs.delete_trades(symbol="ES",
                              field="ts_id",
                              value=ts_id,
                              )
        # Rebuild Backtest's list of TradeSeries, excluding any matching ts_id
        self.tradeseries = [ts for ts in self.tradeseries if ts.ts_id != ts_id]
        return True

    def sort_tradeseries(self):
        self.tradeseries.sort(key=lambda t: t.ts_id)

    def load_tradeseries(self):
        """Attaches any TradeSeries and their linked trades that are found in
        storage and which match this object's bt_id.  This will replace any
        currently attached tradeseries."""
        self.tradeseries = dhs.get_tradeseries_by_field(field="bt_id",
                                                        value=self.bt_id,
                                                        include_trades=True,
                                                        )
        self.sort_tradeseries()

    def restrict_dates(self,
                       new_start_dt: str,
                       new_end_dt: str,
                       update_storage: bool = False,
                       ):
        """Reduce the datetime range of the Backtest which will also reduce
        any attached TradeSeries and remove any Trades that start ouside of
        the boundaries of the new range.  Optionally pass update_storage=True
        to remove Trades and update dates on both Backtest and it's linked
        TradeSeries in storage (destructive) to make this permanent.
        Typically used to clean up failed partial calculation runs or, when
        non-destructive, to set up for analyzing a targetted timeframe of
        special interest within the longer Backtest."""
        os = dhu.dt_as_dt(self.start_dt)
        oe = dhu.dt_as_dt(self.end_dt)
        ns = dhu.dt_as_dt(new_start_dt)
        ne = dhu.dt_as_dt(new_end_dt)
        # Ensure new dates don't expand the daterange, they should only reduce
        # or keep unchanged
        if ns < os:
            raise ValueError(f"new_start_dt {new_start_dt} cannot be earlier "
                             f"than the current self.start_dt {self.start_dt}")
        if ne > oe:
            raise ValueError(f"new_end_dt {new_end_dt} cannot be later "
                             f"than the current self.end_dt {self.end_dt}")
        # Update Backtest start and end dates and optionally store
        self.start_dt = dhu.dt_as_str(new_start_dt)
        self.end_dt = dhu.dt_as_str(new_end_dt)
        if update_storage:
            self.store(store_tradeseries=False, store_trades=False)
        # Update the attached Charts for the new dates as well
        self.chart_tf.restrict_dates(new_start_dt=new_start_dt,
                                     new_end_dt=new_end_dt,
                                     )
        self.chart_1m.restrict_dates(new_start_dt=new_start_dt,
                                     new_end_dt=new_end_dt,
                                     )
        # Update all attached TradeSeries
        for ts in self.tradeseries:
            ts.restrict_dates(new_start_dt=new_start_dt,
                              new_end_dt=new_end_dt,
                              update_storage=update_storage,
                              )

    def config_from_storage(self):
        """This class should be updated in subclasses to allow retrieval and
        configuration of itself from storage if there is a matching bt_id
        stored.  Otherwise it should do nothing as the object has already
        been created and configured by __init__ by the time it gets here.

        Note that in most cases this should by default also load any
        TradeSeries that exist in storage and their associated Trades though
        this could be suppressed by a feature flag if desired in the subclass.
        """
        self.sort_tradeseries()
        # Subclass copies of this method should return True if configuration
        # from storage was successful (a stored version was found and applied)
        # and False if it was not reconfigured from storage.
        return False

    def incorporate_parameters(self):
        """This class should be updated in subclasses to run whatever logic is
        needed to validate any subclass-specific parameters and set them up
        as attributes on the object. This will vary greatly from one type of
        backtest to another.
        """
        pass

    def calculate(self):
        """This class should be updated in subclasses to run whatever logic is
        needed to transform the chart_* attributes into multiple TradeSeries
        using parameters supplied.  This will vary greatly from one type of
        backtest to another.  At the end of the run it will likely need to
        also store the Backtest object along with it's child TradeSeries and
        grandchild Trades.
        """
        pass
