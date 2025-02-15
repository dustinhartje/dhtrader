import json
from datetime import datetime as dt
from copy import deepcopy
import dhutil as dhu
import dhstore as dhs
import dhcharts as dhc


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
        open_drawdown (float): account drawdown level at time of trade open
        close_drawdown (float): account drawdown level after trade was closed
        close_dt (str): datetime trade was closed
        created_dt (str): datetime this object was created
        exit_price (float): price at which trade was closed
        gain_loss (float): account balance gain or loss resulting from trade
        stop_ticks (float): number of ticks to reach stop_target from entry,
            primarily used as trade rules identification during analysis
        prof_ticks (float): number of ticks to reach prof_target from entry,
            primarily used as trade rules identification during analysis
        offset_ticks (float): number of ticks away from target price that
            this trade made entry, primarily used as trade rules
            identification during analysis
        drawdown_impact (float): potential impact on trailing drawdown from
            entry_price to max unrealized profit seen.  Added to gain_loss in
            a losing trade to determine close_drawdown
        symbol (str): ticker being traded
        contracts (int): number of contracts traded
        contract_value (float): value per contract
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
                 stop_target: float,
                 prof_target: float,
                 open_drawdown: float,
                 close_drawdown: float = None,
                 close_dt: str = None,
                 created_dt: str = None,
                 open_epoch: int = None,
                 exit_price: float = None,
                 gain_loss: float = None,
                 stop_ticks: int = 0,
                 prof_ticks: int = 0,
                 offset_ticks: int = 0,
                 drawdown_impact: float = float(0),
                 symbol: str = 'ES',
                 contracts: int = 1,
                 contract_value: float = float(50),
                 is_open: bool = True,
                 profitable: bool = None,
                 name: str = None,
                 version: str = "1.0.0",
                 ts_id: str = None,
                 bt_id: str = None
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
        self.exit_price = exit_price
        self.open_drawdown = open_drawdown
        self.close_drawdown = close_drawdown
        self.gain_loss = gain_loss
        self.stop_ticks = stop_ticks
        self.prof_ticks = prof_ticks
        self.offset_ticks = offset_ticks
        self.drawdown_impact = drawdown_impact
        self.symbol = symbol
        self.contracts = contracts
        self.contract_value = contract_value
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
        # If closing attributes were passed, run close() to ensure all
        # related attributes that may not have been passed in are finalized
        if self.exit_price is not None:
            self.close(price=self.exit_price,
                       dt=self.close_dt,
                       )
        self.open_epoch = dhu.dt_to_epoch(self.open_dt)

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return (self.open_dt == other.open_dt
                and self.direction == other.direction
                and self.entry_price == other.entry_price
                and self.stop_target == other.stop_target
                and self.prof_target == other.prof_target
                # TODO drawdown stats are getting changed when Trades are
                #      stored and then retrieved.  I also have not yet
                #      verified they are calculating properly in the first
                #      place (there's another TODO for this somewhere)
                #      Ignoring for now as they are breaking tests and I'm not
                #      yet using them anyways
                # and self.open_drawdown == other.open_drawdown
                # and self.close_drawdown == other.close_drawdown
                and self.close_dt == other.close_dt
                and self.created_dt == other.created_dt
                and self.open_epoch == other.open_epoch
                and self.exit_price == other.exit_price
                and self.gain_loss == other.gain_loss
                and self.stop_ticks == other.stop_ticks
                and self.prof_ticks == other.prof_ticks
                and self.offset_ticks == other.offset_ticks
                # TODO another drawdown thing to fix later
                # and self.drawdown_impact == other.drawdown_impact
                and self.symbol == other.symbol
                and self.contracts == other.contracts
                and self.contract_value == other.contract_value
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
        if self.open_dt is not None:
            self.open_dt = dhu.dt_as_str(self.open_dt)
        if self.close_dt is not None:
            self.close_dt = dhu.dt_as_str(self.close_dt)
        if self.created_dt is not None:
            self.created_dt = dhu.dt_as_str(self.created_dt)

        return json.dumps(self.__dict__)

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

    def store(self):
        """Store this trade in central storage"""

        return dhs.store_trades(trades=[self])

    def update_drawdown(self,
                        price_seen: float,
                        ):
        """Takes a price, typically the candle high or low, and updates
           the drawdown impact accordingly"""
        # NOTE This only updates the drawdown_impact and is meant for
        # running trade updates.  Final drawdown calculations are done in the
        # close() method.

        # Represent drawdown_impact as negative unless/until the trade closes
        # in profit.  price_diff will be used to calculate it momentarily.
        # In case of a short trade this is inverted by self.flipper
        price_diff = (self.entry_price - price_seen) * self.flipper
        # Use the worst of the current and prior worse drawdown impact
        self.drawdown_impact = min(self.drawdown_impact, price_diff)
        # If we update this after the trade is closed, reclose it to calculate
        # any changes as well
        if not self.is_open:
            self.close(price=self.exit_price,
                       dt=self.close_dt,
                       )

    def close(self,
              price: float,
              dt,
              ):
        self.is_open = False
        self.close_dt = dhu.dt_as_str(dt)
        self.exit_price = price
        contract_multiplier = self.contracts * self.contract_value
        self.gain_loss = (((self.exit_price - self.entry_price)
                          * contract_multiplier)) * self.flipper
        # Close as a profitable trade if we made money
        # TODO I'm not sure if I have drawdown impacts calculating correctly
        #      I may need to do some trades and check the direct impact
        #      in apex to confirm my understanding of how they work.
        #      Specifically when it closes in profit but not at the max
        #      profit it could have seen i.e. it goes up 10pts but does not
        #      reach my profit target and stops out after pulling back 5pts.
        #      Is my drawdown balance left the same as before the trade
        #      because I gained 5 from the profit but lost 5 from the pb?
        #      or it it up 5 because I gained 10 then lost 5 on the drawdown?
        #      In other words, is the drawdown impact always equal to the gain
        #      on a winning trade
        #      TODO can I pull some trades from apex history to check this
        #           so I don't have to wait until sunday evening to test?
        #      TODO I should rewrite tests at the bottom to reflect actual
        #           trades I did so I can confirm all the results, including
        #           drawdown impact, against real results vs theory
        #      TODO be sure to test all scenarios:
        #           * profitable trade hits profit target
        #           * profitable trade with small pullback
        #           * trade comes back to exit at breakeven
        #           * exit at a loss after it goes green substantially first
        #           * trade never sees price above entry just goes to stop
        #           * loss but pulls back towards BE some before I exit
        #           TODO write a unit test that checks each of these scenarios
        #                and confirms all of them get the correct result
        #                This one really does need to be a comprehensive test
        #                I'm running regularly probably as a pipeline
        #                because this has to be right or everything else is
        #                untrustable.  It's worth putting an evening into
        #                figuring out and then maybe if it's quick I can put
        #                a few hours into updating others into unit tests
        #                What do I want it to do on a test failure?  Can
        #                it email me?  would prefer the commit goes through
        #                either way I just want a notification.  Commit hook
        #                testing locally might be an ok first step for now
        #                if it doesn't take very long each time
        #      TODO If I can't test with live trades today, go watch ATF vids
        #           and get my best guess on how it works then test live
        #           with MES sunday evening to confirm, this is not a
        #           blocker to building on Trades yet I just can't trust the
        #           any backtest trades until this is all confirmed and tested
        if self.gain_loss > 0:
            self.profitable = True
            # TODO update this comment if I learn differently in testing
            # On a profitable trade, our drawdown_impact is equal to the
            # profit made on the trade.
            self.drawdown_impact = self.gain_loss
        # Close as a losing trade if we lost money
        else:
            self.profitable = False
            # TODO also update these comments based on real life testing
            # If the trade was a loss, our drawdown is negatively impacted
            # by the delta from the entry to the max profit it reached
            # (drawdown_impact) plus the stop target / amount lost in the trade
            # (gain_loss).  This amount will be negative for a loss.
            self.drawdown_impact = ((self.drawdown_impact
                                     * contract_multiplier)
                                    + self.gain_loss)
        self.close_drawdown = self.open_drawdown + self.drawdown_impact
        # print(f"open_drawdown {self.open_drawdown} drawdown_impact "
        #       f"{self.drawdown_impact} close_drawdown {self.close_drawdown}")


class TradeSeries():
    """Represents a series of trades presumably following the same rules

    Attributes:
        start_dt (str or datetime): Beginning of time period evaluated which
            may be earlier than the first trade datetime
        end_dt (str or datetime): End of time period evaluated
        timeframe (str): timeframe of underlying chart trades were evaluated
            on
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
                 symbol: str = "ES",
                 name: str = "",
                 params_str: str = "",
                 ts_id: str = None,
                 bt_id: str = None,
                 trades: list = None,
                 ):

        self.start_dt = dhu.dt_as_str(start_dt)
        self.end_dt = dhu.dt_as_str(end_dt)
        self.timeframe = timeframe
        self.symbol = symbol
        self.name = name
        self.params_str = params_str
        if ts_id is None:
            self.ts_id = "_".join([self.name,
                                   self.params_str,
                                   str(dhu.dt_to_epoch(dt.now())),
                                   ])
        else:
            self.ts_id = ts_id
        self.bt_id = bt_id
        if trades is None:
            self.trades = []
        else:
            self.trades = trades.copy()

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
        if not isinstance(trade, Trade):
            raise TypeError(f"trade {trade} must be a dhtrades.Trade() obj,"
                            f"got a {type(trade)} instead"
                            )
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
        symbol (str): The symbol or "ticker" being evaluated
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
        tradeseries (list): List of TradeSeries() objects which will be
            created when the Backtest is run
    """
    def __init__(self,
                 start_dt,
                 end_dt,
                 timeframe: str,
                 trading_hours: str,
                 symbol: str,
                 name: str,
                 parameters: dict,
                 bt_id: str = None,
                 class_name: str = "Backtest",
                 chart_tf=None,
                 chart_1m=None,
                 autoload_charts: bool = True,
                 tradeseries: list = None,
                 ):
        self.start_dt = dhu.dt_as_str(start_dt)
        self.end_dt = dhu.dt_as_str(end_dt)
        if dhu.valid_timeframe(timeframe):
            self.timeframe = timeframe
        if dhu.valid_trading_hours(trading_hours):
            self.trading_hours = trading_hours
        dhu.check_tf_th_compatibility(tf=timeframe, th=trading_hours)
        self.symbol = symbol
        self.name = name
        if bt_id is None:
            self.bt_id = "_".join([name, str(dhu.dt_to_epoch(dt.now()))])
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
        self.autoload_charts = autoload_charts
        if self.autoload_charts:
            self.load_charts()

    def to_json(self,
                suppress_tradeseries: bool = True,
                suppress_trades: bool = True,
                suppress_charts: bool = True,
                suppress_chart_candles: bool = True,
                ):
        """returns a json version of this object while normalizing
        custom types (like datetime to string)"""
        working = deepcopy(self.__dict__)
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

        return json.dumps(self.to_json_subs(working))

    def to_json_subs(self, working):
        """Placeholder for subclasses to normalize any additional attributes
        they may have added to make them also JSON serializable."""
        return working

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
        trades."""
        self.chart_tf = dhc.Chart(c_timeframe=self.timeframe,
                                  c_symbol=self.symbol,
                                  c_start=self.start_dt,
                                  c_end=self.end_dt,
                                  autoload=True,
                                  )
        self.chart_1m = dhc.Chart(c_timeframe="1m",
                                  c_symbol=self.symbol,
                                  c_start=self.start_dt,
                                  c_end=self.end_dt,
                                  autoload=True,
                                  )

    def store(self,
              store_tradeseries: bool = True,
              store_trades: bool = True,
              ):
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

    def add_tradeseries(self,
                        ts,
                        ):
        """Add an existing tradeseries to this Backtest, typically used
        for pulling results in from previous runs to update with new data."""
        if not isinstance(ts, TradeSeries):
            raise TypeError(f"ts {ts} must be a dhtrades.TradeSeries() obj, "
                            f"got a {type(ts)} instead!"
                            )
        if self.tradeseries is None:
            self.tradeseries = []
        # Associate this TradeSeries with this Backtest
        ts.bt_id = self.bt_id
        self.tradeseries.append(ts)

    def load_tradeseries(self):
        """Attaches any TradeSeries and it's linked trades that are found in
        storage and which match this object's bt_id.  This will replace any
        currently attached tradeseries."""
        self.tradeseries = dhs.get_tradeseries_by_field(field="bt_id",
                                                        value=self.bt_id,
                                                        include_trades=True,
                                                        )

    def calculate(self):
        """This class should be updated in subclasses to run whatever logic is
        needed to transform the chart_* attributes into multiple TradeSeries
        using parameters supplied.  This will vary greatly from one type of
        backtest to another.  At the end of the run it will likely need to
        also store the Backtest object along with it's child TradeSeries and
        grandchild Trades.
        """
        pass

    # TODO method to check status of this backtest by bt_id
    #      --what did I mean by this?!?  status in storage maybe, like the
    #        range of it's parameters and tradeseries dates?
    # TODO method or update to calc method which will retrieve results
    #      from prior runs and then update them.  Since there may be
    #      many trade series involved this might get tricky, start by just
    #      getting it working on a wipe-and-start-over-each-time basis
    #      then I'll have to work through saving/retrieving/updating carefully


def test_basics():
    """Basics tests used during development and to confirm simple functions
    working as expected"""
    # TODO LOWPRI make these into unit tests some day
    print("\n========================= OUTPUTS ===========================")
    # TODO in lieu of real unit tests, start a test_results empty list and
    #      record a quick oneliner for each easily confirmable test as it
    #      finishes, something like "OK - Trade() Storage and retrieval"
    #      then print them all at the end.  For non-easily-confirmed could
    #      add a note like "UNKNOWN - Visual confirm needed for Trade.pretty()
    print("All objects should print 'pretty' which confirms .to_json(), "
          ".to_clean_dict(), and .pretty() methods all work properly"
          )
    print("\n---------------------------- TRADE ---------------------------")
    out_trade = Trade(open_dt="2025-01-02 12:00:00",
                      direction="long",
                      timeframe="5m",
                      trading_hours="rth",
                      entry_price=5001.50,
                      stop_target=4995,
                      prof_target=5010,
                      open_drawdown=1000,
                      name="DELETEME"
                      )
    print(out_trade.pretty())
    print("\n-------------------------- TRADESERIES -------------------------")
    out_ts = TradeSeries(start_dt="2025-01-02 00:00:00",
                         end_dt="2025-01-05 17:59:00",
                         timeframe="5m",
                         symbol="ES",
                         name="DELETEME_Testing",
                         params_str="1p_2s",
                         trades=None,
                         )
    out_ts.add_trade(out_trade)
    print("Without trades")
    print(out_ts.pretty())
    print("With trades")
    print(out_ts.pretty(suppress_trades=False))
    print("\n--------------------------- BACKTEST --------------------------")
    out_bt = Backtest(start_dt="2025-01-02 12:00:00",
                      end_dt="2025-01-02 12:01:00",
                      symbol="ES",
                      timeframe="1m",
                      trading_hours="eth",
                      name="DELETEME_Testing",
                      parameters={},
                      autoload_charts=True,
                      )
    out_bt.add_tradeseries(out_ts)
    print("Without tradeseries, trades, charts, and candles")
    print(out_bt.pretty())
    print("With tradeseries, trades, charts, and candles")
    print(out_bt.pretty(suppress_tradeseries=False,
                        suppress_trades=False,
                        suppress_charts=False,
                        suppress_chart_candles=False,
                        ))
    print("============================ TRADES ==============================")
    # Trades
    t = Trade(open_dt="2025-01-02 12:00:00",
              direction="long",
              timeframe="5m",
              trading_hours="rth",
              entry_price=5001.50,
              stop_target=4995,
              prof_target=5010,
              open_drawdown=1000,
              name="DELETEME"
              )
    print(f"Created unclosed long test trade:\n{t}")
    print("\nUpdating drawdown_impact")
    t.update_drawdown(price_seen=5009)
    print(t)
    print("\nClosing long trade at a loss.  This should have gain_loss == "
          "-$325 and drawdown_impact == -700.")
    t.close(price=4995, dt="2025-01-02 12:45:00")
    print(t)
    print("\nStoring trade")
    print(t.store())

    print("------------------------------------------------------------------")
    t = Trade(open_dt="2025-01-02 12:01:00",
              close_dt="2025-01-02 12:15:00",
              direction="short",
              timeframe="5m",
              trading_hours="rth",
              entry_price=5001.50,
              stop_target=4995,
              prof_target=5010,
              open_drawdown=1000,
              exit_price=4995,
              name="DELETEMEToo"
              )
    print("Created closed short test trade with a gain.  This should have "
          f"gain_loss == $325 and drawdown_impact == $325 (I think...)\n{t}")
    print("\nUpdating drawdown_impact")
    t.update_drawdown(price_seen=5009)
    print(t)
    print("\nStoring trade")
    print(t.store())

    print("\n\nReviewing trades in storage:")
    print(dhs.review_trades(symbol="ES"))

    print("\nDeleting name=DELETEMEToo trades first")
    print(dhs.delete_trades(symbol="ES",
                            field="name",
                            value="DELETEMEToo",
                            ))
    print("\nReviewing trades in storage to confirm deletion:")
    print(dhs.review_trades(symbol="ES"))
    print("\nDeleting trades with name=DELETEME to finish cleanup")
    print(dhs.delete_trades(symbol="ES",
                            field="name",
                            value="DELETEME",
                            ))
    print("\nReviewing trades in storage to confirm deletion:")
    print(dhs.review_trades(symbol="ES"))

    # TradeSeries
    print("========================== TRADESERIES ===========================")
    print("Creating a TradeSeries")
    ts = TradeSeries(start_dt="2025-01-02 00:00:00",
                     end_dt="2025-01-05 17:59:00",
                     timeframe="5m",
                     symbol="ES",
                     name="DELETEME_Testing",
                     params_str="1p_2s",
                     trades=None,
                     )
    ts_id_to_delete = [ts.ts_id]
    print(ts)
    print("\nAdding two trades out of order")
    ts.add_trade(Trade(open_dt="2025-01-03 12:00:00",
                       close_dt="2025-01-03 12:15:00",
                       direction="short",
                       timeframe="5m",
                       trading_hours="rth",
                       entry_price=5001.50,
                       stop_target=4995,
                       prof_target=5010,
                       open_drawdown=1000,
                       exit_price=4995,
                       name="DELETEME",
                       ))
    ts.add_trade(Trade(open_dt="2025-01-02 14:10:00",
                       close_dt="2025-01-02 15:35:00",
                       direction="short",
                       timeframe="5m",
                       trading_hours="rth",
                       entry_price=5001.50,
                       stop_target=4995,
                       prof_target=5010,
                       open_drawdown=1000,
                       exit_price=4995,
                       name="DELETEME"
                       ))
    print(ts.trades)
    print("\nTesting .get_trade_by_open_dt() method returns a trade for "
          "2025-01-02 14:10:00")
    print(ts.get_trade_by_open_dt("2025-01-02 14:10:00"))
    print("\nTesting .get_trade_by_open_dt() method returns a None for "
          "2025-01-02 15:10:00")
    print(ts.get_trade_by_open_dt("2025-01-02 15:10:00"))
    print("\nCurrent order of trade open_dt fields")
    for t in ts.trades:
        print(t.open_dt)
    print("\nrunning .sort_trades() to fix the ordering:")
    ts.sort_trades()
    for t in ts.trades:
        print(t.open_dt)

    print("\nStoring TradeSeries and child Trades")
    print(ts.store(store_trades=True))
    print("\n\nReviewing tradeseries in storage:")
    print(dhs.review_tradeseries(symbol="ES"))
    print("\n\nReviewing trades in storage:")
    print(dhs.review_trades(symbol="ES"))

    print("\nDeleting TradeSeries objects from mongo using ts_id")
    for t in ts_id_to_delete:
        print(dhs.delete_tradeseries(symbol="ES",
                                     field="ts_id",
                                     value=t
                                     ))
    print("\nDeleting Trade objects from mongo using ts_id")
    print(dhs.delete_trades(symbol="ES",
                            field="ts_id",
                            value=ts.ts_id,
                            ))
    print("\nReviewing again to confirm deletion")
    print("\n\nReviewing tradeseries in storage:")
    print(dhs.review_tradeseries(symbol="ES"))
    print("\n\nReviewing trades in storage:")
    print(dhs.review_trades(symbol="ES"))

    # Backtesters
    print("======================== BACKTESTS================================")
    print("Creating a Backtest object")
    b = Backtest(start_dt="2025-01-02 00:00:00",
                 end_dt="2025-01-04 00:00:00",
                 symbol="ES",
                 timeframe="e1h",
                 trading_hours="eth",
                 name="DELETEME_Testing",
                 parameters={},
                 autoload_charts=True,
                 )
    print(b)
    print("\nAdding the previous test TradeSeries to this test Backtest")
    b.add_tradeseries(ts)
    print(b)
    print("\nLet's make sure our Backtest has turtles all the way down, "
          "i.e. complete set of child objects"
          )
    stuff = {"tradeseries": 0,
             "trades": 0,
             "charts": 0,
             "tf_candles": 0,
             "1m_candles": 0,
             }
    things = {"tradeseries": 1,
              "trades": 2,
              "charts": 2,
              "tf_candles": 40,
              "1m_candles": 2400,
              }
    if b.chart_tf is not None:
        stuff["charts"] += 1
        stuff["tf_candles"] += len(b.chart_tf.c_candles)
    if b.chart_1m is not None:
        stuff["charts"] += 1
        stuff["1m_candles"] += len(b.chart_1m.c_candles)
    if b.tradeseries is not None:
        stuff["tradeseries"] += len(b.tradeseries)
        for ts in b.tradeseries:
            if ts.trades is not None:
                stuff["trades"] += len(ts.trades)
    print(f"Expected: {things}")
    print(f"Received: {stuff}")
    if stuff == things:
        print("OK: They match!")
    else:
        print("ERROR: They don't match...")

    print("------------------------------------------------------------------")
    print("\nReviewing before storing all this junk:")
    print("\nReviewing backtests in storage")
    print(dhs.review_backtests(symbol="ES"))
    print("\nReviewing tradeseries in storage:")
    print(dhs.review_tradeseries(symbol="ES"))
    print("\nReviewing trades in storage:")
    print(dhs.review_trades(symbol="ES"))

    print("\nStoring the backtest and it's child objects")
    b.store(store_tradeseries=True,
            store_trades=True,
            )

    print("\nReviewing after storing all this junk, we should see 1 Backtest, "
          "1 TradeSeries, and 2 Trades all with 'DELETEME' in their names"
          )
    print("\nReviewing backtests in storage")
    print(dhs.review_backtests(symbol="ES"))
    print("\nReviewing tradeseries in storage:")
    print(dhs.review_tradeseries(symbol="ES"))
    print("\nReviewing trades in storage:")
    print(dhs.review_trades(symbol="ES"))

    print("\nAnd now we'll try to delete them all through the bt_id and ts_id "
          "fields")
    print(dhs.delete_backtests(symbol="ES",
                               field="bt_id",
                               value=b.bt_id,
                               ))
    for t in b.tradeseries:
        print(dhs.delete_tradeseries(symbol="ES",
                                     field="ts_id",
                                     value=t.ts_id,
                                     ))
        print(dhs.delete_trades(symbol="ES",
                                field="ts_id",
                                value=t.ts_id,
                                ))

    print("\nReviewing after deletion, no 'DELETEME' objects should exist")
    print("\nReviewing backtests in storage")
    print(dhs.review_backtests(symbol="ES"))
    print("\nReviewing tradeseries in storage:")
    print(dhs.review_tradeseries(symbol="ES"))
    print("\nReviewing trades in storage:")
    print(dhs.review_trades(symbol="ES"))


if __name__ == '__main__':
    test_basics()
