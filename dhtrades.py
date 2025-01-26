import json
from datetime import datetime as dt
import dhutil as dhu
import dhstore as dhs


class Trade():
    """Represents a single trade that could have been made.

    Attributes:
        open_dt (str): datetime trade was initiated
        direction (str): 'long' or 'short'
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
                 entry_price: float,
                 stop_target: float,
                 prof_target: float,
                 open_drawdown: float,
                 close_drawdown: float = None,
                 close_dt: str = None,
                 created_dt: str = None,
                 exit_price: float = None,
                 gain_loss: float = None,
                 stop_ticks: float = float(0),
                 prof_ticks: float = float(0),
                 offset_ticks: float = float(0),
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

    def __str__(self):
        return str(self.to_clean_dict())

    def __repr__(self):
        return str(self)

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
    def __init__(self,
                 start_dt,
                 end_dt,
                 timeframe: str,
                 symbol: str = "ES",
                 name: str = None,
                 ts_id: str = None,
                 uses_drawdown: bool = False,
                 parameters: dict = None,
                 trades: list = None,
                 ):

        self.start_dt = dhu.dt_as_str(start_dt)
        self.end_dt = dhu.dt_as_str(end_dt)
        self.timeframe = timeframe
        self.symbol = symbol
        self.name = name
        self.ts_id = ts_id
        self.uses_drawdown = uses_drawdown
        self.parameters = parameters
        self.trades = trades


def test_basics():
    """Basics tests used during development and to confirm simple functions
    working as expected"""
    # TODO LOWPRI make these into unit tests some day

    # Trades
    print("------------------------------------------------------------------")
    t = Trade(open_dt="2025-01-02 12:00:00",
              direction="long",
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
    print("Storing trade")
    print(t.store())

    print("------------------------------------------------------------------")
    t = Trade(open_dt="2025-01-02 12:00:00",
              close_dt="2025-01-02 12:15:00",
              direction="short",
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
    print("Storing trade")
    t.store()

    print("\n\nReviewing trades in storage:")
    print(dhs.review_trades(symbol="ES"))

    print("\nDeleting DELETEMEToo trades first")
    print(dhs.delete_trades(symbol="ES",
                            field="name",
                            value="DELETEMEToo",
                            ))
    print("\nReviewing trades in storage to confirm deletion:")
    print(dhs.review_trades(symbol="ES"))
    print("\nDeleting DELETEME to finish cleanup")
    print(dhs.delete_trades(symbol="ES",
                            field="name",
                            value="DELETEME",
                            ))
    print("\nReviewing trades in storage to confirm deletion:")
    print(dhs.review_trades(symbol="ES"))

    # TradeSeries

    # Backtesters


if __name__ == '__main__':
    test_basics()
