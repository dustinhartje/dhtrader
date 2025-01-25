import json
from datetime import datetime as dt
import dhutil as dhu


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
        successful (bool): True if trade was profitable (rename to profitable?)
        trade_name (str): Human friendly (?) trade name
        trade_version (str): Trade version if applicable
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
                 successful: bool = None,
                 trade_name: str = None,
                 trade_version: str = None,
                 ):

        self.open_dt = open_dt
        self.close_dt = close_dt
        self.created_dt = dhu.dt_as_str(dt.now())
        self.direction = direction
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
        self.successful = successful
        self.trade_name = trade_name
        self.trade_version = trade_version

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
        price_diff = self.entry_price - price_seen
        # In case of a short trade this is reversed
        if self.direction == 'short':
            price_diff = price_diff * -1
        # Use the worst of the current and prior worse drawdown impact
        self.drawdown_impact = min(self.drawdown_impact, price_diff)

    def close(self,
              price: float,
              time: str,
              ):
        self.is_open = False
        self.close_dt = time
        self.exit_price = price
        contract_multiplier = self.contracts * self.contract_value
        self.gain_loss = ((self.exit_price - self.entry_price)
                          * contract_multiplier)
        # Invert if the trade was short rather than long
        if self.direction == 'short':
            self.gain_loss = self.gain_loss * -1
        # Close as a profitable trade if we made money
        if self.gain_loss > 0:
            self.successful = True
            # On a successful trade, our drawdown_impact is equal to the
            # profit made on the trade.
            self.drawdown_impact = self.gain_loss
        # Close as a losing trade if we lost money
        else:
            self.successful = False
            # If the trade was a loss, our drawdown is negatively impacted
            # by the delta from the entry to the max profit it reached
            # (drawdown_impact) plus the stop target / amount lost in the trade
            # (gain_loss).  This amount will be negative for a loss.
            self.drawdown_impact = ((self.drawdown_impact
                                     * contract_multiplier)
                                    + self.gain_loss)
        self.close_drawdown = self.open_drawdown + self.drawdown_impact
        print(f"open_drawdown {self.open_drawdown} drawdown_impact "
              f"{self.drawdown_impact} close_drawdown {self.close_drawdown}")


def test_basics():
    """Basics tests used during development and to confirm simple functions
    working as expected"""
    # TODO LOWPRI make these into unit tests some day

    # Trades
    t = Trade(open_dt="2025-01-02 12:00:00",
              direction="long",
              entry_price=5001.50,
              stop_target=4995,
              prof_target=5010,
              open_drawdown=1000,
              )
    print(f"Created unclosed test trade:\n{t}")
    print("Updating drawdown_impact")
    t.update_drawdown(price_seen=5009)
    print(t)
    print("Closing at a loss")
    t.close(price=4995, time="2025-01-02 12:45:00")
    print(t)

    # TradeSeries

    # Backtesters


if __name__ == '__main__':
    test_basics()
