# Why DHTrader exists and where it's going

My personal classes representing charting and trading objects to be used in backtesting and analysis.  Objects in this repo should never include proprietary trade systems or ideas, they are just the framework.  Those items belong in private repos only.

This is not a detailed and robust backtesting/analysis platform nor is it intended to be used by others, it's mostly shared for the sake of having a public "code portfolio" and reference for snippets when discussing Python coding with others.  There is no feature roadmap, I just add things as I need them.

Two main reasons exist for building this.  Mostly it's a personal project to help me learn OOP and develop more robust coding skills beyond what my "Ops Guy" mostly-scripting professional career has provided opportunities for.  The reason to have this project outside of learning was to develop a framework to backtest and analyze trades that include a live drawdown factor in proprietary funded trading accounts ("prop firms") which I did not find when initially reviewing the more popular backtesting libraries available.  It also allows me to approach digging into ideas without the bias inherent towards particular techniques or ideas built into popular systems, in which anything that works is likely to get arbitraged out quickly.

See /docs/ for class details

Note to self - docs are not autoupdating, run mkdocs.sh to update them.  Perhaps this can be a pre-commit hook?

# Handling nested objects/subclasses storage and retrieval

To keep logic within each class's own methods where it belongs, I'm writing storage functions in dhstore.py and dhmongo.py so that they only store information pertient to the object in question itself and not any of it's nested objects.  Nested objects might include a Chart(), a list of Trades(), or a number of other items along these lines.  This was decided after several such functions were already created and may not be fully backported.  See Backtest and TradeSeries for examples of this implementation

Each class should then determine, within it's own methods exclusively, whether and how to go about looping through the nested objects it contains when performing storage and retrieval tasks and then call the related functions or methods on each of the nested objects where appropriate.  If there is a need for them to link to the parent they should each include an attribute with a unique id shared by the parent.

Each class with nested objects should include a .to_clean_dict() method which will return a python dictionary of it's storable attributes while stripping any nested objects.  This way the storage function can receive the entire object and call back to it to get just the storable parts to pass to dhmongo as a json object while remaining compatible with future storage systems that may need something different.

# Events suggestions

The following events are what I find helpful to load into my central storage for use in backtesting analysis.  There is no hard fast rule here, it's up to the user to determine what events are relevant for their testing.  That being said, gap analsyis and similar functions will throw errors if market closures are not included.

* Holidays (Closed)
* FOMC Rate Announcements - mark the whole day or just relevant hours?
* OPEX - mark the whole day
* ES Contract rollover periods - TBD, which days to include?
* Big data drops with potential to move the market
  * CPI
  * PPI
  * NFP
  * What else?
* Periods of high volatility due to unexpected news

# Handling drawdowns in prop firm accounts
TODO: Revamp this section once it's all working as expected
TODO: Note sample trades listed in Apex_Drawdown_Observations that were used to build unit tests and confirm calcs
The library makes some efforts to assist traders in factoring trailing drawdowns into their backtests to simulate how real world trading would play out in a prop firm account.  For traders working in real cash accounts these attributes can be set to None to remove their impact from calculations.

Trailing drawdowns are assumed to work like Apex Trader Funding style as that's what I'm using.  The details are listed out at https://support.apextraderfunding.com/hc/en-us/articles/4408610260507-How-Does-the-Trailing-Threshold-Work-Master-Course but I will attempt to summarize in my own words here:

* Each account has a maximum trailing threshold amount, often referred to as a "drawdown distance", which determines the account balance dollar amount at which your account will be liquidated and closed if you lose too much money trading.
* This drawdown distance is udpated live as trades are running, tracking the difference between the entry price and the current price until it is locked in by the trade being closed.  It can never exceed the maximum trailing threshold amount, nor can it go below zero as this will liquidate the account.
* Any running trade that brings your balance below the liquidation threshold, even momentarily, will close the trade automatically and liquidate the account.
* When a profitable trade would take the drawdown distance above the maximum trailing threshold amount, the level at which you will be liquidated increases by the same amount as the "overreach" even if you do not close the trade until it pulls back.

Note that this library only factors in eval style drawdowns that never stop trailing.  It assumes that if you are building a strategy to work with traling drawdowns, it must first work in an eval account.  It also assumes you would not want to change the strategy once the eval is passed as this invalidates the backtest, so no effort is made to account for limited trailing thresholds in "real" accounts that have passed evaluation.  You may backtest both with and without drawdowns if you want to simulate changing the strategy once the account is well above it's trailing threshold but this seems unwise to me so I won't be coding to account for it.

## How this is implemented
TODO: Revamp this section once it's all working as expected

Trade() objects have several attributes and methods involved:
* drawdown_max - maximum drawdown distance allowed by account before trailing
* drawdown_open - account drawdown distance before trade was opened
* drawdown_close - account drawdown distance after trade was closed
* drawdown_impact - amount by which drawdown would be impacted by gain/loss and fees, but before factoring in drawdown_max/trailing adjustments
* drawdown_peak_profit - maximum potential profit seen while trade was open
* .update_drawdown() - method to update drawdown_impact and sometimes drawdown_close if the trade was previously closed.
* .close() - method to close trade which also runs .update_drawdown()

When a Trade is created, it should be given a drawdown_max and drawdown_open as these are static from the Trade's perspective.

While running, the Trade updates it's drawdown_peak_profit attribute which will be locked in on trade closing.

On being closed, the .update_drawdown() method calculates the drawdown_impact and drawdown_close values.  It also factors in drawdown_peak_profit when determining drawdown_close.  If drawdown_peak_profit + drawdown_open > drawdown_max, the drawdown_close will need to move up by the overreach amount to simulate the trailing effect.

Because Trade objects can be added or removed from TradeSeries during future analysis, their drawdown_open and drawdown_max may be modified by the parent TradeSeries.  In these cases .update_drawdown() needs to be rerun to ensure all calculated attributes are also updated.  This is typically handled by TradeSeries.calc_drawdowns() which runs through the Trades in sequence, updating each according to the results of the prior Trade's drawdown recalculations.

TradeSeries() objects also have some attributes and methods:
* drawdown_open - account drawdown distance before the first Trade
* drawdown_close - account drawdown distance after the last Trade
* drawdown_max - maximum drawdown distance allowed by account before trailing
* .calc_drawdowns() - method to recalculate all drawdown effects through all Trades in the TradeSeries, primarily used during later analysis when they may be added, removed, or moved around between different TradeSeries.

Typically, Trade attributes are set during the Backtest creation which adds them to the TradeSeries as they are being created.  However, during later Analysis when Trades may get added or removed or the drawdown_open and drawdown_max might be different in the new TradeSeries, the .calc_drawdowns() method can be used to update all child Trades.
