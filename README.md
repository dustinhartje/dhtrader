# Why DHTrader exists and where it's going

My personal classes representing charting and trading objects to be used in backtesting and analysis.  Objects in this repo should never include proprietary trade systems or ideas, they are just the framework.  Those items belong in private repos only.

This is not a detailed and robust backtesting/analysis platform nor is it intended to be used by others, it's mostly shared for the sake of having a public "code portfolio" and reference for snippets when discussing Python coding with others.  There is no feature roadmap, I just add things as I need them.

Two main reasons exist for building this.  Mostly it's a personal project to help me learn OOP and develop more robust coding skills beyond what my "Ops Guy" mostly-scripting professional career has provided opportunities for.  The reason to have this project outside of learning was to develop a framework to backtest and analyze trades that include a live drawdown factor in proprietary funded trading accounts ("prop firms") which I did not find when initially reviewing the more popular backtesting libraries available.  It also allows me to approach digging into ideas without the bias inherent towards particular techniques or ideas built into popular systems, in which anything that works is likely to get arbitraged out quickly.

See /docs/ for class details

Note to self - docs are not autoupdating, run mkdocs.sh to update them.  Perhaps this can be a pre-commit hook?

# ES Futures Market Era Analysis

For historical backtesting accuracy, the `MARKET_ERAS` configuration in `dhcharts.py` defines trading hours for different periods of ES futures history (2008-present). The configuration was derived from detailed analysis of 6.3M+ candle records and validated to 99.99% accuracy.

See [ES Market Era Analysis](docs/es_market_era_analysis.md) for:
- Complete trading hour specifications across 4 historical periods
- Methodology for identifying era boundaries from actual candle data
- Validation results confirming configuration accuracy
- Technical details on how trading hours evolved from 2008-2026

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

# Analyzing with trailing drawdown effects in prop firm accounts
The library makes some efforts to assist traders in factoring trailing drawdowns into their analysis process to simulate how real world trading would play out in a prop firm account.  This is primarily handled through on-demand methods on the Trade and TradeSeries object types so that it can be applied without recalculating the backtest.  For traders working in real cash accounts these methods can simply be ignored as they are not applied as defaults.

Trailing drawdowns are assumed to work like Apex Trader Funding style as that's what I'm using.  The details are listed out at https://support.apextraderfunding.com/hc/en-us/articles/4408610260507-How-Does-the-Trailing-Threshold-Work-Master-Course but I will attempt to summarize in my own words here:

* Each account has a maximum trailing threshold amount, often referred to as a "drawdown distance", which determines the account balance dollar amount at which your account will be liquidated and closed if you lose too much money trading.
* This drawdown distance is udpated live as trades are running, tracking the difference between the entry price and the current price until it is locked in by the trade being closed.  It can never exceed the maximum trailing threshold amount, nor can it go below zero as this will liquidate the account.
* When a profitable trade would take the drawdown distance above the maximum trailing threshold amount, the level at which you will be liquidated increases by the same amount as the "overreach" even if you do not close the trade until it pulls back.

Note that this library only factors in eval style drawdowns that never stop trailing, and it does not necessarily trigger liquidation out of the box, allowing the user to simply work with the outputs of the applicable methods to decide when and whether to consider it a failed Trade or TradeSeries in the context of their specific strategy ideas.
