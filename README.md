# Why DHTrader exists and where it's going

My personal classes representing charting and trading objects to be used in backtesting and analysis.  Objects in this repo should never include proprietary trade systems or ideas, they are just the framework.  Those items belong in private repos only.

This is not a detailed and robust backtesting/analysis platform nor is it intended to be used by others, it's mostly shared for the sake of having a public "code portfolio" and reference for snippets when discussing Python coding with others.  There is no feature roadmap, I just add things as I need them.

Two main reasons exist for building this.  Mostly it's a personal project to help me learn OOP and develop more robust coding skills beyond what my "Ops Guy" mostly-scripting professional career has provided opportunities for.  The reason to have this project outside of learning was to develop a framework to backtest and analyze trades that include a live drawdown factor in proprietary funded trading accounts ("prop firms") which I did not find when initially reviewing the more popular backtesting libraries available.  It also allows me to approach digging into ideas without the bias inherent towards particular techniques or ideas built into popular systems, in which anything that works is likely to get arbitraged out quickly.

See /docs/ for class details

Note to self - docs are not autoupdating, run mkdocs.sh to update them.  Perhaps this can be a pre-commit hook?

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
