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
