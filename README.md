# Why DHTrader exists and where it's going

My personal classes representing charting and trading objects to be used in backtesting and analysis.  Objects in this repo should never include proprietary trade systems or ideas, they are just the framework.  Those items belong in private repos only.

This is not a detailed and robust backtesting/analysis platform nor is it intended to be used by others, it's mostly shared for the sake of having a public "code portfolio" and reference for snippets when discussing Python coding with others.  There is no feature roadmap, I just add things as I need them.

Two main reasons exist for building this.  Mostly it's a personal project to help me learn OOP and develop more robust coding skills beyond what my "Ops Guy" mostly-scripting professional career has provided opportunities for.  The reason to have this project outside of learning was to develop a framework to backtest and analyze trades that include a live drawdown factor in proprietary funded trading accounts ("prop firms") which I did not find when initially reviewing the more popular backtesting libraries available.  It also allows me to approach digging into ideas without the bias inherent towards particular techniques or ideas built into popular systems, in which anything that works is likely to get arbitraged out quickly.

See /docs/ for class details

Note to self - docs are not autoupdating, run mkdocs.sh to update them.  Perhaps this can be a pre-commit hook?
