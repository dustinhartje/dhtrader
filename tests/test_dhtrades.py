def hide_BacktestEMABounce_test_drawdown_functionality():
    """Test that drawdown impact calculates properly for all scenarios."""
    # TODO consider if this belongs here, perhaps write it in the dhtrader
    #      project test_dhtrades.py instead and trust that has me covered?
    #      or is there a need to double check for these trades specifically?
    # TODO run a few trades on MES in APEX to figure out what actual
    #      results should be expected for these scenarios.
    # TODO long max profit
    # TODO long partial profit (closed after peak and pullback but still ITM)
    # TODO long close at breakeven
    # TODO long close at direct loss (never got green)
    # TODO long close at loss after some green
    # TODO short max profit
    # TODO short partial profit (closed after peak and pullback but still ITM)
    # TODO short close at breakeven
    # TODO short close at direct loss (never got green)
    # TODO short close at loss after some green
