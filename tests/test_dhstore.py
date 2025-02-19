
# TODO Go through dhtrades.py every function/class and write out comments
#      here for things that need testing
# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO Check that list against below tests that were transfered from my
#      original manual testing hacks
# TODO write any remaining tests needed here or in class specific files
# TODO tests needed
# test_basics() review and make sure anything it was doing previously is
#     covered, copying out any useful code before recreating
# list_collections() Ensure it lists a few that should always exist like
#     candles, and probably check that it's length isn't over like 50?
#     -- probably combine with drop_collection() using a dummy test collection
# drop_collection() test carefully by creating a DELETEME collection then
#     dropping it and confirming it's no longer in list
#     --should this even exist in dhstore?  It's kind of mongo specific
#       if anything maybe it should be drop_table or similarly generic
#       and if I keep it shoudl I also have list_tables?  maybe call it stores?
#       buckets?  thingboxes?
# get_symbol_by_ticker() yeah better test that this spits out the right
#     thing with the right type and all that
# combined tests for store_, list_*, review_* get_, delete_ each object type
#     No need to combine linked types in tests, just do each type indivdidually
#     Linked tests should be done at class levels
#     --Trades
#     --TradeSeries
#     --Backtests
#     --Candles
#     --Indicators
#     --IndicatorDataPoints
#     --Events
