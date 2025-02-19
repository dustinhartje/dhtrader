
# TODO Go through dhutil.py every function/class and write out comments
#      here for things that need testing
# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO Check that list against below tests that were transfered from my
#      original manual testing hacks
# TODO write any remaining tests needed here or in class specific files
# TODO remove prompt_yn() after I've converted dhstore.py tests into unittests
#      that's the only place I use it

# TODO tests needed
# test_basics() review and make sure anything it was doing previously is
#     covered, copying out any useful code before recreating
# OperationTimer __init__, __str__, __repr__ similar to other classes
# OperationTimer can probably wrap creation, start, stop, and results
#                into a single function, there's not much to this.  Just make
#                sure it gets correct times (within a few secs of when they are
#                called)  and that results have correct-ish values
# sort_dict() double check it can sort stuff correctly.  what does it
#             even sort by?  key names I guess... should it be renamed to be
#             more clear about what it does while I'm in the neighborhood?
# valid_timeframe() check success for all current timeframes and fail for bad
# valid_trading_hours() ditto
# valid_event_category() ditto
# check_tf_th_compatibility() just test success fail of covered scenarios
# dt_as_*() should return correct formats and fail on bad input
#           -- Confirm fail on incorrectly formatted string dt_to_dt
#           -- Confirm fail on incorrectly formatted string dt_to_str
#              This does not currently fail but should
# dow_name() Check this returns the correct name for each possibility and
#            a reasonable error or None for anything else
# timeframe_delta Check basic success and fail on bad input
# next_candle_start() Check success and fail on rth and eth, various
#                     timeframes, spans across daily closures, weekends,
#                     and event closures
# this_candle_start() check success and fail scenarios
# rangify_candle_times test with a couple of mock setups, don't need to get
#                      too deep here.  make sure it also fails on bad
#                      timeframe or times that aren't formatted right
# generate_zero_volume_candle() ensure success and fail on bad input
# expected_candle_datetimes() Need to review the code in this one but I think
#     I probably just need to mock up what should be expected in a short
#     window for each timeframe and check it returns the expected result.
#     Also check that it doesn't include anything inside closure events
#     or when the market is closed
# remediate_candle_gaps() Need to revisit what this does, what testing is
#    appropriate?
# read_candles_from_csv(), store_candles_from_csv(), compare_candles_vs_csv()
#     can probably combine these into a test that reads, stores, retrieves,
#     and compares to the original csv file
# summarize_candles() likely just do a mockup test here, but review the code
#     and where it gets called from to see if there's anything else I should
#     be confirming  if it's critical to data accuracy
