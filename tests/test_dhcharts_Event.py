import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
import dhcharts as dhc
from dhutil import dt_as_dt, dt_as_str, dow_name

# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO confirm all other TODOs have been cleared from this file
# TODO Tests needed (some of these have already been written partially/fully
# Event __init__stuff, confirm all attributes, check types, check any
#        scenarios where incorrect values can be passed, check any flags that
#        alter attribute calculations
# Event __str__ and __repr__ return strings successfully
# Event to.json and to_clean_dict  return correct types and mock values
# Event.pretty()
# Event __eq and __ne pass and fail scenarios
# Event create and outputs (to_json, to_clean_dict, pretty, __str__, __repr__
# Event store, retreive, delete
# Event .contains_datetime() check success and fail events, including datetimes
#       exactly at beginning and end of events as well as one candle before
#       and one candle after each.  Test rth and eth both on several timeframes


# TODO Update this to be the new create and test_creation functions
def test_Event_create_and_verify_pretty():
    # Check line counts of pretty output, won't change unless class changes
    out_event = dhc.Event(start_dt="2025-01-02 12:00:00",
                          end_dt="2025-01-02 18:00:00",
                          symbol="ES",
                          category="Closed",
                          tags=["holiday"],
                          notes="Test Holiday",
                          )
    assert isinstance(out_event, dhc.Event)
    assert len(out_event.pretty().splitlines()) == 12
