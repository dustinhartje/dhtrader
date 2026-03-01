import pytest
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
# Symbol __eq__ and __ne__ pass and fail scenarios
# Symbol __str__ and __repr__ return strings successfully
# Symbol to.json and to_clean_dict  return correct types and mock values
# Symbol.pretty() (already written below)
# Symbol.set_times() creates correct attributes for ES
# Symbol.get_next_tick_up/down() return correct values including adjacent
#     to day/week/month/holiday closure boundaries, looping several timeframes,
#     eth and rth both
# Symbol.get_next/prev_rth/eth_open/close() return correct values (should
#     run similar tests to get_market_boundaries but use different dates for
#     more coverage.  See if there's a way I can loop this and maybe randomize?
#     Or literally have it check every date for the last year?
#     Maybe have it pick a bunch of random dates then detect the day-of-week
#     to determine what it should test on them.  (do holidays separate and
#     static, just need to ensure random datetimes are outside them)
#     # TODO if this works well maybe revamp get_market_boundaries to also
#            randomize.

# TODO I should probalby have this be a create_symbol() function like other
#      test files?  and it will need to test all the __init__ stuff too
SYMBOL = dhc.Symbol(ticker="ES", name="ES", leverage_ratio=50, tick_size=0.25)


@pytest.mark.storage
# TODO This only requires storage because dhcharts.Symbol.market_is_open uses
#     dhcharts.Event which uses storage to load events.  This could be
#     refactored to allow passing in a list of events and I could generate
#     a static copy of the current events to load and use to speed this up
#     and remove the storage requirement.  In theory.  Some day.
def test_Symbol_market_is_open():
    # ETH
    # Monday through Thursday
    for day in ["13", "14", "15", "16"]:
        date = f"2025-01-{day}"

        # Open at midnight
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:00:00")
        # Open at 12:01am
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:01:00")
        # Open at 9:29am
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:29:00")
        # Open at 9:30am
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:30:00")
        # Open at 4:00pm
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00")
        # Open at 4:59pm
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:59:00")
        # Closed at 5:00pm
        assert not SYMBOL.market_is_open(trading_hours="eth",
                                         target_dt=f"{date} 17:00:00")
        # Closed at 5:59pm
        assert not SYMBOL.market_is_open(trading_hours="eth",
                                         target_dt=f"{date} 17:59:00")
        # Open at 6:00pm
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:00:00")
        # Open at 6:01pm
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:01:00")
        # Open at 11:59pm
        assert SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 23:59:00")
    # Friday
    date = "2025-01-17"
    # Open at midnight
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 00:00:00")
    # Open at 12:01am
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 00:01:00")
    # Open at 9:29am
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:29:00")
    # Open at 9:30am
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:30:00")
    # Open at 4:00pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 16:00:00")
    # Open at 4:59pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 16:59:00")
    # Closed at 5:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:00:00")
    # Closed at 5:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00")
    # Closed at 6:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:00:00")
    # Closed at 6:01pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:01:00")
    # Closed at 11:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 23:59:00")

    # Saturday
    date = "2025-01-18"
    # Closed at midnight and midnight
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:00:00")
    # Closed at 12:01am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:01:00")
    # Closed at 9:29am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:29:00")
    # Closed at 9:30am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:30:00")
    # Closed at 4:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00")
    # Closed at 4:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:59:00")
    # Closed at 5:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:00:00")
    # Closed at 5:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00")
    # Closed at 6:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:00:00")
    # Closed at 6:01pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:01:00")
    # Closed at 11:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 23:59:00")

    # Sunday
    date = "2025-01-19"
    # Closed at midnight
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:00:00")
    # Closed at 12:01am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:01:00")
    # Closed at 9:29am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:29:00")
    # Closed at 9:30am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:30:00")
    # Closed at 4:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00")
    # Closed at 4:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:59:00")
    # Closed at 5:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:00:00")
    # Closed at 5:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00")
    # Opend at 6:00pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 18:00:00")
    # Opend at 6:01pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 18:01:00")
    # Opend at 11:59pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 23:59:00")

    # RTH
    # Monday through Friday
    for day in ["13", "14", "15", "16", "17"]:
        date = f"2025-01-{day}"
        # Closed at midnight
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 00:00:00")
        # Closed at 12:01am
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 00:01:00")
        # Closed at 9:29am
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 09:29:00")
        # Open at 9:30am
        assert SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:30:00")
        # Open at 9:31am
        assert SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:31:00")
        # Open at 12:30pm
        assert SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 12:30:00")
        # Open at 3:59pm
        assert SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 15:59:00")
        # Closed at 4:00pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 16:00:00")
        # Closed at 4:01pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 16:01:00")
        # Closed at 5:59pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 17:59:00")
        # Closed at 6:00pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 18:00:00")
        # Closed at 6:01pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 18:01:00")
        # Closed at 11:59pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 23:59:00")

    # Saturday & Sunday
    for day in ["18", "19"]:
        date = f"2025-01-{day}"
        # Closed at midnight
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 00:00:00")
        # Closed at 12:01am
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 00:01:00")
        # Closed at 9:29am
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 09:29:00")
        # Closed at 9:30am
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 09:30:00")
        # Closed at 9:31am
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 09:31:00")
        # Closed at 12:30pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 12:30:00")
        # Closed at 3:59pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 15:59:00")
        # Closed at 4:00pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 16:00:00")
        # Closed at 4:01pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 16:01:00")
        # Closed at 5:59pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 17:59:00")
        # Closed at 6:00pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 18:00:00")
        # Closed at 6:01pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 18:01:00")
        # Closed at 11:59pm
        assert not SYMBOL.market_is_open(trading_hours="rth",
                                         target_dt=f"{date} 23:59:00")

    # Holidays (will need to pass events for this borred from known)
    events = [dhc.Event(start_dt="2024-02-19 13:00:00",
                        end_dt="2024-02-19 17:59:00",
                        symbol="ES",
                        category="Closed",
                        tags=["holiday"],
                        notes="Presidents Day early close"),
              dhc.Event(start_dt="2024-03-28 17:00:00",
                        end_dt="2024-03-31 17:59:00",
                        symbol="ES",
                        category="Closed",
                        tags=["holiday"],
                        notes="Good Friday closed")
              ]

    # Check a full closure holiday for several times
    date = "2024-03-29"  # Good Friday

    # ETH
    # Closed at midnight
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:00:00",
                                     events=events,
                                     )
    # Closed at 12:01am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 00:01:00",
                                     events=events,
                                     )
    # Closed at 9:29am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:29:00",
                                     events=events,
                                     )
    # Closed at 9:30am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:30:00",
                                     events=events,
                                     )
    # Closed at 9:31am
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 09:31:00",
                                     events=events,
                                     )
    # Closed at 12:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 12:59:00",
                                     events=events,
                                     )
    # Closed at 1:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 13:00:00",
                                     events=events,
                                     )
    # Closed at 1:01pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 13:01:00",
                                     events=events,
                                     )
    # Closed at 3:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 15:59:00",
                                     events=events,
                                     )
    # Closed at 4:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00",
                                     events=events,
                                     )
    # Closed at 4:01pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:01:00",
                                     events=events,
                                     )
    # Closed at 5:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00",
                                     events=events,
                                     )
    # Closed at 6:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:00:00",
                                     events=events,
                                     )
    # Closed at 6:01pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 18:01:00",
                                     events=events,
                                     )
    # Closed at 11:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 23:59:00",
                                     events=events,
                                     )

    # RTH
    # Closed at midnight
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 00:00:00",
                                     events=events,
                                     )
    # Closed at 12:01am
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 00:01:00",
                                     events=events,
                                     )
    # Closed at 9:29am
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:29:00",
                                     events=events,
                                     )
    # Closed at 9:30am
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:30:00",
                                     events=events,
                                     )
    # Closed at 9:31am
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:31:00",
                                     events=events,
                                     )
    # Closed at 12:59pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 12:59:00",
                                     events=events,
                                     )
    # Closed at 1:00pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 13:00:00",
                                     events=events,
                                     )
    # Closed at 1:01pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 13:01:00",
                                     events=events,
                                     )
    # Closed at 3:59pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 15:59:00",
                                     events=events,
                                     )
    # Closed at 4:00pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 16:00:00",
                                     events=events,
                                     )
    # Closed at 4:01pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 16:01:00",
                                     events=events,
                                     )
    # Closed at 5:59pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 17:59:00",
                                     events=events,
                                     )
    # Closed at 6:00pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 18:00:00",
                                     events=events,
                                     )
    # Closed at 6:01pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 18:01:00",
                                     events=events,
                                     )
    # Closed at 11:59pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 23:59:00",
                                     events=events,
                                     )

    # Check an early close holiday for several times
    date = "2024-02-19"

    # ETH
    # Open at midnight
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 00:00:00",
                                 events=events,
                                 )
    # Open at 12:01am
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 00:01:00",
                                 events=events,
                                 )
    # Open at 9:29am
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:29:00",
                                 events=events,
                                 )
    # Open at 9:30am
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:30:00",
                                 events=events,
                                 )
    # Open at 9:31am
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 09:31:00",
                                 events=events,
                                 )
    # Open at 12:59pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 12:59:00",
                                 events=events,
                                 )
    # Closed at 1:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 13:00:00",
                                     events=events,
                                     )
    # Closed at 1:01pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 13:01:00",
                                     events=events,
                                     )
    # Closed at 3:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 15:59:00",
                                     events=events,
                                     )
    # Closed at 4:00pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:00:00",
                                     events=events,
                                     )
    # Closed at 4:01pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 16:01:00",
                                     events=events,
                                     )
    # Closed at 5:59pm
    assert not SYMBOL.market_is_open(trading_hours="eth",
                                     target_dt=f"{date} 17:59:00",
                                     events=events,
                                     )
    # Open at 6:00pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 18:00:00",
                                 events=events,
                                 )
    # Open at 6:01pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 18:01:00",
                                 events=events,
                                 )
    # Open at 11:59pm
    assert SYMBOL.market_is_open(trading_hours="eth",
                                 target_dt=f"{date} 23:59:00",
                                 events=events,
                                 )

    # RTH
    # Closed at midnight
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 00:00:00",
                                     events=events,
                                     )
    # Closed at 12:01am
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 00:01:00",
                                     events=events,
                                     )
    # Closed at 9:29am
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 09:29:00",
                                     events=events,
                                     )
    # Open at 9:30am
    assert SYMBOL.market_is_open(trading_hours="rth",
                                 target_dt=f"{date} 09:30:00",
                                 events=events,
                                 )
    # Open at 9:31am
    assert SYMBOL.market_is_open(trading_hours="rth",
                                 target_dt=f"{date} 09:31:00",
                                 events=events,
                                 )
    # Open at 12:59pm
    assert SYMBOL.market_is_open(trading_hours="rth",
                                 target_dt=f"{date} 12:59:00",
                                 events=events,
                                 )
    # Closed at 1:00pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 13:00:00",
                                     events=events,
                                     )
    # Closed at 1:01pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 13:01:00",
                                     events=events,
                                     )
    # Closed at 3:59pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 15:59:00",
                                     events=events,
                                     )
    # Closed at 4:00pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 16:00:00",
                                     events=events,
                                     )
    # Closed at 4:01pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 16:01:00",
                                     events=events,
                                     )
    # Closed at 5:59pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 17:59:00",
                                     events=events,
                                     )
    # Closed at 6:00pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 18:00:00",
                                     events=events,
                                     )
    # Closed at 6:01pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 18:01:00",
                                     events=events,
                                     )
    # Closed at 11:59pm
    assert not SYMBOL.market_is_open(trading_hours="rth",
                                     target_dt=f"{date} 23:59:00",
                                     events=events,
                                     )


def test_Symbol_get_market_boundary():
    sym = dhc.Symbol(ticker="ES",
                     name="ES",
                     leverage_ratio=50.0,
                     tick_size=0.25,
                     )

    # Testing All boundaries mid-week Wednesday noon datetime
    # 2024-03-20 12:00:00.  This confirms non-weekend mechanics are working
    t = dt_as_dt("2024-03-20 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth")) == "2024-03-20 18:00:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth")) == "2024-03-20 16:59:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth")) == "2024-03-19 18:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth")) == "2024-03-19 16:59:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth")) == "2024-03-21 09:30:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth")) == "2024-03-20 16:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth")) == "2024-03-20 09:30:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth")) == "2024-03-19 16:00:00"

    # Testing Next boundaries from Thursday noon 2024-03-21 12:00:00
    # (should hit Thursday/Friday)")
    # Confirms we don't slip into or over the weekend due to miscalculations
    t = dt_as_dt("2024-03-21 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth")) == "2024-03-21 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth")) == "2024-03-22 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth")) == "2024-03-21 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth")) == "2024-03-21 16:00:00"

    # Testing Next boundaries from Friday noon 2024-03-22 12:00:00
    # (should hit Sunday/Monday)
    # This confirms we span the weekend as expected when appropriate
    t = dt_as_dt("2024-03-22 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth")) == "2024-03-24 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth")) == "2024-03-25 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth")) == "2024-03-22 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth")) == "2024-03-22 16:00:00"

    # Testing Previous boundaries from Tuesday noon 2024-03-19 12:00:00
    # (should hit Monday/Tuesday)
    # Confirms we don't slip into or over the weekend due to miscalculations
    t = dt_as_dt("2024-03-19 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth")) == "2024-03-18 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth")) == "2024-03-19 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth")) == "2024-03-18 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth")) == "2024-03-18 16:00:00"

    # Testing Previous boundaries from Monday noon 2024-03-18 12:00:00
    # (should hit Friday/Sunday)
    # This confirms we span the weekend as expected when appropriate
    t = dt_as_dt("2024-03-18 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth")) == "2024-03-17 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth")) == "2024-03-18 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth")) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth")) == "2024-03-15 16:00:00"

    # Setting up a few events to test that boundary mechanics respect
    events = [dhc.Event(start_dt="2024-03-28 17:00:00",
                        end_dt="2024-03-31 17:59:00",
                        symbol="ES",
                        category="Closed",
                        notes="Good Friday Closed",
                        ),
              dhc.Event(start_dt="2024-03-18 00:00:00",
                        end_dt="2024-03-19 23:59:00",
                        symbol="ES",
                        category="Closed",
                        notes="Tues-Wed Full days closure",
                        ),
              dhc.Event(start_dt="2024-03-18 13:00:00",
                        end_dt="2024-03-18 17:59:00",
                        symbol="ES",
                        category="Closed",
                        notes="Tues early closure",
                        ),
              ]

    # Testing Next against Good Friday closure running Thursday
    # 2024-03-28 17:00:00 through Sunday 2024-03-31 17:59:00

    # Checking from noon on Thursday 2024-03-28 12:00:00
    # This confirms we cross the event and weekend where appropriate.
    t = dt_as_dt("2024-03-28 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-31 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-04-01 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-28 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-28 16:00:00"

    # Testing same closure window from within using Friday at Noon
    # 2024-03-29 12:00:00
    # Confirms times inside a closure are moved outside of it in both direction
    t = dt_as_dt("2024-03-29 12:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-04-01 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-04-01 16:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-28 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-28 16:00:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-31 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-04-01 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-27 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-28 09:30:00"

    # Testing same closure window from the following Monday at Noon
    # 2024-04-01 12:00:00
    # This confirms Previous crosses the event to the prior week.
    t = dt_as_dt("2024-04-01 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-31 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-04-01 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-28 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-28 16:00:00"
    # Testing nested closure window from within both using
    # 2024-03-18 14:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2024-03-18 14:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-20 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-20 16:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 16:00:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-20 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-20 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-17 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 09:30:00"

    # Testing nested closure window from within outer only using
    # 2024-03-18 10:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2024-03-18 10:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-20 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-20 16:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 16:00:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-20 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-20 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-17 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 09:30:00"

    # Testing nested closure window from within outer only using
    # 2024-03-19 06:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2024-03-19 06:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-20 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-20 16:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 16:00:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-20 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-20 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-17 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 09:30:00"


def test_Symbol_get_next_tick_up():
    """Test get_next_tick_up() rounds up to nearest 0.25 (ES tick size)"""
    sym = SYMBOL  # ES with tick_size=0.25

    # Test exact tick boundary - should return same value
    assert sym.get_next_tick_up(100.0) == 100.0
    assert sym.get_next_tick_up(100.25) == 100.25
    assert sym.get_next_tick_up(100.5) == 100.5
    assert sym.get_next_tick_up(100.75) == 100.75

    # Test rounding up to next tick
    assert sym.get_next_tick_up(100.01) == 100.25
    assert sym.get_next_tick_up(100.1) == 100.25
    assert sym.get_next_tick_up(100.2) == 100.25
    assert sym.get_next_tick_up(100.24) == 100.25

    # Test rounding across multiples
    assert sym.get_next_tick_up(100.26) == 100.5
    assert sym.get_next_tick_up(100.51) == 100.75
    assert sym.get_next_tick_up(100.76) == 101.0

    # Test negative values
    assert sym.get_next_tick_up(-100.0) == -100.0
    assert sym.get_next_tick_up(-100.1) == -100.0
    assert sym.get_next_tick_up(-100.24) == -100.0
    assert sym.get_next_tick_up(-100.26) == -100.25

    # Test values near zero
    assert sym.get_next_tick_up(0.0) == 0.0
    assert sym.get_next_tick_up(0.01) == 0.25
    assert sym.get_next_tick_up(0.24) == 0.25
    assert sym.get_next_tick_up(-0.01) == 0.0
    assert sym.get_next_tick_up(-0.24) == 0.0

    # Test decimal precision preservation (should round to 2 decimals)
    assert sym.get_next_tick_up(4987.23) == 4987.25
    assert sym.get_next_tick_up(1000.01) == 1000.25

    # Test large values
    assert sym.get_next_tick_up(9999.99) == 10000.0
    assert sym.get_next_tick_up(5000.01) == 5000.25


def test_Symbol_get_next_tick_down():
    """Test get_next_tick_down() rounds down to nearest 0.25 (ES tick size)"""
    sym = SYMBOL  # ES with tick_size=0.25

    # Test exact tick boundary - should return same value
    assert sym.get_next_tick_down(100.0) == 100.0
    assert sym.get_next_tick_down(100.25) == 100.25
    assert sym.get_next_tick_down(100.5) == 100.5
    assert sym.get_next_tick_down(100.75) == 100.75

    # Test rounding down to previous tick
    assert sym.get_next_tick_down(100.01) == 100.0
    assert sym.get_next_tick_down(100.1) == 100.0
    assert sym.get_next_tick_down(100.2) == 100.0
    assert sym.get_next_tick_down(100.24) == 100.0

    # Test rounding across multiples
    assert sym.get_next_tick_down(100.26) == 100.25
    assert sym.get_next_tick_down(100.51) == 100.5
    assert sym.get_next_tick_down(100.76) == 100.75

    # Test negative values
    assert sym.get_next_tick_down(-100.0) == -100.0
    assert sym.get_next_tick_down(-100.1) == -100.25
    assert sym.get_next_tick_down(-100.24) == -100.25
    assert sym.get_next_tick_down(-100.26) == -100.5

    # Test values near zero
    assert sym.get_next_tick_down(0.0) == 0.0
    assert sym.get_next_tick_down(0.01) == 0.0
    assert sym.get_next_tick_down(0.24) == 0.0
    assert sym.get_next_tick_down(-0.01) == -0.25
    assert sym.get_next_tick_down(-0.24) == -0.25

    # Test decimal precision preservation (should round to 2 decimals)
    assert sym.get_next_tick_down(4987.23) == 4987.0
    assert sym.get_next_tick_down(1000.01) == 1000.0

    # Test large values
    assert sym.get_next_tick_down(9999.99) == 9999.75
    assert sym.get_next_tick_down(5000.01) == 5000.0


def test_Symbol_tick_methods_roundtrip():
    """Test that tick boundaries are idempotent and values roundtrip
    correctly"""
    sym = SYMBOL

    # Test that values already on boundaries stay the same
    for tick in [0.0, 0.25, 0.5, 0.75, 1.0, 100.25, 5000.75]:
        assert sym.get_next_tick_up(tick) == tick
        assert sym.get_next_tick_down(tick) == tick

    # Test that rounding up then down from a boundary returns boundary
    value = 100.1
    rounded_up = sym.get_next_tick_up(value)
    assert rounded_up == 100.25
    # Rounding down an exact boundary stays at that boundary
    rounded_down = sym.get_next_tick_down(rounded_up)
    assert rounded_down == 100.25

    # Test that rounding involves correct nearest boundaries
    # 100.1 should round up to 100.25, and down to 100.0
    rounded_down_direct = sym.get_next_tick_down(100.1)
    assert rounded_down_direct == 100.0

    # 100.6 should round down to 100.5, round up stays at 100.75
    value = 100.6
    rounded_down = sym.get_next_tick_down(value)
    assert rounded_down == 100.5
    rounded_up = sym.get_next_tick_up(value)
    assert rounded_up == 100.75

    # Test idempotent property: rounding twice returns same value
    test_values = [100.1, 100.5, 100.99, 50.23, -100.15]
    for val in test_values:
        # Rounding up twice should equal rounding up once
        up_once = sym.get_next_tick_up(val)
        up_twice = sym.get_next_tick_up(up_once)
        assert up_once == up_twice
        # Same for rounding down
        down_once = sym.get_next_tick_down(val)
        down_twice = sym.get_next_tick_down(down_once)
        assert down_once == down_twice


def test_Symbol_get_era():
    """Test get_era() correctly identifies all 5 market eras based on date"""
    sym = SYMBOL
    import datetime as dt

    # Test Era 1: 2008_thru_2012 (2008-01-01 to 2012-11-16)
    era = sym.get_era("2008-01-01 12:00:00")
    assert era["name"] == "2008_thru_2012"
    assert era["times"]["eth_close"] == dt.time(17, 29, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)

    assert "2008_thru_2012" == sym.get_era("2010-06-15 12:00:00")["name"]
    assert "2008_thru_2012" == sym.get_era("2012-11-16 12:00:00")["name"]

    # Test Era 2: 2012holidays_thru_2015holidays (2012-11-17 to 2015-09-18)
    era = sym.get_era("2012-11-17 12:00:00")
    assert era["name"] == "2012holidays_thru_2015holidays"
    assert era["times"]["eth_close"] == dt.time(17, 15, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)
    # Verify 16:15-16:30 closure on weekdays
    assert era["closed_hours"]["eth"][0] == [
        {"close": "16:15:00", "open": "16:30:00"},
        {"close": "17:16:00", "open": "17:59:59"}
    ]

    era = sym.get_era("2012-11-18 12:00:00")
    assert era["name"] == "2012holidays_thru_2015holidays"
    era = sym.get_era("2015-09-18 12:00:00")
    assert era["name"] == "2012holidays_thru_2015holidays"

    # Test Era 3: 2015holidays_thru_2020 (2015-09-19 to 2020-12-31)
    era = sym.get_era("2015-09-19 12:00:00")
    assert era["name"] == "2015holidays_thru_2020"
    assert era["times"]["eth_close"] == dt.time(16, 59, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)
    # Verify 16:15-16:30 closure on weekdays
    assert era["closed_hours"]["eth"][0] == [
        {"close": "16:15:00", "open": "16:30:00"},
        {"close": "17:00:00", "open": "17:59:59"}
    ]

    era = sym.get_era("2018-06-15 12:00:00")
    assert era["name"] == "2015holidays_thru_2020"
    era = sym.get_era("2020-12-31 12:00:00")
    assert era["name"] == "2015holidays_thru_2020"

    # Test Era 4: 2021-01_thru_2021-06 (2021-01-01 to 2021-06-25)
    era = sym.get_era("2021-01-01 12:00:00")
    assert era["name"] == "2021-01_thru_2021-06"
    assert era["times"]["eth_close"] == dt.time(16, 59, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)
    # Verify 16:15-16:30 closure on weekdays (WITH weekday closure)
    assert era["closed_hours"]["eth"][0] == [
        {"close": "16:15:00", "open": "16:30:00"},
        {"close": "17:00:00", "open": "17:59:59"}
    ]

    assert "2021-01_thru_2021-06" == sym.get_era("2021-03-15 12:00:00")["name"]
    assert "2021-01_thru_2021-06" == sym.get_era("2021-06-25 12:00:00")["name"]

    # Test Era 5: 2021-06_thru_present (2021-06-26 onwards)
    # Critical boundary: 2021-06-26 is where 16:15-16:29 closure was removed
    era = sym.get_era("2021-06-26 12:00:00")
    assert era["name"] == "2021-06_thru_present"
    assert era["times"]["eth_close"] == dt.time(16, 59, 0)
    assert era["times"]["rth_close"] == dt.time(16, 0, 0)
    # Verify 16:15-16:30 closure REMOVED (only 17:00-17:59:59 close)
    assert era["closed_hours"]["eth"][0] == [
        {"close": "17:00:00", "open": "17:59:59"}
    ]

    assert "2021-06_thru_present" == sym.get_era("2021-09-15 12:00:00")["name"]
    assert "2021-06_thru_present" == sym.get_era("2024-06-15 12:00:00")["name"]
    assert "2021-06_thru_present" == sym.get_era("2025-12-31 12:00:00")["name"]

    # Test with datetime objects (not just strings)
    era = sym.get_era(dt.datetime(2010, 5, 1))
    assert era["name"] == "2008_thru_2012"

    era = sym.get_era(dt.datetime(2013, 6, 1))
    assert era["name"] == "2012holidays_thru_2015holidays"

    era = sym.get_era(dt.datetime(2018, 3, 1))
    assert era["name"] == "2015holidays_thru_2020"

    era = sym.get_era(dt.datetime(2021, 3, 1))
    assert era["name"] == "2021-01_thru_2021-06"

    era = sym.get_era(dt.datetime(2024, 5, 1))
    assert era["name"] == "2021-06_thru_present"

    # Test error for date before any era
    with pytest.raises(ValueError, match="No market era defined"):
        sym.get_era("2007-12-31 12:00:00")


def test_Symbol_get_times_for_era():
    """Test get_times_for_era() returns correct market times for all eras"""
    sym = SYMBOL
    import datetime as dt

    # Test 2008_thru_2012 era
    times = sym.get_times_for_era("2008_thru_2012")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(17, 29, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test 2012holidays_thru_2015holidays era
    times = sym.get_times_for_era("2012holidays_thru_2015holidays")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(17, 15, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test 2015holidays_thru_2020 era
    times = sym.get_times_for_era("2015holidays_thru_2020")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(16, 59, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test 2021-01_thru_2021-06 era (WITH 16:15-16:30 closure)
    times = sym.get_times_for_era("2021-01_thru_2021-06")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(16, 59, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test 2021-06_thru_present era (latest, no 16:15-16:30 closure)
    times = sym.get_times_for_era("2021-06_thru_present")
    assert times["eth_open"] == dt.time(18, 0, 0)
    assert times["eth_close"] == dt.time(16, 59, 0)
    assert times["rth_open"] == dt.time(9, 30, 0)
    assert times["rth_close"] == dt.time(16, 0, 0)

    # Test by era dict (2012holidays_thru_2015holidays)
    era = sym.get_era("2014-06-15 12:00:00")
    times = sym.get_times_for_era(era)
    assert times["eth_close"] == dt.time(17, 15, 0)

    # Test error for unknown era name
    with pytest.raises(ValueError, match="Unknown era name"):
        sym.get_times_for_era("nonexistent_era")


def test_Symbol_get_closed_hours_for_era():
    """Test get_closed_hours_for_era() for all 5 MARKET_ERAS"""
    sym = SYMBOL
    import datetime as dt

    # Test 2008_thru_2012 era
    closed = sym.get_closed_hours_for_era("2010-06-15 12:00:00", "eth")
    assert len(closed[0]) == 2
    assert closed[0][0]["close"] == dt.time(16, 16, 0)
    assert closed[0][0]["open"] == dt.time(16, 30, 0)
    assert closed[0][1]["close"] == dt.time(17, 30, 0)

    # Test 2012holidays_thru_2015holidays era
    closed = sym.get_closed_hours_for_era("2014-06-15 12:00:00", "eth")
    assert len(closed[0]) == 2
    assert closed[0][0]["close"] == dt.time(16, 15, 0)
    assert closed[0][0]["open"] == dt.time(16, 30, 0)
    assert closed[0][1]["close"] == dt.time(17, 16, 0)
    assert closed[0][1]["open"] == dt.time(17, 59, 59)

    # Test 2015holidays_thru_2020 era
    closed = sym.get_closed_hours_for_era("2018-06-15 12:00:00", "eth")
    assert len(closed[0]) == 2
    assert closed[0][0]["close"] == dt.time(16, 15, 0)
    assert closed[0][0]["open"] == dt.time(16, 30, 0)
    assert closed[0][1]["close"] == dt.time(17, 0, 0)

    # Test 2021-01_thru_2021-06 era (WITH 16:15-16:30 closure)
    closed = sym.get_closed_hours_for_era("2021-03-15 12:00:00", "eth")
    assert len(closed[0]) == 2
    assert closed[0][0]["close"] == dt.time(16, 15, 0)
    assert closed[0][0]["open"] == dt.time(16, 30, 0)
    assert closed[0][1]["close"] == dt.time(17, 0, 0)

    # Test 2021-06_thru_present era (NO 16:15-16:30 closure)
    closed = sym.get_closed_hours_for_era("2024-06-15 12:00:00", "eth")
    assert len(closed[0]) == 1
    assert closed[0][0]["close"] == dt.time(17, 0, 0)

    # Verify RTH closed consistently across all eras at 16:00
    closed2008 = sym.get_closed_hours_for_era(
        "2010-06-15 12:00:00", "rth")
    closed2024 = sym.get_closed_hours_for_era(
        "2024-06-15 12:00:00", "rth")
    assert closed2008[0][1]["close"] == dt.time(16, 0, 0)
    assert closed2024[0][1]["close"] == dt.time(16, 0, 0)

    # Test caching - call twice and verify same object returned
    closed1 = sym.get_closed_hours_for_era("2024-06-15 12:00:00", "eth")
    closed2 = sym.get_closed_hours_for_era("2024-06-20 12:00:00", "eth")
    assert closed1 is closed2  # Should be same cached object

    # Different era should return different object
    closed3 = sym.get_closed_hours_for_era("2010-06-15 12:00:00", "eth")
    assert closed1 is not closed3

    # Test error for invalid trading_hours
    with pytest.raises(ValueError, match="trading_hours.*not defined"):
        sym.get_closed_hours_for_era("2024-06-15 12:00:00", "invalid")


def test_Symbol_market_is_open_historical_eras():
    """Test market_is_open() across all eras with transitions"""
    sym = SYMBOL

    # Test 2008-2012 era
    date = "2010-03-10"
    assert sym.market_is_open(trading_hours="rth",
                              target_dt=f"{date} 15:59:00",
                              check_closed_events=False)
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt=f"{date} 16:00:00",
                                  check_closed_events=False)
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date} 16:16:00",
                                  check_closed_events=False)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:30:00",
                              check_closed_events=False)

    # Test 2015holidays_thru_2020 era (same 16:15 closure)
    date = "2018-03-14"
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date} 16:15:00",
                                  check_closed_events=False)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:30:00",
                              check_closed_events=False)

    # Test 2021-01_thru_2021-06 era (HAS 16:15-16:30 closure)
    date = "2021-03-10"
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date} 16:15:00",
                                  check_closed_events=False)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:30:00",
                              check_closed_events=False)

    # Critical transition: 2021-06-21 (with closure) vs 2021-06-28 (no)
    # Both are Mondays (day_of_week=0) for fair comparison
    # 2021-06-21 is last Monday of 2021-01_thru_2021-06 era
    date_with = "2021-06-21"
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date_with} 16:15:00",
                                  check_closed_events=False)

    # 2021-06-28 is first Monday of 2021-06_thru_present era
    date_without = "2021-06-28"
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date_without} 16:15:00",
                              check_closed_events=False)

    # Test 2021-06_thru_present era (NO 16:15-16:30 closure)
    date = "2024-03-13"
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt=f"{date} 16:00:00",
                                  check_closed_events=False)
    assert sym.market_is_open(trading_hours="rth",
                              target_dt=f"{date} 15:59:00",
                              check_closed_events=False)
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt=f"{date} 17:00:00",
                                  check_closed_events=False)
    # ETH should be open at 4:15pm in current era (no closure)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:15:00",
                              check_closed_events=False)

    # ETH should be open at 4:15pm in current era (no closure)
    assert sym.market_is_open(trading_hours="eth",
                              target_dt=f"{date} 16:15:00",
                              check_closed_events=False)


def test_Symbol_get_market_boundary_historical_eras():
    """Test get_market_boundary() with historical era dates"""
    sym = SYMBOL

    # Test 2008-2012 era: RTH closes at 16:00
    # Checking from noon on Wednesday 2010-03-10 12:00:00
    t = dt_as_dt("2010-03-10 12:00:00")

    # Next RTH Close should be at 16:00
    result = sym.get_next_close(target_dt=t,
                                trading_hours="rth",
                                adjust_for_events=False)
    assert dt_as_str(result) == "2010-03-10 16:00:00"

    # Previous RTH Close should be at 16:00 previous day
    result = sym.get_previous_close(target_dt=t,
                                    trading_hours="rth",
                                    adjust_for_events=False)
    assert dt_as_str(result) == "2010-03-09 16:00:00"

    # Test 2008-2012 era: ETH closes at 17:29
    # Next ETH Close should be at 17:29 (not 16:59 like current era)
    result = sym.get_next_close(target_dt=t,
                                trading_hours="eth",
                                adjust_for_events=False)
    assert dt_as_str(result) == "2010-03-10 17:29:00"

    # Previous ETH Close should be at 17:29 previous day
    result = sym.get_previous_close(target_dt=t,
                                    trading_hours="eth",
                                    adjust_for_events=False)
    assert dt_as_str(result) == "2010-03-09 17:29:00"

    # Test era transition: date right before change (2012-11-16 is Friday)
    t = dt_as_dt("2012-11-16 12:00:00")
    result = sym.get_next_close(target_dt=t,
                                trading_hours="rth",
                                adjust_for_events=False)
    # Should use old era times (16:00)
    assert dt_as_str(result) == "2012-11-16 16:00:00"

    # Test era transition: Monday after change (2012-11-19 is Monday,
    # 11/17 is Saturday, 11/18 is Sunday)
    # Use Monday to avoid weekend complications
    t = dt_as_dt("2012-11-19 12:00:00")
    result = sym.get_next_close(target_dt=t,
                                trading_hours="rth",
                                adjust_for_events=False)
    # Should use new era times (16:00, same as previous era)
    assert dt_as_str(result) == "2012-11-19 16:00:00"

    # Test era transition: RTH open stays the same across eras
    t1 = dt_as_dt("2012-11-16 08:00:00")
    result1 = sym.get_next_open(target_dt=t1,
                                trading_hours="rth",
                                adjust_for_events=False)
    assert dt_as_str(result1) == "2012-11-16 09:30:00"

    t2 = dt_as_dt("2012-11-19 08:00:00")  # Monday after era change
    result2 = sym.get_next_open(target_dt=t2,
                                trading_hours="rth",
                                adjust_for_events=False)
    # RTH open unchanged across eras (both 9:30)
    assert dt_as_str(result2) == "2012-11-19 09:30:00"


def test_Symbol_init():
    """Test Symbol __init__ creates object with correct attributes"""
    sym = dhc.Symbol(ticker="ES",
                     name="E-mini S&P 500",
                     leverage_ratio=50.0,
                     tick_size=0.25)

    assert sym.ticker == "ES"
    assert sym.name == "E-mini S&P 500"
    assert sym.leverage_ratio == 50.0
    assert sym.tick_size == 0.25

    # Verify set_times() was called during init
    assert hasattr(sym, "eth_open_time")
    assert hasattr(sym, "eth_close_time")
    assert hasattr(sym, "rth_open_time")
    assert hasattr(sym, "rth_close_time")
    assert hasattr(sym, "eth_week_open")
    assert hasattr(sym, "eth_week_close")
    assert hasattr(sym, "rth_week_open")
    assert hasattr(sym, "rth_week_close")
    assert hasattr(sym, "_closed_hours_cache")

    # Test that leverage_ratio and tick_size are converted to float
    sym2 = dhc.Symbol(ticker="DELETEME", name="Test",
                      leverage_ratio="100", tick_size="1.5")
    assert isinstance(sym2.leverage_ratio, float)
    assert sym2.leverage_ratio == 100.0
    assert isinstance(sym2.tick_size, float)
    assert sym2.tick_size == 1.5


def test_Symbol_equality():
    """Test Symbol __eq__ and __ne__ methods"""
    sym1 = dhc.Symbol(ticker="ES", name="ES",
                      leverage_ratio=50, tick_size=0.25)
    sym2 = dhc.Symbol(ticker="ES", name="ES",
                      leverage_ratio=50, tick_size=0.25)
    sym3 = dhc.Symbol(ticker="ES", name="ES",
                      leverage_ratio=50, tick_size=0.5)  # Different tick
    sym4 = dhc.Symbol(ticker="DELETEME", name="DELETEME",
                      leverage_ratio=20, tick_size=0.25)  # Different ticker

    # Test __eq__
    assert sym1 == sym2
    assert not (sym1 == sym3)
    assert not (sym1 == sym4)

    # Test __ne__
    assert not (sym1 != sym2)
    assert sym1 != sym3
    assert sym1 != sym4

    # Test different attributes affect equality
    sym5 = dhc.Symbol(ticker="ES", name="Different Name",
                      leverage_ratio=50, tick_size=0.25)
    assert sym1 != sym5


def test_Symbol_string_representations():
    """Test Symbol __str__, __repr__, and pretty methods"""
    sym = dhc.Symbol(ticker="ES", name="ES",
                     leverage_ratio=50, tick_size=0.25)

    # Test __str__ returns a string
    str_result = str(sym)
    assert isinstance(str_result, str)
    assert "ticker" in str_result
    assert "ES" in str_result

    # Test __repr__ returns a string
    repr_result = repr(sym)
    assert isinstance(repr_result, str)
    assert "ticker" in repr_result

    # Test __str__ and __repr__ are the same
    assert str_result == repr_result

    # Test pretty() returns formatted JSON string
    pretty_result = sym.pretty()
    assert isinstance(pretty_result, str)
    # Should have indentation (newlines and spaces)
    assert "\n" in pretty_result
    assert "    " in pretty_result
    # Should contain key attributes
    assert '"ticker"' in pretty_result
    assert '"ES"' in pretty_result
    assert '"leverage_ratio"' in pretty_result
    assert '"tick_size"' in pretty_result


def test_Symbol_serialization():
    """Test Symbol to_json and to_clean_dict methods"""
    import json
    sym = dhc.Symbol(ticker="ES", name="ES",
                     leverage_ratio=50, tick_size=0.25)

    # Test to_json returns valid JSON string
    json_str = sym.to_json()
    assert isinstance(json_str, str)

    # Should be valid JSON
    parsed = json.loads(json_str)
    assert isinstance(parsed, dict)
    assert parsed["ticker"] == "ES"
    assert parsed["name"] == "ES"
    assert parsed["leverage_ratio"] == 50.0
    assert parsed["tick_size"] == 0.25

    # Time attributes should be strings
    assert isinstance(parsed["eth_open_time"], str)
    assert isinstance(parsed["eth_close_time"], str)
    assert isinstance(parsed["rth_open_time"], str)
    assert isinstance(parsed["rth_close_time"], str)

    # Test to_clean_dict returns dict
    clean_dict = sym.to_clean_dict()
    assert isinstance(clean_dict, dict)
    assert clean_dict["ticker"] == "ES"
    assert clean_dict["leverage_ratio"] == 50.0

    # Should match parsed JSON
    assert clean_dict == parsed


def test_Symbol_set_times():
    """Test Symbol set_times() sets correct values for current era"""
    import datetime as dt
    sym = dhc.Symbol(ticker="ES", name="ES",
                     leverage_ratio=50, tick_size=0.25)

    # Should use latest era times (2021-06_thru_present)
    assert sym.eth_open_time == dt.time(18, 0, 0)
    assert sym.eth_close_time == dt.time(16, 59, 0)
    assert sym.rth_open_time == dt.time(9, 30, 0)
    assert sym.rth_close_time == dt.time(16, 0, 0)

    # Check week schedules
    assert sym.eth_week_open["day_of_week"] == 6  # Sunday
    assert sym.eth_week_open["time"] == dt.time(18, 0, 0)
    assert sym.eth_week_close["day_of_week"] == 4  # Friday
    assert sym.eth_week_close["time"] == dt.time(16, 59, 0)

    assert sym.rth_week_open["day_of_week"] == 0  # Monday
    assert sym.rth_week_open["time"] == dt.time(9, 30, 0)
    assert sym.rth_week_close["day_of_week"] == 4  # Friday
    assert sym.rth_week_close["time"] == dt.time(16, 0, 0)

    # Test unknown ticker raises error
    with pytest.raises(ValueError, match="times have not yet been defined"):
        dhc.Symbol(ticker="UNKNOWN", name="Unknown",
                   leverage_ratio=1, tick_size=0.01)
