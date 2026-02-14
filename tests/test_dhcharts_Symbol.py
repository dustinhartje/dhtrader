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
                     trading_hours="rth")) == "2024-03-20 16:14:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_open(target_dt=t,
                     trading_hours="rth")) == "2024-03-20 09:30:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth")) == "2024-03-19 16:14:00"

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
                     trading_hours="rth")) == "2024-03-21 16:14:00"

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
                     trading_hours="rth")) == "2024-03-22 16:14:00"

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
                     trading_hours="rth")) == "2024-03-18 16:14:00"

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
                     trading_hours="rth")) == "2024-03-15 16:14:00"

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
                     events=events)) == "2024-03-28 16:14:00"

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
                     events=events)) == "2024-04-01 16:14:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-28 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-28 16:14:00"
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
                     events=events)) == "2024-03-28 16:14:00"
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
                     events=events)) == "2024-03-20 16:14:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 16:14:00"
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
                     events=events)) == "2024-03-20 16:14:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 16:14:00"
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
                     events=events)) == "2024-03-20 16:14:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="eth",
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_close(target_dt=t,
                     trading_hours="rth",
                     events=events)) == "2024-03-15 16:14:00"
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
