import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
import dhcharts as dhc
from dhutil import dt_as_dt, dt_as_str, dow_name

# TODO Review the rest of the Symbol class, I have not gone through it
#      thoroughly yet
# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?

# ################################ Symbol() #################################

SYMBOL = dhc.Symbol(ticker="ES", name="ES", leverage_ratio=50, tick_size=0.25)


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
    date = f"2025-01-17"
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
    date = f"2025-01-18"
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
    date = f"2025-01-19"
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


def test_dhcharts_Symbol_market_is_open_transferred():
    # TODO LOWPRI review vs above method, this one was transferred over from
    #      dhcharts.py and likely mostly redundant
    sym = dhc.Symbol(ticker="ES",
                     name="ES",
                     leverage_ratio=50.0,
                     tick_size=0.25,
                     )

    # Sunday 2024-03-17
    # Monday 2024-03-18
    # Wednesday 2024-03-20
    # Friday 2024-03-22
    # Saturday 2024-03-23

    # ETH
    # Saturday should fail at 4am, 5:30pm, and 9pm
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt="2024-03-23 04:00:00")
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt="2024-03-23 17:30:00")
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt="2024-03-23 21:00:00")
    # Friday should succeed at noon, fail after 5pm
    assert sym.market_is_open(trading_hours="eth",
                              target_dt="2024-03-22 12:00:00")
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt="2024-03-22 17:30:00")
    # Sunday should fail at noon, succeed after 6pm
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt="2024-03-17 12:00:00")
    assert sym.market_is_open(trading_hours="eth",
                              target_dt="2024-03-17 19:30:00")
    # Wednesday should succeed at noon and 8pm, fail at 5:30pm
    assert sym.market_is_open(trading_hours="eth",
                              target_dt="2024-03-20 12:00:00")
    assert sym.market_is_open(trading_hours="eth",
                              target_dt="2024-03-20 20:00:00")
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt="2024-03-20 17:30:00")

    # RTH
    # Saturday should fail at 4am, 5:30pm, and 9pm
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-16 04:00:00")
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-16 17:30:00")
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-16 21:00:00")
    # Sunday should fail at 4am, 5:30pm, and 9pm
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-17 04:00:00")
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-17 17:30:00")
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-17 21:00:00")
    # Monday should fail at 8am, succeed after 2pm
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-18 08:00:00")
    assert sym.market_is_open(trading_hours="rth",
                              target_dt="2024-03-18 14:00:00")
    # Friday should succeed at noon, fail after 4pm
    assert sym.market_is_open(trading_hours="rth",
                              target_dt="2024-03-22 12:00:00")
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-22 17:00:00")
    # Wednesday should fail at 4am, succeed at noon, and fail at 8pm
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-20 04:00:00")
    assert sym.market_is_open(trading_hours="rth",
                              target_dt="2024-03-20 12:00:00")
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-20 20:00:00")

    # Market should not indicate open during Closed events like holidays even
    # during market hours.  Using noon on Good Friday to test noon eth and rth
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-29 12:00:00")
    assert not sym.market_is_open(trading_hours="eth",
                                  target_dt="2024-03-29 12:00:00")

    # And just to be sure it's not going to flip somehow test during
    # daily eth closure window on the same holiday:
    assert not sym.market_is_open(trading_hours="rth",
                                  target_dt="2024-03-29 17:30:00")


def test_dhcharts_Symbol_get_market_boundary():
    sym = dhc.Symbol(ticker="ES",
                     name="ES",
                     leverage_ratio=50.0,
                     tick_size=0.25,
                     )

    # Testing All boundaries mid-week Wednesday noon datetime
    # 2024-03-20 12:00:00.  This confirms non-weekend mechanics are working
    t = dt_as_dt("2024-03-20 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_eth_open(t)) == "2024-03-20 18:00:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_eth_close(t)) == "2024-03-20 16:59:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_eth_open(t)) == "2024-03-19 18:00:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_eth_close(t)) == "2024-03-19 16:59:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_rth_open(t)) == "2024-03-21 09:30:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_rth_close(t)) == "2024-03-20 16:14:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_rth_open(t)) == "2024-03-20 09:30:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_rth_close(t)) == "2024-03-19 16:14:00"

    # Testing Next boundaries from Thursday noon 2024-03-21 12:00:00
    # (should hit Thursday/Friday)")
    # Confirms we don't slip into or over the weekend due to miscalculations
    t = dt_as_dt("2024-03-21 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_eth_open(t)) == "2024-03-21 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_rth_open(t)) == "2024-03-22 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_eth_close(t)) == "2024-03-21 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_rth_close(t)) == "2024-03-21 16:14:00"

    # Testing Next boundaries from Friday noon 2024-03-22 12:00:00
    # (should hit Sunday/Monday)
    # This confirms we span the weekend as expected when appropriate
    t = dt_as_dt("2024-03-22 12:00:00")
    # Next ETH Open
    assert dt_as_str(sym.get_next_eth_open(t)) == "2024-03-24 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_rth_open(t)) == "2024-03-25 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_eth_close(t)) == "2024-03-22 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_rth_close(t)) == "2024-03-22 16:14:00"

    # Testing Previous boundaries from Tuesday noon 2024-03-19 12:00:00
    # (should hit Monday/Tuesday)
    # Confirms we don't slip into or over the weekend due to miscalculations
    t = dt_as_dt("2024-03-19 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_eth_open(t)) == "2024-03-18 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_rth_open(t)) == "2024-03-19 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_eth_close(t)) == "2024-03-18 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_rth_close(t)) == "2024-03-18 16:14:00"

    # Testing Previous boundaries from Monday noon 2024-03-18 12:00:00
    # (should hit Friday/Sunday)
    # This confirms we span the weekend as expected when appropriate
    t = dt_as_dt("2024-03-18 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_eth_open(t)) == "2024-03-17 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_rth_open(t)) == "2024-03-18 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_eth_close(t)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_rth_close(t)) == "2024-03-15 16:14:00"

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
    assert dt_as_str(sym.get_next_eth_open(t,
                     events=events)) == "2024-03-31 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_rth_open(t,
                     events=events)) == "2024-04-01 09:30:00"
    # Next ETH Close
    assert dt_as_str(sym.get_next_eth_close(t,
                     events=events)) == "2024-03-28 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_rth_close(t,
                     events=events)) == "2024-03-28 16:14:00"

    # Testing same closure window from within using Friday at Noon
    # 2024-03-29 12:00:00
    # Confirms times inside a closure are moved outside of it in both direction
    t = dt_as_dt("2024-03-29 12:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_eth_close(t,
                     events=events)) == "2024-04-01 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_rth_close(t,
                     events=events)) == "2024-04-01 16:14:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_eth_close(t,
                     events=events)) == "2024-03-28 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_rth_close(t,
                     events=events)) == "2024-03-28 16:14:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_eth_open(t,
                     events=events)) == "2024-03-31 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_rth_open(t,
                     events=events)) == "2024-04-01 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_eth_open(t,
                     events=events)) == "2024-03-27 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_rth_open(t,
                     events=events)) == "2024-03-28 09:30:00"

    # Testing same closure window from the following Monday at Noon
    # 2024-04-01 12:00:00
    # This confirms Previous crosses the event to the prior week.
    t = dt_as_dt("2024-04-01 12:00:00")
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_eth_open(t,
                     events=events)) == "2024-03-31 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_rth_open(t,
                     events=events)) == "2024-04-01 09:30:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_eth_close(t,
                     events=events)) == "2024-03-28 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_rth_close(t,
                     events=events)) == "2024-03-28 16:14:00"
    # Testing nested closure window from within both using
    # 2024-03-18 14:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2024-03-18 14:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_eth_close(t,
                     events=events)) == "2024-03-20 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_rth_close(t,
                     events=events)) == "2024-03-20 16:14:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_eth_close(t,
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_rth_close(t,
                     events=events)) == "2024-03-15 16:14:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_eth_open(t,
                     events=events)) == "2024-03-20 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_rth_open(t,
                     events=events)) == "2024-03-20 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_eth_open(t,
                     events=events)) == "2024-03-17 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_rth_open(t,
                     events=events)) == "2024-03-15 09:30:00"

    # Testing nested closure window from within outer only using
    # 2024-03-18 10:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2024-03-18 10:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_eth_close(t,
                     events=events)) == "2024-03-20 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_rth_close(t,
                     events=events)) == "2024-03-20 16:14:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_eth_close(t,
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_rth_close(t,
                     events=events)) == "2024-03-15 16:14:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_eth_open(t,
                     events=events)) == "2024-03-20 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_rth_open(t,
                     events=events)) == "2024-03-20 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_eth_open(t,
                     events=events)) == "2024-03-17 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_rth_open(t,
                     events=events)) == "2024-03-15 09:30:00"

    # Testing nested closure window from within outer only using
    # 2024-03-19 06:00:00
    # This confirms that multiple overlapping events don't muck it up.
    t = dt_as_dt("2024-03-19 06:00:00")
    # Next ETH Close
    assert dt_as_str(sym.get_next_eth_close(t,
                     events=events)) == "2024-03-20 16:59:00"
    # Next RTH Close
    assert dt_as_str(sym.get_next_rth_close(t,
                     events=events)) == "2024-03-20 16:14:00"
    # Previous ETH Close
    assert dt_as_str(sym.get_previous_eth_close(t,
                     events=events)) == "2024-03-15 16:59:00"
    # Previous RTH Close
    assert dt_as_str(sym.get_previous_rth_close(t,
                     events=events)) == "2024-03-15 16:14:00"
    # Next ETH Open
    assert dt_as_str(sym.get_next_eth_open(t,
                     events=events)) == "2024-03-20 18:00:00"
    # Next RTH Open
    assert dt_as_str(sym.get_next_rth_open(t,
                     events=events)) == "2024-03-20 09:30:00"
    # Previous ETH Open
    assert dt_as_str(sym.get_previous_eth_open(t,
                     events=events)) == "2024-03-17 18:00:00"
    # Previous RTH Open
    assert dt_as_str(sym.get_previous_rth_open(t,
                     events=events)) == "2024-03-15 09:30:00"
