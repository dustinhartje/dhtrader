import site
# This hacky crap is needed to help imports between files in dhtrader
# find each other when run by a script in another folder (even tests).
site.addsitedir('modulepaths')
import dhcharts as dhc
from dhutil import dt_as_dt, dt_as_str, dow_name

# ################################ Symbol() #################################

SYMBOL = dhc.Symbol(ticker="ES", name="ES", leverage_ratio=50, tick_size=0.25)


def test_Symbol_market_is_open():
    # ETH
    # Monday through Thursday
    for day in ["13", "14", "15", "16"]:
        date = f"2025-01-{day}"
        dow = dt_as_dt(f"{date} 00:00:00").weekday()
        name = dow_name(dow)
        print(f"\n==== {date} {dow} {name} ====")

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
    dow = dt_as_dt(f"{date} 00:00:00").weekday()
    name = dow_name(dow)
    name = "Friday"
    print(f"\n==== {date} {dow} {name} ====")
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
    dow = dt_as_dt(f"{date} 00:00:00").weekday()
    name = dow_name(dow)
    print(f"\n==== {date} {dow} {name} ====")
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
    dow = dt_as_dt(f"{date} 00:00:00").weekday()
    name = dow_name(dow)
    print(f"\n==== {date} {dow} {name} ====")
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
        dow = dt_as_dt(f"{date} 00:00:00").weekday()
        name = dow_name(dow)
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
        dow = dt_as_dt(f"{date} 00:00:00").weekday()
        name = dow_name(dow)
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
    dow = dt_as_dt(f"{date} 00:00:00").weekday()
    name = dow_name(dow)

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
    dow = dt_as_dt(f"{date} 00:00:00").weekday()
    name = dow_name(dow)

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

# TODO I've only done the one method so far, review what else Symbols can do
#      and write more tests
