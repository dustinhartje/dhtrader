# Test trades overview

The following trades were performed in an Apex evaluation account to capture true results for use in unit testing.  The drawdown distance and account balance were captured for each trade as well as a few other details to confirm how the fees and drawdowns are calculated

The only thing I was not able to test in this account was what happens when a trade goes into profit beyond it's maximum drawdown distance.  I'll try to do that one in another account that has been reset to get at least one trade to test this with.

Trades were all made against ES and show fees of $3.10/contract (vs $1.04/contract observed for MES)

All trades used 1 contract unless otherwise specified


# Sample trades in order of execution
Contracts: 2
Drawdown distance before: 1757.28
Acct balance before: 245483.93
Drawdown distance after: 1776.08
Acct balance after: 245502.73
Entry price: 5749.50
Entry time: 3/9/2025 11:12:52pm
Peak profit seen: did not record, probably won't matter
Direction: long
Exit price: 5749.75
Exit time: 3/9/2025 11:13:07pm
G/L of: 18.80
Drawdown change: 18.80

not sure if it dipped below entry initiall but it did go up several ticks and then fell back into a trailing stop for 1 tick profit
2 contracts and 1 tick should be worth $25, so g/l of 18.8 implies $3.10/contract in fees
long trade with more than 1 contract
long partial profit (closed after peak and pullback but still ITM)
--------------------------------------------------------------------
Drawdown distance before: 1776.08
Acct balance before: 245502.73
Drawdown distance after: 1735.48
Acct balance after: 245462.13
Entry price: 5749.25
Entry time:3/9/25 11:18:05pm
Peak profit seen: 1 tick over entry
Direction: long
Exit price: 5748.50
Exit time: 3/9/2025 11:20:03pm
G/L of: -40.6
Drawdown change: -40.6

dropped 1 tick under entry iniitally and got at least 1 tick over as well before falling to stop target for "max loss"
closed 3 ticks below entry so raw loss would be 37.50.  $3.10 fee per contract
confirmed here
long close at loss after some green
--------------------------------------------------------------------
Drawdown distance before: 1735.48
Acct balance before: 245462.13
Drawdown distance after: 1732.38
Acct balance after: 245459.03
Entry price: 5746.0
Entry time:3/9/2025 11:27:33pm
Peak profit seen: 4 ticks (but didn't fill)
Direction: long
Exit price: 5746
Exit time: 3/9/2025 11:29:09pm
G/L of: -3.10
Drawdown change: -3.10

went up to profit target but didn't fill then fell back below entry.  I moved
profit target down to BE and it popped back up for the fill
long close at breakeven after some downside seen
--------------------------------------------------------------------
Drawdown distance before: 1732.38
Acct balance before: 245459.03
Drawdown distance after: 1754.28
Acct balance after: 245480.93
Entry price: 5746.25
Entry time:3/9/2025 11:31:05pm
Peak profit seen: 2 ticks
Direction: long
Exit price: 5746.75
Exit time: 3/9/2025 11:31:31 pm
G/L of: 21.90
Drawdown change: 21.90

never went red, straight to profit target
long max profit
--------------------------------------------------------------------
3 contracts

Drawdown distance before: 1779.38
Acct balance before: 245506.03
Drawdown distance after: 1882.58
Acct balance after: 245609.23
Entry price: 5752.25
Entry time: 3/9/2025 11:52:44pm
Peak profit seen: 3 ticks (exit)
Direction: short
Exit price: 5751.50
Exit time: 3/9/2025 11:54:04-13 (partial fills from multi contracts)
G/L of: 103.20
Drawdown change: 103.20

went to max profit with no downside seen
3 contracts * 3 ticks should see 37.5 profit per tick for 112.50 total
minues 3.10 * 3 contracts in fees = 103.20 just as shown

short max profit
short trade with more than 1 contract
--------------------------------------------------------------------
Drawdown distance before: 1929.48
Acct balance before: 245656.13
Drawdown distance after: 1901.38
Acct balance after: 245628.03
Entry price: 5751.25
Entry time: 3/10/25 12:01:01am
Peak profit seen: 0 ticks
Direction: long
Exit price: 5750.75
Exit time: 3/10/2025 12:01:43pm
G/L of: -28.10
Drawdown change: -28.10
never say any green, straight to stop target

long close at direct loss (never got green)
--------------------------------------------------------------------
Drawdown distance before: 1901.38
Acct balance before: 245628.03
Drawdown distance after: 1835.78
Acct balance after: 245562.43
Entry price: 5750.50
Entry time: 3/10/2025 12:04:05am
Peak profit seen: 0 ticks
Direction: short
Exit price: 5751.75
Exit time: 3/10/2025 12:05:12am
G/L of: -65.6
Drawdown change: -65.6
no profit seen, straight to stop target
short close at direct loss (never got green)
--------------------------------------------------------------------
Drawdown distance before: 1835.78
Acct balance before: 245562.43
Drawdown distance after: 1807.68
Acct balance after: 245534.33
Entry price: 5752.75
Entry time: 3/10/2025 12:10:03am
Peak profit seen: 2 ticks
Direction: short
Exit price: 5753.25
Exit time: 3/10/2025 12:11:08am
G/L of: -28.10
Drawdown change: -28.10
got open of candle, initially went into green 2 ticks then flipped red
short close at loss after some green
--------------------------------------------------------------------
Drawdown distance before: 1807.68
Acct balance before: 245534.33
Drawdown distance after: 1804.58
Acct balance after: 245531.23
Entry price: 5754.25
Entry time: 3/10/2025 12:12:56am
Peak profit seen: 0 ticks (I think?)
Direction: short
Exit price: 5754.25
Exit time: 3/10/2025 12:13:54am
G/L of: -3.10
Drawdown change: -3.10
initially went negative 2 ticks, caught at breakeven
short close at breakeven after some downside seen
--------------------------------------------------------------------
Drawdown distance before: 1804.58
Acct balance before: 245531.23
Drawdown distance after: 1826.48
Acct balance after: 245553.13
Entry price: 5754.50
Entry time:3/10/2025 12:16:24am
Peak profit seen: 4 ticks
Direction: short
Exit price: 5754.00
Exit time: 3/10/2025 12:19:58am
G/L of: 21.90
Drawdown change: 21.90
short partial profit (closed after peak and pullback but still ITM)

=======================================================================
Using a few reset accounts to try to get trades around max drawdowns
=======================================================================
Account #84
Drawdown distance before: 6500 
Acct balance before: 250000
Drawdown distance after: 6234.40
Acct balance after: 249734.40
Entry price: 5756.25
Entry time: 3/10/2025 12:30:02am
Peak profit seen: 0 ticks
Peak loss seen: 5750.50 (23 ticks from entry, 2 ticks past exit)
Direction: long
Exit price: 5751
Exit time: 3/10/2025 12:37:03
G/L of: -265.60
Drawdown change: -265.60
Trade went directly into loss, let it fall and then got a bit of a pull back and exited so this is a trade that went from max drawdown directly into loss, got somewonat of a pullback, and was exited at a loss but not max loss.  Result was even changes on both drawdown and pullback


--------------------------------------------------------------------
Account #84
Drawdown distance before: 6234.40
Acct balance before: 249734.40
Drawdown distance after: 6448.45
Acct balance after: 249993.80
Entry time: 3/10/2025 12:40:41am
Entry price: 5752.50
Direction: short
Exit price: 5747.25
Exit time: 3/10/2025 12:54:10am
G/L of: 259.40
Drawdown change: 214.05
Observed trailing change (G/L - Drawdown change) = 45.35

Started under max drawdown, pushed through it by a few ticks, then fell under it to get closed by my trailing stop.  This trade shows how breaching the max drawdown triggers a trailing distance and resulting in Gain/Loss and Drawdown Distance changes not being equal for such trades because drawdown distance gain was limited by the max drawdown while Gain continued to increase, then both fell back equally from the peak profit seen.


trade starting below max drawdown, pushing past it, then falling back to close at least a few ticks below it
--------------------------------------------------------------------
Account #85
Drawdown distance before: 6500
Acct balance before: 250000
Drawdown distance after: 6500
Acct balance after: 250034.40
Entry price: 5748.25
Entry time: 3/10/2025 12:52:13am
Peak profit seen: 3 ticks
Direction: short
Exit price: 5747.50
Exit time: 3/10/2025 12:53:35am
G/L of: 34.40
Drawdown change: 0

trade starting at max drawdown and ending in profit, should result in 0 drawdown distance change but a positive gain/loss
--------------------------------------------------------------------
Account #84

Contracts: 2
Drawdown distance before: 6161.05
Acct balance before: 249706.40
Drawdown distance after: 6321.90
Acct balance after: 250275.20
Entry price: 5625.75
Entry time: 3/14/2025 14:36:49
Peak profit seen: 5633.75
Direction: long
Exit price: 5631.50
Exit time: 3/14/2025 14:41:21
G/L of: 568.80
Drawdown change: 160.85
Observed trailing change (G/L - Drawdown change) = 407.95

long trade, multiple contracts, goes into drawdown_limit then partial pullback
--------------------------------------------------------------------
Account #84

Contracts: 3
Drawdown distance before: 6321.90
Acct balance before: 250275.20
Drawdown distance after: 6382.85
Acct balance after: 250378.40
Entry price: 5629.50
Entry time: 3/14/2025 14:51:28
Peak profit seen: 5627.50 
Direction: short
Exit price: 5628.75
Exit time: 3/14/2025 14:52:22
G/L of: 112.20
Drawdown change: 60.95
Observed trailing change (G/L - Drawdown change) = 51.25

new short trade to check since first failed me, multiple contracts, goes into drawdown_limit then partial pullback
