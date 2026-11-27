package io.lmb.controlplane;

import io.lmb.sbe.SymbolState;

import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/** The symbol state transition table of ARCHITECTURE.md section 3.1, and path finding over it. */
public final class SymbolStates
{
    private static final Map<SymbolState, Set<SymbolState>> ALLOWED = new EnumMap<>(SymbolState.class);

    static
    {
        ALLOWED.put(SymbolState.PRE_OPEN, EnumSet.of(SymbolState.AUCTION_CALL, SymbolState.CONTINUOUS, SymbolState.CLOSED));
        ALLOWED.put(SymbolState.AUCTION_CALL, EnumSet.of(SymbolState.CONTINUOUS, SymbolState.HALTED, SymbolState.CLOSED));
        ALLOWED.put(SymbolState.CONTINUOUS, EnumSet.of(SymbolState.HALTED, SymbolState.AUCTION_CALL, SymbolState.CLOSED));
        ALLOWED.put(SymbolState.HALTED, EnumSet.of(SymbolState.AUCTION_CALL, SymbolState.CONTINUOUS, SymbolState.CLOSED));
        ALLOWED.put(SymbolState.CLOSED, EnumSet.of(SymbolState.PRE_OPEN));
        ALLOWED.put(SymbolState.REMOVED, EnumSet.noneOf(SymbolState.class));
    }

    private SymbolStates()
    {
    }

    public static boolean allowed(final SymbolState from, final SymbolState to)
    {
        if (from == null || to == null || to == SymbolState.NULL_VAL || from == SymbolState.NULL_VAL)
        {
            return false;
        }
        if (to == SymbolState.REMOVED)
        {
            return from != SymbolState.REMOVED;
        }
        return ALLOWED.get(from).contains(to);
    }

    /** First step of the shortest path from -> to (never through REMOVED), or null if none / already there. */
    public static SymbolState nextHop(final SymbolState from, final SymbolState to)
    {
        if (from == to || from == SymbolState.REMOVED || to == SymbolState.REMOVED && from == null)
        {
            return null;
        }
        if (allowed(from, to))
        {
            return to;
        }
        final Map<SymbolState, SymbolState> firstStep = new EnumMap<>(SymbolState.class);
        final ArrayDeque<SymbolState> queue = new ArrayDeque<>();
        for (final SymbolState n : ALLOWED.get(from))
        {
            firstStep.put(n, n);
            queue.add(n);
        }
        while (!queue.isEmpty())
        {
            final SymbolState s = queue.poll();
            if (s == to)
            {
                return firstStep.get(s);
            }
            for (final SymbolState n : ALLOWED.get(s))
            {
                if (!firstStep.containsKey(n) && n != from)
                {
                    firstStep.put(n, firstStep.get(s));
                    queue.add(n);
                }
            }
        }
        return null;
    }

    /**
     * {@link #nextHop(SymbolState, SymbolState)} for a symbol that may be in a matcher-initiated volatility auction
     * (AUCTION_CALL entered with cause VOLATILITY_INTERRUPTION). During one, a hop to CONTINUOUS gives null (the
     * matcher's timer ends the auction; commanding CONTINUOUS would cut it short), and a target of AUCTION_CALL gives
     * AUCTION_CALL (the matcher lets a scheduled or operator auction take the volatility auction over, which cancels
     * its timer). HALTED, CLOSED and PRE_OPEN (via CLOSED) go as usual: any such transition ends the volatility auction.
     */
    public static SymbolState nextHop(final SymbolState from, final boolean volatilityAuction, final SymbolState to)
    {
        if (volatilityAuction && from == SymbolState.AUCTION_CALL)
        {
            if (to == SymbolState.AUCTION_CALL)
            {
                return SymbolState.AUCTION_CALL;
            }
            final SymbolState hop = nextHop(from, to);
            return hop == SymbolState.CONTINUOUS ? null : hop;
        }
        return nextHop(from, to);
    }

    /** {@link #desiredHop(SymbolState, boolean, SymbolState, String)} for a symbol not in a volatility auction. */
    public static SymbolState desiredHop(final SymbolState observed, final SymbolState calendarTarget, final String operatorState)
    {
        return desiredHop(observed, false, calendarTarget, operatorState);
    }

    /**
     * The single transition to command now for a symbol, given what the shard reports (state, and whether it is in a
     * volatility auction), what the calendar (or autoOpen) wants (null = no opinion right now) and the operator
     * override (null, HALTED or AUCTION_CALL). Volatility auctions as in {@link #nextHop(SymbolState, boolean, SymbolState)}.
     */
    public static SymbolState desiredHop(final SymbolState observed, final boolean volatilityAuction,
        final SymbolState calendarTarget, final String operatorState)
    {
        if (observed == null || observed == SymbolState.REMOVED)
        {
            return null;
        }
        if ("HALTED".equals(operatorState))
        {
            if (observed == SymbolState.AUCTION_CALL || observed == SymbolState.CONTINUOUS)
            {
                return SymbolState.HALTED;
            }
            if (calendarTarget == SymbolState.CLOSED && observed == SymbolState.HALTED)
            {
                return SymbolState.CLOSED;
            }
            return null;
        }
        if ("AUCTION_CALL".equals(operatorState) && calendarTarget != SymbolState.CLOSED)
        {
            return nextHop(observed, volatilityAuction, SymbolState.AUCTION_CALL);
        }
        if (calendarTarget == null)
        {
            return null;
        }
        return nextHop(observed, volatilityAuction, calendarTarget);
    }
}
