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
     * The single transition to command now for a symbol, given what the shard reports, what the calendar
     * (or autoOpen) wants (null = no opinion right now) and the operator override (null, HALTED or AUCTION_CALL).
     */
    public static SymbolState desiredHop(final SymbolState observed, final SymbolState calendarTarget, final String operatorState)
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
            return nextHop(observed, SymbolState.AUCTION_CALL);
        }
        if (calendarTarget == null)
        {
            return null;
        }
        return nextHop(observed, calendarTarget);
    }
}
