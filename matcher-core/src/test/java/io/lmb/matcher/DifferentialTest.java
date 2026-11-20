package io.lmb.matcher;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Differential test: the real machine and {@link ReferenceMatcher} are fed the same seeded random log
 * (orders, cancels, replaces, duplicates, reference prices, auctions, halts, scheduled transitions,
 * mass cancels, risk changes, day rolls, malformed input) and must emit the same events, entry by entry.
 */
class DifferentialTest
{
    @ParameterizedTest
    @ValueSource(longs = {101, 102, 103, 104, 105, 106, 107, 108, 109, 110})
    void realMachineMatchesReferenceModel(final long seed)
    {
        final Harness h = new Harness();
        h.checkInvariants = false;
        final RandomFlow flow = new RandomFlow(seed, true);
        flow.setup().forEach(h::send);
        RandomFlow.drive(h, flow, 2_500);

        final ReferenceMatcher ref = new ReferenceMatcher(0);
        int ev = 0;
        long trades = 0;
        for (int i = 0; i < h.entries.size(); i++)
        {
            final Harness.Entry e = h.entries.get(i);
            final List<String> expected = e.command() != null ? ref.onCommand(e.command()) : ref.onTimer(e.timerId());
            final List<String> actual = new ArrayList<>();
            for (int k = 0; k < expected.size() && ev + k < h.events.size(); k++)
            {
                actual.add(h.events.get(ev + k).text());
            }
            if (!expected.equals(actual))
            {
                final StringBuilder sb = new StringBuilder("seed " + seed + " diverged at entry " + i + "\nexpected:\n");
                expected.forEach(s -> sb.append("  ").append(s).append('\n'));
                sb.append("actual:\n");
                actual.forEach(s -> sb.append("  ").append(s).append('\n'));
                fail(sb.toString());
            }
            for (final String s : expected)
            {
                if (s.startsWith("TRADE"))
                {
                    trades++;
                }
            }
            ev += expected.size();
        }
        assertTrue(ev == h.events.size(), "all events accounted for");
        assertTrue(trades > 50, "the run traded (" + trades + ")");
    }
}
