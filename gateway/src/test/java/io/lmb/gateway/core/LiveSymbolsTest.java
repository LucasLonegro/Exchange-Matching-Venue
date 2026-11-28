package io.lmb.gateway.core;

import io.lmb.common.SymbolDirectory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class LiveSymbolsTest
{
    @Test
    void definitionsAddAndChangeSymbols()
    {
        final LiveSymbols s = new LiveSymbols();
        s.onDefined(0, 1, "AAPL", 1, 1, 19_000, 1000, 100_000);  // from the control plane's snapshot
        final SymbolDirectory.Symbol aapl = s.byTicker("AAPL");
        assertNull(s.byTicker("IBM"));

        s.onDefined(0, 9, "IBM", 1, 1, 24_000, 1000, 50_000);   // added through the control plane
        assertEquals(0, s.byTicker("IBM").shard());
        assertEquals(50_000, s.byId(9).maxOrderQty());

        s.onDefined(0, 1, "AAPL", 1, 1, 19_500, 1000, 100_000);  // only the reference price moved
        assertSame(aapl, s.byTicker("AAPL"));
        s.onDefined(0, 1, "AAPL", 1, 1, 19_500, 1000, 10);       // limit changed
        assertEquals(10, s.byTicker("AAPL").maxOrderQty());

        s.onDefined(1, 9, "IBMX", 1, 1, 24_000, 1000, 50_000);   // renamed and moved
        assertNull(s.byTicker("IBM"));
        assertEquals(1, s.byTicker("IBMX").shard());
        assertEquals(2, s.all().size());
    }
}
