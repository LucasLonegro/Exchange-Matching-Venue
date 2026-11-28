package io.lmb.gateway.core;

import io.lmb.common.SymbolDirectory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The gateway's routing table of symbols: filled from the control plane's symbol snapshot (GatewayCore applies
 * each new one) and from the {@code SymbolDefined} events of every shard's stream, so symbols added or changed
 * through the control plane become routable without a gateway restart. Core thread only.
 */
public final class LiveSymbols
{
    private final Map<String, SymbolDirectory.Symbol> byTicker = new HashMap<>();
    private final Map<Integer, SymbolDirectory.Symbol> byId = new LinkedHashMap<>();

    public LiveSymbols()
    {
    }

    public SymbolDirectory.Symbol byTicker(final String ticker)
    {
        return byTicker.get(ticker);
    }

    public SymbolDirectory.Symbol byId(final int symbolId)
    {
        return byId.get(symbolId);
    }

    public Collection<SymbolDirectory.Symbol> all()
    {
        return new ArrayList<>(byId.values());
    }

    /** A definition seen in shard {@code shard}'s event stream; the stream is the truth for routing. */
    public void onDefined(final int shard, final int symbolId, final String ticker, final long tickSize, final long lotSize,
                          final long referencePrice, final int priceBandBps, final long maxOrderQty)
    {
        final SymbolDirectory.Symbol old = byId.get(symbolId);
        if (old != null && old.shard() == shard && old.ticker().equals(ticker) && old.tickSize() == tickSize &&
            old.lotSize() == lotSize && old.priceBandBps() == priceBandBps && old.maxOrderQty() == maxOrderQty)
        {
            return; // unchanged (reference price updates do not matter to the gateway)
        }
        put(new SymbolDirectory.Symbol(symbolId, ticker, shard, tickSize, lotSize, referencePrice, priceBandBps, maxOrderQty));
    }

    private void put(final SymbolDirectory.Symbol s)
    {
        final SymbolDirectory.Symbol previous = byId.put(s.symbolId(), s);
        if (previous != null && !previous.ticker().equals(s.ticker()))
        {
            byTicker.remove(previous.ticker(), previous);
        }
        byTicker.put(s.ticker(), s);
    }
}
