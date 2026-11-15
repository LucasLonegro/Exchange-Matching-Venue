package io.lmb.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static reference data: which symbols exist and which shard owns each. Loaded from a CSV with header
 * {@code symbolId,ticker,shard,tickSize,lotSize,referencePrice,priceBandBps,maxOrderQty}
 * (prices in cents). The control plane pushes these into the shards as SymbolDefinition commands.
 */
public final class SymbolDirectory
{
    public record Symbol(
        int symbolId, String ticker, int shard, long tickSize, long lotSize,
        long referencePrice, int priceBandBps, long maxOrderQty)
    {
    }

    private final List<Symbol> symbols;
    private final Map<String, Symbol> byTicker = new HashMap<>();
    private final Map<Integer, Symbol> byId = new HashMap<>();

    public SymbolDirectory(final List<Symbol> symbols)
    {
        this.symbols = Collections.unmodifiableList(new ArrayList<>(symbols));
        for (final Symbol s : symbols)
        {
            if (byTicker.put(s.ticker(), s) != null || byId.put(s.symbolId(), s) != null)
            {
                throw new IllegalArgumentException("duplicate symbol: " + s);
            }
        }
    }

    public static SymbolDirectory load(final Path csv)
    {
        final List<Symbol> out = new ArrayList<>();
        try
        {
            for (final String raw : Files.readAllLines(csv))
            {
                final String line = raw.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith("symbolId"))
                {
                    continue;
                }
                final String[] f = line.split(",");
                out.add(new Symbol(
                    Integer.parseInt(f[0].trim()), f[1].trim(), Integer.parseInt(f[2].trim()),
                    Long.parseLong(f[3].trim()), Long.parseLong(f[4].trim()), Long.parseLong(f[5].trim()),
                    Integer.parseInt(f[6].trim()), Long.parseLong(f[7].trim())));
            }
        }
        catch (final IOException e)
        {
            throw new IllegalStateException("cannot read symbols file " + csv, e);
        }
        return new SymbolDirectory(out);
    }

    public List<Symbol> all()
    {
        return symbols;
    }

    /** null if unknown. */
    public Symbol byTicker(final String ticker)
    {
        return byTicker.get(ticker);
    }

    /** null if unknown. */
    public Symbol byId(final int symbolId)
    {
        return byId.get(symbolId);
    }
}
