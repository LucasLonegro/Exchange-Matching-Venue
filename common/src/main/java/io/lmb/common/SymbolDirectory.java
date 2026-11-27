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
 * (prices in cents), optionally followed by {@code dynamicBandBps,volatilityAuctionNs} (band-stp; defaults
 * {@link #DEFAULT_DYNAMIC_BAND_BPS} and {@link #DEFAULT_VOLATILITY_AUCTION_NS} when the columns are absent or
 * empty). When the file has a header line, optional columns are located by name, so their order does not matter.
 * The control plane pushes these into the shards as SymbolDefinition commands.
 */
public final class SymbolDirectory
{
    // band-stp: defaults for the optional dynamic collar columns
    /** Dynamic collar disabled: exactly the static-band-only behaviour. */
    public static final int DEFAULT_DYNAMIC_BAND_BPS = 0;
    /** Five minutes. Only used when dynamicBandBps > 0. */
    public static final long DEFAULT_VOLATILITY_AUCTION_NS = 300_000_000_000L;

    public record Symbol(
        int symbolId, String ticker, int shard, long tickSize, long lotSize,
        long referencePrice, int priceBandBps, long maxOrderQty,
        int dynamicBandBps, long volatilityAuctionNs)
    {
        /** Without the band-stp parameters: dynamic collar disabled. */
        public Symbol(
            final int symbolId, final String ticker, final int shard, final long tickSize, final long lotSize,
            final long referencePrice, final int priceBandBps, final long maxOrderQty)
        {
            this(symbolId, ticker, shard, tickSize, lotSize, referencePrice, priceBandBps, maxOrderQty,
                DEFAULT_DYNAMIC_BAND_BPS, DEFAULT_VOLATILITY_AUCTION_NS);
        }
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
            int dynCol = 8;
            int volCol = 9;
            for (final String raw : Files.readAllLines(csv))
            {
                final String line = raw.trim();
                if (line.isEmpty() || line.startsWith("#"))
                {
                    continue;
                }
                final String[] f = line.split(",", -1);
                if (line.startsWith("symbolId"))
                {
                    dynCol = -1;
                    volCol = -1;
                    for (int i = 8; i < f.length; i++)
                    {
                        final String name = f[i].trim();
                        if (name.equals("dynamicBandBps"))
                        {
                            dynCol = i;
                        }
                        else if (name.equals("volatilityAuctionNs"))
                        {
                            volCol = i;
                        }
                    }
                    continue;
                }
                out.add(new Symbol(
                    Integer.parseInt(f[0].trim()), f[1].trim(), Integer.parseInt(f[2].trim()),
                    Long.parseLong(f[3].trim()), Long.parseLong(f[4].trim()), Long.parseLong(f[5].trim()),
                    Integer.parseInt(f[6].trim()), Long.parseLong(f[7].trim()),
                    (int)optional(f, dynCol, DEFAULT_DYNAMIC_BAND_BPS), optional(f, volCol, DEFAULT_VOLATILITY_AUCTION_NS)));
            }
        }
        catch (final IOException e)
        {
            throw new IllegalStateException("cannot read symbols file " + csv, e);
        }
        return new SymbolDirectory(out);
    }

    private static long optional(final String[] f, final int col, final long dflt)
    {
        if (col < 0 || col >= f.length || f[col].trim().isEmpty())
        {
            return dflt;
        }
        return Long.parseLong(f[col].trim());
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
