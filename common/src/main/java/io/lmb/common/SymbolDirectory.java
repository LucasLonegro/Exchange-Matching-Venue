package io.lmb.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Which symbols exist and which shard owns each (prices in cents). The control plane owns them: they are created
 * through its API ({@code POST /api/symbols}), pushed into the shards as SymbolDefinition commands, and served to the
 * other services as a snapshot ({@code GET /internal/v1/symbols}, see {@link SymbolDirectorySource}). Consumers that
 * read the shards' event streams also follow their {@code SymbolDefined} events.
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

    private final String version;
    private final List<Symbol> symbols;
    private final Map<String, Symbol> byTicker = new HashMap<>();
    private final Map<Integer, Symbol> byId = new HashMap<>();

    public SymbolDirectory(final List<Symbol> symbols)
    {
        this("", symbols);
    }

    public SymbolDirectory(final String version, final List<Symbol> symbols)
    {
        this.version = version;
        this.symbols = Collections.unmodifiableList(new ArrayList<>(symbols));
        for (final Symbol s : symbols)
        {
            if (byTicker.put(s.ticker(), s) != null || byId.put(s.symbolId(), s) != null)
            {
                throw new IllegalArgumentException("duplicate symbol: " + s);
            }
        }
    }

    /** The control plane's {@code GET /internal/v1/symbols} body. */
    public static SymbolDirectory fromJson(final String json)
    {
        final Map<String, Object> root = Json.parseObject(json);
        final List<Symbol> out = new ArrayList<>();
        final Object list = root.get("symbols");
        if (list instanceof List<?> l)
        {
            for (final Object o : l)
            {
                final Map<?, ?> m = (Map<?, ?>)o;
                out.add(new Symbol(
                    (int)num(m, "symbolId"), (String)m.get("ticker"), (int)num(m, "shard"), num(m, "tickSize"),
                    num(m, "lotSize"), num(m, "referencePrice"), (int)num(m, "priceBandBps"), num(m, "maxOrderQty"),
                    m.get("dynamicBandBps") == null ? DEFAULT_DYNAMIC_BAND_BPS : (int)num(m, "dynamicBandBps"),
                    m.get("volatilityAuctionNs") == null ? DEFAULT_VOLATILITY_AUCTION_NS : num(m, "volatilityAuctionNs")));
            }
        }
        final Object v = root.get("version");
        return new SymbolDirectory(v == null ? "" : v.toString(), out);
    }

    /**
     * The JSON body for these symbols. The version is a hash of the symbol list, so equal content gives an equal
     * version whichever control plane instance serves it.
     */
    public static String toJson(final List<Symbol> symbols)
    {
        final List<Map<String, Object>> list = maps(symbols);
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("version", sha256Prefix(Json.write(list)));
        root.put("symbols", list);
        return Json.write(root);
    }

    /** The version {@link #toJson} gives these symbols (the server's ETag). */
    public static String contentVersion(final List<Symbol> symbols)
    {
        return sha256Prefix(Json.write(maps(symbols)));
    }

    private static List<Map<String, Object>> maps(final List<Symbol> symbols)
    {
        final List<Map<String, Object>> list = new ArrayList<>();
        for (final Symbol s : symbols)
        {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("symbolId", s.symbolId());
            m.put("ticker", s.ticker());
            m.put("shard", s.shard());
            m.put("tickSize", s.tickSize());
            m.put("lotSize", s.lotSize());
            m.put("referencePrice", s.referencePrice());
            m.put("priceBandBps", s.priceBandBps());
            m.put("maxOrderQty", s.maxOrderQty());
            m.put("dynamicBandBps", s.dynamicBandBps());
            m.put("volatilityAuctionNs", s.volatilityAuctionNs());
            list.add(m);
        }
        return list;
    }

    private static String sha256Prefix(final String s)
    {
        try
        {
            final byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d, 0, 12);
        }
        catch (final NoSuchAlgorithmException e)
        {
            throw new IllegalStateException(e);
        }
    }

    private static long num(final Map<?, ?> m, final String key)
    {
        final Object v = m.get(key);
        if (!(v instanceof Number n))
        {
            throw new IllegalArgumentException("symbol field " + key + " missing or not a number: " + m);
        }
        return n.longValue();
    }

    /** The snapshot version ({@link #toJson}); empty for directories built in memory. */
    public String version()
    {
        return version;
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
