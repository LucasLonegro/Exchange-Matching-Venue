package io.lmb.common;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Where a service gets its {@link SymbolDirectory}: the control plane's {@code GET /internal/v1/symbols}, polled
 * (see {@link PolledSnapshot}), or a fixed directory (tests). The control plane is not on the trading path: while
 * both instances are down the last snapshot stays in force, and the cache file carries it across restarts.
 *
 * <p>Topology keys: {@code lmb.symbols.url} (comma-separated base URLs; default every control plane instance's
 * REST port), {@code lmb.symbols.token} (the control plane's {@code lmb.controlplane.serviceToken}) and
 * {@code lmb.symbols.pollMs}.
 */
public final class SymbolDirectorySource extends PolledSnapshot<SymbolDirectory>
{
    /** The local default of {@code lmb.controlplane.serviceToken} / {@code lmb.symbols.token}. */
    public static final String LOCAL_TOKEN = "lmb-local-symbols";

    private static final Codec<SymbolDirectory> CODEC = new Codec<>()
    {
        @Override
        public SymbolDirectory parse(final String json)
        {
            return SymbolDirectory.fromJson(json);
        }

        @Override
        public String version(final SymbolDirectory d)
        {
            return d.version();
        }

        @Override
        public String describe(final SymbolDirectory d)
        {
            return "version " + d.version() + ": " + d.all().size() + " symbols";
        }
    };

    private SymbolDirectorySource(final SymbolDirectory fixed)
    {
        super("symbol directory", CODEC, fixed);
    }

    private SymbolDirectorySource(final List<String> urls, final String token, final long pollMs, final Path cacheFile,
        final String name)
    {
        super("symbol directory", CODEC, urls, "/internal/v1/symbols", token, pollMs, cacheFile, "symbol-directory-" + name);
    }

    public static SymbolDirectorySource fixed(final SymbolDirectory symbols)
    {
        return new SymbolDirectorySource(symbols);
    }

    /** Polls the given control plane base URLs; call {@link #start()}. */
    public static SymbolDirectorySource polling(
        final List<String> urls, final String token, final long pollMs, final Path cacheFile, final String name)
    {
        return new SymbolDirectorySource(urls, token, pollMs, cacheFile, name);
    }

    /** Polls the control plane named by the topology; call {@link #start()}. */
    public static SymbolDirectorySource fromTopology(final Topology t, final Path cacheFile, final String name)
    {
        return polling(urls(t), t.get("lmb.symbols.token", LOCAL_TOKEN), t.getLong("lmb.symbols.pollMs", 5_000),
            cacheFile, name);
    }

    /** {@code lmb.symbols.url}, else {@code http://<host>:<restPort>} of every control plane instance. */
    public static List<String> urls(final Topology t)
    {
        final String explicit = t.get("lmb.symbols.url", null);
        final List<String> out = new ArrayList<>();
        if (explicit != null && !explicit.isBlank())
        {
            for (final String u : explicit.split(","))
            {
                if (!u.isBlank())
                {
                    out.add(u.trim());
                }
            }
            return out;
        }
        for (int i = 0; i < t.controlPlaneInstances(); i++)
        {
            out.add("http://" + t.controlPlaneHost(i) + ":" + t.controlPlaneRestPort(i));
        }
        return out;
    }

    @Override
    public SymbolDirectorySource start()
    {
        super.start();
        return this;
    }
}
