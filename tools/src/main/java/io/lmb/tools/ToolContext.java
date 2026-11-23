package io.lmb.tools;

import io.lmb.common.FirmDirectory;
import io.lmb.common.SymbolDirectory;
import io.lmb.common.Topology;
import io.lmb.tools.client.OrderSession;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Command-line options ({@code --key value} or {@code --flag}) plus what every tool derives from them:
 * topology ({@code --topology}, else {@code -Dlmb.topology} / {@code LMB_TOPOLOGY}), reference data, and the
 * gateway endpoint ({@code --gateway N}, or {@code --host}/{@code --port} to override).
 */
public final class ToolContext
{
    private final Map<String, String> opts = new HashMap<>();
    public final Topology topology;
    public final SymbolDirectory symbols;
    public final FirmDirectory firms;
    public final String protocol;
    public final String host;
    public final int port;

    public ToolContext(final String[] args)
    {
        for (int i = 0; i < args.length; i++)
        {
            final String a = args[i];
            if (!a.startsWith("--"))
            {
                throw new IllegalArgumentException("unexpected argument '" + a + "'");
            }
            final String key = a.substring(2);
            final int eq = key.indexOf('=');
            if (eq > 0)
            {
                opts.put(key.substring(0, eq), key.substring(eq + 1));
            }
            else if (i + 1 < args.length && !args[i + 1].startsWith("--"))
            {
                opts.put(key, args[++i]);
            }
            else
            {
                opts.put(key, "true");
            }
        }
        topology = opts.containsKey("topology") ? Topology.load(Path.of(opts.get("topology"))) : Topology.load();
        symbols = SymbolDirectory.load(topology.symbolsFile());
        firms = FirmDirectory.load(topology.firmsFile());
        protocol = get("protocol", "grpc");
        final int gw = getInt("gateway", 0);
        host = get("host", topology.gatewayHost(gw));
        port = getInt("port", "fix".equals(protocol) ? topology.gatewayFixPort(gw) : topology.gatewayGrpcPort(gw));
    }

    public String get(final String key, final String defaultValue)
    {
        return opts.getOrDefault(key, defaultValue);
    }

    public int getInt(final String key, final int defaultValue)
    {
        return opts.containsKey(key) ? Integer.parseInt(opts.get(key)) : defaultValue;
    }

    public long getLong(final String key, final long defaultValue)
    {
        return opts.containsKey(key) ? Long.parseLong(opts.get(key)) : defaultValue;
    }

    public boolean has(final String key)
    {
        return opts.containsKey(key);
    }

    /** A session for the given venue session id; firm and API key come from the firms file. */
    public OrderSession session(final long sessionId)
    {
        final FirmDirectory.Firm firm = firms.bySession(sessionId);
        if (firm == null)
        {
            throw new IllegalArgumentException("session " + sessionId + " is not in " + topology.firmsFile());
        }
        return OrderSession.create(protocol, host, port, firm.firmId(), sessionId, firm.apiKey());
    }

    /** Client order ids unique across runs against the same venue: wall-clock millis (mod 10^10) * 10^8 + n. */
    public static long clOrdIdBase()
    {
        return (System.currentTimeMillis() % 10_000_000_000L) * 100_000_000L;
    }
}
