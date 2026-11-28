package io.lmb.tools;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.lmb.common.Json;
import io.lmb.common.SymbolDirectory;
import io.lmb.common.Topology;
import io.lmb.proto.orderentry.ListSymbolsRequest;
import io.lmb.proto.orderentry.OrderQueryGrpc;
import io.lmb.proto.orderentry.SymbolInfo;
import io.lmb.tools.client.OrderSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Command-line options ({@code --key value} or {@code --flag}) plus what every tool derives from them:
 * topology ({@code --topology}, else {@code -Dlmb.topology} / {@code LMB_TOPOLOGY}), the gateway endpoint
 * ({@code --gateway N}, or {@code --host}/{@code --port} to override), the symbols (the gateway's
 * {@code ListSymbols}) and the sessions' API keys ({@code --credentials FILE}, else the topology's
 * {@code lmb.tools.credentials}: the file scripts/local-up.sh writes when it seeds the local teams).
 */
public final class ToolContext
{
    private final Map<String, String> opts = new HashMap<>();
    public final Topology topology;
    public final String protocol;
    public final String host;
    public final int port;
    private final int gateway;
    private SymbolDirectory symbols;
    private Map<Long, SessionKey> credentials;

    /** A session's firm and API key, from the credentials file. */
    public record SessionKey(long sessionId, int firmId, String apiKey)
    {
    }

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
        protocol = get("protocol", "grpc");
        gateway = getInt("gateway", 0);
        host = get("host", topology.gatewayHost(gateway));
        port = getInt("port", "fix".equals(protocol) ? topology.gatewayFixPort(gateway) : topology.gatewayGrpcPort(gateway));
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

    /**
     * The gateway's symbols ({@code ListSymbols} on its gRPC port, {@code --grpc-port} to override; retried for
     * up to 15 s while the list is empty). Prices are the listing reference prices.
     */
    public synchronized SymbolDirectory symbols()
    {
        if (symbols != null)
        {
            return symbols;
        }
        final int grpcPort = getInt("grpc-port", "fix".equals(protocol) ? topology.gatewayGrpcPort(gateway) : port);
        final ManagedChannel channel = ManagedChannelBuilder.forAddress(host, grpcPort).usePlaintext().build();
        try
        {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (true)
            {
                final List<SymbolDirectory.Symbol> list = new ArrayList<>();
                for (final SymbolInfo s : OrderQueryGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
                    .listSymbols(ListSymbolsRequest.getDefaultInstance()).getSymbolsList())
                {
                    list.add(new SymbolDirectory.Symbol(s.getSymbolId(), s.getTicker(), s.getShardId(), s.getTickSizeCents(),
                        s.getLotSize(), s.getReferencePriceCents(), 0, 0));
                }
                if (!list.isEmpty() || System.nanoTime() > deadline)
                {
                    symbols = new SymbolDirectory(list);
                    return symbols;
                }
                Thread.sleep(500);
            }
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while listing symbols", e);
        }
        finally
        {
            channel.shutdownNow();
        }
    }

    /** {@code --credentials}, else the topology's {@code lmb.tools.credentials}. */
    public Path credentialsFile()
    {
        final String f = get("credentials", topology.get("lmb.tools.credentials", null));
        if (f == null || f.isBlank())
        {
            throw new IllegalArgumentException("no credentials file: pass --credentials FILE (scripts/local-up.sh writes one " +
                "when it seeds the local teams; its topology names it as lmb.tools.credentials)");
        }
        return Path.of(f);
    }

    /**
     * The credentials file: {@code {"sessions":[{"sessionId":1001,"firmId":100,"apiKey":"lmb_..."}, ...]}}. Keys are
     * issued by the access service; the file is only how a local run hands them to its tools.
     */
    public synchronized Map<Long, SessionKey> credentials()
    {
        if (credentials == null)
        {
            final Path file = credentialsFile();
            final Map<Long, SessionKey> out = new HashMap<>();
            try
            {
                final Object sessions = Json.parseObject(Files.readString(file)).get("sessions");
                if (sessions instanceof List<?> l)
                {
                    for (final Object o : l)
                    {
                        final Map<?, ?> m = (Map<?, ?>)o;
                        final long id = ((Number)m.get("sessionId")).longValue();
                        out.put(id, new SessionKey(id, ((Number)m.get("firmId")).intValue(), (String)m.get("apiKey")));
                    }
                }
            }
            catch (final IOException | RuntimeException e)
            {
                throw new IllegalArgumentException("cannot read credentials file " + file + ": " + e, e);
            }
            credentials = out;
        }
        return credentials;
    }

    public SessionKey sessionKey(final long sessionId)
    {
        final SessionKey k = credentials().get(sessionId);
        if (k == null)
        {
            throw new IllegalArgumentException("session " + sessionId + " has no key in " + credentialsFile());
        }
        return k;
    }

    /** A session for the given venue session id; firm and API key come from the credentials file. */
    public OrderSession session(final long sessionId)
    {
        final SessionKey k = sessionKey(sessionId);
        return OrderSession.create(protocol, host, port, k.firmId(), sessionId, k.apiKey());
    }

    /** Client order ids unique across runs against the same venue: wall-clock millis (mod 10^10) * 10^8 + n. */
    public static long clOrdIdBase()
    {
        return (System.currentTimeMillis() % 10_000_000_000L) * 100_000_000L;
    }
}
