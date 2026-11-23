package io.lmb.feeds;

import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

/**
 * {@link VenueFeedHandler} for the simulated venues ({@link SimFeedProtocol} over UDP): one
 * non-blocking socket per venue and line, arbitrated by a {@link LineArbitrator} per venue.
 * Receive timestamps are taken from the injected clock as each datagram is read.
 */
public final class UdpVenueFeedHandler implements VenueFeedHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(UdpVenueFeedHandler.class);
    private static final int MAX_DATAGRAMS_PER_SOCKET_PER_POLL = 256;
    private static final int SOCKET_RECEIVE_BUFFER = 1 << 20;

    private final String bindHost;
    private final int basePort;
    private final int venueCount;
    private final EpochNanoClock clock;
    private final long gapTimeoutNs;
    private final long silenceTimeoutNs;

    private final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(64 * 1024);
    private final UnsafeBuffer receiveView = new UnsafeBuffer(receiveBuffer);
    private DatagramChannel[] channels;
    private volatile LineArbitrator[] arbitrators;

    public UdpVenueFeedHandler(
        final String bindHost,
        final int basePort,
        final int venueCount,
        final EpochNanoClock clock,
        final long gapTimeoutNs,
        final long silenceTimeoutNs)
    {
        if (venueCount < 1 || venueCount > FeedsConfig.MAX_VENUES)
        {
            throw new IllegalArgumentException("venueCount " + venueCount);
        }
        this.bindHost = bindHost;
        this.basePort = basePort;
        this.venueCount = venueCount;
        this.clock = clock;
        this.gapTimeoutNs = gapTimeoutNs;
        this.silenceTimeoutNs = silenceTimeoutNs;
    }

    @Override
    public String name()
    {
        return UdpFeedHandlerProvider.NAME;
    }

    @Override
    public int venueCount()
    {
        return venueCount;
    }

    @Override
    public void start(final Listener listener)
    {
        if (channels != null)
        {
            throw new IllegalStateException("already started");
        }
        final LineArbitrator[] arbs = new LineArbitrator[venueCount];
        channels = new DatagramChannel[venueCount * 2];
        try
        {
            for (int v = 0; v < venueCount; v++)
            {
                arbs[v] = new LineArbitrator(v, listener, gapTimeoutNs, silenceTimeoutNs);
                for (int line = 0; line < 2; line++)
                {
                    final DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET);
                    channels[v * 2 + line] = ch;
                    ch.setOption(StandardSocketOptions.SO_RCVBUF, SOCKET_RECEIVE_BUFFER);
                    ch.bind(new InetSocketAddress(bindHost, FeedsConfig.linePort(basePort, v, line)));
                    ch.configureBlocking(false);
                }
            }
        }
        catch (final IOException e)
        {
            close();
            throw new UncheckedIOException("cannot bind feed sockets on " + bindHost + ":" + basePort, e);
        }
        arbitrators = arbs;
        LOG.info("udp-sim feed handler listening on {} ports {}..{} for {} venues",
            bindHost, basePort, basePort + 2 * venueCount - 1, venueCount);
    }

    @Override
    public int poll()
    {
        final LineArbitrator[] arbitrators = this.arbitrators;
        int work = 0;
        try
        {
            for (int v = 0; v < venueCount; v++)
            {
                final LineArbitrator arbitrator = arbitrators[v];
                for (int line = 0; line < 2; line++)
                {
                    final DatagramChannel ch = channels[v * 2 + line];
                    for (int i = 0; i < MAX_DATAGRAMS_PER_SOCKET_PER_POLL; i++)
                    {
                        receiveBuffer.clear();
                        if (ch.receive(receiveBuffer) == null)
                        {
                            break;
                        }
                        final long receiveTsNs = clock.nanoTime();
                        arbitrator.onPacket(line, receiveView, 0, receiveBuffer.position(), receiveTsNs);
                        work++;
                    }
                }
            }
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
        final long now = clock.nanoTime();
        for (int v = 0; v < venueCount; v++)
        {
            work += arbitrators[v].checkTimeouts(now);
        }
        return work;
    }

    /** Arbitration state of a venue (tests and metrics). */
    LineArbitrator arbitrator(final int venueId)
    {
        return arbitrators[venueId];
    }

    @Override
    public void registerMetrics(final PrometheusRegistry registry)
    {
        CounterWithCallback.builder()
            .name("lmb_feeds_line_packets")
            .help("Datagrams received per venue and line (before arbitration)")
            .labelNames("venue", "line")
            .callback(cb -> forEachLine((v, l) -> cb.call(arbitrators[v].linePackets(l), "" + v, lineName(l))))
            .register(registry);
        CounterWithCallback.builder()
            .name("lmb_feeds_line_missed_messages")
            .help("Sequence numbers missing on one line (masked by the other line unless also a venue gap)")
            .labelNames("venue", "line")
            .callback(cb -> forEachLine((v, l) -> cb.call(arbitrators[v].lineMissed(l), "" + v, lineName(l))))
            .register(registry);
        GaugeWithCallback.builder()
            .name("lmb_feeds_line_silence_seconds")
            .help("Time since the last datagram on the line")
            .labelNames("venue", "line")
            .callback(cb ->
            {
                final long now = clock.nanoTime();
                forEachLine((v, l) ->
                {
                    final long last = arbitrators[v].lineLastReceiveNs.get(l);
                    cb.call(last == Long.MIN_VALUE ? Double.NaN : (now - last) / 1e9, "" + v, lineName(l));
                });
            })
            .register(registry);
        CounterWithCallback.builder()
            .name("lmb_feeds_arbitration_duplicates")
            .help("Packets dropped by A/B arbitration as already seen")
            .labelNames("venue")
            .callback(cb ->
            {
                for (int v = 0; arbitrators != null && v < venueCount; v++)
                {
                    cb.call(arbitrators[v].duplicates(), "" + v);
                }
            })
            .register(registry);
        CounterWithCallback.builder()
            .name("lmb_feeds_malformed_packets")
            .help("Datagrams rejected as malformed")
            .labelNames("venue")
            .callback(cb ->
            {
                for (int v = 0; arbitrators != null && v < venueCount; v++)
                {
                    cb.call(arbitrators[v].malformed(), "" + v);
                }
            })
            .register(registry);
    }

    private interface LineVisitor
    {
        void visit(int venue, int line);
    }

    private void forEachLine(final LineVisitor visitor)
    {
        if (arbitrators == null)
        {
            return;
        }
        for (int v = 0; v < venueCount; v++)
        {
            visitor.visit(v, SimFeedProtocol.LINE_A);
            visitor.visit(v, SimFeedProtocol.LINE_B);
        }
    }

    private static String lineName(final int line)
    {
        return line == SimFeedProtocol.LINE_A ? "A" : "B";
    }

    @Override
    public void close()
    {
        if (channels == null)
        {
            return;
        }
        for (final DatagramChannel ch : channels)
        {
            if (ch != null)
            {
                try
                {
                    ch.close();
                }
                catch (final IOException e)
                {
                    LOG.warn("closing feed socket", e);
                }
            }
        }
    }
}
