package io.lmb.marketdata;

import io.lmb.marketdata.sbe.HeartbeatEncoder;
import io.lmb.marketdata.sbe.MessageHeaderEncoder;
import io.lmb.marketdata.wire.MdWire;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;
import java.util.function.LongPredicate;

/**
 * One UDP feed (A or B): an incremental channel and a snapshot channel, each to a list of destinations
 * (multicast groups or unicast addresses). Packs whole frames into datagrams of at most maxDatagram bytes;
 * the caller flushes at every batch end, so a batch's last message is never held back. Sends a Heartbeat on
 * a channel idle for heartbeatIntervalMs. Publisher thread only; allocation-free.
 */
public final class FeedPublisher implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(FeedPublisher.class);

    private final class Channel
    {
        final int id;
        final List<InetSocketAddress> destinations;
        final ByteBuffer buffer;
        final UnsafeBuffer view;
        int position = MdWire.PACKET_HEADER_LENGTH;
        int count;
        long packetSeq;
        long lastSendNs;
        LongPredicate dropFilter;

        Channel(final int id, final List<InetSocketAddress> destinations)
        {
            this.id = id;
            this.destinations = destinations;
            this.buffer = ByteBuffer.allocateDirect(maxDatagram);
            this.view = new UnsafeBuffer(buffer);
        }
    }

    private final long sessionId;
    private final int feedId;
    private final int maxDatagram;
    private final long heartbeatIntervalNs;
    private final PublisherMetrics metrics;
    private final DatagramChannel socket;
    private final Channel incremental;
    private final Channel snapshot;
    private final UnsafeBuffer heartbeat = new UnsafeBuffer(new byte[64]);
    private final int heartbeatLength;

    public FeedPublisher(
        final long sessionId, final int feedId, final List<InetSocketAddress> incrementalDestinations,
        final List<InetSocketAddress> snapshotDestinations, final String multicastInterface, final int ttl,
        final int maxDatagram, final long heartbeatIntervalMs, final PublisherMetrics metrics) throws IOException
    {
        this.sessionId = sessionId;
        this.feedId = feedId;
        this.maxDatagram = maxDatagram;
        this.heartbeatIntervalNs = heartbeatIntervalMs * 1_000_000L;
        this.metrics = metrics;
        this.socket = DatagramChannel.open(StandardProtocolFamily.INET);
        socket.configureBlocking(false);
        socket.setOption(StandardSocketOptions.SO_SNDBUF, 4 * 1024 * 1024);
        socket.setOption(StandardSocketOptions.IP_MULTICAST_TTL, ttl);
        socket.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
        if (multicastInterface != null && !multicastInterface.isBlank())
        {
            final NetworkInterface nif = NetworkInterface.getByName(multicastInterface);
            if (nif == null)
            {
                throw new IllegalArgumentException("unknown network interface " + multicastInterface);
            }
            socket.setOption(StandardSocketOptions.IP_MULTICAST_IF, nif);
        }
        this.incremental = new Channel(MdWire.CHANNEL_INCREMENTAL, incrementalDestinations);
        this.snapshot = new Channel(MdWire.CHANNEL_SNAPSHOT, snapshotDestinations);
        final HeartbeatEncoder hb = new HeartbeatEncoder()
            .wrapAndApplyHeader(heartbeat, 0, new MessageHeaderEncoder()).heartbeatIntervalMs(heartbeatIntervalMs);
        this.heartbeatLength = MdWire.HEADER_LENGTH + hb.encodedLength();
    }

    /** Test hook: datagrams of the channel whose packetSeq matches are counted as sent but not sent. */
    void dropFilter(final int channel, final LongPredicate filter)
    {
        (channel == MdWire.CHANNEL_INCREMENTAL ? incremental : snapshot).dropFilter = filter;
    }

    public boolean hasSnapshotChannel()
    {
        return !snapshot.destinations.isEmpty();
    }

    /** Queues one sequenced message (messageHeader + body) on the incremental channel. */
    public void addIncremental(final DirectBuffer buffer, final int offset, final int length, final long nowNs)
    {
        addFrame(incremental, buffer, offset, length, nowNs);
    }

    public void flushIncremental(final long nowNs)
    {
        flush(incremental, nowNs);
    }

    /** Snapshot frames (as written by {@link SnapshotWriter}) on the snapshot channel, then flush. */
    public void sendSnapshot(final DirectBuffer buffer, final int offset, final int length, final long nowNs)
    {
        int pos = offset;
        while (pos < offset + length)
        {
            final int len = MdWire.frameLength(buffer, pos);
            addFrame(snapshot, buffer, pos + MdWire.FRAME_PREFIX_LENGTH, len, nowNs);
            pos += MdWire.FRAME_PREFIX_LENGTH + len;
        }
        flush(snapshot, nowNs);
    }

    /** Heartbeats on idle channels. Returns the number sent. */
    public int heartbeats(final long nowNs)
    {
        return heartbeat(incremental, nowNs) + heartbeat(snapshot, nowNs);
    }

    private int heartbeat(final Channel c, final long nowNs)
    {
        if (c.destinations.isEmpty() || c.count != 0 || nowNs - c.lastSendNs < heartbeatIntervalNs)
        {
            return 0;
        }
        addFrame(c, heartbeat, 0, heartbeatLength, nowNs);
        flush(c, nowNs);
        metrics.heartbeats.incrementAndGet();
        return 1;
    }

    private void addFrame(final Channel c, final DirectBuffer buffer, final int offset, final int length, final long nowNs)
    {
        if (c.destinations.isEmpty())
        {
            return;
        }
        if (c.position + MdWire.FRAME_PREFIX_LENGTH + length > maxDatagram || c.count == 0xFFFF)
        {
            flush(c, nowNs);
        }
        if (MdWire.PACKET_HEADER_LENGTH + MdWire.FRAME_PREFIX_LENGTH + length > maxDatagram)
        {
            throw new IllegalStateException("frame of " + length + " bytes does not fit a datagram of " + maxDatagram);
        }
        MdWire.putFrameLength(c.view, c.position, length);
        c.view.putBytes(c.position + MdWire.FRAME_PREFIX_LENGTH, buffer, offset, length);
        c.position += MdWire.FRAME_PREFIX_LENGTH + length;
        c.count++;
    }

    private void flush(final Channel c, final long nowNs)
    {
        if (c.count == 0)
        {
            return;
        }
        c.packetSeq++;
        MdWire.writePacketHeader(c.view, 0, sessionId, feedId, c.id, c.count, c.packetSeq, System.currentTimeMillis() * 1_000_000L);
        final boolean drop = c.dropFilter != null && c.dropFilter.test(c.packetSeq);
        for (int i = 0; i < c.destinations.size() && !drop; i++)
        {
            c.buffer.limit(c.position).position(0);
            try
            {
                if (socket.send(c.buffer, c.destinations.get(i)) == 0)
                {
                    metrics.sendErrors.incrementAndGet(); // socket buffer full: dropped, receivers recover
                }
                else
                {
                    metrics.bytesSent.addAndGet(c.position);
                }
            }
            catch (final IOException e)
            {
                metrics.sendErrors.incrementAndGet();
                LOG.debug("send to {} failed: {}", c.destinations.get(i), e.toString());
            }
        }
        (c.id == MdWire.CHANNEL_INCREMENTAL ? metrics.incrementalPackets : metrics.snapshotPackets).incrementAndGet();
        c.position = MdWire.PACKET_HEADER_LENGTH;
        c.count = 0;
        c.lastSendNs = nowNs;
    }

    @Override
    public void close()
    {
        try
        {
            socket.close();
        }
        catch (final IOException ignore)
        {
        }
    }
}
