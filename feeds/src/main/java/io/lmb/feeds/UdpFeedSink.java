package io.lmb.feeds;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;

/**
 * {@link FeedSimulator.Sink} over UDP unicast: every packet goes to each target (one per consolidator
 * instance, standing in for a multicast group per venue and line) at
 * {@code FeedsConfig.linePort(targetBasePort, venue, line)}. One non-blocking socket.
 */
final class UdpFeedSink implements FeedSimulator.Sink, AutoCloseable
{
    private final DatagramChannel channel;
    private final InetSocketAddress[] addresses; // (target * venues + venue) * 2 + line
    private final int targets;
    private final int venues;

    UdpFeedSink(final List<InetSocketAddress> targetBases, final int venues)
    {
        this.targets = targetBases.size();
        this.venues = venues;
        addresses = new InetSocketAddress[targets * venues * 2];
        for (int t = 0; t < targets; t++)
        {
            final InetSocketAddress base = targetBases.get(t);
            for (int v = 0; v < venues; v++)
            {
                for (int line = 0; line < 2; line++)
                {
                    addresses[(t * venues + v) * 2 + line] =
                        new InetSocketAddress(base.getHostString(), FeedsConfig.linePort(base.getPort(), v, line));
                }
            }
        }
        try
        {
            channel = DatagramChannel.open(StandardProtocolFamily.INET);
            channel.configureBlocking(false);
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException("cannot open feed simulator socket", e);
        }
    }

    @Override
    public boolean send(final int venueId, final int line, final ByteBuffer packet)
    {
        final int start = packet.position();
        boolean ok = true;
        for (int t = 0; t < targets; t++)
        {
            packet.position(start);
            try
            {
                ok &= channel.send(packet, addresses[(t * venues + venueId) * 2 + line]) > 0;
            }
            catch (final IOException e)
            {
                ok = false; // e.g. no route; the consolidator side sees it as loss
            }
        }
        return ok;
    }

    @Override
    public void close()
    {
        try
        {
            channel.close();
        }
        catch (final IOException ignore)
        {
        }
    }
}
