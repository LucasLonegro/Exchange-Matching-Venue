package io.lmb.marketdata;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.common.Events;

/**
 * Wraps a venue's event streams to behave like the cluster client does at its edges: a shard can be held
 * "connecting" (poll returns 0, isConnected false), a stream can be opened earlier than asked (at-least-once
 * redelivery after a restart) and one eventSeq can be lost (a gap the publisher must refuse).
 */
final class GatedVenue implements VenueClients
{
    private final VenueClients inner;
    private final boolean[] connected;
    volatile long rewind;
    volatile long dropEventSeq = -1;

    GatedVenue(final VenueClients inner)
    {
        this.inner = inner;
        this.connected = new boolean[inner.shardCount()];
        java.util.Arrays.fill(connected, true);
    }

    synchronized void setConnected(final int shard, final boolean value)
    {
        connected[shard] = value;
    }

    private synchronized boolean connected(final int shard)
    {
        return connected[shard];
    }

    @Override
    public int shardCount()
    {
        return inner.shardCount();
    }

    @Override
    public CommandClient commandClient(final int shardId)
    {
        return inner.commandClient(shardId);
    }

    @Override
    public EventStreamClient eventStream(final int shardId, final long fromEventSeq)
    {
        final EventStreamClient s = inner.eventStream(shardId, fromEventSeq == LATEST ? LATEST : Math.max(0, fromEventSeq - rewind));
        return new EventStreamClient()
        {
            public int shardId()
            {
                return shardId;
            }

            public int poll(final EventHandler handler, final int limit)
            {
                if (!connected(shardId))
                {
                    return 0;
                }
                final long drop = dropEventSeq;
                return s.poll((b, o, l) ->
                {
                    if (Events.eventSeq(b, o) != drop)
                    {
                        handler.onEvent(b, o, l);
                    }
                }, limit);
            }

            public long lastEventSeq()
            {
                return s.lastEventSeq();
            }

            public boolean isConnected()
            {
                return connected(shardId) && s.isConnected();
            }

            public void close()
            {
                s.close();
            }
        };
    }

    @Override
    public void close()
    {
    }
}
