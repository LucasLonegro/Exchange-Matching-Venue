package io.lmb.replayer;

import io.lmb.api.EventStreamClient;

/** A source of one shard's events in eventSeq order: recorded files or a live {@link EventStreamClient}. */
public interface EventSource extends AutoCloseable
{
    /** Delivers up to {@code limit} events; returns how many (0 when none is available right now). */
    int poll(EventStreamClient.EventHandler handler, int limit);

    /** eventSeq of the last event delivered (or the starting point). */
    long lastEventSeq();

    @Override
    void close();

    /** Adapts a live client. */
    static EventSource of(final EventStreamClient client)
    {
        return new EventSource()
        {
            @Override
            public int poll(final EventStreamClient.EventHandler handler, final int limit)
            {
                return client.poll(handler, limit);
            }

            @Override
            public long lastEventSeq()
            {
                return client.lastEventSeq();
            }

            @Override
            public void close()
            {
                client.close();
            }
        };
    }
}
