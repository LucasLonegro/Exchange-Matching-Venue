package io.lmb.api;

import io.lmb.common.Topology;

import java.util.ServiceLoader;

/**
 * Entry point for every service that talks to the shards. Owns the shared transport resources
 * (media driver, Aeron client). cluster-client provides the production implementation;
 * testkit provides an in-memory one.
 */
public interface VenueClients extends AutoCloseable
{
    interface Provider
    {
        VenueClients connect(Topology topology, String clientName);
    }

    /** Production wiring: finds the provider on the classpath (lmb-cluster-client). */
    static VenueClients connect(final Topology topology, final String clientName)
    {
        return ServiceLoader.load(Provider.class).findFirst()
            .orElseThrow(() -> new IllegalStateException("no VenueClients.Provider on the classpath (is lmb-cluster-client present?)"))
            .connect(topology, clientName);
    }

    int shardCount();

    /** A new command client for the shard. The caller owns and closes it. */
    CommandClient commandClient(int shardId);

    /**
     * A new event stream reader delivering events with eventSeq > fromEventSeq.
     * Use 0 to read from the start of the retained stream, or {@link #LATEST} to start at the live edge.
     */
    EventStreamClient eventStream(int shardId, long fromEventSeq);

    long LATEST = -1L;

    @Override
    void close();
}
