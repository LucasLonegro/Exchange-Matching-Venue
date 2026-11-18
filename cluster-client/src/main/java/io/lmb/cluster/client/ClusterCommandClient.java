package io.lmb.cluster.client;

import io.aeron.Aeron;
import io.aeron.cluster.client.AeronCluster;
import io.lmb.api.CommandClient;
import io.lmb.common.Topology;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CommandClient} over an Aeron Cluster ingress session. Aeron handles leader redirects during
 * connect and NewLeader events afterwards; when the session is lost entirely this client reconnects
 * asynchronously from {@link #poll()}. {@link #offer} never blocks: it returns false on back pressure,
 * while disconnected and during elections.
 */
public final class ClusterCommandClient implements CommandClient
{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterCommandClient.class);

    private final int shardId;
    private final AeronCluster.Context template;
    private final ClientConfig config;
    private final Aeron aeron;

    private AeronCluster cluster;
    private AeronCluster.AsyncConnect connecting;
    private long nextConnectAttemptNs;
    private long lastSendNs;
    private long connectCount;
    private boolean closed;

    ClusterCommandClient(
        final Aeron aeron, final Topology topology, final int shardId, final ClientConfig config, final String name)
    {
        this.aeron = aeron;
        this.shardId = shardId;
        this.config = config;
        this.template = new AeronCluster.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .ingressEndpoints(topology.ingressEndpoints(shardId))
            .ingressChannel("aeron:udp?term-length=" + config.termLength)
            .egressChannel("aeron:udp?endpoint=" + config.clientHost + ":0|term-length=" + config.termLength)
            .messageTimeoutNs(config.messageTimeoutNs)
            .credentialsSupplier(SharedSecret.credentials(config.clusterSecret))
            .idleStrategy(new BackoffIdleStrategy())
            .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
            })
            .clientName("lmb-" + name + "-shard-" + shardId);
        poll();
    }

    @Override
    public int shardId()
    {
        return shardId;
    }

    @Override
    public boolean offer(final DirectBuffer buffer, final int offset, final int length)
    {
        final AeronCluster c = cluster;
        if (c == null || c.isClosed() || !c.egressSubscription().isConnected())
        {
            // no live egress from a leader (it died or an election is in progress): anything offered now
            // would most likely be lost, so report it as not accepted
            return false;
        }
        final long result = c.offer(buffer, offset, length);
        if (result > 0)
        {
            lastSendNs = aeron.context().nanoClock().nanoTime();
            return true;
        }
        return false;
    }

    @Override
    public int poll()
    {
        if (closed)
        {
            return 0;
        }
        int work = 0;
        final long now = aeron.context().nanoClock().nanoTime();
        if (cluster != null)
        {
            try
            {
                work += cluster.pollEgress();
            }
            catch (final RuntimeException e)
            {
                LOG.warn("shard {}: egress poll failed: {}", shardId, e.toString());
                CloseHelper.quietClose(cluster);
            }
            if (cluster.isClosed())
            {
                LOG.info("shard {}: cluster session lost, reconnecting", shardId);
                cluster = null;
                nextConnectAttemptNs = now;
            }
            else if (now - lastSendNs >= config.keepAliveIntervalNs)
            {
                if (cluster.sendKeepAlive())
                {
                    lastSendNs = now;
                }
            }
        }
        if (cluster == null)
        {
            if (connecting == null && now - nextConnectAttemptNs >= 0)
            {
                connecting = AeronCluster.asyncConnect(template.clone());
                work++;
            }
            if (connecting != null)
            {
                try
                {
                    final AeronCluster c = connecting.poll();
                    if (c != null)
                    {
                        connecting = null;
                        cluster = c;
                        lastSendNs = now;
                        connectCount++;
                        LOG.info("shard {}: connected to cluster, leader={} sessionId={}",
                            shardId, c.leaderMemberId(), c.clusterSessionId());
                        work++;
                    }
                }
                catch (final RuntimeException e)
                {
                    LOG.debug("shard {}: connect attempt failed: {}", shardId, e.toString());
                    CloseHelper.quietClose(connecting);
                    connecting = null;
                    nextConnectAttemptNs = now + config.reconnectBackoffNs;
                }
            }
        }
        return work;
    }

    @Override
    public boolean isConnected()
    {
        final AeronCluster c = cluster;
        return c != null && !c.isClosed() && c.ingressPublication() != null && c.ingressPublication().isConnected() &&
            c.egressSubscription().isConnected();
    }

    /** Member id of the leader this client last connected to or was redirected to, or -1. */
    public int leaderMemberId()
    {
        final AeronCluster c = cluster;
        return c == null ? -1 : c.leaderMemberId();
    }

    /** Leadership term of the current session, or -1 when disconnected. */
    public long leadershipTermId()
    {
        final AeronCluster c = cluster;
        return c == null ? -1 : c.leadershipTermId();
    }

    /** Number of cluster sessions established so far (reconnects + 1). */
    public long connectCount()
    {
        return connectCount;
    }

    @Override
    public void close()
    {
        if (!closed)
        {
            closed = true;
            CloseHelper.quietClose(connecting);
            CloseHelper.quietClose(cluster);
            connecting = null;
            cluster = null;
        }
    }
}
