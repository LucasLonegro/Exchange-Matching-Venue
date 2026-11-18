package io.lmb.cluster.client;

import io.lmb.common.Topology;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Settings of {@link ClusterVenueClients}. {@link #from(Topology)} reads them from the topology /
 * system properties / environment (see docs/modules/cluster.md for the keys); fields are public and
 * mutable so tests and embedding code can adjust them before connecting.
 */
public final class ClientConfig
{
    /** Address the members use to reach this process (egress, event responses and replays). */
    public String clientHost = "127.0.0.1";
    /** External media driver directory; null launches an embedded driver. */
    public String aeronDir;
    /** Parent directory for the embedded driver's directory; null picks /dev/shm or java.io.tmpdir. */
    public String embeddedDirBase;
    /** Term length of the embedded driver's publications. */
    public int termLength = 256 * 1024;
    /** Shared secret sent as cluster ingress credentials; null sends none. */
    public String clusterSecret;

    /**
     * Embedded driver only: how long a peer may be silent before a publication to it counts as not
     * connected (and an image from it as gone). Shorter means offers to a dead leader fail sooner.
     */
    public long peerTimeoutNs = TimeUnit.SECONDS.toNanos(2);
    public long messageTimeoutNs = TimeUnit.SECONDS.toNanos(5);
    public long keepAliveIntervalNs = TimeUnit.SECONDS.toNanos(1);
    public long reconnectBackoffNs = TimeUnit.MILLISECONDS.toNanos(200);

    public long eventConnectTimeoutNs = TimeUnit.SECONDS.toNanos(2);
    public long eventHeartbeatTimeoutNs = TimeUnit.SECONDS.toNanos(3);
    public long eventStallTimeoutNs = TimeUnit.SECONDS.toNanos(5);
    public long eventKeepAliveIntervalNs = TimeUnit.MILLISECONDS.toNanos(250);
    public long eventMemberCooldownNs = TimeUnit.SECONDS.toNanos(1);
    /** Member to try first; -1 prefers the last known leader. */
    public int eventPreferredMember = -1;
    /** Move from a follower to the leader after this long when caught up; 0 disables. */
    public long eventRehomeAfterNs = TimeUnit.SECONDS.toNanos(10);

    public static ClientConfig from(final Topology t)
    {
        final ClientConfig c = new ClientConfig();
        c.clientHost = t.get("lmb.client.host", c.clientHost);
        final boolean embedded = t.getBool("lmb.client.driver.embedded", t.get("lmb.aeron.dir", null) == null);
        c.aeronDir = embedded ? null : t.get("lmb.aeron.dir", null);
        c.embeddedDirBase = t.get("lmb.client.aeron.dir.base", null);
        c.termLength = parseSize(t.get("lmb.client.termLength", "256k"));
        c.clusterSecret = t.get("lmb.cluster.secret", null);
        c.peerTimeoutNs = ms(t.getLong("lmb.client.peerTimeoutMs", 2000));
        c.messageTimeoutNs = ms(t.getLong("lmb.client.messageTimeoutMs", 5000));
        c.keepAliveIntervalNs = ms(t.getLong("lmb.client.keepAliveIntervalMs", 1000));
        c.reconnectBackoffNs = ms(t.getLong("lmb.client.reconnectBackoffMs", 200));
        c.eventConnectTimeoutNs = ms(t.getLong("lmb.events.connectTimeoutMs", 2000));
        c.eventHeartbeatTimeoutNs = ms(t.getLong("lmb.events.heartbeatTimeoutMs", 3000));
        c.eventStallTimeoutNs = ms(t.getLong("lmb.events.stallTimeoutMs", 5000));
        c.eventKeepAliveIntervalNs = ms(t.getLong("lmb.events.keepAliveIntervalMs", 250));
        c.eventMemberCooldownNs = ms(t.getLong("lmb.events.memberCooldownMs", 1000));
        c.eventPreferredMember = t.getInt("lmb.events.preferredMember", -1);
        c.eventRehomeAfterNs = ms(t.getLong("lmb.events.rehomeAfterMs", 10000));
        return c;
    }

    String embeddedDirBase()
    {
        if (embeddedDirBase != null)
        {
            return embeddedDirBase;
        }
        final File shm = new File("/dev/shm");
        return shm.isDirectory() ? shm.getPath() : System.getProperty("java.io.tmpdir");
    }

    private static long ms(final long v)
    {
        return TimeUnit.MILLISECONDS.toNanos(v);
    }

    /** Parses sizes such as 65536, 64k, 1m. */
    public static int parseSize(final String s)
    {
        final String v = s.trim().toLowerCase();
        final char last = v.charAt(v.length() - 1);
        final long mult = last == 'k' ? 1024L : last == 'm' ? 1024L * 1024 : last == 'g' ? 1024L * 1024 * 1024 : 1L;
        final String digits = mult == 1L ? v : v.substring(0, v.length() - 1);
        return Math.toIntExact(Long.parseLong(digits) * mult);
    }
}
