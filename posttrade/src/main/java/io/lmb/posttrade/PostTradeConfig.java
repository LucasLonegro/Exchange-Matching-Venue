package io.lmb.posttrade;

import io.lmb.common.Topology;

import java.nio.file.Path;

/**
 * Configuration of one post-trade instance. Every key is looked up as system property / environment variable,
 * then the topology file, then the default (see docs/modules/posttrade.md for the table).
 */
public final class PostTradeConfig
{
    public int instance;
    public Path dataDir;
    public String bindHost = "0.0.0.0";
    public int metricsPort;
    public int pollLimit = 256;
    public long restartBackoffMs = 1000;

    public boolean tradeStoreEnabled = true;
    public String dbUrl;
    public String dbUser = "sa";
    public String dbPassword = "";
    public long commitIntervalMs = 100;
    public int commitBatch = 1000;

    public boolean dropCopyEnabled = true;
    public int dropCopyPort;
    public String dropCopyCompId = "LMB";
    /** Password of the all-firms session (Username {@code ALL}); null disables it. */
    public String dropCopyAdminKey;
    public String dropCopyExecTypes = "PARTIAL_FILL,FILL,CANCELED,EXPIRED";
    public int dropCopyTermBufferLength = 256 * 1024;
    public boolean dropCopyDeleteLogsOnStart;
    public long dropCopyCheckpointIntervalMs = 1000;
    /** Drop copy credentials: base URL of the access service's directory, polled (required by PostTradeMain). */
    public String accessUrl;
    /** Bearer token of the access service's directory; the default is the local one (lmb.access.serviceToken). */
    public String accessToken = "lmb-local-access";
    public long accessPollMs = 2000;

    public boolean surveillanceEnabled = true;
    /** Kafka bootstrap servers; null or blank = write JSON lines to {@link #surveillanceFile}. */
    public String kafkaBootstrap;
    public String surveillanceTopic = "lmb.surveillance";
    public Path surveillanceFile;
    public long surveillanceCheckpointIntervalMs = 1000;

    public static PostTradeConfig load(final Topology t, final int instance)
    {
        final PostTradeConfig c = new PostTradeConfig();
        c.instance = instance;
        c.dataDir = Path.of(t.get("lmb.data.dir", "./data")).resolve("posttrade").resolve(Integer.toString(instance))
            .toAbsolutePath();
        c.bindHost = t.get("lmb.posttrade.bindHost", c.bindHost);
        c.metricsPort = t.postTradeMetricsPort() + instance;
        c.pollLimit = t.getInt("lmb.posttrade.pollLimit", c.pollLimit);
        c.restartBackoffMs = t.getLong("lmb.posttrade.restartBackoffMs", c.restartBackoffMs);

        c.tradeStoreEnabled = t.getBool("lmb.posttrade.tradestore.enabled", c.tradeStoreEnabled);
        c.dbUrl = t.get("lmb.posttrade.db.url",
            "jdbc:h2:file:" + c.dataDir.resolve("tradestore") + ";MODE=PostgreSQL");
        c.dbUser = t.get("lmb.posttrade.db.user", c.dbUser);
        c.dbPassword = t.get("lmb.posttrade.db.password", c.dbPassword);
        c.commitIntervalMs = t.getLong("lmb.posttrade.tradestore.commitIntervalMs", c.commitIntervalMs);
        c.commitBatch = t.getInt("lmb.posttrade.tradestore.commitBatch", c.commitBatch);

        c.dropCopyEnabled = t.getBool("lmb.posttrade.dropcopy.enabled", c.dropCopyEnabled);
        c.dropCopyPort = t.dropCopyFixPort() + instance;
        c.dropCopyCompId = t.get("lmb.posttrade.dropcopy.compId", c.dropCopyCompId);
        c.dropCopyAdminKey = t.get("lmb.posttrade.dropcopy.adminKey", null);
        c.accessUrl = t.get("lmb.posttrade.access.url", null);
        c.accessToken = t.get("lmb.posttrade.access.token", c.accessToken);
        c.accessPollMs = t.getLong("lmb.posttrade.access.pollMs", c.accessPollMs);
        c.dropCopyExecTypes = t.get("lmb.posttrade.dropcopy.execTypes", c.dropCopyExecTypes);
        c.dropCopyTermBufferLength = t.getInt("lmb.posttrade.dropcopy.termBufferLength", c.dropCopyTermBufferLength);
        c.dropCopyDeleteLogsOnStart = t.getBool("lmb.posttrade.dropcopy.deleteLogsOnStart", c.dropCopyDeleteLogsOnStart);
        c.dropCopyCheckpointIntervalMs = t.getLong("lmb.posttrade.dropcopy.checkpointIntervalMs",
            c.dropCopyCheckpointIntervalMs);

        c.surveillanceEnabled = t.getBool("lmb.posttrade.surveillance.enabled", c.surveillanceEnabled);
        c.kafkaBootstrap = t.get("lmb.posttrade.kafka.bootstrap", null);
        c.surveillanceTopic = t.get("lmb.posttrade.surveillance.topic", c.surveillanceTopic);
        c.surveillanceFile = Path.of(t.get("lmb.posttrade.surveillance.file",
            c.dataDir.resolve("surveillance.jsonl").toString()));
        c.surveillanceCheckpointIntervalMs = t.getLong("lmb.posttrade.surveillance.checkpointIntervalMs",
            c.surveillanceCheckpointIntervalMs);
        return c;
    }
}
