package io.lmb.ledger;

import io.lmb.common.Topology;

import java.nio.file.Path;

/**
 * Configuration of the ledger. Every key is looked up as system property / environment variable, then the topology
 * file, then the default (table in docs/modules/ledger.md).
 */
public final class LedgerConfig
{
    public int instance;
    public Path dataDir;
    public String bindHost = "0.0.0.0";
    public int metricsPort;
    public int pollLimit = 256;
    public long restartBackoffMs = 1000;
    public String dbUrl;
    public String dbUser = "sa";
    public String dbPassword = "";
    public long commitIntervalMs = 100;
    public int commitBatch = 1000;
    public Path statementsDir;

    public static LedgerConfig load(final Topology t, final int instance)
    {
        final LedgerConfig c = new LedgerConfig();
        c.instance = instance;
        c.dataDir = Path.of(t.get("lmb.data.dir", "./data")).resolve("ledger").resolve(Integer.toString(instance))
            .toAbsolutePath();
        c.bindHost = t.get("lmb.ledger.bindHost", c.bindHost);
        c.metricsPort = t.getInt("lmb.ledger.metricsPort", 9820) + instance;
        c.pollLimit = t.getInt("lmb.ledger.pollLimit", c.pollLimit);
        c.restartBackoffMs = t.getLong("lmb.ledger.restartBackoffMs", c.restartBackoffMs);
        c.dbUrl = t.get("lmb.ledger.db.url", "jdbc:h2:file:" + c.dataDir.resolve("ledger") + ";MODE=PostgreSQL");
        c.dbUser = t.get("lmb.ledger.db.user", c.dbUser);
        c.dbPassword = t.get("lmb.ledger.db.password", c.dbPassword);
        c.commitIntervalMs = t.getLong("lmb.ledger.commitIntervalMs", c.commitIntervalMs);
        c.commitBatch = t.getInt("lmb.ledger.commitBatch", c.commitBatch);
        c.statementsDir = Path.of(t.get("lmb.ledger.statementsDir", c.dataDir.resolve("statements").toString()))
            .toAbsolutePath();
        return c;
    }
}
