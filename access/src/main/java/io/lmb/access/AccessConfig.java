package io.lmb.access;

import io.lmb.common.Topology;

import java.nio.file.Path;

/**
 * Configuration of the access service. Every key is looked up as system property / environment variable, then the
 * topology file, then the default (table in docs/modules/access.md).
 */
public final class AccessConfig
{
    public static final String LOCAL_ADMIN_TOKEN = "lmb-local-admin";
    public static final String LOCAL_SERVICE_TOKEN = "lmb-local-access";

    public int instance;
    public Path dataDir;
    /** Operators' REST API. Loopback by default: expose it only on purpose (and only to operators). */
    public String adminBindHost = "127.0.0.1";
    public int adminPort;
    /** The directory endpoint the gateways and the drop copy poll. */
    public String internalBindHost = "0.0.0.0";
    public int internalPort;
    public String metricsBindHost = "0.0.0.0";
    public int metricsPort;
    public String dbUrl;
    public String dbUser = "sa";
    public String dbPassword = "";
    /** {@code <name> <sha256 of token>} per line; when unset, {@link #adminToken} is the only operator token. */
    public Path operatorsFile;
    public String adminToken = LOCAL_ADMIN_TOKEN;
    /** Bearer token of the internal directory endpoint. */
    public String serviceToken = LOCAL_SERVICE_TOKEN;
    /** false (production): refuse to start with the built-in local tokens. */
    public boolean allowDefaultTokens = true;
    /** Default grace period of a rotation. */
    public long rotationGraceMs = 24 * 3_600_000L;

    public static AccessConfig load(final Topology t, final int instance)
    {
        final AccessConfig c = new AccessConfig();
        c.instance = instance;
        c.dataDir = Path.of(t.get("lmb.data.dir", "./data")).resolve("access").resolve(Integer.toString(instance)).toAbsolutePath();
        c.adminBindHost = t.get("lmb.access.adminBindHost", c.adminBindHost);
        c.adminPort = port(t, instance, "adminPort", 9750);
        c.internalBindHost = t.get("lmb.access.internalBindHost", c.internalBindHost);
        c.internalPort = port(t, instance, "internalPort", 9760);
        c.metricsBindHost = t.get("lmb.access.metricsBindHost", c.metricsBindHost);
        c.metricsPort = port(t, instance, "metricsPort", 9770);
        c.dbUrl = t.get("lmb.access.db.url", "jdbc:h2:file:" + c.dataDir.resolve("access") + ";MODE=PostgreSQL");
        c.dbUser = t.get("lmb.access.db.user", c.dbUser);
        c.dbPassword = t.get("lmb.access.db.password", c.dbPassword);
        final String ops = t.get("lmb.access.operatorsFile", null);
        c.operatorsFile = ops == null || ops.isBlank() ? null : Path.of(ops);
        c.adminToken = t.get("lmb.access.adminToken", c.adminToken);
        c.serviceToken = t.get("lmb.access.serviceToken", c.serviceToken);
        c.allowDefaultTokens = t.getBool("lmb.access.allowDefaultTokens", c.allowDefaultTokens);
        c.rotationGraceMs = t.getLong("lmb.access.rotationGraceMs", c.rotationGraceMs);
        return c;
    }

    /** {@code lmb.access.<i>.<name>}, else {@code lmb.access.<name>} (default {@code base}) plus the instance number. */
    private static int port(final Topology t, final int instance, final String name, final int base)
    {
        final String own = t.get("lmb.access." + instance + "." + name, null);
        return own != null && !own.isBlank() ? Integer.parseInt(own.trim()) : t.getInt("lmb.access." + name, base) + instance;
    }
}
