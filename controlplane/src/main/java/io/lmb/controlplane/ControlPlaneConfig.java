package io.lmb.controlplane;

import io.lmb.common.Config;
import io.lmb.common.Topology;

import java.nio.file.Path;

/** Every configuration key of the control plane, with its default. See docs/modules/controlplane.md. */
public final class ControlPlaneConfig
{
    public String instanceId = "0";
    public int instanceIndex = 0;
    public String dbUrl;
    public String dbUser = "sa";
    public String dbPassword = "";
    public int dbPoolSize = 4;
    public String adminToken = "lmb-local-admin";
    public boolean adminTokenDefaulted = true;
    public String restHost = "127.0.0.1";
    /** 0 = ephemeral, negative = REST disabled. */
    public int restPort = -1;
    public int metricsPort = -1;
    public Path symbolsFile;
    public Path firmsFile;

    public boolean autoOpen = false;
    public long autoOpenDelayMs = 2_000;

    public long tickMs = 10;
    public long commandTimeoutMs = 1_000;
    public int commandMaxAttempts = 20;
    public long persistIntervalMs = 100;
    public long reconcileIntervalMs = 5_000;
    public long refDataReloadMs = 1_000;
    public long adminPollMs = 50;
    public int adminGatewayId = 65_535;
    public long commandRetentionMs = 24L * 3600_000L;
    public long refreshRetentionMs = 10L * 60_000L;

    public long leaseTtlMs = 5_000;
    public long leaseRenewMs = 1_000;
    public long leaseMarginMs = 1_000;

    public boolean schedulerEnabled = true;
    public boolean enforce = true;
    public long schedulerLeadMs = 5L * 60_000L;
    public long schedulerGraceMs = 3_000;
    public String calendarZone = "America/New_York";
    public String calendarWeekdays = "MON,TUE,WED,THU,FRI";
    public String calPreOpen = "07:00";
    public String calOpenAuction = "09:25";
    public String calContinuous = "09:30";
    public String calCloseAuction = "15:55";
    public String calClose = "16:00";
    public String calDayRoll = "18:00";

    public long allocatorIntervalMs = 250;
    public long allocatorRefreshMs = 1_000;
    public double allocatorAlpha = 0.5;
    public double allocatorMinStepFraction = 0.02;

    /** Reads every key from system properties / environment / topology file. */
    public static ControlPlaneConfig load(final Topology t)
    {
        final ControlPlaneConfig c = new ControlPlaneConfig();
        c.instanceId = t.get("lmb.instance.id", "0");
        c.instanceIndex = parseIndex(c.instanceId);
        final String dataDir = t.get("lmb.data.dir", "./data");
        c.dbUrl = t.get("lmb.db.url", "jdbc:h2:file:" + Path.of(dataDir, "controlplane", "controlplane").toAbsolutePath() +
            ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;AUTO_SERVER=TRUE;LOCK_TIMEOUT=10000");
        c.dbUser = t.get("lmb.db.user", c.dbUser);
        c.dbPassword = t.get("lmb.db.password", c.dbPassword);
        c.dbPoolSize = t.getInt("lmb.db.poolSize", c.dbPoolSize);
        final String token = t.get("lmb.controlplane.adminToken", null);
        if (token != null && !token.isBlank())
        {
            c.adminToken = token;
            c.adminTokenDefaulted = false;
        }
        c.restHost = t.get("lmb.controlplane." + c.instanceIndex + ".bindHost", "0.0.0.0");
        c.restPort = t.controlPlaneRestPort(c.instanceIndex);
        c.metricsPort = t.controlPlaneMetricsPort(c.instanceIndex);
        c.symbolsFile = t.symbolsFile();
        c.firmsFile = t.firmsFile();
        c.autoOpen = t.getBool("lmb.controlplane.autoOpen", c.autoOpen);
        c.autoOpenDelayMs = t.getLong("lmb.controlplane.autoOpenDelayMs", c.autoOpenDelayMs);
        c.tickMs = t.getLong("lmb.controlplane.tickMs", c.tickMs);
        c.commandTimeoutMs = t.getLong("lmb.controlplane.commandTimeoutMs", c.commandTimeoutMs);
        c.commandMaxAttempts = t.getInt("lmb.controlplane.commandMaxAttempts", c.commandMaxAttempts);
        c.persistIntervalMs = t.getLong("lmb.controlplane.persistIntervalMs", c.persistIntervalMs);
        c.reconcileIntervalMs = t.getLong("lmb.controlplane.reconcileIntervalMs", c.reconcileIntervalMs);
        c.refDataReloadMs = t.getLong("lmb.controlplane.refDataReloadMs", c.refDataReloadMs);
        c.adminPollMs = t.getLong("lmb.controlplane.adminPollMs", c.adminPollMs);
        c.adminGatewayId = t.getInt("lmb.controlplane.adminGatewayId", c.adminGatewayId);
        c.commandRetentionMs = t.getLong("lmb.controlplane.commandRetentionMs", c.commandRetentionMs);
        c.refreshRetentionMs = t.getLong("lmb.controlplane.refreshRetentionMs", c.refreshRetentionMs);
        c.leaseTtlMs = t.getLong("lmb.controlplane.lease.ttlMs", c.leaseTtlMs);
        c.leaseRenewMs = t.getLong("lmb.controlplane.lease.renewMs", c.leaseRenewMs);
        c.leaseMarginMs = t.getLong("lmb.controlplane.lease.marginMs", c.leaseMarginMs);
        c.schedulerEnabled = t.getBool("lmb.controlplane.scheduler.enabled", c.schedulerEnabled);
        c.enforce = t.getBool("lmb.controlplane.scheduler.enforce", c.enforce);
        c.schedulerLeadMs = t.getLong("lmb.controlplane.scheduler.leadMs", c.schedulerLeadMs);
        c.schedulerGraceMs = t.getLong("lmb.controlplane.scheduler.graceMs", c.schedulerGraceMs);
        c.calendarZone = t.get("lmb.controlplane.calendar.zone", c.calendarZone);
        c.calendarWeekdays = t.get("lmb.controlplane.calendar.weekdays", c.calendarWeekdays);
        c.calPreOpen = t.get("lmb.controlplane.calendar.preOpen", c.calPreOpen);
        c.calOpenAuction = t.get("lmb.controlplane.calendar.openAuction", c.calOpenAuction);
        c.calContinuous = t.get("lmb.controlplane.calendar.continuous", c.calContinuous);
        c.calCloseAuction = t.get("lmb.controlplane.calendar.closeAuction", c.calCloseAuction);
        c.calClose = t.get("lmb.controlplane.calendar.close", c.calClose);
        c.calDayRoll = t.get("lmb.controlplane.calendar.dayRoll", c.calDayRoll);
        c.allocatorIntervalMs = t.getLong("lmb.controlplane.allocator.intervalMs", c.allocatorIntervalMs);
        c.allocatorRefreshMs = t.getLong("lmb.controlplane.allocator.refreshMs", c.allocatorRefreshMs);
        c.allocatorAlpha = Double.parseDouble(t.get("lmb.controlplane.allocator.alpha", Double.toString(c.allocatorAlpha)));
        c.allocatorMinStepFraction = Double.parseDouble(
            t.get("lmb.controlplane.allocator.minStepFraction", Double.toString(c.allocatorMinStepFraction)));
        return c;
    }

    static int parseIndex(final String instanceId)
    {
        try
        {
            return Integer.parseInt(instanceId.trim());
        }
        catch (final NumberFormatException e)
        {
            return 0;
        }
    }

    /** Convenience for code that only has Config (no topology). */
    public static String get(final String key, final String def)
    {
        return Config.get(key, def);
    }
}
