package io.lmb.posttrade.dropcopy;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.AccessDirectorySource;
import io.lmb.common.access.Scope;
import io.lmb.fix.FixDictionaryImpl;
import io.lmb.fix.decoder.LogonDecoder;
import io.lmb.posttrade.PostTradeMetrics;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionAcquireHandler;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;

import java.io.File;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * The drop copy's FIX 4.4 acceptor: an embedded Aeron media driver + Archive (Artio logs every message there and
 * serves FIX ResendRequests from it) and the Artio {@link FixEngine}, under {@code <dataDir>/dropcopy}, plus the one FixLibrary
 * (sole library mode) that the {@link DropCopySink} polls. Lives for the whole process; close it after the sink's
 * runner has stopped.
 *
 * <p>Logon: SenderCompID = any id of 1-32 characters [A-Za-z0-9_.-] (the drop copy cursor key),
 * TargetCompID = the configured compId, Username (553) = firm id and Password (554) = that firm's API key,
 * or Username {@code ALL} with the admin key for a session carrying every firm's reports.
 */
public final class DropCopyEngine implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(DropCopyEngine.class);
    static final Pattern COMP_ID = Pattern.compile("[A-Za-z0-9_.-]{1,32}");
    /** Firm id of a session that receives every firm's reports. */
    public static final int ALL_FIRMS = -1;

    record Logon(int firmId, String resumeFrom)
    {
    }

    private final Path baseDir;
    private final String aeronDir;
    private final String bindHost;
    private final int port;
    private final String compId;
    private final String adminKey;
    private final int termLength;
    private final boolean deleteLogsOnStart;
    private final AccessDirectorySource firms;
    private final PostTradeMetrics metrics;
    final Map<String, Logon> pendingLogons = new ConcurrentHashMap<>();
    private ArchivingMediaDriver driver;
    private FixEngine engine;
    /** Owned here so that a sink restart does not reconnect it (a new library waits for the old one's timeout). */
    private FixLibrary library;
    private volatile SessionAcquireHandler acquireHandler;

    /** Firm logons need a key of that firm with the DROP_COPY scope. */
    public DropCopyEngine(
        final Path dataDir, final String bindHost, final int port, final String compId, final String adminKey,
        final int termLength, final boolean deleteLogsOnStart, final AccessDirectorySource firms, final PostTradeMetrics metrics)
    {
        this.baseDir = dataDir.resolve("dropcopy").toAbsolutePath();
        this.aeronDir = baseDir.resolve("aeron").toString();
        this.bindHost = bindHost;
        this.port = port;
        this.compId = compId;
        this.adminKey = adminKey == null || adminKey.isBlank() ? null : adminKey;
        this.termLength = termLength;
        this.deleteLogsOnStart = deleteLogsOnStart;
        this.firms = firms;
        this.metrics = metrics;
    }

    static IdleStrategy idle()
    {
        return new BackoffIdleStrategy(10, 10, TimeUnit.MICROSECONDS.toNanos(10), TimeUnit.MILLISECONDS.toNanos(1));
    }

    public DropCopyEngine start()
    {
        try
        {
            launch();
        }
        catch (final RuntimeException e)
        {
            close();
            throw e;
        }
        return this;
    }

    private void launch()
    {
        final File base = baseDir.toFile();
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(idle())
            .termBufferSparseFile(true)
            .publicationTermBufferLength(termLength)
            .ipcTermBufferLength(termLength);
        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(base, "archive"))
            .deleteArchiveOnStart(deleteLogsOnStart)
            .controlChannelEnabled(false)
            .archiveClientContext(new io.aeron.archive.client.AeronArchive.Context().controlResponseChannel("aeron:ipc"))
            .localControlChannel("aeron:ipc")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .idleStrategySupplier(DropCopyEngine::idle)
            .segmentFileLength(Math.max(termLength * 4, 1024 * 1024))
            .archiveId(2_000_000L + port);
        driver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);

        final EngineConfiguration ec = new EngineConfiguration()
            .bindTo(bindHost, port)
            .libraryAeronChannel("aeron:ipc")
            .logFileDir(new File(base, "logs").getAbsolutePath())
            .deleteLogFileDirOnStart(deleteLogsOnStart)
            .acceptorfixDictionary(FixDictionaryImpl.class)
            .authenticationStrategy(new Authenticator())
            .initialAcceptedSessionOwner(InitialAcceptedSessionOwner.SOLE_LIBRARY)
            .framerIdleStrategy(idle())
            .archiverIdleStrategy(idle())
            .printStartupWarnings(false)
            .printErrorMessages(false);
        ec.monitoringFile(new File(base, "engine-monitoring").getAbsolutePath());
        ec.aeronContext().aeronDirectoryName(aeronDir);
        ec.aeronArchiveContext().aeronDirectoryName(aeronDir)
            .controlRequestChannel("aeron:ipc")
            .controlResponseChannel("aeron:ipc");
        engine = FixEngine.launch(ec);
        LOG.info("drop copy FIX acceptor listening on {}:{} (TargetCompID {})", bindHost, port, compId);
    }

    /**
     * The FIX library (connected on first use, on the calling thread, which must be the only thread polling it).
     * Newly acquired sessions go to {@code handler} from now on.
     */
    FixLibrary library(final SessionAcquireHandler handler)
    {
        acquireHandler = handler;
        if (library != null && !library.isClosed())
        {
            return library;
        }
        final LibraryConfiguration lc = new LibraryConfiguration()
            .sessionAcquireHandler((session, info) -> acquireHandler.onSessionAcquired(session, info))
            .libraryAeronChannels(List.of("aeron:ipc"))
            .libraryIdleStrategy(idle());
        lc.monitoringFile(baseDir.resolve("library-monitoring").toString());
        lc.aeronContext().aeronDirectoryName(aeronDir);
        final FixLibrary l = FixLibrary.connect(lc);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!l.isConnected())
        {
            l.poll(10);
            if (System.nanoTime() > deadline)
            {
                CloseHelper.quietClose(l);
                throw new IllegalStateException("drop copy library could not connect to the engine");
            }
            Thread.onSpinWait();
        }
        library = l;
        return l;
    }

    String aeronDir()
    {
        return aeronDir;
    }

    Path baseDir()
    {
        return baseDir;
    }

    public int port()
    {
        return port;
    }

    /** Runs on the engine's framer thread: immutable directory data and a concurrent map only. */
    private final class Authenticator implements AuthenticationStrategy
    {
        @Override
        public boolean authenticate(final AbstractLogonDecoder decoder)
        {
            if (!(decoder instanceof LogonDecoder logon))
            {
                return false;
            }
            final String sender = logon.header().senderCompIDAsString();
            final String user = logon.hasUsername() ? logon.usernameAsString().trim() : "";
            final String password = logon.hasPassword() ? logon.passwordAsString() : null;
            int firmId = Integer.MIN_VALUE;
            boolean ok = COMP_ID.matcher(sender).matches() && compId.equals(logon.header().targetCompIDAsString()) &&
                password != null;
            if (ok && "ALL".equals(user))
            {
                ok = adminKey != null && constantTimeEquals(adminKey, password);
                firmId = ALL_FIRMS;
            }
            else if (ok)
            {
                int wanted = Integer.MIN_VALUE;
                try
                {
                    wanted = Integer.parseInt(user);
                }
                catch (final NumberFormatException ignore)
                {
                }
                final AccessDirectory directory = firms.current();
                final AccessDirectory.KeyEntry k = directory == null ? null :
                    directory.authenticateFirm(password, Scope.DROP_COPY, System.currentTimeMillis());
                ok = k != null && k.firmId() == wanted;
                firmId = ok ? wanted : firmId;
            }
            if (ok)
            {
                pendingLogons.put(sender, new Logon(firmId, logon.hasLmbResumeFrom() ? logon.lmbResumeFromAsString() : null));
            }
            else
            {
                metrics.dropCopyLogonsRejected.incrementAndGet();
                LOG.info("drop copy logon refused for SenderCompID {}", sender);
            }
            return ok;
        }
    }

    static boolean constantTimeEquals(final String a, final String b)
    {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(library);
        library = null;
        CloseHelper.quietClose(engine);
        engine = null;
        CloseHelper.quietClose(driver);
        driver = null;
    }
}
