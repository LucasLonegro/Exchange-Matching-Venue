package io.lmb.gateway.fix;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.lmb.common.FirmDirectory;
import io.lmb.fix.FixDictionaryImpl;
import io.lmb.fix.decoder.LogonDecoder;
import io.lmb.gateway.GatewayConfig;
import io.lmb.gateway.core.GatewayCore;
import io.lmb.gateway.core.Reject;
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
import uk.co.real_logic.artio.library.SessionAcquiredInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.CancelOnDisconnectOption;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * FIX 4.4 acceptor on Artio. Runs its own embedded Aeron media driver and Archive (Artio logs every
 * message there and serves FIX resend requests from it) under {@code <dataDir>/fix}. The FixEngine
 * (framer, archiver) has its own threads; the FixLibrary that owns the sessions is polled by the
 * gateway core thread, so FIX sessions call the core directly with no hand-off.
 *
 * <p>Logon: SenderCompID = session id, TargetCompID = {@code lmb.gateway.fix.compId},
 * Username (553) = firm id, Password (554) = API key, checked against the FirmDirectory in the
 * engine's AuthenticationStrategy. CancelOnDisconnectType (35002) 1 or 3 enables cancel-on-disconnect.
 * LmbResumeFrom (20010) "shard:seq;shard:seq" replays reports after those cursors with PossResend=Y.
 */
public final class FixFrontEnd implements GatewayCore.Poller, AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(FixFrontEnd.class);

    record LogonExtras(String resumeFrom)
    {
    }

    private final GatewayConfig cfg;
    private final GatewayCore core;
    private final String aeronDir;
    private final Map<Long, LogonExtras> pendingLogons = new ConcurrentHashMap<>();
    private ArchivingMediaDriver driver;
    private FixEngine engine;
    private FixLibrary library;

    private FixFrontEnd(final GatewayConfig cfg, final GatewayCore core)
    {
        this.cfg = cfg;
        this.core = core;
        this.aeronDir = cfg.dataDir.resolve("fix").resolve("aeron").toAbsolutePath().toString();
    }

    /** Launches driver, archive and engine on the calling thread and registers the library poller with the core. */
    public static AutoCloseable create(final GatewayConfig cfg, final GatewayCore core)
    {
        final FixFrontEnd fe = new FixFrontEnd(cfg, core);
        try
        {
            fe.launch();
        }
        catch (final RuntimeException e)
        {
            fe.close();
            throw e;
        }
        core.addPoller(fe);
        return fe;
    }

    static IdleStrategy idle()
    {
        return new BackoffIdleStrategy(10, 10, TimeUnit.MICROSECONDS.toNanos(10), TimeUnit.MILLISECONDS.toNanos(1));
    }

    private void launch()
    {
        final File base = cfg.dataDir.resolve("fix").toAbsolutePath().toFile();
        final int term = cfg.fixTermBufferLength;
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(idle())
            .termBufferSparseFile(true)
            .publicationTermBufferLength(term)
            .ipcTermBufferLength(term);
        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(base, "archive"))
            .deleteArchiveOnStart(cfg.fixDeleteLogsOnStart)
            .controlChannelEnabled(false)
            .archiveClientContext(new io.aeron.archive.client.AeronArchive.Context().controlResponseChannel("aeron:ipc"))
            .localControlChannel("aeron:ipc")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .idleStrategySupplier(FixFrontEnd::idle)
            .segmentFileLength(Math.max(term * 4, 1024 * 1024))
            .archiveId(1_000_000L + cfg.gatewayId);
        driver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);

        final EngineConfiguration ec = new EngineConfiguration()
            .bindTo(cfg.bindHost, cfg.fixPort)
            .libraryAeronChannel("aeron:ipc")
            .logFileDir(new File(base, "logs").getAbsolutePath())
            .deleteLogFileDirOnStart(cfg.fixDeleteLogsOnStart)
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
        LOG.info("FIX engine listening on {}:{}", cfg.bindHost, cfg.fixPort);
    }

    // ------------------------------------------------------------------ core thread

    @Override
    public void onStart()
    {
        final LibraryConfiguration lc = new LibraryConfiguration()
            .sessionAcquireHandler(this::onSessionAcquired)
            .libraryAeronChannels(List.of("aeron:ipc"))
            .libraryIdleStrategy(idle());
        lc.monitoringFile(cfg.dataDir.resolve("fix").resolve("library-monitoring").toAbsolutePath().toString());
        lc.aeronContext().aeronDirectoryName(aeronDir);
        library = FixLibrary.connect(lc);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!library.isConnected())
        {
            library.poll(10);
            if (System.nanoTime() > deadline)
            {
                throw new IllegalStateException("FIX library could not connect to the engine");
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public int poll(final long nowNs)
    {
        return library.poll(10);
    }

    @Override
    public void onClose()
    {
        final FixLibrary l = library;
        library = null;
        CloseHelper.quietClose(l);
    }

    private SessionHandler onSessionAcquired(final Session session, final SessionAcquiredInfo info)
    {
        final long sessionId = parseLong(session.compositeKey().remoteCompId());
        final FirmDirectory.Firm firm = core.firmOfSession(sessionId);
        final FixSession handler = new FixSession(core, session);
        final LogonExtras extras = pendingLogons.remove(sessionId);
        if (firm == null)
        {
            session.logoutAndDisconnect();
            return handler;
        }
        final GatewayCore.LogonRequest req = new GatewayCore.LogonRequest();
        req.firmId = firm.firmId();
        req.sessionId = sessionId;
        req.preAuthenticated = true;
        final CancelOnDisconnectOption cod = session.cancelOnDisconnectOption();
        req.cancelOnDisconnect = cod == CancelOnDisconnectOption.CANCEL_ON_DISCONNECT_ONLY ||
            cod == CancelOnDisconnectOption.CANCEL_ON_DISCONNECT_OR_LOGOUT ||
            cod == CancelOnDisconnectOption.CANCEL_ON_LOGOUT_ONLY;
        req.resumeFromOwnCursors = true;
        if (extras != null && extras.resumeFrom() != null && !extras.resumeFrom().isBlank())
        {
            parseCursors(extras.resumeFrom(), req);
        }
        final Reject r = core.logon(req, handler);
        if (r != null)
        {
            LOG.info("FIX session {} refused: {}", sessionId, r);
            session.logoutAndDisconnect();
        }
        return handler;
    }

    /** "shard:seq;shard:seq" (also accepts ','). Malformed entries are ignored. */
    static void parseCursors(final String text, final GatewayCore.LogonRequest req)
    {
        final List<long[]> out = new ArrayList<>();
        for (final String part : text.split("[;,]"))
        {
            final int colon = part.indexOf(':');
            if (colon <= 0)
            {
                continue;
            }
            try
            {
                out.add(new long[] {Integer.parseInt(part.substring(0, colon).trim()), Long.parseLong(part.substring(colon + 1).trim())});
            }
            catch (final NumberFormatException ignore)
            {
                // skip
            }
        }
        req.resumeShards = new int[out.size()];
        req.resumeSeqs = new long[out.size()];
        for (int i = 0; i < out.size(); i++)
        {
            req.resumeShards[i] = (int)out.get(i)[0];
            req.resumeSeqs[i] = out.get(i)[1];
        }
    }

    static long parseLong(final String s)
    {
        try
        {
            return Long.parseLong(s.trim());
        }
        catch (final RuntimeException e)
        {
            return -1;
        }
    }

    /** Runs on the engine's framer thread; uses only immutable directory data and a concurrent map. */
    private final class Authenticator implements AuthenticationStrategy
    {
        @Override
        public boolean authenticate(final AbstractLogonDecoder decoder)
        {
            if (!(decoder instanceof LogonDecoder logon))
            {
                return false;
            }
            final long sessionId = parseLong(logon.header().senderCompIDAsString());
            final long firmId = logon.hasUsername() ? parseLong(logon.usernameAsString()) : -1;
            final FirmDirectory.Firm firm = core.firmOfSession(sessionId);
            final boolean ok = firm != null && firm.firmId() == firmId &&
                cfg.fixCompId.equals(logon.header().targetCompIDAsString()) &&
                logon.hasPassword() && GatewayCore.constantTimeEquals(firm.apiKey(), logon.passwordAsString());
            if (ok)
            {
                pendingLogons.put(sessionId, new LogonExtras(logon.hasLmbResumeFrom() ? logon.lmbResumeFromAsString() : null));
            }
            else
            {
                LOG.info("FIX logon refused for SenderCompID {}", logon.header().senderCompIDAsString());
            }
            return ok;
        }
    }

    /** Called after the core thread stopped (the library is closed in {@link #onClose}). */
    @Override
    public void close()
    {
        CloseHelper.quietClose(engine);
        engine = null;
        CloseHelper.quietClose(driver);
        driver = null;
    }
}
