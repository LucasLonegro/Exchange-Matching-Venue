package io.lmb.posttrade.dropcopy;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.lmb.fix.FixDictionaryImpl;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/** A FIX initiator (own driver, archive, engine, library) that records every message it receives, per session. */
final class FixTestClient implements AutoCloseable
{
    private final ArchivingMediaDriver driver;
    private final FixEngine engine;
    private final FixLibrary library;
    /** Received messages by SenderCompID (our side); each is tag -> value. */
    final Map<String, List<Map<Integer, String>>> received = new HashMap<>();

    FixTestClient(final String name)
    {
        final File base = Path.of("target", "test-data", "fixclient-" + name + "-" + System.nanoTime()).toAbsolutePath().toFile();
        final String aeronDir = new File(base, "aeron").getAbsolutePath();
        driver = ArchivingMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir).dirDeleteOnStart(true).dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED).termBufferSparseFile(true)
            .publicationTermBufferLength(256 * 1024).ipcTermBufferLength(256 * 1024)
            .sharedIdleStrategy(new BackoffIdleStrategy(10, 10, 1_000, 1_000_000)),
            new Archive.Context().aeronDirectoryName(aeronDir).archiveDir(new File(base, "archive"))
                .deleteArchiveOnStart(true).controlChannelEnabled(false)
                .archiveClientContext(new io.aeron.archive.client.AeronArchive.Context().controlResponseChannel("aeron:ipc"))
                .localControlChannel("aeron:ipc")
                .replicationChannel("aeron:udp?endpoint=localhost:0").recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED).segmentFileLength(1024 * 1024).archiveId(System.nanoTime())
                .idleStrategySupplier(() -> new BackoffIdleStrategy(10, 10, 1_000, 1_000_000)));
        final EngineConfiguration ec = new EngineConfiguration()
            .libraryAeronChannel("aeron:ipc")
            .acceptorfixDictionary(FixDictionaryImpl.class)
            .logFileDir(new File(base, "logs").getAbsolutePath())
            .deleteLogFileDirOnStart(true)
            .printStartupWarnings(false)
            .printErrorMessages(false);
        ec.monitoringFile(new File(base, "monitoring").getAbsolutePath());
        ec.aeronContext().aeronDirectoryName(aeronDir);
        ec.aeronArchiveContext().aeronDirectoryName(aeronDir).controlRequestChannel("aeron:ipc").controlResponseChannel("aeron:ipc");
        engine = FixEngine.launch(ec);
        final LibraryConfiguration lc = new LibraryConfiguration()
            .sessionAcquireHandler((session, info) -> new Handler())
            .libraryAeronChannels(List.of("aeron:ipc"));
        lc.monitoringFile(new File(base, "lib-monitoring").getAbsolutePath());
        lc.aeronContext().aeronDirectoryName(aeronDir);
        library = FixLibrary.connect(lc);
        final long deadline = System.currentTimeMillis() + 10_000;
        while (!library.isConnected() && System.currentTimeMillis() < deadline)
        {
            library.poll(10);
        }
    }

    /** Returns the session once active, or null if the logon was refused. */
    Session logon(final int port, final String senderCompId, final String user, final String password)
    {
        final SessionConfiguration sc = SessionConfiguration.builder()
            .address("127.0.0.1", port)
            .senderCompId(senderCompId).targetCompId("LMB")
            .credentials(user, password)
            .fixDictionary(FixDictionaryImpl.class)
            .timeoutInMs(5_000)
            .build();
        final Reply<Session> reply = library.initiate(sc);
        final long deadline = System.currentTimeMillis() + 10_000;
        while (reply.isExecuting() && System.currentTimeMillis() < deadline)
        {
            library.poll(10);
        }
        final Session s = reply.resultIfPresent();
        if (s == null)
        {
            return null;
        }
        final long settle = System.currentTimeMillis() + 300; // a refused logon shows up as a disconnect
        while (System.currentTimeMillis() < settle)
        {
            library.poll(10);
        }
        return s.isActive() ? s : null;
    }

    /** ExecutionReports received by the session, PossDup (43=Y, Artio resends) excluded. */
    List<Map<Integer, String>> reports(final String senderCompId)
    {
        final List<Map<Integer, String>> out = new ArrayList<>();
        for (final Map<Integer, String> m : received.getOrDefault(senderCompId, List.of()))
        {
            if ("8".equals(m.get(35)) && !"Y".equals(m.get(43)))
            {
                out.add(m);
            }
        }
        return out;
    }

    void pollUntil(final BooleanSupplier condition, final long timeoutMs, final String what)
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new AssertionError("timed out waiting for " + what);
            }
            if (library.poll(10) == 0)
            {
                Thread.onSpinWait();
            }
        }
    }

    void pollFor(final long ms)
    {
        final long until = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < until)
        {
            library.poll(10);
        }
    }

    private final class Handler implements SessionHandler
    {
        public Action onMessage(
            final DirectBuffer buffer, final int offset, final int length, final int libraryId, final Session session,
            final int sequenceIndex, final long messageType, final long timestampInNs, final long position,
            final OnMessageInfo messageInfo)
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            final Map<Integer, String> m = new HashMap<>();
            for (final String field : new String(bytes, StandardCharsets.US_ASCII).split("\u0001"))
            {
                final int eq = field.indexOf('=');
                if (eq > 0)
                {
                    m.put(Integer.parseInt(field.substring(0, eq)), field.substring(eq + 1));
                }
            }
            received.computeIfAbsent(session.compositeKey().localCompId(), k -> new ArrayList<>()).add(m);
            return Action.CONTINUE;
        }

        public void onTimeout(final int libraryId, final Session session)
        {
        }

        public void onSlowStatus(final int libraryId, final Session session, final boolean hasBecomeSlow)
        {
        }

        public Action onDisconnect(final int libraryId, final Session session, final DisconnectReason reason)
        {
            return Action.CONTINUE;
        }

        public void onSessionStart(final Session session)
        {
        }
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(library);
        CloseHelper.quietClose(engine);
        CloseHelper.quietClose(driver);
    }
}
