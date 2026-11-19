package io.lmb.gateway;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.lmb.fix.FixDictionaryImpl;
import io.lmb.fix.OrdType;
import io.lmb.fix.Side;
import io.lmb.fix.builder.NewOrderSingleEncoder;
import io.lmb.fix.builder.OrderCancelRequestEncoder;
import io.lmb.fix.builder.OrderMassCancelRequestEncoder;
import io.lmb.proto.orderentry.ExecType;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** FIX round trips through the gateway's Artio acceptor, driven by an Artio initiator in the test. */
class FixGatewayTest
{
    private static final AtomicInteger FIX_PORTS = new AtomicInteger(33500 + (int)(ProcessHandle.current().pid() % 20) * 10);

    private final Fixture fx = new Fixture();
    private final List<FixClient> fixClients = new ArrayList<>();

    @AfterEach
    void tearDown()
    {
        fixClients.forEach(FixClient::close);
        fx.close();
    }

    private Gateway fixGateway() throws Exception
    {
        final GatewayConfig cfg = Fixture.config(5);
        cfg.fixEnabled = true;
        cfg.fixPort = FIX_PORTS.getAndIncrement();
        cfg.fixDeleteLogsOnStart = true;
        cfg.fixTermBufferLength = 256 * 1024;
        return fx.gateway(cfg);
    }

    @Test
    void fixOrderCancelFillAndGatewayRejects() throws Exception
    {
        final Gateway g = fixGateway();
        final FixClient c = new FixClient("ok");
        fixClients.add(c);
        final Session s = c.logon(g, "1001", "100", "alpha-key");
        assertNotNull(s, "logon accepted");

        c.newOrder(s, "11", "AAPL", Side.BUY, 100, 19000);
        final Map<Integer, String> ack = c.await(m -> "8".equals(m.get(35)) && "11".equals(m.get(11)) && "0".equals(m.get(150)));
        assertEquals("0", ack.get(20001), "shard id");
        assertEquals("AAPL", ack.get(55));
        assertEquals("100", ack.get(151));
        assertEquals("190", ack.get(44).replaceAll("\\.0+$", ""));

        // a gRPC participant of another firm takes 40: FIX sees a Trade report
        final TestClient b = fx.beta(g);
        b.sell(1, "AAPL", 40, 19000);
        b.awaitReport(1, ExecType.EXEC_FILL);
        final Map<Integer, String> fill = c.await(m -> "8".equals(m.get(35)) && "F".equals(m.get(150)));
        assertEquals("40", fill.get(32));
        assertEquals("1", fill.get(851), "maker = added liquidity");
        assertEquals("60", fill.get(151));

        c.cancel(s, "12", "11", "AAPL");
        final Map<Integer, String> canceled = c.await(m -> "8".equals(m.get(35)) && "4".equals(m.get(150)));
        assertEquals("12", canceled.get(11));
        assertEquals("11", canceled.get(41));

        c.cancel(s, "13", "999", "AAPL");
        final Map<Integer, String> cxlRej = c.await(m -> "9".equals(m.get(35)));
        assertEquals("1", cxlRej.get(434));
        assertEquals("1", cxlRej.get(102), "unknown order");

        c.newOrder(s, "ABC", "AAPL", Side.BUY, 10, 19000);
        final Map<Integer, String> bmr = c.await(m -> "j".equals(m.get(35)));
        assertTrue(bmr.get(58).startsWith("INVALID_FIELD"), bmr.get(58));

        c.newOrder(s, "14", "NOPE", Side.BUY, 10, 19000);
        final Map<Integer, String> unknown = c.await(m -> "j".equals(m.get(35)) && "14".equals(m.get(379)));
        assertEquals("2", unknown.get(380), "unknown security");

        c.newOrder(s, "15", "GOOG", Side.SELL, 10, 17000);
        c.await(m -> "8".equals(m.get(35)) && "15".equals(m.get(11)));
        c.massCancel(s, "16");
        final Map<Integer, String> report = c.await(m -> "r".equals(m.get(35)));
        assertEquals("1", report.get(533), "one open order (GOOG) left to cancel");
        assertEquals(1, fx.machines[0].countCommands(io.lmb.sbe.MassCancelDecoder.TEMPLATE_ID));
        assertEquals(1, fx.machines[1].countCommands(io.lmb.sbe.MassCancelDecoder.TEMPLATE_ID));
    }

    @Test
    void fixLogonWithWrongPasswordIsRefused() throws Exception
    {
        final Gateway g = fixGateway();
        final FixClient c = new FixClient("bad");
        fixClients.add(c);
        final Session s = c.logon(g, "1001", "100", "wrong-key");
        assertTrue(s == null || !s.isActive(), "no active session with a bad password");
        final TestClient grpc = fx.client(g);
        grpc.logon(100, 1001, "alpha-key"); // the session id was never taken
    }

    /** An initiator: its own media driver, archive, engine and library, polled by the test thread. */
    static final class FixClient implements AutoCloseable
    {
        private final ArchivingMediaDriver driver;
        private final FixEngine engine;
        private final FixLibrary library;
        private final List<Map<Integer, String>> received = new ArrayList<>();
        private final UtcTimestampEncoder ts = new UtcTimestampEncoder();

        FixClient(final String name)
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
                    .archiveClientContext(new io.aeron.archive.client.AeronArchive.Context().controlResponseChannel("aeron:ipc")).localControlChannel("aeron:ipc")
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

        Session logon(final Gateway g, final String senderCompId, final String user, final String password)
        {
            final SessionConfiguration sc = SessionConfiguration.builder()
                .address("127.0.0.1", g.core().config().fixPort)
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
            final long activeDeadline = System.currentTimeMillis() + 2_000;
            while (!s.isActive() && s.isConnected() && System.currentTimeMillis() < activeDeadline)
            {
                library.poll(10);
            }
            // a refused logon shows up as a disconnect shortly after connecting
            final long settle = System.currentTimeMillis() + 300;
            while (System.currentTimeMillis() < settle)
            {
                library.poll(10);
            }
            return s;
        }

        private int timestamp()
        {
            return ts.encode(System.currentTimeMillis());
        }

        void newOrder(final Session s, final String clOrdId, final String symbol, final Side side, final long qty, final long cents)
        {
            final NewOrderSingleEncoder e = new NewOrderSingleEncoder();
            final int len = timestamp();
            e.clOrdID(clOrdId).account("7").side(side).ordType(OrdType.LIMIT).price(cents, 2).transactTime(ts.buffer(), len);
            e.instrument().symbol(symbol);
            e.orderQtyData().orderQty(qty, 0);
            send(s, e);
        }

        void cancel(final Session s, final String clOrdId, final String orig, final String symbol)
        {
            final OrderCancelRequestEncoder e = new OrderCancelRequestEncoder();
            final int len = timestamp();
            e.clOrdID(clOrdId).origClOrdID(orig).side(Side.BUY).transactTime(ts.buffer(), len);
            e.instrument().symbol(symbol);
            send(s, e);
        }

        void massCancel(final Session s, final String clOrdId)
        {
            final OrderMassCancelRequestEncoder e = new OrderMassCancelRequestEncoder();
            final int len = timestamp();
            e.clOrdID(clOrdId).massCancelRequestType(io.lmb.fix.MassCancelRequestType.CANCEL_ALL_ORDERS).transactTime(ts.buffer(), len);
            send(s, e);
        }

        private void send(final Session s, final uk.co.real_logic.artio.builder.Encoder e)
        {
            final long deadline = System.currentTimeMillis() + 5_000;
            while (s.trySend(e) < 0)
            {
                library.poll(10);
                if (System.currentTimeMillis() > deadline)
                {
                    fail("could not send " + e);
                }
            }
        }

        Map<Integer, String> await(final Predicate<Map<Integer, String>> p)
        {
            final long deadline = System.currentTimeMillis() + TestClient.TIMEOUT_MS;
            while (true)
            {
                for (final Map<Integer, String> m : received)
                {
                    if (p.test(m))
                    {
                        received.remove(m);
                        return m;
                    }
                }
                if (System.currentTimeMillis() > deadline)
                {
                    fail("no matching FIX message; received " + received);
                }
                library.poll(10);
                Thread.onSpinWait();
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
                received.add(m);
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

    @SuppressWarnings("unused")
    private static void sleep(final long ms)
    {
        try
        {
            TimeUnit.MILLISECONDS.sleep(ms);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unused")
    private static void assertNot(final boolean b)
    {
        assertFalse(b);
    }
}
