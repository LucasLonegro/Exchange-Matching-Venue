package io.lmb.tools.client;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The raw FIX client against a scripted acceptor: framing (BodyLength, CheckSum) and report parsing. */
class FixOrderSessionTest
{
    private static final int PORT = 39900 + (int)(ProcessHandle.current().pid() % 90);

    @Test
    void logonOrderAndExecutionReportsRoundTrip() throws Exception
    {
        final List<Map<Integer, String>> received = new CopyOnWriteArrayList<>();
        try (ServerSocket server = new ServerSocket(PORT))
        {
            final Thread acceptor = new Thread(() -> serve(server, received));
            acceptor.setDaemon(true);
            acceptor.start();

            final List<Report> reports = new CopyOnWriteArrayList<>();
            try (FixOrderSession s = new FixOrderSession("127.0.0.1", PORT, 100, 1001, "alpha-key", "LMB"))
            {
                s.listener(reports::add);
                s.logon(5000);
                s.newOrder(42, "AAPL", 'S', 100, 19000);
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (reports.size() < 2 && System.nanoTime() < deadline)
                {
                    Thread.sleep(10);
                }
            }

            final Map<Integer, String> logon = received.get(0);
            assertEquals("A", logon.get(35));
            assertEquals("1001", logon.get(49));
            assertEquals("LMB", logon.get(56));
            assertEquals("100", logon.get(553));
            assertEquals("alpha-key", logon.get(554));
            assertEquals("Y", logon.get(141));
            final Map<Integer, String> nos = received.get(1);
            assertEquals("D", nos.get(35));
            assertEquals("2", nos.get(34));
            assertEquals("42", nos.get(11));
            assertEquals("2", nos.get(54));
            assertEquals("190.00", nos.get(44));
            assertEquals("100", nos.get(38));

            assertEquals(2, reports.size(), reports.toString());
            assertEquals("NEW", reports.get(0).kind());
            final Report fill = reports.get(1);
            assertEquals("FILL", fill.kind());
            assertEquals(42, fill.clOrdId());
            assertEquals(100, fill.lastQty());
            assertEquals(19000, fill.lastPxCents());
            assertEquals(1, fill.shardId());
            assertEquals(77, fill.eventSeq());
        }
    }

    /** Reads messages, checks BodyLength/CheckSum of each, answers Logon and the order. */
    private static void serve(final ServerSocket server, final List<Map<Integer, String>> received)
    {
        try (Socket c = server.accept())
        {
            final InputStream in = c.getInputStream();
            final OutputStream out = c.getOutputStream();
            int seq = 1;
            while (true)
            {
                final byte[] raw = readMessage(in);
                if (raw == null)
                {
                    return;
                }
                final Map<Integer, String> f = parseAndVerify(raw);
                received.add(f);
                switch (f.get(35))
                {
                    case "A" -> out.write(frame("35=A\u000149=LMB\u000156=1001\u000134=" + seq++ + "\u000198=0\u0001108=5\u0001"));
                    case "D" ->
                    {
                        out.write(frame("35=8\u000134=" + seq++ + "\u000137=9\u000111=42\u000117=e1\u0001150=0\u000139=0\u000155=AAPL\u000154=2" +
                            "\u0001151=100\u000114=0\u000120001=1\u000120002=76\u0001"));
                        out.write(frame("35=8\u000134=" + seq++ + "\u000137=9\u000111=42\u000117=e2\u0001150=F\u000139=2\u000155=AAPL\u000154=2" +
                            "\u000132=100\u000131=190.00\u0001151=0\u000114=100\u000120001=1\u000120002=77\u0001"));
                    }
                    default -> { }
                }
                out.flush();
            }
        }
        catch (final Exception ignored)
        {
            // test ends by closing the socket
        }
    }

    private static byte[] readMessage(final InputStream in) throws Exception
    {
        final StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) >= 0)
        {
            sb.append((char)b);
            final int t = sb.lastIndexOf("\u000110=");
            if (b == 1 && t >= 0 && sb.length() > t + 4 && sb.indexOf("\u0001", t + 1) == sb.length() - 1)
            {
                return sb.toString().getBytes(StandardCharsets.US_ASCII);
            }
        }
        return null;
    }

    private static Map<Integer, String> parseAndVerify(final byte[] raw)
    {
        final String s = new String(raw, StandardCharsets.US_ASCII);
        final Map<Integer, String> f = new HashMap<>();
        for (final String kv : s.split("\u0001"))
        {
            final int eq = kv.indexOf('=');
            f.put(Integer.parseInt(kv.substring(0, eq)), kv.substring(eq + 1));
        }
        final int bodyStart = s.indexOf('\u0001', s.indexOf("9=")) + 1;
        final int trailer = s.lastIndexOf("10=");
        assertEquals(Integer.parseInt(f.get(9)), trailer - bodyStart, "BodyLength");
        int sum = 0;
        for (int i = 0; i < trailer; i++)
        {
            sum += raw[i];
        }
        assertEquals(sum & 0xFF, Integer.parseInt(f.get(10)), "CheckSum");
        assertTrue(f.get(52).matches("\\d{8}-\\d{2}:\\d{2}:\\d{2}\\.\\d{3}"), "SendingTime");
        return f;
    }

    private static byte[] frame(final String inner)
    {
        final String msg = "8=FIX.4.4\u00019=" + inner.length() + "\u0001" + inner;
        int sum = 0;
        for (final byte b : msg.getBytes(StandardCharsets.US_ASCII))
        {
            sum += b;
        }
        return (msg + String.format("10=%03d\u0001", sum & 0xFF)).getBytes(StandardCharsets.US_ASCII);
    }
}
