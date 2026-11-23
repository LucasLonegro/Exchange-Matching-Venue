package io.lmb.tools.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Minimal FIX 4.4 initiator over a plain socket, enough to drive the gateway's Artio acceptor: Logon
 * (ResetSeqNumFlag=Y, Username = firm, Password = API key), NewOrderSingle, OrderCancelRequest, heartbeats,
 * TestRequest answers, ResendRequest answered with a gap fill, Logout. Not a general FIX engine: inbound
 * sequence gaps are ignored (the tool never asks for resends).
 */
public final class FixOrderSession implements OrderSession
{
    private static final byte SOH = 1;
    private static final DateTimeFormatter TS =
        DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private final String host;
    private final int port;
    private final long firmId;
    private final long sessionId;
    private final String apiKey;
    private final String targetCompId;
    private final int heartBtIntSec = 5;
    private final CompletableFuture<Void> logonDone = new CompletableFuture<>();
    private final Object sendLock = new Object();
    private volatile Consumer<Report> listener = r -> { };
    private volatile boolean connected;
    private volatile long lastSentNs;
    private Socket socket;
    private OutputStream out;
    private Thread reader;
    private ScheduledExecutorService timer;
    private int outSeq = 1;

    public FixOrderSession(final String host, final int port, final long firmId, final long sessionId,
                           final String apiKey, final String targetCompId)
    {
        this.host = host;
        this.port = port;
        this.firmId = firmId;
        this.sessionId = sessionId;
        this.apiKey = apiKey;
        this.targetCompId = targetCompId;
    }

    @Override
    public void logon(final long timeoutMs) throws Exception
    {
        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port), (int)timeoutMs);
        out = socket.getOutputStream();
        final InputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
        reader = new Thread(() -> readLoop(in), "fix-reader-" + sessionId);
        reader.setDaemon(true);
        reader.start();
        send("A", "98=0\u0001108=" + heartBtIntSec + "\u0001141=Y\u0001553=" + firmId + "\u0001554=" + apiKey + "\u0001");
        logonDone.get(timeoutMs, TimeUnit.MILLISECONDS);
        connected = true;
        timer = Executors.newSingleThreadScheduledExecutor(r ->
        {
            final Thread t = new Thread(r, "fix-hb-" + sessionId);
            t.setDaemon(true);
            return t;
        });
        timer.scheduleAtFixedRate(() ->
        {
            if (connected && System.nanoTime() - lastSentNs > TimeUnit.SECONDS.toNanos(heartBtIntSec) / 2)
            {
                send("0", "");
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public long newOrder(final long clOrdId, final String symbol, final char side, final long qty, final long priceCents)
    {
        final String now = TS.format(Instant.now());
        return send("D", "11=" + clOrdId + "\u000155=" + symbol + "\u000154=" + (side == 'B' ? '1' : '2') +
            "\u000160=" + now + "\u000138=" + qty + "\u000140=2\u000144=" + cents(priceCents) + "\u000159=0\u0001");
    }

    @Override
    public void cancel(final long clOrdId, final long origClOrdId, final String symbol)
    {
        send("F", "41=" + origClOrdId + "\u000111=" + clOrdId + "\u000155=" + symbol + "\u000154=1\u000160=" +
            TS.format(Instant.now()) + "\u0001");
    }

    private static String cents(final long c)
    {
        return BigDecimal.valueOf(c, 2).toPlainString();
    }

    /** Frames and sends one message; returns System.nanoTime() just before the write. */
    private long send(final String msgType, final String body)
    {
        synchronized (sendLock)
        {
            final String inner = "35=" + msgType + "\u000149=" + sessionId + "\u000156=" + targetCompId + "\u000134=" + outSeq +
                "\u000152=" + TS.format(Instant.now()) + "\u0001" + body;
            return write(inner);
        }
    }

    private long write(final String inner)
    {
        final String head = "8=FIX.4.4\u00019=" + inner.getBytes(StandardCharsets.US_ASCII).length + "\u0001";
        final byte[] msg = (head + inner).getBytes(StandardCharsets.US_ASCII);
        int sum = 0;
        for (final byte b : msg)
        {
            sum += b;
        }
        final byte[] trailer = String.format("10=%03d\u0001", sum & 0xFF).getBytes(StandardCharsets.US_ASCII);
        final long t = System.nanoTime();
        try
        {
            out.write(msg);
            out.write(trailer);
            out.flush();
            outSeq++;
            lastSentNs = t;
        }
        catch (final IOException e)
        {
            connected = false;
            throw new IllegalStateException("FIX send failed: " + e.getMessage(), e);
        }
        return t;
    }

    private void readLoop(final InputStream in)
    {
        final Map<Integer, String> fields = new HashMap<>();
        final StringBuilder tag = new StringBuilder();
        final StringBuilder value = new StringBuilder();
        boolean inValue = false;
        try
        {
            int b;
            while ((b = in.read()) >= 0)
            {
                if (!inValue)
                {
                    if (b == '=')
                    {
                        inValue = true;
                    }
                    else
                    {
                        tag.append((char)b);
                    }
                }
                else if (b == SOH)
                {
                    final int t = Integer.parseInt(tag.toString());
                    fields.put(t, value.toString());
                    tag.setLength(0);
                    value.setLength(0);
                    inValue = false;
                    if (t == 10)
                    {
                        onMessage(fields, System.nanoTime());
                        fields.clear();
                    }
                }
                else
                {
                    value.append((char)b);
                }
            }
        }
        catch (final IOException | RuntimeException e)
        {
            if (connected)
            {
                listener.accept(reject(0, "FIX connection lost: " + e));
            }
        }
        finally
        {
            connected = false;
            logonDone.completeExceptionally(new IllegalStateException("FIX connection closed before logon completed"));
        }
    }

    private void onMessage(final Map<Integer, String> f, final long now)
    {
        final String type = f.getOrDefault(35, "");
        switch (type)
        {
            case "A" -> logonDone.complete(null);
            case "1" -> send("0", "112=" + f.getOrDefault(112, "") + "\u0001");
            case "2" ->
            {
                // ResendRequest: we keep no history; gap fill everything up to what we send next.
                synchronized (sendLock)
                {
                    write("35=4\u000149=" + sessionId + "\u000156=" + targetCompId + "\u000134=" + f.get(7) +
                        "\u000143=Y\u000152=" + TS.format(Instant.now()) + "\u0001122=" + TS.format(Instant.now()) +
                        "\u0001123=Y\u000136=" + outSeq + "\u0001");
                    outSeq--; // write() advanced it, but a gap fill carries an older MsgSeqNum
                }
            }
            case "5" ->
            {
                if (!logonDone.isDone())
                {
                    logonDone.completeExceptionally(new IllegalStateException("logon refused: " + f.getOrDefault(58, "")));
                }
                final boolean wasConnected = connected;
                connected = false;
                if (wasConnected)
                {
                    listener.accept(reject(0, "Logout from gateway: " + f.getOrDefault(58, "")));
                }
            }
            case "8" -> listener.accept(execReport(f, now));
            case "j" -> listener.accept(reject(parseLong(f.get(379)), "BusinessMessageReject: " + f.getOrDefault(58, "")));
            case "9" -> listener.accept(new Report("CANCEL_REJECTED", parseLong(f.get(11)), 0, "", '?', 0, 0, 0, 0, -1, 0,
                false, f.getOrDefault(58, ""), now));
            case "r" -> listener.accept(new Report("MASS_CANCEL_REPORT", parseLong(f.get(1369)), 0, f.getOrDefault(55, ""),
                '?', 0, 0, 0, 0, -1, 0, false, f.getOrDefault(58, ""), now));
            default -> { }
        }
    }

    private static Report execReport(final Map<Integer, String> f, final long now)
    {
        final String execType = f.getOrDefault(150, "");
        final String ordStatus = f.getOrDefault(39, "");
        final String kind = switch (execType)
        {
            case "0" -> "NEW";
            case "F" -> "2".equals(ordStatus) ? "FILL" : "PARTIAL_FILL";
            case "4" -> "CANCELED";
            case "5" -> "REPLACED";
            case "8" -> "REJECTED";
            case "C" -> "EXPIRED";
            default -> "EXEC_" + execType;
        };
        return new Report(kind, parseLong(f.get(11)), parseLong(f.get(37)), f.getOrDefault(55, ""),
            "1".equals(f.get(54)) ? 'B' : 'S', qty(f.get(32)), px(f.get(31)), qty(f.get(151)), qty(f.get(14)),
            (int)parseLong(f.getOrDefault(20001, "-1")), parseLong(f.get(20002)), "Y".equals(f.get(97)) || "Y".equals(f.get(43)),
            f.getOrDefault(58, ""), now);
    }

    private static long qty(final String s)
    {
        return s == null ? 0 : new BigDecimal(s).longValue();
    }

    private static long px(final String s)
    {
        return s == null ? 0 : new BigDecimal(s).movePointRight(2).longValue();
    }

    private static long parseLong(final String s)
    {
        if (s == null)
        {
            return 0;
        }
        try
        {
            return Long.parseLong(s.trim());
        }
        catch (final NumberFormatException e)
        {
            return 0;
        }
    }

    private static Report reject(final long clOrdId, final String text)
    {
        return new Report("SESSION_REJECT", clOrdId, 0, "", '?', 0, 0, 0, 0, -1, 0, false, text, System.nanoTime());
    }

    @Override
    public void listener(final Consumer<Report> listener)
    {
        this.listener = listener;
    }

    @Override
    public boolean isConnected()
    {
        return connected;
    }

    @Override
    public String describe()
    {
        return "fix " + host + ":" + port + " firm=" + firmId + " session=" + sessionId;
    }

    @Override
    public void close()
    {
        if (timer != null)
        {
            timer.shutdownNow();
        }
        if (connected)
        {
            connected = false;
            try
            {
                send("5", "");
                Thread.sleep(200);
            }
            catch (final RuntimeException | InterruptedException ignored)
            {
                // best effort
            }
        }
        connected = false;
        try
        {
            if (socket != null)
            {
                socket.close();
            }
        }
        catch (final IOException ignored)
        {
            // closing
        }
    }
}
