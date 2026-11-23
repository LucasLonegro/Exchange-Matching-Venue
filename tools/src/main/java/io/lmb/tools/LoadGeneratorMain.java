package io.lmb.tools;

import io.lmb.common.SymbolDirectory;
import io.lmb.tools.client.OrderSession;
import io.lmb.tools.client.Report;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Open-loop load generator through a real gateway (gRPC or FIX).
 * <pre>
 * scripts/lmb-run io.lmb.tools.LoadGeneratorMain --topology F [--protocol grpc|fix] [--gateway 0]
 *     [--rate 1000] [--duration-s 30] [--warmup-s 2] [--sessions 1001,2001,1002,2002]
 *     [--symbols AAPL,GOOG] [--qty 1] [--report-s 5] [--drain-s 5]
 * </pre>
 * Orders go out in crossing pairs at the symbol's reference price: one session rests a DAY limit order, a
 * session of another firm takes it (sides alternate per pair so positions stay flat), so the book stays
 * almost empty and every order produces an ack and a fill. {@code --rate} is orders per second over all
 * sessions; sends are scheduled on a fixed timetable (open loop) and latency is measured from the
 * <em>scheduled</em> send time, so a stalled venue shows up as latency instead of a lower send rate
 * (no coordinated omission). The send-to-report time from the actual write is printed as well.
 * Note the gateway's per-session throttle ({@code lmb.gateway.throttle.ratePerSec}, default 500/s): a rate
 * above 500 x sessions is refused with THROTTLED.
 */
public final class LoadGeneratorMain
{
    private static final long MAX_LATENCY_NS = TimeUnit.SECONDS.toNanos(60);

    private final Recorder ackFromSchedule = new Recorder(MAX_LATENCY_NS, 3);
    private final Recorder ackFromSend = new Recorder(MAX_LATENCY_NS, 3);
    private final Recorder fillFromSchedule = new Recorder(MAX_LATENCY_NS, 3);
    private final Map<Long, long[]> pending = new ConcurrentHashMap<>(); // clOrdId -> {scheduledNs, sentNs, acked, session, symbol}
    private final Map<String, AtomicLong> rejects = new ConcurrentHashMap<>();
    private final AtomicLong acks = new AtomicLong();
    private final AtomicLong fills = new AtomicLong();
    private final AtomicLong unknown = new AtomicLong();

    public static void main(final String[] args) throws Exception
    {
        System.exit(new LoadGeneratorMain().run(new ToolContext(args)));
    }

    private void onReport(final Report r)
    {
        final long[] p = pending.get(r.clOrdId());
        if (p == null)
        {
            unknown.incrementAndGet();
            return;
        }
        if (r.isReject())
        {
            rejects.computeIfAbsent(r.kind() + " " + shortText(r.text()), k -> new AtomicLong()).incrementAndGet();
            pending.remove(r.clOrdId());
            return;
        }
        if (p[2] == 0)
        {
            p[2] = 1;
            acks.incrementAndGet();
            record(ackFromSchedule, r.receivedNs() - p[0]);
            record(ackFromSend, r.receivedNs() - p[1]);
        }
        if ("FILL".equals(r.kind()))
        {
            fills.incrementAndGet();
            record(fillFromSchedule, r.receivedNs() - p[0]);
            pending.remove(r.clOrdId());
        }
        else if (!"NEW".equals(r.kind()) && !"PARTIAL_FILL".equals(r.kind()))
        {
            rejects.computeIfAbsent(r.kind(), k -> new AtomicLong()).incrementAndGet();
            pending.remove(r.clOrdId());
        }
    }

    private static String shortText(final String text)
    {
        if (text == null)
        {
            return "";
        }
        final int sp = text.indexOf(' ');
        return sp > 0 ? text.substring(0, sp) : text;
    }

    private static void record(final Recorder recorder, final long ns)
    {
        recorder.recordValue(Math.max(0, Math.min(ns, MAX_LATENCY_NS)));
    }

    int run(final ToolContext ctx) throws Exception
    {
        final double rate = Double.parseDouble(ctx.get("rate", "1000"));
        final long durationS = ctx.getLong("duration-s", 30);
        final long warmupS = ctx.getLong("warmup-s", 2);
        final long qty = ctx.getLong("qty", 1);
        final long reportS = ctx.getLong("report-s", 5);
        final long drainS = ctx.getLong("drain-s", 5);
        final List<SymbolDirectory.Symbol> syms = new ArrayList<>();
        for (final String t : ctx.get("symbols", defaultSymbols(ctx)).split(","))
        {
            syms.add(ctx.symbols.byTicker(t.trim()));
        }
        final List<OrderSession> sessions = new ArrayList<>();
        final List<Long> firmOf = new ArrayList<>();
        for (final String s : ctx.get("sessions", "1001,2001,1002,2002").split(","))
        {
            final long id = Long.parseLong(s.trim());
            final OrderSession session = ctx.session(id);
            session.listener(this::onReport);
            session.logon(10_000);
            sessions.add(session);
            firmOf.add((long)ctx.firms.bySession(id).firmId());
        }
        System.out.printf("LOADGEN start protocol=%s gateway=%s:%d rate=%.0f/s duration=%ds warmup=%ds sessions=%d symbols=%s%n",
            ctx.protocol, ctx.host, ctx.port, rate, durationS, warmupS, sessions.size(), ctx.get("symbols", defaultSymbols(ctx)));

        final long periodNs = (long)(1e9 / rate);
        final long start = System.nanoTime();
        final long warmupEnd = start + TimeUnit.SECONDS.toNanos(warmupS);
        final long end = warmupEnd + TimeUnit.SECONDS.toNanos(durationS);
        long nextReport = warmupEnd + TimeUnit.SECONDS.toNanos(reportS);
        boolean warm = warmupS == 0;
        final Histogram total = new Histogram(MAX_LATENCY_NS, 3);
        final Histogram totalFromSend = new Histogram(MAX_LATENCY_NS, 3);
        final Histogram totalFill = new Histogram(MAX_LATENCY_NS, 3);
        Histogram interval = null;
        long id = ToolContext.clOrdIdBase();
        long sent = 0;
        long sentMeasured = 0;
        long sendErrors = 0;
        long i = 0;
        while (true)
        {
            final long scheduled = start + i * periodNs;
            if (scheduled >= end && (i & 1) == 0)
            {
                break; // only on a pair boundary, so no resting order is left without its taker
            }
            long now;
            while ((now = System.nanoTime()) < scheduled)
            {
                LockSupport.parkNanos(Math.min(scheduled - now, 200_000));
            }
            if (!warm && now >= warmupEnd)
            {
                warm = true;
                ackFromSchedule.getIntervalHistogram();
                ackFromSend.getIntervalHistogram();
                fillFromSchedule.getIntervalHistogram();
                System.out.println("LOADGEN warmup done, measuring");
            }
            if (warm && now >= nextReport)
            {
                interval = ackFromSchedule.getIntervalHistogram(interval);
                total.add(interval);
                totalFromSend.add(ackFromSend.getIntervalHistogram());
                totalFill.add(fillFromSchedule.getIntervalHistogram());
                System.out.printf("LOADGEN t=%ds sent=%d acks=%d fills=%d rejects=%d interval ack %s%n",
                    TimeUnit.NANOSECONDS.toSeconds(now - warmupEnd), sent, acks.get(), fills.get(), rejectCount(),
                    quantiles(interval));
                nextReport += TimeUnit.SECONDS.toNanos(reportS);
            }
            // Pair k = orders 2k (resting) and 2k+1 (taking), on sessions of different firms.
            final long pair = i / 2;
            final boolean taker = (i & 1) == 1;
            final int n = sessions.size();
            final int restIdx = (int)((pair * 2) % n);
            int takeIdx = (int)((pair * 2 + 1) % n);
            if (firmOf.get(takeIdx).equals(firmOf.get(restIdx)))
            {
                for (int k = 0; k < n; k++)
                {
                    if (!firmOf.get(k).equals(firmOf.get(restIdx)))
                    {
                        takeIdx = k;
                        break;
                    }
                }
            }
            final int sessionIdx = taker ? takeIdx : restIdx;
            final OrderSession s = sessions.get(sessionIdx);
            final boolean restSells = (pair & 1) == 0;
            final char side = taker == restSells ? 'B' : 'S';
            final SymbolDirectory.Symbol sym = syms.get((int)(pair % syms.size()));
            final long clOrdId = ++id;
            // sentNs is taken before the write: the report may arrive before newOrder returns.
            final long[] p = {scheduled, System.nanoTime(), 0, sessionIdx, pair % syms.size()};
            pending.put(clOrdId, p);
            try
            {
                s.newOrder(clOrdId, sym.ticker(), side, qty, sym.referencePrice());
                sent++;
                if (warm)
                {
                    sentMeasured++;
                }
            }
            catch (final RuntimeException e)
            {
                pending.remove(clOrdId);
                sendErrors++;
            }
            i++;
        }

        final long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(drainS);
        while (!pending.isEmpty() && System.nanoTime() < drainDeadline)
        {
            Thread.sleep(20);
        }
        // Orders still resting (acked, not filled: e.g. they traded with liquidity a previous run left behind)
        // are canceled so the next run starts from a clean book.
        long resting = 0;
        long noAck = 0;
        for (final Map.Entry<Long, long[]> e : pending.entrySet())
        {
            if (e.getValue()[2] == 1)
            {
                resting++;
                final OrderSession owner = sessions.get((int)e.getValue()[3]);
                if (owner.isConnected())
                {
                    owner.cancel(++id, e.getKey(), syms.get((int)e.getValue()[4]).ticker());
                }
            }
            else
            {
                noAck++;
            }
        }
        Thread.sleep(resting > 0 ? 500 : 0);
        total.add(ackFromSchedule.getIntervalHistogram());
        totalFromSend.add(ackFromSend.getIntervalHistogram());
        totalFill.add(fillFromSchedule.getIntervalHistogram());
        for (final OrderSession s : sessions)
        {
            s.close();
        }

        System.out.printf("LOADGEN result protocol=%s target_rate=%.0f/s sent=%d (measured %d in %.1fs, %.0f/s) acks=%d fills=%d " +
            "rejects=%d unanswered=%d resting_at_end=%d (canceled) send_errors=%d%n", ctx.protocol, rate, sent, sentMeasured,
            durationS * 1.0, sentMeasured / (double)Math.max(1, durationS), acks.get(), fills.get(), rejectCount(), noAck, resting,
            sendErrors);
        if (!rejects.isEmpty())
        {
            System.out.println("LOADGEN rejects " + new TreeMap<>(rejectsSnapshot()));
        }
        System.out.println("LOADGEN ack latency from scheduled send (us): " + quantiles(total));
        System.out.println("LOADGEN ack latency from actual write   (us): " + quantiles(totalFromSend));
        System.out.println("LOADGEN fill latency from scheduled send (us): " + quantiles(totalFill));
        return noAck == 0 && sendErrors == 0 && total.getTotalCount() > 0 ? 0 : 1;
    }

    private Map<String, Long> rejectsSnapshot()
    {
        final Map<String, Long> m = new TreeMap<>();
        rejects.forEach((k, v) -> m.put(k, v.get()));
        return m;
    }

    private long rejectCount()
    {
        long n = 0;
        for (final AtomicLong v : rejects.values())
        {
            n += v.get();
        }
        return n;
    }

    private static String defaultSymbols(final ToolContext ctx)
    {
        final StringBuilder sb = new StringBuilder();
        for (int shard = 0; shard < ctx.topology.shardCount(); shard++)
        {
            for (final SymbolDirectory.Symbol s : ctx.symbols.all())
            {
                if (s.shard() == shard)
                {
                    sb.append(sb.isEmpty() ? "" : ",").append(s.ticker());
                    break;
                }
            }
        }
        return sb.toString();
    }

    static String quantiles(final Histogram h)
    {
        if (h.getTotalCount() == 0)
        {
            return "n=0";
        }
        return String.format("n=%d p50=%.0f p99=%.0f p999=%.0f max=%.0f", h.getTotalCount(),
            h.getValueAtPercentile(50) / 1e3, h.getValueAtPercentile(99) / 1e3, h.getValueAtPercentile(99.9) / 1e3,
            h.getMaxValue() / 1e3);
    }
}
