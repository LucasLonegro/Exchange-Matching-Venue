package io.lmb.tools;

import io.lmb.common.SymbolDirectory;
import io.lmb.tools.client.OrderSession;
import io.lmb.tools.client.Report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * End-to-end check through a real gateway: a seller rests a DAY limit order, a buyer crosses it, and the
 * tool waits for the seller's NEW ack, the buyer's FILL and the seller's FILL. Repeats for every symbol in
 * {@code --symbols} (default: one symbol per shard) and {@code --pairs} times, {@code --interval-ms} apart.
 * <pre>
 * scripts/lmb-run io.lmb.tools.SmokeTestMain --topology F [--protocol grpc|fix] [--gateway 0]
 *     [--symbols AAPL,GOOG] [--pairs 1] [--interval-ms 0] [--qty 100] [--timeout-ms 5000]
 *     [--seller 1001] [--buyer 2001] [--max-failures 0] [--verbose] [--price CENTS --shard N (symbols not in the CSV)]
 * </pre>
 * Prints {@code SMOKE PASS ...} / {@code SMOKE FAIL ...} per pair and {@code SMOKE RESULT ok=N failed=M};
 * exit code 0 when failed &lt;= --max-failures.
 */
public final class SmokeTestMain
{
    private final Map<Long, LinkedBlockingQueue<Report>> byClOrdId = new ConcurrentHashMap<>();
    private final boolean verbose;

    private SmokeTestMain(final boolean verbose)
    {
        this.verbose = verbose;
    }

    public static void main(final String[] args) throws Exception
    {
        final ToolContext ctx = new ToolContext(args);
        System.exit(new SmokeTestMain(ctx.has("verbose")).run(ctx));
    }

    private void onReport(final String who, final Report r)
    {
        if (verbose)
        {
            System.out.println("  " + who + " <- " + r);
        }
        byClOrdId.computeIfAbsent(r.clOrdId(), k -> new LinkedBlockingQueue<>()).add(r);
    }

    /** Waits for a report on clOrdId matching {@code want}; a reject on it ends the wait (returned). */
    private Report await(final long clOrdId, final Predicate<Report> want, final long deadlineNs) throws InterruptedException
    {
        final LinkedBlockingQueue<Report> q = byClOrdId.computeIfAbsent(clOrdId, k -> new LinkedBlockingQueue<>());
        while (true)
        {
            final long left = deadlineNs - System.nanoTime();
            if (left <= 0)
            {
                return null;
            }
            final Report r = q.poll(left, TimeUnit.NANOSECONDS);
            if (r == null)
            {
                return null;
            }
            if (want.test(r) || r.isReject())
            {
                return r;
            }
        }
    }

    int run(final ToolContext ctx) throws Exception
    {
        final List<SymbolDirectory.Symbol> syms = new ArrayList<>();
        if (ctx.has("symbols"))
        {
            for (final String t : ctx.get("symbols", "").split(","))
            {
                SymbolDirectory.Symbol s = ctx.symbols.byTicker(t.trim());
                if (s == null && ctx.has("price"))
                {
                    // A symbol added through the control plane, not in the CSV: price (cents) and shard given.
                    s = new SymbolDirectory.Symbol(0, t.trim(), ctx.getInt("shard", 0), 1, 1, ctx.getLong("price", 0), 0, 0);
                }
                if (s == null)
                {
                    throw new IllegalArgumentException("unknown symbol " + t + " (not in the symbols file; give --price and --shard)");
                }
                syms.add(s);
            }
        }
        else
        {
            for (int shard = 0; shard < ctx.topology.shardCount(); shard++)
            {
                for (final SymbolDirectory.Symbol s : ctx.symbols.all())
                {
                    if (s.shard() == shard)
                    {
                        syms.add(s);
                        break;
                    }
                }
            }
        }
        final int pairs = ctx.getInt("pairs", 1);
        final long intervalMs = ctx.getLong("interval-ms", 0);
        final long qty = ctx.getLong("qty", 100);
        final long timeoutMs = ctx.getLong("timeout-ms", 5000);
        final int maxFailures = ctx.getInt("max-failures", 0);

        final OrderSession seller = ctx.session(ctx.getLong("seller", 1001));
        final OrderSession buyer = ctx.session(ctx.getLong("buyer", 2001));
        seller.listener(r -> onReport("seller", r));
        buyer.listener(r -> onReport("buyer", r));
        System.out.println("SMOKE logon " + seller.describe() + " and " + buyer.describe());
        seller.logon(timeoutMs);
        buyer.logon(timeoutMs);
        System.out.println("SMOKE logged on");

        long id = ToolContext.clOrdIdBase();
        int ok = 0;
        int failed = 0;
        try
        {
            for (int p = 1; p <= pairs; p++)
            {
                for (final SymbolDirectory.Symbol s : syms)
                {
                    final long sellId = ++id;
                    final long buyId = ++id;
                    final long px = s.referencePrice();
                    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
                    final long t0 = seller.newOrder(sellId, s.ticker(), 'S', qty, px);
                    final Report ack = await(sellId, r -> "NEW".equals(r.kind()), deadline);
                    String problem = check("seller NEW", ack, "NEW");
                    long tAck = ack == null ? 0 : ack.receivedNs();
                    Report buyFill = null;
                    Report sellFill = null;
                    if (problem == null)
                    {
                        buyer.newOrder(buyId, s.ticker(), 'B', qty, px);
                        buyFill = await(buyId, r -> "FILL".equals(r.kind()), deadline);
                        problem = check("buyer FILL", buyFill, "FILL");
                        if (problem == null)
                        {
                            sellFill = await(sellId, r -> "FILL".equals(r.kind()), deadline);
                            problem = check("seller FILL", sellFill, "FILL");
                        }
                    }
                    if (problem == null && (buyFill.lastQty() != qty || buyFill.lastPxCents() != px || sellFill.lastQty() != qty))
                    {
                        problem = "fill mismatch: buy " + buyFill + " / sell " + sellFill;
                    }
                    if (problem == null)
                    {
                        ok++;
                        System.out.printf("SMOKE PASS pair=%d symbol=%s shard=%d ack_ms=%.2f fill_ms=%.2f buySeq=%d sellSeq=%d%n",
                            p, s.ticker(), s.shard(), (tAck - t0) / 1e6, (sellFill.receivedNs() - t0) / 1e6,
                            buyFill.eventSeq(), sellFill.eventSeq());
                    }
                    else
                    {
                        failed++;
                        System.out.printf("SMOKE FAIL pair=%d symbol=%s shard=%d %s%n", p, s.ticker(), s.shard(), problem);
                        // Leave no resting liquidity behind that would fill the next pair unexpectedly.
                        safeCancel(seller, ++id, sellId, s.ticker());
                        safeCancel(buyer, ++id, buyId, s.ticker());
                    }
                }
                if (intervalMs > 0 && p < pairs)
                {
                    Thread.sleep(intervalMs);
                }
            }
        }
        finally
        {
            seller.close();
            buyer.close();
        }
        System.out.println("SMOKE RESULT ok=" + ok + " failed=" + failed);
        return failed <= maxFailures && ok > 0 ? 0 : 1;
    }

    private static String check(final String what, final Report r, final String kind)
    {
        if (r == null)
        {
            return what + ": timed out";
        }
        return kind.equals(r.kind()) ? null : what + ": got " + r;
    }

    private static void safeCancel(final OrderSession s, final long clOrdId, final long orig, final String symbol)
    {
        try
        {
            if (s.isConnected())
            {
                s.cancel(clOrdId, orig, symbol);
            }
        }
        catch (final RuntimeException e)
        {
            System.out.println("SMOKE cancel of " + orig + " failed: " + e.getMessage());
        }
    }
}
