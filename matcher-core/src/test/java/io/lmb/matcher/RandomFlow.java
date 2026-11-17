package io.lmb.matcher;

import io.lmb.sbe.CancelReason;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.TimeInForce;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Seeded random command stream exercising every command type: orders around the reference price
 * (crossing and not), IOC and market, cancels and replaces of live and stale ids, possResend and plain
 * duplicates, reference price moves, state changes (immediate and scheduled, including auctions),
 * mass cancels of every scope, risk limit changes and kill switch, day rolls and malformed input.
 */
final class RandomFlow
{
    static final int[] FIRMS = {100, 200, 300};
    static final int[] SYMBOLS = {1, 2, 3};

    private final Random rnd;
    private final boolean controlCommands;
    private final long[] nextClOrd = new long[FIRMS.length];
    private final List<List<Long>> issued = new ArrayList<>();
    private final long[] ref = {10_000, 2_000, 50_000};
    private final List<byte[]> history = new ArrayList<>();
    private long sourceTs = 1;
    private long corr = 1;
    private int date = 20260101;

    RandomFlow(final long seed, final boolean controlCommands)
    {
        this.rnd = new Random(seed);
        this.controlCommands = controlCommands;
        for (int i = 0; i < FIRMS.length; i++)
        {
            issued.add(new ArrayList<>());
            nextClOrd[i] = 1;
        }
    }

    /** Setup commands: three symbols (one with lot 10 and tick 5) and three firms with generous limits. */
    List<byte[]> setup()
    {
        return List.of(
            Cmd.symbol(corr++, 1, "AAA", 1, 1, ref[0], 5_000, 500, SymbolState.CONTINUOUS),
            Cmd.symbol(corr++, 2, "BBB", 1, 1, ref[1], 5_000, 1000, SymbolState.AUCTION_CALL),
            Cmd.symbol(corr++, 3, "CCC", 5, 10, ref[2], 5_000, 300, SymbolState.CONTINUOUS),
            Cmd.risk(corr++, 100, 50_000_000_000L, 20_000, true),
            Cmd.risk(corr++, 200, 50_000_000_000L, 20_000, true),
            Cmd.risk(corr++, 300, 300_000_000L, 3_000, true));
    }

    /** Next command, or null meaning "advance time to fire due timers" (when timeAdvance is supported). */
    byte[] next()
    {
        final byte[] c = generate();
        history.add(c);
        return c;
    }

    private byte[] generate()
    {
        final int fi = rnd.nextInt(FIRMS.length);
        final int firm = FIRMS[fi];
        final int si = rnd.nextInt(SYMBOLS.length);
        final int sym = SYMBOLS[si];
        final long session = firm * 10L + rnd.nextInt(2);
        final long tick = sym == 3 ? 5 : 1;
        final long lot = sym == 3 ? 10 : 1;
        final int r = rnd.nextInt(1000);
        if (r < 500)
        {
            final long clOrd = nextClOrd[fi]++;
            issued.get(fi).add(clOrd);
            final Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
            final long qty = lot * (1 + rnd.nextInt(rnd.nextInt(10) == 0 ? 300 : 20));
            if (r < 30)
            {
                return Cmd.newOrder(session, clOrd, firm, 7, sym, 1 + rnd.nextInt(2), side, OrdType.MARKET,
                    TimeInForce.DAY, Cmd.NULL, qty, false, rnd.nextInt(1000));
            }
            final long offset = (rnd.nextInt(41) - 20 + (side == Side.BUY ? -3 : 3)) * tick;
            long price = (ref[si] / tick) * tick + offset;
            if (rnd.nextInt(50) == 0)
            {
                price += (rnd.nextBoolean() ? 1 : -1) * ref[si]; // way outside the band
            }
            if (rnd.nextInt(100) == 0)
            {
                price += 1; // off tick for CCC
            }
            final TimeInForce tif = rnd.nextInt(7) == 0 ? TimeInForce.IOC : TimeInForce.DAY;
            return Cmd.newOrder(session, clOrd, firm, 7, sym, 1 + rnd.nextInt(2), side, OrdType.LIMIT, tif, price, qty,
                false, rnd.nextInt(1000));
        }
        if (r < 660)
        {
            final long target = pick(fi);
            final long clOrd = nextClOrd[fi]++;
            issued.get(fi).add(clOrd);
            final int symbolForCancel = rnd.nextInt(30) == 0 ? SYMBOLS[rnd.nextInt(3)] : sym;
            return Cmd.cancel(session, clOrd, target, firm, symbolForCancel, 1 + rnd.nextInt(2), false);
        }
        if (r < 790)
        {
            final long target = pick(fi);
            final long clOrd = nextClOrd[fi]++;
            issued.get(fi).add(clOrd);
            final long price = (ref[si] / tick) * tick + (rnd.nextInt(31) - 15) * tick;
            final long qty = lot * (1 + rnd.nextInt(25));
            return Cmd.replace(session, clOrd, target, firm, sym, price, qty, false);
        }
        if (r < 830)
        {
            if (!history.isEmpty())
            {
                final byte[] old = history.get(rnd.nextInt(history.size()));
                final int t = (old[2] & 0xFF);
                if (t >= 1 && t <= 3 && old.length > 20)
                {
                    final byte[] dup = old.clone();
                    if (rnd.nextBoolean())
                    {
                        final int possResendOffset = t == 1 ? 57 : t == 2 ? 42 : 58;
                        dup[8 + possResendOffset] = 1;
                    }
                    return dup;
                }
            }
            return Cmd.ref(sym, ref[si], sourceTs - 1); // stale
        }
        if (r < 880)
        {
            final long move = (rnd.nextInt(21) - 10) * tick;
            ref[si] = Math.max(tick * 100, ref[si] + move);
            if (rnd.nextInt(5) == 0)
            {
                return Cmd.ref(sym, ref[si], sourceTs - rnd.nextInt(2)); // duplicate or stale
            }
            return Cmd.ref(sym, ref[si], ++sourceTs);
        }
        if (!controlCommands)
        {
            return Cmd.ref(sym, ref[si], ++sourceTs);
        }
        if (r < 930)
        {
            final SymbolState[] states = {SymbolState.CONTINUOUS, SymbolState.CONTINUOUS, SymbolState.AUCTION_CALL,
                SymbolState.HALTED, SymbolState.CLOSED, SymbolState.PRE_OPEN, SymbolState.CONTINUOUS};
            final SymbolState target = states[rnd.nextInt(states.length)];
            final long symId = rnd.nextInt(10) == 0 ? Cmd.ALL_SYMBOLS : sym;
            final long eff = rnd.nextInt(4) == 0 ? -1 : 0; // -1: scheduled relative, resolved by the driver
            return Cmd.state(corr++, symId, target, eff);
        }
        if (r < 950)
        {
            final MassCancelScope[] scopes = MassCancelScope.values();
            final MassCancelScope scope = scopes[rnd.nextInt(5)];
            final CancelReason reason = new CancelReason[] {CancelReason.MASS_CANCEL, CancelReason.DISCONNECT, CancelReason.ADMIN}[rnd.nextInt(3)];
            return Cmd.massCancel(corr++, session, firm, sym, 1, 1 + rnd.nextInt(2), scope, reason);
        }
        if (r < 970)
        {
            final long budget = rnd.nextInt(4) == 0 ? rnd.nextInt(5_000_000) : 50_000_000_000L;
            return Cmd.risk(corr++, firm, budget, 1_000 + rnd.nextInt(30_000), rnd.nextInt(8) != 0);
        }
        if (r < 975)
        {
            return Cmd.dayRoll(corr++, ++date);
        }
        if (r < 985)
        {
            final byte[] c = Cmd.limit(session, 999_999, firm, sym, Side.BUY, 100, 1);
            switch (rnd.nextInt(4))
            {
                case 0:
                    return java.util.Arrays.copyOf(c, 10);
                case 1:
                    c[2] = 77;
                    return c;
                case 2:
                    return Cmd.poke(c, 54, 9);
                default:
                    return Cmd.poke(Cmd.dayRoll(1, 1), -4, 7); // schema id 7
            }
        }
        if (r < 995)
        {
            // symbol redefinition / new parameters
            return Cmd.symbol(corr++, sym, "SYM" + sym, tick, lot, Cmd.NULL, 2_000 + rnd.nextInt(5_000),
                200 + rnd.nextInt(1000), SymbolState.CONTINUOUS);
        }
        return Cmd.state(corr++, sym, rnd.nextInt(5) == 0 ? SymbolState.REMOVED : SymbolState.AUCTION_CALL, 0);
    }

    private long pick(final int fi)
    {
        final List<Long> ids = issued.get(fi);
        if (ids.isEmpty() || rnd.nextInt(20) == 0)
        {
            return 1_000_000 + rnd.nextInt(100);
        }
        // bias to recent ids
        final int n = ids.size();
        final int back = Math.min(n - 1, (int)Math.abs(rnd.nextGaussian() * 15));
        return ids.get(n - 1 - back);
    }

    /**
     * Runs n random commands into the harness. Scheduled state commands (eff = -1) get a deadline a few
     * entries ahead; every 25 commands time advances and due timers fire. A removed symbol is redefined.
     */
    static void drive(final Harness h, final RandomFlow flow, final int n)
    {
        for (int i = 0; i < n; i++)
        {
            byte[] c = flow.next();
            if (c.length > 8 && (c[2] & 0xFF) == 12)
            {
                final long eff = new org.agrona.concurrent.UnsafeBuffer(c).getLong(8 + 8, java.nio.ByteOrder.LITTLE_ENDIAN);
                if (eff == -1)
                {
                    c = c.clone();
                    new org.agrona.concurrent.UnsafeBuffer(c).putLong(16, h.now + 1000L * (5 + flow.rnd.nextInt(40)), java.nio.ByteOrder.LITTLE_ENDIAN);
                }
            }
            h.send(c);
            if (i % 25 == 24)
            {
                h.advanceTo(h.now + 1000L * flow.rnd.nextInt(30));
            }
            for (final Symbol s : h.m.symbols)
            {
                if (s.state == Codes.REMOVED)
                {
                    h.send(Cmd.symbol(flow.corr++, s.symbolId, "RE" + s.symbolId, s.symbolId == 3 ? 5 : 1,
                        s.symbolId == 3 ? 10 : 1, flow.ref[s.symbolId - 1], 5_000, 500, SymbolState.CONTINUOUS));
                }
            }
        }
    }
}
