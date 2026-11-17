package io.lmb.matcher;

import io.lmb.sbe.CancelOrderDecoder;
import io.lmb.sbe.DayRolloverDecoder;
import io.lmb.sbe.MassCancelDecoder;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.NewOrderDecoder;
import io.lmb.sbe.ReferencePriceDecoder;
import io.lmb.sbe.ReplaceOrderDecoder;
import io.lmb.sbe.RiskLimitUpdateDecoder;
import io.lmb.sbe.SymbolDefinitionDecoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateCommandDecoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A deliberately simple, slow model of the contract: plain lists, linear scans for the best order,
 * credit and position recomputed from scratch, book deltas derived by diffing aggregated books before
 * and after each step, auctions by brute force over candidate prices. Produces the canonical text of
 * every event (see {@link Ev#text()}) so it can be compared with the real machine.
 */
final class ReferenceMatcher
{
    static final long NULL = Long.MIN_VALUE;
    static final long NULL_SYMBOL = 0xFFFF_FFFFL;
    static final String[] STATES = {"?", "PRE_OPEN", "AUCTION_CALL", "CONTINUOUS", "HALTED", "CLOSED", "REMOVED"};

    static final class O
    {
        long orderId, clOrdId, session, price, riskPrice, qty, cum, time;
        int account, gateway, side, type, tif, firm;
        S sym;

        long leaves()
        {
            return qty - cum;
        }
    }

    static final class S
    {
        int id;
        String ticker;
        long tick, lot, maxQty, bps, ref = NULL, lastTs = Long.MIN_VALUE, bandLow = NULL, bandHigh = NULL;
        int state;
        final List<O> book = new ArrayList<>();
        long aPx = NULL, aV, aImb;
        int aSide;
    }

    static final class F
    {
        int id;
        long budget, maxPos, tradedNet;
        boolean enabled;
    }

    record Pending(long timerId, long corr, long sym, int reason, int target)
    {
    }

    final int shard;
    final LinkedHashMap<Integer, S> symbols = new LinkedHashMap<>();
    final LinkedHashMap<Integer, F> firms = new LinkedHashMap<>();
    final Map<String, Long> positions = new HashMap<>();
    final Map<String, Object> dedupe = new HashMap<>(); // O (live) or String code
    final List<O> live = new ArrayList<>(); // orderId order
    final List<Pending> pending = new ArrayList<>();
    long orderSeq, matchSeq, timerSeq, timeSeq;
    List<String> out;

    ReferenceMatcher(final int shard)
    {
        this.shard = shard;
    }

    List<String> onTimer(final long id)
    {
        out = new ArrayList<>();
        Pending p = null;
        for (final Pending q : pending)
        {
            if (q.timerId == id)
            {
                p = q;
            }
        }
        if (p != null)
        {
            pending.remove(p);
            applyState(p.sym, p.target, p.corr, p.reason);
        }
        out.add("APPLIED n=" + out.size() + " t=0");
        return out;
    }

    List<String> onCommand(final byte[] c)
    {
        out = new ArrayList<>();
        final UnsafeBuffer b = new UnsafeBuffer(c);
        final int t = c.length >= 8 ? b.getShort(2, ByteOrder.LITTLE_ENDIAN) & 0xFFFF : 0;
        final int[] blocks = {0, NewOrderDecoder.BLOCK_LENGTH, CancelOrderDecoder.BLOCK_LENGTH, ReplaceOrderDecoder.BLOCK_LENGTH,
            MassCancelDecoder.BLOCK_LENGTH, 0, 0, 0, 0, 0, ReferencePriceDecoder.BLOCK_LENGTH, SymbolDefinitionDecoder.BLOCK_LENGTH,
            SymbolStateCommandDecoder.BLOCK_LENGTH, RiskLimitUpdateDecoder.BLOCK_LENGTH, DayRolloverDecoder.BLOCK_LENGTH};
        final boolean ok = c.length >= 8 && t < blocks.length && blocks[t] > 0 &&
            (b.getShort(4, ByteOrder.LITTLE_ENDIAN) & 0xFFFF) == 1 &&
            (b.getShort(0, ByteOrder.LITTLE_ENDIAN) & 0xFFFF) >= blocks[t] && c.length >= 8 + blocks[t];
        if (!ok)
        {
            out.add("CMDREJ corr=0 t=" + t + " r=MALFORMED");
        }
        else
        {
            switch (t)
            {
                case 1 -> newOrder(new NewOrderDecoder().wrap(b, 8, blocks[t], 1));
                case 2 -> cancel(new CancelOrderDecoder().wrap(b, 8, blocks[t], 1));
                case 3 -> replace(new ReplaceOrderDecoder().wrap(b, 8, blocks[t], 1));
                case 4 -> massCancel(new MassCancelDecoder().wrap(b, 8, blocks[t], 1));
                case 10 -> ref(new ReferencePriceDecoder().wrap(b, 8, blocks[t], 1));
                case 11 -> symbolDef(new SymbolDefinitionDecoder().wrap(b, 8, blocks[t], 1));
                case 12 -> stateCmd(new SymbolStateCommandDecoder().wrap(b, 8, blocks[t], 1));
                case 13 -> risk(new RiskLimitUpdateDecoder().wrap(b, 8, blocks[t], 1));
                default -> dayRoll(new DayRolloverDecoder().wrap(b, 8, blocks[t], 1));
            }
        }
        out.add("APPLIED n=" + out.size() + " t=" + t);
        return out;
    }

    // ------------------------------------------------------------------ helpers

    S sym(final long id)
    {
        final S s = id > 0xFFFF_FFFFL ? null : symbols.get((int)id);
        return s == null || s.state == 6 ? null : s;
    }

    static String key(final long firm, final long cl)
    {
        return firm + ":" + cl;
    }

    long creditUsed(final F f)
    {
        long open = 0;
        for (final O o : live)
        {
            if (o.firm == f.id)
            {
                open += o.leaves() * o.riskPrice;
            }
        }
        return open + Math.abs(f.tradedNet);
    }

    long position(final int firm, final S s)
    {
        return positions.getOrDefault(firm + "/" + s.id, 0L);
    }

    long openQty(final int firm, final S s, final int side)
    {
        long q = 0;
        for (final O o : live)
        {
            if (o.firm == firm && o.sym == s && o.side == side)
            {
                q += o.leaves();
            }
        }
        return q;
    }

    boolean positionBreached(final F f, final S s, final int side, final long q, final long excludeLeaves)
    {
        final long net = position(f.id, s);
        if (side == 1)
        {
            return net + openQty(f.id, s, 1) - excludeLeaves + q > f.maxPos;
        }
        return net - (openQty(f.id, s, 2) - excludeLeaves) - q < -f.maxPos;
    }

    static String status(final O o)
    {
        return o.cum == 0 ? "NEW" : o.leaves() == 0 ? "FILLED" : "PARTIALLY_FILLED";
    }

    static String typeName(final int t)
    {
        return t == 2 ? "MARKET" : "LIMIT";
    }

    static String tifName(final int t)
    {
        return t == 2 ? "IOC" : "DAY";
    }

    void er(final O o, final long session, final long cl, final long orig, final long match, final long lastQty,
            final long lastPx, final long leaves, final long gwTs, final String exec, final String status, final String liq,
            final String rej, final String cxl)
    {
        out.add(Ev.erText(session, o.orderId, cl, orig, match, o.price, o.qty, lastQty, lastPx, leaves, o.cum, gwTs,
            o.firm, o.account, o.sym.id, o.gateway, exec, status, o.side, typeName(o.type), tifName(o.tif), liq, rej, cxl));
    }

    /** Aggregated book: key "symIdx|side|sortKey" (TreeMap orders it canonically) -> [qty, count, price, symId]. */
    TreeMap<String, long[]> levels()
    {
        final TreeMap<String, long[]> m = new TreeMap<>();
        int idx = 0;
        for (final S s : symbols.values())
        {
            for (final O o : s.book)
            {
                final long sortKey = o.side == 1 ? (1L << 43) - 1 - o.price : o.price;
                final String k = String.format("%08d|%d|%015d", idx, o.side, sortKey);
                final long[] v = m.computeIfAbsent(k, x -> new long[] {0, 0, o.price, s.id});
                v[0] += o.leaves();
                v[1]++;
            }
            idx++;
        }
        return m;
    }

    /** Emits deltas between two aggregated books; returns the symbols touched, in symbol order. */
    List<S> diff(final TreeMap<String, long[]> before, final boolean indicatives)
    {
        final TreeMap<String, long[]> after = levels();
        final TreeMap<String, Boolean> keys = new TreeMap<>();
        before.keySet().forEach(k -> keys.put(k, true));
        after.keySet().forEach(k -> keys.put(k, true));
        final List<S> touched = new ArrayList<>();
        for (final String k : keys.keySet())
        {
            final long[] a = before.get(k);
            final long[] z = after.get(k);
            final long qa = a == null ? 0 : a[0];
            final long ca = a == null ? 0 : a[1];
            final long qz = z == null ? 0 : z[0];
            final long cz = z == null ? 0 : z[1];
            if (qa != qz || ca != cz)
            {
                final long[] any = z != null ? z : a;
                final int side = Integer.parseInt(k.split("\\|")[1]);
                out.add("DELTA sym=" + any[3] + " side=" + side + " px=" + any[2] + " q=" + qz + " n=" + cz);
                final S s = symbols.get((int)any[3]);
                if (!touched.contains(s))
                {
                    touched.add(s);
                }
            }
        }
        if (indicatives)
        {
            for (final S s : touched)
            {
                if (s.state == 2)
                {
                    indicative(s);
                }
            }
        }
        return touched;
    }

    O bestOpposite(final S s, final int side)
    {
        O best = null;
        for (final O o : s.book)
        {
            if (o.side != side)
            {
                if (best == null || (side == 1 ? o.price < best.price : o.price > best.price) ||
                    (o.price == best.price && o.time < best.time))
                {
                    best = o;
                }
            }
        }
        return best;
    }

    void fill(final O o, final long q, final long px)
    {
        o.cum += q;
        final F f = firms.get(o.firm);
        f.tradedNet += o.side == 1 ? q * px : -q * px;
        positions.merge(o.firm + "/" + o.sym.id, o.side == 1 ? q : -q, Long::sum);
    }

    void done(final O o, final String code)
    {
        o.sym.book.remove(o);
        live.remove(o);
        for (final Map.Entry<String, Object> e : dedupe.entrySet())
        {
            if (e.getValue() == o)
            {
                e.setValue(code);
            }
        }
    }

    void match(final O o, final long gwTs)
    {
        while (o.leaves() > 0)
        {
            final O m = bestOpposite(o.sym, o.side);
            if (m == null || (o.side == 1 ? m.price > o.riskPrice : m.price < o.riskPrice))
            {
                return;
            }
            final long q = Math.min(o.leaves(), m.leaves());
            final long mid = ((long)shard << 48) | ++matchSeq;
            fill(o, q, m.price);
            fill(m, q, m.price);
            final O buy = o.side == 1 ? o : m;
            final O sell = o.side == 1 ? m : o;
            out.add("TRADE m=" + mid + " px=" + m.price + " q=" + q + " b=" + buy.orderId + " s=" + sell.orderId + " sym=" +
                o.sym.id + " bf=" + buy.firm + " sf=" + sell.firm + " agg=" + o.side + " auct=FALSE");
            fillEr(o, mid, q, m.price, gwTs, "TAKER");
            fillEr(m, mid, q, m.price, 0, "MAKER");
            if (m.leaves() == 0)
            {
                done(m, "FILLED");
            }
        }
    }

    void fillEr(final O o, final long mid, final long q, final long px, final long gwTs, final String liq)
    {
        final boolean full = o.leaves() == 0;
        er(o, o.session, o.clOrdId, 0, mid, q, px, o.leaves(), gwTs, full ? "FILL" : "PARTIAL_FILL",
            full ? "FILLED" : "PARTIALLY_FILLED", liq, "NONE", "NONE");
    }

    void finish(final O o, final long gwTs)
    {
        if (o.leaves() == 0)
        {
            done(o, "FILLED");
        }
        else if (o.type == 1 && o.tif == 1)
        {
            o.time = ++timeSeq;
            if (!o.sym.book.contains(o))
            {
                o.sym.book.add(o);
            }
        }
        else
        {
            er(o, o.session, o.clOrdId, 0, 0, 0, NULL, 0, gwTs, "CANCELED", "CANCELED", "NONE", "NONE",
                o.type == 2 ? "MARKET_COLLAR" : "IOC_REMAINDER");
            done(o, "CANCELED");
        }
    }

    // ------------------------------------------------------------------ orders

    void newOrder(final NewOrderDecoder d)
    {
        final long cl = d.clOrdId();
        final int side = d.sideRaw();
        final int type = d.ordTypeRaw();
        final int tif = d.timeInForceRaw();
        final int pr = d.possResendRaw();
        if (side < 1 || side > 2 || type < 1 || type > 2 || tif < 1 || tif > 2 || pr > 1)
        {
            out.add("CMDREJ corr=" + cl + " t=1 r=MALFORMED");
            return;
        }
        final int firm = (int)d.firmId();
        final String k = key(d.firmId(), cl);
        final boolean dup = dedupe.containsKey(k);
        if (dup && pr == 1)
        {
            return;
        }
        final boolean market = type == 2;
        final long price = market ? NULL : d.price();
        final long qty = d.quantity();
        final F f = firms.get(firm);
        final S s = sym(d.symbolId());
        String rej = null;
        long riskPrice = 0;
        if (f == null)
        {
            rej = "UNKNOWN_FIRM";
        }
        else if (!f.enabled)
        {
            rej = "FIRM_DISABLED";
        }
        else if (s == null)
        {
            rej = "UNKNOWN_SYMBOL";
        }
        else if (s.state == 1 || s.state == 5)
        {
            rej = "SYMBOL_CLOSED";
        }
        else if (s.state == 4)
        {
            rej = "SYMBOL_HALTED";
        }
        else if (s.state == 2 && market)
        {
            rej = "MARKET_ORDER_NOT_ALLOWED";
        }
        else if (qty <= 0 || qty > s.maxQty || qty % s.lot != 0)
        {
            rej = "INVALID_QTY";
        }
        else if (!market && (price <= 0 || price > 1_000_000_000L || price % s.tick != 0))
        {
            rej = "INVALID_PRICE";
        }
        else if (!market && s.ref != NULL && (price < s.bandLow || price > s.bandHigh))
        {
            rej = "PRICE_BAND";
        }
        else if (market && s.ref == NULL)
        {
            rej = "NO_REFERENCE_PRICE";
        }
        else if (dup)
        {
            rej = "DUPLICATE_CLORDID";
        }
        else
        {
            riskPrice = Math.max(0, market ? (side == 1 ? s.bandHigh : s.bandLow) : price);
            if (qty * riskPrice > f.budget - creditUsed(f))
            {
                rej = "CREDIT_LIMIT";
            }
            else if (positionBreached(f, s, side, qty, 0))
            {
                rej = "POSITION_LIMIT";
            }
            else if (market && s.book.stream().noneMatch(o -> o.side != side))
            {
                rej = "NO_LIQUIDITY";
            }
        }
        if (!dup)
        {
            dedupe.put(k, "SEEN");
        }
        if (rej != null)
        {
            out.add(Ev.erText(d.sessionId(), 0, cl, 0, 0, price, qty, 0, NULL, 0, 0, d.gatewayInTsNs(), d.firmId(),
                d.accountId(), d.symbolId(), d.gatewayId(), "REJECTED", "REJECTED", side, typeName(type), tifName(tif),
                "NONE", rej, "NONE"));
            return;
        }
        final O o = new O();
        o.orderId = ((long)shard << 48) | ++orderSeq;
        o.clOrdId = cl;
        o.session = d.sessionId();
        o.price = price;
        o.riskPrice = riskPrice;
        o.qty = qty;
        o.account = (int)d.accountId();
        o.gateway = d.gatewayId();
        o.side = side;
        o.type = type;
        o.tif = tif;
        o.firm = firm;
        o.sym = s;
        dedupe.put(k, o);
        live.add(o);
        final TreeMap<String, long[]> before = levels();
        er(o, o.session, cl, 0, 0, 0, NULL, qty, d.gatewayInTsNs(), "NEW", "NEW", "NONE", "NONE", "NONE");
        if (s.state == 3)
        {
            match(o, d.gatewayInTsNs());
        }
        finish(o, d.gatewayInTsNs());
        diff(before, true);
    }

    String doneStatusOf(final Object v)
    {
        if (v instanceof String str)
        {
            switch (str)
            {
                case "FILLED":
                    return "FILLED";
                case "CANCELED":
                    return "CANCELED";
                case "EXPIRED":
                    return "EXPIRED";
                default:
                    return "REJECTED";
            }
        }
        return "REJECTED";
    }

    void cancelReject(final O o, final Object target, final long session, final long cl, final long orig, final long gwTs,
                      final long firm, final long symId, final int gw, final String rej)
    {
        if (o != null)
        {
            out.add(Ev.erText(session, o.orderId, cl, orig, 0, o.price, o.qty, 0, NULL, o.leaves(), o.cum, gwTs, firm,
                o.account, o.sym.id, o.gateway, "CANCEL_REJECTED", status(o), o.side, typeName(o.type), tifName(o.tif),
                "NONE", rej, "NONE"));
        }
        else
        {
            out.add(Ev.erText(session, 0, cl, orig, 0, NULL, 0, 0, NULL, 0, 0, gwTs, firm, 0, symId, gw, "CANCEL_REJECTED",
                doneStatusOf(target), 255, "LIMIT", "DAY", "NONE", rej, "NONE"));
        }
    }

    void cancel(final CancelOrderDecoder d)
    {
        final long cl = d.clOrdId();
        if (d.possResendRaw() > 1)
        {
            out.add("CMDREJ corr=" + cl + " t=2 r=MALFORMED");
            return;
        }
        final String k = key(d.firmId(), cl);
        final boolean dup = dedupe.containsKey(k);
        if (dup && d.possResendRaw() == 1)
        {
            return;
        }
        final F f = firms.get((int)d.firmId());
        String rej = null;
        O o = null;
        Object target = null;
        if (f == null)
        {
            rej = "UNKNOWN_FIRM";
        }
        else if (dup)
        {
            rej = "DUPLICATE_CLORDID";
        }
        else
        {
            target = dedupe.get(key(d.firmId(), d.origClOrdId()));
            if (target instanceof O lo)
            {
                if (lo.sym.id != d.symbolId())
                {
                    rej = "UNKNOWN_ORDER";
                }
                else
                {
                    o = lo;
                }
            }
            else if ("FILLED".equals(target) || "CANCELED".equals(target) || "EXPIRED".equals(target))
            {
                rej = "TOO_LATE_TO_CANCEL";
            }
            else
            {
                rej = "UNKNOWN_ORDER";
            }
        }
        if (!dup)
        {
            dedupe.put(k, "SEEN");
        }
        if (rej != null)
        {
            cancelReject(o, target, d.sessionId(), cl, d.origClOrdId(), d.gatewayInTsNs(), d.firmId(), d.symbolId(), d.gatewayId(), rej);
            return;
        }
        dedupe.put(k, "CANCELED");
        final TreeMap<String, long[]> before = levels();
        cancelResting(o, "CANCELED", "CLIENT", cl, d.origClOrdId(), d.sessionId(), d.gatewayInTsNs());
        diff(before, true);
    }

    void cancelResting(final O o, final String exec, final String reason, final long cl, final long orig, final long session,
                       final long gwTs)
    {
        er(o, session, cl, orig, 0, 0, NULL, 0, gwTs, exec, exec.equals("EXPIRED") ? "EXPIRED" : "CANCELED", "NONE", "NONE", reason);
        done(o, exec.equals("EXPIRED") ? "EXPIRED" : "CANCELED");
    }

    void replace(final ReplaceOrderDecoder d)
    {
        final long cl = d.clOrdId();
        if (d.possResendRaw() > 1)
        {
            out.add("CMDREJ corr=" + cl + " t=3 r=MALFORMED");
            return;
        }
        final String k = key(d.firmId(), cl);
        final boolean dup = dedupe.containsKey(k);
        if (dup && d.possResendRaw() == 1)
        {
            return;
        }
        final F f = firms.get((int)d.firmId());
        final S s = sym(d.symbolId());
        final long newPrice = d.newPrice();
        final long newQty = d.newQuantity();
        String rej = null;
        O o = null;
        Object target = null;
        if (f == null)
        {
            rej = "UNKNOWN_FIRM";
        }
        else if (!f.enabled)
        {
            rej = "FIRM_DISABLED";
        }
        else if (s == null)
        {
            rej = "UNKNOWN_SYMBOL";
        }
        else
        {
            target = dedupe.get(key(d.firmId(), d.origClOrdId()));
            if (target instanceof O lo)
            {
                if (lo.sym != s)
                {
                    target = null;
                    rej = "UNKNOWN_ORDER";
                }
                else
                {
                    o = lo;
                }
            }
            else if ("FILLED".equals(target) || "CANCELED".equals(target) || "EXPIRED".equals(target))
            {
                rej = "TOO_LATE_TO_CANCEL";
            }
            else
            {
                rej = "UNKNOWN_ORDER";
            }
        }
        if (rej == null)
        {
            final long newLeaves = newQty - o.cum;
            if (s.state == 4)
            {
                rej = "SYMBOL_HALTED";
            }
            else if (s.state == 1 || s.state == 5)
            {
                rej = "SYMBOL_CLOSED";
            }
            else if (newQty <= 0 || newQty > s.maxQty || newQty % s.lot != 0)
            {
                rej = "INVALID_QTY";
            }
            else if (newQty <= o.cum)
            {
                rej = "INVALID_REPLACE";
            }
            else if (newPrice <= 0 || newPrice > 1_000_000_000L || newPrice % s.tick != 0)
            {
                rej = "INVALID_PRICE";
            }
            else if (s.ref != NULL && (newPrice < s.bandLow || newPrice > s.bandHigh))
            {
                rej = "PRICE_BAND";
            }
            else if (dup)
            {
                rej = "DUPLICATE_CLORDID";
            }
            else
            {
                final long oldN = o.leaves() * o.riskPrice;
                final long newN = newLeaves * newPrice;
                if (newN > oldN && newN - oldN > f.budget - creditUsed(f))
                {
                    rej = "CREDIT_LIMIT";
                }
                else if (newLeaves > o.leaves() && positionBreached(f, s, o.side, newLeaves, o.leaves()))
                {
                    rej = "POSITION_LIMIT";
                }
            }
        }
        if (!dup)
        {
            dedupe.put(k, "SEEN");
        }
        if (rej != null)
        {
            cancelReject(o, target, d.sessionId(), cl, d.origClOrdId(), d.gatewayInTsNs(), d.firmId(), d.symbolId(), d.gatewayId(), rej);
            return;
        }
        final TreeMap<String, long[]> before = levels();
        final boolean keep = newPrice == o.price && newQty <= o.qty;
        dedupe.put(key(o.firm, o.clOrdId), "SEEN");
        dedupe.put(k, o);
        o.clOrdId = cl;
        o.gateway = d.gatewayId();
        o.session = d.sessionId();
        o.qty = newQty;
        if (keep)
        {
            er(o, o.session, cl, d.origClOrdId(), 0, 0, NULL, o.leaves(), d.gatewayInTsNs(), "REPLACED", status(o), "NONE", "NONE", "NONE");
        }
        else
        {
            s.book.remove(o);
            o.price = newPrice;
            o.riskPrice = newPrice;
            er(o, o.session, cl, d.origClOrdId(), 0, 0, NULL, o.leaves(), d.gatewayInTsNs(), "REPLACED", status(o), "NONE", "NONE", "NONE");
            if (s.state == 3)
            {
                match(o, d.gatewayInTsNs());
            }
            finish(o, d.gatewayInTsNs());
        }
        diff(before, true);
    }

    void massCancel(final MassCancelDecoder d)
    {
        final int scope = d.scopeRaw();
        final int reason = d.reasonRaw();
        if (scope < 1 || scope > 5 || (reason != 2 && reason != 3 && reason != 8))
        {
            out.add("CMDREJ corr=" + d.requestId() + " t=4 r=MALFORMED");
            return;
        }
        final String reasonName = reason == 2 ? "MASS_CANCEL" : reason == 3 ? "DISCONNECT" : "ADMIN";
        final int firm = (int)d.firmId();
        final boolean firmKnown = firms.containsKey(firm);
        final S s = sym(d.symbolId());
        final TreeMap<String, long[]> before = levels();
        int n = 0;
        for (final O o : new ArrayList<>(live))
        {
            final boolean hit = switch (scope)
            {
                case 1 -> firmKnown && o.firm == firm;
                case 2 -> firmKnown && o.firm == firm && o.session == d.sessionId();
                case 3 -> firmKnown && o.firm == firm && o.sym == s;
                case 4 -> o.gateway == d.targetGatewayId();
                default -> o.sym == s;
            };
            if (hit)
            {
                cancelResting(o, "CANCELED", reasonName, o.clOrdId, 0, o.session, o.firm == firm ? d.gatewayInTsNs() : 0);
                n++;
            }
        }
        diff(before, true);
        out.add("MASSDONE req=" + d.requestId() + " sess=" + d.sessionId() + " f=" + d.firmId() + " n=" + n + " g=" +
            d.gatewayId() + " scope=" + MassCancelScope.get((short)scope));
    }

    // ------------------------------------------------------------------ control

    static void band(final S s)
    {
        if (s.ref == NULL)
        {
            s.bandLow = NULL;
            s.bandHigh = NULL;
            return;
        }
        final java.math.BigInteger ref = java.math.BigInteger.valueOf(s.ref);
        final java.math.BigInteger den = java.math.BigInteger.valueOf(10_000L * s.tick);
        long low;
        if (s.bps >= 10_000)
        {
            low = s.tick;
        }
        else
        {
            final java.math.BigInteger[] qr = ref.multiply(java.math.BigInteger.valueOf(10_000L - s.bps)).divideAndRemainder(den);
            low = (qr[0].longValue() + (qr[1].signum() > 0 ? 1 : 0)) * s.tick;
        }
        low = Math.max(low, s.tick);
        long high = ref.multiply(java.math.BigInteger.valueOf(10_000L + s.bps)).divide(den).longValue() * s.tick;
        high = Math.min(high, (1_000_000_000L / s.tick) * s.tick);
        s.bandLow = low;
        s.bandHigh = high;
    }

    void ref(final ReferencePriceDecoder d)
    {
        final S s = sym(d.symbolId());
        if (s == null)
        {
            out.add("CMDREJ corr=" + d.sourceTsNs() + " t=10 r=UNKNOWN_SYMBOL");
            return;
        }
        if (d.sourceTsNs() <= s.lastTs)
        {
            return;
        }
        if (d.referencePrice() <= 0 || d.referencePrice() > 1_000_000_000L)
        {
            out.add("CMDREJ corr=" + d.sourceTsNs() + " t=10 r=INVALID_PRICE");
            return;
        }
        s.ref = d.referencePrice();
        s.lastTs = d.sourceTsNs();
        band(s);
        out.add("REF sym=" + s.id + " ref=" + s.ref + " bid=" + d.nbbBid() + " ofr=" + d.nbbOffer() + " band=" + s.bandLow +
            ".." + s.bandHigh + " ts=" + s.lastTs);
        if (s.state == 2)
        {
            indicative(s);
        }
    }

    void symbolDef(final SymbolDefinitionDecoder d)
    {
        final long corr = d.correlationId();
        final int init = d.initialStateRaw();
        if (init < 1 || init > 6)
        {
            out.add("CMDREJ corr=" + corr + " t=11 r=MALFORMED");
            return;
        }
        final long tick = d.tickSize();
        final long ref = d.referencePrice();
        String rej = null;
        if (tick <= 0 || tick > 1_000_000_000L || (ref != NULL && (ref <= 0 || ref > 1_000_000_000L)))
        {
            rej = "INVALID_PRICE";
        }
        else if (d.lotSize() <= 0 || d.lotSize() > 1_000_000_000L || d.maxOrderQty() <= 0 || d.maxOrderQty() > 1_000_000_000L)
        {
            rej = "INVALID_QTY";
        }
        else if (d.symbolId() == NULL_SYMBOL)
        {
            rej = "UNKNOWN_SYMBOL";
        }
        S s = symbols.get((int)d.symbolId());
        final boolean fresh = s == null || s.state == 6;
        if (rej == null && fresh && init == 6)
        {
            rej = "INVALID_STATE_TRANSITION";
        }
        if (rej != null)
        {
            out.add("CMDREJ corr=" + corr + " t=11 r=" + rej);
            return;
        }
        if (s == null)
        {
            s = new S();
            s.id = (int)d.symbolId();
            symbols.put(s.id, s);
        }
        if (fresh)
        {
            s.state = init;
            s.ref = ref;
            s.lastTs = Long.MIN_VALUE;
            s.aPx = NULL;
            s.aV = 0;
            s.aImb = 0;
            s.aSide = 0;
        }
        else if (ref != NULL && s.lastTs == Long.MIN_VALUE)
        {
            s.ref = ref;
        }
        s.tick = tick;
        s.lot = d.lotSize();
        s.maxQty = d.maxOrderQty();
        s.bps = d.priceBandBps();
        s.ticker = d.ticker().trim();
        band(s);
        out.add("SYMDEF corr=" + corr + " sym=" + s.id + " tick=" + s.tick + " lot=" + s.lot + " ref=" + s.ref + " max=" +
            s.maxQty + " bps=" + s.bps + " ticker=" + s.ticker + " state=" + STATES[s.state]);
        if (s.state == 2)
        {
            indicative(s);
        }
    }

    void stateCmd(final SymbolStateCommandDecoder d)
    {
        final long corr = d.correlationId();
        final int target = d.targetStateRaw();
        if (target < 1 || target > 6)
        {
            out.add("CMDREJ corr=" + corr + " t=12 r=MALFORMED");
            return;
        }
        if (d.symbolId() != NULL_SYMBOL && sym(d.symbolId()) == null)
        {
            out.add("CMDREJ corr=" + corr + " t=12 r=UNKNOWN_SYMBOL");
            return;
        }
        if (d.effectiveTimeNs() > 0)
        {
            pending.add(new Pending(++timerSeq, corr, d.symbolId(), d.reasonCode(), target));
            return;
        }
        applyState(d.symbolId(), target, corr, d.reasonCode());
    }

    void applyState(final long symId, final int target, final long corr, final int reason)
    {
        if (symId == NULL_SYMBOL)
        {
            for (final S s : symbols.values())
            {
                if (s.state != 6 && Codes.allowed((byte)s.state, (byte)target))
                {
                    transition(s, target, corr, reason);
                }
            }
            return;
        }
        final S s = sym(symId);
        if (s == null)
        {
            out.add("CMDREJ corr=" + corr + " t=12 r=UNKNOWN_SYMBOL");
        }
        else if (!Codes.allowed((byte)s.state, (byte)target))
        {
            out.add("CMDREJ corr=" + corr + " t=12 r=INVALID_STATE_TRANSITION");
        }
        else
        {
            transition(s, target, corr, reason);
        }
    }

    boolean crossed(final S s)
    {
        long bb = Long.MIN_VALUE;
        long ba = Long.MAX_VALUE;
        for (final O o : s.book)
        {
            if (o.side == 1)
            {
                bb = Math.max(bb, o.price);
            }
            else
            {
                ba = Math.min(ba, o.price);
            }
        }
        return bb >= ba;
    }

    void transition(final S s, final int target, final long corr, final int reason)
    {
        final int prev = s.state;
        if ((prev == 2 && (target == 3 || target == 5)) || (target == 3 && crossed(s)))
        {
            uncross(s);
        }
        s.state = target;
        out.add("STATE corr=" + corr + " sym=" + s.id + " rc=" + reason + " " + STATES[prev] + "->" + STATES[target]);
        if (target == 5 || target == 6)
        {
            final TreeMap<String, long[]> before = levels();
            for (final O o : new ArrayList<>(live))
            {
                if (o.sym == s)
                {
                    cancelResting(o, target == 5 ? "EXPIRED" : "CANCELED", target == 5 ? "END_OF_DAY" : "SYMBOL_REMOVED",
                        o.clOrdId, 0, o.session, 0);
                }
            }
            diff(before, false);
            s.aPx = NULL;
            s.aV = 0;
            s.aImb = 0;
            s.aSide = 0;
        }
        else if (target == 2)
        {
            s.aPx = NULL;
            s.aV = 0;
            s.aImb = 0;
            s.aSide = 0;
            indicative(s);
        }
    }

    /** Brute force: [price, volume, imbalance, side]. */
    long[] auction(final S s)
    {
        final List<Long> candidates = new ArrayList<>();
        for (final O o : s.book)
        {
            candidates.add(o.price);
        }
        if (s.ref != NULL && s.ref % s.tick == 0)
        {
            candidates.add(s.ref);
        }
        long[] best = {NULL, 0, 0, 0};
        for (final long p : candidates)
        {
            long buy = 0;
            long sell = 0;
            for (final O o : s.book)
            {
                if (o.side == 1 && o.price >= p)
                {
                    buy += o.leaves();
                }
                if (o.side == 2 && o.price <= p)
                {
                    sell += o.leaves();
                }
            }
            final long v = Math.min(buy, sell);
            if (v == 0)
            {
                continue;
            }
            final long imb = Math.abs(buy - sell);
            final long dist = s.ref == NULL ? 0 : Math.abs(p - s.ref);
            final long bestDist = best[0] == NULL || s.ref == NULL ? 0 : Math.abs(best[0] - s.ref);
            final boolean better = best[0] == NULL || v > best[1] || (v == best[1] && (imb < best[2] ||
                (imb == best[2] && (dist < bestDist || (dist == bestDist && p < best[0])))));
            if (better)
            {
                best = new long[] {p, v, imb, buy == sell ? 0 : buy > sell ? 1 : 2};
            }
        }
        return best;
    }

    void indicative(final S s)
    {
        final long[] a = auction(s);
        if (a[0] != s.aPx || a[1] != s.aV || a[2] != s.aImb || a[3] != s.aSide)
        {
            s.aPx = a[0];
            s.aV = a[1];
            s.aImb = a[2];
            s.aSide = (int)a[3];
            out.add("AUCT sym=" + s.id + " px=" + a[0] + " v=" + a[1] + " imb=" + a[2] + " side=" + (a[3] == 0 ? 255 : a[3]) + " final=FALSE");
        }
    }

    void uncross(final S s)
    {
        final long[] a = auction(s);
        final TreeMap<String, long[]> before = levels();
        if (a[1] > 0)
        {
            final Comparator<O> bidOrder = Comparator.<O>comparingLong(o -> -o.price).thenComparingLong(o -> o.time);
            final Comparator<O> askOrder = Comparator.<O>comparingLong(o -> o.price).thenComparingLong(o -> o.time);
            while (true)
            {
                final O b = s.book.stream().filter(o -> o.side == 1 && o.price >= a[0]).min(bidOrder).orElse(null);
                final O x = s.book.stream().filter(o -> o.side == 2 && o.price <= a[0]).min(askOrder).orElse(null);
                if (b == null || x == null)
                {
                    break;
                }
                final long q = Math.min(b.leaves(), x.leaves());
                final long mid = ((long)shard << 48) | ++matchSeq;
                fill(b, q, a[0]);
                fill(x, q, a[0]);
                out.add("TRADE m=" + mid + " px=" + a[0] + " q=" + q + " b=" + b.orderId + " s=" + x.orderId + " sym=" + s.id +
                    " bf=" + b.firm + " sf=" + x.firm + " agg=255 auct=TRUE");
                fillEr(b, mid, q, a[0], 0, "AUCTION");
                fillEr(x, mid, q, a[0], 0, "AUCTION");
                if (b.leaves() == 0)
                {
                    done(b, "FILLED");
                }
                if (x.leaves() == 0)
                {
                    done(x, "FILLED");
                }
            }
        }
        diff(before, false);
        out.add("AUCT sym=" + s.id + " px=" + a[0] + " v=" + a[1] + " imb=" + a[2] + " side=" + (a[3] == 0 ? 255 : a[3]) + " final=TRUE");
        s.aPx = NULL;
        s.aV = 0;
        s.aImb = 0;
        s.aSide = 0;
    }

    void risk(final RiskLimitUpdateDecoder d)
    {
        final long corr = d.correlationId();
        if (d.enabledRaw() > 1)
        {
            out.add("CMDREJ corr=" + corr + " t=13 r=MALFORMED");
            return;
        }
        if (d.creditBudget() < 0)
        {
            out.add("CMDREJ corr=" + corr + " t=13 r=CREDIT_LIMIT");
            return;
        }
        if (d.maxPositionPerSymbol() < 0)
        {
            out.add("CMDREJ corr=" + corr + " t=13 r=POSITION_LIMIT");
            return;
        }
        final F f = firms.computeIfAbsent((int)d.firmId(), id ->
        {
            final F n = new F();
            n.id = id;
            return n;
        });
        f.budget = d.creditBudget();
        f.maxPos = d.maxPositionPerSymbol();
        f.enabled = d.enabledRaw() == 1;
        out.add("RISK corr=" + corr + " f=" + d.firmId() + " budget=" + f.budget + " used=" + creditUsed(f) + " maxPos=" +
            f.maxPos + " en=" + (f.enabled ? "TRUE" : "FALSE"));
    }

    void dayRoll(final DayRolloverDecoder d)
    {
        final TreeMap<String, long[]> before = levels();
        int n = 0;
        for (final O o : new ArrayList<>(live))
        {
            cancelResting(o, "EXPIRED", "END_OF_DAY", o.clOrdId, 0, o.session, 0);
            n++;
        }
        diff(before, false);
        dedupe.clear();
        firms.values().forEach(f -> f.tradedNet = 0);
        positions.clear();
        for (final S s : symbols.values())
        {
            if (s.state != 6 && s.state != 1)
            {
                out.add("STATE corr=" + d.correlationId() + " sym=" + s.id + " rc=0 " + STATES[s.state] + "->PRE_OPEN");
                s.state = 1;
                s.aPx = NULL;
                s.aV = 0;
                s.aImb = 0;
                s.aSide = 0;
            }
        }
        out.add("DAYROLLED corr=" + d.correlationId() + " date=" + d.tradingDate() + " n=" + n);
    }

    static String stateName(final SymbolState s)
    {
        return s.name();
    }
}
