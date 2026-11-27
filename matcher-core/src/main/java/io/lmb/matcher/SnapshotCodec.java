package io.lmb.matcher;

import io.lmb.api.SnapshotSink;
import io.lmb.common.HashChain;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * Hand-written little-endian snapshot format; only the current version is read. Everything is written in a canonical order that
 * does not depend on hash table layout, pool indices or configured capacities, so two machines with the
 * same history produce byte-identical snapshots and a restored machine produces the same next snapshot.
 *
 * <pre>
 * magic "LMBM" int32, version int32, shardId int32
 * eventSeq, orderSeq, matchSeq, timerSeq int64; tradingDate int32; chain hash 32 bytes
 * symbols   (definition order): symbolId, ticker[8], tick, lot, maxQty, bps, ref, lastRefSourceTs, state, auction indicative,
 *                               dynamicBandBps, volatilityAuctionNs, volTimerId, volEndNs (lastTradePx is in dayCycle)
 * firms     (creation order):   firmId, budget, maxPos, enabled, openNotional, tradedNet
 * firmSyms  (creation order):   firmIdx, symbolIdx, netPosition, openBuyQty, openSellQty
 * orders    (orderId order):    every live order's fields
 * levels    per symbol, BUY then SELL, best first: price, count, orderIds in FIFO order
 * dedupe    (insertion order):  firmId, clOrdId, code (LIVE codes carry the orderId)
 * pending   (timerId order):    scheduled transitions
 * dayCycle  (definition order, one per symbol): lastTradePx, closePx, prevClosePx int64; closeSource int8;
 *           flags int8 (bit 0 tradedToday, bit 1 hasClose)
 * crc32 of everything above
 * </pre>
 * The bytes are split into chunks of at most {@link SnapshotSink#MAX_CHUNK_LENGTH}.
 */
final class SnapshotCodec
{
    static final int MAGIC = 0x4D424D4C; // "LMBM"
    static final int VERSION = 2;
    private static final byte TAG_LIVE = 0;

    private SnapshotCodec()
    {
    }

    private static final class Out
    {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(64 * 1024);
        int pos;

        void i32(final int v)
        {
            buf.putInt(pos, v, ByteOrder.LITTLE_ENDIAN);
            pos += 4;
        }

        void i64(final long v)
        {
            buf.putLong(pos, v, ByteOrder.LITTLE_ENDIAN);
            pos += 8;
        }

        void i8(final int v)
        {
            buf.putByte(pos, (byte)v);
            pos += 1;
        }

        void bytes(final byte[] b, final int len)
        {
            buf.putBytes(pos, b, 0, len);
            pos += len;
        }
    }

    static final class Loader
    {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(64 * 1024);
        int length;
        int pos;

        void append(final DirectBuffer src, final int offset, final int len)
        {
            buf.putBytes(length, src, offset, len);
            length += len;
        }

        private void need(final int n)
        {
            if (pos + n > length)
            {
                throw new IllegalStateException("truncated snapshot at " + pos);
            }
        }

        int i32()
        {
            need(4);
            final int v = buf.getInt(pos, ByteOrder.LITTLE_ENDIAN);
            pos += 4;
            return v;
        }

        long i64()
        {
            need(8);
            final long v = buf.getLong(pos, ByteOrder.LITTLE_ENDIAN);
            pos += 8;
            return v;
        }

        byte i8()
        {
            need(1);
            return buf.getByte(pos++);
        }

        void bytes(final byte[] dst, final int len)
        {
            need(len);
            buf.getBytes(pos, dst, 0, len);
            pos += len;
        }
    }

    static void write(final MatcherShard m, final SnapshotSink sink)
    {
        final Out out = new Out();
        out.i32(MAGIC);
        out.i32(VERSION);
        out.i32(m.shardId);
        out.i64(m.ew.eventSeq);
        out.i64(m.orderSeq);
        out.i64(m.matchSeq);
        out.i64(m.timerSeq);
        out.i32(m.tradingDate);
        out.bytes(m.ew.chain.current(), HashChain.HASH_LENGTH);

        out.i32(m.symbols.size());
        for (int i = 0; i < m.symbols.size(); i++)
        {
            final Symbol s = m.symbols.get(i);
            out.i32(s.symbolId);
            out.bytes(s.ticker, 8);
            out.i64(s.tickSize);
            out.i64(s.lotSize);
            out.i64(s.maxOrderQty);
            out.i64(s.bandBps);
            out.i64(s.refPrice);
            out.i64(s.lastRefSourceTs);
            out.i8(s.state);
            out.i64(s.bids.indexTick);
            out.i64(s.auctPrice);
            out.i64(s.auctMatched);
            out.i64(s.auctImbalance);
            out.i8(s.auctSide);
            // band-stp
            out.i64(s.dynamicBandBps);
            out.i64(s.volatilityAuctionNs);
            out.i64(s.volTimerId);
            out.i64(s.volEndNs);
        }

        out.i32(m.firms.size());
        for (int i = 0; i < m.firms.size(); i++)
        {
            final Firm f = m.firms.get(i);
            out.i32(f.firmId);
            out.i64(f.creditBudget);
            out.i64(f.maxPositionPerSymbol);
            out.i8(f.enabled ? 1 : 0);
            out.i64(f.openNotional);
            out.i64(f.tradedNet);
        }

        out.i32(m.firmSymbols.size());
        for (int i = 0; i < m.firmSymbols.size(); i++)
        {
            final FirmSymbol fs = m.firmSymbols.get(i);
            out.i32(fs.firm.idx);
            out.i32(fs.symbol.idx);
            out.i64(fs.netPosition);
            out.i64(fs.openBuyQty);
            out.i64(fs.openSellQty);
        }

        out.i32(m.allOrders.size);
        for (Order o = m.allOrders.head; o != null; o = o.next[Order.L_GLOBAL])
        {
            out.i64(o.orderId);
            out.i64(o.clOrdId);
            out.i64(o.sessionId);
            out.i64(o.price);
            out.i64(o.riskPrice);
            out.i64(o.qty);
            out.i64(o.cumQty);
            out.i32(o.accountId);
            out.i32(o.gatewayId);
            out.i8(o.side);
            out.i8(o.ordType);
            out.i8(o.tif);
            out.i32(o.firm.idx);
            out.i32(o.symbol.idx);
        }

        for (int i = 0; i < m.symbols.size(); i++)
        {
            final Symbol s = m.symbols.get(i);
            writeSide(out, s.bids);
            writeSide(out, s.asks);
        }

        final LongPairIntMap d = m.dedupe;
        out.i32(d.size());
        for (int e = 0; e < d.size(); e++)
        {
            out.i64(d.keyA(e));
            out.i64(d.keyB(e));
            final int v = d.value(e);
            if (v >= 0)
            {
                out.i8(TAG_LIVE);
                out.i64(m.orderPool.byIndex(v).orderId);
            }
            else
            {
                out.i8(-v); // 1..4
            }
        }

        out.i32(m.pending.size());
        for (int i = 0; i < m.pending.size(); i++)
        {
            final PendingTransition p = m.pending.get(i);
            out.i64(p.timerId);
            out.i64(p.correlationId);
            out.i64(p.symbolId);
            out.i32(p.reasonCode);
            out.i8(p.targetState);
            out.i64(p.effectiveTimeNs);
        }

        // day-cycle section
        for (int i = 0; i < m.symbols.size(); i++)
        {
            final Symbol s = m.symbols.get(i);
            out.i64(s.lastTradePx);
            out.i64(s.closePx);
            out.i64(s.prevClosePx);
            out.i8(s.closeSource);
            out.i8((s.tradedToday ? 1 : 0) | (s.hasClose ? 2 : 0));
        }

        final CRC32 crc = new CRC32();
        crc.update(out.buf.byteArray(), 0, out.pos);
        out.i32((int)crc.getValue());

        for (int off = 0; off < out.pos; off += SnapshotSink.MAX_CHUNK_LENGTH)
        {
            sink.write(out.buf, off, Math.min(SnapshotSink.MAX_CHUNK_LENGTH, out.pos - off));
        }
    }

    private static void writeSide(final Out out, final BookSide side)
    {
        out.i32(side.size);
        for (int i = side.size - 1; i >= 0; i--)
        {
            final PriceLevel l = side.sorted[i];
            out.i64(l.price);
            out.i32(l.orders.size);
            for (Order o = l.orders.head; o != null; o = o.next[Order.L_LEVEL])
            {
                out.i64(o.orderId);
            }
        }
    }

    static void load(final MatcherShard m, final Loader in)
    {
        if (!m.symbols.isEmpty() || !m.firms.isEmpty() || m.allOrders.size != 0 || m.ew.eventSeq != 0)
        {
            throw new IllegalStateException("snapshot must be loaded into a fresh machine");
        }
        if (in.length < 4)
        {
            throw new IllegalStateException("snapshot too short");
        }
        final CRC32 crc = new CRC32();
        crc.update(in.buf.byteArray(), 0, in.length - 4);
        if ((int)crc.getValue() != in.buf.getInt(in.length - 4, ByteOrder.LITTLE_ENDIAN))
        {
            throw new IllegalStateException("snapshot checksum mismatch");
        }
        if (in.i32() != MAGIC)
        {
            throw new IllegalStateException("not a matcher-core snapshot");
        }
        final int version = in.i32();
        if (version != VERSION)
        {
            throw new IllegalStateException("unsupported matcher-core snapshot version " + version);
        }
        final int shard = in.i32();
        if (shard != m.shardId)
        {
            throw new IllegalStateException("snapshot of shard " + shard + " loaded into shard " + m.shardId);
        }
        m.ew.eventSeq = in.i64();
        m.orderSeq = in.i64();
        m.matchSeq = in.i64();
        m.timerSeq = in.i64();
        m.tradingDate = in.i32();
        final byte[] hash = new byte[HashChain.HASH_LENGTH];
        in.bytes(hash, HashChain.HASH_LENGTH);
        m.ew.chain.set(hash, 0);

        final int nSym = in.i32();
        for (int i = 0; i < nSym; i++)
        {
            final Symbol s = m.restoreSymbol(in.i32());
            in.bytes(s.ticker, 8);
            s.tickSize = in.i64();
            s.lotSize = in.i64();
            s.maxOrderQty = in.i64();
            s.bandBps = in.i64();
            s.refPrice = in.i64();
            s.lastRefSourceTs = in.i64();
            s.state = in.i8();
            final long indexTick = in.i64();
            s.bids.indexTick = indexTick;
            s.asks.indexTick = indexTick;
            s.auctPrice = in.i64();
            s.auctMatched = in.i64();
            s.auctImbalance = in.i64();
            s.auctSide = in.i8();
            // band-stp
            s.dynamicBandBps = in.i64();
            s.volatilityAuctionNs = in.i64();
            s.volTimerId = in.i64();
            s.volEndNs = in.i64();
            m.restoreVolatilityTimer(s);
            s.computeBand();
            if (s.refPrice != Codes.NULL_PRICE)
            {
                s.bids.maybeRecentre(s.refPrice);
                s.asks.maybeRecentre(s.refPrice);
            }
        }

        final int nFirm = in.i32();
        final long[] expectedOpen = new long[nFirm];
        for (int i = 0; i < nFirm; i++)
        {
            final Firm f = m.restoreFirm(in.i32());
            f.creditBudget = in.i64();
            f.maxPositionPerSymbol = in.i64();
            f.enabled = in.i8() != 0;
            expectedOpen[i] = in.i64();
            f.tradedNet = in.i64();
        }

        final int nFs = in.i32();
        final long[] expectedFs = new long[nFs * 2];
        for (int i = 0; i < nFs; i++)
        {
            final FirmSymbol fs = m.fs(m.firms.get(in.i32()), m.symbols.get(in.i32()));
            fs.netPosition = in.i64();
            expectedFs[2 * i] = in.i64();
            expectedFs[2 * i + 1] = in.i64();
        }

        final int nOrders = in.i32();
        for (int i = 0; i < nOrders; i++)
        {
            final Order o = m.orderPool.acquire();
            o.orderId = in.i64();
            o.clOrdId = in.i64();
            o.sessionId = in.i64();
            o.price = in.i64();
            o.riskPrice = in.i64();
            o.qty = in.i64();
            o.cumQty = in.i64();
            o.accountId = in.i32();
            o.gatewayId = in.i32();
            o.side = in.i8();
            o.ordType = in.i8();
            o.tif = in.i8();
            o.firm = m.firms.get(in.i32());
            o.symbol = m.symbols.get(in.i32());
            o.fs = m.fs(o.firm, o.symbol);
            o.dedupeEntry = -1;
            m.restoreRestingOrder(o);
            o.firm.openNotional += o.leaves() * o.riskPrice;
            if (o.isBuy())
            {
                o.fs.openBuyQty += o.leaves();
            }
            else
            {
                o.fs.openSellQty += o.leaves();
            }
        }

        int placed = 0;
        for (int i = 0; i < nSym; i++)
        {
            final Symbol s = m.symbols.get(i);
            placed += readSide(m, in, s.bids);
            placed += readSide(m, in, s.asks);
        }
        if (placed != nOrders)
        {
            throw new IllegalStateException("snapshot levels hold " + placed + " orders, expected " + nOrders);
        }

        final int nDedupe = in.i32();
        for (int e = 0; e < nDedupe; e++)
        {
            final long a = in.i64();
            final long b = in.i64();
            final byte tag = in.i8();
            if (tag == TAG_LIVE)
            {
                final Order o = m.orderByOrderId(in.i64());
                if (o == null)
                {
                    throw new IllegalStateException("dedupe entry refers to unknown order");
                }
                o.dedupeEntry = m.dedupe.insert(a, b, o.poolIndex);
            }
            else
            {
                m.dedupe.insert(a, b, -tag);
            }
        }

        final int nPending = in.i32();
        for (int i = 0; i < nPending; i++)
        {
            m.pending.add(new PendingTransition(in.i64(), in.i64(), in.i64(), in.i32(), in.i8(), in.i64()));
        }
        for (int i = 0; i < nSym; i++)
        {
            final Symbol s = m.symbols.get(i);
            s.lastTradePx = in.i64();
            s.closePx = in.i64();
            s.prevClosePx = in.i64();
            s.closeSource = in.i8();
            final byte flags = in.i8();
            if (s.closeSource < Codes.CLOSE_NONE || s.closeSource > Codes.CLOSE_PREVIOUS_CLOSE || (flags & ~3) != 0)
            {
                throw new IllegalStateException("invalid day-cycle entry in snapshot");
            }
            s.tradedToday = (flags & 1) != 0;
            s.hasClose = (flags & 2) != 0;
        }
        in.i32(); // crc, already verified
        if (in.pos != in.length)
        {
            throw new IllegalStateException("trailing bytes in snapshot");
        }

        for (Order o = m.allOrders.head; o != null; o = o.next[Order.L_GLOBAL])
        {
            if (o.dedupeEntry < 0)
            {
                throw new IllegalStateException("order " + o.orderId + " has no dedupe entry");
            }
        }
        for (int i = 0; i < nFirm; i++)
        {
            if (m.firms.get(i).openNotional != expectedOpen[i])
            {
                throw new IllegalStateException("firm open notional mismatch after restore");
            }
        }
        for (int i = 0; i < nFs; i++)
        {
            final FirmSymbol fs = m.firmSymbols.get(i);
            if (fs.openBuyQty != expectedFs[2 * i] || fs.openSellQty != expectedFs[2 * i + 1])
            {
                throw new IllegalStateException("open quantity mismatch after restore");
            }
        }
    }

    private static int readSide(final MatcherShard m, final Loader in, final BookSide side)
    {
        final int nLevels = in.i32();
        int total = 0;
        for (int i = 0; i < nLevels; i++)
        {
            final long price = in.i64();
            final int count = in.i32();
            final PriceLevel l = m.restoreLevel(side, price);
            for (int k = 0; k < count; k++)
            {
                final Order o = m.orderByOrderId(in.i64());
                if (o == null || o.symbol != side.symbol || o.isBuy() != side.buy || o.price != price)
                {
                    throw new IllegalStateException("inconsistent level in snapshot");
                }
                m.restoreAppendToLevel(side, l, o);
            }
            total += count;
        }
        return total;
    }
}
