package io.lmb.marketdata;

import io.lmb.marketdata.book.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Publisher state at a batch boundary: per shard the eventSeq of the last applied CommandApplied, per
 * symbol its md_seq, parameters and book. Because the {@link BookBuilder} is a pure function of the event
 * streams, a builder restored from a checkpoint and fed the events after those eventSeqs produces exactly
 * the messages (same md_seq, same bytes) that the original produced or would have produced.
 *
 * <p>File {@code state.bin} in the instance's data directory: magic, version, sessionId, shards, symbols,
 * CRC32 of everything before it. Written to a temporary file and atomically renamed.
 */
public final class StateCheckpoint
{
    private static final Logger LOG = LoggerFactory.getLogger(StateCheckpoint.class);
    private static final int MAGIC = 0x4C4D4D44; // "LMMD"
    private static final int VERSION = 1;
    public static final String FILE_NAME = "state.bin";

    private StateCheckpoint()
    {
    }

    /** Serialises the builder's committed state. Must run on the builder's thread. Allocates. */
    public static byte[] serialize(final BookBuilder builder, final long sessionId)
    {
        try
        {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream(64 * 1024);
            final DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(sessionId);
            final int[] shards = builder.shardIds();
            out.writeInt(shards.length);
            for (final int shard : shards)
            {
                out.writeInt(shard);
                out.writeLong(builder.committedEventSeq(shard));
            }
            out.writeInt(builder.sortedSymbolIds().size());
            for (int i = 0; i < builder.sortedSymbolIds().size(); i++)
            {
                final SymbolBook s = builder.symbol(builder.sortedSymbolIds().getInt(i));
                out.writeInt(s.symbolId);
                out.writeInt(s.shardId);
                out.writeLong(s.seq);
                out.writeLong(s.lastTransactTimeNs);
                out.writeLong(s.tickSize);
                out.writeLong(s.lotSize);
                out.writeLong(s.referencePrice);
                out.writeInt(s.state);
                out.write(s.ticker);
                for (int side = OrderBook.BID; side <= OrderBook.ASK; side++)
                {
                    final int n = s.book.levels(side);
                    out.writeInt(n);
                    for (int k = 0; k < n; k++)
                    {
                        out.writeLong(s.book.price(side, k));
                        out.writeLong(s.book.quantity(side, k));
                        out.writeInt(s.book.orderCount(side, k));
                    }
                }
            }
            out.flush();
            final CRC32 crc = new CRC32();
            crc.update(bytes.toByteArray());
            out.writeLong(crc.getValue());
            out.flush();
            return bytes.toByteArray();
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e); // in-memory stream
        }
    }

    /** Writes atomically (temp file, fsync, rename). Any thread. */
    public static void write(final Path dir, final byte[] data) throws IOException
    {
        Files.createDirectories(dir);
        final Path tmp = dir.resolve(FILE_NAME + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING))
        {
            ch.write(java.nio.ByteBuffer.wrap(data));
            ch.force(true);
        }
        Files.move(tmp, dir.resolve(FILE_NAME), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Restores the builder (which must be fresh) from the checkpoint in dir. Returns false, leaving the
     * builder untouched, if there is no checkpoint; throws if it is corrupt or from another sessionId.
     */
    public static boolean restore(final Path dir, final BookBuilder builder, final long sessionId) throws IOException
    {
        final Path file = dir.resolve(FILE_NAME);
        if (!Files.exists(file))
        {
            return false;
        }
        final byte[] data = Files.readAllBytes(file);
        if (data.length < 8)
        {
            throw new IOException("checkpoint " + file + " truncated");
        }
        final CRC32 crc = new CRC32();
        crc.update(data, 0, data.length - 8);
        final DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        in.skipNBytes(data.length - 8);
        if (in.readLong() != crc.getValue())
        {
            throw new IOException("checkpoint " + file + " fails its CRC");
        }
        final DataInputStream d = new DataInputStream(new ByteArrayInputStream(data, 0, data.length - 8));
        if (d.readInt() != MAGIC || d.readInt() != VERSION)
        {
            throw new IOException("checkpoint " + file + " has an unknown format");
        }
        final long fileSession = d.readLong();
        if (fileSession != sessionId)
        {
            throw new IOException("checkpoint " + file + " is for sessionId " + fileSession + ", configured " + sessionId);
        }
        final int shards = d.readInt();
        for (int i = 0; i < shards; i++)
        {
            builder.restoreShard(d.readInt(), d.readLong());
        }
        final int symbols = d.readInt();
        for (int i = 0; i < symbols; i++)
        {
            final int symbolId = d.readInt();
            final SymbolBook s = builder.restoreSymbol(symbolId, d.readInt());
            s.seq = d.readLong();
            s.lastTransactTimeNs = d.readLong();
            s.tickSize = d.readLong();
            s.lotSize = d.readLong();
            s.referencePrice = d.readLong();
            s.state = d.readInt();
            d.readFully(s.ticker);
            for (int side = OrderBook.BID; side <= OrderBook.ASK; side++)
            {
                final int n = d.readInt();
                for (int k = 0; k < n; k++)
                {
                    s.book.set(side, d.readLong(), d.readLong(), d.readInt());
                }
            }
        }
        LOG.info("restored market data state from {}: {} shards, {} symbols", file, shards, symbols);
        return true;
    }
}
