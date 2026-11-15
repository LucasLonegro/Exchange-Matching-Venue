package io.lmb.common;

import org.agrona.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The audit hash chain carried by CommandApplied events. For log entry n:
 *
 * <pre>
 * chain(n) = SHA-256( chain(n-1) | int64le(logPosition) | int64le(timestampNs) | input | event_0 | ... | event_k-1 )
 * </pre>
 *
 * where input is the command's bytes (SBE messageHeader + body) or, for a timer, int64le(correlationId);
 * event_i are the bytes of every event emitted for the entry (messageHeader + body) excluding the
 * CommandApplied itself; chain(0) is 32 zero bytes. Usage per entry: begin, update*, finish.
 * Not thread-safe; allocation-free after construction.
 */
public final class HashChain
{
    public static final int HASH_LENGTH = 32;

    private final MessageDigest sha;
    private final byte[] chain = new byte[HASH_LENGTH];
    private final byte[] scratch = new byte[4096];
    private final ByteBuffer longs = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);

    public HashChain()
    {
        try
        {
            sha = MessageDigest.getInstance("SHA-256");
        }
        catch (final NoSuchAlgorithmException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public void begin(final long logPosition, final long timestampNs)
    {
        sha.reset();
        sha.update(chain, 0, HASH_LENGTH);
        longs.putLong(0, logPosition).putLong(8, timestampNs);
        sha.update(longs.array(), 0, 16);
    }

    public void update(final DirectBuffer buffer, final int offset, final int length)
    {
        int remaining = length;
        int position = offset;
        while (remaining > 0)
        {
            final int n = Math.min(remaining, scratch.length);
            buffer.getBytes(position, scratch, 0, n);
            sha.update(scratch, 0, n);
            position += n;
            remaining -= n;
        }
    }

    public void updateLong(final long value)
    {
        longs.putLong(0, value);
        sha.update(longs.array(), 0, 8);
    }

    /** Completes the entry and returns the new chain value (the internal array: copy it, do not modify it). */
    public byte[] finish()
    {
        try
        {
            sha.digest(chain, 0, HASH_LENGTH);
        }
        catch (final DigestException e)
        {
            throw new IllegalStateException(e);
        }
        return chain;
    }

    /** Current chain value (the internal array: copy it, do not modify it). */
    public byte[] current()
    {
        return chain;
    }

    /** Restore from a snapshot. */
    public void set(final byte[] src, final int offset)
    {
        System.arraycopy(src, offset, chain, 0, HASH_LENGTH);
    }
}
