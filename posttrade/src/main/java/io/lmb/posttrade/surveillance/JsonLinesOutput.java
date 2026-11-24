package io.lmb.posttrade.surveillance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Local surveillance output: one JSON record per line, appended to a file. The file is its own cursor: on open a
 * torn last line (a crash mid-write) is truncated and the resume point per shard is the largest eventSeq in the
 * file, so every record is in the file exactly once. Flush = write + fsync.
 */
public final class JsonLinesOutput implements SurveillanceOutput
{
    private static final Logger LOG = LoggerFactory.getLogger(JsonLinesOutput.class);
    private static final String SHARD = "{\"shard\":";
    private static final String SEQ = ",\"eventSeq\":";

    private final Path file;
    private FileOutputStream fos;
    private BufferedOutputStream out;

    public JsonLinesOutput(final Path file)
    {
        this.file = file.toAbsolutePath();
    }

    @Override
    public long[] open(final int shardCount) throws IOException
    {
        Files.createDirectories(file.getParent());
        final long[] cursor = new long[shardCount];
        if (Files.exists(file))
        {
            truncateTornLine();
            long lines = 0;
            try (BufferedReader r = new BufferedReader(new InputStreamReader(Files.newInputStream(file), StandardCharsets.US_ASCII), 1 << 16))
            {
                String line;
                while ((line = r.readLine()) != null)
                {
                    lines++;
                    final int shardEnd = line.indexOf(',', SHARD.length());
                    final int seqEnd = line.indexOf(',', shardEnd + SEQ.length());
                    if (!line.startsWith(SHARD) || shardEnd < 0 || seqEnd < 0 || !line.startsWith(SEQ, shardEnd))
                    {
                        throw new IOException(file + ": malformed record at line " + lines);
                    }
                    final int s = Integer.parseInt(line, SHARD.length(), shardEnd, 10);
                    final long seq = Long.parseLong(line, shardEnd + SEQ.length(), seqEnd, 10);
                    if (s < shardCount && seq > cursor[s])
                    {
                        cursor[s] = seq;
                    }
                }
            }
            LOG.info("surveillance file {}: {} records, resuming after {}", file, lines, java.util.Arrays.toString(cursor));
        }
        fos = new FileOutputStream(file.toFile(), true);
        out = new BufferedOutputStream(fos, 1 << 16);
        return cursor;
    }

    private void truncateTornLine() throws IOException
    {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            final long size = ch.size();
            final ByteBuffer buf = ByteBuffer.allocate(4096);
            long end = size;
            long keep = 0;
            search:
            while (end > 0)
            {
                final long start = Math.max(0, end - buf.capacity());
                buf.clear().limit((int)(end - start));
                while (buf.hasRemaining() && ch.read(buf, start + buf.position()) >= 0)
                {
                    // fill
                }
                for (int i = (int)(end - start) - 1; i >= 0; i--)
                {
                    if (buf.get(i) == '\n')
                    {
                        keep = start + i + 1;
                        break search;
                    }
                }
                end = start;
            }
            if (keep < size)
            {
                LOG.warn("surveillance file {}: truncating a torn last record ({} bytes)", file, size - keep);
                ch.truncate(keep);
                ch.force(true);
            }
        }
    }

    @Override
    public void write(final int shardId, final long eventSeq, final CharSequence json) throws IOException
    {
        for (int i = 0, n = json.length(); i < n; i++)
        {
            out.write(json.charAt(i));
        }
        out.write('\n');
    }

    @Override
    public void skip(final int shardId, final long eventSeq)
    {
    }

    @Override
    public void flush() throws IOException
    {
        out.flush();
        fos.getChannel().force(false);
    }

    @Override
    public void close(final boolean clean)
    {
        if (out == null)
        {
            return;
        }
        try
        {
            flush(); // complete lines only (a torn one is truncated at the next open), so always safe to keep
        }
        catch (final IOException e)
        {
            LOG.warn("surveillance file {}: final flush failed", file, e);
        }
        try
        {
            fos.close();
        }
        catch (final IOException e)
        {
            LOG.warn("surveillance file {}: close failed", file, e);
        }
        out = null;
        fos = null;
    }
}
