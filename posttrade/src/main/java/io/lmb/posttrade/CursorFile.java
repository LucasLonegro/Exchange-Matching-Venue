package io.lmb.posttrade;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * A small durable key/value checkpoint (same pattern as the market data publisher's {@code state.bin}):
 * {@code key=value} lines followed by {@code crc=<hex>} (CRC32 of everything before it), written to a
 * temporary file, fsynced and atomically renamed over the previous one. A missing file reads as empty;
 * a torn or corrupt file is an error (the operator decides, nothing is silently reset).
 */
public final class CursorFile
{
    private static final String CRC_PREFIX = "crc=";

    private CursorFile()
    {
    }

    public static Map<String, String> read(final Path file) throws IOException
    {
        final Map<String, String> out = new LinkedHashMap<>();
        if (!Files.exists(file))
        {
            return out;
        }
        final String text = Files.readString(file, StandardCharsets.UTF_8);
        final int crcAt = text.lastIndexOf(CRC_PREFIX);
        if (crcAt < 0 || (crcAt > 0 && text.charAt(crcAt - 1) != '\n'))
        {
            throw new IOException("checkpoint " + file + " has no CRC line (torn write?)");
        }
        final String body = text.substring(0, crcAt);
        final CRC32 crc = new CRC32();
        crc.update(body.getBytes(StandardCharsets.UTF_8));
        if (!Long.toHexString(crc.getValue()).equals(text.substring(crcAt + CRC_PREFIX.length()).trim()))
        {
            throw new IOException("checkpoint " + file + " fails its CRC");
        }
        for (final String line : body.split("\n"))
        {
            final int eq = line.indexOf('=');
            if (eq > 0)
            {
                out.put(line.substring(0, eq), line.substring(eq + 1));
            }
        }
        return out;
    }

    public static void write(final Path file, final Map<String, String> values) throws IOException
    {
        final StringBuilder sb = new StringBuilder();
        values.forEach((k, v) ->
        {
            if (k.indexOf('=') >= 0 || k.indexOf('\n') >= 0 || v.indexOf('\n') >= 0)
            {
                throw new IllegalArgumentException("bad checkpoint entry " + k);
            }
            sb.append(k).append('=').append(v).append('\n');
        });
        final byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
        final CRC32 crc = new CRC32();
        crc.update(body);
        final byte[] tail = (CRC_PREFIX + Long.toHexString(crc.getValue()) + "\n").getBytes(StandardCharsets.UTF_8);
        final Path dir = file.toAbsolutePath().getParent();
        Files.createDirectories(dir);
        final Path tmp = dir.resolve(file.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING))
        {
            ch.write(ByteBuffer.wrap(body));
            ch.write(ByteBuffer.wrap(tail));
            ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
