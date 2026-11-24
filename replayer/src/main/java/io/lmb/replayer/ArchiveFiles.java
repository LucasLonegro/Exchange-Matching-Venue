package io.lmb.replayer;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.CatalogHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingState;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Offline, read-only access to any recording of an Aeron Archive directory ({@code archive.catalog} plus
 * {@code <recordingId>-<segmentBase>.rec} segment files), without a media driver. Used for the event stream
 * recordings and the service snapshot recordings of a cluster member; the Raft log itself is read with
 * {@code io.lmb.cluster.RaftLogReader}.
 */
public final class ArchiveFiles
{
    /** Geometry of one recording from the catalog; {@code stopPosition} is {@link AeronArchive#NULL_POSITION} while active. */
    public record Recording(long recordingId, long startPosition, long stopPosition, int termLength, int segmentLength,
        int streamId)
    {
    }

    /** Receives one complete (reassembled) message; {@code position} is the recording position after it. */
    @FunctionalInterface
    public interface MessageHandler
    {
        void onMessage(DirectBuffer buffer, int offset, int length, long position);
    }

    private final File archiveDir;
    private Map<Long, Recording> catalog;

    public ArchiveFiles(final File archiveDir)
    {
        this.archiveDir = archiveDir;
        this.catalog = readCatalog(archiveDir);
    }

    public File dir()
    {
        return archiveDir;
    }

    /** The recording, re-reading the catalog once if it is not known yet; null if absent. */
    public Recording recording(final long recordingId)
    {
        Recording r = catalog.get(recordingId);
        if (r == null)
        {
            catalog = readCatalog(archiveDir);
            r = catalog.get(recordingId);
        }
        return r;
    }

    /** A cursor over the messages of a recording from {@code fromPosition} (a message boundary). */
    public Cursor cursor(final long recordingId, final long fromPosition)
    {
        final Recording r = recording(recordingId);
        if (r == null)
        {
            throw new IllegalStateException("recording " + recordingId + " is not in the catalog of " + archiveDir);
        }
        return new Cursor(r, Math.max(fromPosition, r.startPosition()));
    }

    /**
     * Pull-based reader of one recording. {@link #next} delivers at most one message per call, so two
     * recordings can be consumed in lock step. Reads up to the stop position or, for an active recording, up to
     * the last complete frame found in the files (later calls see data appended since). Not thread safe.
     */
    public final class Cursor implements AutoCloseable
    {
        private final Recording recording;
        private final ExpandableArrayBuffer assembly = new ExpandableArrayBuffer(4096);
        private final UnsafeBuffer segment = new UnsafeBuffer(0, 0);
        private MappedByteBuffer mapped;
        private long mappedBase = -1;
        private long position;

        Cursor(final Recording recording, final long position)
        {
            this.recording = recording;
            this.position = position;
        }

        public long position()
        {
            return position;
        }

        public Recording recording()
        {
            return recording;
        }

        /** Delivers the next complete message; false if none is available (end of recording or of recorded data). */
        public boolean next(final MessageHandler handler)
        {
            long p = position;
            int assembled = -1;
            while (recording.stopPosition() == AeronArchive.NULL_POSITION || p < recording.stopPosition())
            {
                if (!map(p))
                {
                    break;
                }
                final int offset = (int)(p - mappedBase);
                if (offset + DataHeaderFlyweight.HEADER_LENGTH > segment.capacity())
                {
                    break;
                }
                final int frameLength = segment.getIntVolatile(offset);
                if (frameLength <= 0)
                {
                    break;
                }
                final int aligned = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
                if (offset + aligned > segment.capacity())
                {
                    break;
                }
                final int termOffset = segment.getInt(offset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
                if (termOffset != (int)(p & (recording.termLength() - 1)))
                {
                    throw new IllegalStateException("position " + p + " of recording " + recording.recordingId() +
                        " in " + archiveDir + " is not a frame boundary");
                }
                final long next = p + aligned;
                final int type = segment.getShort(offset + HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
                p = next;
                if (type != HeaderFlyweight.HDR_TYPE_DATA)
                {
                    if (assembled < 0)
                    {
                        position = p;
                    }
                    continue;
                }
                final byte flags = segment.getByte(offset + HeaderFlyweight.FLAGS_FIELD_OFFSET);
                final int payload = offset + DataHeaderFlyweight.HEADER_LENGTH;
                final int payloadLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;
                if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED)
                {
                    position = p;
                    handler.onMessage(segment, payload, payloadLength, p);
                    return true;
                }
                if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) != 0)
                {
                    assembly.putBytes(0, segment, payload, payloadLength);
                    assembled = payloadLength;
                }
                else if (assembled >= 0)
                {
                    assembly.putBytes(assembled, segment, payload, payloadLength);
                    assembled += payloadLength;
                    if ((flags & FrameDescriptor.END_FRAG_FLAG) != 0)
                    {
                        position = p;
                        handler.onMessage(assembly, 0, assembled, p);
                        return true;
                    }
                }
                else
                {
                    position = p; // a continuation without its beginning: skip
                }
            }
            unmap(); // the next call maps the files again and sees data appended meanwhile
            return false;
        }

        private boolean map(final long p)
        {
            final long base = AeronArchive.segmentFileBasePosition(
                recording.startPosition(), p, recording.termLength(), recording.segmentLength());
            if (base == mappedBase)
            {
                return true;
            }
            unmap();
            final File file = new File(archiveDir, recording.recordingId() + "-" + base + ".rec");
            if (!file.isFile())
            {
                return false;
            }
            try (FileChannel ch = FileChannel.open(file.toPath(), StandardOpenOption.READ))
            {
                mapped = ch.map(FileChannel.MapMode.READ_ONLY, 0, Math.min(ch.size(), recording.segmentLength()));
                segment.wrap(mapped);
                mappedBase = base;
                return true;
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        private void unmap()
        {
            if (mapped != null)
            {
                IoUtil.unmap(mapped);
                mapped = null;
                segment.wrap(0, 0);
            }
            mappedBase = -1;
        }

        @Override
        public void close()
        {
            unmap();
        }
    }

    static Map<Long, Recording> readCatalog(final File archiveDir)
    {
        final File file = new File(archiveDir, "archive.catalog");
        final Map<Long, Recording> result = new HashMap<>();
        if (!file.isFile())
        {
            return result;
        }
        final byte[] bytes;
        try
        {
            bytes = Files.readAllBytes(file.toPath());
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
        final UnsafeBuffer b = new UnsafeBuffer(bytes);
        final CatalogHeaderDecoder header = new CatalogHeaderDecoder().wrap(
            b, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);
        final int alignment = header.alignment() != 0 ? header.alignment() : 1024;
        int offset = header.alignment() != 0 ? CatalogHeaderDecoder.BLOCK_LENGTH : 1024;
        final RecordingDescriptorHeaderDecoder dh = new RecordingDescriptorHeaderDecoder();
        final RecordingDescriptorDecoder d = new RecordingDescriptorDecoder();
        while (offset + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH <= bytes.length)
        {
            dh.wrap(b, offset, RecordingDescriptorHeaderDecoder.BLOCK_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);
            final int length = dh.length();
            if (length <= 0)
            {
                break;
            }
            if (dh.state() == RecordingState.VALID)
            {
                d.wrap(b, offset + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH, RecordingDescriptorDecoder.BLOCK_LENGTH,
                    RecordingDescriptorDecoder.SCHEMA_VERSION);
                result.put(d.recordingId(), new Recording(d.recordingId(), d.startPosition(), d.stopPosition(),
                    d.termBufferLength(), d.segmentFileLength(), d.streamId()));
            }
            offset += BitUtil.align(length + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH, alignment);
        }
        return result;
    }
}
