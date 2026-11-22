package io.lmb.cluster;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.CatalogHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingState;
import io.aeron.cluster.RecordingLog;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the committed Raft log of a shard back from a member's directory (or a ClusterBackup's), without
 * a media driver or Archive process: {@code <dir>/cluster/recording.log} says which Archive recording holds
 * each leadership term, {@code <dir>/archive/archive.catalog} gives the recordings' geometry and the
 * {@code <recordingId>-<base>.rec} segment files hold the frames. Entries are delivered in log order with
 * the same {@code logPosition} and timestamp the live state machine received, so replaying the commands
 * and timers into a fresh machine reproduces the event stream (docs/modules/cluster.md section 6).
 *
 * <p>The metadata is read once by {@link #open}; {@link #read} maps segment files read-only and is
 * allocation-free per entry. The files of a running member may be read up to its commit position. Not
 * thread safe.
 */
public final class RaftLogReader implements AutoCloseable
{
    /** Receives log entries. Buffers are only valid during the call. */
    public interface Handler
    {
        /** A client command (raw LMB SBE bytes) applied at {@code logPosition} with cluster time {@code timestampNs}. */
        void onCommand(long logPosition, long timestampNs, long clusterSessionId, DirectBuffer buffer, int offset, int length);

        /** A cluster timer that fired at {@code logPosition}. */
        default void onTimer(final long logPosition, final long timestampNs, final long correlationId)
        {
        }

        /** Any other cluster message (session open/close, snapshot action, new leadership term...). */
        default void onClusterMessage(
            final int templateId, final long logPosition, final DirectBuffer buffer, final int offset, final int length)
        {
        }
    }

    /** A leadership term: its entries are in recording {@code recordingId} from {@code termBaseLogPosition}. */
    public record Term(long recordingId, long leadershipTermId, long termBaseLogPosition, long logPosition, long timestamp)
    {
        /** True when the term has ended ({@link #logPosition} is its agreed end). */
        public boolean ended()
        {
            return logPosition != AeronArchive.NULL_POSITION;
        }
    }

    /** A snapshot; serviceId -1 is the consensus module's, 0 the LMB service's (machine state). */
    public record Snapshot(long recordingId, long leadershipTermId, long logPosition, long timestamp, int serviceId)
    {
    }

    private record Recording(long recordingId, long startPosition, long stopPosition, int termLength, int segmentLength)
    {
    }

    private record Span(Recording recording, long start, long end)
    {
    }

    private final File archiveDir;
    private final List<Term> terms;
    private final List<Snapshot> snapshots;
    private final List<Span> spans = new ArrayList<>();
    private final ExpandableArrayBuffer assembly = new ExpandableArrayBuffer(4096);
    private final UnsafeBuffer segmentBuffer = new UnsafeBuffer(0, 0);
    private MappedByteBuffer mapped;
    private String mappedName;

    private RaftLogReader(final File archiveDir, final List<Term> terms, final List<Snapshot> snapshots)
    {
        this.archiveDir = archiveDir;
        this.terms = terms;
        this.snapshots = snapshots;
    }

    /** Opens the log of a member directory, i.e. {@code <lmb.data.dir>/cluster-node/shard-S-member-M}. */
    public static RaftLogReader open(final File memberDir)
    {
        return open(new File(memberDir, "cluster"), new File(memberDir, "archive"));
    }

    public static RaftLogReader open(final File clusterDir, final File archiveDir)
    {
        if (!new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).isFile())
        {
            throw new IllegalArgumentException("no " + RecordingLog.RECORDING_LOG_FILE_NAME + " in " + clusterDir);
        }
        final List<Term> terms = new ArrayList<>();
        final List<Snapshot> snapshots = new ArrayList<>();
        try (RecordingLog log = new RecordingLog(clusterDir, false))
        {
            for (final RecordingLog.Entry e : log.entries())
            {
                if (!e.isValid)
                {
                    continue;
                }
                if (e.type == RecordingLog.ENTRY_TYPE_TERM)
                {
                    terms.add(new Term(e.recordingId, e.leadershipTermId, e.termBaseLogPosition, e.logPosition, e.timestamp));
                }
                else if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT)
                {
                    snapshots.add(new Snapshot(e.recordingId, e.leadershipTermId, e.logPosition, e.timestamp, e.serviceId));
                }
            }
        }
        terms.sort(Comparator.comparingLong(Term::leadershipTermId));
        final RaftLogReader reader = new RaftLogReader(archiveDir, List.copyOf(terms), List.copyOf(snapshots));
        reader.buildSpans(readCatalog(archiveDir));
        return reader;
    }

    private void buildSpans(final Map<Long, Recording> catalog)
    {
        long cursor = 0;
        for (int i = 0; i < terms.size(); i++)
        {
            final Term t = terms.get(i);
            final Recording r = catalog.get(t.recordingId());
            if (r == null)
            {
                throw new IllegalStateException("log recording " + t.recordingId() + " of term " + t.leadershipTermId() +
                    " is not in the archive catalog of " + archiveDir);
            }
            final long recordedEnd = r.stopPosition() != AeronArchive.NULL_POSITION ? r.stopPosition() : scanEnd(r);
            long end = t.ended() ? t.logPosition() : (i + 1 < terms.size() ? terms.get(i + 1).termBaseLogPosition() : recordedEnd);
            end = Math.min(end, recordedEnd);
            final long start = Math.max(Math.max(t.termBaseLogPosition(), r.startPosition()), cursor);
            if (end > start || (spans.isEmpty() && end == start))
            {
                spans.add(new Span(r, start, end));
                cursor = end;
            }
        }
    }

    public List<Term> terms()
    {
        return terms;
    }

    public List<Snapshot> snapshots()
    {
        return snapshots;
    }

    /** First position whose entries are present (0 unless the log was purged after a snapshot). */
    public long startPosition()
    {
        return spans.isEmpty() ? 0 : spans.get(0).start();
    }

    /** Position after the last recorded entry (may include entries not yet committed, see class docs). */
    public long recordedPosition()
    {
        return spans.isEmpty() ? 0 : spans.get(spans.size() - 1).end();
    }

    /** Highest position known to be committed from the files alone: ended terms and snapshots. */
    public long knownCommittedPosition()
    {
        long p = 0;
        for (final Term t : terms)
        {
            if (t.ended())
            {
                p = Math.max(p, t.logPosition());
            }
        }
        for (final Snapshot s : snapshots)
        {
            p = Math.max(p, s.logPosition());
        }
        return Math.min(p, recordedPosition());
    }

    /** Reads every recorded entry from {@link #startPosition()}; returns the position reached. */
    public long read(final Handler handler)
    {
        return read(startPosition(), recordedPosition(), handler);
    }

    /**
     * Delivers the complete entries in [fromPosition, toPosition) in log order.
     *
     * @param fromPosition an entry boundary: 0, a snapshot position or a value returned by this method
     * @param toPosition   bound, e.g. a commit position; entries ending after it are not delivered
     * @return the position after the last entry delivered (fromPosition if none), to resume from
     */
    public long read(final long fromPosition, final long toPosition, final Handler handler)
    {
        if (fromPosition < startPosition())
        {
            throw new IllegalArgumentException("position " + fromPosition + " is before the first recorded entry " +
                startPosition() + " (restore the machine from a snapshot first)");
        }
        final RaftLogDecoder decoder = new RaftLogDecoder(handler);
        long reached = fromPosition;
        for (final Span span : spans)
        {
            if (span.end() <= reached)
            {
                continue;
            }
            final long from = Math.max(span.start(), reached);
            final long to = Math.min(span.end(), toPosition);
            if (from >= to)
            {
                break;
            }
            reached = readSpan(span.recording(), from, to, decoder);
            if (reached < span.end())
            {
                break;
            }
        }
        return reached;
    }

    /** Returns the frame boundary to resume from: after the last complete entry and any padding after it. */
    private long readSpan(final Recording r, final long from, final long to, final RaftLogDecoder decoder)
    {
        long position = from;
        long resume = from;
        int assembled = -1;
        while (position < to)
        {
            if (!mapSegment(r, position))
            {
                break;
            }
            final int offset = (int)(position - segmentBase(r, position));
            if (offset + DataHeaderFlyweight.HEADER_LENGTH > segmentBuffer.capacity())
            {
                break;
            }
            final int frameLength = segmentBuffer.getInt(offset + HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
            if (frameLength <= 0)
            {
                break;
            }
            final int termOffset = segmentBuffer.getInt(offset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
            if (termOffset != (int)(position & (r.termLength() - 1)))
            {
                throw new IllegalStateException("position " + position + " of recording " + r.recordingId() +
                    " is not a frame boundary");
            }
            final int aligned = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            final long next = position + aligned;
            if (next > to || offset + aligned > segmentBuffer.capacity())
            {
                break;
            }
            final int type = segmentBuffer.getShort(offset + HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
            if (type == HeaderFlyweight.HDR_TYPE_DATA)
            {
                final byte flags = segmentBuffer.getByte(offset + HeaderFlyweight.FLAGS_FIELD_OFFSET);
                final int payload = offset + DataHeaderFlyweight.HEADER_LENGTH;
                final int payloadLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;
                if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED)
                {
                    decoder.decode(segmentBuffer, payload, payloadLength, next);
                    assembled = -1;
                }
                else if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) != 0)
                {
                    assembly.putBytes(0, segmentBuffer, payload, payloadLength);
                    assembled = payloadLength;
                }
                else if (assembled >= 0)
                {
                    assembly.putBytes(assembled, segmentBuffer, payload, payloadLength);
                    assembled += payloadLength;
                    if ((flags & FrameDescriptor.END_FRAG_FLAG) != 0)
                    {
                        decoder.decode(assembly, 0, assembled, next);
                        assembled = -1;
                    }
                }
            }
            position = next;
            if (assembled < 0)
            {
                resume = position;
            }
        }
        return resume;
    }

    private static long segmentBase(final Recording r, final long position)
    {
        return AeronArchive.segmentFileBasePosition(r.startPosition(), position, r.termLength(), r.segmentLength());
    }

    private boolean mapSegment(final Recording r, final long position)
    {
        final String name = r.recordingId() + "-" + segmentBase(r, position) + ".rec";
        if (name.equals(mappedName))
        {
            return true;
        }
        unmap();
        final File file = new File(archiveDir, name);
        if (!file.isFile())
        {
            return false;
        }
        try (FileChannel ch = FileChannel.open(file.toPath(), StandardOpenOption.READ))
        {
            mapped = ch.map(FileChannel.MapMode.READ_ONLY, 0, Math.min(ch.size(), r.segmentLength()));
            segmentBuffer.wrap(mapped);
            mappedName = name;
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
            mappedName = null;
            segmentBuffer.wrap(0, 0);
        }
    }

    /** End of the data of an active recording: walks the frames of its last segment file. */
    private long scanEnd(final Recording r)
    {
        final String prefix = r.recordingId() + "-";
        long lastBase = -1;
        final String[] names = archiveDir.list();
        for (final String n : names == null ? new String[0] : names)
        {
            if (n.startsWith(prefix) && n.endsWith(".rec"))
            {
                lastBase = Math.max(lastBase, Long.parseLong(n.substring(prefix.length(), n.length() - 4)));
            }
        }
        if (lastBase < 0)
        {
            return r.startPosition();
        }
        long position = Math.max(lastBase, r.startPosition());
        while (mapSegment(r, position))
        {
            final int offset = (int)(position - segmentBase(r, position));
            if (offset + DataHeaderFlyweight.HEADER_LENGTH > segmentBuffer.capacity())
            {
                break;
            }
            final int frameLength = segmentBuffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
            final int aligned = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            if (frameLength <= 0 || offset + aligned > segmentBuffer.capacity())
            {
                break;
            }
            position += aligned;
        }
        return position;
    }

    private static Map<Long, Recording> readCatalog(final File archiveDir)
    {
        final File file = new File(archiveDir, "archive.catalog");
        final Map<Long, Recording> result = new HashMap<>();
        if (!file.isFile())
        {
            return Collections.unmodifiableMap(result);
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
                    d.termBufferLength(), d.segmentFileLength()));
            }
            offset += BitUtil.align(length + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH, alignment);
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public void close()
    {
        unmap();
    }
}
