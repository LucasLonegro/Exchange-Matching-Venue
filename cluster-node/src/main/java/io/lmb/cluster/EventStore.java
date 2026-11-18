package io.lmb.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.lmb.common.Events;
import io.lmb.sbe.EventHeaderDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Materialises the shard's event stream on this member: appends each event emitted by the state machine
 * to an exclusive IPC publication recorded by the member's Archive, skipping events already recorded
 * before a restart (the log replays them), and maintains the {@link EventIndex}.
 *
 * <p>Each start of the service opens a new recording ("segment") whose first eventSeq is one past what
 * is already recorded, or one past the snapshot the machine was loaded from if that is further (a member
 * whose recordings were lost does not have those events and says so). On disk, under {@code events/}:
 * {@code segments.dat} lists (recordingId, firstSeq) and {@code seg-<recordingId>.idx} holds sparse
 * (eventSeq, position) pairs. Both are validated against the Archive catalog on start, and the last
 * eventSeq of each segment is found by scanning its tail.
 *
 * <p>All methods except construction run on the clustered service thread.
 */
public final class EventStore implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(EventStore.class);
    static final int EVENTS_STREAM_ID = 7010;
    private static final int SCAN_STREAM_ID = 7011;
    private static final int MIN_EVENT_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH + EventHeaderDecoder.ENCODED_LENGTH;
    private static final long WAIT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    private final Aeron aeron;
    private final AeronArchive.Context archiveContext;
    private final Path dir;
    private final String channel;
    private final int indexInterval;
    private final EventIndex index;
    private final IdleStrategy idle;
    private final ByteBuffer entryBuffer = ByteBuffer.allocateDirect(16).order(ByteOrder.LITTLE_ENDIAN);

    private AeronArchive archive;
    private ExclusivePublication publication;
    private EventIndex.Segment segment;
    private FileChannel indexFile;
    private long skipUpTo;
    private long nextSeq;
    private long appended;
    private long skipped;

    public EventStore(
        final Aeron aeron, final AeronArchive.Context archiveContext, final Path dir, final int termLength,
        final int indexInterval, final EventIndex index, final IdleStrategy idle)
    {
        this.aeron = aeron;
        this.archiveContext = archiveContext;
        this.dir = dir;
        this.channel = "aeron:ipc?alias=lmb-events|term-length=" + termLength;
        this.indexInterval = Math.max(1, indexInterval);
        this.index = index;
        this.idle = idle;
    }

    /**
     * Opens the stream after the machine has been restored.
     *
     * @param machineLastSeq lastEventSeq of the machine after loading its snapshot (0 if none)
     */
    public void open(final long machineLastSeq)
    {
        try
        {
            Files.createDirectories(dir);
            archive = AeronArchive.connect(archiveContext.clone());
            final List<EventIndex.Segment> valid = loadSegments();
            long recordedLast = 0;
            for (final EventIndex.Segment s : valid)
            {
                recordedLast = Math.max(recordedLast, s.lastSeq());
            }
            if (machineLastSeq > recordedLast)
            {
                LOG.warn("events {}..{} are not recorded on this member (restored from a snapshot at eventSeq {}); " +
                    "consumers needing them will be sent to other members", recordedLast + 1, machineLastSeq, machineLastSeq);
            }
            skipUpTo = recordedLast;
            final long firstSeq = Math.max(recordedLast, machineLastSeq) + 1;
            for (final EventIndex.Segment s : valid)
            {
                index.addSegment(s);
            }
            startSegment(firstSeq, valid);
            index.ready(true);
            LOG.info("event store open: recorded up to eventSeq {}, machine at {}, new segment {} from eventSeq {}",
                recordedLast, machineLastSeq, segment.recordingId(), firstSeq);
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public long skippedCount()
    {
        return skipped;
    }

    public long appendedCount()
    {
        return appended;
    }

    /** Appends one event (skipping it if already recorded). Allocation-free unless a new segment is needed. */
    public void append(final DirectBuffer buffer, final int offset, final int length)
    {
        final long seq = Events.eventSeq(buffer, offset);
        if (seq <= skipUpTo)
        {
            skipped++;
            return;
        }
        if (seq != nextSeq)
        {
            if (seq < nextSeq)
            {
                LOG.error("event {} already appended (next expected {}); dropping", seq, nextSeq);
                return;
            }
            LOG.error("event stream gap: expected eventSeq {} got {}; starting a new segment", nextSeq, seq);
            rollSegment(seq);
        }
        final long startPosition = publication.position();
        long result;
        boolean warned = false;
        idle.reset();
        while ((result = publication.offer(buffer, offset, length)) < 0)
        {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
            {
                LOG.error("event publication unusable ({}); starting a new segment at eventSeq {}", result, seq);
                rollSegment(seq);
                continue;
            }
            if (!warned && result == Publication.NOT_CONNECTED)
            {
                LOG.warn("event recording not connected; waiting");
                warned = true;
            }
            idle.idle();
        }
        nextSeq = seq + 1;
        appended++;
        if (seq != segment.firstSeq() && (seq - segment.firstSeq()) % indexInterval == 0)
        {
            // startPosition may be a term padding frame, which is still a valid frame boundary to replay from
            segment.add(seq, startPosition);
            writeEntry(seq, startPosition);
        }
        segment.lastSeq(seq);
    }

    private void writeEntry(final long seq, final long position)
    {
        try
        {
            entryBuffer.clear();
            entryBuffer.putLong(0, seq).putLong(8, position);
            entryBuffer.limit(16);
            while (entryBuffer.hasRemaining())
            {
                indexFile.write(entryBuffer);
            }
        }
        catch (final IOException e)
        {
            LOG.error("cannot write event index entry (index stays valid in memory)", e);
        }
    }

    private void rollSegment(final long firstSeq)
    {
        closeCurrentSegment();
        final List<EventIndex.Segment> all = new ArrayList<>(List.of(index.segments()));
        try
        {
            startSegment(firstSeq, all);
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private void startSegment(final long firstSeq, final List<EventIndex.Segment> existing) throws IOException
    {
        publication = archive.addRecordedExclusivePublication(channel, EVENTS_STREAM_ID);
        final CountersReader counters = aeron.countersReader();
        final long deadline = System.nanoTime() + WAIT_TIMEOUT_NS;
        int counterId;
        idle.reset();
        while ((counterId = RecordingPos.findCounterIdBySession(
            counters, publication.sessionId(), archive.archiveId())) == CountersReader.NULL_COUNTER_ID)
        {
            if (System.nanoTime() > deadline)
            {
                throw new IllegalStateException("event recording did not start");
            }
            idle.idle();
        }
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        segment = new EventIndex.Segment(recordingId, firstSeq, publication.position(), true);
        nextSeq = firstSeq;
        indexFile = FileChannel.open(dir.resolve("seg-" + recordingId + ".idx"),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

        final List<long[]> records = new ArrayList<>();
        for (final EventIndex.Segment s : existing)
        {
            records.add(new long[]{ s.recordingId(), s.firstSeq() });
        }
        records.add(new long[]{ recordingId, firstSeq });
        writeSegmentsFile(records);
        index.addSegment(segment);
    }

    private void writeSegmentsFile(final List<long[]> records) throws IOException
    {
        final ByteBuffer b = ByteBuffer.allocate(16 * records.size()).order(ByteOrder.LITTLE_ENDIAN);
        for (final long[] r : records)
        {
            b.putLong(r[0]).putLong(r[1]);
        }
        b.flip();
        final Path tmp = dir.resolve("segments.dat.tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING))
        {
            while (b.hasRemaining())
            {
                ch.write(b);
            }
            ch.force(true);
        }
        Files.move(tmp, dir.resolve("segments.dat"), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private List<EventIndex.Segment> loadSegments() throws IOException
    {
        final List<EventIndex.Segment> result = new ArrayList<>();
        final Path file = dir.resolve("segments.dat");
        if (!Files.exists(file))
        {
            return result;
        }
        final ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(file)).order(ByteOrder.LITTLE_ENDIAN);
        long previousLast = 0;
        while (b.remaining() >= 16)
        {
            final long recordingId = b.getLong();
            final long firstSeq = b.getLong();
            final long[] extent = new long[]{ AeronArchive.NULL_POSITION, AeronArchive.NULL_POSITION };
            final int found = archive.listRecording(recordingId,
                (controlSessionId, correlationId, rid, startTimestamp, stopTimestamp, startPosition, stopPosition,
                initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
                originalChannel, sourceIdentity) ->
                {
                    extent[0] = startPosition;
                    extent[1] = stopPosition;
                });
            if (found == 0 || extent[1] == AeronArchive.NULL_POSITION)
            {
                LOG.warn("event segment recording {} (from eventSeq {}) missing or still active; ignored", recordingId, firstSeq);
                continue;
            }
            final long start = extent[0];
            final long stop = extent[1];
            if (stop <= start)
            {
                continue;
            }
            if (firstSeq <= previousLast)
            {
                LOG.warn("event segment {} overlaps previous segment; ignored", recordingId);
                continue;
            }
            final EventIndex.Segment s = new EventIndex.Segment(recordingId, firstSeq, start, false);
            final Path idx = dir.resolve("seg-" + recordingId + ".idx");
            if (Files.exists(idx))
            {
                final ByteBuffer ib = ByteBuffer.wrap(Files.readAllBytes(idx)).order(ByteOrder.LITTLE_ENDIAN);
                long lastIndexed = firstSeq;
                while (ib.remaining() >= 16)
                {
                    final long seq = ib.getLong();
                    final long pos = ib.getLong();
                    if (seq > lastIndexed && pos > start && pos < stop && (pos & 31) == 0)
                    {
                        s.add(seq, pos);
                        lastIndexed = seq;
                    }
                }
            }
            final long last = scanTail(s, stop);
            if (last < firstSeq)
            {
                continue;
            }
            s.lastSeq(last);
            previousLast = last;
            result.add(s);
            LOG.info("event segment recording {}: eventSeq {}..{}", recordingId, firstSeq, last);
        }
        return result;
    }

    /** Reads the recording from the last trustworthy index entry to its end; returns the last contiguous eventSeq. */
    private long scanTail(final EventIndex.Segment s, final long stop)
    {
        int entry = s.count() - 1;
        while (true)
        {
            final long fromSeq = s.seqAt(entry);
            final long fromPos = s.positionAt(entry);
            final long last = scan(s.recordingId(), fromPos, stop, fromSeq);
            if (last >= fromSeq || entry == 0)
            {
                return last;
            }
            LOG.warn("event index entry {} of recording {} is inconsistent; falling back", fromSeq, s.recordingId());
            entry--;
        }
    }

    /** Returns the last eventSeq of a contiguous run starting with expectedFirst, or expectedFirst-1. */
    private long scan(final long recordingId, final long fromPos, final long stop, final long expectedFirst)
    {
        final long[] state = new long[]{ expectedFirst - 1, 0 };
        final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) ->
        {
            if (state[1] != 0 || length < MIN_EVENT_LENGTH)
            {
                return;
            }
            final long seq = Events.eventSeq(buffer, offset);
            if (seq == state[0] + 1)
            {
                state[0] = seq;
            }
            else if (seq > state[0] + 1)
            {
                state[1] = 1;
            }
        });
        try (Subscription sub = archive.replay(recordingId, fromPos, stop - fromPos, "aeron:ipc", SCAN_STREAM_ID))
        {
            final long deadline = System.nanoTime() + WAIT_TIMEOUT_NS;
            Image image = null;
            while (true)
            {
                if (image == null)
                {
                    image = sub.imageCount() > 0 ? sub.imageAtIndex(0) : null;
                }
                final int n = sub.poll(assembler, 256);
                if (image != null && (image.position() >= stop || image.isEndOfStream() || image.isClosed()) && n == 0)
                {
                    break;
                }
                if (System.nanoTime() > deadline)
                {
                    LOG.warn("scan of recording {} timed out", recordingId);
                    break;
                }
                if (n == 0)
                {
                    idle.idle();
                }
            }
        }
        return state[0];
    }

    private void closeCurrentSegment()
    {
        if (publication != null)
        {
            final long target = publication.position();
            final CountersReader counters = aeron.countersReader();
            final int counterId = RecordingPos.findCounterIdByRecording(counters, segment.recordingId(), archive.archiveId());
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (counterId != CountersReader.NULL_COUNTER_ID &&
                counters.getCounterValue(counterId) < target && System.nanoTime() < deadline)
            {
                Thread.yield();
            }
            try
            {
                archive.stopRecording(publication);
            }
            catch (final RuntimeException e)
            {
                LOG.debug("stopRecording: {}", e.toString());
            }
            CloseHelper.quietClose(publication);
            publication = null;
            segment.stop();
        }
        CloseHelper.quietClose(indexFile);
        indexFile = null;
    }

    @Override
    public void close()
    {
        index.ready(false);
        if (archive != null)
        {
            try
            {
                closeCurrentSegment();
            }
            catch (final RuntimeException e)
            {
                LOG.debug("closing event segment: {}", e.toString());
            }
            CloseHelper.quietClose(archive);
            archive = null;
        }
    }
}
