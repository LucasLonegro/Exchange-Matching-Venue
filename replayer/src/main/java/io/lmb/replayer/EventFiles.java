package io.lmb.replayer;

import io.lmb.api.EventStreamClient;
import io.lmb.common.Events;
import org.agrona.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * The event stream materialised by a cluster member, read offline from its directory:
 * {@code <memberDir>/events/segments.dat} lists the event recordings ({@code (recordingId, firstSeq)}, 16-byte
 * little-endian records) and {@code <memberDir>/archive} holds them. Delivers events in eventSeq order, each
 * exactly once, through the same {@link EventSource} interface as a live {@link EventStreamClient}.
 */
public final class EventFiles implements EventSource
{
    /** One event recording segment. */
    public record Segment(long recordingId, long firstSeq)
    {
    }

    private final ArchiveFiles archive;
    private final List<Segment> segments;
    private int segmentIndex = -1;
    private ArchiveFiles.Cursor cursor;
    private long lastSeq;
    private EventStreamClient.EventHandler current;
    private final ArchiveFiles.MessageHandler forward = this::onMessage;
    private boolean delivered;

    /** Events with eventSeq > afterSeq from the member directory. */
    public EventFiles(final File memberDir, final long afterSeq)
    {
        this(new ArchiveFiles(new File(memberDir, "archive")), readSegments(new File(memberDir, "events")), afterSeq);
    }

    public EventFiles(final ArchiveFiles archive, final List<Segment> segments, final long afterSeq)
    {
        this.archive = archive;
        this.segments = segments;
        this.lastSeq = afterSeq;
    }

    /** Before the first poll: deliver only events with eventSeq > afterSeq. */
    public void skipThrough(final long afterSeq)
    {
        lastSeq = Math.max(lastSeq, afterSeq);
    }

    public List<Segment> segments()
    {
        return segments;
    }

    /** First eventSeq present in the files (Long.MAX_VALUE if none). */
    public long firstAvailableSeq()
    {
        return segments.isEmpty() ? Long.MAX_VALUE : segments.get(0).firstSeq();
    }

    public static List<Segment> readSegments(final File eventsDir)
    {
        final File file = new File(eventsDir, "segments.dat");
        final List<Segment> result = new ArrayList<>();
        if (!file.isFile())
        {
            return result;
        }
        try
        {
            final ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(file.toPath())).order(ByteOrder.LITTLE_ENDIAN);
            while (b.remaining() >= 16)
            {
                result.add(new Segment(b.getLong(), b.getLong()));
            }
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
        return result;
    }

    @Override
    public int poll(final EventStreamClient.EventHandler handler, final int limit)
    {
        int count = 0;
        current = handler;
        while (count < limit)
        {
            if (cursor == null && !advanceSegment())
            {
                break;
            }
            delivered = false;
            if (!cursor.next(forward))
            {
                if (segmentIndex + 1 < segments.size())
                {
                    cursor.close();
                    cursor = null; // a later segment exists: this one is closed
                    continue;
                }
                break;
            }
            if (delivered)
            {
                count++;
            }
        }
        current = null;
        return count;
    }

    private boolean advanceSegment()
    {
        while (segmentIndex + 1 < segments.size())
        {
            segmentIndex++;
            final Segment s = segments.get(segmentIndex);
            final boolean lastOne = segmentIndex + 1 == segments.size();
            if (!lastOne && segments.get(segmentIndex + 1).firstSeq() <= lastSeq + 1)
            {
                continue; // everything wanted from this segment is in a later one
            }
            if (archive.recording(s.recordingId()) == null)
            {
                throw new IllegalStateException("event recording " + s.recordingId() + " (from eventSeq " + s.firstSeq() +
                    ") is missing from " + archive.dir());
            }
            cursor = archive.cursor(s.recordingId(), 0);
            return true;
        }
        return false;
    }

    private void onMessage(final DirectBuffer buffer, final int offset, final int length, final long position)
    {
        if (!Events.isEvent(buffer, offset))
        {
            return;
        }
        final long seq = Events.eventSeq(buffer, offset);
        if (seq <= lastSeq)
        {
            return;
        }
        lastSeq = seq;
        delivered = true;
        current.onEvent(buffer, offset, length);
    }

    @Override
    public long lastEventSeq()
    {
        return lastSeq;
    }

    @Override
    public void close()
    {
        if (cursor != null)
        {
            cursor.close();
            cursor = null;
        }
    }
}
