package io.lmb.replayer;

import io.lmb.api.ShardStateMachineFactory;
import io.lmb.cluster.RaftLogReader;

import java.io.File;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Verifies a member directory offline: reads its committed Raft log with {@link RaftLogReader}, applies every
 * command and timer to a fresh machine from the {@link ShardStateMachineFactory}, and compares every event (and
 * so every {@code CommandApplied.chainHash}) with the event stream the member recorded in its own archive
 * ({@link EventFiles}). Reports the first divergence. Works on a stopped member, a running one (its committed
 * part), or a ClusterBackup directory.
 *
 * <p>Start point: position 0 unless the log was purged ({@code startPosition() > 0}) or {@link Options#fromSnapshot}
 * asks for it; then the machine is restored from a service snapshot ({@link SnapshotLoader}) and the log is
 * replayed from the snapshot position.
 */
public final class OfflineVerifier
{
    private static final Pattern MEMBER_DIR = Pattern.compile("shard-(\\d+)-member-\\d+");

    /**
     * @param toPosition   bound for the log (e.g. a known commit position); -1 = the recorded end
     * @param fromSnapshot start from the latest service snapshot instead of position 0
     * @param shardId      the shard; -1 = from the directory name ({@code shard-S-member-M}) or the first event
     */
    public record Options(long toPosition, boolean fromSnapshot, int shardId)
    {
        public static Options defaults()
        {
            return new Options(-1, false, -1);
        }
    }

    /** Outcome; {@link #ok()} when no divergence was found. */
    public record Report(
        File memberDir, int shardId, long startPosition, long snapshotPosition, long endPosition, long entries,
        long eventsCompared, long eventsUnverified, long entriesUnverified, long verifiedSeq, long verifiedPosition,
        String lastChainHash, Divergence divergence)
    {
        public boolean ok()
        {
            return divergence == null;
        }

        @Override
        public String toString()
        {
            return (ok() ? "OK" : "DIVERGED") + " shard " + shardId + " dir " + memberDir +
                "\n  log [" + startPosition + ", " + endPosition + ")" + (snapshotPosition >= 0 ? " from snapshot at " + snapshotPosition : "") +
                "\n  entries " + entries + " (unverified " + entriesUnverified + "), events compared " + eventsCompared +
                " (unverified " + eventsUnverified + ")" +
                "\n  verified through eventSeq " + verifiedSeq + " logPosition " + verifiedPosition +
                "\n  last chain hash " + lastChainHash + (ok() ? "" : "\n  " + divergence);
        }
    }

    private OfflineVerifier()
    {
    }

    public static Report verify(final File memberDir, final ShardStateMachineFactory factory, final Options options)
    {
        final File archiveDir = new File(memberDir, "archive");
        try (RaftLogReader log = RaftLogReader.open(memberDir);
            EventFiles events = new EventFiles(memberDir, 0))
        {
            final ArchiveFiles archive = new ArchiveFiles(archiveDir);
            final int shardId = options.shardId() >= 0 ? options.shardId() : shardOf(memberDir);
            final long end = options.toPosition() >= 0 ? Math.min(options.toPosition(), log.recordedPosition()) : log.recordedPosition();

            final ReplayVerifier verifier = new ReplayVerifier(factory.create(shardId), events);
            long start = 0;
            long snapshotPosition = -1;
            long afterSeq = 0;
            if (options.fromSnapshot() || log.startPosition() > 0)
            {
                final RaftLogReader.Snapshot snapshot = options.fromSnapshot() ?
                    SnapshotLoader.latestServiceSnapshot(log.snapshots(), end) :
                    earliestServiceSnapshotFrom(log, log.startPosition());
                if (snapshot == null)
                {
                    throw new IllegalStateException("the log of " + memberDir + " starts at " + log.startPosition() +
                        " and there is no service snapshot to start from" + (options.fromSnapshot() ? " at or before " + end : ""));
                }
                final SnapshotLoader.Loaded loaded = SnapshotLoader.load(archive, snapshot, verifier.machine());
                start = snapshot.logPosition();
                snapshotPosition = start;
                afterSeq = loaded.lastEventSeq();
                verifier.startAt(start);
            }
            events.skipThrough(afterSeq);
            verifier.firstComparableSeq(Math.max(afterSeq + 1, events.firstAvailableSeq() == Long.MAX_VALUE ? 1 : events.firstAvailableSeq()));
            log.read(start, end, verifier);
            if (end == log.recordedPosition())
            {
                verifier.checkStreamExhausted();
            }
            return new Report(memberDir, shardId, start, snapshotPosition, verifier.appliedPosition(), verifier.entries(),
                verifier.eventsCompared(), verifier.eventsUnverified(), verifier.entriesUnverified(), verifier.verifiedSeq(),
                verifier.verifiedPosition(), HexFormat.of().formatHex(verifier.lastChainHash()), verifier.divergence());
        }
    }

    static RaftLogReader.Snapshot earliestServiceSnapshotFrom(final RaftLogReader log, final long from)
    {
        RaftLogReader.Snapshot best = null;
        for (final RaftLogReader.Snapshot s : log.snapshots())
        {
            if (s.serviceId() == 0 && s.logPosition() >= from && s.logPosition() <= log.recordedPosition() &&
                (best == null || s.logPosition() < best.logPosition()))
            {
                best = s;
            }
        }
        return best;
    }

    static int shardOf(final File memberDir)
    {
        final Matcher m = MEMBER_DIR.matcher(memberDir.getName());
        if (m.matches())
        {
            return Integer.parseInt(m.group(1));
        }
        final int[] shard = { 0 };
        try (EventFiles first = new EventFiles(memberDir, 0))
        {
            first.poll((b, o, l) -> shard[0] = io.lmb.common.Events.shardId(b, o), 1);
        }
        return shard[0];
    }
}
