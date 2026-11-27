package io.lmb.replayer;

import io.lmb.api.ShardStateMachine;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.api.TimerScheduler;
import io.lmb.cluster.RaftLogReader;
import io.lmb.common.Events;
import io.lmb.common.HashChain;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.TradeDecoder;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Runs the shard state machine offline over a recorded Raft log (optionally from a snapshot, optionally bounded)
 * followed by injected commands written as {@link CommandText} lines, and writes:
 * <ul>
 *   <li>{@code events.bin}: every event produced, each as int32 little-endian length + the SBE bytes (the same
 *       bytes the venue publishes)</li>
 *   <li>{@code fills.csv}: one row per {@code Trade} event</li>
 *   <li>{@code summary.txt}: counts per event type, trades, volume, notional, last eventSeq and chain hash</li>
 * </ul>
 *
 * <p>Command file: one command per line, {@code #} comments and blank lines ignored. An optional first token
 * {@code @<timestampNs>} sets the cluster timestamp of that command (it must not go backwards); otherwise it is
 * the previous timestamp + {@link Options#stepNs}. Timers the machine schedules fire before the first injected
 * command whose timestamp reaches their deadline (with timestamp = max(deadline, previous timestamp)), and at the
 * end when {@link Options#drainTimers} is set. Injected commands get synthetic, increasing log positions after the
 * log's end (frame-aligned as in a real log). Timers pending in a recorded log are honoured only when replaying
 * from position 0 (a snapshot does not carry the consensus module's timers).
 */
public final class Backtest
{
    static final int SESSION_HEADER_LENGTH = 32;
    static final int FRAME_HEADER_LENGTH = 32;

    /** What to run. {@code logDir} and {@code commandFile} may each be null (not both). */
    public record Options(File logDir, long toPosition, boolean fromSnapshot, File commandFile, int shardId,
        long stepNs, boolean drainTimers, File outDir)
    {
    }

    /** Totals of a run. */
    public record Result(long logEntries, long injected, long timersFired, long timersPending, long events,
        long trades, long volume, long notional, long rejects, long lastEventSeq, String lastChainHash, long endPosition)
    {
    }

    private final ShardStateMachine machine;
    private final TreeMap<Long, Long> timersByCorrelation = new TreeMap<>();
    private final ExpandableArrayBuffer commandBuffer = new ExpandableArrayBuffer(1024);
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final TradeDecoder trade = new TradeDecoder();
    private final Map<Integer, Long> countsByTemplate = new TreeMap<>();
    private final byte[] chainHash = new byte[HashChain.HASH_LENGTH];
    private final byte[] lengthPrefix = new byte[4];
    private byte[] scratch = new byte[1024];
    private final OutputStream events;
    private final PrintWriter fills;
    private long logEntries;
    private long injected;
    private long timersFired;
    private long eventCount;
    private long trades;
    private long volume;
    private long notional;
    private long rejects;
    private long lastEventSeq;
    private long position;
    private long timestampNs;

    private Backtest(final ShardStateMachine machine, final OutputStream events, final PrintWriter fills)
    {
        this.machine = machine;
        this.events = events;
        this.fills = fills;
        machine.bind(this::onEvent, new TimerScheduler()
        {
            @Override
            public void schedule(final long correlationId, final long deadlineNs)
            {
                timersByCorrelation.put(correlationId, deadlineNs);
            }

            @Override
            public void cancel(final long correlationId)
            {
                timersByCorrelation.remove(correlationId);
            }
        });
        fills.println("eventSeq,timestampNs,logPosition,symbolId,matchId,price,quantity,buyOrderId,sellOrderId," +
            "buyFirmId,sellFirmId,aggressorSide,isAuction");
    }

    public static Result run(final ShardStateMachineFactory factory, final Options options)
    {
        if (options.logDir() == null && options.commandFile() == null)
        {
            throw new IllegalArgumentException("nothing to run: give a log directory, a command file, or both");
        }
        final int shardId = options.shardId() >= 0 ? options.shardId() :
            options.logDir() != null ? OfflineVerifier.shardOf(options.logDir()) : 0;
        if (!options.outDir().isDirectory() && !options.outDir().mkdirs())
        {
            throw new IllegalStateException("cannot create " + options.outDir());
        }
        try (OutputStream events = new BufferedOutputStream(new FileOutputStream(new File(options.outDir(), "events.bin")), 1 << 16);
            PrintWriter fills = new PrintWriter(Files.newBufferedWriter(new File(options.outDir(), "fills.csv").toPath())))
        {
            final Backtest bt = new Backtest(factory.create(shardId), events, fills);
            if (options.logDir() != null)
            {
                bt.replayLog(options);
            }
            if (options.commandFile() != null)
            {
                bt.inject(options.commandFile(), options.stepNs());
            }
            if (options.drainTimers())
            {
                bt.fireTimers(Long.MAX_VALUE);
            }
            events.flush();
            fills.flush();
            final Result result = bt.result();
            bt.writeSummary(new File(options.outDir(), "summary.txt"), options, result);
            return result;
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private void replayLog(final Options options)
    {
        try (RaftLogReader log = RaftLogReader.open(options.logDir()))
        {
            final long end = options.toPosition() >= 0 ? Math.min(options.toPosition(), log.recordedPosition()) : log.recordedPosition();
            long start = 0;
            if (options.fromSnapshot() || log.startPosition() > 0)
            {
                final RaftLogReader.Snapshot snapshot = options.fromSnapshot() ?
                    SnapshotLoader.latestServiceSnapshot(log.snapshots(), end) :
                    OfflineVerifier.earliestServiceSnapshotFrom(log, log.startPosition());
                if (snapshot == null)
                {
                    throw new IllegalStateException("no service snapshot to start from in " + options.logDir());
                }
                lastEventSeq = SnapshotLoader.load(new ArchiveFiles(new File(options.logDir(), "archive")), snapshot, machine).lastEventSeq();
                start = snapshot.logPosition();
            }
            position = log.read(start, end, new RaftLogReader.Handler()
            {
                @Override
                public void onCommand(final long logPosition, final long ts, final long clusterSessionId,
                    final DirectBuffer buffer, final int offset, final int length)
                {
                    logEntries++;
                    timestampNs = ts;
                    machine.onCommand(buffer, offset, length, logPosition, ts);
                }

                @Override
                public void onTimer(final long logPosition, final long ts, final long correlationId)
                {
                    logEntries++;
                    timestampNs = ts;
                    timersByCorrelation.remove(correlationId);
                    machine.onTimer(correlationId, logPosition, ts);
                }
            });
        }
    }

    private void inject(final File commandFile, final long stepNs) throws IOException
    {
        final CommandText text = new CommandText();
        int lineNo = 0;
        try (BufferedReader in = Files.newBufferedReader(commandFile.toPath(), StandardCharsets.UTF_8))
        {
            String line;
            while ((line = in.readLine()) != null)
            {
                lineNo++;
                String command = line.strip();
                if (command.isEmpty() || command.startsWith("#"))
                {
                    continue;
                }
                long ts = timestampNs + stepNs;
                if (command.startsWith("@"))
                {
                    final int space = command.indexOf(' ');
                    if (space < 0)
                    {
                        throw new IllegalArgumentException(commandFile + ":" + lineNo + ": a timestamp without a command");
                    }
                    ts = Long.parseLong(command.substring(1, space));
                    command = command.substring(space + 1).strip();
                    if (ts < timestampNs)
                    {
                        throw new IllegalArgumentException(commandFile + ":" + lineNo + ": timestamp " + ts +
                            " goes back from " + timestampNs);
                    }
                }
                final int length;
                try
                {
                    length = text.encode(command, commandBuffer);
                }
                catch (final IllegalArgumentException e)
                {
                    throw new IllegalArgumentException(commandFile + ":" + lineNo + ": " + e.getMessage(), e);
                }
                fireTimers(ts);
                timestampNs = ts;
                position += BitUtil.align(FRAME_HEADER_LENGTH + SESSION_HEADER_LENGTH + length, FRAME_HEADER_LENGTH);
                injected++;
                machine.onCommand(commandBuffer, 0, length, position, ts);
            }
        }
    }

    /** Fires the scheduled timers with deadline <= upTo, in (deadline, correlationId) order. */
    private void fireTimers(final long upTo)
    {
        while (true)
        {
            long bestId = 0;
            long bestDeadline = Long.MAX_VALUE;
            boolean found = false;
            for (final Map.Entry<Long, Long> e : timersByCorrelation.entrySet())
            {
                if (e.getValue() <= upTo && (!found || e.getValue() < bestDeadline))
                {
                    bestId = e.getKey();
                    bestDeadline = e.getValue();
                    found = true;
                }
            }
            if (!found)
            {
                return;
            }
            timersByCorrelation.remove(bestId);
            timestampNs = Math.max(timestampNs, bestDeadline);
            position += BitUtil.align(FRAME_HEADER_LENGTH + 40, FRAME_HEADER_LENGTH); // a TimerEvent frame
            timersFired++;
            machine.onTimer(bestId, position, timestampNs);
        }
    }

    private void onEvent(final DirectBuffer buffer, final int offset, final int length)
    {
        eventCount++;
        final int template = Events.templateId(buffer, offset);
        countsByTemplate.merge(template, 1L, Long::sum);
        lastEventSeq = Events.eventSeq(buffer, offset);
        if (template == ReplayVerifier.COMMAND_APPLIED)
        {
            buffer.getBytes(offset + ReplayVerifier.CHAIN_HASH_OFFSET, chainHash);
        }
        else if (template == TradeDecoder.TEMPLATE_ID)
        {
            trade.wrapAndApplyHeader(buffer, offset, header);
            trades++;
            volume += trade.quantity();
            notional += trade.price() * trade.quantity();
            fills.append(Long.toString(lastEventSeq)).append(',')
                .append(Long.toString(Events.timestampNs(buffer, offset))).append(',')
                .append(Long.toString(Events.logPosition(buffer, offset))).append(',')
                .append(Long.toString(trade.symbolId())).append(',')
                .append(Long.toString(trade.matchId())).append(',')
                .append(Long.toString(trade.price())).append(',')
                .append(Long.toString(trade.quantity())).append(',')
                .append(Long.toString(trade.buyOrderId())).append(',')
                .append(Long.toString(trade.sellOrderId())).append(',')
                .append(Long.toString(trade.buyFirmId())).append(',')
                .append(Long.toString(trade.sellFirmId())).append(',')
                .append(String.valueOf(trade.aggressorSide())).append(',')
                .append(String.valueOf(trade.isAuction())).append('\n');
        }
        else if (template == REJECTED)
        {
            rejects++;
        }
        try
        {
            lengthPrefix[0] = (byte)length;
            lengthPrefix[1] = (byte)(length >>> 8);
            lengthPrefix[2] = (byte)(length >>> 16);
            lengthPrefix[3] = (byte)(length >>> 24);
            events.write(lengthPrefix);
            if (scratch.length < length)
            {
                scratch = new byte[Math.max(length, scratch.length * 2)];
            }
            buffer.getBytes(offset, scratch, 0, length);
            events.write(scratch, 0, length);
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static final int REJECTED = io.lmb.sbe.CommandRejectedDecoder.TEMPLATE_ID;

    private Result result()
    {
        return new Result(logEntries, injected, timersFired, timersByCorrelation.size(), eventCount, trades, volume, notional,
            rejects, lastEventSeq, HexFormat.of().formatHex(chainHash), position);
    }

    private void writeSummary(final File file, final Options options, final Result r) throws IOException
    {
        final List<String> lines = new ArrayList<>();
        lines.add("log " + (options.logDir() == null ? "-" : options.logDir() + (options.fromSnapshot() ? " (from snapshot)" : "") +
            (options.toPosition() >= 0 ? " to " + options.toPosition() : "")));
        lines.add("commands " + (options.commandFile() == null ? "-" : options.commandFile()));
        lines.add("log entries " + r.logEntries());
        lines.add("injected commands " + r.injected());
        lines.add("timers fired " + r.timersFired() + ", pending " + r.timersPending());
        lines.add("events " + r.events());
        for (final Map.Entry<Integer, Long> e : countsByTemplate.entrySet())
        {
            lines.add("  " + templateName(e.getKey()) + " " + e.getValue());
        }
        lines.add("trades " + r.trades() + ", volume " + r.volume() + ", notional " + r.notional());
        lines.add("rejects " + r.rejects());
        lines.add("last eventSeq " + r.lastEventSeq());
        lines.add("last chain hash " + r.lastChainHash());
        lines.add("end position " + r.endPosition());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
    }

    private static final Map<Integer, String> NAMES = new HashMap<>();

    static String templateName(final int templateId)
    {
        if (NAMES.isEmpty())
        {
            for (final String n : new String[] { "ExecutionReport", "Trade", "BookDelta", "SymbolStateChanged",
                "AuctionUpdate", "ReferencePriceApplied", "MassCancelDone", "RiskLimitApplied", "SymbolDefined",
                "CommandRejected", "DayRolled", "ClosingPrice", "CommandApplied" })
            {
                try
                {
                    NAMES.put(Class.forName("io.lmb.sbe." + n + "Decoder").getField("TEMPLATE_ID").getInt(null), n);
                }
                catch (final ReflectiveOperationException ignore)
                {
                    // schema without this message
                }
            }
        }
        return NAMES.getOrDefault(templateId, "template-" + templateId);
    }
}
