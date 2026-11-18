package io.lmb.cluster;

import io.lmb.api.EventSink;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.api.SnapshotSink;
import io.lmb.api.TimerScheduler;
import io.lmb.common.HashChain;
import io.lmb.sbe.CommandAppliedEncoder;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportEncoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.NewOrderDecoder;
import io.lmb.sbe.NewOrderEncoder;
import io.lmb.sbe.SymbolStateChangedEncoder;
import io.lmb.sbe.SymbolStateCommandDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Small deterministic machine for the cluster tests. For each command it emits 1..3 ExecutionReports
 * (clOrdId echoed for NewOrder) and a CommandApplied with the hash chain. SymbolStateCommand with
 * effectiveTimeNs > 0 schedules a timer whose firing emits SymbolStateChanged + CommandApplied.
 * Snapshot: a state chunk plus a full-size filler chunk (exercises MAX_CHUNK_LENGTH).
 */
final class TestMachine implements ShardStateMachine
{
    static final class Factory implements ShardStateMachineFactory
    {
        final List<TestMachine> created = new CopyOnWriteArrayList<>();

        @Override
        public ShardStateMachine create(final int shardId)
        {
            final TestMachine m = new TestMachine(shardId);
            created.add(m);
            return m;
        }
    }

    private final int shardId;
    private final UnsafeBuffer out = new UnsafeBuffer(new byte[512]);
    private final UnsafeBuffer chunk = new UnsafeBuffer(new byte[SnapshotSink.MAX_CHUNK_LENGTH]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ExecutionReportEncoder er = new ExecutionReportEncoder();
    private final CommandAppliedEncoder applied = new CommandAppliedEncoder();
    private final SymbolStateChangedEncoder stateChanged = new SymbolStateChangedEncoder();
    private final NewOrderDecoder newOrder = new NewOrderDecoder();
    private final SymbolStateCommandDecoder stateCommand = new SymbolStateCommandDecoder();
    private final HashChain chain = new HashChain();

    private EventSink sink;
    private TimerScheduler timers;
    private long eventSeq;
    private long commands;
    private long timersFired;
    private long checksum;
    private int eventIdx;
    private long logPosition;
    private long timestampNs;
    private int chunksLoaded;
    volatile boolean restoredFromSnapshot;

    TestMachine(final int shardId)
    {
        this.shardId = shardId;
    }

    @Override
    public int shardId()
    {
        return shardId;
    }

    @Override
    public void bind(final EventSink events, final TimerScheduler timers)
    {
        this.sink = events;
        this.timers = timers;
    }

    @Override
    public void onCommand(
        final DirectBuffer buffer, final int offset, final int length, final long logPosition, final long timestampNs)
    {
        begin(logPosition, timestampNs);
        chain.update(buffer, offset, length);
        commands++;
        for (int i = offset; i < offset + length; i++)
        {
            checksum = checksum * 31 + buffer.getByte(i);
        }
        int templateId = 0;
        long clOrdId = 0;
        if (length >= MessageHeaderDecoder.ENCODED_LENGTH)
        {
            headerDecoder.wrap(buffer, offset);
            templateId = headerDecoder.templateId();
            if (templateId == NewOrderDecoder.TEMPLATE_ID &&
                length >= MessageHeaderDecoder.ENCODED_LENGTH + NewOrderDecoder.BLOCK_LENGTH)
            {
                newOrder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                clOrdId = newOrder.clOrdId();
            }
            else if (templateId == SymbolStateCommandDecoder.TEMPLATE_ID)
            {
                stateCommand.wrapAndApplyHeader(buffer, offset, headerDecoder);
                if (stateCommand.effectiveTimeNs() > 0)
                {
                    timers.schedule(stateCommand.correlationId(), stateCommand.effectiveTimeNs());
                }
            }
        }
        final int reports = 1 + (int)(Math.floorMod(clOrdId, 3));
        for (int i = 0; i < reports; i++)
        {
            out.setMemory(0, out.capacity(), (byte)0); // unset fields must be deterministic too
            er.wrapAndApplyHeader(out, 0, headerEncoder);
            header(er.eventHeader());
            er.clOrdId(clOrdId).orderId(commands).execType(ExecType.NEW).leavesQty(i);
            emit(MessageHeaderEncoder.ENCODED_LENGTH + er.encodedLength());
        }
        finish(templateId);
    }

    @Override
    public void onTimer(final long correlationId, final long logPosition, final long timestampNs)
    {
        begin(logPosition, timestampNs);
        chain.updateLong(correlationId);
        timersFired++;
        out.setMemory(0, out.capacity(), (byte)0);
        stateChanged.wrapAndApplyHeader(out, 0, headerEncoder);
        header(stateChanged.eventHeader());
        stateChanged.correlationId(correlationId).symbolId(1).reasonCode(0);
        emit(MessageHeaderEncoder.ENCODED_LENGTH + stateChanged.encodedLength());
        finish(0);
    }

    private void begin(final long logPosition, final long timestampNs)
    {
        this.logPosition = logPosition;
        this.timestampNs = timestampNs;
        this.eventIdx = 0;
        chain.begin(logPosition, timestampNs);
    }

    private void header(final io.lmb.sbe.EventHeaderEncoder h)
    {
        h.eventSeq(++eventSeq).logPosition(logPosition).timestampNs(timestampNs).eventIdx(eventIdx++)
            .shardId(shardId).reserved(0);
    }

    private void emit(final int length)
    {
        chain.update(out, 0, length);
        sink.onEvent(out, 0, length);
    }

    private void finish(final int templateId)
    {
        final int count = eventIdx;
        final byte[] hash = chain.finish();
        out.setMemory(0, out.capacity(), (byte)0);
        applied.wrapAndApplyHeader(out, 0, headerEncoder);
        header(applied.eventHeader());
        applied.eventCount(count).commandTemplateId(templateId).putChainHash(hash, 0, HashChain.HASH_LENGTH);
        sink.onEvent(out, 0, MessageHeaderEncoder.ENCODED_LENGTH + applied.encodedLength());
    }

    @Override
    public void takeSnapshot(final SnapshotSink sink)
    {
        final MutableDirectBuffer b = chunk;
        b.putLong(0, eventSeq, ByteOrder.LITTLE_ENDIAN);
        b.putLong(8, commands, ByteOrder.LITTLE_ENDIAN);
        b.putLong(16, timersFired, ByteOrder.LITTLE_ENDIAN);
        b.putLong(24, checksum, ByteOrder.LITTLE_ENDIAN);
        b.putBytes(32, chain.current(), 0, HashChain.HASH_LENGTH);
        sink.write(b, 0, 64);
        for (int i = 0; i < SnapshotSink.MAX_CHUNK_LENGTH; i++)
        {
            b.putByte(i, (byte)(i * 7 + eventSeq));
        }
        sink.write(b, 0, SnapshotSink.MAX_CHUNK_LENGTH);
    }

    @Override
    public void onSnapshotChunk(final DirectBuffer buffer, final int offset, final int length)
    {
        if (chunksLoaded == 0)
        {
            eventSeq = buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
            commands = buffer.getLong(offset + 8, ByteOrder.LITTLE_ENDIAN);
            timersFired = buffer.getLong(offset + 16, ByteOrder.LITTLE_ENDIAN);
            checksum = buffer.getLong(offset + 24, ByteOrder.LITTLE_ENDIAN);
            final byte[] h = new byte[HashChain.HASH_LENGTH];
            buffer.getBytes(offset + 32, h);
            chain.set(h, 0);
        }
        else
        {
            if (length != SnapshotSink.MAX_CHUNK_LENGTH)
            {
                throw new IllegalStateException("filler chunk length " + length);
            }
            for (int i = 0; i < length; i++)
            {
                if (buffer.getByte(offset + i) != (byte)(i * 7 + eventSeq))
                {
                    throw new IllegalStateException("filler chunk corrupt at " + i);
                }
            }
        }
        chunksLoaded++;
    }

    @Override
    public void onSnapshotLoaded()
    {
        if (chunksLoaded != 2)
        {
            throw new IllegalStateException("expected 2 chunks, got " + chunksLoaded);
        }
        restoredFromSnapshot = true;
    }

    @Override
    public long lastEventSeq()
    {
        return eventSeq;
    }

    long commands()
    {
        return commands;
    }

    long timersFired()
    {
        return timersFired;
    }

    long checksum()
    {
        return checksum;
    }

    /** Encodes a NewOrder carrying clOrdId. */
    static int newOrder(final MutableDirectBuffer buffer, final long clOrdId)
    {
        final NewOrderEncoder e = new NewOrderEncoder();
        e.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        e.sessionId(1).clOrdId(clOrdId).price(100).quantity(10).firmId(1).symbolId(1);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }
}
