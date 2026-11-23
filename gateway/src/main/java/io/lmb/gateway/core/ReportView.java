package io.lmb.gateway.core;

import io.lmb.common.Events;
import io.lmb.common.SymbolDirectory;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;

/**
 * Read-only view of one ExecutionReport event (messageHeader + body) handed to outlets. Valid only
 * during the call it is passed to.
 */
public final class ReportView
{
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final ExecutionReportDecoder decoder = new ExecutionReportDecoder();
    private final LiveSymbols symbols;
    private DirectBuffer buffer;
    private int offset;
    private int length;

    public ReportView(final LiveSymbols symbols)
    {
        this.symbols = symbols;
    }

    public ReportView wrap(final DirectBuffer buffer, final int offset, final int length)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        decoder.wrapAndApplyHeader(buffer, offset, header);
        return this;
    }

    public ExecutionReportDecoder decoder()
    {
        return decoder;
    }

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public int length()
    {
        return length;
    }

    public int shardId()
    {
        return Events.shardId(buffer, offset);
    }

    public long eventSeq()
    {
        return Events.eventSeq(buffer, offset);
    }

    public long timestampNs()
    {
        return Events.timestampNs(buffer, offset);
    }

    /** "shard-logPosition-eventIdx". Allocates. */
    public String execId()
    {
        return Events.execId(buffer, offset);
    }

    /** Ticker of the report's symbol, or the numeric id if the directory does not know it. */
    public String ticker()
    {
        final SymbolDirectory.Symbol s = symbols.byId((int)decoder.symbolId());
        return s != null ? s.ticker() : Long.toString(decoder.symbolId());
    }
}
