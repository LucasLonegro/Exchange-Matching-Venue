package io.lmb.posttrade.surveillance;

import io.lmb.common.Events;
import io.lmb.posttrade.PostTradeMetrics;
import io.lmb.posttrade.PostTradeSink;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.TradeDecoder;
import org.agrona.DirectBuffer;

/**
 * Surveillance feed: every ExecutionReport (all exec types, rejects included) and every Trade as one JSON object
 * per event, written to a {@link SurveillanceOutput} (Kafka topic, or a JSON-lines file locally). Other events
 * only advance the cursor. Every record starts with {@code {"shard":S,"eventSeq":N,"type":...}};
 * (shard, eventSeq) identifies the record, so consumers can drop duplicates.
 */
public final class SurveillanceSink implements PostTradeSink
{
    private final SurveillanceOutput output;
    private final long flushIntervalMs;
    private final PostTradeMetrics metrics;
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final ExecutionReportDecoder report = new ExecutionReportDecoder();
    private final TradeDecoder trade = new TradeDecoder();
    private final StringBuilder sb = new StringBuilder(512);
    private boolean dirty;
    private long nextFlushMs;

    public SurveillanceSink(final SurveillanceOutput output, final long flushIntervalMs, final PostTradeMetrics metrics)
    {
        this.output = output;
        this.flushIntervalMs = flushIntervalMs;
        this.metrics = metrics;
    }

    @Override
    public String name()
    {
        return "surveillance";
    }

    @Override
    public long[] open(final int shardCount) throws Exception
    {
        nextFlushMs = 0;
        dirty = false;
        return output.open(shardCount);
    }

    @Override
    public void onEvent(final int shardId, final long eventSeq, final DirectBuffer buffer, final int offset, final int length)
        throws Exception
    {
        header.wrap(buffer, offset);
        final int templateId = header.templateId();
        if (templateId == ExecutionReportDecoder.TEMPLATE_ID)
        {
            report.wrapAndApplyHeader(buffer, offset, header);
            start(shardId, eventSeq, "ExecutionReport", buffer, offset);
            appendReport(report);
        }
        else if (templateId == TradeDecoder.TEMPLATE_ID)
        {
            trade.wrapAndApplyHeader(buffer, offset, header);
            start(shardId, eventSeq, "Trade", buffer, offset);
            appendTrade(trade);
        }
        else
        {
            output.skip(shardId, eventSeq);
            return;
        }
        sb.append('}');
        output.write(shardId, eventSeq, sb);
        metrics.surveillanceRecords.incrementAndGet();
        dirty = true;
    }

    /** Starts the record in {@code sb}; the caller appends the body and the closing brace. */
    private void start(final int shardId, final long eventSeq, final String type, final DirectBuffer buffer, final int offset)
    {
        sb.setLength(0);
        sb.append("{\"shard\":").append(shardId).append(",\"eventSeq\":").append(eventSeq)
            .append(",\"type\":\"").append(type).append('"')
            .append(",\"timestampNs\":").append(Events.timestampNs(buffer, offset));
        field("execId", Events.execId(buffer, offset));
    }

    private void appendReport(final ExecutionReportDecoder d)
    {
        field("execType", d.execType().name());
        field("ordStatus", d.ordStatus().name());
        field("orderId", d.orderId());
        field("clOrdId", d.clOrdId());
        field("origClOrdId", d.origClOrdId());
        field("sessionId", d.sessionId());
        field("firmId", d.firmId());
        field("accountId", d.accountId());
        field("symbolId", d.symbolId());
        field("side", d.side().name());
        field("ordType", d.ordType().name());
        field("timeInForce", d.timeInForce().name());
        if (d.price() != ExecutionReportDecoder.priceNullValue())
        {
            field("price", d.price());
        }
        field("orderQty", d.orderQty());
        field("leavesQty", d.leavesQty());
        field("cumQty", d.cumQty());
        field("lastQty", d.lastQty());
        field("lastPx", d.lastPx());
        field("matchId", d.matchId());
        field("liquidity", d.liquidity().name());
        field("rejectReason", d.rejectReason().name());
        field("cancelReason", d.cancelReason().name());
    }

    private void appendTrade(final TradeDecoder t)
    {
        field("matchId", t.matchId());
        field("symbolId", t.symbolId());
        field("price", t.price());
        field("quantity", t.quantity());
        field("buyOrderId", t.buyOrderId());
        field("sellOrderId", t.sellOrderId());
        field("buyFirmId", t.buyFirmId());
        field("sellFirmId", t.sellFirmId());
        field("aggressorSide", t.aggressorSide().name());
        sb.append(",\"auction\":").append(t.isAuction() == io.lmb.sbe.BooleanType.TRUE);
    }

    private void field(final String name, final long value)
    {
        sb.append(",\"").append(name).append("\":").append(value);
    }

    private void field(final String name, final String value)
    {
        sb.append(",\"").append(name).append("\":\"");
        for (int i = 0; i < value.length(); i++)
        {
            final char c = value.charAt(i);
            if (c == '"' || c == '\\')
            {
                sb.append('\\').append(c);
            }
            else if (c < 0x20 || c > 0x7e)
            {
                sb.append(String.format("\\u%04x", (int)c));
            }
            else
            {
                sb.append(c);
            }
        }
        sb.append('"');
    }

    @Override
    public int doWork(final long nowMs) throws Exception
    {
        if (dirty && nowMs >= nextFlushMs)
        {
            output.flush();
            dirty = false;
            nextFlushMs = nowMs + flushIntervalMs;
            return 1;
        }
        return output.poll();
    }

    @Override
    public void close(final boolean clean)
    {
        output.close(clean);
    }
}
