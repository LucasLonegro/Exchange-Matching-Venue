package io.lmb.posttrade;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.api.VenueClients;
import io.lmb.common.Events;
import io.lmb.sbe.BooleanType;
import io.lmb.sbe.CancelOrderEncoder;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.NewOrderEncoder;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.RiskLimitUpdateEncoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolDefinitionEncoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.TimeInForce;
import io.lmb.testkit.InMemoryVenue;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.function.BooleanSupplier;

/**
 * Test venue: testkit's {@link InMemoryVenue} driven by the real matcher, plus the cluster client's edge
 * behaviours: streams opened {@code rewind} events earlier than asked (at-least-once redelivery) and one
 * eventSeq that can be skipped (a gap the runner must refuse).
 */
public final class TestVenue implements VenueClients
{
    public static final long T0 = 1_700_000_000_000_000_000L;

    public final InMemoryVenue venue;
    public volatile long rewind;
    public volatile long skipEventSeq = -1;
    private final CommandClient[] clients;
    private final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(256);
    private final MessageHeaderEncoder hdr = new MessageHeaderEncoder();
    private long nextClOrdId = 1;
    private long nextCorrelationId = 1;

    public TestVenue(final int shards)
    {
        final ShardStateMachine[] machines = new ShardStateMachine[shards];
        final ShardStateMachineFactory factory = ShardStateMachineFactory.load();
        for (int i = 0; i < shards; i++)
        {
            machines[i] = factory.create(i);
        }
        venue = new InMemoryVenue(T0, machines);
        clients = new CommandClient[shards];
        for (int i = 0; i < shards; i++)
        {
            clients[i] = venue.commandClient(i);
        }
    }

    /** Firms 100 and 200 and symbol (shard + 1) "SYM<shard>" in continuous trading on every shard. */
    public TestVenue standardSetup()
    {
        for (int s = 0; s < shardCount(); s++)
        {
            firm(s, 100);
            firm(s, 200);
            symbol(s, s + 1, "SYM" + s, 10_000);
        }
        return this;
    }

    private void send(final int shard, final int length)
    {
        if (!clients[shard].offer(buf, 0, length))
        {
            throw new IllegalStateException("offer refused");
        }
        venue.advanceTimeBy(1_000);
    }

    public void firm(final int shard, final int firmId)
    {
        final RiskLimitUpdateEncoder e = new RiskLimitUpdateEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).creditBudget(Long.MAX_VALUE / 4).maxPositionPerSymbol(Long.MAX_VALUE / 4)
            .firmId(firmId).enabled(BooleanType.TRUE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    public void symbol(final int shard, final int symbolId, final String ticker, final long refPrice)
    {
        final SymbolDefinitionEncoder e = new SymbolDefinitionEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).tickSize(1).lotSize(1).referencePrice(refPrice).maxOrderQty(1_000_000)
            .symbolId(symbolId).priceBandBps(5000).dynamicBandBps(0).volatilityAuctionNs(0).ticker(ticker)
            .initialState(SymbolState.CONTINUOUS);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    public long order(final int shard, final int firmId, final Side side, final long price, final long qty)
    {
        final long id = nextClOrdId++;
        final NewOrderEncoder e = new NewOrderEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.sessionId(firmId * 10L).clOrdId(id).price(price).quantity(qty).gatewayInTsNs(0).firmId(firmId).accountId(1)
            .symbolId(shard + 1).gatewayId(0).side(side).ordType(OrdType.LIMIT).timeInForce(TimeInForce.DAY)
            .possResend(BooleanType.FALSE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
        return id;
    }

    public void cancel(final int shard, final int firmId, final long origClOrdId)
    {
        final CancelOrderEncoder e = new CancelOrderEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.sessionId(firmId * 10L).clOrdId(nextClOrdId++).origClOrdId(origClOrdId).gatewayInTsNs(0).firmId(firmId)
            .symbolId(shard + 1).gatewayId(0).possResend(BooleanType.FALSE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    /** One resting sell from firm 200 fully taken by a buy from firm 100: one Trade, two fills. */
    public void cross(final int shard, final long price, final long qty)
    {
        order(shard, 200, Side.SELL, price, qty);
        order(shard, 100, Side.BUY, price, qty);
    }

    /** Number of events of the template emitted so far by the shard. */
    public long count(final int shard, final int templateId)
    {
        long n = 0;
        for (long seq = 1; seq <= venue.eventCount(shard); seq++)
        {
            if (Events.templateId(new UnsafeBuffer(venue.event(shard, seq)), 0) == templateId)
            {
                n++;
            }
        }
        return n;
    }

    public static void await(final BooleanSupplier condition, final long timeoutMs, final String what)
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new AssertionError("timed out waiting for " + what);
            }
            try
            {
                Thread.sleep(5);
            }
            catch (final InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    @Override
    public int shardCount()
    {
        return venue.shardCount();
    }

    @Override
    public CommandClient commandClient(final int shardId)
    {
        return venue.commandClient(shardId);
    }

    @Override
    public EventStreamClient eventStream(final int shardId, final long fromEventSeq)
    {
        final EventStreamClient inner = venue.eventStream(shardId,
            fromEventSeq == LATEST ? LATEST : Math.max(0, fromEventSeq - rewind));
        return new EventStreamClient()
        {
            public int shardId()
            {
                return shardId;
            }

            public int poll(final EventHandler handler, final int limit)
            {
                return inner.poll((b, o, l) ->
                {
                    if (Events.eventSeq(b, o) != skipEventSeq)
                    {
                        handler.onEvent(b, o, l);
                    }
                }, limit);
            }

            public long lastEventSeq()
            {
                return inner.lastEventSeq();
            }

            public boolean isConnected()
            {
                return true;
            }

            public void close()
            {
                inner.close();
            }
        };
    }

    @Override
    public void close()
    {
        venue.close();
    }
}
