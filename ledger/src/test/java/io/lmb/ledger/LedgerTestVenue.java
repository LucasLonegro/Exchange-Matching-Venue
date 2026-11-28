package io.lmb.ledger;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.api.VenueClients;
import io.lmb.sbe.BooleanType;
import io.lmb.sbe.DayRolloverEncoder;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.NewOrderEncoder;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.RiskLimitUpdateEncoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolDefinitionEncoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateCommandEncoder;
import io.lmb.sbe.TimeInForce;
import io.lmb.testkit.InMemoryVenue;
import org.agrona.ExpandableArrayBuffer;

/**
 * The real matcher in testkit's {@link InMemoryVenue}, with the commands the ledger tests need (firms, symbols,
 * orders, state changes, day rollovers) and the cluster client's at-least-once edge: streams opened {@code rewind}
 * events earlier than asked.
 */
final class LedgerTestVenue implements VenueClients
{
    static final long T0 = 1_700_000_000_000_000_000L;

    final InMemoryVenue venue;
    volatile long rewind;
    private final CommandClient[] clients;
    private final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(256);
    private final MessageHeaderEncoder hdr = new MessageHeaderEncoder();
    private long nextClOrdId = 1;
    private long nextCorrelationId = 1;

    LedgerTestVenue(final int shards)
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

    private void send(final int shard, final int length)
    {
        if (!clients[shard].offer(buf, 0, length))
        {
            throw new IllegalStateException("offer refused");
        }
        venue.advanceTimeBy(1_000);
    }

    void firm(final int shard, final int firmId)
    {
        final RiskLimitUpdateEncoder e = new RiskLimitUpdateEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).creditBudget(10_000_000_000_000_000L).maxPositionPerSymbol(1_000_000_000L)
            .firmId(firmId).enabled(BooleanType.TRUE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    void symbol(final int shard, final int symbolId, final String ticker, final long refPrice, final SymbolState initial)
    {
        final SymbolDefinitionEncoder e = new SymbolDefinitionEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).tickSize(1).lotSize(1).referencePrice(refPrice).maxOrderQty(1_000_000)
            .symbolId(symbolId).priceBandBps(5000).dynamicBandBps(0).volatilityAuctionNs(0).ticker(ticker)
            .initialState(initial);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    void state(final int shard, final int symbolId, final SymbolState target)
    {
        final SymbolStateCommandEncoder e = new SymbolStateCommandEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).effectiveTimeNs(0).symbolId(symbolId).reasonCode(0).targetState(target);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    void roll(final int shard, final int tradingDate)
    {
        final DayRolloverEncoder e = new DayRolloverEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).tradingDate(tradingDate);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    void order(final int shard, final int symbolId, final int firmId, final Side side, final long price, final long qty)
    {
        final NewOrderEncoder e = new NewOrderEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.sessionId(firmId * 10L).clOrdId(nextClOrdId++).price(price).quantity(qty).gatewayInTsNs(0).firmId(firmId)
            .accountId(1).symbolId(symbolId).gatewayId(0).side(side).ordType(OrdType.LIMIT).timeInForce(TimeInForce.DAY)
            .possResend(BooleanType.FALSE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    /** A resting order of {@code seller} fully taken by {@code buyer}: one Trade of qty at price. */
    void trade(final int shard, final int symbolId, final int buyer, final int seller, final long price, final long qty)
    {
        order(shard, symbolId, seller, Side.SELL, price, qty);
        order(shard, symbolId, buyer, Side.BUY, price, qty);
    }

    long eventCount(final int shard)
    {
        return venue.eventCount(shard);
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
        return venue.eventStream(shardId, fromEventSeq == LATEST ? LATEST : Math.max(0, fromEventSeq - rewind));
    }

    @Override
    public void close()
    {
        venue.close();
    }
}
