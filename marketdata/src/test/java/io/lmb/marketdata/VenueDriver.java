package io.lmb.marketdata;

import io.lmb.api.CommandClient;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.sbe.BooleanType;
import io.lmb.sbe.CancelOrderEncoder;
import io.lmb.sbe.DayRolloverEncoder;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.NewOrderEncoder;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.ReplaceOrderEncoder;
import io.lmb.sbe.RiskLimitUpdateEncoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolDefinitionEncoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateCommandEncoder;
import io.lmb.sbe.TimeInForce;
import io.lmb.testkit.InMemoryVenue;
import org.agrona.ExpandableArrayBuffer;

/** Drives the real matcher state machine through the in-memory venue. Test helper. */
final class VenueDriver
{
    static final long T0 = 1_700_000_000_000_000_000L;

    final InMemoryVenue venue;
    private final CommandClient[] clients;
    private final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(256);
    private final MessageHeaderEncoder hdr = new MessageHeaderEncoder();
    private long nextClOrdId = 1;
    private long nextCorrelationId = 1;

    VenueDriver(final int shards)
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
        e.correlationId(nextCorrelationId++).creditBudget(Long.MAX_VALUE / 4).maxPositionPerSymbol(Long.MAX_VALUE / 4)
            .firmId(firmId).enabled(BooleanType.TRUE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    void symbol(final int shard, final int symbolId, final String ticker, final long refPrice, final SymbolState state)
    {
        final SymbolDefinitionEncoder e = new SymbolDefinitionEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).tickSize(1).lotSize(1).referencePrice(refPrice).maxOrderQty(1_000_000)
            .symbolId(symbolId).priceBandBps(5000).ticker(ticker).initialState(state);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    void state(final int shard, final int symbolId, final SymbolState state)
    {
        final SymbolStateCommandEncoder e = new SymbolStateCommandEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).effectiveTimeNs(0).symbolId(symbolId).reasonCode(7).targetState(state);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    long order(final int shard, final int firmId, final int symbolId, final Side side, final long price, final long qty)
    {
        return order(shard, firmId, symbolId, side, price, qty, OrdType.LIMIT, TimeInForce.DAY);
    }

    long order(
        final int shard, final int firmId, final int symbolId, final Side side, final long price, final long qty,
        final OrdType type, final TimeInForce tif)
    {
        final long id = nextClOrdId++;
        final NewOrderEncoder e = new NewOrderEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.sessionId(firmId * 10L).clOrdId(id).price(type == OrdType.MARKET ? Long.MIN_VALUE : price).quantity(qty)
            .gatewayInTsNs(0).firmId(firmId).accountId(1).symbolId(symbolId).gatewayId(0).side(side).ordType(type)
            .timeInForce(tif).possResend(BooleanType.FALSE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
        return id;
    }

    void cancel(final int shard, final int firmId, final int symbolId, final long origClOrdId)
    {
        final CancelOrderEncoder e = new CancelOrderEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.sessionId(firmId * 10L).clOrdId(nextClOrdId++).origClOrdId(origClOrdId).gatewayInTsNs(0).firmId(firmId)
            .symbolId(symbolId).gatewayId(0).possResend(BooleanType.FALSE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    long replace(final int shard, final int firmId, final int symbolId, final long origClOrdId, final long price, final long qty)
    {
        final long id = nextClOrdId++;
        final ReplaceOrderEncoder e = new ReplaceOrderEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.sessionId(firmId * 10L).clOrdId(id).origClOrdId(origClOrdId).newPrice(price).newQuantity(qty).gatewayInTsNs(0)
            .firmId(firmId).symbolId(symbolId).gatewayId(0).possResend(BooleanType.FALSE);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
        return id;
    }

    void dayRollover(final int shard, final int date)
    {
        final DayRolloverEncoder e = new DayRolloverEncoder().wrapAndApplyHeader(buf, 0, hdr);
        e.correlationId(nextCorrelationId++).tradingDate(date);
        send(shard, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    /** A random but reproducible session on one shard: symbols, auction, continuous trading, cancels, replaces. */
    void randomSession(final int shard, final int[] symbolIds, final long seed, final int steps)
    {
        final java.util.Random rnd = new java.util.Random(seed);
        final java.util.List<long[]> live = new java.util.ArrayList<>(); // {symbolId, clOrdId, firm}
        for (int i = 0; i < steps; i++)
        {
            final int symbolId = symbolIds[rnd.nextInt(symbolIds.length)];
            final int firm = 1 + rnd.nextInt(3);
            final int action = rnd.nextInt(100);
            if (action < 55 || live.isEmpty())
            {
                final Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
                final long price = 10_000 + (side == Side.BUY ? -1 : 1) * (rnd.nextInt(20) - 4);
                final boolean ioc = rnd.nextInt(10) == 0;
                final long id = order(shard, firm, symbolId, side, price, 1 + rnd.nextInt(500), OrdType.LIMIT,
                    ioc ? TimeInForce.IOC : TimeInForce.DAY);
                if (!ioc)
                {
                    live.add(new long[] {symbolId, id, firm});
                }
            }
            else if (action < 80)
            {
                final long[] o = live.remove(rnd.nextInt(live.size()));
                cancel(shard, (int)o[2], (int)o[0], o[1]);
            }
            else if (action < 95)
            {
                final long[] o = live.get(rnd.nextInt(live.size()));
                o[1] = replace(shard, (int)o[2], (int)o[0], o[1], 10_000 + rnd.nextInt(16) - 8, 1 + rnd.nextInt(400));
            }
            else if (action < 97)
            {
                order(shard, firm, symbolId, rnd.nextBoolean() ? Side.BUY : Side.SELL, 0, 1 + rnd.nextInt(800),
                    OrdType.MARKET, TimeInForce.IOC);
            }
            else if (action < 99)
            {
                state(shard, symbolId, SymbolState.AUCTION_CALL);
                for (int k = 0; k < 6; k++)
                {
                    order(shard, 1 + rnd.nextInt(3), symbolId, k % 2 == 0 ? Side.BUY : Side.SELL,
                        10_000 + rnd.nextInt(10) - 5, 1 + rnd.nextInt(300));
                }
                state(shard, symbolId, SymbolState.CONTINUOUS);
            }
            else
            {
                state(shard, symbolId, SymbolState.HALTED);
                state(shard, symbolId, SymbolState.CONTINUOUS);
            }
        }
    }
}
