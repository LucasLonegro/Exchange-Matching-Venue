package io.lmb.tools;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.lmb.proto.marketdata.AuctionIndicative;
import io.lmb.proto.marketdata.BookSnapshot;
import io.lmb.proto.marketdata.ClosingPrice;
import io.lmb.proto.marketdata.MarketDataGrpc;
import io.lmb.proto.marketdata.MarketDataUpdate;
import io.lmb.proto.marketdata.PriceLevel;
import io.lmb.proto.marketdata.ReferencePrice;
import io.lmb.proto.marketdata.SubscribeRequest;
import io.lmb.proto.marketdata.SymbolStatus;
import io.lmb.proto.marketdata.TradePrint;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Prints a market data publisher's gRPC stream, one line per update: book snapshots (top of book, state and
 * its cause, reference price and band, last close), incrementals, trades, state changes with their cause,
 * auction indicatives, reference price / band updates and official closes.
 * <pre>
 * scripts/lmb-run io.lmb.tools.MarketDataWatchMain --topology F [--md 0] [--symbols AAPL,GOOG] [--depth 5]
 *     [--conflation-ms 0] [--count N (stop after N updates)] [--host H --port P (override the publisher)]
 * </pre>
 * Each line starts with {@code MD <symbol> seq=<mdSeq> }; prices are in cents.
 */
public final class MarketDataWatchMain
{
    private MarketDataWatchMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final ToolContext ctx = new ToolContext(args);
        final int md = ctx.getInt("md", 0);
        final String host = ctx.get("host", ctx.topology.marketDataHost(md));
        final int port = ctx.getInt("port", ctx.topology.marketDataGrpcPort(md));
        final long count = ctx.getLong("count", Long.MAX_VALUE);
        final SubscribeRequest.Builder req = SubscribeRequest.newBuilder()
            .setDepth(ctx.getInt("depth", 5)).setConflationMs(ctx.getInt("conflation-ms", 0));
        final String symbols = ctx.get("symbols", "");
        if (!symbols.isBlank())
        {
            Arrays.stream(symbols.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(req::addSymbols);
        }
        System.out.println("MD subscribe " + host + ":" + port + (symbols.isBlank() ? " all symbols" : " " + symbols));
        final ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        try
        {
            final Iterator<MarketDataUpdate> it = MarketDataGrpc.newBlockingStub(channel).subscribe(req.build());
            for (long n = 0; n < count && it.hasNext(); n++)
            {
                System.out.println(format(it.next()));
            }
        }
        finally
        {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** One line for an update. */
    public static String format(final MarketDataUpdate u)
    {
        final StringBuilder sb = new StringBuilder("MD ").append(u.getSymbol()).append(" seq=").append(u.getMdSeq());
        switch (u.getBodyCase())
        {
            case SNAPSHOT ->
            {
                final BookSnapshot s = u.getSnapshot();
                sb.append(" SNAPSHOT state=").append(s.getState()).append(cause(s.getStateCause()))
                    .append(" bid=").append(top(s.getBidsList().isEmpty() ? null : s.getBids(0)))
                    .append(" ask=").append(top(s.getAsksList().isEmpty() ? null : s.getAsks(0)))
                    .append(" levels=").append(s.getBidsCount()).append('/').append(s.getAsksCount());
                if (s.hasReference())
                {
                    reference(sb, s.getReference());
                }
                if (s.hasLastClose())
                {
                    sb.append(" lastClose:");
                    close(sb, s.getLastClose());
                }
            }
            case INCREMENTAL -> sb.append(" BOOK bidChanges=").append(u.getIncremental().getBidChangesCount())
                .append(" askChanges=").append(u.getIncremental().getAskChangesCount());
            case TRADE ->
            {
                final TradePrint t = u.getTrade();
                sb.append(" TRADE ").append(t.getQuantity()).append('@').append(t.getPriceCents())
                    .append(" match=").append(t.getMatchId())
                    .append(t.getAuction() ? " auction" : " aggressor=" + t.getAggressorSide());
            }
            case STATUS ->
            {
                final SymbolStatus s = u.getStatus();
                sb.append(" STATUS state=").append(s.getState()).append(cause(s.getCause()))
                    .append(" reasonCode=").append(s.getReasonCode());
            }
            case AUCTION ->
            {
                final AuctionIndicative a = u.getAuction();
                sb.append(" AUCTION price=").append(a.getHasPrice() ? Long.toString(a.getPriceCents()) : "-")
                    .append(" matched=").append(a.getMatchedQuantity()).append(" imbalance=")
                    .append(a.getImbalanceQuantity()).append(a.getImbalanceSide().isEmpty() ? "" : " " + a.getImbalanceSide())
                    .append(a.getFinal() ? " final" : "");
            }
            case REFERENCE ->
            {
                sb.append(" REFERENCE");
                reference(sb, u.getReference());
            }
            case CLOSE ->
            {
                sb.append(" CLOSE");
                close(sb, u.getClose());
            }
            default -> sb.append(" ?").append(u.getBodyCase());
        }
        return sb.toString();
    }

    private static String cause(final String cause)
    {
        return cause.isEmpty() ? "" : " cause=" + cause;
    }

    private static String top(final PriceLevel l)
    {
        return l == null ? "-" : l.getQuantity() + "@" + l.getPriceCents();
    }

    private static void reference(final StringBuilder sb, final ReferencePrice r)
    {
        sb.append(" ref=").append(r.getHasReferencePrice() ? Long.toString(r.getReferencePriceCents()) : "-")
            .append(" band=").append(r.getHasBand() ? r.getBandLowCents() + ".." + r.getBandHighCents() : "-");
    }

    private static void close(final StringBuilder sb, final ClosingPrice c)
    {
        sb.append(" price=").append(c.getHasPrice() ? Long.toString(c.getPriceCents()) : "-")
            .append(" prevClose=").append(c.getHasPreviousClose() ? Long.toString(c.getPreviousCloseCents()) : "-")
            .append(" date=").append(c.getTradingDate()).append(" source=").append(c.getSource());
    }
}
