package io.lmb.feeds;

import io.lmb.api.CommandClient;
import io.lmb.common.SymbolDirectory;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.ReferencePriceEncoder;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * NBBO consolidator (ARCHITECTURE.md 5.3). Listens to a {@link VenueFeedHandler}, keeps each venue's
 * top of book per symbol, and submits {@code ReferencePrice} commands to the owning shard.
 *
 * <ul>
 *   <li>Freshness: a venue counts only while its handler reports it UP and its newest in-sequence data
 *       is at most {@code staleNs} old, where the data's time is {@code min(venueTs, receiveTs)} (so a
 *       silent venue and a venue whose own clock lags both age out).</li>
 *   <li>A {@link VenueStatus#GAP} voids all of that venue's quotes until each symbol is quoted again.</li>
 *   <li>NBB = highest bid, NBO = lowest offer over fresh venues with a valid quote (a venue quote that
 *       is self-locked, self-crossed or empty is ignored). Nothing is published with fewer than
 *       {@code minVenues} contributing venues, a one-sided NBBO or a crossed NBBO; a locked NBBO is
 *       published. Reference price = midpoint rounded half up to the symbol's tick.</li>
 *   <li>Throttling per symbol: a reference within {@code minChangeTicks} ticks of the last one
 *       submitted is suppressed; at most one command per {@code minIntervalNs} (a change in between
 *       waits, the latest value wins); back pressure retries on the next duty cycle.</li>
 *   <li>{@code sourceTsNs} = the newest venue data time seen on any venue, forced strictly increasing
 *       per symbol. Two consolidators on the same feed produce nearly equal values and the shard keeps
 *       the newest (ARCHITECTURE.md 3.1).</li>
 * </ul>
 *
 * Single-threaded: the listener callbacks and {@link #doWork()} run on the duty thread. Allocation-free
 * after construction. Iteration orders are fixed (symbol id order, venue order).
 */
public final class NbboConsolidator implements VenueFeedHandler.Listener
{
    private final ConsolidatorConfig config;
    private final EpochNanoClock clock;
    private final ConsolidatorStats stats;
    private final CommandClient[] clients;
    private final int venues;
    private final int symbolCount;

    // symbols, in symbol id order
    private final Int2IntHashMap indexBySymbolId = new Int2IntHashMap(-1);
    private final int[] symbolIds;
    private final long[] tick;
    private final int[] shardOf;

    // venues
    private final boolean[] venueUp;
    private final boolean[] venueFresh;
    private final int[] venueEpoch;
    private final long[] venueAsOfNs;
    private long venueTimeWatermarkNs = Long.MIN_VALUE;

    // venue x symbol quotes, index v * symbolCount + s
    private final long[] quoteBid;
    private final long[] quoteAsk;
    private final int[] quoteEpoch;

    // per symbol state
    private final boolean[] dirty;
    private final int[] dirtyList;
    private int dirtyCount;
    private final long[] dirtySinceRxNs;
    private final boolean[] available;
    private final boolean[] hasSubmitted;
    private final long[] lastRef;
    private final long[] lastSubmitNs;
    private final long[] lastSourceTsNs;
    private final boolean[] pending;
    private final int[] pendingList;
    private int pendingCount;
    private final long[] pendingRef;
    private final long[] pendingBid;
    private final long[] pendingAsk;
    private final int[] pendingVenues;
    private final long[] pendingTriggerNs;

    private final boolean[] shardRefused;
    private volatile boolean allConnected;

    private final UnsafeBuffer encodeBuffer = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final ReferencePriceEncoder encoder = new ReferencePriceEncoder();

    /**
     * @param clients command client per shard id ({@code clients[shard]}); every symbol's shard must have one
     */
    public NbboConsolidator(
        final ConsolidatorConfig config,
        final SymbolDirectory symbols,
        final CommandClient[] clients,
        final EpochNanoClock clock)
    {
        this.config = config;
        this.clock = clock;
        this.clients = clients.clone();
        this.venues = config.venueCount();
        this.stats = new ConsolidatorStats(venues);

        final List<SymbolDirectory.Symbol> sorted = symbols.all().stream()
            .sorted(Comparator.comparingInt(SymbolDirectory.Symbol::symbolId)).toList();
        symbolCount = sorted.size();
        symbolIds = new int[symbolCount];
        tick = new long[symbolCount];
        shardOf = new int[symbolCount];
        for (int s = 0; s < symbolCount; s++)
        {
            final SymbolDirectory.Symbol sym = sorted.get(s);
            if (sym.shard() < 0 || sym.shard() >= clients.length || clients[sym.shard()] == null)
            {
                throw new IllegalArgumentException("no command client for shard " + sym.shard() + " of " + sym);
            }
            if (sym.tickSize() <= 0)
            {
                throw new IllegalArgumentException("tick size must be positive: " + sym);
            }
            symbolIds[s] = sym.symbolId();
            tick[s] = sym.tickSize();
            shardOf[s] = sym.shard();
            indexBySymbolId.put(sym.symbolId(), s);
        }

        venueUp = new boolean[venues];
        venueFresh = new boolean[venues];
        venueEpoch = new int[venues];
        venueAsOfNs = new long[venues];
        Arrays.fill(venueAsOfNs, Long.MIN_VALUE);

        quoteBid = new long[venues * symbolCount];
        quoteAsk = new long[venues * symbolCount];
        quoteEpoch = new int[venues * symbolCount];
        Arrays.fill(quoteEpoch, -1);

        dirty = new boolean[symbolCount];
        dirtyList = new int[symbolCount];
        dirtySinceRxNs = new long[symbolCount];
        Arrays.fill(dirtySinceRxNs, Long.MIN_VALUE);
        available = new boolean[symbolCount];
        hasSubmitted = new boolean[symbolCount];
        lastRef = new long[symbolCount];
        lastSubmitNs = new long[symbolCount];
        lastSourceTsNs = new long[symbolCount];
        pending = new boolean[symbolCount];
        pendingList = new int[symbolCount];
        pendingRef = new long[symbolCount];
        pendingBid = new long[symbolCount];
        pendingAsk = new long[symbolCount];
        pendingVenues = new int[symbolCount];
        pendingTriggerNs = new long[symbolCount];
        shardRefused = new boolean[clients.length];
    }

    public ConsolidatorStats stats()
    {
        return stats;
    }

    /** True once every shard's command client reported connected on the duty thread. */
    public boolean allShardsConnected()
    {
        return allConnected;
    }

    // ---------------------------------------------------------------- listener (duty thread)

    @Override
    public void onQuote(final NormalizedQuote q)
    {
        final int v = q.venueId();
        if (v < 0 || v >= venues)
        {
            stats.invalidQuotes.incrementAndGet();
            return;
        }
        stats.quotes.incrementAndGet(v);
        advanceVenueTime(v, Math.min(q.venueTsNs(), q.receiveTsNs()));
        final int s = indexBySymbolId.get(q.symbolId());
        if (s < 0)
        {
            stats.unknownSymbol.incrementAndGet();
            return;
        }
        long bid = q.hasBid() ? q.bidPrice() : NormalizedQuote.NO_PRICE;
        long ask = q.hasAsk() ? q.askPrice() : NormalizedQuote.NO_PRICE;
        if ((bid == NormalizedQuote.NO_PRICE && ask == NormalizedQuote.NO_PRICE) ||
            (bid != NormalizedQuote.NO_PRICE && ask != NormalizedQuote.NO_PRICE && bid >= ask))
        {
            stats.invalidQuotes.incrementAndGet();
            bid = NormalizedQuote.NO_PRICE; // the venue's previous quote is replaced either way
            ask = NormalizedQuote.NO_PRICE;
        }
        final int i = v * symbolCount + s;
        quoteBid[i] = bid;
        quoteAsk[i] = ask;
        quoteEpoch[i] = venueEpoch[v];
        markDirty(s, q.receiveTsNs());
    }

    @Override
    public void onHeartbeat(final int venueId, final long venueTsNs, final long receiveTsNs)
    {
        if (venueId >= 0 && venueId < venues)
        {
            advanceVenueTime(venueId, Math.min(venueTsNs, receiveTsNs));
        }
    }

    @Override
    public void onVenueStatus(final int venueId, final VenueStatus status, final long missedMessages, final long tsNs)
    {
        if (venueId < 0 || venueId >= venues)
        {
            return;
        }
        switch (status)
        {
            case UP -> venueUp[venueId] = true;
            case STALE -> venueUp[venueId] = false;
            case GAP ->
            {
                venueEpoch[venueId]++;
                stats.gaps.incrementAndGet(venueId);
                stats.gapMessages.addAndGet(venueId, missedMessages);
                markAllDirty();
            }
        }
    }

    // ---------------------------------------------------------------- duty cycle

    /** Freshness, NBBO for changed symbols, throttled submission, command client duty cycles. */
    public int doWork()
    {
        final long now = clock.nanoTime();
        int work = updateFreshness(now);
        work += recomputeDirty(now);
        work += submitPending(now);
        boolean connected = true;
        for (final CommandClient client : clients)
        {
            if (client != null)
            {
                work += client.poll();
                connected &= client.isConnected();
            }
        }
        if (connected != allConnected)
        {
            allConnected = connected;
        }
        return work;
    }

    private int updateFreshness(final long now)
    {
        int changes = 0;
        for (int v = 0; v < venues; v++)
        {
            final long asOf = venueAsOfNs[v];
            final long age = asOf == Long.MIN_VALUE ? Long.MAX_VALUE : now - asOf;
            final boolean fresh = venueUp[v] && age <= config.staleNs();
            stats.venueDataAgeNs.lazySet(v, asOf == Long.MIN_VALUE ? 0 : age);
            if (fresh != venueFresh[v])
            {
                venueFresh[v] = fresh;
                stats.venueFresh.set(v, fresh ? 1 : 0);
                if (!fresh)
                {
                    stats.staleTransitions.incrementAndGet(v);
                }
                markAllDirty();
                changes++;
            }
        }
        return changes;
    }

    private int recomputeDirty(final long now)
    {
        final int n = dirtyCount;
        for (int k = 0; k < n; k++)
        {
            final int s = dirtyList[k];
            dirty[s] = false;
            recompute(s, now);
        }
        dirtyCount = 0;
        return n;
    }

    private void recompute(final int s, final long now)
    {
        long bestBid = Long.MIN_VALUE;
        long bestAsk = Long.MAX_VALUE;
        int contributing = 0;
        for (int v = 0; v < venues; v++)
        {
            final int i = v * symbolCount + s;
            if (!venueFresh[v] || quoteEpoch[i] != venueEpoch[v])
            {
                continue;
            }
            final long bid = quoteBid[i];
            final long ask = quoteAsk[i];
            if (bid == NormalizedQuote.NO_PRICE && ask == NormalizedQuote.NO_PRICE)
            {
                continue;
            }
            contributing++;
            if (bid != NormalizedQuote.NO_PRICE && bid > bestBid)
            {
                bestBid = bid;
            }
            if (ask != NormalizedQuote.NO_PRICE && ask < bestAsk)
            {
                bestAsk = ask;
            }
        }

        if (contributing < config.minVenues() || bestBid == Long.MIN_VALUE || bestAsk == Long.MAX_VALUE)
        {
            if (available[s])
            {
                available[s] = false;
                stats.unavailable.incrementAndGet();
            }
            cancelPending(s);
            return;
        }
        if (bestBid > bestAsk)
        {
            stats.crossed.incrementAndGet();
            cancelPending(s); // keep the last reference rather than publish a mid of a crossed market
            return;
        }
        if (bestBid == bestAsk)
        {
            stats.locked.incrementAndGet();
        }
        available[s] = true;

        final long ref = midToTick(bestBid, bestAsk, tick[s]);
        if (hasSubmitted[s] && Math.abs(ref - lastRef[s]) < config.minChangeTicks() * tick[s])
        {
            stats.suppressedMinChange.incrementAndGet();
            cancelPending(s);
            return;
        }
        if (pending[s])
        {
            stats.superseded.incrementAndGet();
        }
        else
        {
            pending[s] = true;
            pendingList[pendingCount++] = s;
            pendingTriggerNs[s] = dirtySinceRxNs[s] != Long.MIN_VALUE ? dirtySinceRxNs[s] : now;
        }
        dirtySinceRxNs[s] = Long.MIN_VALUE;
        pendingRef[s] = ref;
        pendingBid[s] = bestBid;
        pendingAsk[s] = bestAsk;
        pendingVenues[s] = contributing;
    }

    private int submitPending(final long now)
    {
        if (pendingCount == 0)
        {
            return 0;
        }
        Arrays.fill(shardRefused, false);
        int submitted = 0;
        int kept = 0;
        for (int k = 0; k < pendingCount; k++)
        {
            final int s = pendingList[k];
            if (!pending[s])
            {
                continue; // cancelled
            }
            final int shard = shardOf[s];
            if ((hasSubmitted[s] && now - lastSubmitNs[s] < config.minIntervalNs()) || shardRefused[shard])
            {
                pendingList[kept++] = s;
                continue;
            }
            final long sourceTs = hasSubmitted[s] ?
                Math.max(venueTimeWatermarkNs, lastSourceTsNs[s] + 1) : venueTimeWatermarkNs;
            encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
                .nbbBid(pendingBid[s])
                .nbbOffer(pendingAsk[s])
                .referencePrice(pendingRef[s])
                .sourceTsNs(sourceTs)
                .symbolId(symbolIds[s])
                .sourceId(config.sourceId())
                .venueCount((short)pendingVenues[s]);
            final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
            if (clients[shard].offer(encodeBuffer, 0, length))
            {
                pending[s] = false;
                hasSubmitted[s] = true;
                lastRef[s] = pendingRef[s];
                lastSubmitNs[s] = now;
                lastSourceTsNs[s] = sourceTs;
                stats.submitted.incrementAndGet();
                stats.quoteToCommand.record(clock.nanoTime() - pendingTriggerNs[s]);
                submitted++;
            }
            else
            {
                stats.backPressure.incrementAndGet();
                shardRefused[shard] = true;
                pendingList[kept++] = s;
            }
        }
        pendingCount = kept;
        return submitted;
    }

    // ---------------------------------------------------------------- helpers

    /** Midpoint of bid and ask rounded half up to a multiple of {@code tick}. */
    static long midToTick(final long bid, final long ask, final long tick)
    {
        return Math.floorDiv(bid + ask + tick, 2 * tick) * tick;
    }

    private void cancelPending(final int s)
    {
        if (pending[s])
        {
            pending[s] = false; // removed from pendingList lazily by submitPending
            stats.superseded.incrementAndGet();
        }
        dirtySinceRxNs[s] = Long.MIN_VALUE;
    }

    private void advanceVenueTime(final int v, final long t)
    {
        if (t > venueAsOfNs[v])
        {
            venueAsOfNs[v] = t;
        }
        if (t > venueTimeWatermarkNs)
        {
            venueTimeWatermarkNs = t;
        }
    }

    private void markDirty(final int s, final long rxNs)
    {
        if (!dirty[s])
        {
            dirty[s] = true;
            dirtyList[dirtyCount++] = s;
        }
        if (dirtySinceRxNs[s] == Long.MIN_VALUE || rxNs < dirtySinceRxNs[s])
        {
            dirtySinceRxNs[s] = rxNs;
        }
    }

    private void markAllDirty()
    {
        for (int s = 0; s < symbolCount; s++)
        {
            if (!dirty[s])
            {
                dirty[s] = true;
                dirtyList[dirtyCount++] = s;
            }
        }
    }

    // ---------------------------------------------------------------- inspection (tests, duty thread only)

    /** Last reference price submitted for the symbol, or {@link NormalizedQuote#NO_PRICE}. */
    long lastSubmittedReference(final int symbolId)
    {
        final int s = indexBySymbolId.get(symbolId);
        return s >= 0 && hasSubmitted[s] ? lastRef[s] : NormalizedQuote.NO_PRICE;
    }

    boolean isVenueFresh(final int venueId)
    {
        return venueFresh[venueId];
    }
}
