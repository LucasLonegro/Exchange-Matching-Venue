package io.lmb.feeds;

import io.lmb.api.CommandClient;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.ReferencePriceDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

/** Records accepted ReferencePrice commands; can refuse offers (back pressure) and report disconnected. */
final class FakeCommandClient implements CommandClient
{
    record Ref(long symbolId, long bid, long offer, long ref, long sourceTs, int sourceId, int venues)
    {
    }

    final int shard;
    final List<Ref> accepted = new ArrayList<>();
    int refuseNext;
    volatile boolean connected = true;
    int polls;

    FakeCommandClient(final int shard)
    {
        this.shard = shard;
    }

    @Override
    public int shardId()
    {
        return shard;
    }

    @Override
    public synchronized boolean offer(final DirectBuffer buffer, final int offset, final int length)
    {
        if (refuseNext > 0)
        {
            refuseNext--;
            return false;
        }
        final UnsafeBuffer copy = new UnsafeBuffer(new byte[length]);
        copy.putBytes(0, buffer, offset, length);
        final MessageHeaderDecoder header = new MessageHeaderDecoder().wrap(copy, 0);
        if (header.templateId() != ReferencePriceDecoder.TEMPLATE_ID)
        {
            throw new AssertionError("unexpected template " + header.templateId());
        }
        final ReferencePriceDecoder d = new ReferencePriceDecoder().wrapAndApplyHeader(copy, 0, header);
        accepted.add(new Ref(d.symbolId(), d.nbbBid(), d.nbbOffer(), d.referencePrice(), d.sourceTsNs(), d.sourceId(),
            d.venueCount()));
        return true;
    }

    synchronized List<Ref> snapshot()
    {
        return List.copyOf(accepted);
    }

    @Override
    public int poll()
    {
        polls++;
        return 0;
    }

    @Override
    public boolean isConnected()
    {
        return connected;
    }

    @Override
    public void close()
    {
    }
}
