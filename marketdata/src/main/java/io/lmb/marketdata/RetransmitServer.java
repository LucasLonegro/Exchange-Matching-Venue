package io.lmb.marketdata;

import io.lmb.marketdata.sbe.MessageHeaderDecoder;
import io.lmb.marketdata.sbe.MessageHeaderEncoder;
import io.lmb.marketdata.sbe.RetransmitRequestDecoder;
import io.lmb.marketdata.sbe.RetransmitResponseEncoder;
import io.lmb.marketdata.sbe.RetransmitStatus;
import io.lmb.marketdata.sbe.SnapshotRequestDecoder;
import io.lmb.marketdata.wire.MdWire;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * TCP retransmission service. A client sends RetransmitRequest frames (uint16le length + SBE message);
 * each is answered with one RetransmitResponse frame followed by its messageCount sequenced message
 * frames. Connections persist; requests on one connection are answered in order.
 *
 * <p>Status rules: UNKNOWN_SYMBOL if the symbol never published; INVALID_RANGE if fromSeq < 1 or
 * toSeq < fromSeq; TOO_OLD if fromSeq is older than the oldest message held (use the snapshot channel);
 * otherwise OK with fromSeq..min(toSeq, lastSeq, fromSeq + maxPerRequest - 1), possibly empty when
 * fromSeq is beyond lastSeq. Clients re-request the remainder of a capped range.
 */
public final class RetransmitServer implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(RetransmitServer.class);

    private final RetransmitHistory history;
    private final int maxPerRequest;
    private final int maxConnections;
    private final int idleTimeoutMs;
    private final PublisherMetrics metrics;
    private final IntFunction<CompletableFuture<byte[]>> snapshots;
    private final ServerSocket serverSocket;
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private final Thread acceptor;
    private volatile boolean running = true;

    public RetransmitServer(
        final InetSocketAddress bind, final RetransmitHistory history, final int maxPerRequest, final int maxConnections,
        final int idleTimeoutMs, final PublisherMetrics metrics) throws IOException
    {
        this(bind, history, maxPerRequest, maxConnections, idleTimeoutMs, metrics, null);
    }

    /**
     * @param snapshots source of a symbol's current snapshot frames (null result = unknown symbol), usually
     *                  {@link MarketDataEngine#requestSnapshot}; null to refuse SnapshotRequests
     */
    public RetransmitServer(
        final InetSocketAddress bind, final RetransmitHistory history, final int maxPerRequest, final int maxConnections,
        final int idleTimeoutMs, final PublisherMetrics metrics, final IntFunction<CompletableFuture<byte[]>> snapshots)
        throws IOException
    {
        this.snapshots = snapshots;
        this.history = history;
        this.maxPerRequest = maxPerRequest;
        this.maxConnections = maxConnections;
        this.idleTimeoutMs = idleTimeoutMs;
        this.metrics = metrics;
        this.serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(bind, 64);
        acceptor = Thread.ofPlatform().daemon().name("md-retransmit-accept").unstarted(this::acceptLoop);
        acceptor.start();
    }

    public int port()
    {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop()
    {
        while (running)
        {
            try
            {
                final Socket socket = serverSocket.accept();
                if (connections.size() >= maxConnections)
                {
                    metrics.retransmitRejectedConnections.incrementAndGet();
                    socket.close();
                    continue;
                }
                connections.add(socket);
                Thread.ofVirtual().name("md-retransmit-conn").start(() -> serve(socket));
            }
            catch (final IOException e)
            {
                if (running)
                {
                    LOG.warn("retransmit accept failed", e);
                }
            }
        }
    }

    private void serve(final Socket socket)
    {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final RetransmitRequestDecoder request = new RetransmitRequestDecoder();
        final SnapshotRequestDecoder snapshotRequest = new SnapshotRequestDecoder();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final RetransmitResponseEncoder response = new RetransmitResponseEncoder();
        final RetransmitHistory.Range range = new RetransmitHistory.Range();
        final byte[] in = new byte[65536];
        final UnsafeBuffer inBuffer = new UnsafeBuffer(in);
        final int responseHead = MdWire.FRAME_PREFIX_LENGTH + MdWire.HEADER_LENGTH + RetransmitResponseEncoder.BLOCK_LENGTH;
        final byte[] out = new byte[responseHead + maxPerRequest * RetransmitHistory.SLOT_SIZE];
        final UnsafeBuffer outBuffer = new UnsafeBuffer(out);
        try (socket)
        {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(idleTimeoutMs);
            final InputStream is = new BufferedInputStream(socket.getInputStream());
            final OutputStream os = new BufferedOutputStream(socket.getOutputStream(), 65536);
            while (running)
            {
                readFully(is, in, 0, 2);
                final int len = inBuffer.getShort(0, MdWire.ORDER) & 0xFFFF;
                readFully(is, in, 0, len);
                if (len < MdWire.HEADER_LENGTH)
                {
                    return;
                }
                header.wrap(inBuffer, 0);
                if (header.schemaId() == MdWire.SCHEMA_ID && header.templateId() == SnapshotRequestDecoder.TEMPLATE_ID
                    && snapshots != null && len >= MdWire.HEADER_LENGTH + header.blockLength())
                {
                    snapshotRequest.wrap(inBuffer, MdWire.HEADER_LENGTH, header.blockLength(), header.version());
                    serveSnapshot(snapshotRequest.requestId(), (int)snapshotRequest.symbolId(), outBuffer, response,
                        headerEncoder, os);
                    continue;
                }
                if (header.schemaId() != MdWire.SCHEMA_ID || header.templateId() != RetransmitRequestDecoder.TEMPLATE_ID
                    || len < MdWire.HEADER_LENGTH + header.blockLength())
                {
                    LOG.warn("unexpected message on retransmit connection {}: template {}", socket.getRemoteSocketAddress(),
                        header.templateId());
                    return;
                }
                request.wrap(inBuffer, MdWire.HEADER_LENGTH, header.blockLength(), header.version());
                final long requestId = request.requestId();
                final long fromSeq = request.fromSeq();
                final long toSeq = request.toSeq();
                final int symbolId = (int)request.symbolId();

                RetransmitStatus status = RetransmitStatus.OK;
                range.count = 0;
                range.bytes = 0;
                range.oldestSeq = 0;
                range.lastSeq = 0;
                range.fromSeq = fromSeq;
                range.toSeq = fromSeq - 1;
                final RetransmitHistory.SymbolHistory h = history.get(symbolId);
                if (h == null)
                {
                    status = RetransmitStatus.UNKNOWN_SYMBOL;
                    metrics.retransmitUnknown.incrementAndGet();
                }
                else if (fromSeq < 1 || toSeq < fromSeq)
                {
                    status = RetransmitStatus.INVALID_RANGE;
                    metrics.retransmitInvalid.incrementAndGet();
                    range.oldestSeq = h.oldestSeq();
                    range.lastSeq = h.lastSeq();
                }
                else
                {
                    h.copyRange(fromSeq, toSeq, maxPerRequest, outBuffer, responseHead, range);
                    if (fromSeq < range.oldestSeq)
                    {
                        status = RetransmitStatus.TOO_OLD;
                        metrics.retransmitTooOld.incrementAndGet();
                    }
                    else
                    {
                        metrics.retransmitOk.incrementAndGet();
                        metrics.retransmitMessages.addAndGet(range.count);
                    }
                }
                final int count = status == RetransmitStatus.OK ? range.count : 0;
                final int bytes = status == RetransmitStatus.OK ? range.bytes : 0;
                MdWire.putFrameLength(outBuffer, 0, MdWire.HEADER_LENGTH + RetransmitResponseEncoder.BLOCK_LENGTH);
                response.wrapAndApplyHeader(outBuffer, MdWire.FRAME_PREFIX_LENGTH, headerEncoder)
                    .requestId(requestId)
                    .fromSeq(fromSeq)
                    .toSeq(count > 0 ? range.toSeq : fromSeq - 1)
                    .oldestAvailableSeq(range.oldestSeq)
                    .lastSeq(range.lastSeq)
                    .symbolId(symbolId)
                    .messageCount(count)
                    .status(status);
                os.write(out, 0, responseHead + bytes);
                os.flush();
            }
        }
        catch (final EOFException | SocketTimeoutException e)
        {
            // client went away or idle; normal
        }
        catch (final SocketException e)
        {
            if (running)
            {
                LOG.debug("retransmit connection closed: {}", e.toString());
            }
        }
        catch (final IOException e)
        {
            LOG.warn("retransmit connection failed", e);
        }
        finally
        {
            connections.remove(socket);
        }
    }

    private void serveSnapshot(
        final long requestId, final int symbolId, final UnsafeBuffer out, final RetransmitResponseEncoder response,
        final MessageHeaderEncoder headerEncoder, final OutputStream os) throws IOException
    {
        byte[] frames;
        try
        {
            frames = snapshots.apply(symbolId).get(5, TimeUnit.SECONDS);
        }
        catch (final Exception e)
        {
            throw new IOException("snapshot unavailable: " + e, e);
        }
        final RetransmitHistory.SymbolHistory h = history.get(symbolId);
        int count = 0;
        long lastSeq = 0;
        if (frames != null)
        {
            final UnsafeBuffer f = new UnsafeBuffer(frames);
            // SnapshotBegin.lastSeq is the first body field of the first frame.
            lastSeq = f.getLong(MdWire.FRAME_PREFIX_LENGTH + MdWire.HEADER_LENGTH, MdWire.ORDER);
            for (int pos = 0; pos < frames.length; pos += MdWire.FRAME_PREFIX_LENGTH + MdWire.frameLength(f, pos))
            {
                count++;
            }
            metrics.snapshotsSent.incrementAndGet();
        }
        else
        {
            metrics.retransmitUnknown.incrementAndGet();
        }
        final int head = MdWire.FRAME_PREFIX_LENGTH + MdWire.HEADER_LENGTH + RetransmitResponseEncoder.BLOCK_LENGTH;
        MdWire.putFrameLength(out, 0, MdWire.HEADER_LENGTH + RetransmitResponseEncoder.BLOCK_LENGTH);
        response.wrapAndApplyHeader(out, MdWire.FRAME_PREFIX_LENGTH, headerEncoder)
            .requestId(requestId)
            .fromSeq(lastSeq)
            .toSeq(lastSeq)
            .oldestAvailableSeq(h == null ? 0 : h.oldestSeq())
            .lastSeq(lastSeq)
            .symbolId(symbolId)
            .messageCount(count)
            .status(frames == null ? RetransmitStatus.UNKNOWN_SYMBOL : RetransmitStatus.OK);
        os.write(out.byteArray(), 0, head);
        if (frames != null)
        {
            os.write(frames);
        }
        os.flush();
    }

    private static void readFully(final InputStream is, final byte[] dst, final int offset, final int length) throws IOException
    {
        int n = 0;
        while (n < length)
        {
            final int r = is.read(dst, offset + n, length - n);
            if (r < 0)
            {
                throw new EOFException();
            }
            n += r;
        }
    }

    @Override
    public void close()
    {
        running = false;
        try
        {
            serverSocket.close();
        }
        catch (final IOException ignore)
        {
        }
        for (final Socket s : connections)
        {
            try
            {
                s.close();
            }
            catch (final IOException ignore)
            {
            }
        }
        try
        {
            acceptor.join(2000);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
