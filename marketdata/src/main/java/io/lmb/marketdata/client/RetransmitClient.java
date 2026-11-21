package io.lmb.marketdata.client;

import io.lmb.marketdata.sbe.MessageHeaderDecoder;
import io.lmb.marketdata.sbe.MessageHeaderEncoder;
import io.lmb.marketdata.sbe.RetransmitRequestEncoder;
import io.lmb.marketdata.sbe.RetransmitResponseDecoder;
import io.lmb.marketdata.sbe.RetransmitStatus;
import io.lmb.marketdata.sbe.SnapshotRequestEncoder;
import io.lmb.marketdata.wire.MdWire;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Blocking client of the TCP retransmission service (one persistent connection, reconnects on demand).
 * Recovery path only: allocates. Not thread-safe.
 */
public final class RetransmitClient implements AutoCloseable
{
    /** One answer: the response fields and the frames that followed it (each one SBE message). */
    public static final class Response
    {
        public RetransmitStatus status;
        public long requestId;
        public long fromSeq;
        public long toSeq;
        public long oldestAvailableSeq;
        public long lastSeq;
        public int symbolId;
        public final List<byte[]> messages = new ArrayList<>();
    }

    private final InetSocketAddress address;
    private final int timeoutMs;
    private final UnsafeBuffer out = new UnsafeBuffer(new byte[128]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final RetransmitResponseDecoder responseDecoder = new RetransmitResponseDecoder();
    private Socket socket;
    private DataInputStream in;
    private OutputStream os;
    private long nextRequestId = 1;

    public RetransmitClient(final InetSocketAddress address, final int timeoutMs)
    {
        this.address = address;
        this.timeoutMs = timeoutMs;
    }

    public InetSocketAddress address()
    {
        return address;
    }

    /** Messages fromSeq..toSeq (inclusive) of the symbol; the server may serve fewer (see Response.toSeq). */
    public Response retransmit(final int symbolId, final long fromSeq, final long toSeq) throws IOException
    {
        final RetransmitRequestEncoder e = new RetransmitRequestEncoder()
            .wrapAndApplyHeader(out, MdWire.FRAME_PREFIX_LENGTH, headerEncoder)
            .requestId(nextRequestId++).fromSeq(fromSeq).toSeq(toSeq).symbolId(symbolId);
        return exchange(MdWire.HEADER_LENGTH + e.encodedLength());
    }

    /** The symbol's current snapshot (messages: SnapshotBegin, SnapshotLevels*, SnapshotEnd). */
    public Response snapshot(final int symbolId) throws IOException
    {
        final SnapshotRequestEncoder e = new SnapshotRequestEncoder()
            .wrapAndApplyHeader(out, MdWire.FRAME_PREFIX_LENGTH, headerEncoder)
            .requestId(nextRequestId++).symbolId(symbolId);
        return exchange(MdWire.HEADER_LENGTH + e.encodedLength());
    }

    private Response exchange(final int messageLength) throws IOException
    {
        MdWire.putFrameLength(out, 0, messageLength);
        try
        {
            connect();
            os.write(out.byteArray(), 0, MdWire.FRAME_PREFIX_LENGTH + messageLength);
            os.flush();
            final byte[] head = readFrame();
            final UnsafeBuffer hb = new UnsafeBuffer(head);
            headerDecoder.wrap(hb, 0);
            if (headerDecoder.templateId() != RetransmitResponseDecoder.TEMPLATE_ID)
            {
                throw new IOException("unexpected template " + headerDecoder.templateId());
            }
            responseDecoder.wrap(hb, MdWire.HEADER_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
            final Response r = new Response();
            r.status = responseDecoder.status();
            r.requestId = responseDecoder.requestId();
            r.fromSeq = responseDecoder.fromSeq();
            r.toSeq = responseDecoder.toSeq();
            r.oldestAvailableSeq = responseDecoder.oldestAvailableSeq();
            r.lastSeq = responseDecoder.lastSeq();
            r.symbolId = (int)responseDecoder.symbolId();
            final long count = responseDecoder.messageCount();
            for (long i = 0; i < count; i++)
            {
                r.messages.add(readFrame());
            }
            return r;
        }
        catch (final IOException e)
        {
            close();
            throw e;
        }
    }

    private byte[] readFrame() throws IOException
    {
        final int lo = in.readUnsignedByte();
        final int hi = in.readUnsignedByte();
        final byte[] msg = new byte[lo | (hi << 8)];
        in.readFully(msg);
        return msg;
    }

    private void connect() throws IOException
    {
        if (socket == null)
        {
            final Socket s = new Socket();
            s.connect(address, timeoutMs);
            s.setSoTimeout(timeoutMs);
            s.setTcpNoDelay(true);
            socket = s;
            in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
            os = s.getOutputStream();
        }
    }

    @Override
    public void close()
    {
        if (socket != null)
        {
            try
            {
                socket.close();
            }
            catch (final IOException ignore)
            {
            }
            socket = null;
        }
    }
}
