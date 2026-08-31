package com.rediscone;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Per-client connection state, attached to a SelectionKey in the NIO event loop.
 * Manages read buffering, RESP decoding, and write queuing for a single client.
 */
public class ClientSession {

    private final SocketChannel channel;
    private final RespDecoder decoder;
    private final ByteBuffer readBuffer;
    private final Queue<ByteBuffer> writeQueue;

    // Replication state
    private boolean isReplica = false;
    private volatile long acknowledgedOffset = 0;

    public ClientSession(SocketChannel channel) {
        this.channel = channel;
        this.decoder = new RespDecoder();
        this.readBuffer = ByteBuffer.allocate(4096);
        this.writeQueue = new LinkedList<>();
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public int read() throws IOException {
        readBuffer.clear();
        int bytesRead = channel.read(readBuffer);
        if (bytesRead > 0) {
            readBuffer.flip();
            decoder.feed(readBuffer);
        }
        return bytesRead;
    }

    public List<String> getNextCommand() {
        return decoder.decode();
    }

    public void queueResponse(byte[] data) {
        if (data != null && data.length > 0) {
            writeQueue.add(ByteBuffer.wrap(data));
        }
    }

    public boolean doWrite() throws IOException {
        while (!writeQueue.isEmpty()) {
            ByteBuffer buf = writeQueue.peek();
            channel.write(buf);
            if (buf.hasRemaining()) {
                return false;
            }
            writeQueue.poll();
        }
        return true;
    }

    public boolean hasDataToWrite() {
        return !writeQueue.isEmpty();
    }

    // ── Replication accessors ───────────────────────────────────────

    public boolean isReplica() {
        return isReplica;
    }

    public void setReplica(boolean replica) {
        isReplica = replica;
    }

    public long getAcknowledgedOffset() {
        return acknowledgedOffset;
    }

    public void setAcknowledgedOffset(long offset) {
        this.acknowledgedOffset = offset;
    }
}
