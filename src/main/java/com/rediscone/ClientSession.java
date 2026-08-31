package com.rediscone;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Per-client connection state, attached to a SelectionKey in the NIO event loop.
 * Manages read buffering, RESP decoding, write queuing, and transaction state
 * for a single client.
 */
public class ClientSession {

    private final SocketChannel channel;
    private final RespDecoder decoder;
    private final ByteBuffer readBuffer;
    private final Queue<ByteBuffer> writeQueue;

    // Replication state
    private boolean isReplica = false;
    private volatile long acknowledgedOffset = 0;

    // NIO key for triggering OP_WRITE from deferred responses (e.g. WAIT)
    private SelectionKey selectionKey;

    // Transaction state (MULTI/EXEC/DISCARD)
    private boolean inTransaction = false;
    private List<List<String>> transactionQueue = new ArrayList<>();

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

    // ── SelectionKey accessor ────────────────────────────────────────

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public void setSelectionKey(SelectionKey key) {
        this.selectionKey = key;
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

    // ── Transaction methods (MULTI/EXEC/DISCARD) ────────────────────

    public boolean isInTransaction() {
        return inTransaction;
    }

    /**
     * Begin a transaction. Subsequent commands will be queued.
     */
    public void startTransaction() {
        inTransaction = true;
        transactionQueue.clear();
    }

    /**
     * Queue a command for deferred execution within a transaction.
     */
    public void queueCommand(List<String> command) {
        transactionQueue.add(command);
    }

    /**
     * Execute a transaction: return all queued commands and reset state.
     */
    public List<List<String>> executeTransaction() {
        List<List<String>> commands = new ArrayList<>(transactionQueue);
        transactionQueue.clear();
        inTransaction = false;
        return commands;
    }

    /**
     * Discard a transaction: clear the queue and exit transaction mode.
     */
    public void discardTransaction() {
        transactionQueue.clear();
        inTransaction = false;
    }
}
