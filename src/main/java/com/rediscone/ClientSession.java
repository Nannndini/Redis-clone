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

    public ClientSession(SocketChannel channel) {
        this.channel = channel;
        this.decoder = new RespDecoder();
        this.readBuffer = ByteBuffer.allocate(4096);
        this.writeQueue = new LinkedList<>();
    }

    public SocketChannel getChannel() {
        return channel;
    }

    /**
     * Read available data from the channel into the read buffer,
     * then feed it to the RESP decoder.
     * @return number of bytes read, or -1 if the client disconnected.
     */
    public int read() throws IOException {
        readBuffer.clear();
        int bytesRead = channel.read(readBuffer);
        if (bytesRead > 0) {
            readBuffer.flip();
            decoder.feed(readBuffer);
        }
        return bytesRead;
    }

    /**
     * Try to decode the next complete RESP command from buffered data.
     * @return parsed command, or null if more data is needed.
     */
    public List<String> getNextCommand() {
        return decoder.decode();
    }

    /**
     * Queue a response to be written back to the client.
     */
    public void queueResponse(byte[] data) {
        writeQueue.add(ByteBuffer.wrap(data));
    }

    /**
     * Write queued data to the channel.
     * @return true if all queued data has been written.
     */
    public boolean doWrite() throws IOException {
        while (!writeQueue.isEmpty()) {
            ByteBuffer buf = writeQueue.peek();
            channel.write(buf);
            if (buf.hasRemaining()) {
                // Socket buffer is full — try again later
                return false;
            }
            writeQueue.poll();
        }
        return true;
    }

    /**
     * Check if there is data waiting to be written.
     */
    public boolean hasDataToWrite() {
        return !writeQueue.isEmpty();
    }
}
