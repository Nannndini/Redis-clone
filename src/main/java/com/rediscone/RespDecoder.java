package com.rediscone;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * ByteBuffer-based incremental RESP decoder for NIO.
 * Handles partial reads: if a complete command isn't available, returns null
 * and preserves state so parsing can resume when more data arrives.
 */
public class RespDecoder {

    private final ByteBuffer buffer;

    public RespDecoder() {
        this.buffer = ByteBuffer.allocate(64 * 1024); // 64KB buffer
    }

    /**
     * Append incoming data to the internal buffer.
     */
    public void feed(ByteBuffer data) {
        buffer.put(data);
    }

    /**
     * Try to decode a complete RESP command from the buffer.
     * Returns a List of string arguments if a complete command is available,
     * or null if more data is needed.
     */
    public List<String> decode() {
        buffer.flip(); // switch to read mode

        if (buffer.remaining() == 0) {
            buffer.compact();
            return null;
        }

        int startPos = buffer.position();

        try {
            // Read array header: *N\r\n
            String arrayLine = readLine(buffer);
            if (arrayLine == null) {
                buffer.position(startPos);
                buffer.compact();
                return null;
            }

            if (arrayLine.charAt(0) != '*') {
                // Skip malformed data
                buffer.compact();
                return null;
            }

            int numElements = Integer.parseInt(arrayLine.substring(1));
            List<String> command = new ArrayList<>(numElements);

            for (int i = 0; i < numElements; i++) {
                // Read bulk string header: $N\r\n
                String bulkHeader = readLine(buffer);
                if (bulkHeader == null) {
                    // Incomplete — reset to start and wait for more data
                    buffer.position(startPos);
                    buffer.compact();
                    return null;
                }

                if (bulkHeader.charAt(0) != '$') {
                    buffer.compact();
                    return null;
                }

                int length = Integer.parseInt(bulkHeader.substring(1));

                // Check if we have enough bytes for data + \r\n
                if (buffer.remaining() < length + 2) {
                    buffer.position(startPos);
                    buffer.compact();
                    return null;
                }

                byte[] data = new byte[length];
                buffer.get(data);

                // Consume \r\n
                buffer.get(); // \r
                buffer.get(); // \n

                command.add(new String(data, StandardCharsets.UTF_8));
            }

            // Successfully parsed a complete command — compact remaining data
            buffer.compact();
            return command;

        } catch (NumberFormatException e) {
            // Malformed RESP data — discard and compact
            buffer.compact();
            return null;
        }
    }

    /**
     * Read a line (terminated by \r\n) from the buffer.
     * Returns the line content without \r\n, or null if \r\n not yet available.
     * Advances the buffer position past \r\n on success.
     */
    private String readLine(ByteBuffer buf) {
        int start = buf.position();
        int limit = buf.limit();

        for (int i = start; i < limit - 1; i++) {
            if (buf.get(i) == '\r' && buf.get(i + 1) == '\n') {
                byte[] lineBytes = new byte[i - start];
                buf.position(start);
                buf.get(lineBytes);
                buf.get(); // skip \r
                buf.get(); // skip \n
                return new String(lineBytes, StandardCharsets.UTF_8);
            }
        }

        // \r\n not found — need more data
        return null;
    }
}
