package com.rediscone;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * InputStream-based RESP parser.
 * Parses RESP arrays of bulk strings (the format Redis clients use to send commands).
 * Uses byte-level reading to correctly handle binary data in bulk strings.
 */
public class RespParser {

    private final BufferedInputStream in;

    public RespParser(InputStream inputStream) {
        this.in = (inputStream instanceof BufferedInputStream)
                ? (BufferedInputStream) inputStream
                : new BufferedInputStream(inputStream);
    }

    /**
     * Parse a single RESP command (array of bulk strings) from the stream.
     * @return List of string arguments, or null if the stream is closed.
     */
    public List<String> parseCommand() throws IOException {
        String line = readLine();
        if (line == null) {
            return null;
        }

        if (line.isEmpty() || line.charAt(0) != '*') {
            throw new IOException("Expected RESP array (*), got: " + line);
        }

        int numElements = Integer.parseInt(line.substring(1));
        List<String> command = new ArrayList<>(numElements);

        for (int i = 0; i < numElements; i++) {
            String bulkHeader = readLine();
            if (bulkHeader == null) {
                return null;
            }
            if (bulkHeader.charAt(0) != '$') {
                throw new IOException("Expected bulk string ($), got: " + bulkHeader);
            }

            int length = Integer.parseInt(bulkHeader.substring(1));
            byte[] data = new byte[length];
            int totalRead = 0;
            while (totalRead < length) {
                int bytesRead = in.read(data, totalRead, length - totalRead);
                if (bytesRead == -1) {
                    return null;
                }
                totalRead += bytesRead;
            }

            // Consume trailing \r\n
            int cr = in.read();
            int lf = in.read();
            if (cr != '\r' || lf != '\n') {
                throw new IOException("Expected CRLF after bulk string data");
            }

            command.add(new String(data, StandardCharsets.UTF_8));
        }

        return command;
    }

    /**
     * Read a line terminated by \r\n from the input stream.
     * Returns the line content WITHOUT the trailing \r\n.
     * Returns null if end-of-stream is reached before any data.
     */
    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int prev = -1;
        while (true) {
            int b = in.read();
            if (b == -1) {
                return sb.length() == 0 ? null : sb.toString();
            }
            if (prev == '\r' && b == '\n') {
                // Remove the trailing \r we already appended
                sb.setLength(sb.length() - 1);
                return sb.toString();
            }
            sb.append((char) b);
            prev = b;
        }
    }
}
