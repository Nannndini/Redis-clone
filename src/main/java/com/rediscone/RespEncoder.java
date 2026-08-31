package com.rediscone;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * RESP (Redis Serialization Protocol) encoder.
 * All methods return byte[] ready to be written to a client socket.
 */
public class RespEncoder {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    /** Encode a RESP Simple String: +<string>\r\n */
    public static byte[] simpleString(String s) {
        return ("+" + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Encode a RESP Bulk String: $<len>\r\n<data>\r\n */
    public static byte[] bulkString(String s) {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        byte[] header = ("$" + data.length + "\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[header.length + data.length + CRLF.length];
        System.arraycopy(header, 0, result, 0, header.length);
        System.arraycopy(data, 0, result, header.length, data.length);
        System.arraycopy(CRLF, 0, result, header.length + data.length, CRLF.length);
        return result;
    }

    /** Encode a RESP Null Bulk String: $-1\r\n */
    public static byte[] nullBulkString() {
        return "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    }

    /** Encode a RESP Integer: :<number>\r\n */
    public static byte[] integer(long n) {
        return (":" + n + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Encode a RESP Error: -ERR <message>\r\n */
    public static byte[] error(String message) {
        return ("-ERR " + message + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Encode a raw RESP error string (prefix already included): -<type> <message>\r\n */
    public static byte[] errorRaw(String fullError) {
        return ("-" + fullError + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Encode a RESP Array of Bulk Strings */
    public static byte[] array(List<String> items) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] header = ("*" + items.size() + "\r\n").getBytes(StandardCharsets.UTF_8);
        out.writeBytes(header);
        for (String item : items) {
            out.writeBytes(bulkString(item));
        }
        return out.toByteArray();
    }

    /** Encode a RESP Array from raw byte[] elements (each already RESP-encoded) */
    public static byte[] arrayOfEncoded(List<byte[]> encodedElements) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] header = ("*" + encodedElements.size() + "\r\n").getBytes(StandardCharsets.UTF_8);
        out.writeBytes(header);
        for (byte[] element : encodedElements) {
            out.writeBytes(element);
        }
        return out.toByteArray();
    }

    /** Encode an empty RESP Array: *0\r\n */
    public static byte[] emptyArray() {
        return "*0\r\n".getBytes(StandardCharsets.UTF_8);
    }

    /** Encode a command as a RESP Array of Bulk Strings (for sending commands to master/replica) */
    public static byte[] command(String... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(("*" + parts.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String part : parts) {
            out.writeBytes(bulkString(part));
        }
        return out.toByteArray();
    }
}
