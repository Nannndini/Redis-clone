package com.rediscone;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * RDB (Redis Database) file parser.
 * Loads key-value pairs from an RDB file into the DataStore on startup.
 *
 * RDB file format (simplified):
 *   REDIS<version>  (magic + 4-digit version)
 *   [0xFA aux-key aux-value]*  (auxiliary metadata)
 *   [0xFE db-number [0xFB ht-size expire-ht-size] key-value-pairs]* (databases)
 *   0xFF  (EOF marker)
 *   <8-byte CRC64 checksum>
 */
public class RdbLoader {

    private final DataStore dataStore;

    public RdbLoader(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Load an RDB file into the data store.
     * @param path the path to the RDB file
     * @return true if the file was loaded successfully
     */
    public boolean load(Path path) {
        if (path == null || !Files.exists(path)) {
            return false;
        }

        try (InputStream raw = new FileInputStream(path.toFile());
             BufferedInputStream in = new BufferedInputStream(raw)) {

            // Read magic string "REDIS"
            byte[] magic = readBytes(in, 5);
            if (!"REDIS".equals(new String(magic, StandardCharsets.US_ASCII))) {
                System.err.println("RDB: invalid magic string");
                return false;
            }

            // Read version (4 bytes ASCII)
            byte[] version = readBytes(in, 4);
            System.out.println("RDB: loading version " + new String(version, StandardCharsets.US_ASCII));

            // Parse sections
            long currentExpiry = -1;
            boolean reachedEof = false;

            while (!reachedEof) {
                int opcode = in.read();
                if (opcode == -1) {
                    break;
                }

                switch (opcode) {
                    case 0xFA: // Auxiliary field
                        readString(in); // key
                        readString(in); // value
                        break;

                    case 0xFE: // Database selector
                        readLength(in); // db number — we only support db 0
                        break;

                    case 0xFB: // Hash table size info
                        readLength(in); // main hash table size
                        readLength(in); // expiry hash table size
                        break;

                    case 0xFC: // Expire time in milliseconds (8 bytes, little-endian)
                        currentExpiry = readLittleEndianLong(in, 8);
                        break;

                    case 0xFD: // Expire time in seconds (4 bytes, little-endian)
                        currentExpiry = readLittleEndianLong(in, 4) * 1000;
                        break;

                    case 0xFF: // EOF
                        reachedEof = true;
                        break;

                    default:
                        // This is a value type byte — read key and value
                        int valueType = opcode;
                        String key = readString(in);
                        String value = readStringValue(in, valueType);

                        if (value != null) {
                            dataStore.setWithAbsoluteExpiry(key, value, currentExpiry);
                        }
                        currentExpiry = -1; // Reset expiry for next entry
                        break;
                }
            }

            System.out.println("RDB: loaded " + dataStore.keys().size() + " keys");
            return true;

        } catch (IOException e) {
            System.err.println("RDB: error loading file: " + e.getMessage());
            return false;
        }
    }

    /**
     * Read a length-encoded string from the RDB stream.
     */
    private String readString(InputStream in) throws IOException {
        int[] lengthResult = readLength(in);
        int length = lengthResult[0];
        int encoding = lengthResult[1];

        if (encoding == -1) {
            // Normal string — read 'length' bytes
            byte[] data = readBytes(in, length);
            return new String(data, StandardCharsets.UTF_8);
        }

        // Special encoding: integer stored as string
        switch (encoding) {
            case 0: // 8-bit integer
                return String.valueOf(in.read());
            case 1: // 16-bit integer (little-endian)
                return String.valueOf(readLittleEndianLong(in, 2));
            case 2: // 32-bit integer (little-endian)
                return String.valueOf(readLittleEndianLong(in, 4));
            case 3: // LZF compressed — skip for now
                int compressedLen = readLength(in)[0];
                int uncompressedLen = readLength(in)[0];
                readBytes(in, compressedLen); // skip compressed data
                return "";
            default:
                throw new IOException("RDB: unknown special encoding: " + encoding);
        }
    }

    /**
     * Read a value based on its value type.
     * Only type 0 (string) is fully supported.
     */
    private String readStringValue(InputStream in, int valueType) throws IOException {
        if (valueType == 0) {
            // String encoding
            return readString(in);
        }
        // Unsupported value types — skip
        System.err.println("RDB: skipping unsupported value type " + valueType);
        return null;
    }

    /**
     * Read a length-encoded integer from the RDB stream.
     * Returns [length, specialEncoding] where specialEncoding is -1 for normal lengths.
     */
    private int[] readLength(InputStream in) throws IOException {
        int firstByte = in.read();
        if (firstByte == -1) {
            throw new IOException("RDB: unexpected end of stream in length encoding");
        }

        int type = (firstByte & 0xC0) >> 6;

        switch (type) {
            case 0: // 6-bit length
                return new int[]{firstByte & 0x3F, -1};

            case 1: // 14-bit length
                int secondByte = in.read();
                return new int[]{((firstByte & 0x3F) << 8) | secondByte, -1};

            case 2: // 32-bit length (big-endian)
                byte[] fourBytes = readBytes(in, 4);
                int length = ((fourBytes[0] & 0xFF) << 24)
                        | ((fourBytes[1] & 0xFF) << 16)
                        | ((fourBytes[2] & 0xFF) << 8)
                        | (fourBytes[3] & 0xFF);
                return new int[]{length, -1};

            case 3: // Special encoding
                int specialType = firstByte & 0x3F;
                return new int[]{0, specialType};

            default:
                throw new IOException("RDB: invalid length encoding type: " + type);
        }
    }

    /**
     * Read exactly N bytes from the stream.
     */
    private byte[] readBytes(InputStream in, int count) throws IOException {
        byte[] data = new byte[count];
        int totalRead = 0;
        while (totalRead < count) {
            int read = in.read(data, totalRead, count - totalRead);
            if (read == -1) {
                throw new IOException("RDB: unexpected end of stream (wanted "
                        + count + " bytes, got " + totalRead + ")");
            }
            totalRead += read;
        }
        return data;
    }

    /**
     * Read an N-byte little-endian unsigned integer from the stream.
     */
    private long readLittleEndianLong(InputStream in, int byteCount) throws IOException {
        byte[] bytes = readBytes(in, byteCount);
        long result = 0;
        for (int i = 0; i < byteCount; i++) {
            result |= ((long) (bytes[i] & 0xFF)) << (8 * i);
        }
        return result;
    }
}
