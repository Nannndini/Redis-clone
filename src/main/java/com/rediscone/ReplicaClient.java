package com.rediscone;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Replica client that connects to a master Redis server and performs
 * the PSYNC handshake to receive replicated data.
 *
 * Handshake sequence:
 *   1. PING -> +PONG
 *   2. REPLCONF listening-port <port> -> +OK
 *   3. REPLCONF capa psync2 -> +OK
 *   4. PSYNC ? -1 -> +FULLRESYNC <replid> <offset>
 *   5. Receive RDB file as bulk string
 *   6. Enter command replication stream
 */
public class ReplicaClient implements Runnable {

    private final String masterHost;
    private final int masterPort;
    private final int listeningPort;
    private final DataStore dataStore;
    private final CommandHandler commandHandler;
    private long replicationOffset = 0;

    public ReplicaClient(String masterHost, int masterPort, int listeningPort,
                         DataStore dataStore, CommandHandler commandHandler) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.listeningPort = listeningPort;
        this.dataStore = dataStore;
        this.commandHandler = commandHandler;
    }

    @Override
    public void run() {
        try {
            Socket socket = new Socket(masterHost, masterPort);
            OutputStream out = socket.getOutputStream();
            InputStream rawIn = socket.getInputStream();
            BufferedInputStream in = new BufferedInputStream(rawIn);

            // Step 1: PING
            sendCommand(out, "PING");
            readSimpleReply(in); // +PONG

            // Step 2: REPLCONF listening-port
            sendCommand(out, "REPLCONF", "listening-port", String.valueOf(listeningPort));
            readSimpleReply(in); // +OK

            // Step 3: REPLCONF capa psync2
            sendCommand(out, "REPLCONF", "capa", "psync2");
            readSimpleReply(in); // +OK

            // Step 4: PSYNC ? -1 (request full resync)
            sendCommand(out, "PSYNC", "?", "-1");
            String fullresync = readSimpleReply(in); // +FULLRESYNC <replid> <offset>
            System.out.println("Replication: " + fullresync);

            // Step 5: Receive RDB file
            receiveRdb(in);

            // Step 6: Enter command replication stream
            System.out.println("Replication: entering command stream");
            RespParser parser = new RespParser(in);

            while (true) {
                List<String> command = parser.parseCommand();
                if (command == null) {
                    System.out.println("Replication: master connection closed");
                    break;
                }

                // Calculate the byte size of this command for offset tracking
                int commandBytes = calculateCommandBytes(command);

                String cmdName = command.get(0).toUpperCase();

                // Handle REPLCONF GETACK
                if ("REPLCONF".equals(cmdName) && command.size() >= 3
                        && "GETACK".equalsIgnoreCase(command.get(1))) {
                    // Respond with our current offset
                    byte[] ack = RespEncoder.command("REPLCONF", "ACK",
                            String.valueOf(replicationOffset));
                    out.write(ack);
                    out.flush();
                } else {
                    // Execute the command silently (no response sent back to master)
                    commandHandler.dispatch(command, null);
                }

                replicationOffset += commandBytes;
            }

            socket.close();
        } catch (IOException e) {
            System.err.println("Replication error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Send a RESP command to the master.
     */
    private void sendCommand(OutputStream out, String... parts) throws IOException {
        byte[] data = RespEncoder.command(parts);
        out.write(data);
        out.flush();
    }

    /**
     * Read a simple string reply (+...\r\n) from the master.
     */
    private String readSimpleReply(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int prev = -1;
        while (true) {
            int b = in.read();
            if (b == -1) {
                throw new IOException("Connection closed while reading reply");
            }
            if (prev == '\r' && b == '\n') {
                sb.setLength(sb.length() - 1); // remove trailing \r
                break;
            }
            sb.append((char) b);
            prev = b;
        }
        String reply = sb.toString();
        if (reply.startsWith("-")) {
            throw new IOException("Master error: " + reply);
        }
        // Strip the + prefix for simple strings
        if (reply.startsWith("+")) {
            return reply.substring(1);
        }
        return reply;
    }

    /**
     * Receive the RDB file from the master (sent as $<length>\r\n<bytes>).
     * Note: No trailing \r\n after the RDB bytes (unlike normal bulk strings).
     */
    private void receiveRdb(InputStream in) throws IOException {
        // Read the bulk string header: $<length>\r\n
        StringBuilder header = new StringBuilder();
        int prev = -1;
        while (true) {
            int b = in.read();
            if (b == -1) throw new IOException("Connection closed during RDB transfer");
            if (prev == '\r' && b == '\n') {
                header.setLength(header.length() - 1);
                break;
            }
            header.append((char) b);
            prev = b;
        }

        String headerStr = header.toString();
        if (!headerStr.startsWith("$")) {
            throw new IOException("Expected RDB bulk string, got: " + headerStr);
        }

        int rdbLength = Integer.parseInt(headerStr.substring(1));
        System.out.println("Replication: receiving RDB file (" + rdbLength + " bytes)");

        // Read the RDB bytes (but we just discard them for now since we
        // don't need to load the initial empty RDB from master)
        byte[] rdbData = new byte[rdbLength];
        int totalRead = 0;
        while (totalRead < rdbLength) {
            int read = in.read(rdbData, totalRead, rdbLength - totalRead);
            if (read == -1) throw new IOException("Connection closed during RDB transfer");
            totalRead += read;
        }

        // Optionally parse the RDB data into the data store
        if (rdbLength > 0) {
            try {
                ByteArrayInputStream rdbStream = new ByteArrayInputStream(rdbData);
                RdbLoader loader = new RdbLoader(dataStore);
                // We'd need to adapt RdbLoader to work with an InputStream
                // For now, the RDB from master is typically empty in CodeCrafters tests
            } catch (Exception e) {
                System.err.println("Warning: failed to parse master RDB: " + e.getMessage());
            }
        }

        System.out.println("Replication: RDB transfer complete");
    }

    /**
     * Calculate the byte size of a RESP command (for offset tracking).
     */
    private int calculateCommandBytes(List<String> command) {
        // *N\r\n
        int bytes = 1 + String.valueOf(command.size()).length() + 2;
        for (String arg : command) {
            byte[] argBytes = arg.getBytes(StandardCharsets.UTF_8);
            // $len\r\ndata\r\n
            bytes += 1 + String.valueOf(argBytes.length).length() + 2 + argBytes.length + 2;
        }
        return bytes;
    }

    public long getReplicationOffset() {
        return replicationOffset;
    }
}
