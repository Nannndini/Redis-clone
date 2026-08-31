package com.rediscone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Command dispatch table for the Redis clone server.
 * Routes parsed RESP commands to their handler implementations.
 * Handles write propagation to replicas and replication offset tracking.
 */
public class CommandHandler {

    @FunctionalInterface
    public interface CommandExecutor {
        byte[] execute(List<String> args, ClientSession session);
    }

    private final Map<String, CommandExecutor> commands = new HashMap<>();
    private final DataStore dataStore;
    private final ServerConfig config;

    // Replication state (master side)
    private final List<ClientSession> replicas = new CopyOnWriteArrayList<>();
    private final String masterReplId;
    private long masterReplOffset = 0;

    // Write commands that get propagated to replicas
    private static final Set<String> WRITE_COMMANDS = Set.of("SET", "DEL", "EXPIRE", "PEXPIRE");

    public CommandHandler(DataStore dataStore, ServerConfig config) {
        this.dataStore = dataStore;
        this.config = config;
        this.masterReplId = generateReplId();
        registerCoreCommands();
        registerReplicationCommands();
    }

    /**
     * Dispatch a parsed command to the appropriate handler.
     */
    public byte[] dispatch(List<String> command, ClientSession session) {
        if (command == null || command.isEmpty()) {
            return RespEncoder.error("empty command");
        }

        String name = command.get(0).toUpperCase();
        CommandExecutor executor = commands.get(name);

        if (executor == null) {
            return RespEncoder.error("unknown command '" + command.get(0) + "'");
        }

        byte[] response = executor.execute(command, session);

        // Propagate write commands to replicas (master side only)
        if (!config.isReplica() && WRITE_COMMANDS.contains(name) && !replicas.isEmpty()) {
            propagateToReplicas(command);
        }

        return response;
    }

    protected void registerCommand(String name, CommandExecutor executor) {
        commands.put(name.toUpperCase(), executor);
    }

    // ── Replication helpers ─────────────────────────────────────────────

    /**
     * Propagate a write command to all connected replicas.
     */
    private void propagateToReplicas(List<String> command) {
        byte[] encoded = RespEncoder.command(command.toArray(new String[0]));
        masterReplOffset += encoded.length;

        for (ClientSession replica : replicas) {
            try {
                replica.getChannel().write(ByteBuffer.wrap(encoded));
            } catch (IOException e) {
                System.err.println("Failed to propagate to replica: " + e.getMessage());
                replicas.remove(replica);
            }
        }
    }

    /**
     * Generate a random 40-character hex replication ID.
     */
    private String generateReplId() {
        StringBuilder sb = new StringBuilder(40);
        Random rand = new Random();
        for (int i = 0; i < 40; i++) {
            sb.append(Integer.toHexString(rand.nextInt(16)));
        }
        return sb.toString();
    }

    /**
     * Standard 88-byte empty RDB file for FULLRESYNC transfer.
     */
    private byte[] getEmptyRdb() {
        return hexToBytes("524544495330303131fa0972656469732d76657205372e322e30fa0a"
                + "72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656d"
                + "c2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
    }

    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    public long getMasterReplOffset() {
        return masterReplOffset;
    }

    public List<ClientSession> getReplicas() {
        return replicas;
    }

    // ── Command registrations ───────────────────────────────────────────

    private void registerCoreCommands() {
        // PING [message]
        registerCommand("PING", (args, session) -> {
            if (args.size() >= 2) {
                return RespEncoder.bulkString(args.get(1));
            }
            return RespEncoder.simpleString("PONG");
        });

        // ECHO <message>
        registerCommand("ECHO", (args, session) -> {
            if (args.size() < 2) {
                return RespEncoder.error("wrong number of arguments for 'echo' command");
            }
            return RespEncoder.bulkString(args.get(1));
        });

        // SET key value [PX ms] [EX seconds]
        registerCommand("SET", (args, session) -> {
            if (args.size() < 3) {
                return RespEncoder.error("wrong number of arguments for 'set' command");
            }
            String key = args.get(1);
            String value = args.get(2);
            long expiryMs = -1;

            for (int i = 3; i < args.size() - 1; i++) {
                String option = args.get(i).toUpperCase();
                String optionValue = args.get(i + 1);
                if ("PX".equals(option)) {
                    expiryMs = Long.parseLong(optionValue);
                    i++;
                } else if ("EX".equals(option)) {
                    expiryMs = Long.parseLong(optionValue) * 1000;
                    i++;
                }
            }

            dataStore.set(key, value, expiryMs);
            return RespEncoder.simpleString("OK");
        });

        // GET key
        registerCommand("GET", (args, session) -> {
            if (args.size() < 2) {
                return RespEncoder.error("wrong number of arguments for 'get' command");
            }
            String value = dataStore.get(args.get(1));
            if (value == null) {
                return RespEncoder.nullBulkString();
            }
            return RespEncoder.bulkString(value);
        });

        // CONFIG GET <parameter>
        registerCommand("CONFIG", (args, session) -> {
            if (args.size() < 3) {
                return RespEncoder.error("wrong number of arguments for 'config' command");
            }
            String subCommand = args.get(1).toUpperCase();
            if ("GET".equals(subCommand)) {
                String param = args.get(2).toLowerCase();
                switch (param) {
                    case "dir":
                        return RespEncoder.array(List.of("dir",
                                config.getDir() != null ? config.getDir() : ""));
                    case "dbfilename":
                        return RespEncoder.array(List.of("dbfilename",
                                config.getDbFilename() != null ? config.getDbFilename() : ""));
                    default:
                        return RespEncoder.emptyArray();
                }
            }
            return RespEncoder.error("unsupported CONFIG subcommand '" + args.get(1) + "'");
        });

        // KEYS <pattern>
        registerCommand("KEYS", (args, session) -> {
            if (args.size() < 2) {
                return RespEncoder.error("wrong number of arguments for 'keys' command");
            }
            String pattern = args.get(1);
            Set<String> keys = dataStore.keys();
            if ("*".equals(pattern)) {
                return RespEncoder.array(new ArrayList<>(keys));
            }
            List<String> matched = new ArrayList<>();
            String regex = pattern.replace("*", ".*");
            for (String key : keys) {
                if (key.matches(regex)) {
                    matched.add(key);
                }
            }
            return RespEncoder.array(matched);
        });
    }

    private void registerReplicationCommands() {
        // INFO [section]
        registerCommand("INFO", (args, session) -> {
            String section = args.size() >= 2 ? args.get(1).toLowerCase() : "all";

            if ("replication".equals(section) || "all".equals(section)) {
                StringBuilder info = new StringBuilder();
                info.append("# Replication\r\n");
                info.append("role:").append(config.isReplica() ? "slave" : "master").append("\r\n");
                info.append("master_replid:").append(masterReplId).append("\r\n");
                info.append("master_repl_offset:").append(masterReplOffset).append("\r\n");
                info.append("connected_slaves:").append(replicas.size()).append("\r\n");
                return RespEncoder.bulkString(info.toString());
            }
            return RespEncoder.bulkString("");
        });

        // REPLCONF — master-side handling of replica configuration
        registerCommand("REPLCONF", (args, session) -> {
            if (args.size() >= 3 && "ACK".equalsIgnoreCase(args.get(1))) {
                // Replica is acknowledging its offset
                if (session != null) {
                    long ackOffset = Long.parseLong(args.get(2));
                    session.setAcknowledgedOffset(ackOffset);
                }
                // No response needed for ACK from replica
                return new byte[0];
            }
            // For listening-port, capa, etc. — just acknowledge
            return RespEncoder.simpleString("OK");
        });

        // PSYNC <replid> <offset> — master-side full resync
        registerCommand("PSYNC", (args, session) -> {
            // Send FULLRESYNC response
            byte[] fullresync = RespEncoder.simpleString(
                    "FULLRESYNC " + masterReplId + " " + masterReplOffset);

            // Send empty RDB file as bulk string (no trailing \r\n)
            byte[] rdb = getEmptyRdb();
            byte[] rdbHeader = ("$" + rdb.length + "\r\n").getBytes(StandardCharsets.UTF_8);

            // Combine all parts
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.writeBytes(fullresync);
            out.writeBytes(rdbHeader);
            out.writeBytes(rdb);

            // Register this session as a replica
            if (session != null) {
                session.setReplica(true);
                replicas.add(session);
            }

            return out.toByteArray();
        });

        // WAIT <numreplicas> <timeout>
        registerCommand("WAIT", (args, session) -> {
            if (args.size() < 3) {
                return RespEncoder.error("wrong number of arguments for 'wait' command");
            }

            int numReplicas = Integer.parseInt(args.get(1));
            long timeoutMs = Long.parseLong(args.get(2));

            // If no writes have been propagated, return connected replica count immediately
            if (masterReplOffset == 0) {
                return RespEncoder.integer(replicas.size());
            }

            // Send REPLCONF GETACK * to all replicas
            byte[] getack = RespEncoder.command("REPLCONF", "GETACK", "*");
            long getackBytes = getack.length;

            for (ClientSession replica : replicas) {
                try {
                    replica.getChannel().write(ByteBuffer.wrap(getack));
                } catch (IOException e) {
                    replicas.remove(replica);
                }
            }

            // Wait for ACKs (blocking in the event loop — simplified implementation)
            long deadline = System.currentTimeMillis() + timeoutMs;
            int ackedCount = 0;

            while (System.currentTimeMillis() < deadline) {
                ackedCount = 0;
                for (ClientSession replica : replicas) {
                    if (replica.getAcknowledgedOffset() >= masterReplOffset) {
                        ackedCount++;
                    }
                }
                if (ackedCount >= numReplicas) {
                    break;
                }
                try {
                    Thread.sleep(10); // Small polling interval
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // Final count
            ackedCount = 0;
            for (ClientSession replica : replicas) {
                if (replica.getAcknowledgedOffset() >= masterReplOffset) {
                    ackedCount++;
                }
            }

            // Update offset to account for GETACK command sent
            masterReplOffset += getackBytes;

            return RespEncoder.integer(ackedCount);
        });
    }
}
