package com.rediscone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
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
    private final List<PendingWait> pendingWaits = new ArrayList<>();
    private final List<PendingBlock> pendingBlocks = new ArrayList<>();

    // Write commands that get propagated to replicas
    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "DEL", "EXPIRE", "PEXPIRE", "LPUSH", "RPUSH");

    public CommandHandler(DataStore dataStore, ServerConfig config) {
        this.dataStore = dataStore;
        this.config = config;
        this.masterReplId = generateReplId();
        registerCoreCommands();
        registerReplicationCommands();
        registerTransactionCommands();
        registerListCommands();
    }

    // Transaction-aware commands that are never queued
    private static final Set<String> TRANSACTION_COMMANDS = Set.of("MULTI", "EXEC", "DISCARD");

    /**
     * Dispatch a parsed command to the appropriate handler.
     * When a session is inside a MULTI block, commands are queued instead
     * of executed (except MULTI, EXEC, and DISCARD themselves).
     */
    public byte[] dispatch(List<String> command, ClientSession session) {
        if (command == null || command.isEmpty()) {
            return RespEncoder.error("empty command");
        }

        String name = command.get(0).toUpperCase();

        // Transaction interception: queue commands inside a MULTI block
        if (session != null && session.isInTransaction() && !TRANSACTION_COMMANDS.contains(name)) {
            CommandExecutor executor = commands.get(name);
            if (executor == null) {
                // Queue even unknown commands — they'll error at EXEC time
                // (Real Redis queues them and returns ERR on EXEC for each)
                session.queueCommand(command);
                return RespEncoder.simpleString("QUEUED");
            }
            session.queueCommand(command);
            return RespEncoder.simpleString("QUEUED");
        }

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

        // WAIT <numreplicas> <timeout> — non-blocking deferred response
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

            // Capture the offset that replicas must acknowledge
            long targetOffset = masterReplOffset;

            // Send REPLCONF GETACK * to all replicas
            byte[] getack = RespEncoder.command("REPLCONF", "GETACK", "*");
            masterReplOffset += getack.length;

            for (ClientSession replica : replicas) {
                try {
                    replica.getChannel().write(ByteBuffer.wrap(getack));
                } catch (IOException e) {
                    replicas.remove(replica);
                }
            }

            // Check if enough replicas have already acknowledged
            int ackedCount = 0;
            for (ClientSession replica : replicas) {
                if (replica.getAcknowledgedOffset() >= targetOffset) {
                    ackedCount++;
                }
            }
            if (ackedCount >= numReplicas) {
                return RespEncoder.integer(ackedCount);
            }

            // Defer: register a pending wait for the event loop to resolve
            long deadline = System.currentTimeMillis() + timeoutMs;
            pendingWaits.add(new PendingWait(session, numReplicas, deadline, targetOffset));
            return new byte[0]; // No immediate response — resolved by processPendingWaits()
        });
    }

    private void registerTransactionCommands() {
        // MULTI — start a transaction
        registerCommand("MULTI", (args, session) -> {
            if (session == null) {
                return RespEncoder.error("MULTI is not allowed in this context");
            }
            if (session.isInTransaction()) {
                return RespEncoder.error("MULTI calls can not be nested");
            }
            session.startTransaction();
            return RespEncoder.simpleString("OK");
        });

        // EXEC — execute all queued commands atomically
        registerCommand("EXEC", (args, session) -> {
            if (session == null || !session.isInTransaction()) {
                return RespEncoder.error("EXEC without MULTI");
            }
            List<List<String>> queued = session.executeTransaction();
            List<byte[]> results = new ArrayList<>();
            for (List<String> cmd : queued) {
                String cmdName = cmd.get(0).toUpperCase();
                CommandExecutor exec = commands.get(cmdName);
                if (exec == null) {
                    results.add(RespEncoder.error("unknown command '" + cmd.get(0) + "'"));
                } else {
                    byte[] result = exec.execute(cmd, session);
                    results.add(result);
                    // Propagate write commands to replicas
                    if (!config.isReplica() && WRITE_COMMANDS.contains(cmdName) && !replicas.isEmpty()) {
                        propagateToReplicas(cmd);
                    }
                }
            }
            return RespEncoder.arrayOfEncoded(results);
        });

        // DISCARD — discard all queued commands
        registerCommand("DISCARD", (args, session) -> {
            if (session == null || !session.isInTransaction()) {
                return RespEncoder.error("DISCARD without MULTI");
            }
            session.discardTransaction();
            return RespEncoder.simpleString("OK");
        });
    }

    // ── Pending WAIT infrastructure ─────────────────────────────────────

    /**
     * Tracks a deferred WAIT command awaiting replica acknowledgements.
     */
    private static class PendingWait {
        final ClientSession session;
        final int targetReplicas;
        final long deadline;
        final long targetOffset;

        PendingWait(ClientSession session, int targetReplicas, long deadline, long targetOffset) {
            this.session = session;
            this.targetReplicas = targetReplicas;
            this.deadline = deadline;
            this.targetOffset = targetOffset;
        }
    }

    /**
     * Returns true if there are WAIT commands pending resolution.
     * Used by the event loop to decide on select() timeout.
     */
    public boolean hasPendingWaits() {
        return !pendingWaits.isEmpty();
    }

    /**
     * Check all pending WAIT commands and resolve any that have met their
     * target replica count or exceeded their deadline. Called once per
     * event loop iteration from Main, after processing selected keys.
     */
    public void processPendingWaits() {
        if (pendingWaits.isEmpty()) {
            return;
        }

        Iterator<PendingWait> it = pendingWaits.iterator();
        while (it.hasNext()) {
            PendingWait pw = it.next();

            int ackedCount = 0;
            for (ClientSession replica : replicas) {
                if (replica.getAcknowledgedOffset() >= pw.targetOffset) {
                    ackedCount++;
                }
            }

            if (ackedCount >= pw.targetReplicas || System.currentTimeMillis() >= pw.deadline) {
                // Resolve: queue the integer response on the waiting client's session
                byte[] response = RespEncoder.integer(ackedCount);
                pw.session.queueResponse(response);

                // Signal the selector to flush the queued response
                SelectionKey key = pw.session.getSelectionKey();
                if (key != null && key.isValid()) {
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                }

                it.remove();
            }
        }
    }

    // ── Pending BLPOP infrastructure ────────────────────────────────────

    /**
     * Tracks a deferred BLPOP command waiting for data on one or more keys.
     */
    private static class PendingBlock {
        final ClientSession session;
        final List<String> keys;
        final long deadline; // epoch millis, 0 = wait forever

        PendingBlock(ClientSession session, List<String> keys, long deadline) {
            this.session = session;
            this.keys = keys;
            this.deadline = deadline;
        }
    }

    /**
     * Returns true if there are BLPOP commands pending resolution.
     * Used by the event loop to decide on select() timeout.
     */
    public boolean hasPendingBlocks() {
        return !pendingBlocks.isEmpty();
    }

    /**
     * Check all pending BLPOP commands. Resolve any whose key now has data,
     * or whose deadline has passed (respond with null bulk string array).
     * Called once per event loop iteration from Main.
     */
    public void processPendingBlocks() {
        if (pendingBlocks.isEmpty()) {
            return;
        }

        Iterator<PendingBlock> it = pendingBlocks.iterator();
        while (it.hasNext()) {
            PendingBlock pb = it.next();

            // Try each key in order — first non-empty key wins (Redis semantics)
            boolean resolved = false;
            for (String key : pb.keys) {
                if (dataStore.listExists(key)) {
                    String value = dataStore.lpop(key);
                    if (value != null) {
                        // BLPOP response is a 2-element array: [key, value]
                        byte[] response = RespEncoder.array(List.of(key, value));
                        pb.session.queueResponse(response);

                        SelectionKey selKey = pb.session.getSelectionKey();
                        if (selKey != null && selKey.isValid()) {
                            selKey.interestOps(selKey.interestOps() | SelectionKey.OP_WRITE);
                        }

                        it.remove();
                        resolved = true;
                        break;
                    }
                }
            }

            if (!resolved && pb.deadline > 0 && System.currentTimeMillis() >= pb.deadline) {
                // Timeout — respond with null array
                pb.session.queueResponse(RespEncoder.nullBulkString());

                SelectionKey selKey = pb.session.getSelectionKey();
                if (selKey != null && selKey.isValid()) {
                    selKey.interestOps(selKey.interestOps() | SelectionKey.OP_WRITE);
                }

                it.remove();
            }
        }
    }

    /**
     * Called by LPUSH/RPUSH handlers to immediately wake up any BLPOP
     * clients waiting on the given key, without waiting for the next
     * processPendingBlocks() tick.
     */
    private void resolveBlockedClientsForKey(String key) {
        Iterator<PendingBlock> it = pendingBlocks.iterator();
        while (it.hasNext()) {
            PendingBlock pb = it.next();
            if (pb.keys.contains(key) && dataStore.listExists(key)) {
                String value = dataStore.lpop(key);
                if (value != null) {
                    byte[] response = RespEncoder.array(List.of(key, value));
                    pb.session.queueResponse(response);

                    SelectionKey selKey = pb.session.getSelectionKey();
                    if (selKey != null && selKey.isValid()) {
                        selKey.interestOps(selKey.interestOps() | SelectionKey.OP_WRITE);
                    }

                    it.remove();
                    break; // Only wake the first waiting client (FIFO)
                }
            }
        }
    }

    // ── List commands ───────────────────────────────────────────────────

    private void registerListCommands() {
        // LPUSH key value [value ...]
        registerCommand("LPUSH", (args, session) -> {
            if (args.size() < 3) {
                return RespEncoder.error("wrong number of arguments for 'lpush' command");
            }
            String key = args.get(1);
            String[] values = args.subList(2, args.size()).toArray(new String[0]);
            int newLen = dataStore.lpush(key, values);

            // Wake up any BLPOP clients waiting on this key
            resolveBlockedClientsForKey(key);

            return RespEncoder.integer(newLen);
        });

        // RPUSH key value [value ...]
        registerCommand("RPUSH", (args, session) -> {
            if (args.size() < 3) {
                return RespEncoder.error("wrong number of arguments for 'rpush' command");
            }
            String key = args.get(1);
            String[] values = args.subList(2, args.size()).toArray(new String[0]);
            int newLen = dataStore.rpush(key, values);

            // Wake up any BLPOP clients waiting on this key
            resolveBlockedClientsForKey(key);

            return RespEncoder.integer(newLen);
        });

        // LPOP key
        registerCommand("LPOP", (args, session) -> {
            if (args.size() < 2) {
                return RespEncoder.error("wrong number of arguments for 'lpop' command");
            }
            String value = dataStore.lpop(args.get(1));
            if (value == null) {
                return RespEncoder.nullBulkString();
            }
            return RespEncoder.bulkString(value);
        });

        // LRANGE key start stop
        registerCommand("LRANGE", (args, session) -> {
            if (args.size() < 4) {
                return RespEncoder.error("wrong number of arguments for 'lrange' command");
            }
            String key = args.get(1);
            int start = Integer.parseInt(args.get(2));
            int stop = Integer.parseInt(args.get(3));
            List<String> elements = dataStore.lrange(key, start, stop);
            return RespEncoder.array(elements);
        });

        // LLEN key
        registerCommand("LLEN", (args, session) -> {
            if (args.size() < 2) {
                return RespEncoder.error("wrong number of arguments for 'llen' command");
            }
            return RespEncoder.integer(dataStore.llen(args.get(1)));
        });

        // BLPOP key [key ...] timeout
        registerCommand("BLPOP", (args, session) -> {
            if (args.size() < 3) {
                return RespEncoder.error("wrong number of arguments for 'blpop' command");
            }

            // Last argument is the timeout in seconds (0 = block forever)
            double timeoutSec = Double.parseDouble(args.get(args.size() - 1));
            List<String> keys = args.subList(1, args.size() - 1);

            // Check each key immediately — if any has data, pop and return now
            for (String key : keys) {
                if (dataStore.listExists(key)) {
                    String value = dataStore.lpop(key);
                    if (value != null) {
                        return RespEncoder.array(List.of(key, value));
                    }
                }
            }

            // No data available — defer response
            long deadline = (timeoutSec == 0)
                    ? 0  // 0 = block indefinitely
                    : System.currentTimeMillis() + (long) (timeoutSec * 1000);
            pendingBlocks.add(new PendingBlock(session, new ArrayList<>(keys), deadline));
            return new byte[0]; // No immediate response — resolved by processPendingBlocks()
        });
    }
}
