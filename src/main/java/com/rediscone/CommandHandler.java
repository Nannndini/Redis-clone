package com.rediscone;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Command dispatch table for the Redis clone server.
 * Routes parsed RESP commands to their handler implementations.
 */
public class CommandHandler {

    /**
     * Functional interface for command executors.
     */
    @FunctionalInterface
    public interface CommandExecutor {
        byte[] execute(List<String> args, ClientSession session);
    }

    private final Map<String, CommandExecutor> commands = new HashMap<>();
    private final DataStore dataStore;

    public CommandHandler(DataStore dataStore) {
        this.dataStore = dataStore;
        registerCoreCommands();
    }

    /**
     * Dispatch a parsed command to the appropriate handler.
     * @param command List of string arguments (command name + args)
     * @param session The client session issuing the command
     * @return RESP-encoded response bytes
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

        return executor.execute(command, session);
    }

    /**
     * Register a command handler.
     */
    protected void registerCommand(String name, CommandExecutor executor) {
        commands.put(name.toUpperCase(), executor);
    }

    /**
     * Register the core built-in commands.
     */
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

            // Parse optional PX/EX arguments
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
    }
}
