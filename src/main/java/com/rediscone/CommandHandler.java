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

    public CommandHandler() {
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
    }
}
