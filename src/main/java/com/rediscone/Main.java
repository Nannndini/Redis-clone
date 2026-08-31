package com.rediscone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;

/**
 * Redis clone server entry point.
 * Uses a single-threaded NIO Selector event loop, mirroring Redis's actual
 * single-threaded architecture for maximum correctness and performance.
 */
public class Main {

    private static final int DEFAULT_PORT = 6379;
    private static CommandHandler commandHandler;
    private static ServerConfig config;

    public static void main(String[] args) throws IOException {
        config = ServerConfig.parse(args);
        int port = config.getPort();
        DataStore dataStore = new DataStore();
        commandHandler = new CommandHandler(dataStore, config);

        // Load RDB file if configured
        if (config.getRdbPath() != null) {
            RdbLoader loader = new RdbLoader(dataStore);
            loader.load(config.getRdbPath());
        }

        // Start replica handshake if configured as replica
        if (config.isReplica()) {
            ReplicaClient replicaClient = new ReplicaClient(
                    config.getReplicaOfHost(), config.getReplicaOfPort(),
                    port, dataStore, commandHandler);
            Thread replicaThread = new Thread(replicaClient, "replica-client");
            replicaThread.setDaemon(true);
            replicaThread.start();
        }

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.configureBlocking(false);

        Selector selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Redis clone listening on port " + port);

        // Main event loop
        while (true) {
            // Use short timeout when WAIT commands are pending, so we
            // can process incoming REPLCONF ACK replies and check deadlines.
            // Otherwise block until an IO event arrives.
            if (commandHandler.hasPendingWaits()) {
                selector.select(50);
            } else {
                selector.select();
            }

            Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
            while (keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();
                keyIterator.remove();

                if (!key.isValid()) {
                    continue;
                }

                try {
                    if (key.isAcceptable()) {
                        handleAccept(serverChannel, selector);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                } catch (Exception e) {
                    System.err.println("Client error: " + e.getMessage());
                    closeClient(key);
                }
            }

            // Resolve any pending WAIT commands whose target replica
            // count is met or whose deadline has passed.
            commandHandler.processPendingWaits();
        }
    }

    /**
     * Accept a new client connection and register it with the selector.
     */
    private static void handleAccept(ServerSocketChannel serverChannel, Selector selector)
            throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        if (clientChannel == null) {
            return;
        }
        clientChannel.configureBlocking(false);

        ClientSession session = new ClientSession(clientChannel);
        SelectionKey clientKey = clientChannel.register(selector, SelectionKey.OP_READ, session);
        session.setSelectionKey(clientKey);

        System.out.println("Client connected: " + clientChannel.getRemoteAddress());
    }

    /**
     * Handle readable event: read data, parse RESP commands, send responses.
     */
    private static void handleRead(SelectionKey key) throws IOException {
        ClientSession session = (ClientSession) key.attachment();

        int bytesRead = session.read();
        if (bytesRead == -1) {
            // Client disconnected
            System.out.println("Client disconnected: " + session.getChannel().getRemoteAddress());
            closeClient(key);
            return;
        }

        // Process all complete commands available in the buffer
        List<String> command;
        while ((command = session.getNextCommand()) != null) {
            byte[] response = commandHandler.dispatch(command, session);
            session.queueResponse(response);
        }

        // If we have data to write, register interest in OP_WRITE
        if (session.hasDataToWrite()) {
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }
    }

    /**
     * Handle writable event: flush queued responses to the client.
     */
    private static void handleWrite(SelectionKey key) throws IOException {
        ClientSession session = (ClientSession) key.attachment();

        if (session.doWrite()) {
            // All data written — stop watching for OP_WRITE
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    /**
     * Clean up a client connection.
     */
    private static void closeClient(SelectionKey key) {
        try {
            key.cancel();
            key.channel().close();
        } catch (IOException e) {
            // Ignore close errors
        }
    }
}