package com.rediscone;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Server configuration parsed from CLI arguments.
 * Supports --dir, --dbfilename, --port, and --replicaof flags.
 */
public class ServerConfig {

    private String dir = null;
    private String dbFilename = null;
    private int port = 6379;
    private String replicaOfHost = null;
    private int replicaOfPort = -1;

    /**
     * Parse CLI arguments into configuration.
     * Supports: --dir <path>, --dbfilename <name>, --port <num>, --replicaof <host> <port>
     */
    public static ServerConfig parse(String[] args) {
        ServerConfig config = new ServerConfig();

        for (int i = 0; i < args.length; i++) {
            switch (args[i].toLowerCase()) {
                case "--dir":
                    if (i + 1 < args.length) {
                        config.dir = args[++i];
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        config.dbFilename = args[++i];
                    }
                    break;
                case "--port":
                    if (i + 1 < args.length) {
                        config.port = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--replicaof":
                    if (i + 2 < args.length) {
                        config.replicaOfHost = args[++i];
                        config.replicaOfPort = Integer.parseInt(args[++i]);
                    }
                    break;
            }
        }

        return config;
    }

    public String getDir() {
        return dir;
    }

    public String getDbFilename() {
        return dbFilename;
    }

    public int getPort() {
        return port;
    }

    public String getReplicaOfHost() {
        return replicaOfHost;
    }

    public int getReplicaOfPort() {
        return replicaOfPort;
    }

    public boolean isReplica() {
        return replicaOfHost != null && replicaOfPort > 0;
    }

    /**
     * Get the full path to the RDB file, or null if dir/dbfilename are not configured.
     */
    public Path getRdbPath() {
        if (dir == null || dbFilename == null) {
            return null;
        }
        return Paths.get(dir, dbFilename);
    }
}
