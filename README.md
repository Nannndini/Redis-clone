# Redis-clone
A Redis server built from scratch in Java.
## Features

- RESP protocol parser/encoder (arrays, bulk strings, errors, integers)
- Single-threaded NIO Selector event loop for multi-client support
- In-memory key-value store with PX/EX expiry
- RDB persistence loading + CONFIG GET
- Master-replica replication: PSYNC handshake, write propagation, WAIT
- Transactions: MULTI, EXEC, DISCARD
- Lists: LPUSH, RPUSH, LPOP, LRANGE, LLEN, BLPOP (blocking pop)

## Supported Commands

PING, ECHO, SET, GET, CONFIG, KEYS, INFO, REPLCONF, PSYNC, WAIT,
MULTI, EXEC, DISCARD, LPUSH, RPUSH, LPOP, LRANGE, LLEN, BLPOP

## Notable bugs found and fixed during development

- **WAIT deadlock**: the original WAIT implementation blocked the single 
  event-loop thread while polling for replica ACKs — but that same thread 
  is the only one that can read the incoming ACK data, so it deadlocked 
  itself. Fixed by making WAIT non-blocking: pending waits are tracked and 
  resolved once per event loop iteration instead of blocking inline.
- **BLPOP under-wake**: when multiple clients were blocked on the same key, 
  a single LPUSH pushing multiple values only woke one waiting client 
  instead of all clients that had data available for them. Fixed by looping 
  until no more blocked clients could be satisfied.

## Build & Run

\`\`\`
mvn compile
java -cp target/classes com.rediscone.Main
\`\`\`