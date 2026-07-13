# Redis4J

A simplified Redis implementation in Java with Netty.

## Features

- Full RESP (Redis Serialization Protocol) support
- String, List, Hash, Set data types
- Key expiration management
- RDB persistence
- AOF persistence with sequential batch writes and configurable fsync policy
- Netty-based high-performance network layer

## Requirements

- JDK 17+
- Maven 3.6+

## Build

```bash
mvn clean package
```

## Run Server

```bash
java -cp target/redis4j-1.0.0-SNAPSHOT.jar com.redis4j.Redis4J server
java -cp target/redis4j-1.0.0-SNAPSHOT.jar com.redis4j.Redis4J server -p 6380
```

Enable AOF persistence (default policy: `everysec`):

```bash
java -jar redis4j-server/target/redis4j-server-1.0.0-SNAPSHOT.jar --appendonly --appendfsync everysec
```

Supported AOF flush policies are `always`, `everysec`, and `no`. The AOF file is
stored as `data/appendonly.aof` by default. Records use a framed binary format
with length and CRC32 fields so an incomplete tail can be detected and truncated
during startup recovery.

Use `BGREWRITEAOF` to compact the current command history manually. Redis4J also
starts a background rewrite automatically after the file reaches 64 MiB and has
grown by 100% since the previous rewrite. These thresholds can be changed with
`ServerConfig.setAutoAofRewriteMinSize(...)` and
`ServerConfig.setAutoAofRewritePercentage(...)`; set the percentage to `0` to
disable automatic rewrites.

Rewritten AOF files use hybrid persistence by default: a compact RDB snapshot is
stored first, followed by AOF records generated while the rewrite was running.
Startup recovery loads the RDB section and then replays only the incremental AOF
tail. Existing pure AOF files remain supported. Use
`--aof-use-rdb-preamble false` or
`ServerConfig.setAofUseRdbPreamble(false)` to keep pure-AOF rewrites.

Limit the approximate memory used by keys and values with `--maxmemory`. Size
suffixes `kb`, `mb`, and `gb` are supported. The default value is `0` (unlimited):

```bash
java -jar redis4j-server/target/redis4j-server-1.0.0-SNAPSHOT.jar \
  --maxmemory 256mb --maxmemory-policy allkeys-lru
```

Supported policies are `noeviction`, `allkeys-lru`, `allkeys-random`,
`volatile-lru`, `volatile-random`, and `volatile-ttl`. Volatile policies only
evict keys with an expiry. If a policy cannot free enough memory, the write is
rolled back and returns an OOM error. Eviction deletes are included in AOF so
evicted keys do not reappear after restart.

## Run Client

```bash
java -cp target/redis4j-1.0.0-SNAPSHOT.jar com.redis4j.Redis4J client
java -cp target/redis4j-1.0.0-SNAPSHOT.jar com.redis4j.Redis4J client -h localhost -p 6379
```

## Supported Commands

### String Commands
- GET, SET, SETNX, SETEX
- MGET, MSET
- INCR, DECR, INCRBY, DECRBY
- STRLEN, APPEND

### Key Commands
- DEL, EXISTS
- EXPIRE, EXPIREAT, PEXPIREAT, PERSIST, TTL, PTTL
- RENAME, TYPE
- KEYS, SCAN, DBSIZE
- FLUSHDB, FLUSHALL
- PING, ECHO
- BGREWRITEAOF

### List Commands
- LPUSH, RPUSH
- LPOP, RPOP
- LLEN, LRANGE, LINDEX
- LSET, LTRIM

### Hash Commands
- HSET, HGET
- HGETALL, HDEL
- HEXISTS, HLEN
- HKEYS, HVALS, HSCAN
- HMSET, HMGET, HINCRBY

### Set Commands
- SADD, SREM
- SMEMBERS, SISMEMBER, SSCAN
- SCARD
- SINTER, SUNION, SDIFF
- SMOVE, SPOP, SRANDMEMBER

### Sorted Set Commands
- ZADD, ZREM, ZSCORE, ZCARD, ZINCRBY
- ZRANGE, ZREVRANGE, ZRANK, ZREVRANK
- ZCOUNT, ZRANGEBYSCORE, ZSCAN

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                      RedisServer                     │
│  ┌─────────────┐         ┌─────────────────────────┐│
│  │ bossGroup   │ ─────── │ workerGroup             ││
│  └─────────────┘         └─────────────────────────┘│
│                                   │                  │
│                          ┌────────▼────────┐         │
│                          │ Pipeline        │         │
│                          │  RespDecoder    │         │
│                          │  ClientHandler  │         │
│                          │  RespEncoder    │         │
│                          └─────────────────┘         │
└─────────────────────────────────────────────────────┘
```

## License

MIT
