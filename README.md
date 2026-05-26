# Redis4J

A simplified Redis implementation in Java with Netty.

## Features

- Full RESP (Redis Serialization Protocol) support
- String, List, Hash, Set data types
- Key expiration management
- RDB persistence
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
- EXPIRE, PERSIST, TTL, PTTL
- RENAME, TYPE
- KEYS, DBSIZE
- FLUSHDB, FLUSHALL
- PING, ECHO

### List Commands
- LPUSH, RPUSH
- LPOP, RPOP
- LLEN, LRANGE, LINDEX
- LSET, LTRIM

### Hash Commands
- HSET, HGET
- HGETALL, HDEL
- HEXISTS, HLEN
- HKEYS, HVALS
- HMSET, HMGET, HINCRBY

### Set Commands
- SADD, SREM
- SMEMBERS, SISMEMBER
- SCARD
- SINTER, SUNION, SDIFF
- SMOVE, SPOP, SRANDMEMBER

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
