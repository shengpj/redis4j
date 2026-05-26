# Redis4J 开发计划

## 项目概述

使用 Java 实现一个简化版的 Redis，包含 Server 端和 Client 端，通讯协议遵循 Redis Serialization Protocol (RESP)。

## 多模块架构

```
redis4j/                          # 父项目 (pom)
├── pom.xml                        # 父 POM，管理依赖版本
├── redis4j-common/                # 公共模块
│   ├── pom.xml
│   └── src/main/java/com/redis4j/
│       └── protocol/              # RESP 协议层
│           ├── RespType.java     # 协议类型枚举
│           ├── RespObject.java   # RESP 对象
│           ├── RespEncoder.java  # RESP 编码器
│           └── RespDecoder.java  # RESP 解码器
│
├── redis4j-server/               # 服务端模块
│   ├── pom.xml
│   └── src/main/java/com/redis4j/
│       ├── server/              # 服务端
│       │   ├── RedisServer.java # Netty Server
│       │   ├── ServerConfig.java
│       │   └── ChannelHandlers.java
│       ├── storage/             # 存储引擎
│       │   ├── DataStore.java
│       │   ├── DataType.java
│       │   ├── MemoryStore.java
│       │   └── type/
│       │       ├── RedisValue.java
│       │       ├── RedisString.java
│       │       ├── RedisList.java
│       │       ├── RedisHash.java
│       │       └── RedisSet.java
│       ├── command/             # 命令处理
│       │   ├── Command.java
│       │   ├── CommandRegistry.java
│       │   └── impl/
│       │       ├── StringCommands.java
│       │       ├── KeyCommands.java
│       │       ├── ListCommands.java
│       │       ├── HashCommands.java
│       │       └── SetCommands.java
│       └── persistence/         # 持久化
│           ├── RDBWriter.java
│           ├── RDBReader.java
│           └── PersistenceManager.java
│
└── redis4j-client/               # 客户端模块
    ├── pom.xml
    └── src/main/java/com/redis4j/
        └── client/
            ├── RedisClient.java   # Netty Client
            ├── RedisCommands.java  # 命令封装
            └── ChannelHandlers.java
```

## 模块依赖关系

```
┌─────────────────┐
│ redis4j-parent  │
│     (POM)       │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐  ┌──────────┐
│common  │  │  server  │
└───┬────┘  └──────┬───┘
    │              │
    │              ▼
    │         ┌────────┐
    └────────►│ client │
              └────────┘
```

## 技术选型

| 组件 | 选择 | 理由 |
|------|------|------|
| 语言 | JDK 17 | 长期支持版本，现代化特性 |
| 构建 | Maven | 依赖管理，多模块支持 |
| 网络框架 | Netty 4.2.13.Final | 高性能 NIO 框架 |
| 存储 | ConcurrentHashMap | 线程安全 |
| 日志 | SLF4J + Logback | 灵活配置 |

## RESP 协议实现

RESP (Redis Serialization Protocol) 支持以下类型：

| 类型 | 标识 | 示例 |
|------|------|------|
| Simple String | `+` | `+OK\r\n` |
| Error | `-` | `-ERR unknown command\r\n` |
| Integer | `:` | `:100\r\n` |
| Bulk String | `$` | `$5\r\nhello\r\n` |
| Array | `*` | `*2\r\n$3\r\nfoo\r\n:100\r\n` |

## 已实现功能

### 服务端
- Netty NIO 网络框架
- RESP 协议编解码
- 内存存储引擎（ConcurrentHashMap）
- 自动过期 key 清理
- 50+ Redis 命令

### 客户端
- Netty Bootstrap 连接
- 同步/异步命令发送
- 命令封装 API

### 持久化
- RDB 快照框架

## 快速启动

```bash
# 编译所有模块
mvn clean compile

# 运行服务端
cd redis4j-server
mvn exec:java -Dexec.mainClass="com.redis4j.server.RedisServer"

# 运行客户端
cd redis4j-client
mvn exec:java -Dexec.mainClass="com.redis4j.client.RedisClient"

# 打包
mvn package

# 运行测试
mvn test
```

## 后续扩展方向

- AOF 持久化
- 客户端连接池
- 发布/订阅
- 事务支持 (MULTI/EXEC)
- Lua 脚本
- 集群支持

---

**版本**: 3.0（多模块架构）
**更新日期**: 2026-05-22
