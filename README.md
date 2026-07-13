# Redis4J

Redis4J 是一个使用 Java 17 和 Netty 实现的单机 Redis 兼容服务，包含服务端、RESP 协议公共模块和交互式客户端。项目覆盖常用数据类型、键过期、RDB/AOF 混合持久化、内存淘汰、发布订阅以及基础可观测性能力，适合学习 Redis 核心机制和 Netty 服务端工程实践。

## 功能概览

- 基于 Netty 的异步网络层和 RESP 编解码。
- 支持 String、List、Hash、Set、Sorted Set 五种数据类型。
- 支持键过期、主动清理和访问时惰性清理。
- 提供 `MemoryStore` 和 `PartitionedMemoryStore` 两种内存存储实现。
- 支持 RDB、AOF 以及 RDB 前导 + AOF 增量的混合持久化。
- AOF 使用单写线程、顺序批量写入、CRC32 校验和可配置刷盘策略。
- 支持 AOF 后台重写、自动重写和异常尾部截断。
- 支持最大内存限制和多种淘汰策略。
- 支持最大连接数、请求大小、命令队列和客户端输出缓冲区限制。
- 支持 `PUBLISH`、`SUBSCRIBE`、`UNSUBSCRIBE` 发布订阅。
- 提供 `INFO`、`SLOWLOG`、`CLIENT LIST`、`CONFIG GET` 等排障命令。

## 环境要求

- JDK 17 或更高版本
- Maven 3.6 或更高版本

## 项目结构

```text
redis4j
|-- redis4j-common   RESP 协议模型、聚合器和公共工具
|-- redis4j-server   Netty 服务端、命令、存储、持久化和资源控制
|-- redis4j-client   Netty 客户端、命令封装和 JLine 交互终端
`-- pom.xml          Maven 多模块父工程
```

## 构建项目

在项目根目录执行：

```bash
mvn clean package
```

跳过测试构建：

```bash
mvn clean package -DskipTests
```

服务端可执行 JAR 生成在：

```text
redis4j-server/target/redis4j-server-1.0.0-SNAPSHOT.jar
```

## 启动服务端

使用默认配置启动，监听 `0.0.0.0:6379`：

```bash
java -jar redis4j-server/target/redis4j-server-1.0.0-SNAPSHOT.jar
```

指定监听地址、端口和存储实现：

```bash
java -jar redis4j-server/target/redis4j-server-1.0.0-SNAPSHOT.jar --host 127.0.0.1 --port 6380 --store partitioned
```

启用 AOF、内存限制和连接资源限制：

```bash
java -jar redis4j-server/target/redis4j-server-1.0.0-SNAPSHOT.jar \
  --appendonly \
  --appendfsync everysec \
  --maxmemory 256mb \
  --maxmemory-policy allkeys-lru \
  --maxclients 10000 \
  --timeout 30 \
  --client-output-buffer-limit-normal 8mb \
  --client-output-buffer-limit-pubsub 32mb
```

容量参数支持纯数字字节数以及大小写不敏感的 `kb`、`mb`、`gb` 后缀，例如 `1048576`、`1mb`、`2GB`。

### 启动参数

以下参数由 `RedisServer.main` 直接解析：

| 参数 | 参数值 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `-p`, `--port` | 整数 | `6379` | 服务监听端口 |
| `-h`, `--host` | 地址 | `0.0.0.0` | 服务绑定地址 |
| `--store`, `--datastore` | `memory` / `partitioned` | `partitioned` | 内存存储实现 |
| `--appendonly` | 无 | 关闭 | 启用 AOF 持久化 |
| `--appendfsync` | `always` / `everysec` / `no` | `everysec` | AOF 刷盘策略 |
| `--aof-use-rdb-preamble` | `true` / `false` | `true` | AOF 重写时是否使用 RDB 前导形成混合文件 |
| `--maxmemory` | 字节数或容量 | `0` | 数据内存上限；`0` 表示不限制 |
| `--maxmemory-policy` | 淘汰策略 | `noeviction` | 达到内存上限后的处理策略 |
| `--maxclients` | 正整数 | `10000` | 最大并发客户端连接数 |
| `--timeout` | 非负秒数 | `30` | 连接读空闲检测周期；`0` 表示关闭检测 |
| `--client-output-buffer-limit-normal` | 字节数或容量 | `8mb` | 普通客户端待发送 RESP 数据上限；`0` 表示不限制 |
| `--client-output-buffer-limit-pubsub` | 字节数或容量 | `32mb` | Pub/Sub 客户端待发送 RESP 数据上限；`0` 表示不限制 |
| `--slowlog-log-slower-than` | 微秒 | `10000` | 慢命令阈值；`-1` 表示关闭慢日志 |
| `--slowlog-max-len` | 非负整数 | `128` | 慢日志最大记录数；`0` 表示不保留记录 |
| `--daemon` | 无 | 关闭 | 兼容参数；当前版本仍以前台方式运行 |

### 其他服务端配置

以下配置目前需要在创建 `ServerConfig` 时通过 Java API 设置，不能直接通过命令行修改：

- Worker 线程数和分区数量。
- RESP 最大帧长度、最大数组元素数。
- 单连接待执行命令数和全局命令线程池队列容量。
- 数据目录、AOF 文件名和 AOF 队列容量。
- AOF 自动重写最小文件大小和增长百分比。

服务启动时会打印完整的生效配置。运行期间可以使用 `CONFIG GET *` 查看配置；当前只支持读取，不支持 `CONFIG SET`。

## 启动内置客户端

客户端模块当前不是包含全部依赖的 fat JAR，推荐通过 Maven 启动。先把多模块依赖安装到本地仓库：

```bash
mvn install -DskipTests
```

连接默认服务端 `localhost:6379`：

```bash
mvn -pl redis4j-client exec:java \
  "-Dexec.mainClass=com.redis4j.client.RedisClientDemo" \
  "-Dexec.args=localhost 6379"
```

客户端的主机和端口是位置参数，不是 `-h`、`-p` 选项。进入终端后可以直接执行命令：

```text
Connected to Redis4J server at localhost:6379
redis4j> SET user:1 Alice
OK
redis4j> GET user:1
Alice
redis4j> INFO CLIENTS
# Clients
...
redis4j> quit
```

终端内置命令：

- `help`：显示客户端支持的命令摘要。
- `exit`、`quit`：关闭连接并退出。
- `Ctrl+C`：取消当前输入。
- `Ctrl+D`：退出客户端。

## 数据类型

| 数据类型 | Java 内部类型 | 主要能力 |
| --- | --- | --- |
| String | `RedisString` | 字符串读写、批量读写、追加、整数增减 |
| List | `RedisList` | 左右压入和弹出、区间读取、索引读写、裁剪 |
| Hash | `RedisHash` | 字段读写、批量读写、字段删除、整数增量、扫描 |
| Set | `RedisSet` | 成员增删、集合运算、随机成员、成员移动、扫描 |
| Sorted Set | `RedisSortedSet` | 分值排序、排名、分值范围、增量更新、扫描 |

TTL 属于键级能力，可应用于上述所有数据类型。当前只维护一个逻辑数据库；`SELECT` 为兼容命令，不会切换到独立数据库。

## 服务端支持的命令

### String 命令

- `GET`、`SET`、`SETNX`、`SETEX`
- `MGET`、`MSET`
- `INCR`、`INCRBY`、`DECR`、`DECRBY`
- `STRLEN`、`APPEND`

当前 `SET` 实现接收 `key value`，不支持 Redis 完整版 `SET` 的 `NX`、`XX`、`EX`、`PX` 等组合选项；相应能力可使用 `SETNX`、`SETEX`。

### Key 和通用命令

- `DEL`、`EXISTS`、`TYPE`、`RENAME`
- `EXPIRE`、`EXPIREAT`、`PEXPIREAT`、`TTL`、`PTTL`、`PERSIST`
- `KEYS`、`SCAN`、`DBSIZE`
- `FLUSHDB`、`FLUSHALL`
- `PING`、`ECHO`、`SELECT`、`TIME`

`SCAN` 支持 `MATCH pattern` 和 `COUNT count`，`COUNT` 取值范围为 `1` 到 `10000`。

### List 命令

- `LPUSH`、`RPUSH`
- `LPOP`、`RPOP`
- `LLEN`、`LRANGE`、`LINDEX`
- `LSET`、`LTRIM`

### Hash 命令

- `HSET`、`HSETNX`、`HGET`、`HGETALL`
- `HDEL`、`HEXISTS`、`HLEN`
- `HKEYS`、`HVALS`
- `HMSET`、`HMGET`、`HINCRBY`
- `HSCAN`

`HSCAN` 支持 `MATCH` 和 `COUNT` 选项。

### Set 命令

- `SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`、`SCARD`
- `SINTER`、`SUNION`、`SDIFF`
- `SMOVE`、`SPOP`、`SRANDMEMBER`
- `SSCAN`

`SSCAN` 支持 `MATCH` 和 `COUNT` 选项。

### Sorted Set 命令

- `ZADD`、`ZREM`、`ZSCORE`、`ZCARD`、`ZINCRBY`
- `ZRANGE`、`ZREVRANGE`
- `ZRANK`、`ZREVRANK`
- `ZCOUNT`、`ZRANGEBYSCORE`
- `ZSCAN`

`ZRANGE`、`ZREVRANGE` 支持 `WITHSCORES`。`ZRANGEBYSCORE` 支持开闭区间分值、`WITHSCORES` 和 `LIMIT offset count`。`ZSCAN` 支持 `MATCH` 和 `COUNT`。

### 持久化命令

- `SAVE`：同步生成 RDB 快照。
- `BGSAVE`：后台生成 RDB 快照。
- `LASTSAVE`：返回最近一次成功保存时间。
- `BGREWRITEAOF`：后台重写 AOF，仅在启用 AOF 时注册。

### Pub/Sub 命令

- `PUBLISH`
- `SUBSCRIBE`
- `UNSUBSCRIBE`

当前支持基于频道的发布订阅，不支持模式订阅 `PSUBSCRIBE`、`PUNSUBSCRIBE`。

### 可观测性命令

- `INFO MEMORY`：查看估算内存、内存上限、淘汰策略和键数量。
- `INFO CLIENTS`：查看当前连接数、峰值连接数、最大连接数和拒绝次数。
- `SLOWLOG GET [count]`、`SLOWLOG LEN`、`SLOWLOG RESET`。
- `CLIENT LIST`：查看连接地址、空闲时间、订阅数、待发送字节和最近命令。
- `CONFIG GET pattern`：按 glob 模式读取当前生效配置。

## 内置交互客户端支持的命令

内置客户端为常用命令提供了参数解析和结果格式化。服务端支持但未列在本节的命令，可以通过其他兼容 RESP 的客户端调用。

- String：`GET`、`SET`、`SETEX`、`SETNX`、`MGET`、`MSET`、`INCR`、`INCRBY`、`DECR`、`DECRBY`、`STRLEN`、`APPEND`。
- Key：`DEL`、`EXISTS`、`EXPIRE`、`EXPIREAT`、`TTL`、`PTTL`、`PERSIST`、`RENAME`、`TYPE`、`KEYS`、`SCAN`、`DBSIZE`、`FLUSHDB`、`FLUSHALL`。
- List：`LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LRANGE`、`LLEN`。
- Hash：`HSET`、`HSETNX`、`HGET`、`HGETALL`、`HDEL`、`HEXISTS`、`HLEN`、`HSCAN`。
- Set：`SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`、`SCARD`、`SSCAN`。
- Sorted Set：`ZADD`、`ZREM`、`ZSCORE`、`ZCARD`、`ZINCRBY`、`ZRANGE`、`ZREVRANGE`、`ZRANK`、`ZREVRANK`、`ZCOUNT`、`ZRANGEBYSCORE`、`ZSCAN`。
- 服务管理：`PING`、`ECHO`、`SELECT`、`TIME`、`SAVE`、`BGSAVE`、`LASTSAVE`、`INFO MEMORY`、`INFO CLIENTS`。
- 发布订阅：`PUBLISH`、`SUBSCRIBE`、`UNSUBSCRIBE`。
- 可观测性：`SLOWLOG GET|LEN|RESET`、`CLIENT LIST`、`CONFIG GET`。

## 持久化

### RDB

- 默认文件：`./data/dump.rdb`。
- 服务启动且没有可恢复的 AOF 时加载 RDB。
- 默认每 `900` 秒执行一次后台快照。
- 服务正常关闭前会生成最终 RDB 快照。
- 可使用 `SAVE`、`BGSAVE`、`LASTSAVE` 手动管理快照。

### AOF

使用以下参数启用：

```bash
java -jar redis4j-server/target/redis4j-server-1.0.0-SNAPSHOT.jar --appendonly --appendfsync everysec
```

- 默认文件：`./data/appendonly.aof`。
- `always`：每批写入后执行 `force`，延迟较高，持久性最强。
- `everysec`：最多每秒执行一次 `force`，是默认策略。
- `no`：由操作系统决定刷盘时机，吞吐量优先。
- 记录包含长度和 CRC32，可识别并截断不完整或损坏的文件尾部。
- 写线程发生故障时，当前命令会回滚内存修改，避免内存与持久化状态继续分叉。

### 混合持久化和 AOF 重写

AOF 重写默认写入 RDB 前导，随后追加重写期间产生的 AOF 增量。恢复时先加载 RDB 区域，再重放增量命令。设置 `--aof-use-rdb-preamble false` 可以生成纯 AOF 重写文件。

自动重写默认条件：

- AOF 文件至少达到 `64 MiB`。
- 相比上次重写后的文件大小增长至少 `100%`。

这两个阈值目前通过 `ServerConfig.setAutoAofRewriteMinSize(...)` 和 `ServerConfig.setAutoAofRewritePercentage(...)` 调整；百分比设置为 `0` 可关闭自动重写。

## 内存上限和淘汰策略

示例：

```bash
java -jar redis4j-server/target/redis4j-server-1.0.0-SNAPSHOT.jar --maxmemory 256mb --maxmemory-policy allkeys-lru
```

支持的策略：

| 策略 | 说明 |
| --- | --- |
| `noeviction` | 不淘汰；无法满足内存限制时拒绝写命令并回滚 |
| `allkeys-lru` | 从全部键中淘汰最近最少访问的键 |
| `allkeys-random` | 从全部键中随机淘汰 |
| `volatile-lru` | 只从设置了过期时间的键中按 LRU 淘汰 |
| `volatile-random` | 只从设置了过期时间的键中随机淘汰 |
| `volatile-ttl` | 优先淘汰剩余 TTL 最短的键 |

内存用量是基于键和值结构的估算值，不等同于 JVM 进程 RSS。触发淘汰的业务命令和淘汰产生的 `DEL` 会作为同一 AOF 记录持久化；持久化失败时同时恢复业务键和被淘汰键。

## 连接和资源限制

- RESP 单帧默认最大 `1 MiB`。
- RESP 数组默认最多 `1024` 个元素。
- 单连接最多排队 `1024` 条待处理命令。
- 全局命令线程池队列默认容量为 `1024`。
- 最大客户端连接数默认 `10000`，超限连接收到错误后关闭。
- 普通客户端输出缓冲区默认上限 `8 MiB`。
- Pub/Sub 客户端输出缓冲区默认上限 `32 MiB`。
- 输出缓冲区按 RESP 编码后的实际待发送字节统计，超限时关闭慢客户端。
- 空闲检测周期默认 `30` 秒，可通过 `--timeout 0` 关闭。

## 测试

运行全部测试：

```bash
mvn test
```

使用 Netty paranoid 模式检查引用计数泄漏：

```bash
mvn "-Dio.netty.leakDetection.level=paranoid" test
```

## 当前边界

- 当前为单机实现，不包含主从复制、哨兵和集群模式。
- 不支持事务、Lua 脚本、ACL 和认证。
- 仅维护一个逻辑数据库，`SELECT` 不提供真正的数据隔离。
- `CONFIG` 当前只支持 `GET`。
- Pub/Sub 当前不支持模式订阅。
- `--daemon` 当前不会将进程转为后台服务。
