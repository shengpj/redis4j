package com.redis4j.storage;

import com.redis4j.storage.type.RedisValue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 数据存储接口
 */
public interface DataStore {

    // ==================== String 操作 ====================

    /**
     * 设置字符串值
     */
    void set(String key, String value);

    /**
     * 设置字符串值和过期时间（秒）
     */
    void setEx(String key, String value, long seconds);

    /**
     * 设置字符串值，仅当 key 不存在时
     */
    boolean setNx(String key, String value);

    /**
     * 获取字符串值
     */
    String get(String key);

    /**
     * 批量获取字符串值
     */
    String[] mGet(String... keys);

    /**
     * 批量设置字符串值
     */
    void mSet(Map<String, String> keyValues);

    /**
     * 自增（原子操作）
     */
    long incr(String key);

    /**
     * 自增指定值
     */
    long incrBy(String key, long delta);

    /**
     * 自减（原子操作）
     */
    long decr(String key);

    /**
     * 自减指定值
     */
    long decrBy(String key, long delta);

    /**
     * 获取字符串长度
     */
    long strlen(String key);

    /**
     * 追加字符串
     */
    long append(String key, String value);

    // ==================== Key 操作 ====================

    /**
     * 删除 key
     */
    long del(String... keys);

    /**
     * 判断 key 是否存在
     */
    boolean exists(String key);

    /**
     * 批量判断 key 是否存在
     */
    long exists(String... keys);

    /**
     * 设置 key 的过期时间（秒）
     */
    boolean expire(String key, long seconds);

    /**
     * 设置 key 的过期时间（毫秒）
     */
    boolean expireMs(String key, long milliseconds);

    /**
     * 获取 key 的剩余生存时间（秒）
     * -2 表示 key 不存在，-1 表示没有设置过期时间
     */
    long ttl(String key);

    /**
     * 获取 key 的剩余生存时间（毫秒）
     */
    long pttl(String key);

    /**
     * 移除 key 的过期时间
     */
    boolean persist(String key);

    /**
     * 重命名 key
     */
    void rename(String key, String newKey);

    /**
     * 获取 key 的数据类型
     */
    DataType type(String key);

    /**
     * 获取所有匹配的 key（注意：生产环境应避免使用 KEYS 命令）
     */
    Set<String> keys(String pattern);

    /**
     * 获取数据库 key 数量
     */
    long dbSize();

    // ==================== List 操作 ====================

    /**
     * 从左侧推入元素
     */
    long lPush(String key, String... values);

    /**
     * 从右侧推入元素
     */
    long rPush(String key, String... values);

    /**
     * 从左侧弹出元素
     */
    String lPop(String key);

    /**
     * 从右侧弹出元素
     */
    String rPop(String key);

    /**
     * 获取列表长度
     */
    long lLen(String key);

    /**
     * 获取列表范围内的元素
     */
    String[] lRange(String key, long start, long stop);

    /**
     * 设置列表指定位置的元素
     */
    void lSet(String key, long index, String value);

    /**
     * 修列表长度
     */
    void lTrim(String key, long start, long stop);

    /**
     * 获取列表指定位置的元素
     */
    String lIndex(String key, long index);

    // ==================== Hash 操作 ====================

    /**
     * 设置 hash 字段
     */
    long hSet(String key, String field, String value);

    /**
     * 设置 hash 字段，仅当 field 不存在时
     */
    boolean hSetNx(String key, String field, String value);

    /**
     * 获取 hash 字段值
     */
    String hGet(String key, String field);

    /**
     * 获取所有 hash 字段和值
     */
    Map<String, String> hGetAll(String key);

    /**
     * 删除 hash 字段
     */
    long hDel(String key, String... fields);

    /**
     * 判断 hash 字段是否存在
     */
    boolean hExists(String key, String field);

    /**
     * 获取 hash 字段数量
     */
    long hLen(String key);

    /**
     * 获取所有 hash 字段
     */
    Set<String> hKeys(String key);

    /**
     * 获取所有 hash 值
     */
    String[] hVals(String key);

    /**
     * 批量设置 hash 字段
     */
    long hMSet(String key, Map<String, String> fieldValues);

    /**
     * 批量获取 hash 字段值
     */
    String[] hMGet(String key, String... fields);

    /**
     * 自增 hash 字段值
     */
    long hIncrBy(String key, String field, long delta);

    // ==================== Set 操作 ====================

    /**
     * 添加 set 成员
     */
    long sAdd(String key, String... members);

    /**
     * 删除 set 成员
     */
    long sRem(String key, String... members);

    /**
     * 获取 set 所有成员
     */
    Set<String> sMembers(String key);

    /**
     * 判断成员是否在 set 中
     */
    boolean sIsMember(String key, String member);

    /**
     * 获取 set 成员数量
     */
    long sCard(String key);

    /**
     * 获取多个 set 的交集
     */
    Set<String> sInter(String... keys);

    /**
     * 获取多个 set 的并集
     */
    Set<String> sUnion(String... keys);

    /**
     * 获取多个 set 的差集
     */
    Set<String> sDiff(String... keys);

    /**
     * 移动成员到另一个 set
     */
    boolean sMove(String srcKey, String destKey, String member);

    /**
     * 随机获取一个成员并删除
     */
    String sPop(String key);

    /**
     * 随机获取一个成员（不删除）
     */
    String sRandMember(String key);

    /**
     * 随机获取多个成员（不删除）
     */
    String[] sRandMember(String key, long count);

    // ==================== 服务器操作 ====================

    /**
     * 清空当前数据库
     */
    void flushDb();

    /**
     * 清空所有数据库
     */
    void flushAll();

    /**
     * 关闭存储（用于清理资源）
     */
    void close();
}
