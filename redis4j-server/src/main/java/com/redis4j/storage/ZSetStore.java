package com.redis4j.storage;

import java.util.List;
import java.util.Map;

public interface ZSetStore {
    long zAdd(String key, Map<String, Double> members);
    long zRem(String key, String... members);
    Double zScore(String key, String member);
    long zCard(String key);
    double zIncrBy(String key, double increment, String member);
    List<ScoredMember> zRange(String key, long start, long stop, boolean reverse);
    Long zRank(String key, String member, boolean reverse);
    long zCount(String key, double min, boolean minInclusive, double max, boolean maxInclusive);
    List<ScoredMember> zRangeByScore(String key, double min, boolean minInclusive,
                                     double max, boolean maxInclusive, long offset, long count);
    Map<String, Double> zGetAll(String key);

    record ScoredMember(String member, double score) {}
}
