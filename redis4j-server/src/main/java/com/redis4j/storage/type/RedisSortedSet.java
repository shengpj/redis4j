package com.redis4j.storage.type;

import com.redis4j.storage.DataType;
import com.redis4j.storage.ZSetStore.ScoredMember;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public final class RedisSortedSet implements RedisValue {
    private static final Comparator<ScoredMember> ORDER = Comparator
            .comparingDouble(ScoredMember::score)
            .thenComparing(ScoredMember::member);

    private final Map<String, Double> scores = new HashMap<>();
    private final NavigableSet<ScoredMember> ordered = new TreeSet<>(ORDER);

    public RedisSortedSet() {}

    public RedisSortedSet(Map<String, Double> initial) {
        initial.forEach(this::add);
    }

    @Override
    public DataType getType() {
        return DataType.ZSET;
    }

    @Override
    public Object getValue() {
        return scores;
    }

    public boolean add(String member, double score) {
        validateScore(score);
        score = normalizeZero(score);
        Double previous = scores.put(member, score);
        if (previous != null) ordered.remove(new ScoredMember(member, previous));
        ordered.add(new ScoredMember(member, score));
        return previous == null;
    }

    public boolean remove(String member) {
        Double score = scores.remove(member);
        return score != null && ordered.remove(new ScoredMember(member, score));
    }

    public Double score(String member) {
        return scores.get(member);
    }

    public double increment(String member, double delta) {
        validateScore(delta);
        double updated = scores.getOrDefault(member, 0.0) + delta;
        validateScore(updated);
        add(member, updated);
        return normalizeZero(updated);
    }

    public long size() {
        return scores.size();
    }

    public boolean isEmpty() {
        return scores.isEmpty();
    }

    public Map<String, Double> copyScores() {
        return new HashMap<>(scores);
    }

    public Map<String, Double> getScores() {
        return scores;
    }

    public List<ScoredMember> range(long start, long stop, boolean reverse) {
        int size = ordered.size();
        long from = normalizeIndex(start, size);
        long to = normalizeIndex(stop, size);
        if (from < 0) from = 0;
        if (to >= size) to = size - 1L;
        if (from > to || from >= size || to < 0) return List.of();

        List<ScoredMember> result = new ArrayList<>((int) (to - from + 1));
        Iterator<ScoredMember> iterator = reverse ? ordered.descendingIterator() : ordered.iterator();
        long index = 0;
        while (iterator.hasNext() && index <= to) {
            ScoredMember value = iterator.next();
            if (index >= from) result.add(value);
            index++;
        }
        return result;
    }

    public Long rank(String member, boolean reverse) {
        if (!scores.containsKey(member)) return null;
        Iterator<ScoredMember> iterator = reverse ? ordered.descendingIterator() : ordered.iterator();
        long rank = 0;
        while (iterator.hasNext()) {
            if (iterator.next().member().equals(member)) return rank;
            rank++;
        }
        return null;
    }

    public List<ScoredMember> rangeByScore(double min, boolean minInclusive,
                                            double max, boolean maxInclusive,
                                            long offset, long count) {
        if (offset < 0 || count <= 0 || min > max) return List.of();
        List<ScoredMember> result = new ArrayList<>();
        long skipped = 0;
        for (ScoredMember value : ordered) {
            if (!within(value.score(), min, minInclusive, max, maxInclusive)) continue;
            if (skipped++ < offset) continue;
            result.add(value);
            if (result.size() >= count) break;
        }
        return result;
    }

    public long count(double min, boolean minInclusive, double max, boolean maxInclusive) {
        if (min > max) return 0;
        return ordered.stream()
                .filter(value -> within(value.score(), min, minInclusive, max, maxInclusive))
                .count();
    }

    private static boolean within(double score, double min, boolean minInclusive,
                                  double max, boolean maxInclusive) {
        return (score > min || minInclusive && score == min)
                && (score < max || maxInclusive && score == max);
    }

    private static long normalizeIndex(long index, int size) {
        return index < 0 ? size + index : index;
    }

    private static double normalizeZero(double score) {
        return score == 0.0 ? 0.0 : score;
    }

    private static void validateScore(double score) {
        if (Double.isNaN(score)) throw new IllegalArgumentException("score is not a valid float");
    }

    @Override
    public boolean isExpired() {
        return false;
    }

    @Override
    public long getTtl() {
        return -1;
    }
}
