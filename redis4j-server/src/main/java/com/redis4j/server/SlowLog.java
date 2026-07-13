package com.redis4j.server;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

final class SlowLog {
    private static final int MAX_ARGUMENTS = 32;
    private static final int MAX_ARGUMENT_LENGTH = 128;

    private final long thresholdMicros;
    private final int maxLength;
    private final AtomicLong nextId = new AtomicLong();
    private final Deque<Entry> entries = new ArrayDeque<>();

    SlowLog(long thresholdMicros, int maxLength) {
        if (thresholdMicros < -1) throw new IllegalArgumentException("thresholdMicros cannot be less than -1");
        if (maxLength < 0) throw new IllegalArgumentException("maxLength cannot be negative");
        this.thresholdMicros = thresholdMicros;
        this.maxLength = maxLength;
    }

    synchronized void record(String command, String[] args, long durationMicros,
                             String clientAddress, String clientName) {
        if (thresholdMicros < 0 || durationMicros < thresholdMicros || maxLength == 0) return;
        entries.addFirst(new Entry(nextId.getAndIncrement(), Instant.now().getEpochSecond(),
                Math.max(0, durationMicros), arguments(command, args),
                clientAddress == null ? "" : clientAddress,
                clientName == null ? "" : clientName));
        while (entries.size() > maxLength) entries.removeLast();
    }

    synchronized List<Entry> get(int count) {
        if (count <= 0) return List.of();
        List<Entry> result = new ArrayList<>(Math.min(count, entries.size()));
        int copied = 0;
        for (Entry entry : entries) {
            if (copied++ >= count) break;
            result.add(entry);
        }
        return result;
    }

    synchronized int length() {
        return entries.size();
    }

    synchronized void reset() {
        entries.clear();
    }

    private static List<String> arguments(String command, String[] args) {
        List<String> values = new ArrayList<>();
        values.add(truncate(command));
        boolean truncated = args.length > MAX_ARGUMENTS - 1;
        int copied = Math.min(args.length, truncated ? MAX_ARGUMENTS - 2 : MAX_ARGUMENTS - 1);
        for (int i = 0; i < copied; i++) values.add(truncate(args[i]));
        if (truncated) values.add("... (" + (args.length - copied) + " more arguments)");
        return List.copyOf(values);
    }

    private static String truncate(String value) {
        if (value == null) return "";
        if (value.length() <= MAX_ARGUMENT_LENGTH) return value;
        return value.substring(0, MAX_ARGUMENT_LENGTH) + "... (" + (value.length() - MAX_ARGUMENT_LENGTH)
                + " more characters)";
    }

    record Entry(long id, long timestampSeconds, long durationMicros,
                 List<String> arguments, String clientAddress, String clientName) {}
}
