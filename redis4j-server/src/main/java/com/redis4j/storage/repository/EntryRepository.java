package com.redis4j.storage.repository;

import com.redis4j.storage.Entry;

import java.util.Enumeration;
import java.util.Set;
import java.util.function.BiFunction;

/** Container abstraction used by Redis storage semantics. */
public interface EntryRepository {
    Entry get(String key);
    Entry put(String key, Entry entry);
    Entry putIfAbsent(String key, Entry entry);
    Entry compute(String key, BiFunction<String, Entry, Entry> function);
    Entry computeIfPresent(String key, BiFunction<String, Entry, Entry> function);
    Entry remove(String key);
    boolean remove(String key, Entry entry);
    Set<String> keySet();
    Enumeration<String> keys();
    long size();
    void clear();
}
