package com.redis4j.storage.repository;

import com.redis4j.storage.Entry;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public final class ConcurrentMapEntryRepository implements EntryRepository {
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    @Override public synchronized Entry get(String key) { return entries.get(key); }
    @Override public synchronized Entry put(String key, Entry entry) { return entries.put(key, entry); }
    @Override public synchronized Entry putIfAbsent(String key, Entry entry) { return entries.putIfAbsent(key, entry); }
    @Override public synchronized Entry compute(String key, BiFunction<String, Entry, Entry> fn) { return entries.compute(key, fn); }
    @Override public synchronized Entry computeIfPresent(String key, BiFunction<String, Entry, Entry> fn) { return entries.computeIfPresent(key, fn); }
    @Override public synchronized Entry remove(String key) { return entries.remove(key); }
    @Override public synchronized boolean remove(String key, Entry entry) { return entries.remove(key, entry); }
    @Override public synchronized Set<String> keySet() { return new HashSet<>(entries.keySet()); }
    @Override public synchronized Enumeration<String> keys() { return java.util.Collections.enumeration(new HashSet<>(entries.keySet())); }
    @Override public synchronized long size() { return entries.mappingCount(); }
    @Override public synchronized void clear() { entries.clear(); }
}
