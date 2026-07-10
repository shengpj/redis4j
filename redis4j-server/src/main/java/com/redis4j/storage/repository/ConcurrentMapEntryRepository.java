package com.redis4j.storage.repository;

import com.redis4j.storage.Entry;

import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public final class ConcurrentMapEntryRepository implements EntryRepository {
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    @Override public Entry get(String key) { return entries.get(key); }
    @Override public Entry put(String key, Entry entry) { return entries.put(key, entry); }
    @Override public Entry putIfAbsent(String key, Entry entry) { return entries.putIfAbsent(key, entry); }
    @Override public Entry compute(String key, BiFunction<String, Entry, Entry> fn) { return entries.compute(key, fn); }
    @Override public Entry computeIfPresent(String key, BiFunction<String, Entry, Entry> fn) { return entries.computeIfPresent(key, fn); }
    @Override public Entry remove(String key) { return entries.remove(key); }
    @Override public boolean remove(String key, Entry entry) { return entries.remove(key, entry); }
    @Override public Set<String> keySet() { return entries.keySet(); }
    @Override public Enumeration<String> keys() { return entries.keys(); }
    @Override public long size() { return entries.mappingCount(); }
    @Override public void clear() { entries.clear(); }
}
