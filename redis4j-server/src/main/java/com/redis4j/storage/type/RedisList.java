package com.redis4j.storage.type;

import com.redis4j.storage.DataType;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * List 类型值
 */
public class RedisList implements RedisValue, Iterable<String> {

    private final Deque<String> list;

    public RedisList() {
        this.list = new ArrayDeque<>();
    }

    public RedisList(Deque<String> initial) {
        this.list = new ArrayDeque<>(initial);
    }

    @Override
    public DataType getType() {
        return DataType.LIST;
    }

    @Override
    public Object getValue() {
        return list;
    }

    public Deque<String> getList() {
        return list;
    }

    public void lPush(String... values) {
        for (String value : values) {
            list.offerFirst(value);
        }
    }

    public void rPush(String... values) {
        for (String value : values) {
            list.offerLast(value);
        }
    }

    public String lPop() {
        return list.pollFirst();
    }

    public String rPop() {
        return list.pollLast();
    }

    public String peekFirst() {
        return list.peekFirst();
    }

    public String peekLast() {
        return list.peekLast();
    }

    public long size() {
        return list.size();
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public void add(String value) {
        list.offerLast(value);
    }

    public void add(int index, String value) {
        if (index < 0 || index > list.size()) {
            return;
        }
        if (index == 0) {
            list.offerFirst(value);
        } else if (index == list.size()) {
            list.offerLast(value);
        } else {
            java.util.ArrayList<String> temp = new java.util.ArrayList<>(list);
            temp.add(index, value);
            list.clear();
            list.addAll(temp);
        }
    }

    public String remove(int index) {
        if (index < 0) {
            index = list.size() + index;
        }
        if (index < 0 || index >= list.size()) {
            return null;
        }
        java.util.ArrayList<String> temp = new java.util.ArrayList<>(list);
        String removed = temp.remove(index);
        list.clear();
        list.addAll(temp);
        return removed;
    }

    public void clear() {
        list.clear();
    }

    public String get(long index) {
        if (index < 0) {
            index = list.size() + index;
        }
        if (index < 0 || index >= list.size()) {
            return null;
        }
        return (String) list.toArray()[Long.valueOf(index).intValue()];
    }

    public void set(long index, String value) {
        if (index < 0) {
            index = list.size() + index;
        }
        if (index < 0 || index >= list.size()) {
            return;
        }

        java.util.ArrayList<String> temp = new java.util.ArrayList<>(list);
        temp.set((int) index, value);
        list.clear();
        list.addAll(temp);
    }

    public void trim(long start, long stop) {
        if (stop < 0) {
            stop = list.size() + stop;
        }
        if (start < 0) {
            start = list.size() + start;
        }

        if (start >= list.size() || stop < 0 || start > stop) {
            list.clear();
            return;
        }

        start = Math.max(0, start);
        stop = Math.min(list.size() - 1, stop);

        Deque<String> newList = new ArrayDeque<>();
        Iterator<String> iterator = list.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            String value = iterator.next();
            if (i >= start && i <= stop) {
                newList.add(value);
            }
            i++;
        }
        list.clear();
        list.addAll(newList);
    }

    @Override
    public Iterator<String> iterator() {
        return list.iterator();
    }

    @Override
    public boolean isExpired() {
        return false;
    }

    @Override
    public long getTtl() {
        return -1;
    }

    @Override
    public String toString() {
        return "RedisList{" +
                "list=" + list +
                ", size=" + list.size() +
                '}';
    }
}
