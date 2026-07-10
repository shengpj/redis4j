package com.redis4j.storage;

import java.util.Set;

public interface SetStore {
    long sAdd(String key, String... members);
    long sRem(String key, String... members);
    Set<String> sMembers(String key);
    boolean sIsMember(String key, String member);
    long sCard(String key);
    Set<String> sInter(String... keys);
    Set<String> sUnion(String... keys);
    Set<String> sDiff(String... keys);
    boolean sMove(String srcKey, String destKey, String member);
    String sPop(String key);
    String sRandMember(String key);
    String[] sRandMember(String key, long count);
}
