package com.redis4j.storage.memory;

import java.util.List;

/** 在不修改数据的前提下计算出的淘汰计划。 */
public record EvictionPlan(boolean sufficient, List<String> keys) {
    public EvictionPlan {
        keys = List.copyOf(keys);
    }
}
