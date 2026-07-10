package com.redis4j.command;

/** Describes the number of arguments accepted by a Redis command. */
public record Arity(int minimum, int maximum) {
    public static Arity exact(int count) { return new Arity(count, count); }
    public static Arity atLeast(int count) { return new Arity(count, Integer.MAX_VALUE); }

    public static Arity fromLegacy(int arityIncludingCommand) {
        return arityIncludingCommand > 0
                ? exact(arityIncludingCommand - 1)
                : atLeast(Math.abs(arityIncludingCommand));
    }

    public boolean accepts(int count) {
        return count >= minimum && count <= maximum;
    }
}
