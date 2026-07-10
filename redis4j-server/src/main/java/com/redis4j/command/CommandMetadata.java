package com.redis4j.command;

import java.util.Locale;

/** Immutable command metadata used by registration and validation. */
public record CommandMetadata(String name, Arity arity) {
    public CommandMetadata {
        name = name.toUpperCase(Locale.ROOT);
    }
}
