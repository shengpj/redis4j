package com.redis4j.protocol.response;

import java.util.List;

/** Protocol-independent result produced by a Redis command. */
public sealed interface CommandResponse permits CommandResponse.SimpleString,
        CommandResponse.Error, CommandResponse.IntegerValue,
        CommandResponse.BulkString, CommandResponse.ArrayValue {

    record SimpleString(String value) implements CommandResponse {}
    record Error(String value) implements CommandResponse {}
    record IntegerValue(long value) implements CommandResponse {}
    record BulkString(String value) implements CommandResponse {}
    record ArrayValue(List<CommandResponse> values) implements CommandResponse {}
}
