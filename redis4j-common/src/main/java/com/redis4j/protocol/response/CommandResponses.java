package com.redis4j.protocol.response;

import java.util.Arrays;
import java.util.List;

public final class CommandResponses {
    private CommandResponses() {}

    public static CommandResponse simpleString(String value) { return new CommandResponse.SimpleString(value); }
    public static CommandResponse error(String value) { return new CommandResponse.Error(value); }
    public static CommandResponse error(String prefix, String value) { return error(prefix + " " + value); }
    public static CommandResponse integer(long value) { return new CommandResponse.IntegerValue(value); }
    public static CommandResponse bulkString(String value) { return new CommandResponse.BulkString(value); }
    public static CommandResponse bulkString(byte[] value) {
        return bulkString(value == null ? null : new String(value, java.nio.charset.StandardCharsets.UTF_8));
    }
    public static CommandResponse nullBulkString() { return bulkString((String) null); }
    public static CommandResponse array(List<CommandResponse> values) { return new CommandResponse.ArrayValue(values); }
    public static CommandResponse array(CommandResponse... values) { return array(Arrays.asList(values)); }
    public static CommandResponse nullArray() { return array((List<CommandResponse>) null); }
    public static CommandResponse ok() { return simpleString("OK"); }
    public static CommandResponse pong() { return simpleString("PONG"); }
}
