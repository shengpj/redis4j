package com.redis4j.persistence.aof;

import java.util.Arrays;
import java.util.Locale;

/** AOF 中保存的一条规范化命令。 */
public record AofCommand(String name, String[] args) {
    public AofCommand {
        name = name.toUpperCase(Locale.ROOT);
        args = Arrays.copyOf(args, args.length);
    }

    @Override
    public String[] args() {
        return Arrays.copyOf(args, args.length);
    }
}
