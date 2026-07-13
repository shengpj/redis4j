package com.redis4j.command.impl;

import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.protocol.response.CommandResponses;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** SCAN 系列命令共享的选项解析、glob 匹配和分页逻辑。 */
final class ScanSupport {
    private static final int DEFAULT_COUNT = 10;
    private static final int MAX_COUNT = 10_000;

    private ScanSupport() {}

    static Options parse(String[] args, int cursorIndex, int optionsStart) {
        long parsedCursor;
        try {
            parsedCursor = Long.parseLong(args[cursorIndex]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid cursor");
        }
        if (parsedCursor < 0 || parsedCursor > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid cursor");
        }

        String pattern = "*";
        int count = DEFAULT_COUNT;
        for (int i = optionsStart; i < args.length; i += 2) {
            if (i + 1 >= args.length) throw new IllegalArgumentException("syntax error");
            String option = args[i].toUpperCase(Locale.ROOT);
            if ("MATCH".equals(option)) {
                pattern = args[i + 1];
            } else if ("COUNT".equals(option)) {
                try {
                    count = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("COUNT must be a positive integer");
                }
                if (count <= 0 || count > MAX_COUNT) {
                    throw new IllegalArgumentException("COUNT must be between 1 and " + MAX_COUNT);
                }
            } else {
                throw new IllegalArgumentException("syntax error");
            }
        }
        return new Options((int) parsedCursor, pattern, count);
    }

    static Page scan(List<String> sortedValues, Options options) {
        int index = Math.min(options.cursor(), sortedValues.size());
        int end = Math.min(sortedValues.size(), index + options.count());
        List<String> matches = new ArrayList<>();
        while (index < end) {
            String value = sortedValues.get(index++);
            if (globMatches(options.pattern(), value)) matches.add(value);
        }
        return new Page(index >= sortedValues.size() ? 0 : index, matches);
    }

    static CommandResponse response(Page page) {
        List<CommandResponse> values = page.values().stream()
                .map(CommandResponses::bulkString)
                .toList();
        return CommandResponses.array(
                CommandResponses.bulkString(Integer.toString(page.nextCursor())),
                CommandResponses.array(values));
    }

    private static boolean globMatches(String pattern, String value) {
        int patternIndex = 0;
        int valueIndex = 0;
        int starIndex = -1;
        int starMatch = -1;
        while (valueIndex < value.length()) {
            if (patternIndex < pattern.length()
                    && (pattern.charAt(patternIndex) == '?' || pattern.charAt(patternIndex) == value.charAt(valueIndex))) {
                patternIndex++;
                valueIndex++;
            } else if (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') {
                starIndex = patternIndex++;
                starMatch = valueIndex;
            } else if (starIndex >= 0) {
                patternIndex = starIndex + 1;
                valueIndex = ++starMatch;
            } else {
                return false;
            }
        }
        while (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') patternIndex++;
        return patternIndex == pattern.length();
    }

    record Options(int cursor, String pattern, int count) {}
    record Page(int nextCursor, List<String> values) {
        Page {
            values = List.copyOf(values);
        }
    }
}
