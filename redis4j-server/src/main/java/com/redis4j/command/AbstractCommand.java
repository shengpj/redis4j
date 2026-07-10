package com.redis4j.command;

import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.protocol.response.CommandResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Template method for command validation, execution and error handling. */
public abstract class AbstractCommand implements Command {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public final CommandResponse execute(String[] args) {
        if (!validate(args)) {
            return CommandResponses.error("ERR " + getValidationErrorMessage());
        }
        try {
            CommandResponse result = doExecute(args);
            logExecution(args, result);
            return result;
        } catch (IllegalArgumentException e) {
            return CommandResponses.error("ERR " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error executing command: {}", getName(), e);
            return CommandResponses.error("ERR " + e.getMessage());
        }
    }

    protected boolean validate(String[] args) {
        return args != null && metadata().arity().accepts(args.length);
    }

    protected String getValidationErrorMessage() {
        int arity = getArity();
        String command = getName().toLowerCase();
        if (arity > 0) return "wrong number of arguments for '" + command + "' command";
        return "wrong number of arguments for '" + command + "' command (at least "
                + Math.abs(arity) + " required)";
    }

    protected abstract CommandResponse doExecute(String[] args);

    protected long parseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
    }

    protected void logExecution(String[] args, CommandResponse result) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executed {} with args={}, result={}", getName(),
                    java.util.Arrays.toString(args), formatResult(result));
        }
    }

    private String formatResult(CommandResponse response) {
        if (response instanceof CommandResponse.SimpleString value) return value.value();
        if (response instanceof CommandResponse.Error value) return value.value();
        if (response instanceof CommandResponse.IntegerValue value) return String.valueOf(value.value());
        if (response instanceof CommandResponse.BulkString value) return value.value() == null ? "(nil)" : value.value();
        if (response instanceof CommandResponse.ArrayValue value) return value.values() == null ? "(nil)" : value.values().size() + " elements";
        return response.getClass().getSimpleName();
    }
}
