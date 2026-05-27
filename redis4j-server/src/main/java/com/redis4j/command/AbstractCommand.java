package com.redis4j.command;

import com.redis4j.protocol.RedisMessageHelper;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 命令抽象基类
 * 提供统一的参数校验、错误处理和日志
 */
public abstract class AbstractCommand implements Command {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public final RedisMessage execute(String[] args) {
        // 1. 参数校验
        if (!validate(args)) {
            return RedisMessageHelper.error("ERR", getValidationErrorMessage());
        }

        // 2. 执行命令
        try {
            RedisMessage result = doExecute(args);
            // 3. 日志记录
            logExecution(args, result);
            return result;
        } catch (IllegalArgumentException e) {
            return RedisMessageHelper.error("ERR", e.getMessage());
        } catch (Exception e) {
            logger.error("Error executing command: {}", getName(), e);
            return RedisMessageHelper.error("ERR", e.getMessage());
        }
    }

    /**
     * 参数校验
     * @return true 表示参数合法
     */
    protected boolean validate(String[] args) {
        int arity = getArity();
        if (arity > 0) {
            // 固定参数数量
            return args != null && args.length == arity - 1;
        } else if (arity == -1) {
            // 可变参数，至少 1 个
            return args != null && args.length >= 1;
        } else if (arity == -2) {
            // 可变参数，至少 2 个
            return args != null && args.length >= 2;
        }
        return args != null;
    }

    /**
     * 获取参数校验失败的错误消息
     */
    protected String getValidationErrorMessage() {
        int arity = getArity();
        String cmd = getName().toLowerCase();
        if (arity > 0) {
            return "wrong number of arguments for '" + cmd + "' command";
        } else if (arity == -1) {
            return "wrong number of arguments for '" + cmd + "' command (at least 1 required)";
        } else if (arity == -2) {
            return "wrong number of arguments for '" + cmd + "' command (at least 2 required)";
        }
        return "invalid arguments for '" + cmd + "' command";
    }

    /**
     * 具体命令执行逻辑
     */
    protected abstract RedisMessage doExecute(String[] args);

    /**
     * 记录命令执行日志（可选重写）
     */
    protected void logExecution(String[] args, RedisMessage result) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executed {} with args={}, result={}", getName(), java.util.Arrays.toString(args), result.getClass().getSimpleName());
        }
    }
}
