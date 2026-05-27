package com.redis4j.command;

import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * 命令扫描器
 * 自动扫描并注册带 @RedisCommand 注解的命令类
 */
public class CommandScanner {

    private static final Logger logger = LoggerFactory.getLogger(CommandScanner.class);

    private final DataStore dataStore;

    public CommandScanner(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * 扫描指定包下的命令类并注册
     */
    public Map<String, Command> scanAndRegister(String... packageNames) {
        Map<String, Command> commands = new HashMap<>();

        for (String packageName : packageNames) {
            scanPackage(packageName).forEach(cmd -> {
                String name = cmd.getName().toUpperCase();
                if (commands.containsKey(name)) {
                    logger.warn("Command {} already registered, skipping", name);
                } else {
                    commands.put(name, cmd);
                    logger.debug("Registered command: {}", name);
                }
            });
        }

        logger.info("Scanned and registered {} commands from packages: {}", commands.size(), Arrays.toString(packageNames));
        return commands;
    }

    /**
     * 扫描指定包下的所有命令类
     */
    private List<Command> scanPackage(String packageName) {
        List<Command> commands = new ArrayList<>();

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            String path = packageName.replace('.', '/');
            Enumeration<java.net.URL> resources = classLoader.getResources(path);

            List<String> classNames = new ArrayList<>();
            while (resources.hasMoreElements()) {
                java.net.URL resource = resources.nextElement();
                java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(resource.openStream()));

                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.endsWith(".class")) {
                        classNames.add(packageName + '.' + line.substring(0, line.length() - 6));
                    }
                }
                reader.close();
            }

            for (String className : classNames) {
                try {
                    Class<?> clazz = classLoader.loadClass(className);
                    if (clazz.isAnnotationPresent(RedisCommand.class) &&
                        Command.class.isAssignableFrom(clazz) &&
                        !clazz.isInterface() &&
                        !Modifier.isAbstract(clazz.getModifiers())) {

                        Constructor<?> constructor;
                        try {
                            constructor = clazz.getDeclaredConstructor(DataStore.class);
                        } catch (NoSuchMethodException e) {
                            logger.trace("Class {} has no (DataStore) constructor, skipping", className);
                            continue;
                        }
                        Command command = (Command) constructor.newInstance(dataStore);
                        commands.add(command);
                    }
                } catch (ClassNotFoundException e) {
                    logger.trace("Could not load class: {}", className);
                }
            }
        } catch (Exception e) {
            logger.error("Error scanning package: {}", packageName, e);
        }

        return commands;
    }

    private static class Modifier {
        public static boolean isAbstract(int mod) {
            return (mod & java.lang.reflect.Modifier.ABSTRACT) != 0;
        }
    }
}
