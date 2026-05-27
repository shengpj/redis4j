package com.redis4j.command;

import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 命令扫描器
 * 自动扫描并注册带 @RedisCommand 注解的命令类
 * 支持目录和 JAR 两种 classpath 资源形式
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
                    // logger.debug("Registered command: {}", name);
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
            Enumeration<URL> resources = classLoader.getResources(path);

            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                String protocol = resource.getProtocol();
                if ("file".equals(protocol)) {
                    commands.addAll(scanDirectory(packageName, path, classLoader, resource));
                } else if ("jar".equals(protocol)) {
                    commands.addAll(scanJar(packageName, resource));
                }
            }
        } catch (Exception e) {
            logger.error("Error scanning package: {}", packageName, e);
        }

        return commands;
    }

    /**
     * 扫描目录形式的 classpath 资源
     */
    private List<Command> scanDirectory(String packageName, String path, ClassLoader classLoader, URL resource) {
        List<Command> commands = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.openStream()))) {
            List<String> classNames = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.endsWith(".class")) {
                    classNames.add(packageName + '.' + line.substring(0, line.length() - 6));
                }
            }
            commands.addAll(instantiateCommands(classNames));
        } catch (IOException e) {
            logger.error("Error reading directory: {}", path, e);
        }
        return commands;
    }

    /**
     * 扫描 JAR 形式的 classpath 资源
     */
    private List<Command> scanJar(String packageName, URL resource) {
        List<Command> commands = new ArrayList<>();
        String jarPath = resource.getPath();
        // 处理 jar:file:/path/to/jar!/path 格式
        int separatorIndex = jarPath.indexOf("!");
        if (separatorIndex < 0) return commands;

        String jarFilePath = jarPath.substring(0, separatorIndex);
        // 处理 file: 前缀
        if (jarFilePath.startsWith("file:")) {
            jarFilePath = jarFilePath.substring(5);
        }

        String prefix = packageName.replace('.', '/') + '/';

        try (JarFile jarFile = new JarFile(jarFilePath)) {
            List<String> classNames = new ArrayList<>();
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                if (entryName.startsWith(prefix) && entryName.endsWith(".class") && entryName.indexOf('/', prefix.length()) < 0) {
                    String className = entryName.substring(0, entryName.length() - 6).replace('/', '.');
                    classNames.add(className);
                }
            }
            commands.addAll(instantiateCommands(classNames));
        } catch (IOException e) {
            logger.error("Error scanning JAR: {}", jarFilePath, e);
        }
        return commands;
    }

    /**
     * 实例化有效的命令类
     */
    private List<Command> instantiateCommands(List<String> classNames) {
        List<Command> commands = new ArrayList<>();
        for (String className : classNames) {
            try {
                Class<?> clazz = Class.forName(className);
                if (clazz.isAnnotationPresent(RedisCommand.class) &&
                        Command.class.isAssignableFrom(clazz) &&
                        !clazz.isInterface() &&
                        !Modifier.isAbstract(clazz.getModifiers())) {

                    Constructor<?> constructor = clazz.getDeclaredConstructor(DataStore.class);
                    Command command = (Command) constructor.newInstance(dataStore);
                    commands.add(command);
                }
            } catch (ClassNotFoundException e) {
                logger.trace("Could not load class: {}", className);
            } catch (NoSuchMethodException e) {
                logger.trace("Class {} has no (DataStore) constructor, skipping", className);
            } catch (Exception e) {
                logger.trace("Could not instantiate command class: {}", className, e);
            }
        }
        return commands;
    }
}
