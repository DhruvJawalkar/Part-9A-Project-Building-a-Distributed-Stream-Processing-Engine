package dev.dhruv.streaming.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads the job's classes, which the engine was not compiled against.
 *
 * <h2>The problem this solves</h2>
 *
 * <p>A worker is a general-purpose process. It is built and started with no knowledge of any
 * particular job, and then asked to run one -- which means deserializing a {@code BotFilter} and
 * a {@code ClickEvent} it has never heard of. Java deserialization resolves classes through the
 * classloader that happens to be in scope, and the engine's own loader does not have them, so
 * the deployment fails with a {@code ClassNotFoundException} that is entirely correct and
 * entirely unhelpful.
 *
 * <p>Every real engine has this problem and every one of them solves it by separating engine
 * classes from user classes. Flink calls the result the user-code classloader; Spark ships a JAR
 * to executors; Storm distributes a topology JAR. The shapes differ, the necessity does not: a
 * process that runs arbitrary user code must be able to load classes it was never compiled
 * against.
 *
 * <h2>The shortcut this project takes</h2>
 *
 * <p>A production engine would ship the job's JAR as part of submission, so that a worker
 * receives the code along with the instruction to run it. Here the worker is simply told where
 * the classes already are, through the {@code JOB_CLASSPATH} environment variable, and it builds
 * a loader over them at startup.
 *
 * <p>That is a real simplification with a real consequence, and it is worth naming rather than
 * glossing: every worker must be given the same classpath, and changing the job means restarting
 * the workers. Shipping the JAR at submission is what removes both limits.
 */
public final class UserCodeClassLoader {

    private static final Logger log = LoggerFactory.getLogger(UserCodeClassLoader.class);

    private static volatile ClassLoader instance = UserCodeClassLoader.class.getClassLoader();

    private UserCodeClassLoader() {
    }

    /**
     * Builds the loader from a classpath and installs it.
     *
     * @param classpath entries separated by the platform path separator. Each may be a jar or a
     *                  directory of classes. An empty or blank value leaves the engine's own
     *                  loader in place, which is correct when the job runs in this same process.
     */
    public static void configure(String classpath) {
        if (classpath == null || classpath.isBlank()) {
            log.debug("no JOB_CLASSPATH set; user classes must already be on the engine classpath");
            return;
        }

        List<URL> urls = new ArrayList<>();
        for (String entry : classpath.split(File.pathSeparator)) {
            if (entry.isBlank()) {
                continue;
            }
            try {
                urls.add(new File(entry.trim()).toURI().toURL());
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("not a usable classpath entry: " + entry, e);
            }
        }

        // Parent is the engine's loader, so engine types resolve to the one definition that
        // exists. Only classes the engine does not have -- the user's -- come from these URLs.
        instance = new URLClassLoader(
                "user-code", urls.toArray(URL[]::new), UserCodeClassLoader.class.getClassLoader());

        log.info("user code will be loaded from {} classpath entr{}",
                urls.size(), urls.size() == 1 ? "y" : "ies");
    }

    /**
     * Returns the loader to resolve user classes through.
     *
     * @return the user-code loader, or the engine's own if none was configured
     */
    public static ClassLoader get() {
        return instance;
    }
}
