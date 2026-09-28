package dev.dhruv.streaming.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class UserCodeClassLoaderTest {

    @Test
    void installsUserLoaderAsThreadContextLoader(@TempDir Path userClasses) {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            UserCodeClassLoader.configure(userClasses.toString());

            assertThat(Thread.currentThread().getContextClassLoader())
                    .isSameAs(UserCodeClassLoader.get());
            assertThat(UserCodeClassLoader.get().getParent())
                    .isSameAs(UserCodeClassLoader.class.getClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }
}
