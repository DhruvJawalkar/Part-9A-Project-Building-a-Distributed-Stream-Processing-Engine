package dev.dhruv.streaming.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Java object stream that resolves job-owned classes through the deployed user-code loader.
 *
 * <p>User types appear in more places than the submitted operator itself: network records,
 * keyed values, timer keys, and operator-owned checkpoint state can all contain them. Every
 * deserialization boundary therefore uses this stream rather than relying on the engine
 * application classloader by accident.
 */
public final class UserCodeObjectInputStream extends ObjectInputStream {

    public UserCodeObjectInputStream(InputStream input) throws IOException {
        super(input);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass description)
            throws IOException, ClassNotFoundException {
        try {
            return Class.forName(description.getName(), false, UserCodeClassLoader.get());
        } catch (ClassNotFoundException ignored) {
            return super.resolveClass(description);
        }
    }
}
