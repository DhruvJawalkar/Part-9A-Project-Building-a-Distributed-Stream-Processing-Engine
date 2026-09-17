package dev.dhruv.streaming.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Optional;

/**
 * Turns user-supplied objects into bytes for the wire, and back.
 *
 * <p>Used for the things that travel in a {@code TaskDeployment}: the operator a task will run,
 * its key selector, its timestamp assigner. All of them are objects the job author constructed
 * in {@code main()} in one process, which have to be reconstituted in another.
 *
 * <p>This is the same Java serialization {@link TaskInstances} uses to give each subtask a
 * private copy, and the constraint it imposes is the same one. Configuration travels in fields;
 * resources are acquired in {@code open()} on the worker that will use them. An operator holding
 * a database connection fails here, with a message saying so, rather than mysteriously later.
 */
public final class SerializationUtil {

    private SerializationUtil() {
    }

    /**
     * Serializes an object.
     *
     * @param value what to serialize
     * @return its bytes
     * @throws IllegalArgumentException if it cannot be serialized
     */
    public static byte[] toBytes(Serializable value) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(value);
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    value.getClass().getName() + " could not be serialized, so it cannot be sent"
                            + " to a worker. Every field must be serializable; resources belong"
                            + " in a transient field populated in open().", e);
        }
    }

    /**
     * Serializes an optional value, or returns empty bytes if there is nothing to send.
     *
     * @param value what to serialize, if present
     * @return its bytes, or an empty array
     */
    public static byte[] toBytesOrEmpty(Optional<? extends Serializable> value) {
        return value.map(SerializationUtil::toBytes).orElseGet(() -> new byte[0]);
    }

    /**
     * Reads an object back.
     *
     * @param bytes bytes produced by {@link #toBytes}
     * @param <T>   the expected type
     * @return the object
     * @throws IllegalArgumentException if the bytes cannot be read, which usually means the
     *                                  job's classes are missing from this worker's classpath
     */
    @SuppressWarnings("unchecked")
    public static <T> T fromBytes(byte[] bytes) {
        try (ObjectInputStream in = new UserCodeObjectInputStream(
                new ByteArrayInputStream(bytes))) {
            return (T) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "could not deserialize a deployed object. The job's classes must be reachable"
                            + " from this process -- see UserCodeClassLoader and the"
                            + " JOB_CLASSPATH environment variable.", e);
        }
    }

    /**
     * An object stream that resolves classes through the user-code loader.
     *
     * <p>The default behaviour is the problem this exists to fix. {@link ObjectInputStream}
     * resolves classes using the nearest classloader on the calling stack, which inside the
     * engine is the engine's own -- and the engine has never heard of {@code ClickEvent}.
     * Overriding {@code resolveClass} points it at the loader that does.
     */
    private static final class UserCodeObjectInputStream extends ObjectInputStream {

        UserCodeObjectInputStream(java.io.InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(java.io.ObjectStreamClass description)
                throws IOException, ClassNotFoundException {
            try {
                return Class.forName(description.getName(), false, UserCodeClassLoader.get());
            } catch (ClassNotFoundException e) {
                return super.resolveClass(description);
            }
        }
    }

    /**
     * Reads an object back, or returns empty if nothing was sent.
     *
     * @param bytes bytes produced by {@link #toBytesOrEmpty}
     * @param <T>   the expected type
     * @return the object, if there was one
     */
    public static <T> Optional<T> fromBytesOrEmpty(byte[] bytes) {
        return bytes.length == 0 ? Optional.empty() : Optional.of(fromBytes(bytes));
    }
}
