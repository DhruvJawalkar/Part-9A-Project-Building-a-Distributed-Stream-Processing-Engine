package dev.dhruv.streaming.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Gives each subtask its own copy of the user's operator or source, by serializing it and
 * reading it back.
 *
 * <h2>Why this is not an optimisation to skip</h2>
 *
 * <p>A job graph holds one instance of each operator: the object the user constructed in
 * {@code main()}. An operator running four ways needs four, because {@code open()} puts
 * resources in its fields -- a Kafka consumer, a file handle, a writer -- and four subtasks
 * sharing one object means four threads sharing one of those. The failure is immediate and
 * loud with a Kafka consumer, which checks. With a plain field it is silent, and shows up as
 * records vanishing.
 *
 * <p>A distributed engine never notices this problem, because shipping a task to a worker
 * deserializes it there and every subtask gets a private copy for free. That is exactly why
 * doing the same thing here matters: without it, a job would behave one way in a single JVM and
 * another way once distributed, and the local executor would be lying about what the engine
 * does.
 *
 * <p>Doing it by serialization rather than by cloning has a second benefit. It fails at
 * startup, in Phase 1, on any operator that is not actually serializable -- rather than in
 * Phase 2, on deployment, with a much longer path between the mistake and the message.
 */
final class TaskInstances {

    private TaskInstances() {
    }

    /**
     * Returns an independent copy of a serializable user function.
     *
     * @param original the instance held in the job graph
     * @param <T>      the function type
     * @return a private copy for one subtask
     * @throws IllegalArgumentException if the instance cannot be serialized, which in practice
     *                                  means it holds a field that should have been transient
     *                                  and populated in {@code open()} instead
     */
    @SuppressWarnings("unchecked")
    static <T> T copyOf(T original) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(original);
            }
            try (UserCodeObjectInputStream in =
                         new UserCodeObjectInputStream(
                                 new ByteArrayInputStream(bytes.toByteArray()))) {
                return (T) in.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    original.getClass().getName() + " could not be serialized, so it cannot be"
                            + " given to a subtask. Every field must be serializable; resources"
                            + " belong in a transient field populated in open().", e);
        }
    }
}
