package dev.dhruv.streaming.api.graph;

/**
 * Thrown when a job graph is not something the engine could run.
 *
 * <p>Every one of these is raised at build time, in the client process, before a single task
 * is scheduled. That is the entire point of validating the graph: a job with a cycle in it or
 * an operator at parallelism zero is broken in a way no amount of running will reveal usefully,
 * and the failure should land where the mistake was made rather than as a strange symptom on a
 * worker three minutes later.
 */
public class InvalidJobGraphException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message what is wrong with the graph, specifically enough to fix it
     */
    public InvalidJobGraphException(String message) {
        super(message);
    }
}
