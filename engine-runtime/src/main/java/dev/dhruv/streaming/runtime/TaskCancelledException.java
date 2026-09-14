package dev.dhruv.streaming.runtime;

/**
 * Thrown to unwind a task thread that has been cancelled mid-record.
 *
 * <p>Cancellation reaches a task as a thread interrupt, which surfaces wherever the task
 * happens to be blocked -- and that may well be inside a call an operator made. Operator code
 * should not have to know about cancellation, so the interrupt is converted here into an
 * unchecked exception that unwinds cleanly through user code to the run loop, which recognises
 * it and shuts the task down quietly rather than reporting a failure.
 *
 * <p>A cancelled task is not a failed task. Telling them apart matters from Phase 4 on, where
 * recovery cancels every task of a job on purpose and must not mistake its own tidying for the
 * fault it is recovering from.
 */
public class TaskCancelledException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message where the task was when it was cancelled
     */
    public TaskCancelledException(String message) {
        super(message);
    }
}
