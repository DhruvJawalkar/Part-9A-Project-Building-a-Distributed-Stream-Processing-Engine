package dev.dhruv.streaming.metadata;

/**
 * Where a job is in its life.
 *
 * <pre>
 *   CREATED --&gt; RUNNING --&gt; FINISHED
 *                  |
 *                  +------&gt; FAILING --&gt; RESTARTING --&gt; RUNNING
 *                                      \\--&gt; FAILED
 * </pre>
 *
 * <p>Phase 4 inserts {@code RESTARTING} between {@code FAILING} and {@code RUNNING}. The
 * transition represents a whole-job rewind to the last completed checkpoint, never a patch to
 * one surviving task.
 *
 * <p>Every transition is written to etcd <em>before</em> it is acted on. The ordering matters:
 * a master that cancelled tasks and then crashed before recording why would restart with no
 * idea that it had been in the middle of failing a job, and would find a half-cancelled job it
 * believed was running.
 */
public enum JobState {

    /** Submitted and persisted, not yet scheduled. */
    CREATED,

    /** Tasks are deployed and processing. */
    RUNNING,

    /** Every task reported finished. Only a bounded source ever reaches this. */
    FINISHED,

    /** Something failed; tasks are being cancelled. */
    FAILING,

    /** Tasks were cancelled and the master is waiting to redeploy all of them from a checkpoint. */
    RESTARTING,

    /** Terminal: no recovery point or no restart attempt remained. */
    FAILED,

    /** Terminal: an operator deliberately stopped the job through the status API. */
    CANCELLED;

    /**
     * Returns whether this state admits no further transitions.
     *
     * @return true for {@code FINISHED}, {@code FAILED} and {@code CANCELLED}
     */
    public boolean isTerminal() {
        return this == FINISHED || this == FAILED || this == CANCELLED;
    }
}
