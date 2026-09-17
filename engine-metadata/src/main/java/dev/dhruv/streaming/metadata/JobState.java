package dev.dhruv.streaming.metadata;

/**
 * Where a job is in its life.
 *
 * <pre>
 *   CREATED --&gt; RUNNING --&gt; FINISHED
 *                  |
 *                  +------&gt; FAILING --&gt; FAILED
 * </pre>
 *
 * <p>Phase 4 inserts {@code RESTARTING} between {@code FAILING} and {@code RUNNING}. Phase 2
 * has no checkpoint to recover to, so a failure here is terminal -- which is worth experiencing
 * before building the machinery that avoids it.
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

    /** Terminal. Phase 4 replaces most paths here with a restart from the last checkpoint. */
    FAILED;

    /**
     * Returns whether this state admits no further transitions.
     *
     * @return true for {@code FINISHED} and {@code FAILED}
     */
    public boolean isTerminal() {
        return this == FINISHED || this == FAILED;
    }
}
