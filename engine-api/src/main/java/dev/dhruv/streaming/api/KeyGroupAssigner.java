package dev.dhruv.streaming.api;

/**
 * Maps a key to the subtask responsible for it, in two hops rather than one.
 *
 * <pre>
 *     key  --&gt;  key group  --&gt;  subtask
 *              (fixed forever)  (changes on rescale)
 * </pre>
 *
 * <p>The obvious implementation is {@code hash(key) % parallelism}, and it is wrong for any
 * job that holds state. Change the parallelism from 4 to 5 and every key is suddenly expected
 * at a different subtask than the one holding its state. Restoring a checkpoint would have to
 * reshuffle state key by key, which means reading all of it, which means the operation scales
 * with state size rather than with the change being made.
 *
 * <p>The indirection fixes that. Keys hash into a fixed number of key groups, decided once at
 * job creation and never again. Key groups are assigned to subtasks in contiguous ranges, so
 * rescaling reassigns whole groups rather than individual keys, and each subtask loads a
 * contiguous slice of the snapshot. The first hop never changes; only the second one does.
 *
 * <p>The cost of this is a constraint that real engines impose too and that is worth feeling
 * directly: {@link #NUM_KEY_GROUPS} can never change for a job that has state it wants to
 * keep. It is a ceiling on parallelism chosen before the job has ever run.
 */
public final class KeyGroupAssigner {

    /**
     * Number of key groups. NOT the number of running tasks.
     *
     * <p>Fixed at job creation; changing it invalidates all existing checkpointed state,
     * because every key would hash into a different group. It is also the maximum parallelism
     * the job can ever reach, since a subtask needs at least one group to be worth scheduling.
     *
     * <p>Exposed as the job-level config knob {@code maxParallelism}.
     */
    public static final int NUM_KEY_GROUPS = 128;

    private KeyGroupAssigner() {
    }

    /**
     * Returns the key group a key belongs to. Stable for the life of the job.
     *
     * @param key the key, whose {@code hashCode} must be stable across processes and restarts
     * @return a key group in {@code [0, NUM_KEY_GROUPS)}
     */
    public static int keyGroupFor(Object key) {
        return Math.abs(murmurHash(key.hashCode())) % NUM_KEY_GROUPS;
    }

    /**
     * Returns the subtask that owns a key at a given parallelism.
     *
     * <p>Key groups are assigned to subtasks in contiguous ranges, so rescaling moves whole
     * groups rather than individual keys.
     *
     * @param key         the key
     * @param parallelism the operator's current parallelism
     * @return a subtask index in {@code [0, parallelism)}
     */
    public static int subtaskFor(Object key, int parallelism) {
        return keyGroupFor(key) * parallelism / NUM_KEY_GROUPS;
    }

    /**
     * The MurmurHash3 32-bit finalizer, applied to the key's own {@code hashCode}.
     *
     * <p>Java's {@code hashCode} contracts for good equality behaviour, not for good bit
     * distribution. {@code String.hashCode} in particular clusters badly for the short,
     * similar identifiers that make up most real key spaces, and the clustering survives the
     * modulo. Running the avalanche step over it costs a handful of instructions and turns a
     * lumpy key group distribution into a flat one.
     *
     * @param code the key's hash code
     * @return a well-distributed 32-bit hash
     */
    private static int murmurHash(int code) {
        code *= 0xcc9e2d51;
        code = Integer.rotateLeft(code, 15);
        code *= 0x1b873593;
        code ^= code >>> 16;
        code *= 0x85ebca6b;
        code ^= code >>> 13;
        code *= 0xc2b2ae35;
        code ^= code >>> 16;
        return code;
    }
}
