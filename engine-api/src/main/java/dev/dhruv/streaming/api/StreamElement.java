package dev.dhruv.streaming.api;

/**
 * Everything that travels between two tasks is a {@code StreamElement}: either a data
 * record, a watermark, or a checkpoint barrier.
 *
 * <p>Carrying control elements <em>in band</em> with the data, on the same channel and in
 * the same order, is the single most important structural decision in this engine. It is
 * what makes both watermark propagation and barrier alignment possible: a task can reason
 * about "everything before this marker" purely from the order elements arrive in, with no
 * side channel to correlate against and no clock to trust.
 *
 * <p>The hierarchy is sealed because the runtime's task loop switches exhaustively over it.
 * Adding a fourth kind of element should be a deliberate act that makes the compiler point
 * at every place that has to care.
 *
 * @see StreamRecord
 * @see Watermark
 * @see CheckpointBarrier
 */
public sealed interface StreamElement
        permits StreamRecord, Watermark, CheckpointBarrier {
}
