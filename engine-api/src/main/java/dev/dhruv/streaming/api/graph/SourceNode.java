package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.TimestampAssigner;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * A node that produces records rather than consuming them.
 *
 * <p>Sources carry the job's entire event-time configuration, which is the right place for it:
 * event time enters the job exactly here and is only propagated afterwards. A source with no
 * {@link #timestampAssigner()} produces records the engine cannot reason about temporally, and
 * any window downstream of it will never fire.
 *
 * <h2>Why a nullable component with an Optional accessor</h2>
 *
 * <p>{@link Optional} is deliberately not serializable -- it was designed as a return type, not
 * a field. This graph <em>is</em> serialized, into etcd at submission and into a task deployment
 * on its way to a worker, so an {@code Optional} component would fail the moment a job left the
 * client process.
 *
 * <p>So the component is nullable and carries an {@code OrNull} suffix to say so, while the
 * accessor callers actually use returns {@code Optional}. The engine's own rule -- never null in
 * a public API -- is kept where it matters, at the boundary people call.
 *
 * @param id                    stable operator id
 * @param parallelism           number of parallel subtasks
 * @param source                the source implementation, serialized to whichever worker runs it
 * @param timestampAssignerOrNull how to read event time out of a record; null if this source has
 *                              no event time configured. Prefer {@link #timestampAssigner()}.
 * @param outOfOrderness        how far behind the maximum seen event time the watermark is held.
 *                              The job's tolerance for lateness, paid for in latency: every
 *                              window closes this much later than it otherwise could.
 * @param idleTimeout           how long a subtask may produce nothing before it is marked idle
 *                              and excluded from the downstream watermark minimum. Without this,
 *                              one silent partition holds the event-time clock of the entire job
 *                              at its last value and all output stops.
 */
public record SourceNode(
        String id,
        int parallelism,
        Source<?> source,
        TimestampAssigner<?> timestampAssignerOrNull,
        Duration outOfOrderness,
        Duration idleTimeout
) implements LogicalOperator {

    private static final long serialVersionUID = 1L;

    /**
     * Returns how to read event time out of this source's records, if it was configured.
     *
     * @return the timestamp assigner, if any
     */
    public Optional<TimestampAssigner<?>> timestampAssigner() {
        return Optional.ofNullable(timestampAssignerOrNull);
    }

    @Override
    public List<String> upstreamIds() {
        return List.of();
    }
}
