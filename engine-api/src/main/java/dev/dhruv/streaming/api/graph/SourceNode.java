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
 * @param id                 stable operator id
 * @param parallelism        number of parallel subtasks
 * @param source             the source implementation, serialized to whichever worker runs it
 * @param timestampAssigner  how to read event time out of a record; empty means this source
 *                           has no event time configured
 * @param outOfOrderness     how far behind the maximum seen event time the watermark is held.
 *                           The job's tolerance for lateness, paid for in latency: every
 *                           window closes this much later than it otherwise could.
 * @param idleTimeout        how long a subtask may produce nothing before it is marked idle
 *                           and excluded from the downstream watermark minimum. Without this,
 *                           one silent partition holds the event-time clock of the entire job
 *                           at its last value and all output stops.
 */
public record SourceNode(
        String id,
        int parallelism,
        Source<?> source,
        Optional<TimestampAssigner<?>> timestampAssigner,
        Duration outOfOrderness,
        Duration idleTimeout
) implements LogicalOperator {

    @Override
    public List<String> upstreamIds() {
        return List.of();
    }
}
