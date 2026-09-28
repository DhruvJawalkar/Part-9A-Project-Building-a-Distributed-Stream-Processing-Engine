package dev.dhruv.streaming.master;

import dev.dhruv.streaming.metadata.CompletedCheckpoint;
import dev.dhruv.streaming.metadata.MetadataStore;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;

/**
 * Turns independently acknowledged task snapshots into one consistent checkpoint.
 *
 * <p>The coordinator injects a barrier <em>only at sources</em>. Every other task observes that
 * barrier in normal input order, aligns it if necessary, snapshots, forwards it, and then sends
 * an acknowledgement here. It is tempting to ask every task to snapshot directly; that would
 * snapshot each operator at a different point in the stream and create a recovery point that
 * never existed in a running job.
 *
 * <p>Only a complete acknowledgement set is durable. An incomplete checkpoint is merely a
 * failed attempt and is discarded on timeout, which is safer than offering recovery a mixture
 * of old and new task state.
 */
public final class CheckpointCoordinator implements AutoCloseable {

    /** In-memory operational counters; durable recovery remains the latest completed pointer. */
    public record Status(long intervalMillis, long completed, long failed,
                         List<Completed> history) {
    }

    /** Aggregate facts from one fully acknowledged checkpoint. */
    public record Completed(long id, long durationMs, long stateBytes, long alignmentMs,
                            long completedAt) {
    }

    /** Checkpoint cadence and the maximum time an in-flight barrier may take. */
    public record Config(Duration interval, Duration timeout) {
        public Config {
            Objects.requireNonNull(interval, "interval");
            Objects.requireNonNull(timeout, "timeout");
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("checkpoint interval must be positive");
            }
            if (timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("checkpoint timeout must be positive");
            }
        }
    }

    /** Stable identity of a task in acknowledgements and checkpoint metadata. */
    public record TaskKey(String operatorId, int subtaskIndex) {
        public TaskKey {
            if (operatorId == null || operatorId.isBlank()) {
                throw new IllegalArgumentException("operatorId must not be blank");
            }
            if (subtaskIndex < 0) {
                throw new IllegalArgumentException("subtaskIndex must not be negative");
            }
        }

        public String asMetadataKey() {
            return operatorId + ":" + subtaskIndex;
        }
    }

    /** What the coordinator needs from the data plane. */
    public interface Actions {
        /** Injects the barrier at sources only. */
        void triggerSources(String jobId, long checkpointId, long triggerTimestamp);

        /** Reaches sinks only after the entire checkpoint has become durable. */
        void notifySinks(String jobId, long checkpointId);

        /** Lets the owner surface a timeout without treating it as a task failure. */
        default void checkpointAborted(String jobId, long checkpointId, String reason) {
        }
    }

    private final MetadataStore metadata;
    private final Config config;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final Map<String, JobCheckpoints> jobs = new LinkedHashMap<>();

    public CheckpointCoordinator(MetadataStore metadata, Config config) {
        this(metadata, config, Clock.systemUTC(), daemonScheduler());
    }

    CheckpointCoordinator(MetadataStore metadata,
                          Config config,
                          Clock clock,
                          ScheduledExecutorService scheduler) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /** Starts periodic checkpoint attempts for a running job. */
    public synchronized void start(String jobId, Set<TaskKey> tasks, Actions actions) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(tasks, "tasks");
        Objects.requireNonNull(actions, "actions");
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("cannot checkpoint a job with no tasks");
        }
        stop(jobId);
        Optional<CompletedCheckpoint> previous = metadata.getLatestCompletedCheckpoint(jobId);
        JobCheckpoints job = new JobCheckpoints(Set.copyOf(tasks), actions,
                previous.map(CompletedCheckpoint::checkpointId).orElse(0L));
        jobs.put(jobId, job);
        job.periodic = scheduler.scheduleWithFixedDelay(
                () -> trigger(jobId), config.interval().toMillis(), config.interval().toMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /** Stops periodic work and abandons any non-durable in-flight checkpoint. */
    public synchronized void stop(String jobId) {
        JobCheckpoints job = jobs.remove(jobId);
        if (job != null) {
            if (job.periodic != null) {
                job.periodic.cancel(false);
            }
            abort(jobId, job, "job stopped before checkpoint completed");
        }
    }

    /**
     * Triggers now, primarily useful for a deterministic test or an explicit demo.
     *
     * @return the new checkpoint id, or empty when one is already in flight
     */
    public synchronized Optional<Long> trigger(String jobId) {
        JobCheckpoints job = jobs.get(jobId);
        if (job == null || job.inFlight != null) {
            return Optional.empty();
        }
        long checkpointId = ++job.lastCheckpointId;
        long timestamp = clock.millis();
        InFlight checkpoint = new InFlight(checkpointId, timestamp, job.tasks);
        job.inFlight = checkpoint;
        checkpoint.timeout = scheduler.schedule(() -> timeout(jobId, checkpointId),
                config.timeout().toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        try {
            job.actions.triggerSources(jobId, checkpointId, timestamp);
        } catch (RuntimeException failure) {
            abort(jobId, job, "could not inject source barriers: " + failure.getMessage());
            throw failure;
        }
        return Optional.of(checkpointId);
    }

    /** Receives one task's acknowledgement. Duplicates and stale acknowledgements are harmless. */
    public synchronized void acknowledge(String jobId,
                                         TaskKey task,
                                         long checkpointId,
                                         String stateHandleUri,
                                         long alignmentMillis,
                                         long stateSizeBytes) {
        JobCheckpoints job = jobs.get(jobId);
        if (job == null || job.inFlight == null || job.inFlight.id != checkpointId) {
            return;
        }
        if (!job.tasks.contains(task)) {
            throw new IllegalArgumentException("checkpoint acknowledgement from unexpected task "
                    + task.asMetadataKey() + " for job " + jobId);
        }
        if (alignmentMillis < 0 || stateSizeBytes < 0) {
            throw new IllegalArgumentException("checkpoint measurements must not be negative");
        }
        job.inFlight.acks.putIfAbsent(task, new CompletedCheckpoint.TaskState(
                Objects.requireNonNull(stateHandleUri, "stateHandleUri"), stateSizeBytes,
                alignmentMillis));
        if (job.inFlight.acks.size() != job.tasks.size()) {
            return;
        }

        InFlight complete = job.inFlight;
        Map<String, CompletedCheckpoint.TaskState> handles = new LinkedHashMap<>();
        complete.acks.forEach((key, value) -> handles.put(key.asMetadataKey(), value));
        // Durable before sink notification: a sink commit may survive a master crash, so the
        // recovery pointer must already describe the interval the sink is permitted to expose.
        try {
            metadata.putLatestCompletedCheckpoint(jobId,
                    new CompletedCheckpoint(complete.id, complete.triggerTimestamp, handles));
        } catch (RuntimeException failure) {
            abort(jobId, job, "could not persist completed checkpoint: " + failure.getMessage());
            throw failure;
        }
        complete.timeout.cancel(false);
        job.inFlight = null;
        job.recordCompleted(complete, clock.millis());
        job.actions.notifySinks(jobId, complete.id);
    }

    /** Returns the currently in-flight id, useful to status reporting without exposing mutability. */
    public synchronized Optional<Long> inFlightCheckpoint(String jobId) {
        JobCheckpoints job = jobs.get(jobId);
        return job == null || job.inFlight == null ? Optional.empty() : Optional.of(job.inFlight.id);
    }

    /** Configured interval shared by every job this coordinator owns. */
    public long intervalMillis() {
        return config.interval().toMillis();
    }

    /** Returns counters known by this coordinator instance without inventing durable history. */
    public synchronized Status status(String jobId) {
        JobCheckpoints job = jobs.get(jobId);
        if (job == null) {
            return new Status(config.interval().toMillis(), 0, 0, List.of());
        }
        return new Status(config.interval().toMillis(), job.completed, job.failed, List.copyOf(job.history));
    }

    private synchronized void timeout(String jobId, long checkpointId) {
        JobCheckpoints job = jobs.get(jobId);
        if (job != null && job.inFlight != null && job.inFlight.id == checkpointId) {
            abort(jobId, job, "timed out after " + config.timeout().toMillis() + "ms with "
                    + job.inFlight.acks.size() + "/" + job.tasks.size() + " task acknowledgements");
        }
    }

    private void abort(String jobId, JobCheckpoints job, String reason) {
        InFlight checkpoint = job.inFlight;
        if (checkpoint == null) {
            return;
        }
        checkpoint.timeout.cancel(false);
        job.inFlight = null;
        job.failed++;
        job.actions.checkpointAborted(jobId, checkpoint.id, reason);
    }

    @Override
    public synchronized void close() {
        new LinkedHashSet<>(jobs.keySet()).forEach(this::stop);
        scheduler.shutdownNow();
    }

    private static ScheduledExecutorService daemonScheduler() {
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "checkpoint-coordinator");
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }

    private static final class JobCheckpoints {
        private final Set<TaskKey> tasks;
        private final Actions actions;
        private long lastCheckpointId;
        private ScheduledFuture<?> periodic;
        private InFlight inFlight;
        private long completed;
        private long failed;
        private final java.util.ArrayDeque<Completed> history = new java.util.ArrayDeque<>();

        private JobCheckpoints(Set<TaskKey> tasks, Actions actions, long lastCheckpointId) {
            this.tasks = tasks;
            this.actions = actions;
            this.lastCheckpointId = lastCheckpointId;
        }

        private void recordCompleted(InFlight checkpoint, long completedAt) {
            completed++;
            long stateBytes = checkpoint.acks.values().stream()
                    .mapToLong(CompletedCheckpoint.TaskState::stateSizeBytes).sum();
            long alignmentMs = checkpoint.acks.values().stream()
                    .mapToLong(CompletedCheckpoint.TaskState::alignmentMillis).max().orElse(0);
            history.addLast(new Completed(checkpoint.id, Math.max(0, completedAt - checkpoint.triggerTimestamp),
                    stateBytes, alignmentMs, completedAt));
            while (history.size() > 20) {
                history.removeFirst();
            }
        }
    }

    private static final class InFlight {
        private final long id;
        private final long triggerTimestamp;
        private final Map<TaskKey, CompletedCheckpoint.TaskState> acks = new LinkedHashMap<>();
        private ScheduledFuture<?> timeout;

        private InFlight(long id, long triggerTimestamp, Set<TaskKey> ignored) {
            this.id = id;
            this.triggerTimestamp = triggerTimestamp;
        }
    }
}
