package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.CheckpointableSource;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.SourceLagReporter;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.TimestampAssigner;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Objects;
import java.nio.file.Path;
import java.nio.file.Files;
import java.net.URI;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * One running instance of one source.
 *
 * <p>A source task has no input gate, which is the one structural way it differs from every
 * other task: records originate here rather than arriving. So instead of a loop that waits on
 * inputs, it has a loop that asks the source for whatever it has.
 *
 * <p>The pull shape is what makes backpressure reach all the way to the broker. When the job
 * downstream slows, this task blocks inside {@code emit} -- on a full queue locally, or waiting
 * for credit across the network -- which means it stops calling {@link Source#poll}, which means
 * nothing is fetched. No signal has to travel backwards and no component has to decide what to
 * discard. The source simply is not asked.
 */
public final class SourceTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SourceTask.class);

    private final String taskId;
    private final Source<Object> source;
    private final RuntimeSourceContext context;
    private final Output output;
    private final Optional<TimestampAssigner<Object>> timestampAssigner;
    private final Optional<BoundedOutOfOrdernessGenerator> watermarkGenerator;
    private final TaskMetricGroup metrics;
    private final Counter recordsOut;

    private volatile boolean running = true;
    private volatile boolean started;
    private volatile long currentWatermark = Long.MIN_VALUE;
    private volatile Optional<StateHandle> restoreHandle = Optional.empty();
    private volatile long lastCheckpointId;
    private volatile long lastCheckpointDurationMillis;
    private volatile long lastCheckpointStateBytes;
    private final ConcurrentLinkedQueue<CheckpointRequest> pendingCheckpoints = new ConcurrentLinkedQueue<>();

    /** See {@link OperatorTask}: nothing listens in Phase 1, the worker listens from Phase 2. */
    private BiConsumer<String, Throwable> failureListener = (taskId, failure) -> {
    };
    private Consumer<CheckpointResult> checkpointListener = result -> {
    };

    /**
     * Creates a source task.
     *
     * @param taskId            identity for logs and for reporting failures
     * @param source            the user's source, already a private copy for this subtask
     * @param context           what this subtask should read
     * @param output            where records go
     * @param timestampAssigner how to read event time out of a record, if configured
     * @param metrics           this subtask's metric group
     */
    @SuppressWarnings("unchecked")
    public SourceTask(String taskId,
                      Source<?> source,
                      RuntimeSourceContext context,
                      Output output,
                      Optional<? extends TimestampAssigner<?>> timestampAssigner,
                      TaskMetricGroup metrics) {
        this(taskId, source, context, output, timestampAssigner, 0, 0, metrics);
    }

    /**
     * Creates an event-time aware source task.
     *
     * <p>Idleness is emitted in band as a watermark status rather than inferred from an absent
     * watermark downstream. Absence is ambiguous on a distributed channel: it can mean a slow
     * sender, a congested network, or a genuinely idle source.
     */
    @SuppressWarnings("unchecked")
    public SourceTask(String taskId,
                      Source<?> source,
                      RuntimeSourceContext context,
                      Output output,
                      Optional<? extends TimestampAssigner<?>> timestampAssigner,
                      long outOfOrdernessMillis,
                      long idleTimeoutMillis,
                      TaskMetricGroup metrics) {
        this.taskId = taskId;
        this.source = (Source<Object>) source;
        this.context = context;
        this.output = output;
        this.timestampAssigner = (Optional<TimestampAssigner<Object>>) timestampAssigner;
        this.watermarkGenerator = this.timestampAssigner.map(ignored ->
                new BoundedOutOfOrdernessGenerator(outOfOrdernessMillis,
                        idleTimeoutMillis == 0 ? Long.MAX_VALUE : idleTimeoutMillis));
        this.metrics = metrics;
        this.recordsOut = metrics.counter("records-out");
    }

    @Override
    public void run() {
        log.info("source task {} starting", taskId);
        try {
            started = true;
            source.open(context);
            restoreHandle.ifPresent(handle -> {
                try {
                    restoreCheckpoint(handle);
                } catch (Exception e) {
                    throw new SourceRestoreException(e);
                }
            });

            Collector<Object> collector = new SourceCollector();
            boolean moreAvailable = true;
            while (running && moreAvailable) {
                moreAvailable = source.poll(collector);
                emitPeriodicWatermark();
                injectPendingCheckpoints();
                // Ship whatever this poll produced rather than holding a partial buffer until
                // the next one fills it. A source that polls every 200ms and batches by size
                // alone would add the poll interval to every record's latency.
                output.flush();
            }

            if (!moreAvailable) {
                // The source is permanently exhausted. MAX closes every window still open
                // downstream, so a bounded replay finishes cleanly instead of leaving its last
                // sessions unemitted. An unbounded source never reaches this.
                log.info("source task {} reached end of stream", taskId);
                output.broadcast(Watermark.MAX);
                currentWatermark = Long.MAX_VALUE;
                output.flush();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("source task {} interrupted, stopping", taskId);
        } catch (TaskCancelledException e) {
            log.info("source task {} cancelled: {}", taskId, e.getMessage());
        } catch (Exception e) {
            // Connectors commonly surface an interrupt/wakeup as their own runtime exception.
            // Once cancellation has fenced this task, that exception is the expected way out,
            // not a second job failure from a task the master deliberately stopped.
            if (!running) {
                log.info("source task {} stopped during connector poll", taskId);
            } else {
                log.error("source task {} failed", taskId, e);
                failureListener.accept(taskId, e);
            }
        } finally {
            closeQuietly();
            log.info("source task {} stopped after {} records", taskId, recordsOut.count());
        }
    }

    /**
     * Asks this task to stop once the source returns from its current poll.
     */
    public void cancel() {
        running = false;
    }

    /**
     * Returns this task's id.
     *
     * @return the task id
     */
    public String taskId() {
        return taskId;
    }

    /**
     * Returns this task's metrics.
     *
     * @return the metric group
     */
    public TaskMetricGroup metrics() {
        return metrics;
    }

    /** Returns the source's cached lag observation without touching the source client. */
    public Optional<SourceLagReporter.SourceLag> sourceLag() {
        return source instanceof SourceLagReporter reporter ? reporter.sourceLag() : Optional.empty();
    }

    /** Whether this source can ever report input lag. */
    public boolean supportsSourceLag() {
        return source instanceof SourceLagReporter;
    }

    /** Last emitted event-time progress, safely published to the control plane. */
    public long currentWatermark() {
        return currentWatermark;
    }

    /**
     * Registers what to do when this task fails.
     *
     * @param listener called with the task id and the failure
     */
    public void onFailure(BiConsumer<String, Throwable> listener) {
        this.failureListener = listener;
    }

    /**
     * Restores a source position immediately after {@link Source#open(SourceContext)} and before
     * the first poll. Calling this after the task has started would mix two input prefixes, so it
     * is deliberately rejected.
     *
     * @param handle a completed source checkpoint handle
     */
    public void restore(StateHandle handle) {
        if (started) {
            throw new IllegalStateException("cannot restore a running source task " + taskId);
        }
        restoreHandle = Optional.of(handle);
    }

    /**
     * Requests that the source snapshot its next input position and emit a checkpoint barrier.
     *
     * <p>The request is consumed by the source task thread between polls. This matters because a
     * Kafka consumer is single-threaded; taking a snapshot from the gRPC handler would race the
     * poll loop. The ordering is the source half of Chandy-Lamport: all records before the
     * injected marker are in the saved input prefix, while the marker prevents downstream tasks
     * from including later records in the same checkpoint.
     *
     * @param barrier       checkpoint marker to inject
     * @param checkpointDir task-specific durable checkpoint directory
     */
    public void triggerCheckpoint(CheckpointBarrier barrier, Path checkpointDir) {
        pendingCheckpoints.add(new CheckpointRequest(barrier, checkpointDir));
    }

    /** Runs on the source task thread, between poll calls, because KafkaConsumer is not thread-safe. */
    private void injectPendingCheckpoints() throws Exception {
        CheckpointRequest request;
        while ((request = pendingCheckpoints.poll()) != null) {
            injectCheckpoint(request.barrier(), request.checkpointDir());
        }
    }

    private void injectCheckpoint(CheckpointBarrier barrier, Path checkpointDir) throws Exception {
        long started = System.nanoTime();
        Optional<StateHandle> sourceHandle = Optional.empty();
        if (source instanceof CheckpointableSource<?> checkpointable) {
            sourceHandle = Optional.of(checkpointable.snapshot(barrier.checkpointId(), checkpointDir));
        }
        StateHandle taskHandle = snapshotSourceProgress(barrier.checkpointId(), checkpointDir, sourceHandle);
        if (sourceHandle.isEmpty()) {
            // Bounded demo sources have no external read position. They still forward and ack a
            // barrier so the coordinator can complete a checkpoint spanning their downstream
            // state, but have no handle of their own to persist.
            output.broadcast(barrier);
            output.flush();
            recordCheckpoint(barrier, taskHandle, (System.nanoTime() - started) / 1_000_000);
            return;
        }

        // A barrier must be visible downstream before this task acknowledges; otherwise a fast
        // coordinator could declare completion while the downstream pre-barrier prefix is open.
        output.broadcast(barrier);
        output.flush();
        long durationMillis = (System.nanoTime() - started) / 1_000_000;
        lastCheckpointId = barrier.checkpointId();
        lastCheckpointDurationMillis = durationMillis;
        lastCheckpointStateBytes = taskHandle.sizeBytes();
        checkpointListener.accept(new CheckpointResult(barrier.checkpointId(), Optional.of(taskHandle),
                durationMillis, taskHandle.sizeBytes(), 0));
    }

    /** Registers the worker callback that forwards this source acknowledgement to the master. */
    public void onCheckpoint(Consumer<CheckpointResult> listener) {
        checkpointListener = listener;
    }

    /** Latest source checkpoint statistics, used by worker heartbeats. */
    public CheckpointMetrics checkpointMetrics() {
        return new CheckpointMetrics(lastCheckpointId, lastCheckpointDurationMillis,
                lastCheckpointStateBytes, 0);
    }

    private void recordCheckpoint(CheckpointBarrier barrier, StateHandle handle, long durationMillis) {
        lastCheckpointId = barrier.checkpointId();
        lastCheckpointDurationMillis = durationMillis;
        lastCheckpointStateBytes = handle.sizeBytes();
        checkpointListener.accept(new CheckpointResult(barrier.checkpointId(), Optional.of(handle), durationMillis,
                lastCheckpointStateBytes, 0));
    }

    private StateHandle snapshotSourceProgress(long checkpointId, Path checkpointDir,
                                               Optional<StateHandle> sourceHandle) throws IOException {
        Files.createDirectories(checkpointDir);
        Path envelope = checkpointDir.resolve("source-task-" + checkpointId + ".properties");
        Properties saved = new Properties();
        saved.setProperty("watermark.emitted", Long.toString(currentWatermark));
        sourceHandle.ifPresent(handle -> {
            try {
                saved.setProperty("source.handle.path", relativePath(checkpointDir, handle));
            } catch (IOException failure) {
                throw new SourceSnapshotException(failure);
            }
            saved.setProperty("source.handle.size", Long.toString(handle.sizeBytes()));
        });
        watermarkGenerator.ifPresent(generator -> {
            BoundedOutOfOrdernessGenerator.Snapshot progress = generator.snapshot();
            saved.setProperty("watermark.present", "true");
            saved.setProperty("watermark.max-event-time", Long.toString(progress.maxEventTime()));
            saved.setProperty("watermark.observed-event", Boolean.toString(progress.observedEvent()));
            saved.setProperty("watermark.last-activity", Long.toString(progress.lastActivityWallClock()));
            saved.setProperty("watermark.idle", Boolean.toString(progress.idle()));
        });
        try (Writer writer = Files.newBufferedWriter(envelope)) {
            saved.store(writer, "Source position and event-time progress for checkpoint " + checkpointId);
        }
        return new StateHandle(envelope.toAbsolutePath().toUri(), Files.size(envelope));
    }

    private void restoreCheckpoint(StateHandle taskHandle) throws IOException {
        Path envelope = pathFor(taskHandle.uri());
        Properties saved = new Properties();
        try (Reader reader = Files.newBufferedReader(envelope)) {
            saved.load(reader);
        }
        currentWatermark = Long.parseLong(saved.getProperty("watermark.emitted",
                Long.toString(Long.MIN_VALUE)));
        String sourcePath = saved.getProperty("source.handle.path");
        if (sourcePath != null) {
            if (!(source instanceof CheckpointableSource<?> checkpointable)) {
                throw new IOException("source task " + taskId
                        + " received a source checkpoint but its source is not checkpointable");
            }
            long sourceSize = parseLong(saved, "source.handle.size");
            checkpointable.restore(new StateHandle(resolveNestedPath(envelope.getParent(), sourcePath).toUri(),
                    sourceSize));
        }
        if (watermarkGenerator.isPresent() && Boolean.parseBoolean(saved.getProperty("watermark.present"))) {
            watermarkGenerator.get().restore(new BoundedOutOfOrdernessGenerator.Snapshot(
                    parseLong(saved, "watermark.max-event-time"),
                    Boolean.parseBoolean(saved.getProperty("watermark.observed-event")),
                    parseLong(saved, "watermark.last-activity"),
                    Boolean.parseBoolean(saved.getProperty("watermark.idle"))));
        }
    }

    private static long parseLong(Properties saved, String key) throws IOException {
        try {
            return Long.parseLong(Objects.requireNonNull(saved.getProperty(key), "missing " + key));
        } catch (NumberFormatException | NullPointerException e) {
            throw new IOException("invalid source checkpoint property '" + key + "'", e);
        }
    }

    private static Path pathFor(URI uri) throws IOException {
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IOException("source task can only restore a file checkpoint handle: " + uri);
        }
        try {
            return Path.of(uri);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid source checkpoint handle: " + uri, e);
        }
    }

    /** Makes nested source state relocatable when its task checkpoint is materialized elsewhere. */
    private static String relativePath(Path checkpointDirectory, StateHandle handle) throws IOException {
        if (!"file".equalsIgnoreCase(handle.uri().getScheme())) {
            throw new IOException("source checkpoint handle must be a file URI: " + handle.uri());
        }
        Path root = checkpointDirectory.toAbsolutePath().normalize();
        Path nested = Path.of(handle.uri()).toAbsolutePath().normalize();
        if (!nested.startsWith(root) || nested.equals(root)) {
            throw new IOException("source checkpoint handle must be inside its task checkpoint: " + nested);
        }
        return root.relativize(nested).toString();
    }

    private static Path resolveNestedPath(Path directory, String relativePath) throws IOException {
        Path root = directory.toAbsolutePath().normalize();
        Path nested = root.resolve(relativePath).normalize();
        if (!nested.startsWith(root)) {
            throw new IOException("source checkpoint nested path escapes its envelope: " + relativePath);
        }
        return nested;
    }

    /** The data sent from one source task to the worker checkpoint acknowledgement path. */
    public record CheckpointResult(long checkpointId, Optional<StateHandle> stateHandle,
                                   long durationMillis, long stateSizeBytes,
                                   long alignmentMillis) {
    }

    /** The latest per-task checkpoint figures that travel in a worker heartbeat. */
    public record CheckpointMetrics(long checkpointId, long durationMillis, long stateSizeBytes,
                                    long alignmentMillis) {
    }

    private record CheckpointRequest(CheckpointBarrier barrier, Path checkpointDir) {
    }

    private void closeQuietly() {
        try {
            source.close();
        } catch (Exception e) {
            log.warn("source task {} failed to close its source cleanly", taskId, e);
        }
        output.close();
    }

    /**
     * Stamps each emitted value with its event time on the way past.
     *
     * <p>This is the only place in the engine where event time is assigned. A source with no
     * assigner configured emits records stamped {@link Long#MIN_VALUE}, which is honest: the job
     * has not been told when anything happened, and any window downstream will say so by never
     * firing.
     */
    private final class SourceCollector implements Collector<Object> {

        @Override
        public void collect(Object value) {
            collect(value, timestampAssigner
                    .map(assigner -> assigner.extractTimestamp(value))
                    .orElse(Long.MIN_VALUE));
        }

        @Override
        public void collect(Object value, long timestamp) {
            try {
                if (watermarkGenerator.isPresent()) {
                    BoundedOutOfOrdernessGenerator generator = watermarkGenerator.get();
                    boolean wasIdle = generator.isIdle();
                    generator.onEvent(timestamp);
                    if (wasIdle) {
                        // A record alone cannot revive a downstream channel: its watermark
                        // state is tracked independently of the data path.
                        output.broadcast(Watermark.active());
                    }
                }
                output.emit(new StreamRecord<>(value, timestamp));
                recordsOut.increment();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TaskCancelledException("cancelled while emitting from source");
            }
        }
    }

    private static final class SourceRestoreException extends RuntimeException {
        private SourceRestoreException(Exception cause) {
            super(cause);
        }
    }

    private static final class SourceSnapshotException extends RuntimeException {
        private SourceSnapshotException(IOException cause) {
            super(cause);
        }
    }

    private void emitPeriodicWatermark() throws InterruptedException {
        if (watermarkGenerator.isEmpty()) {
            return;
        }
        BoundedOutOfOrdernessGenerator generator = watermarkGenerator.get();
        boolean wasIdle = generator.isIdle();
        Optional<Watermark> watermark = generator.onPeriodicEmit();
        if (!wasIdle && generator.isIdle()) {
            output.broadcast(Watermark.idle());
        } else {
            watermark.ifPresent(value -> {
                try {
                    output.broadcast(value);
                    currentWatermark = value.timestamp();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TaskCancelledException("cancelled while emitting a watermark");
                }
            });
        }
    }
}
