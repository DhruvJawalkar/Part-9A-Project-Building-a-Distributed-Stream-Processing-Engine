package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.CheckpointListener;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.state.StateBackend;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * One running instance of one operator: a thread, an input gate, and the loop between them.
 *
 * <p>This class is the heart of the engine. Event-time watermarks and checkpoint barriers use
 * the same loop as data records: control elements and records arrive on the same channels, in
 * order, and are dispatched by the same switch. That shared ordering is what makes both the
 * watermark minimum and the checkpoint cut meaningful.
 *
 * <p>Nothing in this class changed when the engine became distributed. The gate it reads from
 * is fed by another thread in Phase 1 and by a gRPC stream in Phase 2; the output it writes to
 * is a queue handoff or a network write. Neither fact appears here, which is the only real test
 * of whether {@link InputGate} and {@link Output} were drawn in the right place.
 *
 * <h2>Single-threaded on purpose</h2>
 *
 * <p>Exactly one thread ever runs an operator. Everything an operator touches -- its fields, its
 * state, its collector -- is therefore free of concurrency, and no user code in this project
 * needs a lock or a synchronised block. Parallelism comes from running more subtasks, not from
 * more threads inside one. Giving that up would buy very little and would make keyed state,
 * timers and snapshotting all significantly harder to reason about.
 */
public final class OperatorTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OperatorTask.class);

    /**
     * How long to wait on an empty gate before looking around.
     *
     * <p>The loop must regain control periodically even when no records are arriving, so it can
     * notice cancellation. From Phase 3 it also needs to wake up to emit watermarks on a
     * schedule, which an indefinitely blocking take would prevent.
     */
    private static final long POLL_TIMEOUT_MILLIS = 100;

    private final String taskId;
    private final Operator<Object, Object> operator;
    private final InputGate inputGate;
    private final Output output;
    private final OutputCollector<Object> collector;
    private final RuntimeOperatorContext context;
    private final StateBackend stateBackend;
    private final TimerService timerService;
    private final WatermarkTracker watermarkTracker;
    private final BarrierAligner barrierAligner;
    private final Optional<KeySelector<Object, Object>> keySelector;
    private final Path checkpointDirectory;
    private final TaskMetricGroup metrics;
    private final Counter recordsIn;

    private int endOfStreamMarkersSeen;
    private final boolean[] endOfStreamChannels;
    private volatile boolean running = true;
    private volatile boolean started;
    private volatile boolean taskThreadTerminated;
    private final Object checkpointLifecycleLock = new Object();
    private Optional<StateHandle> restoreHandle = Optional.empty();
    private Serializable restoredOperatorCheckpointState;
    private boolean restoreOperatorCheckpointState;
    private final ConcurrentLinkedQueue<CheckpointNotification> pendingCheckpointNotifications =
            new ConcurrentLinkedQueue<>();
    /** Prepared external work keeps a bounded sink alive until completion or abort arrives. */
    private final Set<Long> preparedExternalCheckpoints = new HashSet<>();
    /**
     * Monotonic fence for coordinator-aborted barriers. Checkpoint ids increase within an
     * execution attempt, so one high-water mark is enough and cannot grow without bound.
     */
    private final AtomicLong highestAbortedCheckpointId = new AtomicLong();

    private volatile long lastCheckpointId;
    private volatile long lastCheckpointDurationMillis;
    private volatile long lastCheckpointStateBytes;
    private volatile long lastAlignmentMillis;

    private Consumer<CheckpointResult> checkpointListener = result -> {
    };

    /**
     * What to do when this task fails. In Phase 1 nothing listens and a failure is only logged;
     * from Phase 2 the worker listens and reports to the master, which fails the job.
     */
    private BiConsumer<String, Throwable> failureListener = (taskId, failure) -> {
    };

    /**
     * Creates a task.
     *
     * @param taskId   identity for logs and for reporting failures to the master
     * @param operator the user logic, already a private copy for this subtask
     * @param inputGate where records arrive, one queue per upstream subtask
     * @param output   where results go
     * @param metrics  this subtask's metric group
     */
    @SuppressWarnings("unchecked")
    public OperatorTask(String taskId,
                        Operator<?, ?> operator,
                        InputGate inputGate,
                        Output output,
                        TaskMetricGroup metrics) {
        this(taskId, operator, inputGate, output, Optional.empty(), metrics);
    }

    /**
     * Creates a task whose optional selector is the same one used on the upstream hash edge.
     * Keeping the selector at both points is intentional: routing and keyed state must agree
     * even after user code has crossed a process boundary.
     */
    @SuppressWarnings("unchecked")
    public OperatorTask(String taskId,
                        Operator<?, ?> operator,
                        InputGate inputGate,
                        Output output,
                        Optional<? extends KeySelector<?, ?>> keySelector,
                        TaskMetricGroup metrics) {
        this(taskId, operator, inputGate, output, keySelector,
                new InMemoryStateBackend(), defaultCheckpointDirectory(taskId), metrics);
    }

    /**
     * Creates a task with an explicit state backend and durable checkpoint location.
     *
     * <p>Workers use this overload with {@link RocksDbStateBackend}; the local executor keeps
     * the heap backend through the smaller constructor above. Keeping the choice at task
     * assembly means operator code remains completely independent of where its state lives.
     */
    @SuppressWarnings("unchecked")
    public OperatorTask(String taskId,
                        Operator<?, ?> operator,
                        InputGate inputGate,
                        Output output,
                        Optional<? extends KeySelector<?, ?>> keySelector,
                        StateBackend stateBackend,
                        Path checkpointDirectory,
                        TaskMetricGroup metrics) {
        this.taskId = taskId;
        this.operator = (Operator<Object, Object>) operator;
        this.inputGate = inputGate;
        this.output = output;
        this.collector = new OutputCollector<>(output);
        this.stateBackend = Objects.requireNonNull(stateBackend, "stateBackend");
        this.timerService = new TimerService();
        this.watermarkTracker = new WatermarkTracker(inputGate.channelCount());
        this.barrierAligner = new BarrierAligner(inputGate);
        this.endOfStreamChannels = new boolean[inputGate.channelCount()];
        this.context = new RuntimeOperatorContext(metrics, stateBackend, timerService,
                watermarkTracker);
        this.keySelector = (Optional<KeySelector<Object, Object>>) keySelector;
        this.checkpointDirectory = Objects.requireNonNull(checkpointDirectory,
                "checkpointDirectory").toAbsolutePath();
        this.metrics = metrics;
        this.recordsIn = metrics.counter("records-in");
    }

    @Override
    public void run() {
        log.info("task {} starting", taskId);
        try {
            started = true;
            if (restoreHandle.isPresent()) {
                restoreCheckpoint(restoreHandle.get());
            }
            operator.open(context);
            if (restoreOperatorCheckpointState && operator instanceof CheckpointListener listener) {
                listener.restoreCheckpointState(restoredOperatorCheckpointState);
            }

            while (running) {
                processPendingCheckpointNotifications();
                Optional<InputGate.IncomingElement> incoming =
                        inputGate.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (incoming.isEmpty()) {
                    // Nothing arrived. Ship anything batched rather than let it wait for
                    // traffic that may not come.
                    output.flush();
                    stopAfterEndOfInputWhenCheckpointCallbacksAreDone();
                    continue;
                }

                StreamElement element = incoming.get().element();
                int channel = incoming.get().channelIndex();

                switch (element) {
                    case StreamRecord<?> record -> processRecord(record);
                    case Watermark watermark -> handleWatermark(watermark, channel);
                    case CheckpointBarrier barrier -> handleBarrier(barrier, channel);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("task {} interrupted, stopping", taskId);
        } catch (TaskCancelledException e) {
            log.info("task {} cancelled: {}", taskId, e.getMessage());
        } catch (Exception e) {
            // Phase 2 reports this to the master, which fails the job. Phase 4 makes that a
            // restart from the last completed checkpoint instead.
            log.error("task {} failed", taskId, e);
            failureListener.accept(taskId, e);
        } finally {
            synchronized (checkpointLifecycleLock) {
                try {
                    processPendingCheckpointNotifications();
                } catch (Exception failure) {
                    log.error("task {} failed while draining checkpoint callbacks", taskId, failure);
                    failureListener.accept(taskId, failure);
                }
                taskThreadTerminated = true;
            }
            closeQuietly();
            log.info("task {} stopped after {} records", taskId, recordsIn.count());
        }
    }

    private void processRecord(StreamRecord<?> record) throws Exception {
        if (keySelector.isPresent()) {
            Object key = keySelector.get().getKey(record.value());
            if (key == null) {
                throw new IllegalArgumentException("key selector for task '" + taskId
                        + "' returned null");
            }
            stateBackend.setCurrentKey(key);
            context.setCurrentKey(key);
        }
        collector.setCurrentTimestamp(record.timestamp());
        operator.processElement(asObjectRecord(record), collector);
        recordsIn.increment();
    }

    /**
     * Maintains per-channel progress, advances the minimum across active inputs, fires event
     * timers, and forwards only genuine progress.
     *
     * <p>{@link Watermark#MAX} arriving on a channel means that channel will carry nothing
     * further, and once every channel has said so, this task is done.
     *
     * <p>Counting them is not optional, and it is Phase 3's problem in miniature: a task with
     * four input channels that stopped at the first end-of-stream marker would abandon the
     * records the other three still had in flight. A watermark is a claim about one channel, and
     * a task's own clock can only follow from all of its channels together.
     */
    private void handleWatermark(Watermark watermark, int channelIndex) throws Exception {
        if (!watermark.isIdle() && watermark.timestamp() == Watermark.MAX.timestamp()) {
            if (!endOfStreamChannels[channelIndex]) {
                endOfStreamChannels[channelIndex] = true;
                endOfStreamMarkersSeen++;
                // An ended input is terminal MAX in this task's minimum, so it no longer holds
                // back watermarks from inputs that are still producing. This is deliberately
                // different from forwarding MAX: an idle input may still revive, and the task
                // itself must only finish after every channel has ended.
                watermarkTracker.endOfInput(channelIndex).ifPresent(value -> {
                    try {
                        forwardProgressedWatermark(value);
                    } catch (Exception e) {
                        throw new WatermarkHandlingException(e);
                    }
                });
            }
            if (endOfStreamMarkersSeen == inputGate.channelCount()) {
                watermarkTracker.endOfAllInputs().ifPresent(value -> {
                    try {
                        forwardProgressedWatermark(value);
                    } catch (Exception e) {
                        throw new WatermarkHandlingException(e);
                    }
                });
                output.flush();
                stopAfterEndOfInputWhenCheckpointCallbacksAreDone();
            }
            return;
        }

        boolean allInputsWereIdle = watermarkTracker.allInputsIdle();
        Optional<Watermark> progressed = watermarkTracker.onWatermark(channelIndex, watermark);
        boolean allInputsAreIdle = watermarkTracker.allInputsIdle();

        // A status transition is useful even when it cannot advance this task's clock. A
        // downstream multi-input task otherwise has no way to distinguish a silent upstream
        // task from one whose own inputs are all idle.
        if (allInputsWereIdle != allInputsAreIdle) {
            output.broadcast(allInputsAreIdle ? Watermark.idle() : Watermark.active());
        }
        if (progressed.isPresent()) {
            forwardProgressedWatermark(progressed.get());
        }

    }

    private void forwardProgressedWatermark(Watermark watermark) throws Exception {
        timerService.advanceTo(watermark.timestamp(), stateBackend, (timestamp, key) -> {
            if (operator instanceof KeyedOperator<?, ?, ?> keyed) {
                @SuppressWarnings("unchecked")
                KeyedOperator<Object, Object, Object> typed =
                        (KeyedOperator<Object, Object, Object>) keyed;
                collector.setCurrentTimestamp(timestamp);
                context.setCurrentKey(key);
                typed.onEventTimer(timestamp, key, collector);
            }
        });
        collector.setCurrentTimestamp(watermark.timestamp());
        operator.onWatermark(watermark.timestamp(), collector);
        output.broadcast(watermark);
    }

    private void handleBarrier(CheckpointBarrier barrier, int channelIndex) throws Exception {
        if (barrier.checkpointId() <= highestAbortedCheckpointId.get()) {
            // The abort RPC can overtake a barrier delayed in the network. Such a barrier must
            // disappear completely: no alignment, pre-commit, snapshot, forwarding, or ack.
            return;
        }
        Optional<BarrierAligner.AlignedCheckpoint> aligned =
                barrierAligner.onBarrier(barrier, channelIndex);
        if (aligned.isEmpty()) {
            return;
        }

        BarrierAligner.AlignedCheckpoint checkpoint = aligned.get();
        long snapshotStarted = System.nanoTime();
        try {
            long checkpointId = checkpoint.barrier().checkpointId();
            Serializable operatorCheckpointState = operator instanceof CheckpointListener listener
                    && checkpointLifecycleEnabled() ? listener.preCommit(checkpointId) : null;
            StateHandle handle = snapshotCheckpoint(checkpointId, operatorCheckpointState);
            if (checkpointLifecycleEnabled()) {
                preparedExternalCheckpoints.add(checkpointId);
            }

            // This order is the checkpoint protocol, not an implementation detail. Downstream
            // must see the barrier before the coordinator can observe this acknowledgement;
            // blocked post-barrier records are released only after both have happened.
            output.broadcast(checkpoint.barrier());
            output.flush();

            long durationMillis = (System.nanoTime() - snapshotStarted) / 1_000_000;
            lastCheckpointId = checkpoint.barrier().checkpointId();
            lastCheckpointDurationMillis = durationMillis;
            lastCheckpointStateBytes = handle.sizeBytes();
            lastAlignmentMillis = checkpoint.alignmentMillis();
            checkpointListener.accept(new CheckpointResult(lastCheckpointId, handle,
                    durationMillis, handle.sizeBytes(), checkpoint.alignmentMillis()));
            barrierAligner.completeAlignment(lastCheckpointId);
        } catch (Exception failure) {
            barrierAligner.abortAlignment(checkpoint.barrier().checkpointId());
            throw failure;
        }
    }

    /** Restores a completed task checkpoint before this task processes its first record. */
    public void restore(StateHandle handle) {
        if (started) {
            throw new IllegalStateException("cannot restore a running operator task " + taskId);
        }
        restoreHandle = Optional.of(Objects.requireNonNull(handle, "handle"));
    }

    /** Registers the worker callback that forwards checkpoint acknowledgement to the master. */
    public void onCheckpoint(Consumer<CheckpointResult> listener) {
        checkpointListener = Objects.requireNonNull(listener, "listener");
    }

    /** Releases inputs held by a checkpoint that the coordinator has abandoned. */
    public void abortCheckpoint(long checkpointId) {
        highestAbortedCheckpointId.accumulateAndGet(checkpointId, Math::max);
        // BarrierAligner is synchronized specifically so the control plane can release
        // backpressured channels even while the task thread is publishing its acknowledgement.
        // User operator callbacks remain queued and single-threaded below.
        barrierAligner.abortAlignment(checkpointId);
        enqueueCheckpointNotification(CheckpointNotification.aborted(checkpointId));
    }

    /**
     * Queues a durable-checkpoint notification for this task's thread.
     *
     * <p>gRPC handler threads must not call user operators directly: the same sink instance is
     * processing records on this task thread. The run loop drains this queue between stream
     * elements, preserving the single-threaded operator guarantee.
     */
    public CompletableFuture<Void> notifyCheckpointComplete(long checkpointId) {
        return enqueueCheckpointNotification(CheckpointNotification.completed(checkpointId));
    }

    /** Latest checkpoint figures, carried in the worker heartbeat. */
    public CheckpointMetrics checkpointMetrics() {
        return new CheckpointMetrics(lastCheckpointId, lastCheckpointDurationMillis,
                lastCheckpointStateBytes, lastAlignmentMillis);
    }

    private StateHandle snapshotCheckpoint(long checkpointId, Serializable operatorCheckpointState)
            throws IOException {
        Path directory = checkpointDirectory.resolve("checkpoint-" + checkpointId);
        Files.createDirectories(directory);
        StateHandle keyedState = stateBackend.snapshot(checkpointId,
                directory.resolve("keyed-state"));
        Path envelope = directory.resolve("operator-task.bin");
        OperatorSnapshot snapshot = new OperatorSnapshot(relativePath(directory, keyedState),
                keyedState.sizeBytes(), timerService.snapshot(), watermarkTracker.snapshot(),
                operatorCheckpointState, checkpointId);
        try (ObjectOutputStream outputStream =
                     new ObjectOutputStream(Files.newOutputStream(envelope))) {
            outputStream.writeObject(snapshot);
        }
        return new StateHandle(envelope.toAbsolutePath().toUri(),
                Math.addExact(Files.size(envelope), keyedState.sizeBytes()));
    }

    private void restoreCheckpoint(StateHandle taskHandle) throws IOException {
        Path envelope = filePath(taskHandle.uri());
        try (ObjectInputStream input = new ObjectInputStream(Files.newInputStream(envelope))) {
            Object value = input.readObject();
            if (!(value instanceof OperatorSnapshot snapshot)) {
                throw new IOException("operator checkpoint has an unexpected payload");
            }
            stateBackend.restore(new StateHandle(resolveNestedPath(envelope.getParent(), snapshot.stateHandlePath()).toUri(),
                    snapshot.stateSizeBytes()));
            timerService.restore(snapshot.timerSnapshot());
            watermarkTracker.restore(snapshot.watermarkSnapshot());
            restoredOperatorCheckpointState = snapshot.operatorCheckpointState();
            restoreOperatorCheckpointState = true;
            if (checkpointLifecycleEnabled() && snapshot.checkpointId() > 0) {
                preparedExternalCheckpoints.add(snapshot.checkpointId());
            }
        } catch (ClassNotFoundException failure) {
            throw new IOException("could not deserialize operator checkpoint", failure);
        }
    }

    private static Path filePath(URI uri) throws IOException {
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IOException("operator task can only restore a file checkpoint handle: "
                    + uri);
        }
        try {
            return Path.of(uri);
        } catch (IllegalArgumentException failure) {
            throw new IOException("invalid operator checkpoint handle: " + uri, failure);
        }
    }

    /** Makes the RocksDB snapshot location independent of the worker that restores it. */
    private static String relativePath(Path checkpointDirectory, StateHandle handle) throws IOException {
        Path root = checkpointDirectory.toAbsolutePath().normalize();
        Path nested = filePath(handle.uri()).toAbsolutePath().normalize();
        if (!nested.startsWith(root) || nested.equals(root)) {
            throw new IOException("keyed-state handle must be inside its task checkpoint: " + nested);
        }
        return root.relativize(nested).toString();
    }

    private static Path resolveNestedPath(Path directory, String relativePath) throws IOException {
        Path root = directory.toAbsolutePath().normalize();
        Path nested = root.resolve(relativePath).normalize();
        if (!nested.startsWith(root)) {
            throw new IOException("keyed-state path escapes task checkpoint: " + relativePath);
        }
        return nested;
    }

    private static Path defaultCheckpointDirectory(String taskId) {
        return Path.of(System.getProperty("java.io.tmpdir"),
                "distributed-stream-processing-engine", "checkpoints",
                taskId.replaceAll("[^A-Za-z0-9._-]", "_"));
    }

    /**
     * Asks this task to stop after it finishes the element in hand.
     *
     * <p>Cooperative rather than an interrupt, so a task stops between records instead of
     * halfway through one. An interrupt follows only if the task does not notice in time.
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

    /**
     * Returns this task's input gate, so a worker can feed it from the network.
     *
     * @return the input gate
     */
    public InputGate inputGate() {
        return inputGate;
    }

    /**
     * Registers what to do when this task fails.
     *
     * @param listener called with the task id and the failure
     */
    public void onFailure(BiConsumer<String, Throwable> listener) {
        this.failureListener = listener;
    }

    private void closeQuietly() {
        try {
            operator.close();
        } catch (Exception e) {
            log.warn("task {} failed to close its operator cleanly", taskId, e);
        }
        output.close();
        stateBackend.close();
    }

    @SuppressWarnings("unchecked")
    private static StreamRecord<Object> asObjectRecord(StreamRecord<?> record) {
        return (StreamRecord<Object>) record;
    }

    private CompletableFuture<Void> enqueueCheckpointNotification(CheckpointNotification notification) {
        synchronized (checkpointLifecycleLock) {
            if (taskThreadTerminated) {
                // The mailbox was drained before close; later duplicate completion notices are
                // already reflected in the task's durable state and must not reopen a sink.
                notification.completion().complete(null);
                return notification.completion();
            }
            pendingCheckpointNotifications.add(notification);
            return notification.completion();
        }
    }

    private void processPendingCheckpointNotifications() throws Exception {
        CheckpointNotification notification;
        while ((notification = pendingCheckpointNotifications.poll()) != null) {
            try {
                deliverCheckpointNotification(notification);
                notification.completion().complete(null);
                preparedExternalCheckpoints.remove(notification.checkpointId());
            } catch (Exception failure) {
                notification.completion().completeExceptionally(failure);
                throw failure;
            }
        }
        stopAfterEndOfInputWhenCheckpointCallbacksAreDone();
    }

    private void deliverCheckpointNotification(CheckpointNotification notification) throws Exception {
        if (!notification.completed()) {
            // BarrierAligner and user operators both belong to this task thread. The atomic
            // high-water mark above fences newly arriving barriers immediately; this releases
            // any channel that was already blocked by the abandoned alignment.
            barrierAligner.abortAlignment(notification.checkpointId());
        }
        if (operator instanceof CheckpointListener listener) {
            if (notification.completed()) {
                listener.notifyCheckpointComplete(notification.checkpointId());
            } else {
                listener.notifyCheckpointAborted(notification.checkpointId());
            }
        }
    }

    private boolean checkpointLifecycleEnabled() {
        return operator instanceof CheckpointListener
                && (!(operator instanceof OperatorChain chain) || chain.hasCheckpointListeners());
    }

    private void stopAfterEndOfInputWhenCheckpointCallbacksAreDone() {
        if (endOfStreamMarkersSeen == inputGate.channelCount()
                && preparedExternalCheckpoints.isEmpty()) {
            running = false;
        }
    }

    private static final class WatermarkHandlingException extends RuntimeException {
        private WatermarkHandlingException(Exception cause) {
            super(cause);
        }
    }

    /** Data reported by a task after its barrier is forwarded. */
    public record CheckpointResult(long checkpointId, StateHandle stateHandle,
                                   long durationMillis, long stateSizeBytes,
                                   long alignmentMillis) {
    }

    /** Latest per-task checkpoint figures for heartbeats and the status API. */
    public record CheckpointMetrics(long checkpointId, long durationMillis,
                                    long stateSizeBytes, long alignmentMillis) {
    }

    private record OperatorSnapshot(String stateHandlePath,
                                    long stateSizeBytes,
                                    TimerService.TimerSnapshot timerSnapshot,
                                    WatermarkTracker.Snapshot watermarkSnapshot,
                                    Serializable operatorCheckpointState,
                                    long checkpointId)
            implements Serializable {
    }

    private record CheckpointNotification(long checkpointId, boolean completed,
                                          CompletableFuture<Void> completion) {
        private static CheckpointNotification completed(long checkpointId) {
            return new CheckpointNotification(checkpointId, true, new CompletableFuture<>());
        }

        private static CheckpointNotification aborted(long checkpointId) {
            return new CheckpointNotification(checkpointId, false, new CompletableFuture<>());
        }
    }
}
