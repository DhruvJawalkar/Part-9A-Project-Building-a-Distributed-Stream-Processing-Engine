package dev.dhruv.streaming.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Decides which workers are alive, from the beats they send.
 *
 * <h2>Three missed beats, not one</h2>
 *
 * <p>A worker sends a beat every second and is declared dead after three are missed. The
 * threshold is the interesting part, because both directions of getting it wrong are bad in
 * ways worth feeling.
 *
 * <p>Too tight, and an ordinary garbage-collection pause or a momentary network stall gets a
 * perfectly healthy worker declared dead -- and in Phase 2 that means failing a job that was
 * running fine. Too loose, and a genuinely dead worker keeps its tasks nominally assigned while
 * the job silently stops making progress, which is worse, because nothing in any log says so.
 *
 * <p>Three seconds is not a derived number. It is a guess, tuned to be longer than a stop-the-
 * world pause and shorter than a person's patience. Every engine has this number and every one
 * of them has picked it the same way.
 *
 * <h2>The thing this cannot do</h2>
 *
 * <p>Silence does not distinguish a dead worker from an unreachable one. A worker that is up,
 * processing records, and merely partitioned from the master looks exactly like a worker that
 * has crashed. This is the failure detector problem, and it has no solution -- only choices
 * about which mistake to make. Here the master assumes the worst and fails the job, which is
 * safe in Phase 2 because there is nothing for a partitioned worker to corrupt.
 */
public final class TaskTracker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TaskTracker.class);

    /** How often a worker is expected to beat. */
    public static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(1);

    /** How many beats may be missed before a worker is presumed dead. */
    public static final int MISSED_BEATS_BEFORE_DEAD = 3;

    private final Map<String, Long> lastBeatNanos = new ConcurrentHashMap<>();
    private final Map<String, WorkerHealth> health = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final Consumer<String> onWorkerDead;
    private final long deathThresholdNanos;

    /**
     * Creates a tracker and starts checking for missed beats.
     *
     * @param onWorkerDead called with the worker id when one is declared dead. Called from the
     *                     tracker's own thread, once per worker, and never again for that
     *                     worker unless it registers afresh.
     */
    public TaskTracker(Consumer<String> onWorkerDead) {
        this.onWorkerDead = onWorkerDead;
        this.deathThresholdNanos =
                HEARTBEAT_INTERVAL.multipliedBy(MISSED_BEATS_BEFORE_DEAD).toNanos();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "task-tracker");
            thread.setDaemon(true);
            return thread;
        });
        // Checked more often than the interval so that detection latency is bounded by the
        // threshold rather than by the threshold plus a whole check period.
        scheduler.scheduleAtFixedRate(this::checkForDeadWorkers,
                HEARTBEAT_INTERVAL.toMillis(), HEARTBEAT_INTERVAL.toMillis() / 2,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Records that a worker has registered and is expected to start beating.
     *
     * @param workerId the worker
     */
    public void workerRegistered(String workerId) {
        lastBeatNanos.put(workerId, System.nanoTime());
        health.put(workerId, WorkerHealth.ALIVE);
        log.info("tracking worker {}", workerId);
    }

    /**
     * Records a beat.
     *
     * @param workerId the worker that beat
     */
    public void heartbeatReceived(String workerId) {
        lastBeatNanos.put(workerId, System.nanoTime());
        // A master restart rebuilds this in-memory table from the first beat sent by each
        // surviving worker. Its durable address is loaded independently from etcd.
        health.put(workerId, WorkerHealth.ALIVE);
    }

    /**
     * Stops tracking a worker that has shut down cleanly.
     *
     * <p>Distinct from dying: a worker that says goodbye is removed without the death callback
     * firing, because a planned shutdown is not a job failure.
     *
     * @param workerId the worker
     */
    public void workerDeregistered(String workerId) {
        lastBeatNanos.remove(workerId);
        health.remove(workerId);
        log.info("worker {} deregistered", workerId);
    }

    /**
     * Returns the workers currently believed alive.
     *
     * @return their ids
     */
    public List<String> aliveWorkers() {
        return health.entrySet().stream()
                .filter(entry -> entry.getValue() == WorkerHealth.ALIVE)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    /**
     * Returns whether a worker is currently believed alive.
     *
     * @param workerId the worker
     * @return true if it is beating
     */
    public boolean isAlive(String workerId) {
        return health.get(workerId) == WorkerHealth.ALIVE;
    }

    private void checkForDeadWorkers() {
        long now = System.nanoTime();
        lastBeatNanos.forEach((workerId, lastBeat) -> {
            if (health.get(workerId) != WorkerHealth.ALIVE) {
                return;                         // already declared; do not fire twice
            }
            long silentNanos = now - lastBeat;
            if (silentNanos > deathThresholdNanos) {
                health.put(workerId, WorkerHealth.DEAD);
                log.error("worker {} missed {} heartbeats ({}ms of silence): presumed dead",
                        workerId, MISSED_BEATS_BEFORE_DEAD,
                        TimeUnit.NANOSECONDS.toMillis(silentNanos));
                try {
                    onWorkerDead.accept(workerId);
                } catch (Exception e) {
                    log.error("handler for the death of worker {} itself failed", workerId, e);
                }
            }
        });
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    private enum WorkerHealth {
        ALIVE,
        DEAD
    }
}
