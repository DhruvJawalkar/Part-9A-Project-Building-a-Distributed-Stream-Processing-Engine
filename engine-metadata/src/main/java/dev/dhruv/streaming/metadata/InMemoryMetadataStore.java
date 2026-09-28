package dev.dhruv.streaming.metadata;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A {@link MetadataStore} kept in maps, for tests and for running a master without etcd.
 *
 * <p>Its point is that a test about the job state machine should fail when the job state machine
 * is wrong, and for no other reason. Pointing those tests at a container makes them slower, and
 * worse, makes them able to fail for reasons that have nothing to do with what they assert.
 *
 * <p>It is honest about one thing and dishonest about another, and both are worth naming. It
 * genuinely implements watches, so code that reacts to workers appearing and disappearing is
 * exercised properly. It does <em>not</em> implement lease expiry: a registration here lives
 * until it is closed, so a worker that "crashes" in a test using this store has to be closed
 * explicitly. The real timing behaviour of a TTL is what the etcd integration test is for.
 */
public final class InMemoryMetadataStore implements MetadataStore {

    private final Map<String, byte[]> graphs = new ConcurrentHashMap<>();
    private final Map<String, JobState> states = new ConcurrentHashMap<>();
    private final Map<String, String> causes = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> assignments = new ConcurrentHashMap<>();
    private final Map<String, CompletedCheckpoint> checkpoints = new ConcurrentHashMap<>();
    private final Map<String, RegisteredWorker> workers = new ConcurrentHashMap<>();
    private final List<Consumer<List<RegisteredWorker>>> watchers = new CopyOnWriteArrayList<>();

    @Override
    public void putJobGraph(String jobId, byte[] serializedGraph) {
        graphs.put(jobId, serializedGraph.clone());
    }

    @Override
    public Optional<byte[]> getJobGraph(String jobId) {
        return Optional.ofNullable(graphs.get(jobId)).map(byte[]::clone);
    }

    @Override
    public void putJobState(String jobId, JobState state) {
        states.put(jobId, state);
    }

    @Override
    public Optional<JobState> getJobState(String jobId) {
        return Optional.ofNullable(states.get(jobId));
    }

    @Override
    public void putJobFailureCause(String jobId, String cause) {
        causes.put(jobId, cause);
    }

    @Override
    public Optional<String> getJobFailureCause(String jobId) {
        return Optional.ofNullable(causes.get(jobId));
    }

    @Override
    public void putAssignments(String jobId, Map<String, String> jobAssignments) {
        assignments.put(jobId, Map.copyOf(jobAssignments));
    }

    @Override
    public Map<String, String> getAssignments(String jobId) {
        return assignments.getOrDefault(jobId, Map.of());
    }

    @Override
    public void putLatestCompletedCheckpoint(String jobId, CompletedCheckpoint checkpoint) {
        checkpoints.put(jobId, checkpoint);
    }

    @Override
    public Optional<CompletedCheckpoint> getLatestCompletedCheckpoint(String jobId) {
        return Optional.ofNullable(checkpoints.get(jobId));
    }

    @Override
    public List<String> listJobs() {
        return graphs.keySet().stream().sorted().toList();
    }

    @Override
    public WorkerRegistration registerWorker(RegisteredWorker worker, long ttlSeconds) {
        workers.put(worker.workerId(), worker);
        notifyWatchers();
        return () -> {
            workers.remove(worker.workerId());
            notifyWatchers();
        };
    }

    @Override
    public List<RegisteredWorker> listWorkers() {
        List<RegisteredWorker> snapshot = new ArrayList<>(workers.values());
        snapshot.sort(Comparator.comparing(RegisteredWorker::workerId));
        return snapshot;
    }

    @Override
    public AutoCloseable watchWorkers(Consumer<List<RegisteredWorker>> onChange) {
        watchers.add(onChange);
        return () -> watchers.remove(onChange);
    }

    @Override
    public void close() {
        watchers.clear();
    }

    private void notifyWatchers() {
        List<RegisteredWorker> snapshot = listWorkers();
        watchers.forEach(watcher -> watcher.accept(snapshot));
    }
}
