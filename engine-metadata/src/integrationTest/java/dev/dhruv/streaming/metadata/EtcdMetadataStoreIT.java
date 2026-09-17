package dev.dhruv.streaming.metadata;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the etcd-backed metadata store.
 *
 * <p>Needs a live etcd: {@code docker compose up -d etcd}. Run with
 * {@code ./gradlew :engine-master:integrationTest}.
 *
 * <p>These exist for the behaviour the in-memory store deliberately does not imitate. Two things
 * in particular can only be checked against the real thing, and both are load-bearing for
 * Phase 2's failure handling: a lease that expires without anyone deleting anything, and a
 * watch that fires when it does.
 */
class EtcdMetadataStoreIT {

    private static final String ENDPOINT = System.getenv().getOrDefault(
            "ETCD_ENDPOINTS", "http://localhost:2379");

    private EtcdMetadataStore store;
    private String jobId;

    @BeforeEach
    void setUp() {
        store = new EtcdMetadataStore(ENDPOINT);
        // A unique job id per test, so a run leaves nothing behind that another run trips over.
        jobId = "it-job-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
    }

    @Test
    @Timeout(30)
    @DisplayName("round-trips a job graph, state, cause and assignments")
    void roundTripsJobMetadata() {
        store.putJobGraph(jobId, "serialized-graph".getBytes());
        store.putJobState(jobId, JobState.RUNNING);
        store.putAssignments(jobId, Map.of(
                "clicks:0", "worker-1",
                "clicks:1", "worker-2",
                "console:0", "worker-3"));

        assertThat(store.getJobGraph(jobId)).get().asInstanceOf(
                org.assertj.core.api.InstanceOfAssertFactories.BYTE_ARRAY)
                .containsExactly("serialized-graph".getBytes());
        assertThat(store.getJobState(jobId)).contains(JobState.RUNNING);
        assertThat(store.getAssignments(jobId))
                .containsEntry("clicks:0", "worker-1")
                .containsEntry("console:0", "worker-3")
                .hasSize(3);
    }

    @Test
    @Timeout(30)
    @DisplayName("survives a master restart: a fresh store reads what the old one wrote")
    void survivesMasterRestart() {
        // The acceptance criterion, at store level. A new client over the same etcd is exactly
        // what a restarted master sees.
        store.putJobGraph(jobId, "serialized-graph".getBytes());
        store.putJobState(jobId, JobState.RUNNING);
        store.putAssignments(jobId, Map.of("clicks:0", "worker-1"));
        store.close();

        store = new EtcdMetadataStore(ENDPOINT);

        assertThat(store.getJobState(jobId)).contains(JobState.RUNNING);
        assertThat(store.getAssignments(jobId)).containsEntry("clicks:0", "worker-1");
        assertThat(store.listJobs()).contains(jobId);
    }

    @Test
    @Timeout(30)
    @DisplayName("records a failure cause alongside the state")
    void recordsFailureCause() {
        store.putJobFailureCause(jobId, "worker worker-2 died holding 2 task(s)");
        store.putJobState(jobId, JobState.FAILED);

        assertThat(store.getJobFailureCause(jobId))
                .get().asString().contains("worker-2");
        assertThat(store.getJobState(jobId)).contains(JobState.FAILED);
    }

    @Test
    @Timeout(30)
    @DisplayName("a registered worker is visible to another client")
    void workerRegistrationIsVisible() {
        RegisteredWorker worker = worker("it-worker-" + UUID.randomUUID());

        try (var ignored = store.registerWorker(worker, 10)) {
            assertThat(store.listWorkers())
                    .extracting(RegisteredWorker::workerId)
                    .contains(worker.workerId());

            // Another client, as the master would be if the worker registered itself.
            try (EtcdMetadataStore reader = new EtcdMetadataStore(ENDPOINT)) {
                assertThat(reader.listWorkers())
                        .extracting(RegisteredWorker::workerId)
                        .contains(worker.workerId());
            }
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("a worker that stops renewing disappears on its own")
    void leaseExpiresWithoutAnyoneDeletingAnything() {
        // The mechanism behind "kill -9 on a worker is detected". Nothing deletes this key.
        // The worker simply stops renewing, and etcd forgets it.
        //
        // A 2-second TTL rather than the production 10, so the test is not mostly waiting.
        RegisteredWorker worker = worker("it-dying-" + UUID.randomUUID());
        var registration = store.registerWorker(worker, 2);

        assertThat(store.listWorkers())
                .extracting(RegisteredWorker::workerId).contains(worker.workerId());

        // Closing stops the keep-alive. This is the polite version of a process being killed:
        // in both cases the renewals stop and nothing announces it.
        registration.close();

        await().atMost(15, TimeUnit.SECONDS)
                .pollInterval(250, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(store.listWorkers())
                        .extracting(RegisteredWorker::workerId)
                        .doesNotContain(worker.workerId()));
    }

    @Test
    @Timeout(30)
    @DisplayName("watching the worker prefix reports arrivals and departures")
    void watchReportsClusterChanges() {
        // The master watches rather than polls, so that a new worker can be scheduled onto
        // immediately and a lost one fails its job immediately.
        List<List<String>> observed = new CopyOnWriteArrayList<>();
        RegisteredWorker worker = worker("it-watched-" + UUID.randomUUID());

        try (var watch = store.watchWorkers(workers ->
                observed.add(workers.stream().map(RegisteredWorker::workerId).toList()))) {

            var registration = store.registerWorker(worker, 10);

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(observed).anyMatch(ids -> ids.contains(worker.workerId())));

            registration.close();

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(observed.getLast()).doesNotContain(worker.workerId()));
        } catch (Exception e) {
            throw new AssertionError("watch failed", e);
        }
    }

    @Test
    @Timeout(30)
    @DisplayName("lists workers in a stable order, so compilation is reproducible")
    void workerOrderIsStable() {
        // A restarted master must compile the same job to the same plan, which it cannot do if
        // the worker list arrives in whatever order etcd felt like.
        String suffix = UUID.randomUUID().toString();
        try (var a = store.registerWorker(worker("it-c-" + suffix), 10);
             var b = store.registerWorker(worker("it-a-" + suffix), 10);
             var c = store.registerWorker(worker("it-b-" + suffix), 10)) {

            List<String> first = store.listWorkers().stream()
                    .map(RegisteredWorker::workerId).filter(id -> id.endsWith(suffix)).toList();
            List<String> second = store.listWorkers().stream()
                    .map(RegisteredWorker::workerId).filter(id -> id.endsWith(suffix)).toList();

            assertThat(first).isEqualTo(second).isSorted();
        }
    }

    private static RegisteredWorker worker(String workerId) {
        return new RegisteredWorker(workerId, "10.0.0.1", 9091, 9191, 8);
    }
}
