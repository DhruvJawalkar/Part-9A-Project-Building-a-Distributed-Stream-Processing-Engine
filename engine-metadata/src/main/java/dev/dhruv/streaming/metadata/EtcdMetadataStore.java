package dev.dhruv.streaming.metadata;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.support.CloseableClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The etcd-backed {@link MetadataStore}.
 *
 * <p>etcd rather than a database for two properties this engine actually uses: leases, which
 * make worker liveness expire on its own, and watches, which turn "tell me when the cluster
 * changes" into something the store pushes rather than something the master asks about on a
 * timer.
 *
 * <p>Values are stored as plain bytes with no schema of their own. That keeps the store honest
 * about its job -- it is a place to put things that must survive a restart, not a model of the
 * engine.
 */
public final class EtcdMetadataStore implements MetadataStore {

    private static final Logger log = LoggerFactory.getLogger(EtcdMetadataStore.class);

    private static final String JOBS_PREFIX = "/jobs/";
    private static final String WORKERS_PREFIX = "/workers/";

    private final Client client;
    private final KV kv;
    private final Lease lease;
    private final Watch watch;

    /**
     * Connects to etcd.
     *
     * @param endpoints comma-separated etcd endpoints, e.g. {@code http://localhost:2379}
     */
    public EtcdMetadataStore(String endpoints) {
        this.client = Client.builder().endpoints(endpoints.split(",")).build();
        this.kv = client.getKVClient();
        this.lease = client.getLeaseClient();
        this.watch = client.getWatchClient();
        log.info("connected to etcd at {}", endpoints);
    }

    // ---------------------------------------------------------------------------------
    // Jobs
    // ---------------------------------------------------------------------------------

    @Override
    public void putJobGraph(String jobId, byte[] serializedGraph) {
        put(jobKey(jobId, "graph"), ByteSequence.from(serializedGraph));
    }

    @Override
    public Optional<byte[]> getJobGraph(String jobId) {
        return get(jobKey(jobId, "graph")).map(ByteSequence::getBytes);
    }

    @Override
    public void putJobState(String jobId, JobState state) {
        put(jobKey(jobId, "state"), utf8(state.name()));
        log.info("job {} -> {}", jobId, state);
    }

    @Override
    public Optional<JobState> getJobState(String jobId) {
        return get(jobKey(jobId, "state")).map(value -> JobState.valueOf(text(value)));
    }

    @Override
    public void putJobFailureCause(String jobId, String cause) {
        put(jobKey(jobId, "cause"), utf8(cause));
    }

    @Override
    public Optional<String> getJobFailureCause(String jobId) {
        return get(jobKey(jobId, "cause")).map(EtcdMetadataStore::text);
    }

    @Override
    public void putAssignments(String jobId, Map<String, String> assignments) {
        // One key holding the whole map, rather than a key per assignment. The master reads
        // assignments all at once or not at all, and a single value means a restarting master
        // can never observe half of them.
        StringBuilder encoded = new StringBuilder();
        assignments.forEach((slice, workerId) ->
                encoded.append(slice).append('=').append(workerId).append('\n'));
        put(jobKey(jobId, "assignments"), utf8(encoded.toString()));
    }

    @Override
    public Map<String, String> getAssignments(String jobId) {
        return get(jobKey(jobId, "assignments"))
                .map(EtcdMetadataStore::text)
                .map(EtcdMetadataStore::decodeAssignments)
                .orElseGet(Map::of);
    }

    @Override
    public void putLatestCompletedCheckpoint(String jobId, CompletedCheckpoint checkpoint) {
        put(jobKey(jobId, "checkpoints/latest"), ByteSequence.from(encodeCheckpoint(checkpoint)));
        log.info("job {} completed checkpoint {} with {} task handle(s)", jobId,
                checkpoint.checkpointId(), checkpoint.taskStates().size());
    }

    @Override
    public Optional<CompletedCheckpoint> getLatestCompletedCheckpoint(String jobId) {
        return get(jobKey(jobId, "checkpoints/latest"))
                .map(ByteSequence::getBytes)
                .map(EtcdMetadataStore::decodeCheckpoint);
    }

    private static Map<String, String> decodeAssignments(String encoded) {
        Map<String, String> assignments = new LinkedHashMap<>();
        for (String line : encoded.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            int split = line.indexOf('=');
            assignments.put(line.substring(0, split), line.substring(split + 1));
        }
        return assignments;
    }

    /*
     * A compact binary value rather than Java serialization or a JSON library. Metadata is an
     * engine boundary, not a place where a change to a user operator's serial form should make a
     * job unrecoverable. The order is explicit too, so inspecting the bytes is deterministic.
     */
    private static byte[] encodeCheckpoint(CompletedCheckpoint checkpoint) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(checkpoint.checkpointId());
            out.writeLong(checkpoint.triggerTimestamp());
            out.writeInt(checkpoint.taskStates().size());
            checkpoint.taskStates().entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> writeCheckpointTask(out, entry));
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new MetadataStoreException("could not encode completed checkpoint", e);
        }
    }

    private static void writeCheckpointTask(DataOutputStream out,
                                            Map.Entry<String, CompletedCheckpoint.TaskState> entry) {
        try {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue().stateHandleUri());
            out.writeLong(entry.getValue().stateSizeBytes());
            out.writeLong(entry.getValue().alignmentMillis());
        } catch (IOException e) {
            throw new MetadataStoreException("could not encode checkpoint task", e);
        }
    }

    private static CompletedCheckpoint decodeCheckpoint(byte[] encoded) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            long checkpointId = in.readLong();
            long triggerTimestamp = in.readLong();
            int count = in.readInt();
            if (count < 1) {
                throw new IllegalArgumentException("completed checkpoint had no task states");
            }
            Map<String, CompletedCheckpoint.TaskState> tasks = new LinkedHashMap<>();
            for (int index = 0; index < count; index++) {
                String taskId = in.readUTF();
                tasks.put(taskId, new CompletedCheckpoint.TaskState(
                        in.readUTF(), in.readLong(), in.readLong()));
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("completed checkpoint has trailing bytes");
            }
            return new CompletedCheckpoint(checkpointId, triggerTimestamp, tasks);
        } catch (IOException e) {
            throw new MetadataStoreException("could not decode completed checkpoint", e);
        }
    }

    @Override
    public List<String> listJobs() {
        return prefixKeys(JOBS_PREFIX).stream()
                .map(key -> key.substring(JOBS_PREFIX.length()))
                .map(rest -> rest.substring(0, rest.indexOf('/')))
                .distinct()
                .sorted()
                .toList();
    }

    // ---------------------------------------------------------------------------------
    // Workers
    // ---------------------------------------------------------------------------------

    @Override
    public WorkerRegistration registerWorker(RegisteredWorker worker, long ttlSeconds) {
        try {
            long leaseId = lease.grant(ttlSeconds).get(10, TimeUnit.SECONDS).getID();

            kv.put(utf8(WORKERS_PREFIX + worker.workerId()),
                    utf8(encode(worker)),
                    PutOption.builder().withLeaseId(leaseId).build())
                    .get(10, TimeUnit.SECONDS);

            // keepAlive renews in the background for as long as the returned client is open.
            // Closing it -- or dying -- stops the renewals, and etcd drops the key when the TTL
            // runs out. This is the whole mechanism: nothing has to notice a worker is gone.
            CloseableClient keepAlive = lease.keepAlive(leaseId, new NoOpObserver());

            log.info("registered worker {} at {} with a {}s lease",
                    worker.workerId(), worker.address(), ttlSeconds);

            return () -> {
                keepAlive.close();
                try {
                    lease.revoke(leaseId).get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // A revoke that fails is not a problem: the lease expires on its own, which
                    // is exactly the path a crashed worker takes.
                    log.debug("could not revoke lease for {}", worker.workerId(), e);
                }
            };
        } catch (Exception e) {
            throw new MetadataStoreException("could not register worker " + worker.workerId(), e);
        }
    }

    @Override
    public List<RegisteredWorker> listWorkers() {
        try {
            List<KeyValue> entries = kv.get(utf8(WORKERS_PREFIX),
                            GetOption.builder().isPrefix(true).build())
                    .get(10, TimeUnit.SECONDS).getKvs();

            List<RegisteredWorker> workers = new ArrayList<>(entries.size());
            for (KeyValue entry : entries) {
                workers.add(decode(text(entry.getValue())));
            }
            // Sorted so that compiling the same job against the same cluster twice produces the
            // same plan, whatever order etcd happened to return.
            workers.sort(Comparator.comparing(RegisteredWorker::workerId));
            return workers;
        } catch (Exception e) {
            throw new MetadataStoreException("could not list workers", e);
        }
    }

    @Override
    public AutoCloseable watchWorkers(Consumer<List<RegisteredWorker>> onChange) {
        Watch.Watcher watcher = watch.watch(
                utf8(WORKERS_PREFIX),
                io.etcd.jetcd.options.WatchOption.builder().isPrefix(true).build(),
                response -> onChange.accept(listWorkers()));
        return watcher::close;
    }

    // ---------------------------------------------------------------------------------

    @Override
    public void close() {
        client.close();
    }

    private void put(String key, ByteSequence value) {
        try {
            kv.put(utf8(key), value).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MetadataStoreException("interrupted writing " + key, e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new MetadataStoreException("could not write " + key, e);
        }
    }

    private Optional<ByteSequence> get(String key) {
        try {
            List<KeyValue> entries = kv.get(utf8(key)).get(10, TimeUnit.SECONDS).getKvs();
            return entries.isEmpty() ? Optional.empty() : Optional.of(entries.getFirst().getValue());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MetadataStoreException("interrupted reading " + key, e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new MetadataStoreException("could not read " + key, e);
        }
    }

    private List<String> prefixKeys(String prefix) {
        try {
            return kv.get(utf8(prefix), GetOption.builder().isPrefix(true).build())
                    .get(10, TimeUnit.SECONDS).getKvs().stream()
                    .map(entry -> text(entry.getKey()))
                    .toList();
        } catch (Exception e) {
            throw new MetadataStoreException("could not list " + prefix, e);
        }
    }

    private static String jobKey(String jobId, String leaf) {
        return JOBS_PREFIX + jobId + "/" + leaf;
    }

    private static String encode(RegisteredWorker worker) {
        return String.join("|", worker.workerId(), worker.host(),
                String.valueOf(worker.rpcPort()), String.valueOf(worker.dataPort()),
                String.valueOf(worker.slots()));
    }

    private static RegisteredWorker decode(String encoded) {
        String[] parts = encoded.split("\\|");
        return new RegisteredWorker(parts[0], parts[1],
                Integer.parseInt(parts[2]), Integer.parseInt(parts[3]),
                Integer.parseInt(parts[4]));
    }

    private static ByteSequence utf8(String value) {
        return ByteSequence.from(value, StandardCharsets.UTF_8);
    }

    private static String text(ByteSequence value) {
        return value.toString(StandardCharsets.UTF_8);
    }

    /** keepAlive requires an observer; the renewals themselves are what matter. */
    private static final class NoOpObserver
            implements io.grpc.stub.StreamObserver<io.etcd.jetcd.lease.LeaseKeepAliveResponse> {

        @Override
        public void onNext(io.etcd.jetcd.lease.LeaseKeepAliveResponse response) {
        }

        @Override
        public void onError(Throwable error) {
            log.warn("lease keep-alive failed; this worker will expire from etcd", error);
        }

        @Override
        public void onCompleted() {
        }
    }
}
