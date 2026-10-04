package dev.dhruv.streaming.master;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.dhruv.streaming.metadata.CompletedCheckpoint;
import dev.dhruv.streaming.metadata.MetadataStore;
import dev.dhruv.streaming.rpc.TaskStatus;
import dev.dhruv.streaming.rpc.SourcePartitionLag;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Small, deliberately explicit HTTP view of the master. The control plane remains gRPC; this
 * server is read-mostly operational evidence and has no dependency on a web framework.
 */
public final class StatusApi implements AutoCloseable {

    private final JobMaster jobs;
    private final MetadataStore metadata;
    private final HttpServer server;
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final Map<String, TaskMeters> taskMeters = new ConcurrentHashMap<>();

    public StatusApi(int port, JobMaster jobs, MetadataStore metadata) throws IOException {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("status port must be between 0 and 65535");
        }
        this.jobs = Objects.requireNonNull(jobs, "jobs");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/jobs", this::jobs);
        this.server.createContext("/metrics", this::metrics);
        this.server.setExecutor(Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "master-status-api");
            thread.setDaemon(true);
            return thread;
        }));
        new JvmMemoryMetrics().bindTo(registry);
    }

    /** Starts serving; the requested port may be zero in a focused test. */
    public void start() {
        server.start();
    }

    /** Returns the port actually bound by this instance. */
    public int port() {
        return server.getAddress().getPort();
    }

    private void jobs(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        List<String> segments = pathSegments(exchange.getRequestURI().getPath());
        if (segments.size() == 1) {
            if (!"GET".equals(method)) {
                methodNotAllowed(exchange, "GET");
                return;
            }
            writeJson(exchange, 200, jobsList());
            return;
        }
        if (segments.size() != 2 && segments.size() != 3) {
            notFound(exchange);
            return;
        }
        String jobId = segments.get(1);
        if (metadata.getJobState(jobId).isEmpty()) {
            notFound(exchange);
            return;
        }
        if (segments.size() == 2) {
            if (!"GET".equals(method)) {
                methodNotAllowed(exchange, "GET");
                return;
            }
            writeJson(exchange, 200, job(jobId));
            return;
        }
        String resource = segments.get(2);
        if ("cancel".equals(resource)) {
            if (!"POST".equals(method)) {
                methodNotAllowed(exchange, "POST");
                return;
            }
            if (!jobs.cancelJob(jobId)) {
                writeJson(exchange, 409, "{\"error\":\"job is already terminal\"}");
                return;
            }
            writeJson(exchange, 202, "{\"jobId\":" + quote(jobId) + ",\"state\":\"CANCELLED\"}");
            return;
        }
        if (!"GET".equals(method)) {
            methodNotAllowed(exchange, "GET");
            return;
        }
        switch (resource) {
            case "tasks" -> writeJson(exchange, 200, tasks(jobId));
            case "checkpoints" -> writeJson(exchange, 200, checkpoints(jobId));
            case "lag" -> writeJson(exchange, 200, lag(jobId));
            default -> notFound(exchange);
        }
    }

    private void metrics(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            methodNotAllowed(exchange, "GET");
            return;
        }
        refreshMeters();
        byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private String jobsList() {
        List<String> known = new ArrayList<>(metadata.listJobs());
        known.sort(String::compareTo);
        return "[" + known.stream().map(this::job).reduce((left, right) -> left + "," + right)
                .orElse("") + "]";
    }

    private String job(String jobId) {
        Map<String, String> assignments = metadata.getAssignments(jobId);
        return "{\"jobId\":" + quote(jobId)
                + ",\"name\":" + jobs.jobName(jobId).map(StatusApi::quote).orElse("null")
                + ",\"state\":" + quote(metadata.getJobState(jobId).orElseThrow().name())
                + ",\"startedAt\":" + jobs.startedAt(jobId).map(value -> quote(value.toString())).orElse("null")
                + ",\"taskCount\":" + assignments.size()
                + ",\"restartCount\":" + jobs.restartCount(jobId)
                + ",\"vertices\":" + vertices(jobId)
                + ",\"failureCause\":" + metadata.getJobFailureCause(jobId).map(StatusApi::quote).orElse("null")
                + "}";
    }

    private String tasks(String jobId) {
        Map<String, String> assignments = metadata.getAssignments(jobId);
        Map<String, TaskStatus> statuses = jobs.taskStatuses(jobId);
        List<String> keys = new ArrayList<>(assignments.keySet());
        statuses.keySet().stream().filter(key -> !assignments.containsKey(key)).forEach(keys::add);
        keys.sort(String::compareTo);
        List<String> items = new ArrayList<>();
        for (String key : keys) {
            TaskStatus status = statuses.get(key);
            items.add(task(key, assignments.get(key), status));
        }
        return "[" + String.join(",", items) + "]";
    }

    private String checkpoints(String jobId) {
        CheckpointCoordinator.Status status = jobs.checkpointStatus(jobId);
        String latest = metadata.getLatestCompletedCheckpoint(jobId)
                .map(this::checkpoint).orElse("null");
        String inFlight = jobs.inFlightCheckpoint(jobId).map(String::valueOf).orElse("null");
        return "{\"jobId\":" + quote(jobId)
                + ",\"intervalMs\":" + status.intervalMillis()
                + ",\"completed\":" + status.completed()
                + ",\"failed\":" + status.failed()
                + ",\"history\":" + checkpointHistory(status.history())
                + ",\"inFlightCheckpointId\":" + inFlight + "}";
    }

    private String lag(String jobId) {
        Map<String, SourcePartitionLag> partitions = new java.util.TreeMap<>();
        boolean available = false;
        for (TaskStatus status : jobs.taskStatuses(jobId).values()) {
            available |= status.getSourceLagAvailable();
            status.getSourceLagList().forEach(partition -> partitions.put(
                    partition.getTopic() + ':' + partition.getPartition(), partition));
        }
        long total = partitions.values().stream().mapToLong(SourcePartitionLag::getLagRecords).sum();
        String entries = partitions.values().stream().map(partition -> "{\"topic\":"
                        + quote(partition.getTopic()) + ",\"partition\":" + partition.getPartition()
                        + ",\"lagRecords\":" + partition.getLagRecords() + "}")
                .reduce((left, right) -> left + "," + right).orElse("");
        return "{\"jobId\":" + quote(jobId) + ",\"available\":" + available
                + ",\"lagRecords\":" + total + ",\"maxLagMillis\":null,\"perPartition\":["
                + entries + "]}";
    }

    private static String checkpointHistory(List<CheckpointCoordinator.Completed> history) {
        return "[" + history.stream().map(checkpoint -> "{\"id\":" + checkpoint.id()
                + ",\"durationMs\":" + checkpoint.durationMs()
                + ",\"stateBytes\":" + checkpoint.stateBytes()
                + ",\"alignmentMs\":" + checkpoint.alignmentMs()
                + ",\"completedAt\":" + checkpoint.completedAt() + "}")
                .reduce((left, right) -> left + "," + right).orElse("") + "]";
    }

    private static String task(String taskId, String workerId, TaskStatus status) {
        if (status == null) {
            return "{\"operatorId\":" + quote(operatorId(taskId)) + ",\"subtask\":" + subtask(taskId)
                    + ",\"worker\":" + nullableQuote(workerId)
                    + ",\"state\":\"UNKNOWN\",\"heartbeatReceived\":false,\"watermark\":null,\"backpressured\":null}";
        }
        return "{\"operatorId\":" + quote(operatorId(taskId)) + ",\"subtask\":" + subtask(taskId)
                + ",\"worker\":" + nullableQuote(workerId)
                + ",\"state\":" + quote(status.getState().name())
                + ",\"heartbeatReceived\":true"
                + ",\"watermark\":" + (status.hasCurrentWatermark()
                        ? Long.toString(status.getCurrentWatermark()) : "null")
                + ",\"recordsIn\":" + status.getRecordsIn()
                + ",\"recordsOut\":" + status.getRecordsOut()
                + ",\"lastCheckpointId\":" + status.getLastCheckpointId()
                + ",\"lastCheckpointDurationMillis\":" + status.getLastCheckpointDurationMillis()
                + ",\"lastCheckpointStateBytes\":" + status.getLastCheckpointStateBytes()
                + ",\"lastAlignmentMillis\":" + status.getLastAlignmentMillis()
                + ",\"inputQueuedElements\":" + status.getInputQueuedElements()
                + ",\"inputCapacity\":" + status.getInputCapacity()
                + ",\"backpressured\":" + status.getBackpressured() + "}";
    }

    private String checkpoint(CompletedCheckpoint checkpoint) {
        List<String> tasks = checkpoint.taskStates().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> "{\"taskId\":" + quote(entry.getKey())
                        + ",\"stateSizeBytes\":" + entry.getValue().stateSizeBytes()
                        + ",\"alignmentMillis\":" + entry.getValue().alignmentMillis() + "}")
                .toList();
        return "{\"checkpointId\":" + checkpoint.checkpointId()
                + ",\"triggerTimestamp\":" + checkpoint.triggerTimestamp()
                + ",\"tasks\":[" + String.join(",", tasks) + "]}";
    }

    private String vertices(String jobId) {
        return jobs.planOf(jobId).map(plan -> "[" + plan.vertices().stream()
                .sorted(java.util.Comparator.comparing(dev.dhruv.streaming.master.graph.ExecutionVertex::id))
                .map(vertex -> "{\"operatorId\":" + quote(vertex.operatorId())
                        + ",\"subtask\":" + vertex.subtaskIndex()
                        + ",\"parallelism\":" + vertex.parallelism() + "}")
                .reduce((left, right) -> left + "," + right).orElse("") + "]").orElse("[]");
    }

    private static String operatorId(String taskId) {
        return taskId.substring(0, taskId.lastIndexOf(':'));
    }

    private static int subtask(String taskId) {
        return Integer.parseInt(taskId.substring(taskId.lastIndexOf(':') + 1));
    }

    private synchronized void refreshMeters() {
        Set<String> active = new HashSet<>();
        for (String jobId : metadata.listJobs()) {
            if (metadata.getJobState(jobId).map(state -> state.isTerminal()).orElse(true)) {
                continue;
            }
            for (Map.Entry<String, TaskStatus> entry : jobs.taskStatuses(jobId).entrySet()) {
                TaskStatus status = entry.getValue();
                String identity = jobId + '\u0000' + entry.getKey();
                active.add(identity);
                TaskMeters meters = taskMeters.computeIfAbsent(identity, ignored -> {
                    TaskMeters created = new TaskMeters();
                    Tags tags = Tags.of("job", jobId, "operator", operatorId(entry.getKey()),
                            "subtask", Integer.toString(subtask(entry.getKey())));
                    Gauge.builder("stream.engine.records.in.total", created.recordsIn, AtomicLong::get).tags(tags).register(registry);
                    Gauge.builder("stream.engine.records.out.total", created.recordsOut, AtomicLong::get).tags(tags).register(registry);
                    Gauge.builder("stream.engine.checkpoint.duration.milliseconds", created.checkpointDuration, AtomicLong::get).tags(tags).register(registry);
                    Gauge.builder("stream.engine.checkpoint.alignment.milliseconds", created.alignment, AtomicLong::get).tags(tags).register(registry);
                    Gauge.builder("stream.engine.state.size.bytes", created.stateBytes, AtomicLong::get).tags(tags).register(registry);
                    Gauge.builder("stream.engine.current.watermark", created.watermark, AtomicReference::get)
                            .tags(tags).register(registry);
                    Gauge.builder("stream.engine.source.lag.records", created.sourceLag, AtomicReference::get)
                            .tags(tags).register(registry);
                    created.registered = registry.getMeters().stream()
                            .filter(meter -> java.util.stream.StreamSupport.stream(tags.spliterator(), false).allMatch(tag ->
                                    tag.getValue().equals(meter.getId().getTag(tag.getKey())))).toList();
                    return created;
                });
                meters.recordsIn.set(status.getRecordsIn());
                meters.recordsOut.set(status.getRecordsOut());
                meters.checkpointDuration.set(status.getLastCheckpointDurationMillis());
                meters.alignment.set(status.getLastAlignmentMillis());
                meters.stateBytes.set(status.getLastCheckpointStateBytes());
                meters.watermark.set(status.hasCurrentWatermark()
                        ? (double) status.getCurrentWatermark() : Double.NaN);
                meters.sourceLag.set(status.getSourceLagAvailable()
                        ? (double) status.getSourceLagList().stream()
                                .mapToLong(SourcePartitionLag::getLagRecords).sum() : Double.NaN);
            }
        }
        taskMeters.entrySet().removeIf(entry -> {
            if (active.contains(entry.getKey())) {
                return false;
            }
            entry.getValue().registered.forEach(registry::remove);
            return true;
        });
    }

    private static List<String> pathSegments(String path) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            return List.of();
        }
        return java.util.Arrays.stream(path.split("/"))
                .filter(segment -> !segment.isBlank()).toList();
    }

    private static String quote(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 2).append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) {
                        escaped.append("\\u00")
                                .append(Character.forDigit(character >>> 4, 16))
                                .append(Character.forDigit(character & 0xf, 16));
                    } else {
                        escaped.append(character);
                    }
                }
            }
        }
        return escaped.append('"').toString();
    }

    private static String nullableQuote(String value) {
        return value == null ? "null" : quote(value);
    }

    private static void writeJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static void notFound(HttpExchange exchange) throws IOException {
        writeJson(exchange, 404, "{\"error\":\"not found\"}");
    }

    private static void methodNotAllowed(HttpExchange exchange, String allow) throws IOException {
        exchange.getResponseHeaders().set("Allow", allow);
        writeJson(exchange, 405, "{\"error\":\"method not allowed\"}");
    }

    @Override
    public void close() {
        server.stop(0);
        registry.close();
    }

    private static final class TaskMeters {
        private final AtomicLong recordsIn = new AtomicLong();
        private final AtomicLong recordsOut = new AtomicLong();
        private final AtomicLong checkpointDuration = new AtomicLong();
        private final AtomicLong alignment = new AtomicLong();
        private final AtomicLong stateBytes = new AtomicLong();
        private final AtomicReference<Double> watermark = new AtomicReference<>(Double.NaN);
        private final AtomicReference<Double> sourceLag = new AtomicReference<>(Double.NaN);
        private List<Meter> registered = List.of();
    }
}
