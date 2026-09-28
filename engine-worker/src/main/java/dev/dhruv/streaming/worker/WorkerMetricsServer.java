package dev.dhruv.streaming.worker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Prometheus scrape endpoint backed by the worker's live task objects. */
final class WorkerMetricsServer implements AutoCloseable {

    private final String workerId;
    private final TaskManager tasks;
    private final HttpServer server;
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final Map<String, TaskMeters> meters = new ConcurrentHashMap<>();

    WorkerMetricsServer(int port, String workerId, TaskManager tasks) throws IOException {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("metrics port must be between 0 and 65535");
        }
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", this::metrics);
        server.setExecutor(Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "worker-metrics-api");
            thread.setDaemon(true);
            return thread;
        }));
        new JvmMemoryMetrics().bindTo(registry);
    }

    void start() {
        server.start();
    }

    int port() {
        return server.getAddress().getPort();
    }

    private void metrics(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", "GET");
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }
        refresh();
        byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private void refresh() {
        for (RunningTask task : tasks.runningTasks().values()) {
            TaskMeters current = meters.computeIfAbsent(task.taskKey(), ignored -> register(task));
            current.recordsIn.set(task.recordsIn());
            current.recordsOut.set(task.recordsOut());
            current.queued.set(task.inputQueuedElements());
            current.capacity.set(task.inputCapacity());
            current.backpressured.set(task.isBackpressured() ? 1 : 0);
            current.checkpointDuration.set(task.lastCheckpointDurationMillis());
            current.alignment.set(task.lastAlignmentMillis());
            current.stateBytes.set(task.lastCheckpointStateBytes());
            task.sourceLag().ifPresent(lag -> current.sourceLagRecords.set((double) lag.totalLagRecords()));
        }
    }

    private TaskMeters register(RunningTask task) {
        TaskMeters created = new TaskMeters();
        Tags tags = Tags.of("worker", workerId, "job", task.jobId(), "operator", task.operatorId(),
                "subtask", Integer.toString(task.subtaskIndex()));
        Gauge.builder("stream.engine.records.in.total", created.recordsIn, AtomicLong::get).tags(tags).register(registry);
        Gauge.builder("stream.engine.records.out.total", created.recordsOut, AtomicLong::get).tags(tags).register(registry);
        Gauge.builder("stream.engine.input.queued.elements", created.queued, AtomicLong::get).tags(tags).register(registry);
        Gauge.builder("stream.engine.input.capacity", created.capacity, AtomicLong::get).tags(tags).register(registry);
        Gauge.builder("stream.engine.backpressured", created.backpressured, AtomicLong::get).tags(tags).register(registry);
        Gauge.builder("stream.engine.checkpoint.duration.milliseconds", created.checkpointDuration, AtomicLong::get).tags(tags).register(registry);
        Gauge.builder("stream.engine.checkpoint.alignment.milliseconds", created.alignment, AtomicLong::get).tags(tags).register(registry);
        Gauge.builder("stream.engine.state.size.bytes", created.stateBytes, AtomicLong::get).tags(tags).register(registry);
        if (task.supportsSourceLag()) {
            // NaN is Prometheus' explicit "not sampled yet", not a made-up zero during startup.
            Gauge.builder("stream.engine.source.lag.records", created.sourceLagRecords,
                    AtomicReference::get).tags(tags).register(registry);
        }
        return created;
    }

    @Override
    public void close() {
        server.stop(0);
        registry.close();
    }

    private static final class TaskMeters {
        private final AtomicLong recordsIn = new AtomicLong();
        private final AtomicLong recordsOut = new AtomicLong();
        private final AtomicLong queued = new AtomicLong();
        private final AtomicLong capacity = new AtomicLong();
        private final AtomicLong backpressured = new AtomicLong();
        private final AtomicLong checkpointDuration = new AtomicLong();
        private final AtomicLong alignment = new AtomicLong();
        private final AtomicLong stateBytes = new AtomicLong();
        private final AtomicReference<Double> sourceLagRecords = new AtomicReference<>(Double.NaN);
    }
}
