package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.metadata.InMemoryMetadataStore;
import dev.dhruv.streaming.metadata.JobState;
import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.rpc.TaskId;
import dev.dhruv.streaming.rpc.TaskState;
import dev.dhruv.streaming.rpc.TaskStatus;
import dev.dhruv.streaming.rpc.SourcePartitionLag;
import dev.dhruv.streaming.rpc.CheckpointAck;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StatusApiTest {

    private final InMemoryMetadataStore metadata = new InMemoryMetadataStore();
    private final JobMaster master = new JobMaster(metadata, new NoopDeployer());
    private StatusApi api;

    @AfterEach
    void close() {
        if (api != null) {
            api.close();
        }
        master.close();
        metadata.close();
    }

    @Test
    void exposesDeterministicJobTaskCheckpointAndLagViewsAndCancels() throws Exception {
        metadata.registerWorker(new RegisteredWorker("worker-a", "localhost", 1, 2, 8), 60);
        JobGraph graph = graph();
        ExecutionGraph plan = master.submit(graph, new byte[] {1});
        String taskKey = plan.assignments().keySet().stream().sorted().findFirst().orElseThrow();
        int separator = taskKey.lastIndexOf(':');
        master.onTaskStatus(TaskStatus.newBuilder()
                .setTaskId(TaskId.newBuilder().setJobId(graph.jobId())
                        .setOperatorId(taskKey.substring(0, separator))
                        .setSubtaskIndex(Integer.parseInt(taskKey.substring(separator + 1))))
                .setState(TaskState.TASK_RUNNING).setRecordsIn(17).setRecordsOut(13)
                .setLastCheckpointId(2).setLastCheckpointDurationMillis(7)
                .setLastCheckpointStateBytes(21).setLastAlignmentMillis(3)
                .setInputQueuedElements(512).setInputCapacity(512).setBackpressured(true)
                .setSourceLagAvailable(true).addSourceLag(SourcePartitionLag.newBuilder()
                        .setTopic("clicks").setPartition(2).setLagRecords(9)).build());
        api = new StatusApi(0, master, metadata);
        api.start();

        HttpResponse<String> jobs = get("/jobs");
        assertThat(jobs.statusCode()).isEqualTo(200);
        assertThat(jobs.body()).startsWith("[").contains("\"jobId\":\"" + graph.jobId() + "\"",
                "\"name\":\"status-api\"", "\"state\":\"RUNNING\"", "\"startedAt\":");

        assertThat(get("/jobs/" + graph.jobId()).body()).contains("\"state\":\"RUNNING\"",
                "\"vertices\":[", "\"restartCount\":0");

        HttpResponse<String> tasks = get("/jobs/" + graph.jobId() + "/tasks");
        assertThat(tasks.body()).startsWith("[").contains("\"operatorId\":", "\"subtask\":",
                "\"worker\":", "\"recordsIn\":17", "\"recordsOut\":13", "\"watermark\":null",
                "\"backpressured\":true");
        long checkpointId = master.triggerCheckpoint(graph.jobId()).orElseThrow();
        plan.assignments().keySet().forEach(key -> master.acknowledgeCheckpoint(ack(graph.jobId(), key,
                checkpointId)));
        assertThat(get("/jobs/" + graph.jobId() + "/checkpoints").body())
                .contains("\"intervalMs\":10000", "\"completed\":1", "\"failed\":0",
                        "\"id\":1", "\"durationMs\":", "\"stateBytes\":",
                        "\"alignmentMs\":", "\"completedAt\":", "\"inFlightCheckpointId\":null");
        assertThat(get("/jobs/" + graph.jobId() + "/lag").body())
                .contains("\"available\":true", "\"maxLagMillis\":null", "\"lagRecords\":9",
                        "\"topic\":\"clicks\"");
        assertThat(get("/metrics").body()).contains("stream_engine_records_in_total", "17.0",
                "stream_engine_source_lag_records", "9.0");

        HttpResponse<String> cancelled = request("POST", "/jobs/" + graph.jobId() + "/cancel");
        assertThat(cancelled.statusCode()).isEqualTo(202);
        assertThat(master.stateOf(graph.jobId())).contains(JobState.CANCELLED);
        assertThat(request("POST", "/jobs").statusCode()).isEqualTo(405);
        assertThat(get("/jobs/missing").statusCode()).isEqualTo(404);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return request("GET", path);
    }

    private HttpResponse<String> request(String method, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + api.port() + path))
                .method(method, HttpRequest.BodyPublishers.noBody()).build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static JobGraph graph() {
        JobGraph.Builder job = JobGraph.named("status-api");
        job.source("source", source()).parallelism(1)
                .sink("sink", sink()).parallelism(1);
        return job.build();
    }

    private static Source<String> source() {
        return new Source<>() {
            @Override public void open(SourceContext context) { }
            @Override public boolean poll(Collector<String> output) { return false; }
            @Override public void close() { }
        };
    }

    private static Operator<String, Void> sink() {
        return (value, output) -> { };
    }

    private static CheckpointAck ack(String jobId, String key, long checkpointId) {
        int separator = key.lastIndexOf(':');
        return CheckpointAck.newBuilder().setTaskId(TaskId.newBuilder().setJobId(jobId)
                        .setOperatorId(key.substring(0, separator))
                        .setSubtaskIndex(Integer.parseInt(key.substring(separator + 1))))
                .setCheckpointId(checkpointId).setStateHandleUri("file:///checkpoint/" + key)
                .setStateSizeBytes(4).setAlignmentMillis(2).build();
    }

    private static final class NoopDeployer implements TaskDeployer {
        @Override public void deploy(JobGraph graph, ExecutionGraph plan, List<RegisteredWorker> workers) { }
        @Override public void triggerSources(JobGraph graph, ExecutionGraph plan, long checkpointId,
                                             long triggerTimestamp) { }
        @Override public void cancelAll(String jobId, ExecutionGraph plan) { }
    }
}
