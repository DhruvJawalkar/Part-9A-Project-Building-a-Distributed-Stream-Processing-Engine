package dev.dhruv.streaming.worker;

import com.google.protobuf.ByteString;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.rpc.ChainedOperator;
import dev.dhruv.streaming.rpc.InputChannel;
import dev.dhruv.streaming.rpc.MasterCommand;
import dev.dhruv.streaming.rpc.MasterServiceGrpc;
import dev.dhruv.streaming.rpc.OperatorKind;
import dev.dhruv.streaming.rpc.TaskDeployment;
import dev.dhruv.streaming.rpc.WorkerBeat;
import dev.dhruv.streaming.runtime.SerializationUtil;
import dev.dhruv.streaming.runtime.transport.DataTransportClient;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.TaskInputRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class WorkerObservabilityTest {

    @TempDir
    Path directory;

    @Test
    void removesCancelledGaugesAndKeepsJobIdentityAcrossRedeployment() throws Exception {
        try (DataTransportClient transport = new DataTransportClient(32);
             TaskManager manager = new TaskManager("worker-a", "localhost", 0,
                     new TaskInputRegistry(), transport, directory);
             WorkerMetricsServer metrics = new WorkerMetricsServer(0, "worker-a", manager)) {
            metrics.start();
            manager.deploy(deployment("job-one"));
            RunningTask task = manager.runningTasks().get("transform#0");
            InputGate gate = task.inputGate();
            gate.blockChannel(0);
            for (int i = 0; i < InputGate.DEFAULT_CHANNEL_CAPACITY; i++) {
                gate.enqueue(0, new StreamRecord<>("blocked", i));
            }
            assertThat(task.inputQueuedElements()).isLessThan(task.inputCapacity());
            assertThat(task.isBackpressured()).isTrue();
            String first = scrape(metrics);
            assertThat(first).contains("job=\"job-one\"", "stream_engine_backpressured");
            assertThat(first.lines().filter(line -> line.startsWith("stream_engine_backpressured{")))
                    .allMatch(line -> line.endsWith(" 1.0"));

            // Redeploy before the next scrape: keying gauges solely by operator#subtask would
            // retain the old job's label and update it with the new job's values.
            manager.cancel("transform", 0);
            manager.deploy(deployment("job-two"));
            String second = scrape(metrics);
            assertThat(second).contains("job=\"job-two\"").doesNotContain("job=\"job-one\"");
            assertThat(second.lines().filter(line -> line.startsWith("stream_engine_records_in_total{")))
                    .hasSize(1);
            manager.cancel("transform", 0);
            assertThat(scrape(metrics)).doesNotContain("job=\"job-two\"");
        }
    }

    @Test
    void heartbeatDistinguishesNoWatermarkFromEstablishedEpochWatermark() throws Exception {
        List<WorkerBeat> beats = new CopyOnWriteArrayList<>();
        Server server = ServerBuilder.forPort(0).addService(new MasterServiceGrpc.MasterServiceImplBase() {
            @Override
            public StreamObserver<WorkerBeat> heartbeat(StreamObserver<MasterCommand> responses) {
                return new StreamObserver<>() {
                    @Override public void onNext(WorkerBeat beat) { beats.add(beat); }
                    @Override public void onError(Throwable error) { }
                    @Override public void onCompleted() { responses.onCompleted(); }
                };
            }
        }).build().start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext().build();
        try (DataTransportClient transport = new DataTransportClient(32);
             TaskManager manager = new TaskManager("worker-a", "localhost", 0,
                     new TaskInputRegistry(), transport, directory);
             HeartbeatClient heartbeat = new HeartbeatClient("worker-a",
                     MasterServiceGrpc.newStub(channel), manager)) {
            manager.deploy(deployment("job-one"));
            heartbeat.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> !beats.isEmpty());
            assertThat(beats.getFirst().getTasks(0).hasCurrentWatermark()).isFalse();
            RunningTask task = manager.runningTasks().get("transform#0");
            task.inputGate().enqueue(0, new Watermark(0));
            task.inputGate().enqueue(1, new Watermark(0));
            await().atMost(Duration.ofSeconds(5)).until(() -> beats.stream()
                    .anyMatch(beat -> beat.getTasks(0).hasCurrentWatermark()));
            assertThat(beats.getLast().getTasks(0).getCurrentWatermark()).isZero();
            assertThat(task.currentWatermark()).isZero();
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static TaskDeployment deployment(String jobId) {
        Operator<String, String> operator = (record, output) -> { };
        return TaskDeployment.newBuilder().setJobId(jobId).setSubtaskIndex(0).setParallelism(1)
                .addOperators(ChainedOperator.newBuilder().setOperatorId("transform")
                        .setKind(OperatorKind.OPERATOR_TRANSFORM)
                        .setSerializedOperator(ByteString.copyFrom(SerializationUtil.toBytes(operator))))
                .addInputs(InputChannel.newBuilder().setUpstreamOperatorId("left"))
                .addInputs(InputChannel.newBuilder().setUpstreamOperatorId("right"))
                .build();
    }

    private static String scrape(WorkerMetricsServer metrics) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + metrics.port() + "/metrics")).build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }
}
