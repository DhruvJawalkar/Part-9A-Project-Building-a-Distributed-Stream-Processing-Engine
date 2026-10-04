package dev.dhruv.streaming.worker;

import com.google.protobuf.ByteString;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.rpc.ChainedOperator;
import dev.dhruv.streaming.rpc.MasterCommand;
import dev.dhruv.streaming.rpc.MasterServiceGrpc;
import dev.dhruv.streaming.rpc.OperatorKind;
import dev.dhruv.streaming.rpc.TaskDeployment;
import dev.dhruv.streaming.rpc.TaskState;
import dev.dhruv.streaming.rpc.WorkerBeat;
import dev.dhruv.streaming.runtime.SerializationUtil;
import dev.dhruv.streaming.runtime.transport.DataTransportClient;
import dev.dhruv.streaming.runtime.transport.TaskInputRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class HeartbeatClientTest {
    @TempDir
    Path directory;

    @Test
    void synchronousOpenFailureCannotReviveItsRetiredRequestObserver() throws Exception {
        AtomicInteger openings = new AtomicInteger();
        AtomicInteger healthyBeats = new AtomicInteger();
        AtomicInteger failedGenerationBeats = new AtomicInteger();
        try (DataTransportClient transport = new DataTransportClient(32);
             TaskManager manager = manager(transport);
             HeartbeatClient heartbeat = new HeartbeatClient("worker-a", responses -> {
                 if (openings.incrementAndGet() == 1) {
                     // gRPC may call onError before heartbeat(...) returns its request observer.
                     responses.onError(Status.UNAVAILABLE.asRuntimeException());
                     return requests(failedGenerationBeats, false);
                 }
                 return requests(healthyBeats, false);
             }, manager)) {
            heartbeat.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> healthyBeats.get() >= 2);
            assertThat(openings.get()).isEqualTo(2);
            assertThat(failedGenerationBeats.get()).isZero();
        }
    }

    @Test
    void delayedOldCallbacksCannotRetireTheReplacementStream() throws Exception {
        List<StreamObserver<MasterCommand>> responses = new CopyOnWriteArrayList<>();
        AtomicInteger healthyBeats = new AtomicInteger();
        try (DataTransportClient transport = new DataTransportClient(32);
             TaskManager manager = manager(transport);
             HeartbeatClient heartbeat = new HeartbeatClient("worker-a", replies -> {
                 responses.add(replies);
                 return requests(healthyBeats, responses.size() == 1);
             }, manager)) {
            heartbeat.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> healthyBeats.get() >= 1);
            assertThat(responses).hasSize(2);
            responses.getFirst().onError(Status.UNAVAILABLE.withDescription("late old-stream failure")
                    .asRuntimeException());
            responses.getFirst().onCompleted();
            await().atMost(Duration.ofSeconds(5)).until(() -> healthyBeats.get() >= 3);
            assertThat(responses).hasSize(2);
        }
    }

    @Test
    void reopenedGrpcStreamReportsTasksThatFinishedDuringMasterOutage() throws Exception {
        List<WorkerBeat> firstBeats = new CopyOnWriteArrayList<>();
        List<WorkerBeat> recoveredBeats = new CopyOnWriteArrayList<>();
        Server server = startServer(0, firstBeats);
        int port = server.getPort();
        Server replacement = null;
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        try (DataTransportClient transport = new DataTransportClient(32);
             TaskManager manager = manager(transport);
             HeartbeatClient heartbeat = new HeartbeatClient("worker-a",
                     MasterServiceGrpc.newStub(channel), manager)) {
            Operator<String, String> operator = (record, output) -> { };
            manager.deploy(TaskDeployment.newBuilder().setJobId("bounded-job").setParallelism(1)
                    .addOperators(ChainedOperator.newBuilder().setOperatorId("bounded")
                            .setKind(OperatorKind.OPERATOR_TRANSFORM)
                            .setSerializedOperator(ByteString.copyFrom(SerializationUtil.toBytes(operator))))
                    .build());
            heartbeat.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> !firstBeats.isEmpty());
            assertThat(firstBeats.getFirst().getTasks(0).getState()).isEqualTo(TaskState.TASK_RUNNING);

            server.shutdownNow();
            assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            RunningTask task = manager.runningTasks().get("bounded#0");
            task.inputGate().enqueue(0, Watermark.MAX);
            await().atMost(Duration.ofSeconds(5)).until(task::isFinished);
            replacement = startServer(port, recoveredBeats);
            await().atMost(Duration.ofSeconds(10)).until(() -> recoveredBeats.stream()
                    .anyMatch(beat -> beat.getTasksCount() == 1
                            && beat.getTasks(0).getState() == TaskState.TASK_FINISHED));
            assertThat(recoveredBeats.getLast().getTasks(0).hasCurrentWatermark()).isTrue();
            assertThat(recoveredBeats.getLast().getTasks(0).getCurrentWatermark()).isEqualTo(Long.MAX_VALUE);
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            if (replacement != null) {
                replacement.shutdownNow();
            }
        }
    }

    private TaskManager manager(DataTransportClient transport) {
        return new TaskManager("worker-a", "localhost", 0, new TaskInputRegistry(), transport, directory);
    }

    private static StreamObserver<WorkerBeat> requests(AtomicInteger beats, boolean failSending) {
        return new StreamObserver<>() {
            @Override public void onNext(WorkerBeat beat) {
                if (failSending) {
                    throw Status.UNAVAILABLE.asRuntimeException();
                }
                beats.incrementAndGet();
            }
            @Override public void onError(Throwable error) { }
            @Override public void onCompleted() { }
        };
    }

    private static Server startServer(int port, List<WorkerBeat> beats) throws Exception {
        return ServerBuilder.forPort(port).addService(new MasterServiceGrpc.MasterServiceImplBase() {
            @Override
            public StreamObserver<WorkerBeat> heartbeat(StreamObserver<MasterCommand> responses) {
                return new StreamObserver<>() {
                    @Override public void onNext(WorkerBeat beat) { beats.add(beat); }
                    @Override public void onError(Throwable error) { }
                    @Override public void onCompleted() { responses.onCompleted(); }
                };
            }
        }).build().start();
    }
}
