package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end tests for the data plane, over a real gRPC connection on a real socket.
 *
 * <p>These exist because the transport's two worst bugs were both invisible to a unit test and
 * both looked like nothing happening at all -- no error, every component healthy, and no records
 * arriving. A test that actually opens a connection and sends something is the only kind that
 * catches either.
 */
class CreditFlowControlTest {

    private TaskInputRegistry registry;
    private Server server;
    private DataTransportClient client;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        registry = new TaskInputRegistry();
        port = freePort();
        server = NettyServerBuilder.forPort(port)
                .addService(new DataTransportService(registry))
                .executor(Executors.newCachedThreadPool())
                .build()
                .start();
        client = new DataTransportClient(4);
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.shutdownNow();
    }

    @Test
    @Timeout(30)
    @DisplayName("a sender ships records without waiting to be asked twice")
    void recordsCrossTheWire() throws Exception {
        // The regression test for the credit deadlock. A sender starts with zero credit, so if
        // the receiver only granted credit in response to a buffer, neither side could ever move
        // and this test would hang rather than fail.
        InputGate gate = new InputGate(1);
        registry.register("sessions", 0, gate,
                Map.of(TaskInputRegistry.taskKey("drop-bots", 0), 0));

        ResultSubpartition subpartition = client.openSubpartition(
                TaskInputRegistry.taskKey("drop-bots", 0), "sessions", 0, "localhost", port);

        subpartition.add(new StreamRecord<>("m-1001", 1757836800000L));
        subpartition.add(new StreamRecord<>("m-1002", 1757836820000L));
        subpartition.flush();

        List<Object> received = drain(gate, 2);

        assertThat(received).hasSize(2);
        assertThat(((StreamRecord<?>) received.getFirst()).value()).isEqualTo("m-1001");
        assertThat(((StreamRecord<?>) received.get(1)).timestamp()).isEqualTo(1757836820000L);
    }

    @Test
    @Timeout(30)
    @DisplayName("watermarks and records arrive in the order they were sent")
    void preservesOrderAcrossTheWire() throws Exception {
        // Order is the property that makes a marker mean anything: "everything before this
        // barrier" is only well defined because the channel does not reorder.
        InputGate gate = new InputGate(1);
        registry.register("sessions", 0, gate,
                Map.of(TaskInputRegistry.taskKey("drop-bots", 0), 0));

        ResultSubpartition subpartition = client.openSubpartition(
                TaskInputRegistry.taskKey("drop-bots", 0), "sessions", 0, "localhost", port);

        subpartition.add(new StreamRecord<>("first", 1L));
        subpartition.add(new Watermark(500L));
        subpartition.add(new StreamRecord<>("second", 600L));
        subpartition.flush();

        List<Object> received = drain(gate, 3);

        assertThat(received).hasSize(3);
        assertThat(received.get(1)).isEqualTo(new Watermark(500L));
    }

    @Test
    @Timeout(30)
    @DisplayName("keeps sending well past the initial credit allowance")
    void creditIsReplenished() throws Exception {
        // Four buffers of initial credit, and far more than four buffers' worth of records. If
        // credit were granted once and never renewed, this would stall partway through.
        InputGate gate = new InputGate(1, 4096, channel -> {
        });
        registry.register("sessions", 0, gate,
                Map.of(TaskInputRegistry.taskKey("drop-bots", 0), 0));

        ResultSubpartition subpartition = client.openSubpartition(
                TaskInputRegistry.taskKey("drop-bots", 0), "sessions", 0, "localhost", port);

        int total = 2_000;
        for (int i = 0; i < total; i++) {
            subpartition.add(new StreamRecord<>("v" + i, i));
        }
        subpartition.flush();

        await().atMost(20, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(gate.queuedElements()).isEqualTo(total));
    }

    @Test
    @Timeout(30)
    @DisplayName("a buffer for a task that is not here is dropped, not fatal")
    void unknownDestinationIsDropped() throws Exception {
        // Legitimate rather than exceptional: a buffer can arrive moments after its destination
        // was cancelled. The receiver must not die of it.
        ResultSubpartition subpartition = client.openSubpartition(
                TaskInputRegistry.taskKey("drop-bots", 0), "nobody", 0, "localhost", port);

        subpartition.add(new StreamRecord<>("orphan", 1L));
        subpartition.flush();

        Thread.sleep(500);
        assertThat(server.isTerminated()).isFalse();
    }

    private static List<Object> drain(InputGate gate, int count) throws InterruptedException {
        List<Object> taken = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Optional<InputGate.IncomingElement> element = gate.poll(10, TimeUnit.SECONDS);
            if (element.isEmpty()) {
                break;
            }
            taken.add(element.get().element());
        }
        return taken;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
