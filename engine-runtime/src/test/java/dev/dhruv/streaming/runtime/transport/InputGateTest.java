package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the per-channel input gate.
 *
 * <p>Two of these are really about Phase 3 and Phase 4 -- fairness across channels and the
 * ability to block one without blocking the task. They are worth having now, because both
 * properties are easy to lose in a refactor and neither fails loudly when it goes.
 */
class InputGateTest {

    @Test
    void reportsBackpressureWhenOnlyOneOfSeveralChannelsIsFull() throws Exception {
        InputGate gate = new InputGate(3, 2, ignored -> { });
        assertThat(gate.hasFullChannel()).isFalse();
        gate.enqueue(1, new StreamRecord<>("first", 1));
        assertThat(gate.hasFullChannel()).isFalse();
        gate.enqueue(1, new StreamRecord<>("second", 2));
        assertThat(gate.queuedElements()).isLessThan(gate.totalCapacity());
        assertThat(gate.hasFullChannel()).isTrue();
        gate.poll(0, TimeUnit.MILLISECONDS);
        assertThat(gate.hasFullChannel()).isFalse();
        assertThat(new InputGate(0).hasFullChannel()).isFalse();
    }

    @Test
    @Timeout(10)
    @DisplayName("reports which channel an element arrived on")
    void reportsTheChannel() throws Exception {
        InputGate gate = new InputGate(3);

        gate.enqueue(2, new StreamRecord<>("from-channel-2", 100L));

        Optional<InputGate.IncomingElement> taken = gate.poll(1, TimeUnit.SECONDS);

        assertThat(taken).isPresent();
        assertThat(taken.get().channelIndex()).isEqualTo(2);
    }

    @Test
    @Timeout(10)
    @DisplayName("carries watermarks and barriers as well as records")
    void carriesControlElements() throws Exception {
        InputGate gate = new InputGate(1);

        gate.enqueue(0, new StreamRecord<>("value", 1L));
        gate.enqueue(0, new Watermark(500L));
        gate.enqueue(0, new CheckpointBarrier(7L, 1234L));

        List<Object> taken = drain(gate, 3);

        // In band, in order: the ordering is what makes both watermark propagation and barrier
        // alignment work at all.
        assertThat(taken).hasSize(3);
        assertThat(taken.get(0)).isInstanceOf(StreamRecord.class);
        assertThat(taken.get(1)).isEqualTo(new Watermark(500L));
        assertThat(taken.get(2)).isEqualTo(new CheckpointBarrier(7L, 1234L));
    }

    @Test
    @Timeout(10)
    @DisplayName("visits channels round-robin so a busy one cannot starve a quiet one")
    void pollingIsFair() throws Exception {
        // Fairness is not cosmetic. A starved channel's watermark never advances, which in
        // Phase 3 holds back the whole task's event-time clock.
        InputGate gate = new InputGate(2);

        for (int i = 0; i < 10; i++) {
            gate.enqueue(0, new StreamRecord<>("busy-" + i, i));
        }
        gate.enqueue(1, new StreamRecord<>("quiet", 99L));

        List<Object> firstFour = drain(gate, 4);
        List<String> values = firstFour.stream()
                .map(element -> ((StreamRecord<?>) element).value().toString()).toList();

        assertThat(values)
                .as("the quiet channel must be served within a couple of polls")
                .contains("quiet");
    }

    @Test
    @Timeout(10)
    @DisplayName("a blocked channel stops being consumed while the others continue")
    void blockingOneChannelLeavesTheOthersRunning() throws Exception {
        // Phase 4's alignment in miniature: this is exactly what happens when a barrier arrives
        // on one channel and the task keeps working on the rest.
        InputGate gate = new InputGate(2);

        gate.enqueue(0, new StreamRecord<>("blocked-channel", 1L));
        gate.enqueue(1, new StreamRecord<>("open-channel", 2L));

        gate.blockChannel(0);

        Optional<InputGate.IncomingElement> first = gate.poll(1, TimeUnit.SECONDS);
        Optional<InputGate.IncomingElement> second = gate.poll(200, TimeUnit.MILLISECONDS);

        assertThat(first).isPresent();
        assertThat(first.get().channelIndex()).isEqualTo(1);
        assertThat(second).as("the blocked channel must not be served").isEmpty();
        assertThat(gate.blockedChannelCount()).isEqualTo(1);

        // Nothing was lost: unblocking releases what piled up behind it.
        gate.unblockAllChannels();
        Optional<InputGate.IncomingElement> afterUnblock = gate.poll(1, TimeUnit.SECONDS);
        assertThat(afterUnblock).isPresent();
        assertThat(afterUnblock.get().channelIndex()).isEqualTo(0);
    }

    @Test
    @Timeout(20)
    @DisplayName("a full channel makes its producer wait, which is backpressure")
    void fullChannelBlocksTheProducer() throws Exception {
        InputGate gate = new InputGate(1, 4, channel -> {
        });
        CountDownLatch producerBlocked = new CountDownLatch(1);
        AtomicInteger enqueued = new AtomicInteger();

        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < 8; i++) {
                    gate.enqueue(0, new StreamRecord<>("v" + i, i));
                    enqueued.incrementAndGet();
                    if (enqueued.get() == 4) {
                        producerBlocked.countDown();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "producer");

        producer.start();
        assertThat(producerBlocked.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200);

        // Capacity is four, so the producer is parked on the fifth rather than buffering it.
        assertThat(enqueued.get())
                .as("the producer must wait rather than grow the queue")
                .isEqualTo(4);

        drain(gate, 8);
        producer.join(5_000);
        assertThat(enqueued.get()).isEqualTo(8);
    }

    @Test
    @Timeout(10)
    @DisplayName("reports a drained channel so a remote sender can be granted credit")
    void reportsDrainedChannels() throws Exception {
        List<Integer> drained = new ArrayList<>();
        InputGate gate = new InputGate(2, 16, drained::add);

        gate.enqueue(1, new StreamRecord<>("value", 1L));
        gate.poll(1, TimeUnit.SECONDS);

        assertThat(drained).containsExactly(1);
    }

    @Test
    @Timeout(10)
    @DisplayName("returns empty rather than hanging when nothing arrives")
    void pollTimesOut() throws Exception {
        // The task loop relies on this: it must regain control periodically even on an idle
        // stream, so that it can notice cancellation and, from Phase 3, emit watermarks.
        InputGate gate = new InputGate(1);

        assertThat(gate.poll(100, TimeUnit.MILLISECONDS)).isEmpty();
    }

    private static List<Object> drain(InputGate gate, int count) throws InterruptedException {
        List<Object> taken = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Optional<InputGate.IncomingElement> element = gate.poll(2, TimeUnit.SECONDS);
            if (element.isEmpty()) {
                break;
            }
            taken.add(element.get().element());
        }
        return taken;
    }
}
