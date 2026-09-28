package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.runtime.transport.InputGate;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BarrierAlignerTest {

    @Test
    void blocksArrivedChannelsContinuesOtherInputsThenDrainsAfterCompletion() throws Exception {
        InputGate gate = new InputGate(2);
        AtomicLong clock = new AtomicLong(1_000_000L);
        BarrierAligner aligner = new BarrierAligner(gate, clock::get);
        CheckpointBarrier barrier = new CheckpointBarrier(7, 123L);

        gate.enqueue(0, barrier);
        gate.enqueue(0, new StreamRecord<>("after-channel-zero-barrier", 2));
        gate.enqueue(1, new StreamRecord<>("still-before-checkpoint", 3));

        InputGate.IncomingElement first = gate.poll(1, TimeUnit.SECONDS).orElseThrow();
        assertThat(first.element()).isEqualTo(barrier);
        assertThat(aligner.onBarrier(barrier, first.channelIndex())).isEmpty();
        assertThat(gate.blockedChannelCount()).isEqualTo(1);

        // The record behind channel zero's barrier stays queued; channel one keeps making the
        // pre-checkpoint prefix complete.
        InputGate.IncomingElement continued = gate.poll(1, TimeUnit.SECONDS).orElseThrow();
        assertThat(continued.channelIndex()).isEqualTo(1);
        assertThat(continued.element()).isEqualTo(new StreamRecord<>("still-before-checkpoint", 3));

        gate.enqueue(1, barrier);
        InputGate.IncomingElement last = gate.poll(1, TimeUnit.SECONDS).orElseThrow();
        clock.addAndGet(6_500_000L);
        BarrierAligner.AlignedCheckpoint aligned = aligner.onBarrier(
                (CheckpointBarrier) last.element(), last.channelIndex()).orElseThrow();

        assertThat(aligned.barrier()).isEqualTo(barrier);
        assertThat(aligned.alignmentMillis()).isEqualTo(6);
        assertThat(aligner.isSnapshotReady()).isTrue();
        assertThat(gate.blockedChannelCount()).isEqualTo(2);

        // OperatorTask will snapshot, forward the barrier, then acknowledge before this call.
        aligner.completeAlignment(7);
        assertThat(gate.blockedChannelCount()).isZero();
        assertThat(gate.poll(1, TimeUnit.SECONDS).orElseThrow().element())
                .isEqualTo(new StreamRecord<>("after-channel-zero-barrier", 2));
    }

    @Test
    void rejectsAnOvertakingOrRepeatedBarrierAndCanAbortTheStalledAlignment() {
        InputGate gate = new InputGate(2);
        BarrierAligner aligner = new BarrierAligner(gate, () -> 0L);
        CheckpointBarrier first = new CheckpointBarrier(7, 1L);

        assertThat(aligner.onBarrier(first, 0)).isEmpty();
        assertThatThrownBy(() -> aligner.onBarrier(new CheckpointBarrier(8, 2L), 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("while aligning checkpoint 7");
        assertThatThrownBy(() -> aligner.onBarrier(first, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("more than once");

        aligner.abortAlignment(7);
        assertThat(gate.blockedChannelCount()).isZero();
        assertThat(aligner.arrivedBarrierCount()).isZero();
    }

    @Test
    void singleInputTasksDoNotBlockOrWaitForAlignment() {
        InputGate gate = new InputGate(1);
        BarrierAligner aligner = new BarrierAligner(gate, () -> 999_000_000L);

        assertThat(aligner.onBarrier(new CheckpointBarrier(1, 1L), 0))
                .contains(new BarrierAligner.AlignedCheckpoint(new CheckpointBarrier(1, 1L), 0));
        assertThat(aligner.onBarrier(new CheckpointBarrier(2, 2L), 0))
                .contains(new BarrierAligner.AlignedCheckpoint(new CheckpointBarrier(2, 2L), 0));
        assertThat(gate.blockedChannelCount()).isZero();
    }
}
