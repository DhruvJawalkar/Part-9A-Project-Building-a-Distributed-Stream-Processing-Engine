package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.state.ValueState;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.Output;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the task loop, not only the independent timer and state data structures. */
class OperatorTaskEventTimeTest {

    private static final ConcurrentLinkedQueue<String> TIMER_RESULTS = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<Long> WATERMARKS = new ConcurrentLinkedQueue<>();

    @AfterEach
    void clearStatics() {
        TIMER_RESULTS.clear();
        WATERMARKS.clear();
    }

    @Test
    @DisplayName("restores keyed state for timers and forwards only progressed watermarks")
    void runsTheKeyedEventTimePath() throws Exception {
        InputGate gate = new InputGate(1);
        RecordingOutput output = new RecordingOutput();
        KeySelector<Event, String> selector = Event::key;
        OperatorTask task = new OperatorTask("keyed#0", new TimerOperator(), gate, output,
                java.util.Optional.of(selector), new TaskMetricGroup("keyed", 0));

        Thread thread = new Thread(task);
        thread.start();
        gate.enqueue(0, new StreamRecord<>(new Event("member-1", "saved"), 1));
        gate.enqueue(0, new Watermark(10));
        gate.enqueue(0, new Watermark(10));
        gate.enqueue(0, Watermark.MAX);
        thread.join(2_000);

        assertThat(thread.isAlive()).isFalse();
        assertThat(TIMER_RESULTS).containsExactly("member-1:saved");
        assertThat(WATERMARKS).containsExactly(10L, Long.MAX_VALUE);
        assertThat(output.elements.stream().filter(Watermark.class::isInstance)).hasSize(2);
    }

    @Test
    @DisplayName("an ended input no longer holds back live channels but does not end the task")
    void letsRemainingChannelsAdvanceAfterOneInputEnds() throws Exception {
        InputGate gate = new InputGate(2);
        RecordingOutput output = new RecordingOutput();
        CountDownLatch progressedPastEndedInput = new CountDownLatch(1);
        Operator<String, String> operator = new Operator<>() {
            @Override
            public void processElement(StreamRecord<String> record, Collector<String> out) {
            }

            @Override
            public void onWatermark(long watermark, Collector<String> out) {
                WATERMARKS.add(watermark);
                if (watermark == 200L) {
                    progressedPastEndedInput.countDown();
                }
            }
        };
        OperatorTask task = new OperatorTask("watermarks#0", operator, gate, output,
                new TaskMetricGroup("watermarks", 0));

        Thread thread = new Thread(task);
        thread.start();
        gate.enqueue(0, new Watermark(100));
        gate.enqueue(1, Watermark.MAX);
        gate.enqueue(0, new Watermark(200));

        assertThat(progressedPastEndedInput.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(thread.isAlive()).isTrue();
        assertThat(WATERMARKS).containsExactly(100L, 200L);
        assertThat(output.elements).doesNotContain(Watermark.MAX);

        gate.enqueue(0, Watermark.MAX);
        thread.join(2_000);

        assertThat(thread.isAlive()).isFalse();
        assertThat(WATERMARKS).containsExactly(100L, 200L, Long.MAX_VALUE);
        assertThat(output.elements).containsExactly(new Watermark(100), new Watermark(200),
                Watermark.MAX);
    }

    private record Event(String key, String value) implements java.io.Serializable { }

    private static final class TimerOperator implements KeyedOperator<String, Event, String> {
        private transient ValueState<String> state;
        private transient OperatorContext context;

        @Override
        public void open(OperatorContext context) {
            this.context = context;
            state = context.getValueState("latest", String.class);
        }

        @Override
        public void processElement(StreamRecord<Event> record, Collector<String> out)
                throws Exception {
            state.update(record.value().value());
            context.registerEventTimer(10);
        }

        @Override
        public void onEventTimer(long timestamp, String key, Collector<String> out)
                throws Exception {
            TIMER_RESULTS.add(key + ":" + state.value().orElseThrow());
        }

        @Override
        public void onWatermark(long watermark, Collector<String> out) {
            WATERMARKS.add(watermark);
        }
    }

    private static final class RecordingOutput implements Output {
        private final List<StreamElement> elements = new ArrayList<>();

        @Override
        public void emit(StreamRecord<?> record) {
            elements.add(record);
        }

        @Override
        public void broadcast(StreamElement element) {
            elements.add(element);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
