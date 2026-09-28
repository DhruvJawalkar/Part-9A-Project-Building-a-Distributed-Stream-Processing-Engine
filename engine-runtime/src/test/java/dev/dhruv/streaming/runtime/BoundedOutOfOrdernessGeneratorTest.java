package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Watermark;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedOutOfOrdernessGeneratorTest {

    @Test
    void emitsMaximumObservedEventTimeMinusAllowedDisorder() {
        AtomicLong clock = new AtomicLong(1_000L);
        BoundedOutOfOrdernessGenerator generator =
                new BoundedOutOfOrdernessGenerator(50L, 500L, clock::get);

        generator.onEvent(1_000L);
        generator.onEvent(980L);

        assertThat(generator.onPeriodicEmit()).contains(new Watermark(950L));
        assertThat(generator.isIdle()).isFalse();
    }

    @Test
    void marksASilentSourceIdleAndAnEventMakesItActiveAgain() {
        AtomicLong clock = new AtomicLong(1_000L);
        BoundedOutOfOrdernessGenerator generator =
                new BoundedOutOfOrdernessGenerator(10L, 100L, clock::get);
        generator.onEvent(1_000L);

        clock.set(1_101L);
        assertThat(generator.onPeriodicEmit()).isEmpty();
        assertThat(generator.isIdle()).isTrue();

        generator.onEvent(1_200L);
        assertThat(generator.isIdle()).isFalse();
        assertThat(generator.onPeriodicEmit()).contains(new Watermark(1_190L));
    }

    @Test
    void doesNotEmitBeforeTheFirstEvent() {
        AtomicLong clock = new AtomicLong(1_000L);
        BoundedOutOfOrdernessGenerator generator =
                new BoundedOutOfOrdernessGenerator(10L, 100L, clock::get);

        assertThat(generator.onPeriodicEmit()).isEmpty();
    }
}
