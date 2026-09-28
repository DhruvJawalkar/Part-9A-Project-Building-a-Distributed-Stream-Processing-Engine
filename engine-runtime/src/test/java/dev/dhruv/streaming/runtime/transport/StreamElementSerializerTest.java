package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the wire format.
 */
class StreamElementSerializerTest {

    /** Stands in for a job author's own event type. */
    private record ClickLike(String memberId, String itemId) implements Serializable {
    }

    @Test
    @DisplayName("round-trips all three kinds of element in order")
    void roundTripsEverything() throws IOException {
        List<StreamElement> original = List.of(
                new StreamRecord<>(new ClickLike("m-1001", "cat-7741"), 1757836800000L),
                new Watermark(1757836795000L),
                Watermark.idle(),
                new StreamRecord<>(new ClickLike("m-1002", "cat-3390"), 1757836820000L),
                new CheckpointBarrier(42L, 1757836830000L));

        List<StreamElement> restored =
                StreamElementSerializer.deserialize(StreamElementSerializer.serialize(original));

        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("preserves event time exactly")
    void preservesTimestamps() throws IOException {
        // Event time is the field every windowing decision is made against, so a lossy
        // round-trip here would be silently wrong rather than loudly broken.
        StreamRecord<String> record = new StreamRecord<>("value", Long.MAX_VALUE - 1);

        List<StreamElement> restored = StreamElementSerializer.deserialize(
                StreamElementSerializer.serialize(List.of(record)));

        assertThat(((StreamRecord<?>) restored.getFirst()).timestamp())
                .isEqualTo(Long.MAX_VALUE - 1);
    }

    @Test
    @DisplayName("control elements are far cheaper than data records")
    void controlElementsAreCheap() throws IOException {
        // Ten bytes for a watermark against hundreds for a record. This is what makes carrying
        // control elements in band with the data affordable in the first place.
        int watermarkBytes = StreamElementSerializer
                .serialize(List.<StreamElement>of(new Watermark(1L))).length;
        int recordBytes = StreamElementSerializer
                .serialize(List.<StreamElement>of(
                        new StreamRecord<>(new ClickLike("m-1001", "cat-7741"), 1L))).length;

        assertThat(watermarkBytes).isEqualTo(10);
        assertThat(recordBytes).isGreaterThan(watermarkBytes * 5);
    }

    @Test
    @DisplayName("an empty batch round-trips to nothing")
    void emptyBatch() throws IOException {
        assertThat(StreamElementSerializer.deserialize(
                StreamElementSerializer.serialize(List.of()))).isEmpty();
    }

    @Test
    @DisplayName("rejects a payload it cannot parse")
    void rejectsGarbage() {
        assertThatThrownBy(() -> StreamElementSerializer.deserialize(new byte[] {99, 0, 0}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown stream element tag");
    }

    @Test
    @DisplayName("refuses a record whose value is not serializable")
    void refusesUnserializableValues() {
        // The failure a job author meets the first time their event type crosses a process
        // boundary. Worth a clear error here rather than deep inside gRPC.
        StreamRecord<Object> record = new StreamRecord<>(new Object(), 1L);

        assertThatThrownBy(() -> StreamElementSerializer.serialize(List.of(record)))
                .isInstanceOf(java.io.NotSerializableException.class);
    }
}
