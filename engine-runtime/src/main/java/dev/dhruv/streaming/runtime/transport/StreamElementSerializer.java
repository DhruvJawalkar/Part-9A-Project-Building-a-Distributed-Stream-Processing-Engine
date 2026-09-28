package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.runtime.UserCodeObjectInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns stream elements into bytes and back.
 *
 * <h2>Why a tag byte, and why control elements are cheap</h2>
 *
 * <p>Each element is written as a one-byte tag followed by its payload. The tag is what makes
 * the sealed hierarchy survive the wire: a receiver reads one byte and knows whether the next
 * eight are a watermark's timestamp or the start of a user object.
 *
 * <p>The asymmetry between the three cases is worth noticing. A watermark costs ten bytes and a
 * barrier seventeen, because both are just longs; a data record costs whatever Java
 * serialization makes of the user's object, which is typically hundreds of bytes and a great
 * deal more work. Control elements travelling in band with data is therefore close to free,
 * which is part of why the design in {@link StreamElement} is affordable at all.
 *
 * <h2>Java serialization, deliberately</h2>
 *
 * <p>User records go through {@link ObjectOutputStream}. That is slow, produces large output,
 * and would be the first thing to replace in an engine that cared about throughput -- real ones
 * generate or configure a serializer per type precisely to avoid it.
 *
 * <p>It is the right choice here for two reasons. It works for any {@link java.io.Serializable}
 * record a job author writes, with no registration step and no schema, so the job code in
 * {@code lms-job} stays about the LMS rather than about serialization. And it is the same
 * mechanism {@code TaskInstances} already uses to copy operators, so a reader meets one idea
 * rather than two.
 */
public final class StreamElementSerializer {

    private static final byte TAG_RECORD = 1;
    private static final byte TAG_WATERMARK = 2;
    private static final byte TAG_BARRIER = 3;

    private StreamElementSerializer() {
    }

    /**
     * Serializes a batch of elements into one payload.
     *
     * <p>A batch rather than an element at a time, because every network write carries fixed
     * overhead -- a gRPC frame, a system call, a round of flow-control accounting -- and paying
     * it per record is what makes a naive transport slow.
     *
     * @param elements the elements to write, in order
     * @return the serialized payload
     * @throws IOException if an element cannot be serialized, which for a data record means the
     *                     user's type is not serializable
     */
    public static byte[] serialize(List<StreamElement> elements) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            for (StreamElement element : elements) {
                switch (element) {
                    case StreamRecord<?> record -> {
                        out.writeByte(TAG_RECORD);
                        out.writeLong(record.timestamp());
                        byte[] value = serializeValue(record.value());
                        out.writeInt(value.length);
                        out.write(value);
                    }
                    case Watermark watermark -> {
                        out.writeByte(TAG_WATERMARK);
                        out.writeLong(watermark.timestamp());
                        out.writeByte(watermark.status().ordinal());
                    }
                    case CheckpointBarrier barrier -> {
                        out.writeByte(TAG_BARRIER);
                        out.writeLong(barrier.checkpointId());
                        out.writeLong(barrier.triggerTimestamp());
                    }
                }
            }
        }
        return bytes.toByteArray();
    }

    /**
     * Reads a payload back into elements.
     *
     * @param payload bytes produced by {@link #serialize}
     * @return the elements, in the order they were written
     * @throws IOException if the payload is malformed or a user class is missing from this
     *                     worker's classpath
     */
    public static List<StreamElement> deserialize(byte[] payload) throws IOException {
        List<StreamElement> elements = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            while (in.available() > 0) {
                byte tag = in.readByte();
                switch (tag) {
                    case TAG_RECORD -> {
                        long timestamp = in.readLong();
                        byte[] value = new byte[in.readInt()];
                        in.readFully(value);
                        elements.add(new StreamRecord<>(deserializeValue(value), timestamp));
                    }
                    case TAG_WATERMARK -> {
                        long timestamp = in.readLong();
                        int status = in.readUnsignedByte();
                        Watermark.Status[] statuses = Watermark.Status.values();
                        if (status >= statuses.length) {
                            throw new IOException("unknown watermark status " + status);
                        }
                        elements.add(new Watermark(timestamp, statuses[status]));
                    }
                    case TAG_BARRIER ->
                            elements.add(new CheckpointBarrier(in.readLong(), in.readLong()));
                    default -> throw new IOException(
                            "unknown stream element tag " + tag + "; the sender and receiver"
                                    + " disagree about the wire format");
                }
            }
        }
        return elements;
    }

    private static byte[] serializeValue(Object value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static Object deserializeValue(byte[] value) throws IOException {
        // Records off the wire are user classes too, so they resolve the same way a deployed
        // operator does -- through the user-code loader rather than the engine's.
        try (UserCodeObjectInputStream in =
                     new UserCodeObjectInputStream(new ByteArrayInputStream(value))) {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(
                    "a record arrived whose class this worker cannot load. The job's classes must"
                            + " be reachable from every worker that runs it.", e);
        }
    }
}
