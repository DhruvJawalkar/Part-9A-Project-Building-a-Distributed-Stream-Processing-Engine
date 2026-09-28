package dev.dhruv.streaming.connectors.file;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A bounded JSON Lines source used by deterministic demos and tests.
 *
 * <p>Every source subtask walks the same file but accepts only the lines whose stable content
 * hash belongs to it. That is intentionally less efficient than pre-splitting the fixture, but
 * it makes the partitioning rule visible and means a fixture has one canonical ordering. The
 * source emits one accepted record per poll: replay is driven by the task loop, never by wall
 * clock sleeps, so the input itself remains reproducible.
 *
 * <p>Unlike {@code KafkaSource}, malformed input fails the source. A replay fixture is part of
 * a test or a demonstration; silently dropping a malformed line would make the claimed result
 * impossible to reproduce or audit.
 *
 * @param <T> record type decoded from each JSON line
 */
public final class FileReplaySource<T> implements Source<T> {

    private static final long serialVersionUID = 1L;

    private final String fileName;
    private final Class<T> valueType;

    private transient BufferedReader reader;
    private transient ObjectMapper mapper;
    private transient int subtaskIndex;
    private transient int parallelism;
    private transient long lineNumber;

    private FileReplaySource(String fileName, Class<T> valueType) {
        this.fileName = Objects.requireNonNull(fileName, "fileName");
        this.valueType = Objects.requireNonNull(valueType, "valueType");
    }

    /**
     * Creates a bounded replay source from a JSON Lines fixture.
     *
     * <p>The path must be visible from every worker which runs a source subtask. Keeping it as
     * configuration rather than an open file handle makes the source safe to serialize for
     * remote deployment.
     *
     * @param file JSON Lines fixture to replay
     * @param valueType type represented by each line
     * @param <T> record type
     * @return a source which reaches EOF after replaying the fixture
     */
    public static <T> FileReplaySource<T> of(Path file, Class<T> valueType) {
        return new FileReplaySource<>(Objects.requireNonNull(file, "file").toString(), valueType);
    }

    @Override
    public void open(SourceContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        if (context.parallelism() < 1) {
            throw new IllegalArgumentException("source parallelism must be at least one");
        }
        if (context.subtaskIndex() < 0 || context.subtaskIndex() >= context.parallelism()) {
            throw new IllegalArgumentException("source subtask index must be within its parallelism");
        }

        reader = Files.newBufferedReader(Path.of(fileName));
        mapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        subtaskIndex = context.subtaskIndex();
        parallelism = context.parallelism();
        lineNumber = 0;
    }

    @Override
    public boolean poll(Collector<T> out) throws IOException {
        requireOpen();
        Objects.requireNonNull(out, "out");

        String line;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            if (line.isBlank() || subtaskFor(line) != subtaskIndex) {
                continue;
            }
            try {
                out.collect(mapper.readValue(line, valueType));
                return true;
            } catch (IOException e) {
                throw new IOException("cannot decode JSON replay fixture '" + fileName
                        + "' at line " + lineNumber, e);
            }
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    private int subtaskFor(String line) {
        return Math.floorMod(line.hashCode(), parallelism);
    }

    private void requireOpen() {
        if (reader == null || mapper == null) {
            throw new IllegalStateException("FileReplaySource must be opened before polling");
        }
    }
}
