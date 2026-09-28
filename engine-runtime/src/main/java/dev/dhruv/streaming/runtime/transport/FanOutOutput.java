package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;

import java.util.List;

/**
 * Sends each element to every independently routed downstream edge.
 *
 * <p>An edge has its own {@link ResultPartitionWriter}: it can therefore use a different
 * exchange strategy and key selector from its siblings. This object is deliberately only the
 * small composition layer; routing within one edge remains the writer's responsibility.
 */
public final class FanOutOutput implements Output {

    private final List<Output> outputs;

    public FanOutOutput(List<? extends Output> outputs) {
        this.outputs = List.copyOf(outputs);
    }

    @Override
    public void emit(StreamRecord<?> record) throws InterruptedException {
        for (Output output : outputs) {
            output.emit(record);
        }
    }

    @Override
    public void broadcast(StreamElement element) throws InterruptedException {
        for (Output output : outputs) {
            output.broadcast(element);
        }
    }

    @Override
    public void flush() throws InterruptedException {
        for (Output output : outputs) {
            output.flush();
        }
    }

    @Override
    public void close() {
        for (Output output : outputs) {
            output.close();
        }
    }
}
