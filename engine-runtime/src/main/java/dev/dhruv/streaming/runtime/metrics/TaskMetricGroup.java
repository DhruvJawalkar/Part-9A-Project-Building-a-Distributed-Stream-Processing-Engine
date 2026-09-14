package dev.dhruv.streaming.runtime.metrics;

import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.metrics.Histogram;
import dev.dhruv.streaming.api.metrics.MetricGroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * The metrics of one subtask.
 *
 * <p>Scoping is the whole point. Every metric registered here is qualified by the operator id
 * and subtask index, so a job running four sessions subtasks reports four separate
 * {@code records-in} series rather than one sum.
 *
 * <p>That is not a reporting nicety. The hot-key failure the project exists partly to
 * demonstrate is invisible in aggregate -- total throughput looks fine, no process has died,
 * no log line is red -- and shows up only as one subtask's series diverging from its siblings.
 * A metric system that summed by default would hide the single most instructive failure in the
 * engine.
 */
public final class TaskMetricGroup implements MetricGroup {

    private final String operatorId;
    private final int subtaskIndex;

    private final Map<String, SimpleCounter> counters = new ConcurrentHashMap<>();
    private final Map<String, SimpleHistogram> histograms = new ConcurrentHashMap<>();
    private final Map<String, LongSupplier> gauges = new ConcurrentHashMap<>();

    /**
     * Creates a metric group for one subtask.
     *
     * @param operatorId   the operator these metrics belong to
     * @param subtaskIndex which subtask of it
     */
    public TaskMetricGroup(String operatorId, int subtaskIndex) {
        this.operatorId = operatorId;
        this.subtaskIndex = subtaskIndex;
    }

    @Override
    public Counter counter(String name) {
        return counters.computeIfAbsent(name, ignored -> new SimpleCounter());
    }

    @Override
    public Histogram histogram(String name) {
        return histograms.computeIfAbsent(name, ignored -> new SimpleHistogram());
    }

    @Override
    public void gauge(String name, LongSupplier value) {
        gauges.put(name, value);
    }

    /**
     * Returns the operator these metrics belong to.
     *
     * @return the operator id
     */
    public String operatorId() {
        return operatorId;
    }

    /**
     * Returns which subtask of that operator these metrics belong to.
     *
     * @return the subtask index
     */
    public int subtaskIndex() {
        return subtaskIndex;
    }

    /**
     * Returns a snapshot of every metric, keyed by name.
     *
     * <p>Gauges are sampled at the moment this is called. Phase 7 turns this into a Prometheus
     * scrape; until then it is what tests assert against and what the executor logs on
     * shutdown.
     *
     * @return metric name to current value
     */
    public Map<String, Long> snapshot() {
        Map<String, Long> values = new ConcurrentHashMap<>();
        counters.forEach((name, counter) -> values.put(name, counter.count()));
        histograms.forEach((name, histogram) -> {
            values.put(name + ".count", histogram.count());
            values.put(name + ".mean", (long) histogram.mean());
            values.put(name + ".max", histogram.max());
        });
        gauges.forEach((name, supplier) -> values.put(name, supplier.getAsLong()));
        return values;
    }

    @Override
    public String toString() {
        return operatorId + "(" + subtaskIndex + ")";
    }
}
