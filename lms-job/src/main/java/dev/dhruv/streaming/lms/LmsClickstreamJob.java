package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.JobExecutor;
import dev.dhruv.streaming.api.JobExecutors;
import dev.dhruv.streaming.api.graph.DataStream;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.connectors.console.ConsoleSink;
import dev.dhruv.streaming.connectors.kafka.KafkaSource;

import java.time.Duration;

/**
 * The LMS catalog clickstream job.
 *
 * <p>Phase 1 runs the front of it: read clicks from Kafka, drop bot traffic, print what
 * survives. Session aggregation arrives in Phase 3, the conversion join in Phase 5, and the
 * Iceberg sink in Phase 6, at which point this same method describes the whole pipeline.
 *
 * <p>What this class deliberately cannot do is reach into the engine. It compiles against the
 * API and the connectors and nothing else, so every capability it uses has to have been
 * expressed as a user-facing abstraction first. When that becomes awkward, the API is wrong.
 */
public final class LmsClickstreamJob {

    private static final String CLICKS_TOPIC = "lms.catalog.clicks";

    /**
     * How far out of order click events are expected to arrive.
     *
     * <p>Five seconds of patience, paid for in five seconds of latency on every window. Mobile
     * clients buffer events while backgrounded and flush them late, so zero would drop real
     * traffic; a minute would make every answer a minute stale.
     */
    private static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    /**
     * How long a partition may be silent before it stops holding back the event-time clock.
     *
     * <p>Unused until Phase 3, and the single most instructive setting in the job. With four
     * partitions and uneven traffic, one quiet partition without this would pin the watermark
     * for the entire job and stop every window firing, while every process stayed up and every
     * log stayed clean.
     */
    private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(30);

    private LmsClickstreamJob() {
    }

    /**
     * Builds the job graph and runs it until the process is stopped.
     *
     * @param args unused
     * @throws InterruptedException if the main thread is interrupted while the job runs
     */
    public static void main(String[] args) throws InterruptedException {
        JobGraph graph = buildGraph();

        try (JobExecutor execution = JobExecutors.local(graph)) {
            Runtime.getRuntime().addShutdownHook(new Thread(execution::close, "shutdown"));
            execution.start();
            execution.awaitTermination();
        }
    }

    /**
     * Describes the job.
     *
     * <p>Separate from {@link #main} so that tests can assemble and inspect the graph without
     * starting anything. Nothing runs while a graph is being described.
     *
     * @return the validated job graph
     */
    static JobGraph buildGraph() {
        JobGraph.Builder job = JobGraph.named("lms-clickstream");

        DataStream<ClickEvent> clicks =
                job.source("clicks", KafkaSource.of(CLICKS_TOPIC, ClickEvent.class))
                        .withEventTime(ClickEvent::eventTimeMillis, OUT_OF_ORDERNESS)
                        .withIdleness(IDLE_TIMEOUT)
                        .parallelism(4);

        clicks.filter("drop-bots", new BotFilter())
                .parallelism(4)
                .sink("console", new ConsoleSink<ClickEvent>("click | "))
                .parallelism(2);

        return job.build();
    }
}
