package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.Either;
import dev.dhruv.streaming.api.IntervalJoinOperator;
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
 * <p>The graph reads clicks and borrows from Kafka. Clean clicks fan out: one branch builds
 * member sessions, while RESULT_CLICK records on the other are tagged and interval-joined
 * with tagged borrows by member and catalog item. Both outputs remain console sinks until
 * Phase 6 replaces them with checkpoint-transactional Iceberg sinks.
 *
 * <p>What this class deliberately cannot do is reach into the engine. It compiles against the
 * API and the connectors and nothing else, so every capability it uses has to have been
 * expressed as a user-facing abstraction first. When that becomes awkward, the API is wrong.
 */
public final class LmsClickstreamJob {

    private static final String CLICKS_TOPIC = "lms.catalog.clicks";
    private static final String BORROWS_TOPIC = "lms.catalog.borrows";

    /** A result click converts only when the matching borrow follows within 30 minutes. */
    private static final Duration CONVERSION_INTERVAL = Duration.ofMinutes(30);

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
    public static JobGraph buildGraph() {
        JobGraph.Builder job = JobGraph.named("lms-clickstream");

        DataStream<ClickEvent> clicks =
                job.source("clicks", KafkaSource.of(CLICKS_TOPIC, ClickEvent.class))
                        .withEventTime(ClickEvent::eventTimeMillis, OUT_OF_ORDERNESS)
                        .withIdleness(IDLE_TIMEOUT)
                        .parallelism(4);

        DataStream<BorrowEvent> borrows =
                job.source("borrows", KafkaSource.of(BORROWS_TOPIC, BorrowEvent.class))
                        .withEventTime(BorrowEvent::eventTimeMillis, OUT_OF_ORDERNESS)
                        .withIdleness(IDLE_TIMEOUT)
                        .parallelism(4);

        DataStream<ClickEvent> cleanClicks = clicks.filter("drop-bots", new BotFilter())
                .parallelism(4);

        cleanClicks
                .keyBy("by-member", ClickEvent::memberId)
                .process("sessions", new SessionAggregator())
                .parallelism(4)
                .sink("console", new ConsoleSink<SessionRow>("session | "))
                .parallelism(2);

        DataStream<Either<ClickEvent, BorrowEvent>> clickJoinInput = cleanClicks
                .filter("result-clicks", ClickEvent::isResultClick)
                .<Either<ClickEvent, BorrowEvent>>process("tag-result-clicks", (record, out) -> out.collect(
                        Either.<ClickEvent, BorrowEvent>left(record.value()), record.timestamp()))
                .parallelism(4);

        DataStream<Either<ClickEvent, BorrowEvent>> borrowJoinInput = borrows
                .<Either<ClickEvent, BorrowEvent>>process("tag-borrows", (record, out) -> out.collect(
                        Either.<ClickEvent, BorrowEvent>right(record.value()), record.timestamp()))
                .parallelism(4);

        clickJoinInput.union("conversion-inputs", borrowJoinInput)
                .parallelism(4)
                .keyBy("by-member-and-item", value -> value.fold(
                        click -> new ConversionKey(click.memberId(), click.catalogItemId()),
                        borrow -> new ConversionKey(borrow.memberId(), borrow.catalogItemId())))
                .process("conversions", new IntervalJoinOperator<ConversionKey, ClickEvent,
                        BorrowEvent, ConversionRow>(Duration.ZERO, CONVERSION_INTERVAL,
                        ConversionRow::from))
                .parallelism(4)
                .sink("conversion-console", new ConsoleSink<ConversionRow>("conversion | "))
                .parallelism(2);

        return job.build();
    }
}
