package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Either;
import dev.dhruv.streaming.api.IntervalJoinOperator;
import dev.dhruv.streaming.api.JobExecutors;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.graph.DataStream;
import dev.dhruv.streaming.api.graph.JobGraph;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end proof that the two bounded source branches meet through the real DAG runtime. */
class ConversionPipelineAcceptanceTest {

    private static final List<ConversionRow> OUTPUT = new CopyOnWriteArrayList<>();

    @BeforeEach
    void clearOutput() {
        OUTPUT.clear();
    }

    @Test
    @Timeout(10)
    void joinsClicksAndBorrowsAcrossARealTwoInputUnionRegardlessOfArrivalOrder() throws Exception {
        ClickEvent alphaClick = click("alpha", "book-a", "distributed systems", 100);
        ClickEvent betaClick = click("beta", "book-b", "stream processing", 290);
        ClickEvent tooEarly = click("gamma", "book-c", "old result", 100);
        BorrowEvent alphaBorrow = borrow("alpha", "book-a", "loan-a", 130);
        BorrowEvent betaBorrow = borrow("beta", "book-b", "loan-b", 300);
        BorrowEvent tooLate = borrow("gamma", "book-c", "loan-c", 500);

        JobGraph.Builder job = JobGraph.named("conversion-acceptance");
        DataStream<ClickEvent> clicks = job.source("clicks",
                        new ListSource<>(List.of(alphaClick, betaClick, tooEarly)))
                .withEventTime(ClickEvent::eventTimeMillis, Duration.ZERO);
        DataStream<BorrowEvent> borrows = job.source("borrows",
                        new ListSource<>(List.of(alphaBorrow, betaBorrow, tooLate)))
                .withEventTime(BorrowEvent::eventTimeMillis, Duration.ZERO);

        DataStream<Either<ClickEvent, BorrowEvent>> left = clicks
                .<Either<ClickEvent, BorrowEvent>>process("tag-clicks", (record, out) ->
                        out.collect(Either.left(record.value()), record.timestamp()));
        DataStream<Either<ClickEvent, BorrowEvent>> right = borrows
                .<Either<ClickEvent, BorrowEvent>>process("tag-borrows", (record, out) ->
                        out.collect(Either.right(record.value()), record.timestamp()));

        left.union("conversion-inputs", right)
                .keyBy("by-member-and-item", value -> value.fold(
                        click -> new ConversionKey(click.memberId(), click.catalogItemId()),
                        borrow -> new ConversionKey(borrow.memberId(), borrow.catalogItemId())))
                .process("conversions", new IntervalJoinOperator<ConversionKey, ClickEvent,
                        BorrowEvent, ConversionRow>(Duration.ZERO, Duration.ofMillis(30),
                        ConversionRow::from))
                .sink("capture", new CapturingSink());

        try (var execution = JobExecutors.local(job.build())) {
            execution.start();
            execution.awaitTermination();
        }

        assertThat(OUTPUT).containsExactlyInAnyOrder(
                ConversionRow.from(alphaClick, alphaBorrow),
                ConversionRow.from(betaClick, betaBorrow));
    }

    private static ClickEvent click(String member, String item, String term, long timestamp) {
        return new ClickEvent(member, item, term, ClickEvent.RESULT_CLICK, timestamp);
    }

    private static BorrowEvent borrow(String member, String item, String loan, long timestamp) {
        return new BorrowEvent(member, item, loan, timestamp);
    }

    private static final class ListSource<T> implements Source<T> {
        private static final long serialVersionUID = 1L;
        private final List<T> records;
        private transient int next;

        private ListSource(List<T> records) {
            this.records = List.copyOf(records);
        }

        @Override
        public void open(SourceContext context) {
            next = 0;
        }

        @Override
        public boolean poll(Collector<T> out) {
            if (next == records.size()) {
                return false;
            }
            out.collect(records.get(next++));
            return true;
        }

        @Override
        public void close() {
        }
    }

    private static final class CapturingSink implements Operator<ConversionRow, Void> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<ConversionRow> record, Collector<Void> out) {
            OUTPUT.add(record.value());
        }
    }
}
