package dev.dhruv.streaming.master.graph;

import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.graph.JobGraph;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for compiling a logical graph into a physical plan.
 */
class ExecutionGraphCompilerTest {

    private static final List<RegisteredWorker> THREE_WORKERS = List.of(
            new RegisteredWorker("worker-1", "10.0.0.1", 9091, 9191, 8),
            new RegisteredWorker("worker-2", "10.0.0.2", 9091, 9191, 8),
            new RegisteredWorker("worker-3", "10.0.0.3", 9091, 9191, 8));

    @Test
    @DisplayName("expands each operator into one vertex per subtask")
    void expandsToVertices() {
        ExecutionGraph plan = ExecutionGraphCompiler.compile(lmsShapedJob(), THREE_WORKERS);

        // clicks x4, drop-bots x4, console x2
        assertThat(plan.vertices()).hasSize(10);
        assertThat(plan.vertices()).filteredOn(v -> v.operatorId().equals("clicks"))
                .hasSize(4)
                .extracting(ExecutionVertex::subtaskIndex)
                .containsExactly(0, 1, 2, 3);
    }

    @Test
    @DisplayName("records the input exchange on each vertex")
    void recordsExchangeStrategy() {
        ExecutionGraph plan = ExecutionGraphCompiler.compile(lmsShapedJob(), THREE_WORKERS);

        assertThat(plan.vertices()).filteredOn(v -> v.operatorId().equals("drop-bots"))
                .allMatch(v -> v.inputExchange() == ExchangeStrategy.FORWARD);
        assertThat(plan.vertices()).filteredOn(v -> v.operatorId().equals("console"))
                .allMatch(v -> v.inputExchange() == ExchangeStrategy.REBALANCE);
    }

    @Test
    @DisplayName("spreads slices across every registered worker")
    void distributesAcrossWorkers() {
        // The acceptance criterion behind this: a submitted job must actually occupy all three
        // workers, not just as many workers as there are chain groups.
        ExecutionGraph plan = ExecutionGraphCompiler.compile(lmsShapedJob(), THREE_WORKERS);

        assertThat(plan.workerIds()).containsExactly("worker-1", "worker-2", "worker-3");
        assertThat(plan.taskCount()).isEqualTo(6);   // 4 chained slices + 2 sink slices
    }

    @Test
    @DisplayName("balances slices rather than piling subtask zero onto one worker")
    void roundRobinRunsAcrossChainGroups() {
        ExecutionGraph plan = ExecutionGraphCompiler.compile(lmsShapedJob(), THREE_WORKERS);

        Map<String, Long> perWorker = plan.assignments().values().stream()
                .collect(Collectors.groupingBy(TaskAssignment::workerId, Collectors.counting()));

        // Six slices over three workers: nobody holds more than one extra.
        assertThat(perWorker.values()).allMatch(count -> count >= 1 && count <= 2);
    }

    @Test
    @DisplayName("keeps a chained slice together on one worker")
    void chainedSliceIsOneTask() {
        ExecutionGraph plan = ExecutionGraphCompiler.compile(lmsShapedJob(), THREE_WORKERS);

        ChainGroup chained = plan.chainGroup("clicks").orElseThrow();
        assertThat(chained.operatorIds()).containsExactly("clicks", "drop-bots");

        // One assignment for the pair, not one each. That is what "no serialization between
        // them" means concretely: they are not separately schedulable.
        assertThat(plan.assignments()).containsKey("clicks:0");
        assertThat(plan.assignments()).doesNotContainKey("drop-bots:0");
    }

    @Test
    @DisplayName("refuses to schedule with no workers")
    void needsAtLeastOneWorker() {
        assertThatThrownBy(() -> ExecutionGraphCompiler.compile(lmsShapedJob(), List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no workers registered");
    }

    @Test
    @DisplayName("refuses a job larger than the cluster")
    void refusesWhenSlotsAreShort() {
        List<RegisteredWorker> tiny = List.of(
                new RegisteredWorker("worker-1", "10.0.0.1", 9091, 9191, 1));

        assertThatThrownBy(() -> ExecutionGraphCompiler.compile(lmsShapedJob(), tiny))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("slots");
    }

    @Test
    @DisplayName("is deterministic for the same job and the same workers")
    void compilationIsDeterministic() {
        // The master recompiles after a restart and must reach the same plan it recorded in
        // etcd, or recovery would reassign tasks that are already running.
        JobGraph job = lmsShapedJob();

        ExecutionGraph first = ExecutionGraphCompiler.compile(job, THREE_WORKERS);
        ExecutionGraph second = ExecutionGraphCompiler.compile(job, THREE_WORKERS);

        assertThat(second.assignments()).isEqualTo(first.assignments());
    }

    // -------------------------------------------------------------------------------------

    /** The Phase 1 LMS job shape: source x4 -> filter x4 -> console x2. */
    private static JobGraph lmsShapedJob() {
        JobGraph.Builder job = JobGraph.named("lms-clickstream");
        job.source("clicks", noopSource()).parallelism(4)
                .filter("drop-bots", value -> true).parallelism(4)
                .sink("console", printSink()).parallelism(2);
        return job.build();
    }

    private static Operator<String, Void> printSink() {
        return (record, out) -> {
        };
    }

    private static Source<String> noopSource() {
        return new Source<>() {
            @Override
            public void open(SourceContext context) {
            }

            @Override
            public boolean poll(Collector<String> out) {
                return false;
            }

            @Override
            public void close() {
            }
        };
    }
}
