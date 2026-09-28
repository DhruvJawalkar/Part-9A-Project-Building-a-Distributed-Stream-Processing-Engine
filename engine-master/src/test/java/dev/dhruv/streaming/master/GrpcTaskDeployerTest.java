package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraphCompiler;
import dev.dhruv.streaming.metadata.RegisteredWorker;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcTaskDeployerTest {

    @Test
    void responseLostAfterTaskStartsStillRollsBackThatUncertainAttempt() {
        RegisteredWorker worker = new RegisteredWorker(
                "worker-1", "127.0.0.1", 9091, 10091, 1);
        JobGraph graph = oneTaskJob();
        ExecutionGraph plan = ExecutionGraphCompiler.compile(graph, List.of(worker));
        Set<String> running = new HashSet<>();
        Set<String> cancelled = new HashSet<>();

        GrpcTaskDeployer deployer = new GrpcTaskDeployer((target, deployment) -> {
            String task = deployment.getOperators(0).getOperatorId()
                    + ':' + deployment.getSubtaskIndex();
            running.add(task); // The worker acted, but its response is lost.
            throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        }, (target, taskId) -> {
            String task = taskId.getOperatorId() + ':' + taskId.getSubtaskIndex();
            cancelled.add(task);
            running.remove(task);
        });

        assertThatThrownBy(() -> deployer.deploy(graph, plan, List.of(worker)))
                .isInstanceOf(io.grpc.StatusRuntimeException.class);

        assertThat(running).isEmpty();
        assertThat(cancelled).containsExactly("source:0");
    }

    private static JobGraph oneTaskJob() {
        JobGraph.Builder job = JobGraph.named("uncertain-deploy");
        job.source("source", noopSource())
                .sink("sink", noopSink());
        return job.build();
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

    private static Operator<String, Void> noopSink() {
        return (record, out) -> {
        };
    }
}
