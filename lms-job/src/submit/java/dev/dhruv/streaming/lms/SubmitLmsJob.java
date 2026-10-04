package dev.dhruv.streaming.lms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dhruv.streaming.master.JobClient;
import dev.dhruv.streaming.metadata.JobState;
import dev.dhruv.streaming.rpc.SubmitAck;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

/**
 * Submits the LMS clickstream job to a running cluster.
 *
 * <p>The same {@code buildGraph()} the single-process job uses. That is the point worth noticing:
 * nothing about the job changes between running it in one JVM and running it across three
 * machines. Only who executes it does.
 */
public final class SubmitLmsJob {

    private SubmitLmsJob() {
    }

    /**
     * Submits and prints what the master decided.
     *
     * @param args unused
     * @throws Exception if the cluster is not ready or the job cannot be scheduled
     */
    public static void main(String[] args) throws Exception {
        String masterHost = env("MASTER_HOST", "localhost");
        int masterPort = Integer.parseInt(env("MASTER_PORT", "7000"));
        int minimumWorkers = Integer.parseInt(env("MIN_WORKERS", "3"));

        if (Boolean.parseBoolean(env("REUSE_RUNNING_JOB", "false"))) {
            String statusUrl = env("MASTER_STATUS_URL", "http://" + masterHost + ":8080");
            HttpRequest request = HttpRequest.newBuilder(URI.create(statusUrl.replaceAll("/$", "") + "/jobs"))
                    .timeout(Duration.ofSeconds(15)).GET().build();
            HttpResponse<String> response = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
                    .build().send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("cannot inspect existing jobs before bootstrap: HTTP "
                        + response.statusCode() + " from " + request.uri());
            }
            Optional<String> reusable = reusableJob(response.body());
            if (reusable.isPresent()) {
                System.out.println("Reusing existing lms-clickstream job " + reusable.orElseThrow()
                        + "; the master owns recovery of this graph.");
                return;
            }
        }

        try (JobClient client = new JobClient(masterHost, masterPort)) {
            SubmitAck ack = client.submit(LmsClickstreamJob.buildGraph(
                    LmsClickstreamJob.SessionAggregationMode.fromSystemProperty()), minimumWorkers, 60);

            System.out.println();
            System.out.println("submitted: " + ack.getTaskCount() + " tasks across "
                    + ack.getWorkerIdsCount() + " workers");
            ack.getAssignmentsList().forEach(line -> System.out.println("  " + line));
        }
    }

    /** Fail closed if the status response cannot prove there is at most one live LMS graph. */
    static Optional<String> reusableJob(String response) throws Exception {
        JsonNode jobs = new ObjectMapper().readTree(response);
        if (jobs == null || !jobs.isArray()) {
            throw new IllegalStateException("master /jobs must return a JSON array");
        }
        String reusable = null;
        for (JsonNode job : jobs) {
            if (!"lms-clickstream".equals(job.path("name").asText())) {
                continue;
            }
            JobState state;
            try {
                state = JobState.valueOf(job.path("state").asText());
            } catch (IllegalArgumentException invalidState) {
                throw new IllegalStateException("master returned an unknown LMS job state", invalidState);
            }
            if (state.isTerminal()) {
                continue;
            }
            String jobId = job.path("jobId").asText();
            if (jobId.isBlank()) {
                throw new IllegalStateException("master returned a live LMS graph without a jobId");
            }
            if (reusable != null) {
                throw new IllegalStateException("multiple live lms-clickstream graphs exist; cancel the extra "
                        + "jobs through the status API before restarting the Compose bootstrap");
            }
            reusable = jobId;
        }
        return Optional.ofNullable(reusable);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
