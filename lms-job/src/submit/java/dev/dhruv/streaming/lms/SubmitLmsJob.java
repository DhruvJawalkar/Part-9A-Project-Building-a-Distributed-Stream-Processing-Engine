package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.master.JobClient;
import dev.dhruv.streaming.rpc.SubmitAck;

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

        try (JobClient client = new JobClient(masterHost, masterPort)) {
            SubmitAck ack = client.submit(LmsClickstreamJob.buildGraph(), minimumWorkers, 60);

            System.out.println();
            System.out.println("submitted: " + ack.getTaskCount() + " tasks across "
                    + ack.getWorkerIdsCount() + " workers");
            ack.getAssignmentsList().forEach(line -> System.out.println("  " + line));
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
