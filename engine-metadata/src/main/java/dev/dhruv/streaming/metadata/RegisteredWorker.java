package dev.dhruv.streaming.metadata;

/**
 * A worker the master currently believes is alive.
 *
 * <p>"Currently believes" is doing real work in that sentence. This record is the master's
 * model of something in another process that it can only observe through heartbeats, and the
 * model is always slightly out of date. Phase 2's failure handling is entirely about what to do
 * when the belief turns out to be wrong.
 *
 * @param workerId unique id chosen by the worker at startup
 * @param host     where to reach it
 * @param rpcPort  port serving WorkerService, the control plane
 * @param dataPort port serving DataTransportService, kept separate so that saturated record
 *                 traffic cannot delay a heartbeat and get a healthy worker declared dead
 * @param slots    how many vertical pipeline slices this worker will accept
 */
public record RegisteredWorker(
        String workerId,
        String host,
        int rpcPort,
        int dataPort,
        int slots
) {

    /**
     * Returns the worker's control-plane address, for logging.
     *
     * @return {@code host:rpcPort}
     */
    public String address() {
        return host + ":" + rpcPort;
    }
}
