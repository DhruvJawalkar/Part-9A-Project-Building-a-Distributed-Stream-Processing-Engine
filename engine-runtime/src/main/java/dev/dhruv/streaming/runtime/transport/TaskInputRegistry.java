package dev.dhruv.streaming.runtime.transport;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What a worker needs to deliver an arriving buffer to the right task, on the right channel.
 *
 * <p>A buffer off the wire says who sent it and which operator and subtask it is for. Two
 * questions follow, and this registry answers both.
 *
 * <ul>
 *   <li><b>Which task?</b> The target operator and subtask name one running task on this worker,
 *       and therefore one input gate.</li>
 *   <li><b>Which channel of it?</b> The sender's identity maps to one of that gate's channels.
 *       This is the part that cannot be skipped: a gate with four inputs has to know which of
 *       the four an element arrived on, because a watermark is a claim about one channel and a
 *       barrier is an instruction to stop consuming one channel.</li>
 * </ul>
 *
 * <p>Both mappings come from the {@code TaskDeployment} that started the task, so they are known
 * before any data arrives. Nothing is discovered from traffic.
 */
public final class TaskInputRegistry {

    private final Map<String, InputGate> gates = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Integer>> channelsByTask = new ConcurrentHashMap<>();

    /**
     * Returns the wire identity of a subtask: {@code operatorId#subtaskIndex}.
     *
     * @param operatorId   the operator
     * @param subtaskIndex which subtask of it
     * @return the identity used in {@code DataBuffer.sender_task_id}
     */
    public static String taskKey(String operatorId, int subtaskIndex) {
        return operatorId + "#" + subtaskIndex;
    }

    /**
     * Registers a task's gate and the senders that feed it.
     *
     * @param operatorId       the operator this task runs
     * @param subtaskIndex     which subtask
     * @param gate             where its arrivals go
     * @param senderToChannel  each upstream subtask's wire identity, mapped to the channel index
     *                         it occupies at this gate
     */
    public void register(String operatorId,
                         int subtaskIndex,
                         InputGate gate,
                         Map<String, Integer> senderToChannel) {
        String key = taskKey(operatorId, subtaskIndex);
        gates.put(key, gate);
        channelsByTask.put(key, Map.copyOf(senderToChannel));
    }

    /**
     * Forgets a task that has stopped.
     *
     * @param operatorId   the operator
     * @param subtaskIndex which subtask
     */
    public void unregister(String operatorId, int subtaskIndex) {
        String key = taskKey(operatorId, subtaskIndex);
        gates.remove(key);
        channelsByTask.remove(key);
    }

    /**
     * Finds where an arriving buffer should be delivered.
     *
     * @param targetOperatorId which operator the buffer is for
     * @param targetSubtask    which subtask of it
     * @param senderTaskId     who sent it
     * @return the gate and channel, or empty if this worker is not running that task -- which
     *         happens legitimately when a buffer arrives just after a task was cancelled
     */
    public Optional<Destination> resolve(String targetOperatorId,
                                         int targetSubtask,
                                         String senderTaskId) {
        String key = taskKey(targetOperatorId, targetSubtask);
        InputGate gate = gates.get(key);
        if (gate == null) {
            return Optional.empty();
        }
        Integer channel = channelsByTask.getOrDefault(key, Map.of()).get(senderTaskId);
        if (channel == null) {
            return Optional.empty();
        }
        return Optional.of(new Destination(gate, channel));
    }

    /**
     * Where a buffer goes.
     *
     * @param gate         the receiving task's gate
     * @param channelIndex which of its channels the sender occupies
     */
    public record Destination(InputGate gate, int channelIndex) {
    }
}
