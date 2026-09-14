package dev.dhruv.streaming.api.state;

import java.net.URI;

/**
 * A pointer to one task's written snapshot, plus how big it turned out to be.
 *
 * <p>What crosses the wire to the master on a checkpoint acknowledgement is this handle, not
 * the state itself. The master's checkpoint metadata is therefore small and uniform however
 * large the job's state is: a list of URIs. Recovery hands each task back its own handle and
 * lets it fetch its own bytes.
 *
 * @param uri       where the snapshot was written, resolvable by the task that will restore it
 * @param sizeBytes size of the written snapshot, reported for observability. State size is one
 *                  of the two numbers that tell you what a checkpoint actually costs, the
 *                  other being alignment time.
 */
public record StateHandle(URI uri, long sizeBytes) {
}
