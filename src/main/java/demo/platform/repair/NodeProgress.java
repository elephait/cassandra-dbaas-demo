package demo.platform.repair;

import java.time.Instant;

/**
 * Persisted progress of one node in a repair plan. {@code submittedAt} starts the node's own observation
 * clock; {@code delayed} is set once the node runs longer than the configured soft threshold.
 */
public record NodeProgress(int ordinal, String node, String state, Integer commandId, Long processStartedAt,
                           int attempts, Instant submittedAt, boolean delayed, Instant nextActionAt,
                           String detail, Instant updatedAt) {
}
