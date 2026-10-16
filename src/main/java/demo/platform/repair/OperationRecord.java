package demo.platform.repair;

import java.time.Instant;
import java.util.UUID;

/**
 * Durable repair request. Component names match {@code OperationEntity} attributes, so Spring Data reads
 * this record directly as a DTO projection.
 */
public record OperationRecord(
    UUID id,
    String clusterId,
    String keyspace,
    String owner,
    RepairState state,
    String detail,
    boolean delayed,
    Instant updatedAt) {

    public record RepairDecision(OperationRecord operation, boolean created) {
    }

    /** API view: the internal owner token is never exposed. */
    public record RepairOperation(UUID id, String clusterId, String keyspace, RepairState state, String detail,
                                  boolean delayed, Instant updatedAt) {
        static RepairOperation of(OperationRecord operation) {
            return new RepairOperation(operation.id(), operation.clusterId(), operation.keyspace(),
                operation.state(), operation.detail(), operation.delayed(), operation.updatedAt());
        }
    }
}
