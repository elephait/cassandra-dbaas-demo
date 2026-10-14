package demo.platform.repair;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OperationRecord(
    UUID id,
    String clusterId,
    String keyspace,
    String owner,
    RepairState state,
    String detail,
    long repairPolicyVersion,
    Instant updated) {
    public OperationRecord(UUID id, String clusterId, String keyspace, String owner,
                           RepairState state, String detail, Instant updated) {
        this(id, clusterId, keyspace, owner, state, detail, 1, updated);
    }

    public record RepairPolicy(String clusterId, List<String> nodes, String command,
                               int leaseMinutes, long version) {
        public RepairPolicy {
            if (clusterId == null || nodes == null)
                throw new IllegalArgumentException("Cluster and repair nodes are required");
            nodes = List.copyOf(nodes);
            if (nodes.isEmpty() || nodes.stream().anyMatch(node -> node == null || node.isBlank()))
                throw new IllegalArgumentException("At least one nonblank repair node is required");
            if (command == null || command.isBlank()) throw new IllegalArgumentException("Repair command is required");
            if (!command.contains("{keyspace}"))
                throw new IllegalArgumentException("Repair command must target {keyspace}");
            if (nodes.size() > 1 && !command.contains("{node}"))
                throw new IllegalArgumentException("Multi-node repair command must contain {node}");
            if (nodes.stream().anyMatch(node -> !node.matches("[A-Za-z0-9_.-]{1,255}")) || nodes.stream().distinct().count() != nodes.size())
                throw new IllegalArgumentException("Repair targets must be unique safe node names");
            if (leaseMinutes < 1) throw new IllegalArgumentException("Repair lease must be positive");
        }

        public RepairPolicy withVersion(long newVersion) {
            return new RepairPolicy(clusterId, nodes, command, leaseMinutes, newVersion);
        }
    }

    public record RepairDecision(OperationRecord operation, boolean created) {
    }

    public record RepairOperation(UUID id, String clusterId, String keyspace, RepairState state,
                                  String detail, long repairPolicyVersion, Instant updated) {
    }
}
