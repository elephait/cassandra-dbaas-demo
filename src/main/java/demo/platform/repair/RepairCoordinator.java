package demo.platform.repair;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.keyspace.KeyspaceNames;
import demo.platform.repair.OperationRecord.RepairOperation;
import demo.platform.repair.persistence.RepairExecutionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/** API-facing repair use cases: admission, receipts, progress and acknowledgement. */
@Service
@RequiredArgsConstructor
public class RepairCoordinator {
    private final ClusterRegistryService clusters;
    private final RepairQueueService queue;
    private final RepairExecutionRepository execution;
    private final RepairTopology topology;

    private static void validateRequestKey(String requestKey) {
        if (requestKey == null || !requestKey.matches("[A-Za-z0-9._:-]{1,128}"))
            throw new IllegalArgumentException(
                "Idempotency-Key required: 1..128 letters, digits, '.', '_', ':', or '-'");
    }

    public RepairOperation requestRepair(String clusterId, String keyspace, boolean approved, String requestKey) {
        if (!approved) throw new IllegalArgumentException("Explicit approved=true required");
        validateRequestKey(requestKey);
        KeyspaceNames.validate(keyspace);
        // A replay returns the original receipt without touching Cassandra, even if the cluster is degraded.
        OperationRecord previous = queue.find(clusterId, requestKey, keyspace);
        if (previous != null) return RepairOperation.of(previous);
        ClusterRecord.Settings cluster = clusters.requireEnabled(clusterId);
        topology.requireReadyPlan(clusterId, keyspace);
        // Admission re-checks the configuration version under the cluster lock.
        return RepairOperation.of(queue.accept(clusterId, keyspace, requestKey, cluster.configVersion()).operation());
    }

    public RepairOperation get(UUID id) {
        return RepairOperation.of(require(id));
    }

    public List<NodeProgress> nodes(UUID id) {
        require(id);
        return execution.nodeProgress(id);
    }

    public List<RepairOperation> list(String clusterId, boolean activeOnly, int limit) {
        clusters.requireEnabled(clusterId);
        return queue.list(clusterId, activeOnly, limit).stream().map(RepairOperation::of).toList();
    }

    public RepairOperation acknowledge(UUID id, boolean cassandraActivityVerified) {
        if (!cassandraActivityVerified)
            throw new IllegalArgumentException("Explicit cassandraActivityVerified=true required");
        execution.acknowledge(id);
        return get(id);
    }

    private OperationRecord require(UUID id) {
        OperationRecord operation = queue.get(id);
        if (operation == null) throw new ControlPlaneExceptions.NotFound("Unknown operation");
        return operation;
    }
}
