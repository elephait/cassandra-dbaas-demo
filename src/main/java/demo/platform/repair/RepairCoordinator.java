package demo.platform.repair;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.KeyspaceNames;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.repair.OperationRecord.RepairOperation;
import demo.platform.repair.persistence.RepairExecutionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RepairCoordinator {
    private final CassandraConnectionManager connections;
    private final ClusterRegistryService clusters;
    private final RepairQueueService queue;
    private final RepairExecutionRepository execution;

    private static RepairOperation toView(OperationRecord operation) {
        return new RepairOperation(operation.id(), operation.clusterId(), operation.keyspace(),
            operation.state(), operation.detail(), operation.repairPolicyVersion(), operation.updated());
    }

    private static void validateRequestKey(String requestKey) {
        if (requestKey == null || !requestKey.matches("[A-Za-z0-9._:-]{1,128}"))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                "Idempotency-Key required: 1..128 letters, digits, '.', '_', ':', or '-'");
    }

    public RepairOperation requestRepair(String clusterId, String keyspace, boolean approved, String requestKey) {
        if (!approved)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Explicit approved=true required");
        validateRequestKey(requestKey);
        KeyspaceNames.validate(keyspace);
        OperationRecord previous = queue.find(clusterId, requestKey, keyspace);
        if (previous != null) return toView(previous);
        ClusterRecord cluster = clusters.requireEnabled(clusterId);
        var session = connections.requireSession(cluster);
        if (!session.getMetadata().getKeyspaces().containsKey(
            com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal(keyspace)))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Provision keyspace first");
        long up = session.getMetadata().getNodes().values().stream()
            .filter(node -> node.getState().name().equals("UP"))
            .filter(node -> cluster.datacenter().equals(node.getDatacenter())).count();
        if (up != cluster.expectedNodes())
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                "All expected nodes must be UP before repair");
        return toView(queue.accept(clusterId, keyspace, requestKey, cluster.configVersion()).operation());
    }

    public RepairOperation get(UUID id) {
        OperationRecord operation = queue.get(id);
        if (operation == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown operation");
        return toView(operation);
    }

    public List<RepairOperation> list(String clusterId, boolean activeOnly, int limit) {
        clusters.requireEnabled(clusterId);
        return queue.list(clusterId, activeOnly, limit).stream()
            .map(RepairCoordinator::toView).toList();
    }

    public RepairOperation acknowledge(UUID id) {
        execution.reconcileExpired();
        execution.acknowledge(id);
        return get(id);
    }
}
