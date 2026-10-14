package demo.platform.repair.persistence;

import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.keyspace.KeyspaceNames;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.OperationRecord.RepairDecision;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.RepairState;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class RepairQueueRepository {
    private final OperationJpaRepository operations;
    private final ClusterRepository clusters;
    private final RepairPolicyRepository repairPolicies;

    static OperationRecord map(ResultSet rs) throws SQLException {
        return new OperationRecord(UUID.fromString(rs.getString("operation_id")), rs.getString("cluster_id"),
            rs.getString("keyspace_name"), rs.getString("owner_id"), RepairState.valueOf(rs.getString("state")),
            rs.getString("detail"), rs.getLong("repair_policy_version"), rs.getTimestamp("updated_at").toInstant());
    }

    @Transactional
    public RepairDecision accept(String clusterId, String keyspace, String requestKey) {
        return accept(clusterId, keyspace, requestKey, -1);
    }

    @Transactional
    public RepairDecision accept(String clusterId, String keyspace, String requestKey, long expectedConfigVersion) {
        KeyspaceNames.validate(keyspace);
        clusters.lockEnabled(clusterId);
        OperationRecord previous = find(clusterId, requestKey, keyspace);
        if (previous != null) return new RepairDecision(previous, false);
        if (expectedConfigVersion >= 0 && clusters.get(clusterId).configVersion() != expectedConfigVersion)
            throw new ControlPlaneExceptions.Conflict("Cluster configuration changed before repair acceptance; retry");
        if (operations.countByClusterIdAndState(clusterId, RepairState.QUEUED) >= 1000)
            throw new ControlPlaneExceptions.Conflict("Repair queue capacity reached");
        RepairPolicy policy = repairPolicies.get(clusterId);
        if (policy == null) throw new IllegalStateException("Missing repair policy for cluster " + clusterId);
        UUID id = UUID.randomUUID();
        operations.saveAndFlush(new OperationEntity(id, clusterId, keyspace, requestKey,
            policy.version(), operations.databaseTime().toInstant()));
        return new RepairDecision(get(id), true);
    }

    @Transactional(readOnly = true)
    public OperationRecord find(String clusterId, String requestKey, String keyspace) {
        OperationRecord previous = operations.findReceiptByClusterIdAndRequestKey(clusterId, requestKey)
            .map(OperationJpaRepository.Receipt::toRecord).orElse(null);
        if (previous != null && !previous.keyspace().equals(keyspace))
            throw new ControlPlaneExceptions.Conflict("Idempotency-Key was already used for a different repair request");
        return previous;
    }

    @Transactional(readOnly = true)
    public OperationRecord get(UUID id) {
        return operations.findReceiptById(id).map(OperationJpaRepository.Receipt::toRecord).orElse(null);
    }

    @Transactional(readOnly = true)
    public List<OperationRecord> list(String clusterId, boolean activeOnly, int limit) {
        var page = PageRequest.of(0, limit);
        var receipts = activeOnly
            ? operations.findByClusterIdAndStateInOrderByUpdatedAtDescIdAsc(clusterId,
            List.of(RepairState.QUEUED, RepairState.CLAIMED, RepairState.RUNNING, RepairState.UNKNOWN), page)
            : operations.findByClusterIdOrderByUpdatedAtDescIdAsc(clusterId, page);
        return receipts.stream().map(OperationJpaRepository.Receipt::toRecord).toList();
    }

    @Transactional(readOnly = true)
    public int retryCount(UUID id) {
        return operations.findRetryCountById(id).orElseThrow().getRetryCount();
    }
}
