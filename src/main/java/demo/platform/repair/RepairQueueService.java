package demo.platform.repair;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.PendingClusterWork;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.common.DatabaseClock;
import demo.platform.keyspace.KeyspaceNames;
import demo.platform.repair.OperationRecord.RepairDecision;
import demo.platform.repair.persistence.OperationEntity;
import demo.platform.repair.persistence.OperationJpaRepository;
import demo.platform.repair.persistence.RepairLockEntity;
import demo.platform.repair.persistence.RepairLockJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/** Durable, idempotent admission of repair requests, plus read access to them. */
@Service
@RequiredArgsConstructor
public class RepairQueueService implements PendingClusterWork {
    static final int MAX_QUEUED_PER_CLUSTER = 1000;

    private final OperationJpaRepository operations;
    private final ClusterRegistryService clusters;
    private final RepairLockJpaRepository locks;
    private final DatabaseClock clock;

    @Override
    @Transactional(readOnly = true)
    public boolean hasPendingWork(String clusterId) {
        return operations.existsByClusterIdAndStateIn(clusterId, RepairState.ACTIVE);
    }

    @Transactional
    public RepairDecision accept(String clusterId, String keyspace, String requestKey) {
        return accept(clusterId, keyspace, requestKey, -1);
    }

    /**
     * The cluster row lock serializes admission per cluster: a replayed key returns the original receipt,
     * and the (cluster_id, request_key) unique constraint backs this up across replicas.
     */
    @Transactional
    public RepairDecision accept(String clusterId, String keyspace, String requestKey, long expectedConfigVersion) {
        KeyspaceNames.validate(keyspace);
        var cluster = clusters.lockForAdmission(clusterId, -1);
        OperationRecord previous = find(clusterId, requestKey, keyspace);
        if (previous != null) return new RepairDecision(previous, false);
        clusters.validateConfigurationVersion(cluster, expectedConfigVersion);
        if (operations.countByClusterIdAndState(clusterId, RepairState.QUEUED) >= MAX_QUEUED_PER_CLUSTER)
            throw new ControlPlaneExceptions.Conflict("Repair queue capacity reached");
        // The cluster's single repair reservation row is created on first use, under the cluster lock.
        if (!locks.existsById(clusterId)) locks.saveAndFlush(new RepairLockEntity(clusterId));
        UUID id = UUID.randomUUID();
        operations.saveAndFlush(new OperationEntity(id, clusterId, keyspace, requestKey, clock.now()));
        return new RepairDecision(get(id), true);
    }

    @Transactional(readOnly = true)
    public OperationRecord find(String clusterId, String requestKey, String keyspace) {
        OperationRecord previous = operations.findRecordByClusterIdAndRequestKey(clusterId, requestKey).orElse(null);
        if (previous != null && !previous.keyspace().equals(keyspace))
            throw new ControlPlaneExceptions.Conflict("Idempotency-Key was already used for a different repair request");
        return previous;
    }

    @Transactional(readOnly = true)
    public OperationRecord get(UUID id) {
        return operations.findRecordById(id).orElse(null);
    }

    @Transactional(readOnly = true)
    public List<OperationRecord> list(String clusterId, boolean activeOnly, int limit) {
        var page = PageRequest.of(0, limit);
        return activeOnly
            ? operations.findRecordsByClusterIdAndStateInOrderByUpdatedAtDescIdAsc(clusterId, RepairState.ACTIVE, page)
            : operations.findRecordsByClusterIdOrderByUpdatedAtDescIdAsc(clusterId, page);
    }
}
