package demo.platform.repair;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.KeyspaceNames;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord.RepairDecision;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.persistence.OperationEntity;
import demo.platform.repair.persistence.OperationJpaRepository;
import demo.platform.repair.persistence.RepairLockEntity;
import demo.platform.repair.persistence.RepairLockJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RepairQueueService {
    private final OperationJpaRepository operations;
    private final RepairLockJpaRepository locks;
    private final ClusterRegistryService clusters;
    private final RepairConfigurationService repairConfiguration;

    // Initialization joins cluster registration; neither side may commit on its own.
    @Transactional(propagation = Propagation.MANDATORY)
    public void initializeCluster(String clusterId) {
        locks.saveAndFlush(new RepairLockEntity(clusterId));
    }

    @Transactional(readOnly = true)
    public boolean hasPendingWork(String clusterId) {
        return operations.countByClusterIdAndStateIn(clusterId, List.of(
            RepairState.QUEUED, RepairState.CLAIMED, RepairState.RUNNING, RepairState.UNKNOWN)) > 0;
    }

    @Transactional
    public RepairDecision accept(String clusterId, String keyspace, String requestKey) {
        return accept(clusterId, keyspace, requestKey, -1);
    }

    @Transactional
    public RepairDecision accept(String clusterId, String keyspace, String requestKey, long expectedConfigVersion) {
        KeyspaceNames.validate(keyspace);
        var cluster = clusters.lockForAdmission(clusterId, -1);
        OperationRecord previous = find(clusterId, requestKey, keyspace);
        if (previous != null) return new RepairDecision(previous, false);
        clusters.validateConfigurationVersion(cluster, expectedConfigVersion);
        if (operations.countByClusterIdAndState(clusterId, RepairState.QUEUED) >= 1000)
            throw new ControlPlaneExceptions.Conflict("Repair queue capacity reached");
        RepairPolicy policy = repairConfiguration.getPolicy(clusterId);
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
