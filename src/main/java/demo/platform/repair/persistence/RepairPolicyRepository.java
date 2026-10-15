package demo.platform.repair.persistence;

import demo.platform.repair.OperationRecord.RepairPolicy;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

public interface RepairPolicyRepository extends JpaRepository<RepairPolicyEntity, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<RepairPolicyEntity> findLockedByClusterId(String clusterId);

    @Transactional(readOnly = true)
    default RepairPolicy get(String clusterId) {
        return findById(clusterId).map(RepairPolicyRepository::toDomain).orElse(null);
    }

    @Transactional
    default RepairPolicy save(RepairPolicy policy) {
        RepairPolicyEntity entity = findLockedByClusterId(policy.clusterId())
            .orElseGet(() -> new RepairPolicyEntity(policy.clusterId()));
        if (!entity.nodes().isEmpty() && toDomain(entity).equals(policy.withVersion(entity.version())))
            return toDomain(entity);
        entity.update(policy.nodes(), policy.command(), policy.leaseMinutes());
        return toDomain(saveAndFlush(entity));
    }

    private static RepairPolicy toDomain(RepairPolicyEntity entity) {
        return new RepairPolicy(entity.clusterId(), entity.nodes(), entity.command(),
            entity.leaseMinutes(), entity.version());
    }
}
