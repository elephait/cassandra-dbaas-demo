package demo.platform.repair.persistence;

import demo.platform.repair.OperationRecord;
import demo.platform.repair.RepairState;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Admission and read queries, all derived. {@link OperationRecord} is a DTO projection, so reads always
 * see the current row even after JDBC state transitions, and never return a cached entity.
 */
public interface OperationJpaRepository extends JpaRepository<OperationEntity, UUID> {
    Optional<OperationRecord> findRecordById(UUID id);

    Optional<OperationRecord> findRecordByClusterIdAndRequestKey(String clusterId, String requestKey);

    List<OperationRecord> findRecordsByClusterIdOrderByUpdatedAtDescIdAsc(String clusterId, Pageable page);

    List<OperationRecord> findRecordsByClusterIdAndStateInOrderByUpdatedAtDescIdAsc(
        String clusterId, Collection<RepairState> states, Pageable page);

    long countByClusterIdAndState(String clusterId, RepairState state);

    boolean existsByClusterIdAndStateIn(String clusterId, Collection<RepairState> states);
}
