package demo.platform.repair.persistence;

import java.sql.Timestamp;

import demo.platform.repair.OperationRecord;
import demo.platform.repair.RepairState;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OperationJpaRepository extends JpaRepository<OperationEntity, UUID> {
    // Closed projections read current SQL values after JDBC queue transitions.
    interface Receipt {
        UUID getId();
        String getClusterId();
        String getKeyspace();
        String getOwner();
        RepairState getState();
        String getDetail();
        long getRepairPolicyVersion();
        Instant getUpdatedAt();

        default OperationRecord toRecord() {
            return new OperationRecord(getId(), getClusterId(), getKeyspace(), getOwner(),
                getState(), getDetail(), getRepairPolicyVersion(), getUpdatedAt());
        }
    }

    interface RetryCount {
        int getRetryCount();
    }

    Optional<Receipt> findReceiptById(UUID id);
    Optional<Receipt> findReceiptByClusterIdAndRequestKey(String clusterId, String requestKey);
    List<Receipt> findByClusterIdOrderByUpdatedAtDescIdAsc(String clusterId, Pageable page);
    List<Receipt> findByClusterIdAndStateInOrderByUpdatedAtDescIdAsc(
        String clusterId, Collection<RepairState> states, Pageable page);
    long countByClusterIdAndState(String clusterId, RepairState state);
    long countByClusterIdAndStateIn(String clusterId, Collection<RepairState> states);
    Optional<RetryCount> findRetryCountById(UUID id);

    @Query("select current_timestamp")
    Timestamp databaseTime();
}
