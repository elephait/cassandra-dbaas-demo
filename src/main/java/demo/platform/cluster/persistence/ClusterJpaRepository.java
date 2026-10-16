package demo.platform.cluster.persistence;

import demo.platform.cluster.ClusterRecord;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

import java.util.List;
import java.util.Optional;

/**
 * Derived queries only. Record-returning finders are DTO projections: they select scalar columns and never
 * load the lazy contact-point or address-mapping collections.
 */
public interface ClusterJpaRepository extends JpaRepository<ClusterEntity, String> {
    /**
     * {@code SELECT ... FOR UPDATE} on the cluster row: the per-cluster mutex that serializes configuration
     * changes with keyspace and repair admission.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<ClusterEntity> findForUpdateById(String id);

    Optional<ClusterRecord.Settings> findSettingsById(String id);

    List<ClusterRecord.Summary> findSummariesByOrderByIdAsc();
}
