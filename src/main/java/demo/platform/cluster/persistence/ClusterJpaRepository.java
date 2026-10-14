package demo.platform.cluster.persistence;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

import java.util.Optional;

public interface ClusterJpaRepository extends JpaRepository<ClusterEntity, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<ClusterEntity> findLockedById(String id);
}
