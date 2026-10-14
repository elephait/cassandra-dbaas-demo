package demo.platform.repair.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface RepairLockJpaRepository extends JpaRepository<RepairLockEntity, String> {
}
