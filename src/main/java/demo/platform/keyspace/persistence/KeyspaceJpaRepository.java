package demo.platform.keyspace.persistence;

import demo.platform.keyspace.ProvisioningState;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public interface KeyspaceJpaRepository extends JpaRepository<KeyspaceEntity, KeyspaceId> {
    long countById_ClusterId(String clusterId);

    @Query("select count(k) from KeyspaceEntity k where k.id.clusterId = :clusterId and k.state = :provisioning and k.owner is not null and k.expiresAt > current_instant")
    long countProvisioningByState(String clusterId, ProvisioningState provisioning);

    List<KeyspaceEntity> findById_ClusterIdOrderByUpdatedAtDescId_KeyspaceAsc(String clusterId, Pageable page);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("update KeyspaceEntity k set k.expiresAt = :expires, k.updatedAt = current_instant where k.id = :id and k.owner = :owner and k.state = :provisioning and k.expiresAt > current_instant")
    int renew(KeyspaceId id, String owner, Instant expires, ProvisioningState provisioning);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("update KeyspaceEntity k set k.state = :ready, k.owner = null, k.expiresAt = null, k.updatedAt = current_instant where k.id = :id and k.owner = :owner and k.state = :provisioning and k.expiresAt > current_instant")
    int complete(KeyspaceId id, String owner, ProvisioningState provisioning, ProvisioningState ready);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("update KeyspaceEntity k set k.owner = null, k.expiresAt = null, k.updatedAt = current_instant where k.id = :id and k.owner = :owner and k.state = :provisioning")
    int release(KeyspaceId id, String owner, ProvisioningState provisioning);

    @Query("select count(k) > 0 from KeyspaceEntity k where k.id = :id and k.owner = :owner and k.state = :provisioning and k.expiresAt > current_instant")
    boolean owns(KeyspaceId id, String owner, ProvisioningState provisioning);

    @Query("select current_timestamp")
    Timestamp databaseTime();
}
