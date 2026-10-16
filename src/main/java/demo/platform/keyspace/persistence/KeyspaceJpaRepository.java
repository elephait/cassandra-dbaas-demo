package demo.platform.keyspace.persistence;

import demo.platform.keyspace.ProvisioningState;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;

/**
 * Reads are derived queries. The three explicit updates are compare-and-set lease transitions: the
 * WHERE clause is the fence, and the affected-row count tells the caller whether it still owns the lease.
 */
public interface KeyspaceJpaRepository extends JpaRepository<KeyspaceEntity, KeyspaceId> {
    long countByIdClusterId(String clusterId);

    boolean existsByIdClusterIdAndStateAndExpiresAtAfter(String clusterId, ProvisioningState state, Instant now);

    boolean existsByIdAndOwnerAndStateAndExpiresAtAfter(KeyspaceId id, String owner, ProvisioningState state,
                                                        Instant now);

    List<KeyspaceEntity> findByIdClusterIdOrderByUpdatedAtDescIdKeyspaceAsc(String clusterId, Pageable page);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("""
        update KeyspaceEntity k set k.expiresAt = :expires, k.updatedAt = current_instant
        where k.id = :id and k.owner = :owner and k.state = :provisioning and k.expiresAt > current_instant""")
    int renew(KeyspaceId id, String owner, Instant expires, ProvisioningState provisioning);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("""
        update KeyspaceEntity k set k.state = :ready, k.owner = null, k.expiresAt = null, k.updatedAt = current_instant
        where k.id = :id and k.owner = :owner and k.state = :provisioning and k.expiresAt > current_instant""")
    int complete(KeyspaceId id, String owner, ProvisioningState provisioning, ProvisioningState ready);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("""
        update KeyspaceEntity k set k.owner = null, k.expiresAt = null, k.updatedAt = current_instant
        where k.id = :id and k.owner = :owner and k.state = :provisioning""")
    int release(KeyspaceId id, String owner, ProvisioningState provisioning);
}
