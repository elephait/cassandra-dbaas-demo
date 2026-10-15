package demo.platform.cluster.persistence;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface ClusterJpaRepository extends JpaRepository<ClusterEntity, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<ClusterEntity> findLockedById(String id);

    // Closed projection: scalars only, so it never touches the lazily loaded element collections or the
    // repair policy. Session management polls this on a timer; it needs nothing else.
    interface ConfigState {
        String getId();

        long getConfigVersion();

        boolean isEnabled();
    }

    @Query("select c.id as id, c.configVersion as configVersion, c.enabled as enabled "
        + "from ClusterEntity c where c.enabled = true order by c.id")
    List<ConfigState> findEnabledConfigStates();

    @Query("select c.id as id, c.configVersion as configVersion, c.enabled as enabled "
        + "from ClusterEntity c where c.id = :clusterId")
    Optional<ConfigState> findConfigState(String clusterId);

    @Query("select new demo.platform.cluster.ClusterRecord$Settings(c.id, c.datacenter, c.expectedNodes, "
        + "c.replicationFactor, c.maxKeyspaces, c.provisioningLeaseSeconds, c.enabled, c.configVersion) "
        + "from ClusterEntity c where c.id = :clusterId")
    Optional<demo.platform.cluster.ClusterRecord.Settings> findSettings(String clusterId);

    interface Summary {
        String getId();
        String getDatacenter();
        int getExpectedNodes();
        int getReplicationFactor();
        boolean isEnabled();
        long getConfigVersion();
    }

    @Query("select c.id as id, c.datacenter as datacenter, c.expectedNodes as expectedNodes, "
        + "c.replicationFactor as replicationFactor, c.enabled as enabled, c.configVersion as configVersion "
        + "from ClusterEntity c order by c.id")
    List<Summary> findSummaries();
}
