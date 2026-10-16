package demo.platform.cluster;

import demo.platform.cluster.persistence.ClusterEntity;
import demo.platform.cluster.persistence.ClusterJpaRepository;
import demo.platform.common.ControlPlaneExceptions;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Read-side cluster lookups used by the other domains. Has no dependencies on them. */
@Service
@RequiredArgsConstructor
public class ClusterRegistryService {
    private final ClusterJpaRepository clusters;

    @Transactional(readOnly = true)
    public ClusterRecord.Settings requireEnabled(String clusterId) {
        var settings = clusters.findSettingsById(clusterId).orElse(null);
        if (settings == null || !settings.enabled()) throw new ControlPlaneExceptions.UnknownCluster(clusterId);
        return settings;
    }

    /** Full configuration, including connection endpoints; null when the cluster is not registered. */
    @Transactional(readOnly = true)
    public ClusterRecord get(String clusterId) {
        return clusters.findById(clusterId).map(ClusterEntity::toRecord).orElse(null);
    }

    @Transactional(readOnly = true)
    public List<ClusterRecord.Summary> list() {
        return clusters.findSummariesByOrderByIdAsc();
    }

    /**
     * Locks the cluster row for the caller's transaction, so its quota reservation or repair receipt commits
     * before any configuration change can. A non-negative expected version must match the locked row.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public ClusterRecord lockForAdmission(String clusterId, long expectedConfigVersion) {
        ClusterRecord cluster = clusters.findForUpdateById(clusterId).map(ClusterEntity::toRecord).orElse(null);
        if (cluster == null || !cluster.enabled()) throw new ControlPlaneExceptions.UnknownCluster(clusterId);
        validateConfigurationVersion(cluster, expectedConfigVersion);
        return cluster;
    }

    public void validateConfigurationVersion(ClusterRecord cluster, long expectedConfigVersion) {
        if (expectedConfigVersion >= 0 && cluster.configVersion() != expectedConfigVersion)
            throw new ControlPlaneExceptions.Conflict("Cluster configuration changed before admission; retry");
    }
}
