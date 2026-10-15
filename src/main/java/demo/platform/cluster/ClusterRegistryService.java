package demo.platform.cluster;

import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ControlPlaneExceptions;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ClusterRegistryService {
    private final ClusterRepository clusters;

    @Transactional(readOnly = true)
    public ClusterRecord requireEnabled(String clusterId) {
        return requireEnabled(clusterId, clusters.get(clusterId));
    }

    // Admission belongs to the caller's transaction: the cluster lock must remain held until its
    // quota reservation or repair receipt commits. This service has no connection-management work.
    @Transactional(propagation = Propagation.MANDATORY)
    public ClusterRecord lockForAdmission(String clusterId, long expectedConfigVersion) {
        ClusterRecord cluster = requireEnabled(clusterId, clusters.lockConfiguration(clusterId));
        validateConfigurationVersion(cluster, expectedConfigVersion);
        return cluster;
    }

    public void validateConfigurationVersion(ClusterRecord cluster, long expectedConfigVersion) {
        if (expectedConfigVersion >= 0 && cluster.configVersion() != expectedConfigVersion)
            throw new ControlPlaneExceptions.Conflict("Cluster configuration changed before admission; retry");
    }

    private static ClusterRecord requireEnabled(String clusterId, ClusterRecord cluster) {
        if (cluster == null || !cluster.enabled()) throw new ControlPlaneExceptions.UnknownCluster(clusterId);
        return cluster;
    }
}
