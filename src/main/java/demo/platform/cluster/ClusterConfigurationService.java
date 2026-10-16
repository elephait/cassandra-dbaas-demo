package demo.platform.cluster;

import demo.platform.cluster.persistence.ClusterEntity;
import demo.platform.cluster.persistence.ClusterJpaRepository;
import demo.platform.common.ControlPlaneExceptions;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Writes cluster configuration. Separate from the registry so other domains can depend on reads only. */
@Service
@RequiredArgsConstructor
public class ClusterConfigurationService {
    private final ClusterJpaRepository clusters;
    private final ObjectProvider<PendingClusterWork> pendingWork;

    /** Creates or updates a cluster. An identical replay is a no-op and keeps the version. */
    @Transactional
    public ClusterRecord save(ClusterRecord cluster) {
        // Same row lock as admission: no keyspace or repair can be admitted half-way through a change.
        ClusterEntity entity = clusters.findForUpdateById(cluster.id()).orElse(null);
        if (entity != null) {
            if (entity.matches(cluster)) return entity.toRecord();
            if (pendingWork.orderedStream().anyMatch(work -> work.hasPendingWork(cluster.id())))
                throw new ControlPlaneExceptions.Conflict(
                    "Cluster configuration cannot change while provisioning or repair work is pending");
        } else {
            entity = new ClusterEntity(cluster.id());
        }
        entity.apply(cluster);
        return clusters.saveAndFlush(entity).toRecord();
    }

    /**
     * Applies bootstrap configuration once: when the cluster is missing or still the migration's placeholder
     * (version 0). Returns true when it was applied.
     */
    @Transactional
    public boolean register(ClusterRecord bootstrap) {
        ClusterEntity entity = clusters.findForUpdateById(bootstrap.id()).orElse(null);
        if (entity != null && entity.configVersion() > 0) return false;
        if (entity == null) entity = new ClusterEntity(bootstrap.id());
        entity.apply(bootstrap);
        clusters.saveAndFlush(entity);
        return true;
    }
}
