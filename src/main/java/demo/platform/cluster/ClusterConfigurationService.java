package demo.platform.cluster;

import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.keyspace.KeyspaceProvisioningService;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.RepairConfigurationService;
import demo.platform.repair.RepairQueueService;
import demo.platform.repair.OperationRecord.RepairPolicy;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ClusterConfigurationService {
    private final ClusterRepository clusters;
    private final RepairConfigurationService repairs;
    private final RepairQueueService queue;
    private final KeyspaceProvisioningService provisioning;

    @Transactional
    public ClusterConfiguration register(ClusterConfiguration configuration) {
        ClusterRecord previous = clusters.lockConfiguration(configuration.cluster().id());
        boolean bootstrap = previous == null || previous.configVersion() == 0;
        ClusterRecord cluster = bootstrap ? clusters.save(configuration.cluster()) : previous;
        if (previous == null) queue.initializeCluster(cluster.id());
        RepairPolicy existing = repairs.getPolicy(cluster.id());
        RepairPolicy policy = bootstrap || existing == null
            ? repairs.savePolicy(configuration.repairPolicy()) : existing;
        return new ClusterConfiguration(cluster, policy);
    }

    @Transactional
    public ClusterConfiguration save(ClusterConfiguration configuration) {
        String clusterId = configuration.cluster().id();
        // Lock first: provisioning and repair admission use this same cluster row.
        ClusterRecord previous = clusters.lockConfiguration(clusterId);
        if (previous != null) rejectUpdateWithPendingWork(clusterId);
        ClusterRecord cluster = clusters.save(configuration.cluster());
        if (previous == null) queue.initializeCluster(clusterId);
        RepairPolicy policy = repairs.savePolicy(configuration.repairPolicy());
        return new ClusterConfiguration(cluster, policy);
    }

    private void rejectUpdateWithPendingWork(String clusterId) {
        if (queue.hasPendingWork(clusterId) || provisioning.hasPendingWork(clusterId))
            throw new ControlPlaneExceptions.Conflict(
                "Cluster configuration cannot change while provisioning or repair work is pending");
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public ClusterConfiguration get(String clusterId) {
        ClusterRecord cluster = clusters.get(clusterId);
        if (cluster == null) return null;
        RepairPolicy policy = repairs.getPolicy(cluster.id());
        return policy == null ? null : new ClusterConfiguration(cluster, policy);
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public List<ClusterConfiguration> list(boolean enabledOnly) {
        var policies = repairs.listPolicies();
        return clusters.list(enabledOnly).stream()
            .map(cluster -> new ClusterConfiguration(cluster, policies.get(cluster.id())))
            .toList();
    }
}
