package demo.platform.cluster.persistence;

import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.ClusterRecord;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.persistence.RepairPolicyRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class ClusterConfigurationRepository {
    private final ClusterRepository clusters;
    private final RepairPolicyRepository repairPolicies;
    private final TransactionTemplate transactions;

    public ClusterConfiguration register(ClusterConfiguration configuration) {
        return transactions.execute(status -> {
            ClusterRecord previous = clusters.lockConfiguration(configuration.cluster().id());
            boolean bootstrap = previous == null || previous.configVersion() == 0;
            ClusterRecord cluster = clusters.registerConfiguredCluster(configuration.cluster());
            RepairPolicy existing = repairPolicies.get(cluster.id());
            RepairPolicy policy = bootstrap || existing == null
                ? repairPolicies.save(configuration.repairPolicy()) : existing;
            return new ClusterConfiguration(cluster, policy);
        });
    }

    public ClusterConfiguration save(ClusterConfiguration configuration) {
        return transactions.execute(status -> {
            ClusterRecord cluster = clusters.save(configuration.cluster());
            RepairPolicy policy = repairPolicies.save(configuration.repairPolicy());
            return new ClusterConfiguration(cluster, policy);
        });
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public ClusterConfiguration get(String clusterId) {
        ClusterRecord cluster = clusters.get(clusterId);
        if (cluster == null) return null;
        RepairPolicy policy = repairPolicies.get(cluster.id());
        return policy == null ? null : new ClusterConfiguration(cluster, policy);
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public List<ClusterConfiguration> list(boolean enabledOnly) {
        var policies = repairPolicies.list();
        return clusters.list(enabledOnly).stream()
            .map(cluster -> new ClusterConfiguration(cluster, policies.get(cluster.id())))
            .toList();
    }
}
