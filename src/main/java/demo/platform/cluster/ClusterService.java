package demo.platform.cluster;

import demo.platform.cluster.cassandra.CassandraSessions;
import demo.platform.cluster.cassandra.ClusterMetadataRepository;
import demo.platform.common.ControlPlaneExceptions;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

/** Cluster API use cases. */
@Service
@RequiredArgsConstructor
public class ClusterService {
    private final ClusterRegistryService registry;
    private final ClusterConfigurationService configurations;
    private final CassandraSessions sessions;
    private final ClusterMetadataRepository metadata;

    public List<ClusterRecord.Summary> list() {
        return registry.list();
    }

    public ClusterRecord details(String clusterId) {
        ClusterRecord cluster = registry.get(clusterId);
        if (cluster == null) throw new ControlPlaneExceptions.UnknownCluster(clusterId);
        return cluster;
    }

    public ClusterRecord save(ClusterRecord cluster) {
        ClusterRecord saved = configurations.save(cluster);
        // A new version is picked up lazily on the next request; a disabled cluster gets no requests,
        // so its session is closed here.
        if (!saved.enabled()) sessions.invalidate(saved.id());
        return saved;
    }

    public ClusterHealth health(String clusterId) {
        ClusterRecord.Settings cluster = registry.requireEnabled(clusterId);
        var observed = metadata.nodes(cluster);
        if (observed.isEmpty())
            return new ClusterHealth(clusterId, List.of(), 0, cluster.expectedNodes(), false, "UNAVAILABLE",
                "Cassandra connection is being established");
        var nodes = observed.get().stream()
            .map(node -> new ClusterHealth.NodeView(node.address(), node.state(), node.datacenter())).toList();
        long up = observed.get().stream()
            .filter(node -> node.up() && cluster.datacenter().equals(node.datacenter())).count();
        long quorum = cluster.replicationFactor() / 2L + 1;
        String status = up == cluster.expectedNodes() ? "HEALTHY" : up >= quorum ? "DEGRADED" : "UNAVAILABLE";
        return new ClusterHealth(clusterId, nodes, up, cluster.expectedNodes(), up >= quorum, status,
            "Driver observations; successful LOCAL_QUORUM requests are the availability proof");
    }
}
