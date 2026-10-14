package demo.platform.cluster;

import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.persistence.ClusterConfigurationRepository;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.CassandraConnectionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class ClusterService {
    private final CassandraConnectionManager connections;
    private final ClusterConfigurationRepository configurations;
    private final ClusterRepository clusters;

    public List<ClusterConfiguration> list() {
        return configurations.list(false);
    }

    public ClusterConfiguration save(ClusterConfiguration configuration) {
        ClusterConfiguration previous = configurations.get(configuration.cluster().id());
        ClusterConfiguration saved = configurations.save(configuration);
        if (previous == null || previous.cluster().configVersion() != saved.cluster().configVersion())
            connections.invalidate(saved.cluster().id());
        connections.requestRefresh();
        return saved;
    }

    public Map<String, Object> health(String clusterId) {
        ClusterRecord cluster = clusters.requireEnabled(clusterId);
        var available = connections.currentSession(cluster);
        if (available.isEmpty()) {
            return Map.of("clusterId", clusterId, "nodes", List.of(), "up", 0,
                "expected", cluster.expectedNodes(), "quorumAvailable", false,
                "status", "UNAVAILABLE", "note", "Cassandra connection retry is in progress");
        }
        var nodes = available.get().getMetadata().getNodes().values().stream().map(node -> Map.of(
            "address", node.getEndPoint().toString(), "state", node.getState().name(),
            "datacenter", String.valueOf(node.getDatacenter()))).toList();
        long up = nodes.stream().filter(node -> node.get("state").equals("UP"))
            .filter(node -> cluster.datacenter().equals(node.get("datacenter"))).count();
        long quorum = cluster.replicationFactor() / 2L + 1;
        return Map.of("clusterId", clusterId, "nodes", nodes, "up", up,
            "expected", cluster.expectedNodes(), "quorumAvailable", up >= quorum,
            "status", up == cluster.expectedNodes() ? "HEALTHY" : up >= quorum ? "DEGRADED" : "UNAVAILABLE",
            "note", "Driver observations; successful LOCAL_QUORUM requests are the availability proof");
    }
}
