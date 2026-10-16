package demo.platform.repair.jmx;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * JMX endpoints and repair timing. Endpoints and credentials are deployment configuration, never
 * control-plane state or API output.
 */
@Component
@ConfigurationProperties(prefix = "demo.repair.jmx")
@Data
public class JmxRepairProperties {
    // Keyed by registered cluster id, then by node name. A cluster's entries are its repair plan.
    private Map<String, Map<String, Endpoint>> targets = new HashMap<>();
    // How often a running node repair is polled, and how often an unready node is retried.
    private int pollSeconds = 20;
    // Lease for one claim or one submit/poll action. A worker that dies mid-action loses it after this.
    private int actionLeaseSeconds = 60;
    // Soft threshold per node: past it the node and its operation are flagged delayed; polling continues.
    private int delayedAfterSeconds = 86_400;
    // Hard limit per node: past it the outcome is UNKNOWN and an operator must verify Cassandra.
    private int observationDeadlineSeconds = 604_800;

    /** The cluster's repair targets, validated, in node-name order (the order nodes are repaired in). */
    public SortedMap<String, Endpoint> requireTargets(String cluster) {
        Map<String, Endpoint> configured = targets.getOrDefault(cluster, Map.of());
        if (configured.isEmpty()) throw new IllegalArgumentException("No JMX repair targets configured for " + cluster);
        SortedMap<String, Endpoint> plan = new TreeMap<>();
        configured.keySet().forEach(node -> plan.put(node, require(cluster, node)));
        return plan;
    }

    public Endpoint require(String cluster, String node) {
        Endpoint endpoint = targets.getOrDefault(cluster, Map.of()).get(node);
        if (endpoint == null || endpoint.serviceUrl == null || endpoint.hostId == null)
            throw new IllegalArgumentException("Missing JMX service-url/host-id for " + cluster + "/" + node);
        UUID.fromString(endpoint.hostId);
        return endpoint;
    }

    @PostConstruct
    public void validateTiming() {
        if (pollSeconds < 1 || actionLeaseSeconds < 5 || delayedAfterSeconds < pollSeconds
            || observationDeadlineSeconds <= delayedAfterSeconds)
            throw new IllegalArgumentException("JMX timing requires poll >= 1s, action lease >= 5s and "
                + "poll <= delayed-after < observation-deadline");
    }

    @Data
    public static class Endpoint {
        private String serviceUrl;
        private String hostId;
        private String username;
        private String password;
    }
}
