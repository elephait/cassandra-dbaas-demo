package demo.platform.repair.jmx;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import lombok.Data;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
@ConfigurationProperties(prefix = "demo.repair.jmx")
@Data
public class JmxRepairProperties {
    // Keyed by registered cluster id, then registered policy node name.
    // Credentials stay in deployment configuration, never operation receipts.
    private Map<String, Map<String, Endpoint>> targets = new HashMap<>();
    private int pollSeconds = 20;
    private int actionLeaseSeconds = 60;
    private int observationDeadlineSeconds = 86400;

    @Data
    public static class Endpoint {
        private String serviceUrl;
        private String hostId;
        private String username;
        private String password;
    }

    public Endpoint require(String cluster, String node) {
        Endpoint endpoint = targets.getOrDefault(cluster, Map.of()).get(node);
        if (endpoint == null || endpoint.serviceUrl == null || endpoint.hostId == null)
            throw new IllegalArgumentException("Missing JMX service-url/host-id for " + cluster + "/" + node);
        UUID.fromString(endpoint.hostId);
        return endpoint;
    }

    public void validateTiming() {
        if (pollSeconds < 1 || actionLeaseSeconds < 5 || observationDeadlineSeconds < actionLeaseSeconds)
            throw new IllegalArgumentException("Invalid JMX polling/lease/deadline settings");
    }
}
