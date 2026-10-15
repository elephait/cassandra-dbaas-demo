package demo.platform.cluster;

import demo.platform.repair.OperationRecord.RepairPolicy;
import java.net.InetSocketAddress;
import java.util.List;

public record ClusterRecord(
    String id,
    List<ContactPoint> contactPoints,
    String datacenter,
    int expectedNodes,
    int replicationFactor,
    List<AddressMapping> addressMappings,
    int requestTimeoutSeconds,
    int reconnectSeconds,
    int maxKeyspaces,
    int provisioningLeaseSeconds,
    boolean enabled,
    long configVersion) {

    public static final String DEFAULT_CLUSTER = "default";

    public ClusterRecord {
        if (id == null || contactPoints == null || addressMappings == null)
            throw new IllegalArgumentException("Cluster id, contact points and mappings are required");
        if (!id.matches("[a-z][a-z0-9_-]{0,63}"))
            throw new IllegalArgumentException("Cluster id must match [a-z][a-z0-9_-]{0,63}");
        contactPoints = List.copyOf(contactPoints);
        addressMappings = List.copyOf(addressMappings);
        if (contactPoints.isEmpty()) throw new IllegalArgumentException("At least one contact point is required");
        if (datacenter == null || datacenter.isBlank()) throw new IllegalArgumentException("Datacenter is required");
        if (expectedNodes < 1 || replicationFactor < 1 || requestTimeoutSeconds < 1
            || reconnectSeconds < 1 || maxKeyspaces < 1 || provisioningLeaseSeconds < 3)
            throw new IllegalArgumentException("Cluster numeric settings must be positive");
        if (replicationFactor > expectedNodes)
            throw new IllegalArgumentException("Replication factor cannot exceed expected local replicas");
        if (addressMappings.stream().map(AddressMapping::discoveredHost).distinct().count() != addressMappings.size())
            throw new IllegalArgumentException("Discovered address mappings must be unique");
    }

    public ClusterRecord withVersion(long version) {
        return new ClusterRecord(id, contactPoints, datacenter, expectedNodes, replicationFactor,
            addressMappings, requestTimeoutSeconds, reconnectSeconds, maxKeyspaces,
            provisioningLeaseSeconds, enabled, version);
    }

    public boolean addressTranslationEnabled() {
        return !addressMappings.isEmpty();
    }

    public record ContactPoint(String host, int port) {
        public ContactPoint {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("Contact point host is required");
            host = host.trim();
            if (port < 1 || port > 65535) throw new IllegalArgumentException("Contact point port must be 1..65535");
        }

        public InetSocketAddress toSocketAddress() {
            return new InetSocketAddress(host, port);
        }
    }

    public record AddressMapping(String discoveredHost, ContactPoint target) {
        public AddressMapping {
            if (discoveredHost == null || discoveredHost.isBlank())
                throw new IllegalArgumentException("Discovered host is required");
            if (target == null) throw new IllegalArgumentException("Address mapping target is required");
        }
    }

    public record ClusterConfiguration(ClusterRecord cluster, RepairPolicy repairPolicy) {
        public ClusterConfiguration {
            if (cluster == null || repairPolicy == null)
                throw new IllegalArgumentException("Cluster and repair policy are required");
            if (!cluster.id().equals(repairPolicy.clusterId()))
                throw new IllegalArgumentException("Cluster and repair policy ids must match");
        }
    }
}
