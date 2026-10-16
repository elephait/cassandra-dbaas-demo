package demo.platform.cluster;

/** HTTP view of a cluster. Connection endpoints are deliberately not echoed back. */
public record ClusterResponse(
    String id,
    String datacenter,
    int expectedNodes,
    int replicationFactor,
    int requestTimeoutSeconds,
    int reconnectSeconds,
    int maxKeyspaces,
    int provisioningLeaseSeconds,
    boolean enabled,
    long configVersion) {

    static ClusterResponse from(ClusterRecord cluster) {
        return new ClusterResponse(cluster.id(), cluster.datacenter(), cluster.expectedNodes(),
            cluster.replicationFactor(), cluster.requestTimeoutSeconds(), cluster.reconnectSeconds(),
            cluster.maxKeyspaces(), cluster.provisioningLeaseSeconds(), cluster.enabled(), cluster.configVersion());
    }
}
