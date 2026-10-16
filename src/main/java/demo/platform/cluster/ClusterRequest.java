package demo.platform.cluster;

import java.util.List;

/** HTTP input for PUT /clusters/{id}; identity comes from the URL, the version from persistence. */
public record ClusterRequest(
    List<ClusterRecord.ContactPoint> contactPoints,
    String datacenter,
    int expectedNodes,
    int replicationFactor,
    List<ClusterRecord.AddressMapping> addressMappings,
    int requestTimeoutSeconds,
    int reconnectSeconds,
    int maxKeyspaces,
    int provisioningLeaseSeconds,
    boolean enabled) {

    ClusterRecord toRecord(String id) {
        return new ClusterRecord(id, contactPoints, datacenter, expectedNodes, replicationFactor,
            addressMappings == null ? List.of() : addressMappings, requestTimeoutSeconds, reconnectSeconds,
            maxKeyspaces, provisioningLeaseSeconds, enabled, 0);
    }
}
