package demo.platform.cluster;

import demo.platform.cluster.ClusterRecord.AddressMapping;
import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.misc.AdminAccess;
import demo.platform.repair.OperationRecord.RepairPolicy;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class ClusterController {
    private final ClusterService service;
    private final AdminAccess admin;

    @GetMapping("/clusters")
    public List<ClusterResponse> clusters(
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        admin.require(supplied);
        return service.list().stream().map(ClusterResponse::from).toList();
    }

    @PutMapping("/clusters/{clusterId}")
    public ClusterResponse saveCluster(
        @PathVariable String clusterId,
        @RequestBody ClusterRequest request,
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        admin.require(supplied);
        return ClusterResponse.from(service.save(request.toConfiguration(clusterId)));
    }

    @GetMapping("/clusters/{clusterId}/health")
    public Map<String, Object> health(@PathVariable String clusterId) {
        return service.health(clusterId);
    }

    public record ContactPointRequest(String host, int port) {
        ContactPoint toDomain() {
            return new ContactPoint(host, port);
        }
    }

    public record AddressMappingRequest(String discoveredHost, ContactPointRequest target) {
        AddressMapping toDomain() {
            if (target == null) throw new IllegalArgumentException("Mapping target is required");
            return new AddressMapping(discoveredHost, target.toDomain());
        }
    }

    public record RepairPolicyRequest(List<String> nodes, String command, int leaseMinutes) {
        RepairPolicy toDomain(String id) {
            return new RepairPolicy(id, nodes, command, leaseMinutes, 0);
        }
    }

    public record ClusterRequest(
        List<ContactPointRequest> contactPoints,
        String datacenter,
        int expectedNodes,
        int replicationFactor,
        List<AddressMappingRequest> addressMappings,
        int requestTimeoutSeconds,
        int reconnectSeconds,
        int maxKeyspaces,
        int provisioningLeaseSeconds,
        boolean enabled,
        RepairPolicyRequest repairPolicy) {

        ClusterConfiguration toConfiguration(String id) {
            if (contactPoints == null || repairPolicy == null)
                throw new IllegalArgumentException("Contact points and repair policy are required");
            ClusterRecord cluster = new ClusterRecord(id,
                contactPoints.stream().map(ContactPointRequest::toDomain).toList(), datacenter,
                expectedNodes, replicationFactor,
                addressMappings == null ? List.of()
                    : addressMappings.stream().map(AddressMappingRequest::toDomain).toList(),
                requestTimeoutSeconds, reconnectSeconds, maxKeyspaces,
                provisioningLeaseSeconds, enabled, 0);
            return new ClusterConfiguration(cluster, repairPolicy.toDomain(id));
        }
    }

    public record ClusterResponse(
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
        long configVersion,
        RepairPolicyResponse repairPolicy) {

        static ClusterResponse from(ClusterConfiguration configuration) {
            ClusterRecord cluster = configuration.cluster();
            RepairPolicy policy = configuration.repairPolicy();
            return new ClusterResponse(cluster.id(), cluster.contactPoints(), cluster.datacenter(),
                cluster.expectedNodes(), cluster.replicationFactor(), cluster.addressMappings(),
                cluster.requestTimeoutSeconds(), cluster.reconnectSeconds(), cluster.maxKeyspaces(),
                cluster.provisioningLeaseSeconds(), cluster.enabled(), cluster.configVersion(),
                new RepairPolicyResponse(policy.nodes(), policy.command(), policy.leaseMinutes(), policy.version()));
        }
    }

    public record RepairPolicyResponse(List<String> nodes, String command, int leaseMinutes, long version) {
    }
}
