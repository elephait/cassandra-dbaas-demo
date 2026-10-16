package demo.platform.repair;

import demo.platform.repair.OperationRecord.RepairOperation;
import demo.platform.common.PageLimits;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/** HTTP mapping only; authorization is applied by the admin-token interceptor. */
@RestController
@RequiredArgsConstructor
public class RepairController {
    private final RepairCoordinator repairs;

    @PostMapping("/clusters/{clusterId}/repair")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public RepairOperation repair(@PathVariable String clusterId, @RequestBody RepairRequest request,
                                  @RequestHeader(value = "Idempotency-Key", required = false) String requestKey) {
        return repairs.requestRepair(clusterId, request.keyspace(), request.approved(), requestKey);
    }

    @GetMapping("/clusters/{clusterId}/operations")
    public List<RepairOperation> operations(@PathVariable String clusterId,
                                            @RequestParam(defaultValue = "true") boolean activeOnly,
                                            @RequestParam(defaultValue = "50") int limit) {
        return repairs.list(clusterId, activeOnly, PageLimits.require(limit));
    }

    @GetMapping("/operations/{id}")
    public RepairOperation operation(@PathVariable UUID id) {
        return repairs.get(id);
    }

    @GetMapping("/operations/{id}/nodes")
    public List<NodeProgress> nodes(@PathVariable UUID id) {
        return repairs.nodes(id);
    }

    @PostMapping("/operations/{id}/acknowledge")
    public RepairOperation acknowledge(@PathVariable UUID id, @RequestBody Acknowledgement request) {
        return repairs.acknowledge(id, request.cassandraActivityVerified());
    }

    /** {@code approved} is an explicit operator confirmation; false is rejected. */
    public record RepairRequest(String keyspace, boolean approved) {
    }

    /** Operator attests they verified Cassandra before an UNKNOWN repair releases the cluster. */
    public record Acknowledgement(boolean cassandraActivityVerified) {
    }
}
