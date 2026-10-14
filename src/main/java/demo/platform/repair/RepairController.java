package demo.platform.repair;

import demo.platform.misc.AdminAccess;
import demo.platform.repair.OperationRecord.RepairOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class RepairController {
    private final RepairCoordinator operations;
    private final AdminAccess admin;

    @PostMapping("/clusters/{clusterId}/repair")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public RepairOperation repair(
        @PathVariable String clusterId,
        @RequestBody RepairRequest request,
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied,
        @RequestHeader(value = "Idempotency-Key", required = false) String requestKey) {
        admin.require(supplied);
        return operations.requestRepair(clusterId, request.keyspace(), request.approved(), requestKey);
    }

    @GetMapping("/clusters/{clusterId}/operations")
    public List<RepairOperation> operations(
        @PathVariable String clusterId,
        @RequestParam(defaultValue = "true") boolean activeOnly,
        @RequestParam(defaultValue = "50") int limit,
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        admin.require(supplied);
        return operations.list(clusterId, activeOnly, admin.validateLimit(limit));
    }

    @GetMapping("/operations/{id}")
    public RepairOperation operation(
        @PathVariable UUID id,
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        admin.require(supplied);
        return operations.get(id);
    }

    @PostMapping("/operations/{id}/acknowledge")
    public RepairOperation acknowledge(
        @PathVariable UUID id,
        @RequestBody Acknowledgement request,
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        admin.require(supplied);
        if (!request.cassandraActivityVerified())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                "Explicit cassandraActivityVerified=true required");
        return operations.acknowledge(id);
    }

    public record RepairRequest(String keyspace, boolean approved) {
    }

    public record Acknowledgement(boolean cassandraActivityVerified) {
    }
}
