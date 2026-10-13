package demo.platform;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class PlatformController {
    private final CassandraConnectionManager connections;
    private final RepairService repairs;
    private final CassandraProperties properties;
    private final ControlPlaneRepository controlPlane;

    static String keyspace(String tenant) {
        if (tenant == null || !tenant.matches("[a-z][a-z0-9_]{0,23}"))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Tenant must match [a-z][a-z0-9_]{0,23}");
        return "tenant_" + tenant;
    }

    private void authorize(String supplied) {
        if (!properties.getAdminToken().equals(supplied)) throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Invalid admin token");
    }

    @GetMapping("/cluster/health")
    public Map<String, Object> health() {
        var available = connections.currentSession();
        if (available.isEmpty())
            return Map.of("nodes", java.util.List.of(), "up", 0, "expected", properties.getCluster().getExpectedNodes(),
                    "quorumAvailable", false, "status", "UNAVAILABLE", "note", "Cassandra connection retry is in progress");
        var session = available.get();
        var nodes = session.getMetadata().getNodes().values().stream().map(n -> Map.of(
                "address", n.getEndPoint().toString(), "state", n.getState().name(),
                "datacenter", String.valueOf(n.getDatacenter()))).toList();
        long up = nodes.stream().filter(n -> n.get("state").equals("UP")).count();
        long quorum = properties.getCassandra().getReplicationFactor() / 2L + 1;
        return Map.of("nodes", nodes, "up", up, "expected", properties.getCluster().getExpectedNodes(), "quorumAvailable", up >= quorum,
            "status", up == properties.getCluster().getExpectedNodes() ? "HEALTHY" : up >= quorum ? "DEGRADED" : "UNAVAILABLE",
                "note", "Driver observations; successful LOCAL_QUORUM requests are the availability proof");
    }

    @PostMapping("/tenants/{id}/keyspace")
    public Map<String, Object> provision(@PathVariable String id,
                                         @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        authorize(supplied);
        String ks = keyspace(id);
        var session = connections.requireSession();
        var decision = controlPlane.beginProvision(id, ks, properties.getMaxTenants(),
                Instant.now().plusSeconds(properties.getProvisioningLeaseSeconds()));
        if (!decision.alreadyReady()) {
            try {
                session.execute("CREATE KEYSPACE IF NOT EXISTS " + ks + " WITH replication = {'class':'NetworkTopologyStrategy','" + properties.getDatacenter() + "':" + properties.getCassandra().getReplicationFactor() + "}");
                session.execute("CREATE TABLE IF NOT EXISTS " + ks + ".probe (id uuid PRIMARY KEY, value text)");
                controlPlane.completeProvision(id, decision.owner());
            } catch (RuntimeException e) {
                controlPlane.releaseProvision(id, decision.owner());
                throw e;
            }
        }
        return Map.of("keyspace", ks, "replicationFactor", properties.getCassandra().getReplicationFactor(), "maxTenants", properties.getMaxTenants());
    }

    @GetMapping("/tenants")
    public List<ControlPlaneRepository.TenantRecord> tenants(
            @RequestParam(defaultValue = "50") int limit,
            @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        authorize(supplied);
        return controlPlane.listTenants(validateLimit(limit));
    }

    @PostMapping("/tenants/{id}/probe")
    public Map<String, Object> probe(@PathVariable String id, @RequestBody Probe body) {
        String ks = keyspace(id);
        if (body.value() == null || body.value().length() > 1024)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Value must contain at most 1024 characters");
        var session = connections.requireSession();
        UUID row = UUID.randomUUID();
        session.execute(SimpleStatement.newInstance("INSERT INTO " + ks + ".probe (id,value) VALUES (?,?) USING TTL 3600", row, body.value())
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
        var result = session.execute(SimpleStatement.newInstance("SELECT value FROM " + ks + ".probe WHERE id=?", row)
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).one();
        if (result == null || !body.value().equals(result.getString(0)))
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Read-back mismatch");
        return Map.of("id", row, "value", result.getString(0), "consistency", "LOCAL_QUORUM");
    }

    @PostMapping("/cluster/repair")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public RepairService.Operation repair(@RequestBody RepairRequest request,
                                          @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        authorize(supplied);
        if (!request.approved())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Explicit approved=true required");
        String ks = keyspace(request.tenant());
        var session = connections.requireSession();
        if (!session.getMetadata().getKeyspaces().containsKey(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal(ks)))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Provision tenant first");
        if (session.getMetadata().getNodes().values().stream().filter(n -> n.getState().name().equals("UP")).count() != properties.getCluster().getExpectedNodes())
            throw new ResponseStatusException(HttpStatus.CONFLICT, "All expected nodes must be UP before repair");
        return repairs.start(ks);
    }

    @GetMapping("/operations")
    public List<RepairService.Operation> operations(
            @RequestParam(defaultValue = "true") boolean activeOnly,
            @RequestParam(defaultValue = "50") int limit,
            @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        authorize(supplied);
        return repairs.list(activeOnly, validateLimit(limit));
    }

    @GetMapping("/operations/{id}")
    public RepairService.Operation operation(@PathVariable UUID id,
                                             @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        authorize(supplied);
        return repairs.get(id);
    }

    @PostMapping("/operations/{id}/acknowledge")
    public RepairService.Operation acknowledge(@PathVariable UUID id, @RequestBody Acknowledgement request,
                                                @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        authorize(supplied);
        if (!request.cassandraActivityVerified())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Explicit cassandraActivityVerified=true required");
        return repairs.acknowledge(id);
    }

    @ExceptionHandler(com.datastax.oss.driver.api.core.DriverException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> unavailable() {
        return Map.of("error", "Cassandra request failed; check cluster health and tenant provisioning");
    }

    @ExceptionHandler(ControlPlaneRepository.TenantQuotaExceededException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> quotaExceeded() {
        return Map.of("error", "Tenant quota reached");
    }

    @ExceptionHandler(ControlPlaneRepository.ControlPlaneConflictException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> controlPlaneConflict(ControlPlaneRepository.ControlPlaneConflictException exception) {
        return Map.of("error", exception.getMessage());
    }

    private static int validateLimit(int limit) {
        if (limit < 1 || limit > 200)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be between 1 and 200");
        return limit;
    }

    public record Probe(String value) {
    }

    public record RepairRequest(String tenant, boolean approved) {
    }

    public record Acknowledgement(boolean cassandraActivityVerified) {
    }
}
