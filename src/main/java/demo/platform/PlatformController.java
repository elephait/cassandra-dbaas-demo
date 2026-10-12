package demo.platform;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RestController
public class PlatformController {
    private final CqlSession session;
    private final RepairService repairs;
    private final int maxTenants;
    private final String token;

    public PlatformController(CqlSession session, RepairService repairs,
                              @Value("${demo.max-tenants}") int maxTenants,
                              @Value("${demo.admin-token}") String token) {
        this.session = session;
        this.repairs = repairs;
        this.maxTenants = maxTenants;
        this.token = token;
    }

    static String keyspace(String tenant) {
        if (tenant == null || !tenant.matches("[a-z][a-z0-9_]{0,23}"))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Tenant must match [a-z][a-z0-9_]{0,23}");
        return "tenant_" + tenant;
    }

    private void authorize(String supplied) {
        if (!token.equals(supplied)) throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Invalid admin token");
    }

    @GetMapping("/cluster/health")
    public Map<String, Object> health() {
        var nodes = session.getMetadata().getNodes().values().stream().map(n -> Map.of(
                "address", n.getEndPoint().toString(), "state", n.getState().name(),
                "datacenter", String.valueOf(n.getDatacenter()))).toList();
        long up = nodes.stream().filter(n -> n.get("state").equals("UP")).count();
        return Map.of("nodes", nodes, "up", up, "expected", 3, "quorumAvailable", up >= 2,
                "status", up == 3 ? "HEALTHY" : up >= 2 ? "DEGRADED" : "UNAVAILABLE",
                "note", "Driver observations; successful LOCAL_QUORUM requests are the availability proof");
    }

    @PostMapping("/tenants/{id}/keyspace")
    public synchronized Map<String, Object> provision(@PathVariable String id,
                                                      @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        authorize(supplied);
        String ks = keyspace(id);
        Set<String> existing = new HashSet<>();
        session.execute("SELECT keyspace_name FROM system_schema.keyspaces").forEach(r -> existing.add(r.getString(0)));
        if (!existing.contains(ks) && existing.stream().filter(k -> k.startsWith("tenant_")).count() >= maxTenants)
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Tenant quota reached");
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + ks + " WITH replication = {'class':'NetworkTopologyStrategy','dc1':3}");
        session.execute("CREATE TABLE IF NOT EXISTS " + ks + ".probe (id uuid PRIMARY KEY, value text)");
        return Map.of("keyspace", ks, "replicationFactor", 3, "maxTenants", maxTenants);
    }

    @PostMapping("/tenants/{id}/probe")
    public Map<String, Object> probe(@PathVariable String id, @RequestBody Probe body) {
        String ks = keyspace(id);
        if (body.value() == null || body.value().length() > 1024)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Value must contain at most 1024 characters");
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
        if (!session.getMetadata().getKeyspaces().containsKey(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal(ks)))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Provision tenant first");
        if (session.getMetadata().getNodes().values().stream().filter(n -> n.getState().name().equals("UP")).count() != 3)
            throw new ResponseStatusException(HttpStatus.CONFLICT, "All three nodes must be UP before repair");
        return repairs.start(ks);
    }

    @GetMapping("/operations/{id}")
    public RepairService.Operation operation(@PathVariable UUID id) {
        return repairs.get(id);
    }

    @ExceptionHandler(com.datastax.oss.driver.api.core.DriverException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> unavailable() {
        return Map.of("error", "Cassandra request failed; check cluster health and tenant provisioning");
    }

    public record Probe(String value) {
    }

    public record RepairRequest(String tenant, boolean approved) {
    }
}
