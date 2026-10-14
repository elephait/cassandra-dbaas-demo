package demo.platform.keyspace;

import demo.platform.misc.AdminAccess;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class KeyspaceController {
    private final KeyspaceService service;
    private final AdminAccess admin;

    @PostMapping("/clusters/{clusterId}/keyspaces/{keyspace}")
    public Map<String, Object> provision(
        @PathVariable String clusterId,
        @PathVariable String keyspace,
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        admin.require(supplied);
        return service.provision(clusterId, keyspace);
    }

    @GetMapping("/clusters/{clusterId}/keyspaces")
    public List<KeyspaceRecord> keyspaces(
        @PathVariable String clusterId,
        @RequestParam(defaultValue = "50") int limit,
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        admin.require(supplied);
        return service.list(clusterId, admin.validateLimit(limit));
    }

    @PostMapping("/clusters/{clusterId}/keyspaces/{keyspace}/probe")
    public Map<String, Object> probe(
        @PathVariable String clusterId,
        @PathVariable String keyspace,
        @RequestBody Probe body,
        @RequestHeader(value = "X-Admin-Token", required = false) String supplied) {
        admin.require(supplied);
        return service.probe(clusterId, keyspace, body.value());
    }

    public record Probe(String value) {
    }
}
