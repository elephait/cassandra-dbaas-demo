package demo.platform.keyspace;

import demo.platform.common.PageLimits;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/** HTTP mapping only; authorization is applied by the admin-token interceptor. */
@RestController
@RequestMapping("/clusters/{clusterId}/keyspaces")
@RequiredArgsConstructor
public class KeyspaceController {
    private final KeyspaceService service;

    @PostMapping("/{keyspace}")
    public KeyspaceService.ProvisionResult provision(@PathVariable String clusterId, @PathVariable String keyspace) {
        return service.provision(clusterId, keyspace);
    }

    @GetMapping
    public List<KeyspaceRecord> keyspaces(@PathVariable String clusterId,
                                          @RequestParam(defaultValue = "50") int limit) {
        return service.list(clusterId, PageLimits.require(limit));
    }

    @PostMapping("/{keyspace}/probe")
    public KeyspaceService.ProbeResult probe(@PathVariable String clusterId, @PathVariable String keyspace,
                                             @RequestBody ProbeRequest body) {
        return service.probe(clusterId, keyspace, body.value());
    }

    public record ProbeRequest(String value) {
    }
}
