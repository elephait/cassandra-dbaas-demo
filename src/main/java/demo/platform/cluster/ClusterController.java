package demo.platform.cluster;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/** HTTP mapping only; authorization is applied by the admin-token interceptor. */
@RestController
@RequestMapping("/clusters")
@RequiredArgsConstructor
public class ClusterController {
    private final ClusterService service;

    @GetMapping
    public List<ClusterRecord.Summary> clusters() {
        return service.list();
    }

    @GetMapping("/{clusterId}")
    public ClusterResponse cluster(@PathVariable String clusterId) {
        return ClusterResponse.from(service.details(clusterId));
    }

    @PutMapping("/{clusterId}")
    public ClusterResponse saveCluster(@PathVariable String clusterId, @RequestBody ClusterRequest request) {
        return ClusterResponse.from(service.save(request.toRecord(clusterId)));
    }

    @GetMapping("/{clusterId}/health")
    public ClusterHealth health(@PathVariable String clusterId) {
        return service.health(clusterId);
    }
}
