package demo.platform.repair.jmx;

import demo.platform.misc.AdminAccess;
import demo.platform.repair.RepairCoordinator;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class JmxRepairProgressController {
    private final JmxRepairRepository progress;
    private final RepairCoordinator operations;
    private final AdminAccess admin;

    @GetMapping("/operations/{id}/nodes")
    public List<Map<String, Object>> nodes(@PathVariable UUID id,
            @RequestHeader(value = "X-Admin-Token", required = false) String token) {
        admin.require(token);
        operations.get(id);
        return progress.progress(id);
    }
}
