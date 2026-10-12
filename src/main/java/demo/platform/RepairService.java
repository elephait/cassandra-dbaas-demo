package demo.platform;

import jakarta.annotation.PreDestroy;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class RepairService {
    private final Map<UUID, Operation> operations = new ConcurrentHashMap<>();
    private final AtomicBoolean busy = new AtomicBoolean();
    private final ExecutorService worker = Executors.newSingleThreadExecutor();

    public Operation start(String ks) {
        if (!busy.compareAndSet(false, true))
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Repair already running");
        UUID id = UUID.randomUUID();
        Operation queued = new Operation(id, ks, "QUEUED", "Approved repair queued", Instant.now());
        operations.put(id, queued);
        worker.submit(() -> {
            try {
                update(id, ks, "RUNNING", "Repairing primary ranges sequentially on all nodes");
                Files.createDirectories(Path.of(".local", "operations"));
                for (String node : List.of("cassandra1", "cassandra2", "cassandra3")) {
                    Path log = Path.of(".local", "operations", id + "-" + node + ".log");
                    Process process = new ProcessBuilder("docker", "compose", "exec", "-T", node, "nodetool", "repair", "-pr", ks)
                            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
                    if (!process.waitFor(15, TimeUnit.MINUTES)) {
                        process.destroyForcibly();
                        throw new IllegalStateException("CLI timeout on " + node + "; remote repair may still run. Inspect nodetool before retrying");
                    }
                    if (process.exitValue() != 0)
                        throw new IllegalStateException("Repair failed on " + node + "; see " + log);
                }
                update(id, ks, "SUCCEEDED", "All three primary-range repairs completed");
            } catch (Exception e) {
                update(id, ks, "FAILED", e.getMessage());
            } finally {
                busy.set(false);
            }
        });
        return queued;
    }

    private void update(UUID id, String ks, String state, String detail) {
        Operation op = new Operation(id, ks, state, String.valueOf(detail), Instant.now());
        operations.put(id, op);
        try {
            Files.createDirectories(Path.of(".local", "operations"));
            Files.writeString(Path.of(".local", "operations", id + ".txt"), op + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.err.println("Audit write failed: " + e.getMessage());
        }
    }

    public Operation get(UUID id) {
        Operation op = operations.get(id);
        if (op == null)
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown operation (status memory resets on restart)");
        return op;
    }

    @PreDestroy
    void close() {
        worker.shutdown();
    }

    public record Operation(UUID id, String keyspace, String state, String detail, Instant updated) {
    }
}
