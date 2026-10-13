package demo.platform;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class RepairService {
    private final ExecutorService worker = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService leaseWorker = Executors.newSingleThreadScheduledExecutor();
    private final CassandraProperties properties;
    private final ControlPlaneRepository controlPlane;

    public Operation start(String ks) {
        controlPlane.reconcileExpiredRepair();
        var record = controlPlane.startRepair(ks, Instant.now().plusSeconds(properties.getRepair().getLeaseMinutes() * 60L));
        try {
            worker.submit(() -> run(record));
        } catch (RuntimeException e) {
            controlPlane.updateOperation(new ControlPlaneRepository.OperationRecord(record.id(), ks, record.owner(), "FAILED", e.getMessage(), Instant.now()));
            controlPlane.releaseRepair(record.owner());
            throw e;
        }
        return toOperation(record);
    }

    private void run(ControlPlaneRepository.OperationRecord queued) {
        UUID id = queued.id();
        String ks = queued.keyspace();
        long leaseSeconds = properties.getRepair().getLeaseMinutes() * 60L;
        long heartbeatSeconds = Math.max(1, leaseSeconds / 3);
        ScheduledFuture<?> heartbeat = leaseWorker.scheduleAtFixedRate(
            () -> controlPlane.renewRepair(queued.owner(), Instant.now().plusSeconds(leaseSeconds)),
            heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
        try {
            update(queued, "RUNNING", "Repairing primary ranges sequentially on all nodes");
            Files.createDirectories(Path.of(".local", "operations"));
            for (String node : properties.getRepair().getNodes().split(",")) {
                Path log = Path.of(".local", "operations", id + "-" + node + ".log");
                String command = properties.getRepair().getCommand().replace("{node}", node.trim()).replace("{keyspace}", ks);
                Process process = new ProcessBuilder(command.trim().split("\\s+"))
                        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
                if (!process.waitFor(15, TimeUnit.MINUTES)) {
                    process.destroyForcibly();
                    throw new IllegalStateException("CLI timeout on " + node + "; remote repair may still run. Inspect nodetool before retrying");
                }
                if (process.exitValue() != 0)
                    throw new IllegalStateException("Repair failed on " + node + "; see " + log);
            }
            update(queued, "SUCCEEDED", "All configured primary-range repairs completed");
        } catch (Exception e) {
            update(queued, "FAILED", e.getMessage());
        } finally {
            heartbeat.cancel(false);
            controlPlane.releaseRepair(queued.owner());
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    void reconcileAfterStartup() {
        controlPlane.reconcileExpiredRepair();
    }

    private void update(ControlPlaneRepository.OperationRecord previous, String state, String detail) {
        controlPlane.updateOperation(new ControlPlaneRepository.OperationRecord(
                previous.id(), previous.keyspace(), previous.owner(), state, String.valueOf(detail), Instant.now()));
    }

    private static Operation toOperation(ControlPlaneRepository.OperationRecord record) {
        return new Operation(record.id(), record.keyspace(), record.state(), record.detail(), record.updated());
    }

    public Operation get(UUID id) {
        var operation = controlPlane.getOperation(id);
        if (operation == null)
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown operation");
        return toOperation(operation);
    }

    public List<Operation> list(boolean activeOnly, int limit) {
        return controlPlane.listOperations(activeOnly, limit).stream().map(RepairService::toOperation).toList();
    }

    public Operation acknowledge(UUID id) {
        controlPlane.reconcileExpiredRepair();
        controlPlane.acknowledgeUnknownRepair(id);
        return get(id);
    }

    @PreDestroy
    void close() {
        worker.shutdown();
        leaseWorker.shutdown();
    }

    public record Operation(UUID id, String keyspace, String state, String detail, Instant updated) {
    }
}
