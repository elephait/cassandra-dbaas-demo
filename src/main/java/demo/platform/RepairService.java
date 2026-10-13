package demo.platform;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service @RequiredArgsConstructor @Slf4j
public class RepairService {
    private final ExecutorService worker = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService leaseWorker = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean active = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CassandraProperties properties;
    private final ControlPlaneRepository controlPlane;
    private final CassandraConnectionManager connections;

    public Operation replay(String ks, String key) {
        var op = controlPlane.findRepair(key, ks); return op == null ? null : toOperation(op);
    }
    public Operation start(String ks, String key) {
        if (closing.get()) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Worker shutting down");
        return toOperation(controlPlane.acceptRepair(ks, Instant.now(), key).operation());
    }
    @EventListener(ApplicationReadyEvent.class)
    void startPolling() {
        int seconds = Math.toIntExact(properties.getRepair().getLeaseMinutes() * 60L);
        if (seconds < 3 || properties.getRepair().getNodes() == null || properties.getRepair().getNodes().split(",").length == 0)
            throw new IllegalArgumentException("Positive lease and nonempty repair nodes required");
        poller.scheduleWithFixedDelay(this::poll, 0, 2, TimeUnit.SECONDS);
    }
    void poll() {
        if (closing.get()) return;
        try {
            controlPlane.reconcileExpiredRepair();
            if (!active.compareAndSet(false, true)) return;
            var op = controlPlane.claimRepair(Math.toIntExact(properties.getRepair().getLeaseMinutes() * 60L));
            if (op == null) { active.set(false); return; }
            try { worker.submit(() -> { try { run(op); } finally { active.set(false); } }); }
            catch (RejectedExecutionException e) { active.set(false); /* undispatched claim expires safely */ }
        } catch (Exception e) { active.set(false); log.warn("Repair queue poll failed; will retry", e); }
    }
    void run(ControlPlaneRepository.OperationRecord op) {
        AtomicBoolean lost = new AtomicBoolean();
        ScheduledFuture<?> heartbeat = null;
        try {
            int seconds = Math.toIntExact(properties.getRepair().getLeaseMinutes() * 60L);
            heartbeat = leaseWorker.scheduleAtFixedRate(() -> {
                try { if (!controlPlane.renewRepair(op.owner(), seconds)) lost.set(true); }
                catch (Exception e) { lost.set(true); log.warn("Repair lease renewal failed for {}", op.id(), e); }
            }, Math.max(1, seconds / 3), Math.max(1, seconds / 3), TimeUnit.SECONDS);
            Files.createDirectories(Path.of(".local", "operations"));
            for (String raw : properties.getRepair().getNodes().split(",")) {
                String node = raw.trim();
                if (node.isEmpty()) throw new IllegalArgumentException("Empty repair node");
                if (closing.get() || lost.get() || !controlPlane.ownsRepair(op))
                    throw new IllegalStateException("Worker stopped or ownership lost; no further dispatch permitted");
                if (connections != null) {
                    var session = connections.requireSession();
                    long up = session.getMetadata().getNodes().values().stream()
                            .filter(n -> n.getState() == com.datastax.oss.driver.api.core.metadata.NodeState.UP).count();
                    if (up != properties.getCluster().getExpectedNodes()) throw new IllegalStateException("All expected nodes must be UP before dispatch");
                    SchemaValidator.validate(session, op.keyspace(), properties.getDatacenter(), properties.getCassandra().getReplicationFactor());
                }
                // Persist uncertainty before invoking the external command. A crash after this point is not retry-safe.
                controlPlane.markDispatched(op);
                String command = properties.getRepair().getCommand().replace("{node}", node).replace("{keyspace}", op.keyspace());
                executeCommand(command, Path.of(".local", "operations", op.id() + "-" + node + ".log"));
            }
            if (lost.get()) throw new IllegalStateException("Lease renewal failed during execution");
            controlPlane.finishRepair(new ControlPlaneRepository.OperationRecord(op.id(), op.keyspace(), op.owner(), "SUCCEEDED", "All configured repairs completed", Instant.now()));
        } catch (Exception e) {
            try {
                if (controlPlane.requeueUndispatched(op)) return;
                String detail = "Verify remote activity before acknowledgement: " + e;
                if (detail.length() > 3500) detail = detail.substring(0, 3500);
                controlPlane.updateOperation(new ControlPlaneRepository.OperationRecord(op.id(), op.keyspace(), op.owner(), "UNKNOWN", detail, Instant.now()));
            } catch (Exception stateFailure) { log.warn("Cannot record uncertain repair {} (may already be fenced)", op.id(), stateFailure); }
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        } finally { if (heartbeat != null) heartbeat.cancel(false); }
    }
    void executeCommand(String command, Path path) throws Exception {
        Process process = new ProcessBuilder(command.trim().split("\\s+"))
                .redirectErrorStream(true).redirectOutput(path.toFile()).start();
        try {
            if (!process.waitFor(15, TimeUnit.MINUTES)) throw new TimeoutException("CLI timeout; remote repair may continue");
            if (process.exitValue() != 0) throw new IllegalStateException("Repair CLI failed; inspect " + path);
        } finally { if (process.isAlive()) process.destroyForcibly(); }
    }
    static Operation toOperation(ControlPlaneRepository.OperationRecord op) {
        return new Operation(op.id(), op.keyspace(), op.state(), op.detail(), op.updated());
    }
    public Operation get(UUID id) {
        var op = controlPlane.getOperation(id);
        if (op == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown operation");
        return toOperation(op);
    }
    public List<Operation> list(boolean activeOnly, int limit) { return controlPlane.listOperations(activeOnly, limit).stream().map(RepairService::toOperation).toList(); }
    public Operation acknowledge(UUID id) { controlPlane.reconcileExpiredRepair(); controlPlane.acknowledgeUnknownRepair(id); return get(id); }
    @PreDestroy void close() {
        closing.set(true); poller.shutdownNow(); worker.shutdown();
        try { if (!worker.awaitTermination(5, TimeUnit.SECONDS)) { worker.shutdownNow(); worker.awaitTermination(5, TimeUnit.SECONDS); } }
        catch (InterruptedException e) { worker.shutdownNow(); Thread.currentThread().interrupt(); }
        leaseWorker.shutdownNow();
    }
    public record Operation(UUID id, String keyspace, String state, String detail, Instant updated) {}
}
