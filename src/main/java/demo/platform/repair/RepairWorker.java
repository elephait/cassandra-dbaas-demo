package demo.platform.repair;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ApiExceptionHandler;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.keyspace.SchemaValidator;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairPolicyRepository;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class RepairWorker {
    private final ExecutorService workers;
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService leaseWorker = Executors.newScheduledThreadPool(2);
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final int workerThreads;
    private final ClusterRepository clusters;
    private final RepairExecutionRepository execution;
    private final RepairPolicyRepository repairPolicies;
    private final CassandraConnectionManager connections;
    private final RepairCommandRunner commandRunner;

    @Autowired
    public RepairWorker(ApiExceptionHandler.CassandraProperties properties, ClusterRepository clusters,
                         RepairExecutionRepository execution,
                         RepairPolicyRepository repairPolicies, CassandraConnectionManager connections,
                         RepairCommandRunner commandRunner) {
        this.clusters = clusters;
        this.execution = execution;
        this.repairPolicies = repairPolicies;
        this.connections = connections;
        this.commandRunner = commandRunner;
        this.workerThreads = properties.getRepair().getWorkerThreads();
        if (workerThreads < 1 || workerThreads > 64)
            throw new IllegalArgumentException("Repair worker threads must be 1..64");
        this.workers = Executors.newFixedThreadPool(workerThreads);
    }

    @EventListener(ApplicationReadyEvent.class)
    void startPolling() {
        poller.scheduleWithFixedDelay(this::poll, 0, 2, TimeUnit.SECONDS);
    }

    void poll() {
        if (closing.get()) return;
        try {
            execution.reconcileExpired();
            while (!closing.get() && active.get() < workerThreads) {
                OperationRecord operation = execution.claim();
                if (operation == null) return;
                active.incrementAndGet();
                try {
                    workers.submit(() -> {
                        try {
                            run(operation);
                        } finally {
                            active.decrementAndGet();
                        }
                    });
                } catch (RejectedExecutionException exception) {
                    active.decrementAndGet();
                    execution.requeueUndispatched(operation);
                    return;
                }
            }
        } catch (Exception exception) {
            log.warn("Repair queue poll failed; will retry", exception);
        }
    }

    public void run(OperationRecord operation) {
        AtomicBoolean lost = new AtomicBoolean();
        ScheduledFuture<?> heartbeat = null;
        try {
            ClusterRecord cluster = clusters.requireEnabled(operation.clusterId());
            RepairPolicy policy = requirePolicy(operation);
            int leaseSeconds = Math.toIntExact(policy.leaseMinutes() * 60L);
            int heartbeatSeconds = Math.max(1, leaseSeconds / 3);
            heartbeat = leaseWorker.scheduleAtFixedRate(() -> renewLease(operation, leaseSeconds, lost),
                heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
            Files.createDirectories(Path.of(".local", "operations"));
            boolean dispatched = false;
            for (String node : policy.nodes()) {
                // Admission is re-checked before every node, but RUNNING is persisted only once,
                // before the first remote command. Repeating that transition per node would rely on
                // the database reporting a matched row when nothing but time has changed.
                assertDispatchAllowed(operation, lost);
                validateClusterForDispatch(operation, cluster);
                if (!dispatched) {
                    execution.markDispatched(operation);
                    dispatched = true;
                }
                String command = policy.command().replace("{node}", node)
                    .replace("{keyspace}", operation.keyspace());
                commandRunner.execute(command,
                    Path.of(".local", "operations", operation.id() + "-" + node + ".log"));
            }
            if (lost.get()) throw new IllegalStateException("Lease renewal failed during execution");
            execution.finish(new OperationRecord(operation.id(), operation.clusterId(), operation.keyspace(),
                operation.owner(), RepairState.SUCCEEDED, "All configured repairs completed",
                operation.repairPolicyVersion(), Instant.now()));
        } catch (Exception exception) {
            recordUncertainOrRequeue(operation, exception);
            if (exception instanceof InterruptedException) Thread.currentThread().interrupt();
        } finally {
            if (heartbeat != null) heartbeat.cancel(false);
        }
    }

    private void renewLease(OperationRecord operation, int leaseSeconds, AtomicBoolean lost) {
        try {
            if (!execution.renew(operation.owner(), leaseSeconds)) lost.set(true);
        } catch (Exception exception) {
            lost.set(true);
            log.warn("Repair lease renewal failed for {}", operation.id(), exception);
        }
    }

    private void validateClusterForDispatch(OperationRecord operation, ClusterRecord cluster) {
        if (connections == null) return;
        var session = connections.requireSession(cluster);
        long up = session.getMetadata().getNodes().values().stream()
            .filter(node -> node.getState()
                == com.datastax.oss.driver.api.core.metadata.NodeState.UP)
            .filter(node -> cluster.datacenter().equals(node.getDatacenter())).count();
        if (up != cluster.expectedNodes())
            throw new IllegalStateException("All expected nodes must be UP before dispatch");
        SchemaValidator.validate(session, operation.keyspace(), cluster.datacenter(), cluster.replicationFactor());
    }

    private void assertDispatchAllowed(OperationRecord operation, AtomicBoolean lost) {
        if (closing.get() || lost.get() || !execution.owns(operation))
            throw new IllegalStateException("Worker stopped or ownership lost; no further dispatch permitted");
        ClusterRecord current = clusters.get(operation.clusterId());
        if (current == null || !current.enabled())
            throw new IllegalStateException("Cluster was disabled during repair");
    }

    private RepairPolicy requirePolicy(OperationRecord operation) {
        RepairPolicy policy = repairPolicies.get(operation.clusterId());
        if (policy == null || policy.version() != operation.repairPolicyVersion())
            throw new IllegalStateException("Repair policy changed after operation acceptance");
        return policy;
    }

    private void recordUncertainOrRequeue(OperationRecord operation, Exception exception) {
        try {
            if (execution.deferUndispatched(operation, exception.toString())) return;
            String detail = "Verify remote activity before acknowledgement: " + exception;
            if (detail.length() > 3500) detail = detail.substring(0, 3500);
            execution.markUnknown(new OperationRecord(operation.id(), operation.clusterId(), operation.keyspace(),
                operation.owner(), RepairState.UNKNOWN, detail, operation.repairPolicyVersion(), Instant.now()));
        } catch (Exception stateFailure) {
            log.warn("Cannot record uncertain repair {} (may already be fenced)", operation.id(), stateFailure);
        }
    }

    @PreDestroy
    public void close() {
        closing.set(true);
        poller.shutdownNow();
        workers.shutdown();
        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow();
                workers.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException exception) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
        leaseWorker.shutdownNow();
    }
}
