package demo.platform.repair;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.SchemaValidator;
import demo.platform.misc.ApiExceptionHandler;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.persistence.RepairExecutionRepository;
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
    private final ScheduledExecutorService poller;
    private final ScheduledExecutorService leaseWorker;
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final boolean enabled;
    private final int workerThreads;
    private final int pollIntervalSeconds;
    private final ClusterRegistryService clusters;
    private final RepairExecutionRepository execution;
    private final RepairClaimService claims;
    private final RepairConfigurationService repairConfiguration;
    private final CassandraConnectionManager connections;
    private final RepairCommandRunner commandRunner;

    @Autowired
    public RepairWorker(ApiExceptionHandler.CassandraProperties properties, ClusterRegistryService clusters,
                         RepairExecutionRepository execution, RepairClaimService claims,
                         RepairConfigurationService repairConfiguration, CassandraConnectionManager connections,
                         RepairCommandRunner commandRunner) {
        this.clusters = clusters;
        this.execution = execution;
        this.claims = claims;
        this.repairConfiguration = repairConfiguration;
        this.connections = connections;
        this.commandRunner = commandRunner;
        this.workerThreads = properties.getRepair().getWorkerThreads();
        this.pollIntervalSeconds = properties.getPollIntervalSeconds();
        if (workerThreads < 1 || workerThreads > 64)
            throw new IllegalArgumentException("Repair worker threads must be 1..64");
        this.enabled = properties.getRepair().isWorkerEnabled();
        // An instance with the worker disabled must not hold executor threads at all: it serves API
        // traffic while a separate worker tier owns dispatch. All coordination is in SQL, so adding
        // or losing a worker instance only changes throughput, never correctness.
        this.workers = enabled ? Executors.newFixedThreadPool(workerThreads) : null;
        this.poller = enabled
            ? Executors.newSingleThreadScheduledExecutor(task -> daemon(task, "repair-poller")) : null;
        this.leaseWorker = enabled
            ? Executors.newScheduledThreadPool(2, task -> daemon(task, "repair-lease")) : null;
    }

    private static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    @EventListener(ApplicationReadyEvent.class)
    void startPolling() {
        if (!enabled) {
            log.info("Repair worker is disabled on this instance; it serves API traffic only");
            return;
        }
        poller.scheduleWithFixedDelay(this::poll, 0, pollIntervalSeconds, TimeUnit.SECONDS);
    }

    void poll() {
        // The guard belongs here, not only in startPolling: this is the entry point for the scheduled
        // task as well as for direct invocation.
        if (!enabled || closing.get()) return;
        try {
            execution.reconcileExpired();
            while (!closing.get() && active.get() < workerThreads) {
                OperationRecord operation = claims.claim();
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
            ClusterRecord.Settings cluster = clusters.requireEnabled(operation.clusterId());
            RepairPolicy policy = requirePolicy(operation);
            int leaseSeconds = Math.toIntExact(policy.leaseMinutes() * 60L);
            int heartbeatSeconds = Math.max(1, leaseSeconds / 3);
            // Fixed delay, not fixed rate: a slow renewal must not be chased by an immediate retry.
            heartbeat = leaseWorker.scheduleWithFixedDelay(() -> renewLease(operation, leaseSeconds, lost),
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

    private void validateClusterForDispatch(OperationRecord operation, ClusterRecord.Settings cluster) {
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
        clusters.requireEnabled(operation.clusterId());
    }

    private RepairPolicy requirePolicy(OperationRecord operation) {
        RepairPolicy policy = repairConfiguration.getPolicy(operation.clusterId());
        if (policy == null || policy.version() != operation.repairPolicyVersion())
            throw new IllegalStateException("Repair policy changed after operation acceptance");
        return policy;
    }

    private void recordUncertainOrRequeue(OperationRecord operation, Exception exception) {
        try {
            if (claims.deferUndispatched(operation, exception.toString())) return;
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
        if (!enabled) return;
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
