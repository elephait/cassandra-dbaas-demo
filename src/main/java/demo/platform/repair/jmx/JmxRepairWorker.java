package demo.platform.repair.jmx;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.SchemaValidator;
import demo.platform.misc.ApiExceptionHandler.CassandraProperties;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.repair.*;
import demo.platform.repair.jmx.JmxRepairRepository.Task;
import demo.platform.repair.persistence.RepairExecutionRepository;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@ConditionalOnProperty(name = "demo.repair.transport", havingValue = "jmx", matchIfMissing = true)
public class JmxRepairWorker {
    private final RepairClaimService claims;
    private final RepairExecutionRepository execution;
    private final RepairConfigurationService policies;
    private final JmxRepairRepository progress;
    private final JmxRepairProperties settings;
    private final RepairManagementClient client;
    private final ClusterRegistryService clusters;
    private final CassandraConnectionManager connections;
    private final ScheduledExecutorService poller;
    private final ExecutorService workers;
    private final AtomicInteger active = new AtomicInteger();
    private final int capacity;
    private volatile boolean closing;

    public JmxRepairWorker(CassandraProperties properties, RepairClaimService claims,
            RepairExecutionRepository execution, RepairConfigurationService policies,
            JmxRepairRepository progress, JmxRepairProperties settings, RepairManagementClient client,
            ClusterRegistryService clusters, CassandraConnectionManager connections) {
        this.claims = claims;
        this.execution = execution;
        this.policies = policies;
        this.progress = progress;
        this.settings = settings;
        this.client = client;
        this.clusters = clusters;
        this.connections = connections;
        settings.validateTiming();
        capacity = properties.getRepair().getWorkerThreads();
        if (capacity < 1 || capacity > 64) throw new IllegalArgumentException("Repair worker threads must be 1..64");
        boolean enabled = properties.getRepair().isWorkerEnabled();
        workers = enabled ? Executors.newFixedThreadPool(capacity) : null;
        poller = enabled ? Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "jmx-repair-poller"); t.setDaemon(true); return t;
        }) : null;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (poller != null) poller.scheduleWithFixedDelay(this::poll, 0, 1, TimeUnit.SECONDS);
    }

    public void poll() {
        if (workers == null || closing) return;
        try {
            // Legacy dispatched work remains UNKNOWN on expiry. Persisted JMX runs are excluded
            // from that lease reconciliation and can be polled by any replacement worker.
            execution.reconcileExpired();
            while (!closing && active.get() < capacity) {
                Task task = progress.claim(settings.getActionLeaseSeconds());
                if (task == null) {
                    OperationRecord operation = claims.claim();
                    if (operation == null) return;
                    initialize(operation);
                    continue;
                }
                active.incrementAndGet();
                try {
                    workers.submit(() -> {
                        try { step(task); }
                        finally { active.decrementAndGet(); }
                    });
                } catch (RejectedExecutionException e) {
                    active.decrementAndGet();
                    // No remote request was made. The short claim can expire safely.
                    return;
                }
            }
        } catch (Exception e) { log.warn("JMX repair poll failed; durable work will be revisited", e); }
    }

    private void initialize(OperationRecord operation) {
        try {
            var policy = policies.getPolicy(operation.clusterId());
            if (policy == null || policy.version() != operation.repairPolicyVersion())
                throw new IllegalStateException("Accepted repair policy changed");
            List<JmxRepairRepository.Target> targets = policy.nodes().stream().map(node -> {
                var endpoint = settings.require(operation.clusterId(), node);
                return new JmxRepairRepository.Target(node, endpoint.getServiceUrl(), endpoint.getHostId());
            }).toList();
            validateTargets(operation.clusterId(), operation.keyspace(), targets.stream().map(JmxRepairRepository.Target::hostId).toList());
            progress.initialize(operation, policy, targets);
        } catch (Exception e) {
            claims.deferUndispatched(operation, "JMX plan not dispatched: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void validateTargets(String clusterId, String keyspace, List<String> hostIds) {
        var cluster = clusters.requireEnabled(clusterId);
        var session = connections.requireSession(cluster);
        var local = session.getMetadata().getNodes().values().stream()
            .filter(n -> cluster.datacenter().equals(n.getDatacenter())).toList();
        var liveIds = local.stream().filter(n -> n.getState() == com.datastax.oss.driver.api.core.metadata.NodeState.UP)
            .map(n -> n.getHostId().toString()).collect(java.util.stream.Collectors.toSet());
        if (liveIds.size() != cluster.expectedNodes() || hostIds.size() != liveIds.size()
                || !new HashSet<>(hostIds).equals(liveIds))
            throw new IllegalStateException("JMX host IDs must cover every expected UP node in the configured datacenter");
        SchemaValidator.validate(session, keyspace, cluster.datacenter(), cluster.replicationFactor());
    }

    public void step(Task task) {
        boolean dispatchIntent = false;
        try {
            if ("SUBMITTING".equals(task.state())) {
                progress.unknown(task, "Worker disappeared during JMX submission; do not resubmit without remote verification");
                return;
            }
            if (Instant.now().isAfter(task.startedAt().plusSeconds(settings.getObservationDeadlineSeconds()))) {
                progress.unknown(task, "JMX observation deadline exceeded; repair may still be running");
                return;
            }
            var endpoint = settings.require(task.clusterId(), task.node());
            if (!endpoint.getServiceUrl().equals(task.serviceUrl()) || !endpoint.getHostId().equals(task.hostId())) {
                progress.unknown(task, "JMX endpoint identity configuration changed; inspect the original execution");
                return;
            }
            // Connect and identity checks happen BEFORE recording submission intent. Connection
            // failures here are safe to retry; exceptions from submit itself are ambiguous.
            try (var remote = client.connect(endpoint)) {
                var identity = remote.identity();
                if (!task.hostId().equals(identity.hostId())) {
                    progress.unknown(task, "JMX endpoint host ID does not match the accepted plan"); return;
                }
                if ("PENDING".equals(task.state())) {
                    var targets = policies.getPolicy(task.clusterId()).nodes().stream()
                        .map(n -> settings.require(task.clusterId(), n).getHostId()).toList();
                    validateTargets(task.clusterId(), task.keyspace(), targets);
                    if (closing || !progress.submitting(task, identity.processStartedAt())) return;
                    dispatchIntent = true;
                    int command = remote.submit(task.keyspace());
                    if (command < 0) throw new IllegalStateException("Invalid repair command number");
                    if (!identity.equals(remote.identity())) throw new IllegalStateException("Cassandra restarted during submission");
                    progress.submitted(task, command, settings.getPollSeconds());
                } else {
                    if (task.processStartedAt() == null || task.commandId() == null
                            || task.processStartedAt() != identity.processStartedAt()) {
                        progress.unknown(task, "Cassandra process changed; command number cannot identify the old repair"); return;
                    }
                    var observation = remote.status(task.commandId());
                    if (!identity.equals(remote.identity())) {
                        progress.unknown(task, "Cassandra restarted while observing repair"); return;
                    }
                    switch (observation.status()) {
                        case COMPLETED -> progress.complete(task, observation.detail());
                        case FAILED -> progress.unknown(task, "Cassandra reported FAILED; verify remote cleanup before retry: " + observation.detail());
                        case UNRECOGNIZED -> progress.unknown(task, "Unsupported Cassandra repair status; check version/API compatibility: " + observation.detail());
                        case IN_PROGRESS, MISSING -> progress.defer(task, observation.detail(), settings.getPollSeconds());
                    }
                }
            }
        } catch (Exception e) {
            try {
                // Do not include connection exceptions in API receipts: they can contain service
                // URLs or deployment details. Detailed exception stays in server logs.
                if (dispatchIntent) progress.unknown(task, "JMX submission outcome uncertain; verify remote activity before acknowledgement");
                else progress.defer(task, "JMX observation/readiness unavailable; no new repair submitted", settings.getPollSeconds());
            } catch (Exception lost) { log.warn("Could not record JMX action outcome for {}/{}", task.operationId(), task.ordinal(), lost); }
            log.warn("JMX repair action failed for {}/{}", task.operationId(), task.ordinal(), e);
        }
    }

    @PreDestroy
    public void close() {
        closing = true;
        if (poller != null) poller.shutdownNow();
        if (workers != null) workers.shutdownNow();
        // Interruption does not claim to cancel a Cassandra repair. Durable node state survives.
    }
}
