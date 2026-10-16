package demo.platform.keyspace;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.keyspace.KeyspaceRecord.ProvisionDecision;
import demo.platform.keyspace.cassandra.KeyspaceSchemaRepository;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Keyspace use cases. Orchestrates the provisioning lease (control-plane DB) around idempotent DDL
 * (Cassandra); it issues no statements itself.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KeyspaceService {
    private static final int MAX_PROBE_VALUE_LENGTH = 1024;

    private final ClusterRegistryService clusters;
    private final KeyspaceProvisioningService provisioning;
    private final KeyspaceSchemaRepository schema;
    private final SchemaValidator validator;
    // Renews provisioning leases while DDL runs; daemon so it never blocks shutdown.
    private final ScheduledExecutorService leaseWorker = Executors.newScheduledThreadPool(2, task -> {
        Thread thread = new Thread(task, "provisioning-lease");
        thread.setDaemon(true);
        return thread;
    });

    public ProvisionResult provision(String clusterId, String keyspace) {
        KeyspaceNames.validate(keyspace);
        ClusterRecord.Settings cluster = clusters.requireEnabled(clusterId);
        schema.requireAvailable(cluster);
        ProvisionDecision decision = provisioning.begin(clusterId, keyspace,
            cluster.maxKeyspaces(), cluster.provisioningLeaseSeconds(), cluster.configVersion());
        if (decision.alreadyReady()) validator.validate(cluster, keyspace);
        else provisionOwned(cluster, keyspace, decision.owner());
        return new ProvisionResult(clusterId, keyspace, cluster.replicationFactor(), cluster.maxKeyspaces());
    }

    public List<KeyspaceRecord> list(String clusterId, int limit) {
        clusters.requireEnabled(clusterId);
        return provisioning.list(clusterId, limit);
    }

    /** Real LOCAL_QUORUM write and read-back: proves request availability, unlike driver health. */
    public ProbeResult probe(String clusterId, String keyspace, String value) {
        KeyspaceNames.validate(keyspace);
        if (value == null || value.length() > MAX_PROBE_VALUE_LENGTH)
            throw new IllegalArgumentException("Value is required and must contain at most 1024 characters");
        ClusterRecord.Settings cluster = clusters.requireEnabled(clusterId);
        UUID id = UUID.randomUUID();
        schema.writeProbe(cluster, keyspace, id, value);
        String readBack = schema.readProbe(cluster, keyspace, id).orElse(null);
        if (!value.equals(readBack))
            throw new ControlPlaneExceptions.Unavailable("LOCAL_QUORUM read-back did not return the written value");
        return new ProbeResult(clusterId, id, readBack, "LOCAL_QUORUM");
    }

    private void provisionOwned(ClusterRecord.Settings cluster, String keyspace, String owner) {
        AtomicBoolean lost = new AtomicBoolean();
        int heartbeatSeconds = Math.max(1, cluster.provisioningLeaseSeconds() / 3);
        ScheduledFuture<?> heartbeat = null;
        try {
            heartbeat = leaseWorker.scheduleWithFixedDelay(() -> renewLease(cluster, keyspace, owner, lost),
                heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
            validator.validateIfPresent(cluster, keyspace);
            assertOwner(cluster, keyspace, owner, lost);
            schema.createKeyspace(cluster, keyspace);
            assertOwner(cluster, keyspace, owner, lost);
            schema.createProbeTable(cluster, keyspace);
            validator.validate(cluster, keyspace);
            if (lost.get()) throw new ControlPlaneExceptions.Conflict("Provisioning ownership was lost");
            provisioning.complete(cluster.id(), keyspace, owner);
            log.info("Provisioning completed cluster={} keyspace={} owner={}", cluster.id(), keyspace, owner);
        } catch (RuntimeException exception) {
            try {
                provisioning.release(cluster.id(), keyspace, owner);
            } catch (RuntimeException cleanupFailure) {
                exception.addSuppressed(cleanupFailure);
                log.warn("Cannot release provisioning lease; expiry will recover it cluster={} keyspace={} owner={}",
                    cluster.id(), keyspace, owner, cleanupFailure);
            }
            log.warn("Provisioning failed cluster={} keyspace={} owner={}", cluster.id(), keyspace, owner, exception);
            throw exception;
        } finally {
            if (heartbeat != null) heartbeat.cancel(false);
        }
    }

    private void assertOwner(ClusterRecord.Settings cluster, String keyspace, String owner, AtomicBoolean lost) {
        if (lost.get() || !provisioning.owns(cluster.id(), keyspace, owner))
            throw new ControlPlaneExceptions.Conflict("Provisioning ownership was lost before DDL");
    }

    private void renewLease(ClusterRecord.Settings cluster, String keyspace, String owner, AtomicBoolean lost) {
        if (lost.get()) return;
        try {
            if (!provisioning.renew(cluster.id(), keyspace, owner, cluster.provisioningLeaseSeconds())
                && lost.compareAndSet(false, true))
                log.warn("Provisioning lease lost; stopping DDL cluster={} keyspace={} owner={}",
                    cluster.id(), keyspace, owner);
        } catch (RuntimeException exception) {
            if (lost.compareAndSet(false, true))
                log.warn("Cannot confirm provisioning lease; stopping DDL cluster={} keyspace={} owner={}",
                    cluster.id(), keyspace, owner, exception);
        }
    }

    @PreDestroy
    public void close() {
        leaseWorker.shutdownNow();
    }

    public record ProvisionResult(String clusterId, String keyspace, int replicationFactor, int maxKeyspaces) {
    }

    public record ProbeResult(String clusterId, UUID id, String value, String consistency) {
    }
}
