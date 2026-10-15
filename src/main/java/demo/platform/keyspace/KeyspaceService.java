package demo.platform.keyspace;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.KeyspaceRecord.ProvisionDecision;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.misc.ControlPlaneExceptions;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@RequiredArgsConstructor
@Slf4j
public class KeyspaceService {
    private final CassandraConnectionManager connections;
    private final ClusterRegistryService clusters;
    private final KeyspaceProvisioningService provisioning;
    private final ScheduledExecutorService leaseWorker = Executors.newScheduledThreadPool(2);

    public Map<String, Object> provision(String clusterId, String keyspace) {
        // Reject an unusable identifier before touching the registry or the Cassandra session.
        KeyspaceNames.validate(keyspace);
        ClusterRecord.Settings cluster = clusters.requireEnabled(clusterId);
        CqlSession session = connections.requireSession(cluster);
        ProvisionDecision decision = provisioning.begin(clusterId, keyspace,
            cluster.maxKeyspaces(), cluster.provisioningLeaseSeconds(), cluster.configVersion());
        if (decision.alreadyReady()) {
            SchemaValidator.validate(session, keyspace, cluster.datacenter(), cluster.replicationFactor());
            log.debug("Revalidated ready keyspace cluster={} keyspace={}", clusterId, keyspace);
        } else {
            provisionOwned(cluster, keyspace, decision.owner(), session);
        }
        return Map.of("clusterId", clusterId, "keyspace", keyspace,
            "replicationFactor", cluster.replicationFactor(), "maxKeyspaces", cluster.maxKeyspaces());
    }

    public List<KeyspaceRecord> list(String clusterId, int limit) {
        clusters.requireEnabled(clusterId);
        return provisioning.list(clusterId, limit);
    }

    public Map<String, Object> probe(String clusterId, String keyspace, String value) {
        KeyspaceNames.validate(keyspace);
        if (value == null || value.length() > 1024)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                "Value must contain at most 1024 characters");
        ClusterRecord.Settings cluster = clusters.requireEnabled(clusterId);
        CqlSession session = connections.requireSession(cluster);
        UUID row = UUID.randomUUID();
        session.execute(SimpleStatement.newInstance(
                "INSERT INTO " + keyspace + ".probe (id,value) VALUES (?,?) USING TTL 3600", row, value)
            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
        var result = session.execute(SimpleStatement.newInstance(
                "SELECT value FROM " + keyspace + ".probe WHERE id=?", row)
            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).one();
        if (result == null || !value.equals(result.getString(0)))
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Read-back mismatch");
        return Map.of("clusterId", clusterId, "id", row, "value", result.getString(0),
            "consistency", "LOCAL_QUORUM");
    }

    private void provisionOwned(ClusterRecord.Settings cluster, String keyspace,
                                String owner, CqlSession session) {
        AtomicBoolean lost = new AtomicBoolean();
        int heartbeatSeconds = Math.max(1, cluster.provisioningLeaseSeconds() / 3);
        ScheduledFuture<?> heartbeat = null;
        try {
            log.debug("Provisioning started cluster={} keyspace={} owner={}", cluster.id(), keyspace, owner);
            heartbeat = leaseWorker.scheduleWithFixedDelay(() -> renewLease(cluster, keyspace, owner, lost),
                heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
            SchemaValidator.validateKeyspace(session, keyspace, cluster.datacenter(),
                cluster.replicationFactor(), true);
            assertOwner(cluster, keyspace, owner, lost);
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                + " WITH replication = {'class':'NetworkTopologyStrategy','" + cluster.datacenter().replace("'", "''")
                + "':" + cluster.replicationFactor() + "}");
            assertOwner(cluster, keyspace, owner, lost);
            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace
                + ".probe (id uuid PRIMARY KEY, value text)");
            SchemaValidator.validate(session, keyspace, cluster.datacenter(), cluster.replicationFactor());
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
            if (!provisioning.renew(cluster.id(), keyspace, owner,
                cluster.provisioningLeaseSeconds()) && lost.compareAndSet(false, true))
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
}
