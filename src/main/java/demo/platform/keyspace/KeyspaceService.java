package demo.platform.keyspace;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.keyspace.KeyspaceRecord.ProvisionDecision;
import demo.platform.keyspace.persistence.KeyspaceRepository;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
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
public class KeyspaceService {
    private final CassandraConnectionManager connections;
    private final ClusterRepository clusters;
    private final KeyspaceRepository provisioning;
    private final ScheduledExecutorService leaseWorker = Executors.newScheduledThreadPool(2);

    public Map<String, Object> provision(String clusterId, String keyspace) {
        ClusterRecord cluster = clusters.requireEnabled(clusterId);
        KeyspaceNames.validate(keyspace);
        CqlSession session = connections.requireSession(cluster);
        ProvisionDecision decision = provisioning.begin(clusterId, keyspace,
            cluster.maxKeyspaces(), cluster.provisioningLeaseSeconds(), cluster.configVersion());
        if (decision.alreadyReady()) {
            SchemaValidator.validate(session, keyspace, cluster.datacenter(), cluster.replicationFactor());
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
        ClusterRecord cluster = clusters.requireEnabled(clusterId);
        KeyspaceNames.validate(keyspace);
        if (value == null || value.length() > 1024)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                "Value must contain at most 1024 characters");
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

    private void provisionOwned(ClusterRecord cluster, String keyspace,
                                String owner, CqlSession session) {
        AtomicBoolean lost = new AtomicBoolean();
        int heartbeatSeconds = Math.max(1, cluster.provisioningLeaseSeconds() / 3);
        ScheduledFuture<?> heartbeat = null;
        try {
            heartbeat = leaseWorker.scheduleAtFixedRate(() -> renewLease(cluster, keyspace, owner, lost),
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
        } catch (RuntimeException exception) {
            provisioning.release(cluster.id(), keyspace, owner);
            throw exception;
        } finally {
            if (heartbeat != null) heartbeat.cancel(false);
        }
    }

    private void assertOwner(ClusterRecord cluster, String keyspace, String owner, AtomicBoolean lost) {
        if (lost.get() || !provisioning.owns(cluster.id(), keyspace, owner))
            throw new ControlPlaneExceptions.Conflict("Provisioning ownership was lost before DDL");
    }

    private void renewLease(ClusterRecord cluster, String keyspace, String owner, AtomicBoolean lost) {
        try {
            if (!provisioning.renew(cluster.id(), keyspace, owner,
                cluster.provisioningLeaseSeconds())) lost.set(true);
        } catch (RuntimeException exception) {
            lost.set(true);
        }
    }

    @PreDestroy
    public void close() {
        leaseWorker.shutdownNow();
    }
}
