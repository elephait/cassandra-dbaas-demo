package demo.platform;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.keyspace.KeyspaceProvisioningService;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.RepairClaimService;
import demo.platform.repair.RepairConfigurationService;
import demo.platform.repair.RepairQueueService;
import demo.platform.repair.RepairState;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@Import({ClusterConfigurationService.class, ClusterRepository.class, RepairQueueService.class, RepairExecutionRepository.class, RepairClaimService.class, RepairConfigurationService.class, KeyspaceProvisioningService.class, ClusterRegistryService.class})
class RepairQueueTest {
    @Test
    void deferredClusterDoesNotBlockHealthyCluster() {
        configurations.save(new ClusterConfiguration(cluster("east"), policy("east")));
        configurations.save(new ClusterConfiguration(cluster("west"), policy("west")));
        queue.accept("east", "team", "down-east");
        var east = claims.claim(60);
        assertTrue(claims.deferUndispatched(east, "Cassandra unavailable"));
        assertNull(claims.claim(60));
        queue.accept("west", "team", "healthy-west");
        var west = claims.claim(60);
        assertEquals("west", west.clusterId());
        assertEquals(RepairState.QUEUED, queue.get(east.id()).state());
        assertEquals(1, jdbc.queryForObject("SELECT retry_count FROM operations WHERE operation_id = ?", Integer.class, east.id().toString()));
    }

    @Test
    void repeatedDispatchRequiresLiveOwnershipAndAcknowledgementIsStable() {
        queue.accept("default", "team", "sequence-fence");
        var operation = claims.claim(60);
        execution.markDispatched(operation);
        execution.markDispatched(operation);
        expire(operation);
        execution.reconcileExpired();
        assertFalse(execution.renew(operation.owner(), 60));
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> execution.markDispatched(operation));
        execution.acknowledge(operation.id());
        execution.acknowledge(operation.id());
        assertEquals(RepairState.ACKNOWLEDGED, queue.get(operation.id()).state());
    }

    @Autowired
    ClusterConfigurationService configurations;
    @Autowired
    RepairQueueService queue;
    @Autowired
    RepairExecutionRepository execution;
    @Autowired
    RepairClaimService claims;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    jakarta.persistence.EntityManager entityManager;

    private static ClusterRecord cluster(String id) {
        return new ClusterRecord(id, List.of(new ContactPoint(id + ".example", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 100, 60, true, 0);
    }

    private static RepairPolicy policy(String id) {
        return new RepairPolicy(id, List.of("node1", "node2", "node3"),
            "docker compose exec -T {node} nodetool repair -pr {keyspace}", 60, 0);
    }

    @Test
    void undispatchedClaimCanBeReclaimedButOldOwnerCannotDispatch() {
        var accepted = queue.accept("default", "team", "queued-request");
        assertNull(accepted.operation().owner());
        var old = claims.claim(60);
        assertEquals(accepted.operation().id(), old.id());
        assertNull(claims.claim(60));
        expire(old);
        assertEquals(1, execution.reconcileExpired());
        var replacement = claims.claim(60);
        assertEquals(old.id(), replacement.id());
        assertNotEquals(old.owner(), replacement.owner());
        assertFalse(execution.renew(old.owner(), 60));
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> execution.markDispatched(old));
        execution.markDispatched(replacement);
        assertEquals(RepairState.RUNNING, queue.get(old.id()).state());
    }

    @Test
    void dispatchedExpiryBlocksQueueUntilIdempotentAcknowledgement() {
        var first = queue.accept("default", "team", "first");
        var claim = claims.claim(60);
        execution.markDispatched(claim);
        queue.accept("default", "other", "second");
        expire(claim);
        execution.reconcileExpired();
        assertEquals(RepairState.UNKNOWN, queue.get(first.operation().id()).state());
        assertNull(claims.claim(60));
        assertFalse(execution.renew(claim.owner(), 60));
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> execution.markUnknown(new OperationRecord(claim.id(), claim.clusterId(), claim.keyspace(),
                claim.owner(), RepairState.UNKNOWN, "stale", Instant.now())));
        execution.acknowledge(claim.id());
        execution.acknowledge(claim.id());
        var next = claims.claim(60);
        assertEquals("other", next.keyspace());
        execution.acknowledge(claim.id());
        assertTrue(execution.owns(next));
    }

    @Test
    void independentClustersCanBeClaimedConcurrently() {
        configurations.save(new ClusterConfiguration(cluster("east"), policy("east")));
        configurations.save(new ClusterConfiguration(cluster("west"), policy("west")));
        queue.accept("east", "team", "east-repair");
        queue.accept("west", "team", "west-repair");

        var first = claims.claim(60);
        var second = claims.claim(60);

        assertNotNull(first);
        assertNotNull(second);
        assertNotEquals(first.clusterId(), second.clusterId());
        assertTrue(execution.owns(first));
        assertTrue(execution.owns(second));
    }

    private void expire(OperationRecord operation) {
        jdbc.update("UPDATE repair_locks SET expires_at = ? WHERE owner_id = ?",
            java.sql.Timestamp.from(Instant.now().minusSeconds(60)), operation.owner());
    }

    @Test
    void queuedWorkThatCanNoLongerBeAdmittedFailsVisiblyInsteadOfStalling() {
        queue.accept("default", "team", "stale-policy");
        // Replace the policy version underneath the accepted operation, as an out-of-band policy
        // rewrite would, and drop the first-level cache so the claim reads it.
        jdbc.update("UPDATE repair_policies SET policy_version = policy_version + 1 WHERE cluster_id = 'default'");
        entityManager.clear();

        assertNull(claims.claim(60));

        var stalled = queue.get(queue.list("default", false, 10).getFirst().id());
        assertEquals(RepairState.FAILED, stalled.state());
        assertTrue(stalled.detail().contains("policy version"));
        assertNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'", String.class));
    }
}
