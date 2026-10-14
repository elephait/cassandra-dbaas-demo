package demo.platform;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.RepairState;
import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairPolicyRepository;
import demo.platform.repair.persistence.RepairQueueRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@Import({ClusterRepository.class,     RepairQueueRepository.class, RepairExecutionRepository.class})
class RepairQueueTest {
    @Test
    void deferredClusterDoesNotBlockHealthyCluster() {
        clusters.save(cluster("east"));
        clusters.save(cluster("west"));
        policies.save(policy("east"));
        policies.save(policy("west"));
        queue.accept("east", "team", "down-east");
        var east = execution.claim(60);
        assertTrue(execution.deferUndispatched(east, "Cassandra unavailable"));
        assertNull(execution.claim(60));
        queue.accept("west", "team", "healthy-west");
        var west = execution.claim(60);
        assertEquals("west", west.clusterId());
        assertEquals(RepairState.QUEUED, queue.get(east.id()).state());
        assertEquals(1, jdbc.queryForObject("SELECT retry_count FROM operations WHERE operation_id = ?", Integer.class, east.id().toString()));
    }

    @Test
    void repeatedDispatchRequiresLiveOwnershipAndAcknowledgementIsStable() {
        queue.accept("default", "team", "sequence-fence");
        var operation = execution.claim(60);
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
    ClusterRepository clusters;
    @Autowired
    RepairQueueRepository queue;
    @Autowired
    RepairExecutionRepository execution;
    @Autowired
    RepairPolicyRepository policies;
    @Autowired
    JdbcTemplate jdbc;

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
        var old = execution.claim(60);
        assertEquals(accepted.operation().id(), old.id());
        assertNull(execution.claim(60));
        expire(old);
        assertEquals(1, execution.reconcileExpired());
        var replacement = execution.claim(60);
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
        var claim = execution.claim(60);
        execution.markDispatched(claim);
        queue.accept("default", "other", "second");
        expire(claim);
        execution.reconcileExpired();
        assertEquals(RepairState.UNKNOWN, queue.get(first.operation().id()).state());
        assertNull(execution.claim(60));
        assertFalse(execution.renew(claim.owner(), 60));
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> execution.markUnknown(new OperationRecord(claim.id(), claim.clusterId(), claim.keyspace(),
                claim.owner(), RepairState.UNKNOWN, "stale", Instant.now())));
        execution.acknowledge(claim.id());
        execution.acknowledge(claim.id());
        var next = execution.claim(60);
        assertEquals("other", next.keyspace());
        execution.acknowledge(claim.id());
        assertTrue(execution.owns(next));
    }

    @Test
    void independentClustersCanBeClaimedConcurrently() {
        clusters.save(cluster("east"));
        clusters.save(cluster("west"));
        policies.save(policy("east"));
        policies.save(policy("west"));
        queue.accept("east", "team", "east-repair");
        queue.accept("west", "team", "west-repair");

        var first = execution.claim(60);
        var second = execution.claim(60);

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
}
