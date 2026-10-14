package demo.platform;

import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.keyspace.persistence.KeyspaceRepository;
import demo.platform.repair.OperationRecord;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest(properties = "spring.datasource.url=jdbc:h2:mem:control-plane-test;DB_CLOSE_DELAY=-1")
@Import({ClusterRepository.class, KeyspaceRepository.class,     RepairQueueRepository.class, RepairExecutionRepository.class})
class ControlPlaneRepositoryTest {
    @Test
    void expiredProvisioningOwnerCannotPublishReady() {
        var first = provisioning.begin("default", "expired", 5, 60);
        jdbc.update("UPDATE keyspaces SET expires_at = ? WHERE cluster_id = 'default' AND keyspace_name = 'expired'", java.sql.Timestamp.from(Instant.now().minusSeconds(60)));
        entityManager.clear();
        assertFalse(provisioning.renew("default", "expired", first.owner(), 60));
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> provisioning.complete("default", "expired", first.owner()));
        var second = provisioning.begin("default", "expired", 5, 60);
        assertNotEquals(first.owner(), second.owner());
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> provisioning.complete("default", "expired", first.owner()));
        provisioning.complete("default", "expired", second.owner());
    }

    @Autowired
    KeyspaceRepository provisioning;
    @Autowired
    RepairQueueRepository queue;
    @Autowired
    RepairExecutionRepository execution;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    jakarta.persistence.EntityManager entityManager;

    private static OperationRecord terminal(OperationRecord operation, RepairState state, String detail) {
        return new OperationRecord(operation.id(), operation.clusterId(), operation.keyspace(), operation.owner(),
            state, detail, operation.repairPolicyVersion(), Instant.now());
    }

    @Test
    void reservesKeyspaceQuotaAndMakesProvisioningIdempotent() {
        var first = provisioning.begin("default", "team", 1, 60);
        assertFalse(first.alreadyReady());
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> provisioning.begin("default", "team", 1, 60));
        provisioning.complete("default", "team", first.owner());
        assertTrue(provisioning.begin("default", "team", 1, 60).alreadyReady());
        assertThrows(ControlPlaneExceptions.KeyspaceQuotaExceeded.class,
            () -> provisioning.begin("default", "other", 1, 60));
    }

    @Test
    void resumesProvisioningAfterLeaseExpiry() {
        provisioning.begin("default", "stale", 2, 60);
        jdbc.update("UPDATE keyspaces SET expires_at = ? WHERE cluster_id = 'default' AND keyspace_name = 'stale'",
            java.sql.Timestamp.from(Instant.now().minusSeconds(1)));
        entityManager.clear();
        var resumed = provisioning.begin("default", "stale", 2, 60);
        assertFalse(resumed.alreadyReady());
        provisioning.complete("default", "stale", resumed.owner());
    }

    @Test
    void listsKeyspacesAndActiveOperations() {
        var provision = provisioning.begin("default", "listed", 2, 60);
        provisioning.complete("default", "listed", provision.owner());
        var completed = claimedRepair("listed", Instant.now().plusSeconds(60));
        execution.finish(terminal(completed, RepairState.SUCCEEDED, "done"));
        var active = claimedRepair("listed", Instant.now().plusSeconds(60));

        assertEquals("listed", provisioning.list("default", 10).getFirst().keyspace());
        assertEquals(active.id(), queue.list("default", true, 10).getFirst().id());
        assertEquals(2, queue.list("default", false, 10).size());
    }

    @Test
    void persistsOperationAndPreventsConcurrentRepairLease() {
        var operation = claimedRepair("team", Instant.now().plusSeconds(60));
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> claimedRepair("other", Instant.now().plusSeconds(60)));
        execution.finish(terminal(operation, RepairState.SUCCEEDED, "done"));
        assertEquals(RepairState.SUCCEEDED, queue.get(operation.id()).state());
        assertNotEquals(operation.id(), claimedRepair("other", Instant.now().plusSeconds(60)).id());
    }

    @Test
    void requiresAcknowledgementAfterRepairLeaseExpires() {
        var operation = claimedRepair("team", Instant.now().minusSeconds(1));
        assertEquals(1, execution.reconcileExpired());
        assertEquals(RepairState.UNKNOWN, queue.get(operation.id()).state());
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> claimedRepair("other", Instant.now().plusSeconds(60)));
        execution.acknowledge(operation.id());
        assertNotNull(claimedRepair("other", Instant.now().plusSeconds(60)));
    }

    private OperationRecord claimedRepair(String keyspace, Instant expiry) {
        Integer occupied = jdbc.queryForObject("SELECT COUNT(*) FROM repair_locks "
            + "WHERE cluster_id = 'default' AND owner_id IS NOT NULL", Integer.class);
        if (occupied != null && occupied > 0) throw new ControlPlaneExceptions.Conflict("Repair already running");
        queue.accept("default", keyspace, UUID.randomUUID().toString());
        var operation = execution.claim(60);
        execution.markDispatched(operation);
        jdbc.update("UPDATE repair_locks SET expires_at = ? WHERE owner_id = ?",
            java.sql.Timestamp.from(expiry), operation.owner());
        return queue.get(operation.id());
    }
}
