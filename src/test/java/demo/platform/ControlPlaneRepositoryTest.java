package demo.platform;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.KeyspaceProvisioningService;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.common.DatabaseClock;
import demo.platform.repair.*;
import demo.platform.repair.jmx.JmxRepairProperties;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest(properties = "spring.datasource.url=jdbc:h2:mem:control-plane-test;DB_CLOSE_DELAY=-1")
@Import({KeyspaceProvisioningService.class, RepairQueueService.class, RepairExecutionRepository.class, RepairClaimService.class, ClusterRegistryService.class,
    DatabaseClock.class, JmxRepairProperties.class})
class ControlPlaneRepositoryTest {
    @Autowired
    KeyspaceProvisioningService provisioning;
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
        startedRepair("listed");
        RepairRuns.succeed(execution);
        var active = startedRepair("listed");

        assertEquals("listed", provisioning.list("default", 10).getFirst().keyspace());
        assertEquals(active.id(), queue.list("default", true, 10).getFirst().id());
        assertEquals(2, queue.list("default", false, 10).size());
    }

    @Test
    void oneActiveRepairPerClusterAndCompletionAdmitsTheNext() {
        var operation = startedRepair("team");
        assertNull(startedRepair("other"), "the cluster reservation is held while a repair runs");
        RepairRuns.succeed(execution);
        assertEquals(RepairState.SUCCEEDED, queue.get(operation.id()).state());
        var next = RepairRuns.start(claims, execution);
        assertNotNull(next);
        assertNotEquals(operation.id(), next.id());
    }

    @Test
    void uncertainOutcomeRequiresAcknowledgementBeforeTheNextRepair() {
        var operation = startedRepair("team");
        RepairRuns.becomeUnknown(execution, "Worker lease expired during submission");
        assertEquals(RepairState.UNKNOWN, queue.get(operation.id()).state());
        assertNull(startedRepair("other"));
        execution.acknowledge(operation.id());
        assertNotNull(RepairRuns.start(claims, execution));
    }

    private OperationRecord startedRepair(String keyspace) {
        queue.accept("default", keyspace, UUID.randomUUID().toString());
        return RepairRuns.start(claims, execution);
    }
}
