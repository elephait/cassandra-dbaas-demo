package demo.platform;

import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.common.DatabaseClock;
import demo.platform.keyspace.KeyspaceProvisioningService;
import demo.platform.repair.RepairQueueService;
import demo.platform.repair.persistence.RepairLockJpaRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest(showSql = false)
@Import({ClusterConfigurationService.class, ClusterRegistryService.class, KeyspaceProvisioningService.class,
    RepairQueueService.class, DatabaseClock.class})
class ClusterConfigurationPersistenceTest {
    @Autowired
    ClusterConfigurationService configurations;
    @Autowired
    ClusterRegistryService registry;
    @Autowired
    KeyspaceProvisioningService provisioning;
    @Autowired
    RepairQueueService queue;
    @Autowired
    RepairLockJpaRepository repairLocks;

    private static ClusterRecord cluster(String id, int rf) {
        return new ClusterRecord(id, List.of(new ContactPoint("localhost", 9042)),
            "dc1", 3, rf, List.of(), 10, 60, 5, 60, true, 0);
    }

    @Test
    void repeatingIdenticalConfigurationDoesNotChangeTheVersion() {
        var initial = configurations.save(cluster("repeatable", 3));
        assertEquals(initial.configVersion(), configurations.save(cluster("repeatable", 3)).configVersion());
        assertNotEquals(initial.configVersion(), configurations.save(cluster("repeatable", 1)).configVersion());
    }

    @Test
    void bootstrapReplacesOnlyTheMigrationPlaceholder() {
        assertTrue(configurations.register(cluster("default", 3)), "version 0 is the migration's placeholder");
        var applied = registry.get("default");
        assertEquals(3, applied.replicationFactor());
        assertFalse(configurations.register(cluster("default", 1)), "later starts keep operator changes");
        assertEquals(applied, registry.get("default"));
    }

    @Test
    void repairReservationRowIsCreatedAtFirstAdmissionNotWithTheCluster() {
        configurations.save(cluster("lazy_lock", 3));
        assertFalse(repairLocks.existsById("lazy_lock"));
        queue.accept("lazy_lock", "database", "first-repair");
        assertTrue(repairLocks.existsById("lazy_lock"));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void pendingProvisioningBlocksChangesUntilItCompletes() {
        configurations.save(cluster("domain_reads", 3));
        assertEquals("localhost", registry.get("domain_reads").contactPoints().getFirst().host());
        var ownership = provisioning.begin("domain_reads", "database", 5, 60);
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> configurations.save(cluster("domain_reads", 1)));
        assertEquals(3, registry.requireEnabled("domain_reads").replicationFactor());
        provisioning.complete("domain_reads", "database", ownership.owner());
        configurations.save(cluster("domain_reads", 1));
        assertEquals(1, registry.requireEnabled("domain_reads").replicationFactor());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void queuedRepairBlocksConfigurationChangesButNotIdenticalReplays() {
        var initial = configurations.save(cluster("pending_repair", 3));
        queue.accept("pending_repair", "database", "pending-operation");
        assertEquals(initial, configurations.save(cluster("pending_repair", 3)));
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> configurations.save(cluster("pending_repair", 1)));
        assertEquals(initial, registry.get("pending_repair"));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void staleAdmissionCannotReserveKeyspaceOrEnqueueRepair() {
        var initial = configurations.save(cluster("stale_admission", 3));
        configurations.save(cluster("stale_admission", 1));
        long oldVersion = initial.configVersion();
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> provisioning.begin("stale_admission", "database", 5, 60, oldVersion));
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> queue.accept("stale_admission", "database", "stale-request", oldVersion));
        assertTrue(provisioning.list("stale_admission", 10).isEmpty());
        assertTrue(queue.list("stale_admission", false, 10).isEmpty());
    }
}
