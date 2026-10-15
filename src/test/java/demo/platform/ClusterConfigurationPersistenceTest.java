package demo.platform;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.repair.RepairConfigurationService;
import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.keyspace.KeyspaceProvisioningService;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.RepairQueueService;
import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairLockJpaRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DataJpaTest(showSql = false)
@Import({ClusterRepository.class, ClusterConfigurationService.class, KeyspaceProvisioningService.class, RepairQueueService.class, RepairExecutionRepository.class, RepairConfigurationService.class, ClusterRegistryService.class})
class ClusterConfigurationPersistenceTest {
    @Test
    void repeatingIdenticalConfigurationDoesNotChangeVersions() {
        var initial = configurations.save(configuration("repeatable", 3, List.of("node1")));
        var replay = configurations.save(configuration("repeatable", 3, List.of("node1")));
        assertEquals(initial.cluster().configVersion(), replay.cluster().configVersion());
        assertEquals(initial.repairPolicy().version(), replay.repairPolicy().version());
    }

    @Autowired
    ClusterConfigurationService configurations;
    @Autowired
    ClusterRepository clusters;
    @Autowired
    ClusterRegistryService registry;
    @Autowired
    KeyspaceProvisioningService provisioning;
    @Autowired
    RepairQueueService queue;
    @Autowired
    RepairLockJpaRepository repairLocks;

    private ClusterConfiguration configuration(String id, int rf, List<String> nodes) {
        var clusterId = id;
        return new ClusterConfiguration(new ClusterRecord(clusterId, List.of(new ContactPoint("localhost", 9042)),
            "dc1", 3, rf, List.of(), 10, 60, 5, 60, true, 0),
            new RepairPolicy(clusterId, nodes, "docker exec {node} nodetool repair -pr {keyspace}", 1, 0));
    }

    @Test
    void firstBootstrapSetsClusterAndPolicyAndSubsequentBootstrapPreservesThem() {
        var saved = configurations.register(configuration("default", 3, List.of("node1", "node2", "node3")));
        assertEquals(3, saved.cluster().replicationFactor());
        assertEquals(List.of("node1", "node2", "node3"), saved.repairPolicy().nodes());
        var repeated = configurations.register(configuration("default", 1, List.of("other")));
        assertEquals(saved.cluster(), repeated.cluster());
        assertEquals(saved.repairPolicy(), repeated.repairPolicy());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void domainReadsWorkOutsideOpenSessionInViewAndMixedJpaJdbcCommitTogether() {
        configurations.save(configuration("domain_reads", 3, List.of("node1")));
        assertEquals("localhost", clusters.get("domain_reads").contactPoints().getFirst().host());
        assertEquals(3, registry.requireEnabled("domain_reads").replicationFactor());
        var ownership = provisioning.begin("domain_reads", "database", 5, 60);
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> configurations.save(configuration("domain_reads", 1, List.of("node2"))));
        assertEquals(3, clusters.get("domain_reads").replicationFactor());
        provisioning.complete("domain_reads", "database", ownership.owner());
        configurations.save(configuration("domain_reads", 1, List.of("node2")));
        assertEquals(1, clusters.get("domain_reads").replicationFactor());
        var accepted = queue.accept("domain_reads", "database", "domain-key");
        assertEquals("domain_reads", accepted.operation().clusterId());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void policyWriteFailureRollsBackClusterAndInitialLock() {
        String id = "atomic_registration";
        var invalid = new ClusterConfiguration(configuration(id, 3, List.of("node1")).cluster(),
            new RepairPolicy(id, List.of("node1"), "x".repeat(4001) + " {keyspace}", 1, 0));

        assertThrows(DataIntegrityViolationException.class, () -> configurations.save(invalid));

        assertNull(clusters.get(id));
        assertFalse(repairLocks.existsById(id));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void queuedRepairBlocksConfigurationAndPolicyChangesTogether() {
        String id = "pending_repair";
        var initial = configurations.save(configuration(id, 3, List.of("node1")));
        queue.accept(id, "database", "pending-operation");

        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> configurations.save(configuration(id, 1, List.of("node2"))));

        assertEquals(initial, configurations.get(id));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void staleAdmissionCannotReserveKeyspaceOrEnqueueRepair() {
        String id = "stale_admission";
        var initial = configurations.save(configuration(id, 3, List.of("node1")));
        configurations.save(configuration(id, 1, List.of("node2")));
        long oldVersion = initial.cluster().configVersion();

        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> provisioning.begin(id, "database", 5, 60, oldVersion));
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> queue.accept(id, "database", "stale-request", oldVersion));

        assertTrue(provisioning.list(id, 10).isEmpty());
        assertTrue(queue.list(id, false, 10).isEmpty());
    }
}
