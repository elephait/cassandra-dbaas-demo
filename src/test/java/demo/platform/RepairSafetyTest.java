package demo.platform;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ApiExceptionHandler;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.RepairClaimService;
import demo.platform.repair.RepairCommandRunner;
import demo.platform.repair.RepairConfigurationService;
import demo.platform.repair.RepairQueueService;
import demo.platform.repair.RepairState;
import demo.platform.repair.RepairWorker;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DataJpaTest
@Import({ClusterRepository.class, RepairQueueService.class, RepairExecutionRepository.class, RepairClaimService.class, RepairConfigurationService.class, ClusterRegistryService.class})
class RepairSafetyTest {
    @Test
    void completesEveryNodeWithoutTreatingSecondDispatchAsAConflict() {
        repairConfiguration.savePolicy(new RepairPolicy("default",
            java.util.List.of("node1", "node2", "node3"), "noop {node} {keyspace}", 1, 0));
        java.util.List<String> commands = new java.util.ArrayList<>();
        var runner = new RepairCommandRunner() {
            @Override
            public void execute(String command, Path output) {
                commands.add(command);
            }
        };
        var properties = new ApiExceptionHandler.CassandraProperties();
        properties.getRepair().setWorkerThreads(1);
        var service = new RepairWorker(properties, clusters, execution, claims, repairConfiguration, null, runner);
        queue.accept("default", "sequence", UUID.randomUUID().toString());
        var operation = claims.claim(60);
        try {
            service.run(operation);
            assertEquals(java.util.List.of("noop node1 sequence", "noop node2 sequence", "noop node3 sequence"), commands);
            assertEquals(RepairState.SUCCEEDED, queue.get(operation.id()).state());
            assertNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'", String.class));
        } finally {
            service.close();
        }
    }

    @Test
    void persistsRunningExactlyOnceAndOnlyBeforeTheFirstNodeRuns() {
        repairConfiguration.savePolicy(new RepairPolicy("default",
            java.util.List.of("node1", "node2", "node3"), "noop {node} {keyspace}", 1, 0));
        queue.accept("default", "sequence", UUID.randomUUID().toString());
        var operation = claims.claim(60);
        java.util.List<String> states = new java.util.ArrayList<>();
        var runner = new RepairCommandRunner() {
            @Override
            public void execute(String command, Path output) {
                states.add(queue.get(operation.id()).state().name());
            }
        };
        var properties = new ApiExceptionHandler.CassandraProperties();
        properties.getRepair().setWorkerThreads(1);
        var service = new RepairWorker(properties, clusters, execution, claims, repairConfiguration, null, runner);
        try {
            service.run(operation);
            assertEquals(java.util.List.of("RUNNING", "RUNNING", "RUNNING"), states);
            assertEquals(RepairState.SUCCEEDED, queue.get(operation.id()).state());
        } finally {
            service.close();
        }
    }

    @Autowired
    ClusterRegistryService clusters;
    @Autowired
    RepairQueueService queue;
    @Autowired
    RepairExecutionRepository execution;
    @Autowired
    RepairClaimService claims;
    @Autowired
    RepairConfigurationService repairConfiguration;
    @Autowired
    JdbcTemplate jdbc;

    private static OperationRecord terminal(OperationRecord operation, String detail) {
        return new OperationRecord(operation.id(), operation.clusterId(), operation.keyspace(), operation.owner(),
            RepairState.SUCCEEDED, detail, operation.repairPolicyVersion(), Instant.now());
    }

    @Test
    void timeoutRetainsOwnershipUntilAcknowledgement() {
        var service = serviceThatTimesOut();
        var operation = claimedRepair("team", Instant.now().plusSeconds(60));
        try {
            service.run(operation);
            assertEquals(RepairState.UNKNOWN, queue.get(operation.id()).state());
            assertThrows(ControlPlaneExceptions.Conflict.class,
                () -> claimedRepair("other", Instant.now().plusSeconds(60)));
            execution.acknowledge(operation.id());
            assertNotNull(claimedRepair("other", Instant.now().plusSeconds(60)));
        } finally {
            service.close();
        }
    }

    @Test
    void confirmedCompletionReleasesOwnershipInSameTransaction() {
        var operation = claimedRepair("team", Instant.now().plusSeconds(60));
        execution.finish(terminal(operation, "done"));
        assertEquals(RepairState.SUCCEEDED, queue.get(operation.id()).state());
        var next = claimedRepair("other", Instant.now().plusSeconds(60));
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> execution.finish(terminal(operation, "stale completion")));
        assertEquals(RepairState.RUNNING, queue.get(next.id()).state());
    }

    @Test
    void oversizedTerminalDetailIsBoundedAndStillReleasesOwnership() {
        var operation = claimedRepair("team", Instant.now().plusSeconds(60));
        execution.finish(terminal(operation, "x".repeat(5000)));
        var finished = queue.get(operation.id());
        assertEquals(RepairState.SUCCEEDED, finished.state());
        assertTrue(finished.detail().length() <= 3900, "detail must fit operations.detail");
        assertNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'", String.class));
    }

    @Test
    void failureBeforeAnyNodeRunsRequeuesInsteadOfBurningTheRepair() {
        var properties = new ApiExceptionHandler.CassandraProperties();
        properties.getRepair().setWorkerThreads(1);
        var connections = mock(CassandraConnectionManager.class);
        when(connections.requireSession(any(ClusterRecord.class)))
            .thenThrow(new IllegalStateException("Cassandra unavailable before dispatch"));
        var runner = mock(RepairCommandRunner.class);
        var service = new RepairWorker(properties, clusters, execution, claims, repairConfiguration, connections, runner);
        queue.accept("default", "before_dispatch", UUID.randomUUID().toString());
        var operation = claims.claim(60);
        try {
            service.run(operation);
            var deferred = queue.get(operation.id());
            assertEquals(RepairState.QUEUED, deferred.state());
            verifyNoInteractions(runner);
            assertNull(deferred.owner());
            assertEquals(1, jdbc.queryForObject("SELECT retry_count FROM operations WHERE operation_id = ?",
                Integer.class, operation.id().toString()));
            assertNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'", String.class));
        } finally {
            service.close();
        }
    }

    private RepairWorker serviceThatTimesOut() {
        var properties = new ApiExceptionHandler.CassandraProperties();
        properties.getRepair().setWorkerThreads(1);
        var runner = new RepairCommandRunner() {
            @Override
            public void execute(String command, Path log) throws Exception {
                throw new java.util.concurrent.TimeoutException("simulated lost CLI response");
            }
        };
        return new RepairWorker(properties, clusters, execution, claims, repairConfiguration, null, runner);
    }

    private OperationRecord claimedRepair(String keyspace, Instant expiry) {
        Integer occupied = jdbc.queryForObject("SELECT COUNT(*) FROM repair_locks "
            + "WHERE cluster_id = 'default' AND owner_id IS NOT NULL", Integer.class);
        if (occupied != null && occupied > 0) throw new ControlPlaneExceptions.Conflict("Repair already running");
        queue.accept("default", keyspace, UUID.randomUUID().toString());
        var operation = claims.claim(60);
        execution.markDispatched(operation);
        jdbc.update("UPDATE repair_locks SET expires_at = ? WHERE owner_id = ?",
            java.sql.Timestamp.from(expiry), operation.owner());
        return queue.get(operation.id());
    }
}
