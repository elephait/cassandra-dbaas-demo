package demo.platform;

import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ApiExceptionHandler;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.RepairCommandRunner;
import demo.platform.repair.RepairWorker;
import demo.platform.repair.RepairState;
import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairPolicyRepository;
import demo.platform.repair.persistence.RepairQueueRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@Import({ClusterRepository.class,     RepairQueueRepository.class, RepairExecutionRepository.class})
class RepairSafetyTest {
    @Test
    void completesEveryNodeWithoutTreatingSecondDispatchAsAConflict() {
        policies.save(new RepairPolicy("default",
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
        var service = new RepairWorker(properties, clusters, execution, policies, null, runner);
        queue.accept("default", "sequence", UUID.randomUUID().toString());
        var operation = execution.claim(60);
        try {
            service.run(operation);
            assertEquals(java.util.List.of("noop node1 sequence", "noop node2 sequence", "noop node3 sequence"), commands);
            assertEquals(RepairState.SUCCEEDED, queue.get(operation.id()).state());
            assertNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'", String.class));
        } finally {
            service.close();
        }
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
    void failedTerminalWriteDoesNotReleaseOwnership() {
        var operation = claimedRepair("team", Instant.now().plusSeconds(60));
        assertThrows(org.springframework.dao.DataIntegrityViolationException.class,
            () -> execution.finish(terminal(operation, "x".repeat(5000))));
        assertEquals(RepairState.RUNNING, queue.get(operation.id()).state());
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> claimedRepair("other", Instant.now().plusSeconds(60)));
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
        return new RepairWorker(properties, clusters, execution, policies, null, runner);
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
