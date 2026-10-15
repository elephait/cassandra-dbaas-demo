package demo.platform.repair.jmx;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.repair.*;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@Import({ClusterRepository.class, RepairQueueService.class, RepairExecutionRepository.class,
    RepairClaimService.class, RepairConfigurationService.class, ClusterRegistryService.class, JmxRepairRepository.class})
class JmxRepairRepositoryTest {
    @Autowired JmxRepairRepository progress;
    @Autowired RepairConfigurationService policies;
    @Autowired RepairQueueService queue;
    @Autowired RepairClaimService claims;
    @Autowired RepairExecutionRepository execution;
    @Autowired JdbcTemplate jdbc;

    private OperationRecord plan() {
        var policy = policies.savePolicy(new OperationRecord.RepairPolicy("default", List.of("n1", "n2"), "unused {node} {keyspace}", 1, 0));
        var operation = queue.accept("default", "team", UUID.randomUUID().toString()).operation();
        var claimed = claims.claim(60);
        assertEquals(operation.id(), claimed.id());
        progress.initialize(claimed, policy, List.of(
            new JmxRepairRepository.Target("n1", "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi", UUID.randomUUID().toString()),
            new JmxRepairRepository.Target("n2", "service:jmx:rmi:///jndi/rmi://localhost:7299/jmxrmi", UUID.randomUUID().toString())));
        return claimed;
    }

    private void due() {
        jdbc.update("UPDATE repair_node_progress SET next_action_at = CURRENT_TIMESTAMP");
    }

    private void expire() {
        jdbc.update("UPDATE repair_node_progress SET action_expires_at = TIMESTAMP '2000-01-01 00:00:00'");
    }

    @Test void anotherWorkerPollsPersistedCommandAndOnlyThenAdvancesToNextNode() {
        var operation = plan();
        var first = progress.claim(60);
        assertEquals(0, first.ordinal());
        assertNull(progress.claim(60));
        assertTrue(progress.submitting(first, 1234));
        progress.submitted(first, 17, 1);
        due();
        var poll = progress.claim(60);
        assertNotEquals(first.actionOwner(), poll.actionOwner());
        assertEquals("RUNNING", poll.state());
        assertEquals(17, poll.commandId());
        assertEquals(1234L, poll.processStartedAt());
        assertNull(progress.claim(60));
        progress.complete(poll, "completed");
        assertEquals(RepairState.RUNNING, queue.get(operation.id()).state());
        var second = progress.claim(60);
        assertEquals(1, second.ordinal());
        assertTrue(progress.submitting(second, 9876));
        progress.submitted(second, 0, 1);
        assertEquals(RepairState.SUCCEEDED, queue.get(operation.id()).state());
        assertNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'", String.class));
    }

    @Test void abandonedSubmissionBecomesUnknownAndRetainsClusterReservation() {
        var operation = plan();
        var old = progress.claim(60);
        assertTrue(progress.submitting(old, 1));
        expire();
        var replacement = progress.claim(60);
        assertEquals("SUBMITTING", replacement.state());
        assertThrows(IllegalStateException.class, () -> progress.submitted(old, 19, 1));
        progress.unknown(replacement, "Submission response lost");
        assertEquals(RepairState.UNKNOWN, queue.get(operation.id()).state());
        assertNotNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'", String.class));
        assertNull(progress.claim(60));
        execution.acknowledge(operation.id());
        assertEquals(RepairState.ACKNOWLEDGED, queue.get(operation.id()).state());
        assertNull(progress.claim(60));
    }

    @Test void abandonedPollCanBeReclaimedWithoutResubmissionAndOldWorkerIsFenced() {
        plan();
        var submit = progress.claim(60);
        assertTrue(progress.submitting(submit, 1));
        progress.submitted(submit, 5, 1);
        due();
        var old = progress.claim(60);
        expire();
        var replacement = progress.claim(60);
        assertEquals(5, replacement.commandId());
        assertThrows(IllegalStateException.class, () -> progress.complete(old, "stale completion"));
        progress.defer(replacement, "still running", 1);
    }

    @Test void expiredLegacyLeaseDoesNotInvalidatePersistedJmxRun() {
        var operation = plan();
        jdbc.update("UPDATE repair_locks SET expires_at = TIMESTAMP '2000-01-01 00:00:00'");
        assertEquals(0, execution.reconcileExpired());
        assertEquals(RepairState.RUNNING, queue.get(operation.id()).state());
        assertNotNull(progress.claim(60));
    }
}
