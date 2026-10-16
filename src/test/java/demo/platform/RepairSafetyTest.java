package demo.platform;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.common.DatabaseClock;
import demo.platform.repair.*;
import demo.platform.repair.jmx.JmxRepairProperties;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@Import({RepairQueueService.class, RepairExecutionRepository.class, RepairClaimService.class,
    ClusterRegistryService.class, DatabaseClock.class, JmxRepairProperties.class})
class RepairSafetyTest {
    @Autowired
    RepairQueueService queue;
    @Autowired
    RepairExecutionRepository execution;
    @Autowired
    RepairClaimService claims;
    @Autowired
    JdbcTemplate jdbc;

    private OperationRecord started(String keyspace) {
        queue.accept("default", keyspace, UUID.randomUUID().toString());
        return RepairRuns.start(claims, execution);
    }

    private String reservationOwner() {
        return jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'", String.class);
    }

    @Test
    void completionReleasesTheReservationInTheSameTransactionAndStaleActionsAreFenced() {
        var operation = started("team");
        var task = execution.claimTask(60);
        assertTrue(execution.submitting(task, 1));
        execution.submitted(task, 0, 1);
        assertEquals(RepairState.SUCCEEDED, queue.get(operation.id()).state());
        assertNull(reservationOwner());
        assertThrows(IllegalStateException.class, () -> execution.complete(task, "stale completion"));
        var next = started("other");
        assertNotNull(next);
        assertEquals(RepairState.RUNNING, queue.get(next.id()).state());
    }

    @Test
    void oversizedDetailIsBoundedAndStillRecordsTheOutcome() {
        var operation = started("team");
        RepairRuns.becomeUnknown(execution, "x".repeat(5000));
        var unknown = queue.get(operation.id());
        assertEquals(RepairState.UNKNOWN, unknown.state());
        assertTrue(unknown.detail().length() <= 3900, "detail must fit operations.detail");
        assertEquals(operation.owner(), reservationOwner());
    }
}
