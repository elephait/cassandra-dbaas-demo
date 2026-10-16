package demo.platform;

import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.common.DatabaseClock;
import demo.platform.keyspace.KeyspaceProvisioningService;
import demo.platform.repair.*;
import demo.platform.repair.jmx.JmxRepairProperties;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@Import({ClusterConfigurationService.class, RepairQueueService.class,
    RepairExecutionRepository.class, RepairClaimService.class, KeyspaceProvisioningService.class, ClusterRegistryService.class, DatabaseClock.class, JmxRepairProperties.class})
class RepairQueueTest {
    @Autowired
    ClusterConfigurationService configurations;
    @Autowired
    RepairQueueService queue;
    @Autowired
    RepairExecutionRepository execution;
    @Autowired
    RepairClaimService claims;
    @Autowired
    JmxRepairProperties settings;
    @Autowired
    DatabaseClock clock;
    @Autowired
    TransactionTemplate transactions;
    @Autowired
    JdbcTemplate jdbc;

    private static ClusterRecord cluster(String id) {
        return new ClusterRecord(id, List.of(new ContactPoint(id + ".example", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 100, 60, true, 0);
    }

    private void expire(OperationRecord operation) {
        jdbc.update("UPDATE repair_locks SET expires_at = ? WHERE owner_id = ?",
            Timestamp.from(Instant.now().minusSeconds(60)), operation.owner());
    }

    @Test
    void deferredClusterDoesNotBlockHealthyCluster() {
        configurations.save(cluster("east"));
        configurations.save(cluster("west"));
        queue.accept("east", "team", "down-east");
        var east = claims.claim();
        assertTrue(claims.deferUndispatched(east, "Cassandra unavailable"));
        assertNull(claims.claim());
        queue.accept("west", "team", "healthy-west");
        var west = claims.claim();
        assertEquals("west", west.clusterId());
        assertEquals(RepairState.QUEUED, queue.get(east.id()).state());
        assertEquals(1, execution.retryCount(east.id()));
    }

    @Test
    void expiredUndispatchedClaimIsRequeuedAndOldOwnerCannotStartIt() {
        var accepted = queue.accept("default", "team", "queued-request");
        assertNull(accepted.operation().owner());
        var old = claims.claim();
        assertEquals(accepted.operation().id(), old.id());
        assertNull(claims.claim());
        expire(old);
        assertEquals(1, execution.reconcileExpired());
        var replacement = claims.claim();
        assertEquals(old.id(), replacement.id());
        assertNotEquals(old.owner(), replacement.owner());
        assertThrows(IllegalStateException.class, () -> execution.initialize(old, RepairRuns.plan()));
        execution.initialize(replacement, RepairRuns.plan());
        assertEquals(RepairState.RUNNING, queue.get(old.id()).state());
    }

    @Test
    void uncertainRunKeepsClusterExclusionUntilIdempotentAcknowledgement() {
        var first = queue.accept("default", "team", "first").operation();
        RepairRuns.start(claims, execution);
        var second = queue.accept("default", "other", "second").operation();
        RepairRuns.becomeUnknown(execution, "Submission response lost");
        assertEquals(RepairState.UNKNOWN, queue.get(first.id()).state());
        assertEquals(0, execution.reconcileExpired(), "an UNKNOWN run is never released by lease expiry");
        assertNull(claims.claim());
        execution.acknowledge(first.id());
        execution.acknowledge(first.id());
        assertEquals(RepairState.ACKNOWLEDGED, queue.get(first.id()).state());
        assertEquals(second.id(), claims.claim().id());
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> execution.acknowledge(second.id()));
    }

    @Test
    void independentClustersCanBeClaimedConcurrently() {
        configurations.save(cluster("east"));
        configurations.save(cluster("west"));
        queue.accept("east", "team", "east-repair");
        queue.accept("west", "team", "west-repair");

        var first = claims.claim();
        var second = claims.claim();

        assertNotNull(first);
        assertNotNull(second);
        assertNotEquals(first.clusterId(), second.clusterId());
        assertEquals(2, jdbc.queryForObject("SELECT COUNT(*) FROM repair_locks WHERE cluster_id IN ('east','west') "
            + "AND owner_id IS NOT NULL", Integer.class));
    }

    @Test
    void queueAdmitsTheThousandthButRejectsTheNextAndStillReplaysExistingKeys() {
        var fixtures = new ArrayList<Object[]>();
        for (int i = 0; i < 999; i++) fixtures.add(new Object[]{UUID.randomUUID().toString(), "fixture-" + i});
        jdbc.batchUpdate("INSERT INTO operations(operation_id, cluster_id, keyspace_name, state, updated_at, "
            + "request_key) VALUES (?, 'default', 'team', 'QUEUED', CURRENT_TIMESTAMP, ?)", fixtures);
        var last = queue.accept("default", "team", "thousandth");
        assertTrue(last.created());
        assertFalse(queue.accept("default", "team", "thousandth").created());
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> queue.accept("default", "team", "over-capacity"));
        assertEquals(1000, jdbc.queryForObject("SELECT COUNT(*) FROM operations WHERE cluster_id = 'default'", Integer.class));
    }

    @Test
    void persistedBackoffEnforcesDueTimeAndCapsAtThreeHundredSeconds() {
        queue.accept("default", "team", "backoff");
        for (int delay : new int[]{5, 10, 20, 40, 80, 160, 300, 300}) {
            var operation = claims.claim();
            assertNotNull(operation);
            var before = clock.now();
            assertTrue(claims.deferUndispatched(operation, "not ready"));
            var due = jdbc.queryForObject("SELECT next_attempt_at FROM operations WHERE operation_id = ?",
                (rs, row) -> rs.getTimestamp(1).toInstant(), operation.id().toString());
            assertEquals(delay, Duration.between(before, due).toSeconds());
            assertNull(claims.claim());
            jdbc.update("UPDATE operations SET next_attempt_at = CURRENT_TIMESTAMP WHERE operation_id = ?", operation.id().toString());
        }
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void committedDeferralSurvivesRepositoryAndClaimServiceReplacement() {
        var accepted = queue.accept("default", "team", "persisted-restart").operation();
        try {
            var original = claims.claim();
            assertTrue(claims.deferUndispatched(original, "restart fixture"));
            var replacementExecution = new RepairExecutionRepository(jdbc, transactions, clock);
            var replacementClaims = new RepairClaimService(replacementExecution, settings);
            assertNull(transactions.execute(tx -> replacementClaims.claim()));
            assertEquals(1, replacementExecution.retryCount(accepted.id()));
            jdbc.update("UPDATE operations SET next_attempt_at = CURRENT_TIMESTAMP WHERE operation_id = ?", accepted.id().toString());
            var reclaimed = transactions.execute(tx -> replacementClaims.claim());
            assertEquals(accepted.id(), reclaimed.id());
            assertNotEquals(original.owner(), reclaimed.owner());
        } finally {
            jdbc.update("UPDATE repair_locks SET owner_id = NULL, expires_at = NULL WHERE cluster_id = 'default'");
            jdbc.update("DELETE FROM operations WHERE operation_id = ?", accepted.id().toString());
        }
    }
}
