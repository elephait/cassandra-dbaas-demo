package demo.platform;

import demo.platform.common.DatabaseClock;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.RepairClaimService;
import demo.platform.repair.RepairRuns;
import demo.platform.repair.RepairState;
import demo.platform.repair.jmx.JmxRepairProperties;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two independent "replicas" (separate connection pools and repositories) against real PostgreSQL:
 * proves SKIP LOCKED claims, lease fencing and handoff of an abandoned submission.
 */
@Testcontainers
class PostgreSQLQueueIT {
    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17.6")
        .withDatabaseName("queue_test").withUsername("postgres").withPassword("integration-only");

    private static Replica replica(DriverManagerDataSource dataSource) {
        var jdbc = new JdbcTemplate(dataSource);
        var transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        var execution = new RepairExecutionRepository(jdbc, transactions, new DatabaseClock(jdbc));
        var claims = new RepairClaimService(execution, new JmxRepairProperties()) {
            @Override
            public OperationRecord claim() {
                return transactions.execute(status -> super.claim());
            }
        };
        return new Replica(jdbc, execution, claims);
    }

    private static <T> List<T> race(ExecutorService pool, Callable<T> a, Callable<T> b) throws Exception {
        var gate = new CountDownLatch(1);
        var first = pool.submit(() -> {
            gate.await();
            return a.call();
        });
        var second = pool.submit(() -> {
            gate.await();
            return b.call();
        });
        gate.countDown();
        return java.util.Arrays.asList(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
    }

    private static String state(JdbcTemplate jdbc, UUID id) {
        return jdbc.queryForObject("SELECT state FROM operations WHERE operation_id = ?", String.class, id.toString());
    }

    @Test
    void replicasClaimOnceFenceStaleOwnersAndNeverResubmitAnAbandonedSubmission() throws Exception {
        String url = postgres.getJdbcUrl();
        var aDataSource = new DriverManagerDataSource(url, postgres.getUsername(), postgres.getPassword());
        var bDataSource = new DriverManagerDataSource(url, postgres.getUsername(), postgres.getPassword());
        Flyway.configure().dataSource(aDataSource).load().migrate();
        Replica a = replica(aDataSource);
        Replica b = replica(bDataSource);
        UUID operationId = UUID.randomUUID();
        a.jdbc().update("INSERT INTO operations (operation_id, cluster_id, keyspace_name, state, detail, "
                + "updated_at, request_key) VALUES (?, 'default', 'queue_test', 'QUEUED', 'queued', CURRENT_TIMESTAMP, ?)",
            operationId.toString(), UUID.randomUUID().toString());
        var pool = Executors.newFixedThreadPool(2);
        try {
            // 1. Concurrent run claims: exactly one replica wins the cluster reservation.
            var claims = race(pool, () -> a.claims().claim(), () -> b.claims().claim());
            assertEquals(1, claims.stream().filter(java.util.Objects::nonNull).count());
            var old = claims.get(0) != null ? claims.get(0) : claims.get(1);
            assertEquals(operationId, old.id());

            // 2. The winner dies before persisting a plan: its lease expires and another replica requeues it.
            a.jdbc().update("UPDATE repair_locks SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' "
                + "WHERE owner_id = ?", old.owner());
            assertEquals(1, b.execution().reconcileExpired());
            var replacement = b.claims().claim();
            assertNotEquals(old.owner(), replacement.owner());
            assertThrows(IllegalStateException.class, () -> a.execution().initialize(old, RepairRuns.plan()));
            b.execution().initialize(replacement, RepairRuns.plan());

            // 3. Concurrent node-action claims: exactly one replica gets the action lease.
            var tasks = race(pool, () -> a.execution().claimTask(60), () -> b.execution().claimTask(60));
            assertEquals(1, tasks.stream().filter(java.util.Objects::nonNull).count());
            var task = tasks.get(0) != null ? tasks.get(0) : tasks.get(1);
            assertTrue(a.execution().submitting(task, 1));

            // 4. That worker dies mid-submission; the next claimer must not resubmit.
            a.jdbc().update("UPDATE repair_node_progress SET action_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second'");
            var abandoned = b.execution().claimTask(60);
            assertEquals("SUBMITTING", abandoned.state());
            assertThrows(IllegalStateException.class, () -> a.execution().submitted(task, 17, 1));
            b.execution().unknown(abandoned, "Worker disappeared during JMX submission");
            assertEquals(RepairState.UNKNOWN.name(), state(a.jdbc(), operationId));
            assertEquals(0, a.execution().reconcileExpired());
            assertNull(a.claims().claim());

            // 5. Concurrent acknowledgements are idempotent and release the reservation once.
            race(pool, () -> {
                a.execution().acknowledge(operationId);
                return true;
            }, () -> {
                b.execution().acknowledge(operationId);
                return true;
            });
            assertEquals(RepairState.ACKNOWLEDGED.name(), state(a.jdbc(), operationId));
            assertNull(a.jdbc().queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'",
                String.class));
        } finally {
            pool.shutdownNow();
        }
    }

    private record Replica(JdbcTemplate jdbc, RepairExecutionRepository execution, RepairClaimService claims) {
    }
}
