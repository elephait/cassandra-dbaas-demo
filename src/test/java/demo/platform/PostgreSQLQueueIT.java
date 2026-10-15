package demo.platform;

import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRecord;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.RepairClaimService;
import demo.platform.repair.RepairConfigurationService;
import demo.platform.repair.RepairState;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers
class PostgreSQLQueueIT {
    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17.6")
        .withDatabaseName("queue_test").withUsername("postgres").withPassword("integration-only");

    private record Worker(RepairExecutionRepository execution, RepairClaimService claims) {
    }

    private static Worker worker(JdbcTemplate jdbc, TransactionTemplate transactions) {
        RepairConfigurationService repairConfiguration = mock(RepairConfigurationService.class);
        when(repairConfiguration.getPolicy("default")).thenReturn(policy());
        var execution = new RepairExecutionRepository(jdbc, transactions);
        var claims = new RepairClaimService(execution, repairConfiguration) {
            @Override
            public OperationRecord claim(int seconds) {
                return transactions.execute(status -> super.claim(seconds));
            }
        };
        return new Worker(execution, claims);
    }

    private static OperationRecord operation(JdbcTemplate jdbc, UUID id) {
        return jdbc.queryForObject("SELECT operation_id, cluster_id, keyspace_name, owner_id, state, detail, "
                + "repair_policy_version, updated_at FROM operations WHERE operation_id = ?",
            (rs, row) -> new OperationRecord(UUID.fromString(rs.getString("operation_id")),
                rs.getString("cluster_id"), rs.getString("keyspace_name"), rs.getString("owner_id"),
                RepairState.valueOf(rs.getString("state")), rs.getString("detail"), rs.getLong("repair_policy_version"),
                rs.getTimestamp("updated_at").toInstant()), id.toString());
    }

    private static ClusterRecord cluster() {
        return new ClusterRecord("default", List.of(new ContactPoint("localhost", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 10, 60, true, 1);
    }

    private static RepairPolicy policy() {
        return new RepairPolicy("default", List.of("node1"),
            "nodetool repair -pr {keyspace}", 60, 1);
    }

    @Test
    void twoIndependentWorkersClaimOnceAndRecoverUndispatchedWork() throws Exception {
        String url = postgres.getJdbcUrl();
        String user = postgres.getUsername();
        String password = postgres.getPassword();
        var aDataSource = new DriverManagerDataSource(url, user, password);
        var bDataSource = new DriverManagerDataSource(url, user, password);
        Flyway.configure().dataSource(aDataSource).load().migrate();
        var aJdbc = new JdbcTemplate(aDataSource);
        var bJdbc = new JdbcTemplate(bDataSource);
        UUID operationId = UUID.randomUUID();
        aJdbc.update("INSERT INTO operations (operation_id, cluster_id, keyspace_name, state, detail, "
                + "updated_at, request_key, repair_policy_version) "
                + "VALUES (?, 'default', 'queue_test', 'QUEUED', 'queued', CURRENT_TIMESTAMP, ?, 1)",
            operationId.toString(), UUID.randomUUID().toString());

        Worker a = worker(aJdbc,
            new TransactionTemplate(new DataSourceTransactionManager(aDataSource)));
        Worker b = worker(bJdbc,
            new TransactionTemplate(new DataSourceTransactionManager(bDataSource)));
        var pool = Executors.newFixedThreadPool(2);
        var gate = new CountDownLatch(1);
        try {
            var firstClaim = pool.submit(() -> {
                gate.await();
                return a.claims().claim(60);
            });
            var secondClaim = pool.submit(() -> {
                gate.await();
                return b.claims().claim(60);
            });
            gate.countDown();
            var first = firstClaim.get(10, TimeUnit.SECONDS);
            var second = secondClaim.get(10, TimeUnit.SECONDS);
            assertTrue((first == null) != (second == null));
            var old = first == null ? second : first;
            assertEquals(operationId, old.id());
            aJdbc.update("UPDATE repair_locks SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' "
                + "WHERE owner_id = ?", old.owner());
            b.execution().reconcileExpired();
            var replacement = b.claims().claim(60);
            assertNotEquals(old.owner(), replacement.owner());
            assertFalse(a.execution().renew(old.owner(), 60));
            assertThrows(ControlPlaneExceptions.Conflict.class, () -> a.execution().markDispatched(old));
            b.execution().markDispatched(replacement);
            aJdbc.update("UPDATE repair_locks SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' "
                + "WHERE owner_id = ?", replacement.owner());
            a.execution().reconcileExpired();
            assertEquals(RepairState.UNKNOWN, operation(aJdbc, operationId).state());
            assertNull(a.claims().claim(60));
            var acknowledgementA = pool.submit(() -> a.execution().acknowledge(operationId));
            var acknowledgementB = pool.submit(() -> b.execution().acknowledge(operationId));
            acknowledgementA.get(10, TimeUnit.SECONDS);
            acknowledgementB.get(10, TimeUnit.SECONDS);
            a.execution().acknowledge(operationId);
        } finally {
            pool.shutdownNow();
        }
    }
}
