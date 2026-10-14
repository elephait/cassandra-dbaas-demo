package demo.platform;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.RepairState;
import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairPolicyRepository;
import demo.platform.repair.persistence.RepairQueueRepository;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnabledIfEnvironmentVariable(named = "DBAAS_QUEUE_TEST_JDBC_URL", matches = ".+")
class PostgreSQLQueueTest {
    private static RepairExecutionRepository execution(JdbcTemplate jdbc, TransactionTemplate transactions) {
        ClusterRepository clusters = mock(ClusterRepository.class);
        RepairPolicyRepository policies = mock(RepairPolicyRepository.class);
        RepairQueueRepository queue = mock(RepairQueueRepository.class);
        when(clusters.get("default")).thenReturn(cluster());
        when(policies.get("default")).thenReturn(policy());
        when(queue.get(any(UUID.class))).thenAnswer(invocation -> operation(jdbc, invocation.getArgument(0)));
        return new RepairExecutionRepository(jdbc, transactions, clusters, queue, policies);
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
        String url = System.getenv("DBAAS_QUEUE_TEST_JDBC_URL");
        String user = System.getenv().getOrDefault("DBAAS_QUEUE_TEST_USER", "postgres");
        String password = System.getenv().getOrDefault("DBAAS_QUEUE_TEST_PASSWORD", "queue-demo-only");
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

        RepairExecutionRepository a = execution(aJdbc,
            new TransactionTemplate(new DataSourceTransactionManager(aDataSource)));
        RepairExecutionRepository b = execution(bJdbc,
            new TransactionTemplate(new DataSourceTransactionManager(bDataSource)));
        var pool = Executors.newFixedThreadPool(2);
        var gate = new CountDownLatch(1);
        try {
            var firstClaim = pool.submit(() -> {
                gate.await();
                return a.claim(60);
            });
            var secondClaim = pool.submit(() -> {
                gate.await();
                return b.claim(60);
            });
            gate.countDown();
            var first = firstClaim.get(10, TimeUnit.SECONDS);
            var second = secondClaim.get(10, TimeUnit.SECONDS);
            assertTrue((first == null) != (second == null));
            var old = first == null ? second : first;
            assertEquals(operationId, old.id());
            aJdbc.update("UPDATE repair_locks SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' "
                + "WHERE owner_id = ?", old.owner());
            b.reconcileExpired();
            var replacement = b.claim(60);
            assertNotEquals(old.owner(), replacement.owner());
            assertFalse(a.renew(old.owner(), 60));
            assertThrows(ControlPlaneExceptions.Conflict.class, () -> a.markDispatched(old));
            b.markDispatched(replacement);
            aJdbc.update("UPDATE repair_locks SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' "
                + "WHERE owner_id = ?", replacement.owner());
            a.reconcileExpired();
            assertEquals(RepairState.UNKNOWN, operation(aJdbc, operationId).state());
            assertNull(a.claim(60));
            var acknowledgementA = pool.submit(() -> a.acknowledge(operationId));
            var acknowledgementB = pool.submit(() -> b.acknowledge(operationId));
            acknowledgementA.get(10, TimeUnit.SECONDS);
            acknowledgementB.get(10, TimeUnit.SECONDS);
            a.acknowledge(operationId);
        } finally {
            pool.shutdownNow();
        }
    }
}
