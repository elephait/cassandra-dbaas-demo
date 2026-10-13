package demo.platform;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.flywaydb.core.Flyway;
import java.time.Instant;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Run only against a disposable dedicated database: migrations create the POC schema. */
@EnabledIfEnvironmentVariable(named = "DBAAS_QUEUE_TEST_JDBC_URL", matches = ".+")
class PostgreSQLQueueTest {
    @Test void twoIndependentWorkersClaimOnceAndRecoverUndispatchedWork() throws Exception {
        String url = System.getenv("DBAAS_QUEUE_TEST_JDBC_URL");
        String user = System.getenv().getOrDefault("DBAAS_QUEUE_TEST_USER", "postgres");
        String password = System.getenv().getOrDefault("DBAAS_QUEUE_TEST_PASSWORD", "queue-demo-only");
        var aDs = new DriverManagerDataSource(url, user, password);
        var bDs = new DriverManagerDataSource(url, user, password);
        Flyway.configure().dataSource(aDs).load().migrate();
        var aJdbc = new JdbcTemplate(aDs); var bJdbc = new JdbcTemplate(bDs);
        var a = new ControlPlaneRepository(aJdbc, new TransactionTemplate(new DataSourceTransactionManager(aDs)));
        var b = new ControlPlaneRepository(bJdbc, new TransactionTemplate(new DataSourceTransactionManager(bDs)));
        var accepted = a.acceptRepair("tenant_queue_test", Instant.now(), java.util.UUID.randomUUID().toString());
        var pool = Executors.newFixedThreadPool(2); var gate = new CountDownLatch(1);
        try {
            var x = pool.submit(() -> { gate.await(); return a.claimRepair(60); });
            var y = pool.submit(() -> { gate.await(); return b.claimRepair(60); }); gate.countDown();
            var first = x.get(10, TimeUnit.SECONDS); var second = y.get(10, TimeUnit.SECONDS);
            assertTrue((first == null) != (second == null));
            var old = first == null ? second : first; assertEquals(accepted.operation().id(), old.id());
            aJdbc.update("UPDATE repair_lock SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE owner_id = ?", old.owner());
            b.reconcileExpiredRepair(); var replacement = b.claimRepair(60);
            assertNotEquals(old.owner(), replacement.owner()); assertFalse(a.renewRepair(old.owner(), 60));
            assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class, () -> a.markDispatched(old));
            b.markDispatched(replacement);
            aJdbc.update("UPDATE repair_lock SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE owner_id = ?", replacement.owner());
            a.reconcileExpiredRepair(); assertEquals("UNKNOWN", a.getOperation(old.id()).state());
            assertNull(a.claimRepair(60)); b.acknowledgeUnknownRepair(old.id()); a.acknowledgeUnknownRepair(old.id());
        } finally { pool.shutdownNow(); }
    }
}
