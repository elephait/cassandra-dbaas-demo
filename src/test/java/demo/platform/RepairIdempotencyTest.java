package demo.platform;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@JdbcTest
@ContextConfiguration(classes = ControlPlaneRepository.class)
class RepairIdempotencyTest {
    @Autowired ControlPlaneRepository repository;

    @Test void replaysActiveAndCompletedRequestsAndRejectsConflictingReuse() {
        var first = repository.acceptRepair("tenant_team", Instant.now().plusSeconds(60), "repair-test");
        var repeated = repository.acceptRepair("tenant_team", Instant.now().plusSeconds(60), "repair-test");
        assertTrue(first.created()); assertFalse(repeated.created());
        assertEquals(first.operation().id(), repeated.operation().id());
        assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class,
                () -> repository.acceptRepair("tenant_other", Instant.now().plusSeconds(60), "repair-test"));
        var op = repository.claimRepair(60);
        repository.finishRepair(new ControlPlaneRepository.OperationRecord(op.id(), op.keyspace(), op.owner(), "SUCCEEDED", "done", Instant.now()));
        var after = repository.acceptRepair("tenant_team", Instant.now().plusSeconds(60), "repair-test");
        assertFalse(after.created()); assertEquals("SUCCEEDED", after.operation().state());
        assertEquals(op.id(), after.operation().id());
        assertTrue(repository.acceptRepair("tenant_team", Instant.now().plusSeconds(60), "intentional-next-repair").created());
    }

    @Test @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrentSubmissionsHaveExactlyOneCreator() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var gate = new CountDownLatch(1);
        Callable<ControlPlaneRepository.RepairDecision> accept = () -> {
            assertTrue(gate.await(5, TimeUnit.SECONDS));
            return repository.acceptRepair("tenant_race", Instant.now().plusSeconds(60), "concurrent-request");
        };
        try {
            var a = pool.submit(accept); var b = pool.submit(accept); gate.countDown();
            var first = a.get(10, TimeUnit.SECONDS); var second = b.get(10, TimeUnit.SECONDS);
            assertEquals(first.operation().id(), second.operation().id());
            assertNotEquals(first.created(), second.created());
            var op = repository.claimRepair(60);
            repository.finishRepair(new ControlPlaneRepository.OperationRecord(op.id(), op.keyspace(), op.owner(), "SUCCEEDED", "done", Instant.now()));
        } finally { pool.shutdownNow(); }
    }
}

