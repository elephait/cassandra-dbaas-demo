package demo.platform;

import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.OperationRecord.RepairDecision;
import demo.platform.repair.RepairState;
import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairPolicyRepository;
import demo.platform.repair.persistence.RepairQueueRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@Import({ClusterRepository.class,     RepairQueueRepository.class, RepairExecutionRepository.class})
class RepairIdempotencyTest {
    @Autowired
    RepairQueueRepository queue;
    @Autowired
    RepairExecutionRepository execution;

    private static OperationRecord terminal(OperationRecord operation) {
        return new OperationRecord(operation.id(), operation.clusterId(), operation.keyspace(), operation.owner(),
            RepairState.SUCCEEDED, "done", Instant.now());
    }

    @Test
    void replaysActiveAndCompletedRequestsAndRejectsConflictingReuse() {
        var first = queue.accept("default", "team", "repair-test");
        var repeated = queue.accept("default", "team", "repair-test");
        assertTrue(first.created());
        assertFalse(repeated.created());
        assertEquals(first.operation().id(), repeated.operation().id());
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> queue.accept("default", "other", "repair-test"));
        var operation = execution.claim(60);
        execution.finish(terminal(operation));
        var after = queue.accept("default", "team", "repair-test");
        assertFalse(after.created());
        assertEquals(RepairState.SUCCEEDED, after.operation().state());
        assertEquals(operation.id(), after.operation().id());
        assertTrue(queue.accept("default", "team", "intentional-next-repair").created());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrentSubmissionsHaveExactlyOneCreator() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var gate = new CountDownLatch(1);
        Callable<RepairDecision> accept = () -> {
            assertTrue(gate.await(5, TimeUnit.SECONDS));
            return queue.accept("default", "race", "concurrent-request");
        };
        try {
            var a = pool.submit(accept);
            var b = pool.submit(accept);
            gate.countDown();
            var first = a.get(10, TimeUnit.SECONDS);
            var second = b.get(10, TimeUnit.SECONDS);
            assertEquals(first.operation().id(), second.operation().id());
            assertNotEquals(first.created(), second.created());
            execution.finish(terminal(execution.claim(60)));
        } finally {
            pool.shutdownNow();
        }
    }
}
