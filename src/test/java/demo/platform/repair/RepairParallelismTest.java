package demo.platform.repair;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.keyspace.KeyspaceProvisioningService;
import demo.platform.misc.ApiExceptionHandler;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DataJpaTest(showSql = false)
@Import({ClusterConfigurationService.class, ClusterRepository.class, RepairQueueService.class, RepairExecutionRepository.class, RepairClaimService.class, RepairConfigurationService.class, KeyspaceProvisioningService.class, ClusterRegistryService.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class RepairParallelismTest {
    @Autowired
    ClusterRegistryService clusters;
    @Autowired
    ClusterConfigurationService configurations;
    @Autowired
    RepairConfigurationService repairConfiguration;
    @Autowired
    RepairQueueService queue;
    @Autowired
    RepairExecutionRepository execution;
    @Autowired
    RepairClaimService claims;
    @Autowired
    JdbcTemplate jdbc;

    private void register(String id) {
        var clusterId = id;
        configurations.save(new ClusterConfiguration(new ClusterRecord(clusterId, List.of(new ContactPoint(id + ".example", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 5, 60, true, 0),
            new RepairPolicy(clusterId, List.of("node1"), "noop {keyspace}", 1, 0)));
    }

    @Test
    void separateClustersRunInParallelWhileOneClusterCannotOverlap() throws Exception {
        register("parallel_a");
        register("parallel_b");
        var first = queue.accept("parallel_a", "one", UUID.randomUUID().toString()).operation();
        var second = queue.accept("parallel_a", "two", UUID.randomUUID().toString()).operation();
        var independent = queue.accept("parallel_b", "one", UUID.randomUUID().toString()).operation();
        var started = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var completions = new CountDownLatch(2);
        var concurrency = new AtomicInteger();
        var maximum = new AtomicInteger();
        var runner = new RepairCommandRunner() {
            @Override
            public void execute(String command, Path output) throws Exception {
                maximum.accumulateAndGet(concurrency.incrementAndGet(), Math::max);
                started.countDown();
                try {
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                } finally {
                    concurrency.decrementAndGet();
                    completions.countDown();
                }
            }
        };
        var properties = new ApiExceptionHandler.CassandraProperties();
        properties.getRepair().setWorkerThreads(2);
        var service = new RepairWorker(properties, clusters, execution, claims, repairConfiguration, null, runner);
        try {
            service.poll();
            assertTrue(started.await(10, TimeUnit.SECONDS));
            assertEquals(2, maximum.get());
            assertEquals(RepairState.RUNNING, queue.get(first.id()).state());
            assertEquals(RepairState.QUEUED, queue.get(second.id()).state());
            assertEquals(RepairState.RUNNING, queue.get(independent.id()).state());
            release.countDown();
            assertTrue(completions.await(10, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            service.close();
        }
        assertEquals(RepairState.SUCCEEDED, queue.get(first.id()).state());
        assertEquals(RepairState.SUCCEEDED, queue.get(independent.id()).state());
        var next = claims.claim(60);
        assertEquals(second.id(), next.id());
        execution.finish(new OperationRecord(next.id(), next.clusterId(), next.keyspace(), next.owner(), RepairState.FAILED, "Test cleanup before dispatch", next.repairPolicyVersion(), java.time.Instant.now()));
    }

    @Test
    void instanceWithWorkerDisabledServesApiWithoutClaimingWork() {
        register("worker_off");
        var queued = queue.accept("worker_off", "one", UUID.randomUUID().toString()).operation();
        var properties = new ApiExceptionHandler.CassandraProperties();
        properties.getRepair().setWorkerEnabled(false);
        var service = new RepairWorker(properties, clusters, execution, claims, repairConfiguration, null, new RepairCommandRunner());
        try {
            service.poll();
            assertEquals(RepairState.QUEUED, queue.get(queued.id()).state());
            assertNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id = 'worker_off'",
                String.class));
        } finally {
            service.close();
        }
    }
}
