package demo.platform.keyspace;

import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.persistence.KeyspaceEntity;
import demo.platform.keyspace.persistence.KeyspaceId;
import demo.platform.keyspace.persistence.KeyspaceJpaRepository;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.common.DatabaseClock;
import demo.platform.repair.RepairQueueService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest(showSql = false)
@Import({ClusterConfigurationService.class, KeyspaceProvisioningService.class, ClusterRegistryService.class, RepairQueueService.class,
    DatabaseClock.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class KeyspaceConcurrencyTest {
    @Autowired
    ClusterConfigurationService configurations;
    @Autowired
    KeyspaceProvisioningService provisioning;
    @Autowired
    KeyspaceJpaRepository keyspaces;
    @Autowired
    TransactionTemplate transactions;
    @Autowired
    DatabaseClock clock;

    private static <T> List<T> race(int count, IntFunction<T> task) throws Exception {
        var pool = Executors.newFixedThreadPool(count);
        var ready = new CountDownLatch(count);
        var start = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                final int index = i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Race start timed out");
                    return task.apply(index);
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            List<T> results = new ArrayList<>();
            for (var future : futures) results.add(future.get(20, TimeUnit.SECONDS));
            return results;
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private String createCluster(int quota) {
        String id = "race_" + UUID.randomUUID().toString().replace("-", "");
        configurations.save(new ClusterRecord(id, List.of(new ContactPoint("localhost", 9042)), "dc1", 1, 1,
            List.of(), 10, 60, quota, 60, true, 0));
        return id;
    }

    @Test
    void concurrentRequestsReserveOneIdentity() throws Exception {
        String cluster = createCluster(4);
        var outcomes = race(8, ignored -> {
            try {
                return provisioning.begin(cluster, "shared", 4, 60).owner();
            } catch (ControlPlaneExceptions.Conflict conflict) {
                return null;
            }
        });
        var owners = outcomes.stream().filter(java.util.Objects::nonNull).toList();
        assertEquals(1, owners.size());
        assertEquals(1, keyspaces.countByIdClusterId(cluster));
        provisioning.complete(cluster, "shared", owners.getFirst());
        assertTrue(provisioning.begin(cluster, "shared", 4, 60).alreadyReady());
    }

    @Test
    void concurrentReservationsCannotExceedQuota() throws Exception {
        String cluster = createCluster(5);
        var outcomes = race(8, index -> {
            try {
                provisioning.begin(cluster, "db_" + index, 5, 60);
                return true;
            } catch (ControlPlaneExceptions.KeyspaceQuotaExceeded quota) {
                return false;
            }
        });
        assertEquals(5, outcomes.stream().filter(Boolean::booleanValue).count());
        assertEquals(5, keyspaces.countByIdClusterId(cluster));
    }

    @Test
    void expiredOwnerCannotRenewCompleteOrReleaseReplacement() {
        String cluster = createCluster(1);
        var old = provisioning.begin(cluster, "recover", 1, 60);
        transactions.executeWithoutResult(status -> {
            KeyspaceEntity row = keyspaces.findById(new KeyspaceId(cluster, "recover")).orElseThrow();
            row.claim(old.owner(), clock.now().minusSeconds(120), 60);
            keyspaces.saveAndFlush(row);
        });
        var replacement = provisioning.begin(cluster, "recover", 1, 60);
        assertNotEquals(old.owner(), replacement.owner());
        assertFalse(provisioning.renew(cluster, "recover", old.owner(), 60));
        assertThrows(ControlPlaneExceptions.Conflict.class,
            () -> provisioning.complete(cluster, "recover", old.owner()));
        provisioning.release(cluster, "recover", old.owner());
        assertTrue(provisioning.owns(cluster, "recover", replacement.owner()));
        provisioning.complete(cluster, "recover", replacement.owner());
        assertTrue(provisioning.begin(cluster, "recover", 1, 60).alreadyReady());
    }
}
