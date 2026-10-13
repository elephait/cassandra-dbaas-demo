package demo.platform;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.test.context.ContextConfiguration;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@JdbcTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:control-plane-test;DB_CLOSE_DELAY=-1"
})
@ContextConfiguration(classes = ControlPlaneRepository.class)
class ControlPlaneRepositoryTest {
    @Autowired
    private ControlPlaneRepository repository;

    @Test
    void reservesTenantQuotaAndMakesProvisioningIdempotent() {
        var first = repository.beginProvision("team", "tenant_team", 1, Instant.now().plusSeconds(60));
        assertFalse(first.alreadyReady());
        assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class,
                () -> repository.beginProvision("team", "tenant_team", 1, Instant.now().plusSeconds(60)));
        repository.completeProvision("team", first.owner());
        assertTrue(repository.beginProvision("team", "tenant_team", 1, Instant.now().plusSeconds(60)).alreadyReady());
        assertThrows(ControlPlaneRepository.TenantQuotaExceededException.class,
                () -> repository.beginProvision("other", "tenant_other", 1, Instant.now().plusSeconds(60)));
    }

    @Test
    void resumesProvisioningAfterLeaseExpiry() {
        repository.beginProvision("stale", "tenant_stale", 2, Instant.now().minusSeconds(1));
        var resumed = repository.beginProvision("stale", "tenant_stale", 2, Instant.now().plusSeconds(60));
        assertFalse(resumed.alreadyReady());
        repository.completeProvision("stale", resumed.owner());
    }

        @Test
        void listsTenantsAndActiveOperations() {
                var provision = repository.beginProvision("listed", "tenant_listed", 2, Instant.now().plusSeconds(60));
                repository.completeProvision("listed", provision.owner());
                var completed = repository.startRepair("tenant_listed", Instant.now().plusSeconds(60));
                repository.updateOperation(new ControlPlaneRepository.OperationRecord(
                                completed.id(), completed.keyspace(), completed.owner(), "SUCCEEDED", "done", Instant.now()));
                repository.releaseRepair(completed.owner());
                var active = repository.startRepair("tenant_listed", Instant.now().plusSeconds(60));

                assertEquals("listed", repository.listTenants(10).getFirst().id());
                assertEquals(active.id(), repository.listOperations(true, 10).getFirst().id());
                assertEquals(2, repository.listOperations(false, 10).size());
        }

    @Test
    void persistsOperationAndPreventsConcurrentRepairLease() {
        var operation = repository.startRepair("tenant_team", Instant.now().plusSeconds(60));
        assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class,
                () -> repository.startRepair("tenant_other", Instant.now().plusSeconds(60)));

        repository.updateOperation(new ControlPlaneRepository.OperationRecord(
                operation.id(), operation.keyspace(), operation.owner(), "SUCCEEDED", "done", Instant.now()));
        var stored = repository.getOperation(operation.id());
        assertNotNull(stored);
        assertEquals("SUCCEEDED", stored.state());
        repository.releaseRepair(operation.owner());
        assertFalse(repository.startRepair("tenant_other", Instant.now().plusSeconds(60)).id().equals(operation.id()));
    }

        @Test
        void requiresAcknowledgementAfterRepairLeaseExpires() {
                var operation = repository.startRepair("tenant_team", Instant.now().minusSeconds(1));
                assertEquals(1, repository.reconcileExpiredRepair());
                assertEquals("UNKNOWN", repository.getOperation(operation.id()).state());
                assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class,
                                () -> repository.startRepair("tenant_other", Instant.now().plusSeconds(60)));
                repository.acknowledgeUnknownRepair(operation.id());
                assertNotNull(repository.startRepair("tenant_other", Instant.now().plusSeconds(60)));
        }
}
