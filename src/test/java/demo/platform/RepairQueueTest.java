package demo.platform;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

@JdbcTest @ContextConfiguration(classes = ControlPlaneRepository.class)
class RepairQueueTest {
    @Autowired ControlPlaneRepository repository;
    @Autowired JdbcTemplate jdbc;
    @Test void undispatchedClaimCanBeReclaimedButOldOwnerCannotDispatch() {
        var accepted = repository.acceptRepair("tenant_team", Instant.now(), "queued-request");
        assertNull(accepted.operation().owner());
        var old = repository.claimRepair(60);
        assertEquals(accepted.operation().id(), old.id());
        assertNull(repository.claimRepair(60));
        expire(old);
        assertEquals(1, repository.reconcileExpiredRepair());
        var replacement = repository.claimRepair(60);
        assertEquals(old.id(), replacement.id()); assertNotEquals(old.owner(), replacement.owner());
        assertFalse(repository.renewRepair(old.owner(), 60));
        assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class, () -> repository.markDispatched(old));
        repository.markDispatched(replacement);
        assertEquals("RUNNING", repository.getOperation(old.id()).state());
    }
    @Test void dispatchedExpiryBlocksQueueUntilIdempotentAcknowledgement() {
        var first = repository.acceptRepair("tenant_team", Instant.now(), "first");
        var claim = repository.claimRepair(60); repository.markDispatched(claim);
        repository.acceptRepair("tenant_other", Instant.now(), "second");
        expire(claim); repository.reconcileExpiredRepair();
        assertEquals("UNKNOWN", repository.getOperation(first.operation().id()).state());
        assertNull(repository.claimRepair(60)); assertFalse(repository.renewRepair(claim.owner(), 60));
        assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class, () -> repository.updateOperation(new ControlPlaneRepository.OperationRecord(claim.id(), claim.keyspace(), claim.owner(), "UNKNOWN", "stale", Instant.now())));
        repository.acknowledgeUnknownRepair(claim.id()); repository.acknowledgeUnknownRepair(claim.id());
        var next = repository.claimRepair(60); assertEquals("tenant_other", next.keyspace());
        repository.acknowledgeUnknownRepair(claim.id()); assertTrue(repository.ownsRepair(next));
    }
    private void expire(ControlPlaneRepository.OperationRecord op) {
        jdbc.update("UPDATE repair_lock SET expires_at = ? WHERE owner_id = ?", java.sql.Timestamp.from(Instant.now().minusSeconds(60)), op.owner());
    }
}

