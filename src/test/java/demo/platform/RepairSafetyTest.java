package demo.platform;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.test.context.ContextConfiguration;
import java.nio.file.Path;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

@JdbcTest
@ContextConfiguration(classes = ControlPlaneRepository.class)
class RepairSafetyTest {
    @Autowired ControlPlaneRepository repository;

    @Test void timeoutRetainsOwnershipUntilAcknowledgement() {
        var properties = new CassandraProperties();
        properties.getRepair().setNodes("node1");
        properties.getRepair().setCommand("fake {node} {keyspace}");
        var service = new RepairService(properties, repository, null) {
            @Override void executeCommand(String command, Path log) throws Exception {
                throw new java.util.concurrent.TimeoutException("simulated lost CLI response");
            }
        };
        var op = claimedRepair("tenant_team", Instant.now().plusSeconds(60));
        try {
            service.run(op);
            assertEquals("UNKNOWN", repository.getOperation(op.id()).state());
            assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class,
                    () -> claimedRepair("tenant_other", Instant.now().plusSeconds(60)));
            repository.acknowledgeUnknownRepair(op.id());
            assertNotNull(claimedRepair("tenant_other", Instant.now().plusSeconds(60)));
        } finally { service.close(); }
    }

    @Test void confirmedCompletionReleasesOwnershipInSameTransaction() {
        var op = claimedRepair("tenant_team", Instant.now().plusSeconds(60));
        repository.finishRepair(new ControlPlaneRepository.OperationRecord(op.id(), op.keyspace(), op.owner(),
                "SUCCEEDED", "done", Instant.now()));
        assertEquals("SUCCEEDED", repository.getOperation(op.id()).state());
        var next = claimedRepair("tenant_other", Instant.now().plusSeconds(60));
        assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class,
                () -> repository.finishRepair(new ControlPlaneRepository.OperationRecord(op.id(), op.keyspace(), op.owner(),
                        "SUCCEEDED", "stale completion", Instant.now())));
        assertEquals("RUNNING", repository.getOperation(next.id()).state());
    }

    @Test void failedTerminalWriteDoesNotReleaseOwnership() {
        var op = claimedRepair("tenant_team", Instant.now().plusSeconds(60));
        assertThrows(org.springframework.dao.DataIntegrityViolationException.class,
                () -> repository.finishRepair(new ControlPlaneRepository.OperationRecord(op.id(), op.keyspace(), op.owner(),
                        "SUCCEEDED", "x".repeat(5000), Instant.now())));
        assertEquals("RUNNING", repository.getOperation(op.id()).state());
        assertThrows(ControlPlaneRepository.ControlPlaneConflictException.class,
                () -> claimedRepair("tenant_other", Instant.now().plusSeconds(60)));
    }
    @Autowired org.springframework.jdbc.core.JdbcTemplate queueJdbc;
    private ControlPlaneRepository.OperationRecord claimedRepair(String keyspace, Instant expiry) {
        Integer occupied = queueJdbc.queryForObject("SELECT COUNT(*) FROM repair_lock WHERE owner_id IS NOT NULL", Integer.class);
        if (occupied != null && occupied > 0) throw new ControlPlaneRepository.ControlPlaneConflictException("Repair already running");
        repository.acceptRepair(keyspace, expiry, java.util.UUID.randomUUID().toString());
        var op = repository.claimRepair(60);
        repository.markDispatched(op);
        queueJdbc.update("UPDATE repair_lock SET expires_at = ? WHERE owner_id = ?", java.sql.Timestamp.from(expiry), op.owner());
        return repository.getOperation(op.id());
    }}


