package demo.platform;

import lombok.RequiredArgsConstructor;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class ControlPlaneRepository {
    private static final int LOCK_ROW = 1;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;

    public ProvisionDecision beginProvision(String tenant, String keyspace, int maxTenants, Instant leaseExpiry) {
        return transactions.execute(status -> {
            jdbc.queryForObject("SELECT id FROM control_lock WHERE id = ? FOR UPDATE", Integer.class, LOCK_ROW);
            String owner = UUID.randomUUID().toString();
            try {
                var tenantRow = jdbc.queryForMap("SELECT state, owner_id, expires_at FROM tenants WHERE tenant_id = ?", tenant);
                if ("READY".equals(tenantRow.get("state")))
                    return new ProvisionDecision(true, null);
                Timestamp expires = (Timestamp) tenantRow.get("expires_at");
                if (tenantRow.get("owner_id") != null && expires != null && expires.toInstant().isAfter(Instant.now()))
                    throw new ControlPlaneConflictException("Tenant provisioning is already in progress");
                jdbc.update("UPDATE tenants SET owner_id = ?, expires_at = ?, updated_at = ? WHERE tenant_id = ?",
                        owner, Timestamp.from(leaseExpiry), now(), tenant);
                return new ProvisionDecision(false, owner);
            } catch (EmptyResultDataAccessException ignored) {
                Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM tenants WHERE state IN ('PROVISIONING', 'READY')", Integer.class);
                if (count != null && count >= maxTenants)
                    throw new TenantQuotaExceededException();
                jdbc.update("INSERT INTO tenants (tenant_id, keyspace_name, state, owner_id, expires_at, updated_at) VALUES (?, ?, 'PROVISIONING', ?, ?, ?)",
                        tenant, keyspace, owner, Timestamp.from(leaseExpiry), now());
                return new ProvisionDecision(false, owner);
            }
        });
    }

    public void completeProvision(String tenant, String owner) {
        int changed = jdbc.update("UPDATE tenants SET state = 'READY', owner_id = NULL, expires_at = NULL, updated_at = ? WHERE tenant_id = ? AND owner_id = ?",
                now(), tenant, owner);
        if (changed != 1)
            throw new ControlPlaneConflictException("Provisioning ownership was lost");
    }

    public void releaseProvision(String tenant, String owner) {
        jdbc.update("UPDATE tenants SET owner_id = NULL, expires_at = NULL, updated_at = ? WHERE tenant_id = ? AND state = 'PROVISIONING' AND owner_id = ?",
                now(), tenant, owner);
    }

    public RepairDecision acceptRepair(String keyspace, Instant leaseExpiry, String requestKey) {
        return transactions.execute(status -> {
            jdbc.queryForObject("SELECT id FROM control_lock WHERE id = ? FOR UPDATE", Integer.class, LOCK_ROW);
            var previous = findRepair(requestKey, keyspace);
            if (previous != null) return new RepairDecision(previous, false);
            Integer pending = jdbc.queryForObject("SELECT COUNT(*) FROM operations WHERE state = 'QUEUED'", Integer.class);
            if (pending != null && pending >= 1000) throw new ControlPlaneConflictException("Repair queue capacity reached");
            UUID id = UUID.randomUUID();
            jdbc.update("INSERT INTO operations (operation_id, keyspace_name, owner_id, state, detail, updated_at, request_key, request_fingerprint) VALUES (?, ?, ?, 'QUEUED', ?, ?, ?, ?)",
                    id.toString(), keyspace, null, "Approved repair queued", now(), requestKey, fingerprint(keyspace));
            return new RepairDecision(new OperationRecord(id, keyspace, null, "QUEUED", "Approved repair queued", Instant.now()), true);
        });
    }

    public OperationRecord findRepair(String requestKey, String keyspace) {
        var rows = jdbc.queryForList("SELECT operation_id, request_fingerprint FROM operations WHERE request_key = ?", requestKey);
        if (rows.isEmpty()) return null;
        var row = rows.getFirst();
        if (!fingerprint(keyspace).equals(row.get("request_fingerprint")))
            throw new ControlPlaneConflictException("Idempotency-Key was already used for a different repair request");
        return getOperation(UUID.fromString((String) row.get("operation_id")));
    }

    private static String fingerprint(String keyspace) { return "repair:v1:approved=true:" + keyspace; }
    public record RepairDecision(OperationRecord operation, boolean created) {}

    public boolean renewRepair(String owner, Instant leaseExpiry) {
        return jdbc.update("UPDATE repair_lock SET expires_at = ? WHERE id = ? AND owner_id = ? AND expires_at > CURRENT_TIMESTAMP AND EXISTS (SELECT 1 FROM operations WHERE owner_id = ? AND state IN ('CLAIMED','RUNNING'))",
                Timestamp.from(leaseExpiry), LOCK_ROW, owner, owner) == 1;
    }

    public boolean renewRepair(String owner, int seconds) { return renewRepair(owner, databaseTime().plusSeconds(seconds)); }
    private Instant databaseTime() {
        return jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", (rs, row) -> rs.getTimestamp(1).toInstant());
    }
    public OperationRecord claimRepair(int seconds) {
        if (seconds < 3) throw new IllegalArgumentException("Repair lease must be at least three seconds");
        return transactions.execute(status -> {
            var locks = jdbc.queryForList("SELECT owner_id FROM repair_lock WHERE id = ? FOR UPDATE SKIP LOCKED", LOCK_ROW);
            if (locks.isEmpty() || locks.getFirst().get("owner_id") != null) return null;
            var queued = jdbc.query("SELECT operation_id, keyspace_name, owner_id, state, detail, updated_at FROM operations WHERE state = 'QUEUED' ORDER BY updated_at, operation_id LIMIT 1 FOR UPDATE SKIP LOCKED", (rs, row) -> operationRecord(rs));
            if (queued.isEmpty()) return null;
            var op = queued.getFirst(); String owner = UUID.randomUUID().toString();
            jdbc.update("UPDATE repair_lock SET owner_id = ?, expires_at = ? WHERE id = ?", owner, Timestamp.from(databaseTime().plusSeconds(seconds)), LOCK_ROW);
            jdbc.update("UPDATE operations SET state = 'CLAIMED', owner_id = ?, updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND state = 'QUEUED'", owner, op.id().toString());
            return getOperation(op.id());
        });
    }
    public boolean ownsRepair(OperationRecord op) {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM repair_lock l JOIN operations o ON o.owner_id = l.owner_id WHERE l.id = ? AND l.owner_id = ? AND l.expires_at > CURRENT_TIMESTAMP AND o.operation_id = ? AND o.state IN ('CLAIMED','RUNNING')", Integer.class, LOCK_ROW, op.owner(), op.id().toString());
        return count != null && count == 1;
    }
    public boolean requeueUndispatched(OperationRecord op) {
        return transactions.execute(status -> {
            jdbc.queryForObject("SELECT id FROM repair_lock WHERE id = ? FOR UPDATE", Integer.class, LOCK_ROW);
            int changed = jdbc.update("UPDATE operations SET state = 'QUEUED', owner_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND owner_id = ? AND state = 'CLAIMED' AND EXISTS (SELECT 1 FROM repair_lock WHERE id = ? AND owner_id = ?)", op.id().toString(), op.owner(), LOCK_ROW, op.owner());
            if (changed == 1) jdbc.update("UPDATE repair_lock SET owner_id = NULL, expires_at = NULL WHERE id = ? AND owner_id = ?", LOCK_ROW, op.owner());
            return changed == 1;
        });
    }
    public void markDispatched(OperationRecord op) {
        transactions.executeWithoutResult(status -> {
            jdbc.queryForObject("SELECT id FROM repair_lock WHERE id = ? FOR UPDATE", Integer.class, LOCK_ROW);
            if (!ownsRepair(op)) throw new ControlPlaneConflictException("Repair ownership lost before dispatch");
            jdbc.update("UPDATE operations SET state = 'RUNNING', detail = 'Remote dispatch may have begun', updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND owner_id = ? AND state = 'CLAIMED'", op.id().toString(), op.owner());
        });
    }

    public int reconcileExpiredRepair() {
        return transactions.execute(status -> {
            var lock = jdbc.queryForMap("SELECT owner_id, expires_at FROM repair_lock WHERE id = ? FOR UPDATE", LOCK_ROW);
            String owner = (String) lock.get("owner_id");
            Timestamp expires = (Timestamp) lock.get("expires_at");
            if (owner == null || expires == null || expires.toInstant().isAfter(databaseTime()))
                return 0;
            int requeued = jdbc.update("UPDATE operations SET state = 'QUEUED', owner_id = NULL, detail = 'Undispatched claim expired; safe to claim again', updated_at = CURRENT_TIMESTAMP WHERE owner_id = ? AND state = 'CLAIMED'", owner);
            if (requeued > 0) {
                jdbc.update("UPDATE repair_lock SET owner_id = NULL, expires_at = NULL WHERE id = ? AND owner_id = ?", LOCK_ROW, owner);
                return requeued;
            }
            return jdbc.update("UPDATE operations SET state = 'UNKNOWN', detail = ?, updated_at = ? WHERE owner_id = ? AND state IN ('QUEUED', 'RUNNING')",
                    "Worker lease expired; verify Cassandra repair activity before acknowledgement", now(), owner);
        });
    }

    public void acknowledgeUnknownRepair(UUID operationId) {
        transactions.executeWithoutResult(status -> {
            var lock = jdbc.queryForMap("SELECT owner_id FROM repair_lock WHERE id = ? FOR UPDATE", LOCK_ROW);
            var existing = getOperation(operationId);
            if (existing != null && existing.state().equals("ACKNOWLEDGED")) return;
            String owner;
            try {
                owner = jdbc.queryForObject("SELECT owner_id FROM operations WHERE operation_id = ? AND state = 'UNKNOWN'",
                        String.class, operationId.toString());
            } catch (EmptyResultDataAccessException exception) {
                throw new ControlPlaneConflictException("Operation is not awaiting acknowledgement");
            }
            if (owner == null || !owner.equals(lock.get("owner_id")))
                throw new ControlPlaneConflictException("Operation is not the unresolved repair");
            jdbc.update("UPDATE operations SET state = 'ACKNOWLEDGED', detail = ?, updated_at = ? WHERE operation_id = ?",
                    "Operator verified Cassandra repair activity and acknowledged the unknown outcome", now(), operationId.toString());
            jdbc.update("UPDATE repair_lock SET owner_id = NULL, expires_at = NULL WHERE id = ? AND owner_id = ?", LOCK_ROW, owner);
        });
    }

    public void updateOperation(OperationRecord operation) {
        if (!"UNKNOWN".equals(operation.state())) throw new IllegalArgumentException("Worker updates only record uncertain outcomes; use finishRepair for completion");
        int changed = jdbc.update("UPDATE operations SET state = ?, detail = ?, updated_at = ? WHERE operation_id = ? AND owner_id = ? AND state IN ('CLAIMED','RUNNING') AND EXISTS (SELECT 1 FROM repair_lock WHERE id = ? AND owner_id = ?)",
                operation.state(), operation.detail(), Timestamp.from(operation.updated()), operation.id().toString(), operation.owner(), LOCK_ROW, operation.owner());
        if (changed != 1) throw new ControlPlaneConflictException("Repair ownership/state lost");
    }

    public void finishRepair(OperationRecord operation) {
        if (!List.of("SUCCEEDED", "FAILED").contains(operation.state()))
            throw new IllegalArgumentException("Only confirmed terminal outcomes can release repair ownership");
        transactions.executeWithoutResult(status -> {
            String owner = jdbc.queryForObject("SELECT owner_id FROM repair_lock WHERE id = ? FOR UPDATE", String.class, LOCK_ROW);
            if (!operation.owner().equals(owner) || !ownsRepair(operation))
                throw new ControlPlaneConflictException("Repair ownership was lost");
            int changed = jdbc.update("UPDATE operations SET state = ?, detail = ?, updated_at = ? WHERE operation_id = ? AND owner_id = ? AND state IN ('CLAIMED', 'RUNNING')",
                    operation.state(), operation.detail(), Timestamp.from(operation.updated()), operation.id().toString(), owner);
            if (changed != 1)
                throw new ControlPlaneConflictException("Repair is no longer active");
            jdbc.update("UPDATE repair_lock SET owner_id = NULL, expires_at = NULL WHERE id = ? AND owner_id = ?", LOCK_ROW, owner);
        });
    }

    public OperationRecord getOperation(UUID id) {
        try {
            return jdbc.queryForObject("SELECT operation_id, keyspace_name, owner_id, state, detail, updated_at FROM operations WHERE operation_id = ?",
                    (rs, row) -> operationRecord(rs), id.toString());
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<TenantRecord> listTenants(int limit) {
        return jdbc.query("SELECT tenant_id, keyspace_name, state, updated_at FROM tenants ORDER BY updated_at DESC LIMIT ?",
                (rs, row) -> new TenantRecord(rs.getString("tenant_id"), rs.getString("keyspace_name"),
                        rs.getString("state"), rs.getTimestamp("updated_at").toInstant()), limit);
    }

    public List<OperationRecord> listOperations(boolean activeOnly, int limit) {
        String sql = "SELECT operation_id, keyspace_name, owner_id, state, detail, updated_at FROM operations"
                + (activeOnly ? " WHERE state IN ('QUEUED', 'CLAIMED', 'RUNNING', 'UNKNOWN')" : "")
                + " ORDER BY updated_at DESC LIMIT ?";
        return jdbc.query(sql, (rs, row) -> operationRecord(rs), limit);
    }

    private static OperationRecord operationRecord(java.sql.ResultSet result) throws java.sql.SQLException {
        return new OperationRecord(UUID.fromString(result.getString("operation_id")),
                result.getString("keyspace_name"), result.getString("owner_id"), result.getString("state"),
                result.getString("detail"), result.getTimestamp("updated_at").toInstant());
    }

    private static Timestamp now() {
        return Timestamp.from(Instant.now());
    }

    public record ProvisionDecision(boolean alreadyReady, String owner) {
    }

    public record OperationRecord(UUID id, String keyspace, String owner, String state, String detail, Instant updated) {
    }

    public record TenantRecord(String id, String keyspace, String state, Instant updated) {
    }

    public static class TenantQuotaExceededException extends RuntimeException {
    }

    public static class ControlPlaneConflictException extends RuntimeException {
        public ControlPlaneConflictException(String message) {
            super(message);
        }
    }
}

