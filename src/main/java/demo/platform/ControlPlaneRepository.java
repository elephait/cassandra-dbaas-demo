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

    public OperationRecord startRepair(String keyspace, Instant leaseExpiry) {
        return transactions.execute(status -> {
            String owner = UUID.randomUUID().toString();
            var lock = jdbc.queryForMap("SELECT owner_id, expires_at FROM repair_lock WHERE id = ? FOR UPDATE", LOCK_ROW);
            Timestamp expires = (Timestamp) lock.get("expires_at");
            if (lock.get("owner_id") != null)
                throw new ControlPlaneConflictException(expires != null && expires.toInstant().isAfter(Instant.now())
                        ? "Repair already running" : "Previous repair outcome is unknown; operator acknowledgement required");
            UUID id = UUID.randomUUID();
            jdbc.update("UPDATE repair_lock SET owner_id = ?, expires_at = ? WHERE id = ?", owner, Timestamp.from(leaseExpiry), LOCK_ROW);
            jdbc.update("INSERT INTO operations (operation_id, keyspace_name, owner_id, state, detail, updated_at) VALUES (?, ?, ?, 'QUEUED', ?, ?)",
                    id.toString(), keyspace, owner, "Approved repair queued", now());
            return new OperationRecord(id, keyspace, owner, "QUEUED", "Approved repair queued", Instant.now());
        });
    }

    public boolean renewRepair(String owner, Instant leaseExpiry) {
        return jdbc.update("UPDATE repair_lock SET expires_at = ? WHERE id = ? AND owner_id = ?",
                Timestamp.from(leaseExpiry), LOCK_ROW, owner) == 1;
    }

    public int reconcileExpiredRepair() {
        return transactions.execute(status -> {
            var lock = jdbc.queryForMap("SELECT owner_id, expires_at FROM repair_lock WHERE id = ? FOR UPDATE", LOCK_ROW);
            String owner = (String) lock.get("owner_id");
            Timestamp expires = (Timestamp) lock.get("expires_at");
            if (owner == null || expires == null || expires.toInstant().isAfter(Instant.now()))
                return 0;
            return jdbc.update("UPDATE operations SET state = 'UNKNOWN', detail = ?, updated_at = ? WHERE owner_id = ? AND state IN ('QUEUED', 'RUNNING')",
                    "Worker lease expired; verify Cassandra repair activity before acknowledgement", now(), owner);
        });
    }

    public void acknowledgeUnknownRepair(UUID operationId) {
        transactions.executeWithoutResult(status -> {
            var lock = jdbc.queryForMap("SELECT owner_id FROM repair_lock WHERE id = ? FOR UPDATE", LOCK_ROW);
            String owner;
            try {
                owner = jdbc.queryForObject("SELECT owner_id FROM operations WHERE operation_id = ? AND state = 'UNKNOWN'",
                        String.class, operationId.toString());
            } catch (EmptyResultDataAccessException exception) {
                throw new ControlPlaneConflictException("Operation is not awaiting acknowledgement");
            }
            if (owner == null || !owner.equals(lock.get("owner_id")))
                throw new ControlPlaneConflictException("Operation is not the unresolved repair");
            jdbc.update("UPDATE operations SET detail = ?, updated_at = ? WHERE operation_id = ?",
                    "Operator verified Cassandra repair activity and acknowledged the unknown outcome", now(), operationId.toString());
            jdbc.update("UPDATE repair_lock SET owner_id = NULL, expires_at = NULL WHERE id = ? AND owner_id = ?", LOCK_ROW, owner);
        });
    }

    public void updateOperation(OperationRecord operation) {
        jdbc.update("UPDATE operations SET state = ?, detail = ?, updated_at = ? WHERE operation_id = ?",
                operation.state(), operation.detail(), Timestamp.from(operation.updated()), operation.id().toString());
    }

    public void releaseRepair(String owner) {
        jdbc.update("UPDATE repair_lock SET owner_id = NULL, expires_at = NULL WHERE id = ? AND owner_id = ?", LOCK_ROW, owner);
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
                + (activeOnly ? " WHERE state IN ('QUEUED', 'RUNNING', 'UNKNOWN')" : "")
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
