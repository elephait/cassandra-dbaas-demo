package demo.platform.repair.persistence;

import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.RepairState;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

// Explicit SQL is limited to SKIP LOCKED claims and atomic owner/lease-fenced queue transitions.
@Repository
@RequiredArgsConstructor
public class RepairExecutionRepository {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ClusterRepository clusters;
    private final RepairQueueRepository queue;
    private final RepairPolicyRepository repairPolicies;

    public OperationRecord claim() {
        return transactions.execute(status -> claimSelected(null));
    }

    public OperationRecord claim(int leaseSeconds) {
        if (leaseSeconds < 3) throw new IllegalArgumentException("Repair lease must be at least three seconds");
        return transactions.execute(status -> claimSelected(leaseSeconds));
    }

    private OperationRecord claimSelected(Integer requestedLeaseSeconds) {
        var eligible = jdbc.query("SELECT l.cluster_id FROM repair_locks l WHERE l.owner_id IS NULL "
                + "AND EXISTS (SELECT 1 FROM clusters c WHERE c.cluster_id = l.cluster_id AND c.enabled = TRUE) "
                + "AND EXISTS (SELECT 1 FROM operations o WHERE o.cluster_id = l.cluster_id AND o.state = 'QUEUED' "
                + "AND o.next_attempt_at <= CURRENT_TIMESTAMP) ORDER BY (SELECT MIN(q.updated_at) FROM operations q "
                + "WHERE q.cluster_id = l.cluster_id AND q.state = 'QUEUED' AND q.next_attempt_at <= CURRENT_TIMESTAMP), "
                + "l.cluster_id LIMIT 1 FOR UPDATE SKIP LOCKED",
            (rs, row) -> rs.getString(1));
        if (eligible.isEmpty()) return null;
        String clusterId = eligible.getFirst();
        var queued = jdbc.query("SELECT o.operation_id, o.cluster_id, o.keyspace_name, o.owner_id, o.state, "
                + "o.detail, o.repair_policy_version, o.updated_at FROM operations o WHERE o.state = 'QUEUED' "
                + "AND o.cluster_id = ? AND o.next_attempt_at <= CURRENT_TIMESTAMP "
                + "AND EXISTS (SELECT 1 FROM repair_locks l WHERE l.cluster_id = o.cluster_id "
                + "AND l.owner_id IS NULL) ORDER BY o.updated_at, o.operation_id "
                + "LIMIT 1 FOR UPDATE SKIP LOCKED",
            (rs, row) -> RepairQueueRepository.map(rs), clusterId);
        if (queued.isEmpty()) return null;
        OperationRecord operation = queued.getFirst();
        var cluster = clusters.get(operation.clusterId());
        if (cluster == null || !cluster.enabled()) return null;
        RepairPolicy policy = repairPolicies.get(operation.clusterId());
        if (policy == null || policy.version() != operation.repairPolicyVersion()) return null;
        int leaseSeconds = requestedLeaseSeconds == null
            ? Math.toIntExact(policy.leaseMinutes() * 60L) : requestedLeaseSeconds;
        String owner = UUID.randomUUID().toString();
        int locked = jdbc.update("UPDATE repair_locks SET owner_id = ?, expires_at = ? "
                + "WHERE cluster_id = ? AND owner_id IS NULL",
            owner, Timestamp.from(databaseTime().plusSeconds(leaseSeconds)), operation.clusterId());
        if (locked != 1) return null;
        int claimed = jdbc.update("UPDATE operations SET state = 'CLAIMED', owner_id = ?, "
                + "updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND state = 'QUEUED'",
            owner, operation.id().toString());
        if (claimed != 1) throw new ControlPlaneExceptions.Conflict("Repair claim was lost");
        return queue.get(operation.id());
    }

    public boolean renew(String owner, int seconds) {
        return jdbc.update("UPDATE repair_locks SET expires_at = ? WHERE owner_id = ? "
                + "AND expires_at > CURRENT_TIMESTAMP AND EXISTS (SELECT 1 FROM operations "
                + "WHERE owner_id = ? AND state IN ('CLAIMED','RUNNING'))",
            Timestamp.from(databaseTime().plusSeconds(seconds)), owner, owner) == 1;
    }

    public boolean owns(OperationRecord operation) {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM repair_locks l JOIN operations o "
                + "ON o.cluster_id = l.cluster_id AND o.owner_id = l.owner_id WHERE l.cluster_id = ? "
                + "AND l.owner_id = ? AND l.expires_at > CURRENT_TIMESTAMP AND o.operation_id = ? "
                + "AND o.state IN ('CLAIMED','RUNNING')",
            Integer.class, operation.clusterId(), operation.owner(), operation.id().toString());
        return count != null && count == 1;
    }

    public boolean requeueUndispatched(OperationRecord operation) {
        return requeueUndispatched(operation, "Undispatched attempt deferred", 0);
    }

    public boolean deferUndispatched(OperationRecord operation, String reason) {
        int attempts = queue.retryCount(operation.id());
        int delay = Math.min(300, 5 << Math.min(attempts, 6));
        return requeueUndispatched(operation, reason, delay);
    }

    private boolean requeueUndispatched(OperationRecord operation, String reason, int delay) {
        return transactions.execute(status -> {
            lock(operation.clusterId());
            int changed = jdbc.update("UPDATE operations SET state = 'QUEUED', owner_id = NULL, "
                    + "detail = ?, next_attempt_at = ?, retry_count = retry_count + 1, updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND owner_id = ? "
                    + "AND state = 'CLAIMED' AND EXISTS (SELECT 1 FROM repair_locks "
                    + "WHERE cluster_id = ? AND owner_id = ?)",
                reason.substring(0, Math.min(reason.length(), 3500)), Timestamp.from(databaseTime().plusSeconds(delay)),
                operation.id().toString(), operation.owner(), operation.clusterId(), operation.owner());
            if (changed == 1) release(operation.clusterId(), operation.owner());
            return changed == 1;
        });
    }

    public void markDispatched(OperationRecord operation) {
        transactions.executeWithoutResult(status -> {
            lock(operation.clusterId());
            if (!owns(operation)) throw new ControlPlaneExceptions.Conflict("Repair ownership lost before dispatch");
            int changed = jdbc.update("UPDATE operations SET state = 'RUNNING', "
                    + "detail = 'Remote dispatch may have begun', updated_at = CURRENT_TIMESTAMP "
                    + "WHERE operation_id = ? AND owner_id = ? AND state IN ('CLAIMED','RUNNING')",
                operation.id().toString(), operation.owner());
            if (changed != 1) throw new ControlPlaneExceptions.Conflict("Repair is no longer claimable");
        });
    }

    public int reconcileExpired() {
        List<String> clusterIds = jdbc.query("SELECT cluster_id FROM repair_locks WHERE owner_id IS NOT NULL "
            + "AND expires_at <= CURRENT_TIMESTAMP", (rs, row) -> rs.getString(1));
        return clusterIds.stream().mapToInt(this::reconcileExpired).sum();
    }

    private int reconcileExpired(String clusterId) {
        return transactions.execute(status -> {
            var rows = jdbc.queryForList("SELECT owner_id, expires_at FROM repair_locks "
                + "WHERE cluster_id = ? FOR UPDATE", clusterId);
            if (rows.isEmpty()) return 0;
            String owner = (String) rows.getFirst().get("owner_id");
            Timestamp expires = (Timestamp) rows.getFirst().get("expires_at");
            if (owner == null || expires == null || expires.toInstant().isAfter(databaseTime())) return 0;
            int requeued = jdbc.update("UPDATE operations SET state = 'QUEUED', owner_id = NULL, "
                + "detail = 'Undispatched claim expired; safe to claim again', "
                + "updated_at = CURRENT_TIMESTAMP WHERE cluster_id = ? AND owner_id = ? "
                + "AND state = 'CLAIMED'", clusterId, owner);
            if (requeued > 0) {
                release(clusterId, owner);
                return requeued;
            }
            return jdbc.update("UPDATE operations SET state = 'UNKNOWN', detail = ?, "
                    + "updated_at = CURRENT_TIMESTAMP WHERE cluster_id = ? AND owner_id = ? "
                    + "AND state = 'RUNNING'",
                "Worker lease expired; verify Cassandra repair activity before acknowledgement", clusterId, owner);
        });
    }

    public void acknowledge(UUID operationId) {
        transactions.executeWithoutResult(status -> {
            OperationRecord existing = queue.get(operationId);
            if (existing == null)
                throw new ControlPlaneExceptions.Conflict("Operation is not awaiting acknowledgement");
            lock(existing.clusterId());
            existing = queue.get(operationId);
            if (existing.state() == RepairState.ACKNOWLEDGED) return;
            if (existing.state() != RepairState.UNKNOWN || existing.owner() == null)
                throw new ControlPlaneExceptions.Conflict("Operation is not awaiting acknowledgement");
            int changed = jdbc.update("UPDATE operations SET state = 'ACKNOWLEDGED', detail = ?, "
                    + "updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND state = 'UNKNOWN'",
                "Operator verified Cassandra repair activity and acknowledged the unknown outcome",
                operationId.toString());
            if (changed != 1) throw new ControlPlaneExceptions.Conflict("Operation acknowledgement was lost");
            release(existing.clusterId(), existing.owner());
        });
    }

    public void markUnknown(OperationRecord operation) {
        if (operation.state() != RepairState.UNKNOWN)
            throw new IllegalArgumentException("Only UNKNOWN is allowed here");
        int changed = jdbc.update("UPDATE operations SET state = 'UNKNOWN', detail = ?, updated_at = ? "
                + "WHERE operation_id = ? AND cluster_id = ? AND owner_id = ? "
                + "AND state IN ('CLAIMED','RUNNING') AND EXISTS (SELECT 1 FROM repair_locks "
                + "WHERE cluster_id = ? AND owner_id = ?)",
            operation.detail(), Timestamp.from(operation.updated()), operation.id().toString(),
            operation.clusterId(), operation.owner(), operation.clusterId(), operation.owner());
        if (changed != 1) throw new ControlPlaneExceptions.Conflict("Repair ownership/state lost");
    }

    public void finish(OperationRecord operation) {
        if (!operation.state().confirmedTerminal())
            throw new IllegalArgumentException("Only confirmed terminal outcomes can release repair ownership");
        transactions.executeWithoutResult(status -> {
            lock(operation.clusterId());
            if (!owns(operation)) throw new ControlPlaneExceptions.Conflict("Repair ownership was lost");
            int changed = jdbc.update("UPDATE operations SET state = ?, detail = ?, updated_at = ? "
                    + "WHERE operation_id = ? AND cluster_id = ? AND owner_id = ? "
                    + "AND state IN ('CLAIMED','RUNNING')",
                operation.state().name(), operation.detail(), Timestamp.from(operation.updated()),
                operation.id().toString(), operation.clusterId(), operation.owner());
            if (changed != 1) throw new ControlPlaneExceptions.Conflict("Repair is no longer active");
            release(operation.clusterId(), operation.owner());
        });
    }

    private void lock(String clusterId) {
        jdbc.queryForObject("SELECT cluster_id FROM repair_locks WHERE cluster_id = ? FOR UPDATE",
            String.class, clusterId);
    }

    private void release(String clusterId, String owner) {
        jdbc.update("UPDATE repair_locks SET owner_id = NULL, expires_at = NULL "
            + "WHERE cluster_id = ? AND owner_id = ?", clusterId, owner);
    }

    private Instant databaseTime() {
        return jdbc.queryForObject("SELECT CURRENT_TIMESTAMP",
            (rs, row) -> rs.getTimestamp(1).toInstant());
    }
}
