package demo.platform.repair.persistence;

import demo.platform.common.ControlPlaneExceptions;
import demo.platform.common.DatabaseClock;
import demo.platform.repair.NodeProgress;
import demo.platform.repair.OperationRecord;
import demo.platform.repair.RepairState;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Fenced repair state transitions across operations, node progress and the per-cluster reservation.
 *
 * <p>Explicit SQL rather than JPA on purpose: row locks ({@code FOR UPDATE SKIP LOCKED}) and compare-and-set
 * row counts are the correctness mechanism. Every write names the owner it expects; an affected-row count
 * other than one means ownership was lost, and the caller must stop.
 */
@Repository
@RequiredArgsConstructor
public class RepairExecutionRepository {
    // operations.detail is VARCHAR(4000): bounding every write means a long diagnostic never rolls back a transition.
    private static final int MAX_DETAIL_LENGTH = 3900;
    private static final String OPERATION_COLUMNS =
        "o.operation_id, o.cluster_id, o.keyspace_name, o.owner_id, o.state, o.detail, o.delayed, o.updated_at";
    // Fence for every write to a claimed node action: same row, same owner, lease still valid.
    private static final String ACTION_FENCE = " WHERE operation_id = ? AND ordinal = ? AND action_owner = ? "
        + "AND action_expires_at > CURRENT_TIMESTAMP";
    private static final String RELEASE_ACTION = "action_owner = NULL, action_expires_at = NULL";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final DatabaseClock clock;

    // ---------------------------------------------------------------------------------------------
    // Run admission: QUEUED -> CLAIMED -> RUNNING, or back to QUEUED when nothing was dispatched.
    // ---------------------------------------------------------------------------------------------

    /** Oldest due request of a cluster with no active repair. Caller owns the transaction through claim. */
    @Transactional(propagation = Propagation.MANDATORY)
    public OperationRecord selectNextQueued() {
        String due = "o.state = 'QUEUED' AND o.next_attempt_at <= CURRENT_TIMESTAMP";
        // EXISTS rather than JOIN keeps the row lock on repair_locks only, portably across PostgreSQL and H2.
        var eligible = jdbc.queryForList("SELECT l.cluster_id FROM repair_locks l WHERE l.owner_id IS NULL "
            + "AND EXISTS (SELECT 1 FROM clusters c WHERE c.cluster_id = l.cluster_id AND c.enabled = TRUE) "
            + "AND EXISTS (SELECT 1 FROM operations o WHERE o.cluster_id = l.cluster_id AND " + due + ") "
            + "ORDER BY (SELECT MIN(o.updated_at) FROM operations o WHERE o.cluster_id = l.cluster_id AND " + due + "), "
            + "l.cluster_id LIMIT 1 FOR UPDATE SKIP LOCKED", String.class);
        if (eligible.isEmpty()) return null;
        return first(jdbc.query("SELECT " + OPERATION_COLUMNS + " FROM operations o WHERE o.cluster_id = ? AND " + due
            + " ORDER BY o.updated_at, o.operation_id LIMIT 1 FOR UPDATE SKIP LOCKED", (rs, row) -> operation(rs),
            eligible.getFirst()));
    }

    /** Takes the cluster reservation with a short lease; it becomes durable once a node plan is persisted. */
    @Transactional(propagation = Propagation.MANDATORY)
    public OperationRecord claim(OperationRecord operation, int leaseSeconds) {
        String owner = UUID.randomUUID().toString();
        Instant now = clock.now();
        if (jdbc.update("UPDATE repair_locks SET owner_id = ?, expires_at = ? WHERE cluster_id = ? AND owner_id IS NULL",
            owner, Timestamp.from(now.plusSeconds(leaseSeconds)), operation.clusterId()) != 1) return null;
        if (jdbc.update("UPDATE operations SET state = 'CLAIMED', owner_id = ?, updated_at = ? "
            + "WHERE operation_id = ? AND state = 'QUEUED'", owner, Timestamp.from(now), operation.id().toString()) != 1)
            throw new ControlPlaneExceptions.Conflict("Repair claim was lost");
        return new OperationRecord(operation.id(), operation.clusterId(), operation.keyspace(), owner,
            RepairState.CLAIMED, operation.detail(), false, now);
    }

    public int retryCount(UUID operationId) {
        return jdbc.queryForObject("SELECT retry_count FROM operations WHERE operation_id = ?", Integer.class,
            operationId.toString());
    }

    /** Returns a claimed, undispatched request to the queue with a persisted retry time and releases the cluster. */
    public boolean requeueUndispatched(OperationRecord operation, String reason, int delaySeconds) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            lockCluster(operation.clusterId());
            boolean requeued = requeueClaimed(operation.clusterId(), operation.owner(), reason, after(delaySeconds), 1) == 1;
            if (requeued) release(operation.clusterId(), operation.owner());
            return requeued;
        }));
    }

    /**
     * Requeues claims whose worker died before persisting a node plan; nothing was dispatched, so this is
     * always safe. A run with a plan holds a durable reservation (expires_at NULL) and is never selected here.
     */
    public int reconcileExpired() {
        String expired = "owner_id IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP";
        return jdbc.queryForList("SELECT cluster_id FROM repair_locks WHERE " + expired, String.class).stream()
            .mapToInt(clusterId -> {
                Integer requeued = transactions.execute(status -> {
                    var owners = jdbc.queryForList("SELECT owner_id FROM repair_locks WHERE cluster_id = ? AND "
                        + expired + " FOR UPDATE", String.class, clusterId);
                    if (owners.isEmpty()) return 0; // Already renewed, released or reconciled by another replica.
                    String owner = owners.getFirst();
                    int changed = requeueClaimed(clusterId, owner, "Undispatched claim expired; safe to claim again",
                        after(0), 0);
                    // Defensive: never release a reservation a dispatched or uncertain run still holds.
                    if (count("SELECT COUNT(*) FROM operations WHERE cluster_id = ? AND owner_id = ? "
                        + "AND state IN ('RUNNING','UNKNOWN')", clusterId, owner) == 0) release(clusterId, owner);
                    return changed;
                });
                return requeued == null ? 0 : requeued;
            }).sum();
    }

    /** Persists the node plan and makes the reservation durable, atomically with CLAIMED -> RUNNING. */
    public void initialize(OperationRecord operation, List<Target> targets) {
        transactions.executeWithoutResult(status -> {
            lockCluster(operation.clusterId());
            if (jdbc.update("UPDATE operations SET state = 'RUNNING', detail = 'JMX node plan persisted; awaiting dispatch', "
                    + "updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND owner_id = ? AND state = 'CLAIMED' "
                    + "AND EXISTS (SELECT 1 FROM repair_locks WHERE cluster_id = ? AND owner_id = ? "
                    + "AND expires_at > CURRENT_TIMESTAMP)", operation.id().toString(), operation.owner(),
                operation.clusterId(), operation.owner()) != 1)
                throw new IllegalStateException("Initialization ownership lost");
            jdbc.batchUpdate("INSERT INTO repair_node_progress(operation_id, ordinal, node_name, service_url, "
                    + "expected_host_id, state, next_action_at, updated_at) "
                    + "VALUES (?, ?, ?, ?, ?, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                Stream.iterate(0, i -> i < targets.size(), i -> i + 1).map(i -> new Object[]{operation.id().toString(),
                    i, targets.get(i).node(), targets.get(i).serviceUrl(), targets.get(i).hostId()}).toList());
            // NULL expiry = durable reservation until SUCCEEDED or acknowledgement; node rows carry short leases.
            jdbc.update("UPDATE repair_locks SET expires_at = NULL WHERE cluster_id = ? AND owner_id = ?",
                operation.clusterId(), operation.owner());
        });
    }

    /** UNKNOWN -> ACKNOWLEDGED after operator verification; releases the reservation. Idempotent. */
    public void acknowledge(UUID operationId) {
        transactions.executeWithoutResult(status -> {
            OperationRecord existing = get(operationId);
            if (existing == null) throw new ControlPlaneExceptions.NotFound("Unknown operation");
            lockCluster(existing.clusterId());
            existing = get(operationId);
            if (existing.state() == RepairState.ACKNOWLEDGED) return;
            if (existing.state() != RepairState.UNKNOWN)
                throw new ControlPlaneExceptions.Conflict("Operation is not awaiting acknowledgement");
            setOperationState(operationId.toString(), RepairState.UNKNOWN, RepairState.ACKNOWLEDGED,
                "Operator verified Cassandra repair activity and acknowledged the unknown outcome");
            release(existing.clusterId(), existing.owner());
        });
    }

    // ---------------------------------------------------------------------------------------------
    // Node actions: one short-leased submit or poll at a time, in plan order.
    // ---------------------------------------------------------------------------------------------

    /** Claims the next due node action of any running plan; earlier nodes must have succeeded. */
    public Task claimTask(int leaseSeconds) {
        return transactions.execute(status -> {
            Task task = first(jdbc.query("SELECT n.operation_id, n.ordinal, n.node_name, n.service_url, "
                    + "n.expected_host_id, n.state, n.command_id, n.process_started_at, n.submitted_at, n.delayed, "
                    + "(SELECT cluster_id FROM operations WHERE operation_id = n.operation_id) AS cluster_id, "
                    + "(SELECT keyspace_name FROM operations WHERE operation_id = n.operation_id) AS keyspace_name "
                    + "FROM repair_node_progress n WHERE n.state IN ('PENDING','SUBMITTING','RUNNING') "
                    + "AND n.next_action_at <= CURRENT_TIMESTAMP "
                    + "AND (n.action_owner IS NULL OR n.action_expires_at <= CURRENT_TIMESTAMP) "
                    + "AND EXISTS (SELECT 1 FROM operations o WHERE o.operation_id = n.operation_id AND o.state = 'RUNNING') "
                    + "AND NOT EXISTS (SELECT 1 FROM repair_node_progress p WHERE p.operation_id = n.operation_id "
                    + "AND p.ordinal < n.ordinal AND p.state <> 'SUCCEEDED') "
                    + "ORDER BY n.next_action_at, n.operation_id, n.ordinal LIMIT 1 FOR UPDATE SKIP LOCKED",
                (rs, i) -> new Task(rs.getString("operation_id"), rs.getInt("ordinal"), rs.getString("node_name"),
                    rs.getString("service_url"), rs.getString("expected_host_id"), rs.getString("state"),
                    (Integer) rs.getObject("command_id"), (Long) rs.getObject("process_started_at"),
                    instant(rs, "submitted_at"), rs.getBoolean("delayed"), null,
                    rs.getString("cluster_id"), rs.getString("keyspace_name"))));
            if (task == null) return null;
            String owner = UUID.randomUUID().toString();
            jdbc.update("UPDATE repair_node_progress SET action_owner = ?, action_expires_at = ? "
                + "WHERE operation_id = ? AND ordinal = ?", owner, after(leaseSeconds), task.operationId(), task.ordinal());
            return task.withActionOwner(owner);
        });
    }

    /**
     * Last database boundary before the non-idempotent JMX submit. From here on, a worker that disappears
     * leaves SUBMITTING behind, and no replacement may resubmit automatically.
     */
    public boolean submitting(Task task, long processStartedAt) {
        return jdbc.update("UPDATE repair_node_progress SET state = 'SUBMITTING', process_started_at = ?, "
                + "submitted_at = CURRENT_TIMESTAMP, attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP"
                + ACTION_FENCE + " AND state = 'PENDING' "
                + "AND EXISTS (SELECT 1 FROM operations WHERE operation_id = ? AND state = 'RUNNING')",
            processStartedAt, task.operationId(), task.ordinal(), task.actionOwner(), task.operationId()) == 1;
    }

    public void submitted(Task task, int command, int pollSeconds) {
        if (command == 0) complete(task, "No ranges require repair");
        else updateNode(task, "state = 'RUNNING', command_id = ?, next_action_at = ?, detail = 'JMX repair accepted', "
            + "updated_at = CURRENT_TIMESTAMP, " + RELEASE_ACTION, command, after(pollSeconds));
    }

    /** Releases the action lease and schedules the next observation; never resubmits. */
    public void defer(Task task, String detail, int pollSeconds) {
        updateNode(task, "next_action_at = ?, detail = ?, " + RELEASE_ACTION, after(pollSeconds), truncate(detail));
    }

    /** Flags a node, and its operation, as running past the soft threshold. Polling continues. */
    public void markDelayed(Task task, String detail) {
        transactions.executeWithoutResult(status -> {
            updateNode(task, "delayed = TRUE, detail = ?, updated_at = CURRENT_TIMESTAMP", truncate(detail));
            jdbc.update("UPDATE operations SET delayed = TRUE, detail = ?, updated_at = CURRENT_TIMESTAMP "
                + "WHERE operation_id = ? AND state = 'RUNNING'", truncate(detail), task.operationId());
        });
    }

    /** Node succeeded; the last node completes the operation and releases the reservation atomically. */
    public void complete(Task task, String detail) {
        finishNode(task, RepairState.SUCCEEDED, detail);
    }

    /** Uncertain outcome. The reservation is deliberately retained until an operator acknowledges. */
    public void unknown(Task task, String detail) {
        finishNode(task, RepairState.UNKNOWN, detail);
    }

    public List<NodeProgress> nodeProgress(UUID operationId) {
        return jdbc.query("SELECT ordinal, node_name, state, command_id, process_started_at, attempts, submitted_at, "
                + "delayed, next_action_at, detail, updated_at FROM repair_node_progress WHERE operation_id = ? "
                + "ORDER BY ordinal",
            (rs, i) -> new NodeProgress(rs.getInt("ordinal"), rs.getString("node_name"), rs.getString("state"),
                (Integer) rs.getObject("command_id"), (Long) rs.getObject("process_started_at"),
                rs.getInt("attempts"), instant(rs, "submitted_at"), rs.getBoolean("delayed"),
                instant(rs, "next_action_at"), rs.getString("detail"), instant(rs, "updated_at")),
            operationId.toString());
    }

    // ---------------------------------------------------------------------------------------------
    // Shared building blocks
    // ---------------------------------------------------------------------------------------------

    /** Terminal node transition under the cluster lock, propagated to the operation in the same transaction. */
    private void finishNode(Task task, RepairState outcome, String detail) {
        transactions.executeWithoutResult(status -> {
            lockCluster(task.clusterId());
            if (!RepairState.RUNNING.name().equals(jdbc.queryForObject("SELECT state FROM operations "
                + "WHERE operation_id = ?", String.class, task.operationId())))
                throw new IllegalStateException("Run no longer active");
            updateNode(task, "state = ?, detail = ?, updated_at = CURRENT_TIMESTAMP, " + RELEASE_ACTION,
                outcome.name(), truncate(detail));
            if (outcome == RepairState.UNKNOWN) {
                setOperationState(task.operationId(), RepairState.RUNNING, RepairState.UNKNOWN, detail);
            } else if (count("SELECT COUNT(*) FROM repair_node_progress WHERE operation_id = ? "
                + "AND state <> 'SUCCEEDED'", task.operationId()) == 0) {
                String owner = jdbc.queryForObject("SELECT owner_id FROM operations WHERE operation_id = ?",
                    String.class, task.operationId());
                setOperationState(task.operationId(), RepairState.RUNNING, RepairState.SUCCEEDED,
                    "All JMX node repairs completed");
                release(task.clusterId(), owner);
            }
        });
    }

    /** Fenced update of a claimed node action; losing the fence throws, so a stale worker stops. */
    private void updateNode(Task task, String assignments, Object... values) {
        Object[] arguments = Stream.concat(Stream.of(values),
            Stream.of(task.operationId(), task.ordinal(), task.actionOwner())).toArray();
        if (jdbc.update("UPDATE repair_node_progress SET " + assignments + ACTION_FENCE, arguments) != 1)
            throw new IllegalStateException("JMX action ownership lost");
    }

    private int requeueClaimed(String clusterId, String owner, String detail, Timestamp nextAttempt, int retryIncrement) {
        return jdbc.update("UPDATE operations SET state = 'QUEUED', owner_id = NULL, detail = ?, next_attempt_at = ?, "
                + "retry_count = retry_count + ?, updated_at = CURRENT_TIMESTAMP "
                + "WHERE cluster_id = ? AND owner_id = ? AND state = 'CLAIMED'",
            truncate(detail), nextAttempt, retryIncrement, clusterId, owner);
    }

    private void setOperationState(String operationId, RepairState from, RepairState to, String detail) {
        jdbc.update("UPDATE operations SET state = ?, detail = ?, updated_at = CURRENT_TIMESTAMP "
            + "WHERE operation_id = ? AND state = ?", to.name(), truncate(detail), operationId, from.name());
    }

    private void lockCluster(String clusterId) {
        jdbc.queryForObject("SELECT cluster_id FROM repair_locks WHERE cluster_id = ? FOR UPDATE", String.class, clusterId);
    }

    private void release(String clusterId, String owner) {
        jdbc.update("UPDATE repair_locks SET owner_id = NULL, expires_at = NULL WHERE cluster_id = ? AND owner_id = ?",
            clusterId, owner);
    }

    private int count(String sql, Object... arguments) {
        Integer count = jdbc.queryForObject(sql, Integer.class, arguments);
        return count == null ? 0 : count;
    }

    private OperationRecord get(UUID id) {
        return first(jdbc.query("SELECT " + OPERATION_COLUMNS + " FROM operations o WHERE o.operation_id = ?",
            (rs, row) -> operation(rs), id.toString()));
    }

    private Timestamp after(int seconds) {
        return Timestamp.from(clock.now().plusSeconds(seconds));
    }

    private static <T> T first(List<T> rows) {
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private static String truncate(String detail) {
        return detail == null || detail.length() <= MAX_DETAIL_LENGTH ? detail : detail.substring(0, MAX_DETAIL_LENGTH);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private static OperationRecord operation(ResultSet rs) throws SQLException {
        return new OperationRecord(UUID.fromString(rs.getString("operation_id")), rs.getString("cluster_id"),
            rs.getString("keyspace_name"), rs.getString("owner_id"), RepairState.valueOf(rs.getString("state")),
            rs.getString("detail"), rs.getBoolean("delayed"), instant(rs, "updated_at"));
    }

    public record Target(String node, String serviceUrl, String hostId) {
    }

    /** One claimed node action. {@code submittedAt} is null until the node's repair has been submitted. */
    public record Task(String operationId, int ordinal, String node, String serviceUrl, String hostId,
                       String state, Integer commandId, Long processStartedAt, Instant submittedAt,
                       boolean delayed, String actionOwner, String clusterId, String keyspace) {
        Task withActionOwner(String owner) {
            return new Task(operationId, ordinal, node, serviceUrl, hostId, state, commandId, processStartedAt,
                submittedAt, delayed, owner, clusterId, keyspace);
        }
    }
}
