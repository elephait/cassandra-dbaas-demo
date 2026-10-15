package demo.platform.repair.jmx;

import demo.platform.repair.OperationRecord;
import demo.platform.repair.OperationRecord.RepairPolicy;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class JmxRepairRepository {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;

    public record Target(String node, String serviceUrl, String hostId) {}
    public record Task(String operationId, int ordinal, String node, String serviceUrl,
                       String hostId, String state, Integer commandId, Long processStartedAt,
                       String actionOwner, String clusterId, String keyspace, Instant startedAt) {}

    public void initialize(OperationRecord operation, RepairPolicy policy, List<Target> targets) {
        transactions.executeWithoutResult(tx -> {
            jdbc.queryForObject("SELECT cluster_id FROM repair_locks WHERE cluster_id = ? FOR UPDATE",
                String.class, operation.clusterId());
            int changed = jdbc.update("UPDATE operations SET repair_transport = 'jmx', state = 'RUNNING', "
                + "detail = 'JMX node plan persisted; awaiting dispatch', updated_at = CURRENT_TIMESTAMP "
                + "WHERE operation_id = ? AND owner_id = ? AND state = 'CLAIMED' AND EXISTS "
                + "(SELECT 1 FROM repair_locks WHERE cluster_id = ? AND owner_id = ? "
                + "AND expires_at > CURRENT_TIMESTAMP)", operation.id().toString(), operation.owner(),
                operation.clusterId(), operation.owner());
            if (changed != 1) throw new IllegalStateException("Initialization ownership lost");
            for (int i = 0; i < targets.size(); i++) {
                Target target = targets.get(i);
                jdbc.update("INSERT INTO repair_node_progress(operation_id, ordinal, node_name, service_url, "
                    + "expected_host_id, state, next_action_at, updated_at) "
                    + "VALUES (?, ?, ?, ?, ?, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                    operation.id().toString(), i, target.node(), target.serviceUrl(), target.hostId());
            }
            // Null expiry means durable reservation, NOT an abandoned worker lease. Node rows
            // carry the short action leases. Expiry reconciliation explicitly excludes JMX runs.
            jdbc.update("UPDATE repair_locks SET expires_at = NULL WHERE cluster_id = ? AND owner_id = ?",
                operation.clusterId(), operation.owner());
        });
    }

    public Task claim(int seconds) {
        return transactions.execute(tx -> {
            var rows = jdbc.query("SELECT n.*, (SELECT cluster_id FROM operations WHERE operation_id = n.operation_id) AS cluster_id, "
                + "(SELECT keyspace_name FROM operations WHERE operation_id = n.operation_id) AS keyspace_name, "
                + "(SELECT updated_at FROM operations WHERE operation_id = n.operation_id) AS run_started_at "
                + "FROM repair_node_progress n WHERE n.state IN ('PENDING','SUBMITTING','RUNNING') "
                + "AND n.next_action_at <= CURRENT_TIMESTAMP "
                + "AND (n.action_owner IS NULL OR n.action_expires_at <= CURRENT_TIMESTAMP) "
                + "AND EXISTS (SELECT 1 FROM operations o WHERE o.operation_id = n.operation_id AND o.state = 'RUNNING' "
                + "AND o.repair_transport = 'jmx') "
                + "AND NOT EXISTS (SELECT 1 FROM repair_node_progress p WHERE p.operation_id = n.operation_id "
                + "AND p.ordinal < n.ordinal AND p.state <> 'SUCCEEDED') "
                + "ORDER BY n.next_action_at, n.operation_id, n.ordinal LIMIT 1 FOR UPDATE SKIP LOCKED",
                (rs, i) -> new Task(rs.getString("operation_id"), rs.getInt("ordinal"), rs.getString("node_name"),
                    rs.getString("service_url"), rs.getString("expected_host_id"), rs.getString("state"),
                    (Integer) rs.getObject("command_id"), (Long) rs.getObject("process_started_at"), null,
                    rs.getString("cluster_id"), rs.getString("keyspace_name"), rs.getTimestamp("run_started_at").toInstant()));
            if (rows.isEmpty()) return null;
            Task row = rows.getFirst();
            String owner = UUID.randomUUID().toString();
            jdbc.update("UPDATE repair_node_progress SET action_owner = ?, action_expires_at = ? "
                + "WHERE operation_id = ? AND ordinal = ?", owner, after(seconds), row.operationId(), row.ordinal());
            return new Task(row.operationId(), row.ordinal(), row.node(), row.serviceUrl(), row.hostId(), row.state(),
                row.commandId(), row.processStartedAt(), owner, row.clusterId(), row.keyspace(), row.startedAt());
        });
    }

    // Last database boundary before a non-idempotent JMX call. If a worker disappears from
    // here onward, no replacement is allowed to resubmit this task automatically.
    public boolean submitting(Task task, long processStartedAt) {
        return jdbc.update("UPDATE repair_node_progress SET state = 'SUBMITTING', process_started_at = ?, "
            + "attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND ordinal = ? "
            + "AND action_owner = ? AND action_expires_at > CURRENT_TIMESTAMP AND state = 'PENDING' "
            + "AND EXISTS (SELECT 1 FROM operations WHERE operation_id = ? AND state = 'RUNNING')",
            processStartedAt, task.operationId(), task.ordinal(), task.actionOwner(), task.operationId()) == 1;
    }

    public void submitted(Task task, int command, int pollSeconds) {
        if (command == 0) { complete(task, "No ranges require repair"); return; }
        fenced(task, "UPDATE repair_node_progress SET state = 'RUNNING', command_id = ?, "
            + "next_action_at = ?, detail = 'JMX repair accepted', action_owner = NULL, action_expires_at = NULL "
            + "WHERE operation_id = ? AND ordinal = ? AND action_owner = ? AND action_expires_at > CURRENT_TIMESTAMP",
            command, after(pollSeconds));
    }

    public void defer(Task task, String detail, int pollSeconds) {
        fenced(task, "UPDATE repair_node_progress SET next_action_at = ?, detail = ?, "
            + "action_owner = NULL, action_expires_at = NULL WHERE operation_id = ? AND ordinal = ? "
            + "AND action_owner = ? AND action_expires_at > CURRENT_TIMESTAMP", after(pollSeconds), truncate(detail));
    }

    public void complete(Task task, String detail) {
        transactions.executeWithoutResult(tx -> {
            lockRun(task);
            fenced(task, "UPDATE repair_node_progress SET state = 'SUCCEEDED', detail = ?, updated_at = CURRENT_TIMESTAMP, "
                + "action_owner = NULL, action_expires_at = NULL WHERE operation_id = ? AND ordinal = ? "
                + "AND action_owner = ? AND action_expires_at > CURRENT_TIMESTAMP", truncate(detail));
            int pending = jdbc.queryForObject("SELECT COUNT(*) FROM repair_node_progress WHERE operation_id = ? "
                + "AND state <> 'SUCCEEDED'", Integer.class, task.operationId());
            if (pending == 0) {
                jdbc.update("UPDATE operations SET state = 'SUCCEEDED', detail = 'All JMX node repairs completed', "
                    + "updated_at = CURRENT_TIMESTAMP WHERE operation_id = ? AND state = 'RUNNING'", task.operationId());
                jdbc.update("UPDATE repair_locks SET owner_id = NULL, expires_at = NULL WHERE cluster_id = ? "
                    + "AND owner_id = (SELECT owner_id FROM operations WHERE operation_id = ?)", task.clusterId(), task.operationId());
            }
        });
    }

    public void unknown(Task task, String detail) {
        transactions.executeWithoutResult(tx -> {
            lockRun(task);
            fenced(task, "UPDATE repair_node_progress SET state = 'UNKNOWN', detail = ?, updated_at = CURRENT_TIMESTAMP, "
                + "action_owner = NULL, action_expires_at = NULL WHERE operation_id = ? AND ordinal = ? "
                + "AND action_owner = ? AND action_expires_at > CURRENT_TIMESTAMP", truncate(detail));
            jdbc.update("UPDATE operations SET state = 'UNKNOWN', detail = ?, updated_at = CURRENT_TIMESTAMP "
                + "WHERE operation_id = ? AND state = 'RUNNING'", truncate(detail), task.operationId());
            // Deliberately retain cluster reservation, even on a Cassandra-reported failure:
            // no automatic new attempt until remote activity has been verified/fenced.
        });
    }

    private void lockRun(Task task) {
        jdbc.queryForObject("SELECT cluster_id FROM repair_locks WHERE cluster_id = ? FOR UPDATE", String.class, task.clusterId());
        String state = jdbc.queryForObject("SELECT state FROM operations WHERE operation_id = ?", String.class, task.operationId());
        if (!"RUNNING".equals(state)) throw new IllegalStateException("Run no longer active");
    }

    private void fenced(Task task, String sql, Object... prefix) {
        Object[] values = java.util.Arrays.copyOf(prefix, prefix.length + 3);
        values[prefix.length] = task.operationId();
        values[prefix.length + 1] = task.ordinal();
        values[prefix.length + 2] = task.actionOwner();
        if (jdbc.update(sql, values) != 1) throw new IllegalStateException("JMX action ownership lost");
    }

    private Timestamp after(int seconds) {
        Instant now = jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", (rs, i) -> rs.getTimestamp(1).toInstant());
        return Timestamp.from(now.plusSeconds(seconds));
    }

    private static String truncate(String value) {
        return value == null || value.length() <= 3900 ? value : value.substring(0, 3900);
    }

    public List<java.util.Map<String, Object>> progress(UUID id) {
        return jdbc.queryForList("SELECT ordinal, node_name, state, command_id, process_started_at, attempts, "
            + "next_action_at, detail, updated_at FROM repair_node_progress WHERE operation_id = ? ORDER BY ordinal", id.toString());
    }
}
