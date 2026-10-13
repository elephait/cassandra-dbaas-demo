package demo.platform;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.util.HashMap;
import java.util.Map;

/** Check actual system schema rather than interpreting IF NOT EXISTS as validation. */
final class SchemaValidator {
    private SchemaValidator() {}

    static void validateKeyspace(CqlSession session, String keyspace, String dc, int rf, boolean allowMissing) {
        var row = session.execute(SimpleStatement.newInstance(
                "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspace)).one();
        if (row == null) {
            if (allowMissing) return;
            throw conflict("Keyspace " + keyspace + " is missing");
        }
        validateReplication(row.getMap("replication", String.class, String.class), dc, rf);
    }

    static void validate(CqlSession session, String keyspace, String dc, int rf) {
        if (!session.checkSchemaAgreement())
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Cassandra schema agreement is pending; retry provisioning");
        validateKeyspace(session, keyspace, dc, rf, false);
        Map<String, Column> columns = new HashMap<>();
        session.execute(SimpleStatement.newInstance(
                "SELECT column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
                keyspace, "probe")).forEach(row -> columns.put(row.getString("column_name"),
                new Column(row.getString("kind"), row.getInt("position"), row.getString("type"))));
        validateColumns(columns);
    }

    static void validateReplication(Map<String, String> replication, String dc, int rf) {
        Map<String, String> actual = new HashMap<>(replication);
        String strategy = actual.remove("class");
        if (!("NetworkTopologyStrategy".equals(strategy) || "org.apache.cassandra.locator.NetworkTopologyStrategy".equals(strategy))
                || !actual.equals(Map.of(dc, Integer.toString(rf))))
            throw conflict("Keyspace replication mismatch: expected NetworkTopologyStrategy {" + dc + ":" + rf
                    + "}, actual " + replication + ". Explicit schema reconciliation is required");
    }

    static void validateColumns(Map<String, Column> columns) {
        var expected = Map.of("id", new Column("partition_key", 0, "uuid"), "value", new Column("regular", -1, "text"));
        if (!expected.equals(columns)) throw conflict("Probe table mismatch: expected id uuid PRIMARY KEY and value text; actual " + columns);
    }

    record Column(String kind, int position, String type) {}
    private static ResponseStatusException conflict(String message) {
        return new ResponseStatusException(HttpStatus.CONFLICT, message);
    }
}
