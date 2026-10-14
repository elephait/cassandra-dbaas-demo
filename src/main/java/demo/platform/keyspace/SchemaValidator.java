package demo.platform.keyspace;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.Map;

public final class SchemaValidator {
    private SchemaValidator() {
    }

    public static void validateKeyspace(CqlSession session, String keyspace, String dc,
                                        int replicationFactor, boolean allowMissing) {
        var row = session.execute(SimpleStatement.newInstance(
            "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspace)).one();
        if (row == null) {
            if (allowMissing) return;
            throw conflict("Keyspace " + keyspace + " is missing");
        }
        validateReplication(row.getMap("replication", String.class, String.class), dc, replicationFactor);
    }

    public static void validate(CqlSession session, String keyspace, String dc, int replicationFactor) {
        if (!session.checkSchemaAgreement())
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Cassandra schema agreement is pending; retry provisioning");
        validateKeyspace(session, keyspace, dc, replicationFactor, false);
        Map<String, Column> columns = new HashMap<>();
        session.execute(SimpleStatement.newInstance(
                "SELECT column_name, kind, position, type FROM system_schema.columns "
                    + "WHERE keyspace_name = ? AND table_name = ?", keyspace, "probe"))
            .forEach(row -> columns.put(row.getString("column_name"),
                new Column(row.getString("kind"), row.getInt("position"), row.getString("type"))));
        validateColumns(columns);
    }

    public static void validateReplication(Map<String, String> replication, String dc, int replicationFactor) {
        Map<String, String> actual = new HashMap<>(replication);
        String strategy = actual.remove("class");
        if (!("NetworkTopologyStrategy".equals(strategy)
            || "org.apache.cassandra.locator.NetworkTopologyStrategy".equals(strategy))
            || !actual.equals(Map.of(dc, Integer.toString(replicationFactor)))) {
            throw conflict("Keyspace replication mismatch: expected NetworkTopologyStrategy {" + dc + ":"
                + replicationFactor + "}, actual " + replication
                + ". Explicit schema reconciliation is required");
        }
    }

    public static void validateColumns(Map<String, Column> columns) {
        var expected = Map.of("id", new Column("partition_key", 0, "uuid"),
            "value", new Column("regular", -1, "text"));
        if (!expected.equals(columns))
            throw conflict("Probe table mismatch: expected id uuid PRIMARY KEY and value text; actual " + columns);
    }

    private static ResponseStatusException conflict(String message) {
        return new ResponseStatusException(HttpStatus.CONFLICT, message);
    }

    public record Column(String kind, int position, String type) {
    }
}
