package demo.platform.keyspace.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.cassandra.CassandraSessions;
import demo.platform.keyspace.KeyspaceNames;
import demo.platform.keyspace.SchemaValidator.Column;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Every CQL statement the keyspace domain issues. Identifiers cannot be bound in CQL DDL, so each keyspace
 * name is re-validated against the strict identifier pattern before it is concatenated.
 */
@Repository
@RequiredArgsConstructor
public class KeyspaceSchemaRepository {
    static final String PROBE_TABLE = "probe";

    private final CassandraSessions sessions;

    private CqlSession session(ClusterRecord.Settings cluster) {
        return sessions.require(cluster);
    }

    /** Fails fast (503) when this replica has no session yet, before any lease is taken. */
    public void requireAvailable(ClusterRecord.Settings cluster) {
        session(cluster);
    }

    public boolean schemaAgreed(ClusterRecord.Settings cluster) {
        return session(cluster).checkSchemaAgreement();
    }

    public Optional<Map<String, String>> replication(ClusterRecord.Settings cluster, String keyspace) {
        var row = session(cluster).execute(SimpleStatement.newInstance(
            "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspace)).one();
        return row == null ? Optional.empty() : Optional.of(row.getMap("replication", String.class, String.class));
    }

    public Map<String, Column> probeColumns(ClusterRecord.Settings cluster, String keyspace) {
        Map<String, Column> columns = new HashMap<>();
        session(cluster).execute(SimpleStatement.newInstance(
                "SELECT column_name, kind, position, type FROM system_schema.columns "
                    + "WHERE keyspace_name = ? AND table_name = ?", keyspace, PROBE_TABLE))
            .forEach(row -> columns.put(row.getString("column_name"),
                new Column(row.getString("kind"), row.getInt("position"), row.getString("type"))));
        return columns;
    }

    public void createKeyspace(ClusterRecord.Settings cluster, String keyspace) {
        session(cluster).execute("CREATE KEYSPACE IF NOT EXISTS " + KeyspaceNames.validate(keyspace)
            + " WITH replication = {'class':'NetworkTopologyStrategy','" + cluster.datacenter().replace("'", "''")
            + "':" + cluster.replicationFactor() + "}");
    }

    public void createProbeTable(ClusterRecord.Settings cluster, String keyspace) {
        session(cluster).execute("CREATE TABLE IF NOT EXISTS " + KeyspaceNames.validate(keyspace) + "." + PROBE_TABLE
            + " (id uuid PRIMARY KEY, value text)");
    }

    public void writeProbe(ClusterRecord.Settings cluster, String keyspace, UUID id, String value) {
        session(cluster).execute(SimpleStatement.newInstance("INSERT INTO " + KeyspaceNames.validate(keyspace) + "."
                + PROBE_TABLE + " (id, value) VALUES (?, ?) USING TTL 3600", id, value)
            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
    }

    public Optional<String> readProbe(ClusterRecord.Settings cluster, String keyspace, UUID id) {
        var row = session(cluster).execute(SimpleStatement.newInstance("SELECT value FROM "
                + KeyspaceNames.validate(keyspace) + "." + PROBE_TABLE + " WHERE id = ?", id)
            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).one();
        return row == null ? Optional.empty() : Optional.ofNullable(row.getString(0));
    }
}
