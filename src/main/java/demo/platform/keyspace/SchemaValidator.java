package demo.platform.keyspace;

import demo.platform.cluster.ClusterRecord;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.keyspace.cassandra.KeyspaceSchemaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Decides whether a keyspace matches what provisioning creates: NetworkTopologyStrategy with exactly the
 * configured datacenter and RF, plus the probe table. The rules are pure functions; reads go through
 * {@link KeyspaceSchemaRepository}.
 */
@Component
@RequiredArgsConstructor
public class SchemaValidator {
    private final KeyspaceSchemaRepository schema;

    /** Full check after schema agreement: keyspace must exist and match. Used by provisioning and repair. */
    public void validate(ClusterRecord.Settings cluster, String keyspace) {
        if (!schema.schemaAgreed(cluster))
            throw new ControlPlaneExceptions.Unavailable("Cassandra schema agreement is pending; retry");
        var replication = schema.replication(cluster, keyspace).orElseThrow(() ->
            new ControlPlaneExceptions.NotFound("Keyspace " + keyspace + " is not provisioned"));
        validateReplication(replication, cluster.datacenter(), cluster.replicationFactor());
        validateColumns(schema.probeColumns(cluster, keyspace));
    }

    /** Before DDL: an absent keyspace is fine, an existing one must not have drifted. */
    public void validateIfPresent(ClusterRecord.Settings cluster, String keyspace) {
        schema.replication(cluster, keyspace).ifPresent(replication ->
            validateReplication(replication, cluster.datacenter(), cluster.replicationFactor()));
    }

    public static void validateReplication(Map<String, String> replication, String dc, int replicationFactor) {
        Map<String, String> actual = new HashMap<>(replication);
        String strategy = actual.remove("class");
        if (!("NetworkTopologyStrategy".equals(strategy)
            || "org.apache.cassandra.locator.NetworkTopologyStrategy".equals(strategy))
            || !actual.equals(Map.of(dc, Integer.toString(replicationFactor))))
            throw new ControlPlaneExceptions.Conflict("Keyspace replication mismatch: expected NetworkTopologyStrategy {"
                + dc + ":" + replicationFactor + "}, actual " + replication + ". Explicit schema reconciliation is required");
    }

    public static void validateColumns(Map<String, Column> columns) {
        var expected = Map.of("id", new Column("partition_key", 0, "uuid"),
            "value", new Column("regular", -1, "text"));
        if (!expected.equals(columns))
            throw new ControlPlaneExceptions.Conflict(
                "Probe table mismatch: expected id uuid PRIMARY KEY and value text; actual " + columns);
    }

    public record Column(String kind, int position, String type) {
    }
}
