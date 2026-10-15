package db.migration;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** JDBC backfill avoids database-specific ordered string aggregation (H2/PostgreSQL). */
public class V3__inline_repair_policy_nodes extends BaseJavaMigration {
    @Override
    public void migrate(Context context) throws Exception {
        var connection = context.getConnection();
        Map<String, List<String>> policies = new LinkedHashMap<>();
        try (var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT cluster_id FROM repair_policies ORDER BY cluster_id")) {
            while (rows.next()) policies.put(rows.getString(1), new ArrayList<>());
        }
        try (var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT cluster_id, node_name FROM repair_policy_nodes ORDER BY cluster_id, ordinal")) {
            // V1's cluster_id foreign key guarantees every node references a policy.
            // Policies with zero node rows are backfilled to an empty string, not NULL.
            while (rows.next()) policies.get(rows.getString(1)).add(rows.getString(2));
        }
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("ALTER TABLE repair_policies ADD COLUMN node_names TEXT");
        }
        try (var update = connection.prepareStatement("UPDATE repair_policies SET node_names = ? WHERE cluster_id = ?")) {
            for (var policy : policies.entrySet()) {
                update.setString(1, String.join(",", policy.getValue()));
                update.setString(2, policy.getKey());
                update.addBatch();
            }
            update.executeBatch();
        }
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("ALTER TABLE repair_policies ALTER COLUMN node_names SET NOT NULL");
            statement.executeUpdate("DROP TABLE repair_policy_nodes");
        }
    }
}
