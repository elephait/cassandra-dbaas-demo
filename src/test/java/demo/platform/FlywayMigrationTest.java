package demo.platform;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

class FlywayMigrationTest {
    @Test
    void createsKeyspaceIdentityAndIsRepeatable() throws Exception {
        String url = "jdbc:h2:mem:clean-schema;DB_CLOSE_DELAY=-1";
        var flyway = Flyway.configure().dataSource(url, "sa", "").load();
        assertEquals("3", flyway.migrate().targetSchemaVersion);
        assertEquals(0, flyway.migrate().migrationsExecuted);
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var columns = connection.getMetaData().getColumns(null, "PUBLIC", "KEYSPACES", null)) {
            var names = new java.util.HashSet<String>();
            while (columns.next()) names.add(columns.getString("COLUMN_NAME"));
            assertTrue(names.containsAll(java.util.Set.of("CLUSTER_ID", "KEYSPACE_NAME", "OWNER_ID", "EXPIRES_AT")));
            assertEquals(6, names.size());
        }
    }

    @Test
    void upgradesExistingPoliciesWithoutNodeTableAndPreservesNodeOrder() throws Exception {
        String url = "jdbc:h2:mem:inline-policy-upgrade;DB_CLOSE_DELAY=-1";
        Flyway.configure().dataSource(url, "sa", "").target("2").load().migrate();
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM repair_policy_nodes WHERE cluster_id = 'default'");
            statement.executeUpdate("INSERT INTO repair_policy_nodes VALUES ('default', 2, 'node3')");
            statement.executeUpdate("INSERT INTO repair_policy_nodes VALUES ('default', 0, 'node1')");
            statement.executeUpdate("INSERT INTO repair_policy_nodes VALUES ('default', 1, 'node2')");
        }
        var flyway = Flyway.configure().dataSource(url, "sa", "").load();
        assertEquals("3", flyway.migrate().targetSchemaVersion);
        assertEquals(0, flyway.migrate().migrationsExecuted);
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT node_names FROM repair_policies WHERE cluster_id = 'default'")) {
                assertTrue(rows.next());
                assertEquals("node1,node2,node3", rows.getString(1));
            }
            try (var tables = connection.getMetaData().getTables(null, "PUBLIC", "REPAIR_POLICY_NODES", null)) {
                assertFalse(tables.next());
            }
        }
    }
}
