package demo.platform;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FlywayMigrationTest {
    @Test
    void singleMigrationCreatesTheSchemaAndIsRepeatable() throws Exception {
        String url = "jdbc:h2:mem:clean-schema;DB_CLOSE_DELAY=-1";
        var flyway = Flyway.configure().dataSource(url, "sa", "").load();
        assertEquals("1", flyway.migrate().targetSchemaVersion);
        assertEquals(0, flyway.migrate().migrationsExecuted);
        try (var connection = DriverManager.getConnection(url, "sa", "")) {
            assertEquals(Set.of("CLUSTER_ID", "KEYSPACE_NAME", "STATE", "OWNER_ID", "EXPIRES_AT", "UPDATED_AT"),
                columns(connection, "KEYSPACES"));
            assertTrue(columns(connection, "REPAIR_NODE_PROGRESS").containsAll(Set.of("SUBMITTED_AT", "DELAYED")));
            assertTrue(columns(connection, "OPERATIONS").contains("DELAYED"));
            try (var tables = connection.getMetaData().getTables(null, "PUBLIC", "REPAIR_POLICIES", null)) {
                assertFalse(tables.next(), "repair targets are deployment configuration, not a table");
            }
            try (var statement = connection.createStatement();
                 var rows = statement.executeQuery("SELECT owner_id FROM repair_locks WHERE cluster_id = 'default'")) {
                assertTrue(rows.next());
                assertNull(rows.getString(1));
            }
        }
    }

    private static Set<String> columns(java.sql.Connection connection, String table) throws Exception {
        var names = new HashSet<String>();
        try (var columns = connection.getMetaData().getColumns(null, "PUBLIC", table, null)) {
            while (columns.next()) names.add(columns.getString("COLUMN_NAME"));
        }
        return names;
    }
}
