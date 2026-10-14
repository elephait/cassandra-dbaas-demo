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
        assertEquals("2", flyway.migrate().targetSchemaVersion);
        assertEquals(0, flyway.migrate().migrationsExecuted);
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var columns = connection.getMetaData().getColumns(null, "PUBLIC", "KEYSPACES", null)) {
            var names = new java.util.HashSet<String>();
            while (columns.next()) names.add(columns.getString("COLUMN_NAME"));
            assertTrue(names.containsAll(java.util.Set.of("CLUSTER_ID", "KEYSPACE_NAME", "OWNER_ID", "EXPIRES_AT")));
            assertEquals(6, names.size());
        }
    }
}
