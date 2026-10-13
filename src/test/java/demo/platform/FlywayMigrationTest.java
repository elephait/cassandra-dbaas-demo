package demo.platform;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlywayMigrationTest {
    @Test
    void baselinesAndUpgradesPreFlywaySchema() throws Exception {
        String url = "jdbc:h2:mem:flyway-upgrade;DB_CLOSE_DELAY=-1";
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE control_lock (id INTEGER PRIMARY KEY)");
            statement.execute("INSERT INTO control_lock VALUES (1)");
            statement.execute("CREATE TABLE tenants (tenant_id VARCHAR(24) PRIMARY KEY, keyspace_name VARCHAR(255) NOT NULL UNIQUE, state VARCHAR(32) NOT NULL, updated_at TIMESTAMP NOT NULL)");
            statement.execute("CREATE TABLE operations (operation_id VARCHAR(36) PRIMARY KEY, keyspace_name VARCHAR(255) NOT NULL, state VARCHAR(32) NOT NULL, detail VARCHAR(4000), updated_at TIMESTAMP NOT NULL)");
            statement.execute("CREATE TABLE repair_lock (id INTEGER PRIMARY KEY, owner_id VARCHAR(36), expires_at TIMESTAMP)");
            statement.execute("INSERT INTO repair_lock VALUES (1, 'worker-1', CURRENT_TIMESTAMP)");
            statement.execute("INSERT INTO operations VALUES ('00000000-0000-0000-0000-000000000001', 'tenant_team', 'RUNNING', 'repairing', CURRENT_TIMESTAMP)");
        }

        var result = Flyway.configure()
                .dataSource(url, "sa", "")
                .baselineOnMigrate(true)
                .baselineVersion("1")
                .load()
                .migrate();

        assertEquals("4", result.targetSchemaVersion);
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var columns = connection.getMetaData().getColumns(null, "PUBLIC", "OPERATIONS", "OWNER_ID");
             var statement = connection.prepareStatement("SELECT owner_id FROM operations WHERE operation_id = ?")) {
            assertTrue(columns.next());
            statement.setString(1, "00000000-0000-0000-0000-000000000001");
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals("worker-1", rows.getString(1));
            }
        }
    }
}

