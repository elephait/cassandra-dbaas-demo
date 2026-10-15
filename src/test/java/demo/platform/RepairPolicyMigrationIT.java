package demo.platform;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.sql.DriverManager;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class RepairPolicyMigrationIT {
    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17.6")
        .withDatabaseName("policy_migration").withUsername("postgres").withPassword("integration-only");

    @Test void postgresUpgradePreservesOrderedTargetsAndDropsJoinTable() throws Exception {
        Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
            .target("2").load().migrate();
        try (var connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM repair_policy_nodes");
            statement.executeUpdate("INSERT INTO repair_policy_nodes VALUES ('default', 2, 'node3'), ('default', 0, 'node1'), ('default', 1, 'node2')");
        }
        var flyway = Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()).load();
        assertEquals("3", flyway.migrate().targetSchemaVersion);
        assertEquals(0, flyway.migrate().migrationsExecuted);
        try (var connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT node_names FROM repair_policies WHERE cluster_id = 'default'")) {
                assertTrue(rows.next());
                assertEquals("node1,node2,node3", rows.getString(1));
            }
            try (var tables = connection.getMetaData().getTables(null, "public", "repair_policy_nodes", null)) {
                assertFalse(tables.next());
            }
        }
    }
}
