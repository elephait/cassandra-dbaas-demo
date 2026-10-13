package demo.platform;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

class SchemaValidatorTest {
    @Test void rejectsRf1AndWrongStrategy() {
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> SchemaValidator.validateReplication(Map.of("class", "NetworkTopologyStrategy", "dc1", "1"), "dc1", 3));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> SchemaValidator.validateReplication(Map.of("class", "SimpleStrategy", "replication_factor", "3"), "dc1", 3));
        assertDoesNotThrow(() -> SchemaValidator.validateReplication(Map.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", "dc1", "3"), "dc1", 3));
    }

    @Test void rejectsWrongPrimaryKeyAndMissingTable() {
        assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> SchemaValidator.validateColumns(Map.of()));
        assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> SchemaValidator.validateColumns(Map.of(
                "id", new SchemaValidator.Column("regular", -1, "uuid"), "value", new SchemaValidator.Column("partition_key", 0, "text"))));
        assertDoesNotThrow(() -> SchemaValidator.validateColumns(Map.of(
                "id", new SchemaValidator.Column("partition_key", 0, "uuid"), "value", new SchemaValidator.Column("regular", -1, "text"))));
    }

    @Test void readyTenantIsRevalidatedAndDriftIsRejectedWithoutDdl() {
        var session = mock(CqlSession.class);
        var connections = mock(CassandraConnectionManager.class);
        var repository = mock(ControlPlaneRepository.class);
        var properties = new CassandraProperties(); properties.setAdminToken("secret"); properties.setDatacenter("dc1");
        when(connections.requireSession()).thenReturn(session);
        when(repository.beginProvision(eq("team"), eq("tenant_team"), anyInt(), any()))
                .thenReturn(new ControlPlaneRepository.ProvisionDecision(true, null));
        when(session.checkSchemaAgreement()).thenReturn(true);
        var result = mock(ResultSet.class); var row = mock(Row.class);
        when(session.execute(any(SimpleStatement.class))).thenReturn(result); when(result.one()).thenReturn(row);
        when(row.getMap("replication", String.class, String.class)).thenReturn(Map.of("class", "NetworkTopologyStrategy", "dc1", "1"));
        var controller = new PlatformController(connections, null, properties, repository);
        var error = assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> controller.provision("team", "secret"));
        assertEquals(409, error.getStatusCode().value());
        verify(session, never()).execute(anyString());
        verify(repository, never()).completeProvision(anyString(), anyString());
    }
}
