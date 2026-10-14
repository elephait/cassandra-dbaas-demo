package demo.platform;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.keyspace.KeyspaceRecord.ProvisionDecision;
import demo.platform.keyspace.KeyspaceService;
import demo.platform.keyspace.SchemaValidator;
import demo.platform.keyspace.persistence.KeyspaceRepository;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SchemaValidatorTest {
    @Test
    void cleanupFailurePreservesOriginalProvisioningFailure() {
        var session = mock(CqlSession.class);
        var connections = mock(CassandraConnectionManager.class);
        var clusters = mock(ClusterRepository.class);
        var provisioning = mock(KeyspaceRepository.class);
        when(clusters.requireEnabled("default")).thenReturn(cluster());
        when(connections.requireSession(any(ClusterRecord.class))).thenReturn(session);
        when(provisioning.begin(eq("default"), eq("team"), anyInt(), anyInt(), anyLong()))
            .thenReturn(new ProvisionDecision(false, "owner"));
        when(provisioning.owns("default", "team", "owner")).thenReturn(true);
        when(session.execute(any(SimpleStatement.class))).thenReturn(mock(ResultSet.class));
        var failure = new IllegalStateException("DDL failed");
        var cleanup = new IllegalStateException("SQL unavailable during cleanup");
        when(session.execute(anyString())).thenThrow(failure);
        doThrow(cleanup).when(provisioning).release("default", "team", "owner");
        var service = new KeyspaceService(connections, clusters, provisioning);
        try {
            assertSame(failure, assertThrows(IllegalStateException.class,
                () -> service.provision("default", "team")));
            assertArrayEquals(new Throwable[]{cleanup}, failure.getSuppressed());
            verify(provisioning, never()).complete(anyString(), anyString(), anyString());
        } finally {
            service.close();
        }
    }

    private static ClusterRecord cluster() {
        return new ClusterRecord("default", List.of(new ContactPoint("127.0.0.1", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 100, 60, true, 1);
    }

    @Test
    void failedDdlReleasesOwnershipWithoutPublishingReady() {
        var session = mock(CqlSession.class);
        var connections = mock(CassandraConnectionManager.class);
        var clusters = mock(ClusterRepository.class);
        var provisioning = mock(KeyspaceRepository.class);
        when(clusters.requireEnabled("default")).thenReturn(cluster());
        when(connections.requireSession(any(ClusterRecord.class))).thenReturn(session);
        when(provisioning.begin(eq("default"), eq("team"), anyInt(), anyInt(), anyLong()))
            .thenReturn(new ProvisionDecision(false, "owner"));
        when(provisioning.owns("default", "team", "owner")).thenReturn(true);
        when(session.execute(any(SimpleStatement.class))).thenReturn(mock(ResultSet.class));
        var failure = new IllegalStateException("injected DDL failure");
        when(session.execute(anyString())).thenThrow(failure);
        var service = new KeyspaceService(connections, clusters, provisioning);
        try {
            assertSame(failure, assertThrows(IllegalStateException.class,
                () -> service.provision("default", "team")));
            verify(provisioning).release("default", "team", "owner");
            verify(provisioning, never()).complete(anyString(), anyString(), anyString());
        } finally {
            service.close();
        }
    }

    @Test
    void lostProvisioningOwnerCannotExecuteDdl() {
        var session = mock(CqlSession.class);
        var connections = mock(CassandraConnectionManager.class);
        var clusters = mock(ClusterRepository.class);
        var provisioning = mock(KeyspaceRepository.class);
        when(clusters.requireEnabled("default")).thenReturn(cluster());
        when(connections.requireSession(any(ClusterRecord.class))).thenReturn(session);
        when(provisioning.begin(eq("default"), eq("team"), anyInt(), anyInt(), anyLong()))
            .thenReturn(new ProvisionDecision(false, "old-owner"));
        when(session.execute(any(SimpleStatement.class))).thenReturn(mock(ResultSet.class));
        var service = new KeyspaceService(connections, clusters, provisioning);
        try {
            assertThrows(ControlPlaneExceptions.Conflict.class,
                () -> service.provision("default", "team"));
            verify(session, never()).execute(anyString());
            verify(provisioning).release("default", "team", "old-owner");
            verify(provisioning, never()).complete(anyString(), anyString(), anyString());
        } finally {
            service.close();
        }
    }

    @Test
    void rejectsRf1AndWrongStrategy() {
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> SchemaValidator.validateReplication(Map.of("class", "NetworkTopologyStrategy", "dc1", "1"), "dc1", 3));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> SchemaValidator.validateReplication(Map.of("class", "SimpleStrategy", "replication_factor", "3"), "dc1", 3));
        assertDoesNotThrow(() -> SchemaValidator.validateReplication(Map.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", "dc1", "3"), "dc1", 3));
    }

    @Test
    void rejectsWrongPrimaryKeyAndMissingTable() {
        assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> SchemaValidator.validateColumns(Map.of()));
        assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> SchemaValidator.validateColumns(Map.of(
            "id", new SchemaValidator.Column("regular", -1, "uuid"), "value", new SchemaValidator.Column("partition_key", 0, "text"))));
        assertDoesNotThrow(() -> SchemaValidator.validateColumns(Map.of(
            "id", new SchemaValidator.Column("partition_key", 0, "uuid"), "value", new SchemaValidator.Column("regular", -1, "text"))));
    }

    @Test
    void readyKeyspaceIsRevalidatedAndDriftIsRejectedWithoutDdl() {
        var session = mock(CqlSession.class);
        var connections = mock(CassandraConnectionManager.class);
        var clusters = mock(ClusterRepository.class);
        var provisioning = mock(KeyspaceRepository.class);
        when(connections.requireSession(any(demo.platform.cluster.ClusterRecord.class))).thenReturn(session);
        when(clusters.requireEnabled("default")).thenReturn(new ClusterRecord("default",
            List.of(new ContactPoint("127.0.0.1", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 100, 60, true, 1));
        when(provisioning.begin(eq("default"), eq("team"), anyInt(), anyInt(), anyLong()))
            .thenReturn(new ProvisionDecision(true, null));
        when(session.checkSchemaAgreement()).thenReturn(true);
        var result = mock(ResultSet.class);
        var row = mock(Row.class);
        when(session.execute(any(SimpleStatement.class))).thenReturn(result);
        when(result.one()).thenReturn(row);
        when(row.getMap("replication", String.class, String.class)).thenReturn(Map.of("class", "NetworkTopologyStrategy", "dc1", "1"));
        var service = new KeyspaceService(connections, clusters, provisioning);
        var error = assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> service.provision("default", "team"));
        assertEquals(409, error.getStatusCode().value());
        verify(session, never()).execute(anyString());
        verify(provisioning, never()).complete(anyString(), anyString(), anyString());
        service.close();
    }
}
