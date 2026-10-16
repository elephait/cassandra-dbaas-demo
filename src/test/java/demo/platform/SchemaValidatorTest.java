package demo.platform;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.keyspace.KeyspaceProvisioningService;
import demo.platform.keyspace.KeyspaceRecord.ProvisionDecision;
import demo.platform.keyspace.KeyspaceService;
import demo.platform.keyspace.SchemaValidator;
import demo.platform.keyspace.SchemaValidator.Column;
import demo.platform.keyspace.cassandra.KeyspaceSchemaRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Validation rules, and provisioning orchestration against a mocked Cassandra schema repository. */
class SchemaValidatorTest {
    private static final ClusterRecord.Settings CLUSTER = new ClusterRecord("default",
        List.of(new ContactPoint("127.0.0.1", 9042)), "dc1", 3, 3, List.of(), 10, 60, 100, 60, true, 1).settings();
    private static final Map<String, String> GOOD_REPLICATION = Map.of("class", "NetworkTopologyStrategy", "dc1", "3");
    private static final Map<String, Column> GOOD_COLUMNS = Map.of("id", new Column("partition_key", 0, "uuid"),
        "value", new Column("regular", -1, "text"));

    private final KeyspaceSchemaRepository schema = mock(KeyspaceSchemaRepository.class);
    private final KeyspaceProvisioningService provisioning = mock(KeyspaceProvisioningService.class);
    private final ClusterRegistryService clusters = mock(ClusterRegistryService.class);
    private KeyspaceService service;

    @BeforeEach
    void setUp() {
        when(clusters.requireEnabled("default")).thenReturn(CLUSTER);
        when(schema.schemaAgreed(CLUSTER)).thenReturn(true);
        when(schema.probeColumns(CLUSTER, "team")).thenReturn(GOOD_COLUMNS);
        service = new KeyspaceService(clusters, provisioning, schema, new SchemaValidator(schema));
    }

    @AfterEach
    void tearDown() {
        service.close();
    }

    private void owned(String owner) {
        when(provisioning.begin(eq("default"), eq("team"), anyInt(), anyInt(), anyLong()))
            .thenReturn(new ProvisionDecision(false, owner));
    }

    @Test
    void createsValidatesAndPublishesReady() {
        owned("owner");
        when(provisioning.owns("default", "team", "owner")).thenReturn(true);
        when(schema.replication(CLUSTER, "team")).thenReturn(Optional.empty(), Optional.of(GOOD_REPLICATION));
        service.provision("default", "team");
        var order = inOrder(schema, provisioning);
        order.verify(schema).createKeyspace(CLUSTER, "team");
        order.verify(schema).createProbeTable(CLUSTER, "team");
        order.verify(provisioning).complete("default", "team", "owner");
    }

    @Test
    void failedDdlReleasesOwnershipAndKeepsCleanupFailureAsSuppressed() {
        owned("owner");
        when(provisioning.owns("default", "team", "owner")).thenReturn(true);
        when(schema.replication(CLUSTER, "team")).thenReturn(Optional.empty());
        var failure = new IllegalStateException("DDL failed");
        var cleanup = new IllegalStateException("SQL unavailable during cleanup");
        doThrow(failure).when(schema).createKeyspace(CLUSTER, "team");
        doThrow(cleanup).when(provisioning).release("default", "team", "owner");
        assertSame(failure, assertThrows(IllegalStateException.class, () -> service.provision("default", "team")));
        assertArrayEquals(new Throwable[]{cleanup}, failure.getSuppressed());
        verify(provisioning).release("default", "team", "owner");
        verify(provisioning, never()).complete(anyString(), anyString(), anyString());
    }

    @Test
    void lostProvisioningOwnerCannotExecuteDdl() {
        owned("old-owner");
        when(schema.replication(CLUSTER, "team")).thenReturn(Optional.empty());
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> service.provision("default", "team"));
        verify(schema, never()).createKeyspace(any(), anyString());
        verify(provisioning).release("default", "team", "old-owner");
    }

    @Test
    void readyKeyspaceIsRevalidatedAndDriftIsRejectedWithoutDdl() {
        when(provisioning.begin(eq("default"), eq("team"), anyInt(), anyInt(), anyLong()))
            .thenReturn(new ProvisionDecision(true, null));
        when(schema.replication(CLUSTER, "team")).thenReturn(Optional.of(Map.of("class", "NetworkTopologyStrategy", "dc1", "1")));
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> service.provision("default", "team"));
        verify(schema, never()).createKeyspace(any(), anyString());
        verify(provisioning, never()).complete(anyString(), anyString(), anyString());
    }

    @Test
    void probeReportsUnavailableWhenReadBackDiffers() {
        when(schema.readProbe(eq(CLUSTER), eq("team"), any())).thenReturn(Optional.of("other"));
        assertThrows(ControlPlaneExceptions.Unavailable.class, () -> service.probe("default", "team", "value"));
        assertThrows(IllegalArgumentException.class, () -> service.probe("default", "team", "x".repeat(1025)));
    }

    @Test
    void rejectsRf1AndWrongStrategy() {
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> SchemaValidator.validateReplication(
            Map.of("class", "NetworkTopologyStrategy", "dc1", "1"), "dc1", 3));
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> SchemaValidator.validateReplication(
            Map.of("class", "SimpleStrategy", "replication_factor", "3"), "dc1", 3));
        assertDoesNotThrow(() -> SchemaValidator.validateReplication(
            Map.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", "dc1", "3"), "dc1", 3));
    }

    @Test
    void rejectsWrongPrimaryKeyAndMissingTable() {
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> SchemaValidator.validateColumns(Map.of()));
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> SchemaValidator.validateColumns(Map.of(
            "id", new Column("regular", -1, "uuid"), "value", new Column("partition_key", 0, "text"))));
        assertDoesNotThrow(() -> SchemaValidator.validateColumns(GOOD_COLUMNS));
    }
}
