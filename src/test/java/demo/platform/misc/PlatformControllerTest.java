package demo.platform.misc;

import demo.platform.cluster.ClusterController;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterService;
import demo.platform.cluster.LocalAddressTranslator;
import demo.platform.cluster.persistence.ClusterConfigurationRepository;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.AdminAccess;
import demo.platform.misc.ApiExceptionHandler;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.keyspace.KeyspaceNames;
import demo.platform.keyspace.KeyspaceController;
import demo.platform.repair.OperationRecord.RepairOperation;
import demo.platform.repair.RepairController;
import demo.platform.repair.RepairCoordinator;
import demo.platform.repair.RepairState;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PlatformControllerTest {
    private static ApiExceptionHandler.CassandraProperties properties() {
        var properties = new ApiExceptionHandler.CassandraProperties();
        properties.setAdminToken("secret");
        return properties;
    }

    static ClusterRecord cluster(String id) {
        return new ClusterRecord(id, List.of(new ContactPoint("127.0.0.1", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 100, 60, true, 1);
    }

    @Test
    void repairReplayWorksWithoutCassandraAndDoesNotDispatchAgain() {
        var properties = properties();
        var repairs = mock(RepairCoordinator.class);
        var original = new RepairOperation(UUID.randomUUID(), "default", "team",
            RepairState.SUCCEEDED, "done", 1, Instant.now());
        when(repairs.requestRepair("default", "team", true, "same-key")).thenReturn(original);
        var controller = new RepairController(repairs, new AdminAccess(properties));

        assertEquals(original, controller.repair("default",
            new RepairController.RepairRequest("team", true), "secret", "same-key"));
        verify(repairs, times(1)).requestRepair("default", "team", true, "same-key");
    }

    @Test
    void translatesEveryDiscoveredReplicaToDistinctHostPort() {
        try (var translator = new LocalAddressTranslator(Map.of(
            "172.28.91.11", new InetSocketAddress("127.0.0.1", 9042),
            "172.28.91.12", new InetSocketAddress("127.0.0.1", 9043),
            "172.28.91.13", new InetSocketAddress("127.0.0.1", 9044)))) {
            for (int index = 0; index < 3; index++) {
                var result = translator.translate(
                    new InetSocketAddress("172.28.91." + (11 + index), 9042));
                assertEquals("127.0.0.1", result.getAddress().getHostAddress());
                assertEquals(9042 + index, result.getPort());
            }
        }
    }

    @Test
    void rejectsUnauthorizedAndUnapprovedRequestsBeforeDatabaseAccess() {
        var properties = properties();
        var admin = new AdminAccess(properties);
        var provisioning = new KeyspaceController(null, admin);
        var repairService = mock(RepairCoordinator.class);
        when(repairService.requestRepair("default", "team", false, "test-key"))
            .thenThrow(new org.springframework.web.server.ResponseStatusException(
                org.springframework.http.HttpStatus.BAD_REQUEST, "Explicit approved=true required"));
        var repairs = new RepairController(repairService, admin);

        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> provisioning.provision("default", "team", null));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> repairs.repair("default", new RepairController.RepairRequest("team", false),
                "secret", "test-key"));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> provisioning.probe("default", "team", new KeyspaceController.Probe("value"), null));

    }

    @Test
    void rejectsCqlInjectionAndInvalidIdentifiers() {
        for (String value : new String[]{"x; DROP KEYSPACE system", "../x", "", "UPPER", "a".repeat(49)})
            assertThrows(IllegalArgumentException.class,
                () -> KeyspaceNames.validate(value));
        assertEquals("team_1", KeyspaceNames.validate("team_1"));
    }

    @Test
    void reportsUnavailableWithoutCassandra() {
        var properties = properties();
        var connections = new CassandraConnectionManager(properties, mock(ClusterConfigurationRepository.class));
        var clusters = mock(ClusterRepository.class);
        when(clusters.requireEnabled("default")).thenReturn(cluster("default"));
        var controller = new ClusterController(
            new ClusterService(connections, mock(ClusterConfigurationRepository.class), clusters),
            new AdminAccess(properties));

        var health = controller.health("default");

        assertEquals("UNAVAILABLE", health.get("status"));
        assertFalse((boolean) health.get("quorumAvailable"));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> connections.requireSession(cluster("default")));
        connections.close();
    }

    @Test
    void rejectsUnboundedCollectionRequests() {
        var properties = properties();
        var admin = new AdminAccess(properties);
        var provisioning = new KeyspaceController(null, admin);
        var repairs = new RepairController(null, admin);

        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> provisioning.keyspaces("default", 201, "secret"));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> repairs.operations("default", true, 0, "secret"));

    }
}
