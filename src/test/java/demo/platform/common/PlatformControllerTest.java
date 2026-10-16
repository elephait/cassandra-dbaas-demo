package demo.platform.common;

import com.datastax.oss.driver.api.core.CqlSession;
import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterController;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.ClusterService;
import demo.platform.cluster.cassandra.CassandraSessions;
import demo.platform.cluster.cassandra.ClusterMetadataRepository;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.cassandra.LocalAddressTranslator;
import demo.platform.keyspace.KeyspaceController;
import demo.platform.keyspace.KeyspaceNames;
import demo.platform.repair.OperationRecord.RepairOperation;
import demo.platform.repair.RepairController;
import demo.platform.repair.RepairCoordinator;
import demo.platform.repair.RepairState;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PlatformControllerTest {
    private static PlatformProperties properties() {
        var properties = new PlatformProperties();
        properties.setAdminToken("secret");
        return properties;
    }

    private static ClusterRecord cluster(String id) {
        return new ClusterRecord(id, List.of(new ContactPoint("127.0.0.1", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 100, 60, true, 1);
    }

    @Test
    void adminTokenIsRequiredAndComparedExactly() {
        var interceptor = new AdminTokenInterceptor(properties());
        var response = new MockHttpServletResponse();
        for (String supplied : new String[]{null, "", "secre", "secret ", "SECRET"}) {
            var request = new MockHttpServletRequest();
            if (supplied != null) request.addHeader(AdminTokenInterceptor.HEADER, supplied);
            assertThrows(ControlPlaneExceptions.Forbidden.class, () -> interceptor.preHandle(request, response, null));
        }
        var request = new MockHttpServletRequest();
        request.addHeader(AdminTokenInterceptor.HEADER, "secret");
        assertTrue(interceptor.preHandle(request, response, null));
    }

    @Test
    void repairReplayReturnsTheOriginalReceipt() {
        var repairs = mock(RepairCoordinator.class);
        var original = new RepairOperation(UUID.randomUUID(), "default", "team",
            RepairState.SUCCEEDED, "done", false, Instant.now());
        when(repairs.requestRepair("default", "team", true, "same-key")).thenReturn(original);
        var controller = new RepairController(repairs);

        assertEquals(original, controller.repair("default", new RepairController.RepairRequest("team", true), "same-key"));
        verify(repairs, times(1)).requestRepair("default", "team", true, "same-key");
    }

    @Test
    void unapprovedOrUnkeyedRepairIsRejectedBeforeAnyDependencyIsUsed() {
        var coordinator = new RepairCoordinator(null, null, null, null);
        assertThrows(IllegalArgumentException.class, () -> coordinator.requestRepair("default", "team", false, "key"));
        assertThrows(IllegalArgumentException.class, () -> coordinator.requestRepair("default", "team", true, null));
        assertThrows(IllegalArgumentException.class, () -> coordinator.requestRepair("default", "team", true, "bad key!"));
        assertThrows(IllegalArgumentException.class, () -> coordinator.acknowledge(UUID.randomUUID(), false));
    }

    @Test
    void translatesEveryDiscoveredReplicaToDistinctHostPort() {
        try (var translator = new LocalAddressTranslator(Map.of(
            "172.28.91.11", new InetSocketAddress("127.0.0.1", 9042),
            "172.28.91.12", new InetSocketAddress("127.0.0.1", 9043),
            "172.28.91.13", new InetSocketAddress("127.0.0.1", 9044)))) {
            for (int index = 0; index < 3; index++) {
                var result = translator.translate(new InetSocketAddress("172.28.91." + (11 + index), 9042));
                assertEquals("127.0.0.1", result.getAddress().getHostAddress());
                assertEquals(9042 + index, result.getPort());
            }
        }
    }

    @Test
    void rejectsCqlInjectionAndInvalidIdentifiers() {
        for (String value : new String[]{"x; DROP KEYSPACE system", "../x", "", "UPPER", "a".repeat(49), "system_auth"})
            assertThrows(IllegalArgumentException.class, () -> KeyspaceNames.validate(value));
        assertEquals("team_1", KeyspaceNames.validate("team_1"));
    }

    @Test
    void reportsUnavailableWithoutCassandra() {
        var clusters = mock(ClusterRegistryService.class);
        when(clusters.requireEnabled("default")).thenReturn(cluster("default").settings());
        when(clusters.get("default")).thenReturn(cluster("default"));
        var connections = new CassandraSessions(clusters) {
            @Override
            protected CompletionStage<CqlSession> connect(ClusterRecord cluster) {
                return new CompletableFuture<>(); // never connects
            }
        };
        var controller = new ClusterController(new ClusterService(clusters, mock(ClusterConfigurationService.class),
            connections, new ClusterMetadataRepository(connections)));
        try {
            var health = controller.health("default");
            assertEquals("UNAVAILABLE", health.status());
            assertFalse(health.quorumAvailable());
            assertThrows(ControlPlaneExceptions.Unavailable.class,
                () -> connections.require(cluster("default").settings()));
        } finally {
            connections.close();
        }
    }

    @Test
    void rejectsUnboundedCollectionRequests() {
        assertThrows(IllegalArgumentException.class, () -> new KeyspaceController(null).keyspaces("default", 201));
        assertThrows(IllegalArgumentException.class, () -> new RepairController(null).operations("default", true, 0));
    }
}
