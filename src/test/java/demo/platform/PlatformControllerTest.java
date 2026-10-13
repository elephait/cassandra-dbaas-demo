package demo.platform;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import java.util.Map;

class PlatformControllerTest {
    @Test
    void repairReplayWorksWithoutCassandraAndDoesNotDispatchAgain() {
        var properties = new CassandraProperties(); properties.setAdminToken("secret");
        var repairs = org.mockito.Mockito.mock(RepairService.class);
        var original = new RepairService.Operation(java.util.UUID.randomUUID(), "tenant_team", "SUCCEEDED", "done", java.time.Instant.now());
        org.mockito.Mockito.when(repairs.replay("tenant_team", "same-key")).thenReturn(original);
        var controller = new PlatformController(null, repairs, properties, null);
        assertEquals(original, controller.repair(new PlatformController.RepairRequest("team", true), "secret", "same-key"));
        org.mockito.Mockito.verify(repairs, org.mockito.Mockito.never()).start(org.mockito.Mockito.anyString(), org.mockito.Mockito.anyString());
    }
    @Test
    void translatesEveryDiscoveredReplicaToDistinctHostPort() {
        var translator = new LocalAddressTranslator(Map.of(
            "172.28.91.11", new InetSocketAddress("127.0.0.1", 9042),
            "172.28.91.12", new InetSocketAddress("127.0.0.1", 9043),
            "172.28.91.13", new InetSocketAddress("127.0.0.1", 9044)));
        for (int i = 0; i < 3; i++) {
            var result = translator.translate(new java.net.InetSocketAddress("172.28.91." + (11 + i), 9042));
            assertEquals("127.0.0.1", result.getAddress().getHostAddress());
            assertEquals(9042 + i, result.getPort());
        }
    }

    @Test
    void rejectsUnauthorizedProvisionAndUnapprovedRepairBeforeDatabaseAccess() {
        var properties = new CassandraProperties();
        properties.setAdminToken("secret");
        var controller = new PlatformController(null, null, properties, null);
        assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> controller.provision("team", null));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> controller.repair(new PlatformController.RepairRequest("team", false), "secret", "test-key"));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> controller.tenants(50, null));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> controller.operations(true, 50, null));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> controller.operation(java.util.UUID.randomUUID(), null));
    }

    @Test
    void rejectsCqlInjectionAndInvalidIdentifiers() {
        for (String value : new String[]{"x; DROP KEYSPACE system", "../x", "", "UPPER", "a".repeat(25)})
            assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> PlatformController.keyspace(value));
        assertEquals("tenant_team_1", PlatformController.keyspace("team_1"));
    }

    @Test
    void reportsUnavailableWithoutCassandra() {
        var properties = new CassandraProperties();
        properties.getCluster().setExpectedNodes(3);
        var connections = new CassandraConnectionManager(properties);
        var controller = new PlatformController(connections, null, properties, null);

        var health = controller.health();

        assertEquals("UNAVAILABLE", health.get("status"));
        assertFalse((boolean) health.get("quorumAvailable"));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> connections.requireSession());
        connections.close();
    }

    @Test
    void rejectsUnboundedCollectionRequests() {
        var properties = new CassandraProperties();
        properties.setAdminToken("secret");
        var controller = new PlatformController(null, null, properties, null);

        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> controller.tenants(201, "secret"));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> controller.operations(true, 0, "secret"));
    }
}
