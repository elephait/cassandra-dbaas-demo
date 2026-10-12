package demo.platform;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlatformControllerTest {
    @Test
    void translatesEveryDiscoveredReplicaToDistinctHostPort() {
        var translator = new LocalAddressTranslator(null);
        for (int i = 0; i < 3; i++) {
            var result = translator.translate(new java.net.InetSocketAddress("172.28.91." + (11 + i), 9042));
            assertEquals("127.0.0.1", result.getAddress().getHostAddress());
            assertEquals(9042 + i, result.getPort());
        }
    }

    @Test
    void rejectsUnauthorizedProvisionAndUnapprovedRepairBeforeDatabaseAccess() {
        var controller = new PlatformController(null, null, 5, "secret");
        assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> controller.provision("team", null));
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> controller.repair(new PlatformController.RepairRequest("team", false), "secret"));
    }

    @Test
    void rejectsCqlInjectionAndInvalidIdentifiers() {
        for (String value : new String[]{"x; DROP KEYSPACE system", "../x", "", "UPPER", "a".repeat(25)})
            assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> PlatformController.keyspace(value));
        assertEquals("tenant_team_1", PlatformController.keyspace("team_1"));
    }
}
