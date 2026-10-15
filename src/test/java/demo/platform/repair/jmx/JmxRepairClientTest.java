package demo.platform.repair.jmx;

import org.junit.jupiter.api.Test;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class JmxRepairClientTest {
    // These are decoder/plumbing tests, not evidence of the real Cassandra contract.
    // JmxCassandraIT verifies the pinned server; verification/jmx retains its reports.
    @Test void decodesKnownParentRepairStates() {
        for (String state : List.of("IN_PROGRESS", "COMPLETED", "FAILED"))
            assertEquals(RepairManagementClient.Status.valueOf(state),
                JmxRepairClient.decodeStatus(List.of(state, "message")).status());
    }

    @Test void missingStatusIsDistinctFromAnUnrecognizedResponse() {
        assertEquals(RepairManagementClient.Status.MISSING, JmxRepairClient.decodeStatus(null).status());
    }

    @Test void incompatibleResponsesAreVisibleInsteadOfBeingRetriedAsConnectivityFailures() {
        for (Object response : List.of(List.of("NEW_SERVER_STATE"), List.of(), "bad response"))
            assertEquals(RepairManagementClient.Status.UNRECOGNIZED, JmxRepairClient.decodeStatus(response).status());
    }
    public interface StorageServiceMBean {
        String getLocalHostId();
        int repairAsync(String keyspace, Map<String, String> options);
        List<String> getParentRepairStatus(int command);
    }
    public static class StorageService implements StorageServiceMBean {
        String keyspace;
        Map<String, String> options;
        public String getLocalHostId() { return "a4bc61ec-0ef2-4e28-9cbe-5a2ce03667f2"; }
        public int repairAsync(String keyspace, Map<String, String> options) {
            this.keyspace = keyspace; this.options = options; return 23;
        }
        public List<String> getParentRepairStatus(int command) {
            return command == 23 ? List.of("COMPLETED", "finished") : null;
        }
    }

    @Test void invokesRealJmxTransportWithCassandraSignaturesAndOptions() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var registry = LocateRegistry.createRegistry(port);
        var server = MBeanServerFactory.createMBeanServer();
        var storage = new StorageService();
        server.registerMBean(storage, new ObjectName("org.apache.cassandra.db:type=StorageService"));
        server.registerMBean(ManagementFactory.getRuntimeMXBean(), new ObjectName("java.lang:type=Runtime"));
        var url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + port + "/jmxrmi");
        var connector = JMXConnectorServerFactory.newJMXConnectorServer(url, Map.of(), server);
        try {
            connector.start();
            var endpoint = new JmxRepairProperties.Endpoint();
            endpoint.setServiceUrl(url.toString());
            try (var remote = new JmxRepairClient().connect(endpoint)) {
                assertEquals(storage.getLocalHostId(), remote.identity().hostId());
                assertTrue(remote.identity().processStartedAt() > 0);
                assertEquals(23, remote.submit("team"));
                assertEquals("team", storage.keyspace);
                assertEquals("true", storage.options.get("primaryRange"));
                assertEquals("true", storage.options.get("incremental"));
                assertEquals(RepairManagementClient.Status.COMPLETED, remote.status(23).status());
                assertEquals(RepairManagementClient.Status.MISSING, remote.status(99).status());
            }
        } finally {
            connector.stop();
            UnicastRemoteObject.unexportObject(registry, true);
            MBeanServerFactory.releaseMBeanServer(server);
        }
    }
}
