package demo.platform.repair.jmx;

import org.springframework.stereotype.Component;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class JmxRepairClient implements RepairManagementClient {
    private static final ObjectName STORAGE;
    private static final ObjectName RUNTIME;
    static {
        try {
            STORAGE = new ObjectName("org.apache.cassandra.db:type=StorageService");
            RUNTIME = new ObjectName("java.lang:type=Runtime");
        } catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    @Override
    public Session connect(JmxRepairProperties.Endpoint endpoint) throws Exception {
        Map<String, Object> environment = new HashMap<>();
        environment.put("com.sun.jndi.rmi.factory.socket", new RegistrySocketFactory());
        if (endpoint.getUsername() != null)
            environment.put(JMXConnector.CREDENTIALS,
                new String[] { endpoint.getUsername(), endpoint.getPassword() });
        JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(endpoint.getServiceUrl()), environment);
        var connection = connector.getMBeanServerConnection();
        return new Session() {
            public Identity identity() throws Exception {
                return new Identity((String) connection.getAttribute(STORAGE, "LocalHostId"),
                    (Long) connection.getAttribute(RUNTIME, "StartTime"));
            }
            public int submit(String keyspace) throws Exception {
                // Cassandra 5.0.5 Repair.createOptions sets incremental = !fullRepair for
                // ordinary repairs; -pr only selects ranges, --full selects full repair.
                // See verification/jmx/cassandra-5.0.5-api.txt for binary evidence.
                return (Integer) connection.invoke(STORAGE, "repairAsync",
                    new Object[] { keyspace, Map.of("primaryRange", "true", "incremental", "true",
                        "parallelism", "parallel", "jobThreads", "1") },
                    new String[] { String.class.getName(), Map.class.getName() });
            }
            public Observation status(int commandId) throws Exception {
                Object value = connection.invoke(STORAGE, "getParentRepairStatus",
                    new Object[] { commandId }, new String[] { "int" });
                return decodeStatus(value);
            }
            public void close() throws Exception { connector.close(); }
        };
    }

    static Observation decodeStatus(Object value) {
        if (value == null) return new Observation(Status.MISSING, "Cassandra command status is absent");
        if (!(value instanceof List<?> parts) || parts.isEmpty())
            return new Observation(Status.UNRECOGNIZED, "Malformed Cassandra repair status response");
        Status status = switch (String.valueOf(parts.getFirst())) {
            case "IN_PROGRESS" -> Status.IN_PROGRESS;
            case "COMPLETED" -> Status.COMPLETED;
            case "FAILED" -> Status.FAILED;
            default -> Status.UNRECOGNIZED;
        };
        return new Observation(status, parts.toString());
    }

    // Registry lookup has a separate TCP connection from the returned RMI service stub.
    // Service response/handshake limits are installed at application startup (OpenJDK 21).
    private static final class RegistrySocketFactory implements java.rmi.server.RMIClientSocketFactory {
        public java.net.Socket createSocket(String host, int port) throws java.io.IOException {
            java.net.Socket socket = new java.net.Socket();
            try {
                socket.connect(new java.net.InetSocketAddress(host, port), 5000);
                socket.setSoTimeout(10000);
                return socket;
            } catch (java.io.IOException e) { socket.close(); throw e; }
        }
    }
}
