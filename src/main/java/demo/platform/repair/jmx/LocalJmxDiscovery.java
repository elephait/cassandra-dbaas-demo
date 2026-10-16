package demo.platform.repair.jmx;

import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

/**
 * Local launcher helper: discovers the Compose nodes' JMX host IDs for run-local.cmd. Not used by the app.
 */
public final class LocalJmxDiscovery {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Expected the generated configuration file path");
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "10000");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "10000");
        var properties = new Properties();
        for (int i = 0; i < 3; i++) {
            String url = "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + (17198 + 100 * i) + "/jmxrmi";
            try (var connection = JMXConnectorFactory.connect(new JMXServiceURL(url))) {
                String hostId = (String) connection.getMBeanServerConnection().getAttribute(
                    new ObjectName("org.apache.cassandra.db:type=StorageService"), "LocalHostId");
                String prefix = "demo.repair.jmx.targets.default.cassandra" + (i + 1);
                properties.setProperty(prefix + ".host-id", UUID.fromString(hostId).toString());
                properties.setProperty(prefix + ".service-url", url);
            }
        }
        Path file = Path.of(args[0]).toAbsolutePath();
        Files.createDirectories(file.getParent());
        try (var writer = Files.newBufferedWriter(file)) {
            properties.store(writer, "Discovered from the local Compose cluster");
        }
        System.out.println("Local JMX endpoints discovered");
    }
}
