package demo.platform.repair.jmx;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.InetSocketAddress;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Opt-in real 5.0.5 compatibility test against the main compose.yml.
 */
@EnabledIfSystemProperty(named = "jmx.live", matches = "true")
class JmxCassandraIT {
    @Test
    void repairsAllNodesThroughNativeAsyncApi() throws Exception {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "10000");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "10000");
        try (var cql = CqlSession.builder().addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
            .withLocalDatacenter("dc1")
            .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10)).build()).build()) {
            String version = cql.execute("SELECT release_version FROM system.local").one().getString("release_version");
            assertEquals("5.0.5", version, "This test certifies only the pinned Cassandra version");
            System.out.println("JMX_EVIDENCE Cassandra version=" + version);
            cql.execute("CREATE KEYSPACE IF NOT EXISTS jmx_review WITH replication = {'class':'NetworkTopologyStrategy','dc1':3}");
            cql.execute("CREATE TABLE IF NOT EXISTS jmx_review.probe (id int PRIMARY KEY, value text)");
            cql.execute("INSERT INTO jmx_review.probe (id, value) VALUES (1, 'native-api')");
            var client = new JmxRepairClient();
            for (int registryPort : new int[]{17198, 17298, 17398}) {
                var endpoint = new JmxRepairProperties.Endpoint();
                endpoint.setServiceUrl("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + registryPort + "/jmxrmi");
                int command;
                RepairManagementClient.Identity identity;
                try (var remote = client.connect(endpoint)) {
                    identity = remote.identity();
                    assertNotNull(identity.hostId());
                    command = remote.submit("jmx_review");
                    assertTrue(command > 0, "Primary range repair must submit actual work");
                    System.out.println("JMX_EVIDENCE coordinator=" + identity.hostId() + " command=" + command + " submitted; connection will close");
                }
                // Drop the submitting JMX connection. Each poll reconnects like a replacement worker.
                boolean done = false;
                long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
                while (System.nanoTime() < deadline) {
                    try (var remote = client.connect(endpoint)) {
                        assertEquals(identity, remote.identity());
                        var observation = remote.status(command);
                        System.out.println("JMX_EVIDENCE coordinator=" + identity.hostId() + " command=" + command + " status=" + observation.detail());
                        assertNotEquals(RepairManagementClient.Status.UNRECOGNIZED, observation.status(), observation.detail());
                        assertNotEquals(RepairManagementClient.Status.FAILED, observation.status(), observation.detail());
                        if (observation.status() == RepairManagementClient.Status.COMPLETED) {
                            done = true;
                            break;
                        }
                    }
                    Thread.sleep(1000);
                }
                assertTrue(done, "Repair did not finish before test deadline");
            }
            assertEquals("native-api", cql.execute("SELECT value FROM jmx_review.probe WHERE id = 1").one().getString("value"));
        }
    }
}
