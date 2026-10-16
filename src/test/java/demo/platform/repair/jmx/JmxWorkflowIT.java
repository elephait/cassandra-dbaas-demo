package demo.platform.repair.jmx;

import com.datastax.oss.driver.api.core.CqlSession;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.cassandra.CassandraSessions;
import demo.platform.common.DatabaseClock;
import demo.platform.common.PlatformProperties;
import demo.platform.repair.RepairClaimService;
import demo.platform.repair.RepairExecutionService;
import demo.platform.repair.RepairTopology;
import demo.platform.repair.RepairWorker;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real HTTP admission + PostgreSQL + replacement worker + main Compose cluster.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EnabledIfSystemProperty(named = "jmx.live", matches = "true")
class JmxWorkflowIT {
    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17.6")
        .withDatabaseName("jmx_workflow").withUsername("postgres").withPassword("integration-only");
    @LocalServerPort
    int port;
    @Autowired
    ConfigurableApplicationContext context;
    @Autowired
    CassandraSessions sessions;
    @Autowired
    ClusterRegistryService clusters;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    JsonMapper json;

    @DynamicPropertySource
    static void configuration(DynamicPropertyRegistry registry) throws Exception {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "10000");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "10000");
        var properties = new LinkedHashMap<String, String>();
        properties.put("spring.datasource.url", postgres.getJdbcUrl());
        properties.put("spring.datasource.username", postgres.getUsername());
        properties.put("spring.datasource.password", postgres.getPassword());
        properties.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
        properties.put("demo.contact-points", "127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044");
        properties.put("demo.datacenter", "dc1");
        properties.put("demo.cluster.expected-nodes", "3");
        properties.put("demo.cassandra.replication-factor", "3");
        properties.put("demo.max-keyspaces", "20");
        properties.put("demo.admin-token", "workflow-test");
        properties.put("demo.repair.worker-enabled", "false");
        properties.put("demo.repair.jmx.poll-seconds", "1");
        var mappings = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            int cqlPort = 9042 + i;
            try (var cql = CqlSession.builder().addContactPoint(new InetSocketAddress("127.0.0.1", cqlPort))
                .withLocalDatacenter("dc1").build()) {
                String address = cql.execute("SELECT broadcast_address FROM system.local").one()
                    .getInetAddress("broadcast_address").getHostAddress();
                mappings.add(address + "=127.0.0.1:" + cqlPort);
            }
            var endpoint = new JmxRepairProperties.Endpoint();
            endpoint.setServiceUrl("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + (17198 + i * 100) + "/jmxrmi");
            try (var remote = new JmxRepairClient().connect(endpoint)) {
                properties.put("demo.repair.jmx.targets.default.n" + (i + 1) + ".host-id", remote.identity().hostId());
            }
            properties.put("demo.repair.jmx.targets.default.n" + (i + 1) + ".service-url", endpoint.getServiceUrl());
        }
        properties.put("demo.address-translation.enabled", "true");
        properties.put("demo.address-translation.mappings", String.join(",", mappings));
        properties.forEach((key, value) -> registry.add(key, () -> value));
    }

    private static void await(Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (!condition.call()) {
            if (System.nanoTime() > deadline) fail("Live workflow did not converge");
            Thread.sleep(100);
        }
    }

    private RepairWorker newWorker() {
        var properties = new PlatformProperties();
        properties.getRepair().setWorkerThreads(1);
        return new RepairWorker(properties,
            new RepairExecutionService(context.getBean(RepairClaimService.class), context.getBean(RepairExecutionRepository.class),
                context.getBean(JmxRepairProperties.class),
                context.getBean(RepairManagementClient.class), context.getBean(RepairTopology.class),
                context.getBean(DatabaseClock.class)));
    }

    private CqlSession session() {
        return sessions.require(clusters.requireEnabled("default"));
    }

    private void ready() throws Exception {
        await(() -> sessions.current(clusters.requireEnabled("default"))
            .map(s -> s.checkSchemaAgreement() && s.getMetadata().getNodes().values().stream()
                .filter(n -> n.getState().name().equals("UP")).count() == 3).orElse(false));
    }

    private HttpResponse<String> post(HttpClient client, String path, String body, String key) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .header("X-Admin-Token", "workflow-test").header("Content-Type", "application/json")
            .header("Idempotency-Key", key).timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void httpRepairPersistsProgressAndReplacementWorkerCompletesAndReleasesReservation() throws Exception {
        ready();
        String name = "workflow_" + UUID.randomUUID().toString().replace("-", "");
        try (var http = HttpClient.newHttpClient()) {
            await(() -> post(http, "/clusters/default/keyspaces/" + name, "", "provision").statusCode() == 200);
            String key = UUID.randomUUID().toString();
            String body = "{\"keyspace\":\"" + name + "\",\"approved\":true}";
            var receipt = post(http, "/clusters/default/repair", body, key);
            assertEquals(202, receipt.statusCode(), receipt.body());
            String id = json.readTree(receipt.body()).get("id").asText();
            assertEquals(id, json.readTree(post(http, "/clusters/default/repair", body, key).body()).get("id").asText());
            var progress = context.getBean(RepairExecutionRepository.class);
            var first = newWorker();
            try {
                // One poll starts the run (plan initialization runs on the worker pool); later polls submit node 0.
                await(() -> {
                    first.poll();
                    return progress.nodeProgress(UUID.fromString(id)).stream()
                        .anyMatch(n -> n.ordinal() == 0 && "RUNNING".equals(n.state()) && n.commandId() != null);
                });
                assertNotNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id='default'", String.class));
            } finally {
                first.close();
            }
            var replacement = newWorker();
            try {
                await(() -> {
                    replacement.poll();
                    return "SUCCEEDED".equals(jdbc.queryForObject("SELECT state FROM operations WHERE operation_id=?", String.class, id));
                });
            } finally {
                replacement.close();
            }
            var nodes = progress.nodeProgress(UUID.fromString(id));
            assertEquals(3, nodes.size());
            for (var node : nodes) {
                assertEquals("SUCCEEDED", node.state());
                assertEquals(1, node.attempts());
                assertTrue(node.commandId() > 0);
            }
            var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                    + "/operations/" + id + "/nodes")).header("X-Admin-Token", "workflow-test").GET().build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), response.body());
            var apiNodes = json.readTree(response.body());
            assertEquals(3, apiNodes.size());
            for (var node : apiNodes) assertEquals("SUCCEEDED", node.get("state").asText());
            assertNull(jdbc.queryForObject("SELECT owner_id FROM repair_locks WHERE cluster_id='default'", String.class));
            System.out.println("JMX_WORKFLOW_EVIDENCE HTTP admission, deduplication, persisted commands, replacement worker, all three nodes succeeded, lock released");
        }
    }

    @Test
    void realProvisioningRejectsSchemaDriftAndRecoversAfterDdlWithoutReceipt() throws Exception {
        ready();
        String prefix = "schema_" + UUID.randomUUID().toString().replace("-", "");
        try (var http = HttpClient.newHttpClient()) {
            String partial = prefix + "_partial";
            session().execute("CREATE KEYSPACE " + partial + " WITH replication={'class':'NetworkTopologyStrategy','dc1':3}");
            await(() -> session().checkSchemaAgreement());
            // Simulate interruption after remote keyspace DDL but before our provisioning receipt.
            await(() -> post(http, "/clusters/default/keyspaces/" + partial, "", "partial").statusCode() == 200);
            assertEquals(200, post(http, "/clusters/default/keyspaces/" + partial, "", "partial").statusCode());
            assertEquals(200, post(http, "/clusters/default/keyspaces/" + partial + "/probe", "{\"value\":\"live\"}", "probe").statusCode());
            session().execute("ALTER KEYSPACE " + partial + " WITH replication={'class':'NetworkTopologyStrategy','dc1':1}");
            await(() -> session().checkSchemaAgreement());
            var mismatch = post(http, "/clusters/default/keyspaces/" + partial, "", "rf");
            assertEquals(409, mismatch.statusCode(), mismatch.body());
            assertEquals("1", session().execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name='"
                + partial + "'").one().getMap("replication", String.class, String.class).get("dc1"));
            String table = prefix + "_table";
            session().execute("CREATE KEYSPACE " + table + " WITH replication={'class':'NetworkTopologyStrategy','dc1':3}");
            session().execute("CREATE TABLE " + table + ".probe(id text PRIMARY KEY, value text)");
            await(() -> session().checkSchemaAgreement());
            var badTable = post(http, "/clusters/default/keyspaces/" + table, "", "table");
            assertEquals(409, badTable.statusCode(), badTable.body());
            assertEquals("text", session().execute("SELECT type FROM system_schema.columns WHERE keyspace_name='"
                + table + "' AND table_name='probe' AND column_name='id'").one().getString("type"));
        }
    }
}
