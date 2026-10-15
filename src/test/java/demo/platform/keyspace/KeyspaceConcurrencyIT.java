package demo.platform.keyspace;

import demo.platform.PlatformApplication;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.persistence.KeyspaceId;
import demo.platform.keyspace.persistence.KeyspaceJpaRepository;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.misc.ControlPlaneExceptions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KeyspaceConcurrencyIT {
    private static final String ADMIN_TOKEN = "integration-only";
    private static final int QUOTA = 4;

    @Container
    static final CassandraContainer cassandra = new CassandraContainer("cassandra:5.0.5")
        .withEnv("MAX_HEAP_SIZE", "512M").withEnv("HEAP_NEWSIZE", "100M")
        .withStartupTimeout(Duration.ofMinutes(5));

    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17.6")
        .withDatabaseName("dbaas").withUsername("dbaas").withPassword("integration-only");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        configuration().forEach((key, value) -> registry.add(key, () -> value));
    }

    private static Map<String, String> configuration() {
        var properties = new LinkedHashMap<String, String>();
        properties.put("spring.datasource.url", postgres.getJdbcUrl());
        properties.put("spring.datasource.username", postgres.getUsername());
        properties.put("spring.datasource.password", postgres.getPassword());
        properties.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
        properties.put("spring.datasource.hikari.maximum-pool-size", "4");
        properties.put("demo.contact-points", cassandra.getHost() + ":" + cassandra.getMappedPort(9042));
        properties.put("demo.datacenter", cassandra.getLocalDatacenter());
        properties.put("demo.cluster.expected-nodes", "1");
        properties.put("demo.cassandra.replication-factor", "1");
        properties.put("demo.cassandra.connection-threads", "1");
        properties.put("demo.cassandra.reconnect-seconds", "2");
        String discovered = cassandra.getContainerInfo().getNetworkSettings().getNetworks()
            .values().iterator().next().getIpAddress();
        properties.put("demo.address-translation.enabled", "true");
        properties.put("demo.address-translation.mappings", discovered + "=" + cassandra.getHost()
            + ":" + cassandra.getMappedPort(9042));
        properties.put("demo.admin-token", ADMIN_TOKEN);
        properties.put("demo.max-keyspaces", Integer.toString(QUOTA));
        properties.put("demo.repair.nodes", "integration-node");
        properties.put("demo.repair.worker-threads", "1");
        properties.put("server.address", "127.0.0.1");
        properties.put("server.port", "0");
        return properties;
    }

    @LocalServerPort
    int port;
    @Autowired
    ConfigurableApplicationContext first;
    @Autowired
    JsonMapper json;

    @Test
    void httpProvisioningIsDeduplicatedQuotaSafeAndRecoverableAcrossInstances() throws Exception {
        String[] arguments = configuration().entrySet().stream()
            .map(entry -> "--" + entry.getKey() + "=" + entry.getValue()).toArray(String[]::new);
        try (var second = new SpringApplicationBuilder(PlatformApplication.class).run(arguments);
             var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            int otherPort = second.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            assertNotEquals(port, otherPort);
            assertNotSame(first.getBean(KeyspaceProvisioningService.class), second.getBean(KeyspaceProvisioningService.class));
            awaitConnected(first);
            awaitConnected(second);
            var rejected = client.send(HttpRequest.newBuilder(url(port, "unauthorized"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(403, rejected.statusCode());

            String shared = name("shared");
            List<HttpResponse<String>> requests = race(List.of(
                () -> post(client, port, shared), () -> post(client, otherPort, shared)));
            requests.forEach(response -> assertTrue(List.of(200, 409, 503).contains(response.statusCode()), response.body()));
            awaitReady(client, otherPort, shared);
            assertEquals(200, post(client, port, shared).statusCode());
            assertEquals(1, countReady(client, otherPort, shared));

            // Reserve a second identity on the instance that will stop, without running a heartbeat.
            String recovery = name("recovery");
            var oldOwner = second.getBean(KeyspaceProvisioningService.class).begin("default", recovery, QUOTA, 60);

            // Three new identities compete for two remaining slots, over the HTTP API.
            var names = List.of(name("quota_a"), name("quota_b"), name("quota_c"));
            List<HttpResponse<String>> quotaRequests = race(List.of(
                () -> post(client, port, names.get(0)),
                () -> post(client, otherPort, names.get(1)),
                () -> post(client, port, names.get(2))));
            int quotaRejections = 0;
            for (int index = 0; index < quotaRequests.size(); index++) {
                var response = quotaRequests.get(index);
                if (response.statusCode() == 409) {
                    assertEquals("Keyspace quota reached", json.readTree(response.body()).get("error").asText());
                    quotaRejections++;
                } else {
                    assertTrue(List.of(200, 503).contains(response.statusCode()), response.body());
                    awaitReady(client, port, names.get(index));
                }
            }
            assertEquals(1, quotaRejections);
            assertEquals(QUOTA, list(client, otherPort).size());

            var entities = second.getBean(KeyspaceJpaRepository.class);
            second.getBean(TransactionTemplate.class).executeWithoutResult(status -> {
                var row = entities.findById(new KeyspaceId("default", recovery)).orElseThrow();
                // Deterministic expiry fixture, not a claim that closing an instance cancels remote work.
                row.claim(oldOwner.owner(), entities.databaseTime().toInstant().minusSeconds(120), 60);
                entities.saveAndFlush(row);
            });
            second.close();
            awaitReady(client, port, recovery);
            var survivor = first.getBean(KeyspaceProvisioningService.class);
            assertFalse(survivor.renew("default", recovery, oldOwner.owner(), 60));
            assertThrows(ControlPlaneExceptions.Conflict.class,
                () -> survivor.complete("default", recovery, oldOwner.owner()));
            survivor.release("default", recovery, oldOwner.owner());
            assertEquals(1, countReady(client, port, recovery));
            assertEquals(QUOTA, list(client, port).size());
        }
    }

    private JsonNode list(HttpClient client, int serverPort) throws Exception {
        var response = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + serverPort
            + "/clusters/default/keyspaces?limit=200")).header("X-Admin-Token", ADMIN_TOKEN)
            .timeout(Duration.ofSeconds(30)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        return json.readTree(response.body());
    }

    private long countReady(HttpClient client, int serverPort, String keyspace) throws Exception {
        long count = 0;
        for (var row : list(client, serverPort)) {
            if (keyspace.equals(row.get("keyspace").asText())) {
                assertEquals("READY", row.get("state").asText());
                count++;
            }
        }
        return count;
    }

    private static HttpResponse<String> post(HttpClient client, int serverPort, String keyspace) throws Exception {
        return client.send(HttpRequest.newBuilder(url(serverPort, keyspace)).header("X-Admin-Token", ADMIN_TOKEN)
            .timeout(Duration.ofSeconds(30)).POST(HttpRequest.BodyPublishers.noBody()).build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static URI url(int serverPort, String keyspace) {
        return URI.create("http://127.0.0.1:" + serverPort + "/clusters/default/keyspaces/" + keyspace);
    }

    private static String name(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static void awaitReady(HttpClient client, int serverPort, String keyspace) throws Exception {
        await(Duration.ofSeconds(90), () -> {
            var response = post(client, serverPort, keyspace);
            assertTrue(List.of(200, 409, 503).contains(response.statusCode()), response.body());
            return response.statusCode() == 200;
        });
    }

    private static void awaitConnected(ConfigurableApplicationContext context) throws Exception {
        await(Duration.ofSeconds(120), () -> {
            var cluster = context.getBean(ClusterRegistryService.class).requireEnabled("default");
            return context.getBean(CassandraConnectionManager.class).currentSession(cluster)
                .map(session -> session.getMetadata().getNodes().values().stream()
                    .anyMatch(node -> node.getState().name().equals("UP")) && session.checkSchemaAgreement())
                .orElse(false);
        });
    }

    private static void await(Duration timeout, Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.call()) {
            if (System.nanoTime() >= deadline) fail("Condition did not converge within " + timeout);
            Thread.sleep(250);
        }
    }

    private static <T> List<T> race(List<Callable<T>> requests) throws Exception {
        var pool = Executors.newFixedThreadPool(requests.size());
        var ready = new CountDownLatch(requests.size());
        var start = new CountDownLatch(1);
        try {
            var futures = requests.stream().map(request -> pool.submit(() -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Race start timed out");
                return request.call();
            })).toList();
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            var results = new java.util.ArrayList<T>();
            for (var future : futures) results.add(future.get(60, TimeUnit.SECONDS));
            return results;
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}
