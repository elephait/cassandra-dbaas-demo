package demo.platform;

import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ApiExceptionHandler;
import demo.platform.misc.CassandraConnectionManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
    "spring.datasource.url=jdbc:h2:mem:disconnected-startup;DB_CLOSE_DELAY=-1",
    "demo.contact-points=127.0.0.1:1",
    "demo.address-translation.enabled=false"
})
class PlatformApplicationTest {
    @Autowired
    private CassandraConnectionManager connections;
    @Autowired
    private ApiExceptionHandler.CassandraProperties properties;
    @Autowired
    private ClusterRepository clusters;

    @Test
    void startsWithoutCassandra() {
        assertTrue(connections.currentSession(clusters.requireEnabled("default")).isEmpty());
        assertEquals(60, properties.getCassandra().getReconnectSeconds());
    }
}
