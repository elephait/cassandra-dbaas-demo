package demo.platform;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.persistence.ClusterRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest(showSql = false, properties = "spring.jpa.properties.hibernate.generate_statistics=true")
@Import({ClusterRepository.class, ClusterRegistryService.class})
class ClusterInventoryPersistenceTest {
    @Autowired
    ClusterRegistryService registry;
    @Autowired
    ClusterRepository clusters;
    @Autowired
    EntityManager entityManager;
    @Autowired
    EntityManagerFactory entityManagerFactory;

    @Test
    void enabledLookupUsesOneScalarQueryWithoutCollections() {
        var expected = clusters.get("default").settings();
        entityManager.clear();
        var statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.clear();
        var settings = registry.requireEnabled("default");
        assertEquals("default", settings.id());
        assertTrue(settings.enabled());
        assertEquals(expected, settings);
        assertEquals(1, statistics.getPrepareStatementCount());
        assertEquals(0, statistics.getEntityLoadCount());
        assertEquals(0, statistics.getCollectionLoadCount());
    }

    @Test
    void inventoryAndConnectionPollingUseOneScalarQueryEach() {
        entityManager.clear();
        var statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.clear();
        var inventory = registry.list();
        assertEquals("default", inventory.getFirst().id());
        assertEquals(1, statistics.getPrepareStatementCount());
        assertEquals(0, statistics.getEntityLoadCount());
        assertEquals(0, statistics.getCollectionLoadCount());

        statistics.clear();
        var states = clusters.listEnabledConfigStates();
        assertEquals("default", states.getFirst().clusterId());
        assertEquals(1, statistics.getPrepareStatementCount());
        assertEquals(0, statistics.getEntityLoadCount());
        assertEquals(0, statistics.getCollectionLoadCount());
    }
}
