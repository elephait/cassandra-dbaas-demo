package demo.platform;

import demo.platform.cluster.ClusterRegistryService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The hot read paths are single scalar queries: no entity or collection loading. */
@DataJpaTest(showSql = false, properties = "spring.jpa.properties.hibernate.generate_statistics=true")
@Import(ClusterRegistryService.class)
class ClusterInventoryPersistenceTest {
    @Autowired
    ClusterRegistryService registry;
    @Autowired
    EntityManager entityManager;
    @Autowired
    EntityManagerFactory entityManagerFactory;

    private org.hibernate.stat.Statistics freshStatistics() {
        entityManager.clear();
        var statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.clear();
        return statistics;
    }

    @Test
    void enabledLookupUsesOneScalarQueryWithoutCollections() {
        var expected = registry.get("default").settings();
        var statistics = freshStatistics();
        var settings = registry.requireEnabled("default");
        assertEquals(expected, settings);
        assertTrue(settings.enabled());
        assertEquals(1, statistics.getPrepareStatementCount());
        assertEquals(0, statistics.getEntityLoadCount());
        assertEquals(0, statistics.getCollectionLoadCount());
    }

    @Test
    void inventoryUsesOneScalarQuery() {
        var statistics = freshStatistics();
        assertEquals("default", registry.list().getFirst().id());
        assertEquals(1, statistics.getPrepareStatementCount());
        assertEquals(0, statistics.getEntityLoadCount());
        assertEquals(0, statistics.getCollectionLoadCount());
    }
}
