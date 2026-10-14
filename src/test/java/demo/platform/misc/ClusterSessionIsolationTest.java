package demo.platform.misc;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.LocalAddressTranslator;
import demo.platform.cluster.persistence.ClusterConfigurationRepository;
import demo.platform.misc.ApiExceptionHandler;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.repair.OperationRecord.RepairPolicy;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class ClusterSessionIsolationTest {
    @Test
    void translatedSessionsHaveIndependentDriverConfiguration() {
        var a = DriverConfigLoader.programmaticBuilder().withStringMap(LocalAddressTranslator.MAPPINGS,
            Map.of("172.28.91.11", "127.0.0.1:9042")).build();
        var b = DriverConfigLoader.programmaticBuilder().withStringMap(LocalAddressTranslator.MAPPINGS,
            Map.of("172.28.91.11", "127.0.0.1:9142")).build();
        var first = mock(DriverContext.class);
        var second = mock(DriverContext.class);
        when(first.getConfig()).thenReturn(a.getInitialConfig());
        when(second.getConfig()).thenReturn(b.getInitialConfig());
        var t1 = new LocalAddressTranslator(first);
        var t2 = new LocalAddressTranslator(second);
        var discovered = new InetSocketAddress("172.28.91.11", 9042);
        assertEquals(9042, t1.translate(discovered).getPort());
        assertEquals(9142, t2.translate(discovered).getPort());
        a.close();
        b.close();
    }

    @Test
    void connectingSessionCannotBePublishedAfterShutdown() throws Exception {
        var registry = mock(ClusterConfigurationRepository.class);
        var cluster = new ClusterRecord("east", List.of(new ContactPoint("localhost", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 5, 60, true, 1);
        var configuration = new ClusterConfiguration(cluster, new RepairPolicy(cluster.id(), List.of("node1"), "noop {keyspace}", 1, 1));
        when(registry.list(true)).thenReturn(List.of(configuration));
        when(registry.get("east")).thenReturn(configuration);
        var started = new CountDownLatch(1);
        var released = new CountDownLatch(1);
        var closed = new CountDownLatch(1);
        var session = mock(CqlSession.class);
        doAnswer(call -> {
            closed.countDown();
            return null;
        }).when(session).close();
        var manager = new CassandraConnectionManager(new ApiExceptionHandler.CassandraProperties(), registry) {
            @Override
            protected CqlSession buildSession(ClusterRecord ignored) {
                started.countDown();
                try {
                    released.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) { /* simulate slow native connect ignoring interruption */ }
                return session;
            }
        };
        manager.refresh();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        manager.close();
        released.countDown();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        assertTrue(manager.currentSession(cluster).isEmpty());
    }
}
