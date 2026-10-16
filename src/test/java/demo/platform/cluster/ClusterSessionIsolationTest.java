package demo.platform.cluster;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.cassandra.CassandraSessions;
import demo.platform.cluster.cassandra.LocalAddressTranslator;
import demo.platform.common.ControlPlaneExceptions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ClusterSessionIsolationTest {
    private final ClusterRegistryService registry = mock(ClusterRegistryService.class);
    private final AtomicReference<ClusterRecord> stored = new AtomicReference<>(cluster(1, 60));
    private final List<CompletableFuture<CqlSession>> connects = new ArrayList<>();
    private final CassandraSessions sessions = new CassandraSessions(registry) {
        @Override
        protected CompletionStage<CqlSession> connect(ClusterRecord cluster) {
            var connect = new CompletableFuture<CqlSession>();
            connects.add(connect);
            return connect;
        }
    };

    ClusterSessionIsolationTest() {
        when(registry.get("east")).thenAnswer(call -> stored.get());
    }

    private static ClusterRecord cluster(long version, int reconnectSeconds) {
        return new ClusterRecord("east", List.of(new ContactPoint("localhost", 9042)),
            "dc1", 1, 1, List.of(), 10, reconnectSeconds, 5, 60, true, version);
    }

    @Test
    void connectsLazilyWithoutBlockingAndOncePerVersion() {
        var v1 = stored.get().settings();
        assertTrue(sessions.current(v1).isEmpty(), "first use starts the connect and returns immediately");
        assertThrows(ControlPlaneExceptions.Unavailable.class, () -> sessions.require(v1));
        var session = mock(CqlSession.class);
        connects.getFirst().complete(session);
        assertSame(session, sessions.require(v1));
        sessions.require(v1);
        assertEquals(1, connects.size());
    }

    @Test
    void newVersionReplacesAndGracefullyClosesTheOldSessionButStaleRequestsNeverDowngrade() {
        var old = mock(CqlSession.class);
        sessions.current(stored.get().settings());
        connects.getFirst().complete(old);
        var v1 = stored.get().settings();

        stored.set(cluster(2, 60));
        var v2 = stored.get().settings();
        assertTrue(sessions.current(v2).isEmpty());
        verify(old).closeAsync();
        var replacement = mock(CqlSession.class);
        connects.get(1).complete(replacement);
        assertSame(replacement, sessions.require(v2));

        assertTrue(sessions.current(v1).isEmpty(), "a request that read the old version is rejected");
        assertEquals(2, connects.size(), "and does not rebuild the superseded version");
    }

    @Test
    void failedConnectIsRetriedOnlyAfterTheReconnectDelay() throws Exception {
        stored.set(cluster(1, 1));
        var v1 = stored.get().settings();
        sessions.current(v1);
        connects.getFirst().completeExceptionally(new IllegalStateException("unreachable"));
        sessions.current(v1);
        assertEquals(1, connects.size(), "no reconnect storm while the delay runs");
        Thread.sleep(1_100);
        sessions.current(v1);
        assertEquals(2, connects.size(), "retried once the cluster's reconnect delay has passed");
    }

    @Test
    void connectFinishingAfterShutdownIsClosed() {
        sessions.current(stored.get().settings());
        sessions.close();
        var late = mock(CqlSession.class);
        connects.getFirst().complete(late);
        verify(late).closeAsync();
        assertTrue(sessions.current(stored.get().settings()).isEmpty());
    }

    @Test
    void disabledClusterHasNoSession() {
        var enabled = stored.get().settings();
        sessions.current(enabled);
        var session = mock(CqlSession.class);
        connects.getFirst().complete(session);
        stored.set(new ClusterRecord("east", List.of(new ContactPoint("localhost", 9042)),
            "dc1", 1, 1, List.of(), 10, 60, 5, 60, false, 2));
        assertTrue(sessions.current(cluster(2, 60).settings()).isEmpty());
        verify(session).closeAsync();
    }

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
        var discovered = new InetSocketAddress("172.28.91.11", 9042);
        assertEquals(9042, new LocalAddressTranslator(first).translate(discovered).getPort());
        assertEquals(9142, new LocalAddressTranslator(second).translate(discovered).getPort());
        a.close();
        b.close();
    }
}
