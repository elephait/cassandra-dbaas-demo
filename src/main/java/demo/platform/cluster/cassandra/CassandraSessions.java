package demo.platform.cluster.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.common.ControlPlaneExceptions;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * One driver {@link CqlSession} per registered cluster and configuration version. The driver is the connection
 * pool: it owns per-node connections, node reconnection, load balancing and retries. This class only decides
 * which session matches a cluster's current configuration.
 *
 * <p>Sessions are built lazily and asynchronously on first use, so requests never block on a connect. Every
 * request carries the configuration version it read from the database; a newer version replaces the session,
 * and the superseded one is closed gracefully (in-flight requests complete). A failed initial connect is
 * retried after the cluster's {@code reconnectSeconds}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CassandraSessions {
    private final ClusterRegistryService clusters;
    private final Map<String, Entry> sessions = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /** The connected session for exactly this configuration version; empty while connecting. Never blocks. */
    public Optional<CqlSession> current(ClusterRecord.Settings cluster) {
        Entry entry = entryFor(cluster);
        return entry == null || entry.configVersion() != cluster.configVersion() ? Optional.empty() : entry.connected();
    }

    public CqlSession require(ClusterRecord.Settings cluster) {
        return current(cluster).orElseThrow(() -> new ControlPlaneExceptions.Unavailable(
            "Cassandra session for cluster " + cluster.id() + " is connecting; retry"));
    }

    /** Closes this replica's session for the cluster; the next request connects with current configuration. */
    public void invalidate(String clusterId) {
        Entry removed = sessions.remove(clusterId);
        if (removed != null) removed.close();
    }

    private Entry entryFor(ClusterRecord.Settings cluster) {
        if (closed) return null;
        Entry existing = sessions.get(cluster.id());
        if (existing != null && !existing.needsReplacement(cluster.configVersion())) return existing;
        // Connection endpoints are read outside the map lock; the compute below only swaps entries.
        ClusterRecord config = clusters.get(cluster.id());
        if (config == null || !config.enabled()) {
            invalidate(cluster.id());
            return null;
        }
        return sessions.compute(cluster.id(), (id, current) -> {
            if (current != null && !current.needsReplacement(config.configVersion())) return current;
            if (current != null) current.close();
            log.info("Connecting to Cassandra cluster {} at configuration version {}", id, config.configVersion());
            CompletableFuture<CqlSession> session = start(config);
            session.whenComplete((connected, failure) -> {
                if (failure == null) log.info("Connected to Cassandra cluster {} at configuration version {}",
                    id, config.configVersion());
                else log.warn("Cassandra cluster {} could not connect; retrying in {}s: {}",
                    id, config.reconnectSeconds(), failure.getMessage());
            });
            return new Entry(config.configVersion(), session,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(config.reconnectSeconds()));
        });
    }

    private CompletableFuture<CqlSession> start(ClusterRecord cluster) {
        try {
            return connect(cluster).toCompletableFuture();
        } catch (RuntimeException invalidConfiguration) {
            return CompletableFuture.failedFuture(invalidConfiguration);
        }
    }

    protected CompletionStage<CqlSession> connect(ClusterRecord cluster) {
        var config = DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(cluster.requestTimeoutSeconds()))
            .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(cluster.reconnectSeconds()));
        if (cluster.addressTranslationEnabled()) {
            config.withStringMap(LocalAddressTranslator.MAPPINGS, cluster.addressMappings().stream().collect(
                    Collectors.toMap(ClusterRecord.AddressMapping::discoveredHost,
                        mapping -> mapping.target().host() + ":" + mapping.target().port())))
                .withClass(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, LocalAddressTranslator.class);
        }
        var builder = CqlSession.builder().withLocalDatacenter(cluster.datacenter()).withConfigLoader(config.build());
        cluster.contactPoints().forEach(point -> builder.addContactPoint(point.toSocketAddress()));
        return builder.buildAsync();
    }

    @PreDestroy
    public void close() {
        closed = true;
        sessions.values().forEach(Entry::close);
        sessions.clear();
    }

    private record Entry(long configVersion, CompletableFuture<CqlSession> session, long retryAtNanos) {
        Optional<CqlSession> connected() {
            return session.isDone() && !session.isCompletedExceptionally() ? Optional.of(session.join()) : Optional.empty();
        }

        /** Newer configuration, or a failed connect whose retry delay has passed. Never downgrades. */
        boolean needsReplacement(long requestedVersion) {
            if (requestedVersion != configVersion) return requestedVersion > configVersion;
            return session.isCompletedExceptionally() && System.nanoTime() >= retryAtNanos;
        }

        /** Graceful: closes now if connected, or as soon as a pending connect completes. */
        void close() {
            session.thenAccept(CqlSession::closeAsync);
        }
    }
}
