package demo.platform.misc;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.AddressMapping;
import demo.platform.cluster.ClusterRecord.ClusterConfiguration;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.LocalAddressTranslator;
import demo.platform.repair.OperationRecord.RepairPolicy;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CassandraConnectionManager {
    private final ScheduledExecutorService connector = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "cassandra-cluster-connector");
        thread.setDaemon(true);
        return thread;
    });
    private final Map<String, SessionHolder> sessions = new ConcurrentHashMap<>();
    private final Set<String> connecting = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> nextAttempts = new ConcurrentHashMap<>();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final Object publicationLock = new Object();
    private final ExecutorService connectionWorkers;
    private final ApiExceptionHandler.CassandraProperties bootstrap;
    private final ClusterConfigurationService configurations;

    @Autowired
    public CassandraConnectionManager(ApiExceptionHandler.CassandraProperties bootstrap,
                                      ClusterConfigurationService configurations) {
        this.bootstrap = bootstrap;
        this.configurations = configurations;
        int threads = bootstrap.getCassandra().getConnectionThreads();
        if (threads < 1 || threads > 64) throw new IllegalArgumentException("Connection threads must be 1..64");
        this.connectionWorkers = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(threads), new ThreadPoolExecutor.AbortPolicy());
    }

    CassandraConnectionManager(ApiExceptionHandler.CassandraProperties bootstrap) {
        this(bootstrap, null);
    }

    private static List<String> split(String value) {
        if (value == null || value.isBlank()) return List.of();
        return Arrays.stream(value.split(",")).map(String::trim)
            .filter(item -> !item.isBlank()).toList();
    }

    private static ContactPoint contactPoint(String value) {
        int separator = value.lastIndexOf(':');
        if (separator < 1) throw new IllegalArgumentException("Contact point must use host:port: " + value);
        return new ContactPoint(value.substring(0, separator), Integer.parseInt(value.substring(separator + 1)));
    }

    private static AddressMapping addressMapping(String value) {
        String[] parts = value.split("=", 2);
        if (parts.length != 2) throw new IllegalArgumentException("Address mapping must use host=host:port: " + value);
        return new AddressMapping(parts[0].trim(), contactPoint(parts[1]));
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        if (configurations == null) return;
        configurations.register(defaultConfiguration());
        connector.scheduleWithFixedDelay(this::refresh, 0,
            bootstrap.getPollIntervalSeconds(), TimeUnit.SECONDS);
    }

    public Optional<CqlSession> currentSession(ClusterRecord.Settings cluster) {
        SessionHolder holder = sessions.get(cluster.id());
        return holder == null || holder.configVersion() != cluster.configVersion()
            ? Optional.empty() : Optional.of(holder.session());
    }

    public CqlSession requireSession(ClusterRecord.Settings cluster) {
        SessionHolder holder = sessions.get(cluster.id());
        if (holder == null || holder.configVersion() != cluster.configVersion())
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Cluster session is reconnecting to the current configuration");
        return holder.session();
    }

    public void invalidate(String clusterId) {
        nextAttempts.remove(clusterId);
        SessionHolder removed = sessions.remove(clusterId);
        if (removed != null) removed.session().close();
    }

    public void requestRefresh() {
        if (!closing.get()) {
            try {
                connector.execute(this::refresh);
            } catch (RejectedExecutionException shutdown) { /* concurrent shutdown: next process will refresh */ }
        }
    }

    void refresh() {
        if (closing.get() || configurations == null) return;
        try {
            // Scalar-only: periodic refresh must not hydrate cluster contact points,
            // address mappings or repair policies just to compare a configuration version.
            var enabled = configurations.listConnectionStates();
            var enabledIds = enabled.stream().map(ClusterRecord.ConfigState::clusterId)
                .collect(Collectors.toSet());
            sessions.keySet().stream().filter(id -> !enabledIds.contains(id)).toList().forEach(this::invalidate);
            for (var state : enabled) {
                String clusterId = state.clusterId();
                SessionHolder existing = sessions.get(clusterId);
                if ((existing == null || existing.configVersion() != state.configVersion())
                    && System.nanoTime() >= nextAttempts.getOrDefault(clusterId, 0L) && connecting.add(clusterId)) {
                    try {
                        connectionWorkers.submit(() -> {
                            try {
                                connectIfNeeded(clusterId, state.configVersion());
                            } finally {
                                connecting.remove(clusterId);
                            }
                        });
                    } catch (RejectedExecutionException full) {
                        connecting.remove(clusterId);
                    }
                }
            }
        } catch (RuntimeException exception) {
            log.warn("Cannot refresh Cassandra cluster registry; retrying", exception);
        }
    }

    private void connectIfNeeded(String clusterId, long expectedConfigVersion) {
        SessionHolder existing = sessions.get(clusterId);
        if (existing != null && existing.configVersion() == expectedConfigVersion) return;
        CqlSession candidate = null;
        boolean published = false;
        int reconnectSeconds = bootstrap.getCassandra().getReconnectSeconds();
        try {
            ClusterRecord cluster = configurations.connectionDetails(clusterId);
            if (cluster == null || !cluster.enabled() || cluster.configVersion() != expectedConfigVersion) return;
            reconnectSeconds = cluster.reconnectSeconds();
            if (existing != null) invalidate(clusterId);
            candidate = buildSession(cluster);
            // A slow connection must not publish a session for a disabled or superseded configuration.
            var current = configurations.connectionState(clusterId);
            synchronized (publicationLock) {
                if (closing.get() || current == null || !current.enabled()
                    || current.configVersion() != cluster.configVersion()) return;
                SessionHolder raced = sessions.putIfAbsent(clusterId,
                    new SessionHolder(cluster.configVersion(), candidate));
                if (raced == null) {
                    published = true;
                    nextAttempts.remove(clusterId);
                    log.info("Connected to Cassandra cluster {} at configuration version {}", clusterId,
                        cluster.configVersion());
                }
            }
        } catch (RuntimeException exception) {
            nextAttempts.put(clusterId, System.nanoTime() + TimeUnit.SECONDS.toNanos(reconnectSeconds));
            log.warn("Cassandra cluster {} could not connect; retrying: {}", clusterId, exception.getMessage());
        } finally {
            if (candidate != null && !published) {
                try {
                    candidate.close();
                } catch (RuntimeException closeFailure) {
                    log.warn("Cannot close unpublished Cassandra session for {}", clusterId, closeFailure);
                }
            }
        }
    }

    protected CqlSession buildSession(ClusterRecord cluster) {
        var builder = CqlSession.builder().withLocalDatacenter(cluster.datacenter());
        cluster.contactPoints().stream().map(ContactPoint::toSocketAddress).forEach(builder::addContactPoint);
        var driverConfig = DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(cluster.requestTimeoutSeconds()));
        if (cluster.addressTranslationEnabled()) {
            var mappings = cluster.addressMappings().stream().collect(Collectors.toMap(
                AddressMapping::discoveredHost, mapping -> mapping.target().host() + ":" + mapping.target().port()));
            driverConfig.withStringMap(LocalAddressTranslator.MAPPINGS, mappings)
                .withClass(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, LocalAddressTranslator.class);
        }
        builder.withConfigLoader(driverConfig.build());
        return builder.build();
    }

    private ClusterConfiguration defaultConfiguration() {
        String id = ClusterRecord.DEFAULT_CLUSTER;
        List<ContactPoint> points = split(bootstrap.getContactPoints()).stream()
            .map(CassandraConnectionManager::contactPoint).toList();
        List<AddressMapping> mappings = bootstrap.getAddressTranslation().isEnabled()
            ? split(bootstrap.getAddressTranslation().getMappings()).stream()
            .map(CassandraConnectionManager::addressMapping).toList()
            : List.of();
        ClusterRecord cluster = new ClusterRecord(id, points, bootstrap.getDatacenter(),
            bootstrap.getCluster().getExpectedNodes(), bootstrap.getCassandra().getReplicationFactor(),
            mappings, bootstrap.getCassandra().getRequestTimeoutSeconds(),
            bootstrap.getCassandra().getReconnectSeconds(), bootstrap.getMaxKeyspaces(),
            bootstrap.getProvisioningLeaseSeconds(), true, 1);
        RepairPolicy policy = new RepairPolicy(id, split(bootstrap.getRepair().getNodes()),
            bootstrap.getRepair().getCommand(), bootstrap.getRepair().getLeaseMinutes(), 1);
        return new ClusterConfiguration(cluster, policy);
    }

    @PreDestroy
    void close() {
        List<SessionHolder> detached;
        synchronized (publicationLock) {
            closing.set(true);
            detached = List.copyOf(sessions.values());
            sessions.clear();
        }
        connector.shutdownNow();
        connectionWorkers.shutdownNow();
        detached.forEach(holder -> holder.session().close());
    }

    private record SessionHolder(long configVersion, CqlSession session) {
    }
}
