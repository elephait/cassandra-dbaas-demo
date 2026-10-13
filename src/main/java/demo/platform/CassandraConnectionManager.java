package demo.platform;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class CassandraConnectionManager {
    private final ScheduledExecutorService connector = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "cassandra-connector");
        thread.setDaemon(true);
        return thread;
    });
    private final CassandraProperties properties;
    private volatile CqlSession session;

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        connector.scheduleWithFixedDelay(this::connectIfNeeded, 0,
                properties.getCassandra().getReconnectSeconds(), TimeUnit.SECONDS);
    }

    public Optional<CqlSession> currentSession() {
        return Optional.ofNullable(session);
    }

    public CqlSession requireSession() {
        return currentSession().orElseThrow(() -> new ResponseStatusException(
                HttpStatus.SERVICE_UNAVAILABLE, "Cassandra is unavailable; connection retry is in progress"));
    }

    private void connectIfNeeded() {
        if (session != null)
            return;
        try {
            session = buildSession();
            log.info("Connected to Cassandra");
        } catch (RuntimeException exception) {
            log.warn("Cassandra is unavailable; retrying in {} seconds: {}",
                    properties.getCassandra().getReconnectSeconds(), exception.getMessage());
        }
    }

    private CqlSession buildSession() {
        var builder = CqlSession.builder().withLocalDatacenter(properties.getDatacenter());
        for (String point : properties.getContactPoints().split(",")) {
            String[] parts = point.trim().split(":");
            builder.addContactPoint(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
        }
        var driverConfig = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                        Duration.ofSeconds(properties.getCassandra().getRequestTimeoutSeconds()));
        if (properties.getAddressTranslation().isEnabled()) {
            LocalAddressTranslator.configure(parseMappings(properties.getAddressTranslation().getMappings()));
            driverConfig.withClass(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, LocalAddressTranslator.class);
        }
        builder.withConfigLoader(driverConfig.build());
        return builder.build();
    }

    private static Map<String, InetSocketAddress> parseMappings(String mappings) {
        Map<String, InetSocketAddress> result = new HashMap<>();
        if (mappings.isBlank())
            return result;
        for (String mapping : mappings.split(",")) {
            String[] parts = mapping.trim().split("=", 2);
            if (parts.length != 2)
                throw new IllegalArgumentException("Address mapping must use discovered-host=reachable-host:port: " + mapping);
            String[] target = parts[1].split(":", 2);
            if (target.length != 2)
                throw new IllegalArgumentException("Address mapping target must use host:port: " + mapping);
            result.put(parts[0].trim(), new InetSocketAddress(target[0].trim(), Integer.parseInt(target[1].trim())));
        }
        return result;
    }

    @PreDestroy
    void close() {
        connector.shutdownNow();
        CqlSession current = session;
        if (current != null)
            current.close();
    }
}