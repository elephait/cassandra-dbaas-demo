package demo.platform;

import demo.platform.cluster.ClusterConfigurationService;
import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.AddressMapping;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.cassandra.CassandraSessions;
import demo.platform.common.PlatformProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Registers the "default" cluster from deployment configuration on first start, then starts connecting to it.
 */
@Component
@RequiredArgsConstructor
@Slf4j
class PlatformBootstrap {
    private final PlatformProperties properties;
    private final ClusterConfigurationService configurations;
    private final ClusterRegistryService clusters;
    private final CassandraSessions sessions;

    private static List<String> split(String value) {
        if (value == null || value.isBlank()) return List.of();
        return Arrays.stream(value.split(",")).map(String::trim).filter(item -> !item.isBlank()).toList();
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
        if (configurations.register(defaultCluster())) log.info("Registered bootstrap cluster {}", ClusterRecord.DEFAULT_CLUSTER);
        try {
            sessions.current(clusters.requireEnabled(ClusterRecord.DEFAULT_CLUSTER)); // warm up; never blocks
        } catch (RuntimeException disabled) {
            log.info("Default cluster is not enabled; no session started");
        }
    }

    private ClusterRecord defaultCluster() {
        List<ContactPoint> points = split(properties.getContactPoints()).stream()
            .map(PlatformBootstrap::contactPoint).toList();
        List<AddressMapping> mappings = properties.getAddressTranslation().isEnabled()
            ? split(properties.getAddressTranslation().getMappings()).stream().map(PlatformBootstrap::addressMapping).toList()
            : List.of();
        return new ClusterRecord(ClusterRecord.DEFAULT_CLUSTER, points, properties.getDatacenter(),
            properties.getCluster().getExpectedNodes(), properties.getCassandra().getReplicationFactor(), mappings,
            properties.getCassandra().getRequestTimeoutSeconds(), properties.getCassandra().getReconnectSeconds(),
            properties.getMaxKeyspaces(), properties.getProvisioningLeaseSeconds(), true, 0);
    }
}
