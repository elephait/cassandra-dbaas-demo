package demo.platform.cluster.persistence;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.AddressMapping;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.hibernate.annotations.BatchSize;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Entity
@Table(name = "clusters")
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ClusterEntity {
    @Id
    @Column(name = "cluster_id", length = 64)
    private String id;

    @ElementCollection(fetch = FetchType.LAZY)
    @BatchSize(size = 32)
    @CollectionTable(name = "cluster_contact_points", joinColumns = @JoinColumn(name = "cluster_id"))
    @OrderColumn(name = "ordinal")
    private List<ContactPointEmbeddable> contactPoints = new ArrayList<>();

    @Column(name = "datacenter", nullable = false, length = 128)
    private String datacenter;

    @Column(name = "expected_nodes", nullable = false)
    private int expectedNodes;

    @Column(name = "replication_factor", nullable = false)
    private int replicationFactor;

    @ElementCollection(fetch = FetchType.LAZY)
    @BatchSize(size = 32)
    @CollectionTable(name = "cluster_address_mappings", joinColumns = @JoinColumn(name = "cluster_id"))
    private Set<AddressMappingEmbeddable> addressMappings = new HashSet<>();

    @Column(name = "request_timeout_seconds", nullable = false)
    private int requestTimeoutSeconds;

    @Column(name = "reconnect_seconds", nullable = false)
    private int reconnectSeconds;

    @Column(name = "max_keyspaces", nullable = false)
    private int maxKeyspaces;

    @Column(name = "provisioning_lease_seconds", nullable = false)
    private int provisioningLeaseSeconds;

    @Column(name = "enabled", nullable = false)
    private boolean enabled;

    @Version
    @Column(name = "config_version", nullable = false)
    private long configVersion;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public ClusterEntity(String id) {
        this.id = id;
    }

    /** Copies settings from the domain record; the version is managed by JPA. */
    public void apply(ClusterRecord cluster) {
        contactPoints.clear();
        cluster.contactPoints().forEach(point -> contactPoints.add(new ContactPointEmbeddable(point.host(), point.port())));
        addressMappings.clear();
        cluster.addressMappings().forEach(mapping -> addressMappings.add(new AddressMappingEmbeddable(
            mapping.discoveredHost(), mapping.target().host(), mapping.target().port())));
        datacenter = cluster.datacenter();
        expectedNodes = cluster.expectedNodes();
        replicationFactor = cluster.replicationFactor();
        requestTimeoutSeconds = cluster.requestTimeoutSeconds();
        reconnectSeconds = cluster.reconnectSeconds();
        maxKeyspaces = cluster.maxKeyspaces();
        provisioningLeaseSeconds = cluster.provisioningLeaseSeconds();
        enabled = cluster.enabled();
    }

    /** True when applying the record would change nothing, so the version must not move. */
    public boolean matches(ClusterRecord cluster) {
        return toRecord().equals(cluster.withVersion(configVersion));
    }

    public ClusterRecord toRecord() {
        List<ContactPoint> points = contactPoints.stream()
            .map(point -> new ContactPoint(point.host(), point.port())).toList();
        List<AddressMapping> mappings = addressMappings.stream()
            .map(mapping -> new AddressMapping(mapping.discoveredHost(),
                new ContactPoint(mapping.targetHost(), mapping.targetPort())))
            .sorted(Comparator.comparing(AddressMapping::discoveredHost)).toList();
        return new ClusterRecord(id, points, datacenter, expectedNodes, replicationFactor, mappings,
            requestTimeoutSeconds, reconnectSeconds, maxKeyspaces, provisioningLeaseSeconds, enabled, configVersion);
    }

    @PrePersist
    @PreUpdate
    void updateTimestamp() {
        updatedAt = Instant.now();
    }
}
