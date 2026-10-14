package demo.platform.cluster.persistence;

import org.hibernate.annotations.BatchSize;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.ArrayList;
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
    @JdbcTypeCode(SqlTypes.TIMESTAMP)
    private Instant updatedAt;

    public ClusterEntity(String id) {
        this.id = id;
    }

    public void update(List<ContactPointEmbeddable> points, String datacenter, int expectedNodes,
                       int replicationFactor, Set<AddressMappingEmbeddable> mappings,
                       int requestTimeoutSeconds, int reconnectSeconds, int maxKeyspaces,
                       int provisioningLeaseSeconds, boolean enabled) {
        this.contactPoints.clear();
        this.contactPoints.addAll(points);
        this.datacenter = datacenter;
        this.expectedNodes = expectedNodes;
        this.replicationFactor = replicationFactor;
        this.addressMappings.clear();
        this.addressMappings.addAll(mappings);
        this.requestTimeoutSeconds = requestTimeoutSeconds;
        this.reconnectSeconds = reconnectSeconds;
        this.maxKeyspaces = maxKeyspaces;
        this.provisioningLeaseSeconds = provisioningLeaseSeconds;
        this.enabled = enabled;
    }

    public List<ContactPointEmbeddable> contactPoints() {
        return List.copyOf(contactPoints);
    }

    public Set<AddressMappingEmbeddable> addressMappings() {
        return Set.copyOf(addressMappings);
    }

    @PrePersist
    @PreUpdate
    void updateTimestamp() {
        updatedAt = Instant.now();
    }
}
