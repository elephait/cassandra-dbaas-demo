package demo.platform.cluster.persistence;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.AddressMapping;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ClusterRepository {
    private final ClusterJpaRepository entities;
    @jakarta.persistence.PersistenceContext
    private jakarta.persistence.EntityManager entityManager;

    @Transactional(propagation = Propagation.MANDATORY)
    public ClusterRecord lockConfiguration(String clusterId) {
        ClusterEntity existing = entities.findLockedById(clusterId).orElse(null);
        if (existing == null) return null;
        entityManager.refresh(existing);
        return toDomain(existing);
    }

    private static void apply(ClusterEntity entity, ClusterRecord cluster) {
        List<ContactPointEmbeddable> points = cluster.contactPoints().stream()
            .map(point -> new ContactPointEmbeddable(point.host(), point.port())).toList();
        HashSet<AddressMappingEmbeddable> mappings = new HashSet<>();
        cluster.addressMappings().forEach(mapping -> mappings.add(new AddressMappingEmbeddable(
            mapping.discoveredHost(), mapping.target().host(), mapping.target().port())));
        entity.update(points, cluster.datacenter(), cluster.expectedNodes(), cluster.replicationFactor(), mappings,
            cluster.requestTimeoutSeconds(), cluster.reconnectSeconds(), cluster.maxKeyspaces(),
            cluster.provisioningLeaseSeconds(), cluster.enabled());
    }

    // For callers that already took the row lock through lockConfiguration in this same transaction.
    // Re-issuing the locking SELECT would be pure overhead, so the entity is resolved from the
    // persistence context when present and only looked up when it is genuinely new.
    @Transactional(propagation = Propagation.MANDATORY)
    public ClusterRecord saveLocked(ClusterRecord cluster) {
        ClusterEntity existing = entityManager.find(ClusterEntity.class, cluster.id());
        if (existing == null) return insert(cluster);
        return update(existing, cluster);
    }

    @Transactional(readOnly = true)
    public ClusterRecord get(String clusterId) {
        return entities.findById(clusterId).map(this::toDomain).orElse(null);
    }

    @Transactional(readOnly = true)
    public ClusterRecord.Settings getSettings(String clusterId) {
        return entities.findSettings(clusterId).orElse(null);
    }

    // Scalar-only read for callers that only compare configuration versions.
    @Transactional(readOnly = true)
    public List<ClusterRecord.ConfigState> listEnabledConfigStates() {
        return entities.findEnabledConfigStates().stream()
            .map(state -> new ClusterRecord.ConfigState(state.getId(), state.getConfigVersion(), state.isEnabled()))
            .toList();
    }

    private ClusterRecord insert(ClusterRecord cluster) {
        ClusterEntity entity = new ClusterEntity(cluster.id());
        apply(entity, cluster);
        entity = entities.saveAndFlush(entity);
        return toDomain(entity);
    }

    private ClusterRecord update(ClusterEntity entity, ClusterRecord cluster) {
        if (toDomain(entity).equals(cluster.withVersion(entity.configVersion()))) return toDomain(entity);
        apply(entity, cluster);
        entities.flush();
        return toDomain(entity);
    }

    private ClusterRecord toDomain(ClusterEntity entity) {
        List<ContactPoint> points = entity.contactPoints().stream()
            .map(point -> new ContactPoint(point.host(), point.port())).toList();
        List<AddressMapping> mappings = entity.addressMappings().stream()
            .map(mapping -> new AddressMapping(mapping.discoveredHost(),
                new ContactPoint(mapping.targetHost(), mapping.targetPort())))
            .sorted(Comparator.comparing(AddressMapping::discoveredHost)).toList();
        return new ClusterRecord(entity.id(), points, entity.datacenter(), entity.expectedNodes(),
            entity.replicationFactor(), mappings, entity.requestTimeoutSeconds(), entity.reconnectSeconds(),
            entity.maxKeyspaces(), entity.provisioningLeaseSeconds(), entity.enabled(), entity.configVersion());
    }


    @Transactional(readOnly = true)
    public ClusterRecord.ConfigState getConfigState(String clusterId) {
        return entities.findConfigState(clusterId)
            .map(state -> new ClusterRecord.ConfigState(state.getId(), state.getConfigVersion(), state.isEnabled()))
            .orElse(null);
    }

    @Transactional(readOnly = true)
    public List<ClusterRecord.Summary> listSummaries() {
        return entities.findSummaries().stream().map(summary -> new ClusterRecord.Summary(summary.getId(),
            summary.getDatacenter(), summary.getExpectedNodes(), summary.getReplicationFactor(),
            summary.isEnabled(), summary.getConfigVersion())).toList();
    }
}
