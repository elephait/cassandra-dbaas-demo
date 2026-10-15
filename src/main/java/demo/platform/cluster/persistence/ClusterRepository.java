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

    @Transactional(propagation = Propagation.MANDATORY)
    public ClusterRecord save(ClusterRecord cluster) {
        ClusterEntity existing = entities.findLockedById(cluster.id()).orElse(null);
        if (existing == null) return insert(cluster);
        return update(existing, cluster);
    }

    @Transactional(readOnly = true)
    public ClusterRecord get(String clusterId) {
        return entities.findById(clusterId).map(this::toDomain).orElse(null);
    }

    @Transactional(readOnly = true)
    public List<ClusterRecord> list(boolean enabledOnly) {
        return entities.findAll().stream().map(this::toDomain)
            .filter(cluster -> !enabledOnly || cluster.enabled())
            .sorted(Comparator.comparing(cluster -> cluster.id()))
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

}
