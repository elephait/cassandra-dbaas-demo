package demo.platform.cluster.persistence;

import java.util.Comparator;
import org.springframework.transaction.annotation.Propagation;

import demo.platform.cluster.ClusterRecord;
import demo.platform.repair.RepairState;
import demo.platform.keyspace.ProvisioningState;
import demo.platform.cluster.ClusterRecord.AddressMapping;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.misc.ControlPlaneExceptions;
import demo.platform.keyspace.persistence.KeyspaceJpaRepository;
import demo.platform.repair.persistence.OperationJpaRepository;
import demo.platform.repair.persistence.RepairLockEntity;
import demo.platform.repair.persistence.RepairLockJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ClusterRepository {
    public static final String DEFAULT_CLUSTER = "default";

    private final ClusterJpaRepository entities;
    private final KeyspaceJpaRepository keyspaces;
    private final OperationJpaRepository operations;
    private final RepairLockJpaRepository repairLocks;
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

    @Transactional
    public ClusterRecord registerConfiguredCluster(ClusterRecord configured) {
        ClusterEntity existing = entities.findLockedById(configured.id()).orElse(null);
        if (existing == null) return insert(configured);
        if (existing.configVersion() == 0) return update(existing, configured);
        return toDomain(existing);
    }

    @Transactional
    public ClusterRecord save(ClusterRecord cluster) {
        ClusterEntity existing = entities.findLockedById(cluster.id()).orElse(null);
        if (existing == null) return insert(cluster);
        rejectUpdateWithPendingWork(cluster.id());
        return update(existing, cluster);
    }

    @Transactional(readOnly = true)
    public ClusterRecord get(String clusterId) {
        return entities.findById(clusterId).map(this::toDomain).orElse(null);
    }

    @Transactional(readOnly = true)
    public ClusterRecord requireEnabled(String clusterId) {
        ClusterRecord cluster = get(clusterId);
        if (cluster == null || !cluster.enabled()) throw new ControlPlaneExceptions.UnknownCluster(clusterId);
        return cluster;
    }

    @Transactional(readOnly = true)
    public List<ClusterRecord> list(boolean enabledOnly) {
        return entities.findAll().stream().map(this::toDomain)
            .filter(cluster -> !enabledOnly || cluster.enabled())
            .sorted(Comparator.comparing(cluster -> cluster.id()))
            .toList();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void lockEnabled(String clusterId) {
        ClusterEntity cluster = entities.findLockedById(clusterId)
            .orElseThrow(() -> new ControlPlaneExceptions.UnknownCluster(clusterId));
        if (!cluster.enabled()) throw new ControlPlaneExceptions.UnknownCluster(clusterId);
    }

    private ClusterRecord insert(ClusterRecord cluster) {
        ClusterEntity entity = new ClusterEntity(cluster.id());
        apply(entity, cluster);
        entity = entities.saveAndFlush(entity);
        repairLocks.saveAndFlush(new RepairLockEntity(cluster.id()));
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

    private void rejectUpdateWithPendingWork(String clusterId) {
        if (operations.countByClusterIdAndStateIn(clusterId, List.of(
            RepairState.QUEUED, RepairState.CLAIMED,
            RepairState.RUNNING, RepairState.UNKNOWN)) > 0 || keyspaces.countProvisioningByState(clusterId, ProvisioningState.PROVISIONING) > 0)
            throw new ControlPlaneExceptions.Conflict(
                "Cluster configuration cannot change while provisioning or repair work is pending");
    }
}
