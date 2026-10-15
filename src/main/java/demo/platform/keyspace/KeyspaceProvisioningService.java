package demo.platform.keyspace;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.keyspace.KeyspaceRecord.ProvisionDecision;
import demo.platform.keyspace.persistence.KeyspaceEntity;
import demo.platform.keyspace.persistence.KeyspaceId;
import demo.platform.keyspace.persistence.KeyspaceJpaRepository;
import demo.platform.misc.ControlPlaneExceptions;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class KeyspaceProvisioningService {
    private final KeyspaceJpaRepository keyspaces;
    private final ClusterRegistryService clusters;

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public ProvisionDecision begin(String clusterId, String keyspace, int maxKeyspaces, int leaseSeconds) {
        return begin(clusterId, keyspace, maxKeyspaces, leaseSeconds, -1);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public ProvisionDecision begin(String clusterId, String keyspace, int maxKeyspaces, int leaseSeconds,
                                   long expectedConfigVersion) {
        KeyspaceNames.validate(keyspace);
        if (leaseSeconds < 3) throw new IllegalArgumentException("Provisioning lease must be at least three seconds");
        // Lock before counting: READ_COMMITTED observes the preceding owner's committed reservation.
        // This serializes same-cluster admission across replicas, including absent keyspaces.
        clusters.lockForAdmission(clusterId, expectedConfigVersion);
        var id = new KeyspaceId(clusterId, keyspace);
        KeyspaceEntity existing = keyspaces.findById(id).orElse(null);
        Instant now = keyspaces.databaseTime().toInstant();
        if (existing != null) {
            if (existing.getState() == ProvisioningState.READY) return new ProvisionDecision(true, null);
            if (existing.getOwner() != null && existing.getExpiresAt() != null && existing.getExpiresAt().isAfter(now))
                throw new ControlPlaneExceptions.Conflict("Keyspace provisioning is already in progress");
        } else {
            if (keyspaces.countById_ClusterId(clusterId) >= maxKeyspaces)
                throw new ControlPlaneExceptions.KeyspaceQuotaExceeded();
            existing = new KeyspaceEntity(id);
        }
        String owner = UUID.randomUUID().toString();
        existing.claim(owner, now, leaseSeconds);
        keyspaces.saveAndFlush(existing);
        return new ProvisionDecision(false, owner);
    }

    @Transactional
    public boolean renew(String clusterId, String keyspace, String owner, int seconds) {
        return keyspaces.renew(new KeyspaceId(clusterId, keyspace), owner,
            keyspaces.databaseTime().toInstant().plusSeconds(seconds), ProvisioningState.PROVISIONING) == 1;
    }

    @Transactional
    public void complete(String clusterId, String keyspace, String owner) {
        if (keyspaces.complete(new KeyspaceId(clusterId, keyspace), owner,
            ProvisioningState.PROVISIONING, ProvisioningState.READY) != 1)
            throw new ControlPlaneExceptions.Conflict("Provisioning ownership was lost");
    }

    @Transactional
    public void release(String clusterId, String keyspace, String owner) {
        keyspaces.release(new KeyspaceId(clusterId, keyspace), owner, ProvisioningState.PROVISIONING);
    }

    @Transactional(readOnly = true)
    public boolean owns(String clusterId, String keyspace, String owner) {
        return keyspaces.owns(new KeyspaceId(clusterId, keyspace), owner, ProvisioningState.PROVISIONING);
    }

    @Transactional(readOnly = true)
    public boolean hasPendingWork(String clusterId) {
        return keyspaces.countProvisioningByState(clusterId, ProvisioningState.PROVISIONING) > 0;
    }

    @Transactional(readOnly = true)
    public List<KeyspaceRecord> list(String clusterId, int limit) {
        return keyspaces.findById_ClusterIdOrderByUpdatedAtDescId_KeyspaceAsc(clusterId, PageRequest.of(0, limit))
            .stream().map(k -> new KeyspaceRecord(k.getId().getClusterId(), k.getId().getKeyspace(),
                k.getState(), k.getUpdatedAt())).toList();
    }
}
