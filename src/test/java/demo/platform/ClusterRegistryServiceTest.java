package demo.platform;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.persistence.ClusterEntity;
import demo.platform.cluster.persistence.ClusterJpaRepository;
import demo.platform.common.ControlPlaneExceptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ClusterRegistryServiceTest {
    private final ClusterJpaRepository repository = mock(ClusterJpaRepository.class);
    private final ClusterRegistryService registry = new ClusterRegistryService(repository);

    private static ClusterRecord cluster(boolean enabled) {
        return new ClusterRecord("default", List.of(new ContactPoint("localhost", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 5, 60, enabled, 2);
    }

    private void locked(ClusterRecord cluster) {
        var entity = mock(ClusterEntity.class);
        when(entity.toRecord()).thenReturn(cluster);
        when(repository.findForUpdateById("default")).thenReturn(Optional.of(entity));
    }

    @Test
    void lookupRequiresRegisteredEnabledCluster() {
        when(repository.findSettingsById("default")).thenReturn(Optional.empty());
        assertThrows(ControlPlaneExceptions.UnknownCluster.class, () -> registry.requireEnabled("default"));
        when(repository.findSettingsById("default")).thenReturn(Optional.of(cluster(false).settings()));
        assertThrows(ControlPlaneExceptions.UnknownCluster.class, () -> registry.requireEnabled("default"));
        var enabled = cluster(true).settings();
        when(repository.findSettingsById("default")).thenReturn(Optional.of(enabled));
        assertSame(enabled, registry.requireEnabled("default"));
    }

    @Test
    void admissionValidatesVersionFromLockedRow() {
        locked(cluster(true));
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> registry.lockForAdmission("default", 1));
        assertEquals(cluster(true), registry.lockForAdmission("default", 2));
        assertEquals(cluster(true), registry.lockForAdmission("default", -1));
        verify(repository, never()).findById(anyString());
    }

    @Test
    void admissionRejectsMissingOrDisabledCluster() {
        when(repository.findForUpdateById("default")).thenReturn(Optional.empty());
        assertThrows(ControlPlaneExceptions.UnknownCluster.class, () -> registry.lockForAdmission("default", -1));
        locked(cluster(false));
        assertThrows(ControlPlaneExceptions.UnknownCluster.class, () -> registry.lockForAdmission("default", -1));
    }
}
