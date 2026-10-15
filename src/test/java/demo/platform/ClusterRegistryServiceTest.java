package demo.platform;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRecord.ContactPoint;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.persistence.ClusterRepository;
import demo.platform.misc.ControlPlaneExceptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ClusterRegistryServiceTest {
    private static ClusterRecord cluster(boolean enabled) {
        return new ClusterRecord("default", List.of(new ContactPoint("localhost", 9042)),
            "dc1", 3, 3, List.of(), 10, 60, 5, 60, enabled, 2);
    }

    @Test
    void lookupRequiresRegisteredEnabledCluster() {
        var repository = mock(ClusterRepository.class);
        var registry = new ClusterRegistryService(repository);
        assertThrows(ControlPlaneExceptions.UnknownCluster.class, () -> registry.requireEnabled("default"));
        when(repository.get("default")).thenReturn(cluster(false));
        assertThrows(ControlPlaneExceptions.UnknownCluster.class, () -> registry.requireEnabled("default"));
        var enabled = cluster(true);
        when(repository.get("default")).thenReturn(enabled);
        assertSame(enabled, registry.requireEnabled("default"));
    }

    @Test
    void admissionValidatesVersionFromLockedSnapshot() {
        var repository = mock(ClusterRepository.class);
        var registry = new ClusterRegistryService(repository);
        var current = cluster(true);
        when(repository.lockConfiguration("default")).thenReturn(current);
        assertThrows(ControlPlaneExceptions.Conflict.class, () -> registry.lockForAdmission("default", 1));
        assertSame(current, registry.lockForAdmission("default", 2));
        assertSame(current, registry.lockForAdmission("default", -1));
        verify(repository, never()).get(anyString());
    }

    @Test
    void admissionRejectsMissingOrDisabledCluster() {
        var repository = mock(ClusterRepository.class);
        var registry = new ClusterRegistryService(repository);
        assertThrows(ControlPlaneExceptions.UnknownCluster.class, () -> registry.lockForAdmission("default", -1));
        when(repository.lockConfiguration("default")).thenReturn(cluster(false));
        assertThrows(ControlPlaneExceptions.UnknownCluster.class, () -> registry.lockForAdmission("default", -1));
    }
}
