package demo.platform.repair.jmx;

import demo.platform.cluster.ClusterRegistryService;
import demo.platform.misc.ApiExceptionHandler.CassandraProperties;
import demo.platform.misc.CassandraConnectionManager;
import demo.platform.repair.RepairClaimService;
import demo.platform.repair.RepairConfigurationService;
import demo.platform.repair.persistence.RepairExecutionRepository;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Map;
import static org.mockito.Mockito.*;

class JmxRepairWorkerTest {
    private final JmxRepairRepository progress = mock(JmxRepairRepository.class);
    private final RepairManagementClient client = mock(RepairManagementClient.class);
    private final JmxRepairProperties properties = new JmxRepairProperties();
    private final JmxRepairProperties.Endpoint endpoint = new JmxRepairProperties.Endpoint();

    private JmxRepairRepository.Task task(String state) {
        return new JmxRepairRepository.Task("op", 0, "n1", "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi", "host", state,
            7, 123L, "claim", "default", "team", Instant.now());
    }

    private JmxRepairWorker worker() {
        var configuration = new CassandraProperties();
        configuration.getRepair().setWorkerEnabled(false);
        endpoint.setHostId("host");
        endpoint.setServiceUrl(task("RUNNING").serviceUrl());
        // The mocked worker tests override identity configuration validation; separate real JMX
        // transport and repository tests exercise the actual values and persistence boundaries.
        var settings = spy(properties);
        doReturn(endpoint).when(settings).require("default", "n1");
        return new JmxRepairWorker(configuration, mock(RepairClaimService.class), mock(RepairExecutionRepository.class),
            mock(RepairConfigurationService.class), progress, settings, client,
            mock(ClusterRegistryService.class), mock(CassandraConnectionManager.class));
    }

    @Test void missingStatusDoesNotResubmitOrAdvance() throws Exception {
        try (var unused = new CloseWorker(worker())) {
            var remote = mock(RepairManagementClient.Session.class);
            when(client.connect(endpoint)).thenReturn(remote);
            when(remote.identity()).thenReturn(new RepairManagementClient.Identity("host", 123));
            when(remote.status(7)).thenReturn(new RepairManagementClient.Observation(RepairManagementClient.Status.MISSING, "absent"));
            var task = task("RUNNING");
            unused.worker.step(task);
            verify(progress).defer(task, "absent", properties.getPollSeconds());
            verify(remote, never()).submit(anyString());
            verify(progress, never()).complete(any(), anyString());
        }
    }

    @Test void cassandraRestartDoesNotReuseCommandNumber() throws Exception {
        try (var unused = new CloseWorker(worker())) {
            var remote = mock(RepairManagementClient.Session.class);
            when(client.connect(endpoint)).thenReturn(remote);
            when(remote.identity()).thenReturn(new RepairManagementClient.Identity("host", 456));
            unused.worker.step(task("RUNNING"));
            verify(progress).unknown(any(), contains("process changed"));
            verify(remote, never()).status(anyInt());
            verify(remote, never()).submit(anyString());
        }
    }

    @Test void expiredSubmissionIsNeverResubmitted() throws Exception {
        try (var unused = new CloseWorker(worker())) {
            unused.worker.step(task("SUBMITTING"));
            verify(progress).unknown(any(), contains("disappeared"));
            verifyNoInteractions(client);
        }
    }

    @Test void incompatibleServerStatusImmediatelyBlocksTheRun() throws Exception {
        try (var unused = new CloseWorker(worker())) {
            var remote = mock(RepairManagementClient.Session.class);
            when(client.connect(endpoint)).thenReturn(remote);
            when(remote.identity()).thenReturn(new RepairManagementClient.Identity("host", 123));
            when(remote.status(7)).thenReturn(new RepairManagementClient.Observation(
                RepairManagementClient.Status.UNRECOGNIZED, "[NEW_SERVER_STATE]"));
            unused.worker.step(task("RUNNING"));
            verify(progress).unknown(any(), contains("Unsupported Cassandra repair status"));
            verify(progress, never()).defer(any(), anyString(), anyInt());
            verify(remote, never()).submit(anyString());
        }
    }

    private record CloseWorker(JmxRepairWorker worker) implements AutoCloseable {
        public void close() { worker.close(); }
    }
}
