package demo.platform.repair;

import demo.platform.common.DatabaseClock;
import demo.platform.repair.jmx.JmxRepairProperties;
import demo.platform.repair.jmx.RepairManagementClient;
import demo.platform.repair.jmx.RepairManagementClient.Identity;
import demo.platform.repair.jmx.RepairManagementClient.Observation;
import demo.platform.repair.jmx.RepairManagementClient.Status;
import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairExecutionRepository.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Instant;

import static org.mockito.Mockito.*;

class RepairExecutionServiceTest {
    private static final String URL = "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi";
    private static final String HOST = "11111111-1111-4111-8111-111111111111";
    private static final Instant SUBMITTED = Instant.parse("2030-01-01T00:00:00Z");

    private final RepairExecutionRepository progress = mock(RepairExecutionRepository.class);
    private final RepairManagementClient client = mock(RepairManagementClient.class);
    private final RepairManagementClient.Session remote = mock(RepairManagementClient.Session.class);
    private final RepairTopology topology = mock(RepairTopology.class);
    private final DatabaseClock clock = mock(DatabaseClock.class);
    private final JmxRepairProperties properties = new JmxRepairProperties();
    private final JmxRepairProperties.Endpoint endpoint = new JmxRepairProperties.Endpoint();
    private RepairExecutionService worker;

    private static Task running(boolean delayed) {
        return new Task("op", 0, "n1", URL, HOST, "RUNNING", 7, 123L, SUBMITTED, delayed, "claim", "default", "team");
    }

    private static Task pending() {
        return new Task("op", 0, "n1", URL, HOST, "PENDING", null, null, null, false, "claim", "default", "team");
    }

    private void runningFor(long seconds) {
        when(clock.now()).thenReturn(SUBMITTED.plusSeconds(seconds));
    }

    @BeforeEach
    void setUp() throws Exception {
        endpoint.setHostId(HOST);
        endpoint.setServiceUrl(URL);
        // Identity configuration is stubbed; transport and persistence have their own tests.
        var settings = spy(properties);
        doReturn(endpoint).when(settings).require("default", "n1");
        when(client.connect(endpoint)).thenReturn(remote);
        when(remote.identity()).thenReturn(new Identity(HOST, 123));
        runningFor(60);
        worker = new RepairExecutionService(mock(RepairClaimService.class), progress,
            settings, client, topology, clock);
    }

    @AfterEach
    void tearDown() {
        worker.close();
    }

    @Test
    void missingStatusDoesNotResubmitOrAdvance() throws Exception {
        when(remote.status(7)).thenReturn(new Observation(Status.MISSING, "absent"));
        var task = running(false);
        worker.step(task);
        verify(progress).defer(task, "absent", properties.getPollSeconds());
        verify(remote, never()).submit(anyString());
        verify(progress, never()).complete(any(), anyString());
    }

    @Test
    void cassandraRestartDoesNotReuseCommandNumber() throws Exception {
        when(remote.identity()).thenReturn(new Identity(HOST, 456));
        worker.step(running(false));
        verify(progress).unknown(any(), contains("process changed"));
        verify(remote, never()).status(anyInt());
        verify(remote, never()).submit(anyString());
    }

    @Test
    void expiredSubmissionIsNeverResubmitted() {
        var task = new Task("op", 0, "n1", URL, HOST, "SUBMITTING", null, 123L, SUBMITTED, false, "claim",
            "default", "team");
        worker.step(task);
        verify(progress).unknown(any(), contains("disappeared"));
        verifyNoInteractions(client);
    }

    @Test
    void incompatibleServerStatusImmediatelyBlocksTheRun() throws Exception {
        when(remote.status(7)).thenReturn(new Observation(Status.UNRECOGNIZED, "[NEW_SERVER_STATE]"));
        worker.step(running(false));
        verify(progress).unknown(any(), contains("Unsupported Cassandra repair status"));
        verify(progress, never()).defer(any(), anyString(), anyInt());
        verify(remote, never()).submit(anyString());
    }

    @Test
    void nativeFailureBlocksTheRunInsteadOfRetrying() throws Exception {
        when(remote.status(7)).thenReturn(new Observation(Status.FAILED, "[FAILED]"));
        worker.step(running(false));
        verify(progress).unknown(any(), contains("Cassandra reported FAILED"));
        verify(progress, never()).complete(any(), anyString());
        verify(progress, never()).defer(any(), anyString(), anyInt());
    }

    // Connectivity failures before any submission intent are safe to retry.
    @Test
    void connectionFailureBeforeSubmissionDefersWithoutRecordingIntent() throws Exception {
        when(client.connect(endpoint)).thenThrow(new RuntimeException("JMX unreachable"));
        worker.step(pending());
        verify(progress).defer(any(), contains("no new repair submitted"), anyInt());
        verify(progress, never()).submitting(any(), anyLong());
        verify(progress, never()).unknown(any(), anyString());
    }

    @Test
    void ambiguousSubmitTimeoutIsNeverDeferredOrResubmitted() throws Exception {
        var task = pending();
        when(progress.submitting(task, 123)).thenReturn(true);
        when(remote.submit("team")).thenThrow(new SocketTimeoutException("response lost"));
        worker.step(task);
        verify(topology).requireReadyPlan("default", "team");
        verify(progress).unknown(eq(task), contains("submission outcome uncertain"));
        verify(progress, never()).submitted(any(), anyInt(), anyInt());
        verify(progress, never()).defer(any(), anyString(), anyInt());
        verify(remote, times(1)).submit("team");
    }

    @Test
    void shutdownDuringSubmitRetainsUncertainty() throws Exception {
        var task = pending();
        when(progress.submitting(task, 123)).thenReturn(true);
        when(remote.submit("team")).thenAnswer(call -> {
            worker.close();
            throw new InterruptedException("shutdown while response was in flight");
        });
        worker.step(task);
        verify(progress).unknown(eq(task), contains("submission outcome uncertain"));
        verify(progress, never()).submitted(any(), anyInt(), anyInt());
        verify(progress, never()).defer(any(), anyString(), anyInt());
    }

    @Test
    void unreadyTopologyDefersBeforeAnySubmission() throws Exception {
        doThrow(new IllegalStateException("node down")).when(topology).requireReadyPlan("default", "team");
        worker.step(pending());
        verify(progress, never()).submitting(any(), anyLong());
        verify(remote, never()).submit(anyString());
        verify(progress).defer(any(), contains("no new repair submitted"), anyInt());
    }

    @Test
    void longRunningNodeIsFlaggedDelayedOnceAndPollingContinues() throws Exception {
        runningFor(properties.getDelayedAfterSeconds() + 1);
        when(remote.status(7)).thenReturn(new Observation(Status.IN_PROGRESS, "[IN_PROGRESS]"));
        var task = running(false);
        worker.step(task);
        verify(progress).markDelayed(eq(task), contains("delayed threshold"));
        verify(progress).defer(task, "[IN_PROGRESS]", properties.getPollSeconds());
        verify(progress, never()).unknown(any(), anyString());

        var alreadyFlagged = running(true);
        worker.step(alreadyFlagged);
        verify(progress, never()).markDelayed(eq(alreadyFlagged), anyString());
        verify(progress).defer(alreadyFlagged, "[IN_PROGRESS]", properties.getPollSeconds());
    }

    @Test
    void observationDeadlineIsPerNodeAndEndsOnlyThatRunWithoutResubmitting() {
        runningFor(properties.getObservationDeadlineSeconds() + 1);
        worker.step(running(true));
        verify(progress).unknown(any(), contains("observation deadline"));
        verifyNoInteractions(client);
    }

    @Test
    void pendingNodeHasNoObservationClock() throws Exception {
        // However long the run has existed, a node that has not been submitted cannot time out.
        runningFor(properties.getObservationDeadlineSeconds() * 10L);
        var task = pending();
        when(progress.submitting(task, 123)).thenReturn(true);
        when(remote.submit("team")).thenReturn(42);
        worker.step(task);
        verify(progress).submitted(task, 42, properties.getPollSeconds());
        verify(progress, never()).unknown(any(), anyString());
    }

    @Test
    void observationFailuresBackOffUntilTheNodeDeadline() throws Exception {
        when(remote.status(7)).thenThrow(new IOException("temporary disconnect"));
        when(clock.now()).thenReturn(SUBMITTED.plusSeconds(1), SUBMITTED.plusSeconds(30),
            SUBMITTED.plusSeconds(properties.getObservationDeadlineSeconds() + 1));
        var task = running(true);
        worker.step(task);
        worker.step(task);
        worker.step(task);
        verify(progress, times(2)).defer(eq(task), anyString(), anyInt());
        verify(progress).unknown(eq(task), contains("deadline"));
        verify(remote, times(2)).status(7);
        verify(remote, never()).submit(anyString());
    }
}
