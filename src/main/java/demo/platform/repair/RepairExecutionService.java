package demo.platform.repair;

import demo.platform.common.DatabaseClock;
import demo.platform.repair.jmx.JmxRepairProperties;
import demo.platform.repair.jmx.RepairManagementClient;
import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairExecutionRepository.Task;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Executes one short unit of repair work at a time: initialize a claimed run, submit one node, or poll
 * one node. Cassandra runs the repair itself; this class only submits and observes, so a worker thread is
 * held for one JMX round trip, never for the duration of a repair.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RepairExecutionService {
    private final RepairClaimService claims;
    private final RepairExecutionRepository execution;
    private final JmxRepairProperties settings;
    private final RepairManagementClient client;
    private final RepairTopology topology;
    private final DatabaseClock clock;
    private volatile boolean closing;

    private static String describe(Exception e) {
        return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }

    public void reconcileExpired() {
        execution.reconcileExpired();
    }

    /**
     * Claims the next unit of work and returns it for the worker pool, or null when there is none.
     * The caller's polling thread performs only database claims; Cassandra and JMX calls run in the action.
     */
    public Runnable nextAction() {
        if (closing) return null;
        Task task = execution.claimTask(settings.getActionLeaseSeconds());
        if (task != null) return () -> step(task);
        OperationRecord operation = claims.claim();
        return operation == null ? null : () -> initialize(operation);
    }

    /** CLAIMED -> RUNNING with a persisted node plan, or back to QUEUED with backoff. Nothing is dispatched. */
    void initialize(OperationRecord operation) {
        try {
            // The plan is snapshotted here; every later step is checked against this snapshot.
            execution.initialize(operation, topology.requireReadyPlan(operation.clusterId(), operation.keyspace()));
        } catch (Exception notReady) {
            try {
                claims.deferUndispatched(operation, "Repair plan not started: " + describe(notReady));
            } catch (Exception lost) {
                // The claim lease expires and reconciliation requeues it.
                log.warn("Could not defer repair {}; its claim lease will expire", operation.id(), lost);
            }
        }
    }

    void step(Task task) {
        boolean dispatchIntent = false;
        try {
            if ("SUBMITTING".equals(task.state())) {
                execution.unknown(task, "Worker disappeared during JMX submission; do not resubmit without remote verification");
                return;
            }
            // Checked before connecting, so an unreachable node still reaches its deadline.
            if (task.submittedAt() != null && !withinDeadline(task)) return;
            var endpoint = settings.require(task.clusterId(), task.node());
            if (!endpoint.getServiceUrl().equals(task.serviceUrl()) || !endpoint.getHostId().equals(task.hostId())) {
                execution.unknown(task, "JMX endpoint identity configuration changed; inspect the original execution");
                return;
            }
            // Connect and identity checks happen BEFORE recording submission intent: failures here are safe
            // to retry. Exceptions from submit itself are ambiguous.
            try (var remote = client.connect(endpoint)) {
                var identity = remote.identity();
                if (!task.hostId().equals(identity.hostId())) {
                    execution.unknown(task, "JMX endpoint host ID does not match the accepted plan");
                    return;
                }
                if ("PENDING".equals(task.state())) {
                    topology.requireReadyPlan(task.clusterId(), task.keyspace());
                    if (closing || !execution.submitting(task, identity.processStartedAt())) return;
                    dispatchIntent = true;
                    int command = remote.submit(task.keyspace());
                    if (command < 0) throw new IllegalStateException("Invalid repair command number");
                    if (!identity.equals(remote.identity()))
                        throw new IllegalStateException("Cassandra restarted during submission");
                    execution.submitted(task, command, settings.getPollSeconds());
                } else {
                    observe(task, remote, identity);
                }
            }
        } catch (Exception e) {
            try {
                // Receipts carry no exception text: it can contain service URLs. Details stay in logs.
                if (dispatchIntent)
                    execution.unknown(task, "JMX submission outcome uncertain; verify remote activity before acknowledgement");
                else
                    execution.defer(task, "JMX observation/readiness unavailable; no new repair submitted",
                        settings.getPollSeconds());
            } catch (Exception lost) {
                log.warn("Could not record JMX action outcome for {}/{}", task.operationId(), task.ordinal(), lost);
            }
            log.warn("JMX repair action failed for {}/{}", task.operationId(), task.ordinal(), e);
        }
    }

    private void observe(Task task, RepairManagementClient.Session remote, RepairManagementClient.Identity identity)
        throws Exception {
        if (task.processStartedAt() == null || task.commandId() == null
            || task.processStartedAt() != identity.processStartedAt()) {
            execution.unknown(task, "Cassandra process changed; command number cannot identify the old repair");
            return;
        }
        var observation = remote.status(task.commandId());
        if (!identity.equals(remote.identity())) {
            execution.unknown(task, "Cassandra restarted while observing repair");
            return;
        }
        switch (observation.status()) {
            case COMPLETED -> execution.complete(task, observation.detail());
            case FAILED -> execution.unknown(task,
                "Cassandra reported FAILED; verify remote cleanup before retry: " + observation.detail());
            case UNRECOGNIZED -> execution.unknown(task,
                "Unsupported Cassandra repair status; check version/API compatibility: " + observation.detail());
            case IN_PROGRESS, MISSING -> execution.defer(task, observation.detail(), settings.getPollSeconds());
        }
    }

    /**
     * Per-node clocks, measured from this node's own submission (not from the start of the run), so a
     * long serial plan never times out a node that has just started. Past the soft threshold the node is
     * flagged delayed and polling continues; past the hard deadline the outcome becomes UNKNOWN.
     */
    private boolean withinDeadline(Task task) {
        long runningSeconds = Duration.between(task.submittedAt(), clock.now()).toSeconds();
        if (runningSeconds > settings.getObservationDeadlineSeconds()) {
            execution.unknown(task, "Node repair exceeded the observation deadline of "
                + settings.getObservationDeadlineSeconds() + "s; it may still be running");
            return false;
        }
        if (!task.delayed() && runningSeconds > settings.getDelayedAfterSeconds())
            execution.markDelayed(task, "Node repair running for " + runningSeconds + "s, past the delayed "
                + "threshold of " + settings.getDelayedAfterSeconds() + "s; still polling");
        return true;
    }

    @PreDestroy
    public void close() {
        closing = true;
    }
}
