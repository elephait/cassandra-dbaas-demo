package demo.platform.repair;

import demo.platform.repair.persistence.RepairExecutionRepository;
import demo.platform.repair.persistence.RepairExecutionRepository.Target;

import java.util.List;
import java.util.UUID;

/**
 * Drives repairs through the same persisted path production uses (claim, plan, submit, observe), without
 * Cassandra: the JMX round trips are replaced by their recorded outcomes.
 */
public final class RepairRuns {
    private RepairRuns() {
    }

    public static List<Target> plan() {
        return List.of(new Target("n1", "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi",
            UUID.randomUUID().toString()));
    }

    /** Claims the next queued request and persists its node plan; null when the cluster is reserved. */
    public static OperationRecord start(RepairClaimService claims, RepairExecutionRepository execution) {
        OperationRecord claimed = claims.claim();
        if (claimed != null) execution.initialize(claimed, plan());
        return claimed;
    }

    /** Submits the next node; Cassandra reports no ranges to repair, which completes the node. */
    public static void succeed(RepairExecutionRepository execution) {
        var task = execution.claimTask(60);
        if (!execution.submitting(task, 1)) throw new IllegalStateException("Could not record submission");
        execution.submitted(task, 0, 1);
    }

    /** Records an uncertain outcome for the next node action. */
    public static void becomeUnknown(RepairExecutionRepository execution, String detail) {
        execution.unknown(execution.claimTask(60), detail);
    }
}
