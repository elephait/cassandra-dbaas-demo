package demo.platform.repair;

import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.persistence.RepairExecutionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class RepairClaimService {
    private final RepairExecutionRepository execution;
    private final RepairConfigurationService repairConfiguration;

    @Transactional
    public OperationRecord claim() {
        return claimNext(null);
    }

    @Transactional
    public OperationRecord claim(int leaseSeconds) {
        if (leaseSeconds < 3) throw new IllegalArgumentException("Repair lease must be at least three seconds");
        return claimNext(leaseSeconds);
    }

    public boolean deferUndispatched(OperationRecord operation, String reason) {
        int attempts = execution.retryCount(operation.id());
        int delaySeconds = Math.min(300, 5 << Math.min(attempts, 6));
        return execution.requeueUndispatched(operation, reason, delaySeconds);
    }

    private OperationRecord claimNext(Integer requestedLeaseSeconds) {
        OperationRecord operation = execution.selectNextQueued();
        if (operation == null) return null;
        RepairPolicy policy = repairConfiguration.getPolicy(operation.clusterId());
        if (policy == null) {
            execution.rejectQueued(operation, "Repair policy is missing for this cluster");
            return null;
        }
        if (policy.version() != operation.repairPolicyVersion()) {
            execution.rejectQueued(operation, "Repair policy version " + policy.version()
                + " replaced the accepted version " + operation.repairPolicyVersion()
                + "; submit a new repair request");
            return null;
        }
        int leaseSeconds = requestedLeaseSeconds == null
            ? Math.toIntExact(policy.leaseMinutes() * 60L) : requestedLeaseSeconds;
        return execution.claim(operation, leaseSeconds);
    }
}
