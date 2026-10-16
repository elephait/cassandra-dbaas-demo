package demo.platform.repair;

import demo.platform.repair.jmx.JmxRepairProperties;
import demo.platform.repair.persistence.RepairExecutionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Moves one queued request to CLAIMED, or returns it to the queue with backoff. */
@Service
@RequiredArgsConstructor
public class RepairClaimService {
    private static final int MAX_BACKOFF_SECONDS = 300;

    private final RepairExecutionRepository execution;
    private final JmxRepairProperties settings;

    /**
     * Selection and claim share one transaction, so the SKIP LOCKED rows stay locked until the claim commits.
     * The lease is short: it only has to survive plan initialization, after which the reservation is durable.
     */
    @Transactional
    public OperationRecord claim() {
        OperationRecord operation = execution.selectNextQueued();
        return operation == null ? null : execution.claim(operation, settings.getActionLeaseSeconds());
    }

    /** Nothing was dispatched, so requeueing is safe; exponential backoff is persisted (5s .. 300s). */
    public boolean deferUndispatched(OperationRecord operation, String reason) {
        int attempts = execution.retryCount(operation.id());
        int delaySeconds = Math.min(MAX_BACKOFF_SECONDS, 5 << Math.min(attempts, 6));
        return execution.requeueUndispatched(operation, reason, delaySeconds);
    }
}
