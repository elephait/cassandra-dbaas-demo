package demo.platform.repair;

import java.util.List;

public enum RepairState {
    QUEUED, CLAIMED, RUNNING, UNKNOWN, SUCCEEDED, ACKNOWLEDGED;

    /** States that hold, or wait for, the cluster's single repair reservation. */
    public static final List<RepairState> ACTIVE = List.of(QUEUED, CLAIMED, RUNNING, UNKNOWN);
}
