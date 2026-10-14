package demo.platform.repair;

public enum RepairState {
    QUEUED, CLAIMED, RUNNING, UNKNOWN, SUCCEEDED, FAILED, ACKNOWLEDGED;

    public boolean confirmedTerminal() {
        return this == SUCCEEDED || this == FAILED;
    }
}
