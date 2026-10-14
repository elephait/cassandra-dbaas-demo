package demo.platform.keyspace;

import java.time.Instant;

public record KeyspaceRecord(String clusterId, String keyspace, ProvisioningState state, Instant updated) {

    public record ProvisionDecision(boolean alreadyReady, String owner) {
    }
}
