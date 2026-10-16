package demo.platform.cluster;

/**
 * Implemented by domains that act on a cluster (keyspace provisioning, repair). Cluster configuration may
 * not change while any of them has work in flight. The interface keeps the dependency one-way: those domains
 * depend on the cluster domain, never the reverse.
 */
public interface PendingClusterWork {
    boolean hasPendingWork(String clusterId);
}
