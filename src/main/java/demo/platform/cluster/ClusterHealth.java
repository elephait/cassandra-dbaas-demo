package demo.platform.cluster;

import java.util.List;

/**
 * Driver-observed availability. A successful LOCAL_QUORUM probe, not this view, proves request availability.
 */
public record ClusterHealth(String clusterId, List<NodeView> nodes, long up, int expected,
                            boolean quorumAvailable, String status, String note) {

    public record NodeView(String address, String state, String datacenter) {
    }
}
