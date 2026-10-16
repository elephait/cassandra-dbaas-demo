package demo.platform.cluster.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import demo.platform.cluster.ClusterRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/** Driver-observed cluster topology. The only place outside the session holder that reads driver metadata. */
@Repository
@RequiredArgsConstructor
public class ClusterMetadataRepository {
    private final CassandraSessions sessions;

    private static List<CassandraNode> nodes(CqlSession session) {
        return session.getMetadata().getNodes().values().stream()
            .map(node -> new CassandraNode(String.valueOf(node.getHostId()), node.getEndPoint().toString(),
                node.getState().name(), String.valueOf(node.getDatacenter())))
            .toList();
    }

    /** Empty while this replica is still connecting to the cluster. */
    public Optional<List<CassandraNode>> nodes(ClusterRecord.Settings cluster) {
        return sessions.current(cluster).map(ClusterMetadataRepository::nodes);
    }

    public List<CassandraNode> requireNodes(ClusterRecord.Settings cluster) {
        return nodes(sessions.require(cluster));
    }

    public record CassandraNode(String hostId, String address, String state, String datacenter) {
        public boolean up() {
            return NodeState.UP.name().equals(state);
        }
    }
}
