package demo.platform.repair;

import demo.platform.cluster.ClusterRecord;
import demo.platform.cluster.ClusterRegistryService;
import demo.platform.cluster.cassandra.ClusterMetadataRepository;
import demo.platform.cluster.cassandra.ClusterMetadataRepository.CassandraNode;
import demo.platform.common.ControlPlaneExceptions;
import demo.platform.keyspace.SchemaValidator;
import demo.platform.repair.jmx.JmxRepairProperties;
import demo.platform.repair.persistence.RepairExecutionRepository.Target;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * One readiness rule shared by admission, plan initialization and each node submission: the configured JMX
 * targets must be exactly the UP nodes of the configured datacenter, and the keyspace schema must match what
 * provisioning created. A repair never starts against a degraded or drifted cluster.
 */
@Component
@RequiredArgsConstructor
public class RepairTopology {
    private final ClusterRegistryService clusters;
    private final ClusterMetadataRepository metadata;
    private final SchemaValidator schema;
    private final JmxRepairProperties jmx;

    /** Returns the configured JMX targets in node-name order, or throws if the cluster is not ready. */
    public List<Target> requireReadyPlan(String clusterId, String keyspace) {
        ClusterRecord.Settings cluster = clusters.requireEnabled(clusterId);
        List<Target> targets = jmx.requireTargets(clusterId).entrySet().stream()
            .map(entry -> new Target(entry.getKey(), entry.getValue().getServiceUrl(), entry.getValue().getHostId()))
            .toList();

        List<CassandraNode> local = metadata.requireNodes(cluster).stream()
            .filter(node -> cluster.datacenter().equals(node.datacenter())).toList();
        Set<String> up = local.stream().filter(CassandraNode::up).map(CassandraNode::hostId).collect(Collectors.toSet());
        if (local.size() != cluster.expectedNodes() || up.size() != cluster.expectedNodes())
            throw new ControlPlaneExceptions.Conflict("All expected nodes must be UP before repair");
        Set<String> planned = targets.stream().map(Target::hostId).collect(Collectors.toCollection(HashSet::new));
        if (planned.size() != targets.size() || !planned.equals(up))
            throw new ControlPlaneExceptions.Conflict("JMX targets must cover exactly the configured datacenter's nodes");
        schema.validate(cluster, keyspace);
        return targets;
    }
}
