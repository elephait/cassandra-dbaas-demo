package demo.platform.repair.persistence;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import jakarta.persistence.*;

import java.util.List;

@Entity
@Table(name = "repair_policies")
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class RepairPolicyEntity {
    @Id
    @Column(name = "cluster_id", length = 64)
    private String clusterId;

    // RepairPolicy validates node names to exclude commas. This ordered configuration
    // value belongs on the policy; per-operation execution has its own progress table.
    @Column(name = "node_names", nullable = false, columnDefinition = "TEXT")
    private String nodeNames = "";

    @Column(name = "command", nullable = false, length = 4000)
    private String command;

    @Column(name = "lease_minutes", nullable = false)
    private int leaseMinutes;

    @Version
    @Column(name = "policy_version", nullable = false)
    private long version;

    public RepairPolicyEntity(String clusterId) {
        this.clusterId = clusterId;
    }

    public void update(List<String> nodes, String command, int leaseMinutes) {
        this.nodeNames = String.join(",", nodes);
        this.command = command;
        this.leaseMinutes = leaseMinutes;
    }

    public List<String> nodes() {
        return nodeNames.isEmpty() ? List.of() : List.of(nodeNames.split(",", -1));
    }
}
