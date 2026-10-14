package demo.platform.repair.persistence;

import org.hibernate.annotations.BatchSize;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import jakarta.persistence.*;

import java.util.ArrayList;
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

    @ElementCollection(fetch = FetchType.LAZY)
    @BatchSize(size = 32)
    @CollectionTable(name = "repair_policy_nodes", joinColumns = @JoinColumn(name = "cluster_id"))
    @OrderColumn(name = "ordinal")
    @Column(name = "node_name", nullable = false)
    private List<String> nodes = new ArrayList<>();

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
        this.nodes.clear();
        this.nodes.addAll(nodes);
        this.command = command;
        this.leaseMinutes = leaseMinutes;
    }

    public List<String> nodes() {
        return List.copyOf(nodes);
    }
}
