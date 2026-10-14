package demo.platform.repair.persistence;

import jakarta.persistence.*;
import demo.platform.repair.RepairState;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "operations")
@NoArgsConstructor(access = lombok.AccessLevel.PROTECTED)
public class OperationEntity {
    @Id
    @Column(name = "operation_id", length = 36)
    @JdbcTypeCode(SqlTypes.VARCHAR)
    private UUID id;
    @Column(name = "cluster_id", nullable = false, length = 64)
    private String clusterId;
    @Column(name = "keyspace_name", nullable = false, length = 48)
    private String keyspace;
    @Column(name = "owner_id", length = 36)
    private String owner;
    @Column(nullable = false, length = 32)
    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.VARCHAR)
    private RepairState state;
    @Column(length = 4000)
    private String detail;
    @Column(name = "request_key", nullable = false, length = 128)
    private String requestKey;
    @Column(name = "repair_policy_version", nullable = false)
    private long repairPolicyVersion;
    @Column(name = "updated_at", nullable = false)
    @JdbcTypeCode(SqlTypes.TIMESTAMP)
    private Instant updatedAt;
    @Column(name = "next_attempt_at", nullable = false)
    @JdbcTypeCode(SqlTypes.TIMESTAMP)
    private Instant nextAttemptAt;
    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    public OperationEntity(UUID id, String clusterId, String keyspace, String requestKey, long policyVersion, Instant now) {
        this.id = id;
        this.clusterId = clusterId;
        this.keyspace = keyspace;
        this.requestKey = requestKey;
        this.repairPolicyVersion = policyVersion;
        this.state = RepairState.QUEUED;
        this.detail = "Approved repair queued";
        this.updatedAt = now;
        this.nextAttemptAt = now;
    }
}
