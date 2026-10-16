package demo.platform.repair.persistence;

import demo.platform.repair.RepairState;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "operations")
// Inserted through JPA; later state transitions are fenced SQL in RepairExecutionRepository.
@NoArgsConstructor(access = AccessLevel.PROTECTED)
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
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;
    @Column(name = "retry_count", nullable = false)
    private int retryCount;
    @Column(name = "delayed", nullable = false)
    private boolean delayed;

    public OperationEntity(UUID id, String clusterId, String keyspace, String requestKey, Instant now) {
        this.id = id;
        this.clusterId = clusterId;
        this.keyspace = keyspace;
        this.requestKey = requestKey;
        this.state = RepairState.QUEUED;
        this.detail = "Approved repair queued";
        this.updatedAt = now;
        this.nextAttemptAt = now;
    }
}
