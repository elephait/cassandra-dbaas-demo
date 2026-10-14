package demo.platform.repair.persistence;

import jakarta.persistence.*;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "repair_locks")
@NoArgsConstructor(access = lombok.AccessLevel.PROTECTED)
public class RepairLockEntity {
    @Id
    @Column(name = "cluster_id", length = 64)
    private String clusterId;
    @Column(name = "owner_id", length = 36)
    private String owner;
    @Column(name = "expires_at")
    @JdbcTypeCode(SqlTypes.TIMESTAMP)
    private Instant expiresAt;

    public RepairLockEntity(String clusterId) {
        this.clusterId = clusterId;
    }
}
