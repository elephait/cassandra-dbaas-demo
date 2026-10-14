package demo.platform.keyspace.persistence;

import jakarta.persistence.*;
import demo.platform.keyspace.ProvisioningState;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "keyspaces")
@Getter
@NoArgsConstructor(access = lombok.AccessLevel.PROTECTED)
public class KeyspaceEntity {
    @EmbeddedId
    private KeyspaceId id;
    @Column(nullable = false, length = 32)
    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.VARCHAR)
    private ProvisioningState state;
    @Column(name = "owner_id", length = 36)
    private String owner;
    @Column(name = "expires_at")
    @JdbcTypeCode(SqlTypes.TIMESTAMP)
    private Instant expiresAt;
    @Column(name = "updated_at", nullable = false)
    @JdbcTypeCode(SqlTypes.TIMESTAMP)
    private Instant updatedAt;

    public KeyspaceEntity(KeyspaceId id) {
        this.id = id;
        this.state = ProvisioningState.PROVISIONING;
    }

    public void claim(String owner, Instant now, int leaseSeconds) {
        this.owner = owner;
        this.expiresAt = now.plusSeconds(leaseSeconds);
        this.updatedAt = now;
    }
}
