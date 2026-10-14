package demo.platform.keyspace.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Embeddable
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = lombok.AccessLevel.PROTECTED)
public class KeyspaceId implements Serializable {
    @Column(name = "cluster_id", length = 64)
    private String clusterId;
    @Column(name = "keyspace_name", length = 48)
    private String keyspace;

    public KeyspaceId(String clusterId, String keyspace) {
        this.clusterId = clusterId;
        this.keyspace = keyspace;
    }
}
