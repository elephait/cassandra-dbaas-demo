package demo.platform.cluster.persistence;

import lombok.AllArgsConstructor;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
@EqualsAndHashCode
@AllArgsConstructor
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AddressMappingEmbeddable {
    @Column(name = "discovered_host", nullable = false)
    private String discoveredHost;

    @Column(name = "target_host", nullable = false)
    private String targetHost;

    @Column(name = "target_port", nullable = false)
    private int targetPort;
}
