package demo.platform.cluster.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.*;
import lombok.experimental.Accessors;

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
