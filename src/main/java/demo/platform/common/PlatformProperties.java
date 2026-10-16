package demo.platform.common;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Deployment configuration under {@code demo.*}: seeds the bootstrap "default" cluster and sizes the local
 * repair worker. Repair targets are under {@code demo.repair.jmx}. Registered clusters themselves live in the control-plane database.
 */
@Data
@ConfigurationProperties(prefix = "demo")
public class PlatformProperties {
    private String contactPoints;
    private String datacenter;
    private int maxKeyspaces;
    private String adminToken;
    private int provisioningLeaseSeconds = 60;
    private Cluster cluster = new Cluster();
    private Cassandra cassandra = new Cassandra();
    private AddressTranslation addressTranslation = new AddressTranslation();
    private Repair repair = new Repair();

    @Data
    public static class Cluster {
        private int expectedNodes = 3;
    }

    @Data
    public static class Cassandra {
        private int replicationFactor = 3;
        private int requestTimeoutSeconds = 10;
        private int reconnectSeconds = 60;
    }

    @Data
    public static class AddressTranslation {
        private boolean enabled;
        private String mappings;
    }

    @Data
    public static class Repair {
        private int workerThreads = 8;
        // false lets an instance serve API traffic only; coordination is in the database, so worker
        // instances can be added or removed independently.
        private boolean workerEnabled = true;
    }
}
