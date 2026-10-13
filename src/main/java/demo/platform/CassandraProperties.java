package demo.platform;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "demo")
public class CassandraProperties {
    private String contactPoints;
    private String datacenter;
    private int maxTenants;
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
        private String nodes;
        private String command;
        private int leaseMinutes = 60;
    }
}