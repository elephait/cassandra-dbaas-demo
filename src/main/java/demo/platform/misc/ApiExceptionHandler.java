package demo.platform.misc;

import org.springframework.dao.DataAccessResourceFailureException;

import com.datastax.oss.driver.api.core.DriverException;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> invalid(IllegalArgumentException exception) {
        return Map.of("error", exception.getMessage() == null ? "Invalid request" : exception.getMessage());
    }

    @ExceptionHandler(DataAccessResourceFailureException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> sqlUnavailable() {
        return Map.of("error", "Control-plane database unavailable; retry safely using the same request key");
    }

    @ExceptionHandler(DriverException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> unavailable() {
        return Map.of("error", "Cassandra request failed; check cluster health and keyspace provisioning");
    }

    @ExceptionHandler(ControlPlaneExceptions.KeyspaceQuotaExceeded.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> quotaExceeded() {
        return Map.of("error", "Keyspace quota reached");
    }

    @ExceptionHandler(ControlPlaneExceptions.Conflict.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> conflict(ControlPlaneExceptions.Conflict exception) {
        return Map.of("error", exception.getMessage());
    }

    @ExceptionHandler(ControlPlaneExceptions.UnknownCluster.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> unknownCluster(ControlPlaneExceptions.UnknownCluster exception) {
        return Map.of("error", exception.getMessage());
    }

    @Data
    @ConfigurationProperties(prefix = "demo")
    public static class CassandraProperties {
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
            private int connectionThreads = 8;
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
            private int workerThreads = 8;
            // Off lets an instance serve API traffic only, so repair execution can run as its own
            // auto-scaled tier. Coordination stays in SQL, so adding or losing workers is safe.
            private boolean workerEnabled = true;
        }
    }
}
