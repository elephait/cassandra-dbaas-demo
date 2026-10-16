package demo.platform.common;

/**
 * Domain failures raised by services. The web layer maps each type to one HTTP status, so services
 * never depend on HTTP types. Invalid input is reported with {@link IllegalArgumentException}.
 */
public final class ControlPlaneExceptions {
    private ControlPlaneExceptions() {
    }

    /** The request conflicts with current state (409). */
    public static class Conflict extends RuntimeException {
        public Conflict(String message) {
            super(message);
        }
    }

    /** The cluster's keyspace quota is already reserved (409). */
    public static class KeyspaceQuotaExceeded extends RuntimeException {
        public KeyspaceQuotaExceeded() {
            super("Keyspace quota reached");
        }
    }

    /** The cluster is not registered or is disabled (404). */
    public static class UnknownCluster extends RuntimeException {
        public UnknownCluster(String clusterId) {
            super("Unknown or disabled cluster: " + clusterId);
        }
    }

    /** A referenced resource does not exist (404). */
    public static class NotFound extends RuntimeException {
        public NotFound(String message) {
            super(message);
        }
    }

    /** A dependency is temporarily unavailable; retrying later is safe (503). */
    public static class Unavailable extends RuntimeException {
        public Unavailable(String message) {
            super(message);
        }
    }

    /** The caller is not authorized (403). */
    public static class Forbidden extends RuntimeException {
        public Forbidden(String message) {
            super(message);
        }
    }
}
