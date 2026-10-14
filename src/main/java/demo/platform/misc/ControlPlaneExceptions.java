package demo.platform.misc;

public final class ControlPlaneExceptions {
    private ControlPlaneExceptions() {
    }

    public static class Conflict extends RuntimeException {
        public Conflict(String message) {
            super(message);
        }
    }

    public static class KeyspaceQuotaExceeded extends RuntimeException {
    }

    public static class UnknownCluster extends RuntimeException {
        public UnknownCluster(String clusterId) {
            super("Unknown or disabled cluster: " + clusterId);
        }
    }
}
