package demo.platform.repair.jmx;

public interface RepairManagementClient {
    record Identity(String hostId, long processStartedAt) {}
    enum Status { IN_PROGRESS, COMPLETED, FAILED, MISSING, UNRECOGNIZED }
    record Observation(Status status, String detail) {}

    interface Session extends AutoCloseable {
        Identity identity() throws Exception;
        int submit(String keyspace) throws Exception;
        Observation status(int commandId) throws Exception;
        @Override void close() throws Exception;
    }

    Session connect(JmxRepairProperties.Endpoint endpoint) throws Exception;
}
