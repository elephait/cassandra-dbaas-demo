package demo.platform.repair.jmx;

public interface RepairManagementClient {
    Session connect(JmxRepairProperties.Endpoint endpoint) throws Exception;

    enum Status {IN_PROGRESS, COMPLETED, FAILED, MISSING, UNRECOGNIZED}

    interface Session extends AutoCloseable {
        Identity identity() throws Exception;

        int submit(String keyspace) throws Exception;

        Observation status(int commandId) throws Exception;

        @Override
        void close() throws Exception;
    }

    record Identity(String hostId, long processStartedAt) {
    }

    record Observation(Status status, String detail) {
    }
}
