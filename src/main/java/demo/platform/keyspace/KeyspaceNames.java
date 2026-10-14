package demo.platform.keyspace;

public final class KeyspaceNames {
    private KeyspaceNames() {
    }

    public static String validate(String keyspace) {
        if (keyspace == null || !keyspace.matches("[a-z][a-z0-9_]{0,47}")
            || keyspace.equals("system") || keyspace.startsWith("system_"))
            throw new IllegalArgumentException("Keyspace must match [a-z][a-z0-9_]{0,47} and cannot be a system keyspace");
        return keyspace;
    }
}
