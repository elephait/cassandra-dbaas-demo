# JMX review verification

Verified October 15, 2026 from `C:\dev\cassandra-dbaas-demo` using `./verify-jmx.ps1`.

| Suite | Tests | Failures | Errors | Skipped |
|---|---:|---:|---:|---:|
| Surefire unit/persistence | 72 | 0 | 0 | 0 |
| Failsafe: real Cassandra 5.0.5 JMX | 1 | 0 | 0 | 0 |
| Failsafe: PostgreSQL 17.6 migration | 1 | 0 | 0 | 0 |

The build completed successfully. The fixture's two Cassandra containers were
stopped afterward. Run `./verify-jmx.ps1` to reproduce; cached dependencies and
Docker are prerequisites (omit Maven offline mode on a first uncached build).

## Evidence

* [Live repair report](demo.platform.repair.jmx.JmxCassandraIT.txt)
* [PostgreSQL migration report](demo.platform.RepairPolicyMigrationIT.txt)
* [Actual Cassandra response observations](live-status-observations.txt)
* [Pinned binary API and repair-mode evidence](cassandra-5.0.5-api.txt)

The actual server returned IN_PROGRESS and COMPLETED for command 1 on each of
two distinct coordinator host IDs. Submission connections were closed before
polling reconnected. This verifies the asynchronous native API and completion
mapping, not full application-worker failover or repair-data completeness.

The pinned server jar's enum additionally verifies FAILED exists. No live FAILED
scenario was injected in this run. The stub MBean unit test verifies plumbing,
not the server's semantics. Future server versions need new compatibility evidence.

The migration integration test upgraded a version-2 PostgreSQL database, preserved
node order in node_names, confirmed repair_policy_nodes was absent, and reran
Flyway without further migrations. The unit suite also covers empty node lists.

Original XML/text reports remain in target/surefire-reports and
target/failsafe-reports. Only plain result reports and selected server observations
are retained here; XML JVM/environment properties are intentionally not copied.
