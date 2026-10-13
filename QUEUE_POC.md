# Durable repair queue POC

Verified locally: 25 tests passed with the optional PostgreSQL test enabled. A live smoke check ran two separate
256 MiB Java processes against shared PostgreSQL and the existing three-node Cassandra cluster; cross-instance
submission returned the same operation ID and real repair completed successfully. Test tenant: queue_smoke.

Repair submission persists QUEUED in SQL and returns 202. Every instance polls every two seconds, claiming work in a short transaction with FOR UPDATE SKIP LOCKED. SQL ownership is committed before remote work. There is one bounded active worker per JVM and one shared cluster repair lock. Queue depth is capped at 1000 waiting repairs; request keys remain idempotent.

States: QUEUED -> CLAIMED -> RUNNING -> SUCCEEDED. CLAIMED means the worker has not crossed the persisted dispatch boundary. If it expires, the operation is returned to QUEUED with a new owner on the next claim. RUNNING is persisted before external invocation, so its remote outcome might be uncertain even if the process crashed just before actual invocation. Expired RUNNING becomes UNKNOWN and retains the lock. UNKNOWN requires operator inspection and acknowledgement; acknowledgement is idempotent and records ACKNOWLEDGED. It cannot cancel remote work.

Workers renew leases against database time. Failed renewal or lost ownership stops subsequent dispatch, but cannot cancel remote Cassandra activity already running. State/owner checks prevent a replaced worker from dispatching or completing its old claim. A process pause immediately after a dispatch check cannot be fenced inside Cassandra by SQL: never acknowledge uncertainty until the old process is stopped and remote tasks have been inspected. This is a conservative single-cluster POC, not exactly-once remote execution.

Worker preflight checks Cassandra health and schema before dispatch. Failure before dispatch returns work to QUEUED; failure after any dispatch records UNKNOWN. The poller catches SQL errors and retries later. Shutdown stops admission/polling, allows a five-second drain, then interrupts local work before stopping lease renewal. CLI interruption destroys only the local CLI; the remote outcome stays uncertain. Logs remain under .local/operations on the executing host.

## Run two local application copies

From this directory, start the Cassandra cluster as usual and a small shared PostgreSQL container:

```powershell
docker compose up -d --wait --wait-timeout 900
docker compose -f compose.yml -f compose-queue.yml up -d --wait control-plane-db
mvn -B '-Dmaven.repo.local=.m2' package
```

In each of two terminals, set the shared datasource overrides while retaining the local Cassandra configuration:

```powershell
$env:SPRING_PROFILES_ACTIVE='local'
$env:SPRING_DATASOURCE_URL='jdbc:postgresql://localhost:15432/dbaas'
$env:SPRING_DATASOURCE_USERNAME='dbaas'
$env:SPRING_DATASOURCE_PASSWORD='queue-demo-only'
$env:SPRING_DATASOURCE_DRIVER_CLASS_NAME='org.postgresql.Driver'
```

Terminal A:

```powershell
java -Xmx256m -jar target/dbaas-demo-1.0.0.jar --server.port=8088
```

Terminal B:

```powershell
java -Xmx256m -jar target/dbaas-demo-1.0.0.jar --server.port=8089
```

Both processes must run from this directory and target the same Cassandra cluster and PostgreSQL database. H2 remains the default for one process only. Each Java heap is capped at 256 MiB and PostgreSQL container at 512 MiB; monitor total usage with docker stats. No broker or additional service is required.

Submit with an Idempotency-Key to either instance and retrieve status through the other. Repeated submission with the same body/key must return the same operation ID. Distinct accepted operations can wait in QUEUED; only one is dispatched at a time for this cluster. `verify-recovery.ps1` now polls CLAIMED as well as QUEUED/RUNNING.

## PostgreSQL regression test

Use a **dedicated disposable test database**, not the active demo database. The test applies schema migrations and creates operation records.

```powershell
docker run -d --name dbaas-queue-test --memory 512m -p 127.0.0.1:15433:5432 -e POSTGRES_PASSWORD=queue-demo-only postgres:17.6
docker exec dbaas-queue-test pg_isready -U postgres
$env:DBAAS_QUEUE_TEST_JDBC_URL='jdbc:postgresql://localhost:15433/postgres'
mvn -B '-Dmaven.repo.local=.m2' test
Remove-Item Env:DBAAS_QUEUE_TEST_JDBC_URL
docker rm -f dbaas-queue-test
```

The optional PostgreSQL test uses two separate datasources/transactions to race claims, then exercises undispatched expiry, stale-owner rejection, uncertain dispatch and acknowledgement. Default tests exercise the same state model on H2. These tests do not constitute 1000-cluster throughput testing or a multi-host maintenance runner.

Still single-cluster: adding replicas makes admission/status available through multiple copies and allows queued work to survive an accepting-process failure. It does not increase repair concurrency for that cluster. Cluster-scoped locks/routing, asynchronous provisioning, centralized logs and fleet capacity remain future work.
