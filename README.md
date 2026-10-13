# Mini Cassandra DBaaS platform

Java 21+ / Spring Boot control plane for a three-node Cassandra cluster, following
the [shared proposal](https://chatgpt.com/share/6acc39ef-d5b4-83e9-8c58-fcfdb3ca69e2). Includes tenant provisioning, RF3
replication, LOCAL_QUORUM writes and reads, failure/recovery, and approved asynchronous repair.

## Profiles and control-plane database

Configuration is YAML-based. Shared settings are in `application.yml`; environment-specific settings are in
`application-local.yml`, `application-qa.yml`, and `application-prod.yml`.

The default profile is `local`, which uses a file-backed H2 control-plane database at `.local/control-plane`. QA and
production use PostgreSQL and require `DBAAS_CONTROL_PLANE_JDBC_URL`, `DBAAS_CONTROL_PLANE_DB_USER`, and
`DBAAS_CONTROL_PLANE_DB_PASSWORD` in production. Select a profile with `SPRING_PROFILES_ACTIVE=local|qa|prod`.

Flyway applies versioned migrations from `src/main/resources/db/migration` automatically before the application starts.
For an existing pre-Flyway control-plane database, `baseline-on-migrate` records the original schema as version 1 and
applies the ownership upgrade in version 2. Fresh databases run both migrations. Use a dedicated control-plane database
or schema: baselining an unrelated non-empty schema would incorrectly treat it as version 1. Migrations are forward-only;
back up production PostgreSQL before deployment and do not edit a migration after it has shipped.

Tenant provisioning state, repair operation state, and the repair lease are persisted through the profile datasource.
Local H2 stores this state in `.local/control-plane`; QA and production use PostgreSQL.

`verify-recovery.ps1` is local-only. It refuses to run when a non-local Spring profile is active and must never be used
against QA or production.

## Run on Windows

Install/start Docker Desktop with Linux containers. Java 21+ and Maven must be on PATH. Run from this directory:

```powershell
docker compose up -d --wait --wait-timeout 900
docker compose exec -T cassandra1 nodetool status
./run-local.ps1
```

In a second PowerShell terminal in this directory:

```powershell
$admin = @{'X-Admin-Token'='local-demo-only'}
Invoke-RestMethod http://localhost:8088/cluster/health
Invoke-RestMethod http://localhost:8088/tenants/interview/keyspace -Method Post -Headers $admin
Invoke-RestMethod http://localhost:8088/tenants/interview/probe -Method Post -ContentType application/json -Body '{"value":"hello"}'
$env:SPRING_PROFILES_ACTIVE='local'
./verify-recovery.ps1
```

Each Cassandra container has a 1.5 GiB memory limit and 512 MiB JVM heap. Allow about 8 GiB for Docker/WSL on a 16 GiB
machine. Startup is sequential to avoid concurrent bootstrap. Volumes preserve node identity/data across stop/start.
The JAR is deployment-independent: Cassandra contact points, datacenter, replication, expected node count, repair nodes,
repair command, and optional address translation are configured through `application.properties` or `DBAAS_*` environment
variables. The default values target this local Compose demo. Set `DBAAS_ADDRESS_TRANSLATION_ENABLED=false` when the app can
reach the addresses advertised by Cassandra directly. For host-based Compose, configure
`DBAAS_ADDRESS_TRANSLATION_MAPPINGS` as `discovered-host=reachable-host:port,...`.

## API

| Endpoint                    | Behavior                                                                           |
|-----------------------------|------------------------------------------------------------------------------------|
| GET /cluster/health         | Driver-observed node states and quorum estimate                                    |
| GET /tenants                | Admin header; tenant inventory, bounded by `limit`                                 |
| POST /tenants/{id}/keyspace | Admin header; idempotent RF3 keyspace and probe table                              |
| POST /tenants/{id}/probe    | LOCAL_QUORUM write/read-back with one-hour TTL                                     |
| POST /cluster/repair        | Admin header; body `{"tenant":"interview","approved":true}`; 202 with operation ID |
| GET /operations             | Admin header; unresolved operations by default; `activeOnly=false` includes history |
| GET /operations/{id}        | Admin header; one operation by ID                                                  |
| POST /operations/{id}/acknowledge | Admin header; releases an UNKNOWN repair after manual verification          |

Collection endpoints default to 50 records and accept `limit=1..200`. `/operations` defaults to
`activeOnly=true`, which returns `QUEUED`, `RUNNING`, and `UNKNOWN` operations. Item lookup remains
`/operations/{id}` so collection responses always remain lists and item responses always remain single objects.

The tenant quota caps the number of tenant keyspaces at five by default; it is not a storage/throughput quota. Identifier
validation prevents CQL injection. Provisioning uses a database transaction and a 60-second ownership lease by default.
A concurrent request is rejected while that lease is active; after a worker crash or released failed attempt, a retry
repeats the safe `CREATE ... IF NOT EXISTS` statements and completes the tenant. Configure the lease with
`DBAAS_PROVISIONING_LEASE_SECONDS`.

Repair requires all three nodes observed UP, uses a database-backed ownership lease, and executes `nodetool repair -pr`
sequentially on all three nodes for the tenant keyspace. The worker renews its lease periodically. The application
reconciles an expired repair at startup and before every new repair request by marking its operation `UNKNOWN`. It keeps
the repair lock held, so an uncertain remote repair cannot overlap with a retry automatically.

An operator means the database administrator or on-call engineer responsible for the Cassandra cluster. For an
`UNKNOWN` operation, that person must inspect Cassandra logs and `nodetool compactionstats`/`nodetool netstats` on the
affected nodes and establish that the old repair completed or is no longer active. The acknowledgement is an explicit
human attestation, not an automated Cassandra check:

```powershell
$body = '{"cassandraActivityVerified":true}'
Invoke-RestMethod "http://localhost:8088/operations/<operation-id>/acknowledge" `
	-Method Post -Headers $admin -ContentType application/json -Body $body
```

Only after acknowledgement is the global repair lock released and a new repair accepted. The operation remains
`UNKNOWN` as an audit record, with its detail updated to record the acknowledgement.

## Cassandra connectivity

The control plane starts when Cassandra is unavailable, provided its SQL control-plane database is reachable and Flyway
migrations succeed. Cassandra connection attempts run outside the application startup thread and repeat every 60 seconds
by default; configure the interval with `DBAAS_RECONNECT_SECONDS`. After an initial connection, the Cassandra driver
handles temporary node outages and reconnection.

While no Cassandra session is available, `/cluster/health` remains callable and reports `UNAVAILABLE`. Provisioning,
probes, and repair requests return HTTP 503 without reserving new provisioning or repair work. Persisted operation lookup
and explicit acknowledgement remain available because they use only the SQL control-plane database. This does not make
the utility independent of PostgreSQL/H2: failure of the control-plane datasource or Flyway migration still prevents
startup.

## Failure demonstration

`verify-recovery.ps1` performs 20 write/read-back pairs in each phase: healthy, one node down, restored, and repaired.
RF3 requires two replicas for LOCAL_QUORUM. Driver health is an observation; successful quorum requests are the
availability proof. Repair reconciles replica differences; successful client read-backs alone do not prove that every
replica contains all missed writes.

The script stops cassandra3, restarts it with its volume, waits for three UP nodes, requests repair, and polls its
result. Its finally block restarts the stopped node after a failure. The initial request after failure can time out
while detection converges; inspect health before retrying the demonstration. Two down replicas should cause requests to
fail with 503.

## Scope

Run one control-plane process from this directory so the default Docker repair command finds the correct project. For
another deployment, set `DBAAS_REPAIR_COMMAND` with `{node}` and `{keyspace}` placeholders. Operation lookup,
provisioning ownership, and repair ownership survive application restarts through the control-plane database. PostgreSQL
is required for multiple control-plane servers; file-backed local H2 is not shared.

The control plane intentionally allows one repair at a time because this demo manages one Cassandra cluster. The API
dispatches repair asynchronously, but increasing its thread pool would not bypass the database repair lock. Independent
clusters would need a `cluster_id` on operations and one lock per cluster before their repairs could run in parallel.
`SELECT ... FOR UPDATE SKIP LOCKED` is not used or needed for this single-cluster policy; it would be appropriate for a
future durable queue with multiple workers claiming independent jobs.

No Spring State Machine is used. The small state model is enforced by conditional SQL ownership transitions. This does
not provide automatic repair resumption: after a worker is lost, the outcome is deliberately `UNKNOWN` and requires
manual verification. The acknowledgement endpoint cannot itself prove that remote Cassandra activity stopped. There is
also no per-cluster model, external job broker, automatic probe scheduler, rate limiter, authentication beyond the admin
token, or multi-datacenter repair orchestration. Probes occur only when `/probe` is called; they do not acquire the
control-plane locks but each call performs a LOCAL_QUORUM write and read.

All nodes share one physical host, so this proves logical node resilience, not physical HA. Local CQL authentication is
disabled; published ports and API bind to localhost. Set `DBAAS_ADMIN_TOKEN` to override the development token. No
Kafka, frontend, or cloud provisioning is included. Production needs authorization, capacity enforcement, monitoring,
fault-domain separation, and durable maintenance coordination.

```powershell
mvn -B '-Dmaven.repo.local=.m2' verify
docker compose stop  # retains data
docker compose down # retains volumes
# docker compose down -v intentionally destroys demo data
```
