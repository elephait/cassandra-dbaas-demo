# Mini Cassandra DBaaS platform

Java 21+ / Spring Boot control plane for a three-node Cassandra cluster, following
the [shared proposal](https://chatgpt.com/share/6acc39ef-d5b4-83e9-8c58-fcfdb3ca69e2). Includes tenant provisioning, RF3
replication, LOCAL_QUORUM writes and reads, failure/recovery, and approved asynchronous repair.

## Prerequisites and dependencies

### Required for the local demo

| Dependency | Requirement and purpose |
|------------|-------------------------|
| JDK | JDK **21 or later**, including `javac`; a JRE alone cannot build the project. Java 25 was used for the verified build. |
| Apache Maven | Install Maven and put `mvn` on PATH. The project has no Maven wrapper. Maven downloads the Java libraries and builds/tests the executable JAR. |
| Docker Desktop | Running **Linux container engine**, with the `docker compose` plugin supporting `--wait`. On Windows, use the WSL2 backend with WSL2 installed and hardware virtualization enabled. |
| PowerShell | Required for `run-local.ps1`, `verify-recovery.ps1`, and the documented `Invoke-RestMethod` commands. |
| Cassandra | Compose downloads and runs **`cassandra:5.0.5`** on three nodes. No host Cassandra installation is required. |
| Network access | Required initially for Maven dependencies and the Cassandra Docker image. Configure your proxy if downloads are blocked. Cached dependencies/images allow later offline use. |
| Local resources | A 16 GiB host with an SSD is the demo target; allocate roughly 8 GiB to Docker/WSL. Each Cassandra container is capped at 1.5 GiB with a 512 MiB heap; the launcher caps the API heap at 384 MiB. Leave disk space for Maven cache, images, Cassandra volumes, and the H2 database. |

Set `JAVA_HOME` to your JDK installation and ensure its `bin` directory, Maven's `bin`, and Docker CLI are on PATH.
Run from the project directory so relative database paths and Docker Compose repair commands resolve correctly.
Ports **8088**, **9042**, **9043**, and **9044** must be available. The Docker subnet **172.28.91.0/24** must not overlap
another local network; changing it also requires updating the configured address-translation mappings.

Check the tools before starting:

```powershell
java -version
javac -version
mvn -version
docker version
docker compose version
docker info
$PSVersionTable.PSVersion
```

`mvn -version` shows the actual JDK Maven uses; check that it matches the intended installation.
`docker info` must include a **Server** section with `OSType: linux`; client information alone does not mean the engine is running.
If PowerShell blocks scripts, use a policy permitted by your machine's administrator rather than disabling protections globally.

### Java libraries supplied by Maven

These dependencies are declared in `pom.xml` and do not need separate installation:

| Library | Purpose |
|---------|---------|
| Spring Boot **4.1.1**, Spring MVC, embedded Tomcat | REST API, configuration, application lifecycle, and HTTP server. |
| Spring JDBC and HikariCP | SQL control-plane persistence and connection pooling. |
| Flyway and its PostgreSQL database module | Versioned control-plane schema migrations. |
| H2 | Embedded file-backed database for the default `local` profile. No standalone H2 service is needed. |
| PostgreSQL JDBC driver | Connects QA/prod profiles to an external PostgreSQL server. |
| DataStax Java driver **4.17.0** | Cassandra sessions, topology discovery, consistency, and reconnection. |
| Lombok | Generates getters, constructors, and logging fields at build time. Its annotation processor is configured explicitly for newer JDKs. |
| Spring Boot test/JDBC test support | JUnit-based tests and test database support; used by `mvn verify`. |

Spring Boot dependency management supplies versions for libraries without explicit versions in `pom.xml`.
Cassandra's `nodetool`, `cqlsh`, and Linux shell utilities run **inside the Cassandra containers**, so you do not need
Python, `nodetool`, `cqlsh`, or a separate Linux shell installed on the Windows host.

### Optional or environment-specific tools

- **PostgreSQL server:** required for QA/prod control-plane profiles, not local. Create a dedicated database/schema and
  provide `DBAAS_CONTROL_PLANE_JDBC_URL`, `DBAAS_CONTROL_PLANE_DB_USER`, and `DBAAS_CONTROL_PLANE_DB_PASSWORD`.
  The JDBC driver is bundled, but the database server is not supplied by this Compose file.
- **Git:** needed to clone, commit, and push the project; not required to run a downloaded copy.
- **IntelliJ IDEA:** optional for editing/debugging. Enable annotation processing if its own compiler does not generate
  Lombok members; command-line Maven already configures the processor.

Prometheus/Grafana, Ansible, Kafka, Kubernetes, and Node.js are not dependencies of the current implementation.

## Profiles and control-plane database

Repair execution now uses a durable SQL-backed queue. See [QUEUE_POC.md](QUEUE_POC.md) for the state model,
two-instance PostgreSQL setup and real PostgreSQL claim/recovery tests. The default local H2 setup still supports one process.

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
repair command, and optional address translation are configured through `application.yml` and profile YAML files or `DBAAS_*` environment
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
`activeOnly=true`, which returns `QUEUED`, `CLAIMED`, `RUNNING`, and `UNKNOWN` operations. Item lookup remains
`/operations/{id}` so collection responses always remain lists and item responses always remain single objects.

The tenant quota caps the number of tenant keyspaces at five by default; it is not a storage/throughput quota. Identifier
validation prevents CQL injection. Provisioning uses a database transaction and a 60-second ownership lease by default.
Provisioning validates actual Cassandra replication and the probe table before returning success, including for a tenant
already recorded as READY. It requires NetworkTopologyStrategy with exactly the configured datacenter/RF and a probe table
with `id uuid PRIMARY KEY` and `value text`. Schema disagreement returns 503; incompatible or missing schema returns 409.
An existing RF1 keyspace is rejected before table creation. No replication alteration or destructive reconciliation is performed.
If external schema drift occurs, the stored tenant record is historical; the provisioning endpoint revalidates the live schema.
A concurrent request is rejected while that lease is active; after a worker crash or released failed attempt, a retry
repeats the safe `CREATE ... IF NOT EXISTS` statements and completes the tenant. Configure the lease with
`DBAAS_PROVISIONING_LEASE_SECONDS`.

Repair requests require approval and an Idempotency-Key, and are persisted as QUEUED before returning 202.
Workers poll SQL every two seconds and claim undispatched work atomically. Only one repair executes at a time for this
single cluster. Claims and lease decisions use database time; replaced owners cannot dispatch or complete old claims.
Expired CLAIMED work is safely requeued. Expired RUNNING work becomes UNKNOWN and keeps the repair lock because remote
Cassandra activity may continue. After verifying that the old worker and remote tasks have stopped, explicitly acknowledge
UNKNOWN through POST /operations/{id}/acknowledge with cassandraActivityVerified=true. Acknowledgement is idempotent and
records ACKNOWLEDGED. CLI timeouts and failures never cause automatic retry of possibly dispatched work.

The same Idempotency-Key and approved tenant repair return the original operation; different content returns 409.
Use a new key for an intentional later repair. Worker preflight requires all expected nodes UP and compatible schema.
Commands run outside SQL transactions. Full setup, limitations and examples are in [QUEUE_POC.md](QUEUE_POC.md).

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

Run local control-plane processes from this directory so the default Docker repair command finds the correct project. For
another deployment, set `DBAAS_REPAIR_COMMAND` with `{node}` and `{keyspace}` placeholders. Operation lookup,
provisioning ownership, and repair ownership survive application restarts through the control-plane database. PostgreSQL
is required for multiple control-plane servers; file-backed local H2 is not shared.

The control plane intentionally allows one repair at a time because this demo manages one Cassandra cluster. The API
queues repair durably, but increasing its thread pool would not bypass the database repair lock. Independent
clusters would need a `cluster_id` on operations and one lock per cluster before their repairs could run in parallel.
`SELECT ... FOR UPDATE SKIP LOCKED` now implements durable claims. The shared cluster lock still prevents overlapping
repairs against this one cluster.

No Spring State Machine is used. The small state model is enforced by conditional SQL ownership transitions. This does
not automatically retry possibly dispatched work: expired RUNNING work becomes `UNKNOWN` and requires manual verification.
Undispatched CLAIMED work can be safely requeued. The acknowledgement endpoint cannot prove remote tasks stopped. There is
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

