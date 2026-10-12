# Mini Cassandra DBaaS platform

Java 21+ / Spring Boot control plane for a three-node Cassandra cluster, following
the [shared proposal](https://chatgpt.com/share/6acc39ef-d5b4-83e9-8c58-fcfdb3ca69e2). Includes tenant provisioning, RF3
replication, LOCAL_QUORUM writes and reads, failure/recovery, and approved asynchronous repair.

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
./verify-recovery.ps1
```

Each Cassandra container has a 1.5 GiB memory limit and 512 MiB JVM heap. Allow about 8 GiB for Docker/WSL on a 16 GiB
machine. Startup is sequential to avoid concurrent bootstrap. Volumes preserve node identity/data across stop/start.
Docker-discovered private addresses are translated to host ports 9042/9043/9044 by `LocalAddressTranslator`; change both
the translator and Compose subnet if 172.28.91.0/24 overlaps your network.

## API

| Endpoint                    | Behavior                                                                           |
|-----------------------------|------------------------------------------------------------------------------------|
| GET /cluster/health         | Driver-observed node states and quorum estimate                                    |
| POST /tenants/{id}/keyspace | Admin header; idempotent RF3 keyspace and probe table                              |
| POST /tenants/{id}/probe    | LOCAL_QUORUM write/read-back with one-hour TTL                                     |
| POST /cluster/repair        | Admin header; body `{"tenant":"interview","approved":true}`; 202 with operation ID |
| GET /operations/{id}        | QUEUED, RUNNING, SUCCEEDED, or FAILED                                              |

The tenant quota caps the number of tenant keyspaces at five; it is not a storage/throughput quota. Identifier
validation prevents CQL injection. Provisioning is serialized within one JVM. Repair requires all three nodes observed
UP, permits one repair at a time, and executes `nodetool repair -pr` sequentially on all three nodes for the tenant
keyspace. Audit changes and command logs go to `.local/operations`.

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

Run one control-plane process from this directory so Docker Compose finds the correct project. The locks and operation
lookup are JVM-local; lookup resets on restart, while audit files persist. Restart or CLI timeout during repair can
leave the remote repair running. Inspect node logs and `nodetool compactionstats` before submitting another repair.
Persistent operation recovery and distributed locks are follow-on work.

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
