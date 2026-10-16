<# : batch part (cmd.exe runs these lines; PowerShell sees a comment)
@echo off
set "DBAAS_HOME=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "& ([scriptblock]::Create([IO.File]::ReadAllText('%~f0'))) %*"
exit /b %ERRORLEVEL%
#>
# PowerShell part. cmd.exe launches it with a process-scoped execution-policy bypass, so it runs whatever
# the machine's policy is.
param(
    [switch]$Recovery,
    [string]$BaseUrl = 'http://localhost:8088',
    [string]$AdminToken = 'local-demo-only',
    [ValidateRange(1, 300)][int]$PollIntervalSeconds = 5
)
# Default: build, unit tests and integration tests (live JMX against the Compose cluster, PostgreSQL via
# Testcontainers). Safe while the app runs: run-local.cmd runs a copy of the jar.
# -Recovery: exercises the RUNNING local application: node down, quorum still served, recovery, repair.
$ErrorActionPreference = 'Stop'
Push-Location $env:DBAAS_HOME
$startedServices = @()
$recoveryStopped = $false
function Invoke-DockerChecked([string[]]$Arguments) {
    & docker.exe @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker failed: $Arguments" }
}
function Probe([string]$Phase, [hashtable]$Headers) {
    1..20 | ForEach-Object {
        $value = "$Phase-$_"
        $result = Invoke-RestMethod "$BaseUrl/clusters/default/keyspaces/interview/probe" -Method Post `
            -Headers $Headers -ContentType application/json -Body (@{value=$value} | ConvertTo-Json)
        if ($result.value -ne $value -or $result.consistency -ne 'LOCAL_QUORUM') { throw 'Probe mismatch' }
        Start-Sleep -Milliseconds 250
    }
    Write-Host "$Phase : quorum writes and read-backs passed"
}
try {
    if ($Recovery) {
        $headers = @{'X-Admin-Token'=$AdminToken}
        Invoke-RestMethod "$BaseUrl/clusters/default/keyspaces/interview" -Method Post -Headers $headers | Out-Null
        Probe 'healthy' $headers
        Invoke-DockerChecked @('compose', 'stop', 'cassandra3')
        $recoveryStopped = $true
        Probe 'one-node-down' $headers
        Invoke-DockerChecked @('compose', 'start', 'cassandra3')
        $deadline = (Get-Date).AddMinutes(10)
        do {
            Start-Sleep -Seconds $PollIntervalSeconds
            $health = Invoke-RestMethod "$BaseUrl/clusters/default/health"
        } while ($health.up -ne 3 -and (Get-Date) -lt $deadline)
        if ($health.up -ne 3) { throw 'Node recovery timed out' }
        $recoveryStopped = $false
        Probe 'recovered' $headers
        $headers['Idempotency-Key'] = [guid]::NewGuid().ToString()
        $operation = Invoke-RestMethod "$BaseUrl/clusters/default/repair" -Method Post -Headers $headers `
            -ContentType application/json -Body '{"keyspace":"interview","approved":true}'
        $deadline = (Get-Date).AddMinutes(45)
        do {
            Start-Sleep -Seconds $PollIntervalSeconds
            $status = Invoke-RestMethod "$BaseUrl/operations/$($operation.id)" -Headers $headers
        } while ($status.state -in @('QUEUED','CLAIMED','RUNNING') -and (Get-Date) -lt $deadline)
        if ($status.state -ne 'SUCCEEDED') { throw "Repair did not succeed: $($status | ConvertTo-Json -Compress)" }
        Probe 'after-repair' $headers
        Write-Host 'PASS: node failure, recovery and repair'
        return
    }

    $running = @(& docker.exe compose ps --status running --services)
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect the local Compose cluster' }
    $startedServices = @('cassandra1','cassandra2','cassandra3') | Where-Object { $_ -notin $running }
    Invoke-DockerChecked @('compose', 'up', '-d', '--wait', '--wait-timeout', '900')
    & mvn -B -Pintegration '-Djmx.live=true' verify
    if ($LASTEXITCODE -ne 0) { throw 'Verification failed; see target/surefire-reports and target/failsafe-reports' }
    # Same endpoint discovery run-local.cmd uses.
    & java -cp target/classes demo.platform.repair.jmx.LocalJmxDiscovery .local/jmx-local.properties
    if ($LASTEXITCODE -ne 0) { throw 'Local JMX endpoint discovery failed' }
    Write-Host 'PASS: unit tests, live JMX repair workflow, provisioning concurrency and PostgreSQL coordination'
} finally {
    if ($recoveryStopped) { Invoke-DockerChecked @('compose','start','cassandra3') }
    if ($startedServices.Count -gt 0) { Invoke-DockerChecked (@('compose','stop') + @($startedServices)) }
    Pop-Location
}
